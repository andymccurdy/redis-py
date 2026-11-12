import asyncio
import socket
from unittest import mock

import pytest
import redis.asyncio
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import SentinelManagedConnection
from redis.backoff import NoBackoff
from redis.exceptions import ConnectionError, ResponseError

pytestmark = pytest.mark.asyncio


@pytest.mark.fixed_client
async def test_connect_retry_on_timeout_error(connect_args):
    """Test that the _connect function is retried in case of a timeout"""
    connection_pool = mock.AsyncMock()
    connection_pool.get_master_address = mock.AsyncMock(
        return_value=(connect_args["host"], connect_args["port"])
    )
    conn = SentinelManagedConnection(
        retry_on_timeout=True,
        retry=Retry(NoBackoff(), 3),
        connection_pool=connection_pool,
    )
    origin_connect = conn._connect
    conn._connect = mock.AsyncMock()

    async def mock_connect():
        # connect only on the last retry
        if conn._connect.call_count <= 2:
            raise socket.timeout
        else:
            return await origin_connect()

    conn._connect.side_effect = mock_connect
    await conn.connect()
    assert conn._connect.call_count == 3
    assert connection_pool.get_master_address.call_count == 3
    await conn.disconnect()


@pytest.mark.fixed_client
async def test_read_response_disconnects_on_base_exception(connect_args):
    """
    Mirror of the sync test: a BaseException at the socket read leaves the reply
    unread, so the connection must be closed rather than reused. See #1128.
    """
    connection_pool = mock.AsyncMock()
    connection_pool.get_master_address = mock.AsyncMock(
        return_value=(connect_args["host"], connect_args["port"])
    )
    connection_pool.is_master = True
    connection_pool.check_connection = False
    conn = SentinelManagedConnection(connection_pool=connection_pool)
    await conn.connect()
    try:
        await conn.send_command("PING")
        with mock.patch.object(
            conn, "_read_response_from_parser", side_effect=KeyboardInterrupt
        ):
            with pytest.raises(KeyboardInterrupt):
                await conn.read_response()
        assert conn._reader is None
    finally:
        await conn.disconnect()


DEMOTION_ERROR = (
    "UNBLOCKED force unblock from blocking operation, "
    "instance state changed (master -> replica?)"
)


async def _demoted_master_connection(connect_args):
    connection_pool = mock.AsyncMock()
    connection_pool.get_master_address = mock.AsyncMock(
        return_value=(connect_args["host"], connect_args["port"])
    )
    connection_pool.is_master = True
    connection_pool.check_connection = False
    conn = SentinelManagedConnection(connection_pool=connection_pool)
    await conn.connect()
    return conn


@pytest.mark.fixed_client
async def test_unblocked_by_demotion_is_a_connection_error(connect_args):
    """
    Mirror of the sync test: a master demoted to a replica unblocks blocked
    clients with UNBLOCKED and closes the connection, which means the same as
    READONLY for a master connection.
    """
    conn = await _demoted_master_connection(connect_args)
    try:
        await conn.send_command("PING")
        with mock.patch.object(
            conn,
            "_read_response_from_parser",
            return_value=ResponseError(DEMOTION_ERROR),
        ):
            with pytest.raises(ConnectionError) as e:
                await conn.read_response()
        assert str(e.value) == "The previous master is now a slave"
        assert not conn.is_connected
    finally:
        await conn.disconnect()


@pytest.mark.fixed_client
async def test_unblocked_by_demotion_on_a_replica_connection_is_raised(
    connect_args,
):
    conn = await _demoted_master_connection(connect_args)
    conn.connection_pool.is_master = False
    try:
        await conn.send_command("PING")
        with mock.patch.object(
            conn,
            "_read_response_from_parser",
            return_value=ResponseError(DEMOTION_ERROR),
        ):
            with pytest.raises(ResponseError) as e:
                await conn.read_response()
        assert not isinstance(e.value, ConnectionError)
        assert conn.is_connected
    finally:
        await conn.disconnect()


@pytest.mark.fixed_client
@pytest.mark.onlynoncluster
async def test_client_unblock_error_is_not_a_demotion(connect_args):
    """
    Mirror of the sync test: CLIENT UNBLOCK ... ERROR also answers UNBLOCKED,
    with another message. The server did not change role, so the error must
    reach the caller.
    """
    conn = await _demoted_master_connection(connect_args)
    admin = redis.asyncio.Redis(host=connect_args["host"], port=connect_args["port"])
    try:
        await conn.send_command("CLIENT", "ID")
        client_id = await conn.read_response()
        await conn.send_command("BLPOP", "redis-py:unblocked-test", 5)
        unblocked = False
        for _ in range(50):
            if await admin.client_unblock(client_id, error=True):
                unblocked = True
                break
            await asyncio.sleep(0.01)
        assert unblocked
        with pytest.raises(ResponseError) as e:
            await conn.read_response()
        assert str(e.value).startswith("UNBLOCKED client unblocked")
        assert not isinstance(e.value, ConnectionError)
    finally:
        await admin.aclose()
        await conn.disconnect()
