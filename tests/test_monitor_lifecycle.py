import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
import redis
import redis.asyncio as aioredis
from redis.asyncio.connection import Connection as AsyncConnection
from redis.connection import Connection
from redis.exceptions import ConnectionError, NoPermissionError, RedisError


@pytest.mark.fixed_client
@pytest.mark.parametrize("failure_stage", ["send", "read", "response"])
def test_monitor_failed_entry_releases_connection(monkeypatch, failure_stage):
    monkeypatch.setattr(Connection, "connect", Mock())
    monkeypatch.setattr(Connection, "can_read", Mock(return_value=False))
    send = Mock()
    read = Mock(return_value=b"OK")
    if failure_stage == "send":
        send.side_effect = ConnectionError("connection lost")
    elif failure_stage == "read":
        read.side_effect = NoPermissionError("MONITOR is not permitted")
    else:
        read.return_value = b"unexpected"
    monkeypatch.setattr(Connection, "send_command", send)
    monkeypatch.setattr(Connection, "read_response", read)
    pool = redis.ConnectionPool(max_connections=1)
    client = redis.Redis(connection_pool=pool)

    with pytest.raises(RedisError):
        with client.monitor():
            pytest.fail("failed MONITOR must not enter its context")

    connection = pool.get_connection()
    pool.release(connection)
    pool.disconnect()


@pytest.mark.fixed_client
def test_monitor_entry_preserves_error_when_disconnect_fails(monkeypatch):
    monkeypatch.setattr(Connection, "connect", Mock())
    monkeypatch.setattr(Connection, "can_read", Mock(return_value=False))
    failure = NoPermissionError("MONITOR is not permitted")
    monkeypatch.setattr(Connection, "send_command", Mock(side_effect=failure))
    pool = redis.ConnectionPool(max_connections=1)
    client = redis.Redis(connection_pool=pool)
    monitor = client.monitor()
    disconnect = Mock(side_effect=ConnectionError("disconnect failed"))
    monkeypatch.setattr(monitor.connection, "disconnect", disconnect)

    with pytest.raises(NoPermissionError) as raised:
        with monitor:
            pytest.fail("failed MONITOR must not enter its context")

    assert raised.value is failure
    disconnect.assert_called_once()
    connection = pool.get_connection()
    pool.release(connection)
    disconnect.side_effect = None
    pool.disconnect()


@pytest.mark.fixed_client
@pytest.mark.parametrize("failure_stage", ["send", "read", "response"])
async def test_async_monitor_failed_entry_releases_connection(
    monkeypatch, failure_stage
):
    monkeypatch.setattr(AsyncConnection, "connect", AsyncMock())
    monkeypatch.setattr(AsyncConnection, "can_read", AsyncMock(return_value=False))
    send = AsyncMock()
    read = AsyncMock(return_value=b"OK")
    if failure_stage == "send":
        send.side_effect = ConnectionError("connection lost")
    elif failure_stage == "read":
        read.side_effect = NoPermissionError("MONITOR is not permitted")
    else:
        read.return_value = b"unexpected"
    monkeypatch.setattr(AsyncConnection, "send_command", send)
    monkeypatch.setattr(AsyncConnection, "read_response", read)
    pool = aioredis.ConnectionPool(max_connections=1)
    client = aioredis.Redis(connection_pool=pool)

    with pytest.raises(RedisError):
        async with client.monitor():
            pytest.fail("failed MONITOR must not enter its context")

    connection = await pool.get_connection()
    await pool.release(connection)
    await pool.aclose()


@pytest.mark.fixed_client
async def test_async_monitor_entry_preserves_error_when_disconnect_fails(monkeypatch):
    monkeypatch.setattr(AsyncConnection, "connect", AsyncMock())
    monkeypatch.setattr(AsyncConnection, "can_read", AsyncMock(return_value=False))
    failure = NoPermissionError("MONITOR is not permitted")
    monkeypatch.setattr(AsyncConnection, "send_command", AsyncMock(side_effect=failure))
    pool = aioredis.ConnectionPool(max_connections=1)
    client = aioredis.Redis(connection_pool=pool)
    monitor = client.monitor()
    await monitor.connect()
    disconnect = AsyncMock(side_effect=ConnectionError("disconnect failed"))
    monkeypatch.setattr(monitor.connection, "disconnect", disconnect)

    with pytest.raises(NoPermissionError) as raised:
        async with monitor:
            pytest.fail("failed MONITOR must not enter its context")

    assert raised.value is failure
    disconnect.assert_awaited_once()
    connection = await pool.get_connection()
    await pool.release(connection)
    disconnect.side_effect = None
    await pool.aclose()


@pytest.mark.fixed_client
async def test_async_monitor_cancelled_entry_releases_connection(monkeypatch):
    monkeypatch.setattr(AsyncConnection, "connect", AsyncMock())
    monkeypatch.setattr(AsyncConnection, "can_read", AsyncMock(return_value=False))
    monkeypatch.setattr(AsyncConnection, "send_command", AsyncMock())
    reading = asyncio.Event()

    async def read_response(*args, **kwargs):
        reading.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(AsyncConnection, "read_response", read_response)
    pool = aioredis.ConnectionPool(max_connections=1)
    client = aioredis.Redis(connection_pool=pool)

    async def monitor():
        async with client.monitor():
            pytest.fail("cancelled MONITOR must not enter its context")

    task = asyncio.create_task(monitor())
    await asyncio.wait_for(reading.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    connection = await pool.get_connection()
    await pool.release(connection)
    await pool.aclose()


@pytest.mark.fixed_client
def test_monitor_successful_context_releases_connection(monkeypatch):
    monkeypatch.setattr(Connection, "connect", Mock())
    monkeypatch.setattr(Connection, "can_read", Mock(return_value=False))
    monkeypatch.setattr(Connection, "send_command", Mock())
    monkeypatch.setattr(Connection, "read_response", Mock(return_value=b"OK"))
    pool = redis.ConnectionPool(max_connections=1)
    client = redis.Redis(connection_pool=pool)
    with client.monitor():
        with pytest.raises(redis.MaxConnectionsError):
            pool.get_connection()
    connection = pool.get_connection()
    pool.release(connection)
    pool.disconnect()


@pytest.mark.fixed_client
async def test_async_monitor_successful_context_releases_connection(monkeypatch):
    monkeypatch.setattr(AsyncConnection, "connect", AsyncMock())
    monkeypatch.setattr(AsyncConnection, "can_read", AsyncMock(return_value=False))
    monkeypatch.setattr(AsyncConnection, "send_command", AsyncMock())
    monkeypatch.setattr(AsyncConnection, "read_response", AsyncMock(return_value=b"OK"))
    pool = aioredis.ConnectionPool(max_connections=1)
    client = aioredis.Redis(connection_pool=pool)
    async with client.monitor():
        with pytest.raises(redis.MaxConnectionsError):
            await pool.get_connection()
    connection = await pool.get_connection()
    await pool.release(connection)
    await pool.aclose()
