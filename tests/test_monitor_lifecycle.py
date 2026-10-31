import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
import redis
import redis.asyncio as aioredis
from redis.asyncio.connection import Connection as AsyncConnection
from redis.connection import Connection
from redis.event import (
    AfterConnectionReleasedEvent,
    AsyncAfterConnectionReleasedEvent,
    AsyncEventListenerInterface,
    EventDispatcher,
    EventListenerInterface,
)
from redis.exceptions import ConnectionError, NoPermissionError, RedisError


@pytest.mark.fixed_client
async def test_async_monitor_retry_after_cancelled_entry_reserves_connection(
    monkeypatch,
):
    monkeypatch.setattr(AsyncConnection, "connect", AsyncMock())
    monkeypatch.setattr(AsyncConnection, "can_read", AsyncMock(return_value=False))
    monkeypatch.setattr(AsyncConnection, "send_command", AsyncMock())
    reading = asyncio.Event()

    async def read_response(*args, **kwargs):
        if not reading.is_set():
            reading.set()
            await asyncio.Event().wait()
        return b"OK"

    monkeypatch.setattr(AsyncConnection, "read_response", read_response)
    pool = aioredis.ConnectionPool(max_connections=1)
    monitor = aioredis.Redis(connection_pool=pool).monitor()

    async def enter():
        async with monitor:
            pytest.fail("cancelled MONITOR entered its context")

    task = asyncio.create_task(enter())
    await asyncio.wait_for(reading.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    async with monitor:
        with pytest.raises(redis.MaxConnectionsError):
            await pool.get_connection()
    connection = await pool.get_connection()
    await pool.release(connection)
    await pool.aclose()


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


class _FailingReleaseListener(EventListenerInterface):
    def listen(self, event):
        raise RuntimeError("release listener failed")


class _AsyncFailingReleaseListener(AsyncEventListenerInterface):
    async def listen(self, event):
        raise RuntimeError("release listener failed")


@pytest.mark.fixed_client
@pytest.mark.parametrize("release_error", [False, True])
def test_monitor_cleanup_interrupt_is_not_replaced_by_entry_error(
    monkeypatch, release_error
):
    monkeypatch.setattr(Connection, "connect", Mock())
    monkeypatch.setattr(Connection, "can_read", Mock(return_value=False))
    monkeypatch.setattr(
        Connection,
        "send_command",
        Mock(side_effect=NoPermissionError("MONITOR is not permitted")),
    )
    dispatcher = EventDispatcher()
    listener = _FailingReleaseListener()
    if release_error:
        dispatcher.register_listeners({AfterConnectionReleasedEvent: [listener]})
    pool = redis.ConnectionPool(max_connections=1, event_dispatcher=dispatcher)
    client = redis.Redis(connection_pool=pool)
    monitor = client.monitor()
    disconnect = Mock(side_effect=KeyboardInterrupt())
    monkeypatch.setattr(monitor.connection, "disconnect", disconnect)

    try:
        with pytest.raises(KeyboardInterrupt):
            with monitor:
                pytest.fail("failed MONITOR must not enter its context")
        connection = pool.get_connection()
        if release_error:
            with pytest.raises(RuntimeError, match="release listener failed"):
                pool.release(connection)
        else:
            pool.release(connection)
    finally:
        if release_error:
            dispatcher.unregister_listeners({AfterConnectionReleasedEvent: [listener]})
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
@pytest.mark.parametrize("failures", [1, 2])
def test_monitor_retry_reserves_connection(monkeypatch, failures):
    monkeypatch.setattr(Connection, "connect", Mock())
    monkeypatch.setattr(Connection, "can_read", Mock(return_value=False))
    monkeypatch.setattr(Connection, "send_command", Mock())
    monkeypatch.setattr(
        Connection,
        "read_response",
        Mock(side_effect=[NoPermissionError("MONITOR denied")] * failures + [b"OK"]),
    )
    pool = redis.ConnectionPool(max_connections=1)
    monitor = redis.Redis(connection_pool=pool).monitor()
    for _ in range(failures):
        with pytest.raises(NoPermissionError):
            with monitor:
                pytest.fail("failed MONITOR entered its context")
    with monitor:
        with pytest.raises(redis.MaxConnectionsError):
            pool.get_connection()
    # A second exit must not return or disconnect a different borrower's lease.
    connection = pool.get_connection()
    disconnect = Mock()
    monkeypatch.setattr(connection, "disconnect", disconnect)
    monitor.__exit__()
    disconnect.assert_not_called()
    with pytest.raises(redis.MaxConnectionsError):
        pool.get_connection()
    pool.release(connection)
    pool.disconnect()


@pytest.mark.fixed_client
@pytest.mark.parametrize("failures", [1, 2])
async def test_async_monitor_retry_reserves_connection(monkeypatch, failures):
    monkeypatch.setattr(AsyncConnection, "connect", AsyncMock())
    monkeypatch.setattr(AsyncConnection, "can_read", AsyncMock(return_value=False))
    monkeypatch.setattr(AsyncConnection, "send_command", AsyncMock())
    monkeypatch.setattr(
        AsyncConnection,
        "read_response",
        AsyncMock(
            side_effect=[NoPermissionError("MONITOR denied")] * failures + [b"OK"]
        ),
    )
    pool = aioredis.ConnectionPool(max_connections=1)
    monitor = aioredis.Redis(connection_pool=pool).monitor()
    for _ in range(failures):
        with pytest.raises(NoPermissionError):
            async with monitor:
                pytest.fail("failed MONITOR entered its context")
    async with monitor:
        with pytest.raises(redis.MaxConnectionsError):
            await pool.get_connection()
    connection = await pool.get_connection()
    disconnect = AsyncMock()
    monkeypatch.setattr(connection, "disconnect", disconnect)
    await monitor.__aexit__()
    disconnect.assert_not_awaited()
    with pytest.raises(redis.MaxConnectionsError):
        await pool.get_connection()
    await pool.release(connection)
    await pool.aclose()


@pytest.mark.fixed_client
def test_monitor_retry_checkout_failure_does_not_touch_other_borrower(monkeypatch):
    monkeypatch.setattr(Connection, "connect", Mock())
    monkeypatch.setattr(Connection, "can_read", Mock(return_value=False))
    send = Mock()
    monkeypatch.setattr(Connection, "send_command", send)
    read = Mock(side_effect=NoPermissionError("MONITOR denied"))
    monkeypatch.setattr(Connection, "read_response", read)
    pool = redis.ConnectionPool(max_connections=1)
    monitor = redis.Redis(connection_pool=pool).monitor()
    with pytest.raises(NoPermissionError):
        with monitor:
            pytest.fail("failed MONITOR entered its context")
    connection = pool.get_connection()
    disconnect = Mock()
    monkeypatch.setattr(connection, "disconnect", disconnect)
    send.reset_mock()
    with pytest.raises(redis.MaxConnectionsError):
        with monitor:
            pytest.fail("MONITOR entered without acquiring a connection")
    send.assert_not_called()
    disconnect.assert_not_called()
    with pytest.raises(redis.MaxConnectionsError):
        pool.get_connection()
    pool.release(connection)
    pool.disconnect()


@pytest.mark.fixed_client
async def test_async_monitor_retry_checkout_failure_does_not_touch_other_borrower(
    monkeypatch,
):
    monkeypatch.setattr(AsyncConnection, "connect", AsyncMock())
    monkeypatch.setattr(AsyncConnection, "can_read", AsyncMock(return_value=False))
    send = AsyncMock()
    monkeypatch.setattr(AsyncConnection, "send_command", send)
    monkeypatch.setattr(
        AsyncConnection,
        "read_response",
        AsyncMock(side_effect=NoPermissionError("MONITOR denied")),
    )
    pool = aioredis.ConnectionPool(max_connections=1)
    monitor = aioredis.Redis(connection_pool=pool).monitor()
    with pytest.raises(NoPermissionError):
        async with monitor:
            pytest.fail("failed MONITOR entered its context")
    connection = await pool.get_connection()
    disconnect = AsyncMock()
    monkeypatch.setattr(connection, "disconnect", disconnect)
    send.reset_mock()
    with pytest.raises(redis.MaxConnectionsError):
        async with monitor:
            pytest.fail("MONITOR entered without acquiring a connection")
    send.assert_not_awaited()
    disconnect.assert_not_awaited()
    with pytest.raises(redis.MaxConnectionsError):
        await pool.get_connection()
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
@pytest.mark.parametrize("cleanup_stage", ["disconnect", "release"])
@pytest.mark.parametrize("release_error", [False, True])
async def test_async_monitor_cancellation_during_failed_entry_cleanup(
    monkeypatch, cleanup_stage, release_error
):
    monkeypatch.setattr(AsyncConnection, "connect", AsyncMock())
    monkeypatch.setattr(AsyncConnection, "can_read", AsyncMock(return_value=False))
    monkeypatch.setattr(AsyncConnection, "send_command", AsyncMock())
    dispatcher = EventDispatcher()
    listener = _AsyncFailingReleaseListener()
    if release_error:
        dispatcher.register_listeners({AsyncAfterConnectionReleasedEvent: [listener]})
    pool = aioredis.ConnectionPool(max_connections=1, event_dispatcher=dispatcher)
    client = aioredis.Redis(connection_pool=pool)
    cleaning = asyncio.Event()
    released = asyncio.Event()
    original_release = pool.release
    held_lock = False

    async def read_response(*args, **kwargs):
        nonlocal held_lock
        if cleanup_stage == "release":
            # Acquire only after the monitor has borrowed its connection.
            await pool._lock.acquire()
            held_lock = True
        raise NoPermissionError("MONITOR is not permitted")

    async def disconnect(*args, **kwargs):
        if cleanup_stage == "disconnect":
            cleaning.set()
            await asyncio.Event().wait()

    async def release(connection):
        if cleanup_stage == "release":
            cleaning.set()
        try:
            await original_release(connection)
        finally:
            released.set()

    monkeypatch.setattr(AsyncConnection, "read_response", read_response)
    monkeypatch.setattr(AsyncConnection, "disconnect", disconnect)
    monkeypatch.setattr(pool, "release", release)

    async def monitor():
        async with client.monitor():
            pytest.fail("failed MONITOR must not enter its context")

    task = asyncio.create_task(monitor())
    try:
        await asyncio.wait_for(cleaning.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        if held_lock:
            pool._lock.release()
            held_lock = False
        await asyncio.wait_for(released.wait(), 1)
        connection = await pool.get_connection()
        if release_error:
            with pytest.raises(RuntimeError, match="release listener failed"):
                await original_release(connection)
        else:
            await original_release(connection)
    finally:
        if held_lock:
            pool._lock.release()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if release_error:
            dispatcher.unregister_listeners(
                {AsyncAfterConnectionReleasedEvent: [listener]}
            )
        monkeypatch.setattr(AsyncConnection, "disconnect", AsyncMock())
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
