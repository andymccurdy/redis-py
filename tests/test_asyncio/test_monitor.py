import asyncio
from unittest.mock import AsyncMock

import pytest
import redis.asyncio as redis
from redis.event import (
    AsyncAfterConnectionReleasedEvent,
    AsyncEventListenerInterface,
    EventDispatcher,
)
from redis.exceptions import MaxConnectionsError, NoPermissionError
from tests.conftest import (
    skip_if_redis_enterprise,
    skip_ifnot_redis_enterprise,
    skip_if_server_version_lt,
)

from .conftest import wait_for_command


@pytest.mark.onlynoncluster
class TestMonitor:
    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    @pytest.mark.parametrize("entry_failure", [False, True])
    async def test_release_error_does_not_restore_handled_interrupt(
        self, r, entry_failure
    ):
        username = "async-ambient-monitor"
        await r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            nopass=True,
            commands=["+ping"] if entry_failure else ["+ping", "+monitor"],
        )
        dispatcher = EventDispatcher()
        listener = AsyncMock(spec=AsyncEventListenerInterface)
        release_error = RuntimeError("release listener failed")
        listener.listen.side_effect = release_error
        pool = redis.ConnectionPool(
            **{
                **r.connection_pool.connection_kwargs,
                "username": username,
                "password": None,
                "event_dispatcher": dispatcher,
            },
            max_connections=1,
        )
        client = redis.Redis(connection_pool=pool)
        dispatcher.register_listeners({AsyncAfterConnectionReleasedEvent: [listener]})
        observed = None
        try:
            try:
                try:
                    raise KeyboardInterrupt()
                except KeyboardInterrupt:
                    async with client.monitor():
                        pass
            except BaseException as error:
                observed = error
            if entry_failure:
                assert isinstance(observed, NoPermissionError)
            else:
                assert observed is release_error
            listener.listen.assert_awaited_once()
            dispatcher.unregister_listeners(
                {AsyncAfterConnectionReleasedEvent: [listener]}
            )
            assert await client.ping()
        finally:
            dispatcher.unregister_listeners(
                {AsyncAfterConnectionReleasedEvent: [listener]}
            )
            await client.aclose()
            await pool.disconnect()
            await r.acl_deluser(username)

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    @pytest.mark.parametrize("interrupt_cleanup", [False, True])
    async def test_failed_entry_releases_connection(
        self, r, monkeypatch, interrupt_cleanup
    ):
        username = "async-monitor-no-permission"
        await r.acl_setuser(
            username, enabled=True, reset=True, nopass=True, commands=["+ping"]
        )
        dispatcher = EventDispatcher()
        pool = redis.ConnectionPool(
            **{
                **r.connection_pool.connection_kwargs,
                "username": username,
                "password": None,
                "event_dispatcher": dispatcher,
            },
            max_connections=1,
        )
        client = redis.Redis(connection_pool=pool)
        task = None
        listener = AsyncMock(spec=AsyncEventListenerInterface)
        listener.listen.side_effect = RuntimeError("release listener failed")
        try:
            monitor = client.monitor()
            await monitor.connect()
            disconnecting = asyncio.Event()
            cancellation = []

            async def wait_closed():
                disconnecting.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError as error:
                    cancellation.append(error)
                    raise

            async def enter():
                async with monitor:
                    pytest.fail("MONITOR must not enter without permission")

            if interrupt_cleanup:
                dispatcher.register_listeners(
                    {AsyncAfterConnectionReleasedEvent: [listener]}
                )
                monkeypatch.setattr(
                    monitor.connection._writer, "wait_closed", wait_closed
                )
            task = asyncio.create_task(enter())
            if interrupt_cleanup:
                await asyncio.wait_for(disconnecting.wait(), 5)
                task.cancel()
            with pytest.raises(
                asyncio.CancelledError if interrupt_cleanup else NoPermissionError
            ) as raised:
                await task
            if interrupt_cleanup:
                assert task.cancelled()
                assert raised.value is cancellation[0]
                listener.listen.assert_awaited_once()
                dispatcher.unregister_listeners(
                    {AsyncAfterConnectionReleasedEvent: [listener]}
                )
            assert await client.ping()

            await r.acl_setuser(username, enabled=True, commands=["+monitor"])
            async with monitor:
                with pytest.raises(MaxConnectionsError):
                    await client.ping()
            assert await client.ping()
        finally:
            dispatcher.unregister_listeners(
                {AsyncAfterConnectionReleasedEvent: [listener]}
            )
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await client.aclose()
            await pool.disconnect()
            await r.acl_deluser(username)

    async def test_cancelled_entry_releases_connection(self, r, monkeypatch):
        pool = redis.ConnectionPool(
            **r.connection_pool.connection_kwargs, max_connections=1
        )
        client = redis.Redis(connection_pool=pool)
        monitor = client.monitor()
        await monitor.connect()
        connection = monitor.connection
        reading = asyncio.Event()
        read_response = connection.read_response

        async def wait_for_response(*args, **kwargs):
            # MONITOR was sent on the real connection; hold its reply for cancellation.
            reading.set()
            await asyncio.Event().wait()
            return await read_response(*args, **kwargs)

        monkeypatch.setattr(connection, "read_response", wait_for_response)
        task = asyncio.create_task(monitor.__aenter__())
        try:
            await asyncio.wait_for(reading.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            monkeypatch.setattr(connection, "read_response", read_response)
            assert await client.ping()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            monkeypatch.setattr(connection, "read_response", read_response)
            await monitor.__aexit__()
            await client.aclose()
            await pool.disconnect()

    async def test_wait_command_not_found(self, r):
        """Make sure the wait_for_command func works when command is not found"""
        async with r.monitor() as m:
            response = await wait_for_command(r, m, "nothing")
            assert response is None

    async def test_response_values(self, r):
        db = r.connection_pool.connection_kwargs.get("db", 0)
        async with r.monitor() as m:
            await r.ping()
            response = await wait_for_command(r, m, "PING")
            assert isinstance(response["time"], float)
            assert response["db"] == db
            assert response["client_type"] in ("tcp", "unix")
            assert isinstance(response["client_address"], str)
            assert isinstance(response["client_port"], str)
            assert response["command"] == "PING"

    async def test_command_with_quoted_key(self, r):
        async with r.monitor() as m:
            await r.get('foo"bar')
            response = await wait_for_command(r, m, 'GET foo"bar')
            assert response["command"] == 'GET foo"bar'

    async def test_command_with_binary_data(self, r):
        async with r.monitor() as m:
            byte_string = b"foo\x92"
            await r.get(byte_string)
            response = await wait_for_command(r, m, "GET foo\\x92")
            assert response["command"] == "GET foo\\x92"

    async def test_command_with_escaped_data(self, r):
        async with r.monitor() as m:
            byte_string = b"foo\\x92"
            await r.get(byte_string)
            response = await wait_for_command(r, m, "GET foo\\\\x92")
            assert response["command"] == "GET foo\\\\x92"

    @skip_if_redis_enterprise()
    async def test_lua_script(self, r):
        async with r.monitor() as m:
            script = 'return redis.call("GET", "foo")'
            assert await r.eval(script, 0) is None
            response = await wait_for_command(r, m, "GET foo")
            assert response["command"] == "GET foo"
            assert response["client_type"] == "lua"
            assert response["client_address"] == "lua"
            assert response["client_port"] == ""

    @skip_ifnot_redis_enterprise()
    async def test_lua_script_in_enterprise(self, r):
        async with r.monitor() as m:
            script = 'return redis.call("GET", "foo")'
            assert await r.eval(script, 0) is None
            response = await wait_for_command(r, m, "GET foo")
            assert response is None
