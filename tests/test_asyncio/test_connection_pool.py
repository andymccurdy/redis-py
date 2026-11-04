import asyncio
import re
from unittest import mock
from unittest.mock import patch

import pytest
import pytest_asyncio
import redis.asyncio as redis
from redis._parsers import _AsyncHiredisParser, _AsyncRESP3Parser
from redis.utils import HIREDIS_AVAILABLE
from redis.asyncio.connection import (
    BlockingConnectionPool,
    Connection,
    ConnectionPool,
    to_bool,
)
from unittest.mock import AsyncMock, MagicMock

from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.observability.metrics import CloseReason
from redis.auth.token import TokenInterface
from tests.conftest import skip_if_redis_enterprise, skip_if_server_version_lt

from .compat import aclosing
from .conftest import asynccontextmanager
from .test_pubsub import EVENT_LOOP_TIMER_SLACK, wait_for_message


def assert_kwargs_subset(actual, expected):
    for key, value in expected.items():
        assert key in actual, f"missing key {key!r} in {actual!r}"
        assert actual[key] == value, (
            f"value mismatch for {key!r}: {actual[key]!r} != {value!r}"
        )


@pytest.mark.onlynoncluster
class TestRedisAutoReleaseConnectionPool:
    @pytest_asyncio.fixture
    async def r(self, create_redis) -> redis.Redis:
        """This is necessary since r and r2 create ConnectionPools behind the scenes"""
        r = await create_redis()
        r.auto_close_connection_pool = True
        yield r

    @staticmethod
    def get_total_connected_connections(pool):
        return len(pool._available_connections) + len(pool._in_use_connections)

    @staticmethod
    async def create_two_conn(r: redis.Redis):
        if not r.single_connection_client:  # Single already initialized connection
            r.connection = await r.connection_pool.get_connection()
        return await r.connection_pool.get_connection()

    @staticmethod
    def has_no_connected_connections(pool: redis.ConnectionPool):
        return not any(
            x.is_connected
            for x in pool._available_connections + list(pool._in_use_connections)
        )

    async def test_auto_disconnect_redis_created_pool(self, r: redis.Redis):
        new_conn = await self.create_two_conn(r)
        assert new_conn != r.connection
        assert self.get_total_connected_connections(r.connection_pool) == 2
        await r.aclose()
        assert self.has_no_connected_connections(r.connection_pool)

    async def test_do_not_auto_disconnect_redis_created_pool(self, r2: redis.Redis):
        assert r2.auto_close_connection_pool is False, (
            "The connection pool should not be disconnected as a manually created "
            "connection pool was passed in in conftest.py"
        )
        new_conn = await self.create_two_conn(r2)
        assert self.get_total_connected_connections(r2.connection_pool) == 2
        await r2.aclose()
        assert r2.connection_pool._in_use_connections == {new_conn}
        assert new_conn.is_connected
        assert len(r2.connection_pool._available_connections) == 1
        assert r2.connection_pool._available_connections[0].is_connected

    async def test_auto_release_override_true_manual_created_pool(self, r: redis.Redis):
        assert r.auto_close_connection_pool is True, "This is from the class fixture"
        await self.create_two_conn(r)
        await r.aclose()
        assert self.get_total_connected_connections(r.connection_pool) == 2, (
            "The connection pool should not be disconnected as a manually created "
            "connection pool was passed in in conftest.py"
        )
        assert self.has_no_connected_connections(r.connection_pool)

    @pytest.mark.parametrize("auto_close_conn_pool", [True, False])
    async def test_close_override(self, r: redis.Redis, auto_close_conn_pool):
        r.auto_close_connection_pool = auto_close_conn_pool
        await self.create_two_conn(r)
        await r.aclose(close_connection_pool=True)
        assert self.has_no_connected_connections(r.connection_pool)

    @pytest.mark.parametrize("auto_close_conn_pool", [True, False])
    async def test_negate_auto_close_client_pool(
        self, r: redis.Redis, auto_close_conn_pool
    ):
        r.auto_close_connection_pool = auto_close_conn_pool
        new_conn = await self.create_two_conn(r)
        await r.aclose(close_connection_pool=False)
        assert not self.has_no_connected_connections(r.connection_pool)
        assert r.connection_pool._in_use_connections == {new_conn}
        assert r.connection_pool._available_connections[0].is_connected
        assert self.get_total_connected_connections(r.connection_pool) == 2


class DummyConnection(Connection):
    description_format = "DummyConnection<>"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self._connected = False

    def repr_pieces(self):
        return [("id", id(self)), ("kwargs", self.kwargs)]

    async def connect(self):
        self._connected = True

    async def disconnect(self):
        self._connected = False

    @property
    def is_connected(self):
        return self._connected

    async def can_read(self) -> bool:
        return False

    def set_re_auth_token(self, token: TokenInterface):
        pass

    async def re_auth(self):
        pass

    def should_reconnect(self):
        return False


class TestConnectionPool:
    @asynccontextmanager
    async def get_pool(
        self,
        connection_kwargs=None,
        max_connections=None,
        connection_class=redis.Connection,
    ):
        connection_kwargs = connection_kwargs or {}
        pool = redis.ConnectionPool(
            connection_class=connection_class,
            max_connections=max_connections,
            **connection_kwargs,
        )
        try:
            yield pool
        finally:
            await pool.disconnect(inuse_connections=True)

    @pytest.mark.fixed_client
    async def test_connection_creation(self):
        connection_kwargs = {"foo": "bar", "biz": "baz"}
        async with self.get_pool(
            connection_kwargs=connection_kwargs, connection_class=DummyConnection
        ) as pool:
            connection = await pool.get_connection()
            assert isinstance(connection, DummyConnection)
            assert_kwargs_subset(connection.kwargs, connection_kwargs)

    @pytest.mark.fixed_client
    async def test_aclosing(self):
        connection_kwargs = {"foo": "bar", "biz": "baz"}
        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            max_connections=None,
            **connection_kwargs,
        )
        async with aclosing(pool):
            pass

    async def test_multiple_connections(self, master_host):
        connection_kwargs = {"host": master_host[0]}
        async with self.get_pool(connection_kwargs=connection_kwargs) as pool:
            c1 = await pool.get_connection()
            c2 = await pool.get_connection()
            assert c1 != c2

    async def test_max_connections(self, master_host):
        connection_kwargs = {"host": master_host[0]}
        async with self.get_pool(
            max_connections=2, connection_kwargs=connection_kwargs
        ) as pool:
            await pool.get_connection()
            await pool.get_connection()
            with pytest.raises(redis.ConnectionError):
                await pool.get_connection()

    @pytest.mark.fixed_client
    async def test_failed_re_auth_does_not_stop_the_other_connections(self):
        async with self.get_pool(connection_class=DummyConnection) as pool:
            failing = mock.Mock(spec=Connection, retry=Retry(NoBackoff(), 0))
            failing.send_command.side_effect = redis.ConnectionError("broken")
            other = mock.Mock(spec=Connection, retry=Retry(NoBackoff(), 0))
            in_use = mock.Mock(spec=Connection)
            pool._available_connections.extend([failing, other])
            pool._in_use_connections.add(in_use)
            token = mock.Mock()
            token.try_get.return_value = "oid"
            token.get_value.return_value = "value"

            with pytest.raises(redis.ConnectionError, match="broken"):
                await pool.re_auth_callback(token)

            other.send_command.assert_awaited_once_with("AUTH", "oid", "value")
            in_use.set_re_auth_token.assert_called_once_with(token)

    @pytest.mark.fixed_client
    async def test_re_auth_stops_at_authentication_error(self):
        async with self.get_pool(connection_class=DummyConnection) as pool:
            failing = mock.Mock(spec=Connection, retry=Retry(NoBackoff(), 3))
            failing.read_response.side_effect = redis.AuthenticationError("expired")
            other = mock.Mock(spec=Connection, retry=Retry(NoBackoff(), 0))
            in_use = mock.Mock(spec=Connection)
            pool._available_connections.extend([failing, other])
            pool._in_use_connections.add(in_use)
            token = mock.Mock()
            token.try_get.return_value = "oid"
            token.get_value.return_value = "value"

            with pytest.raises(redis.AuthenticationError, match="expired"):
                await pool.re_auth_callback(token)

            failing.read_response.assert_awaited_once()
            other.send_command.assert_not_awaited()
            in_use.set_re_auth_token.assert_not_called()

    async def test_reuse_previously_released_connection(self, master_host):
        connection_kwargs = {"host": master_host[0]}
        async with self.get_pool(connection_kwargs=connection_kwargs) as pool:
            c1 = await pool.get_connection()
            await pool.release(c1)
            c2 = await pool.get_connection()
            assert c1 == c2

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("operation", ["acquire", "refresh"])
    async def test_release_reserves_connection_during_re_auth(self, operation):
        async with self.get_pool(connection_class=DummyConnection) as pool:
            connection = await pool.get_connection()
            re_auth_started = asyncio.Event()
            finish_re_auth = asyncio.Event()

            async def re_auth():
                re_auth_started.set()
                await finish_re_auth.wait()

            connection.re_auth = AsyncMock(side_effect=re_auth)
            connection.set_re_auth_token = MagicMock()
            connection.retry = MagicMock()
            connection.retry.call_with_retry = AsyncMock()
            token = MagicMock(spec=TokenInterface)
            release_task = asyncio.create_task(pool.release(connection))
            other = None
            try:
                await re_auth_started.wait()
                assert connection in pool._in_use_connections
                assert connection not in pool._available_connections
                if operation == "acquire":
                    other = await asyncio.wait_for(pool.get_connection(), timeout=1)
                    assert other is not connection
                else:
                    await asyncio.wait_for(pool.re_auth_callback(token), timeout=1)
                    connection.set_re_auth_token.assert_called_once_with(token)
                    connection.retry.call_with_retry.assert_not_awaited()
            finally:
                finish_re_auth.set()
                await release_task
                if other is not None:
                    await pool.release(other)
            assert connection not in pool._in_use_connections
            assert connection in pool._available_connections

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    async def test_release_listener_can_take_pool_lock(self, pool_class):
        async with pool_class(connection_class=DummyConnection) as pool:
            connection = await pool.get_connection()
            connection.should_reconnect = MagicMock(return_value=False)
            connection.disconnect = AsyncMock()

            async def re_auth():
                async with pool._lock:
                    assert connection in pool._in_use_connections
                    connection.should_reconnect.return_value = True

            connection.re_auth = AsyncMock(side_effect=re_auth)
            await asyncio.wait_for(pool.release(connection), timeout=1)
            connection.disconnect.assert_awaited_once()
            assert connection not in pool._in_use_connections
            assert connection in pool._available_connections

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "error", [RuntimeError("listener failed"), asyncio.CancelledError()]
    )
    async def test_release_listener_failure_returns_connection(self, error):
        async with self.get_pool(connection_class=DummyConnection) as pool:
            connection = await pool.get_connection()
            connection.re_auth = AsyncMock(side_effect=error)
            with patch(
                "redis.asyncio.connection.record_connection_count",
                new_callable=AsyncMock,
            ) as record:
                with pytest.raises(type(error)):
                    await pool.release(connection)
                assert record.await_count == 2
            assert connection not in pool._in_use_connections
            assert pool._available_connections == [connection]

    @pytest.mark.fixed_client
    async def test_repr_contains_db_info_tcp(self):
        connection_kwargs = {
            "host": "localhost",
            "port": 6379,
            "db": 1,
            "client_name": "test-client",
        }
        async with self.get_pool(
            connection_kwargs=connection_kwargs, connection_class=redis.Connection
        ) as pool:
            expected = "host=localhost,port=6379,db=1,client_name=test-client"
            assert expected in repr(pool)

    @pytest.mark.fixed_client
    async def test_repr_contains_db_info_unix(self):
        connection_kwargs = {"path": "/abc", "db": 1, "client_name": "test-client"}
        async with self.get_pool(
            connection_kwargs=connection_kwargs,
            connection_class=redis.UnixDomainSocketConnection,
        ) as pool:
            expected = "path=/abc,db=1,client_name=test-client"
            assert expected in repr(pool)

    async def test_pool_disconnect(self, master_host):
        connection_kwargs = {
            "host": master_host[0],
            "port": master_host[1],
        }
        async with self.get_pool(connection_kwargs=connection_kwargs) as pool:
            conn = await pool.get_connection()
            await pool.disconnect(inuse_connections=True)
            assert not conn.is_connected

            await conn.connect()
            await pool.disconnect(inuse_connections=False)
            assert conn.is_connected

    async def test_pool_context_manager(self):
        pool = redis.ConnectionPool(connection_class=DummyConnection)

        async with pool as entered:
            assert entered is pool
            conn = await pool.get_connection()
            await conn.connect()
            assert conn.is_connected

        # exiting the context closes the pool, disconnecting all connections
        assert not conn.is_connected

    @pytest.mark.fixed_client
    async def test_lock_not_held_during_connection_establishment(self):
        """
        Test that the connection pool lock is not held during the
        ensure_connection call, which involves socket connection and handshake.
        This is important for performance under high load.
        """
        lock_states = []

        class SlowConnectConnection(DummyConnection):
            """Connection that simulates slow connection establishment"""

            async def connect(self):
                # Check if the pool's lock is held during connection
                # We access the pool through the outer scope
                lock_states.append(pool._lock.locked())
                # Simulate slow connection
                await asyncio.sleep(0.01)
                self._connected = True

        async with self.get_pool(connection_class=SlowConnectConnection) as pool:
            # Get a connection - this should call connect() outside the lock
            connection = await pool.get_connection()

            # Verify the lock was NOT held during connect
            assert len(lock_states) > 0, "connect() should have been called"
            assert lock_states[0] is False, (
                "Lock should not be held during connection establishment"
            )

            await pool.release(connection)

    @pytest.mark.fixed_client
    async def test_concurrent_connection_acquisition_performance(self):
        """
        Test that multiple concurrent connection acquisitions don't block
        each other during connection establishment.
        """
        num_connections = 3
        started = 0
        all_started = asyncio.Event()

        class SlowConnectConnection(DummyConnection):
            """Connection whose connect() finishes only once every connect started"""

            async def connect(self):
                nonlocal started
                started += 1
                if started == num_connections:
                    all_started.set()
                await all_started.wait()
                self._connected = True

        async with self.get_pool(
            connection_class=SlowConnectConnection, max_connections=10
        ) as pool:
            # If the lock were held during connect(), the second connect() could
            # not start, the first would wait forever, and wait_for would time out.
            try:
                connections = await asyncio.wait_for(
                    asyncio.gather(
                        *[pool.get_connection() for _ in range(num_connections)]
                    ),
                    timeout=5,
                )
            except asyncio.TimeoutError:
                pytest.fail(
                    f"only {started} of {num_connections} connect() calls started; "
                    "the lock was held during connection establishment"
                )

            # Clean up
            for conn in connections:
                await pool.release(conn)

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "parser_class",
        [
            _AsyncRESP3Parser,
            pytest.param(
                _AsyncHiredisParser,
                marks=pytest.mark.skipif(
                    not HIREDIS_AVAILABLE, reason="hiredis is not installed"
                ),
            ),
        ],
    )
    @pytest.mark.parametrize("maint_notifications_enabled", [True, False])
    @pytest.mark.parametrize("reset", [False, True])
    async def test_get_connection_replaces_closed_idle_connection(
        self, maint_notifications_enabled, parser_class, reset
    ):
        """
        A pooled connection whose socket the server closed while it sat idle
        must be reconnected at checkout. Regression test for #4252: with
        maintenance notifications enabled, the pending-push-data exemption
        must not swallow the EOF signal and hand out the dead connection.
        A reset (RST) leaves no EOF but an exception on the stream, and must
        be detected the same way.
        """
        async with self.get_pool(connection_class=redis.Connection) as pool:
            conn = pool.make_connection()
            conn.set_parser(parser_class)

            # simulate a connection that was healthy when released to the
            # pool but whose socket the server has since closed or reset
            eof_stream = asyncio.StreamReader()
            if reset:
                eof_stream.set_exception(ConnectionResetError())
            else:
                eof_stream.feed_eof()
            conn._parser._connected = True
            conn._parser._stream = eof_stream
            if parser_class is _AsyncHiredisParser:
                # on_connect() normally creates the hiredis reader; the test
                # never dials, so give it one with an empty buffer
                conn._parser._reader = mock.Mock(has_data=mock.Mock(return_value=False))
            fake_writer = mock.Mock()
            fake_writer.wait_closed = AsyncMock(return_value=None)
            conn._reader = eof_stream
            conn._writer = fake_writer

            async def fake_connect():
                # mirror the real connect(): a no-op unless disconnected
                if conn.is_connected:
                    return
                conn._parser._connected = True
                conn._parser._stream = asyncio.StreamReader()
                conn._reader = conn._parser._stream
                conn._writer = fake_writer

            conn.connect = AsyncMock(side_effect=fake_connect)
            pool._available_connections.append(conn)

            with mock.patch.object(
                pool,
                "maint_notifications_enabled",
                return_value=maint_notifications_enabled,
            ):
                checked_out = await pool.get_connection()

            assert checked_out is conn
            # ensure_connection() detected the EOF and reconnected: once for
            # the initial ensure, once after discarding the dead socket
            assert conn.connect.await_count == 2
            assert conn._parser._stream is not eof_stream
            await pool.release(conn)


class TestBlockingConnectionPool:
    @asynccontextmanager
    async def get_pool(self, connection_kwargs=None, max_connections=10, timeout=20):
        connection_kwargs = connection_kwargs or {}
        pool = redis.BlockingConnectionPool(
            connection_class=DummyConnection,
            max_connections=max_connections,
            timeout=timeout,
            **connection_kwargs,
        )
        try:
            yield pool
        finally:
            await pool.disconnect(inuse_connections=True)

    async def test_connection_creation(self, master_host):
        connection_kwargs = {
            "foo": "bar",
            "biz": "baz",
            "host": master_host[0],
            "port": master_host[1],
        }
        async with self.get_pool(connection_kwargs=connection_kwargs) as pool:
            connection = await pool.get_connection()
            assert isinstance(connection, DummyConnection)
            assert_kwargs_subset(connection.kwargs, connection_kwargs)

    async def test_pool_disconnect(self, master_host):
        connection_kwargs = {
            "foo": "bar",
            "biz": "baz",
            "host": master_host[0],
            "port": master_host[1],
        }
        async with self.get_pool(connection_kwargs=connection_kwargs) as pool:
            conn = await pool.get_connection()
            await pool.disconnect()
            assert not conn.is_connected

            await conn.connect()
            await pool.disconnect(inuse_connections=False)
            assert conn.is_connected

    async def test_pool_context_manager(self):
        pool = redis.BlockingConnectionPool(connection_class=DummyConnection)

        async with pool as entered:
            assert entered is pool
            conn = await pool.get_connection()
            await conn.connect()
            assert conn.is_connected

        # exiting the context closes the pool, disconnecting all connections
        assert not conn.is_connected

    async def test_multiple_connections(self, master_host):
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        async with self.get_pool(connection_kwargs=connection_kwargs) as pool:
            c1 = await pool.get_connection()
            c2 = await pool.get_connection()
            assert c1 != c2

    async def test_connection_pool_blocks_until_timeout(self, master_host):
        """When out of connections, block for timeout seconds, then raise"""
        connection_kwargs = {"host": master_host[0]}
        async with self.get_pool(
            max_connections=1, timeout=0.1, connection_kwargs=connection_kwargs
        ) as pool:
            c1 = await pool.get_connection()

            start = asyncio.get_running_loop().time()
            with patch(
                "redis.asyncio.connection.record_connection_timeout",
                new_callable=AsyncMock,
            ) as record:
                with pytest.raises(redis.ConnectionError):
                    await pool.get_connection()

            # we should have waited at least some period of time
            assert asyncio.get_running_loop().time() - start >= 0.05
            record.assert_awaited_once()
            await c1.disconnect()

    async def test_connection_pool_blocks_until_conn_available(self, master_host):
        """
        When out of connections, block until another connection is released
        to the pool
        """
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        async with self.get_pool(
            max_connections=1, timeout=2, connection_kwargs=connection_kwargs
        ) as pool:
            c1 = await pool.get_connection()

            async def target():
                await asyncio.sleep(0.1)
                await pool.release(c1)

            start = asyncio.get_running_loop().time()
            release = asyncio.create_task(target())
            # time get_connection alone: it must block until the release
            await pool.get_connection()
            elapsed = asyncio.get_running_loop().time() - start
            # and wake on it, well before the 2 s pool timeout
            assert 0.1 - EVENT_LOOP_TIMER_SLACK <= elapsed < 1.0
            await release

    async def test_reuse_previously_released_connection(self, master_host):
        connection_kwargs = {"host": master_host[0]}
        async with self.get_pool(connection_kwargs=connection_kwargs) as pool:
            c1 = await pool.get_connection()
            await pool.release(c1)
            c2 = await pool.get_connection()
            assert c1 == c2

    @pytest.mark.fixed_client
    async def test_checkout_waits_for_idle_re_auth(self):
        async with self.get_pool(max_connections=1) as pool:
            connection = await pool.get_connection()
            await pool.release(connection)
            auth_started = asyncio.Event()
            finish_auth = asyncio.Event()
            checkout_started = asyncio.Event()

            async def read_auth_response():
                auth_started.set()
                await finish_auth.wait()
                return b"OK"

            async def checkout():
                checkout_started.set()
                return await pool.get_connection()

            connection.retry = Retry(NoBackoff(), 0)
            connection.send_command = AsyncMock()
            connection.read_response = AsyncMock(side_effect=read_auth_response)
            token = mock.Mock()
            token.try_get.return_value = "default"
            token.get_value.return_value = "token"

            refresh = asyncio.create_task(pool.re_auth_callback(token))
            await auth_started.wait()
            acquire = asyncio.create_task(checkout())
            try:
                await checkout_started.wait()
                assert not acquire.done()
                assert pool._available_connections == [connection]
                assert not pool._in_use_connections
            finally:
                finish_auth.set()
                await asyncio.gather(refresh, acquire)

            assert acquire.result() is connection
            connection.send_command.assert_awaited_once_with("AUTH", "default", "token")
            await pool.release(connection)

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("cancel", [False, True], ids=["timeout", "cancel"])
    async def test_checkout_interrupted_while_pool_locked(self, cancel):
        async with self.get_pool(max_connections=1, timeout=0.01) as pool:
            connection = await pool.get_connection()
            await pool.release(connection)
            checkout_started = asyncio.Event()

            async def checkout():
                checkout_started.set()
                return await pool.get_connection()

            async with pool._lock:
                acquire = asyncio.create_task(checkout())
                await checkout_started.wait()
                if cancel:
                    acquire.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await acquire
                else:
                    with pytest.raises(
                        redis.ConnectionError, match="No connection available"
                    ):
                        await acquire
                assert pool._available_connections == [connection]
                assert not pool._in_use_connections

            assert await pool.get_connection() is connection
            await pool.release(connection)

    @pytest.mark.fixed_client
    def test_repr_contains_db_info_tcp(self):
        pool = redis.ConnectionPool(
            host="localhost", port=6379, client_name="test-client"
        )
        expected = "host=localhost,port=6379,client_name=test-client"
        assert expected in repr(pool)

    @pytest.mark.fixed_client
    def test_repr_contains_db_info_unix(self):
        pool = redis.ConnectionPool(
            connection_class=redis.UnixDomainSocketConnection,
            path="abc",
            db=0,
            client_name="test-client",
        )
        expected = "path=abc,db=0,client_name=test-client"
        assert expected in repr(pool)

    @pytest.mark.fixed_client
    def test_repr_redacts_sensitive_information(self):
        """Test that __repr__ redacts sensitive values like password and username."""
        pool = ConnectionPool(
            host="localhost",
            port=6379,
            password="secret_password_123",
            username="myuser",
            ssl_password="ssl_secret_456",
            db=0,
        )
        repr_output = repr(pool)

        # Verify sensitive values are redacted
        assert "secret_password_123" not in repr_output
        assert "myuser" not in repr_output
        assert "ssl_secret_456" not in repr_output

        # Verify the REDACTED placeholder is present
        assert "<REDACTED>" in repr_output

        # Verify non-sensitive values are still visible
        assert "host=localhost" in repr_output
        assert "port=6379" in repr_output
        assert "db=0" in repr_output


@pytest.mark.fixed_client
class TestConnectionPoolURLParsing:
    def test_hostname(self):
        pool = redis.ConnectionPool.from_url("redis://my.host")
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(pool.connection_kwargs, {"host": "my.host"})

    def test_quoted_hostname(self):
        pool = redis.ConnectionPool.from_url("redis://my %2F host %2B%3D+")
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(pool.connection_kwargs, {"host": "my / host +=+"})

    def test_port(self):
        pool = redis.ConnectionPool.from_url("redis://localhost:6380")
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(
            pool.connection_kwargs, {"host": "localhost", "port": 6380}
        )

    @skip_if_server_version_lt("6.0.0")
    def test_username(self):
        pool = redis.ConnectionPool.from_url("redis://myuser:@localhost")
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(
            pool.connection_kwargs, {"host": "localhost", "username": "myuser"}
        )

    @skip_if_server_version_lt("6.0.0")
    def test_quoted_username(self):
        pool = redis.ConnectionPool.from_url(
            "redis://%2Fmyuser%2F%2B name%3D%24+:@localhost"
        )
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(
            pool.connection_kwargs,
            {
                "host": "localhost",
                "username": "/myuser/+ name=$+",
            },
        )

    def test_password(self):
        pool = redis.ConnectionPool.from_url("redis://:mypassword@localhost")
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(
            pool.connection_kwargs, {"host": "localhost", "password": "mypassword"}
        )

    def test_quoted_password(self):
        pool = redis.ConnectionPool.from_url(
            "redis://:%2Fmypass%2F%2B word%3D%24+@localhost"
        )
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(
            pool.connection_kwargs,
            {
                "host": "localhost",
                "password": "/mypass/+ word=$+",
            },
        )

    @skip_if_server_version_lt("6.0.0")
    def test_username_and_password(self):
        pool = redis.ConnectionPool.from_url("redis://myuser:mypass@localhost")
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(
            pool.connection_kwargs,
            {
                "host": "localhost",
                "username": "myuser",
                "password": "mypass",
            },
        )

    def test_db_as_argument(self):
        pool = redis.ConnectionPool.from_url("redis://localhost", db=1)
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(pool.connection_kwargs, {"host": "localhost", "db": 1})

    def test_db_in_path(self):
        pool = redis.ConnectionPool.from_url("redis://localhost/2", db=1)
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(pool.connection_kwargs, {"host": "localhost", "db": 2})

    def test_db_in_querystring(self):
        pool = redis.ConnectionPool.from_url("redis://localhost/2?db=3", db=1)
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(pool.connection_kwargs, {"host": "localhost", "db": 3})

    def test_extra_typed_querystring_options(self):
        pool = redis.ConnectionPool.from_url(
            "redis://localhost/2?socket_timeout=20&socket_connect_timeout=10"
            "&socket_keepalive=&retry_on_timeout=Yes&max_connections=10"
        )

        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(
            pool.connection_kwargs,
            {
                "host": "localhost",
                "db": 2,
                "socket_timeout": 20.0,
                "socket_connect_timeout": 10.0,
                "retry_on_timeout": True,
            },
        )
        assert pool.max_connections == 10

    def test_boolean_parsing(self):
        for expected, value in (
            (None, None),
            (None, ""),
            (False, 0),
            (False, "0"),
            (False, "f"),
            (False, "F"),
            (False, "False"),
            (False, "n"),
            (False, "N"),
            (False, "No"),
            (False, "off"),
            (False, "OFF"),
            (False, "Off"),
            (True, 1),
            (True, "1"),
            (True, "y"),
            (True, "Y"),
            (True, "Yes"),
        ):
            assert expected is to_bool(value)

    def test_client_name_in_querystring(self):
        pool = redis.ConnectionPool.from_url("redis://location?client_name=test-client")
        assert pool.connection_kwargs["client_name"] == "test-client"

    def test_querystring_value_percent_decoded_once(self):
        # A single percent-encoded sequence is decoded exactly once.
        pool = redis.ConnectionPool.from_url(
            "redis://location?client_name=worker%20name"
        )
        assert pool.connection_kwargs["client_name"] == "worker name"

    def test_querystring_value_not_double_percent_decoded(self):
        # A literal percent sign in a query value (encoded as %2520) must survive
        # as "%20" rather than being decoded a second time into a space. See #4208.
        pool = redis.ConnectionPool.from_url(
            "redis://location?client_name=worker%2520name"
        )
        assert pool.connection_kwargs["client_name"] == "worker%20name"

    def test_invalid_extra_typed_querystring_options(self):
        with pytest.raises(ValueError):
            redis.ConnectionPool.from_url(
                "redis://localhost/2?socket_timeout=_&socket_connect_timeout=abc"
            )

    def test_retry_on_error_querystring(self):
        pool = redis.ConnectionPool.from_url(
            "redis://localhost?retry_on_error=ConnectionError"
        )
        assert pool.connection_kwargs["retry_on_error"] == [redis.ConnectionError]

    def test_retry_on_error_querystring_multiple(self):
        pool = redis.ConnectionPool.from_url(
            "redis://localhost?retry_on_error=ConnectionError,TimeoutError"
        )
        assert pool.connection_kwargs["retry_on_error"] == [
            redis.ConnectionError,
            redis.TimeoutError,
        ]

    def test_retry_on_error_querystring_brackets(self):
        pool = redis.ConnectionPool.from_url(
            "redis://localhost?retry_on_error=[ConnectionError,TimeoutError]"
        )
        assert pool.connection_kwargs["retry_on_error"] == [
            redis.ConnectionError,
            redis.TimeoutError,
        ]

    def test_retry_on_error_querystring_blank(self):
        with pytest.raises(ValueError, match="Invalid value for 'retry_on_error'"):
            redis.ConnectionPool.from_url("redis://localhost?retry_on_error=,")

    def test_retry_on_error_querystring_invalid(self):
        with pytest.raises(ValueError, match="Invalid value for 'retry_on_error'"):
            redis.ConnectionPool.from_url(
                "redis://localhost?retry_on_error=NotARealError"
            )

    def test_from_url_retry_on_error(self):
        client = redis.Redis.from_url(
            "redis://localhost?retry_on_error=ConnectionError"
        )
        assert client.connection_pool.connection_kwargs["retry_on_error"] == [
            redis.ConnectionError
        ]

    def test_extra_querystring_options(self):
        pool = redis.ConnectionPool.from_url("redis://localhost?a=1&b=2")
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(
            pool.connection_kwargs, {"host": "localhost", "a": "1", "b": "2"}
        )

    def test_calling_from_subclass_returns_correct_instance(self):
        pool = redis.BlockingConnectionPool.from_url("redis://localhost")
        assert isinstance(pool, redis.BlockingConnectionPool)

    def test_client_creates_connection_pool(self):
        r = redis.Redis.from_url("redis://myhost")
        assert r.connection_pool.connection_class == redis.Connection
        assert_kwargs_subset(r.connection_pool.connection_kwargs, {"host": "myhost"})

    def test_invalid_scheme_raises_error(self):
        with pytest.raises(ValueError) as cm:
            redis.ConnectionPool.from_url("localhost")
        assert str(cm.value) == (
            "Redis URL must specify one of the following schemes "
            "(redis://, rediss://, unix://)"
        )

    def test_invalid_scheme_raises_error_when_double_slash_missing(self):
        with pytest.raises(ValueError) as cm:
            redis.ConnectionPool.from_url("redis:foo.bar.com:12345")
        assert str(cm.value) == (
            "Redis URL must specify one of the following schemes "
            "(redis://, rediss://, unix://)"
        )

    def test_uppercase_scheme_is_accepted(self):
        # URL schemes are case-insensitive (RFC 3986)
        pool = redis.ConnectionPool.from_url("REDIS://my.host")
        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(pool.connection_kwargs, {"host": "my.host"})

        ssl_pool = redis.ConnectionPool.from_url("REDISS://my.host")
        assert ssl_pool.connection_class == redis.SSLConnection
        assert_kwargs_subset(ssl_pool.connection_kwargs, {"host": "my.host"})

        unix_pool = redis.ConnectionPool.from_url("UNIX:///tmp/redis.sock")
        assert unix_pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(unix_pool.connection_kwargs, {"path": "/tmp/redis.sock"})


@pytest.mark.fixed_client
class TestBlockingConnectionPoolURLParsing:
    def test_extra_typed_querystring_options(self):
        pool = redis.BlockingConnectionPool.from_url(
            "redis://localhost/2?socket_timeout=20&socket_connect_timeout=10"
            "&socket_keepalive=&retry_on_timeout=Yes&max_connections=10&timeout=13.37"
        )

        assert pool.connection_class == redis.Connection
        assert_kwargs_subset(
            pool.connection_kwargs,
            {
                "host": "localhost",
                "db": 2,
                "socket_timeout": 20.0,
                "socket_connect_timeout": 10.0,
                "retry_on_timeout": True,
            },
        )
        assert pool.max_connections == 10
        assert pool.timeout == 13.37

    def test_invalid_extra_typed_querystring_options(self):
        with pytest.raises(ValueError):
            redis.BlockingConnectionPool.from_url(
                "redis://localhost/2?timeout=_not_a_float_"
            )


@pytest.mark.fixed_client
class TestConnectionPoolUnixSocketURLParsing:
    def test_defaults(self):
        pool = redis.ConnectionPool.from_url("unix:///socket")
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(pool.connection_kwargs, {"path": "/socket"})

    @skip_if_server_version_lt("6.0.0")
    def test_username(self):
        pool = redis.ConnectionPool.from_url("unix://myuser:@/socket")
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(
            pool.connection_kwargs, {"path": "/socket", "username": "myuser"}
        )

    @skip_if_server_version_lt("6.0.0")
    def test_quoted_username(self):
        pool = redis.ConnectionPool.from_url(
            "unix://%2Fmyuser%2F%2B name%3D%24+:@/socket"
        )
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(
            pool.connection_kwargs,
            {
                "path": "/socket",
                "username": "/myuser/+ name=$+",
            },
        )

    def test_password(self):
        pool = redis.ConnectionPool.from_url("unix://:mypassword@/socket")
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(
            pool.connection_kwargs, {"path": "/socket", "password": "mypassword"}
        )

    def test_quoted_password(self):
        pool = redis.ConnectionPool.from_url(
            "unix://:%2Fmypass%2F%2B word%3D%24+@/socket"
        )
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(
            pool.connection_kwargs,
            {
                "path": "/socket",
                "password": "/mypass/+ word=$+",
            },
        )

    def test_quoted_path(self):
        pool = redis.ConnectionPool.from_url(
            "unix://:mypassword@/my%2Fpath%2Fto%2F..%2F+_%2B%3D%24ocket"
        )
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(
            pool.connection_kwargs,
            {
                "path": "/my/path/to/../+_+=$ocket",
                "password": "mypassword",
            },
        )

    def test_db_as_argument(self):
        pool = redis.ConnectionPool.from_url("unix:///socket", db=1)
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(pool.connection_kwargs, {"path": "/socket", "db": 1})

    def test_db_in_querystring(self):
        pool = redis.ConnectionPool.from_url("unix:///socket?db=2", db=1)
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(pool.connection_kwargs, {"path": "/socket", "db": 2})

    def test_client_name_in_querystring(self):
        pool = redis.ConnectionPool.from_url("redis://location?client_name=test-client")
        assert pool.connection_kwargs["client_name"] == "test-client"

    def test_extra_querystring_options(self):
        pool = redis.ConnectionPool.from_url("unix:///socket?a=1&b=2")
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(
            pool.connection_kwargs, {"path": "/socket", "a": "1", "b": "2"}
        )

    def test_connection_class_override(self):
        class MyConnection(redis.UnixDomainSocketConnection):
            pass

        pool = redis.ConnectionPool.from_url(
            "unix:///socket", connection_class=MyConnection
        )
        assert pool.connection_class == MyConnection


@pytest.mark.fixed_client
class TestSSLConnectionURLParsing:
    def test_host(self):
        pool = redis.ConnectionPool.from_url("rediss://my.host")
        assert pool.connection_class == redis.SSLConnection
        assert_kwargs_subset(pool.connection_kwargs, {"host": "my.host"})

    def test_connection_class_override(self):
        class MyConnection(redis.SSLConnection):
            pass

        pool = redis.ConnectionPool.from_url(
            "rediss://my.host", connection_class=MyConnection
        )
        assert pool.connection_class == MyConnection

    def test_cert_reqs_options(self):
        import ssl

        class DummyConnectionPool(redis.ConnectionPool):
            def get_connection(self):
                return self.make_connection()

        pool = DummyConnectionPool.from_url("rediss://?ssl_cert_reqs=none")
        assert pool.get_connection().cert_reqs == ssl.CERT_NONE

        pool = DummyConnectionPool.from_url("rediss://?ssl_cert_reqs=optional")
        assert pool.get_connection().cert_reqs == ssl.CERT_OPTIONAL

        pool = DummyConnectionPool.from_url("rediss://?ssl_cert_reqs=required")
        assert pool.get_connection().cert_reqs == ssl.CERT_REQUIRED

        pool = DummyConnectionPool.from_url("rediss://?ssl_check_hostname=False")
        assert pool.get_connection().check_hostname is False

        pool = DummyConnectionPool.from_url("rediss://?ssl_check_hostname=True")
        assert pool.get_connection().check_hostname is True


class TestConnection:
    @pytest.mark.fixed_client
    async def test_on_connect_error(self):
        """
        An error in Connection.on_connect should disconnect from the server
        see for details: https://github.com/andymccurdy/redis-py/issues/368
        """
        # this assumes the Redis server being tested against doesn't have
        # 9999 databases ;)
        bad_connection = redis.Redis(db=9999)
        # an error should be raised on connect
        with pytest.raises(redis.RedisError):
            await bad_connection.info()
        pool = bad_connection.connection_pool
        assert len(pool._available_connections) == 1
        assert not pool._available_connections[0]._reader

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    async def test_busy_loading_disconnects_socket(self, r):
        """
        If Redis raises a LOADING error, the connection should be
        disconnected and a BusyLoadingError raised
        """
        with pytest.raises(redis.BusyLoadingError):
            await r.execute_command("DEBUG", "ERROR", "LOADING fake message")
        if r.connection:
            assert not r.connection._reader

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    async def test_busy_loading_from_pipeline_immediate_command(self, r):
        """
        BusyLoadingErrors should raise from Pipelines that execute a
        command immediately, like WATCH does.
        """
        pipe = r.pipeline()
        with pytest.raises(redis.BusyLoadingError):
            await pipe.immediate_execute_command(
                "DEBUG", "ERROR", "LOADING fake message"
            )
        pool = r.connection_pool
        assert pipe.connection
        assert pipe.connection in pool._in_use_connections
        assert not pipe.connection._reader

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    async def test_busy_loading_from_pipeline(self, r):
        """
        BusyLoadingErrors should be raised from a pipeline execution
        regardless of the raise_on_error flag.
        """
        pipe = r.pipeline()
        pipe.execute_command("DEBUG", "ERROR", "LOADING fake message")
        with pytest.raises(redis.BusyLoadingError):
            await pipe.execute()
        pool = r.connection_pool
        assert not pipe.connection
        assert len(pool._available_connections) == 1
        assert not pool._available_connections[0]._reader

    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    async def test_read_only_error(self, r):
        """READONLY errors get turned into ReadOnlyError exceptions"""
        with pytest.raises(redis.ReadOnlyError):
            await r.execute_command("DEBUG", "ERROR", "READONLY blah blah")

    @skip_if_redis_enterprise()
    async def test_oom_error(self, r):
        """OOM errors get turned into OutOfMemoryError exceptions"""
        with pytest.raises(redis.OutOfMemoryError):
            # note: don't use the DEBUG OOM command since it's not the same
            # as the db being full
            await r.execute_command("DEBUG", "ERROR", "OOM blah blah")

    @pytest.mark.fixed_client
    def test_connect_from_url_tcp(self):
        connection = redis.Redis.from_url("redis://localhost:6379?db=0")
        pool = connection.connection_pool

        conn_name, pool_name, conn_kwargs = re.match(
            r"< .*?([^\.]+) \( < .*?([^\.]+) \( (.+) \) > \) >", repr(pool), re.VERBOSE
        ).groups()
        assert conn_name == "ConnectionPool"
        assert pool_name == "Connection"
        assert "db=0" in conn_kwargs
        assert "host=localhost" in conn_kwargs
        assert "port=6379" in conn_kwargs

    @pytest.mark.fixed_client
    def test_connect_from_url_unix(self):
        connection = redis.Redis.from_url("unix:///path/to/socket")
        pool = connection.connection_pool

        assert re.match(
            r"< .*?([^\.]+) \( < .*?([^\.]+) \( (.+) \) > \) >", repr(pool), re.VERBOSE
        ).groups() == (
            "ConnectionPool",
            "UnixDomainSocketConnection",
            "path=/path/to/socket",
        )

    @skip_if_redis_enterprise()
    async def test_connect_no_auth_supplied_when_required(self, r):
        """
        AuthenticationError should be raised when the server requires a
        password but one isn't supplied.
        """
        with pytest.raises(redis.AuthenticationError):
            await r.execute_command(
                "DEBUG", "ERROR", "ERR Client sent AUTH, but no password is set"
            )

    @skip_if_redis_enterprise()
    async def test_connect_invalid_password_supplied(self, r):
        """AuthenticationError should be raised when sending the wrong password"""
        with pytest.raises(redis.AuthenticationError):
            await r.execute_command("DEBUG", "ERROR", "ERR invalid password")


@pytest.mark.onlynoncluster
class TestMultiConnectionClient:
    @pytest_asyncio.fixture()
    async def r(self, create_redis, server):
        redis = await create_redis(single_connection_client=False)
        yield redis
        await redis.flushall()


@pytest.mark.onlynoncluster
# r.connection is the connection under test, as in the sync tests, so only the
# single-connection variant of create_redis applies
@pytest.mark.parametrize("create_redis", [(True,)], ids=["single"], indirect=True)
class TestHealthCheck:
    interval = 60

    @pytest_asyncio.fixture()
    async def r(self, create_redis):
        redis = await create_redis(health_check_interval=self.interval)
        yield redis
        await redis.flushall()

    def assert_interval_advanced(self, connection, start):
        # next_health_check is set from the clock when a reply is read,
        # which happens between `start` and now
        now = asyncio.get_running_loop().time()
        assert (
            start + self.interval <= connection.next_health_check <= now + self.interval
        )

    async def test_health_check_runs(self, r):
        r.connection.next_health_check = asyncio.get_running_loop().time() - 1
        start = asyncio.get_running_loop().time()
        await r.connection.check_health()
        self.assert_interval_advanced(r.connection, start)

    async def test_arbitrary_command_invokes_health_check(self, r):
        # invoke a command to make sure the connection is entirely setup
        await r.get("foo")
        r.connection.next_health_check = asyncio.get_running_loop().time() - 1
        start = asyncio.get_running_loop().time()
        with mock.patch.object(
            r.connection, "send_command", wraps=r.connection.send_command
        ) as m:
            await r.get("foo")
            m.assert_called_with("PING", check_health=False)

        self.assert_interval_advanced(r.connection, start)

    async def test_arbitrary_command_advances_next_health_check(self, r):
        await r.get("foo")
        next_health_check = r.connection.next_health_check
        # ensure that the event loop's `time()` advances a bit
        await asyncio.sleep(0.001)
        await r.get("foo")
        assert next_health_check < r.connection.next_health_check

    async def test_health_check_not_invoked_within_interval(self, r):
        await r.get("foo")
        with mock.patch.object(
            r.connection, "send_command", wraps=r.connection.send_command
        ) as m:
            await r.get("foo")
            ping_call_spec = (("PING",), {"check_health": False})
            assert ping_call_spec not in m.call_args_list

    async def test_health_check_in_pipeline(self, r):
        async with r.pipeline(transaction=False) as pipe:
            pipe.connection = await pipe.connection_pool.get_connection()
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                responses = await pipe.set("foo", "bar").get("foo").execute()
                m.assert_any_call("PING", check_health=False)
                assert responses == [True, b"bar"]

    async def test_health_check_in_transaction(self, r):
        async with r.pipeline(transaction=True) as pipe:
            pipe.connection = await pipe.connection_pool.get_connection()
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                responses = await pipe.set("foo", "bar").get("foo").execute()
                m.assert_any_call("PING", check_health=False)
                assert responses == [True, b"bar"]

    async def test_health_check_in_watched_pipeline(self, r):
        await r.set("foo", "bar")
        async with r.pipeline(transaction=False) as pipe:
            pipe.connection = await pipe.connection_pool.get_connection()
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                start = asyncio.get_running_loop().time()
                await pipe.watch("foo")
                # the health check should be called when watching
                m.assert_called_with("PING", check_health=False)
                self.assert_interval_advanced(pipe.connection, start)
                assert await pipe.get("foo") == b"bar"

                # reset the mock to clear the call list and schedule another
                # health check
                m.reset_mock()
                pipe.connection.next_health_check = 0

                pipe.multi()
                responses = await pipe.set("foo", "not-bar").get("foo").execute()
                assert responses == [True, b"not-bar"]
                m.assert_any_call("PING", check_health=False)

    async def test_health_check_in_pubsub_before_subscribe(self, r):
        """A health check happens before the first [p]subscribe"""
        p = r.pubsub()
        p.connection = await p.connection_pool.get_connection()
        p.connection.next_health_check = 0
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            assert not p.subscribed
            start = asyncio.get_running_loop().time()
            await p.subscribe("foo")
            # the connection is not yet in pubsub mode, so the normal
            # ping/pong within connection.send_command should check
            # the health of the connection
            m.assert_any_call("PING", check_health=False)
            self.assert_interval_advanced(p.connection, start)

            subscribe_message = await wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"

    async def test_health_check_in_pubsub_after_subscribed(self, r):
        """
        Pubsub can handle a new subscribe when it's time to check the
        connection health
        """
        p = r.pubsub()
        p.connection = await p.connection_pool.get_connection()
        p.connection.next_health_check = 0
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            start = asyncio.get_running_loop().time()
            await p.subscribe("foo")
            subscribe_message = await wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            self.assert_interval_advanced(p.connection, start)
            # because we weren't subscribed when sending the subscribe
            # message to 'foo', the connection's standard check_health ran
            # prior to subscribing.
            m.assert_any_call("PING", check_health=False)

            p.connection.next_health_check = 0
            m.reset_mock()

            start = asyncio.get_running_loop().time()
            await p.subscribe("bar")
            # the second subscribe issues exactly only command (the subscribe)
            # and the health check is not invoked
            m.assert_called_once_with("SUBSCRIBE", "bar", check_health=False)

            # since no message has been read since the health check was
            # reset, it should still be 0
            assert p.connection.next_health_check == 0

            subscribe_message = await wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            assert await p.get_message(timeout=1) is None
            # now that the connection is subscribed, the pubsub health
            # check should have taken over and include the HEALTH_CHECK_MESSAGE
            m.assert_any_call("PING", p.HEALTH_CHECK_MESSAGE, check_health=False)
            self.assert_interval_advanced(p.connection, start)

    async def test_health_check_in_pubsub_poll(self, r):
        """
        Polling a pubsub connection that's subscribed will regularly
        check the connection's health.
        """
        p = r.pubsub()
        p.connection = await p.connection_pool.get_connection()
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            start = asyncio.get_running_loop().time()
            await p.subscribe("foo")
            subscribe_message = await wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            self.assert_interval_advanced(p.connection, start)

            # polling the connection before the health check interval
            # doesn't result in another health check
            m.reset_mock()
            next_health_check = p.connection.next_health_check
            # one poll is enough: every get_message runs check_health first
            assert await p.get_message(timeout=0) is None
            assert p.connection.next_health_check == next_health_check
            m.assert_not_called()

            # reset the health check and poll again
            # we should not receive a pong message, but the next_health_check
            # should be advanced
            p.connection.next_health_check = 0
            start = asyncio.get_running_loop().time()
            assert await p.get_message(timeout=1) is None
            m.assert_called_with("PING", p.HEALTH_CHECK_MESSAGE, check_health=False)
            self.assert_interval_advanced(p.connection, start)


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestAsyncConnectionPoolMetricsRecording:
    """Tests for async ConnectionPool metrics recording (create time and wait time)."""

    async def test_connection_pool_records_create_time_on_new_connection(self):
        """Test that ConnectionPool.get_connection() records create time when a new connection is created."""
        from unittest.mock import AsyncMock, MagicMock

        pool = ConnectionPool(max_connections=10)

        # Mock the ensure_connection to avoid actual connection
        mock_connection = MagicMock()
        mock_connection.is_connected = False
        mock_connection.disconnect = AsyncMock()

        # Patch where the functions are used (in redis.asyncio.connection module)
        with patch.object(pool, "make_connection", return_value=mock_connection):
            with patch.object(pool, "ensure_connection", new_callable=AsyncMock):
                with patch(
                    "redis.asyncio.connection.record_connection_create_time",
                    new_callable=AsyncMock,
                ) as mock_record:
                    await pool.get_connection()

                    # Verify record_connection_create_time was called
                    mock_record.assert_called_once()
                    call_kwargs = mock_record.call_args[1]
                    assert call_kwargs["connection_pool"] is pool
                    assert call_kwargs["duration_seconds"] > 0

        await pool.disconnect()

    async def test_connection_pool_does_not_record_create_time_for_existing_connection(
        self,
    ):
        """Test that ConnectionPool.get_connection() does not record create time when reusing an existing connection."""
        from unittest.mock import AsyncMock, MagicMock

        pool = ConnectionPool(max_connections=10)

        # Add a mock connection to available connections
        mock_connection = MagicMock()
        mock_connection.disconnect = AsyncMock()
        pool._available_connections.append(mock_connection)

        with patch.object(pool, "ensure_connection", new_callable=AsyncMock):
            with patch(
                "redis.asyncio.connection.record_connection_create_time",
                new_callable=AsyncMock,
            ) as mock_record:
                await pool.get_connection()

                # Verify record_connection_create_time was NOT called
                mock_record.assert_not_called()

        await pool.disconnect()

    async def test_blocking_connection_pool_records_create_time_and_wait_time(self):
        """Test that BlockingConnectionPool.get_connection() records both create time and wait time."""
        from unittest.mock import AsyncMock, MagicMock

        pool = BlockingConnectionPool(max_connections=10, timeout=5)

        # Mock the ensure_connection to avoid actual connection
        mock_connection = MagicMock()
        mock_connection.is_connected = False
        mock_connection.disconnect = AsyncMock()

        # Patch where the functions are used (in redis.asyncio.connection module)
        with patch.object(pool, "make_connection", return_value=mock_connection):
            with patch.object(pool, "ensure_connection", new_callable=AsyncMock):
                with patch(
                    "redis.asyncio.connection.record_connection_create_time",
                    new_callable=AsyncMock,
                ) as mock_create_time:
                    with patch(
                        "redis.asyncio.connection.record_connection_wait_time",
                        new_callable=AsyncMock,
                    ) as mock_wait_time:
                        await pool.get_connection()

                        # Verify record_connection_create_time was called
                        mock_create_time.assert_called_once()
                        create_kwargs = mock_create_time.call_args[1]
                        assert create_kwargs["connection_pool"] is pool
                        assert create_kwargs["duration_seconds"] > 0

                        # Verify record_connection_wait_time was called
                        mock_wait_time.assert_called_once()
                        wait_kwargs = mock_wait_time.call_args[1]
                        assert "pool_name" in wait_kwargs
                        assert wait_kwargs["duration_seconds"] > 0

        await pool.disconnect()

    async def test_blocking_connection_pool_records_only_wait_time_for_existing_connection(
        self,
    ):
        """Test that BlockingConnectionPool.get_connection() records only wait time when reusing an existing connection."""
        from unittest.mock import AsyncMock, MagicMock

        pool = BlockingConnectionPool(max_connections=10, timeout=5)

        # Add a mock connection to available connections
        mock_connection = MagicMock()
        mock_connection.disconnect = AsyncMock()
        pool._available_connections.append(mock_connection)

        with patch.object(pool, "ensure_connection", new_callable=AsyncMock):
            with patch(
                "redis.asyncio.connection.record_connection_create_time",
                new_callable=AsyncMock,
            ) as mock_create_time:
                with patch(
                    "redis.asyncio.connection.record_connection_wait_time",
                    new_callable=AsyncMock,
                ) as mock_wait_time:
                    await pool.get_connection()

                    # Verify record_connection_create_time was NOT called
                    mock_create_time.assert_not_called()

                    # Verify record_connection_wait_time was still called
                    mock_wait_time.assert_called_once()
                    wait_kwargs = mock_wait_time.call_args[1]
                    assert "pool_name" in wait_kwargs
                    assert wait_kwargs["duration_seconds"] > 0

        await pool.disconnect()

    async def test_connection_disconnect_records_connection_closed_on_error(self):
        """Test that Connection.disconnect() records connection closed with ERROR reason when error is provided."""

        conn = Connection()
        conn._writer = MagicMock()
        conn._writer.close = MagicMock()
        conn._writer.wait_closed = AsyncMock()
        conn._reader = MagicMock()

        with patch(
            "redis.asyncio.connection.record_connection_closed",
            new_callable=AsyncMock,
        ) as mock_record:
            error = ConnectionError("Connection lost")
            await conn.disconnect(error=error)

            # Verify record_connection_closed was called with ERROR reason
            mock_record.assert_called_once()
            call_kwargs = mock_record.call_args[1]
            assert call_kwargs["close_reason"] == CloseReason.ERROR
            assert call_kwargs["error_type"] is error

    async def test_connection_disconnect_records_connection_closed_on_healthcheck_failed(
        self,
    ):
        """Test that Connection.disconnect() records connection closed with HEALTHCHECK_FAILED reason."""

        conn = Connection()
        conn._writer = MagicMock()
        conn._writer.close = MagicMock()
        conn._writer.wait_closed = AsyncMock()
        conn._reader = MagicMock()

        with patch(
            "redis.asyncio.connection.record_connection_closed",
            new_callable=AsyncMock,
        ) as mock_record:
            error = ConnectionError("Health check failed")
            await conn.disconnect(error=error, health_check_failed=True)

            # Verify record_connection_closed was called with HEALTHCHECK_FAILED reason
            mock_record.assert_called_once()
            call_kwargs = mock_record.call_args[1]
            assert call_kwargs["close_reason"] == CloseReason.HEALTHCHECK_FAILED
            assert call_kwargs["error_type"] is error

    async def test_connection_disconnect_records_connection_closed_on_application_close(
        self,
    ):
        """Test that Connection.disconnect() records connection closed with APPLICATION_CLOSE reason for normal close."""

        conn = Connection()
        conn._writer = MagicMock()
        conn._writer.close = MagicMock()
        conn._writer.wait_closed = AsyncMock()
        conn._reader = MagicMock()

        with patch(
            "redis.asyncio.connection.record_connection_closed",
            new_callable=AsyncMock,
        ) as mock_record:
            await conn.disconnect()

            # Verify record_connection_closed was called with APPLICATION_CLOSE reason
            mock_record.assert_called_once()
            call_kwargs = mock_record.call_args[1]
            assert call_kwargs["close_reason"] == CloseReason.APPLICATION_CLOSE
