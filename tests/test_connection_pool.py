import os
import re
import time
from contextlib import closing
from threading import Thread
from unittest import mock
from unittest.mock import MagicMock

import pytest
import redis
from redis.cache import CacheConfig
from redis.commands.metadata import StaticMetadataResolver
from redis.connection import CacheProxyConnection, Connection, to_bool
from redis.event import (
    AfterConnectionReleasedEvent,
    EventDispatcher,
    EventListenerInterface,
)
from redis.maint_notifications import (
    MaintNotificationsConfig,
    MaintNotificationsPoolHandler,
)
from redis.backoff import NoBackoff
from redis.retry import Retry
from redis.utils import SSL_AVAILABLE

from .conftest import (
    _get_client,
    skip_if_redis_enterprise,
    skip_if_resp_version,
    skip_if_server_version_lt,
)
from .test_pubsub import wait_for_message

if SSL_AVAILABLE:
    import ssl


def assert_kwargs_subset(actual, expected):
    """Assert ``expected`` is a subset of ``actual`` (keys present, values equal).

    Used by URL/kwargs parsing tests to remain agnostic of auto-injected
    connection-pool keys (e.g. maintenance-notifications fields added when
    the pool resolves to RESP3) without weakening parsing assertions.
    """
    for key, value in expected.items():
        assert key in actual, f"missing key {key!r} in {actual!r}"
        assert actual[key] == value, (
            f"value mismatch for {key!r}: {actual[key]!r} != {value!r}"
        )


class DummyConnection:
    description_format = "DummyConnection<>"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()
        self._sock = None

    def connect(self):
        self._sock = mock.Mock()

    def disconnect(self):
        self._sock = None

    def can_read(self, timeout: float = 0) -> bool:
        return False

    def should_reconnect(self):
        return False

    def re_auth(self):
        pass


class ConstructionFailure(Exception):
    """Raised by ``failing_connection_class`` instead of building a connection."""


def failing_connection_class(failures):
    """A ``DummyConnection`` whose constructor raises the first ``failures`` times."""
    remaining = [failures]

    class FailingConnection(DummyConnection):
        def __init__(self, **kwargs):
            if remaining[0] > 0:
                remaining[0] -= 1
                raise ConstructionFailure("could not build a connection")
            super().__init__(**kwargs)

    return FailingConnection


class TestConnectionPool:
    def get_pool(
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
        return pool

    @pytest.mark.fixed_client
    def test_connection_creation(self):
        connection_kwargs = {
            "foo": "bar",
            "biz": "baz",
        }
        pool = self.get_pool(
            connection_kwargs=connection_kwargs, connection_class=DummyConnection
        )

        connection = pool.get_connection()
        assert isinstance(connection, DummyConnection)
        assert_kwargs_subset(connection.kwargs, connection_kwargs)

    def test_custom_connection_disables_maint_notifications(self):
        pool = redis.ConnectionPool(connection_class=DummyConnection)

        assert pool.maint_notifications_enabled() is None
        assert "maint_notifications_config" not in pool.connection_kwargs
        assert "maint_notifications_pool_handler" not in pool.connection_kwargs

    def test_custom_connection_rejects_enabled_maint_notifications_config(self):
        with pytest.raises(
            redis.RedisError,
            match=(
                "Maintenance notifications are not supported with .*DummyConnection"
            ),
        ):
            redis.ConnectionPool(
                connection_class=DummyConnection,
                maint_notifications_config=MaintNotificationsConfig(enabled=True),
            )

    @pytest.mark.fixed_client
    def test_closing(self):
        connection_kwargs = {"foo": "bar", "biz": "baz"}
        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            max_connections=None,
            **connection_kwargs,
        )
        with closing(pool):
            pass

    def test_multiple_connections(self, master_host):
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = pool.get_connection()
        c2 = pool.get_connection()
        assert c1 != c2

    def test_max_connections(self, master_host):
        # Use DummyConnection to avoid actual connection to Redis
        # This prevents authentication issues and makes the test more reliable
        # while still properly testing the MaxConnectionsError behavior
        pool = self.get_pool(max_connections=2, connection_class=DummyConnection)
        pool.get_connection()
        pool.get_connection()
        with pytest.raises(redis.MaxConnectionsError):
            pool.get_connection()

    def test_capacity_survives_failed_connection_creation(self):
        pool = self.get_pool(
            max_connections=2, connection_class=failing_connection_class(2)
        )
        for _ in range(2):
            with pytest.raises(ConstructionFailure):
                pool.get_connection()

        c1 = pool.get_connection()
        c2 = pool.get_connection()
        assert c1 != c2
        with pytest.raises(redis.MaxConnectionsError):
            pool.get_connection()

    @pytest.mark.fixed_client
    def test_failed_re_auth_does_not_stop_the_other_connections(self):
        pool = self.get_pool(connection_class=DummyConnection)
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
            pool.re_auth_callback(token)

        other.send_command.assert_called_once_with("AUTH", "oid", "value")
        in_use.set_re_auth_token.assert_called_once_with(token)

    @pytest.mark.fixed_client
    def test_re_auth_stops_at_authentication_error(self):
        pool = self.get_pool(connection_class=DummyConnection)
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
            pool.re_auth_callback(token)

        failing.read_response.assert_called_once()
        other.send_command.assert_not_called()
        in_use.set_re_auth_token.assert_not_called()

    def test_reuse_previously_released_connection(self, master_host):
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = pool.get_connection()
        pool.release(c1)
        c2 = pool.get_connection()
        assert c1 == c2

    def test_release_not_owned_connection(self, master_host):
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool1 = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = pool1.get_connection()
        pool2 = self.get_pool(
            connection_kwargs={"host": master_host[0], "port": master_host[1]}
        )
        c2 = pool2.get_connection()
        pool2.release(c2)

        assert len(pool2._available_connections) == 1

        pool2.release(c1)
        assert len(pool2._available_connections) == 1

    @pytest.mark.fixed_client
    def test_repr_contains_db_info_tcp(self):
        connection_kwargs = {
            "host": "localhost",
            "port": 6379,
            "db": 1,
            "client_name": "test-client",
        }
        pool = self.get_pool(
            connection_kwargs=connection_kwargs, connection_class=redis.Connection
        )
        expected = "host=localhost,port=6379,db=1,client_name=test-client"
        assert expected in repr(pool)

    @pytest.mark.fixed_client
    def test_repr_contains_db_info_unix(self):
        connection_kwargs = {"path": "/abc", "db": 1, "client_name": "test-client"}
        pool = self.get_pool(
            connection_kwargs=connection_kwargs,
            connection_class=redis.UnixDomainSocketConnection,
        )
        expected = "path=/abc,db=1,client_name=test-client"
        assert expected in repr(pool)

    def test_pool_disconnect(self, master_host):
        connection_kwargs = {
            "host": master_host[0],
            "port": master_host[1],
        }
        pool = self.get_pool(connection_kwargs=connection_kwargs)

        conn = pool.get_connection()
        pool.disconnect()
        assert not conn._sock

        conn.connect()
        pool.disconnect(inuse_connections=False)
        assert conn._sock

    def test_pool_context_manager(self):
        pool = self.get_pool(connection_class=DummyConnection)

        with pool as entered:
            assert entered is pool
            conn = pool.get_connection()
            conn.connect()
            assert conn._sock is not None

        # exiting the context closes the pool, disconnecting all connections
        assert conn._sock is None


class TestBlockingConnectionPool:
    def get_pool(self, connection_kwargs=None, max_connections=10, timeout=20):
        connection_kwargs = connection_kwargs or {}
        pool = redis.BlockingConnectionPool(
            connection_class=DummyConnection,
            max_connections=max_connections,
            timeout=timeout,
            **connection_kwargs,
        )
        return pool

    def test_connection_creation(self, master_host):
        connection_kwargs = {
            "foo": "bar",
            "biz": "baz",
            "host": master_host[0],
            "port": master_host[1],
        }

        pool = self.get_pool(connection_kwargs=connection_kwargs)
        connection = pool.get_connection()
        assert isinstance(connection, DummyConnection)
        assert_kwargs_subset(connection.kwargs, connection_kwargs)

    def test_multiple_connections(self, master_host):
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = pool.get_connection()
        c2 = pool.get_connection()
        assert c1 != c2

    def test_capacity_survives_failed_connection_creation(self):
        pool = redis.BlockingConnectionPool(
            connection_class=failing_connection_class(2),
            max_connections=2,
            timeout=0.1,
        )
        for _ in range(2):
            with pytest.raises(ConstructionFailure):
                pool.get_connection()

        c1 = pool.get_connection()
        c2 = pool.get_connection()
        assert c1 != c2
        with pytest.raises(redis.ConnectionError):
            pool.get_connection()

    def test_connection_pool_blocks_until_timeout(self, master_host):
        "When out of connections, block for timeout seconds, then raise"
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(
            max_connections=1, timeout=0.1, connection_kwargs=connection_kwargs
        )
        pool.get_connection()

        start = time.monotonic()
        with mock.patch("redis.connection.record_connection_timeout") as record:
            with pytest.raises(redis.ConnectionError):
                pool.get_connection()
        # we should have waited at least 0.1 seconds
        assert time.monotonic() - start >= 0.1
        record.assert_called_once()

    def test_connection_pool_blocks_until_conn_available(self, master_host):
        """
        When out of connections, block until another connection is released
        to the pool
        """
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(
            max_connections=1, timeout=2, connection_kwargs=connection_kwargs
        )
        c1 = pool.get_connection()

        def target():
            time.sleep(0.1)
            pool.release(c1)

        start = time.monotonic()
        Thread(target=target).start()
        pool.get_connection()
        elapsed = time.monotonic() - start
        # block until the release and wake on it, well before the 2 s pool timeout
        assert 0.1 <= elapsed < 1.0

    def test_reuse_previously_released_connection(self, master_host):
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = pool.get_connection()
        pool.release(c1)
        c2 = pool.get_connection()
        assert c1 == c2

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
        pool = redis.ConnectionPool(
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

    @pytest.mark.onlynoncluster
    @skip_if_resp_version(2)
    @skip_if_server_version_lt("7.4.0")
    def test_initialise_pool_with_cache(self, master_host):
        pool = redis.BlockingConnectionPool(
            connection_class=Connection,
            host=master_host[0],
            port=master_host[1],
            protocol=3,
            cache_config=CacheConfig(),
        )
        assert isinstance(pool.get_connection(), CacheProxyConnection)

    def test_pool_disconnect(self, master_host):
        connection_kwargs = {
            "foo": "bar",
            "biz": "baz",
            "host": master_host[0],
            "port": master_host[1],
        }
        pool = self.get_pool(connection_kwargs=connection_kwargs)

        conn = pool.get_connection()
        pool.disconnect()
        assert not conn._sock

        conn.connect()
        pool.disconnect(inuse_connections=False)
        assert conn._sock

    def test_pool_context_manager(self):
        pool = self.get_pool()

        with pool as entered:
            assert entered is pool
            conn = pool.get_connection()
            conn.connect()
            assert conn._sock is not None

        # exiting the context closes the pool, disconnecting all connections
        assert conn._sock is None


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

    def test_default_protocol_enables_maint_notifications(self):
        """When ``protocol`` is unspecified the pool resolves to RESP3 and
        auto-enables maintenance notifications, injecting handler keys into
        ``connection_kwargs``."""
        pool = redis.ConnectionPool.from_url("redis://localhost")
        kwargs = pool.connection_kwargs
        assert kwargs.get("host") == "localhost"
        # ``protocol=None`` (i.e., absent from the URL) must be preserved on
        # the pool kwargs so higher layers can distinguish "empty" from an
        # explicit user choice.
        assert kwargs.get("protocol") is None
        assert isinstance(
            kwargs.get("maint_notifications_config"), MaintNotificationsConfig
        )
        assert isinstance(
            kwargs.get("maint_notifications_pool_handler"),
            MaintNotificationsPoolHandler,
        )
        assert "orig_host_address" in kwargs
        assert "orig_socket_timeout" in kwargs
        assert "orig_socket_connect_timeout" in kwargs

    def test_explicit_resp2_does_not_enable_maint_notifications(self):
        """Pinning ``protocol=2`` keeps the legacy behavior — no maintenance
        notifications keys are injected."""
        pool = redis.ConnectionPool.from_url("redis://localhost?protocol=2")
        kwargs = pool.connection_kwargs
        assert kwargs.get("host") == "localhost"
        assert kwargs.get("protocol") == 2
        assert "maint_notifications_config" not in kwargs
        assert "maint_notifications_pool_handler" not in kwargs
        assert "orig_host_address" not in kwargs

    def test_explicit_resp3_enables_maint_notifications(self):
        """Pinning ``protocol=3`` enables maintenance notifications in the
        same way as the default (unspecified) case."""
        pool = redis.ConnectionPool.from_url("redis://localhost?protocol=3")
        kwargs = pool.connection_kwargs
        assert kwargs.get("protocol") == 3
        assert isinstance(
            kwargs.get("maint_notifications_config"), MaintNotificationsConfig
        )

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
            "&socket_keepalive=&retry_on_timeout=Yes&max_connections=10&timeout=42"
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
        assert pool.timeout == 42.0

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

    def test_client_disables_maint_notifications(self):
        client = redis.Redis(unix_socket_path="/socket")
        pool = client.connection_pool

        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(pool.connection_kwargs, {"path": "/socket"})
        assert pool.maint_notifications_enabled() is None
        assert "maint_notifications_config" not in pool.connection_kwargs
        assert "maint_notifications_pool_handler" not in pool.connection_kwargs

    def test_client_enables_client_side_caching(self):
        resolver = StaticMetadataResolver()
        client = redis.Redis(
            unix_socket_path="/socket",
            protocol=3,
            cache_config=CacheConfig(),
            metadata_resolver=resolver,
        )
        pool = client.connection_pool

        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert client.get_cache() is not None
        assert pool.metadata_resolver is resolver
        assert isinstance(pool.make_connection(), CacheProxyConnection)

    def test_from_url_enables_client_side_caching(self):
        pool = redis.ConnectionPool.from_url(
            "unix:///socket?protocol=3", cache_config=CacheConfig()
        )

        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert pool.cache is not None
        assert isinstance(pool.make_connection(), CacheProxyConnection)

    def test_client_respects_disabled_maint_notifications_config(self):
        client = redis.Redis(
            unix_socket_path="/socket",
            maint_notifications_config=MaintNotificationsConfig(enabled=False),
        )
        pool = client.connection_pool

        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(pool.connection_kwargs, {"path": "/socket"})
        assert pool.maint_notifications_enabled() is None
        assert pool._maint_notifications_pool_handler is None
        # The disabled config is consumed by ConnectionPool and should not be
        # propagated to individual Unix socket connections.
        assert "maint_notifications_config" not in pool.connection_kwargs
        assert "maint_notifications_pool_handler" not in pool.connection_kwargs

    def test_client_rejects_enabled_maint_notifications_config(self):
        with pytest.raises(
            redis.RedisError,
            match=(
                "Maintenance notifications are not supported with Unix "
                "domain socket connections"
            ),
        ):
            redis.Redis(
                unix_socket_path="/socket",
                maint_notifications_config=MaintNotificationsConfig(enabled=True),
            )

    def test_pool_rejects_enabled_maint_notifications_config(self):
        with pytest.raises(
            redis.RedisError,
            match=(
                "Maintenance notifications are not supported with "
                ".*UnixDomainSocketConnection"
            ),
        ):
            redis.ConnectionPool(
                connection_class=redis.UnixDomainSocketConnection,
                path="/socket",
                maint_notifications_config=MaintNotificationsConfig(enabled=True),
            )

    def test_pool_disables_default_maint_notifications(self):
        pool = redis.ConnectionPool(
            connection_class=redis.UnixDomainSocketConnection,
            path="/socket",
            protocol=3,
        )

        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert_kwargs_subset(pool.connection_kwargs, {"path": "/socket", "protocol": 3})
        assert pool.maint_notifications_enabled() is None
        assert pool._maint_notifications_pool_handler is None
        assert "maint_notifications_config" not in pool.connection_kwargs
        assert "maint_notifications_pool_handler" not in pool.connection_kwargs

    def test_default_config_client_executes_commands_without_maint_notifications(self):
        client = redis.Redis(unix_socket_path="/socket")
        key = "redis-py:unix-socket-default-config"
        responses = [
            {"proto": 3},
            b"OK",
            b"OK",
            b"PONG",
            b"OK",
            b"value",
        ]
        socket_mock = mock.MagicMock()

        with (
            mock.patch.object(
                redis.UnixDomainSocketConnection, "_connect", return_value=socket_mock
            ),
            mock.patch.object(
                redis.UnixDomainSocketConnection, "can_read", return_value=False
            ),
            mock.patch.object(
                redis.UnixDomainSocketConnection, "send_command"
            ) as send_command,
            mock.patch.object(
                redis.UnixDomainSocketConnection, "read_response", side_effect=responses
            ),
            mock.patch.object(
                redis.UnixDomainSocketConnection, "_enable_maintenance_notifications"
            ) as enable,
        ):
            assert client.ping() is True
            assert client.set(key, "value") is True
            assert client.get(key) == b"value"

        client.close()
        enable.assert_not_called()
        sent_commands = [command.args for command in send_command.call_args_list]
        assert ("PING",) in sent_commands
        assert ("SET", key, "value") in sent_commands
        assert ("GET", key) in sent_commands
        for command in sent_commands:
            assert command[:2] != ("CLIENT", "MAINT_NOTIFICATIONS")

    def test_connection_does_not_activate_maint_notifications(self):
        pool = redis.ConnectionPool(
            connection_class=redis.UnixDomainSocketConnection,
            path="/socket",
        )
        conn = pool.make_connection()

        with mock.patch.object(conn, "_enable_maintenance_notifications") as enable:
            conn.activate_maint_notifications_handling_if_enabled()

        enable.assert_not_called()

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
@pytest.mark.skipif(not SSL_AVAILABLE, reason="SSL not installed")
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

    def test_ocsp_options(self):
        pool = redis.ConnectionPool.from_url(
            "rediss://my.host?ssl_validate_ocsp=True&ssl_validate_ocsp_stapled=False"
        )
        assert_kwargs_subset(
            pool.connection_kwargs,
            {"ssl_validate_ocsp": True, "ssl_validate_ocsp_stapled": False},
        )

    def test_ssl_flags_config_parsing(self):
        class DummyConnectionPool(redis.ConnectionPool):
            def get_connection(self):
                return self.make_connection()

        pool = DummyConnectionPool.from_url(
            "rediss://?ssl_include_verify_flags=VERIFY_X509_STRICT,VERIFY_CRL_CHECK_CHAIN"
        )

        assert pool.get_connection().ssl_include_verify_flags == [
            ssl.VerifyFlags.VERIFY_X509_STRICT,
            ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN,
        ]

        pool = DummyConnectionPool.from_url(
            "rediss://?ssl_include_verify_flags=[VERIFY_X509_STRICT, VERIFY_CRL_CHECK_CHAIN]"
        )

        assert pool.get_connection().ssl_include_verify_flags == [
            ssl.VerifyFlags.VERIFY_X509_STRICT,
            ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN,
        ]

        pool = DummyConnectionPool.from_url(
            "rediss://?ssl_exclude_verify_flags=VERIFY_X509_STRICT, VERIFY_CRL_CHECK_CHAIN"
        )

        assert pool.get_connection().ssl_exclude_verify_flags == [
            ssl.VerifyFlags.VERIFY_X509_STRICT,
            ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN,
        ]

        pool = DummyConnectionPool.from_url(
            "rediss://?ssl_include_verify_flags=VERIFY_X509_STRICT, VERIFY_CRL_CHECK_CHAIN&ssl_exclude_verify_flags=VERIFY_CRL_CHECK_LEAF"
        )

        assert pool.get_connection().ssl_include_verify_flags == [
            ssl.VerifyFlags.VERIFY_X509_STRICT,
            ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN,
        ]
        assert pool.get_connection().ssl_exclude_verify_flags == [
            ssl.VerifyFlags.VERIFY_CRL_CHECK_LEAF,
        ]

    def test_ssl_flags_config_invalid_flag(self):
        class DummyConnectionPool(redis.ConnectionPool):
            def get_connection(self):
                return self.make_connection()

        with pytest.raises(ValueError):
            DummyConnectionPool.from_url(
                "rediss://?ssl_include_verify_flags=[VERIFY_X509,VERIFY_CRL_CHECK_CHAIN]"
            )

        with pytest.raises(ValueError):
            DummyConnectionPool.from_url(
                "rediss://?ssl_exclude_verify_flags=[VERIFY_X509_STRICT1, VERIFY_CRL_CHECK_CHAIN]"
            )


class TestConnection:
    @pytest.mark.fixed_client
    def test_on_connect_error(self):
        """
        An error in Connection.on_connect should disconnect from the server
        see for details: https://github.com/andymccurdy/redis-py/issues/368
        """
        # this assumes the Redis server being tested against doesn't have
        # 9999 databases ;)
        bad_connection = redis.Redis(db=9999)
        # an error should be raised on connect
        with pytest.raises(redis.RedisError):
            bad_connection.info()
        pool = bad_connection.connection_pool
        assert len(pool._available_connections) == 1
        assert not pool._available_connections[0]._sock

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    def test_busy_loading_disconnects_socket(self, r):
        """
        If Redis raises a LOADING error, the connection should be
        disconnected and a BusyLoadingError raised
        """
        with pytest.raises(redis.BusyLoadingError):
            r.execute_command("DEBUG", "ERROR", "LOADING fake message")
        assert not r.connection._sock

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    def test_busy_loading_from_pipeline_immediate_command(self, r):
        """
        BusyLoadingErrors should raise from Pipelines that execute a
        command immediately, like WATCH does.
        """
        pipe = r.pipeline()
        with pytest.raises(redis.BusyLoadingError):
            pipe.immediate_execute_command("DEBUG", "ERROR", "LOADING fake message")
        pool = r.connection_pool
        assert pipe.connection
        assert pipe.connection in pool._in_use_connections
        assert not pipe.connection._sock

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    def test_busy_loading_from_pipeline(self, r):
        """
        BusyLoadingErrors should be raised from a pipeline execution
        regardless of the raise_on_error flag.
        """
        pipe = r.pipeline()
        pipe.execute_command("DEBUG", "ERROR", "LOADING fake message")
        with pytest.raises(redis.BusyLoadingError):
            pipe.execute()
        pool = r.connection_pool
        assert not pipe.connection
        assert len(pool._available_connections) == 1
        assert not pool._available_connections[0]._sock

    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    def test_read_only_error(self, r):
        "READONLY errors get turned into ReadOnlyError exceptions"
        with pytest.raises(redis.ReadOnlyError):
            r.execute_command("DEBUG", "ERROR", "READONLY blah blah")

    def test_oom_error(self, r):
        "OOM errors get turned into OutOfMemoryError exceptions"
        with pytest.raises(redis.OutOfMemoryError):
            # note: don't use the DEBUG OOM command since it's not the same
            # as the db being full
            r.execute_command("DEBUG", "ERROR", "OOM blah blah")

    @pytest.mark.fixed_client
    def test_connect_from_url_tcp(self):
        connection = redis.Redis.from_url("redis://localhost:6379?db=0")
        pool = connection.connection_pool

        match = re.match(
            r"< .*?([^\.]+) \( < .*?([^\.]+) \( (.+) \) > \) >", repr(pool), re.VERBOSE
        )
        pool_name, conn_name, kwargs_str = match.groups()
        assert pool_name == "ConnectionPool"
        assert conn_name == "Connection"
        # ``kwargs_str`` may contain extra auto-injected keys (e.g. maintenance
        # notifications fields when the pool resolves to RESP3); only assert
        # the URL-parsed pieces are present.
        for token in ("db=0", "host=localhost", "port=6379"):
            assert token in kwargs_str

    @pytest.mark.fixed_client
    def test_connect_from_url_unix(self):
        connection = redis.Redis.from_url("unix:///path/to/socket")
        pool = connection.connection_pool

        match = re.match(
            r"< .*?([^\.]+) \( < .*?([^\.]+) \( (.+) \) > \) >", repr(pool), re.VERBOSE
        )
        pool_name, conn_name, kwargs_str = match.groups()
        assert pool_name == "ConnectionPool"
        assert conn_name == "UnixDomainSocketConnection"
        assert "path=/path/to/socket" in kwargs_str

    @skip_if_redis_enterprise()
    def test_connect_no_auth_configured(self, r):
        """
        AuthenticationError should be raised when the server is not configured with auth
        but credentials are supplied by the user.
        """
        # Redis < 6
        with pytest.raises(redis.AuthenticationError):
            r.execute_command(
                "DEBUG", "ERROR", "ERR Client sent AUTH, but no password is set"
            )

        # Redis >= 6
        with pytest.raises(redis.AuthenticationError):
            r.execute_command(
                "DEBUG",
                "ERROR",
                "ERR AUTH <password> called without any password "
                "configured for the default user. Are you sure "
                "your configuration is correct?",
            )

    @skip_if_redis_enterprise()
    def test_connect_invalid_auth_credentials_supplied(self, r):
        """
        AuthenticationError should be raised when sending invalid username/password
        """
        # Redis < 6
        with pytest.raises(redis.AuthenticationError):
            r.execute_command("DEBUG", "ERROR", "ERR invalid password")

        # Redis >= 6
        with pytest.raises(redis.AuthenticationError):
            r.execute_command("DEBUG", "ERROR", "WRONGPASS")


@pytest.mark.onlynoncluster
class TestMultiConnectionClient:
    @pytest.fixture()
    def r(self, request):
        return _get_client(redis.Redis, request, single_connection_client=False)

    def test_multi_connection_command(self, r):
        assert not r.connection
        assert r.set("a", "123")
        assert r.get("a") == b"123"


@pytest.mark.onlynoncluster
class TestHealthCheck:
    interval = 60

    @pytest.fixture()
    def r(self, request):
        return _get_client(redis.Redis, request, health_check_interval=self.interval)

    def assert_interval_advanced(self, connection, start):
        # next_health_check is set from the clock when a reply is read,
        # which happens between `start` and now
        now = time.monotonic()
        assert (
            start + self.interval <= connection.next_health_check <= now + self.interval
        )

    def test_health_check_runs(self, r):
        r.connection.next_health_check = time.monotonic() - 1
        start = time.monotonic()
        r.connection.check_health()
        self.assert_interval_advanced(r.connection, start)

    def test_arbitrary_command_invokes_health_check(self, r):
        # invoke a command to make sure the connection is entirely setup
        r.get("foo")
        r.connection.next_health_check = time.monotonic() - 1
        start = time.monotonic()
        with mock.patch.object(
            r.connection, "send_command", wraps=r.connection.send_command
        ) as m:
            r.get("foo")
            m.assert_called_with("PING", check_health=False)

        self.assert_interval_advanced(r.connection, start)

    def test_arbitrary_command_advances_next_health_check(self, r):
        r.get("foo")
        next_health_check = r.connection.next_health_check
        r.get("foo")
        assert next_health_check < r.connection.next_health_check

    def test_health_check_not_invoked_within_interval(self, r):
        r.get("foo")
        with mock.patch.object(
            r.connection, "send_command", wraps=r.connection.send_command
        ) as m:
            r.get("foo")
            ping_call_spec = (("PING",), {"check_health": False})
            assert ping_call_spec not in m.call_args_list

    def test_health_check_in_pipeline(self, r):
        with r.pipeline(transaction=False) as pipe:
            pipe.connection = pipe.connection_pool.get_connection()
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                responses = pipe.set("foo", "bar").get("foo").execute()
                m.assert_any_call("PING", check_health=False)
                assert responses == [True, b"bar"]

    def test_health_check_in_transaction(self, r):
        with r.pipeline(transaction=True) as pipe:
            pipe.connection = pipe.connection_pool.get_connection()
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                responses = pipe.set("foo", "bar").get("foo").execute()
                m.assert_any_call("PING", check_health=False)
                assert responses == [True, b"bar"]

    def test_health_check_in_watched_pipeline(self, r):
        r.set("foo", "bar")
        with r.pipeline(transaction=False) as pipe:
            pipe.connection = pipe.connection_pool.get_connection()
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                start = time.monotonic()
                pipe.watch("foo")
                # the health check should be called when watching
                m.assert_called_with("PING", check_health=False)
                self.assert_interval_advanced(pipe.connection, start)
                assert pipe.get("foo") == b"bar"

                # reset the mock to clear the call list and schedule another
                # health check
                m.reset_mock()
                pipe.connection.next_health_check = 0

                pipe.multi()
                responses = pipe.set("foo", "not-bar").get("foo").execute()
                assert responses == [True, b"not-bar"]
                m.assert_any_call("PING", check_health=False)

    def test_health_check_in_pubsub_before_subscribe(self, r):
        "A health check happens before the first [p]subscribe"
        p = r.pubsub()
        p.connection = p.connection_pool.get_connection()
        p.connection.next_health_check = 0
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            assert not p.subscribed
            start = time.monotonic()
            p.subscribe("foo")
            # the connection is not yet in pubsub mode, so the normal
            # ping/pong within connection.send_command should check
            # the health of the connection
            m.assert_any_call("PING", check_health=False)
            self.assert_interval_advanced(p.connection, start)

            subscribe_message = wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"

    def test_health_check_in_pubsub_after_subscribed(self, r):
        """
        Pubsub can handle a new subscribe when it's time to check the
        connection health
        """
        p = r.pubsub()
        p.connection = p.connection_pool.get_connection()
        p.connection.next_health_check = 0
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            start = time.monotonic()
            p.subscribe("foo")
            subscribe_message = wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            self.assert_interval_advanced(p.connection, start)
            # because we weren't subscribed when sending the subscribe
            # message to 'foo', the connection's standard check_health ran
            # prior to subscribing.
            m.assert_any_call("PING", check_health=False)

            p.connection.next_health_check = 0
            m.reset_mock()

            start = time.monotonic()
            p.subscribe("bar")
            # the second subscribe issues exactly only command (the subscribe)
            # and the health check is not invoked
            m.assert_called_once_with("SUBSCRIBE", "bar", check_health=False)

            # since no message has been read since the health check was
            # reset, it should still be 0
            assert p.connection.next_health_check == 0

            subscribe_message = wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            assert p.get_message(timeout=1) is None
            # now that the connection is subscribed, the pubsub health
            # check should have taken over and include the HEALTH_CHECK_MESSAGE
            m.assert_any_call("PING", p.HEALTH_CHECK_MESSAGE, check_health=False)
            self.assert_interval_advanced(p.connection, start)

    def test_health_check_in_pubsub_poll(self, r):
        """
        Polling a pubsub connection that's subscribed will regularly
        check the connection's health.
        """
        p = r.pubsub()
        p.connection = p.connection_pool.get_connection()
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            start = time.monotonic()
            p.subscribe("foo")
            subscribe_message = wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            self.assert_interval_advanced(p.connection, start)

            # polling the connection before the health check interval
            # doesn't result in another health check
            m.reset_mock()
            next_health_check = p.connection.next_health_check
            # one poll is enough: every get_message runs check_health first
            assert p.get_message(timeout=0) is None
            assert p.connection.next_health_check == next_health_check
            m.assert_not_called()

            # reset the health check and poll again
            # we should not receive a pong message, but the next_health_check
            # should be advanced
            p.connection.next_health_check = 0
            start = time.monotonic()
            assert p.get_message(timeout=1) is None
            m.assert_called_with("PING", p.HEALTH_CHECK_MESSAGE, check_health=False)
            self.assert_interval_advanced(p.connection, start)


@pytest.mark.fixed_client
class TestConnectionPoolReleasedEventEmission:
    """Tests for AfterConnectionReleasedEvent emission from ConnectionPool."""

    def test_connection_released_event_emitted_on_release(self):
        """Test that AfterConnectionReleasedEvent is emitted when releasing a connection."""
        event_dispatcher = EventDispatcher()
        listener = MagicMock(spec=EventListenerInterface)
        event_dispatcher.register_listeners(
            {
                AfterConnectionReleasedEvent: [listener],
            }
        )

        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            event_dispatcher=event_dispatcher,
        )

        conn = pool.get_connection()
        pool.release(conn)

        listener.listen.assert_called_once()
        event = listener.listen.call_args[0][0]
        assert isinstance(event, AfterConnectionReleasedEvent)

    def test_connection_released_event_not_emitted_for_foreign_connection(self):
        """Test that AfterConnectionReleasedEvent is NOT emitted for connections not owned by pool."""
        event_dispatcher = EventDispatcher()
        listener = MagicMock(spec=EventListenerInterface)
        event_dispatcher.register_listeners(
            {
                AfterConnectionReleasedEvent: [listener],
            }
        )

        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            event_dispatcher=event_dispatcher,
        )

        # Create a connection that doesn't belong to this pool
        foreign_conn = DummyConnection()

        pool.release(foreign_conn)

        # Event should NOT be emitted for foreign connection
        listener.listen.assert_not_called()
