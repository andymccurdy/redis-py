"""Regression tests for the connection diagnostic details helper.

Deliberately narrow: the debug log *wording* is not pinned here, because that
would make every message tweak a test failure. What is pinned is the behaviour
the logging change relies on:

- `CacheProxyConnection._host_error()` returning the wrapped host (it used to
  drop the value, rendering "Timeout reading from None").
- `extract_connection_details()` not mangling AF_UNIX paths and not raising from
  a failure path - it runs inside pool locks while handling errors. That includes
  real `UnixDomainSocketConnection`s, which carry a `path` instead of a
  `host`/`port` pair.
- Sync/async parity of the emitted fields, which AGENTS.md requires.
- Nothing being formatted when DEBUG is off, since these sites are on the
  per-command path.
"""

import logging
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import redis.asyncio as redis_async
from redis import Redis
from redis._parsers import _RESP3Parser
from redis._parsers.socket import SocketBuffer
from redis.asyncio.connection import Connection as AsyncConnection
from redis.asyncio.connection import (
    UnixDomainSocketConnection as AsyncUnixDomainSocketConnection,
)
from redis.backoff import NoBackoff
from redis.connection import (
    CacheProxyConnection,
    Connection,
    ConnectionPool,
    UnixDomainSocketConnection,
)
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

LOCAL_PORT = 54321
PEER_IP = "10.1.2.3"
UDS_PATH = "/tmp/redis.sock"


class FakeSock:
    def __init__(self, sockname=("127.0.0.1", LOCAL_PORT), peername=(PEER_IP, 6379)):
        self._sockname = sockname
        self._peername = peername

    def getsockname(self):
        return self._sockname

    def getpeername(self):
        return self._peername

    def gettimeout(self):
        return 0.3

    def close(self):
        pass

    def shutdown(self, how):
        pass


class FakeWriter:
    def __init__(self, sockname=("127.0.0.1", LOCAL_PORT), peername=(PEER_IP, 6379)):
        self._info = {"sockname": sockname, "peername": peername}

    def get_extra_info(self, name):
        return self._info.get(name)

    def close(self):
        pass


def _sync_conn(connected=True):
    conn = Connection(host="myhost.example.com", port=6379)
    if connected:
        conn._sock = FakeSock()
    return conn


def _async_conn(connected=True):
    conn = AsyncConnection(host="myhost.example.com", port=6379)
    if connected:
        conn._writer = FakeWriter()
    return conn


def _sync_uds_conn(connected=True):
    conn = UnixDomainSocketConnection(path=UDS_PATH)
    if connected:
        conn._sock = FakeSock(sockname=UDS_PATH, peername=UDS_PATH)
    return conn


def _async_uds_conn(connected=True):
    conn = AsyncUnixDomainSocketConnection(path=UDS_PATH)
    if connected:
        conn._writer = FakeWriter(sockname=UDS_PATH, peername=UDS_PATH)
    return conn


@pytest.mark.fixed_client
class TestExtractConnectionDetails:
    @pytest.mark.parametrize(
        "factory", [_sync_conn, _async_conn, _sync_uds_conn, _async_uds_conn]
    )
    def test_not_connected(self, factory):
        assert factory(connected=False).extract_connection_details() == "not connected"

    def test_field_layout_matches_across_stacks(self):
        """Same fields, same order, same values - except the one that cannot match.

        `active read timeout` legitimately differs: sync reads the deadline armed
        on the socket, async only has one while a read is actually in flight.
        """

        def parts(details):
            return [p for p in details.split(", ") if not p.startswith("active read")]

        sync_details = _sync_conn().extract_connection_details()
        async_details = _async_conn().extract_connection_details()
        assert "active read timeout" in sync_details
        assert "active read timeout" in async_details
        assert parts(sync_details) == parts(async_details)

    def test_sync_unix_socket_path_is_not_sliced_into_a_port(self):
        """AF_UNIX reports a path string; indexing it yielded a bogus port char."""
        conn = _sync_conn(connected=False)
        conn._sock = FakeSock(sockname="/tmp/redis.sock", peername="/tmp/redis.sock")
        assert "local socket port: None" in conn.extract_connection_details()

    def test_async_unix_socket_path_is_not_sliced_into_a_port(self):
        conn = _async_conn(connected=False)
        conn._writer = FakeWriter(
            sockname="/tmp/redis.sock", peername="/tmp/redis.sock"
        )
        assert "local socket port: None" in conn.extract_connection_details()

    @pytest.mark.parametrize("factory", [_sync_uds_conn, _async_uds_conn])
    def test_unix_domain_socket_details_do_not_raise(self, factory):
        """A UDS connection has no `host`/`port`; rendering used to raise.

        The helper runs on every command failure path before `disconnect()`, so
        raising here replaced the real TimeoutError/ConnectionError with an
        AttributeError and left the connection open.
        """
        details = factory().extract_connection_details()
        assert UDS_PATH in details
        assert "local socket port: None" in details
        # AF_UNIX getpeername() returns the path, not a (host, port) tuple.
        assert "connected to ip None" in details

    def test_unix_domain_socket_field_layout_matches_across_stacks(self):
        def parts(details):
            return [p for p in details.split(", ") if not p.startswith("active read")]

        assert parts(_sync_uds_conn().extract_connection_details()) == parts(
            _async_uds_conn().extract_connection_details()
        )

    def test_sync_does_not_raise_when_getsockname_fails(self):
        conn = _sync_conn(connected=False)
        sock = FakeSock()
        sock.getsockname = MagicMock(side_effect=OSError("boom"))
        conn._sock = sock
        assert "local socket port: None" in conn.extract_connection_details()

    def test_async_does_not_raise_when_get_extra_info_fails(self):
        conn = _async_conn(connected=False)
        writer = FakeWriter()
        writer.get_extra_info = MagicMock(side_effect=OSError("boom"))
        conn._writer = writer
        assert "local socket port: None" in conn.extract_connection_details()


@pytest.mark.fixed_client
class TestDebugGating:
    """These sites are on the per-command path; nothing may be built when off."""

    def test_command_failure_logs_nothing_when_debug_disabled(self, caplog):
        client = Redis(
            host="myhost.example.com", port=6379, retry=Retry(NoBackoff(), 0)
        )
        conn = _sync_conn()
        pool = client.connection_pool
        error = RedisTimeoutError("Timeout reading from myhost.example.com:6379")
        with (
            caplog.at_level(logging.INFO, logger="redis.client"),
            patch.object(pool, "get_connection", return_value=conn),
            patch.object(pool, "release"),
            patch.object(conn, "send_command", side_effect=error),
        ):
            with pytest.raises(RedisTimeoutError):
                client.get("mykey")

        assert caplog.records == []

    def test_pool_logs_nothing_when_debug_disabled(self, caplog):
        pool = ConnectionPool(
            host="myhost.example.com", port=6379, connection_class=Connection
        )
        pool._in_use_connections.add(_sync_conn())
        pool._available_connections.append(_sync_conn())
        with caplog.at_level(logging.INFO, logger="redis.connection"):
            pool.update_active_connections_for_reconnect()
            pool.disconnect_free_connections()

        assert caplog.records == []

    @pytest.mark.asyncio
    async def test_async_pool_logs_nothing_when_debug_disabled(self, caplog):
        pool = redis_async.ConnectionPool(
            host="myhost.example.com", port=6379, connection_class=AsyncConnection
        )
        pool._in_use_connections.add(_async_conn())
        with caplog.at_level(logging.INFO, logger="redis.asyncio.connection"):
            await pool._run_proactive_reconnect_without_locking()

        assert caplog.records == []


@pytest.mark.fixed_client
class TestHostErrorRegression:
    def test_cache_proxy_connection_reports_the_wrapped_host(self):
        """It used to drop the return value, rendering 'Timeout reading from None'."""
        conn = _sync_conn()
        proxy = CacheProxyConnection(conn, MagicMock(), threading.RLock())
        assert proxy._host_error() == "myhost.example.com:6379"


class _FailingWriteSock(FakeSock):
    """A FakeSock whose sendall() fails the way a dropped peer would."""

    def sendall(self, data):
        raise OSError(32, "Broken pipe")


class _RetimeableSock(FakeSock):
    """A FakeSock whose timeout can actually be armed and read back."""

    def __init__(self, timeout):
        super().__init__()
        self._timeout = timeout

    def gettimeout(self):
        return self._timeout

    def settimeout(self, value):
        self._timeout = value


@pytest.mark.fixed_client
class TestFailureDetailsSurviveTheFailurePath:
    """The failure path closes the socket, so the details must be rendered first.

    Before this, `extract_connection_details()` ran only after `disconnect()`
    had already cleared the socket, so every timed out or failed command was
    reported as `details: not connected` - losing the resolved ip, local port
    and the timeout the read actually ran under.
    """

    def test_sync_read_failure_logs_live_socket_state(self, caplog):
        conn = _sync_conn()
        conn._parser = MagicMock()
        conn._parser.read_response.side_effect = RedisTimeoutError(
            "Timeout reading from socket"
        )

        with caplog.at_level(logging.DEBUG, logger="redis.connection"):
            with pytest.raises(RedisTimeoutError):
                conn.read_response()

        # the failure path still tears the socket down ...
        assert conn._sock is None
        # ... but the details were captured while it was alive.
        messages = [record.getMessage() for record in caplog.records]
        assert any(f"local socket port: {LOCAL_PORT}" in m for m in messages)
        assert any(f"connected to ip {PEER_IP}" in m for m in messages)
        assert not any("not connected" in m for m in messages)

    def test_sync_write_failure_logs_live_socket_state(self, caplog):
        conn = _sync_conn()
        conn._parser = MagicMock()
        conn._sock = _FailingWriteSock()

        with caplog.at_level(logging.DEBUG, logger="redis.connection"):
            with pytest.raises(RedisConnectionError):
                conn.send_packed_command([b"PING\r\n"], check_health=False)

        messages = [record.getMessage() for record in caplog.records]
        assert any(f"local socket port: {LOCAL_PORT}" in m for m in messages)
        assert not any("not connected" in m for m in messages)

    @pytest.mark.asyncio
    async def test_async_read_failure_logs_live_transport_state(self, caplog):
        conn = _async_conn()
        # is_connected() needs both halves, otherwise disconnect() returns early
        # and would leave the writer in place regardless of ordering.
        conn._reader = MagicMock()

        with (
            caplog.at_level(logging.DEBUG, logger="redis.asyncio.connection"),
            patch.object(
                conn,
                "_read_response_from_parser",
                new=AsyncMock(side_effect=OSError("boom")),
            ),
        ):
            with pytest.raises(RedisConnectionError):
                await conn.read_response()

        assert conn._writer is None
        messages = [record.getMessage() for record in caplog.records]
        assert any(f"local socket port: {LOCAL_PORT}" in m for m in messages)
        assert not any("not connected" in m for m in messages)

    def test_read_failure_renders_nothing_when_debug_disabled(self, caplog):
        """This is the per-command path; the details must not even be built."""
        conn = _sync_conn()
        conn._parser = MagicMock()
        conn._parser.read_response.side_effect = RedisTimeoutError("boom")

        with (
            caplog.at_level(logging.INFO, logger="redis.connection"),
            patch.object(
                Connection,
                "extract_connection_details",
                side_effect=AssertionError("rendered while DEBUG was off"),
            ),
        ):
            with pytest.raises(RedisTimeoutError):
                conn.read_response()

        assert caplog.records == []


@pytest.mark.fixed_client
class TestParserTimeoutIsReraisedWithTheHost:
    """The parsers raise redis TimeoutError, which is not an OSError.

    It therefore missed both typed branches in `read_response` and fell through
    to `except BaseException`, keeping the parser's undecorated
    "Timeout reading from socket". Async already reported the host, because its
    deadline surfaces as asyncio.TimeoutError.
    """

    def test_sync_timeout_message_carries_the_host(self):
        conn = _sync_conn()
        conn._parser = MagicMock()
        conn._parser.read_response.side_effect = RedisTimeoutError(
            "Timeout reading from socket"
        )

        with pytest.raises(RedisTimeoutError, match="myhost.example.com:6379"):
            conn.read_response()

    def test_sync_timeout_still_disconnects(self):
        conn = _sync_conn()
        conn._parser = MagicMock()
        conn._parser.read_response.side_effect = RedisTimeoutError("boom")

        with pytest.raises(RedisTimeoutError):
            conn.read_response()
        assert conn._sock is None

    def test_sync_timeout_does_not_disconnect_when_opted_out(self):
        conn = _sync_conn()
        conn._parser = MagicMock()
        conn._parser.read_response.side_effect = RedisTimeoutError("boom")

        with pytest.raises(RedisTimeoutError):
            conn.read_response(disconnect_on_error=False)
        assert conn._sock is not None


@pytest.mark.fixed_client
class TestRelaxedTimeoutStaysConsistent:
    """The parser caches the timeout to restore after a per-call override.

    So while a can_read() probe holds the socket at 0, that cache is the pending
    restore value and has to be updated even though the socket must not be: it
    is what arms the new timeout for the reads that follow the probe.
    """

    def test_parser_is_updated_while_socket_is_non_blocking(self):
        conn = _sync_conn()
        conn._sock = _RetimeableSock(0)

        with patch.object(Connection, "update_parser_timeout") as update_parser:
            conn.update_current_socket_timeout(30)

        # a 0 timeout means a can_read() probe is in progress - leave the socket
        # alone, but still arm the value the probe will restore.
        assert conn._sock.gettimeout() == 0
        update_parser.assert_called_once_with(30)

    def test_relaxation_arriving_during_can_read_survives_the_probe(self):
        """
        Regression: a maintenance relaxation raced against a can_read() probe.

        The probe arms timeout 0 on the socket and restores the parser's cached
        value when it finishes. A relaxation that lands inside that window can
        not touch the socket - that would turn a non-blocking read blocking -
        so if it skips the parser too, the probe restores the pre-relaxation
        timeout and the connection never sees the relaxed maintenance deadline.
        """
        conn = _sync_conn()
        conn._sock = _RetimeableSock(1)
        parser = _RESP3Parser(socket_read_size=8192)
        parser._buffer = SocketBuffer(conn._sock, 8192, 1)
        conn._parser = parser

        # A can_read(0) probe: arm the non-blocking timeout the way
        # SocketBuffer._read_from_socket does for a per-call override.
        conn._sock.settimeout(0)
        try:
            # Maintenance relaxes the pool while the probe is in flight.
            conn.update_current_socket_timeout(30)
            assert conn._sock.gettimeout() == 0, "the probe must stay non-blocking"
        finally:
            # The probe's finally clause restores from the parser's cache.
            conn._sock.settimeout(parser._buffer.socket_timeout)

        assert conn._sock.gettimeout() == 30

    def test_parser_follows_the_socket_when_blocking(self):
        conn = _sync_conn()
        conn._sock = _RetimeableSock(1)

        with patch.object(Connection, "update_parser_timeout") as update_parser:
            conn.update_current_socket_timeout(30)

        assert conn._sock.gettimeout() == 30
        update_parser.assert_called_once_with(30)

    def test_resp3_parser_receives_a_none_timeout(self):
        """None means "block indefinitely"; the RESP3 branch used to drop it."""
        conn = _sync_conn()
        conn._sock = _RetimeableSock(1)
        parser = _RESP3Parser(socket_read_size=8192)
        parser._buffer = SocketBuffer(conn._sock, 8192, 1)
        conn._parser = parser

        conn.update_current_socket_timeout(None)

        assert conn._sock.gettimeout() is None
        assert parser._buffer.socket_timeout is None
