import copy
import gc
import os
import platform
import select
import selectors
import socket
import ssl
import subprocess
import sys
import textwrap
import threading
import time
import types
import warnings
import weakref
from errno import EBADF, ECONNREFUSED, ETIMEDOUT, EWOULDBLOCK
from importlib import metadata
from typing import Any
from unittest import mock
from unittest.mock import call, patch, MagicMock, Mock

import pytest
import redis
from packaging.version import Version
from redis import ConnectionPool, Redis
from redis._parsers import _HiredisParser, _RESP2Parser, _RESP3Parser
from redis._parsers.hiredis import NOT_ENOUGH_DATA, _socket_can_read, _socket_is_closed
from redis._parsers.socket import SocketBuffer
from redis.backoff import NoBackoff
from redis.cache import (
    CacheConfig,
    CacheConfigurationInterface,
    CacheEntry,
    CacheEntryStatus,
    CacheInterface,
    CacheKey,
    CacheProxy,
    DefaultCache,
    EvictionPolicy,
    LRUPolicy,
    TrackingMode,
)
from redis.connection import (
    CacheProxyConnection,
    Connection,
    ConnectionInterface,
    HiredisRespSerializer,
    SSLConnection,
    UnixDomainSocketConnection,
    parse_url,
    BlockingConnectionPool,
)
from redis.commands.metadata import DynamicMetadataResolver
from redis.credentials import UsernamePasswordCredentialProvider
from redis.event import (
    EventDispatcher,
)
from redis.exceptions import (
    AskError,
    ConnectionError,
    InvalidResponse,
    MovedError,
    RedisError,
    ResponseError,
    TimeoutError,
)
from redis.observability.attributes import (
    CSCResult,
    DB_CLIENT_CONNECTION_POOL_NAME,
    DB_CLIENT_CONNECTION_STATE,
    ConnectionState,
    get_pool_name,
)
from redis.retry import Retry
from redis.utils import HIREDIS_AVAILABLE, SENTINEL

from .conftest import skip_if_redis_enterprise, skip_if_server_version_lt
from .mocks import MockSocket


class DummyHiredisReader:
    def __init__(self, response=NOT_ENOUGH_DATA, decoded_response=None, has_data=False):
        self.responses = [response]
        self.decoded_response = decoded_response
        self.has_data_value = has_data

    def has_data(self):
        return self.has_data_value

    def gets(self, *args):
        if self.responses:
            response = self.responses.pop(0)
            if args == (False,) or self.decoded_response is None:
                return response
            return self.decoded_response
        return NOT_ENOUGH_DATA


class DummyPushNotification(list):
    pass


def make_hiredis_parser(
    response=NOT_ENOUGH_DATA, decoded_response=None, has_data=False
):
    parser = _HiredisParser.__new__(_HiredisParser)
    parser._reader = DummyHiredisReader(response, decoded_response, has_data)
    parser._hiredis_PushNotificationType = None
    parser._sock = mock.Mock()
    parser._buffer = bytearray(65536)
    parser._socket_timeout = None
    return parser


def _wait_for_peer_close(sock, timeout=5.0):
    """Poll until the peer's FIN shows up in the socket's poll flags.

    Readability alone is not enough: data sent before the close makes the socket
    readable before the FIN arrives. macOS reports POLLHUP, Linux POLLRDHUP.
    Without POLLRDHUP (PyPy) a Linux FIN shows only as POLLIN, which buffered data
    hides, so there the wait ends once the socket is readable plus a short grace.
    """
    rdhup = getattr(select, "POLLRDHUP", None)
    closed_flags = select.POLLHUP | (rdhup or 0)
    poller = select.poll()
    poller.register(sock, select.POLLIN | closed_flags)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        events = [ev for _, ev in poller.poll(50)]
        if any(ev & closed_flags for ev in events):
            return
        if events and rdhup is None:
            time.sleep(0.1)
            return
    raise AssertionError("the peer close did not reach the socket in time")


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlynoncluster
def test_invalid_response(r):
    raw = b"x"
    parser = r.connection._parser
    with mock.patch.object(parser._buffer, "readline", return_value=raw):
        with pytest.raises(InvalidResponse, match=f"Protocol Error: {raw!r}"):
            parser.read_response()


@pytest.mark.fixed_client
def test_hiredis_can_read_detects_reader_data():
    parser = make_hiredis_parser(response=b"OK", has_data=True)

    assert parser.can_read(timeout=0) is True
    assert parser.read_response() == b"OK"


@pytest.mark.fixed_client
def test_hiredis_can_read_returns_true_for_readable_open_socket():
    # socket readable, reader empty, and not closed -> pending data/push, so
    # can_read() reports readable without consuming anything.
    parser = make_hiredis_parser(has_data=False)

    with (
        patch("redis._parsers.hiredis._socket_can_read", return_value=True) as ready,
        patch("redis._parsers.hiredis._socket_is_closed", return_value=False) as closed,
    ):
        assert parser.can_read(timeout=0) is True

    ready.assert_called_once_with(parser._sock, 0)
    closed.assert_called_once_with(parser._sock)


@pytest.mark.fixed_client
def test_hiredis_can_read_raises_on_peer_closed_socket():
    # regression for #4128: a peer-closed socket reads as ready but the reader
    # has no buffered data. can_read() must raise ConnectionError so the pool
    # recycles it, matching the pure-Python and async parsers.
    parser = make_hiredis_parser(has_data=False)

    with (
        patch("redis._parsers.hiredis._socket_can_read", return_value=True),
        patch("redis._parsers.hiredis._socket_is_closed", return_value=True),
    ):
        with pytest.raises(redis.ConnectionError):
            parser.can_read(timeout=0)


@pytest.mark.fixed_client
@pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="hiredis is not installed")
@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
def test_hiredis_can_read_consumes_pending_push_before_reporting_closed():
    # a peer that sends push data and then closes reports the closed poll flags
    # while the data is still buffered. can_read() must keep reporting readable
    # so pending messages (e.g. cache invalidations) get processed, and only
    # raise once the buffer is drained, matching the pure-Python parser which
    # consumes buffered data before raising.
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    client = socket.create_connection(listener.getsockname())
    server, _ = listener.accept()

    parser = _HiredisParser(socket_read_size=65536)
    connection = mock.Mock()
    connection._sock = client
    connection.socket_timeout = None
    connection.encoder.encoding_errors = "strict"
    connection.encoder.decode_responses = False
    parser.on_connect(connection)
    try:
        server.sendall(b">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$5\r\nhello\r\n")
        server.close()  # graceful FIN with the push message still buffered
        _wait_for_peer_close(client)

        assert parser.can_read(timeout=0) is True
        assert parser.read_response(push_request=True) == [b"message", b"ch", b"hello"]
        with pytest.raises(redis.ConnectionError):
            parser.can_read(timeout=0)
    finally:
        parser.on_disconnect()
        client.close()
        listener.close()


@pytest.mark.fixed_client
def test_socket_is_closed_reports_not_closed_with_pending_ssl_data():
    # SSL sockets buffer decrypted bytes above the OS socket layer; those must
    # be processed before the connection can be treated as closed, like
    # kernel-level pending data.
    sock = Mock(spec=["pending", "fileno"])
    sock.pending.return_value = 10

    assert _socket_is_closed(sock) is False


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
@pytest.mark.parametrize(
    "timeout,expected_poll_timeout",
    [(0, 0), (0.001, 1.0), (None, None)],
)
def test_hiredis_socket_can_read_uses_poll(timeout, expected_poll_timeout):
    sock = Mock(spec=["fileno"])
    poller = Mock()
    poller.poll.return_value = [(7, select.POLLIN)]

    with patch("redis._parsers.hiredis.select.poll", return_value=poller):
        assert _socket_can_read(sock, timeout=timeout) is True

    poller.register.assert_called_once_with(sock, select.POLLIN)
    poller.poll.assert_called_once_with(expected_poll_timeout)


@pytest.mark.fixed_client
def test_hiredis_socket_can_read_falls_back_to_default_selector():
    sock = Mock(spec=["fileno"])
    selector = MagicMock()
    selector.select.return_value = [(sock, selectors.EVENT_READ)]
    selector.__enter__.return_value = selector

    with (
        patch("redis._parsers.hiredis._HAS_POLL", False),
        patch(
            "redis._parsers.hiredis.selectors.DefaultSelector", return_value=selector
        ),
    ):
        assert _socket_can_read(sock, timeout=0.001) is True

    selector.register.assert_called_once_with(sock, selectors.EVENT_READ)
    selector.select.assert_called_once_with(0.001)
    # The selector must be used as a context manager so its fd is released; if
    # the `with` block is dropped from the implementation, this catches it.
    selector.__exit__.assert_called_once()


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(select, "poll"),
    reason="No select.poll; default selector falls back to select.select",
)
def test_hiredis_socket_can_read_handles_high_file_descriptor():
    fcntl = pytest.importorskip("fcntl")

    read_fd, write_fd = os.pipe()
    high_read_fd = None
    try:
        try:
            high_read_fd = fcntl.fcntl(read_fd, fcntl.F_DUPFD, 1024)
        except OSError as exc:
            pytest.skip(f"Could not allocate high file descriptor: {exc}")

        assert _socket_can_read(high_read_fd, timeout=0) is False
    finally:
        os.close(read_fd)
        os.close(write_fd)
        if high_read_fd is not None and high_read_fd != read_fd:
            os.close(high_read_fd)


_FD_EXHAUSTION_SCRIPT = textwrap.dedent(
    """
    import os
    import resource
    import socket

    from redis._parsers.hiredis import _socket_can_read

    readable, writable = socket.socketpair()
    empty_sock, peer = socket.socketpair()
    writable.sendall(b"x")

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(soft, 256), hard))
    try:
        while True:
            os.dup(0)
    except OSError:
        pass  # fd table is now full

    assert _socket_can_read(readable, timeout=0) is True
    assert _socket_can_read(empty_sock, timeout=0) is False
    """
)


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
def test_hiredis_socket_can_read_under_fd_exhaustion():
    # Readiness checks run on every connection acquisition, and fd pressure is
    # what pushes sockets onto high fds in the first place, so they must not
    # allocate file descriptors themselves. A per-check epoll/kqueue selector
    # raises OSError (EMFILE) here; the poll() implementation passes.
    #
    # RLIMIT_NOFILE is process-wide, so the check runs in a child interpreter
    # to avoid starving other threads/tests of file descriptors.
    pytest.importorskip("resource")

    # Point the child at the redis package under test, not whatever else
    # happens to be importable from its working directory.
    env = dict(os.environ)
    redis_root = os.path.dirname(os.path.dirname(redis.__file__))
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [redis_root, env.get("PYTHONPATH")])
    )
    result = subprocess.run(
        [sys.executable, "-c", _FD_EXHAUSTION_SCRIPT],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )

    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
def test_socket_is_closed_detects_peer_close():
    # a peer-closed socket reads as ready (it yields EOF), so readiness alone
    # cannot tell it apart from a socket holding pending data; _socket_is_closed()
    # distinguishes them via POLLHUP without consuming data.
    alive, peer = socket.socketpair()
    closed, closing_peer = socket.socketpair()
    try:
        peer.sendall(b"pending push data")
        closing_peer.close()

        assert _socket_is_closed(alive) is False
        assert _socket_is_closed(closed) is True
    finally:
        alive.close()
        peer.close()
        closed.close()


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
def test_socket_is_closed_detects_tcp_peer_half_close():
    # on Linux a graceful peer close (FIN) reports POLLIN|POLLRDHUP and never
    # POLLHUP, so a POLLHUP-only flags check misses it. a real TCP socket pair
    # is required: the kernel (not a mock) produces the event, and AF_UNIX
    # socketpairs set POLLHUP on close and would hide the gap.
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    client = socket.create_connection(listener.getsockname())
    server, _ = listener.accept()
    try:
        assert _socket_is_closed(client) is False
        server.close()  # graceful FIN
        _wait_for_peer_close(client)
        assert _socket_is_closed(client) is True
    finally:
        client.close()
        listener.close()


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
def test_socket_is_closed_defers_close_until_pending_data_is_read():
    # a peer that sends data and then closes reports the closed poll flags
    # while unread bytes remain (POLLIN|POLLHUP on macOS, POLLIN|POLLRDHUP on
    # Linux). that data may carry cache invalidations that must be processed
    # before the connection is dropped, so _socket_is_closed() must report
    # not-closed until the buffer is drained, without consuming anything.
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    client = socket.create_connection(listener.getsockname())
    server, _ = listener.accept()
    try:
        server.sendall(b"pending invalidation")
        server.close()  # graceful FIN with data still buffered
        _wait_for_peer_close(client)

        assert _socket_is_closed(client) is False
        # the MSG_PEEK confirmation must not consume the pending data
        assert client.recv(65536) == b"pending invalidation"
        assert _socket_is_closed(client) is True
    finally:
        client.close()
        listener.close()


@pytest.mark.fixed_client
def test_socket_is_closed_without_poll_reports_not_closed(monkeypatch):
    # without select.poll (e.g. Windows) closed and has-data states are
    # indistinguishable here, so we conservatively report not-closed.
    monkeypatch.setattr("redis._parsers.hiredis._HAS_POLL", False)
    closed, closing_peer = socket.socketpair()
    try:
        closing_peer.close()
        assert _socket_is_closed(closed) is False
    finally:
        closed.close()


@pytest.mark.fixed_client
def test_hiredis_can_read_does_not_decide_disable_decoding():
    raw = b"\xe2\x98\x83"
    parser = make_hiredis_parser(
        response=raw,
        decoded_response=raw.decode(),
        has_data=True,
    )

    assert parser.can_read(timeout=0) is True
    assert parser.read_response(disable_decoding=True) == raw


@pytest.mark.fixed_client
def test_hiredis_can_read_leaves_decoding_to_read_response():
    raw = b"\xe2\x98\x83"
    parser = make_hiredis_parser(
        response=raw,
        decoded_response=raw.decode(),
        has_data=True,
    )

    assert parser.can_read(timeout=0) is True
    assert parser.read_response() == raw.decode()


@pytest.mark.fixed_client
def test_hiredis_read_response_returns_initial_push_notification():
    push_response = DummyPushNotification([b"message", b"channel", b"data"])
    handled_response = object()
    parser = make_hiredis_parser()
    parser._hiredis_PushNotificationType = DummyPushNotification
    parser._reader.responses = [push_response]
    parser.pubsub_push_handler_func = Mock(return_value=handled_response)

    assert parser.read_response(push_request=True) is handled_response
    parser.pubsub_push_handler_func.assert_called_once_with(push_response)


@pytest.mark.fixed_client
def test_hiredis_read_response_skips_initial_push_notification():
    push_response = DummyPushNotification([b"message", b"channel", b"data"])
    parser = make_hiredis_parser()
    parser._hiredis_PushNotificationType = DummyPushNotification
    parser._reader.responses = [push_response, b"OK"]
    parser.pubsub_push_handler_func = Mock(return_value=push_response)

    assert parser.read_response() == b"OK"
    parser.pubsub_push_handler_func.assert_called_once_with(push_response)


@pytest.mark.fixed_client
def test_hiredis_read_response_preserves_timeout_after_initial_push_notification():
    push_response = DummyPushNotification([b"message", b"channel", b"data"])
    parser = make_hiredis_parser()
    parser._hiredis_PushNotificationType = DummyPushNotification
    parser._reader.responses = [push_response, NOT_ENOUGH_DATA]
    parser._sock.recv_into.side_effect = BlockingIOError(
        EWOULDBLOCK, "Resource temporarily unavailable"
    )
    parser.pubsub_push_handler_func = Mock(return_value=push_response)

    with pytest.raises(TimeoutError):
        parser.read_response(timeout=0)

    parser.pubsub_push_handler_func.assert_called_once_with(push_response)


@pytest.mark.fixed_client
def test_hiredis_read_response_timeout_zero_maps_would_block_to_timeout():
    parser = make_hiredis_parser()
    parser._sock.recv_into.side_effect = BlockingIOError(
        EWOULDBLOCK, "Resource temporarily unavailable"
    )

    with pytest.raises(TimeoutError):
        parser.read_response(timeout=0)


@pytest.mark.fixed_client
@pytest.mark.parametrize("cleared_attr", ["_reader", "_sock"])
def test_hiredis_read_from_socket_raises_connection_error_when_disconnected(
    cleared_attr,
):
    # regression for #4003: another thread may disconnect the connection while
    # we are reading (e.g. a shared client closed via `with redis:`), which sets
    # _sock and _reader to None. read_from_socket() must raise a descriptive,
    # retryable ConnectionError rather than an AttributeError.
    parser = make_hiredis_parser()
    parser._sock.recv_into.return_value = 10
    setattr(parser, cleared_attr, None)

    with pytest.raises(ConnectionError, match="Connection closed by server"):
        parser.read_from_socket()


@pytest.mark.fixed_client
@pytest.mark.parametrize("cleared_attr", ["_reader", "_sock"])
def test_hiredis_can_read_raises_connection_error_when_disconnected(cleared_attr):
    # a disconnect from another thread (e.g. the multi-database health check
    # taking a database out of service) clears _sock and _reader while a pub/sub
    # thread sits in can_read(). registering None with poll() raises TypeError,
    # which no retry layer acts on, so the reading thread dies instead of
    # reconnecting. can_read() must report the gone connection the same
    # retryable way read_from_socket() does.
    parser = make_hiredis_parser(has_data=False)
    setattr(parser, cleared_attr, None)

    with pytest.raises(ConnectionError, match="Connection closed by server"):
        parser.can_read(timeout=0)


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
def test_hiredis_can_read_raises_connection_error_when_socket_already_closed():
    # same race one step later: the socket object survives the concurrent
    # disconnect but its file descriptor is already closed, so poll() cannot
    # register it and raises ValueError.
    parser = make_hiredis_parser(has_data=False)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.close()
    parser._sock = sock

    with pytest.raises(ConnectionError, match="Connection closed by server"):
        parser.can_read(timeout=0)


@pytest.mark.fixed_client
def test_hiredis_read_response_uses_local_reader_if_disconnected_mid_read():
    # regression for #4003: if _reader is cleared by a concurrent disconnect
    # after read_from_socket() returns, the in-flight read must still complete
    # via the locally bound reader instead of raising AttributeError on .gets().
    parser = make_hiredis_parser()
    parser._reader.responses = [NOT_ENOUGH_DATA, b"OK"]

    def fake_read_from_socket(*args, **kwargs):
        parser._reader = None  # simulate on_disconnect() from another thread
        return True

    parser.read_from_socket = fake_read_from_socket

    assert parser.read_response() == b"OK"


@pytest.mark.fixed_client
def test_hiredis_read_reports_concurrent_close_when_timeout_restore_fails():
    # a concurrent disconnect() closes the socket as well as clearing the parser
    # state, so restoring the per-call timeout in the finally raises EBADF. an
    # exception from finally replaces the one being propagated, which would drop
    # the ConnectionError the retry and failover layers act on.
    parser = make_hiredis_parser()

    def close_then_report_eof(_):
        # only the restore fails; arming the per-call timeout already succeeded.
        parser._sock.settimeout.side_effect = OSError(EBADF, "Bad file descriptor")
        return 0

    parser._sock.recv_into.side_effect = close_then_report_eof

    with pytest.raises(ConnectionError, match="Connection closed by server"):
        parser.read_from_socket(timeout=1)


@pytest.mark.fixed_client
def test_socket_buffer_timeout_zero_maps_would_block_to_timeout():
    sock = Mock()
    sock.recv.side_effect = BlockingIOError(
        EWOULDBLOCK, "Resource temporarily unavailable"
    )
    socket_buffer = SocketBuffer(sock, socket_read_size=65536, socket_timeout=None)

    with pytest.raises(TimeoutError):
        socket_buffer.readline(timeout=0)


@pytest.mark.fixed_client
def test_socket_buffer_read_after_close_raises_connection_error():
    """
    A read on a closed buffer reports a gone connection.

    ``close()`` runs on whichever thread tears the connection down - the
    multi-database client closes connections from its health check thread - so a
    command thread can reach these reads after it. ConnectionError is what the retry
    and failover layers act on; ``ValueError: I/O operation on closed file`` escapes
    them and surfaces at the caller of the command.
    """
    socket_buffer = SocketBuffer(Mock(), socket_read_size=65536, socket_timeout=None)
    socket_buffer.close()

    with pytest.raises(ConnectionError):
        socket_buffer.readline()

    with pytest.raises(ConnectionError):
        socket_buffer.read(1)

    with pytest.raises(ConnectionError):
        socket_buffer.get_pos()

    with pytest.raises(ConnectionError):
        socket_buffer.unread_bytes()


@pytest.mark.fixed_client
def test_socket_buffer_read_reports_concurrent_close_as_connection_error():
    """
    A close that lands while the read is blocked in recv reports a gone connection.

    This is the window the test above cannot cover: the buffer is open when the read
    starts and closed by the time the data comes back.
    """
    sock = Mock()
    socket_buffer = SocketBuffer(sock, socket_read_size=65536, socket_timeout=None)

    def close_then_answer(_):
        socket_buffer.close()
        return b"+OK\r\n"

    sock.recv.side_effect = close_then_answer

    with pytest.raises(ConnectionError):
        socket_buffer.readline()


@pytest.mark.fixed_client
def test_socket_buffer_reports_concurrent_close_when_timeout_restore_fails():
    """
    A per-call timeout that can no longer be restored does not hide the close.

    ``disconnect()`` closes the socket as well as the buffer, so restoring the
    timeout in the read's ``finally`` raises EBADF. An exception from ``finally``
    replaces the one being propagated, which would drop the ConnectionError the
    retry and failover layers act on.
    """
    sock = Mock()
    socket_buffer = SocketBuffer(sock, socket_read_size=65536, socket_timeout=None)

    def close_then_report_eof(_):
        # Only the restore fails; arming the per-call timeout already succeeded.
        socket_buffer.close()
        sock.settimeout.side_effect = OSError(EBADF, "Bad file descriptor")
        return b""

    sock.recv.side_effect = close_then_report_eof

    with pytest.raises(ConnectionError, match="Connection closed by server"):
        socket_buffer.readline(timeout=1)


@pytest.mark.fixed_client
def test_socket_buffer_cleanup_after_close_does_not_raise():
    """
    ``purge`` and ``rewind`` stay best effort on a closed buffer.

    Both run after a read has already produced its outcome - a parsed response for
    purge, an exception being unwound for rewind - so a connection closed underneath
    them has nothing left to do and nothing to report.
    """
    socket_buffer = SocketBuffer(Mock(), socket_read_size=65536, socket_timeout=None)
    socket_buffer.close()

    socket_buffer.purge()
    socket_buffer.rewind(0)


@pytest.mark.fixed_client
def test_socket_buffer_purge_survives_a_close_landing_mid_purge():
    """
    ``purge`` stays best effort when the close lands inside it.

    This is the window the test above cannot cover: the buffer is open when purge
    reads the unread length and gone by the time it truncates. ``close()`` drops the
    buffer to None, so an unguarded truncate raised AttributeError - which escapes
    purge's best effort wrapper and replaces an already parsed response.
    """
    socket_buffer = SocketBuffer(Mock(), socket_read_size=65536, socket_timeout=None)
    unread_bytes = socket_buffer.unread_bytes

    def close_then_report():
        unread = unread_bytes()
        socket_buffer.close()
        return unread

    socket_buffer.unread_bytes = close_then_report

    socket_buffer.purge()


@skip_if_server_version_lt("4.0.0")
@pytest.mark.redismod
def test_loading_external_modules(r):
    def inner():
        pass

    r.load_external_module("myfuncname", inner)
    assert getattr(r, "myfuncname") == inner
    assert isinstance(getattr(r, "myfuncname"), types.FunctionType)

    # and call it
    from redis.commands import RedisModuleCommands

    j = RedisModuleCommands.json
    r.load_external_module("sometestfuncname", j)

    # d = {'hello': 'world!'}
    # mod = j(r)
    # mod.set("fookey", ".", d)
    # assert mod.get('fookey') == d


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "client_kwargs",
    [
        {"driver_info": None},
        {"lib_name": None, "lib_version": None},
    ],
)
def test_redis_client_preserves_explicit_none_driver_info(client_kwargs):
    if "lib_name" in client_kwargs:
        with pytest.warns(DeprecationWarning):
            client = Redis(**client_kwargs)
    else:
        client = Redis(**client_kwargs)

    assert client.connection_pool.connection_kwargs["driver_info"] is None
    client.close()


@pytest.mark.fixed_client
def test_redis_client_default_driver_info():
    client = Redis()
    driver_info = client.connection_pool.connection_kwargs["driver_info"]

    assert driver_info.formatted_name == "redis-py"
    assert driver_info.lib_version is not None
    client.close()


@pytest.mark.fixed_client
class TestConnection:
    def test_disconnect(self):
        conn = Connection()
        mock_sock = mock.Mock()
        conn._sock = mock_sock
        conn.disconnect()
        mock_sock.shutdown.assert_called_once()
        mock_sock.close.assert_called_once()
        assert conn._sock is None

    def test_disconnect__shutdown_OSError(self):
        """An OSError on socket shutdown will still close the socket."""
        conn = Connection()
        mock_sock = mock.Mock()
        conn._sock = mock_sock
        conn._sock.shutdown.side_effect = OSError
        conn.disconnect()
        mock_sock.shutdown.assert_called_once()
        mock_sock.close.assert_called_once()
        assert conn._sock is None

    def test_disconnect__close_OSError(self):
        """An OSError on socket close will still clear out the socket."""
        conn = Connection()
        mock_sock = mock.Mock()
        conn._sock = mock_sock
        conn._sock.close.side_effect = OSError
        conn.disconnect()
        mock_sock.shutdown.assert_called_once()
        mock_sock.close.assert_called_once()
        assert conn._sock is None

    def test_connect_breaks_exception_reference_cycle(self):
        """
        On connection failure, _connect must not leave its frame retaining the
        raised exception.
        The exception's traceback references the frame, so a retained local
        would form a cycle only reclaimable by the GC.
        The finally clause in _connect breaks it by clearing the local.

        The exception escapes to the caller here, so it cannot be asserted
        dead; the frame is what has to be checked. See the sibling test for
        the reclamation property, which does not depend on the local's name.
        """
        conn = Connection(host="localhost", port=6379)
        addr_info = (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 6379))
        with (
            patch.object(socket, "getaddrinfo", return_value=[addr_info]),
            patch.object(socket, "socket") as socket_factory,
        ):
            socket_factory.return_value.connect.side_effect = OSError("refused")
            with pytest.raises(OSError) as exc_info:
                conn._connect()

        # Locate the _connect frame in the propagated traceback and confirm its
        # err local was cleared, proving no exception<->frame cycle survives.
        connect_frame = None
        tb = exc_info.value.__traceback__
        while tb is not None:
            if tb.tb_frame.f_code.co_name == "_connect":
                connect_frame = tb.tb_frame
            tb = tb.tb_next
        assert connect_frame is not None
        # `is None` alone also holds when no such local exists, so assert the
        # name is present before asserting its value.
        assert "err" in connect_frame.f_locals
        assert connect_frame.f_locals["err"] is None

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Immediate reclamation is a refcounting property",
    )
    def test_connect_breaks_reference_cycle_when_a_later_address_succeeds(self):
        """
        When an address fails but a later one connects, _connect must not
        return while still holding the caught exception.
        """

        class WeakReferenceableOSError(OSError):
            """OSError itself cannot be weak-referenced."""

        conn = Connection(host="localhost", port=6379)
        addr_infos = [
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 6379)),
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.2", 6379)),
        ]

        # Raise from a factory so the test itself never holds a strong
        # reference; the weakref is the only handle on the failure.
        raised_ref = []

        def fail_to_connect(*args, **kwargs):
            err = WeakReferenceableOSError("refused")
            raised_ref.append(weakref.ref(err))
            try:
                raise err
            finally:
                # Clear this frame's local too, or the traceback keeps the
                # exception alive here and the assertion measures the test
                # rather than _connect.
                err = None

        failing_sock, working_sock = MagicMock(), MagicMock()
        failing_sock.connect.side_effect = fail_to_connect

        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            with (
                patch.object(socket, "getaddrinfo", return_value=addr_infos),
                patch.object(
                    socket, "socket", side_effect=[failing_sock, working_sock]
                ),
            ):
                assert conn._connect() is working_sock

            (ref,) = raised_ref
            assert ref() is None
        finally:
            if gc_was_enabled:
                gc.enable()

    @pytest.mark.parametrize(
        "connection_kwargs",
        [
            {"driver_info": None},
            {"lib_name": None, "lib_version": None},
        ],
    )
    def test_client_setinfo_skipped_with_explicit_none(self, connection_kwargs):
        if "lib_name" in connection_kwargs:
            with pytest.warns(DeprecationWarning):
                conn = Connection(protocol=2, **connection_kwargs)
        else:
            conn = Connection(protocol=2, **connection_kwargs)
        conn._parser.on_connect = mock.Mock()
        conn.send_command = mock.Mock()
        conn.read_response = mock.Mock(return_value="OK")

        conn.on_connect_check_health()

        assert conn.driver_info is None
        conn.send_command.assert_not_called()
        conn.read_response.assert_not_called()

    def clear(self, conn):
        conn.retry_on_error.clear()

    # Client-internal test: builds a default localhost Connection, so it cannot
    # target a remote managed Redis Enterprise endpoint.
    @skip_if_redis_enterprise()
    def test_retry_connect_on_timeout_error(self):
        """Test that the _connect function is retried in case of a timeout"""
        conn = Connection(retry_on_timeout=True, retry=Retry(NoBackoff(), 3))
        origin_connect = conn._connect
        conn._connect = mock.Mock()

        def mock_connect():
            # connect only on the last retry
            if conn._connect.call_count <= 2:
                raise socket.timeout
            else:
                return origin_connect()

        conn._connect.side_effect = mock_connect
        conn.connect()
        assert conn._connect.call_count == 3
        self.clear(conn)

    def test_connect_without_retry_on_non_retryable_error(self):
        """Test that the _connect function is not being retried in case of a non-retryable error"""
        with patch.object(Connection, "_connect") as _connect:
            _connect.side_effect = RedisError("")
            conn = Connection(retry_on_timeout=True, retry=Retry(NoBackoff(), 2))
            with pytest.raises(RedisError):
                conn.connect()
            assert _connect.call_count == 1
            self.clear(conn)

    # Client-internal test: builds a default localhost Connection and mocks the
    # socket to count handshake retries, so it needs a co-located server rather
    # than a remote managed Redis Enterprise endpoint.
    @skip_if_redis_enterprise()
    def test_connect_with_retries(self):
        """
        Validate that retries occur for the entire connect+handshake flow when OSError
        happens during the handshake phase.
        """
        with patch.object(socket.socket, "sendall") as sendall:
            sendall.side_effect = OSError(ECONNREFUSED)
            conn = Connection(retry_on_timeout=True, retry=Retry(NoBackoff(), 2))
            with pytest.raises(ConnectionError):
                conn.connect()
            # the handshake commands are the failing ones
            # validate that we don't execute too many commands on each retry
            # 3 retries --> 3 commands
            assert sendall.call_count == 3

    def test_connect_timeout_error_without_retry(self):
        """Test that the _connect function is not being retried if retry_on_timeout is
        set to False"""
        conn = Connection(retry_on_timeout=False)
        conn._connect = mock.Mock()
        conn._connect.side_effect = socket.timeout

        with pytest.raises(TimeoutError, match="Timeout connecting to server"):
            conn.connect()
        assert conn._connect.call_count == 1
        self.clear(conn)


@pytest.mark.onlynoncluster
@pytest.mark.parametrize(
    "parser_class",
    [_RESP2Parser, _RESP3Parser, _HiredisParser],
    ids=["RESP2Parser", "RESP3Parser", "HiredisParser"],
)
def test_connection_parse_response_resume(r: redis.Redis, parser_class):
    """
    This test verifies that the Connection parser,
    be that PythonParser or HiredisParser,
    can be interrupted at IO time and then resume parsing.
    """
    if parser_class is _HiredisParser and not HIREDIS_AVAILABLE:
        pytest.skip("Hiredis not available)")
    args = dict(r.connection_pool.connection_kwargs)
    args["parser_class"] = parser_class
    conn = Connection(**args)
    conn.connect()
    message = (
        b"*3\r\n$7\r\nmessage\r\n$8\r\nchannel1\r\n"
        b"$25\r\nhi\r\nthere\r\n+how\r\nare\r\nyou\r\n"
    )
    mock_socket = MockSocket(message, interrupt_every=2)

    if isinstance(conn._parser, _RESP2Parser) or isinstance(conn._parser, _RESP3Parser):
        conn._parser._buffer._sock = mock_socket
    else:
        conn._parser._sock = mock_socket
    for i in range(100):
        try:
            response = conn.read_response(disconnect_on_error=False)
            break
        except MockSocket.TestError:
            pass

    else:
        pytest.fail("didn't receive a response")
    assert response
    assert i > 0


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "Class",
    [
        Connection,
        SSLConnection,
        UnixDomainSocketConnection,
    ],
)
def test_pack_command(Class):
    """
    This test verifies that the pack_command works
    on all supported connections. #2581
    """
    cmd = (
        "HSET",
        "foo",
        "key",
        "value1",
        b"key_b",
        b"bytes str",
        b"key_i",
        67,
        "key_f",
        3.14159265359,
    )
    expected = (
        b"*10\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n"
        b"$5\r\nkey_b\r\n$9\r\nbytes str\r\n$5\r\nkey_i\r\n$2\r\n67\r\n$5"
        b"\r\nkey_f\r\n$13\r\n3.14159265359\r\n"
    )

    actual = Class().pack_command(*cmd)[0]
    assert actual == expected, f"actual = {actual}, expected = {expected}"


@pytest.mark.fixed_client
@pytest.mark.parametrize("protocol", [2, 3])
def test_pack_command_respects_connection_encoding(protocol):
    connection = Connection(encoding="latin-1", protocol=protocol)

    assert connection.pack_command("SET", "key", "café") == [
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$4\r\ncaf\xe9\r\n"
    ]


@pytest.mark.fixed_client
@pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="hiredis is not installed")
def test_hiredis_command_packer_no_arg_constructor():
    connection = Connection(command_packer=HiredisRespSerializer())
    expected = [b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"]

    assert connection.pack_command("SET", "key", "value") == expected
    assert connection.pack_command("SET", "key", memoryview(b"value")) == expected


@pytest.mark.fixed_client
# Hardcodes a localhost URL, so it cannot target a remote managed Redis Enterprise endpoint.
@skip_if_redis_enterprise()
def test_create_single_connection_client_from_url():
    client = redis.Redis.from_url(
        "redis://localhost:6379/0?", single_connection_client=True
    )
    assert client.connection is not None


@pytest.mark.fixed_client
@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
def test_pool_auto_close(request, from_url):
    """Verify that basic Redis instances have auto_close_connection_pool set to True"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    def get_redis_connection():
        if from_url:
            return Redis.from_url(url)
        return Redis(**url_args)

    r1 = get_redis_connection()
    assert r1.auto_close_connection_pool is True
    r1.close()


@pytest.mark.fixed_client
@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
def test_redis_connection_pool(request, from_url):
    """Verify that basic Redis instances using `connection_pool`
    have auto_close_connection_pool set to False"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    pool = None

    def get_redis_connection():
        nonlocal pool
        if from_url:
            pool = ConnectionPool.from_url(url)
        else:
            pool = ConnectionPool(**url_args)
        return Redis(connection_pool=pool)

    called = 0

    def mock_disconnect(target_pool):
        nonlocal called
        if pool is not None and target_pool is pool:
            called += 1

    with patch.object(ConnectionPool, "disconnect", mock_disconnect):
        with get_redis_connection() as r1:
            assert r1.auto_close_connection_pool is False

    assert called == 0
    pool.disconnect()


@pytest.mark.fixed_client
@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
def test_redis_from_pool(request, from_url):
    """Verify that basic Redis instances created using `from_pool()`
    have auto_close_connection_pool set to True"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    pool = None

    def get_redis_connection():
        nonlocal pool
        if from_url:
            pool = ConnectionPool.from_url(url)
        else:
            pool = ConnectionPool(**url_args)
        return Redis.from_pool(pool)

    called = 0

    def mock_disconnect(target_pool):
        nonlocal called
        if pool is not None and target_pool is pool:
            called += 1

    with patch.object(ConnectionPool, "disconnect", mock_disconnect):
        with get_redis_connection() as r1:
            assert r1.auto_close_connection_pool is True

    assert called == 1
    pool.disconnect()


@pytest.mark.fixed_client
def test_create_secure_client_from_url_with_minimum_ssl_version():
    client = redis.Redis.from_url(
        "rediss://localhost:6379/0?ssl_cert_reqs=none&ssl_min_version={}".format(
            ssl.TLSVersion.TLSv1_3
        )
    )
    assert (
        client.connection_pool.connection_kwargs["ssl_min_version"]
        == ssl.TLSVersion.TLSv1_3
    )


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "conn, error, expected_message",
    [
        (SSLConnection(), OSError(), "Error connecting to localhost:6379."),
        (SSLConnection(), OSError(12), "Error 12 connecting to localhost:6379."),
        (
            SSLConnection(),
            OSError(12, "Some Error"),
            "Error 12 connecting to localhost:6379. Some Error.",
        ),
        (
            UnixDomainSocketConnection(path="unix:///tmp/redis.sock"),
            OSError(),
            "Error connecting to unix:///tmp/redis.sock.",
        ),
        (
            UnixDomainSocketConnection(path="unix:///tmp/redis.sock"),
            OSError(12),
            "Error 12 connecting to unix:///tmp/redis.sock.",
        ),
        (
            UnixDomainSocketConnection(path="unix:///tmp/redis.sock"),
            OSError(12, "Some Error"),
            "Error 12 connecting to unix:///tmp/redis.sock. Some Error.",
        ),
    ],
)
def test_format_error_message(conn, error, expected_message):
    """Test that the _error_message function formats errors correctly"""
    error_message = conn._error_message(error)
    assert error_message == expected_message


@pytest.mark.fixed_client
def test_ssl_handshake_uses_socket_connect_timeout():
    conn = SSLConnection(
        socket_timeout=0.05, socket_connect_timeout=4, ssl_cert_reqs="none"
    )
    handshake_timeouts = []
    ssl_sock = MagicMock()

    def wrap_socket(context, sock, **kwargs):
        handshake_timeouts.append(sock.gettimeout())
        return ssl_sock

    with socket.socket() as sock:
        sock.settimeout(conn.socket_timeout)
        with patch.object(ssl.SSLContext, "wrap_socket", wrap_socket):
            assert conn._wrap_socket_with_ssl(sock) is ssl_sock

    assert handshake_timeouts == [4]
    ssl_sock.settimeout.assert_called_once_with(0.05)


@pytest.mark.fixed_client
def test_network_connection_failure():
    # Match only the stable part of the error message across OS
    exp_err = rf"Error {ECONNREFUSED} connecting to localhost:9999\."
    with pytest.raises(ConnectionError, match=exp_err):
        # nothing listens on this port, so skip the default backoff retries
        redis = Redis(port=9999, retry=Retry(NoBackoff(), 0))
        redis.set("a", "b")


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(socket, "AF_UNIX"),
    reason="Unix domain sockets not supported on this platform",
)
def test_unix_socket_connection_failure():
    exp_err = "Error 2 connecting to unix:///tmp/a.sock. No such file or directory."
    with pytest.raises(ConnectionError, match=exp_err):
        # the socket file does not exist, so skip the default backoff retries
        redis = Redis(
            unix_socket_path="unix:///tmp/a.sock", retry=Retry(NoBackoff(), 0)
        )
        redis.set("a", "b")


@pytest.mark.fixed_client
class TestUnitConnectionPool:
    @pytest.mark.parametrize(
        "max_conn", (-1, "str"), ids=("non-positive", "wrong type")
    )
    def test_throws_error_on_incorrect_max_connections(self, max_conn):
        with pytest.raises(
            ValueError, match='"max_connections" must be a positive integer'
        ):
            ConnectionPool(
                max_connections=max_conn,
            )

    def test_throws_error_on_cache_enable_in_resp2(self):
        with pytest.raises(
            RedisError, match="Client caching is only supported with RESP version 3"
        ):
            ConnectionPool(protocol=2, cache_config=CacheConfig())

    def test_throws_error_on_incorrect_cache_implementation(self):
        with pytest.raises(ValueError, match="Cache must implement CacheInterface"):
            ConnectionPool(protocol=3, cache="wrong")

    def test_returns_custom_cache_implementation(self, mock_cache):
        connection_pool = ConnectionPool(protocol=3, cache=mock_cache)

        assert mock_cache == connection_pool.cache
        connection_pool.disconnect()

    def test_creates_cache_with_custom_cache_factory(
        self, mock_cache_factory, mock_cache
    ):
        mock_cache_factory.get_cache.return_value = mock_cache

        connection_pool = ConnectionPool(
            protocol=3,
            cache_config=CacheConfig(max_size=5),
            cache_factory=mock_cache_factory,
        )

        # Cache is wrapped in CacheProxy for observability
        assert isinstance(connection_pool.cache, CacheProxy)
        assert connection_pool.cache._cache == mock_cache
        connection_pool.disconnect()

    def test_creates_cache_with_given_configuration(self, mock_cache):
        connection_pool = ConnectionPool(
            protocol=3, cache_config=CacheConfig(max_size=100)
        )

        assert isinstance(connection_pool.cache, CacheInterface)
        assert connection_pool.cache.config.get_max_size() == 100
        assert isinstance(connection_pool.cache.eviction_policy, LRUPolicy)
        connection_pool.disconnect()

    def test_make_connection_proxy_connection_on_given_cache(self):
        connection_pool = ConnectionPool(protocol=3, cache_config=CacheConfig())

        assert isinstance(connection_pool.make_connection(), CacheProxyConnection)
        connection_pool.disconnect()

    def test_injects_the_metadata_resolver_into_the_cache_config(self):
        # The eligible set is the resolver's, not the config's: a resolver that carries
        # nothing reports every command ineligible.
        empty_resolver = DynamicMetadataResolver({})
        cache_config = CacheConfig(max_size=17)

        connection_pool = ConnectionPool(
            protocol=3,
            cache_config=cache_config,
            metadata_resolver=empty_resolver,
        )

        assert connection_pool.metadata_resolver is empty_resolver
        assert connection_pool.cache.config.is_allowed_to_cache("GET") is False
        # Every other setting of the caller's config still applies.
        assert connection_pool.cache.config.get_max_size() == 17
        connection_pool.disconnect()

    def test_does_not_write_the_resolver_into_the_callers_cache_config(self):
        """
        A ``CacheConfig`` carries only sizing and eviction settings, so reusing one across
        clients is reasonable - and writing the resolver into it would give every one of them
        whichever resolver was injected last.
        """
        empty_resolver = DynamicMetadataResolver({})
        cache_config = CacheConfig()

        first = ConnectionPool(protocol=3, cache_config=cache_config)
        second = ConnectionPool(
            protocol=3, cache_config=cache_config, metadata_resolver=empty_resolver
        )

        # The caller's object is untouched, so the pool that was given no resolver keeps
        # deciding through the static default.
        assert cache_config.is_allowed_to_cache("GET") is True
        assert first.cache.config.is_allowed_to_cache("GET") is True
        assert second.cache.config.is_allowed_to_cache("GET") is False
        first.disconnect()
        second.disconnect()

    def test_injects_the_metadata_resolver_into_a_given_caches_config(self):
        # ``cache=`` rather than ``cache_config=``: the configuration lives inside the cache
        # the caller handed over and cannot be swapped without rebuilding it, so it is set in
        # place. Both styles end up on the same decision point.
        empty_resolver = DynamicMetadataResolver({})
        cache = DefaultCache(CacheConfig())

        connection_pool = ConnectionPool(
            protocol=3, cache=cache, metadata_resolver=empty_resolver
        )

        assert cache.config.is_allowed_to_cache("GET") is False
        connection_pool.disconnect()

    def test_injects_the_metadata_resolver_into_a_cache_factorys_config(
        self, mock_cache_factory
    ):
        empty_resolver = DynamicMetadataResolver({})
        mock_cache_factory.get_cache.return_value = DefaultCache(CacheConfig())

        connection_pool = ConnectionPool(
            protocol=3,
            cache_config=CacheConfig(),
            cache_factory=mock_cache_factory,
            metadata_resolver=empty_resolver,
        )

        assert connection_pool.cache.config.is_allowed_to_cache("GET") is False
        connection_pool.disconnect()

    def test_leaves_a_custom_cache_configuration_alone(self):
        """
        ``CacheConfigurationInterface`` is public and implemented by third parties, so the
        injection is isinstance-guarded and a custom configuration keeps its own eligibility
        logic.

        The double implements the ABC without subclassing ``CacheConfig``, which is what makes
        the guard observable: a ``CacheConfig`` subclass satisfies the isinstance check, so it
        would be copied and injected into and prove nothing.
        """

        class _AllowEverything(CacheConfigurationInterface):
            def get_cache_class(self):
                return DefaultCache

            def get_max_size(self) -> int:
                return 10

            def get_eviction_policy(self):
                return EvictionPolicy.LRU

            def is_exceeds_max_size(self, count: int) -> bool:
                return count > self.get_max_size()

            def is_allowed_to_cache(self, command: str) -> bool:
                return True

        cache_config = _AllowEverything()
        connection_pool = ConnectionPool(
            protocol=3,
            cache_config=cache_config,
            metadata_resolver=DynamicMetadataResolver({}),
        )

        # Not copied, so the object the cache decides through is the caller's own...
        assert connection_pool.cache.config is cache_config
        # ...and not injected into, so it still answers from its own logic rather than from
        # the resolver, which carries no records and would report everything ineligible.
        assert connection_pool.cache.config.is_allowed_to_cache("SET") is True
        assert cache_config.is_allowed_to_cache("SET") is True
        connection_pool.disconnect()

    def test_cache_config_is_functional_without_an_injected_resolver(self):
        # No client configured one, so the config decides through the static metadata this
        # library ships - which is what keeps a standalone ``CacheConfig()`` usable.
        connection_pool = ConnectionPool(protocol=3, cache_config=CacheConfig())

        assert connection_pool.metadata_resolver is None
        assert connection_pool.cache.config.is_allowed_to_cache("GET") is True
        assert connection_pool.cache.config.is_allowed_to_cache("SET") is False
        connection_pool.disconnect()

    def test_redis_forwards_the_metadata_resolver_to_the_pool(self):
        empty_resolver = DynamicMetadataResolver({})

        client = Redis(
            protocol=3, cache_config=CacheConfig(), metadata_resolver=empty_resolver
        )

        assert client.connection_pool.metadata_resolver is empty_resolver
        assert client.connection_pool.cache.config.is_allowed_to_cache("GET") is False
        client.close()


@pytest.mark.fixed_client
class TestUnitCacheProxyConnection:
    def test_clears_cache_on_disconnect(self, mock_connection, cache_conf):
        cache = DefaultCache(CacheConfig(max_size=10))
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )

        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.get(cache_key).cache_value == b"bar"

        mock_connection.disconnect.return_value = None
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = EventDispatcher()

        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )
        proxy_connection.disconnect()

        assert len(cache.collection) == 0

    def test_cacheable_command_without_keys_bypasses_the_cache(
        self, mock_cache, mock_connection
    ):
        """
        Eligibility and keyability are separate: a cacheable command whose invocation carries
        no key list must be sent normally, with caching skipped.

        Regression for ``ValueError: Cannot create cache key.``, which every eligible command
        method that does not pass ``keys=`` used to raise - ZRANK among them. The reply must
        be the server's, and nothing may be stored under a key the client cannot build.
        """
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = EventDispatcher()

        mock_connection.read_response.return_value = 0
        mock_connection.can_read.return_value = False

        # A real cache holding a VALID entry for a previous command, so that failing to clear
        # the current cache key would serve that entry as this command's reply.
        cache = DefaultCache(CacheConfig(max_size=10))
        stale_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache.set(
            CacheEntry(
                cache_key=stale_key,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )
        proxy_connection._current_command_cache_key = stale_key

        proxy_connection.send_command("ZRANK", "foo", "bar")

        # Sent, once, exactly as given.
        mock_connection.send_command.assert_called_once_with("ZRANK", "foo", "bar")
        # The previous command's key is cleared, so nothing can be served under it...
        assert proxy_connection._current_command_cache_key is None
        # ...the reply the caller receives is the server's, not the cached b"bar"...
        assert proxy_connection.read_response() == 0
        # ...and nothing new was stored, because there is no key to store it under.
        assert cache.size == 1
        assert cache.get(stale_key).cache_value == b"bar"

    def test_failed_read_clears_the_in_progress_cache_entry(self, mock_connection):
        """
        A cacheable command whose read fails must not leave its placeholder behind.

        ``send_command`` stakes out an IN_PROGRESS entry that only a successful
        ``read_response`` resolves. When the read raised - a WRONGTYPE reply for a key of
        another type, a NOPERM under a restricted ACL - that entry stayed in the pool-wide
        cache, and every later call of the same command and key found an entry, skipped the
        network, then read a reply that was never requested.
        """
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.can_read.return_value = False

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        mock_connection.read_response.side_effect = ResponseError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )
        proxy_connection.send_command("GET", "foo", keys=["foo"])
        with pytest.raises(ResponseError):
            proxy_connection.read_response()

        # The error reaches the caller unchanged, and nothing is left behind to serve a
        # later command or to store a reply under.
        assert cache.size == 0
        assert proxy_connection._current_command_cache_key is None

        # So the same command still reaches the server, and gets the server's reply.
        mock_connection.read_response.side_effect = None
        mock_connection.read_response.return_value = b"bar"
        mock_connection.send_command.reset_mock()

        proxy_connection.send_command("GET", "foo", keys=["foo"])
        mock_connection.send_command.assert_called_once_with("GET", "foo", keys=["foo"])
        assert proxy_connection.read_response() == b"bar"

    def test_himport_prepared_is_reassignable_through_proxy(self):
        # Regression: `_himport_prepared` must have a setter that delegates to the
        # wrapped connection, mirroring `_himport_reconciled_revision`. A getter-only
        # property would make `conn._himport_prepared = {}` (as `_reset_himport_state`
        # does) raise AttributeError -- but only when client-side caching is enabled.
        # Exercise the property descriptors directly, no full construction needed.
        proxy = object.__new__(CacheProxyConnection)

        class _Wrapped:
            _himport_prepared = {"fs": 1}
            _himport_reconciled_revision = 3

        proxy._conn = _Wrapped()
        assert proxy._himport_prepared == {"fs": 1}  # getter delegates
        proxy._himport_prepared = {}  # setter delegates (was AttributeError)
        assert proxy._conn._himport_prepared == {}
        # symmetry with the existing reconciled-revision setter
        proxy._himport_reconciled_revision = 7
        assert proxy._conn._himport_reconciled_revision == 7

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Pypy doesn't support side_effect",
    )
    def test_read_response_returns_cached_reply(self, mock_cache, mock_connection):
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = EventDispatcher()

        mock_cache.is_cachable.return_value = True
        # Scripted positionally, so it tracks how many lookups the proxy makes:
        #   1st - ``send_command`` checking for an existing entry (None, so it sends)
        #   2nd - ``read_response`` fetching the placeholder to promote to VALID
        # The rest is slack. ``read_response`` no longer re-derives the hit decision from
        # cache state, which is what removed the lookups this list used to carry.
        mock_cache.get.side_effect = [
            None,
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                ),
                cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                status=CacheEntryStatus.IN_PROGRESS,
                connection_ref=mock_connection,
            ),
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                ),
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            ),
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                ),
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            ),
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                ),
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            ),
        ]
        mock_connection.send_command.return_value = Any
        mock_connection.read_response.return_value = b"bar"
        mock_connection.can_read.return_value = False

        proxy_connection = CacheProxyConnection(
            mock_connection, mock_cache, threading.RLock()
        )
        proxy_connection.send_command(*["GET", "foo"], **{"keys": ["foo"]})
        assert proxy_connection.read_response() == b"bar"
        assert proxy_connection._current_command_cache_key is None
        assert proxy_connection.read_response() == b"bar"

        mock_cache.set.assert_has_calls(
            [
                call(
                    CacheEntry(
                        cache_key=CacheKey(
                            command="GET",
                            redis_keys=("foo",),
                            redis_args=("GET", "foo"),
                        ),
                        cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                        status=CacheEntryStatus.IN_PROGRESS,
                        connection_ref=mock_connection,
                    )
                ),
                call(
                    CacheEntry(
                        cache_key=CacheKey(
                            command="GET",
                            redis_keys=("foo",),
                            redis_args=("GET", "foo"),
                        ),
                        cache_value=b"bar",
                        status=CacheEntryStatus.VALID,
                        connection_ref=mock_connection,
                    )
                ),
            ]
        )

        # Two lookups, both under the same cache key: one in ``send_command`` and one in
        # ``read_response`` to promote the placeholder. There used to be a third, from
        # ``read_response`` re-deriving the hit decision from cache state - which is exactly
        # the lookup that could disagree with what ``send_command`` had already decided.
        mock_cache.get.assert_has_calls(
            [
                call(
                    CacheKey(
                        command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                    )
                ),
                call(
                    CacheKey(
                        command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                    )
                ),
            ]
        )

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Pypy doesn't support side_effect",
    )
    @pytest.mark.parametrize(
        "command,redis_keys,redis_args,cached_value",
        [
            ("ZCARD", ("myset",), ("ZCARD", "myset"), 2),
            ("SCARD", ("myset",), ("SCARD", "myset"), 5),
            ("LLEN", ("mylist",), ("LLEN", "mylist"), 0),
            (
                "LRANGE",
                ("mylist",),
                ("LRANGE", "mylist", "0", "-1"),
                [b"a", b"b"],
            ),
            ("EXISTS", ("foo",), ("EXISTS", "foo"), True),
        ],
        ids=["int-zcard", "int-scard", "int-llen", "list-lrange", "bool-exists"],
    )
    def test_read_response_returns_cached_non_bytes_reply(
        self, mock_cache, mock_connection, command, redis_keys, redis_args, cached_value
    ):
        """Test that cached non-bytes responses (int, list, bool) don't crash.

        Regression test for https://github.com/redis/redis-py/issues/4009
        """
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = EventDispatcher()

        cache_key = CacheKey(
            command=command, redis_keys=redis_keys, redis_args=redis_args
        )
        valid_entry = CacheEntry(
            cache_key=cache_key,
            cache_value=cached_value,
            status=CacheEntryStatus.VALID,
            connection_ref=mock_connection,
        )
        in_progress_entry = CacheEntry(
            cache_key=cache_key,
            cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
            status=CacheEntryStatus.IN_PROGRESS,
            connection_ref=mock_connection,
        )
        mock_cache.is_cachable.return_value = True
        mock_cache.get.side_effect = [
            # 1st send_command: cache.get(key) → None (cache miss)
            None,
            # 1st read_response: cache.get(key) is not None check
            in_progress_entry,
            # 1st read_response: cache.get(key).status check
            in_progress_entry,
            # 1st read_response: cache.get(key) after wire read (to update entry)
            in_progress_entry,
            # 2nd send_command: cache.get(key) → truthy (cache hit, enter branch)
            valid_entry,
            # 2nd send_command: entry = cache.get(key)
            valid_entry,
            # 2nd send_command: re-check cache.get(key) → truthy (return early)
            valid_entry,
            # 2nd read_response: cache.get(key) is not None check
            valid_entry,
            # 2nd read_response: cache.get(key).status check (VALID != IN_PROGRESS)
            valid_entry,
            # 2nd read_response: cache.get(key).cache_value (deep copy)
            valid_entry,
        ]
        mock_connection.send_command.return_value = Any
        mock_connection.read_response.return_value = cached_value
        mock_connection.can_read.return_value = False

        proxy_connection = CacheProxyConnection(
            mock_connection, mock_cache, threading.RLock()
        )
        proxy_connection.send_command(*list(redis_args), **{"keys": list(redis_keys)})
        # First call: cache miss, reads from connection
        assert proxy_connection.read_response() == cached_value
        assert proxy_connection._current_command_cache_key is None

        # Re-issue send_command so _current_command_cache_key is set again;
        # this time send_command sees a VALID entry and returns early.
        proxy_connection.send_command(*list(redis_args), **{"keys": list(redis_keys)})
        # Second call: cache hit — this must not raise TypeError
        assert proxy_connection.read_response() == cached_value
        # Verify the second read_response used the cache, not the wire:
        # mock_connection.read_response should have been called only once
        # (during the first read_response).
        mock_connection.read_response.assert_called_once()

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Pypy doesn't support side_effect",
    )
    def test_triggers_invalidation_processing_on_another_connection(
        self, mock_cache, mock_connection
    ):
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = Mock(spec=EventDispatcher)

        another_conn = copy.deepcopy(mock_connection)
        another_conn.can_read.side_effect = [True, False]
        another_conn.read_response.return_value = None
        cache_entry = CacheEntry(
            cache_key=CacheKey(
                command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
            ),
            cache_value=b"bar",
            status=CacheEntryStatus.VALID,
            connection_ref=another_conn,
        )
        mock_cache.is_cachable.return_value = True
        mock_cache.get.return_value = cache_entry
        mock_connection.can_read.return_value = False

        proxy_connection = CacheProxyConnection(
            mock_connection, mock_cache, threading.RLock()
        )
        proxy_connection.send_command(*["GET", "foo"], **{"keys": ["foo"]})

        assert proxy_connection.read_response() == b"bar"
        assert another_conn.can_read.call_count == 2
        another_conn.read_response.assert_called_once_with(
            push_request=True, timeout=0, disconnect_on_error=False
        )

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Pypy doesn't support side_effect",
    )
    def test_drain_of_another_connection_holds_no_cache_lock(
        self, mock_cache, mock_connection
    ):
        """
        The drain takes the pool lock, and an invalidation it reads runs the owner's
        callback under the owner's cache lock. Holding this connection's cache lock across
        it orders a cache lock before the pool lock, so two connections draining each other,
        or a ``pool.disconnect()`` reaching this one, deadlock.
        """
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = Mock(spec=EventDispatcher)

        another_conn = copy.deepcopy(mock_connection)
        another_conn.can_read.side_effect = [True, False]
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache_entry = CacheEntry(
            cache_key=cache_key,
            cache_value=b"bar",
            status=CacheEntryStatus.VALID,
            connection_ref=another_conn,
        )
        mock_cache.is_cachable.return_value = True
        mock_cache.get.return_value = cache_entry
        mock_connection.can_read.return_value = False

        pool_lock = threading.RLock()
        proxy_connection = CacheProxyConnection(mock_connection, mock_cache, pool_lock)
        acquired = []

        def read_invalidation(**kwargs):
            cache_lock_available = False
            pool_lock_available = False

            def probe_locks():
                nonlocal cache_lock_available, pool_lock_available
                # The sending thread must still hold the pool lock during this probe.
                pool_lock_available = pool_lock.acquire(blocking=False)
                if pool_lock_available:
                    pool_lock.release()
                cache_lock_available = proxy_connection._cache_lock.acquire(
                    blocking=False
                )
                if cache_lock_available:
                    proxy_connection._cache_lock.release()

            other = threading.Thread(target=probe_locks)
            other.start()
            other.join(timeout=5)
            assert not other.is_alive()
            assert not pool_lock_available
            acquired.append(cache_lock_available)
            return None

        another_conn.read_response.side_effect = read_invalidation

        proxy_connection.send_command(*["GET", "foo"], **{"keys": ["foo"]})

        another_conn.read_response.assert_called_once_with(
            push_request=True, timeout=0, disconnect_on_error=False
        )
        assert acquired == [True]
        mock_connection.send_command.assert_not_called()
        assert proxy_connection.read_response() == b"bar"

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Pypy doesn't support side_effect",
    )
    def test_sends_command_when_cache_entry_invalidated_during_drain(
        self, mock_cache, mock_connection
    ):
        """Regression test for issue #3600.

        When another connection's invalidation drain removes the cache entry,
        send_command must fall through and send the command over the wire
        instead of returning early (which would cause read_response to hang).
        """
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = Mock(spec=EventDispatcher)

        another_conn = copy.deepcopy(mock_connection)
        another_conn.can_read.side_effect = [True, False]
        another_conn.read_response.return_value = None

        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache_entry = CacheEntry(
            cache_key=cache_key,
            cache_value=b"bar",
            status=CacheEntryStatus.VALID,
            connection_ref=another_conn,
        )

        mock_cache.is_cachable.return_value = True
        # get() call sequence in send_command:
        #   1st: fetch the entry (truthy → drain the connection that holds it)
        #   2nd: re-check after the drain (None → the entry was invalidated)
        mock_cache.get.side_effect = [cache_entry, None]
        mock_connection.can_read.return_value = False
        mock_connection.send_command.return_value = None

        proxy_connection = CacheProxyConnection(
            mock_connection, mock_cache, threading.RLock()
        )
        proxy_connection.send_command(*["GET", "foo"], **{"keys": ["foo"]})

        # The drain should have happened on the other connection
        assert another_conn.can_read.call_count == 2
        another_conn.read_response.assert_called_once_with(
            push_request=True, timeout=0, disconnect_on_error=False
        )

        # The command must have been sent over the wire (not returned early)
        mock_connection.send_command.assert_called_once_with("GET", "foo", keys=["foo"])

        # An IN_PROGRESS entry must have been set for this connection
        mock_cache.set.assert_called_once_with(
            CacheEntry(
                cache_key=cache_key,
                cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                status=CacheEntryStatus.IN_PROGRESS,
                connection_ref=mock_connection,
            )
        )

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Pypy doesn't support side_effect",
    )
    def test_invalidation_processing_on_another_connection_breaks_on_timeout(
        self, mock_cache, mock_connection
    ):
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = Mock(spec=EventDispatcher)

        another_conn = copy.deepcopy(mock_connection)
        another_conn.can_read.return_value = True
        another_conn.read_response.side_effect = TimeoutError("timeout")

        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache_entry = CacheEntry(
            cache_key=cache_key,
            cache_value=b"bar",
            status=CacheEntryStatus.VALID,
            connection_ref=another_conn,
        )

        mock_cache.is_cachable.return_value = True
        mock_cache.get.side_effect = [cache_entry, cache_entry, cache_entry]
        mock_connection.can_read.return_value = False

        proxy_connection = CacheProxyConnection(
            mock_connection, mock_cache, threading.RLock()
        )
        proxy_connection.send_command(*["GET", "foo"], **{"keys": ["foo"]})

        another_conn.read_response.assert_called_once_with(
            push_request=True, timeout=0, disconnect_on_error=False
        )
        mock_connection.send_command.assert_not_called()

    def test_process_pending_invalidations_breaks_on_timeout(self, mock_connection):
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.can_read.return_value = True
        mock_connection.read_response.side_effect = TimeoutError("timeout")

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        proxy_connection._process_pending_invalidations()

        mock_connection.read_response.assert_called_once_with(
            push_request=True, timeout=0, disconnect_on_error=False
        )

    def test_read_response_propagates_timeout_parameter(self, mock_connection):
        """Test that timeout parameter is propagated to underlying connection."""
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.read_response.return_value = b"OK"

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        # Test with specific timeout value
        proxy_connection.read_response(timeout=0.5)
        mock_connection.read_response.assert_called_with(
            disable_decoding=False,
            timeout=0.5,
            disconnect_on_error=True,
            push_request=False,
        )

    def test_read_response_timeout_default_is_sentinel(self, mock_connection):
        """Test that default timeout value is SENTINEL."""
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.read_response.return_value = b"OK"

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        # Test default timeout is SENTINEL
        proxy_connection.read_response()
        mock_connection.read_response.assert_called_with(
            disable_decoding=False,
            timeout=SENTINEL,
            disconnect_on_error=True,
            push_request=False,
        )

    def test_read_response_timeout_none_passed_through(self, mock_connection):
        """Test that timeout=None is passed through for blocking behavior."""
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.read_response.return_value = b"OK"

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        # Test timeout=None is passed through
        proxy_connection.read_response(timeout=None)
        mock_connection.read_response.assert_called_with(
            disable_decoding=False,
            timeout=None,
            disconnect_on_error=True,
            push_request=False,
        )

    def test_read_response_timeout_zero_passed_through(self, mock_connection):
        """Test that timeout=0 is passed through for non-blocking behavior."""
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.read_response.return_value = b"OK"

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        # Test timeout=0 is passed through
        proxy_connection.read_response(timeout=0)
        mock_connection.read_response.assert_called_with(
            disable_decoding=False,
            timeout=0,
            disconnect_on_error=True,
            push_request=False,
        )

    def test_read_response_all_params_with_timeout(self, mock_connection):
        """Test that all parameters including timeout are correctly passed."""
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.read_response.return_value = b"OK"

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        # Test all parameters together
        proxy_connection.read_response(
            disable_decoding=True,
            timeout=1.5,
            disconnect_on_error=False,
            push_request=True,
        )
        mock_connection.read_response.assert_called_with(
            disable_decoding=True,
            timeout=1.5,
            disconnect_on_error=False,
            push_request=True,
        )

    def test_mutable_list_response_on_cache_miss_does_not_corrupt_cache(
        self, proxy_factory, mock_connection
    ):
        """
        Mutating a mutable response (e.g. from MGET or LRANGE) returned on a cache miss
        must not mutate the cached entry, and subsequent hits must return original data.
        """
        proxy, _ = proxy_factory(TrackingMode.PLAIN)
        mock_connection.read_response.return_value = [b"val1", b"val2"]

        # 1. Cache miss
        proxy.send_command("MGET", "k1", "k2", keys=("k1", "k2"))
        miss_res = proxy.read_response()
        assert miss_res == [b"val1", b"val2"]

        # Mutate the miss response in-place
        miss_res.append(b"corrupted")
        miss_res[0] = b"mutated"

        # 2. Cache hit
        proxy.send_command("MGET", "k1", "k2", keys=("k1", "k2"))
        hit_res = proxy.read_response()
        assert hit_res == [b"val1", b"val2"]
        assert hit_res is not miss_res

        # Mutating the hit response must also not affect subsequent hits
        hit_res.append(b"hit_mutation")
        proxy.send_command("MGET", "k1", "k2", keys=("k1", "k2"))
        hit_res_2 = proxy.read_response()
        assert hit_res_2 == [b"val1", b"val2"]

    def test_mutable_dict_response_on_cache_miss_does_not_corrupt_cache(
        self, proxy_factory, mock_connection
    ):
        """
        Mutating a dict response (e.g. from JSON.GET) on a miss must not corrupt the cache.
        """
        proxy, _ = proxy_factory(TrackingMode.PLAIN)
        mock_connection.read_response.return_value = {"a": 1, "nested": [1, 2]}

        # 1. Miss
        proxy.send_command("JSON.GET", "doc", keys=("doc",))
        miss_res = proxy.read_response()
        assert miss_res == {"a": 1, "nested": [1, 2]}

        # In-place mutations
        miss_res["a"] = 999
        miss_res["nested"].append(3)

        # 2. Hit
        proxy.send_command("JSON.GET", "doc", keys=("doc",))
        hit_res = proxy.read_response()
        assert hit_res == {"a": 1, "nested": [1, 2]}

    def test_immutable_response_identity_preserved(
        self, proxy_factory, mock_connection
    ):
        """
        Immutable responses (e.g. bytes, str, int, None) avoid unnecessary deepcopying.
        """
        proxy, _ = proxy_factory(TrackingMode.PLAIN)
        raw_val = b"hello_world"
        mock_connection.read_response.return_value = raw_val

        proxy.send_command("GET", "k1", keys=("k1",))
        miss_res = proxy.read_response()
        assert miss_res is raw_val

        proxy.send_command("GET", "k1", keys=("k1",))
        hit_res = proxy.read_response()
        assert hit_res is raw_val

    def test_interrupted_clone_drops_placeholder_and_reraises(
        self, proxy_factory, mock_connection, monkeypatch
    ):
        """
        When cloning is interrupted by BaseException (e.g. KeyboardInterrupt),
        read_response must drop the placeholder and re-raise the exception.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        mock_connection.read_response.return_value = [b"large", b"list"]

        def mock_clone(_val):
            raise KeyboardInterrupt()

        monkeypatch.setattr("redis.connection._clone_cache_value", mock_clone)

        proxy.send_command("GET", "k1", keys=("k1",))
        with pytest.raises(KeyboardInterrupt):
            proxy.read_response()

        assert proxy._current_command_cache_key is None
        assert len(cache.collection) == 0


@pytest.fixture()
def proxy_factory(mock_connection):
    """
    Builds a ``CacheProxyConnection`` over the mocked inner connection, with a real
    ``DefaultCache`` configured for the given tracking mode and predicate.
    """

    def build(tracking_mode, cache_predicate=None, max_size=10):
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.can_read.return_value = False

        # ``optin`` with no predicate is inert by design and warns at config time; the
        # tests that pin that warning live in ``tests/test_cache.py``.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            cache = DefaultCache(
                CacheConfig(
                    max_size=max_size,
                    tracking_mode=tracking_mode,
                    cache_predicate=cache_predicate,
                )
            )

        proxy = CacheProxyConnection(mock_connection, cache, threading.RLock())
        return proxy, cache

    return build


def _cache_everything(command, keys):
    return True


def _cache_nothing(command, keys):
    return False


@pytest.mark.fixed_client
class TestTrackingModePairing:
    """
    The wire pairing of ``CLIENT CACHING YES|NO`` with the single read it applies to.

    The server consumes the CACHING flag on the next command that is not a ``CLIENT``
    subcommand, so the pair must reach the socket as one write with nothing between. These
    tests assert that shape against a mocked inner connection; that the pairing actually
    lands on a real server is what the integration tests in ``tests/test_cache.py`` prove.
    """

    @pytest.mark.parametrize(
        "tracking_mode,expected",
        [
            (TrackingMode.PLAIN, ("CLIENT", "TRACKING", "ON")),
            (TrackingMode.OPTIN, ("CLIENT", "TRACKING", "ON", "OPTIN")),
            (TrackingMode.OPTOUT, ("CLIENT", "TRACKING", "ON", "OPTOUT")),
        ],
    )
    def test_the_mode_is_sent_in_the_tracking_handshake(
        self, proxy_factory, mock_connection, tracking_mode, expected
    ):
        # The mode is a connection-setup property: the server refuses to switch a live
        # connection between OPTIN and OPTOUT.
        proxy, _ = proxy_factory(tracking_mode)
        mock_connection._parser = Mock()

        proxy._enable_tracking_callback(mock_connection)

        mock_connection.send_command.assert_called_once_with(*expected)

    def test_the_tracking_callback_flushes_the_cache_only_on_reconnect(
        self, proxy_factory, mock_connection
    ):
        """
        The server destroys a connection's tracking state on disconnect, so entries cached
        through the previous session have lost their invalidation channel. Gated on a previous
        connect because the cache is pool-shared: an unconditional flush would wipe other
        connections' entries every time the pool grows.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        mock_connection._parser = Mock()

        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        # First connect: another connection's entries must survive.
        proxy._enable_tracking_callback(mock_connection)
        assert cache.size == 1

        # Reconnect: the tracking state died with the old socket.
        proxy._enable_tracking_callback(mock_connection)
        assert cache.size == 0

    def test_a_failed_first_tracking_handshake_does_not_flush_on_retry(
        self, proxy_factory, mock_connection
    ):
        """
        A first connect whose ``CLIENT TRACKING`` exchange fails never had tracking on, so
        nothing was cached through it. The next connect is still a first connect and must
        not wipe other connections' entries from the pool-shared cache.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        mock_connection._parser = Mock()

        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=Mock(),
            )
        )

        mock_connection.read_response.side_effect = ConnectionError("lost")
        with pytest.raises(ConnectionError):
            proxy._enable_tracking_callback(mock_connection)

        mock_connection.read_response.side_effect = None
        proxy._enable_tracking_callback(mock_connection)
        assert cache.size == 1

        # Tracking is on now, so the next connect is a reconnect and flushes.
        proxy._enable_tracking_callback(mock_connection)
        assert cache.size == 0

    def test_a_reconnect_inside_send_keeps_the_in_flight_placeholder(
        self, proxy_factory, mock_connection
    ):
        """
        A reconnect inside the send path happens before the command is written, so the reply
        the placeholder is waiting for comes from the new, tracked session. The reconnect
        flush clears everything else, but must keep that placeholder, or the reply is
        returned and never stored.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        mock_connection._parser = Mock()
        proxy._enable_tracking_callback(mock_connection)

        other = CacheKey(
            command="GET", redis_keys=("other",), redis_args=("GET", "other")
        )
        cache.set(
            CacheEntry(
                cache_key=other,
                cache_value=b"old-session",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        reconnected = []

        def reconnect_then_send(*args, **kwargs):
            # Fires the connect callback the way ``send_packed_command`` does when it finds
            # no socket, once - the callback's own ``CLIENT TRACKING`` goes through here too.
            if not reconnected:
                reconnected.append(True)
                proxy._enable_tracking_callback(mock_connection)

        mock_connection.send_command.side_effect = reconnect_then_send
        mock_connection.read_response.return_value = b"bar"

        proxy.send_command("GET", "foo", keys=["foo"])
        assert proxy.read_response() == b"bar"

        assert reconnected, "the reconnect never fired"
        # The old session's entry went with the flush; this read's reply was stored.
        assert cache.get(other) is None
        entry = cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        )
        assert entry.status == CacheEntryStatus.VALID
        assert entry.cache_value == b"bar"

    def test_a_reconnect_flush_drops_another_connections_placeholder(
        self, proxy_factory, mock_connection
    ):
        """
        Only this connection's own placeholder survives the reconnect flush. Another
        connection's is in flight on a socket this reconnect knows nothing about.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        mock_connection._parser = Mock()
        proxy._enable_tracking_callback(mock_connection)

        key = CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        proxy._current_command_cache_key = key
        cache.set(
            CacheEntry(
                cache_key=key,
                cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                status=CacheEntryStatus.IN_PROGRESS,
                connection_ref=Mock(),
            )
        )

        proxy._enable_tracking_callback(mock_connection)

        assert cache.size == 0

    def test_optin_pairs_yes_before_a_stored_miss(self, proxy_factory, mock_connection):
        proxy, cache = proxy_factory(
            TrackingMode.OPTIN, cache_predicate=_cache_everything
        )

        proxy.send_command("GET", "foo", keys=["foo"])

        # One write, the CACHING command first, nothing in between.
        mock_connection.pack_commands.assert_called_once_with(
            [(b"CLIENT", b"CACHING", b"YES"), ("GET", "foo")]
        )
        mock_connection.send_packed_command.assert_called_once_with(
            mock_connection.pack_commands.return_value, check_health=True
        )
        mock_connection.send_command.assert_not_called()
        # The placeholder is in place, so a concurrent invalidation can still cancel it.
        assert cache.size == 1
        assert proxy._pending_caching_reply is True

    def test_optin_sends_a_predicate_excluded_read_alone(
        self, proxy_factory, mock_connection
    ):
        proxy, cache = proxy_factory(TrackingMode.OPTIN, cache_predicate=_cache_nothing)

        proxy.send_command("GET", "foo", keys=["foo"])

        mock_connection.send_command.assert_called_once_with("GET", "foo", keys=["foo"])
        mock_connection.send_packed_command.assert_not_called()
        # Not stored, and no placeholder either: the read is left untracked.
        assert cache.size == 0
        assert proxy._current_command_cache_key is None
        assert proxy._pending_caching_reply is False

    def test_optout_pairs_no_before_a_predicate_excluded_read(
        self, proxy_factory, mock_connection
    ):
        proxy, cache = proxy_factory(
            TrackingMode.OPTOUT, cache_predicate=_cache_nothing
        )

        proxy.send_command("GET", "foo", keys=["foo"])

        mock_connection.pack_commands.assert_called_once_with(
            [(b"CLIENT", b"CACHING", b"NO"), ("GET", "foo")]
        )
        mock_connection.send_command.assert_not_called()
        assert cache.size == 0
        assert proxy._current_command_cache_key is None

    def test_optout_sends_a_stored_miss_alone(self, proxy_factory, mock_connection):
        # Opt-out tracks every trackable read by default, so a read that will be stored needs
        # no CACHING command at all.
        proxy, cache = proxy_factory(TrackingMode.OPTOUT)

        proxy.send_command("GET", "foo", keys=["foo"])

        mock_connection.send_command.assert_called_once_with("GET", "foo", keys=["foo"])
        mock_connection.send_packed_command.assert_not_called()
        assert cache.size == 1

    def test_optout_does_not_pair_before_a_write(self, proxy_factory, mock_connection):
        # A CACHING command in front of a write is consumed with no effect, so sending one
        # would be pure waste.
        proxy, _ = proxy_factory(TrackingMode.OPTOUT)

        proxy.send_command("SET", "foo", "bar")

        mock_connection.send_command.assert_called_once_with("SET", "foo", "bar")
        mock_connection.send_packed_command.assert_not_called()

    def test_optout_pairs_no_before_touch(self, proxy_factory, mock_connection):
        """
        TOUCH is the command opt-out most wants to exempt: read-only and keyed, so the server
        tracks it, but a local cache hit would skip its server-side effect, so it is never
        eligible to store. It is also the one command ``is_replica_safe`` reports False for,
        which is why trackability is decided by ``is_trackable_read`` instead.
        """
        proxy, cache = proxy_factory(TrackingMode.OPTOUT)

        proxy.send_command("TOUCH", "foo", keys=["foo"])

        mock_connection.pack_commands.assert_called_once_with(
            [(b"CLIENT", b"CACHING", b"NO"), ("TOUCH", "foo")]
        )
        assert cache.size == 0

    def test_optout_pairs_no_when_the_invocation_carries_no_keys(
        self, proxy_factory, mock_connection
    ):
        # An eligible read whose command method never plumbed ``keys=`` is a gap in what this
        # client has been taught, not an error. The server tracks it regardless and we will
        # not store it, so exempting it is the right direction.
        proxy, cache = proxy_factory(TrackingMode.OPTOUT)

        proxy.send_command("ZRANK", "foo", "bar")

        mock_connection.pack_commands.assert_called_once_with(
            [(b"CLIENT", b"CACHING", b"NO"), ("ZRANK", "foo", "bar")]
        )
        assert cache.size == 0

    @pytest.mark.parametrize(
        "args,kwargs",
        [
            (("GET", "foo"), {"keys": ["foo"]}),
            (("TOUCH", "foo"), {"keys": ["foo"]}),
            (("SET", "foo", "bar"), {}),
            (("ZRANK", "foo", "bar"), {}),
        ],
        ids=["stored-miss", "touch", "write", "eligible-without-keys"],
    )
    def test_plain_mode_never_pairs(self, proxy_factory, mock_connection, args, kwargs):
        # The compatibility gate: plain mode is byte-for-byte today's behaviour.
        proxy, _ = proxy_factory(TrackingMode.PLAIN)

        proxy.send_command(*args, **kwargs)

        mock_connection.send_command.assert_called_once_with(*args, **kwargs)
        mock_connection.send_packed_command.assert_not_called()
        assert proxy._pending_caching_reply is False

    def test_read_response_consumes_the_caching_reply_first(
        self, proxy_factory, mock_connection
    ):
        proxy, cache = proxy_factory(
            TrackingMode.OPTIN, cache_predicate=_cache_everything
        )
        mock_connection.read_response.side_effect = [b"OK", b"bar"]

        proxy.send_command("GET", "foo", keys=["foo"])

        assert proxy.read_response() == b"bar"
        assert mock_connection.read_response.call_count == 2
        assert proxy._pending_caching_reply is False

        # The data reply is what gets stored, never the ``+OK``.
        stored = cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        )
        assert stored.cache_value == b"bar"
        assert stored.status is CacheEntryStatus.VALID

    def test_a_response_error_on_the_caching_reply_drains_the_data_reply(
        self, proxy_factory, mock_connection
    ):
        """
        The paired read executed on the server regardless. Leaving its reply on the socket
        would return this connection to the pool one reply out of sync, and the next borrower
        would read our answer.
        """
        proxy, cache = proxy_factory(
            TrackingMode.OPTIN, cache_predicate=_cache_everything
        )
        mock_connection.read_response.side_effect = [
            ResponseError("CLIENT CACHING YES is only valid ..."),
            b"bar",
        ]

        proxy.send_command("GET", "foo", keys=["foo"])
        assert cache.size == 1

        with pytest.raises(ResponseError):
            proxy.read_response()

        # Both replies consumed, the placeholder dropped, nothing stored.
        assert mock_connection.read_response.call_count == 2
        assert cache.size == 0
        assert proxy._current_command_cache_key is None

    def test_an_unexpected_caching_reply_fails_the_connection(
        self, proxy_factory, mock_connection
    ):
        proxy, _ = proxy_factory(TrackingMode.OPTIN, cache_predicate=_cache_everything)
        mock_connection.read_response.side_effect = [b"PONG", b"bar"]

        proxy.send_command("GET", "foo", keys=["foo"])

        with pytest.raises(ConnectionError, match="Unexpected CLIENT CACHING reply"):
            proxy.read_response()

    def test_the_ask_redirect_suppression_is_one_shot(
        self, proxy_factory, mock_connection
    ):
        """
        ``ASKING`` and ``CLIENT CACHING`` clear each other on the server, so the redirected
        attempt must be sent alone - and its reply belongs to a migrating slot, so nothing is
        stored for it either. The next read pairs again.
        """
        proxy, cache = proxy_factory(
            TrackingMode.OPTIN, cache_predicate=_cache_everything
        )

        proxy.send_command("ASKING")
        proxy.send_command("GET", "foo", keys=["foo"])

        mock_connection.send_packed_command.assert_not_called()
        assert mock_connection.send_command.call_args_list == [
            call("ASKING"),
            call("GET", "foo", keys=["foo"]),
        ]
        assert cache.size == 0
        assert proxy._current_command_cache_key is None

        proxy.send_command("GET", "foo", keys=["foo"])

        mock_connection.pack_commands.assert_called_once_with(
            [(b"CLIENT", b"CACHING", b"YES"), ("GET", "foo")]
        )

    def test_a_packed_send_consumes_the_ask_redirect_suppression(
        self, proxy_factory, mock_connection
    ):
        """
        The server consumes ``ASKING`` on the next command on the socket, a packed write
        included, so the read after that write pairs and stores as usual.
        """
        proxy, cache = proxy_factory(
            TrackingMode.OPTIN, cache_predicate=_cache_everything
        )

        proxy.send_command("ASKING")
        assert proxy._skip_next_caching is True

        proxy.send_packed_command(b"*1\r\n$4\r\nPING\r\n")
        assert proxy._skip_next_caching is False

        proxy.send_command("GET", "foo", keys=["foo"])

        mock_connection.pack_commands.assert_called_once_with(
            [(b"CLIENT", b"CACHING", b"YES"), ("GET", "foo")]
        )
        assert cache.size == 1

    def test_send_packed_command_clears_the_pending_caching_reply(
        self, proxy_factory, mock_connection
    ):
        proxy, _ = proxy_factory(TrackingMode.OPTIN, cache_predicate=_cache_everything)
        proxy._pending_caching_reply = True

        proxy.send_packed_command(b"*1\r\n$4\r\nPING\r\n")

        assert proxy._pending_caching_reply is False

    def test_a_no_paired_read_leaves_an_existing_entry_untouched(
        self, proxy_factory, mock_connection
    ):
        """
        A read that resolves to "don't cache" never consults the local cache, and never
        overwrites what is already there.

        The entry survives because the not-store branch nulls the current cache key before
        pairing the ``NO``, which is what shuts both the hit lookup and the store-back out of
        the exchange. A key left over from a previous command on this connection is seeded
        here so that null-out is load-bearing rather than incidental.
        """
        proxy, cache = proxy_factory(
            TrackingMode.OPTOUT, cache_predicate=_cache_nothing
        )
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"cached",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        mock_connection.read_response.side_effect = [b"OK", b"fresh"]
        proxy._current_command_cache_key = cache_key

        proxy.send_command("GET", "foo", keys=["foo"])

        mock_connection.pack_commands.assert_called_once_with(
            [(b"CLIENT", b"CACHING", b"NO"), ("GET", "foo")]
        )
        # The server's reply, not the entry sitting in the cache.
        assert proxy.read_response() == b"fresh"
        # ...and that entry is neither evicted nor updated.
        assert cache.size == 1
        assert cache.get(cache_key).cache_value == b"cached"
        assert cache.get(cache_key).status is CacheEntryStatus.VALID

    def test_a_connection_error_on_the_caching_reply_fails_both_commands(
        self, proxy_factory, mock_connection
    ):
        """
        "Fail both, never replay the read alone": a dead socket takes the pair with it.

        Deliberately asymmetric with the ``ResponseError`` path, which drains the paired
        read's reply. Here there is no second reply to take off the socket, so draining would
        block on a connection that is already gone.
        """
        proxy, cache = proxy_factory(
            TrackingMode.OPTIN, cache_predicate=_cache_everything
        )
        mock_connection.read_response.side_effect = ConnectionError("dead socket")

        proxy.send_command("GET", "foo", keys=["foo"])
        assert cache.size == 1

        with pytest.raises(ConnectionError, match="dead socket"):
            proxy.read_response()

        assert mock_connection.read_response.call_count == 1
        # Clear, so a connection that somehow gets reused cannot swallow a later reply as a
        # stale ``+OK``.
        assert proxy._pending_caching_reply is False
        # The placeholder goes with the failed read, as it does for a failed data read,
        # without waiting for a disconnect to flush it.
        assert cache.size == 0
        assert proxy._current_command_cache_key is None

    @pytest.mark.parametrize(
        "side_effect",
        [
            [TimeoutError("timed out")],
            [b"PONG", b"bar"],
        ],
        ids=["timeout", "unexpected-reply"],
    )
    def test_any_failure_on_the_caching_reply_drops_the_placeholder(
        self, proxy_factory, mock_connection, side_effect
    ):
        proxy, cache = proxy_factory(
            TrackingMode.OPTIN, cache_predicate=_cache_everything
        )
        mock_connection.read_response.side_effect = side_effect

        proxy.send_command("GET", "foo", keys=["foo"])
        assert cache.size == 1

        with pytest.raises((TimeoutError, ConnectionError)):
            proxy.read_response()

        assert cache.size == 0
        assert proxy._current_command_cache_key is None

    def test_disconnect_clears_the_pairing_flags(self, proxy_factory, mock_connection):
        proxy, _ = proxy_factory(TrackingMode.OPTOUT)
        proxy._pending_caching_reply = True
        proxy._skip_next_caching = True

        proxy.disconnect()

        assert proxy._pending_caching_reply is False
        assert proxy._skip_next_caching is False


# Every spelling ``pack_command`` accepts: one argument or two, ``str``, ``bytes``,
# ``bytearray`` or ``memoryview``, any case, padded with whitespace or not. Each row is ``(args, the command name the refusal reports)``.
_CACHE_OWNED_SPELLINGS = [
    (("CLIENT CACHING", "NO"), "CLIENT CACHING"),
    (("client caching", "yes"), "CLIENT CACHING"),
    ((b"CLIENT CACHING", b"NO"), "CLIENT CACHING"),
    (("CLIENT", "CACHING", "NO"), "CLIENT CACHING"),
    ((b"CLIENT", b"CACHING", b"YES"), "CLIENT CACHING"),
    ((b"client", "Caching", "no"), "CLIENT CACHING"),
    (("CLIENT TRACKING", "OFF"), "CLIENT TRACKING"),
    (("CLIENT TRACKING", "ON", "REDIRECT", 5), "CLIENT TRACKING"),
    (("client tracking", "on", "bcast"), "CLIENT TRACKING"),
    (("CLIENT", b"TRACKING", b"OFF"), "CLIENT TRACKING"),
    ((b"CLIENT", b"TRACKING", b"OFF"), "CLIENT TRACKING"),
    ((b"CLIENT TRACKING", b"ON", b"OPTIN"), "CLIENT TRACKING"),
    (("RESET",), "RESET"),
    (("reset",), "RESET"),
    ((b"RESET",), "RESET"),
    ((bytearray(b"CLIENT CACHING"), b"NO"), "CLIENT CACHING"),
    (("CLIENT", memoryview(b"CACHING"), "YES"), "CLIENT CACHING"),
    (
        (bytearray(b"CLIENT"), bytearray(b"TRACKING"), bytearray(b"OFF")),
        "CLIENT TRACKING",
    ),
    ((memoryview(b"RESET"),), "RESET"),
    ((" CLIENT TRACKING", "OFF"), "CLIENT TRACKING"),
    (("\tclient tracking", "off"), "CLIENT TRACKING"),
    ((b" CLIENT CACHING", b"NO"), "CLIENT CACHING"),
    ((" RESET",), "RESET"),
    (("reset\t",), "RESET"),
]
_CACHE_OWNED_IDS = [
    "caching-str-one-arg",
    "caching-str-lowercase",
    "caching-bytes-one-arg",
    "caching-str-two-args",
    "caching-bytes-two-args",
    "caching-mixed-case-and-types",
    "tracking-off",
    "tracking-on-redirect",
    "tracking-lowercase-bcast",
    "tracking-str-bytes-two-args",
    "tracking-bytes-two-args",
    "tracking-bytes-one-arg-optin",
    "reset",
    "reset-lowercase",
    "reset-bytes",
    "caching-bytearray-one-arg",
    "caching-memoryview-subcommand",
    "tracking-bytearray-two-args",
    "reset-memoryview",
    "tracking-leading-space",
    "tracking-leading-tab",
    "caching-bytes-leading-space",
    "reset-leading-space",
    "reset-trailing-whitespace",
]


@pytest.mark.fixed_client
class TestUserSentCacheOwnedCommands:
    """
    ``CLIENT CACHING``, ``CLIENT TRACKING`` and ``RESET`` each change the tracking state
    the cache relies on for invalidations: a stray ``CLIENT CACHING`` is consumed by
    whatever the socket sends next, and the other two stop or redirect invalidations while
    the cache keeps storing replies. So the cache refuses them on every connection it
    manages, in every mode.
    """

    @pytest.mark.parametrize("mode", list(TrackingMode))
    @pytest.mark.parametrize(
        "args,command", _CACHE_OWNED_SPELLINGS, ids=_CACHE_OWNED_IDS
    )
    def test_send_command_refuses_cache_owned_commands(
        self, proxy_factory, mock_connection, mode, args, command
    ):
        proxy, _ = proxy_factory(mode)

        with pytest.raises(RedisError, match=f"^{command} cannot be sent"):
            proxy.send_command(*args)

        mock_connection.send_command.assert_not_called()
        mock_connection.send_packed_command.assert_not_called()

    @pytest.mark.parametrize(
        "args,command", _CACHE_OWNED_SPELLINGS, ids=_CACHE_OWNED_IDS
    )
    def test_pack_commands_refuses_cache_owned_commands(
        self, proxy_factory, mock_connection, args, command
    ):
        """
        Every pipeline and transaction packs through ``pack_commands``, so refusing there
        covers them without touching the pipeline code.
        """
        proxy, _ = proxy_factory(TrackingMode.OPTOUT)

        with pytest.raises(RedisError, match=f"^{command} cannot be sent"):
            proxy.pack_commands([("SET", "a", "1"), args, ("GET", "a")])

        mock_connection.pack_commands.assert_not_called()

    @pytest.mark.parametrize(
        "args",
        [
            ("CLIENT", "TRACKINGINFO"),
            (b"CLIENT TRACKINGINFO",),
            ("CLIENT GETREDIR",),
            ("CLIENT ID",),
            ("CLIENT",),
            ("CONFIG", "GET", "maxmemory"),
            ("CONFIG RESETSTAT",),
            ("ACL LOG", b"RESET"),
            ("RENAME", "a", "b"),
            (b"rpush", b"a", b"1"),
            ("GET", "foo"),
            (b"CACHING",),
            (b"TRACKING", b"OFF"),
            (" CLIENT TRACKINGINFO",),
            (bytearray(b"RENAME"), "a", "b"),
        ],
        ids=[
            "trackinginfo",
            "trackinginfo-bytes",
            "getredir",
            "client-id",
            "bare-client",
            "config",
            "config-resetstat",
            "acl-log-reset",
            "rename",
            "five-char-r-command",
            "get",
            "no-client-caching",
            "no-client-tracking",
            "trackinginfo-leading-space",
            "rename-bytearray",
        ],
    )
    def test_other_commands_pass_through(self, proxy_factory, mock_connection, args):
        # ``plain``, so no command is paired and every one reaches ``send_command`` as is.
        proxy, _ = proxy_factory(TrackingMode.PLAIN)

        proxy.send_command(*args)
        proxy.pack_commands([args])

        mock_connection.send_command.assert_called_once_with(*args)
        mock_connection.pack_commands.assert_called_once_with([args])

    def test_the_caches_own_tracking_handshake_is_not_refused(
        self, proxy_factory, mock_connection
    ):
        """
        The connect callback is registered on the wrapped connection, which calls it with
        itself, so the handshake's ``CLIENT TRACKING ON`` never passes the guarded
        ``send_command``. ``test_the_mode_is_sent_in_the_tracking_handshake`` pins what it
        sends.
        """
        proxy, _ = proxy_factory(TrackingMode.OPTOUT)
        mock_connection._parser = Mock()

        mock_connection.register_connect_callback.assert_called_once_with(
            proxy._enable_tracking_callback
        )
        proxy._enable_tracking_callback(mock_connection)

        mock_connection.send_command.assert_called_once_with(
            "CLIENT", "TRACKING", "ON", "OPTOUT"
        )

    def test_the_caches_own_pairing_is_not_refused(
        self, proxy_factory, mock_connection
    ):
        """
        The pairing packs through the wrapped connection, not through the guarded
        ``pack_commands``, so the cache can still send its own ``CLIENT CACHING``.
        """
        proxy, _ = proxy_factory(TrackingMode.OPTIN, cache_predicate=_cache_everything)

        proxy.send_command("GET", "foo", keys=["foo"])

        mock_connection.pack_commands.assert_called_once()
        packed = mock_connection.pack_commands.call_args.args[0]
        assert packed[0] == (b"CLIENT", b"CACHING", b"YES")


@pytest.mark.fixed_client
class TestCacheEntryLifecycle:
    """
    What the pool-wide cache holds after a read, and when it may be trusted.

    A hit is decided once in ``send_command`` and carried to ``read_response``, never
    re-derived from cache state. A MOVED flushes the whole cache, an ASK does not, and a
    read that fails drops only the placeholder this connection staked for it.
    """

    # ---------------------------------------------------------------- the hit decision

    @pytest.mark.parametrize("remove_entry", [False, True], ids=["queued", "removed"])
    def test_an_entry_promoted_during_a_miss_is_not_a_hit(
        self, proxy_factory, mock_connection, remove_entry
    ):
        """An initially IN_PROGRESS entry cannot become a hit without its own drain."""
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        owner_connection = Mock(
            spec=ConnectionInterface,
            retry="mock",
            host="mock",
            port=6379,
            db=0,
            credential_provider=UsernamePasswordCredentialProvider(),
            _event_dispatcher=EventDispatcher(),
        )
        owner_connection.can_read.return_value = False
        owner = CacheProxyConnection(owner_connection, cache, proxy._pool_lock)
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )

        replies = iter([b"old", None])

        def read_owner_reply(**kwargs):
            reply = next(replies)
            if reply is None:
                owner._on_invalidation_callback(["invalidate", [b"foo"]])
            return reply

        owner_connection.read_response.side_effect = read_owner_reply
        owner.send_command("GET", "foo", keys=["foo"])
        candidate = cache.get(cache_key)
        assert candidate.status is CacheEntryStatus.IN_PROGRESS
        # The reply is followed by an invalidation queued on the owner's socket.
        owner_connection.can_read.side_effect = [True, False]
        mock_connection.read_response.return_value = b"new"

        waiting = threading.Event()
        resume = threading.Event()
        errors = []
        real_cache_set = cache.set

        def send_command():
            try:
                proxy.send_command("GET", "foo", keys=["foo"])
            except BaseException as error:
                errors.append(error)

        def set_cache_entry(entry):
            # The miss path installs its placeholder right after classifying the
            # entry as IN_PROGRESS. Stop between the two, while the original
            # placeholder is still the one in the cache.
            if (
                entry.cache_key == cache_key
                and entry.status is CacheEntryStatus.IN_PROGRESS
                and entry.connection_ref is mock_connection
            ):
                waiting.set()
                assert resume.wait(timeout=5)
            return real_cache_set(entry)

        with patch.object(cache, "set", side_effect=set_cache_entry):
            sender = threading.Thread(target=send_command)
            sender.start()
            try:
                assert waiting.wait(timeout=5)
                # Promote the original entry before our miss replaces it.
                assert owner.read_response() == b"old"
                assert cache.get(cache_key) is candidate
                assert candidate.status is CacheEntryStatus.VALID
                if remove_entry:
                    owner._process_pending_invalidations()
                    assert cache.get(cache_key) is None
            finally:
                resume.set()
                sender.join(timeout=5)

            assert not sender.is_alive()
            assert errors == []

        # Promotion, with or without subsequent removal, cannot turn this
        # already-decided miss into a hit.
        assert proxy.read_response() == b"new"
        mock_connection.send_command.assert_called_once_with("GET", "foo", keys=["foo"])
        assert owner_connection.read_response.call_count == (2 if remove_entry else 1)

    def test_an_entry_replaced_during_the_drain_is_not_a_hit(
        self, proxy_factory, mock_connection
    ):
        """Draining one entry's owner does not make a replacement safe to serve."""
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        owner = Mock()
        owner.can_read.side_effect = [True, False]
        candidate = CacheEntry(
            cache_key=cache_key,
            cache_value=b"old",
            status=CacheEntryStatus.VALID,
            connection_ref=owner,
        )
        replacement_owner = Mock()
        replacement_owner.can_read.side_effect = [True, False]
        replacement = CacheEntry(
            cache_key=cache_key,
            cache_value=b"replacement",
            status=CacheEntryStatus.VALID,
            connection_ref=replacement_owner,
        )
        cache.set(candidate)
        mock_connection.read_response.return_value = b"new"

        def replace_during_drain(**kwargs):
            cache.delete_by_redis_keys([b"foo"])
            cache.set(replacement)
            return None

        owner.read_response.side_effect = replace_during_drain

        # A drained-only check would serve the replacement, although its owner's
        # invalidations were never read. Equal hashes do not prove identity either.
        with patch.object(CacheEntry, "__hash__", return_value=0):
            assert candidate == replacement
            assert candidate is not replacement
            proxy.send_command("GET", "foo", keys=["foo"])

        assert proxy.read_response() == b"new"
        mock_connection.send_command.assert_called_once_with("GET", "foo", keys=["foo"])
        owner.read_response.assert_called_once_with(
            push_request=True, timeout=0, disconnect_on_error=False
        )
        replacement_owner.read_response.assert_not_called()

    def test_another_connection_filling_our_placeholder_does_not_skip_our_reply(
        self, proxy_factory, mock_connection
    ):
        """
        The hit is decided once, in ``send_command``, and never re-derived in
        ``read_response``.

        ``send_command`` replaces any entry for the key with an IN_PROGRESS placeholder and
        sends. Another connection reading the same key resolves that very entry object and
        flips it to VALID with *its* reply. A ``read_response`` that asked the cache again
        would hand back that value and never read the reply we did send, leaving the
        connection one reply out of sync for whoever borrows it next.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        mock_connection.read_response.return_value = b"ours"

        proxy.send_command("GET", "foo", keys=["foo"])

        # Stand in for the other connection completing first, in place, on our entry.
        entry = cache.get(cache_key)
        assert entry.status is CacheEntryStatus.IN_PROGRESS
        entry.status = CacheEntryStatus.VALID
        entry.cache_value = b"theirs"
        cache.set(entry)

        # Our own reply, read off the socket.
        assert proxy.read_response() == b"ours"
        assert mock_connection.read_response.call_count == 1

    def test_an_invalidation_after_the_hit_was_decided_still_serves_it(
        self, proxy_factory, mock_connection
    ):
        """
        The other direction of the same rule.

        ``send_command`` resolved a hit and wrote nothing, so there is no reply on the socket.
        A ``read_response`` that asked the cache again would find the entry gone - invalidated
        in between - and read a reply for a command that was never sent.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"cached",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        proxy.send_command("GET", "foo", keys=["foo"])
        mock_connection.send_command.assert_not_called()

        # The invalidation lands between the two calls.
        cache.flush()

        assert proxy.read_response() == b"cached"
        mock_connection.read_response.assert_not_called()

    def test_an_in_progress_entry_from_another_connection_is_not_a_hit(
        self, proxy_factory, mock_connection
    ):
        """
        A placeholder is somebody else's fetch in flight and carries no value to serve.

        Returning early on one used to leave ``read_response`` with no value to hand back and
        a socket it never wrote to, so it read the next reply that happened to arrive.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                status=CacheEntryStatus.IN_PROGRESS,
                connection_ref=mock_connection,
            )
        )
        mock_connection.can_read.return_value = False
        mock_connection.read_response.return_value = b"fresh"

        proxy.send_command("GET", "foo", keys=["foo"])

        # Sent, rather than answered from a placeholder that holds nothing.
        mock_connection.send_command.assert_called_once_with("GET", "foo", keys=["foo"])
        assert proxy.read_response() == b"fresh"

    @pytest.mark.parametrize(
        "tracking_mode", [TrackingMode.PLAIN, TrackingMode.OPTIN, TrackingMode.OPTOUT]
    )
    def test_an_in_progress_entry_is_not_drained(
        self, proxy_factory, mock_connection, tracking_mode
    ):
        """
        The owner's socket of a placeholder is never drained.

        The drain returns non-push replies too, so it would consume the owner's reply
        pair - or another command's reply if the placeholder was stranded. An IN_PROGRESS
        entry is never served either way, so there is nothing for the drain to protect.
        """
        proxy, cache = proxy_factory(tracking_mode, cache_predicate=_cache_everything)
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        owner = Mock()
        # One readable reply, so a regression fails the assertion rather than spinning.
        owner.can_read.side_effect = [True, False]
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                status=CacheEntryStatus.IN_PROGRESS,
                connection_ref=owner,
            )
        )

        proxy.send_command("GET", "foo", keys=["foo"])

        owner.read_response.assert_not_called()
        # Our own placeholder replaced the other one.
        assert cache.get(cache_key).connection_ref is mock_connection

    def test_one_shot_keys_are_indexed_under_the_real_keys(
        self, proxy_factory, mock_connection
    ):
        """
        ``keys`` is materialized once, so a generator is not consumed by the intent check
        and stored with no keys - an entry the reverse index could never invalidate.
        """
        proxy, cache = proxy_factory(
            TrackingMode.PLAIN, cache_predicate=_cache_everything
        )
        mock_connection.read_response.return_value = b"bar"

        proxy.send_command("GET", "foo", keys=(key for key in ["foo"]))
        assert proxy.read_response() == b"bar"

        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        assert cache.get(cache_key).cache_value == b"bar"
        cache.delete_by_redis_keys([b"foo"])
        assert cache.get(cache_key) is None

    def test_a_hit_records_the_metrics_without_touching_the_socket(
        self, proxy_factory, mock_connection
    ):
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache.set(
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                ),
                cache_value=b"cached",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        proxy.send_command("GET", "foo", keys=["foo"])
        with patch("redis.connection.record_csc_request") as record:
            assert proxy.read_response() == b"cached"

        record.assert_called_once_with(result=CSCResult.HIT)

    def test_a_stored_miss_records_a_miss(self, proxy_factory, mock_connection):
        proxy, _ = proxy_factory(TrackingMode.PLAIN)
        mock_connection.read_response.return_value = b"fresh"

        proxy.send_command("GET", "foo", keys=["foo"])
        with patch("redis.connection.record_csc_request") as record:
            assert proxy.read_response() == b"fresh"

        record.assert_called_once_with(result=CSCResult.MISS)

    def test_a_pending_hit_does_not_outlive_a_disconnect(
        self, proxy_factory, mock_connection
    ):
        # A resolved-but-unserved hit belongs to the exchange the dead socket was part of.
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache.set(
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                ),
                cache_value=b"cached",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        mock_connection.read_response.return_value = b"fresh"

        proxy.send_command("GET", "foo", keys=["foo"])
        proxy.disconnect()

        # The next read goes to the wire rather than replaying a stale local value.
        proxy.send_command("GET", "foo", keys=["foo"])
        assert proxy.read_response() == b"fresh"

    @pytest.mark.parametrize("redirect", [AskError, MovedError], ids=["ask", "moved"])
    def test_a_redirect_does_not_flush_the_cache(
        self, proxy_factory, mock_connection, redirect
    ):
        """
        A redirect is a scoped cleanup, not a flush. Under an ASK the slot is still owned by
        this node while it migrates, so its tracking is still good; once a slot moves, the
        server itself invalidates the moved keys on the connection that tracked them, so
        nothing cached needs to go on the client's initiative either.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache.set(
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=("other",), redis_args=("GET", "other")
                ),
                cache_value=b"kept",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        mock_connection.read_response.side_effect = redirect("3999 127.0.0.1:6381")

        proxy.send_command("GET", "foo", keys=["foo"])

        with pytest.raises(redirect):
            proxy.read_response()

        # This read's own placeholder goes - otherwise it would sit in the cache for as long
        # as the slot migrates, since every read of the key is redirected the same way - but
        # nothing else does.
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            )
            is None
        )
        assert (
            cache.get(
                CacheKey(
                    command="GET", redis_keys=("other",), redis_args=("GET", "other")
                )
            ).cache_value
            == b"kept"
        )

    # ---------------------------------------------------------------- failed reads

    def test_a_failed_read_does_not_evict_another_connections_placeholder(
        self, proxy_factory, mock_connection
    ):
        """
        The cache is pool-wide and each proxy's lock is its own. Between our send and our
        failure, another connection's ``send_command`` for the same key can replace the
        entry with its own placeholder; deleting by key alone would evict that fetch while
        it is still in flight.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        other = Mock()
        mock_connection.read_response.side_effect = ResponseError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )

        proxy.send_command("GET", "foo", keys=["foo"])
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                status=CacheEntryStatus.IN_PROGRESS,
                connection_ref=other,
            )
        )

        with pytest.raises(ResponseError):
            proxy.read_response()

        assert cache.get(cache_key).connection_ref is other
        assert proxy._current_command_cache_key is None

    def test_a_failed_read_does_not_evict_a_placeholder_resolved_in_place(
        self, proxy_factory, mock_connection
    ):
        """
        Another connection's successful read can resolve this very entry object to VALID
        without replacing it, so it keeps our ``connection_ref``. The status check is what
        stops our failure from throwing that correctly stored reply away.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        mock_connection.read_response.side_effect = ResponseError("NOPERM")

        proxy.send_command("GET", "foo", keys=["foo"])
        entry = cache.get(cache_key)
        entry.status = CacheEntryStatus.VALID
        entry.cache_value = b"theirs"
        cache.set(entry)

        with pytest.raises(ResponseError):
            proxy.read_response()

        assert cache.get(cache_key).cache_value == b"theirs"
        assert cache.get(cache_key).status == CacheEntryStatus.VALID

    def test_an_error_on_the_caching_reply_is_scoped_the_same_way(
        self, proxy_factory, mock_connection
    ):
        """
        The ``+OK`` of a CLIENT CACHING pair can fail too, and after the paired read is
        drained the placeholder is dropped by the same ``except BaseException`` as a failed
        data read. It uses the same scoped rule, so it cannot evict an entry another
        connection now owns either.
        """
        proxy, cache = proxy_factory(
            TrackingMode.OPTIN, cache_predicate=_cache_everything
        )
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        other = Mock()
        mock_connection.read_response.side_effect = [
            ResponseError("CLIENT CACHING YES is only valid ..."),
            b"bar",
        ]

        proxy.send_command("GET", "foo", keys=["foo"])
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                status=CacheEntryStatus.IN_PROGRESS,
                connection_ref=other,
            )
        )

        with pytest.raises(ResponseError):
            proxy.read_response()

        # Both replies drained, and the other connection's placeholder survives.
        assert mock_connection.read_response.call_count == 2
        assert cache.get(cache_key).connection_ref is other

    def test_a_reply_does_not_promote_another_connections_placeholder(
        self, proxy_factory, mock_connection
    ):
        """
        Between our send and our read, another connection's ``send_command`` for the same
        key can replace the entry with its own placeholder. Promoting that one would bind
        our reply to its ``connection_ref``, and the next hit would drain that connection's
        own reply off its socket.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        other = Mock()
        mock_connection.read_response.return_value = b"ours"

        proxy.send_command("GET", "foo", keys=["foo"])
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                status=CacheEntryStatus.IN_PROGRESS,
                connection_ref=other,
            )
        )

        assert proxy.read_response() == b"ours"

        entry = cache.get(cache_key)
        assert entry.status == CacheEntryStatus.IN_PROGRESS
        assert entry.connection_ref is other
        assert proxy._current_command_cache_key is None

    def test_a_reply_does_not_overwrite_an_entry_resolved_by_another_connection(
        self, proxy_factory, mock_connection
    ):
        """
        The placeholder we staked may already have been replaced and resolved to VALID by
        another connection. Our reply may be the older one, so it must not overwrite it.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        other = Mock()
        mock_connection.read_response.return_value = b"ours"

        proxy.send_command("GET", "foo", keys=["foo"])
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"theirs",
                status=CacheEntryStatus.VALID,
                connection_ref=other,
            )
        )

        assert proxy.read_response() == b"ours"
        assert cache.get(cache_key).cache_value == b"theirs"
        assert cache.get(cache_key).connection_ref is other

    def test_a_nil_reply_does_not_evict_another_connections_entry(
        self, proxy_factory, mock_connection
    ):
        """
        A nil reply is not stored, and drops our own placeholder - under the same scoped
        rule as a failed read, so an entry another connection now owns survives it.
        """
        proxy, cache = proxy_factory(TrackingMode.PLAIN)
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        other = Mock()
        mock_connection.read_response.return_value = None

        proxy.send_command("GET", "foo", keys=["foo"])
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                status=CacheEntryStatus.IN_PROGRESS,
                connection_ref=other,
            )
        )

        assert proxy.read_response() is None
        assert cache.get(cache_key).connection_ref is other
        assert proxy._current_command_cache_key is None

        # Our own placeholder is still dropped on a nil reply.
        cache.flush()
        proxy.send_command("GET", "foo", keys=["foo"])
        assert proxy.read_response() is None
        assert cache.get(cache_key) is None

    @pytest.mark.parametrize("command,keys", [("MGET", []), ("EXISTS", ())])
    def test_an_empty_key_list_is_not_offered_to_the_predicate(
        self, proxy_factory, mock_connection, command, keys
    ):
        """
        ``mget([])`` and ``exists()`` pass an empty key list. It is treated as no key list,
        so a predicate that reads ``keys[0]`` is never called with nothing to read.
        """
        predicate = Mock(side_effect=lambda command, keys: bool(keys[0]))
        proxy, cache = proxy_factory(TrackingMode.OPTIN, cache_predicate=predicate)
        mock_connection.read_response.return_value = []

        proxy.send_command(command, keys=keys)
        assert proxy.read_response() == []

        predicate.assert_not_called()
        mock_connection.send_command.assert_called_once_with(command, keys=keys)
        assert cache.size == 0


@pytest.mark.fixed_client
@pytest.mark.onlynoncluster
@pytest.mark.parametrize(
    "parser_class",
    [_RESP3Parser, _HiredisParser],
    ids=["RESP3Parser", "HiredisParser"],
)
def test_invalidation_pushes_interleave_with_a_caching_pair(r, parser_class):
    """
    The server may send an invalidation message at any reply boundary, including the two
    boundaries a ``CLIENT CACHING`` pair introduces.

    Both parsers have to hand the push to the invalidation handler and carry on to the next
    real reply. If either stopped at the push instead, the ``+OK`` check would see a push list
    and fail the connection, or the paired read would return the push as its own reply. Driven
    over a canned byte stream because a Mock connection cannot exercise a parser at all.
    """
    if parser_class is _HiredisParser and not HIREDIS_AVAILABLE:
        pytest.skip("Hiredis not available")

    args = dict(r.connection_pool.connection_kwargs)
    args["parser_class"] = parser_class
    args["protocol"] = 3
    conn = Connection(**args)
    conn.connect()

    cache = DefaultCache(CacheConfig(max_size=10))
    for key in ("before", "between"):
        cache.set(
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=(key,), redis_args=("GET", key)
                ),
                cache_value=b"stale",
                status=CacheEntryStatus.VALID,
                connection_ref=conn,
            )
        )
    assert cache.size == 2

    proxy = CacheProxyConnection(conn, cache, threading.RLock())
    # ``_enable_tracking_callback`` wires this on connect; this connection is already up.
    conn._parser.set_invalidation_push_handler(proxy._on_invalidation_callback)

    stream = (
        # Ahead of the ``+OK``.
        b">2\r\n$10\r\ninvalidate\r\n*1\r\n$6\r\nbefore\r\n"
        b"+OK\r\n"
        # Between the ``+OK`` and the paired read's reply.
        b">2\r\n$10\r\ninvalidate\r\n*1\r\n$7\r\nbetween\r\n"
        b"$5\r\nfresh\r\n"
    )
    mock_socket = MockSocket(stream)
    if isinstance(conn._parser, _RESP3Parser):
        conn._parser._buffer._sock = mock_socket
    else:
        conn._parser._sock = mock_socket

    proxy._pending_caching_reply = True

    assert proxy.read_response(disconnect_on_error=False) in (b"fresh", "fresh")
    # Both pushes reached the handler rather than being mistaken for command replies.
    assert cache.size == 0


@pytest.mark.fixed_client
class TestConnectionPoolGetConnectionCount:
    """Tests for ConnectionPool.get_connection_count() method."""

    def test_get_connection_count_returns_idle_and_used_counts(self):
        """Test that get_connection_count returns both idle and used connection counts."""
        pool = ConnectionPool(max_connections=10)

        # Initially, no connections exist
        counts = pool.get_connection_count()
        assert len(counts) == 2

        # Check idle connections count
        idle_count, idle_attrs = counts[0]
        assert idle_count == 0
        assert DB_CLIENT_CONNECTION_POOL_NAME in idle_attrs
        assert idle_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.IDLE.value

        # Check used connections count
        used_count, used_attrs = counts[1]
        assert used_count == 0
        assert DB_CLIENT_CONNECTION_POOL_NAME in used_attrs
        assert used_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.USED.value

        pool.disconnect()

    def test_get_connection_count_with_connections_in_use(self):
        """Test get_connection_count when connections are in use."""

        pool = ConnectionPool(max_connections=10)

        # Create mock connections
        mock_conn1 = MagicMock()
        mock_conn1.pid = pool.pid

        mock_conn2 = MagicMock()
        mock_conn2.pid = pool.pid

        # Simulate connections in use
        pool._in_use_connections.add(mock_conn1)
        pool._in_use_connections.add(mock_conn2)

        counts = pool.get_connection_count()

        idle_count, idle_attrs = counts[0]
        used_count, used_attrs = counts[1]

        assert idle_count == 0
        assert used_count == 2
        assert idle_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.IDLE.value
        assert used_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.USED.value

        pool.disconnect()

    def test_get_connection_count_with_available_connections(self):
        """Test get_connection_count when connections are available (idle)."""

        pool = ConnectionPool(max_connections=10)

        # Create mock connections
        mock_conn1 = MagicMock()
        mock_conn1.pid = pool.pid

        mock_conn2 = MagicMock()
        mock_conn2.pid = pool.pid

        mock_conn3 = MagicMock()
        mock_conn3.pid = pool.pid

        # Simulate available connections
        pool._available_connections.append(mock_conn1)
        pool._available_connections.append(mock_conn2)
        pool._available_connections.append(mock_conn3)

        counts = pool.get_connection_count()

        idle_count, idle_attrs = counts[0]
        used_count, used_attrs = counts[1]

        assert idle_count == 3
        assert used_count == 0
        assert idle_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.IDLE.value
        assert used_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.USED.value

        pool.disconnect()

    def test_get_connection_count_mixed_connections(self):
        """Test get_connection_count with both idle and used connections."""

        pool = ConnectionPool(max_connections=10)

        # Create mock connections
        mock_idle = MagicMock()
        mock_idle.pid = pool.pid

        mock_used1 = MagicMock()
        mock_used1.pid = pool.pid

        mock_used2 = MagicMock()
        mock_used2.pid = pool.pid

        # Simulate mixed state
        pool._available_connections.append(mock_idle)
        pool._in_use_connections.add(mock_used1)
        pool._in_use_connections.add(mock_used2)

        counts = pool.get_connection_count()

        idle_count, _ = counts[0]
        used_count, _ = counts[1]

        assert idle_count == 1
        assert used_count == 2

        pool.disconnect()

    def test_get_connection_count_includes_pool_name_in_attributes(self):
        """Test that get_connection_count includes pool name in attributes."""
        from redis.observability.attributes import get_pool_name

        pool = ConnectionPool(max_connections=10)

        counts = pool.get_connection_count()

        _, idle_attrs = counts[0]
        _, used_attrs = counts[1]

        # Both should have the pool name
        assert DB_CLIENT_CONNECTION_POOL_NAME in idle_attrs
        assert DB_CLIENT_CONNECTION_POOL_NAME in used_attrs

        # Pool name should match the format from get_pool_name() (host:port_uniqueID)
        expected_pool_name = get_pool_name(pool)
        assert idle_attrs[DB_CLIENT_CONNECTION_POOL_NAME] == expected_pool_name
        assert used_attrs[DB_CLIENT_CONNECTION_POOL_NAME] == expected_pool_name

        # Verify the pool name has the expected format (host:port_uniqueID)
        assert "unknown:6379_" in expected_pool_name

        # Verify the unique ID is 8 hex characters (matching go-redis)
        parts = expected_pool_name.split("_")
        assert len(parts) == 2, (
            f"Pool name should have format host:port_id, got: {expected_pool_name}"
        )
        unique_id = parts[1]
        assert len(unique_id) == 8, (
            f"Unique ID should be 8 characters, got: {unique_id}"
        )

        pool.disconnect()


@pytest.mark.fixed_client
class TestBlockingConnectionPoolGetConnectionCount:
    """Tests for BlockingConnectionPool.get_connection_count() method."""

    def test_get_connection_count_returns_idle_and_used_counts(self):
        """Test that BlockingConnectionPool.get_connection_count returns both counts."""

        pool = BlockingConnectionPool(max_connections=10)

        # Initially, no connections exist
        counts = pool.get_connection_count()
        assert len(counts) == 2

        idle_count, idle_attrs = counts[0]
        used_count, used_attrs = counts[1]

        assert idle_count == 0
        assert used_count == 0
        assert idle_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.IDLE.value
        assert used_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.USED.value

        pool.disconnect()

    def test_get_connection_count_with_connections_in_queue(self):
        """Test get_connection_count when connections are in the queue (idle)."""

        pool = BlockingConnectionPool(max_connections=10)

        # Create mock connections and add to queue
        mock_conn1 = MagicMock()
        mock_conn1.pid = pool.pid

        mock_conn2 = MagicMock()
        mock_conn2.pid = pool.pid

        # Add connections to the pool's internal list and queue
        pool._connections.append(mock_conn1)
        pool._connections.append(mock_conn2)

        # Clear the queue and add our connections
        while not pool.pool.empty():
            try:
                pool.pool.get_nowait()
            except Exception:
                break

        pool.pool.put_nowait(mock_conn1)
        pool.pool.put_nowait(mock_conn2)

        counts = pool.get_connection_count()

        idle_count, _ = counts[0]
        used_count, _ = counts[1]

        assert idle_count == 2
        assert used_count == 0

        pool.disconnect()


@pytest.mark.fixed_client
def test_parse_url_retry_on_error_resolves_exception_names():
    kw = parse_url("redis://localhost:6379/?retry_on_error=ConnectionError")
    assert kw["retry_on_error"] == [ConnectionError]


@pytest.mark.fixed_client
def test_parse_url_retry_on_error_comma_separated():
    kw = parse_url(
        "redis://localhost:6379/?retry_on_error=ConnectionError,TimeoutError"
    )
    assert kw["retry_on_error"] == [ConnectionError, TimeoutError]


@pytest.mark.fixed_client
def test_parse_url_retry_on_error_bracket_list():
    kw = parse_url(
        "redis://localhost:6379/?retry_on_error=[ConnectionError,TimeoutError]"
    )
    assert kw["retry_on_error"] == [ConnectionError, TimeoutError]


@pytest.mark.fixed_client
def test_parse_url_retry_on_error_blank_entry():
    with pytest.raises(ValueError) as exc_info:
        parse_url("redis://localhost:6379/?retry_on_error=,")
    assert str(exc_info.value) == (
        "Invalid value for 'retry_on_error' in connection URL."
    )


@pytest.mark.fixed_client
def test_parse_url_invalid_db_keeps_stable_message():
    with pytest.raises(ValueError) as exc_info:
        parse_url("redis://localhost:6379/?db=not-an-int")
    assert str(exc_info.value) == "Invalid value for 'db' in connection URL."


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    ("url", "expected_port"),
    (
        ("redis://localhost", None),
        ("redis://localhost:6380", 6380),
        ("redis://localhost:0", 0),
    ),
)
def test_connection_pool_from_url_preserves_explicit_port(url, expected_port):
    kwargs = parse_url(url)
    pool = ConnectionPool.from_url(url)

    assert kwargs.get("port") == expected_port
    assert pool.connection_kwargs.get("port") == expected_port


@pytest.mark.fixed_client
def test_parse_url_retry_on_error_unknown_name():
    with pytest.raises(ValueError) as exc_info:
        parse_url("redis://localhost:6379/?retry_on_error=NotARealError")
    assert str(exc_info.value) == (
        "Invalid value for 'retry_on_error' in connection URL."
    )


@pytest.mark.fixed_client
def test_parse_url_retry_on_error_usable_in_retry():
    kw = parse_url("redis://localhost:6379/?retry_on_error=ConnectionError")
    conn = Connection(**kw)
    assert ConnectionError in conn.retry._supported_errors
    assert all(
        isinstance(err, type) and issubclass(err, Exception)
        for err in conn.retry._supported_errors
    )

    calls = 0

    def do():
        nonlocal calls
        calls += 1
        raise ConnectionError("simulated network drop")

    with pytest.raises(ConnectionError):
        conn.retry.call_with_retry(do=do, fail=lambda e: None)
    assert calls == 2


@pytest.mark.fixed_client
@pytest.mark.parametrize("port", [True, False, 1.5, "nope", None])
def test_connection_rejects_bool_port(port):
    """bool subclasses int; port=True must not become privileged port 1."""
    with pytest.raises(TypeError, match="port must be an integer"):
        redis.Connection(port=port)


@pytest.mark.fixed_client
def test_connection_accepts_numeric_port_string():
    """Callers still pass a decimal string such as \"6379\"."""
    c = redis.Connection(port="6379")
    assert c.port == 6379


@pytest.mark.fixed_client
@pytest.mark.parametrize("port", [-1, 65536, 99999])
def test_connection_rejects_out_of_range_port(port):
    with pytest.raises(ValueError, match="port must be in 0..65535"):
        redis.Connection(port=port)


@pytest.mark.fixed_client
def test_connection_allows_ephemeral_port_zero():
    c = redis.Connection(port=0)
    assert c.port == 0


class _CannedSocket:
    """Serves a canned byte stream, then behaves like an open-but-idle socket."""

    def __init__(self, data):
        self.data = data
        self.timeout = None

    def recv(self, n):
        if not self.data:
            raise socket.timeout("idle")
        chunk, self.data = self.data[:n], self.data[n:]
        return chunk

    def recv_into(self, buffer, nbytes=0):
        # _HiredisParser reads through recv_into, so both parsers must be
        # served here: the CI matrix runs the suite with and without hiredis
        # installed, and pinning one parser would leave the other untested.
        chunk = self.recv(nbytes or len(buffer))
        buffer[: len(chunk)] = chunk
        return len(chunk)

    def settimeout(self, t):
        self.timeout = t

    def gettimeout(self):
        return self.timeout

    def close(self):
        pass

    def shutdown(self, how):
        pass


def _connection_with_stream(data, parser_class=None, **kwargs):
    if parser_class is not None:
        kwargs["parser_class"] = parser_class
    conn = Connection(protocol=2, **kwargs)
    conn._sock = _CannedSocket(data)
    conn._parser.on_connect(conn)
    return conn


def _reconnect_with(conn, monkeypatch, data):
    """Make conn.connect() serve `data`, the way a real reconnect would."""

    def fake_connect():
        if conn._sock is None:
            conn._sock = _CannedSocket(data)
            conn._parser.on_connect(conn)

    monkeypatch.setattr(conn, "connect", fake_connect)


# A binary PUBLISH payload delivered to a decode_responses=True subscriber.
# Encoder.decode runs at the tail of _read_response, after the cursor has
# already passed the payload, so this raises UnicodeDecodeError mid-reply on
# both parser backends.
BINARY_PUBSUB_MESSAGE = b"*3\r\n$7\r\nmessage\r\n$4\r\nchan\r\n$3\r\n\xff\xfe\xfd\r\n"


@pytest.mark.fixed_client
class TestInvalidResponseInvalidatesConnection:
    """A framing violation must drop the connection even when the caller
    passed disconnect_on_error=False, otherwise the rewound bytes stay queued
    and every subsequent read fails identically. See #4291.
    """

    # The signature PubSub.parse_response uses.
    PUBSUB_KWARGS = dict(disconnect_on_error=False, push_request=True)

    def test_framing_error_disconnects_on_pubsub_path(self):
        conn = _connection_with_stream(b"?bogus\r\n+SECOND\r\n")

        with pytest.raises(redis.InvalidResponse):
            conn.read_response(**self.PUBSUB_KWARGS)

        assert conn.is_connected is False

    def test_pubsub_path_recovers_after_framing_error(self, monkeypatch):
        conn = _connection_with_stream(b"?bogus\r\n+SECOND\r\n")
        _reconnect_with(conn, monkeypatch, b"+RECOVERED\r\n")

        with pytest.raises(redis.InvalidResponse):
            conn.read_response(**self.PUBSUB_KWARGS)

        # PubSub.parse_response then calls conn.connect() before reading
        # again. On master the connection is still up, so connect() is a
        # no-op, the poisoned bytes are still queued, and this read raises
        # InvalidResponse again -- forever. After the fix connect() really
        # reconnects and the next read sees a clean stream.
        conn.connect()
        assert conn.read_response(**self.PUBSUB_KWARGS) == b"RECOVERED"

    def test_in_band_response_error_does_not_disconnect(self):
        """The rewind exists for in-band ResponseError; it must keep working."""
        conn = _connection_with_stream(b"-ERR in band\r\n+SECOND\r\n")

        with pytest.raises(redis.ResponseError):
            conn.read_response(**self.PUBSUB_KWARGS)

        assert conn.is_connected is True
        assert conn.read_response(**self.PUBSUB_KWARGS) == b"SECOND"

    def test_framing_error_still_disconnects_by_default(self):
        conn = _connection_with_stream(b"?bogus\r\n")

        with pytest.raises(redis.InvalidResponse):
            conn.read_response()

        assert conn.is_connected is False


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "parser_class",
    [_RESP2Parser, _HiredisParser],
    ids=["RESP2Parser", "HiredisParser"],
)
class TestBinaryPubSubPayloadInvalidatesConnection:
    """The realistic pubsub trigger is not an unknown type byte but
    Encoder.decode at the tail of _read_response: a decode_responses=True
    subscriber handed a binary PUBLISH payload raises UnicodeDecodeError after
    the cursor has passed the payload. Both parser backends raise it, and
    PubSub.parse_response passes disconnect_on_error=False, so before the
    widened predicate the connection stayed up with the undecodable bytes
    queued and every later read raised identically. See #4291.
    """

    PUBSUB_KWARGS = dict(disconnect_on_error=False, push_request=True)

    def _subscriber(self, data, parser_class):
        if parser_class is _HiredisParser and not HIREDIS_AVAILABLE:
            pytest.skip("Hiredis not available")
        return _connection_with_stream(
            data, parser_class, encoding="utf-8", decode_responses=True
        )

    def test_binary_payload_disconnects(self, parser_class):
        conn = self._subscriber(BINARY_PUBSUB_MESSAGE + b"+SECOND\r\n", parser_class)

        with pytest.raises(UnicodeDecodeError):
            conn.read_response(**self.PUBSUB_KWARGS)

        assert conn.is_connected is False

    def test_next_read_is_clean_after_binary_payload(self, parser_class, monkeypatch):
        conn = self._subscriber(BINARY_PUBSUB_MESSAGE + b"+SECOND\r\n", parser_class)
        _reconnect_with(conn, monkeypatch, b"+RECOVERED\r\n")

        with pytest.raises(UnicodeDecodeError):
            conn.read_response(**self.PUBSUB_KWARGS)

        # PubSub.parse_response calls conn.connect() before reading again.
        # Before the fix the connection was still up, connect() was a no-op,
        # and this read raised UnicodeDecodeError on the same queued bytes.
        conn.connect()
        assert conn.read_response(**self.PUBSUB_KWARGS) == "RECOVERED"

    def test_decodable_payload_leaves_connection_up(self, parser_class):
        """A payload that decodes cleanly must not trip the new predicate."""
        conn = self._subscriber(
            b"*3\r\n$7\r\nmessage\r\n$4\r\nchan\r\n$2\r\nhi\r\n", parser_class
        )

        assert conn.read_response(**self.PUBSUB_KWARGS) == ["message", "chan", "hi"]
        assert conn.is_connected is True


def _deeply_nested_reply(depth):
    # Matches what Redis emits for
    #   EVAL "local t={} local c=t for i=1,3000 do local n={} c[1]=n c=n end return t" 0
    # which is an ordinary command reply, not a synthetic stream (#4291).
    return b"*1\r\n" * depth + b"*0\r\n"


# hiredis-py 3.4.1 is the first release whose bundled hiredis limits reply
# nesting depth (to 1024); earlier releases parse a reply of any depth.
HIREDIS_LIMITS_NESTING_DEPTH = HIREDIS_AVAILABLE and Version(
    metadata.version("hiredis")
) >= Version("3.4.1")


@pytest.mark.fixed_client
class TestDeeplyNestedReplyInvalidatesConnection:
    """Deeply nested aggregate replies fail mid-parse: the pure-Python parsers
    exhaust the stack (RecursionError) and hiredis >= 3.4.1 hits its own nesting
    limit (InvalidResponse). Both are unrecoverable by re-parsing, so both must drop
    the connection even under disconnect_on_error=False. #4144 converts the
    pure-Python case into a bounded-depth InvalidResponse; catching
    RecursionError here is what stops the loop until that lands.
    """

    PUBSUB_KWARGS = dict(disconnect_on_error=False, push_request=True)

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="PyPy bounds recursion by stack bytes; unwinding the overflow can "
        "abort the interpreter (#4144 replaces the RecursionError)",
    )
    def test_python_parser_recursion_error_disconnects(self):
        # 3000 levels exceed CPython's recursion limit. PyPy, which would need far
        # more, is skipped: unwinding the RecursionError from a native stack
        # overflow through its JIT frames intermittently aborts the interpreter.
        conn = _connection_with_stream(_deeply_nested_reply(3000), _RESP2Parser)

        # A GC pass at the recursion limit would run finalizers of garbage left
        # by earlier tests with no stack left to report their errors, so collect
        # first and keep GC off during the deep parse.
        gc.collect()
        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            with pytest.raises(RecursionError):
                conn.read_response(**self.PUBSUB_KWARGS)
        finally:
            if gc_was_enabled:
                gc.enable()

        assert conn.is_connected is False

    @pytest.mark.skipif(
        not HIREDIS_LIMITS_NESTING_DEPTH,
        reason="requires hiredis >= 3.4.1, which limits reply nesting depth",
    )
    def test_hiredis_nesting_limit_disconnects(self):
        conn = _connection_with_stream(_deeply_nested_reply(3000), _HiredisParser)

        with pytest.raises(redis.InvalidResponse):
            conn.read_response(**self.PUBSUB_KWARGS)

        assert conn.is_connected is False

    def test_shallow_nesting_still_parses(self):
        conn = _connection_with_stream(_deeply_nested_reply(3), _RESP2Parser)

        assert conn.read_response(**self.PUBSUB_KWARGS) == [[[[]]]]
        assert conn.is_connected is True


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "parser_class",
    [
        _RESP2Parser,
        _RESP3Parser,
        pytest.param(
            _HiredisParser,
            marks=pytest.mark.skipif(
                not HIREDIS_AVAILABLE, reason="hiredis is not installed"
            ),
        ),
    ],
    ids=["RESP2Parser", "RESP3Parser", "HiredisParser"],
)
class TestMalformedNumericFrameInvalidatesConnection:
    """Malformed numeric frames (non-numeric integers, bulk lengths, or array
    lengths such as `:abc\r\n`, `$xyz\r\n`, `*abc\r\n`) are protocol errors, so the
    parser raises InvalidResponse. Before the fix, these stayed queued with
    disconnect_on_error=False, causing infinite retries on the same frame.
    See #4291.
    """

    PUBSUB_KWARGS = dict(disconnect_on_error=False, push_request=True)

    def test_malformed_integer_frame_disconnects(self, parser_class):
        """Malformed integer frame `:abc\r\n` raises InvalidResponse."""
        conn = _connection_with_stream(b":abc\r\n+SECOND\r\n", parser_class)

        with pytest.raises(InvalidResponse):
            conn.read_response(**self.PUBSUB_KWARGS)

        assert conn.is_connected is False

    def test_malformed_bulk_length_disconnects(self, parser_class):
        """Malformed bulk string length `$xyz\r\n` raises InvalidResponse."""
        conn = _connection_with_stream(b"$xyz\r\n+SECOND\r\n", parser_class)

        with pytest.raises(InvalidResponse):
            conn.read_response(**self.PUBSUB_KWARGS)

        assert conn.is_connected is False

    def test_malformed_array_length_disconnects(self, parser_class):
        """Malformed array length `*abc\r\n` raises InvalidResponse."""
        conn = _connection_with_stream(b"*abc\r\n+SECOND\r\n", parser_class)

        with pytest.raises(InvalidResponse):
            conn.read_response(**self.PUBSUB_KWARGS)

        assert conn.is_connected is False

    def test_next_read_is_clean_after_malformed_integer(
        self, parser_class, monkeypatch
    ):
        """Reconnect after malformed integer frame serves new stream cleanly."""
        conn = _connection_with_stream(b":abc\r\n+SECOND\r\n", parser_class)
        _reconnect_with(conn, monkeypatch, b"+RECOVERED\r\n")

        with pytest.raises(InvalidResponse):
            conn.read_response(**self.PUBSUB_KWARGS)

        conn.connect()
        assert conn.read_response(**self.PUBSUB_KWARGS) == b"RECOVERED"

    def test_valid_integer_frame_leaves_connection_up(self, parser_class):
        """Valid integer frames must not trigger the new predicate."""
        conn = _connection_with_stream(b":42\r\n", parser_class)

        assert conn.read_response(**self.PUBSUB_KWARGS) == 42
        assert conn.is_connected is True


@pytest.mark.fixed_client
@pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="hiredis is not installed")
class TestPushHandlerValueErrorKeepsConnection:
    """A ValueError raised by the *push handler* is not a framing error: hiredis
    has already consumed the whole push frame before the handler runs. With
    disconnect_on_error=False the connection must stay up and the next queued
    frame must remain readable. Guards against widening
    UNRECOVERABLE_PARSE_ERRORS to plain ValueError.
    """

    def test_push_handler_value_error_does_not_disconnect(self):
        conn = Connection(protocol=3, parser_class=_HiredisParser)
        conn._sock = _CannedSocket(b">2\r\n$7\r\nmessage\r\n$5\r\nhello\r\n+SECOND\r\n")
        conn._parser.on_connect(conn)

        def boom(response):
            raise ValueError("handler bug")

        conn._parser.pubsub_push_handler_func = boom

        with pytest.raises(ValueError, match="handler bug"):
            conn.read_response(disconnect_on_error=False, push_request=True)

        assert conn.is_connected is True
        assert conn.read_response(disconnect_on_error=False) == b"SECOND"


class _DummyConnection:
    """Minimal connection stub for pool metric tests (no real socket)."""

    description_format = "DummyConnection<>"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()
        self._sock = None

    def connect(self):
        self._sock = MagicMock()

    def disconnect(self):
        self._sock = None

    def can_read(self):
        return False

    def should_reconnect(self):
        return False

    def re_auth(self):
        pass


def _pool_metric_calls(mock_fn, pool_name):
    """Extract (state, delta) tuples from record_connection_count calls for a pool.

    Filters by pool_name to avoid interference from GC of other pools.
    """
    result = []
    for c in mock_fn.call_args_list:
        p = c.kwargs.get("pool_name", c.args[0] if c.args else None)
        if p != pool_name:
            continue
        state = c.kwargs.get("connection_state", c.args[1] if len(c.args) > 1 else None)
        counter = c.kwargs.get("counter", c.args[2] if len(c.args) > 2 else 1)
        result.append((state, counter))
    return result


def _net(calls):
    """Return (idle_net, used_net) from a list of (state, delta) tuples."""
    idle = sum(d for s, d in calls if s == ConnectionState.IDLE)
    used = sum(d for s, d in calls if s == ConnectionState.USED)
    return idle, used


@pytest.mark.fixed_client
@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
@patch("redis.connection.record_connection_count")
def test_same_process_pool_reset_balances_connection_counts(mock_rec, pool_class):
    pool = pool_class(connection_class=_DummyConnection, max_connections=2)
    pool_name = get_pool_name(pool)
    idle = pool.get_connection()
    pool.get_connection()
    pool.release(idle)
    assert _net(_pool_metric_calls(mock_rec, pool_name)) == (1, 1)

    pool.reset()
    assert get_pool_name(pool) == pool_name
    assert _net(_pool_metric_calls(mock_rec, pool_name)) == (0, 0)
    pool.reset()
    assert _net(_pool_metric_calls(mock_rec, pool_name)) == (0, 0)

    connection = pool.get_connection()
    pool.release(connection)
    pool_ref = weakref.ref(pool)
    del pool
    gc.collect()
    assert pool_ref() is None
    assert _net(_pool_metric_calls(mock_rec, pool_name)) == (0, 0)


@pytest.mark.fixed_client
class TestConnectionPoolMetricCount:
    """Tests for db.client.connection.count UpDownCounter accuracy.

    Verifies that get_connection / release produce balanced IDLE and USED
    counter updates across ConnectionPool and BlockingConnectionPool.
    """

    @patch("redis.connection.record_connection_count")
    def test_new_connection_records_only_used(self, mock_rec):
        """A new connection should record USED +1 only (never was idle)."""
        pool = ConnectionPool(connection_class=_DummyConnection, max_connections=10)
        pn = get_pool_name(pool)
        mock_rec.reset_mock()

        conn = pool.get_connection()

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert idle_net == 0, f"New conn should not touch IDLE, got {idle_net}"
        assert used_net == 1
        pool.release(conn)

    @patch("redis.connection.record_connection_count")
    def test_reused_connection_transitions_idle_to_used(self, mock_rec):
        """A reused connection should record IDLE -1, USED +1."""
        pool = ConnectionPool(connection_class=_DummyConnection, max_connections=10)
        pn = get_pool_name(pool)
        conn = pool.get_connection()
        pool.release(conn)
        mock_rec.reset_mock()

        conn2 = pool.get_connection()
        assert conn2 is conn

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert idle_net == -1
        assert used_net == 1
        pool.release(conn2)

    @patch("redis.connection.record_connection_count")
    def test_full_lifecycle_nets_to_zero(self, mock_rec):
        """create -> use -> release -> reuse -> release -> destroy = net 0."""
        pool = ConnectionPool(connection_class=_DummyConnection, max_connections=10)
        pn = get_pool_name(pool)
        mock_rec.reset_mock()

        conn = pool.get_connection()
        pool.release(conn)
        conn = pool.get_connection()
        pool.release(conn)

        # Destroy the pool's connections through the pool's own teardown path.
        pool.reset()

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert idle_net == 0, f"Lifecycle IDLE should net 0, got {idle_net}"
        assert used_net == 0, f"Lifecycle USED should net 0, got {used_net}"

    @patch("redis.connection.record_connection_count")
    def test_release_unowned_does_not_record(self, mock_rec):
        """release() of a connection the pool no longer owns (e.g. inherited by
        a forked child) must not record connection.count. Accounting for inherited
        connections belongs to the parent process."""
        pool = ConnectionPool(connection_class=_DummyConnection, max_connections=10)

        conn = pool.get_connection()
        mock_rec.reset_mock()
        conn.pid = -1  # simulate a connection inherited across a fork
        pool.release(conn)

        assert mock_rec.call_args_list == [], (
            "unowned release must not record connection.count"
        )

    @patch("redis.connection.record_connection_count")
    def test_release_rejected_same_pid_decrements_used(self, mock_rec):
        """A connection checked out by this process but rejected by
        owns_connection() (e.g. SentinelConnectionPool after a master failover)
        must still decrement USED."""
        pool = ConnectionPool(connection_class=_DummyConnection, max_connections=10)
        pn = get_pool_name(pool)

        conn = pool.get_connection()
        mock_rec.reset_mock()
        with patch.object(pool, "owns_connection", return_value=False):
            pool.release(conn)

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert used_net == -1, f"USED must be decremented, got {used_net}"
        assert idle_net == 0, f"IDLE must not increase for dropped conn, got {idle_net}"


@pytest.mark.fixed_client
class TestBlockingConnectionPoolMetricCount:
    """Same metric-count tests for BlockingConnectionPool."""

    def _pool(self):
        return BlockingConnectionPool(
            connection_class=_DummyConnection,
            max_connections=10,
            timeout=0.1,
        )

    @patch("redis.connection.record_connection_count")
    def test_new_connection_records_only_used(self, mock_rec):
        pool = self._pool()
        pn = get_pool_name(pool)
        mock_rec.reset_mock()

        conn = pool.get_connection()

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert idle_net == 0
        assert used_net == 1
        pool.release(conn)

    @patch("redis.connection.record_connection_count")
    def test_release_unowned_does_not_record(self, mock_rec):
        """Unowned (inherited-across-fork) release must not record; accounting
        for inherited connections belongs to the parent process."""
        pool = self._pool()
        mock_rec.reset_mock()

        conn = pool.get_connection()
        mock_rec.reset_mock()
        conn.pid = -1
        pool.release(conn)

        assert mock_rec.call_args_list == [], (
            "unowned release must not record connection.count"
        )

    @patch("redis.connection.record_connection_count")
    def test_release_rejected_same_pid_decrements_used(self, mock_rec):
        """A connection checked out by this process but rejected by
        owns_connection() must decrement USED and be removed from _connections
        so a later reset() does not decrement USED again."""
        pool = self._pool()
        pn = get_pool_name(pool)

        conn = pool.get_connection()
        mock_rec.reset_mock()
        with patch.object(pool, "owns_connection", return_value=False):
            pool.release(conn)

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert used_net == -1, f"USED must be decremented, got {used_net}"
        assert idle_net == 0, f"IDLE must not increase for dropped conn, got {idle_net}"
        assert conn not in pool._connections

        mock_rec.reset_mock()
        pool.reset()

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert used_net == 0, f"reset() must not decrement USED again, got {used_net}"
        assert idle_net == 0, f"reset() must not touch IDLE, got {idle_net}"


@pytest.mark.parametrize("raise_on_timeout", [True, False], ids=["read", "poll"])
def test_hiredis_read_from_socket_os_level_timeout_is_a_connection_error(
    raise_on_timeout,
):
    # socket.timeout is the builtin TimeoutError since Python 3.10, so an
    # ETIMEDOUT the OS raises on a dead socket (e.g. after a network change)
    # lands in the parser's socket.timeout handler. It used to be reported as a
    # read timeout - or, when polling with raise_on_timeout=False, as "no data",
    # which hid the broken connection. The connection is dead: ConnectionError.
    parser = make_hiredis_parser()
    parser._sock.recv_into.side_effect = OSError(ETIMEDOUT, "Operation timed out")

    with pytest.raises(ConnectionError):
        parser.read_from_socket(timeout=1, raise_on_timeout=raise_on_timeout)


@pytest.mark.parametrize("raise_on_timeout", [True, False], ids=["read", "poll"])
def test_socket_buffer_os_level_timeout_is_a_connection_error(raise_on_timeout):
    sock = Mock()
    sock.recv.side_effect = OSError(ETIMEDOUT, "Operation timed out")
    buf = SocketBuffer(sock, 65536, 1)

    with pytest.raises(ConnectionError):
        buf._read_from_socket(timeout=1, raise_on_timeout=raise_on_timeout)


def test_send_packed_command_os_level_timeout_is_a_connection_error():
    conn = Connection(socket_timeout=1)
    conn._sock = Mock()
    conn._sock.sendall.side_effect = OSError(ETIMEDOUT, "Operation timed out")
    conn.disconnect = Mock()

    with pytest.raises(ConnectionError):
        conn.send_packed_command(b"PING", check_health=False)
    conn.disconnect.assert_called_once()
