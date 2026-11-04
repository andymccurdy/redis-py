import asyncio
import gc
import math
import os
import platform
import socket
import ssl
import types
import weakref
from unittest import mock
from errno import ECONNREFUSED, ETIMEDOUT
from unittest.mock import patch

import pytest
import redis
from redis._parsers import (
    _AsyncHiredisParser,
    _AsyncRESP2Parser,
    _AsyncRESP3Parser,
    _AsyncRESPBase,
)
from redis._parsers.hiredis import NOT_ENOUGH_DATA
from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.connection import (
    BlockingConnectionPool,
    Connection,
    HiredisRespSerializer,
    SSLConnection,
    UnixDomainSocketConnection,
    parse_url,
)
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import (
    ConnectionError,
    InvalidResponse,
    ResponseError,
    TimeoutError,
)
from redis.observability.attributes import ConnectionState, get_pool_name
from redis.utils import HIREDIS_AVAILABLE
from tests.conftest import skip_if_server_version_lt

from .mocks import MockStream


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


class DummyAsyncStream:
    def __init__(self, buffer=b"", eof=False, exception=None):
        self._buffer = bytearray(buffer)
        self.eof = eof
        self._exception = exception
        self.read_called = False

    def at_eof(self):
        return self.eof and not self._buffer

    def exception(self):
        return self._exception

    async def read(self, _):
        self.read_called = True
        raise AssertionError("can_read should not read from the stream")

    async def readline(self):
        self.read_called = True
        data = bytes(self._buffer)
        self._buffer.clear()
        return data


def make_async_hiredis_parser(
    stream, response=NOT_ENOUGH_DATA, decoded_response=None, has_data=False
):
    parser = _AsyncHiredisParser.__new__(_AsyncHiredisParser)
    parser._connected = True
    parser._reader = DummyHiredisReader(response, decoded_response, has_data)
    parser._stream = stream
    parser._hiredis_PushNotificationType = None
    return parser


@pytest.mark.fixed_client
def test_connection_default_parser_matches_default_protocol():
    conn = Connection()
    expected_parser_class = (
        _AsyncHiredisParser if HIREDIS_AVAILABLE else _AsyncRESP3Parser
    )
    assert isinstance(conn._parser, expected_parser_class)
    assert conn.protocol == 3


@pytest.mark.fixed_client
@pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="hiredis is not installed")
def test_connection_uses_hiredis_command_packer(monkeypatch):
    calls = []

    def pack_command(args):
        calls.append(args)
        return b"packed"

    monkeypatch.setattr("redis.connection.hiredis.pack_command", pack_command)
    connection = Connection()

    assert isinstance(connection._command_packer, HiredisRespSerializer)
    assert connection.pack_command("SET", "key", "value") == [b"packed"]
    assert calls == [(b"SET", b"key", b"value")]


@pytest.mark.fixed_client
def test_connection_uses_python_command_packer_without_hiredis(monkeypatch):
    monkeypatch.setattr("redis.asyncio.connection.HIREDIS_AVAILABLE", False)
    connection = Connection()

    assert connection._command_packer is None
    assert connection.pack_command("PING") == [b"*1\r\n$4\r\nPING\r\n"]


@pytest.mark.fixed_client
@pytest.mark.parametrize("protocol", [2, 3])
def test_async_pack_command_respects_connection_encoding(protocol):
    connection = Connection(encoding="latin-1", protocol=protocol)

    assert connection.pack_command("SET", "key", "café") == [
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$4\r\ncaf\xe9\r\n"
    ]


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    ("buffer", "expected"),
    [
        (b"", False),
        (b"+OK\r\n", True),
    ],
)
async def test_async_hiredis_can_read_uses_buffer_without_reading(buffer, expected):
    stream = DummyAsyncStream(buffer=buffer)
    parser = make_async_hiredis_parser(stream)

    assert await parser.can_read() is expected
    assert stream.read_called is False


@pytest.mark.fixed_client
async def test_async_hiredis_can_read_raises_on_eof():
    # a server-closed connection must raise like the sync SocketBuffer does,
    # not report readable data (#4252)
    stream = DummyAsyncStream(eof=True)
    parser = make_async_hiredis_parser(stream)

    with pytest.raises(ConnectionError):
        await parser.can_read()
    assert stream.read_called is False


@pytest.mark.fixed_client
@pytest.mark.parametrize("buffer", [b"", b"+OK\r\n"])
async def test_async_hiredis_can_read_raises_on_reset(buffer):
    # a reset (RST) leaves no EOF but an exception on the stream, which
    # asyncio raises before any buffered data, so it must raise like EOF
    stream = DummyAsyncStream(buffer=buffer, exception=ConnectionResetError())
    parser = make_async_hiredis_parser(stream)

    with pytest.raises(ConnectionError):
        await parser.can_read()
    assert stream.read_called is False


@pytest.mark.fixed_client
async def test_async_hiredis_can_read_prefers_buffered_data_over_eof():
    # a reply or push notification the reader already buffered must stay
    # readable even if the server has since closed the connection,
    # matching the sync parser's has_data-first ordering
    stream = DummyAsyncStream(eof=True)
    parser = make_async_hiredis_parser(stream, response=b"OK", has_data=True)

    assert await parser.can_read() is True
    assert await parser.read_response() == b"OK"
    assert stream.read_called is False


@pytest.mark.fixed_client
async def test_async_hiredis_can_read_detects_reader_response():
    stream = DummyAsyncStream()
    parser = make_async_hiredis_parser(stream, response=b"OK", has_data=True)

    assert await parser.can_read() is True
    assert stream.read_called is False


@pytest.mark.fixed_client
async def test_async_hiredis_can_read_detects_real_stream_reader_buffer():
    payload = b"+OK\r\n"
    stream = asyncio.StreamReader()
    stream.feed_data(payload)
    parser = make_async_hiredis_parser(stream)

    assert await parser.can_read() is True
    assert await stream.read(len(payload)) == payload


@pytest.mark.fixed_client
async def test_async_hiredis_can_read_preserves_reader_response():
    stream = DummyAsyncStream()
    parser = make_async_hiredis_parser(stream, response=b"OK", has_data=True)

    assert await parser.can_read() is True
    assert await parser.read_response() == b"OK"
    assert stream.read_called is False


@pytest.mark.fixed_client
async def test_async_hiredis_can_read_does_not_decide_disable_decoding():
    stream = DummyAsyncStream()
    raw = b"\xe2\x98\x83"
    parser = make_async_hiredis_parser(
        stream,
        response=raw,
        decoded_response=raw.decode(),
        has_data=True,
    )

    assert await parser.can_read() is True
    assert await parser.read_response(disable_decoding=True) == raw


@pytest.mark.fixed_client
async def test_async_hiredis_can_read_leaves_decoding_to_read_response():
    stream = DummyAsyncStream()
    raw = b"\xe2\x98\x83"
    parser = make_async_hiredis_parser(
        stream,
        response=raw,
        decoded_response=raw.decode(),
        has_data=True,
    )

    assert await parser.can_read() is True
    assert await parser.read_response() == raw.decode()


@pytest.mark.fixed_client
@pytest.mark.parametrize("parser_class", [_AsyncRESP2Parser, _AsyncRESP3Parser])
async def test_async_resp_can_read_detects_stream_buffer(parser_class):
    stream = DummyAsyncStream(buffer=b"+OK\r\n")
    parser = parser_class(socket_read_size=65536)
    parser._connected = True
    parser._stream = stream

    assert await parser.can_read() is True
    assert stream.read_called is False


@pytest.mark.fixed_client
@pytest.mark.parametrize("parser_class", [_AsyncRESP2Parser, _AsyncRESP3Parser])
async def test_async_resp_can_read_raises_on_eof(parser_class):
    # a server-closed connection must raise like the sync SocketBuffer does,
    # not report readable data (#4252)
    stream = DummyAsyncStream(eof=True)
    parser = parser_class(socket_read_size=65536)
    parser._connected = True
    parser._stream = stream

    with pytest.raises(ConnectionError):
        await parser.can_read()
    assert stream.read_called is False


@pytest.mark.fixed_client
@pytest.mark.parametrize("buffer", [b"", b"+OK\r\n"])
@pytest.mark.parametrize("parser_class", [_AsyncRESP2Parser, _AsyncRESP3Parser])
async def test_async_resp_can_read_raises_on_reset(parser_class, buffer):
    # a reset (RST) leaves no EOF but an exception on the stream, which
    # asyncio raises before any buffered data, so it must raise like EOF
    stream = DummyAsyncStream(buffer=buffer, exception=ConnectionResetError())
    parser = parser_class(socket_read_size=65536)
    parser._connected = True
    parser._stream = stream

    with pytest.raises(ConnectionError):
        await parser.can_read()
    assert stream.read_called is False


@pytest.mark.fixed_client
@pytest.mark.parametrize("parser_class", [_AsyncRESP2Parser, _AsyncRESP3Parser])
async def test_async_resp_can_read_prefers_buffered_data_over_eof(parser_class):
    # data the parser already buffered must stay readable even if the server
    # has since closed the connection, matching the sync SocketBuffer's
    # buffer-first ordering
    stream = DummyAsyncStream(eof=True)
    parser = parser_class(socket_read_size=65536)
    parser._connected = True
    parser._stream = stream
    parser._buffer = b"+OK\r\n"

    assert await parser.can_read() is True
    assert stream.read_called is False


@pytest.mark.fixed_client
@pytest.mark.parametrize("parser_class", [_AsyncRESP2Parser, _AsyncRESP3Parser])
async def test_async_resp_read_response_raises_after_disconnect(parser_class):
    # A late reply read after disconnect could be assigned to the next
    # cluster pipeline command.
    stream = DummyAsyncStream(buffer=b"+OK\r\n")
    parser = parser_class(socket_read_size=65536)
    parser.on_connect(types.SimpleNamespace(_reader=stream, encoder=mock.Mock()))
    parser.on_disconnect()

    with pytest.raises(ConnectionError):
        await parser.read_response()
    assert stream.read_called is False


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "parser_class",
    [_AsyncRESP2Parser, _AsyncRESP3Parser, _AsyncHiredisParser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser", "AsyncHiredisParser"],
)
async def test_async_parser_read_guards_report_a_never_connected_parser(parser_class):
    # The read guards consult _connected, which used to be assigned only by
    # on_connect() / on_disconnect(). A parser that has never connected - the
    # one a fresh Connection owns before connect() - therefore raised
    # AttributeError from those guards instead of the retryable ConnectionError,
    # so no retry or failover layer acted on it. can_read() raises OSError,
    # which Connection.can_read() converts to ConnectionError.
    if parser_class is _AsyncHiredisParser and not HIREDIS_AVAILABLE:
        pytest.skip("Hiredis not available")

    parser = parser_class(socket_read_size=65536)

    assert parser._connected is False
    with pytest.raises(ConnectionError):
        await parser.read_response()
    with pytest.raises(OSError):
        await parser.can_read()


@pytest.mark.fixed_client
async def test_connection_read_guards_report_a_never_connected_connection():
    # End-to-end counterpart: the AttributeError also sailed past
    # Connection.can_read()'s ``except OSError``, so neither the ConnectionError
    # conversion nor the disconnect() it performs happened.
    conn = Connection()

    with pytest.raises(ConnectionError):
        await conn.read_response()
    with pytest.raises(ConnectionError):
        await conn.can_read()


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    ("protocol", "parser_class", "expected_parser_class"),
    [
        (None, _AsyncRESP2Parser, _AsyncRESP3Parser),
        (3, _AsyncRESP2Parser, _AsyncRESP3Parser),
        (2, _AsyncRESP3Parser, _AsyncRESP2Parser),
        (2, _AsyncRESP2Parser, _AsyncRESP2Parser),
        (3, _AsyncRESP3Parser, _AsyncRESP3Parser),
    ],
)
def test_connection_parser_matches_protocol(
    protocol, parser_class, expected_parser_class
):
    kwargs = {"parser_class": parser_class}
    if protocol is not None:
        kwargs["protocol"] = protocol
    conn = Connection(**kwargs)
    assert isinstance(conn._parser, expected_parser_class)


@pytest.mark.fixed_client
def test_get_resolved_ip_uses_async_writer_peer_before_dns():
    conn = Connection(host="redis.example.test")
    writer = mock.Mock()
    writer.get_extra_info.return_value = ("10.0.0.7", 6379)
    conn._writer = writer

    with mock.patch.object(socket, "getaddrinfo") as getaddrinfo:
        try:
            assert conn.get_resolved_ip() == "10.0.0.7"
        finally:
            conn._writer = None

    getaddrinfo.assert_not_called()


@pytest.mark.fixed_client
async def test_send_packed_command_rejects_closed_transport():
    conn = Connection(health_check_interval=0)
    writer = mock.Mock()
    writer.transport.is_closing.return_value = True
    writer.drain = mock.AsyncMock()
    conn._reader = mock.Mock()
    conn._writer = writer

    with pytest.raises(ConnectionError, match="closed") as exc_info:
        await conn.send_packed_command(b"PING", check_health=False)

    assert type(exc_info.value) is ConnectionError
    writer.writelines.assert_not_called()
    writer.drain.assert_not_awaited()
    writer.close.assert_called_once()
    assert not conn.is_connected


@pytest.mark.fixed_client
@pytest.mark.parametrize("socket_timeout", [None, 1])
async def test_send_packed_command_writes_to_open_transport(socket_timeout):
    conn = Connection(socket_timeout=socket_timeout, health_check_interval=0)
    writer = mock.Mock()
    writer.transport.is_closing.return_value = False
    writer.drain = mock.AsyncMock()
    conn._reader = mock.Mock()
    conn._writer = writer

    await conn.send_packed_command(b"PING", check_health=False)

    writer.transport.is_closing.assert_called_once_with()
    writer.writelines.assert_called_once_with([b"PING"])
    writer.drain.assert_awaited_once_with()
    await conn.disconnect(nowait=True)


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    ("error_type", "message"),
    [
        (TypeError, "'NoneType' object is not callable"),
        (AttributeError, "'NoneType' object has no attribute '_add_writer'"),
    ],
)
async def test_send_packed_command_translates_closed_transport_error(
    error_type, message
):
    conn = Connection(health_check_interval=0)
    writer = mock.Mock()
    writer.transport.is_closing.side_effect = [False, True]
    error = error_type(message)
    writer.writelines.side_effect = error
    writer.drain = mock.AsyncMock()
    conn._reader = mock.Mock()
    conn._writer = writer

    with pytest.raises(ConnectionError) as exc_info:
        await conn.send_packed_command(b"PING", check_health=False)

    assert type(exc_info.value) is ConnectionError
    assert str(exc_info.value) == "Connection closed by the server while writing"
    assert exc_info.value.__cause__ is error
    assert writer.transport.is_closing.call_count == 2
    writer.drain.assert_not_awaited()
    writer.close.assert_called_once()
    assert not conn.is_connected


@pytest.mark.fixed_client
async def test_send_packed_command_preserves_type_error_on_open_transport():
    conn = Connection(health_check_interval=0)
    writer = mock.Mock()
    writer.transport.is_closing.return_value = False
    error = TypeError("sequence item 0: expected a bytes-like object")
    writer.writelines.side_effect = error
    writer.drain = mock.AsyncMock()
    conn._reader = mock.Mock()
    conn._writer = writer

    with pytest.raises(TypeError) as exc_info:
        await conn.send_packed_command(b"PING", check_health=False)

    assert exc_info.value is error
    assert writer.transport.is_closing.call_count == 2
    writer.drain.assert_not_awaited()
    writer.close.assert_called_once()
    assert not conn.is_connected


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "client_kwargs",
    [
        {"driver_info": None},
        {"lib_name": None, "lib_version": None},
    ],
)
async def test_redis_client_preserves_explicit_none_driver_info(client_kwargs):
    if "lib_name" in client_kwargs:
        with pytest.warns(DeprecationWarning):
            client = Redis(**client_kwargs)
    else:
        client = Redis(**client_kwargs)

    assert client.connection_pool.connection_kwargs["driver_info"] is None
    await client.aclose()


@pytest.mark.fixed_client
async def test_redis_client_default_driver_info():
    client = Redis()
    driver_info = client.connection_pool.connection_kwargs["driver_info"]

    assert driver_info.formatted_name == "redis-py"
    assert driver_info.lib_version is not None
    await client.aclose()


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "connection_kwargs",
    [
        {"driver_info": None},
        {"lib_name": None, "lib_version": None},
    ],
)
async def test_client_setinfo_skipped_with_explicit_none(connection_kwargs):
    if "lib_name" in connection_kwargs:
        with pytest.warns(DeprecationWarning):
            conn = Connection(protocol=2, **connection_kwargs)
    else:
        conn = Connection(protocol=2, **connection_kwargs)
    conn._parser.on_connect = mock.Mock()
    conn.send_command = mock.AsyncMock()
    conn.read_response = mock.AsyncMock(return_value="OK")

    await conn.on_connect_check_health()

    assert conn.driver_info is None
    conn.send_command.assert_not_awaited()
    conn.read_response.assert_not_awaited()


@pytest.mark.onlynoncluster
async def test_invalid_response(create_redis):
    r = await create_redis(single_connection_client=True)

    raw = b"x"
    fake_stream = MockStream(raw + b"\r\n")

    parser: _AsyncRESPBase = r.connection._parser

    if isinstance(parser, _AsyncRESPBase):
        exp_err = f"Protocol Error: {raw!r}"
    else:
        exp_err = f'Protocol error, got "{raw.decode()}" as reply type byte'

    with mock.patch.object(parser, "_stream", fake_stream):
        with pytest.raises(InvalidResponse, match=exp_err):
            await parser.read_response()

    await r.connection.disconnect()


@pytest.mark.fixed_client
async def test_single_connection():
    """Test that concurrent requests on a single client are synchronised."""
    r = Redis(single_connection_client=True)

    init_call_count = 0
    command_call_count = 0
    in_use = False

    class Retry_:
        async def call_with_retry(self, _, __, with_failure_count=False):
            # If we remove the single-client lock, this error gets raised as two
            # coroutines will be vying for the `in_use` flag due to the two
            # asymmetric sleep calls
            nonlocal command_call_count
            nonlocal in_use
            if in_use is True:
                raise ValueError("Commands should be executed one at a time.")
            in_use = True
            await asyncio.sleep(0.01)
            command_call_count += 1
            await asyncio.sleep(0.03)
            in_use = False
            return "foo"

    mock_conn = mock.AsyncMock(spec=Connection)
    mock_conn.retry = Retry_()
    mock_conn.host = "localhost"
    mock_conn.port = 6379

    async def get_conn():
        # Validate only one client is created in single-client mode when
        # concurrent requests are made
        nonlocal init_call_count
        await asyncio.sleep(0.01)
        init_call_count += 1
        return mock_conn

    with mock.patch.object(r.connection_pool, "get_connection", get_conn):
        with mock.patch.object(r.connection_pool, "release"):
            await asyncio.gather(r.set("a", "b"), r.set("c", "d"))

    assert init_call_count == 1
    assert command_call_count == 2
    r.connection = None  # it was a Mock
    await r.aclose()


@skip_if_server_version_lt("4.0.0")
@pytest.mark.redismod
@pytest.mark.onlynoncluster
async def test_loading_external_modules(r):
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


async def test_socket_param_regression(r):
    """A regression test for issue #1060"""
    conn = UnixDomainSocketConnection()
    _ = await conn.disconnect() is True


async def test_unix_domain_socket_connect_runs_handshake_once():
    """connect() runs the handshake, so _connect() must not run it as well"""
    conn = UnixDomainSocketConnection(path="/tmp/redis.sock")
    with (
        patch(
            "asyncio.open_unix_connection",
            mock.AsyncMock(return_value=(mock.Mock(), mock.Mock())),
        ),
        patch.object(conn, "on_connect_check_health", mock.AsyncMock()) as handshake,
    ):
        await conn.connect()
    handshake.assert_awaited_once()


async def test_can_run_concurrent_commands(r):
    if getattr(r, "connection", None) is not None:
        # Concurrent commands are only supported on pooled or cluster connections
        # since there is no synchronization on a single connection.
        pytest.skip("pool only")
    assert await r.ping() is True
    assert all(await asyncio.gather(*(r.ping() for _ in range(10))))


@pytest.mark.fixed_client
async def test_connect_retry_on_timeout_error(connect_args):
    """Test that the _connect function is retried in case of a timeout"""
    conn = Connection(
        retry_on_timeout=True, retry=Retry(NoBackoff(), 3), **connect_args
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
    await conn.disconnect()


@pytest.mark.fixed_client
async def test_connect_without_retry_on_non_retryable_error():
    """
    Test that the _connect function is not being retried in case of a CancelledError -
    error that is not in the list of retry-able errors"""
    with patch.object(Connection, "_connect") as _connect:
        _connect.side_effect = asyncio.CancelledError("")
        conn = Connection(retry_on_timeout=True, retry=Retry(NoBackoff(), 2))
        with pytest.raises(asyncio.CancelledError):
            await conn.connect()
        assert _connect.call_count == 1


@pytest.mark.fixed_client
async def test_connect_with_retries():
    """
    Test that retries occur for the entire connect+handshake flow when OSError happens during the handshake phase.
    """
    with patch.object(asyncio.StreamWriter, "writelines") as writelines:
        writelines.side_effect = OSError(ECONNREFUSED)
        conn = Connection(retry_on_timeout=True, retry=Retry(NoBackoff(), 2))
        with pytest.raises(ConnectionError):
            await conn.connect()
        # the handshake commands are the failing ones
        # validate that we don't execute too many commands on each retry
        # 3 retries --> 3 commands
        assert writelines.call_count == 3


@pytest.mark.fixed_client
async def test_connect_timeout_error_without_retry():
    """Test that the _connect function is not being retried if retry_on_timeout is
    set to False"""
    conn = Connection(retry_on_timeout=False)
    conn._connect = mock.AsyncMock()
    conn._connect.side_effect = socket.timeout

    with pytest.raises(TimeoutError, match="Timeout connecting to server"):
        await conn.connect()
    assert conn._connect.call_count == 1


@pytest.mark.onlynoncluster
async def test_connection_parse_response_resume(r: redis.Redis):
    """
    This test verifies that the Connection parser,
    be that PythonParser or HiredisParser,
    can be interrupted at IO time and then resume parsing.
    """
    conn = Connection(**r.connection_pool.connection_kwargs)
    await conn.connect()
    message = (
        b"*3\r\n$7\r\nmessage\r\n$8\r\nchannel1\r\n"
        b"$25\r\nhi\r\nthere\r\n+how\r\nare\r\nyou\r\n"
    )

    conn._parser._stream = MockStream(message, interrupt_every=2)
    for i in range(100):
        try:
            response = await conn.read_response(disconnect_on_error=False)
            break
        except MockStream.TestError:
            pass

    else:
        pytest.fail("didn't receive a response")
    assert response
    assert i > 0
    await conn.disconnect()


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "parser_class",
    [_AsyncRESP2Parser, _AsyncRESP3Parser, _AsyncHiredisParser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser", "AsyncHiredisParser"],
)
async def test_connection_disconect_race(parser_class, connect_args):
    """
    This test reproduces the case in issue #2349
    where a connection is closed while the parser is reading to feed the
    internal buffer.The stream `read()` will succeed, but when it returns,
    another task has already called `disconnect()` and is waiting for
    close to finish.  When we attempts to feed the buffer, we will fail
    since the buffer is no longer there.

    This test verifies that a read in progress can finish even
    if the `disconnect()` method is called.
    """
    if parser_class == _AsyncHiredisParser and not HIREDIS_AVAILABLE:
        pytest.skip("Hiredis not available")

    connect_args["parser_class"] = parser_class

    conn = Connection(**connect_args)

    cond = asyncio.Condition()
    # 0 == initial
    # 1 == reader is reading
    # 2 == closer has closed and is waiting for close to finish
    state = 0

    # Mock read function, which wait for a close to happen before returning
    # Can either be invoked as two `read()` calls (HiredisParser)
    # or as a `readline()` followed by `readexact()` (PythonParser)
    chunks = [b"$13\r\n", b"Hello, World!\r\n"]

    async def read(_=None):
        nonlocal state
        async with cond:
            if state == 0:
                state = 1  # we are reading
                cond.notify()
                # wait until the closing task has done
                await cond.wait_for(lambda: state == 2)
        return chunks.pop(0)

    # function closes the connection while reader is still blocked reading
    async def do_close():
        nonlocal state
        async with cond:
            await cond.wait_for(lambda: state == 1)
            state = 2
            cond.notify()
        await conn.disconnect()

    async def do_read():
        return await conn.read_response()

    reader = mock.Mock(spec=asyncio.StreamReader)
    writer = mock.Mock(spec=asyncio.StreamWriter)
    writer.transport.get_extra_info.side_effect = None

    # for HiredisParser
    reader.read.side_effect = read
    # for PythonParser
    reader.readline.side_effect = read
    reader.readexactly.side_effect = read

    async def open_connection(*args, **kwargs):
        return reader, writer

    async def dummy_method(*args, **kwargs):
        pass

    # get dummy stream objects for the connection
    with patch.object(asyncio, "open_connection", open_connection):
        # disable the initial version handshake
        with patch.multiple(
            conn, send_command=dummy_method, read_response=dummy_method
        ):
            await conn.connect()

    vals = await asyncio.gather(do_read(), do_close())
    assert vals == [b"Hello, World!", None]


@pytest.mark.fixed_client
def test_create_single_connection_client_from_url():
    client = Redis.from_url("redis://localhost:6379/0?", single_connection_client=True)
    assert client.single_connection_client is True


@pytest.mark.fixed_client
@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
async def test_pool_auto_close(request, from_url):
    """Verify that basic Redis instances have auto_close_connection_pool set to True"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    async def get_redis_connection():
        if from_url:
            return Redis.from_url(url)
        return Redis(**url_args)

    r1 = await get_redis_connection()
    assert r1.auto_close_connection_pool is True
    await r1.aclose()


@pytest.mark.fixed_client
@skip_if_server_version_lt("5.0.0")
async def test_pool_replaces_connection_killed_while_idle(request):
    """
    Regression test for #4252: a pooled connection the server closed while
    it sat idle (CLIENT KILL, idle timeout, server-side reap) must be
    replaced at pool checkout instead of failing the next command.
    Retries are disabled so recovery can only come from the pool.
    """
    url: str = request.config.getoption("--redis-url")
    r = Redis.from_url(url, retry=Retry(NoBackoff(), 0))
    killer = Redis.from_url(url)
    try:
        cid = await r.client_id()
        conn = r.connection_pool._available_connections[0]
        assert await killer.client_kill_filter(_id=str(cid))

        # wait until the client's event loop has seen the server-side close
        deadline = asyncio.get_running_loop().time() + 3
        while not conn._parser._stream.at_eof():
            assert asyncio.get_running_loop().time() < deadline, (
                "server-side kill never surfaced on the client socket"
            )
            await asyncio.sleep(0.01)

        # the next command must be served by a healthy replacement connection
        assert await r.ping()
    finally:
        await r.aclose()
        await r.connection_pool.disconnect()
        await killer.aclose()
        await killer.connection_pool.disconnect()


@pytest.mark.fixed_client
async def test_close_is_aclose(request):
    """Verify close() calls aclose()"""
    calls = 0

    async def mock_aclose(self):
        nonlocal calls
        calls += 1

    url: str = request.config.getoption("--redis-url")
    r1 = await Redis.from_url(url)
    with patch.object(r1, "aclose", mock_aclose):
        with pytest.deprecated_call():
            await r1.close()
        assert calls == 1

    with pytest.deprecated_call():
        await r1.close()


@pytest.mark.fixed_client
async def test_pool_from_url_deprecation(request):
    url: str = request.config.getoption("--redis-url")

    with pytest.deprecated_call():
        return Redis.from_url(url, auto_close_connection_pool=False)


@pytest.mark.fixed_client
async def test_pool_auto_close_disable(request):
    """Verify that auto_close_connection_pool can be disabled (deprecated)"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    async def get_redis_connection():
        url_args["auto_close_connection_pool"] = False
        with pytest.deprecated_call():
            return Redis(**url_args)

    r1 = await get_redis_connection()
    assert r1.auto_close_connection_pool is False
    await r1.connection_pool.disconnect()
    await r1.aclose()


@pytest.mark.fixed_client
@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
async def test_redis_connection_pool(request, from_url):
    """Verify that basic Redis instances using `connection_pool`
    have auto_close_connection_pool set to False"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    pool = None

    async def get_redis_connection():
        nonlocal pool
        if from_url:
            pool = ConnectionPool.from_url(url)
        else:
            pool = ConnectionPool(**url_args)
        return Redis(connection_pool=pool)

    called = 0

    async def mock_disconnect(_):
        nonlocal called
        called += 1

    with patch.object(ConnectionPool, "disconnect", mock_disconnect):
        async with await get_redis_connection() as r1:
            assert r1.auto_close_connection_pool is False

    assert called == 0
    await pool.disconnect()


@pytest.mark.fixed_client
@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
async def test_redis_from_pool(request, from_url):
    """Verify that basic Redis instances created using `from_pool()`
    have auto_close_connection_pool set to True"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    pool = None

    async def get_redis_connection():
        nonlocal pool
        if from_url:
            pool = ConnectionPool.from_url(url)
        else:
            pool = ConnectionPool(**url_args)
        return Redis.from_pool(pool)

    called = 0

    async def mock_disconnect(_):
        nonlocal called
        called += 1

    with patch.object(ConnectionPool, "disconnect", mock_disconnect):
        async with await get_redis_connection() as r1:
            assert r1.auto_close_connection_pool is True

    assert called == 1
    await pool.disconnect()


@pytest.mark.fixed_client
def test_create_secure_client_from_url_with_minimum_ssl_version():
    client = Redis.from_url(
        "rediss://localhost:6379/0?ssl_cert_reqs=none&ssl_min_version={}".format(
            ssl.TLSVersion.TLSv1_3
        )
    )
    assert (
        client.connection_pool.connection_kwargs["ssl_min_version"]
        == ssl.TLSVersion.TLSv1_3
    )


@pytest.mark.fixed_client
@pytest.mark.parametrize("auto_close", (True, False))
async def test_redis_pool_auto_close_arg(request, auto_close):
    """test that redis instance where pool is provided have
    auto_close_connection_pool set to False, regardless of arg"""

    url: str = request.config.getoption("--redis-url")
    pool = ConnectionPool.from_url(url)

    async def get_redis_connection():
        with pytest.deprecated_call():
            client = Redis(connection_pool=pool, auto_close_connection_pool=auto_close)
        return client

    called = 0

    async def mock_disconnect(_):
        nonlocal called
        called += 1

    with patch.object(ConnectionPool, "disconnect", mock_disconnect):
        async with await get_redis_connection() as r1:
            assert r1.auto_close_connection_pool is False

    assert called == 0
    await pool.disconnect()


async def test_client_garbage_collection(request):
    """
    Test that a Redis client will call _close() on any
    connection that it holds at time of destruction
    """

    url: str = request.config.getoption("--redis-url")
    pool = ConnectionPool.from_url(url)

    # create a client with a connection from the pool
    client = Redis(connection_pool=pool, single_connection_client=True)
    await client.initialize()
    with mock.patch.object(client, "connection") as a:
        # we cannot, in unittests, or from asyncio, reliably trigger garbage collection
        # so we must just invoke the handler
        with pytest.warns(ResourceWarning):
            client.__del__()
            assert a._close.called

    await client.aclose()
    await pool.aclose()


async def test_connection_garbage_collection(request):
    """
    Test that a Connection object will call close() on the
    stream that it holds.
    """

    url: str = request.config.getoption("--redis-url")
    pool = ConnectionPool.from_url(url)

    # create a client with a connection from the pool
    client = Redis(connection_pool=pool, single_connection_client=True)
    await client.initialize()
    conn = client.connection

    with mock.patch.object(conn, "_reader"):
        with mock.patch.object(conn, "_writer") as a:
            # we cannot, in unittests, or from asyncio, reliably trigger
            # garbage collection so we must just invoke the handler
            with pytest.warns(ResourceWarning):
                conn.__del__()
                assert a.close.called

    await client.aclose()
    await pool.aclose()


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
async def test_format_error_message(conn, error, expected_message):
    """Test that the _error_message function formats errors correctly"""
    error_message = conn._error_message(error)
    assert error_message == expected_message


@pytest.mark.fixed_client
async def test_network_connection_failure():
    exp_err = rf"^Error {ECONNREFUSED} connecting to 127.0.0.1:9999.(.+)$"
    with pytest.raises(ConnectionError, match=exp_err):
        # nothing listens on this port, so skip the default backoff retries
        redis = Redis(host="127.0.0.1", port=9999, retry=Retry(NoBackoff(), 0))
        await redis.set("a", "b")


@pytest.mark.fixed_client
async def test_unix_socket_connection_failure():
    exp_err = "Error 2 connecting to unix:///tmp/a.sock. No such file or directory."
    with pytest.raises(ConnectionError, match=exp_err):
        # the socket file does not exist, so skip the default backoff retries
        redis = Redis(
            unix_socket_path="unix:///tmp/a.sock", retry=Retry(NoBackoff(), 0)
        )
        await redis.set("a", "b")


async def test_disconnect_no_current_task(request):
    """
    Regression test for issue #3856:
    On Python 3.13+, asyncio.timeout() raises RuntimeError when called
    outside a running Task (e.g. during GC finalization or event-loop
    callbacks where asyncio.current_task() is None).

    disconnect() should fall back to a synchronous _close() in that case,
    rather than entering async_timeout and raising RuntimeError.
    """
    url: str = request.config.getoption("--redis-url")
    conn = Connection(**parse_url(url))
    await conn.connect()
    assert conn.is_connected

    # Invoke disconnect() from a loop.call_soon callback where
    # asyncio.current_task() returns None — the same context as
    # GC finalization of a suspended coroutine.
    loop = asyncio.get_running_loop()
    error_holder: list = []
    done = asyncio.Event()

    def _disconnect_without_task():
        coro = conn.disconnect(nowait=True)
        try:
            coro.send(None)
        except StopIteration:
            pass
        except BaseException as exc:
            error_holder.append(exc)
        finally:
            coro.close()
            done.set()

    loop.call_soon(_disconnect_without_task)
    await done.wait()

    assert not error_holder, f"disconnect() raised: {error_holder[0]}"
    assert not conn.is_connected


async def test_disconnect_no_current_task_calls_close(request):
    """
    Verify that disconnect() outside a task context calls _close()
    and properly resets parser state.
    """
    url: str = request.config.getoption("--redis-url")
    conn = Connection(**parse_url(url))
    await conn.connect()
    assert conn.is_connected

    with mock.patch.object(conn, "_close", wraps=conn._close) as mock_close:
        with mock.patch.object(
            conn._parser, "on_disconnect", wraps=conn._parser.on_disconnect
        ) as mock_on_disconnect:
            # Simulate the no-task context by patching current_task
            with mock.patch("asyncio.current_task", return_value=None):
                await conn.disconnect(nowait=True)

            mock_close.assert_called_once()
            mock_on_disconnect.assert_called_once()

    assert not conn.is_connected


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
@pytest.mark.asyncio
async def test_parse_url_retry_on_error_usable_in_retry():
    kw = parse_url("redis://localhost:6379/?retry_on_error=ConnectionError")
    conn = Connection(**kw)
    assert ConnectionError in conn.retry._supported_errors
    assert all(
        isinstance(err, type) and issubclass(err, Exception)
        for err in conn.retry._supported_errors
    )

    calls = 0

    async def do():
        nonlocal calls
        calls += 1
        raise ConnectionError("simulated network drop")

    async def fail(error):
        return None

    with pytest.raises(ConnectionError):
        await conn.retry.call_with_retry(do=do, fail=fail)
    assert calls == 2


@pytest.mark.fixed_client
@pytest.mark.parametrize("port", [True, False, 1.5, "nope", None])
def test_async_connection_rejects_bool_port(port):
    """bool subclasses int; port=True must not become privileged port 1."""
    with pytest.raises(TypeError, match="port must be an integer"):
        Connection(port=port)


@pytest.mark.fixed_client
def test_async_connection_accepts_numeric_port_string():
    """Callers still pass a decimal string such as \"6379\"."""
    conn = Connection(port="6379")
    assert conn.port == 6379


@pytest.mark.fixed_client
@pytest.mark.parametrize("port", [-1, 65536, 99999])
def test_async_connection_rejects_out_of_range_port(port):
    with pytest.raises(ValueError, match="port must be in 0..65535"):
        Connection(port=port)


@pytest.mark.fixed_client
@pytest.mark.parametrize("disconnect_on_error", [True, False])
async def test_invalid_response_always_disconnects(disconnect_on_error):
    """A framing violation invalidates the connection regardless of
    disconnect_on_error. The parsers rewind on error, so the offending reply
    stays queued and every later read would fail identically. See #4291.
    """
    conn = Connection()
    with (
        mock.patch.object(
            conn,
            "_read_response_from_parser",
            side_effect=InvalidResponse("Protocol Error"),
        ),
        mock.patch.object(conn, "disconnect") as disconnect,
    ):
        with pytest.raises(InvalidResponse):
            await conn.read_response(disconnect_on_error=disconnect_on_error)

    disconnect.assert_called_once_with(nowait=True)


# A binary PUBLISH payload delivered to a decode_responses=True subscriber.
# Encoder.decode runs at the tail of _read_response, after the payload has
# already been consumed, so this raises UnicodeDecodeError mid-reply.
BINARY_PUBSUB_MESSAGE = b"*3\r\n$7\r\nmessage\r\n$4\r\nchan\r\n$3\r\n\xff\xfe\xfd\r\n"


def _attach_stream(conn, data):
    """Wire `data` onto conn as if a connect() had just succeeded."""
    conn._reader = MockStream(data)
    conn._writer = mock.Mock()
    conn._writer.close = mock.Mock()
    conn._writer.wait_closed = mock.AsyncMock()
    conn._parser.on_connect(conn)


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "parser_class",
    [_AsyncRESP2Parser, _AsyncHiredisParser],
    ids=["AsyncRESP2Parser", "AsyncHiredisParser"],
)
async def test_binary_pubsub_payload_invalidates_connection(parser_class):
    """The async pubsub read path: a decode_responses=True subscriber handed a
    binary PUBLISH payload raises UnicodeDecodeError from Encoder.decode, and
    PubSub.parse_response passes disconnect_on_error=False. The async parser
    re-parses from self._pos = 0 rather than rewinding a socket buffer, so the
    undecodable bytes are still there on the next read and it fails identically
    unless the connection is dropped. See #4291.
    """
    if parser_class is _AsyncHiredisParser and not HIREDIS_AVAILABLE:
        pytest.skip("Hiredis not available")

    conn = Connection(
        protocol=2,
        parser_class=parser_class,
        encoding="utf-8",
        decode_responses=True,
    )
    _attach_stream(conn, BINARY_PUBSUB_MESSAGE + b"+SECOND\r\n")

    with pytest.raises(UnicodeDecodeError):
        await conn.read_response(disconnect_on_error=False, push_request=True)

    assert conn.is_connected is False

    # PubSub.parse_response calls connect() before reading again. Now that the
    # connection was really dropped, the reconnect gets a clean stream; before
    # the fix connect() was a no-op and this read raised again.
    _attach_stream(conn, b"+RECOVERED\r\n")
    assert (
        await conn.read_response(disconnect_on_error=False, push_request=True)
        == "RECOVERED"
    )

    await conn.disconnect()


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "parser_class",
    [
        _AsyncRESP2Parser,
        _AsyncRESP3Parser,
        pytest.param(
            _AsyncHiredisParser,
            marks=pytest.mark.skipif(
                not HIREDIS_AVAILABLE, reason="hiredis is not installed"
            ),
        ),
    ],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser", "AsyncHiredisParser"],
)
class TestAsyncMalformedNumericFrameInvalidatesConnection:
    """Async version: malformed numeric frames raise InvalidResponse and must drop
    the connection even with disconnect_on_error=False. The async parser
    re-parses via self._pos = 0, so undecodable bytes stay queued on retry
    unless the connection is invalidated.
    """

    @pytest.mark.asyncio
    async def test_malformed_integer_frame_disconnects(self, parser_class):
        """Malformed integer frame `:abc\r\n` raises InvalidResponse."""
        conn = Connection(protocol=2, parser_class=parser_class)
        _attach_stream(conn, b":abc\r\n+SECOND\r\n")

        with pytest.raises(InvalidResponse):
            await conn.read_response(disconnect_on_error=False, push_request=True)

        assert conn.is_connected is False
        await conn.disconnect()

    @pytest.mark.asyncio
    async def test_malformed_bulk_length_disconnects(self, parser_class):
        """Malformed bulk string length `$xyz\r\n` raises InvalidResponse."""
        conn = Connection(protocol=2, parser_class=parser_class)
        _attach_stream(conn, b"$xyz\r\n+SECOND\r\n")

        with pytest.raises(InvalidResponse):
            await conn.read_response(disconnect_on_error=False, push_request=True)

        assert conn.is_connected is False
        await conn.disconnect()

    @pytest.mark.asyncio
    async def test_malformed_array_length_disconnects(self, parser_class):
        """Malformed array length `*abc\r\n` raises InvalidResponse."""
        conn = Connection(protocol=2, parser_class=parser_class)
        _attach_stream(conn, b"*abc\r\n+SECOND\r\n")

        with pytest.raises(InvalidResponse):
            await conn.read_response(disconnect_on_error=False, push_request=True)

        assert conn.is_connected is False
        await conn.disconnect()

    @pytest.mark.asyncio
    async def test_next_read_is_clean_after_malformed_integer(self, parser_class):
        """Reconnect after malformed integer frame serves new stream cleanly."""
        conn = Connection(protocol=2, parser_class=parser_class)
        _attach_stream(conn, b":abc\r\n")

        with pytest.raises(InvalidResponse):
            await conn.read_response(disconnect_on_error=False, push_request=True)

        # Reconnect with fresh stream
        _attach_stream(conn, b"+RECOVERED\r\n")
        result = await conn.read_response(disconnect_on_error=False, push_request=True)
        assert result == b"RECOVERED"
        await conn.disconnect()

    @pytest.mark.asyncio
    async def test_valid_integer_frame_leaves_connection_up(self, parser_class):
        """Valid integer frames must not trigger the new predicate."""
        conn = Connection(protocol=2, parser_class=parser_class)
        _attach_stream(conn, b":42\r\n")

        result = await conn.read_response(disconnect_on_error=False, push_request=True)
        assert result == 42
        assert conn.is_connected is True
        await conn.disconnect()


class _DummyAsyncConnection:
    """Minimal async connection stub for pool metric tests (no real socket)."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()
        self._sock = None

    async def connect(self):
        self._sock = mock.MagicMock()

    async def disconnect(self, *args, **kwargs):
        self._sock = None

    async def can_read(self, *args, **kwargs):
        return False

    def should_reconnect(self):
        return False

    def mark_for_reconnect(self):
        pass

    def set_re_auth_token(self, *args, **kwargs):
        pass

    async def re_auth(self, *args, **kwargs):
        pass


def _async_pool_metric_calls(mock_fn, pool_name):
    """Extract (state, delta) tuples from record_connection_count calls for a pool."""
    result = []
    for c in mock_fn.call_args_list:
        p = c.kwargs.get("pool_name", c.args[0] if c.args else None)
        if p != pool_name:
            continue
        state = c.kwargs.get("connection_state", c.args[1] if len(c.args) > 1 else None)
        counter = c.kwargs.get("counter", c.args[2] if len(c.args) > 2 else 1)
        result.append((state, counter))
    return result


def _async_net(calls):
    """Return (idle_net, used_net) from a list of (state, delta) tuples."""
    idle = sum(d for s, d in calls if s == ConnectionState.IDLE)
    used = sum(d for s, d in calls if s == ConnectionState.USED)
    return idle, used


@pytest.mark.fixed_client
class TestAsyncBlockingConnectionPoolMetricCount:
    """db.client.connection.count accuracy for async BlockingConnectionPool.

    get_connection() must record the acquire-side transition so it balances the
    USED -1 / IDLE +1 recorded in release(). Without it, every acquire/release
    cycle drifts USED -1 / IDLE +1 (regression: heavy async + Sentinel apps saw
    the counter run to large -used / +idle values over time).
    """

    def _pool(self, max_connections=10):
        return BlockingConnectionPool(
            connection_class=_DummyAsyncConnection,
            max_connections=max_connections,
            timeout=0.1,
        )

    @patch("redis.asyncio.connection.record_connection_count")
    async def test_new_connection_records_only_used(self, mock_rec):
        pool = self._pool()
        pn = get_pool_name(pool)
        mock_rec.reset_mock()

        conn = await pool.get_connection()

        idle_net, used_net = _async_net(_async_pool_metric_calls(mock_rec, pn))
        assert idle_net == 0, f"New conn should not touch IDLE, got {idle_net}"
        assert used_net == 1
        await pool.release(conn)

    @patch("redis.asyncio.connection.record_connection_count")
    async def test_reused_connection_transitions_idle_to_used(self, mock_rec):
        pool = self._pool()
        pn = get_pool_name(pool)
        conn = await pool.get_connection()
        await pool.release(conn)
        mock_rec.reset_mock()

        conn2 = await pool.get_connection()
        assert conn2 is conn

        idle_net, used_net = _async_net(_async_pool_metric_calls(mock_rec, pn))
        assert idle_net == -1
        assert used_net == 1
        await pool.release(conn2)

    @patch("redis.observability.recorder.record_connection_count")
    @patch("redis.asyncio.connection.record_connection_count")
    async def test_full_lifecycle_nets_to_zero(self, mock_rec, mock_sync_rec):
        """acquire -> release cycles -> destroy must not drift the counter."""
        pool = self._pool()
        pn = get_pool_name(pool)
        mock_rec.reset_mock()

        for _ in range(5):
            conn = await pool.get_connection()
            await pool.release(conn)

        # Destroy the pool's connections through the pool's own teardown path.
        # reset() is not a coroutine, so it records through the sync recorder.
        pool.reset()

        calls = _async_pool_metric_calls(mock_rec, pn)
        calls += _async_pool_metric_calls(mock_sync_rec, pn)
        idle_net, used_net = _async_net(calls)
        assert idle_net == 0, f"Lifecycle IDLE should net 0, got {idle_net}"
        assert used_net == 0, f"Lifecycle USED should net 0, got {used_net}"


@pytest.mark.parametrize(
    "timeout",
    [None, 1.0, math.inf],
    ids=["socket_timeout", "user_timeout", "blocking_read"],
)
async def test_read_response_os_level_timeout_is_a_connection_error(timeout):
    # Since Python 3.11 asyncio.TimeoutError is the builtin TimeoutError, so an
    # ETIMEDOUT the OS raises on a dead socket (e.g. after a network change)
    # lands in the same except clause as our own read timeout. It used to be
    # reported as a TimeoutError - or, with an explicit timeout, swallowed into
    # a None return, which made PubSub.listen() re-read the dead socket in a
    # busy loop. The connection is broken: disconnect and raise ConnectionError.
    conn = Connection(socket_timeout=1)
    conn._read_response_from_parser = mock.AsyncMock(
        side_effect=OSError(ETIMEDOUT, "Operation timed out")
    )
    conn.disconnect = mock.AsyncMock()

    with pytest.raises(ConnectionError):
        await conn.read_response(timeout=timeout)
    conn.disconnect.assert_awaited_once()


async def test_read_response_os_level_timeout_without_disconnect_on_error():
    # The read PubSub.listen() actually does: a blocking read that leaves the
    # disconnect to its retry wrapper, which only reconnects if read_response
    # raises. Returning None here is what kept listen() spinning.
    conn = Connection(socket_timeout=1)
    conn._read_response_from_parser = mock.AsyncMock(
        side_effect=OSError(ETIMEDOUT, "Operation timed out")
    )
    conn.disconnect = mock.AsyncMock()

    with pytest.raises(ConnectionError):
        await conn.read_response(
            timeout=math.inf, disconnect_on_error=False, push_request=True
        )
    conn.disconnect.assert_not_awaited()


async def test_read_response_user_timeout_still_returns_none():
    conn = Connection()

    async def never_answers(*args, **kwargs):
        await asyncio.sleep(10)

    conn._read_response_from_parser = never_answers
    conn.disconnect = mock.AsyncMock()

    assert await conn.read_response(timeout=0.01) is None
    conn.disconnect.assert_not_awaited()


@pytest.mark.skipif(
    platform.python_implementation() == "PyPy",
    reason="Immediate reclamation is a refcounting property",
)
async def test_read_response_error_reply_breaks_reference_cycle():
    """
    read_response raises the ResponseError the parser returned. The exception's
    traceback references the read_response frame, so a ``response`` local still
    bound to it would form a cycle only the cycle collector can reclaim, keeping
    every frame on the traceback alive until then. The sync Connection clears
    the local when it raises; with the collector off, refcounting alone must
    reclaim the exception here too.
    """
    conn = Connection()

    # Build the error in a factory so the test itself never holds a strong
    # reference; the weakref is the only handle on it.
    error_refs = []

    async def error_reply(*args, **kwargs):
        error = ResponseError("NOSCRIPT No matching script. Please use EVAL.")
        error_refs.append(weakref.ref(error))
        return error

    conn._read_response_from_parser = error_reply

    gc.collect()
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        with pytest.raises(ResponseError):
            await conn.read_response()

        (ref,) = error_refs
        assert ref() is None
    finally:
        if gc_was_enabled:
            gc.enable()


async def test_send_packed_command_os_level_timeout_is_a_connection_error():
    conn = Connection(socket_timeout=1)
    conn._reader = conn._writer = mock.Mock()  # look connected
    conn._send_packed_command = mock.AsyncMock(
        side_effect=OSError(ETIMEDOUT, "Operation timed out")
    )
    conn.disconnect = mock.AsyncMock()

    with pytest.raises(ConnectionError):
        await conn.send_packed_command(b"PING", check_health=False)
    conn.disconnect.assert_awaited_once()
    conn._reader = conn._writer = None  # disconnect was mocked out


@pytest.mark.fixed_client
@pytest.mark.parametrize("rotation_stage", [None, "send", "read"])
async def test_re_auth_preserves_newer_token(rotation_stage):
    connection = Connection()
    token = mock.Mock()
    token.try_get.return_value = "first-user"
    token.get_value.return_value = "first-token"
    newer_token = mock.Mock()
    connection.set_re_auth_token(token)

    async def send(*args):
        if rotation_stage == "send":
            connection.set_re_auth_token(newer_token)

    async def read():
        if rotation_stage == "read":
            connection.set_re_auth_token(newer_token)
        return b"OK"

    connection.send_command = mock.AsyncMock(side_effect=send)
    connection.read_response = mock.AsyncMock(side_effect=read)
    await connection.re_auth()
    connection.send_command.assert_awaited_once_with(
        "AUTH", "first-user", "first-token"
    )
    assert connection._re_auth_token is (newer_token if rotation_stage else None)
