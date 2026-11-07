"""socket_timeout applies per socket read in the async client (#3454).

The sync client applies socket_timeout to every individual recv(), so a
large response whose chunks keep arriving within the timeout does not
fail. The async client used to wrap the whole read_response() in a single
deadline, timing out slow-but-healthy responses whose total transfer time
exceeded socket_timeout. These tests drive a real Connection and parser
over an in-memory StreamReader, so no server is needed.
"""

import asyncio
import math
import time
from unittest import mock

import pytest

import redis._parsers.base as parser_base
from redis._parsers.hiredis import _AsyncHiredisParser
from redis._parsers.resp2 import _AsyncRESP2Parser
from redis._parsers.resp3 import _AsyncRESP3Parser
from redis.asyncio.connection import Connection
from redis.exceptions import TimeoutError
from redis.utils import HIREDIS_AVAILABLE

SOCKET_TIMEOUT = 0.3
CHUNK_GAP = 0.08  # every gap between pieces is well below SOCKET_TIMEOUT
READ_SIZE = 16384
PAYLOAD = b"x" * 200_000
BULK_WIRE = b"$%d\r\n%s\r\n" % (len(PAYLOAD), PAYLOAD)

requires_hiredis = pytest.mark.skipif(
    not HIREDIS_AVAILABLE, reason="hiredis is not installed"
)


def make_connection(parser, protocol, socket_timeout=SOCKET_TIMEOUT):
    conn = Connection(
        socket_timeout=socket_timeout,
        socket_read_size=READ_SIZE,
        protocol=protocol,
    )
    conn._parser = parser
    reader = asyncio.StreamReader()
    conn._reader = reader
    conn._writer = mock.Mock()
    parser.on_connect(conn)
    conn.disconnect = mock.AsyncMock()
    return conn, reader


def make_resp2_connection(socket_timeout=SOCKET_TIMEOUT):
    return make_connection(_AsyncRESP2Parser(READ_SIZE), 2, socket_timeout)


def make_resp3_connection(socket_timeout=SOCKET_TIMEOUT):
    return make_connection(_AsyncRESP3Parser(READ_SIZE), 3, socket_timeout)


def make_hiredis_connection(socket_timeout=SOCKET_TIMEOUT):
    return make_connection(_AsyncHiredisParser(READ_SIZE), 2, socket_timeout)


CONNECTION_FACTORIES = [
    pytest.param(make_resp2_connection, id="resp2"),
    pytest.param(make_resp3_connection, id="resp3"),
    pytest.param(
        make_hiredis_connection,
        id="hiredis",
        marks=requires_hiredis,
    ),
]


async def feed_in_pieces(reader, data, piece=READ_SIZE, gap=CHUNK_GAP):
    """Feed data in pieces, waiting `gap` between pieces.

    No single gap exceeds socket_timeout, but the whole transfer takes
    several times longer than socket_timeout.
    """
    for i in range(0, len(data), piece):
        reader.feed_data(data[i : i + piece])
        await asyncio.sleep(gap)
    reader.feed_eof()


@pytest.mark.parametrize("factory", CONNECTION_FACTORIES)
async def test_socket_timeout_is_per_read_not_per_response(factory):
    # Regression test for #3454: a large response delivered in pieces, each
    # arriving promptly, must be read in full even though the total time
    # exceeds socket_timeout. Before the fix, read_response() raised
    # TimeoutError after SOCKET_TIMEOUT seconds.
    conn, reader = factory()
    feeder = asyncio.create_task(feed_in_pieces(reader, BULK_WIRE))

    start = time.monotonic()
    response = await conn.read_response()
    elapsed = time.monotonic() - start
    await feeder

    assert response == PAYLOAD
    # prove the transfer really took longer than one socket_timeout window
    assert elapsed > SOCKET_TIMEOUT


@pytest.mark.parametrize("factory", CONNECTION_FACTORIES)
async def test_socket_timeout_still_fires_when_socket_stalls(factory):
    # The per-read deadline must still fire: a response that starts and
    # then stalls for longer than socket_timeout raises TimeoutError.
    conn, reader = factory()
    reader.feed_data(b"$5\r\nhe")

    start = time.monotonic()
    with pytest.raises(TimeoutError):
        await conn.read_response()
    elapsed = time.monotonic() - start

    assert elapsed < SOCKET_TIMEOUT * 4
    conn.disconnect.assert_awaited()


async def test_socket_timeout_per_line_for_aggregate_replies():
    # An aggregate reply made of many small elements exercises the line
    # reads: each element gets a fresh socket_timeout window.
    conn, reader = make_resp3_connection()
    elements = [b"$5\r\nhello\r\n"] * 30
    wire = b"*%d\r\n" % len(elements) + b"".join(elements)
    feeder = asyncio.create_task(
        feed_in_pieces(reader, wire, piece=len(elements[0]) * 2, gap=0.05)
    )

    response = await conn.read_response()
    await feeder

    assert response == [b"hello"] * 30


async def test_maintenance_relax_reaches_reads_of_in_flight_response():
    # Maintenance notifications relax the socket timeout mid-response via
    # update_current_socket_timeout(). With per-read deadlines the relaxed
    # value must reach the reads of the in-flight response: after the
    # relax, a gap longer than the original socket_timeout is tolerated.
    conn, reader = make_resp2_connection(socket_timeout=0.25)
    wire = BULK_WIRE
    reader.feed_data(wire[:100])

    async def feed_rest_late():
        await asyncio.sleep(0.5)  # gap > original socket_timeout of 0.25
        reader.feed_data(wire[100:])
        reader.feed_eof()

    read_task = asyncio.create_task(conn.read_response())
    feeder = asyncio.create_task(feed_rest_late())
    await asyncio.sleep(0.1)  # the response is now in flight
    conn.update_current_socket_timeout(2.0)

    response = await read_task
    await feeder

    assert response == PAYLOAD


async def test_explicit_timeout_still_bounds_the_whole_read():
    # An explicit caller timeout keeps its historical semantics: it bounds
    # the whole read and returns None when it expires, even though the
    # socket never stalls for a full socket_timeout.
    conn, reader = make_resp2_connection()
    feeder = asyncio.create_task(feed_in_pieces(reader, BULK_WIRE))

    start = time.monotonic()
    response = await conn.read_response(timeout=SOCKET_TIMEOUT / 2)
    elapsed = time.monotonic() - start
    feeder.cancel()

    assert response is None
    assert elapsed < SOCKET_TIMEOUT * 2
    conn.disconnect.assert_not_awaited()


async def test_blocking_read_ignores_socket_timeout():
    # PubSub-style blocking reads (timeout=math.inf) must not be cut short
    # by socket_timeout, however long the response takes in total.
    conn, reader = make_resp2_connection()
    feeder = asyncio.create_task(feed_in_pieces(reader, BULK_WIRE))

    response = await conn.read_response(timeout=math.inf)
    await feeder

    assert response == PAYLOAD


@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(make_resp2_connection, id="resp2"),
        pytest.param(make_resp3_connection, id="resp3"),
    ],
)
async def test_explicit_timeout_mid_bulk_read_preserves_bytes(factory):
    # Regression test: an explicit timeout interrupting a large bulk read
    # (the PubSub.get_message(timeout=...) path) returns None and keeps
    # the connection. Explicit reads use readexactly (see _read), so the
    # bytes the cancelled read had not consumed stay on the stream, and
    # the next read_response() resumes from the stream and returns the
    # full, correct reply instead of parsing the saved header against a
    # stream that starts mid-payload.
    conn, reader = factory()
    # Header plus the first two read-size slices of the payload arrive
    # promptly; the rest is withheld, so the explicit timeout fires
    # mid-payload, after whole pieces have been consumed from the stream.
    reader.feed_data(BULK_WIRE[: READ_SIZE * 2])

    response = await conn.read_response(timeout=SOCKET_TIMEOUT / 2)

    assert response is None
    conn.disconnect.assert_not_awaited()

    feeder = asyncio.create_task(feed_in_pieces(reader, BULK_WIRE[READ_SIZE * 2 :]))
    response = await conn.read_response()
    await feeder

    assert response == PAYLOAD


@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(make_resp2_connection, id="resp2"),
        pytest.param(make_resp3_connection, id="resp3"),
    ],
)
async def test_socket_timeout_mid_bulk_read_preserves_bytes_when_kept(factory):
    # Regression test: on the default path a per-read socket_timeout
    # interrupts _read_exactly after whole pieces were pulled off the
    # stream. A caller passing disconnect_on_error=False (PubSub and
    # push reads) keeps the connection and resumes by re-parsing the
    # saved header, so the pieces must reach the parser's resume state:
    # without them the resumed read starts mid-payload, consumes bytes
    # from the following reply, and returns a corrupted value.
    conn, reader = factory()
    # Header plus the first two read-size slices of the payload arrive
    # promptly; the rest stalls past socket_timeout.
    reader.feed_data(BULK_WIRE[: READ_SIZE * 2])

    with pytest.raises(TimeoutError):
        await conn.read_response(disconnect_on_error=False)
    conn.disconnect.assert_not_awaited()

    feeder = asyncio.create_task(
        feed_in_pieces(reader, BULK_WIRE[READ_SIZE * 2 :] + b"+OK\r\n")
    )
    response = await conn.read_response()
    await feeder

    assert response == PAYLOAD
    # the following reply is intact, not consumed as payload bytes
    assert await conn.read_response() == b"OK"


async def test_reads_arm_timeout_only_when_they_can_wait():
    parser = _AsyncRESP2Parser(READ_SIZE)
    reader = asyncio.StreamReader()
    parser._stream = reader
    parser._apply_read_timeout = True
    parser._read_timeout = 0.05

    with pytest.raises(asyncio.TimeoutError):
        await parser._readline()
    assert parser._active_read_timeout is None

    reader.feed_data(b"OK\r\n")
    assert await parser._readline() == b"OK"
    assert parser._active_read_timeout is None

    parser._read_timeout = None
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(parser._readline(), 0.2)
    assert parser._active_read_timeout is None


async def test_read_exactly_adds_only_the_joined_buffer_to_chunks():
    # The pieces of a bulk read stay in a local list; only the joined
    # buffer is added to the resume state once the read completes, so
    # _chunks does not hold the pieces plus a second full copy of the
    # payload.
    parser = _AsyncRESP2Parser(READ_SIZE)
    reader = asyncio.StreamReader()
    parser._stream = reader
    payload = b"y" * (READ_SIZE * 2 + 100)
    reader.feed_data(payload[:1000])

    read_task = asyncio.create_task(parser._read_exactly(len(payload)))
    await asyncio.sleep(0.05)  # the first piece is consumed on its own
    reader.feed_data(payload[1000:])
    reader.feed_eof()
    result = await read_task

    assert result == payload
    assert parser._chunks == [payload]


async def test_read_exactly_constructs_one_timeout_for_multiple_waits(monkeypatch):
    parser = _AsyncRESP2Parser(READ_SIZE)
    reader = asyncio.StreamReader()
    parser._stream = reader
    parser._apply_read_timeout = True
    parser._read_timeout = SOCKET_TIMEOUT
    real_async_timeout = parser_base.async_timeout
    timeout_count = 0

    def counting_async_timeout(delay):
        nonlocal timeout_count
        timeout_count += 1
        return real_async_timeout(delay)

    monkeypatch.setattr(parser_base, "async_timeout", counting_async_timeout)

    async def feed_pieces():
        for piece in (b"ab", b"cd", b"ef"):
            await asyncio.sleep(0.01)
            reader.feed_data(piece)

    feeder = asyncio.create_task(feed_pieces())
    result = await parser._read_exactly(6)
    await feeder

    assert result == b"abcdef"
    assert timeout_count == 1


@requires_hiredis
async def test_hiredis_read_from_socket_shares_one_timeout_per_response(monkeypatch):
    # One response's socket reads share a single timeout context,
    # rescheduled before each read that waits, instead of entering a
    # fresh context per socket read.
    import redis._parsers.hiredis as hiredis_parser

    real_async_timeout = hiredis_parser.async_timeout
    timeout_count = 0

    def counting_async_timeout(delay):
        nonlocal timeout_count
        timeout_count += 1
        return real_async_timeout(delay)

    monkeypatch.setattr(hiredis_parser, "async_timeout", counting_async_timeout)
    conn, reader = make_hiredis_connection()
    parser = conn._parser
    feeder = asyncio.create_task(feed_in_pieces(reader, BULK_WIRE))

    response = await conn.read_response()
    await feeder

    assert response == PAYLOAD
    assert timeout_count == 1
    assert parser._response_read_timeout is None
    assert parser._active_read_timeout is None
