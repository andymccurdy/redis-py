import select
import selectors
import socket
from logging import getLogger
from typing import Callable, List, Optional, TypedDict, Union

from ..exceptions import ConnectionError, InvalidResponse, RedisError, TimeoutError
from ..typing import EncodableT
from ..utils import (
    HIREDIS_AVAILABLE,
    SENTINEL,
    deprecated_function,
    is_os_level_timeout,
)
from .base import (
    AsyncBaseParser,
    AsyncPushNotificationsParser,
    BaseParser,
    PushNotificationsParser,
)
from .socket import (
    NONBLOCKING_EXCEPTION_ERROR_NUMBERS,
    NONBLOCKING_EXCEPTIONS,
    SERVER_CLOSED_CONNECTION_ERROR,
)

# Used to signal that hiredis-py does not have enough data to parse.
# Using `False` or `None` is not reliable, given that the parser can
# return `False` or `None` for legitimate reasons from RESP payloads.
NOT_ENOUGH_DATA = object()

# select.poll() is unavailable on Windows; fall back to selectors there.
_HAS_POLL = hasattr(select, "poll")

# POLLRDHUP (Linux) reports a peer half-close (FIN) that POLLHUP does not cover.
# It is absent on macOS/Windows, where 0 turns the masks that use it into no-ops.
_POLLRDHUP = getattr(select, "POLLRDHUP", 0)


def _socket_can_read(sock, timeout: float) -> bool:
    # SSL sockets can have decrypted bytes buffered above the OS socket layer.
    if hasattr(sock, "pending") and sock.pending():
        return True
    # timeout=0 must be a non-blocking readiness check only; both branches
    # below are non-destructive and have no FD_SETSIZE limit (select.select
    # raises ValueError for fds >= 1024).
    if _HAS_POLL:
        # Prefer poll() over selectors.DefaultSelector: epoll/kqueue selectors
        # allocate a file descriptor per check and so fail with EMFILE under
        # fd exhaustion - the very condition that pushes sockets onto high
        # fds. poll() allocates nothing.
        poller = select.poll()
        poller.register(sock, select.POLLIN)
        # poll() takes milliseconds (None blocks forever). POLLHUP/POLLERR/
        # POLLNVAL are always reported regardless of the registered mask, so
        # closed or errored sockets still count as readable, like select().
        poll_timeout = None if timeout is None else timeout * 1000
        return bool(poller.poll(poll_timeout))
    with selectors.DefaultSelector() as selector:
        selector.register(sock, selectors.EVENT_READ)
        return bool(selector.select(timeout))


def _socket_is_closed(sock) -> bool:
    # A server-closed socket reads as ready (it yields EOF), so readiness alone
    # cannot tell it apart from a socket holding pending data, and both checks
    # here are non-destructive so pending push messages (e.g. cache
    # invalidations) are left intact to be processed. Without poll() the two
    # states are indistinguishable, so report not-closed.
    if not _HAS_POLL:
        return False
    # Decrypted TLS bytes buffered above the OS socket layer must be processed
    # before the connection can be treated as closed, like kernel-level data.
    if hasattr(sock, "pending") and sock.pending():
        return False
    poller = select.poll()
    poller.register(sock, select.POLLIN | _POLLRDHUP)
    events = poller.poll(0)
    if not events:
        return False
    _, revents = events[0]
    # A readable socket holds either data or EOF, and the poll flags alone
    # cannot always tell which: POLLHUP/POLLRDHUP can be reported while unread
    # data is still buffered (the peer sent data, then closed), and a drained
    # closed socket reports plain POLLIN where POLLRDHUP is unavailable
    # (PyPy). A non-destructive MSG_PEEK settles it: EOF peeks as b"".
    try:
        return sock.recv(1, socket.MSG_PEEK) == b""
    except NONBLOCKING_EXCEPTIONS:
        # Transient would-block: data may still arrive, keep the connection.
        return False
    except ValueError:
        # SSL sockets do not support recv() flags; their buffered plaintext is
        # covered by the pending() check above, so fall back to the poll
        # flags. POLLRDHUP must be in the register mask or poll() won't
        # report it: on Linux a graceful FIN reports POLLIN|POLLRDHUP and
        # never POLLHUP. macOS/Windows set POLLHUP instead (_POLLRDHUP is 0).
        closed_flags = select.POLLHUP | select.POLLERR | select.POLLNVAL | _POLLRDHUP
        return bool(revents & closed_flags)
    except OSError:
        # The socket is errored (POLLERR/POLLNVAL); nothing left to read.
        return True


class _HiredisReaderArgs(TypedDict, total=False):
    protocolError: Callable[[str], Exception]
    replyError: Callable[[str], Exception]
    encoding: Optional[str]
    errors: Optional[str]


class _HiredisParser(BaseParser, PushNotificationsParser):
    "Parser class for connections using Hiredis"

    def __init__(self, socket_read_size):
        if not HIREDIS_AVAILABLE:
            raise RedisError("Hiredis is not installed")
        self.socket_read_size = socket_read_size
        self._buffer = bytearray(socket_read_size)
        self.pubsub_push_handler_func = self.handle_pubsub_push_response
        self.node_moving_push_handler_func = None
        self.maintenance_push_handler_func = None
        self.oss_cluster_maint_push_handler_func = None
        self.invalidation_push_handler_func = None
        self._hiredis_PushNotificationType = None

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def handle_pubsub_push_response(self, response):
        logger = getLogger("push_response")
        logger.debug("Push response: %s", response)
        return response

    def on_connect(self, connection, **kwargs):
        import hiredis

        self._sock = connection._sock
        self._socket_timeout = connection.socket_timeout
        kwargs = {
            "protocolError": InvalidResponse,
            "replyError": self.parse_error,
            "errors": connection.encoder.encoding_errors,
            "notEnoughData": NOT_ENOUGH_DATA,
        }

        if connection.encoder.decode_responses:
            kwargs["encoding"] = connection.encoder.encoding
        self._reader = hiredis.Reader(**kwargs)

        try:
            self._hiredis_PushNotificationType = hiredis.PushNotification
        except AttributeError:
            # hiredis < 3.2
            self._hiredis_PushNotificationType = None

    def on_disconnect(self):
        self._sock = None
        self._reader = None

    def can_read(self, timeout: float = 0) -> bool:
        # TODO: Rename this API; it detects pending data or dirty/closed
        # connection state, not only whether application data can be read.
        reader = self._reader
        sock = self._sock
        # Another thread may disconnect this connection while we are here (e.g.
        # the multi-database health check taking a database out of service);
        # on_disconnect() sets both _sock and _reader to None. Bind them locally
        # so the checks below cannot disagree about the connection, and fail with
        # a descriptive, retryable ConnectionError instead of a TypeError from
        # registering None with poll().
        if reader is None or sock is None:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        if reader.has_data():
            return True
        try:
            if not _socket_can_read(sock, timeout):
                return False
            # the socket reports readable but the reader has no buffered data. a
            # server-closed socket also reads as ready (it yields EOF), so tell the
            # two apart with a non-destructive poll: a peer-closed socket must not be
            # reused, while a readable-but-open socket may just hold a pending push.
            # this mirrors how the pure-Python parser (recv -> b"") and the async
            # parser (StreamReader.at_eof()) already signal a closed connection.
            if _socket_is_closed(sock):
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        except ValueError:
            # Same race as above, one step later: a concurrent disconnect closed
            # the socket, so its file descriptor is -1 and neither poll() nor a
            # selector can register it. The connection is gone either way.
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None
        return True

    def read_from_socket(self, timeout=SENTINEL, raise_on_timeout=True):
        sock = self._sock
        reader = self._reader
        # Another thread may disconnect this connection while we are here (e.g.
        # a shared client closed via `with redis:`); on_disconnect() sets both
        # _sock and _reader to None. Bind them locally and fail with a
        # descriptive, retryable ConnectionError instead of an AttributeError.
        if sock is None or reader is None:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        custom_timeout = timeout is not SENTINEL
        try:
            if custom_timeout:
                sock.settimeout(timeout)
            bufflen = sock.recv_into(self._buffer)
            if bufflen == 0:
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
            reader.feed(self._buffer, 0, bufflen)
            # data was read from the socket and added to the buffer.
            # return True to indicate that data was read.
            return True
        except socket.timeout as e:
            if is_os_level_timeout(e):
                # ETIMEDOUT from the OS (e.g. after a network change): the
                # connection is dead, not slow. Reporting it as a timeout - or
                # as "no data" when polling - would hide a broken connection.
                raise ConnectionError(f"Error while reading from socket: {e.args}")
            if raise_on_timeout:
                raise TimeoutError("Timeout reading from socket")
            return False
        except NONBLOCKING_EXCEPTIONS as ex:
            # if we're in nonblocking mode and the recv raises a
            # blocking error, simply return False indicating that
            # there's no data to be read. otherwise raise the
            # original exception.
            allowed = NONBLOCKING_EXCEPTION_ERROR_NUMBERS.get(ex.__class__, -1)
            if ex.errno == allowed:
                if not raise_on_timeout:
                    return False
                if timeout == 0:
                    raise TimeoutError("Timeout reading from socket")
            raise ConnectionError(f"Error while reading from socket: {ex.args}")
        finally:
            if custom_timeout:
                try:
                    sock.settimeout(self._socket_timeout)
                except OSError:
                    # A disconnect from another thread closes the socket, so there
                    # is nothing left to restore the timeout on. An exception from
                    # here would replace the outcome the body reported - including
                    # the retryable ConnectionError above - with EBADF.
                    pass

    def read_response(
        self,
        disable_decoding=False,
        push_request=False,
        timeout: Union[float, object] = SENTINEL,
    ):
        while True:
            # Bind the reader locally so a concurrent disconnect that clears
            # self._reader can't turn a later .gets() into an AttributeError;
            # re-checking the attribute each time would still race. It is
            # re-bound per frame because a push handler may have disconnected.
            reader = self._reader
            if reader is None:
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

            if disable_decoding:
                response = reader.gets(False)
            else:
                response = reader.gets()

            while response is NOT_ENOUGH_DATA:
                self.read_from_socket(timeout=timeout)
                if disable_decoding:
                    response = reader.gets(False)
                else:
                    response = reader.gets()
            if self._hiredis_PushNotificationType is None or not isinstance(
                response, self._hiredis_PushNotificationType
            ):
                break
            response = self.handle_push_response(response)
            if push_request:
                return response
            # Otherwise read the reply the push interleaved with. Looping
            # instead of recursing keeps a burst of consecutive push frames
            # from growing the stack.
        # if the response is a ConnectionError or the response is a list and
        # the first item is a ConnectionError, raise it as something bad
        # happened
        if isinstance(response, ConnectionError):
            raise response
        elif (
            isinstance(response, list)
            and response
            and isinstance(response[0], ConnectionError)
        ):
            raise response[0]
        return response


class _AsyncHiredisParser(AsyncBaseParser, AsyncPushNotificationsParser):
    """Async implementation of parser class for connections using Hiredis"""

    __slots__ = ("_reader",)

    def __init__(self, socket_read_size: int):
        if not HIREDIS_AVAILABLE:
            raise RedisError("Hiredis is not available.")
        super().__init__(socket_read_size=socket_read_size)
        self._reader = None
        self.pubsub_push_handler_func = self.handle_pubsub_push_response
        self.invalidation_push_handler_func = None
        self._hiredis_PushNotificationType = None

    async def handle_pubsub_push_response(self, response):
        logger = getLogger("push_response")
        logger.debug("Push response: %s", response)
        return response

    def on_connect(self, connection):
        import hiredis

        self._stream = connection._reader
        kwargs: _HiredisReaderArgs = {
            "protocolError": InvalidResponse,
            "replyError": self.parse_error,
            "notEnoughData": NOT_ENOUGH_DATA,
        }
        if connection.encoder.decode_responses:
            kwargs["encoding"] = connection.encoder.encoding
            kwargs["errors"] = connection.encoder.encoding_errors

        self._reader = hiredis.Reader(**kwargs)
        self._connected = True

        try:
            self._hiredis_PushNotificationType = getattr(
                hiredis, "PushNotification", None
            )
        except AttributeError:
            # hiredis < 3.2
            self._hiredis_PushNotificationType = None

    def on_disconnect(self):
        self._connected = False

    @deprecated_function(
        version="8.0.0", reason="Use can_read() instead", name="can_read_destructive"
    )
    async def can_read_destructive(self) -> bool:
        return await self.can_read()

    async def can_read(self) -> bool:
        # TODO: Rename this API; it detects pending data or dirty/closed
        # connection state, not only whether application data can be read.
        if not self._connected:
            raise OSError("Buffer is closed.")
        # buffered data wins over EOF, like the sync parser: a pending
        # response or push notification must stay readable even if the
        # server has since closed the connection.
        if self._reader.has_data():
            return True
        if self._stream.at_eof() or self._stream.exception() is not None:
            # Raise like the sync parser does on a server-closed connection,
            # so callers that tolerate pending data (push notifications)
            # can't mistake EOF for a readable connection.
            # A reset (RST) leaves no EOF but an exception on the stream.
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        # asyncio.StreamReader has no public non-destructive API for checking
        # buffered bytes. Preserve dirty-connection detection for hiredis; tests
        # with a real StreamReader guard this private buffer API in CI.
        return bool(self._stream._buffer)

    async def read_from_socket(self):
        buffer = await self._stream.read(self._read_size)
        if not buffer or not isinstance(buffer, bytes):
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None
        self._reader.feed(buffer)
        # data was read from the socket and added to the buffer.
        # return True to indicate that data was read.
        return True

    async def read_response(
        self, disable_decoding: bool = False, push_request: bool = False
    ) -> Union[EncodableT, List[EncodableT]]:
        while True:
            # If `on_disconnect()` has been called, prohibit any more reads
            # even if they could happen because data might be present.
            # We still allow reads in progress to finish
            if not self._connected:
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

            if disable_decoding:
                response = self._reader.gets(False)
            else:
                response = self._reader.gets()

            while response is NOT_ENOUGH_DATA:
                await self.read_from_socket()
                if disable_decoding:
                    response = self._reader.gets(False)
                else:
                    response = self._reader.gets()
            if self._hiredis_PushNotificationType is None or not isinstance(
                response, self._hiredis_PushNotificationType
            ):
                break
            response = await self.handle_push_response(response)
            if push_request:
                return response
            # Otherwise read the reply the push interleaved with. Looping
            # instead of recursing keeps a burst of consecutive push frames
            # from growing the stack.

        # if the response is a ConnectionError or the response is a list and
        # the first item is a ConnectionError, raise it as something bad
        # happened
        if isinstance(response, ConnectionError):
            raise response
        elif (
            isinstance(response, list)
            and response
            and isinstance(response[0], ConnectionError)
        ):
            raise response[0]
        return response
