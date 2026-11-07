import logging
import sys
from abc import ABC, abstractmethod
from asyncio import IncompleteReadError, StreamReader, get_running_loop
from typing import Awaitable, Callable, List, Optional, Protocol, Union

# the functionality is available in 3.11.x but has a major issue before
# 3.11.3. See https://github.com/redis/redis-py/issues/2633
if sys.version_info >= (3, 11, 3):
    from asyncio import timeout as async_timeout
else:
    from async_timeout import timeout as async_timeout

from redis.maint_notifications import (
    MaintenanceNotification,
    NodeFailedOverNotification,
    NodeFailingOverNotification,
    NodeMigratedNotification,
    NodeMigratingNotification,
    NodeMovingNotification,
    OSSNodeMigratedNotification,
    OSSNodeMigratingNotification,
)
from redis.utils import deprecated_function, safe_str

from ..exceptions import (
    AskError,
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ClusterCrossSlotError,
    ClusterDownError,
    ConnectionError,
    ExecAbortError,
    ExternalAuthProviderError,
    InvalidResponse,
    MasterDownError,
    MaxClientsReachedError,
    ModuleError,
    MovedError,
    NoPermissionError,
    NoScriptError,
    NoSuchFieldsetError,
    OutOfMemoryError,
    ReadOnlyError,
    ResponseError,
    TryAgainError,
)
from ..typing import EncodableT
from .encoders import Encoder
from .socket import SERVER_CLOSED_CONNECTION_ERROR, SocketBuffer

# Errors a parser can raise from the middle of a reply, after the read cursor
# has already advanced past bytes the parser cannot re-interpret. The resume
# machinery from #2510/#2695 rewinds so an *interrupted* read can be re-parsed
# from the start; it cannot help here, because re-parsing the same bytes just
# reproduces the same failure. Connection.read_response drops the connection on
# these regardless of disconnect_on_error, or the caller loops forever (#4291).
#
# - InvalidResponse: unknown reply type byte, or hiredis' own nesting limit.
# - UnicodeDecodeError: Encoder.decode at the tail of _read_response, reached
#   only after the payload has been consumed (decode_responses=True subscriber
#   receiving a binary PUBLISH payload).
# - RecursionError: deeply nested aggregate replies exhaust the stack in the
#   pure-Python parsers. #4144 turns this into InvalidResponse at a bounded
#   depth; until then, catching it here is what stops the loop.
#
# Malformed numeric frames (`:abc\r\n`, `$xyz\r\n`, `*abc\r\n`) are converted to
# InvalidResponse by the pure-Python parsers (_parse_int/_parse_float below), so
# they land here too. Plain ValueError is deliberately NOT in this tuple: a
# user push handler may raise it after the frame was fully consumed, and that
# must not tear down a healthy connection.
UNRECOVERABLE_PARSE_ERRORS = (InvalidResponse, UnicodeDecodeError, RecursionError)


def _parse_int(value: bytes, raw: bytes) -> int:
    """int() for a RESP numeric field; a non-numeric field is a protocol error."""
    try:
        return int(value)
    except ValueError:
        raise InvalidResponse(f"Protocol Error: {raw!r}") from None


def _parse_float(value: bytes, raw: bytes) -> float:
    try:
        return float(value)
    except ValueError:
        raise InvalidResponse(f"Protocol Error: {raw!r}") from None


MODULE_LOAD_ERROR = "Error loading the extension. Please check the server logs."
NO_SUCH_MODULE_ERROR = "Error unloading module: no such module with that name"
MODULE_UNLOAD_NOT_POSSIBLE_ERROR = "Error unloading module: operation not possible."
MODULE_EXPORTS_DATA_TYPES_ERROR = (
    "Error unloading module: the module "
    "exports one or more module-side data "
    "types, can't unload"
)
# user send an AUTH cmd to a server without authorization configured
NO_AUTH_SET_ERROR = {
    # Redis >= 6.0
    "AUTH <password> called without any password "
    "configured for the default user. Are you sure "
    "your configuration is correct?": AuthenticationError,
    # Redis < 6.0
    "Client sent AUTH, but no password is set": AuthenticationError,
}

EXTERNAL_AUTH_PROVIDER_ERROR = {
    "problem with LDAP service": ExternalAuthProviderError,
}

# HIMPORT SET referencing a fieldset the connection has not prepared. The server
# reply is a fixed message with no fieldset name appended (verified against the
# server: always exactly ``ERR no such fieldset``), so an exact match is correct.
NO_SUCH_FIELDSET_ERROR = {
    "no such fieldset": NoSuchFieldsetError,
}

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    EXCEPTION_CLASSES = {
        "ERR": {
            "max number of clients reached": MaxClientsReachedError,
            "invalid password": AuthenticationError,
            # some Redis server versions report invalid command syntax
            # in lowercase
            "wrong number of arguments "
            "for 'auth' command": AuthenticationWrongNumberOfArgsError,
            # some Redis server versions report invalid command syntax
            # in uppercase
            "wrong number of arguments "
            "for 'AUTH' command": AuthenticationWrongNumberOfArgsError,
            MODULE_LOAD_ERROR: ModuleError,
            MODULE_EXPORTS_DATA_TYPES_ERROR: ModuleError,
            NO_SUCH_MODULE_ERROR: ModuleError,
            MODULE_UNLOAD_NOT_POSSIBLE_ERROR: ModuleError,
            **NO_AUTH_SET_ERROR,
            **EXTERNAL_AUTH_PROVIDER_ERROR,
            **NO_SUCH_FIELDSET_ERROR,
        },
        "OOM": OutOfMemoryError,
        "WRONGPASS": AuthenticationError,
        "EXECABORT": ExecAbortError,
        "LOADING": BusyLoadingError,
        "NOSCRIPT": NoScriptError,
        "READONLY": ReadOnlyError,
        "NOAUTH": AuthenticationError,
        "NOPERM": NoPermissionError,
        "ASK": AskError,
        "TRYAGAIN": TryAgainError,
        "MOVED": MovedError,
        "CLUSTERDOWN": ClusterDownError,
        "CROSSSLOT": ClusterCrossSlotError,
        "MASTERDOWN": MasterDownError,
    }

    @classmethod
    def parse_error(cls, response):
        "Parse an error response"
        error_code = response.split(" ")[0]
        if error_code in cls.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1 :]
            exception_class = cls.EXCEPTION_CLASSES[error_code]
            if isinstance(exception_class, dict):
                exception_class = exception_class.get(response, ResponseError)
            return exception_class(response, status_code=error_code)
        return ResponseError(response)

    @abstractmethod
    def on_disconnect(self):
        pass

    @abstractmethod
    def on_connect(self, connection):
        pass


class _RESPBase(BaseParser):
    """Base class for sync-based resp parsing"""

    def __init__(self, socket_read_size):
        self.socket_read_size = socket_read_size
        self.encoder = None
        self._sock = None
        self._buffer = None

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        "Called when the socket connects"
        self._sock = connection._sock
        self._buffer = SocketBuffer(
            self._sock, self.socket_read_size, connection.socket_timeout
        )
        self.encoder = connection.encoder

    def on_disconnect(self):
        "Called when the socket disconnects"
        self._sock = None
        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None
        self.encoder = None

    def can_read(self, timeout: float = 0) -> bool:
        # TODO: Rename this API; it detects pending data or dirty/closed
        # connection state, not only whether application data can be read.
        if self._buffer is None:
            return False
        return self._buffer.can_read(timeout)


class AsyncBaseParser(BaseParser):
    """Base parsing class for the python-backed async parser"""

    __slots__ = (
        "_stream",
        "_read_size",
        "_connected",
        "_read_timeout",
        "_apply_read_timeout",
        "_active_read_timeout",
    )

    def __init__(self, socket_read_size: int):
        self._stream: Optional[StreamReader] = None
        self._read_size = socket_read_size
        # Initialized here, not only in on_connect() / on_disconnect(): the
        # read guards in can_read() and the subclasses' read_response() consult
        # this flag to turn a read on a dead connection into a retryable
        # ConnectionError. A parser that has never connected would otherwise
        # raise AttributeError from those guards, which no retry layer acts on.
        self._connected: bool = False
        # Per-read timeout state, driven by the connection for the duration
        # of one read_response() call (see Connection.read_response):
        # _apply_read_timeout is set when the read falls back to
        # socket_timeout, and _read_timeout is the current deadline value,
        # which a maintenance notification may relax or restore mid-response
        # (see AbstractConnection.update_current_socket_timeout). While a
        # single stream read is in flight, its timeout context is exposed as
        # _active_read_timeout so the connection can reschedule it. Reads
        # with an explicit caller timeout do not apply per-read deadlines,
        # and a None _read_timeout never arms one.
        self._read_timeout: Optional[float] = None
        self._apply_read_timeout: bool = False
        self._active_read_timeout = None

    @deprecated_function(
        version="8.0.0", reason="Use can_read() instead", name="can_read_destructive"
    )
    @abstractmethod
    async def can_read_destructive(self) -> bool:
        pass

    @abstractmethod
    async def can_read(self) -> bool:
        # TODO: Rename this API; it detects pending data or dirty/closed
        # connection state, not only whether application data can be read.
        pass

    async def read_response(
        self, disable_decoding: bool = False
    ) -> Union[EncodableT, ResponseError, None, List[EncodableT]]:
        raise NotImplementedError()


class MaintenanceNotificationsParser:
    """Protocol defining maintenance push notification parsing functionality"""

    @staticmethod
    def parse_oss_maintenance_start_msg(response):
        # Expected message format is:
        # SMIGRATING <seq_number> <slot, range1-range2,...>
        id = response[1]
        slots = safe_str(response[2])
        return OSSNodeMigratingNotification(id, slots)

    @staticmethod
    def parse_oss_maintenance_completed_msg(response):
        # Expected message format is:
        # SMIGRATED <seq_number> [[<src_host:port> <dest_host:port> <slot_range>], ...]
        id = response[1]
        nodes_to_slots_mapping_data = response[2]
        # Build the nodes_to_slots_mapping dict structure:
        # {
        #     "src_host:port": [
        #         {"dest_host:port": "slot_range"},
        #         ...
        #     ],
        #     ...
        # }
        nodes_to_slots_mapping = {}
        for src_node, dest_node, slots in nodes_to_slots_mapping_data:
            src_node_str = safe_str(src_node)
            dest_node_str = safe_str(dest_node)
            slots_str = safe_str(slots)

            if src_node_str not in nodes_to_slots_mapping:
                nodes_to_slots_mapping[src_node_str] = []
            nodes_to_slots_mapping[src_node_str].append({dest_node_str: slots_str})

        return OSSNodeMigratedNotification(id, nodes_to_slots_mapping)

    @staticmethod
    def parse_maintenance_start_msg(response, notification_type):
        # Expected message format is: <notification_type> <seq_number> <time>
        # Examples:
        # MIGRATING 1 10
        # FAILING_OVER 2 20
        id = response[1]
        ttl = response[2]
        return notification_type(id, ttl)

    @staticmethod
    def parse_maintenance_completed_msg(response, notification_type):
        # Expected message format is: <notification_type> <seq_number>
        # Examples:
        # MIGRATED 1
        # FAILED_OVER 2
        id = response[1]
        return notification_type(id)

    @staticmethod
    def parse_moving_msg(response):
        # Expected message format is: MOVING <seq_number> <time> <endpoint>
        id = response[1]
        ttl = response[2]
        if response[3] is None:
            host, port = None, None
        else:
            value = safe_str(response[3])
            host, port = value.rsplit(":", 1)
            port = int(port) if port is not None else None

        return NodeMovingNotification(id, host, port, ttl)


_INVALIDATION_MESSAGE = "invalidate"
_MOVING_MESSAGE = "MOVING"
_MIGRATING_MESSAGE = "MIGRATING"
_MIGRATED_MESSAGE = "MIGRATED"
_FAILING_OVER_MESSAGE = "FAILING_OVER"
_FAILED_OVER_MESSAGE = "FAILED_OVER"
_SMIGRATING_MESSAGE = "SMIGRATING"
_SMIGRATED_MESSAGE = "SMIGRATED"

_MAINTENANCE_MESSAGES = (
    _MIGRATING_MESSAGE,
    _MIGRATED_MESSAGE,
    _FAILING_OVER_MESSAGE,
    _FAILED_OVER_MESSAGE,
    _SMIGRATING_MESSAGE,
)

MSG_TYPE_TO_MAINT_NOTIFICATION_PARSER_MAPPING: dict[
    str, tuple[type[MaintenanceNotification], Callable]
] = {
    _MIGRATING_MESSAGE: (
        NodeMigratingNotification,
        MaintenanceNotificationsParser.parse_maintenance_start_msg,
    ),
    _MIGRATED_MESSAGE: (
        NodeMigratedNotification,
        MaintenanceNotificationsParser.parse_maintenance_completed_msg,
    ),
    _FAILING_OVER_MESSAGE: (
        NodeFailingOverNotification,
        MaintenanceNotificationsParser.parse_maintenance_start_msg,
    ),
    _FAILED_OVER_MESSAGE: (
        NodeFailedOverNotification,
        MaintenanceNotificationsParser.parse_maintenance_completed_msg,
    ),
    _MOVING_MESSAGE: (
        NodeMovingNotification,
        MaintenanceNotificationsParser.parse_moving_msg,
    ),
    _SMIGRATING_MESSAGE: (
        OSSNodeMigratingNotification,
        MaintenanceNotificationsParser.parse_oss_maintenance_start_msg,
    ),
    _SMIGRATED_MESSAGE: (
        OSSNodeMigratedNotification,
        MaintenanceNotificationsParser.parse_oss_maintenance_completed_msg,
    ),
}


class PushNotificationsParser(Protocol):
    """Protocol defining RESP3-specific parsing functionality"""

    pubsub_push_handler_func: Callable
    invalidation_push_handler_func: Optional[Callable] = None
    node_moving_push_handler_func: Optional[Callable] = None
    maintenance_push_handler_func: Optional[Callable] = None
    oss_cluster_maint_push_handler_func: Optional[Callable] = None

    def handle_pubsub_push_response(self, response):
        """Handle pubsub push responses"""
        raise NotImplementedError()

    def handle_push_response(self, response, **kwargs):
        msg_type = response[0]
        if isinstance(msg_type, bytes):
            msg_type = msg_type.decode()

        if msg_type not in (
            _INVALIDATION_MESSAGE,
            *_MAINTENANCE_MESSAGES,
            _MOVING_MESSAGE,
            _SMIGRATED_MESSAGE,
        ):
            return self.pubsub_push_handler_func(response)

        try:
            if (
                msg_type == _INVALIDATION_MESSAGE
                and self.invalidation_push_handler_func
            ):
                return self.invalidation_push_handler_func(response)

            if msg_type == _MOVING_MESSAGE and self.node_moving_push_handler_func:
                parser_function = MSG_TYPE_TO_MAINT_NOTIFICATION_PARSER_MAPPING[
                    msg_type
                ][1]

                notification = parser_function(response)
                return self.node_moving_push_handler_func(notification)

            if msg_type in _MAINTENANCE_MESSAGES and self.maintenance_push_handler_func:
                parser_function = MSG_TYPE_TO_MAINT_NOTIFICATION_PARSER_MAPPING[
                    msg_type
                ][1]
                if msg_type == _SMIGRATING_MESSAGE:
                    notification = parser_function(response)
                else:
                    notification_type = MSG_TYPE_TO_MAINT_NOTIFICATION_PARSER_MAPPING[
                        msg_type
                    ][0]
                    notification = parser_function(response, notification_type)

                if notification is not None:
                    return self.maintenance_push_handler_func(notification)
            if msg_type == _SMIGRATED_MESSAGE and (
                self.oss_cluster_maint_push_handler_func
                or self.maintenance_push_handler_func
            ):
                parser_function = MSG_TYPE_TO_MAINT_NOTIFICATION_PARSER_MAPPING[
                    msg_type
                ][1]
                notification = parser_function(response)

                if notification is not None:
                    if self.maintenance_push_handler_func:
                        self.maintenance_push_handler_func(notification)
                    if self.oss_cluster_maint_push_handler_func:
                        self.oss_cluster_maint_push_handler_func(notification)
        except Exception as e:
            logger.error(
                "Error handling {} message ({}): {}".format(msg_type, response, e)
            )

        return None

    def set_pubsub_push_handler(self, pubsub_push_handler_func):
        self.pubsub_push_handler_func = pubsub_push_handler_func

    def set_invalidation_push_handler(self, invalidation_push_handler_func):
        self.invalidation_push_handler_func = invalidation_push_handler_func

    def set_node_moving_push_handler(self, node_moving_push_handler_func):
        self.node_moving_push_handler_func = node_moving_push_handler_func

    def set_maintenance_push_handler(self, maintenance_push_handler_func):
        self.maintenance_push_handler_func = maintenance_push_handler_func

    def set_oss_cluster_maint_push_handler(self, oss_cluster_maint_push_handler_func):
        self.oss_cluster_maint_push_handler_func = oss_cluster_maint_push_handler_func


class AsyncPushNotificationsParser(Protocol):
    """Protocol defining async RESP3-specific parsing functionality"""

    pubsub_push_handler_func: Callable
    invalidation_push_handler_func: Optional[Callable] = None
    node_moving_push_handler_func: Optional[Callable[..., Awaitable[None]]] = None
    maintenance_push_handler_func: Optional[Callable[..., Awaitable[None]]] = None
    oss_cluster_maint_push_handler_func: Optional[Callable[..., Awaitable[None]]] = None

    async def handle_pubsub_push_response(self, response):
        """Handle pubsub push responses asynchronously"""
        raise NotImplementedError()

    async def handle_push_response(self, response, **kwargs):
        """Handle push responses asynchronously"""

        msg_type = response[0]
        if isinstance(msg_type, bytes):
            msg_type = msg_type.decode()

        if msg_type not in (
            _INVALIDATION_MESSAGE,
            *_MAINTENANCE_MESSAGES,
            _MOVING_MESSAGE,
            _SMIGRATED_MESSAGE,
        ):
            return await self.pubsub_push_handler_func(response)

        try:
            if (
                msg_type == _INVALIDATION_MESSAGE
                and self.invalidation_push_handler_func
            ):
                return await self.invalidation_push_handler_func(response)

            if isinstance(msg_type, bytes):
                msg_type = msg_type.decode()

            if msg_type == _MOVING_MESSAGE and self.node_moving_push_handler_func:
                parser_function = MSG_TYPE_TO_MAINT_NOTIFICATION_PARSER_MAPPING[
                    msg_type
                ][1]
                notification = parser_function(response)
                return await self.node_moving_push_handler_func(notification)

            if msg_type in _MAINTENANCE_MESSAGES and self.maintenance_push_handler_func:
                parser_function = MSG_TYPE_TO_MAINT_NOTIFICATION_PARSER_MAPPING[
                    msg_type
                ][1]
                if msg_type == _SMIGRATING_MESSAGE:
                    notification = parser_function(response)
                else:
                    notification_type = MSG_TYPE_TO_MAINT_NOTIFICATION_PARSER_MAPPING[
                        msg_type
                    ][0]
                    notification = parser_function(response, notification_type)

                if notification is not None:
                    return await self.maintenance_push_handler_func(notification)
            if (
                msg_type == _SMIGRATED_MESSAGE
                and self.oss_cluster_maint_push_handler_func
            ):
                parser_function = MSG_TYPE_TO_MAINT_NOTIFICATION_PARSER_MAPPING[
                    msg_type
                ][1]
                notification = parser_function(response)
                if notification is not None:
                    return await self.oss_cluster_maint_push_handler_func(notification)
        except Exception as e:
            logger.error(
                "Error handling {} message ({}): {}".format(msg_type, response, e)
            )

        return None

    def set_pubsub_push_handler(self, pubsub_push_handler_func):
        """Set the pubsub push handler function"""
        self.pubsub_push_handler_func = pubsub_push_handler_func

    def set_invalidation_push_handler(self, invalidation_push_handler_func):
        """Set the invalidation push handler function"""
        self.invalidation_push_handler_func = invalidation_push_handler_func

    def set_node_moving_push_handler(self, node_moving_push_handler_func):
        self.node_moving_push_handler_func = node_moving_push_handler_func

    def set_maintenance_push_handler(self, maintenance_push_handler_func):
        self.maintenance_push_handler_func = maintenance_push_handler_func

    def set_oss_cluster_maint_push_handler(self, oss_cluster_maint_push_handler_func):
        self.oss_cluster_maint_push_handler_func = oss_cluster_maint_push_handler_func


class _AsyncRESPBase(AsyncBaseParser):
    """Base class for async resp parsing"""

    __slots__ = AsyncBaseParser.__slots__ + ("encoder", "_buffer", "_pos", "_chunks")

    def __init__(self, socket_read_size: int):
        super().__init__(socket_read_size)
        self.encoder: Optional[Encoder] = None
        self._buffer = b""
        self._chunks = []
        self._pos = 0

    def _clear(self):
        self._buffer = b""
        self._chunks.clear()

    def on_connect(self, connection):
        """Called when the stream connects"""
        self._stream = connection._reader
        if self._stream is None:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        self.encoder = connection.encoder
        self._clear()
        self._connected = True

    def on_disconnect(self):
        """Called when the stream disconnects"""
        self._connected = False

    @deprecated_function(
        version="8.0.0",
        reason="Use can_read() instead",
        name="can_read_destructive",
    )
    async def can_read_destructive(self) -> bool:
        return await self.can_read()

    async def can_read(self) -> bool:
        # TODO: Rename this API; it detects pending data or dirty/closed
        # connection state, not only whether application data can be read.
        if not self._connected:
            raise OSError("Buffer is closed.")
        # buffered data wins over EOF, like the sync SocketBuffer: a pending
        # response or push notification must stay readable even if the server
        # has since closed the connection.
        if self._buffer:
            return True
        if self._stream.at_eof() or self._stream.exception() is not None:
            # Raise like the sync SocketBuffer does on a server-closed
            # connection, so callers that tolerate pending data (push
            # notifications) can't mistake EOF for a readable connection.
            # A reset (RST) leaves no EOF but an exception on the stream.
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        # asyncio.StreamReader has no public non-destructive API for checking
        # buffered bytes. Preserve dirty-connection detection for the Python
        # parser and fail loudly if the private buffer API changes.
        return bool(self._stream._buffer)

    async def _read(self, length: int) -> bytes:
        """
        Read `length` bytes of data.  These are assumed to be followed
        by a '\r\n' terminator which is subsequently discarded.
        """
        want = length + 2
        end = self._pos + want
        if len(self._buffer) >= end:
            result = self._buffer[self._pos : end - 2]
        else:
            tail = self._buffer[self._pos :]
            stream = self._stream
            try:
                remaining = want - len(tail)
                if (
                    self._apply_read_timeout
                    and self._read_timeout is not None
                    and len(stream._buffer) < remaining
                ):
                    # The stream does not already hold the whole remainder,
                    # so this read can wait: _read_exactly bounds every
                    # wait by its own socket_timeout window.
                    data = await self._read_exactly(remaining)
                else:
                    # Every other case is served by readexactly. No
                    # per-read deadline applies (explicit caller timeout
                    # or blocking read), a None socket_timeout can never
                    # fire, or the stream already holds the whole
                    # remainder so the read cannot wait at all; a cancelled
                    # readexactly leaves its bytes on the stream, so the
                    # next poll resumes from the stream instead of
                    # re-joining and re-parsing saved pieces on every poll.
                    # Gating the length check on the two flag checks above
                    # also keeps the private buffer access - and its cost
                    # per element of an aggregate reply - off the paths
                    # that cannot wait, notably socket_timeout=None.
                    data = await stream.readexactly(remaining)
                    self._chunks.append(data)
            except IncompleteReadError as error:
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from error
            result = (tail + data)[:-2]
            # The bytes read from the stream were recorded in the resume
            # state by either the readexactly path or _read_exactly.
        self._pos += want
        return result

    async def _read_exactly(self, length: int) -> bytes:
        """Read exactly `length` bytes from the stream.

        The reads use StreamReader.read, which returns as soon as any
        bytes are available, mirroring the sync SocketBuffer, whose recv()
        does the same; readexactly would instead wait for a whole piece
        even when bytes keep arriving. Each read that can actually wait
        is bounded by its own socket_timeout window, so a large payload
        whose pieces keep arriving does not time out merely because the
        whole payload takes longer than socket_timeout to arrive.

        This only runs on the default read path, where per-read timeouts
        apply (see _read): explicit and blocking reads use readexactly,
        whose cancelled reads leave their bytes on the stream for the
        next poll. The pieces are kept in a local list, and only the
        joined buffer is added to self._chunks once the whole payload
        has arrived, so the resume state never holds the pieces plus a
        second full copy of the payload. If the read unwinds early (a
        per-read timeout or a cancellation), the joined pieces are added
        to self._chunks instead: a timeout on the default path usually
        disconnects, but callers that pass disconnect_on_error=False
        (PubSub and push reads) keep the connection and resume by
        re-parsing the saved header, so bytes already pulled off the
        stream must survive in the resume state or the resumed read
        consumes bytes from the following reply.
        """
        remaining = length
        pieces = []
        active_timeout = None
        try:
            while remaining > 0:
                # StreamReader.read returns as soon as any bytes are buffered.
                # Check the flags first, avoiding private stream access unless
                # per-read timeouts apply. StreamReader.read can only wait while
                # its bytearray buffer is empty; only then arm the timeout.
                if (
                    self._apply_read_timeout
                    and self._read_timeout is not None
                    and not self._stream._buffer
                ):
                    if active_timeout is None:
                        active_timeout = async_timeout(self._read_timeout)
                        await active_timeout.__aenter__()
                        self._active_read_timeout = active_timeout
                    else:
                        deadline = get_running_loop().time() + self._read_timeout
                        if hasattr(active_timeout, "reschedule"):
                            active_timeout.reschedule(deadline)
                        elif hasattr(active_timeout, "update"):
                            active_timeout.update(deadline)
                chunk = await self._stream.read(remaining)
                if not chunk:
                    # EOF before `length` bytes: report like readexactly did,
                    # so callers keep raising ConnectionError.
                    partial = b"".join(pieces)
                    raise IncompleteReadError(partial, length)
                pieces.append(chunk)
                remaining -= len(chunk)
        except BaseException:
            # The read unwound with its pieces only in this local list,
            # but the bytes are already off the stream. Retain them in
            # the resume state (see the docstring): a caller keeping the
            # connection resumes by re-parsing the saved header, and
            # without the pieces it would read into the following reply.
            if pieces:
                self._chunks.append(pieces[0] if len(pieces) == 1 else b"".join(pieces))
            raise
        finally:
            if active_timeout is not None:
                try:
                    await active_timeout.__aexit__(*sys.exc_info())
                finally:
                    self._active_read_timeout = None
        if len(pieces) == 1:
            joined = pieces[0]
        else:
            joined = b"".join(pieces)
        self._chunks.append(joined)
        return joined

    async def _readline(self) -> bytes:
        """
        read an unknown number of bytes up to the next '\r\n'
        line separator, which is discarded.
        """
        found = self._buffer.find(b"\r\n", self._pos)
        if found >= 0:
            result = self._buffer[self._pos : found]
        else:
            tail = self._buffer[self._pos :]
            stream = self._stream
            read_timeout = self._read_timeout
            # Each line gets a fresh socket_timeout window, like each read
            # in _read_exactly: the deadline must not be shared across all
            # the lines of one response. Check the flags first to avoid
            # private stream access when per-read timeouts do not apply.
            # readline can only wait while the stream's bytearray buffer
            # holds no line end, so an aggregate reply whose lines are
            # already buffered pays only the flag checks and the (short,
            # first-match) needle scan per line - no timeout context is
            # built for a read that cannot wait.
            if (
                self._apply_read_timeout
                and read_timeout is not None
                # int needle: ~8ns vs ~80ns for a bytes needle on a bytearray
                and 10 not in stream._buffer
            ):
                async with async_timeout(read_timeout) as active_timeout:
                    self._active_read_timeout = active_timeout
                    try:
                        data = await stream.readline()
                    finally:
                        self._active_read_timeout = None
            else:
                data = await stream.readline()
            if not data.endswith(b"\r\n"):
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
            result = (tail + data)[:-2]
            self._chunks.append(data)
        self._pos += len(result) + 2
        return result
