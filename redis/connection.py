import copy
import functools
import ipaddress
import logging
import os
import secrets
import socket
import sys
import threading
import time
import weakref
from abc import ABC, abstractmethod
from itertools import chain
from queue import Empty, Full, LifoQueue
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Type,
    TypeVar,
    Union,
)
from urllib.parse import parse_qs, unquote, urlparse

from redis.cache import (
    CacheConfig,
    CacheEntry,
    CacheEntryStatus,
    CacheFactory,
    CacheFactoryInterface,
    CacheInterface,
    CacheKey,
    CacheProxy,
    TrackingMode,
)
from redis.commands.metadata import MetadataResolver

from . import exceptions as redis_exceptions
from ._defaults import (
    DEFAULT_SOCKET_CONNECT_TIMEOUT,
    DEFAULT_SOCKET_READ_SIZE,
    DEFAULT_SOCKET_TIMEOUT,
    get_default_socket_keepalive_options,
)
from ._parsers import (
    UNRECOVERABLE_PARSE_ERRORS,
    BaseParser,
    Encoder,
    _HiredisParser,
    _RESP2Parser,
    _RESP3Parser,
)
from .auth.token import TokenInterface
from .backoff import NoBackoff
from .credentials import CredentialProvider, UsernamePasswordCredentialProvider
from .driver_info import DriverInfo, resolve_driver_info
from .event import (
    AfterConnectionReleasedEvent,
    EventDispatcher,
    MaintenanceCompletedEvent,
)
from .exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    ChildDeadlockedError,
    ConnectionError,
    DataError,
    MaxConnectionsError,
    RedisError,
    ResponseError,
    TimeoutError,
)
from .himport import HImportRegistry
from .maint_notifications import (
    PENDING_PUSH_NOTIFICATIONS_READ_TIMEOUT,
    MaintenanceState,
    MaintNotificationsConfig,
    MaintNotificationsConnectionHandler,
    MaintNotificationsPoolHandler,
    OSSMaintNotificationsHandler,
    _dispatch_maintenance_event,
)
from .observability.attributes import (
    DB_CLIENT_CONNECTION_POOL_NAME,
    DB_CLIENT_CONNECTION_STATE,
    AttributeBuilder,
    ConnectionState,
    CSCReason,
    CSCResult,
    get_pool_name,
)
from .observability.metrics import CloseReason
from .observability.recorder import (
    init_csc_items,
    record_connection_closed,
    record_connection_count,
    record_connection_create_time,
    record_connection_timeout,
    record_connection_wait_time,
    record_csc_eviction,
    record_csc_network_saved,
    record_csc_request,
    record_error_count,
    register_csc_items_callback,
)
from .retry import Retry
from .utils import (
    CRYPTOGRAPHY_AVAILABLE,
    DEFAULT_RESP_VERSION,
    HIREDIS_AVAILABLE,
    SENTINEL,
    SSL_AVAILABLE,
    check_protocol_version,
    compare_versions,
    deprecated_args,
    ensure_string,
    format_error_message,
    is_os_level_timeout,
    str_if_bytes,
)

if SSL_AVAILABLE:
    import ssl
    from ssl import VerifyFlags
else:
    ssl = None
    VerifyFlags = None

if HIREDIS_AVAILABLE:
    import hiredis

SYM_STAR = b"*"
SYM_DOLLAR = b"$"
SYM_CRLF = b"\r\n"
SYM_EMPTY = b""

DefaultParser: Type[Union[_RESP2Parser, _RESP3Parser, _HiredisParser]]
if HIREDIS_AVAILABLE:
    DefaultParser = _HiredisParser
else:
    DefaultParser = _RESP2Parser

logger = logging.getLogger(__name__)


def add_debug_log_for_connection_failure(
    connection: "AbstractConnection",
    error: BaseException,
    operation: str,
) -> None:
    """
    Render the connection's live state on a failure that is about to close it.

    Must be called *before* ``disconnect()``. ``extract_connection_details()``
    reads the resolved ip, local port and armed read timeout off the socket, so
    once the socket is gone it can only report ``not connected`` - which hides
    exactly the state needed to explain the failure. In particular it is what
    tells apart a read that ran under the original timeout from one that ran
    under a relaxed maintenance timeout.
    """
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            f"{type(error).__name__} while {operation}, "
            f"with connection: {connection}, "
            f"details: {connection.extract_connection_details()}, "
            f"error: {error}",
        )


class HiredisRespSerializer:
    def __init__(self, fallback=None, encode=None):
        self._fallback = fallback
        self._encode = encode

    def pack(self, *args: List):
        """Pack a series of arguments into the Redis protocol"""
        if self._fallback is not None and any(
            isinstance(arg, memoryview) for arg in args
        ):
            return self._fallback(*args)
        output = []

        if isinstance(args[0], str):
            args = tuple(args[0].encode().split()) + args[1:]
        elif b" " in args[0]:
            args = tuple(args[0].split()) + args[1:]
        args = tuple(
            bytes(arg)
            if isinstance(arg, (bytearray, memoryview))
            else self._encode(arg)
            if self._encode is not None
            else arg
            for arg in args
        )
        try:
            output.append(hiredis.pack_command(args))
        except TypeError:
            _, value, traceback = sys.exc_info()
            raise DataError(value).with_traceback(traceback)

        return output


class PythonRespSerializer:
    def __init__(self, buffer_cutoff, encode) -> None:
        self._buffer_cutoff = buffer_cutoff
        self.encode = encode

    def pack(self, *args):
        """Pack a series of arguments into the Redis protocol"""
        output = []
        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. These arguments should be bytestrings so that they are
        # not encoded.
        if isinstance(args[0], str):
            args = tuple(args[0].encode().split()) + args[1:]
        elif b" " in args[0]:
            args = tuple(args[0].split()) + args[1:]

        buff = SYM_EMPTY.join((SYM_STAR, str(len(args)).encode(), SYM_CRLF))

        buffer_cutoff = self._buffer_cutoff
        for arg in map(self.encode, args):
            # to avoid large string mallocs, chunk the command into the
            # output list if we're sending large values or memoryviews
            arg_length = len(arg)
            if (
                len(buff) > buffer_cutoff
                or arg_length > buffer_cutoff
                or isinstance(arg, memoryview)
            ):
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, str(arg_length).encode(), SYM_CRLF)
                )
                output.append(buff)
                output.append(arg)
                buff = SYM_CRLF
            else:
                buff = SYM_EMPTY.join(
                    (
                        buff,
                        SYM_DOLLAR,
                        str(arg_length).encode(),
                        SYM_CRLF,
                        arg,
                        SYM_CRLF,
                    )
                )
        output.append(buff)
        return output


class ConnectionInterface:
    @abstractmethod
    def repr_pieces(self):
        pass

    @abstractmethod
    def register_connect_callback(self, callback):
        pass

    @abstractmethod
    def deregister_connect_callback(self, callback):
        pass

    @abstractmethod
    def set_parser(self, parser_class):
        pass

    @abstractmethod
    def get_protocol(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def on_connect(self):
        pass

    @abstractmethod
    def disconnect(self, *args, **kwargs):
        pass

    @abstractmethod
    def check_health(self):
        pass

    @abstractmethod
    def send_packed_command(self, command, check_health=True):
        pass

    @abstractmethod
    def send_command(self, *args, **kwargs):
        pass

    @abstractmethod
    def can_read(self, timeout: float = 0) -> bool:
        # TODO: Rename this API; it detects pending data or dirty/closed
        # connection state, not only whether application data can be read.
        pass

    @abstractmethod
    def read_response(
        self,
        disable_decoding=False,
        *,
        timeout: Union[float, object] = SENTINEL,
        disconnect_on_error=True,
        push_request=False,
    ):
        pass

    @abstractmethod
    def pack_command(self, *args):
        pass

    @abstractmethod
    def pack_commands(self, commands):
        pass

    @property
    @abstractmethod
    def handshake_metadata(self) -> Union[Dict[bytes, bytes], Dict[str, str]]:
        pass

    @abstractmethod
    def set_re_auth_token(self, token: TokenInterface):
        pass

    @abstractmethod
    def re_auth(self):
        pass

    @abstractmethod
    def mark_for_reconnect(self):
        """
        Mark the connection to be reconnected on the next command.
        This is useful when a connection is moved to a different node.
        """
        pass

    @abstractmethod
    def should_reconnect(self):
        """
        Returns True if the connection should be reconnected.
        """
        pass

    @abstractmethod
    def reset_should_reconnect(self):
        """
        Reset the internal flag to False.
        """
        pass

    @abstractmethod
    def extract_connection_details(self) -> str:
        pass

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """
        Return ``True`` if the connection to the server is active.
        """
        pass


class MaintNotificationsAbstractConnection:
    """
    Abstract class for handling maintenance notifications logic.
    This class is expected to be used as base class together with ConnectionInterface.

    This class is intended to be used with multiple inheritance!

    All logic related to maintenance notifications is encapsulated in this class.
    """

    def __init__(
        self,
        maint_notifications_config: Optional[MaintNotificationsConfig],
        maint_notifications_pool_handler: Optional[
            MaintNotificationsPoolHandler
        ] = None,
        maintenance_state: "MaintenanceState" = MaintenanceState.NONE,
        maintenance_notification_hash: Optional[int] = None,
        orig_host_address: Optional[str] = None,
        orig_socket_timeout: Optional[float] = None,
        orig_socket_connect_timeout: Optional[float] = None,
        oss_cluster_maint_notifications_handler: Optional[
            OSSMaintNotificationsHandler
        ] = None,
        parser: Optional[BaseParser] = None,
        event_dispatcher: Optional[EventDispatcher] = None,
    ):
        """
        Initialize the maintenance notifications for the connection.

        Args:
            maint_notifications_config (MaintNotificationsConfig): The configuration for maintenance notifications.
            maint_notifications_pool_handler (Optional[MaintNotificationsPoolHandler]): The pool handler for maintenance notifications.
            maintenance_state (MaintenanceState): The current maintenance state of the connection.
            maintenance_notification_hash (Optional[int]): The current maintenance notification hash of the connection.
            orig_host_address (Optional[str]): The original host address of the connection.
            orig_socket_timeout (Optional[float]): The original socket timeout of the connection.
            orig_socket_connect_timeout (Optional[float]): The original socket connect timeout of the connection.
            oss_cluster_maint_notifications_handler (Optional[OSSMaintNotificationsHandler]): The OSS cluster handler for maintenance notifications.
            parser (Optional[BaseParser]): The parser to use for maintenance notifications.
                    If not provided, the parser from the connection is used.
                    This is useful when the parser is created after this object.
        """
        self.maint_notifications_config = maint_notifications_config
        self.maintenance_state = maintenance_state
        self.maintenance_notification_hash = maintenance_notification_hash

        if event_dispatcher is not None:
            self.event_dispatcher = event_dispatcher
        else:
            self.event_dispatcher = EventDispatcher()

        self._configure_maintenance_notifications(
            maint_notifications_pool_handler,
            orig_host_address,
            orig_socket_timeout,
            orig_socket_connect_timeout,
            oss_cluster_maint_notifications_handler,
            parser,
        )
        self._processed_start_maint_notifications = set()
        self._skipped_end_maint_notifications = set()

    @abstractmethod
    def _get_parser(self) -> BaseParser:
        pass

    def _get_push_notifications_parser(self) -> Union[_HiredisParser, _RESP3Parser]:
        parser = self._get_parser()
        if not isinstance(parser, (_HiredisParser, _RESP3Parser)):
            raise RedisError(
                "Maintenance notifications are only supported with hiredis and RESP3 parsers!"
            )
        return parser

    def _complete_maintenance_on_disconnect(self) -> None:
        """
        Restore the settings a maintenance relaxed on this connection when it is
        closed, and report the relaxation as completed - the same way the
        completion notification would have been reported. The MOVING state is
        left alone: it is owned by the pool-level TTL cleanup.
        """
        if self.maintenance_state != MaintenanceState.MAINTENANCE:
            return
        self.reset_tmp_settings(reset_relaxed_timeout=True)
        self.maintenance_state = MaintenanceState.NONE
        # reset the sets that keep track of received start maint
        # notifications and skipped end maint notifications
        self.reset_received_notifications()
        if self.maint_notifications_config is not None:
            pool_handler = self._maint_notifications_pool_handler
            _dispatch_maintenance_event(
                self.maint_notifications_config,
                MaintenanceCompletedEvent(
                    connection_pool=(
                        pool_handler.pool if pool_handler is not None else None
                    ),
                    connection=self,
                    state=MaintenanceState.MAINTENANCE,
                    notification=None,
                    config=self.maint_notifications_config,
                ),
            )

    def handle_pending_push_notifications(self) -> None:
        """
        Process the push notifications buffered on this idle connection if it
        is still alive, and report a connection that has to be reconnected.

        A pooled connection that has readable data while idle either has push
        notifications waiting, or has been closed by the server - or both, when
        the notifications announced the maintenance that closed it. The socket
        state cannot be told apart from the pending data without reading it, so
        the notifications are collected first and handed to their handlers only
        once the drain has shown the socket to be alive: the server closes the
        connections of a moved endpoint when the MOVING time-to-live expires,
        so an alive socket means the maintenance is still in progress and the
        announced address current. A notification buffered on a socket the
        server has since closed describes a maintenance that is over, after
        which the configured address is what to connect to again: it is
        discarded, and the ``ConnectionError`` raised here makes the pool
        reconnect before any command is sent.

        Anything read that is not a maintenance notification or a client-side
        cache invalidation - a reply, a null or an error reply left unread by an
        earlier command - means the connection is dirty and is reported the same
        way, as is a drain that does not complete within
        ``PENDING_PUSH_NOTIFICATIONS_READ_TIMEOUT``.
        """
        parser = self._get_push_notifications_parser()
        deferred: list = []

        def defer(handler):
            def deferred_handler(notification):
                deferred.append((handler, notification))

            return deferred_handler

        handlers = (
            (parser.node_moving_push_handler_func, parser.set_node_moving_push_handler),
            (parser.maintenance_push_handler_func, parser.set_maintenance_push_handler),
            (
                parser.oss_cluster_maint_push_handler_func,
                parser.set_oss_cluster_maint_push_handler,
            ),
            # Client-side cache invalidations are pushes too; the ones on a
            # connection that has to be reconnected are covered by the cache
            # flush its disconnect performs
            (
                parser.invalidation_push_handler_func,
                parser.set_invalidation_push_handler,
            ),
        )
        for handler, set_handler in handlers:
            if handler is not None:
                set_handler(defer(handler))

        alive = False
        deadline = time.monotonic() + PENDING_PUSH_NOTIFICATIONS_READ_TIMEOUT
        try:
            while self.can_read():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ConnectionError(
                        "Timed out reading pending push notifications"
                    )
                read = len(deferred)
                # The pool disconnects the connection it handed out - with
                # caching the proxy, not the wrapped connection - on the error.
                # The parsers apply the timeout to each receive of a frame; the
                # deadline bounds the drain across frames.
                self.read_response(
                    push_request=True,
                    timeout=remaining,
                    disconnect_on_error=False,
                )
                # Only a push reaches a deferred handler; anything else is a
                # reply left unread by an earlier command
                if len(deferred) == read:
                    raise ConnectionError("Connection has data")
            alive = True
        except ResponseError as e:
            raise ConnectionError("Connection has data") from e
        finally:
            for handler, set_handler in handlers:
                set_handler(handler)
            if not alive and deferred and logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Discarding {len(deferred)} push notification(s) buffered on "
                    f"a connection that is closed or dirty, reconnecting: {self}"
                )

        for handler, notification in deferred:
            try:
                handler(notification)
            except Exception as e:
                logger.error(f"Error handling {notification}: {e}")

    @abstractmethod
    def _get_socket(self) -> Optional[socket.socket]:
        pass

    @abstractmethod
    def get_protocol(self) -> Union[int, str]:
        """
        Returns:
            The RESP protocol version, or ``None`` if the protocol is not specified,
            in which case the server default will be used.
        """
        pass

    @property
    @abstractmethod
    def host(self) -> str:
        pass

    @host.setter
    @abstractmethod
    def host(self, value: str):
        pass

    @property
    @abstractmethod
    def socket_timeout(self) -> Optional[Union[float, int]]:
        pass

    @socket_timeout.setter
    @abstractmethod
    def socket_timeout(self, value: Optional[Union[float, int]]):
        pass

    @property
    @abstractmethod
    def socket_connect_timeout(self) -> Optional[Union[float, int]]:
        pass

    @socket_connect_timeout.setter
    @abstractmethod
    def socket_connect_timeout(self, value: Optional[Union[float, int]]):
        pass

    @abstractmethod
    def send_command(self, *args, **kwargs):
        pass

    @abstractmethod
    def read_response(
        self,
        disable_decoding=False,
        *,
        timeout: Union[float, object] = SENTINEL,
        disconnect_on_error=True,
        push_request=False,
    ):
        pass

    @abstractmethod
    def disconnect(self, *args, **kwargs):
        pass

    @abstractmethod
    def mark_for_reconnect(self):
        pass

    def _configure_maintenance_notifications(
        self,
        maint_notifications_pool_handler: Optional[
            MaintNotificationsPoolHandler
        ] = None,
        orig_host_address=None,
        orig_socket_timeout=None,
        orig_socket_connect_timeout=None,
        oss_cluster_maint_notifications_handler: Optional[
            OSSMaintNotificationsHandler
        ] = None,
        parser: Optional[BaseParser] = None,
    ):
        """
        Enable maintenance notifications by setting up
        handlers and storing original connection parameters.

        Should be used ONLY with parsers that support push notifications.
        """
        if (
            not self.maint_notifications_config
            or not self.maint_notifications_config.enabled
        ):
            self._maint_notifications_pool_handler = None
            self._maint_notifications_connection_handler = None
            self._oss_cluster_maint_notifications_handler = None
            return

        if not parser:
            raise RedisError(
                "To configure maintenance notifications, a parser must be provided!"
            )

        if not isinstance(parser, _HiredisParser) and not isinstance(
            parser, _RESP3Parser
        ):
            raise RedisError(
                "Maintenance notifications are only supported with hiredis and RESP3 parsers!"
            )

        if maint_notifications_pool_handler:
            # Extract a reference to a new pool handler that copies all properties
            # of the original one and has a different connection reference
            # This is needed because when we attach the handler to the parser
            # we need to make sure that the handler has a reference to the
            # connection that the parser is attached to.
            self._maint_notifications_pool_handler = (
                maint_notifications_pool_handler.get_handler_for_connection()
            )
            self._maint_notifications_pool_handler.set_connection(self)
        else:
            self._maint_notifications_pool_handler = None

        self._maint_notifications_connection_handler = (
            MaintNotificationsConnectionHandler(self, self.maint_notifications_config)
        )

        if oss_cluster_maint_notifications_handler:
            self._oss_cluster_maint_notifications_handler = (
                oss_cluster_maint_notifications_handler
            )
            # Set up OSS cluster handler to parser
            parser.set_oss_cluster_maint_push_handler(
                self._oss_cluster_maint_notifications_handler.handle_notification
            )
        else:
            self._oss_cluster_maint_notifications_handler = None

        # Set up pool handler to parser if available
        if self._maint_notifications_pool_handler:
            parser.set_node_moving_push_handler(
                self._maint_notifications_pool_handler.handle_notification
            )

        # Set up connection handler
        parser.set_maintenance_push_handler(
            self._maint_notifications_connection_handler.handle_notification
        )

        # Store original connection parameters
        self.orig_host_address = orig_host_address if orig_host_address else self.host
        self.orig_socket_timeout = (
            orig_socket_timeout if orig_socket_timeout else self.socket_timeout
        )
        self.orig_socket_connect_timeout = (
            orig_socket_connect_timeout
            if orig_socket_connect_timeout
            else self.socket_connect_timeout
        )

    def set_maint_notifications_pool_handler_for_connection(
        self, maint_notifications_pool_handler: MaintNotificationsPoolHandler
    ):
        # Deep copy the pool handler to avoid sharing the same pool handler
        # between multiple connections, because otherwise each connection will override
        # the connection reference and the pool handler will only hold a reference
        # to the last connection that was set.
        maint_notifications_pool_handler_copy = (
            maint_notifications_pool_handler.get_handler_for_connection()
        )

        maint_notifications_pool_handler_copy.set_connection(self)
        parser = self._get_push_notifications_parser()
        parser.set_node_moving_push_handler(
            maint_notifications_pool_handler_copy.handle_notification
        )

        self._maint_notifications_pool_handler = maint_notifications_pool_handler_copy

        # Update maintenance notification connection handler if it doesn't exist
        if not self._maint_notifications_connection_handler:
            self._maint_notifications_connection_handler = (
                MaintNotificationsConnectionHandler(
                    self, maint_notifications_pool_handler.config
                )
            )
            parser.set_maintenance_push_handler(
                self._maint_notifications_connection_handler.handle_notification
            )
        else:
            self._maint_notifications_connection_handler.config = (
                maint_notifications_pool_handler.config
            )

    def set_maint_notifications_cluster_handler_for_connection(
        self, oss_cluster_maint_notifications_handler: OSSMaintNotificationsHandler
    ):
        parser = self._get_push_notifications_parser()
        parser.set_oss_cluster_maint_push_handler(
            oss_cluster_maint_notifications_handler.handle_notification
        )
        # OSS cluster mode and pool-handler mode are mutually exclusive. Clear
        # any node-moving/pool handler a default (RESP3 "auto") pool wired in
        # __init__ so this existing connection is not configured with both.
        parser.set_node_moving_push_handler(None)
        self._maint_notifications_pool_handler = None

        self._oss_cluster_maint_notifications_handler = (
            oss_cluster_maint_notifications_handler
        )

        # Update maintenance notification connection handler if it doesn't exist
        if not self._maint_notifications_connection_handler:
            self._maint_notifications_connection_handler = (
                MaintNotificationsConnectionHandler(
                    self, oss_cluster_maint_notifications_handler.config
                )
            )
            parser.set_maintenance_push_handler(
                self._maint_notifications_connection_handler.handle_notification
            )
        else:
            self._maint_notifications_connection_handler.config = (
                oss_cluster_maint_notifications_handler.config
            )

    def _should_enable_maint_notifications(self) -> bool:
        # Maintenance notifications are sent only if RESP3 is active
        # and maintenance notifications are enabled
        # and we have a host to determine the endpoint type from.
        host = getattr(self, "host", None)
        return bool(
            check_protocol_version(self.get_protocol(), 3)
            and self.maint_notifications_config
            and self.maint_notifications_config.enabled
            and self._maint_notifications_connection_handler
            and host is not None
        )

    def activate_maint_notifications_handling_if_enabled(self, check_health=True):
        # When the maint_notifications_config enabled mode is "auto",
        # we just log a warning if the handshake fails
        # When the mode is enabled=True, we raise an exception in case of failure
        if self._should_enable_maint_notifications():
            self._enable_maintenance_notifications(
                maint_notifications_config=self.maint_notifications_config,
                check_health=check_health,
            )

    def _enable_maintenance_notifications(
        self, maint_notifications_config: MaintNotificationsConfig, check_health=True
    ):
        # Kept for callers that enable maintenance notifications outside of the
        # connection handshake. During on_connect the send and the response
        # handling are split (see _send_maint_notifications_command /
        # _handle_maint_notifications_response) so the reply can be pipelined
        # with the rest of the handshake.
        self._send_maint_notifications_command(
            maint_notifications_config, check_health=check_health
        )
        self._handle_maint_notifications_response(maint_notifications_config)

    def _maint_notifications_command_args(
        self, maint_notifications_config: MaintNotificationsConfig
    ):
        host = getattr(self, "host", None)
        if host is None:
            raise ValueError(
                "Cannot enable maintenance notifications for connection"
                " object that doesn't have a host attribute."
            )
        endpoint_type = maint_notifications_config.get_endpoint_type(host, self)
        return (
            "CLIENT",
            "MAINT_NOTIFICATIONS",
            "ON",
            "moving-endpoint-type",
            endpoint_type.value,
        )

    def _send_maint_notifications_command(
        self, maint_notifications_config: MaintNotificationsConfig, check_health=True
    ):
        self.send_command(
            *self._maint_notifications_command_args(maint_notifications_config),
            check_health=check_health,
        )

    def _add_maint_notifications_to_handshake(self, deferred_reads, check_health=True):
        # If maintenance notifications are enabled for this connection, send the
        # CLIENT MAINT_NOTIFICATIONS command with the rest of the handshake tail and
        # defer reading its reply (appended to deferred_reads), rather than paying its
        # own round-trip. When enabled == "auto" a failure is logged and swallowed;
        # when enabled is True it raises.
        if not self._should_enable_maint_notifications():
            return
        maint_notifications_config = self.maint_notifications_config
        self._send_maint_notifications_command(
            maint_notifications_config, check_health=check_health
        )
        deferred_reads.append(
            lambda: self._handle_maint_notifications_response(
                maint_notifications_config
            )
        )

    def _handle_maint_notifications_response(
        self, maint_notifications_config: MaintNotificationsConfig
    ):
        try:
            response = self.read_response()
            if not response or str_if_bytes(response) != "OK":
                raise ResponseError(
                    "The server doesn't support maintenance notifications"
                )
        except Exception as e:
            if (
                isinstance(e, ResponseError)
                and maint_notifications_config.enabled == "auto"
            ):
                # Log warning but don't fail the connection
                logger.debug(f"Failed to enable maintenance notifications: {e}")
            else:
                raise

    def _read_ok_or_raise(self, error_message):
        # Read one handshake reply and require it to be "OK".
        if str_if_bytes(self.read_response()) != "OK":
            raise ConnectionError(error_message)

    def _read_optional_setinfo(self):
        # Read one CLIENT SETINFO reply. Older servers may not support the command,
        # so a ResponseError is swallowed instead of failing the connection.
        try:
            self.read_response()
        except ResponseError:
            pass

    def get_resolved_ip(self) -> Optional[str]:
        """
        Extract the resolved IP address from an
        established connection or resolve it from the host.

        First tries to get the actual IP from the socket (most accurate),
        then falls back to DNS resolution if needed.

        Returns:
            str: The resolved IP address, or None if it cannot be determined
        """

        # Method 1: Try to get the actual IP from the established socket connection
        # This is most accurate as it shows the exact IP being used
        try:
            conn_socket = self._get_socket()
            if conn_socket is not None:
                peer_addr = conn_socket.getpeername()
                # For TCP sockets, peer_addr is typically a (host, port) tuple.
                # AF_UNIX sockets report a path string instead, and indexing it
                # would yield the first character of the path.
                if isinstance(peer_addr, tuple) and peer_addr:
                    # Return just the host part
                    return peer_addr[0]
        except (AttributeError, OSError):
            # Socket might not be connected or getpeername() might fail
            pass

        # Method 2: Fallback to DNS resolution of the host
        # This is less accurate but works when socket is not available
        try:
            host = getattr(self, "host", "localhost")
            port = getattr(self, "port", 6379)
            if host:
                # Use getaddrinfo to resolve the hostname to IP
                # This mimics what the connection would do during _connect()
                addr_info = socket.getaddrinfo(
                    host, port, socket.AF_UNSPEC, socket.SOCK_STREAM
                )
                if addr_info:
                    # Return the IP from the first result
                    # addr_info[0] is (family, socktype, proto, canonname, sockaddr)
                    # sockaddr[0] is the IP address
                    return str(addr_info[0][4][0])
        except (AttributeError, OSError, socket.gaierror):
            # DNS resolution might fail
            pass

        return None

    @property
    def maintenance_state(self) -> MaintenanceState:
        return self._maintenance_state

    @maintenance_state.setter
    def maintenance_state(self, state: "MaintenanceState"):
        self._maintenance_state = state

    def add_maint_start_notification(self, id: int):
        self._processed_start_maint_notifications.add(id)

    def get_processed_start_notifications(self) -> set:
        return self._processed_start_maint_notifications

    def add_skipped_end_notification(self, id: int):
        self._skipped_end_maint_notifications.add(id)

    def get_skipped_end_notifications(self) -> set:
        return self._skipped_end_maint_notifications

    def reset_received_notifications(self):
        self._processed_start_maint_notifications.clear()
        self._skipped_end_maint_notifications.clear()

    def getpeername(self):
        """
        Returns the peer name of the connection.
        """
        conn_socket = self._get_socket()
        if conn_socket:
            return conn_socket.getpeername()[0]
        return None

    def update_current_socket_timeout(self, relaxed_timeout: Optional[float] = None):
        conn_socket = self._get_socket()
        if conn_socket:
            timeout = relaxed_timeout if relaxed_timeout != -1 else self.socket_timeout
            # if the current timeout is 0 it means we are in the middle of a can_read call
            # in this case we don't want to change the timeout because the operation
            # is non-blocking and should return immediately
            # Changing the state from non-blocking to blocking in the middle of a read operation
            # will lead to a deadlock
            if conn_socket.gettimeout() != 0:
                conn_socket.settimeout(timeout)
            # Deliberately outside the guard above. The parser caches this value
            # to restore after a per-call timeout override, so while a can_read
            # probe holds the socket at 0 the cache *is* the pending restore
            # value: writing it is how the new timeout gets armed without
            # flipping the socket back to blocking mid-read. Skipping it here
            # instead would let the probe restore the pre-relaxation timeout and
            # lose a maintenance relaxation for the life of the connection.
            self.update_parser_timeout(timeout)

    def update_parser_timeout(self, timeout: Optional[float] = None):
        parser = self._get_parser()
        if parser and parser._buffer:
            # Both parsers cache this value to restore after a per-call timeout
            # override, so both must receive exactly what was armed on the
            # socket - including None, which means "block indefinitely".
            if isinstance(parser, _RESP3Parser):
                parser._buffer.socket_timeout = timeout
            elif isinstance(parser, _HiredisParser):
                parser._socket_timeout = timeout

    def set_tmp_settings(
        self,
        tmp_host_address: Optional[Union[str, object]] = SENTINEL,
        tmp_relaxed_timeout: Optional[float] = -1,
    ):
        """
        SENTINEL keeps the host unchanged. -1 keeps the relaxed timeout unchanged.
        """
        if tmp_host_address and tmp_host_address != SENTINEL:
            self.host = str(tmp_host_address)
        if tmp_relaxed_timeout != -1:
            self.socket_timeout = tmp_relaxed_timeout
            self.socket_connect_timeout = tmp_relaxed_timeout

    def reset_tmp_settings(
        self,
        reset_host_address: bool = False,
        reset_relaxed_timeout: bool = False,
    ):
        if reset_host_address:
            self.host = self.orig_host_address
        if reset_relaxed_timeout:
            self.socket_timeout = self.orig_socket_timeout
            self.socket_connect_timeout = self.orig_socket_connect_timeout


class AbstractConnection(MaintNotificationsAbstractConnection, ConnectionInterface):
    "Manages communication to and from a Redis server"

    @deprecated_args(
        args_to_warn=["lib_name", "lib_version"],
        reason="Use 'driver_info' parameter instead. "
        "lib_name and lib_version will be removed in a future version.",
    )
    def __init__(
        self,
        db: int = 0,
        password: Optional[str] = None,
        socket_timeout: Optional[float] = DEFAULT_SOCKET_TIMEOUT,
        socket_connect_timeout: Optional[float] = DEFAULT_SOCKET_CONNECT_TIMEOUT,
        retry_on_timeout: bool = False,
        retry_on_error: Union[Iterable[Type[Exception]], object] = SENTINEL,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        parser_class=DefaultParser,
        socket_read_size: int = DEFAULT_SOCKET_READ_SIZE,
        health_check_interval: int = 0,
        client_name: Optional[str] = None,
        lib_name: Union[Optional[str], object] = SENTINEL,
        lib_version: Union[Optional[str], object] = SENTINEL,
        driver_info: Union[Optional[DriverInfo], object] = SENTINEL,
        username: Optional[str] = None,
        retry: Union[Any, None] = None,
        redis_connect_func: Optional[Callable[[], None]] = None,
        credential_provider: Optional[CredentialProvider] = None,
        protocol: Optional[int] = None,
        legacy_responses: bool = True,
        command_packer: Optional[Callable[[], None]] = None,
        event_dispatcher: Optional[EventDispatcher] = None,
        maint_notifications_config: Optional[MaintNotificationsConfig] = None,
        maint_notifications_pool_handler: Optional[
            MaintNotificationsPoolHandler
        ] = None,
        maintenance_state: "MaintenanceState" = MaintenanceState.NONE,
        maintenance_notification_hash: Optional[int] = None,
        orig_host_address: Optional[str] = None,
        orig_socket_timeout: Optional[float] = None,
        orig_socket_connect_timeout: Optional[float] = None,
        oss_cluster_maint_notifications_handler: Optional[
            OSSMaintNotificationsHandler
        ] = None,
        himport_registry: HImportRegistry | None = None,
    ):
        """
        Initialize a new Connection.

        To specify a retry policy for specific errors, first set
        `retry_on_error` to a list of the error/s to retry on, then set
        `retry` to a valid `Retry` object.
        To retry on TimeoutError, `retry_on_timeout` can also be set to `True`.

        Parameters
        ----------
        driver_info : DriverInfo, optional
            Driver metadata for CLIENT SETINFO. If provided, lib_name and lib_version
            are ignored. If not provided, a DriverInfo will be created from lib_name
            and lib_version. Explicit None disables CLIENT SETINFO.
        lib_name : str, optional
            **Deprecated.** Use driver_info instead. Library name for CLIENT SETINFO.
        lib_version : str, optional
            **Deprecated.** Use driver_info instead. Library version for CLIENT SETINFO.
        """
        if (username or password) and credential_provider is not None:
            raise DataError(
                "'username' and 'password' cannot be passed along with 'credential_"
                "provider'. Please provide only one of the following arguments: \n"
                "1. 'password' and (optional) 'username'\n"
                "2. 'credential_provider'"
            )
        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher
        self.pid = os.getpid()
        self.db = db
        self.client_name = client_name

        # Handle driver_info: if provided, use it; otherwise create from lib_name/lib_version.
        self.driver_info = resolve_driver_info(driver_info, lib_name, lib_version)

        self.credential_provider = credential_provider
        self.password = password
        self.username = username
        self._socket_timeout = socket_timeout
        if socket_connect_timeout is None:
            socket_connect_timeout = socket_timeout
        self._socket_connect_timeout = socket_connect_timeout
        self.retry_on_timeout = retry_on_timeout
        if retry_on_error is SENTINEL:
            retry_on_errors_list = []
        else:
            retry_on_errors_list = list(retry_on_error)
        if retry_on_timeout:
            # Add TimeoutError to the errors list to retry on
            retry_on_errors_list.append(TimeoutError)
        self.retry_on_error = retry_on_errors_list
        if retry or self.retry_on_error:
            if retry is None:
                self.retry = Retry(NoBackoff(), 1)
            else:
                # deep-copy the Retry object as it is mutable
                self.retry = copy.deepcopy(retry)
            if self.retry_on_error:
                # Update the retry's supported errors with the specified errors
                self.retry.update_supported_errors(self.retry_on_error)
        else:
            self.retry = Retry(NoBackoff(), 0)
        self.health_check_interval = health_check_interval
        self.next_health_check = 0
        self.redis_connect_func = redis_connect_func
        self.encoder = Encoder(encoding, encoding_errors, decode_responses)
        self.handshake_metadata = None
        self._sock = None
        self._socket_read_size = socket_read_size
        self._connect_callbacks = []
        self._buffer_cutoff = 6000
        self._re_auth_token: Optional[TokenInterface] = None
        try:
            p = int(protocol)
        except TypeError:
            p = DEFAULT_RESP_VERSION
        except ValueError:
            raise ConnectionError("protocol must be an integer")
        else:
            if p < 2 or p > 3:
                raise ConnectionError("protocol must be either 2 or 3")
        self.protocol = p
        self.legacy_responses = legacy_responses
        if self.protocol == 3 and parser_class == _RESP2Parser:
            # If the protocol is 3 but the parser is RESP2, change it to RESP3
            # This is needed because the parser might be set before the protocol
            # or might be provided as a kwarg to the constructor
            # We need to react on discrepancy only for RESP2 and RESP3
            # as hiredis supports both
            parser_class = _RESP3Parser
        self.set_parser(parser_class)

        self._command_packer = self._construct_command_packer(command_packer)
        self._should_reconnect = False

        # HIMPORT client-side state. `himport_registry` is the shared client-level
        # registry (empty if unconfigured) and persists across reconnects.
        self.himport_registry = himport_registry
        self._reset_himport_state()

        # Set up maintenance notifications
        MaintNotificationsAbstractConnection.__init__(
            self,
            maint_notifications_config,
            maint_notifications_pool_handler,
            maintenance_state,
            maintenance_notification_hash,
            orig_host_address,
            orig_socket_timeout,
            orig_socket_connect_timeout,
            oss_cluster_maint_notifications_handler,
            self._parser,
            event_dispatcher=self._event_dispatcher,
        )

    def __repr__(self):
        repr_args = ",".join([f"{k}={v}" for k, v in self.repr_pieces()])
        return f"<{self.__class__.__module__}.{self.__class__.__name__}({repr_args})>"

    @abstractmethod
    def repr_pieces(self):
        pass

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass

    @property
    def is_connected(self) -> bool:
        return self._sock is not None

    def _construct_command_packer(self, packer):
        if packer is not None:
            return packer
        elif HIREDIS_AVAILABLE:
            fallback = PythonRespSerializer(self._buffer_cutoff, self.encoder.encode)
            return HiredisRespSerializer(fallback.pack, self.encoder.encode)
        else:
            return PythonRespSerializer(self._buffer_cutoff, self.encoder.encode)

    def register_connect_callback(self, callback):
        """
        Register a callback to be called when the connection is established either
        initially or reconnected.  This allows listeners to issue commands that
        are ephemeral to the connection, for example pub/sub subscription or
        key tracking.  The callback must be a _method_ and will be kept as
        a weak reference.
        """
        wm = weakref.WeakMethod(callback)
        if wm not in self._connect_callbacks:
            self._connect_callbacks.append(wm)

    def deregister_connect_callback(self, callback):
        """
        De-register a previously registered callback.  It will no-longer receive
        notifications on connection events.  Calling this is not required when the
        listener goes away, since the callbacks are kept as weak methods.
        """
        try:
            self._connect_callbacks.remove(weakref.WeakMethod(callback))
        except ValueError:
            pass

    def set_parser(self, parser_class):
        """
        Creates a new instance of parser_class with socket size:
        _socket_read_size and assigns it to the parser for the connection
        :param parser_class: The required parser class
        """
        self._parser = parser_class(socket_read_size=self._socket_read_size)

    def _get_parser(self) -> Union[_HiredisParser, _RESP3Parser, _RESP2Parser]:
        return self._parser

    def connect(self):
        "Connects to the Redis server if not already connected"
        # try once the socket connect with the handshake, retry the whole
        # connect/handshake flow based on retry policy
        self.retry.call_with_retry(
            lambda: self.connect_check_health(
                check_health=True, retry_socket_connect=False
            ),
            lambda error: self.disconnect(error),
        )

    def connect_check_health(
        self, check_health: bool = True, retry_socket_connect: bool = True
    ):
        if self._sock:
            return
        # Track actual retry attempts for error reporting
        actual_retry_attempts = [0]

        def failure_callback(error, failure_count):
            actual_retry_attempts[0] = failure_count
            self.disconnect(error=error, failure_count=failure_count)

        try:
            if retry_socket_connect:
                sock = self.retry.call_with_retry(
                    self._connect,
                    failure_callback,
                    with_failure_count=True,
                )
            else:
                sock = self._connect()
        except socket.timeout:
            e = TimeoutError("Timeout connecting to server")
            record_error_count(
                server_address=self.host,
                server_port=self.port,
                network_peer_address=self.host,
                network_peer_port=self.port,
                error_type=e,
                retry_attempts=actual_retry_attempts[0],
            )
            raise e
        except OSError as e:
            e = ConnectionError(self._error_message(e))
            record_error_count(
                server_address=getattr(self, "host", None),
                server_port=getattr(self, "port", None),
                network_peer_address=getattr(self, "host", None),
                network_peer_port=getattr(self, "port", None),
                error_type=e,
                retry_attempts=actual_retry_attempts[0],
            )
            raise e

        self._sock = sock
        try:
            if self.redis_connect_func is None:
                # Use the default on_connect function
                self.on_connect_check_health(check_health=check_health)
            else:
                # Use the passed function redis_connect_func
                self.redis_connect_func(self)
        except BaseException:
            # clean up after any error in on_connect, which includes whatever a
            # credential provider raises. A socket left in place reads as
            # connected, so the next command would run without AUTH and SELECT.
            self.disconnect()
            raise

        # run any user callbacks. right now the only internal callback
        # is for pubsub channel/pattern resubscription
        # first, remove any dead weakrefs
        self._connect_callbacks = [ref for ref in self._connect_callbacks if ref()]
        for ref in self._connect_callbacks:
            callback = ref()
            if callback:
                callback(self)

    @abstractmethod
    def _connect(self):
        pass

    @abstractmethod
    def _host_error(self):
        pass

    def _error_message(self, exception):
        return format_error_message(self._host_error(), exception)

    def _reset_himport_state(self):
        # A fresh server session has no prepared HIMPORT fieldsets, so the next
        # himport_set must re-prepare on this connection. ``_himport_prepared`` maps
        # fieldset name -> the version prepared on the server; ``_himport_reconciled
        # _revision`` is the registry revision this connection last reconciled discards
        # against. Both are reset on connect/disconnect since the session is gone.
        self._himport_prepared: dict[str, int] = {}
        self._himport_reconciled_revision: int = 0

    def on_connect(self):
        self.on_connect_check_health(check_health=True)

    def on_connect_check_health(self, check_health: bool = True):
        "Initialize the connection, authenticate and select a database"
        # A fresh socket is a new server session: no prepared HIMPORT fieldsets.
        self._reset_himport_state()
        self._parser.on_connect(self)
        parser = self._parser

        auth_args = None
        # if credential provider or username and/or password are set, authenticate
        if self.credential_provider or (self.username or self.password):
            cred_provider = (
                self.credential_provider
                or UsernamePasswordCredentialProvider(self.username, self.password)
            )
            auth_args = cred_provider.get_credentials()

        # if resp version is specified and we have auth args,
        # we need to send them via HELLO
        if auth_args and check_protocol_version(self.protocol, 3):
            if isinstance(self._parser, _RESP2Parser):
                self.set_parser(_RESP3Parser)
                # update cluster exception classes
                self._parser.EXCEPTION_CLASSES = parser.EXCEPTION_CLASSES
                self._parser.on_connect(self)
            if len(auth_args) == 1:
                auth_args = ["default", auth_args[0]]
            # avoid checking health here -- PING will fail if we try
            # to check the health prior to the AUTH
            self.send_command(
                "HELLO", self.protocol, "AUTH", *auth_args, check_health=False
            )
            self.handshake_metadata = self.read_response()
            # if response.get(b"proto") != self.protocol and response.get(
            #     "proto"
            # ) != self.protocol:
            #     raise ConnectionError("Invalid RESP version")
        elif auth_args:
            # avoid checking health here -- PING will fail if we try
            # to check the health prior to the AUTH
            self.send_command("AUTH", *auth_args, check_health=False)

            try:
                auth_response = self.read_response()
            except AuthenticationWrongNumberOfArgsError:
                # a username and password were specified but the Redis
                # server seems to be < 6.0.0 which expects a single password
                # arg. retry auth with just the password.
                # https://github.com/andymccurdy/redis-py/issues/1274
                self.send_command("AUTH", auth_args[-1], check_health=False)
                auth_response = self.read_response()

            if str_if_bytes(auth_response) != "OK":
                raise AuthenticationError("Invalid Username or Password")

        # if resp version is specified, switch to it
        elif check_protocol_version(self.protocol, 3):
            if isinstance(self._parser, _RESP2Parser):
                self.set_parser(_RESP3Parser)
                # update cluster exception classes
                self._parser.EXCEPTION_CLASSES = parser.EXCEPTION_CLASSES
                self._parser.on_connect(self)
            self.send_command("HELLO", self.protocol, check_health=check_health)
            self.handshake_metadata = self.read_response()
            if (
                self.handshake_metadata.get(b"proto") != self.protocol
                and self.handshake_metadata.get("proto") != self.protocol
            ):
                raise ConnectionError("Invalid RESP version")

        # The tail of the handshake (optional CLIENT MAINT_NOTIFICATIONS, then
        # CLIENT SETNAME / SETINFO / SELECT) does not affect control flow -- the replies
        # are only validated or discarded. So we optimize the flow: send every command
        # first (without blocking on a reply between them), then read the replies back in
        # send order. All requests are on the wire before we block on the first read, so
        # the whole tail costs a single round-trip instead of one per command. AUTH/HELLO
        # stays a separate round-trip above because its reply drives the RESP2->RESP3
        # parser upgrade, the pre-6.0 AUTH retry, and proto validation.
        #
        # deferred_reads holds one zero-arg handler per command sent below; each reads
        # exactly one reply (in send order) and validates it. Per-command check_health
        # reproduces the original behavior: at most one health PING/PONG fires before the
        # first tail command (only when no HELLO/AUTH ran, e.g. RESP2 no-auth), and it is
        # self-contained so it never desyncs the deferred replies.
        #
        # TODO: apply the same optimization to the async handshake in
        # redis/asyncio/connection.py, where not all tail commands are handled this way
        # yet (e.g. maintenance notifications are still issued separately there).
        deferred_reads = []

        # Maintenance notifications (RESP3-only, opt-in) go first when enabled, so their
        # reply is read together with the rest of the tail.
        self._add_maint_notifications_to_handshake(deferred_reads, check_health)

        # if a client_name is given, set it
        if self.client_name:
            self.send_command(
                "CLIENT", "SETNAME", self.client_name, check_health=check_health
            )
            deferred_reads.append(
                functools.partial(self._read_ok_or_raise, "Error setting client name")
            )

        # Set the library name and version from driver_info. Older servers may not
        # support CLIENT SETINFO, so any ResponseError to these replies is swallowed.
        if self.driver_info and self.driver_info.formatted_name:
            self.send_command(
                "CLIENT",
                "SETINFO",
                "LIB-NAME",
                self.driver_info.formatted_name,
                check_health=check_health,
            )
            deferred_reads.append(self._read_optional_setinfo)

        if self.driver_info and self.driver_info.lib_version:
            self.send_command(
                "CLIENT",
                "SETINFO",
                "LIB-VER",
                self.driver_info.lib_version,
                check_health=check_health,
            )
            deferred_reads.append(self._read_optional_setinfo)

        # if a database is specified, switch to it
        if self.db:
            self.send_command("SELECT", self.db, check_health=check_health)
            deferred_reads.append(
                functools.partial(self._read_ok_or_raise, "Invalid Database")
            )

        # Read the deferred replies in the order the commands were sent.
        for read_and_validate_response in deferred_reads:
            read_and_validate_response()

    def disconnect(self, *args, **kwargs):
        "Disconnects from the Redis server"
        # The server session is gone, so any HIMPORT fieldsets prepared on this
        # socket no longer exist; reset the tracking.
        self._reset_himport_state()
        # Likewise the relaxation a maintenance applied to it: restored and
        # reported here, ahead of a close that may find no socket to close
        self._complete_maintenance_on_disconnect()
        self._parser.on_disconnect()

        conn_sock = self._sock
        self._sock = None
        # reset the reconnect flag
        self.reset_should_reconnect()

        if conn_sock is None:
            return

        if os.getpid() == self.pid:
            try:
                conn_sock.shutdown(socket.SHUT_RDWR)
            except (OSError, TypeError):
                pass

        try:
            conn_sock.close()
        except OSError:
            pass

        error = kwargs.get("error")
        failure_count = kwargs.get("failure_count")
        health_check_failed = kwargs.get("health_check_failed")

        if error:
            if health_check_failed:
                close_reason = CloseReason.HEALTHCHECK_FAILED
            else:
                close_reason = CloseReason.ERROR

            if failure_count is not None and failure_count > self.retry.get_retries():
                record_error_count(
                    server_address=self.host,
                    server_port=self.port,
                    network_peer_address=self.host,
                    network_peer_port=self.port,
                    error_type=error,
                    retry_attempts=failure_count,
                )

            record_connection_closed(
                close_reason=close_reason,
                error_type=error,
            )
        else:
            record_connection_closed(
                close_reason=CloseReason.APPLICATION_CLOSE,
            )

    def mark_for_reconnect(self):
        self._should_reconnect = True

    def should_reconnect(self):
        return self._should_reconnect

    def reset_should_reconnect(self):
        self._should_reconnect = False

    def _send_ping(self):
        """Send PING, expect PONG in return"""
        self.send_command("PING", check_health=False)
        if str_if_bytes(self.read_response()) != "PONG":
            raise ConnectionError("Bad response from PING health check")

    def _ping_failed(self, error, failure_count):
        """Function to call when PING fails"""
        self.disconnect(
            error=error, failure_count=failure_count, health_check_failed=True
        )

    def check_health(self):
        """Check the health of the connection with a PING/PONG"""
        if self.health_check_interval and time.monotonic() > self.next_health_check:
            self.retry.call_with_retry(
                self._send_ping,
                self._ping_failed,
                with_failure_count=True,
            )

    def send_packed_command(self, command, check_health=True):
        """Send an already packed command to the Redis server"""
        if not self._sock:
            self.connect_check_health(check_health=False)
        # guard against health check recursion
        if check_health:
            self.check_health()
        try:
            if isinstance(command, str):
                command = [command]
            for item in command:
                self._sock.sendall(item)
        except socket.timeout as e:
            add_debug_log_for_connection_failure(self, e, "writing command")
            self.disconnect()
            if is_os_level_timeout(e):
                # ETIMEDOUT from the OS: the connection is dead, not slow.
                raise ConnectionError(
                    f"Error {e.errno} while writing to socket. {e.strerror}."
                ) from e
            raise TimeoutError("Timeout writing to socket")
        except OSError as e:
            add_debug_log_for_connection_failure(self, e, "writing command")
            self.disconnect()
            if len(e.args) == 1:
                errno, errmsg = "UNKNOWN", e.args[0]
            else:
                errno = e.args[0]
                errmsg = e.args[1]
            raise ConnectionError(f"Error {errno} while writing to socket. {errmsg}.")
        except BaseException as e:
            # BaseExceptions can be raised when a socket send operation is not
            # finished, e.g. due to a timeout.  Ideally, a caller could then re-try
            # to send un-sent data. However, the send_packed_command() API
            # does not support it so there is no point in keeping the connection open.
            add_debug_log_for_connection_failure(self, e, "writing command")
            self.disconnect()
            raise

    def send_command(self, *args, **kwargs):
        """Pack and send a command to the Redis server"""
        self.send_packed_command(
            self._command_packer.pack(*args),
            check_health=kwargs.get("check_health", True),
        )

    def can_read(self, timeout: float = 0) -> bool:
        """Poll the socket to see if there's data that can be read."""
        # TODO: Rename this API; it detects pending data or dirty/closed
        # connection state, not only whether application data can be read.
        sock = self._sock
        if not sock:
            self.connect()

        host_error = self._host_error()

        try:
            return self._parser.can_read(timeout)

        except OSError as e:
            self.disconnect()
            raise ConnectionError(f"Error while reading from {host_error}: {e.args}")

    def read_response(
        self,
        disable_decoding=False,
        *,
        timeout: Union[float, object] = SENTINEL,
        disconnect_on_error=True,
        push_request=False,
    ):
        """Read the response from a previously sent command"""

        host_error = self._host_error()

        try:
            if self.protocol in ["3", 3]:
                response = self._parser.read_response(
                    disable_decoding=disable_decoding,
                    push_request=push_request,
                    timeout=timeout,
                )
            else:
                response = self._parser.read_response(
                    disable_decoding=disable_decoding, timeout=timeout
                )
        except socket.timeout as e:
            if disconnect_on_error:
                add_debug_log_for_connection_failure(self, e, "reading response")
                self.disconnect()
            raise TimeoutError(f"Timeout reading from {host_error}")
        except TimeoutError as e:
            # The parsers raise redis.exceptions.TimeoutError, which is not an
            # OSError, so without this branch it would fall through to
            # BaseException and keep the parser's undecorated message. Re-raise
            # it with the host, matching what the async stack already reports.
            if disconnect_on_error:
                add_debug_log_for_connection_failure(self, e, "reading response")
                self.disconnect()
            raise TimeoutError(f"Timeout reading from {host_error}")
        except OSError as e:
            if disconnect_on_error:
                add_debug_log_for_connection_failure(self, e, "reading response")
                self.disconnect()
            raise ConnectionError(f"Error while reading from {host_error} : {e.args}")
        except UNRECOVERABLE_PARSE_ERRORS as e:
            # The parser failed partway through a reply, after the read cursor
            # had already passed bytes it cannot re-interpret. The rewind and
            # disconnect_on_error=False exist so an *interrupted* read can be
            # re-parsed from the start (#2510, #2695) - an in-band ResponseError
            # is returned as a value and purged, never rewound. Re-parsing here
            # just reproduces the same failure, so honouring
            # disconnect_on_error=False would make every later read fail
            # identically, forever. Drop the connection regardless. See #4291.
            add_debug_log_for_connection_failure(self, e, "reading response")
            self.disconnect()
            raise
        except BaseException as e:
            # Also by default close in case of BaseException.  A lot of code
            # relies on this behaviour when doing Command/Response pairs.
            # See #1128.
            if disconnect_on_error:
                add_debug_log_for_connection_failure(self, e, "reading response")
                self.disconnect()
            raise

        if self.health_check_interval:
            self.next_health_check = time.monotonic() + self.health_check_interval

        if isinstance(response, ResponseError):
            try:
                raise response
            finally:
                del response  # avoid creating ref cycles
        return response

    def pack_command(self, *args):
        """Pack a series of arguments into the Redis protocol"""
        return self._command_packer.pack(*args)

    def pack_commands(self, commands):
        """Pack multiple commands into the Redis protocol"""
        output = []
        pieces = []
        buffer_length = 0
        buffer_cutoff = self._buffer_cutoff

        for cmd in commands:
            for chunk in self._command_packer.pack(*cmd):
                chunklen = len(chunk)
                if (
                    buffer_length > buffer_cutoff
                    or chunklen > buffer_cutoff
                    or isinstance(chunk, memoryview)
                ):
                    if pieces:
                        output.append(SYM_EMPTY.join(pieces))
                    buffer_length = 0
                    pieces = []

                if chunklen > buffer_cutoff or isinstance(chunk, memoryview):
                    output.append(chunk)
                else:
                    pieces.append(chunk)
                    buffer_length += chunklen

        if pieces:
            output.append(SYM_EMPTY.join(pieces))
        return output

    def get_protocol(self) -> Union[int, str]:
        return self.protocol

    @property
    def handshake_metadata(self) -> Union[Dict[bytes, bytes], Dict[str, str]]:
        return self._handshake_metadata

    @handshake_metadata.setter
    def handshake_metadata(self, value: Union[Dict[bytes, bytes], Dict[str, str]]):
        self._handshake_metadata = value

    def set_re_auth_token(self, token: TokenInterface):
        self._re_auth_token = token

    def re_auth(self):
        if self._re_auth_token is not None:
            self.send_command(
                "AUTH",
                self._re_auth_token.try_get("oid"),
                self._re_auth_token.get_value(),
            )
            self.read_response()
            self._re_auth_token = None

    def _get_socket(self) -> Optional[socket.socket]:
        return self._sock

    @property
    def socket_timeout(self) -> Optional[Union[float, int]]:
        return self._socket_timeout

    @socket_timeout.setter
    def socket_timeout(self, value: Optional[Union[float, int]]):
        self._socket_timeout = value

    @property
    def socket_connect_timeout(self) -> Optional[Union[float, int]]:
        return self._socket_connect_timeout

    @socket_connect_timeout.setter
    def socket_connect_timeout(self, value: Optional[Union[float, int]]):
        self._socket_connect_timeout = value

    def extract_connection_details(self) -> str:
        """
        Render the connection's identity, maintenance state and effective timeouts.

        This is what the debug logs use to explain a failed or timed out command:

        - ``host`` vs ``orig host`` says whether the connection still points at the
          node being moved away from, or has already been repointed at the new one.
        - ``state`` says whether maintenance handling touched this connection at
          all, so an unaffected node's connections are distinguishable.
        - ``socket_timeout`` vs ``active read timeout`` says which timeout the read
          actually ran under. They diverge for a command that was already in flight
          when the relaxed timeout was applied.
        """
        if self._sock is None:
            return "not connected"

        socket_address = None
        active_read_timeout = None
        try:
            socket_name = self._sock.getsockname()
            # AF_UNIX sockets report a path string rather than a (host, port) tuple
            if isinstance(socket_name, tuple) and len(socket_name) > 1:
                socket_address = socket_name[1]
            # The timeout armed on the socket is what a blocking read is really
            # using, which can lag self.socket_timeout for an in-flight command.
            active_read_timeout = self._sock.gettimeout()
        except (AttributeError, OSError):
            pass

        state = getattr(self.maintenance_state, "value", self.maintenance_state)
        return (
            f"connected to ip {self.get_resolved_ip()}, "
            f"local socket port: {socket_address}, "
            f"host: {self._host_error()} "
            f"(orig: {getattr(self, 'orig_host_address', None)}), "
            f"state: {state}, "
            f"socket_timeout: {self.socket_timeout} "
            f"(orig: {getattr(self, 'orig_socket_timeout', None)}), "
            f"active read timeout: {active_read_timeout}, "
            f"should_reconnect: {self.should_reconnect()}, "
            f"notification_hash: {self.maintenance_notification_hash}"
        )


class Connection(AbstractConnection):
    "Manages TCP communication to and from a Redis server"

    def __init__(
        self,
        host="localhost",
        port=6379,
        socket_keepalive=True,
        socket_keepalive_options=SENTINEL,
        socket_type=0,
        **kwargs,
    ):
        """
        Initialize a TCP connection.

        Parameters
        ----------
        socket_keepalive : bool
            If `True`, TCP keepalive is enabled for TCP socket connections.
        socket_keepalive_options : Mapping[int, int | bytes] | object | None
            Mapping of TCP keepalive socket option constants to values, for
            example `{socket.TCP_KEEPIDLE: 30}`. If left unspecified, redis-py
            uses TCP keepalive defaults when `socket_keepalive` is enabled:
            idle 30 seconds, interval 5 seconds, and 3 probes. Platform-specific
            options that are not available are skipped. Pass `None` or `{}` to
            avoid setting additional TCP keepalive options.
        """
        self._host = host
        # bool subclasses int; port=True would become privileged port 1.
        # Numeric strings stay valid. Callers still pass "6379".
        if isinstance(port, bool):
            raise TypeError("port must be an integer, not bool")
        if isinstance(port, str):
            try:
                port = int(port)
            except ValueError:
                raise TypeError("port must be an integer, not str") from None
        elif not isinstance(port, int):
            raise TypeError(f"port must be an integer, not {type(port).__name__}")
        if not 0 <= port <= 65535:
            raise ValueError(f"port must be in 0..65535, got {port}")
        self.port = port
        self.socket_keepalive = socket_keepalive
        if socket_keepalive_options is SENTINEL:
            socket_keepalive_options = get_default_socket_keepalive_options()
        self.socket_keepalive_options = socket_keepalive_options or {}
        self.socket_type = socket_type
        super().__init__(**kwargs)

    def repr_pieces(self):
        pieces = [("host", self.host), ("port", self.port), ("db", self.db)]
        if self.client_name:
            pieces.append(("client_name", self.client_name))
        return pieces

    def _connect(self):
        "Create a TCP socket connection"
        # we want to mimic what socket.create_connection does to support
        # ipv4/ipv6, but we want to set options prior to calling
        # socket.connect()

        # Last caught connection error.
        # Re-thrown if we are unable to connect to any of the options returned
        # by getaddrinfo.
        # Note that we must clear this variable before returning - otherwise,
        # a caught err's traceback points to this frame, which points to err.
        # Clearing this lets refcounting reclaim the exception immediately
        # without deferring to the python garbage collector.
        err = None

        for res in socket.getaddrinfo(
            self.host, self.port, self.socket_type, socket.SOCK_STREAM
        ):
            family, socktype, proto, canonname, socket_address = res
            sock = None
            try:
                sock = socket.socket(family, socktype, proto)
                # TCP_NODELAY
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                # TCP_KEEPALIVE
                if self.socket_keepalive:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in self.socket_keepalive_options.items():
                        sock.setsockopt(socket.IPPROTO_TCP, k, v)

                # set the socket_connect_timeout before we connect
                sock.settimeout(self.socket_connect_timeout)

                # connect
                sock.connect(socket_address)

                # set the socket_timeout now that we're connected
                sock.settimeout(self.socket_timeout)

                # If a previous connection attempt failed, clear the error
                err = None

                return sock

            except OSError as _:
                err = _
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)  # ensure a clean close
                    except OSError:
                        pass
                    sock.close()

        if err is not None:
            try:
                raise err
            finally:
                # Ensure we clear local references to caught exceptions
                err = None
        raise OSError("socket.getaddrinfo returned an empty list")

    def _host_error(self):
        return f"{self.host}:{self.port}"

    @property
    def host(self) -> str:
        return self._host

    @host.setter
    def host(self, value: str):
        self._host = value


# Distinguishes "no locally-served reply is waiting" from a cached value that happens to be
# falsy. ``None`` cannot do the job: it is a legitimate reply shape, and the cache refuses to
# store it only by convention.
_NO_PENDING_HIT = object()

# A command name that does not start with one of these cannot be ``CLIENT`` or ``RESET``,
# so ``_cache_owned_command`` rejects nearly every command before it allocates anything.
_CLIENT_FIRST_CHARS = frozenset(("c", "C", b"c", b"C"))
_RESET_FIRST_CHARS = frozenset(("r", "R", b"r", b"R"))
# ``pack`` splits the command name on whitespace, so a name padded with it still reaches
# the server as ``CLIENT ...`` or ``RESET``. It encodes a ``str`` name before splitting,
# so ASCII whitespace is the only kind that splits.
_WHITESPACE_CHARS = frozenset(
    (*" \t\n\r\x0b\x0c", *(bytes((c,)) for c in b" \t\n\r\x0b\x0c"))
)

# The ``CLIENT`` subcommands the cache owns. ``TRACKINGINFO`` and ``GETREDIR`` only read the
# tracking state, so they are not here.
_CACHE_OWNED_CLIENT_SUBCOMMANDS = frozenset(("CACHING", "TRACKING"))


def _cache_owned_command(args) -> str | None:
    """
    The name of the command in ``args`` if it is one the cache owns - ``CLIENT CACHING``,
    ``CLIENT TRACKING`` or ``RESET`` - else ``None``. Matches every spelling
    ``pack_command`` accepts: ``"CLIENT CACHING"`` as one argument or ``"CLIENT"``,
    ``"CACHING"`` as two, as ``str``, ``bytes``, ``bytearray`` or ``memoryview``, in any
    case, padded with whitespace or not.
    """
    command = args[0]
    if not isinstance(command, (str, bytes)):
        if not isinstance(command, (bytearray, memoryview)):
            return None
        command = bytes(command)
    first_char = command[:1]
    if first_char in _RESET_FIRST_CHARS:
        # ``RESET`` takes no arguments, so only a five-character name, or one followed by
        # whitespace, can match. The check keeps the many other ``R`` commands (``RPOP``,
        # ``RENAME``, ...) allocation-free.
        if len(command) != 5 and command[5:6] not in _WHITESPACE_CHARS:
            return None
    elif first_char not in _CLIENT_FIRST_CHARS and first_char not in _WHITESPACE_CHARS:
        return None
    words = str_if_bytes(command).upper().split()
    if not words:
        return None
    if words[0] == "RESET":
        return "RESET"
    if words[0] != "CLIENT":
        return None
    if len(words) == 1 and len(args) > 1:
        subcommand = args[1]
        if isinstance(subcommand, (bytearray, memoryview)):
            subcommand = bytes(subcommand)
        if isinstance(subcommand, (str, bytes)):
            words.append(str_if_bytes(subcommand).upper())
    if len(words) > 1 and words[1] in _CACHE_OWNED_CLIENT_SUBCOMMANDS:
        return f"CLIENT {words[1]}"
    return None


_IMMUTABLE_TYPES = (bytes, str, int, float, bool, type(None))


def _clone_cache_value(value: Any) -> Any:
    # Restrict fast path to exact built-in types so subclasses with mutable
    # instance attributes are safely deep-copied.
    if type(value) in _IMMUTABLE_TYPES:
        return value
    return copy.deepcopy(value)


class CacheProxyConnection(MaintNotificationsAbstractConnection, ConnectionInterface):
    DUMMY_CACHE_VALUE = b"foo"
    MIN_ALLOWED_VERSION = "7.4.0"
    DEFAULT_SERVER_NAME = "redis"

    def __init__(
        self,
        conn: ConnectionInterface,
        cache: CacheInterface,
        pool_lock: threading.RLock,
    ):
        self.pid = os.getpid()
        self._conn = conn
        self.retry = self._conn.retry
        self.host = self._conn.host
        # A UnixDomainSocketConnection has no port
        self.port = getattr(self._conn, "port", None)
        self.db = self._conn.db
        self._event_dispatcher = self._conn._event_dispatcher
        self.credential_provider = conn.credential_provider
        self._pool_lock = pool_lock
        self._cache = cache
        self._cache_lock = threading.RLock()
        self._current_command_cache_key = None
        # Read once: the configuration cannot change after the pool built the cache, and the
        # mode is a connection-setup property anyway - the server refuses to switch a live
        # connection between OPTIN and OPTOUT, so a configuration change applies to new
        # connections only.
        self._tracking_mode = cache.config.get_tracking_mode()
        # A CLIENT CACHING ``+OK`` is queued ahead of the next data reply.
        self._pending_caching_reply = False
        # The next command is an ASK-redirected attempt, which must not be paired.
        self._skip_next_caching = False
        # Distinguishes a reconnect from the first connect in the tracking callback.
        self._connected_once = False
        # A reply ``send_command`` resolved from the cache, waiting for ``read_response`` to
        # hand it back. Set means nothing was written, so there is no reply on the socket.
        self._pending_cache_hit = _NO_PENDING_HIT
        self.register_connect_callback(self._enable_tracking_callback)

        if isinstance(self._conn, MaintNotificationsAbstractConnection):
            MaintNotificationsAbstractConnection.__init__(
                self,
                self._conn.maint_notifications_config,
                self._conn._maint_notifications_pool_handler,
                self._conn.maintenance_state,
                self._conn.maintenance_notification_hash,
                self._conn.host,
                self._conn.socket_timeout,
                self._conn.socket_connect_timeout,
                self._conn._oss_cluster_maint_notifications_handler,
                self._conn._get_parser(),
                event_dispatcher=self._conn.event_dispatcher,
            )

    def repr_pieces(self):
        return self._conn.repr_pieces()

    @property
    def is_connected(self) -> bool:
        return self._conn.is_connected

    def register_connect_callback(self, callback):
        self._conn.register_connect_callback(callback)

    def deregister_connect_callback(self, callback):
        self._conn.deregister_connect_callback(callback)

    def set_parser(self, parser_class):
        self._conn.set_parser(parser_class)

    def set_maint_notifications_pool_handler_for_connection(
        self, maint_notifications_pool_handler
    ):
        if isinstance(self._conn, MaintNotificationsAbstractConnection):
            self._conn.set_maint_notifications_pool_handler_for_connection(
                maint_notifications_pool_handler
            )

    def set_maint_notifications_cluster_handler_for_connection(
        self, oss_cluster_maint_notifications_handler
    ):
        if isinstance(self._conn, MaintNotificationsAbstractConnection):
            self._conn.set_maint_notifications_cluster_handler_for_connection(
                oss_cluster_maint_notifications_handler
            )

    def get_protocol(self):
        return self._conn.get_protocol()

    def connect(self):
        self._conn.connect()

        server_name = self._conn.handshake_metadata.get(b"server", None)
        if server_name is None:
            server_name = self._conn.handshake_metadata.get("server", None)
        server_ver = self._conn.handshake_metadata.get(b"version", None)
        if server_ver is None:
            server_ver = self._conn.handshake_metadata.get("version", None)
        if server_ver is None or server_name is None:
            raise ConnectionError("Cannot retrieve information about server version")

        server_ver = ensure_string(server_ver)
        server_name = ensure_string(server_name)

        if (
            server_name != self.DEFAULT_SERVER_NAME
            or compare_versions(server_ver, self.MIN_ALLOWED_VERSION) == 1
        ):
            raise ConnectionError(
                "To maximize compatibility with all Redis products, client-side caching is supported by Redis 7.4 or later"  # noqa: E501
            )

    def on_connect(self):
        self._conn.on_connect()

    def disconnect(self, *args, **kwargs):
        with self._cache_lock:
            self._cache.flush()
        # Both flags describe an exchange on the socket that is about to die with it. A stale
        # ``_pending_caching_reply`` would make the next ``read_response`` swallow a reply;
        # a stale ``_skip_next_caching`` would leak the ASK suppression onto an unrelated
        # later command.
        self._pending_caching_reply = False
        self._skip_next_caching = False
        self._pending_cache_hit = _NO_PENDING_HIT
        # The maintenance handlers are bound to this proxy, so the relaxation
        # a maintenance applied is reported with the proxy as its source
        if isinstance(self._conn, MaintNotificationsAbstractConnection):
            self._complete_maintenance_on_disconnect()
        self._conn.disconnect(*args, **kwargs)

    def check_health(self):
        self._conn.check_health()

    def send_packed_command(self, command, check_health=True):
        # TODO: Investigate if it's possible to unpack command
        #  or extract keys from packed command
        # Pre-packed commands are not individually cacheable, so make sure the
        # next read_response does not try to cache their reply under a stale key.
        self._current_command_cache_key = None
        # A pre-packed write carries no CLIENT CACHING command of its own, so a flag left
        # over from an earlier send must not make the next read_response swallow a reply -
        # nor may a stale locally-served reply be handed back in place of this write's.
        # The server consumes a pending ASKING on this write, so the ASK suppression must
        # not carry over to a later command either.
        self._pending_caching_reply = False
        self._skip_next_caching = False
        self._pending_cache_hit = _NO_PENDING_HIT
        self._conn.send_packed_command(command)

    def send_command(self, *args, **kwargs):
        # Before anything else, so a refused command leaves no state behind.
        self._refuse_cache_owned_command(args)
        self._process_pending_invalidations()
        self._pending_caching_reply = False
        self._pending_cache_hit = _NO_PENDING_HIT

        if self._skip_next_caching:
            # An ASK-redirected attempt.
            # ``ASKING`` like ``CLIENT CACHING`` needs to be immediatelly before the next command
            # so we can't have both additions to the command we are sending,
            # and pairing here would strip the ASK allowance and the read would
            # be redirected again.
            # Cache nothing either: the reply belongs to a migrating
            # slot, and the attempt is tracked by default under optout anyway.
            self._skip_next_caching = False
            self._current_command_cache_key = None
            self._conn.send_command(*args, **kwargs)
            return

        command = args[0]
        if isinstance(command, str) and command.upper() == "ASKING":
            # The cluster executor sends ASKING as its own command on this connection right
            # before the redirected attempt, so observing it here is all the notice needed -
            # no kwarg threading, and the flag mirrors the server's own one-shot semantics.
            self._skip_next_caching = True

        # Eligibility, keyability and intent are three separate questions, and all three must
        # be answered yes before a reply may be stored. The command metadata answers the
        # first; the presence of ``keys`` answers the second, because a command's key
        # positions are supplied by its command method rather than derived here. A cacheable
        # command whose invocation carries no key list is therefore a gap in what this client
        # has been taught, not an error: send it normally and cache nothing. The configured
        # tracking mode and predicate answer the third.
        keys = kwargs.get("keys")
        store = False

        if keys is not None:
            # Materialized once: ``keys`` may be a one-shot iterable, and it is read again
            # below to build the ``CacheKey`` the reverse index is keyed by.
            keys = tuple(keys)

        # An empty key list - ``mget([])``, ``exists()`` - is treated as no key list: there
        # is nothing to track, and the predicate is promised never to see an empty tuple.
        if keys:
            with self._cache_lock:
                # Eligibility is asked with a throwaway empty-keys ``CacheKey`` because
                # ``DefaultCache.is_cachable`` looks only at ``key.command``.
                eligible = self._cache.is_cachable(
                    CacheKey(command=command, redis_keys=(), redis_args=())
                )

            # Intent is asked separately, after the real keys are known, and outside the
            # cache lock: it invokes application code and touches no cache state. Folding it
            # into ``is_cachable`` would also invoke it from ``DefaultCache.set`` and from
            # the eligibility probe above, both with an empty key tuple.
            store = eligible and self._cache.config.should_cache(command, keys)

        if not store:
            self._current_command_cache_key = None

            # One path for keyless and ineligible commands alike, so an eligible read whose
            # command method never plumbed ``keys=`` still gets its optout ``NO``. That is the
            # correct direction: the server tracks it regardless, and we will not store it.
            #
            # Trackability is asked only here, and never affects storage. A ``NO`` in front of
            # a command the server would not track - a write, a keyless read - is consumed
            # with no effect, so the only cost of getting it wrong is a wasted command; the
            # check fails closed, which skips the ``NO`` rather than risking a stored reply.
            if self._tracking_mode is TrackingMode.OPTOUT and (
                self._cache.config.is_trackable_read(command)
            ):
                self._send_with_caching(b"NO", args, kwargs)
            else:
                self._conn.send_command(*args, **kwargs)
            return

        # Creates cache key.
        self._current_command_cache_key = CacheKey(
            command=command, redis_keys=keys, redis_args=args
        )

        is_hit = False
        with self._cache_lock:
            candidate = self._cache.get(self._current_command_cache_key)
            if candidate is None or candidate.status == CacheEntryStatus.IN_PROGRESS:
                # Commit to a miss even if another connection subsequently promotes
                # the old placeholder, and never drain an IN_PROGRESS entry's owner:
                # its socket may still hold the reply to its outstanding command, and
                # the drain (which returns non-push replies too) would consume it.
                # Setting the placeholder here keeps the miss on a single lock section.
                candidate = None
                # Set temporary entry value to prevent
                # race condition from another connection.
                self._cache.set(
                    CacheEntry(
                        cache_key=self._current_command_cache_key,
                        cache_value=self.DUMMY_CACHE_VALUE,
                        status=CacheEntryStatus.IN_PROGRESS,
                        connection_ref=self._conn,
                    )
                )

        if candidate is not None:
            # Drain the connection that cached the entry so invalidations do not
            # accumulate on idle connections. This must run outside our cache lock: the
            # drain takes the pool lock, and invalidations call the owner's callback
            # under its cache lock. Holding ours would order a cache lock before the
            # pool lock and another after it, deadlocking connections draining each
            # other or a pool.disconnect() holding the pool lock while waiting for ours.
            with self._pool_lock:
                self._drain_invalidations(candidate.connection_ref)

            with self._cache_lock:
                # Re-check: the drained entry may have been invalidated or replaced.
                entry = self._cache.get(self._current_command_cache_key)

                # A hit requires this very entry object to have been drained. A
                # replacement filled during the drain may belong to another connection
                # with pending invalidations and is not trusted, even if it is VALID by
                # the time we check here.
                #
                # Whether this read is served locally is decided here, once, and the
                # value is carried to ``read_response`` - which must not re-derive it
                # from cache state. Two things can happen to the entry between the two
                # calls, and re-deriving desynchronises the connection under both:
                #
                # - another connection resolves this very entry object and flips it to
                #   VALID with *its* reply, so a re-derived check would serve that value
                #   and never read the reply we did send;
                # - an invalidation removes it, so a re-derived check would find nothing
                #   and read a reply for a command we never sent.
                if entry is candidate:
                    cached_value = entry.cache_value
                    self._current_command_cache_key = None
                    is_hit = True
                else:
                    # Replace the lost candidate with our own placeholder.
                    self._cache.set(
                        CacheEntry(
                            cache_key=self._current_command_cache_key,
                            cache_value=self.DUMMY_CACHE_VALUE,
                            status=CacheEntryStatus.IN_PROGRESS,
                            connection_ref=self._conn,
                        )
                    )

        if is_hit:
            self._pending_cache_hit = _clone_cache_value(cached_value)
            return

        # Send command over socket only if it's allowed
        # read-only command that not yet cached. Under optin the server remembers nothing
        # unless ``CLIENT CACHING YES`` comes right before the read, so this - the one place
        # that decided to store the reply - is where the pair is sent. Under plain and optout
        # the read is tracked by default and goes out alone.
        if self._tracking_mode is TrackingMode.OPTIN:
            self._send_with_caching(b"YES", args, kwargs)
        else:
            self._conn.send_command(*args, **kwargs)

    def _send_with_caching(self, decision: bytes, args, kwargs) -> None:
        """
        Write ``CLIENT CACHING YES|NO`` and its read to the socket as one write.

        One write is the entire pairing guarantee: the server consumes the CACHING flag on the
        next command that is not a ``CLIENT`` subcommand, so anything the library slipped in
        between would take the flag instead of the read - under optin leaving the read
        untracked, under optout exempting the wrong command. Health-checking is left to
        ``send_packed_command``, which PINGs before it writes anything, so the PING cannot
        land inside the pair.

        Calls the wrapped connection's ``send_packed_command`` rather than this class's own
        override, which clears ``_current_command_cache_key`` and would discard the
        IN_PROGRESS placeholder the caller just set.

        Args:
            decision: ``b"YES"`` under optin, ``b"NO"`` under optout.
            args: The paired command, as ``send_command`` received it.
            kwargs: The paired command's keyword arguments, read for ``check_health``.
        """
        packed = self._conn.pack_commands([(b"CLIENT", b"CACHING", decision), args])
        self._conn.send_packed_command(
            packed, check_health=kwargs.get("check_health", True)
        )
        self._pending_caching_reply = True

    def can_read(self, timeout: float = 0) -> bool:
        # TODO: Rename this API; it detects pending data or dirty/closed
        # connection state, not only whether application data can be read.
        return self._conn.can_read(timeout)

    def read_response(
        self,
        disable_decoding=False,
        *,
        timeout: Union[float, object] = SENTINEL,
        disconnect_on_error=True,
        push_request=False,
    ):
        if self._pending_cache_hit is not _NO_PENDING_HIT:
            # ``send_command`` resolved this read from the cache and wrote nothing, so there
            # is no reply on the socket and the cache is deliberately not consulted again -
            # see the note there on why re-deriving the decision desynchronises the
            # connection.
            response = self._pending_cache_hit
            self._pending_cache_hit = _NO_PENDING_HIT
            record_csc_request(
                result=CSCResult.HIT,
            )
            record_csc_network_saved(
                bytes_saved=len(response) if hasattr(response, "__len__") else 0,
            )
            return response

        if self._current_command_cache_key is not None:
            # A key is still set, so ``send_command`` decided to store this reply and sent the
            # command: by construction a miss.
            record_csc_request(
                result=CSCResult.MISS,
            )

        try:
            if self._pending_caching_reply:
                # Consumed before the wire read below, so an interleaved invalidation cannot
                # be mistaken for either reply of the pair. Inside the ``try`` so a failure
                # here drops the placeholder exactly as a failed data read does.
                self._pending_caching_reply = False
                self._read_caching_reply(
                    timeout=timeout, disconnect_on_error=disconnect_on_error
                )

            response = self._conn.read_response(
                disable_decoding=disable_decoding,
                timeout=timeout,
                disconnect_on_error=disconnect_on_error,
                push_request=push_request,
            )
            if self._current_command_cache_key is not None:
                cloned_response = _clone_cache_value(response)
        except BaseException:
            # The placeholder ``send_command`` staked is resolved only by the read that just
            # failed, so it has to go with it. Left behind, it sits in the pool-wide cache
            # until an invalidation or a disconnect happens to clear it: nothing else will,
            # because this command never completed and so the server is tracking nothing for
            # it. A ``WRONGTYPE`` or ``NOPERM`` reply is enough to strand one, as is the
            # ``-ASK`` on the first attempt of a redirected read.
            self._drop_own_placeholder()
            raise

        with self._cache_lock:
            # Prevent not-allowed command from caching.
            if self._current_command_cache_key is None:
                return response
            # If response is None prevent from caching. Only our own placeholder is
            # dropped: the entry may by now belong to another connection.
            if response is None:
                self._drop_own_placeholder()
                return response

            cache_entry = self._cache.get(self._current_command_cache_key)

            # Promote only the placeholder this connection staked. It may have been
            # invalidated in the meantime, or replaced by another connection's placeholder
            # for the same key - promoting that one would bind our reply to its
            # ``connection_ref``, and a later hit would drain that connection's own reply.
            if (
                cache_entry is not None
                and cache_entry.status == CacheEntryStatus.IN_PROGRESS
                and cache_entry.connection_ref is self._conn
            ):
                cache_entry.cache_value = cloned_response
                cache_entry.status = CacheEntryStatus.VALID
                self._cache.set(cache_entry)

            self._current_command_cache_key = None

        return response

    def _drop_own_placeholder(self) -> None:
        """
        Drop the ``IN_PROGRESS`` placeholder this connection staked, if it is still ours.

        Called when the read that would have resolved the placeholder fails. The cache is
        shared by the whole pool while each proxy's lock is its own, so the entry under this
        key may no longer be the one this connection put there, and the delete is scoped to
        the two conditions that prove it is:

        - still ``IN_PROGRESS`` - another connection's successful read can resolve this very
          entry to ``VALID`` in place, and deleting it would discard a correctly stored reply;
        - ``connection_ref`` is this connection - another connection's ``send_command`` for
          the same key replaces the entry with its own placeholder, and deleting it would
          evict a fetch that is still in flight.

        Clears ``_current_command_cache_key`` either way: the read it pointed at is over.
        """
        key = self._current_command_cache_key
        if key is None:
            return

        with self._cache_lock:
            entry = self._cache.get(key)
            if (
                entry is not None
                and entry.status == CacheEntryStatus.IN_PROGRESS
                and entry.connection_ref is self._conn
            ):
                self._cache.delete_by_cache_keys([key])

        self._current_command_cache_key = None

    def _read_caching_reply(self, *, timeout, disconnect_on_error) -> None:
        """
        Consume the ``+OK`` of a ``CLIENT CACHING`` command sent ahead of a read.

        Called inside ``read_response``'s ``try``, whose handlers clean up after any failure
        here the same way they do after a failed data read. A ``ConnectionError`` fails both
        commands together - never the read alone, whose reply would otherwise be stored
        without the tracking that makes it safe. The wrapped socket is closed, but the cache
        is flushed only when the client's error handling then disconnects this connection,
        or when the next connect re-enables tracking.

        Args:
            timeout: The caller's bound for this read.
            disconnect_on_error: Passed through to the wrapped connection.

        Raises:
            ResponseError: Re-raised after the paired read's reply has been drained.
            ConnectionError: If the server answered something other than ``OK``.
        """
        try:
            reply = self._conn.read_response(
                timeout=timeout, disconnect_on_error=disconnect_on_error
            )
        except ResponseError:
            # The paired read executed on the server regardless. Leaving its reply on the
            # socket would return this connection to the pool one reply out of sync, and the
            # next borrower would read our answer. The placeholder is dropped by the caller's
            # ``except BaseException``, as for any other failed read.
            self._conn.read_response(
                timeout=timeout, disconnect_on_error=disconnect_on_error
            )
            raise

        if str_if_bytes(reply) != "OK":
            raise ConnectionError(f"Unexpected CLIENT CACHING reply: {reply!r}")

    def pack_command(self, *args):
        return self._conn.pack_command(*args)

    def pack_commands(self, commands):
        # Every pipeline and transaction packs through here, so the guard covers them as
        # well. The cache's own commands are unaffected: ``_send_with_caching`` packs
        # through the wrapped connection, and the tracking handshake is sent on it.
        for args in commands:
            self._refuse_cache_owned_command(args)
        return self._conn.pack_commands(commands)

    @staticmethod
    def _refuse_cache_owned_command(args) -> None:
        """
        Refuse a user-sent ``CLIENT CACHING``, ``CLIENT TRACKING`` or ``RESET`` on a
        connection the cache manages.

        Each of them changes the tracking state the cache relies on for invalidations:

        - ``CLIENT CACHING`` sets a flag the server applies to the next command on the
          socket, and a pooled connection promises nothing about which command that is.
          Under ``optout`` a stray ``NO`` would leave the next cached read untracked while
          its reply is still stored, so it would never be invalidated. The cache sends it
          itself, paired with the read it belongs to, whenever the tracking mode needs it.
        - ``CLIENT TRACKING`` turns tracking off or reconfigures it, so the cache would keep
          storing replies that no invalidation reaches. The cache enables tracking itself
          when the connection is established.
        - ``RESET`` clears tracking along with the rest of the connection state.

        Raises:
            RedisError: If ``args`` is one of these commands.
        """
        command = _cache_owned_command(args)
        if command is None:
            return
        if command == "CLIENT CACHING":
            reason = "the cache sends it itself, paired with the read it applies to"
        elif command == "CLIENT TRACKING":
            reason = (
                "the cache enables tracking itself, and changing it would stop "
                "invalidations for cached replies"
            )
        else:
            reason = "it turns off the tracking the cache relies on for invalidations"
        raise RedisError(
            f"{command} cannot be sent on a connection with client-side caching "
            f"enabled: {reason}"
        )

    # HIMPORT state lives on the wrapped connection (HIMPORT is never cacheable);
    # delegate so callers treat the proxy like a plain connection and never need
    # to know a proxy is in play.
    @property
    def himport_registry(self):
        return self._conn.himport_registry

    @property
    def _himport_prepared(self):
        return self._conn._himport_prepared

    @_himport_prepared.setter
    def _himport_prepared(self, value):
        # Delegate reassignment to the wrapped connection, mirroring
        # ``_himport_reconciled_revision``. Production code only mutates the dict
        # in place, but ``_reset_himport_state`` (and any future caller) reassigns
        # it, and a getter-only property here would raise ``AttributeError`` only
        # when client-side caching is enabled -- a caching-specific latent trap.
        self._conn._himport_prepared = value

    @property
    def _himport_reconciled_revision(self):
        return self._conn._himport_reconciled_revision

    @_himport_reconciled_revision.setter
    def _himport_reconciled_revision(self, value):
        self._conn._himport_reconciled_revision = value

    @property
    def handshake_metadata(self) -> Union[Dict[bytes, bytes], Dict[str, str]]:
        return self._conn.handshake_metadata

    def set_re_auth_token(self, token: TokenInterface):
        self._conn.set_re_auth_token(token)

    def re_auth(self):
        self._conn.re_auth()

    def mark_for_reconnect(self):
        self._conn.mark_for_reconnect()

    def should_reconnect(self):
        return self._conn.should_reconnect()

    def reset_should_reconnect(self):
        self._conn.reset_should_reconnect()

    @property
    def host(self) -> str:
        return self._conn.host

    @host.setter
    def host(self, value: str):
        self._conn.host = value

    @property
    def socket_timeout(self) -> Optional[Union[float, int]]:
        return self._conn.socket_timeout

    @socket_timeout.setter
    def socket_timeout(self, value: Optional[Union[float, int]]):
        self._conn.socket_timeout = value

    @property
    def socket_connect_timeout(self) -> Optional[Union[float, int]]:
        return self._conn.socket_connect_timeout

    @socket_connect_timeout.setter
    def socket_connect_timeout(self, value: Optional[Union[float, int]]):
        self._conn.socket_connect_timeout = value

    @property
    def _maint_notifications_connection_handler(
        self,
    ) -> Optional[MaintNotificationsConnectionHandler]:
        if isinstance(self._conn, MaintNotificationsAbstractConnection):
            return self._conn._maint_notifications_connection_handler

    @_maint_notifications_connection_handler.setter
    def _maint_notifications_connection_handler(
        self, value: Optional[MaintNotificationsConnectionHandler]
    ):
        self._conn._maint_notifications_connection_handler = value

    def _get_socket(self) -> Optional[socket.socket]:
        if isinstance(self._conn, MaintNotificationsAbstractConnection):
            return self._conn._get_socket()
        else:
            raise NotImplementedError(
                "Maintenance notifications are not supported by this connection type"
            )

    def _get_parser(self) -> BaseParser:
        return self._conn._get_parser()

    def _get_maint_notifications_connection_instance(
        self, connection
    ) -> MaintNotificationsAbstractConnection:
        """
        Validate that connection instance supports maintenance notifications.
        With this helper method we ensure that we are working
        with the correct connection type.
        After twe validate that connection instance supports maintenance notifications
        we can safely return the connection instance
        as MaintNotificationsAbstractConnection.
        """
        if not isinstance(connection, MaintNotificationsAbstractConnection):
            raise NotImplementedError(
                "Maintenance notifications are not supported by this connection type"
            )
        else:
            return connection

    @property
    def maintenance_state(self) -> MaintenanceState:
        con = self._get_maint_notifications_connection_instance(self._conn)
        return con.maintenance_state

    @maintenance_state.setter
    def maintenance_state(self, state: MaintenanceState):
        con = self._get_maint_notifications_connection_instance(self._conn)
        con.maintenance_state = state

    def getpeername(self):
        con = self._get_maint_notifications_connection_instance(self._conn)
        return con.getpeername()

    def get_resolved_ip(self):
        con = self._get_maint_notifications_connection_instance(self._conn)
        return con.get_resolved_ip()

    def update_current_socket_timeout(self, relaxed_timeout: Optional[float] = None):
        con = self._get_maint_notifications_connection_instance(self._conn)
        con.update_current_socket_timeout(relaxed_timeout)

    def set_tmp_settings(
        self,
        tmp_host_address: Optional[str] = None,
        tmp_relaxed_timeout: Optional[float] = -1,
    ):
        con = self._get_maint_notifications_connection_instance(self._conn)
        con.set_tmp_settings(tmp_host_address, tmp_relaxed_timeout)

    def reset_tmp_settings(
        self,
        reset_host_address: bool = False,
        reset_relaxed_timeout: bool = False,
    ):
        con = self._get_maint_notifications_connection_instance(self._conn)
        con.reset_tmp_settings(reset_host_address, reset_relaxed_timeout)

    def _connect(self):
        self._conn._connect()

    def _host_error(self):
        return self._conn._host_error()

    def _enable_tracking_callback(self, conn: ConnectionInterface) -> None:
        if self._connected_once:
            # The server destroys a connection's tracking state on disconnect, so every entry
            # cached through the previous session has lost its invalidation channel. Paths
            # that reconnect silently inside the send path never call ``disconnect``, so the
            # flush cannot live there alone.
            #
            # Gated on a previous connect because this callback also fires on a first connect
            # and the cache is pool-shared: an unconditional flush would wipe other
            # connections' entries every time the pool grows. Flushing the whole shared cache
            # on reconnect is what ``disconnect`` already does, so the semantics do not
            # change - only the coverage does.
            #
            # The one entry kept is this connection's own in-flight placeholder. Every
            # reconnect that fires this callback happens inside the send path, before the
            # command is written, so the reply it is waiting for comes from the new session -
            # which this callback is about to enable tracking for. Dropping it would cost the
            # fill and gain nothing.
            with self._cache_lock:
                own_placeholder = None
                if self._current_command_cache_key is not None:
                    entry = self._cache.get(self._current_command_cache_key)
                    if (
                        entry is not None
                        and entry.status == CacheEntryStatus.IN_PROGRESS
                        and entry.connection_ref is self._conn
                    ):
                        own_placeholder = entry
                self._cache.flush()
                if own_placeholder is not None:
                    self._cache.set(own_placeholder)

        self._pending_caching_reply = False

        # The mode is sent in the handshake because that is what the server requires:
        # switching OPTIN <-> OPTOUT on a live connection is an error, so a configuration
        # change applies to new connections only.
        args = ["CLIENT", "TRACKING", "ON"]
        if self._tracking_mode is TrackingMode.OPTIN:
            args.append("OPTIN")
        elif self._tracking_mode is TrackingMode.OPTOUT:
            args.append("OPTOUT")

        conn.send_command(*args)
        conn.read_response()
        # Set only once tracking is on: a handshake that fails before this point cached
        # nothing, so the next connect has no session to flush for.
        self._connected_once = True
        conn._parser.set_invalidation_push_handler(self._on_invalidation_callback)

    def _drain_invalidations(self, conn: ConnectionInterface):
        while conn.can_read():
            try:
                conn.read_response(
                    push_request=True, timeout=0, disconnect_on_error=False
                )
            except TimeoutError:
                break
            except UNRECOVERABLE_PARSE_ERRORS:
                # Invalidation replies are read straight off the raw
                # connection, so the disconnect a parse failure now forces
                # bypasses this proxy's disconnect() and its cache flush. The
                # next connect() opens a fresh CLIENT TRACKING session that the
                # server has no invalidation state for, so entries cached under
                # the old session would be served as if still tracked. Flush
                # them here for the same reason disconnect() does.
                with self._cache_lock:
                    self._cache.flush()
                raise

    def _process_pending_invalidations(self):
        self._drain_invalidations(self._conn)

    def _on_invalidation_callback(self, data: List[Union[str, Optional[List[bytes]]]]):
        with self._cache_lock:
            # Flush cache when DB flushed on server-side
            if data[1] is None:
                self._cache.flush()
            else:
                keys_deleted = self._cache.delete_by_redis_keys(data[1])

                if len(keys_deleted) > 0:
                    record_csc_eviction(
                        count=len(keys_deleted),
                        reason=CSCReason.INVALIDATION,
                    )

    def extract_connection_details(self) -> str:
        return self._conn.extract_connection_details()


class SSLConnection(Connection):
    """Manages SSL connections to and from the Redis server(s).
    This class extends the Connection class, adding SSL functionality, and making
    use of ssl.SSLContext (https://docs.python.org/3/library/ssl.html#ssl.SSLContext)
    """  # noqa

    def __init__(
        self,
        ssl_keyfile=None,
        ssl_certfile=None,
        ssl_cert_reqs="required",
        ssl_include_verify_flags: Optional[List["VerifyFlags"]] = None,
        ssl_exclude_verify_flags: Optional[List["VerifyFlags"]] = None,
        ssl_ca_certs=None,
        ssl_ca_data=None,
        ssl_check_hostname=True,
        ssl_ca_path=None,
        ssl_password=None,
        ssl_validate_ocsp=False,
        ssl_validate_ocsp_stapled=False,
        ssl_ocsp_context=None,
        ssl_ocsp_expected_cert=None,
        ssl_min_version=None,
        ssl_ciphers=None,
        **kwargs,
    ):
        """Constructor

        Args:
            ssl_keyfile: Path to an ssl private key. Defaults to None.
            ssl_certfile: Path to an ssl certificate. Defaults to None.
            ssl_cert_reqs: The string value for the SSLContext.verify_mode (none, optional, required),
                           or an ssl.VerifyMode. Defaults to "required".
            ssl_include_verify_flags: A list of flags to be included in the SSLContext.verify_flags. Defaults to None.
            ssl_exclude_verify_flags: A list of flags to be excluded from the SSLContext.verify_flags. Defaults to None.
            ssl_ca_certs: The path to a file of concatenated CA certificates in PEM format. Defaults to None.
            ssl_ca_data: Either an ASCII string of one or more PEM-encoded certificates or a bytes-like object of DER-encoded certificates.
            ssl_check_hostname: If set, match the hostname during the SSL handshake. Defaults to True.
            ssl_ca_path: The path to a directory containing several CA certificates in PEM format. Defaults to None.
            ssl_password: Password for unlocking an encrypted private key. Defaults to None.

            ssl_validate_ocsp: If set, perform a full ocsp validation (i.e not a stapled verification)
            ssl_validate_ocsp_stapled: If set, perform a validation on a stapled ocsp response
            ssl_ocsp_context: A fully initialized OpenSSL.SSL.Context object to be used in verifying the ssl_ocsp_expected_cert
            ssl_ocsp_expected_cert: A PEM armoured string containing the expected certificate to be returned from the ocsp verification service.
            ssl_min_version: The lowest supported SSL version. It affects the supported SSL versions of the SSLContext. None leaves the default provided by ssl module.
            ssl_ciphers: A string listing the ciphers that are allowed to be used. Defaults to None, which means that the default ciphers are used. See https://docs.python.org/3/library/ssl.html#ssl.SSLContext.set_ciphers for more information.

        Raises:
            RedisError
        """  # noqa
        if not SSL_AVAILABLE:
            raise RedisError("Python wasn't built with SSL support")

        self.keyfile = ssl_keyfile
        self.certfile = ssl_certfile
        if ssl_cert_reqs is None:
            ssl_cert_reqs = ssl.CERT_NONE
        elif isinstance(ssl_cert_reqs, str):
            CERT_REQS = {  # noqa: N806
                "none": ssl.CERT_NONE,
                "optional": ssl.CERT_OPTIONAL,
                "required": ssl.CERT_REQUIRED,
            }
            if ssl_cert_reqs not in CERT_REQS:
                raise RedisError(
                    f"Invalid SSL Certificate Requirements Flag: {ssl_cert_reqs}"
                )
            ssl_cert_reqs = CERT_REQS[ssl_cert_reqs]
        self.cert_reqs = ssl_cert_reqs
        self.ssl_include_verify_flags = ssl_include_verify_flags
        self.ssl_exclude_verify_flags = ssl_exclude_verify_flags
        self.ca_certs = ssl_ca_certs
        self.ca_data = ssl_ca_data
        self.ca_path = ssl_ca_path
        self.check_hostname = (
            ssl_check_hostname if self.cert_reqs != ssl.CERT_NONE else False
        )
        self.certificate_password = ssl_password
        self.ssl_validate_ocsp = ssl_validate_ocsp
        self.ssl_validate_ocsp_stapled = ssl_validate_ocsp_stapled
        self.ssl_ocsp_context = ssl_ocsp_context
        self.ssl_ocsp_expected_cert = ssl_ocsp_expected_cert
        self.ssl_min_version = ssl_min_version
        self.ssl_ciphers = ssl_ciphers
        super().__init__(**kwargs)

    def _connect(self):
        """
        Wrap the socket with SSL support, handling potential errors.
        """
        sock = super()._connect()
        try:
            return self._wrap_socket_with_ssl(sock)
        except (OSError, RedisError):
            sock.close()
            raise

    def _wrap_socket_with_ssl(self, sock):
        """
        Wraps the socket with SSL support.

        Args:
            sock: The plain socket to wrap with SSL.

        Returns:
            An SSL wrapped socket.
        """
        context = ssl.create_default_context()
        context.check_hostname = self.check_hostname
        context.verify_mode = self.cert_reqs
        if self.ssl_include_verify_flags:
            for flag in self.ssl_include_verify_flags:
                context.verify_flags |= flag
        if self.ssl_exclude_verify_flags:
            for flag in self.ssl_exclude_verify_flags:
                context.verify_flags &= ~flag
        if self.certfile or self.keyfile:
            context.load_cert_chain(
                certfile=self.certfile,
                keyfile=self.keyfile,
                password=self.certificate_password,
            )
        if (
            self.ca_certs is not None
            or self.ca_path is not None
            or self.ca_data is not None
        ):
            context.load_verify_locations(
                cafile=self.ca_certs, capath=self.ca_path, cadata=self.ca_data
            )
        if self.ssl_min_version is not None:
            context.minimum_version = self.ssl_min_version
        if self.ssl_ciphers:
            context.set_ciphers(self.ssl_ciphers)
        if self.ssl_validate_ocsp is True and CRYPTOGRAPHY_AVAILABLE is False:
            raise RedisError("cryptography is not installed.")

        if self.ssl_validate_ocsp_stapled and self.ssl_validate_ocsp:
            raise RedisError(
                "Either an OCSP staple or pure OCSP connection must be validated "
                "- not both."
            )

        sock.settimeout(self.socket_connect_timeout)
        sslsock = context.wrap_socket(sock, server_hostname=self.host)
        sslsock.settimeout(self.socket_timeout)

        # validation for the stapled case
        if self.ssl_validate_ocsp_stapled:
            import OpenSSL

            from .ocsp import ocsp_staple_verifier

            # if a context is provided use it - otherwise, a basic context
            if self.ssl_ocsp_context is None:
                staple_ctx = OpenSSL.SSL.Context(OpenSSL.SSL.SSLv23_METHOD)
                staple_ctx.use_certificate_file(self.certfile)
                staple_ctx.use_privatekey_file(self.keyfile)
            else:
                staple_ctx = self.ssl_ocsp_context

            staple_ctx.set_ocsp_client_callback(
                ocsp_staple_verifier, self.ssl_ocsp_expected_cert
            )

            #  need another socket
            con = OpenSSL.SSL.Connection(staple_ctx, socket.socket())
            # Send the same SNI as sslsock so the probe is given the same
            # certificate. ssl sends none for an IP address, so neither do we.
            try:
                ipaddress.ip_address(self.host)
            except ValueError:
                con.set_tlsext_host_name(self.host.encode())
            con.request_ocsp()
            con.connect((self.host, self.port))
            con.do_handshake()
            con.shutdown()

            # The staple was validated on a second connection, so it only
            # vouches for sslsock if both presented the same certificate.
            staple_cert = OpenSSL.crypto.dump_certificate(
                OpenSSL.crypto.FILETYPE_ASN1, con.get_peer_certificate()
            )
            if staple_cert != sslsock.getpeercert(True):
                sslsock.close()
                raise ConnectionError(
                    "ocsp stapled certificate does not match the connection certificate"
                )
            return sslsock

        # pure ocsp validation
        if self.ssl_validate_ocsp is True and CRYPTOGRAPHY_AVAILABLE:
            from .ocsp import OCSPVerifier

            o = OCSPVerifier(sslsock, self.host, self.port, self.ca_certs)
            if o.is_valid():
                return sslsock
            else:
                raise ConnectionError("ocsp validation error")
        return sslsock


class UnixDomainSocketConnection(AbstractConnection):
    "Manages UDS communication to and from a Redis server"

    def __init__(self, path="", socket_timeout=DEFAULT_SOCKET_TIMEOUT, **kwargs):
        super().__init__(**kwargs)
        self.path = path
        self.socket_timeout = socket_timeout

    def repr_pieces(self):
        pieces = [("path", self.path), ("db", self.db)]
        if self.client_name:
            pieces.append(("client_name", self.client_name))
        return pieces

    def _connect(self):
        "Create a Unix domain socket connection"
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.socket_connect_timeout)
        try:
            sock.connect(self.path)
        except OSError:
            # Prevent ResourceWarnings for unclosed sockets.
            try:
                sock.shutdown(socket.SHUT_RDWR)  # ensure a clean close
            except OSError:
                pass
            sock.close()
            raise
        sock.settimeout(self.socket_timeout)
        return sock

    def _host_error(self):
        return self.path


FALSE_STRINGS = ("0", "F", "FALSE", "N", "NO", "OFF")


def to_bool(value):
    if value is None or value == "":
        return None
    if isinstance(value, str) and value.upper() in FALSE_STRINGS:
        return False
    return bool(value)


def parse_ssl_verify_flags(value):
    # flags are passed in as a string representation of a list,
    # e.g. VERIFY_X509_STRICT, VERIFY_X509_PARTIAL_CHAIN
    verify_flags_str = value.replace("[", "").replace("]", "")

    verify_flags = []
    for flag in verify_flags_str.split(","):
        flag = flag.strip()
        if not hasattr(VerifyFlags, flag):
            raise ValueError(f"Invalid ssl verify flag: {flag}")
        verify_flags.append(getattr(VerifyFlags, flag))
    return verify_flags


def parse_retry_on_error(value):
    # exception class names are passed as a comma-separated list,
    # e.g. ConnectionError,TimeoutError
    retry_on_error = []
    for name in value.replace("[", "").replace("]", "").split(","):
        name = name.strip()
        if not name:
            raise ValueError("Empty retry_on_error entry")
        exc = getattr(redis_exceptions, name, None)
        if not (isinstance(exc, type) and issubclass(exc, Exception)):
            raise ValueError(f"Unknown redis exception {name!r}")
        retry_on_error.append(exc)
    return retry_on_error


URL_QUERY_ARGUMENT_PARSERS = {
    "db": int,
    "socket_timeout": float,
    "socket_connect_timeout": float,
    "socket_read_size": int,
    "socket_keepalive": to_bool,
    "retry_on_timeout": to_bool,
    "retry_on_error": parse_retry_on_error,
    "max_connections": int,
    "health_check_interval": int,
    "ssl_check_hostname": to_bool,
    "ssl_validate_ocsp": to_bool,
    "ssl_validate_ocsp_stapled": to_bool,
    "ssl_include_verify_flags": parse_ssl_verify_flags,
    "ssl_exclude_verify_flags": parse_ssl_verify_flags,
    "ssl_min_version": int,
    "timeout": float,
    "protocol": int,
    "legacy_responses": to_bool,
}


def parse_url(url):
    # Scheme names are case-insensitive (RFC 3986), so normalize before the
    # prefix check; the "://" is required so a URL like "redis:foo" (which
    # urlparse would still report as the "redis" scheme) is rejected.
    if not url.lower().startswith(("redis://", "rediss://", "unix://")):
        raise ValueError(
            "Redis URL must specify one of the following "
            "schemes (redis://, rediss://, unix://)"
        )

    url = urlparse(url)
    kwargs = {}

    for name, value in parse_qs(url.query).items():
        if value and len(value) > 0:
            # parse_qs() already percent-decodes query values, so use the value
            # as-is; unquoting again here would double-decode (e.g. "%2520" ->
            # "%20" -> " "). See issue #4208.
            value = value[0]
            parser = URL_QUERY_ARGUMENT_PARSERS.get(name)
            if parser:
                try:
                    kwargs[name] = parser(value)
                except (TypeError, ValueError):
                    raise ValueError(f"Invalid value for '{name}' in connection URL.")
            else:
                kwargs[name] = value

    if url.username:
        kwargs["username"] = unquote(url.username)
    if url.password:
        kwargs["password"] = unquote(url.password)

    # We only support redis://, rediss:// and unix:// schemes.
    if url.scheme == "unix":
        if url.path:
            kwargs["path"] = unquote(url.path)
        kwargs["connection_class"] = UnixDomainSocketConnection

    else:  # implied:  url.scheme in ("redis", "rediss"):
        if url.hostname:
            kwargs["host"] = unquote(url.hostname)
        if url.port is not None:
            kwargs["port"] = int(url.port)

        # If there's a path argument, use it as the db argument if a
        # querystring value wasn't specified
        if url.path and "db" not in kwargs:
            try:
                kwargs["db"] = int(unquote(url.path).replace("/", ""))
            except (AttributeError, ValueError):
                pass

        if url.scheme == "rediss":
            kwargs["connection_class"] = SSLConnection

    return kwargs


_CP = TypeVar("_CP", bound="ConnectionPool")


class ConnectionPoolInterface(ABC):
    @abstractmethod
    def get_protocol(self):
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    @deprecated_args(
        args_to_warn=["*"],
        reason="Use get_connection() without args instead",
        version="5.3.0",
    )
    def get_connection(
        self, command_name: Optional[str], *keys, **options
    ) -> ConnectionInterface:
        pass

    @abstractmethod
    def get_encoder(self):
        pass

    @abstractmethod
    def release(self, connection: ConnectionInterface):
        pass

    @abstractmethod
    def disconnect(self, inuse_connections: bool = True):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def set_retry(self, retry: Retry):
        pass

    @abstractmethod
    def re_auth_callback(self, token: TokenInterface):
        pass

    @abstractmethod
    def get_connection_count(self) -> list[tuple[int, dict]]:
        """
        Returns a connection count (both idle and in use).
        """
        pass


class MaintNotificationsAbstractConnectionPool:
    """
    Abstract class for handling maintenance notifications logic.
    This class is mixed into the ConnectionPool classes.

    This class is not intended to be used directly!

    All logic related to maintenance notifications and
    connection pool handling is encapsulated in this class.
    """

    def __init__(
        self,
        maint_notifications_config: Optional[MaintNotificationsConfig] = None,
        oss_cluster_maint_notifications_handler: Optional[
            OSSMaintNotificationsHandler
        ] = None,
        **kwargs,
    ):
        # Initialize maintenance notifications
        is_protocol_supported = check_protocol_version(kwargs.get("protocol"), 3)

        if maint_notifications_config is None and is_protocol_supported:
            maint_notifications_config = MaintNotificationsConfig()

        if maint_notifications_config and maint_notifications_config.enabled:
            if not is_protocol_supported:
                raise RedisError(
                    "Maintenance notifications handlers on connection are only supported with RESP version 3"
                )

            self._event_dispatcher = kwargs.get("event_dispatcher", None)
            if self._event_dispatcher is None:
                self._event_dispatcher = EventDispatcher()

            self._maint_notifications_pool_handler = MaintNotificationsPoolHandler(
                self, maint_notifications_config
            )
            if oss_cluster_maint_notifications_handler:
                self._oss_cluster_maint_notifications_handler = (
                    oss_cluster_maint_notifications_handler
                )
                self._update_connection_kwargs_for_maint_notifications(
                    oss_cluster_maint_notifications_handler=self._oss_cluster_maint_notifications_handler
                )
                self._maint_notifications_pool_handler = None
            else:
                self._oss_cluster_maint_notifications_handler = None
                self._maint_notifications_pool_handler = MaintNotificationsPoolHandler(
                    self, maint_notifications_config
                )

                self._update_connection_kwargs_for_maint_notifications(
                    maint_notifications_pool_handler=self._maint_notifications_pool_handler
                )
        else:
            self._maint_notifications_pool_handler = None
            self._oss_cluster_maint_notifications_handler = None

    def _on_close(self) -> None:
        """Hook invoked from the pool's ``close()`` before the pool is shut down."""
        handler = getattr(self, "_maint_notifications_pool_handler", None)
        if handler is not None:
            handler.cancel_scheduled_tasks()

    def _on_reset_after_fork(self) -> None:
        """Hook invoked by ``reset()``; acts only in a forked child process."""
        handler = getattr(self, "_maint_notifications_pool_handler", None)
        # reset() also runs from __init__, where there is no pid yet
        pid = getattr(self, "pid", None)
        if handler is not None and pid is not None and pid != os.getpid():
            handler.reset_after_fork()

    @property
    @abstractmethod
    def connection_kwargs(self) -> Dict[str, Any]:
        pass

    @connection_kwargs.setter
    @abstractmethod
    def connection_kwargs(self, value: Dict[str, Any]):
        pass

    @abstractmethod
    def _get_pool_lock(self) -> threading.RLock:
        pass

    @abstractmethod
    def _get_free_connections(self) -> Iterable["MaintNotificationsAbstractConnection"]:
        pass

    @abstractmethod
    def _get_in_use_connections(
        self,
    ) -> Iterable["MaintNotificationsAbstractConnection"]:
        pass

    def maint_notifications_enabled(self):
        """
        Returns:
            True if the maintenance notifications are enabled, False otherwise.
            The maintenance notifications config is stored in the pool handler.
            If the pool handler is not set, the maintenance notifications are not enabled.
        """
        if self._oss_cluster_maint_notifications_handler:
            maint_notifications_config = (
                self._oss_cluster_maint_notifications_handler.config
            )
        else:
            maint_notifications_config = (
                self._maint_notifications_pool_handler.config
                if self._maint_notifications_pool_handler
                else None
            )

        return maint_notifications_config and maint_notifications_config.enabled

    def update_maint_notifications_config(
        self,
        maint_notifications_config: MaintNotificationsConfig,
        oss_cluster_maint_notifications_handler: Optional[
            OSSMaintNotificationsHandler
        ] = None,
    ):
        """
        Updates the maintenance notifications configuration.
        This method should be called only if the pool was created
        without enabling the maintenance notifications and
        in a later point in time maintenance notifications
        are requested to be enabled.
        """
        if (
            self.maint_notifications_enabled()
            and not maint_notifications_config.enabled
        ):
            raise ValueError(
                "Cannot disable maintenance notifications after enabling them"
            )
        if oss_cluster_maint_notifications_handler:
            self._oss_cluster_maint_notifications_handler = (
                oss_cluster_maint_notifications_handler
            )
            # OSS cluster mode and pool-handler mode are mutually exclusive
            # (see __init__). A pool created with the default RESP3 "auto"
            # config wires a pool handler before this method runs; clear it so
            # new and existing connections are not configured with both handlers.
            self._maint_notifications_pool_handler = None
        else:
            # first update pool settings
            if self._oss_cluster_maint_notifications_handler:
                # Pool already in OSS cluster mode; update the OSS handler config
                # instead of creating a mutually-exclusive pool handler (which
                # would be silently ignored because the OSS handler wins priority
                # in both update helpers below).
                self._oss_cluster_maint_notifications_handler.config = (
                    maint_notifications_config
                )
            elif not self._maint_notifications_pool_handler:
                self._maint_notifications_pool_handler = MaintNotificationsPoolHandler(
                    self, maint_notifications_config
                )
            else:
                self._maint_notifications_pool_handler.config = (
                    maint_notifications_config
                )

        # then update connection kwargs and existing connections
        self._update_connection_kwargs_for_maint_notifications(
            maint_notifications_pool_handler=self._maint_notifications_pool_handler,
            oss_cluster_maint_notifications_handler=self._oss_cluster_maint_notifications_handler,
        )
        self._update_maint_notifications_configs_for_connections(
            maint_notifications_pool_handler=self._maint_notifications_pool_handler,
            oss_cluster_maint_notifications_handler=self._oss_cluster_maint_notifications_handler,
        )

    def _update_connection_kwargs_for_maint_notifications(
        self,
        maint_notifications_pool_handler: Optional[
            MaintNotificationsPoolHandler
        ] = None,
        oss_cluster_maint_notifications_handler: Optional[
            OSSMaintNotificationsHandler
        ] = None,
    ):
        """
        Update the connection kwargs for all future connections.
        """
        if not self.maint_notifications_enabled():
            return
        if maint_notifications_pool_handler:
            self.connection_kwargs.update(
                {
                    "maint_notifications_pool_handler": maint_notifications_pool_handler,
                    "maint_notifications_config": maint_notifications_pool_handler.config,
                }
            )
        if oss_cluster_maint_notifications_handler:
            self.connection_kwargs.update(
                {
                    "oss_cluster_maint_notifications_handler": oss_cluster_maint_notifications_handler,
                    "maint_notifications_config": oss_cluster_maint_notifications_handler.config,
                }
            )
            # OSS cluster mode and pool-handler mode are mutually exclusive.
            # Drop any pool handler a default (RESP3 "auto") pool creation may
            # have wired so future connections are not configured with both.
            self.connection_kwargs.pop("maint_notifications_pool_handler", None)

        # Store original connection parameters for maintenance notifications.
        if self.connection_kwargs.get("orig_host_address", None) is None:
            # If orig_host_address is None it means we haven't
            # configured the original values yet
            self.connection_kwargs.update(
                {
                    "orig_host_address": self.connection_kwargs.get("host"),
                    "orig_socket_timeout": self.connection_kwargs.get(
                        "socket_timeout", DEFAULT_SOCKET_TIMEOUT
                    ),
                    "orig_socket_connect_timeout": self.connection_kwargs.get(
                        "socket_connect_timeout", DEFAULT_SOCKET_CONNECT_TIMEOUT
                    ),
                }
            )

    def _update_maint_notifications_configs_for_connections(
        self,
        maint_notifications_pool_handler: Optional[
            MaintNotificationsPoolHandler
        ] = None,
        oss_cluster_maint_notifications_handler: Optional[
            OSSMaintNotificationsHandler
        ] = None,
    ):
        """Update the maintenance notifications config for all connections in the pool."""
        with self._get_pool_lock():
            for conn in self._get_free_connections():
                if oss_cluster_maint_notifications_handler:
                    # set cluster handler for conn
                    conn.set_maint_notifications_cluster_handler_for_connection(
                        oss_cluster_maint_notifications_handler
                    )
                    conn.maint_notifications_config = (
                        oss_cluster_maint_notifications_handler.config
                    )
                elif maint_notifications_pool_handler:
                    conn.set_maint_notifications_pool_handler_for_connection(
                        maint_notifications_pool_handler
                    )
                    conn.maint_notifications_config = (
                        maint_notifications_pool_handler.config
                    )
                else:
                    raise ValueError(
                        "Either maint_notifications_pool_handler or oss_cluster_maint_notifications_handler must be set"
                    )
                conn.disconnect()
            for conn in self._get_in_use_connections():
                if oss_cluster_maint_notifications_handler:
                    # Use set_maint_notifications_cluster_handler_for_connection
                    # (not _configure_maintenance_notifications) so the parser is
                    # obtained from the connection itself. _configure_* requires a
                    # parser argument and would raise here; it would also reset the
                    # connection's orig_* settings, which is wrong for an in-use
                    # (active) connection. This mirrors the idle-connection branch
                    # above and the pool-handler branches.
                    conn.set_maint_notifications_cluster_handler_for_connection(
                        oss_cluster_maint_notifications_handler
                    )
                    conn.maint_notifications_config = (
                        oss_cluster_maint_notifications_handler.config
                    )
                elif maint_notifications_pool_handler:
                    conn.set_maint_notifications_pool_handler_for_connection(
                        maint_notifications_pool_handler
                    )
                    conn.maint_notifications_config = (
                        maint_notifications_pool_handler.config
                    )
                else:
                    raise ValueError(
                        "Either maint_notifications_pool_handler or oss_cluster_maint_notifications_handler must be set"
                    )
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "Marking active connection for reconnect after config update "
                        f"config update: {conn}, {conn.extract_connection_details()}"
                    )
                conn.mark_for_reconnect()

    def _should_update_connection(
        self,
        conn: "MaintNotificationsAbstractConnection",
        matching_pattern: Literal[
            "connected_address", "configured_address", "notification_hash"
        ] = "connected_address",
        matching_address: Optional[str] = None,
        matching_notification_hash: Optional[int] = None,
    ) -> bool:
        """
        Check if the connection should be updated based on the matching criteria.
        """
        if matching_pattern == "connected_address":
            if matching_address and conn.getpeername() != matching_address:
                return False
        elif matching_pattern == "configured_address":
            if matching_address and conn.host != matching_address:
                return False
        elif matching_pattern == "notification_hash":
            if (
                matching_notification_hash is not None
                and conn.maintenance_notification_hash != matching_notification_hash
            ):
                return False
        return True

    def update_connection_settings(
        self,
        conn: "MaintNotificationsAbstractConnection",
        state: Optional["MaintenanceState"] = None,
        maintenance_notification_hash: Optional[int] = None,
        host_address: Optional[str] = None,
        relaxed_timeout: Optional[float] = None,
        update_notification_hash: bool = False,
        reset_host_address: bool = False,
        reset_relaxed_timeout: bool = False,
    ):
        """
        Update the settings for a single connection.
        """
        if state:
            self._complete_maintenance_taken_over_by_handoff(conn, state)
            conn.maintenance_state = state

        if update_notification_hash:
            # update the notification hash only if requested
            conn.maintenance_notification_hash = maintenance_notification_hash

        if host_address is not None:
            conn.set_tmp_settings(tmp_host_address=host_address)

        if relaxed_timeout is not None:
            conn.set_tmp_settings(tmp_relaxed_timeout=relaxed_timeout)

        if reset_relaxed_timeout or reset_host_address:
            conn.reset_tmp_settings(
                reset_host_address=reset_host_address,
                reset_relaxed_timeout=reset_relaxed_timeout,
            )

        conn.update_current_socket_timeout(relaxed_timeout)

    def _complete_maintenance_taken_over_by_handoff(
        self, conn: "MaintNotificationsAbstractConnection", state: "MaintenanceState"
    ) -> None:
        """
        Report the MAINTENANCE relaxation of a connection a handoff is taking
        over as completed. The connection's completion notification is skipped
        while it is MOVING, and the handoff's own cleanup restores it without
        looking at what it superseded, so this is the only point at which the
        pair opened by the start event can be closed.
        """
        if (
            state is not MaintenanceState.MOVING
            or conn.maintenance_state is not MaintenanceState.MAINTENANCE
            or conn.maint_notifications_config is None
        ):
            return
        _dispatch_maintenance_event(
            conn.maint_notifications_config,
            MaintenanceCompletedEvent(
                connection_pool=self,
                connection=conn,
                state=MaintenanceState.MAINTENANCE,
                notification=None,
                config=conn.maint_notifications_config,
            ),
        )

    def update_connections_settings(
        self,
        state: Optional["MaintenanceState"] = None,
        maintenance_notification_hash: Optional[int] = None,
        host_address: Optional[str] = None,
        relaxed_timeout: Optional[float] = None,
        matching_address: Optional[str] = None,
        matching_notification_hash: Optional[int] = None,
        matching_pattern: Literal[
            "connected_address", "configured_address", "notification_hash"
        ] = "connected_address",
        update_notification_hash: bool = False,
        reset_host_address: bool = False,
        reset_relaxed_timeout: bool = False,
        include_free_connections: bool = True,
    ):
        """
        Update the settings for all matching connections in the pool.

        This method does not create new connections.
        This method does not affect the connection kwargs.

        :param state: The maintenance state to set for the connection.
        :param maintenance_notification_hash: The hash of the maintenance notification
                                               to set for the connection.
        :param host_address: The host address to set for the connection.
        :param relaxed_timeout: The relaxed timeout to set for the connection.
        :param matching_address: The address to match for the connection.
        :param matching_notification_hash: The notification hash to match for the connection.
        :param matching_pattern: The pattern to match for the connection.
        :param update_notification_hash: Whether to update the notification hash for the connection.
        :param reset_host_address: Whether to reset the host address to the original address.
        :param reset_relaxed_timeout: Whether to reset the relaxed timeout to the original timeout.
        :param include_free_connections: Whether to include free/available connections.
        """
        with self._get_pool_lock():
            for conn in self._get_in_use_connections():
                if self._should_update_connection(
                    conn,
                    matching_pattern,
                    matching_address,
                    matching_notification_hash,
                ):
                    self.update_connection_settings(
                        conn,
                        state=state,
                        maintenance_notification_hash=maintenance_notification_hash,
                        host_address=host_address,
                        relaxed_timeout=relaxed_timeout,
                        update_notification_hash=update_notification_hash,
                        reset_host_address=reset_host_address,
                        reset_relaxed_timeout=reset_relaxed_timeout,
                    )

            if include_free_connections:
                for conn in self._get_free_connections():
                    if self._should_update_connection(
                        conn,
                        matching_pattern,
                        matching_address,
                        matching_notification_hash,
                    ):
                        self.update_connection_settings(
                            conn,
                            state=state,
                            maintenance_notification_hash=maintenance_notification_hash,
                            host_address=host_address,
                            relaxed_timeout=relaxed_timeout,
                            update_notification_hash=update_notification_hash,
                            reset_host_address=reset_host_address,
                            reset_relaxed_timeout=reset_relaxed_timeout,
                        )

    def update_connection_kwargs(
        self,
        **kwargs,
    ):
        """
        Update the connection kwargs for all future connections.

        This method updates the connection kwargs for all future connections created by the pool.
        Existing connections are not affected.
        """
        self.connection_kwargs.update(kwargs)

    def update_active_connections_for_reconnect(
        self,
        moving_address_src: Optional[str] = None,
    ):
        """
        Mark all active connections for reconnect.
        This is used when a cluster node is migrated to a different address.

        :param moving_address_src: The address of the node that is being moved.
        """
        debug = logger.isEnabledFor(logging.DEBUG)
        with self._get_pool_lock():
            for conn in self._get_in_use_connections():
                if self._should_update_connection(
                    conn, "connected_address", moving_address_src
                ):
                    if debug:
                        logger.debug(
                            f"Marking active connection for reconnect: {conn}, "
                            f"{conn.extract_connection_details()}"
                        )
                    conn.mark_for_reconnect()

    def disconnect_free_connections(
        self,
        moving_address_src: Optional[str] = None,
    ):
        """
        Disconnect all free/available connections.
        This is used when a cluster node is migrated to a different address.

        :param moving_address_src: The address of the node that is being moved.
        """
        debug = logger.isEnabledFor(logging.DEBUG)
        with self._get_pool_lock():
            for conn in self._get_free_connections():
                if self._should_update_connection(
                    conn, "connected_address", moving_address_src
                ):
                    if debug:
                        logger.debug(
                            f"Disconnecting free connection: {conn}, "
                            f"{conn.extract_connection_details()}"
                        )
                    conn.disconnect()


class ConnectionPool(MaintNotificationsAbstractConnectionPool, ConnectionPoolInterface):
    """
    Create a connection pool. ``If max_connections`` is set, then this
    object raises :py:class:`~redis.exceptions.ConnectionError` when the pool's
    limit is reached.

    By default, TCP connections are created unless ``connection_class``
    is specified. Use class:`.UnixDomainSocketConnection` for
    unix sockets.
    :py:class:`~redis.SSLConnection` can be used for SSL enabled connections.

    If ``maint_notifications_config`` is provided, the connection pool will support
    maintenance notifications.
    Maintenance notifications are supported only with RESP3.
    If the ``maint_notifications_config`` is not provided but the ``protocol`` is 3,
    the maintenance notifications will be enabled by default.

    If ``metadata_resolver`` is provided, it decides which commands are eligible for
    client-side caching - see `redis.commands.metadata.MetadataResolver`. It is handed to the
    cache configuration, so one resolver can serve cache eligibility and cluster routing
    alike. Defaults to the static command metadata this library ships, which the cache
    configuration resolves through on its own. A resolver built from a live ``COMMAND`` reply
    is a snapshot of the server it was read from, so give each pool its own rather than
    sharing one across pools on different servers.

    Any additional keyword arguments are passed to the constructor of
    ``connection_class``.
    """

    @classmethod
    def from_url(cls: Type[_CP], url: str, **kwargs) -> _CP:
        """
        Return a connection pool configured from the given URL.

        For example::

            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[username@]/path/to/socket.sock?db=0[&password=password]

        Three URL schemes are supported:

        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>
        - ``unix://``: creates a Unix Domain Socket connection.

        The username, password, hostname and path are passed through
        urllib.parse.unquote in order to replace any percent-encoded values
        with their corresponding characters. Querystring values are decoded
        by urllib.parse.parse_qs and are not unquoted again.

        There are several ways to specify a database number. The first value
        found will be used:

            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// or rediss:// schemes, the path argument
               of the url, e.g. redis://localhost/0
            3. A ``db`` keyword argument to this function.

        If none of these options are specified, the default db=0 is used.

        All querystring options are cast to their appropriate Python types.
        Boolean arguments can be specified with string values "True"/"False"
        or "Yes"/"No". Values that cannot be properly cast cause a
        ``ValueError`` to be raised. Once parsed, the querystring arguments
        and keyword arguments are passed to the ``ConnectionPool``'s
        class initializer. In the case of conflicting arguments, querystring
        arguments always win.
        """
        url_options = parse_url(url)

        if "connection_class" in kwargs:
            url_options["connection_class"] = kwargs["connection_class"]

        kwargs.update(url_options)
        return cls(**kwargs)

    def __init__(
        self,
        connection_class=Connection,
        max_connections: Optional[int] = None,
        cache_factory: Optional[CacheFactoryInterface] = None,
        maint_notifications_config: Optional[MaintNotificationsConfig] = None,
        metadata_resolver: Optional[MetadataResolver] = None,
        **connection_kwargs,
    ):
        max_connections = max_connections or 100
        if not isinstance(max_connections, int) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')

        self.connection_class = connection_class
        self._connection_kwargs = connection_kwargs
        self.max_connections = max_connections
        self.cache = None
        self._cache_factory = cache_factory
        self.metadata_resolver = metadata_resolver

        try:
            supports_maint_notifications = issubclass(
                connection_class, MaintNotificationsAbstractConnection
            )
            is_unix_domain_socket_connection = issubclass(
                connection_class, UnixDomainSocketConnection
            )
        except TypeError:
            supports_maint_notifications = False
            is_unix_domain_socket_connection = False

        if is_unix_domain_socket_connection or not supports_maint_notifications:
            if (
                maint_notifications_config
                and maint_notifications_config.enabled is True
            ):
                raise RedisError(
                    "Maintenance notifications are not supported with "
                    f"{connection_class}"
                )
            maint_notifications_config = MaintNotificationsConfig(enabled=False)

        self._event_dispatcher = self._connection_kwargs.get("event_dispatcher", None)
        if self._event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()

        if connection_kwargs.get("cache_config") or connection_kwargs.get("cache"):
            if not check_protocol_version(self._connection_kwargs.get("protocol"), 3):
                raise RedisError("Client caching is only supported with RESP version 3")

            cache = self._connection_kwargs.get("cache")
            cache_config = self._connection_kwargs.get("cache_config")

            # Hand the client-level metadata resolver to the cache configuration, which is
            # where command eligibility is decided, so routing and caching read the same
            # records. Skipped when no resolver was configured, which leaves ``CacheConfig``
            # on the static default it builds for itself.
            #
            # Done on a copy rather than on the caller's object: a ``CacheConfig`` carries
            # only sizing and eviction settings, so reusing one across clients is reasonable,
            # and writing the resolver into it would give every one of them whichever
            # resolver was injected last. Every other setting is read-only after
            # construction, so the copy diverges from the caller's object in nothing but the
            # resolver - and a later ``set_metadata_resolver`` on the original does not reach
            # this pool, which is the intended direction: a pool decides eligibility by the
            # resolver its client was built with. Note that the ``cache=`` /
            # ``cache_factory=`` path below cannot copy, and so does write into the caller's
            # configuration; ``CacheConfig.set_metadata_resolver`` documents the difference.
            #
            # Guarded by ``isinstance`` rather than done through
            # ``CacheConfigurationInterface``: that ABC is public and implemented by third
            # parties, so a custom configuration keeps its own eligibility logic.
            if (
                metadata_resolver is not None
                and cache is None
                and self._cache_factory is None
                and isinstance(cache_config, CacheConfig)
            ):
                cache_config = copy.copy(cache_config)
                cache_config.set_metadata_resolver(metadata_resolver)

            if cache is not None:
                if not isinstance(cache, CacheInterface):
                    raise ValueError("Cache must implement CacheInterface")

                self.cache = cache
            else:
                if self._cache_factory is not None:
                    self.cache = CacheProxy(self._cache_factory.get_cache())
                else:
                    self.cache = CacheFactory(cache_config).get_cache()

            # A caller who supplied a whole cache - ``cache=`` or ``cache_factory=`` - owns
            # the configuration inside it, and it cannot be swapped without rebuilding the
            # cache, so the resolver is set on it in place. This is the one path that writes
            # into the caller's configuration rather than into a copy of it, because a cache
            # reads its configuration on every lookup and ``CacheInterface`` exposes no way to
            # hand it a different one. Sharing one cache object across clients already shares
            # its entries, which couples them far more tightly than its eligibility does, so
            # the asymmetry with the ``cache_config=`` copy above is documented on
            # ``CacheConfig.set_metadata_resolver`` rather than removed.
            if metadata_resolver is not None and (
                cache is not None or self._cache_factory is not None
            ):
                own_config = self.cache.config
                if isinstance(own_config, CacheConfig):
                    own_config.set_metadata_resolver(metadata_resolver)

            init_csc_items()
            register_csc_items_callback(
                callback=lambda: self.cache.size,
                pool_name=get_pool_name(self),
            )

        connection_kwargs.pop("cache", None)
        connection_kwargs.pop("cache_config", None)

        # Resolve the HIMPORT registry. A pre-built ``himport_registry`` (shared, e.g.
        # from the cluster client) takes precedence; otherwise build a fresh empty one.
        # A registry always exists so runtime ``himport_prepare`` mutates a single object
        # every connection already shares. The object stays in ``connection_kwargs`` so
        # it reaches every connection. It is injected unconditionally (like other
        # auto-added pool kwargs), so a custom ``connection_class`` must accept
        # ``**kwargs`` (or a ``himport_registry`` parameter), as built-ins do.
        himport_registry = connection_kwargs.get("himport_registry")
        if himport_registry is None:
            himport_registry = HImportRegistry()
            connection_kwargs["himport_registry"] = himport_registry
        self.himport_registry = himport_registry

        # a lock to protect the critical section in _checkpid().
        # this lock is acquired when the process id changes, such as
        # after a fork. during this time, multiple threads in the child
        # process could attempt to acquire this lock. the first thread
        # to acquire the lock will reset the data structures and lock
        # object of this pool. subsequent threads acquiring this lock
        # will notice the first thread already did the work and simply
        # release the lock.

        self._fork_lock = threading.RLock()
        self._lock = threading.RLock()

        # Generate unique pool ID for observability (matches go-redis behavior)
        self._pool_id = secrets.token_hex(4)

        MaintNotificationsAbstractConnectionPool.__init__(
            self,
            maint_notifications_config=maint_notifications_config,
            **connection_kwargs,
        )

        self.reset()

    # Keys that should be redacted in __repr__ to avoid exposing sensitive information
    SENSITIVE_REPR_KEYS = frozenset(
        {
            "password",
            "username",
            "ssl_password",
            "credential_provider",
        }
    )

    # Internal plumbing kwargs omitted from __repr__ (not user-facing config).
    OMIT_REPR_KEYS = frozenset({"himport_registry"})

    def __repr__(self) -> str:
        conn_kwargs = ",".join(
            [
                f"{k}={'<REDACTED>' if k in self.SENSITIVE_REPR_KEYS else v}"
                for k, v in self.connection_kwargs.items()
                if k not in self.OMIT_REPR_KEYS
            ]
        )
        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__}"
            f"(<{self.connection_class.__module__}.{self.connection_class.__name__}"
            f"({conn_kwargs})>)>"
        )

    @property
    def connection_kwargs(self) -> Dict[str, Any]:
        return self._connection_kwargs

    @connection_kwargs.setter
    def connection_kwargs(self, value: Dict[str, Any]):
        self._connection_kwargs = value

    def get_protocol(self):
        """
        Returns:
            The RESP protocol version, or ``None`` if the protocol is not specified,
            in which case the server default will be used.
        """
        return self.connection_kwargs.get("protocol", None)

    def reset(self) -> None:
        is_fork = hasattr(self, "pid") and self.pid != os.getpid()
        if is_fork:
            # Inherited connections were counted by the parent, not this process.
            self._pool_id = secrets.token_hex(4)
            # A parent thread may have held the lock at fork time, and the
            # handler reset below takes it to revert an active MOVING.
            self._lock = threading.RLock()

        # Record metrics for connections being removed before clearing
        # (only if attributes exist - they won't during __init__)
        if (
            not is_fork
            and hasattr(self, "_available_connections")
            and hasattr(self, "_in_use_connections")
        ):
            with self._lock:
                idle_count = len(self._available_connections)
                in_use_count = len(self._in_use_connections)
                if idle_count > 0 or in_use_count > 0:
                    pool_name = get_pool_name(self)
                    if idle_count > 0:
                        record_connection_count(
                            pool_name=pool_name,
                            connection_state=ConnectionState.IDLE,
                            counter=-idle_count,
                        )
                    if in_use_count > 0:
                        record_connection_count(
                            pool_name=pool_name,
                            connection_state=ConnectionState.USED,
                            counter=-in_use_count,
                        )

        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

        self._on_reset_after_fork()

        # this must be the last operation in this method. while reset() is
        # called when holding _fork_lock, other threads in this process
        # can call _checkpid() which compares self.pid and os.getpid() without
        # holding any lock (for performance reasons). keeping this assignment
        # as the last operation ensures that those other threads will also
        # notice a pid difference and block waiting for the first thread to
        # release _fork_lock. when each of these threads eventually acquire
        # _fork_lock, they will notice that another thread already called
        # reset() and they will immediately release _fork_lock and continue on.
        self.pid = os.getpid()

    def __del__(self) -> None:
        """Clean up connection pool and record metrics when garbage collected."""
        try:
            if getattr(self, "pid", None) != os.getpid():
                return
            if not hasattr(self, "_available_connections") or not hasattr(
                self, "_in_use_connections"
            ):
                return
            # Record metrics for all connections being removed
            idle_count = len(self._available_connections)
            in_use_count = len(self._in_use_connections)
            if idle_count > 0 or in_use_count > 0:
                pool_name = get_pool_name(self)
                if idle_count > 0:
                    record_connection_count(
                        pool_name=pool_name,
                        connection_state=ConnectionState.IDLE,
                        counter=-idle_count,
                    )
                if in_use_count > 0:
                    record_connection_count(
                        pool_name=pool_name,
                        connection_state=ConnectionState.USED,
                        counter=-in_use_count,
                    )
        except Exception:
            pass

    def _checkpid(self) -> None:
        # _checkpid() attempts to keep ConnectionPool fork-safe on modern
        # systems. this is called by all ConnectionPool methods that
        # manipulate the pool's state such as get_connection() and release().
        #
        # _checkpid() determines whether the process has forked by comparing
        # the current process id to the process id saved on the ConnectionPool
        # instance. if these values are the same, _checkpid() simply returns.
        #
        # when the process ids differ, _checkpid() assumes that the process
        # has forked and that we're now running in the child process. the child
        # process cannot use the parent's file descriptors (e.g., sockets).
        # therefore, when _checkpid() sees the process id change, it calls
        # reset() in order to reinitialize the child's ConnectionPool. this
        # will cause the child to make all new connection objects.
        #
        # _checkpid() is protected by self._fork_lock to ensure that multiple
        # threads in the child process do not call reset() multiple times.
        #
        # there is an extremely small chance this could fail in the following
        # scenario:
        #   1. process A calls _checkpid() for the first time and acquires
        #      self._fork_lock.
        #   2. while holding self._fork_lock, process A forks (the fork()
        #      could happen in a different thread owned by process A)
        #   3. process B (the forked child process) inherits the
        #      ConnectionPool's state from the parent. that state includes
        #      a locked _fork_lock. process B will not be notified when
        #      process A releases the _fork_lock and will thus never be
        #      able to acquire the _fork_lock.
        #
        # to mitigate this possible deadlock, _checkpid() will only wait 5
        # seconds to acquire _fork_lock. if _fork_lock cannot be acquired in
        # that time it is assumed that the child is deadlocked and a
        # redis.ChildDeadlockedError error is raised.
        if self.pid != os.getpid():
            acquired = self._fork_lock.acquire(timeout=5)
            if not acquired:
                raise ChildDeadlockedError
            # reset() the instance for the new process if another thread
            # hasn't already done so
            try:
                if self.pid != os.getpid():
                    self.reset()
            finally:
                self._fork_lock.release()

    @deprecated_args(
        args_to_warn=["*"],
        reason="Use get_connection() without args instead",
        version="5.3.0",
    )
    def get_connection(self, command_name=None, *keys, **options) -> "Connection":
        "Get a connection from the pool"

        # Start timing for observability
        self._checkpid()
        is_created = False

        with self._lock:
            try:
                connection = self._available_connections.pop()
            except IndexError:
                # Start timing for observability
                start_time_created = time.monotonic()

                connection = self.make_connection()
                is_created = True
            self._in_use_connections.add(connection)

        # Record state transition for observability
        pool_name = get_pool_name(self)
        if is_created:
            # New connection created and acquired: just USED +1
            # (connection was never idle in the pool)
            record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.USED,
                counter=1,
            )
        else:
            # Existing connection acquired from pool: IDLE -> USED
            record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.IDLE,
                counter=-1,
            )
            record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.USED,
                counter=1,
            )

        try:
            self.ensure_connection(connection)
        except BaseException:
            # release the connection back to the pool so that we don't
            # leak it
            self.release(connection)
            raise

        if is_created:
            record_connection_create_time(
                connection_pool=self,
                duration_seconds=time.monotonic() - start_time_created,
            )

        return connection

    def ensure_connection(self, connection: "Connection") -> None:
        """Ensure that the connection object is connected and valid"""
        connection.connect()
        # connections that the pool provides should be ready to send
        # a command. if not, the connection was either returned to the
        # pool before all data has been read or the socket has been
        # closed. either way, reconnect and verify everything is good.
        # With maintenance notifications enabled the pending data may be
        # push notifications that have to be applied rather than a reason
        # to reconnect; reading them is what tells the two apart, and it
        # raises for a socket the server has closed in the meantime.
        try:
            if connection.can_read():
                if self.maint_notifications_enabled():
                    connection.handle_pending_push_notifications()
                elif self.cache is None:
                    raise ConnectionError("Connection has data")
        except (ConnectionError, TimeoutError, OSError):
            connection.disconnect()
            connection.connect()
            if (
                connection.can_read()
                and self.cache is None
                and not self.maint_notifications_enabled()
            ):
                raise ConnectionError("Connection not ready")

    def get_encoder(self) -> Encoder:
        "Return an encoder based on encoding settings"
        kwargs = self.connection_kwargs
        return Encoder(
            encoding=kwargs.get("encoding", "utf-8"),
            encoding_errors=kwargs.get("encoding_errors", "strict"),
            decode_responses=kwargs.get("decode_responses", False),
        )

    def make_connection(self) -> "ConnectionInterface":
        "Create a new connection"
        if self._created_connections >= self.max_connections:
            raise MaxConnectionsError("Too many connections")

        kwargs = dict(self.connection_kwargs)

        # Create the connection first, then record metrics only on success
        if self.cache is not None:
            connection = CacheProxyConnection(
                self.connection_class(**kwargs), self.cache, self._lock
            )
        else:
            connection = self.connection_class(**kwargs)

        # Only a connection that exists can be released, so count it here.
        self._created_connections += 1

        return connection

    def release(self, connection: "Connection") -> None:
        "Releases the connection back to the pool"
        self._checkpid()
        with self._lock:
            try:
                self._in_use_connections.remove(connection)
            except KeyError:
                # Gracefully fail when a connection is returned to this pool
                # that the pool doesn't actually own
                return

            if self.owns_connection(connection):
                if connection.should_reconnect():
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            "Disconnecting released connection marked for reconnect: "
                            f"{connection}, {connection.extract_connection_details()}"
                        )
                    connection.disconnect()
                self._available_connections.append(connection)
                self._event_dispatcher.dispatch(
                    AfterConnectionReleasedEvent(connection)
                )

                # Record state transition: USED -> IDLE
                pool_name = get_pool_name(self)
                record_connection_count(
                    pool_name=pool_name,
                    connection_state=ConnectionState.USED,
                    counter=-1,
                )
                record_connection_count(
                    pool_name=pool_name,
                    connection_state=ConnectionState.IDLE,
                    counter=1,
                )
            else:
                # Pool doesn't own this connection, do not add it back
                # to the pool.
                connection.disconnect()
                # Subclasses such as SentinelConnectionPool can override
                # owns_connection() with a comparison different from local PID
                # ownership. When such a subclass rejects a connection, also require
                # connection.pid == self.pid before reclaiming its slot.
                if connection.pid == self.pid:
                    self._created_connections -= 1
                    # The connection was checked out by this process, so
                    # decrement USED, which was counted in get_connection().
                    # Connections inherited across a fork are skipped here,
                    # their accounting belongs to the parent process.
                    record_connection_count(
                        pool_name=get_pool_name(self),
                        connection_state=ConnectionState.USED,
                        counter=-1,
                    )
                return

    def owns_connection(self, connection: "Connection") -> int:
        return connection.pid == self.pid

    def disconnect(self, inuse_connections: bool = True) -> None:
        """
        Disconnects connections in the pool

        If ``inuse_connections`` is True, disconnect connections that are
        currently in use, potentially by other threads. Otherwise only disconnect
        connections that are idle in the pool.
        """
        self._checkpid()
        with self._lock:
            if inuse_connections:
                connections = chain(
                    self._available_connections, self._in_use_connections
                )
            else:
                connections = self._available_connections

            for connection in connections:
                connection.disconnect()

    def close(self) -> None:
        """Close the pool, disconnecting all connections"""
        # A forked child must not touch the handler state inherited from the
        # parent: reset it first (the lock may be held by a parent thread).
        self._checkpid()
        try:
            self._on_close()
        finally:
            self.disconnect()

    def __enter__(self: _CP) -> _CP:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def set_retry(self, retry: Retry) -> None:
        self.connection_kwargs.update({"retry": retry})
        for conn in self._available_connections:
            conn.retry = retry
        for conn in self._in_use_connections:
            conn.retry = retry

    def re_auth_callback(self, token: TokenInterface):
        first_error = None
        with self._lock:
            for conn in self._available_connections:
                try:
                    conn.retry.call_with_retry(
                        lambda: conn.send_command(
                            "AUTH", token.try_get("oid"), token.get_value()
                        ),
                        lambda error: self._mock(error),
                    )
                    conn.retry.call_with_retry(
                        lambda: conn.read_response(),
                        lambda error: self._mock(error),
                        is_retryable=lambda error: not isinstance(
                            error, AuthenticationError
                        ),
                    )
                except AuthenticationError:
                    raise
                except Exception as e:
                    if first_error is None:
                        first_error = e
            for conn in self._in_use_connections:
                conn.set_re_auth_token(token)
        if first_error is not None:
            raise first_error

    def _get_pool_lock(self):
        return self._lock

    def _get_free_connections(self):
        with self._lock:
            return list(self._available_connections)

    def _get_in_use_connections(self):
        with self._lock:
            return set(self._in_use_connections)

    def _mock(self, error: RedisError):
        """
        Dummy functions, needs to be passed as error callback to retry object.
        :param error:
        :return:
        """
        pass

    def get_connection_count(self) -> List[tuple[int, dict]]:
        from redis.observability.attributes import get_pool_name

        attributes = AttributeBuilder.build_base_attributes()
        attributes[DB_CLIENT_CONNECTION_POOL_NAME] = get_pool_name(self)
        free_connections_attributes = attributes.copy()
        in_use_connections_attributes = attributes.copy()

        free_connections_attributes[DB_CLIENT_CONNECTION_STATE] = (
            ConnectionState.IDLE.value
        )
        in_use_connections_attributes[DB_CLIENT_CONNECTION_STATE] = (
            ConnectionState.USED.value
        )

        return [
            (len(self._get_free_connections()), free_connections_attributes),
            (len(self._get_in_use_connections()), in_use_connections_attributes),
        ]


class BlockingConnectionPool(ConnectionPool):
    """
    Thread-safe blocking connection pool::

        >>> from redis import Redis, BlockingConnectionPool
        >>> client = Redis(connection_pool=BlockingConnectionPool())

    It performs the same function as the default
    :py:class:`~redis.connection.ConnectionPool` implementation, in that,
    it maintains a pool of reusable connections that can be shared by
    multiple redis clients (safely across threads if required).

    The difference is that, in the event that a client tries to get a
    connection from the pool when all of connections are in use, rather than
    raising a :py:class:`~redis.exceptions.MaxConnectionsError` (as the default
    :py:class:`~redis.connection.ConnectionPool` implementation does), it
    makes the client wait ("blocks") for a specified number of seconds until
    a connection becomes available.

    Use ``max_connections`` to increase / decrease the pool size::

        >>> pool = BlockingConnectionPool(max_connections=10)

    Use ``timeout`` to tell it either how many seconds to wait for a connection
    to become available, or to block forever:

        >>> # Block forever.
        >>> pool = BlockingConnectionPool(timeout=None)

        >>> # Raise a ``ConnectionError`` after five seconds if a connection is
        >>> # not available.
        >>> pool = BlockingConnectionPool(timeout=5)
    """

    def __init__(
        self,
        max_connections=50,
        timeout=20,
        connection_class=Connection,
        queue_class=LifoQueue,
        **connection_kwargs,
    ):
        self.queue_class = queue_class
        self.timeout = timeout
        self._in_maintenance = False
        self._locked = False
        super().__init__(
            connection_class=connection_class,
            max_connections=max_connections,
            **connection_kwargs,
        )

    def reset(self):
        is_fork = hasattr(self, "pid") and self.pid != os.getpid()
        if is_fork:
            # Inherited connections were counted by the parent, not this process.
            self._pool_id = secrets.token_hex(4)
            # A parent thread may have held the lock at fork time, and the
            # handler reset below takes it to revert an active MOVING.
            self._lock = threading.RLock()
            # The thread applying a MOVING in the parent does not exist here
            # to clear these, and a stale flag makes a checkout nest on the
            # lock and leak it.
            self._in_maintenance = False
            self._locked = False

        # Create and fill up a thread safe queue with ``None`` values.
        try:
            if self._in_maintenance:
                self._lock.acquire()
                self._locked = True

            # Record metrics for connections being removed before clearing
            # Note: Access pool.queue directly to avoid deadlock since we may
            # already hold self._lock (which is non-reentrant)
            if (
                not is_fork
                and hasattr(self, "_connections")
                and self._connections
                and hasattr(self, "pool")
            ):
                with self._lock:
                    connections_in_queue = {conn for conn in self.pool.queue if conn}
                    idle_count = len(connections_in_queue)
                    in_use_count = len(self._connections) - idle_count
                    if idle_count > 0 or in_use_count > 0:
                        pool_name = get_pool_name(self)
                        if idle_count > 0:
                            record_connection_count(
                                pool_name=pool_name,
                                connection_state=ConnectionState.IDLE,
                                counter=-idle_count,
                            )
                        if in_use_count > 0:
                            record_connection_count(
                                pool_name=pool_name,
                                connection_state=ConnectionState.USED,
                                counter=-in_use_count,
                            )

            self.pool = self.queue_class(self.max_connections)
            while True:
                try:
                    self.pool.put_nowait(None)
                except Full:
                    break

            # Keep a list of actual connection instances so that we can
            # disconnect them later.
            self._connections = []
        finally:
            if self._locked:
                try:
                    self._lock.release()
                except Exception:
                    pass
                self._locked = False

        self._on_reset_after_fork()

        # this must be the last operation in this method. while reset() is
        # called when holding _fork_lock, other threads in this process
        # can call _checkpid() which compares self.pid and os.getpid() without
        # holding any lock (for performance reasons). keeping this assignment
        # as the last operation ensures that those other threads will also
        # notice a pid difference and block waiting for the first thread to
        # release _fork_lock. when each of these threads eventually acquire
        # _fork_lock, they will notice that another thread already called
        # reset() and they will immediately release _fork_lock and continue on.
        self.pid = os.getpid()

    def __del__(self) -> None:
        """Clean up connection pool and record metrics when garbage collected."""
        try:
            if getattr(self, "pid", None) != os.getpid():
                return
            # Note: Access pool.queue directly to avoid potential deadlock
            # if GC runs while the lock is held by the same thread
            if (
                hasattr(self, "_connections")
                and self._connections
                and hasattr(self, "pool")
            ):
                connections_in_queue = {conn for conn in self.pool.queue if conn}
                idle_count = len(connections_in_queue)
                in_use_count = len(self._connections) - idle_count
                if idle_count > 0 or in_use_count > 0:
                    pool_name = get_pool_name(self)
                    if idle_count > 0:
                        record_connection_count(
                            pool_name=pool_name,
                            connection_state=ConnectionState.IDLE,
                            counter=-idle_count,
                        )
                    if in_use_count > 0:
                        record_connection_count(
                            pool_name=pool_name,
                            connection_state=ConnectionState.USED,
                            counter=-in_use_count,
                        )
        except Exception:
            pass

    def make_connection(self):
        "Make a fresh connection."
        try:
            if self._in_maintenance:
                self._lock.acquire()
                self._locked = True

            if self.cache is not None:
                connection = CacheProxyConnection(
                    self.connection_class(**self.connection_kwargs),
                    self.cache,
                    self._lock,
                )
            else:
                connection = self.connection_class(**self.connection_kwargs)
            self._connections.append(connection)

            return connection
        finally:
            if self._locked:
                try:
                    self._lock.release()
                except Exception:
                    pass
                self._locked = False

    @deprecated_args(
        args_to_warn=["*"],
        reason="Use get_connection() without args instead",
        version="5.3.0",
    )
    def get_connection(self, command_name=None, *keys, **options):
        """
        Get a connection, blocking for ``self.timeout`` until a connection
        is available from the pool.

        If the connection returned is ``None`` then creates a new connection.
        Because we use a last-in first-out queue, the existing connections
        (having been returned to the pool after the initial ``None`` values
        were added) will be returned before ``None`` values. This means we only
        create new connections when we need to, i.e.: the actual number of
        connections will only increase in response to demand.
        """
        start_time_acquired = time.monotonic()
        # Make sure we haven't changed process.
        self._checkpid()
        is_created = False

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a ``ConnectionError``.
        connection = None
        try:
            if self._in_maintenance:
                self._lock.acquire()
                self._locked = True
            try:
                connection = self.pool.get(block=True, timeout=self.timeout)
            except Empty:
                record_connection_timeout(pool_name=get_pool_name(self))
                # Note that this is not caught by the redis client and will be
                # raised unless handled by application code. If you want never to
                raise ConnectionError("No connection available.")

            # If the ``connection`` is actually ``None`` then that's a cue to make
            # a new connection to add to the pool.
            if connection is None:
                # Start timing for observability
                start_time_created = time.monotonic()
                try:
                    connection = self.make_connection()
                except BaseException:
                    # Hand the slot back, or the pool shrinks on every failure.
                    try:
                        self.pool.put_nowait(None)
                    except Full:
                        pass
                    raise
                is_created = True
        finally:
            if self._locked:
                try:
                    self._lock.release()
                except Exception:
                    pass
                self._locked = False

        # Record state transition for observability
        pool_name = get_pool_name(self)
        if is_created:
            # New connection created and acquired: just USED +1
            # (connection was never idle in the pool)
            record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.USED,
                counter=1,
            )
        else:
            # Existing connection acquired from pool: IDLE -> USED
            record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.IDLE,
                counter=-1,
            )
            record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.USED,
                counter=1,
            )

        try:
            self.ensure_connection(connection)
        except BaseException:
            # release the connection back to the pool so that we don't leak it
            self.release(connection)
            raise

        if is_created:
            record_connection_create_time(
                connection_pool=self,
                duration_seconds=time.monotonic() - start_time_created,
            )

        record_connection_wait_time(
            pool_name=pool_name,
            duration_seconds=time.monotonic() - start_time_acquired,
        )

        return connection

    def release(self, connection):
        "Releases the connection back to the pool."
        # Make sure we haven't changed process.
        self._checkpid()

        try:
            if self._in_maintenance:
                self._lock.acquire()
                self._locked = True
            if not self.owns_connection(connection):
                # pool doesn't own this connection. do not add it back
                # to the pool. instead add a None value which is a placeholder
                # that will cause the pool to recreate the connection if
                # its needed.
                connection.disconnect()
                self.pool.put_nowait(None)
                # Subclasses can override owns_connection() with a comparison
                # different from local PID ownership. When such a subclass
                # rejects a connection checked out by this process, stop
                # tracking it and decrement USED, which was counted in
                # get_connection(). Connections inherited across a fork are
                # skipped here, their accounting belongs to the parent process.
                if connection.pid == self.pid:
                    try:
                        self._connections.remove(connection)
                    except ValueError:
                        pass
                    record_connection_count(
                        pool_name=get_pool_name(self),
                        connection_state=ConnectionState.USED,
                        counter=-1,
                    )
                return
            if connection.should_reconnect():
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "Disconnecting released connection marked for reconnect: "
                        f"{connection}, {connection.extract_connection_details()}"
                    )
                connection.disconnect()
            # Put the connection back into the pool.
            pool_name = get_pool_name(self)
            try:
                self.pool.put_nowait(connection)

                # Record state transition: USED -> IDLE
                record_connection_count(
                    pool_name=pool_name,
                    connection_state=ConnectionState.USED,
                    counter=-1,
                )
                record_connection_count(
                    pool_name=pool_name,
                    connection_state=ConnectionState.IDLE,
                    counter=1,
                )
            except Full:
                pass
        finally:
            if self._locked:
                try:
                    self._lock.release()
                except Exception:
                    pass
                self._locked = False

    def disconnect(self, inuse_connections: bool = True):
        """
        Disconnects either all connections in the pool or just the free connections.
        """
        self._checkpid()
        try:
            if self._in_maintenance:
                self._lock.acquire()
                self._locked = True

            if inuse_connections:
                connections = self._connections
            else:
                connections = self._get_free_connections()

            for connection in connections:
                connection.disconnect()
        finally:
            if self._locked:
                try:
                    self._lock.release()
                except Exception:
                    pass
                self._locked = False

    def _get_free_connections(self):
        with self._lock:
            return {conn for conn in self.pool.queue if conn}

    def _get_in_use_connections(self):
        with self._lock:
            # free connections
            connections_in_queue = {conn for conn in self.pool.queue if conn}
            # in self._connections we keep all created connections
            # so the ones that are not in the queue are the in use ones
            return {
                conn for conn in self._connections if conn not in connections_in_queue
            }

    def set_in_maintenance(self, in_maintenance: bool):
        """
        Sets a flag that this Blocking ConnectionPool is in maintenance mode.

        This is used to prevent new connections from being created while we are in maintenance mode.
        The pool will be in maintenance mode only when we are processing a MOVING notification.
        """
        self._in_maintenance = in_maintenance
