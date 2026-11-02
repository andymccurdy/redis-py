import asyncio
import copy
import inspect
import logging
import math
import socket
import sys
import time
import warnings
import weakref
from abc import ABC, abstractmethod
from itertools import chain
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Set,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
    Union,
)
from urllib.parse import ParseResult, parse_qs, unquote, urlparse

from ..observability.attributes import (
    DB_CLIENT_CONNECTION_POOL_NAME,
    DB_CLIENT_CONNECTION_STATE,
    AttributeBuilder,
    ConnectionState,
    get_pool_name,
)
from ..utils import SSL_AVAILABLE, deprecated_function

if SSL_AVAILABLE:
    import ssl
    from ssl import SSLContext, TLSVersion, VerifyFlags
else:
    ssl = None
    TLSVersion = None
    SSLContext = None
    VerifyFlags = None

from ..auth.token import TokenInterface
from ..driver_info import DriverInfo, resolve_driver_info
from ..event import AsyncAfterConnectionReleasedEvent, EventDispatcher
from ..utils import deprecated_args, format_error_message, is_os_level_timeout

# the functionality is available in 3.11.x but has a major issue before
# 3.11.3. See https://github.com/redis/redis-py/issues/2633
if sys.version_info >= (3, 11, 3):
    from asyncio import timeout as async_timeout
else:
    from async_timeout import timeout as async_timeout

from redis import exceptions as redis_exceptions
from redis.asyncio.maint_notifications import (
    AsyncMaintNotificationsConnectionHandler,
    AsyncMaintNotificationsPoolHandler,
    AsyncOSSMaintNotificationsHandler,
)
from redis.asyncio.observability.recorder import (
    record_connection_closed,
    record_connection_count,
    record_connection_create_time,
    record_connection_timeout,
    record_connection_wait_time,
    record_error_count,
)
from redis.asyncio.retry import Retry, _to_async_retry
from redis.backoff import NoBackoff
from redis.connection import HiredisRespSerializer
from redis.credentials import CredentialProvider, UsernamePasswordCredentialProvider
from redis.event import MaintenanceCompletedEvent
from redis.exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    ConnectionError,
    DataError,
    MaxConnectionsError,
    RedisError,
    ResponseError,
    TimeoutError,
)
from redis.himport import HImportRegistry
from redis.maint_notifications import (
    PENDING_PUSH_NOTIFICATIONS_READ_TIMEOUT,
    MaintenanceState,
    MaintNotificationsConfig,
    NodeMovingNotification,
    _build_moving_cleanup_connection_kwargs,
    _build_moving_connection_kwargs,
    _dispatch_maintenance_event,
)
from redis.observability.metrics import CloseReason
from redis.typing import EncodableT
from redis.utils import (
    DEFAULT_RESP_VERSION,
    HIREDIS_AVAILABLE,
    SENTINEL,
    check_protocol_version,
    str_if_bytes,
)

from .._defaults import (
    DEFAULT_SOCKET_CONNECT_TIMEOUT,
    DEFAULT_SOCKET_READ_SIZE,
    DEFAULT_SOCKET_TIMEOUT,
    get_default_socket_keepalive_options,
)
from .._parsers import (
    UNRECOVERABLE_PARSE_ERRORS,
    AsyncPushNotificationsParser,
    BaseParser,
    Encoder,
    _AsyncHiredisParser,
    _AsyncRESP2Parser,
    _AsyncRESP3Parser,
)

SYM_STAR = b"*"
SYM_DOLLAR = b"$"
SYM_CRLF = b"\r\n"
SYM_LF = b"\n"
SYM_EMPTY = b""

DefaultParser: Type[Union[_AsyncRESP2Parser, _AsyncRESP3Parser, _AsyncHiredisParser]]
if HIREDIS_AVAILABLE:
    DefaultParser = _AsyncHiredisParser
else:
    DefaultParser = _AsyncRESP3Parser

logger = logging.getLogger(__name__)


def add_debug_log_for_connection_failure(
    connection: "AbstractConnection",
    error: BaseException,
    operation: str,
) -> None:
    """
    Render the connection's live state on a failure that is about to close it.

    Must be called *before* ``disconnect()``. ``extract_connection_details()``
    reads the local port and the in-flight read deadline off the transport, so
    once it is gone it can only report ``not connected`` - which hides exactly
    the state needed to explain the failure. In particular it is what tells
    apart a read that ran under the original timeout from one that ran under a
    relaxed maintenance timeout.
    """
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            f"{type(error).__name__} while {operation}, "
            f"with connection: {connection}, "
            f"details: {connection.extract_connection_details()}, "
            f"error: {error}",
        )


class ConnectCallbackProtocol(Protocol):
    def __call__(self, connection: "AbstractConnection"): ...


class AsyncConnectCallbackProtocol(Protocol):
    async def __call__(self, connection: "AbstractConnection"): ...


ConnectCallbackT = Union[ConnectCallbackProtocol, AsyncConnectCallbackProtocol]


class AsyncMaintNotificationsAbstractConnection:
    """
    Internal mixin for async maintenance notification state and parser handlers.

    The sync implementation uses the same mixin-style structure. The async
    version keeps the notification state and parser handler installation close
    to the connection without sending the server-side handshake; that is wired
    in a later step.
    """

    __slots__ = ()

    def __init__(
        self,
        maint_notifications_config: MaintNotificationsConfig | None,
        maint_notifications_pool_handler: (
            AsyncMaintNotificationsPoolHandler | None
        ) = None,
        maintenance_state: MaintenanceState = MaintenanceState.NONE,
        maintenance_notification_hash: int | None = None,
        orig_host_address: str | None = None,
        orig_socket_timeout: float | None = None,
        orig_socket_connect_timeout: float | None = None,
        oss_cluster_maint_notifications_handler: (
            AsyncOSSMaintNotificationsHandler | None
        ) = None,
        parser: BaseParser | None = None,
    ) -> None:
        self.maint_notifications_config = maint_notifications_config
        self.maintenance_state = maintenance_state
        self.maintenance_notification_hash = maintenance_notification_hash
        self._processed_start_maint_notifications: set[int] = set()
        self._skipped_end_maint_notifications: set[int] = set()
        self._configure_maintenance_notifications(
            maint_notifications_pool_handler,
            orig_host_address,
            orig_socket_timeout,
            orig_socket_connect_timeout,
            oss_cluster_maint_notifications_handler,
            parser,
        )

    @abstractmethod
    def _get_parser(self) -> BaseParser:
        pass

    def _get_push_notifications_parser(self) -> AsyncPushNotificationsParser:
        parser = self._get_parser()
        if not isinstance(parser, (_AsyncHiredisParser, _AsyncRESP3Parser)):
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

    async def handle_pending_push_notifications(self) -> None:
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
            async def deferred_handler(notification):
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
            while await self.can_read():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ConnectionError(
                        "Timed out reading pending push notifications"
                    )
                read = len(deferred)
                # The pool disconnects the connection it handed out on the error
                try:
                    await asyncio.wait_for(
                        self.read_response(
                            push_request=True, disconnect_on_error=False
                        ),
                        remaining,
                    )
                except asyncio.TimeoutError:
                    raise ConnectionError(
                        "Timed out reading pending push notifications"
                    ) from None
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
                await handler(notification)
            except Exception as e:
                logger.error(f"Error handling {notification}: {e}")

    @abstractmethod
    def get_protocol(self):
        pass

    @abstractmethod
    async def send_command(self, *args: Any, **kwargs: Any) -> None:
        pass

    @abstractmethod
    async def read_response(
        self,
        disable_decoding: bool = False,
        timeout: float | None = None,
        *,
        disconnect_on_error: bool = True,
        push_request: bool | None = False,
    ) -> Any:
        pass

    @abstractmethod
    def getpeername(self) -> str | None:
        pass

    @abstractmethod
    def extract_connection_details(self) -> str:
        pass

    def _configure_maintenance_notifications(
        self,
        maint_notifications_pool_handler: (
            AsyncMaintNotificationsPoolHandler | None
        ) = None,
        orig_host_address: str | None = None,
        orig_socket_timeout: float | None = None,
        orig_socket_connect_timeout: float | None = None,
        oss_cluster_maint_notifications_handler: (
            AsyncOSSMaintNotificationsHandler | None
        ) = None,
        parser: BaseParser | None = None,
    ) -> None:
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

        if not isinstance(parser, _AsyncHiredisParser) and not isinstance(
            parser, _AsyncRESP3Parser
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
            AsyncMaintNotificationsConnectionHandler(
                self, self.maint_notifications_config
            )
        )

        if oss_cluster_maint_notifications_handler:
            self._oss_cluster_maint_notifications_handler = (
                oss_cluster_maint_notifications_handler
            )
            parser.set_oss_cluster_maint_push_handler(
                oss_cluster_maint_notifications_handler.handle_notification
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
        self, maint_notifications_pool_handler: AsyncMaintNotificationsPoolHandler
    ) -> None:
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
                AsyncMaintNotificationsConnectionHandler(
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
        self,
        oss_cluster_maint_notifications_handler: AsyncOSSMaintNotificationsHandler,
    ) -> None:
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
                AsyncMaintNotificationsConnectionHandler(
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

    async def activate_maint_notifications_handling_if_enabled(
        self, check_health: bool = True
    ) -> None:
        # Send maintenance notifications handshake if RESP3 is active
        # and maintenance notifications are enabled
        # and we have a host to determine the endpoint type from
        # When the maint_notifications_config enabled mode is "auto",
        # we just log a warning if the handshake fails
        # When the mode is enabled=True, we raise an exception in case of failure
        host = getattr(self, "host", None)
        if (
            check_protocol_version(self.get_protocol(), 3)
            and self.maint_notifications_config
            and self.maint_notifications_config.enabled
            and self._maint_notifications_connection_handler
            and host is not None
        ):
            await self._enable_maintenance_notifications(
                maint_notifications_config=self.maint_notifications_config,
                check_health=check_health,
            )

    async def _enable_maintenance_notifications(
        self,
        maint_notifications_config: MaintNotificationsConfig,
        check_health: bool = True,
    ) -> None:
        try:
            host = getattr(self, "host", None)
            if host is None:
                raise ValueError(
                    "Cannot enable maintenance notifications for connection"
                    " object that doesn't have a host attribute."
                )

            endpoint_type = maint_notifications_config.get_endpoint_type(host, self)
            await self.send_command(
                "CLIENT",
                "MAINT_NOTIFICATIONS",
                "ON",
                "moving-endpoint-type",
                endpoint_type.value,
                check_health=check_health,
            )
            response = await self.read_response()
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

    def get_resolved_ip(self) -> str | None:
        """
        Extract the resolved IP address from an established connection or host.

        First tries to get the actual peer IP from the async stream writer, then
        falls back to DNS resolution if needed.

        Returns:
            The resolved IP address, or None if it cannot be determined.
        """

        # Method 1: Try to get the actual IP from the established stream.
        # This is most accurate as it shows the exact IP being used.
        try:
            peer_addr = self.getpeername()
            if peer_addr:
                return peer_addr
        except (AttributeError, OSError):
            # Stream might not be connected or peer address lookup might fail.
            pass

        # Method 2: Fall back to the configured host (which may be an IP or an
        # FQDN). Unlike the sync client we intentionally do NOT call
        # socket.getaddrinfo() here: this method runs on the event loop, so a
        # blocking DNS resolution would stall it. On the endpoint-type handshake
        # path (get_endpoint_type) getpeername() above always succeeds because the
        # writer was just connected, so this fallback is only reached by the
        # debug-log call sites during reconnects — where returning the host is
        # fine. A blocking getaddrinfo on an FQDN host there can freeze the loop
        # for seconds and trip unrelated connect timeouts.
        return getattr(self, "host", None)

    @property
    def maintenance_state(self) -> MaintenanceState:
        return self._maintenance_state

    @maintenance_state.setter
    def maintenance_state(self, state: MaintenanceState) -> None:
        self._maintenance_state = state

    def add_maint_start_notification(self, id: int) -> None:
        self._processed_start_maint_notifications.add(id)

    def get_processed_start_notifications(self) -> set[int]:
        return self._processed_start_maint_notifications

    def add_skipped_end_notification(self, id: int) -> None:
        self._skipped_end_maint_notifications.add(id)

    def get_skipped_end_notifications(self) -> set[int]:
        return self._skipped_end_maint_notifications

    def reset_received_notifications(self) -> None:
        self._processed_start_maint_notifications.clear()
        self._skipped_end_maint_notifications.clear()

    def update_current_socket_timeout(
        self, relaxed_timeout: float | None = None
    ) -> None:
        timeout = relaxed_timeout if relaxed_timeout != -1 else self.socket_timeout
        self._reschedule_active_read_timeout(timeout)

    def _reschedule_active_read_timeout(self, timeout: float | None) -> None:
        timeout_context = getattr(self, "_active_read_timeout", None)
        if timeout_context is None:
            # No read_response call is currently inside its socket timeout
            # context, so there is no in-flight deadline to relax or restore.
            return

        if timeout is None:
            # A None socket timeout means the active read should become blocking.
            # Python 3.11's timeout context supports clearing the deadline.
            if hasattr(timeout_context, "reschedule"):
                timeout_context.reschedule(None)
            # Older async-timeout contexts cannot clear a deadline, so reject the
            # current timeout instead of leaving a stale relaxed deadline active.
            elif hasattr(timeout_context, "reject"):
                timeout_context.reject()
            return

        # Active read timeouts are stored as loop-time deadlines, not durations.
        deadline = asyncio.get_running_loop().time() + timeout
        if hasattr(timeout_context, "reschedule"):
            # Python 3.11 asyncio.timeout exposes reschedule().
            timeout_context.reschedule(deadline)
        elif hasattr(timeout_context, "update"):
            # async-timeout exposes update() for the same deadline adjustment.
            timeout_context.update(deadline)

    def set_tmp_settings(
        self,
        tmp_host_address: str | object | None = SENTINEL,
        tmp_relaxed_timeout: float | None = -1,
    ) -> None:
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
    ) -> None:
        if reset_host_address:
            self.host = self.orig_host_address
        if reset_relaxed_timeout:
            self.socket_timeout = self.orig_socket_timeout
            self.socket_connect_timeout = self.orig_socket_connect_timeout


class AbstractConnection(AsyncMaintNotificationsAbstractConnection):
    """Manages communication to and from a Redis server"""

    __slots__ = (
        "db",
        "username",
        "client_name",
        "lib_name",
        "lib_version",
        "credential_provider",
        "password",
        "socket_timeout",
        "socket_connect_timeout",
        "redis_connect_func",
        "retry_on_timeout",
        "retry_on_error",
        "health_check_interval",
        "next_health_check",
        "last_active_at",
        "encoder",
        "ssl_context",
        "protocol",
        "_reader",
        "_writer",
        "_parser",
        "_active_read_timeout",
        "_connect_callbacks",
        "_buffer_cutoff",
        "_lock",
        "_socket_read_size",
        "__dict__",
    )

    @deprecated_args(
        args_to_warn=["lib_name", "lib_version"],
        reason="Use 'driver_info' parameter instead. "
        "lib_name and lib_version will be removed in a future version.",
    )
    def __init__(
        self,
        *,
        db: str | int = 0,
        password: str | None = None,
        socket_timeout: float | None = DEFAULT_SOCKET_TIMEOUT,
        socket_connect_timeout: float | None = DEFAULT_SOCKET_CONNECT_TIMEOUT,
        retry_on_timeout: bool = False,
        retry_on_error: Iterable[Type[Exception]] | object = SENTINEL,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        parser_class: Type[BaseParser] = DefaultParser,
        socket_read_size: int = DEFAULT_SOCKET_READ_SIZE,
        health_check_interval: float = 0,
        client_name: str | None = None,
        lib_name: str | object | None = SENTINEL,
        lib_version: str | object | None = SENTINEL,
        driver_info: DriverInfo | object | None = SENTINEL,
        username: str | None = None,
        retry: Retry | None = None,
        redis_connect_func: ConnectCallbackT | None = None,
        encoder_class: Type[Encoder] = Encoder,
        credential_provider: CredentialProvider | None = None,
        protocol: int | None = None,
        legacy_responses: bool = True,
        event_dispatcher: EventDispatcher | None = None,
        maint_notifications_config: MaintNotificationsConfig | None = None,
        maint_notifications_pool_handler: (
            AsyncMaintNotificationsPoolHandler | None
        ) = None,
        maintenance_state: MaintenanceState = MaintenanceState.NONE,
        maintenance_notification_hash: int | None = None,
        orig_host_address: str | None = None,
        orig_socket_timeout: float | None = None,
        orig_socket_connect_timeout: float | None = None,
        oss_cluster_maint_notifications_handler: (
            AsyncOSSMaintNotificationsHandler | None
        ) = None,
        himport_registry: HImportRegistry | None = None,
    ):
        """
        Initialize a new async Connection.

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
        self.db = db
        self.client_name = client_name

        # Handle driver_info: if provided, use it; otherwise create from lib_name/lib_version.
        self.driver_info = resolve_driver_info(driver_info, lib_name, lib_version)

        self.credential_provider = credential_provider
        self.password = password
        self.username = username
        self.socket_timeout = socket_timeout
        if socket_connect_timeout is None:
            socket_connect_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout
        self.retry_on_timeout = retry_on_timeout
        if retry_on_error is SENTINEL:
            retry_on_error = []
        else:
            # Copy so we never mutate the caller-supplied list (parity with sync).
            retry_on_error = list(retry_on_error)
        if retry_on_timeout:
            retry_on_error.append(TimeoutError)
            retry_on_error.append(socket.timeout)
            retry_on_error.append(asyncio.TimeoutError)
        self.retry_on_error = retry_on_error
        if retry or retry_on_error:
            if not retry:
                self.retry = Retry(NoBackoff(), 1)
            else:
                # deep-copy the Retry object as it is mutable
                self.retry = copy.deepcopy(_to_async_retry(retry))
            # Update the retry's supported errors with the specified errors
            self.retry.update_supported_errors(retry_on_error)
        else:
            self.retry = Retry(NoBackoff(), 0)
        self.health_check_interval = health_check_interval
        self.next_health_check: float = -1
        self.encoder = encoder_class(encoding, encoding_errors, decode_responses)
        self.redis_connect_func = redis_connect_func
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._socket_read_size = socket_read_size
        self._active_read_timeout = None
        self._connect_callbacks: List[weakref.WeakMethod[ConnectCallbackT]] = []
        self._buffer_cutoff = 6000
        self._re_auth_token: Optional[TokenInterface] = None
        self._command_packer = (
            HiredisRespSerializer(self._pack_command_python, self.encoder.encode)
            if HIREDIS_AVAILABLE
            else None
        )
        self._should_reconnect = False

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
        if parser_class != _AsyncHiredisParser:
            # The Python parsers are protocol-specific; hiredis supports both.
            if self.protocol == 3 and parser_class == _AsyncRESP2Parser:
                parser_class = _AsyncRESP3Parser
            elif self.protocol == 2 and parser_class == _AsyncRESP3Parser:
                parser_class = _AsyncRESP2Parser
        self.set_parser(parser_class)

        # HIMPORT client-side state. `himport_registry` is the shared client-level
        # registry (empty if unconfigured) and persists across reconnects.
        self.himport_registry = himport_registry
        self._reset_himport_state()

        AsyncMaintNotificationsAbstractConnection.__init__(
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
        )

    def __del__(self, _warnings: Any = warnings):
        # For some reason, the individual streams don't get properly garbage
        # collected and therefore produce no resource warnings.  We add one
        # here, in the same style as those from the stdlib.
        if getattr(self, "_writer", None):
            _warnings.warn(
                f"unclosed Connection {self!r}", ResourceWarning, source=self
            )

            try:
                asyncio.get_running_loop()
                self._close()
            except RuntimeError:
                # No actions been taken if pool already closed.
                pass

    def _close(self):
        """
        Internal method to silently close the connection without waiting
        """
        if self._writer:
            self._writer.close()
            self._writer = self._reader = None

    def __repr__(self):
        repr_args = ",".join((f"{k}={v}" for k, v in self.repr_pieces()))
        return f"<{self.__class__.__module__}.{self.__class__.__name__}({repr_args})>"

    @abstractmethod
    def repr_pieces(self):
        pass

    @property
    def is_connected(self):
        return self._reader is not None and self._writer is not None

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

    def set_parser(self, parser_class: Type[BaseParser]) -> None:
        """
        Creates a new instance of parser_class with socket size:
        _socket_read_size and assigns it to the parser for the connection
        :param parser_class: The required parser class
        """
        self._parser = parser_class(socket_read_size=self._socket_read_size)

    def _get_parser(self) -> BaseParser:
        return self._parser

    def getpeername(self) -> str | None:
        """
        Returns the peer name of the connection.
        """
        writer = self._writer
        if writer is None:
            return None
        peername = writer.get_extra_info("peername")
        if isinstance(peername, tuple) and peername:
            return str(peername[0])
        return None

    def extract_connection_details(self) -> str:
        """
        Render the connection's identity, maintenance state and effective timeouts.

        This is what the debug logs use to explain a failed or timed out command:

        - ``host`` vs ``orig host`` says whether the connection still points at the
          node being moved away from, or has already been repointed at the new one.
        - ``state`` says whether maintenance handling touched this connection at
          all, so an unaffected node's connections are distinguishable.
        - ``socket_timeout`` vs ``active read timeout`` says which timeout the read
          actually ran under. ``active read timeout`` is the remaining deadline of
          the in-flight ``read_response`` (``None`` when no read is in flight), so
          the two diverge for a command that was already reading when the relaxed
          timeout was applied.
        """
        writer = self._writer
        if writer is None:
            return "not connected"

        socket_address = None
        try:
            socket_name = writer.get_extra_info("sockname")
            # AF_UNIX sockets report a path string rather than a (host, port) tuple
            if isinstance(socket_name, tuple) and len(socket_name) > 1:
                socket_address = socket_name[1]
        except (AttributeError, OSError):
            pass

        # Unlike the sync client there is no timeout armed on the socket; the
        # deadline lives in the timeout context wrapping the in-flight read.
        active_read_timeout = None
        timeout_context = self._active_read_timeout
        if timeout_context is not None:
            try:
                when = timeout_context.when()
                if when is not None:
                    active_read_timeout = round(
                        when - asyncio.get_running_loop().time(), 3
                    )
            except (AttributeError, RuntimeError):
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

    async def connect(self):
        """Connects to the Redis server if not already connected"""
        # try once the socket connect with the handshake, retry the whole
        # connect/handshake flow based on retry policy
        await self.retry.call_with_retry(
            lambda: self.connect_check_health(
                check_health=True, retry_socket_connect=False
            ),
            lambda error, failure_count: self.disconnect(
                error=error, failure_count=failure_count
            ),
            with_failure_count=True,
        )

    async def connect_check_health(
        self, check_health: bool = True, retry_socket_connect: bool = True
    ):
        if self.is_connected:
            return
        # Track actual retry attempts for error reporting
        actual_retry_attempts = 0

        def failure_callback(error, failure_count):
            nonlocal actual_retry_attempts
            actual_retry_attempts = failure_count
            return self.disconnect(error=error, failure_count=failure_count)

        try:
            if retry_socket_connect:
                await self.retry.call_with_retry(
                    lambda: self._connect(),
                    failure_callback,
                    with_failure_count=True,
                )
            else:
                await self._connect()
        except asyncio.CancelledError:
            raise  # in 3.7 and earlier, this is an Exception, not BaseException
        except (socket.timeout, asyncio.TimeoutError):
            e = TimeoutError("Timeout connecting to server")
            await record_error_count(
                server_address=getattr(self, "host", None),
                server_port=getattr(self, "port", None),
                network_peer_address=getattr(self, "host", None),
                network_peer_port=getattr(self, "port", None),
                error_type=e,
                retry_attempts=actual_retry_attempts,
                is_internal=False,
            )
            raise e
        except OSError as e:
            e = ConnectionError(self._error_message(e))
            await record_error_count(
                server_address=getattr(self, "host", None),
                server_port=getattr(self, "port", None),
                network_peer_address=getattr(self, "host", None),
                network_peer_port=getattr(self, "port", None),
                error_type=e,
                retry_attempts=actual_retry_attempts,
                is_internal=False,
            )
            raise e
        except Exception as exc:
            raise ConnectionError(exc) from exc

        try:
            if not self.redis_connect_func:
                # Use the default on_connect function
                await self.on_connect_check_health(check_health=check_health)
            else:
                # Use the passed function redis_connect_func
                (
                    await self.redis_connect_func(self)
                    if asyncio.iscoroutinefunction(self.redis_connect_func)
                    else self.redis_connect_func(self)
                )
        except RedisError:
            # clean up after any error in on_connect
            await self.disconnect()
            raise
        except BaseException:
            # Likewise for whatever a credential provider raises and for a
            # cancelled task. A socket left in place reads as connected, so the
            # next command would run without AUTH and SELECT.
            await self.disconnect(nowait=True)
            raise

        # run any user callbacks. right now the only internal callback
        # is for pubsub channel/pattern resubscription
        # first, remove any dead weakrefs
        self._connect_callbacks = [ref for ref in self._connect_callbacks if ref()]
        for ref in self._connect_callbacks:
            callback = ref()
            task = callback(self)
            if task and inspect.isawaitable(task):
                await task

    def mark_for_reconnect(self):
        self._should_reconnect = True

    def should_reconnect(self):
        return self._should_reconnect

    def reset_should_reconnect(self):
        self._should_reconnect = False

    @abstractmethod
    async def _connect(self):
        pass

    @abstractmethod
    def _host_error(self) -> str:
        pass

    def _error_message(self, exception: BaseException) -> str:
        return format_error_message(self._host_error(), exception)

    def get_protocol(self):
        return self.protocol

    def _reset_himport_state(self) -> None:
        # A fresh server session has no prepared HIMPORT fieldsets, so the next
        # himport_set must re-prepare on this connection. ``_himport_prepared`` maps
        # fieldset name -> the version prepared on the server; ``_himport_reconciled
        # _revision`` is the registry revision this connection last reconciled discards
        # against. Both are reset on connect/disconnect since the session is gone.
        self._himport_prepared: dict[str, int] = {}
        self._himport_reconciled_revision: int = 0

    async def on_connect(self) -> None:
        """Initialize the connection, authenticate and select a database"""
        await self.on_connect_check_health(check_health=True)

    async def on_connect_check_health(self, check_health: bool = True) -> None:
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
            auth_args = await cred_provider.get_credentials_async()

            # if resp version is specified and we have auth args,
            # we need to send them via HELLO
        if auth_args and check_protocol_version(self.protocol, 3):
            if isinstance(self._parser, _AsyncRESP2Parser):
                self.set_parser(_AsyncRESP3Parser)
                # update cluster exception classes
                self._parser.EXCEPTION_CLASSES = parser.EXCEPTION_CLASSES
                self._parser.on_connect(self)
            if len(auth_args) == 1:
                auth_args = ["default", auth_args[0]]
            # avoid checking health here -- PING will fail if we try
            # to check the health prior to the AUTH
            await self.send_command(
                "HELLO", self.protocol, "AUTH", *auth_args, check_health=False
            )
            response = await self.read_response()
            if response.get(b"proto") != int(self.protocol) and response.get(
                "proto"
            ) != int(self.protocol):
                raise ConnectionError("Invalid RESP version")
        # avoid checking health here -- PING will fail if we try
        # to check the health prior to the AUTH
        elif auth_args:
            await self.send_command("AUTH", *auth_args, check_health=False)

            try:
                auth_response = await self.read_response()
            except AuthenticationWrongNumberOfArgsError:
                # a username and password were specified but the Redis
                # server seems to be < 6.0.0 which expects a single password
                # arg. retry auth with just the password.
                # https://github.com/andymccurdy/redis-py/issues/1274
                await self.send_command("AUTH", auth_args[-1], check_health=False)
                auth_response = await self.read_response()

            if str_if_bytes(auth_response) != "OK":
                raise AuthenticationError("Invalid Username or Password")

        # if resp version is specified, switch to it
        elif check_protocol_version(self.protocol, 3):
            if isinstance(self._parser, _AsyncRESP2Parser):
                self.set_parser(_AsyncRESP3Parser)
                # update cluster exception classes
                self._parser.EXCEPTION_CLASSES = parser.EXCEPTION_CLASSES
                self._parser.on_connect(self)
            await self.send_command("HELLO", self.protocol, check_health=check_health)
            response = await self.read_response()
            # if response.get(b"proto") != self.protocol and response.get(
            #     "proto"
            # ) != self.protocol:
            #     raise ConnectionError("Invalid RESP version")

        # Activate maintenance notifications for this connection
        # if enabled in the configuration
        # This is a no-op if maintenance notifications are not enabled
        await self.activate_maint_notifications_handling_if_enabled(
            check_health=check_health
        )

        # if a client_name is given, set it
        if self.client_name:
            await self.send_command(
                "CLIENT",
                "SETNAME",
                self.client_name,
                check_health=check_health,
            )
            if str_if_bytes(await self.read_response()) != "OK":
                raise ConnectionError("Error setting client name")

        # Set the library name and version from driver_info, pipeline for lower startup latency
        lib_name_sent = False
        lib_version_sent = False

        if self.driver_info and self.driver_info.formatted_name:
            await self.send_command(
                "CLIENT",
                "SETINFO",
                "LIB-NAME",
                self.driver_info.formatted_name,
                check_health=check_health,
            )
            lib_name_sent = True

        if self.driver_info and self.driver_info.lib_version:
            await self.send_command(
                "CLIENT",
                "SETINFO",
                "LIB-VER",
                self.driver_info.lib_version,
                check_health=check_health,
            )
            lib_version_sent = True

        # if a database is specified, switch to it. Also pipeline this
        if self.db:
            await self.send_command("SELECT", self.db, check_health=check_health)

        # read responses from pipeline
        for _ in range(sum([lib_name_sent, lib_version_sent])):
            try:
                await self.read_response()
            except ResponseError:
                pass

        if self.db:
            if str_if_bytes(await self.read_response()) != "OK":
                raise ConnectionError("Invalid Database")

    async def disconnect(
        self,
        nowait: bool = False,
        error: Optional[Exception] = None,
        failure_count: Optional[int] = None,
        health_check_failed: bool = False,
    ) -> None:
        """Disconnects from the Redis server"""
        # The server session is gone, so any HIMPORT fieldsets prepared on this
        # socket no longer exist; reset the tracking.
        self._reset_himport_state()
        # Likewise the relaxation a maintenance applied to it: restored and
        # reported here, ahead of a close that may time out or be skipped
        self._complete_maintenance_on_disconnect()
        # On Python 3.13+, asyncio.timeout() raises RuntimeError when called
        # outside a running Task (e.g. during GC finalization or event-loop
        # callbacks).  In that context we fall back to a synchronous close.
        # See https://github.com/redis/redis-py/issues/3856
        if asyncio.current_task() is None:
            self._parser.on_disconnect()
            self.reset_should_reconnect()
            self._close()
            return

        try:
            async with async_timeout(self.socket_connect_timeout):
                self._parser.on_disconnect()
                # Reset the reconnect flag
                self.reset_should_reconnect()
                if not self.is_connected:
                    return
                try:
                    self._writer.close()  # type: ignore[union-attr]
                    # wait for close to finish, except when handling errors and
                    # forcefully disconnecting.
                    if not nowait:
                        await self._writer.wait_closed()  # type: ignore[union-attr]
                except OSError:
                    pass
                finally:
                    self._reader = None
                    self._writer = None
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"Timed out closing connection after {self.socket_connect_timeout}"
            ) from None

        if error:
            if health_check_failed:
                close_reason = CloseReason.HEALTHCHECK_FAILED
            else:
                close_reason = CloseReason.ERROR

            if failure_count is not None and failure_count > self.retry.get_retries():
                await record_error_count(
                    server_address=getattr(self, "host", None),
                    server_port=getattr(self, "port", None),
                    network_peer_address=getattr(self, "host", None),
                    network_peer_port=getattr(self, "port", None),
                    error_type=error,
                    retry_attempts=failure_count,
                )

            await record_connection_closed(
                close_reason=close_reason,
                error_type=error,
            )
        else:
            await record_connection_closed(
                close_reason=CloseReason.APPLICATION_CLOSE,
            )

    async def _send_ping(self):
        """Send PING, expect PONG in return"""
        await self.send_command("PING", check_health=False)
        if str_if_bytes(await self.read_response()) != "PONG":
            raise ConnectionError("Bad response from PING health check")

    async def _ping_failed(self, error, failure_count):
        """Function to call when PING fails"""
        await self.disconnect(
            error=error, failure_count=failure_count, health_check_failed=True
        )

    async def check_health(self):
        """Check the health of the connection with a PING/PONG"""
        if (
            self.health_check_interval
            and asyncio.get_running_loop().time() > self.next_health_check
        ):
            await self.retry.call_with_retry(
                self._send_ping, self._ping_failed, with_failure_count=True
            )

    async def _send_packed_command(self, command: Iterable[bytes]) -> None:
        writer = self._writer
        if writer is None or writer.transport.is_closing():
            raise ConnectionError("Connection closed by the server before write")
        try:
            writer.writelines(command)
            await writer.drain()
        except (TypeError, AttributeError) as e:
            # CPython gh-136234 adds the missing connection-lost check in 3.13.10+
            # and 3.14.1+. Python 3.12, 3.13.0-3.13.9, and 3.14.0 can instead
            # leak TypeError or AttributeError from the transport (#4287).
            if writer.transport.is_closing():
                raise ConnectionError(
                    "Connection closed by the server while writing"
                ) from e
            raise

    async def send_packed_command(
        self, command: Union[bytes, str, Iterable[bytes]], check_health: bool = True
    ) -> None:
        if not self.is_connected:
            await self.connect_check_health(check_health=False)
        if check_health:
            await self.check_health()

        try:
            if isinstance(command, str):
                command = command.encode()
            if isinstance(command, bytes):
                command = [command]
            if self.socket_timeout:
                await asyncio.wait_for(
                    self._send_packed_command(command), self.socket_timeout
                )
            else:
                await self._send_packed_command(command)
        except asyncio.TimeoutError as e:
            add_debug_log_for_connection_failure(self, e, "writing command")
            await self.disconnect(nowait=True)
            if is_os_level_timeout(e):
                # ETIMEDOUT from the OS: the connection is dead, not slow.
                raise ConnectionError(
                    f"Error {e.errno} while writing to socket. {e.strerror}."
                ) from e
            raise TimeoutError("Timeout writing to socket") from None
        except OSError as e:
            add_debug_log_for_connection_failure(self, e, "writing command")
            await self.disconnect(nowait=True)
            if len(e.args) == 1:
                err_no, errmsg = "UNKNOWN", e.args[0]
            else:
                err_no = e.args[0]
                errmsg = e.args[1]
            raise ConnectionError(
                f"Error {err_no} while writing to socket. {errmsg}."
            ) from e
        except BaseException as e:
            # BaseExceptions can be raised when a socket send operation is not
            # finished, e.g. due to a timeout.  Ideally, a caller could then re-try
            # to send un-sent data. However, the send_packed_command() API
            # does not support it so there is no point in keeping the connection open.
            add_debug_log_for_connection_failure(self, e, "writing command")
            await self.disconnect(nowait=True)
            raise

    async def send_command(self, *args: Any, **kwargs: Any) -> None:
        """Pack and send a command to the Redis server"""
        await self.send_packed_command(
            self.pack_command(*args), check_health=kwargs.get("check_health", True)
        )

    @deprecated_function(
        version="8.0.0", reason="Use can_read() instead", name="can_read_destructive"
    )
    async def can_read_destructive(self) -> bool:
        """Check the socket to see if there's data loaded in the buffer."""
        try:
            return await self._parser.can_read()
        except OSError as e:
            await self.disconnect(nowait=True)
            host_error = self._host_error()
            raise ConnectionError(f"Error while reading from {host_error}: {e.args}")

    async def can_read(self) -> bool:
        """Check the socket to see if there's data loaded in the buffer."""
        # TODO: Rename this API; it detects pending data or dirty/closed
        # connection state, not only whether application data can be read.
        try:
            return await self._parser.can_read()
        except OSError as e:
            await self.disconnect(nowait=True)
            host_error = self._host_error()
            raise ConnectionError(f"Error while reading from {host_error}: {e.args}")

    async def read_response(
        self,
        disable_decoding: bool = False,
        timeout: float | None = None,
        *,
        disconnect_on_error: bool = True,
        push_request: bool | None = False,
    ):
        """Read the response from a previously sent command.

        ``timeout`` semantics:
        - ``None`` (default): fall back to ``self.socket_timeout``.
        - ``math.inf``: block indefinitely with no timeout. Used by PubSub
          blocking reads (``listen()`` / ``get_message(timeout=None)`` /
          ``parse_response(block=True)``) where the configured
          ``socket_timeout`` must not abort the read.
        - ``float``: apply that timeout in seconds for this single read.

        TODO(next-major): replace the ``math.inf`` opt-in with a SENTINEL
        default for ``timeout``. After that change, ``timeout=None`` will
        mean "no timeout, block until a response arrives" (matching the
        long-standing PubSub docstring contract) and the SENTINEL default
        will be the value that falls back to ``self.socket_timeout``.
        That swap is a breaking change, so it must wait for a major
        release. Until then, callers that need an indefinitely blocking
        read pass ``math.inf`` explicitly.
        """
        # TODO(next-major): drop the math.inf branch. Use SENTINEL as the
        # default for ``timeout`` and treat ``timeout is None`` as the
        # "no timeout" signal (matching the PubSub docstring contract).
        # Match only positive infinity here. ``-math.inf`` is not a valid
        # "block forever" signal and historically behaved as an already-
        # expired timeout; preserve that.
        if timeout == math.inf:
            read_timeout = None
        else:
            read_timeout = timeout if timeout is not None else self.socket_timeout
        host_error = self._host_error()
        try:
            if read_timeout is not None:
                timeout_context = async_timeout(read_timeout)
                if timeout is None:
                    async with timeout_context as active_timeout:
                        self._active_read_timeout = active_timeout
                        try:
                            response = await self._read_response_from_parser(
                                disable_decoding=disable_decoding,
                                push_request=push_request,
                            )
                        finally:
                            self._active_read_timeout = None
                else:
                    async with timeout_context:
                        response = await self._read_response_from_parser(
                            disable_decoding=disable_decoding,
                            push_request=push_request,
                        )
            else:
                response = await self._read_response_from_parser(
                    disable_decoding=disable_decoding,
                    push_request=push_request,
                )
        except asyncio.TimeoutError as e:
            if is_os_level_timeout(e):
                # The OS reported ETIMEDOUT on the socket (e.g. after a network
                # change): the connection is dead, not slow. Returning None here
                # made PubSub.listen() re-read the dead socket in a busy loop.
                if disconnect_on_error:
                    add_debug_log_for_connection_failure(self, e, "reading response")
                    await self.disconnect(nowait=True)
                raise ConnectionError(
                    f"Error while reading from {host_error} : {e.args}"
                )
            if timeout is not None:
                # user requested timeout, return None. Operation can be retried
                return None
            # it was a self.socket_timeout error.
            if disconnect_on_error:
                add_debug_log_for_connection_failure(self, e, "reading response")
                await self.disconnect(nowait=True)
            raise TimeoutError(f"Timeout reading from {host_error}")
        except OSError as e:
            if disconnect_on_error:
                add_debug_log_for_connection_failure(self, e, "reading response")
                await self.disconnect(nowait=True)
            raise ConnectionError(f"Error while reading from {host_error} : {e.args}")
        except UNRECOVERABLE_PARSE_ERRORS as e:
            # See the sync Connection.read_response and #4291. The async parser
            # re-parses from self._pos = 0 rather than rewinding a socket
            # buffer, but the consequence is the same: the bytes that already
            # failed to parse are still there, so the connection is not
            # reusable no matter what disconnect_on_error says.
            add_debug_log_for_connection_failure(self, e, "reading response")
            await self.disconnect(nowait=True)
            raise
        except BaseException as e:
            # Also by default close in case of BaseException.  A lot of code
            # relies on this behaviour when doing Command/Response pairs.
            # See #1128.
            if disconnect_on_error:
                add_debug_log_for_connection_failure(self, e, "reading response")
                await self.disconnect(nowait=True)
            raise

        if self.health_check_interval:
            next_time = asyncio.get_running_loop().time() + self.health_check_interval
            self.next_health_check = next_time

        if isinstance(response, ResponseError):
            try:
                raise response from None
            finally:
                del response  # avoid creating ref cycles
        return response

    async def _read_response_from_parser(
        self, disable_decoding: bool = False, push_request: bool | None = False
    ):
        if check_protocol_version(self.protocol, 3):
            return await self._parser.read_response(
                disable_decoding=disable_decoding, push_request=push_request
            )
        return await self._parser.read_response(disable_decoding=disable_decoding)

    def pack_command(self, *args: EncodableT) -> List[bytes]:
        if self._command_packer is not None:
            return self._command_packer.pack(*args)
        return self._pack_command_python(*args)

    def _pack_command_python(self, *args: EncodableT) -> List[bytes]:
        """Pack a series of arguments into the Redis protocol"""
        output = []
        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. These arguments should be bytestrings so that they are
        # not encoded.
        assert not isinstance(args[0], float)
        if isinstance(args[0], str):
            args = tuple(args[0].encode().split()) + args[1:]
        elif b" " in args[0]:
            args = tuple(args[0].split()) + args[1:]

        buff = SYM_EMPTY.join((SYM_STAR, str(len(args)).encode(), SYM_CRLF))

        buffer_cutoff = self._buffer_cutoff
        for arg in map(self.encoder.encode, args):
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

    def pack_commands(self, commands: Iterable[Iterable[EncodableT]]) -> List[bytes]:
        """Pack multiple commands into the Redis protocol"""
        output: List[bytes] = []
        pieces: List[bytes] = []
        buffer_length = 0
        buffer_cutoff = self._buffer_cutoff

        for cmd in commands:
            for chunk in self.pack_command(*cmd):
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

    def _socket_is_empty(self):
        """Check if the socket is empty"""
        return len(self._reader._buffer) == 0

    async def process_invalidation_messages(self):
        while not self._socket_is_empty():
            await self.read_response(push_request=True)

    def set_re_auth_token(self, token: TokenInterface):
        self._re_auth_token = token

    async def re_auth(self):
        if self._re_auth_token is not None:
            await self.send_command(
                "AUTH",
                self._re_auth_token.try_get("oid"),
                self._re_auth_token.get_value(),
            )
            await self.read_response()
            self._re_auth_token = None


class Connection(AbstractConnection):
    "Manages TCP communication to and from a Redis server"

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: str | int = 6379,
        socket_keepalive: bool = True,
        socket_keepalive_options: Mapping[int, int | bytes] | object | None = SENTINEL,
        socket_type: int = 0,
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
        self.host = host
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

    def _connection_arguments(self) -> Mapping:
        return {"host": self.host, "port": self.port}

    async def _connect(self):
        """Create a TCP socket connection"""
        async with async_timeout(self.socket_connect_timeout):
            reader, writer = await asyncio.open_connection(
                **self._connection_arguments()
            )
        self._reader = reader
        self._writer = writer
        sock = writer.transport.get_extra_info("socket")
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                # TCP_KEEPALIVE
                if self.socket_keepalive:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in self.socket_keepalive_options.items():
                        sock.setsockopt(socket.SOL_TCP, k, v)

            except (OSError, TypeError):
                # `socket_keepalive_options` might contain invalid options
                # causing an error. Do not leave the connection open.
                writer.close()
                raise

    def _host_error(self) -> str:
        return f"{self.host}:{self.port}"


class SSLConnection(Connection):
    """Manages SSL connections to and from the Redis server(s).
    This class extends the Connection class, adding SSL functionality, and making
    use of ssl.SSLContext (https://docs.python.org/3/library/ssl.html#ssl.SSLContext)
    """

    def __init__(
        self,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_cert_reqs: Union[str, ssl.VerifyMode] = "required",
        ssl_include_verify_flags: Optional[List["ssl.VerifyFlags"]] = None,
        ssl_exclude_verify_flags: Optional[List["ssl.VerifyFlags"]] = None,
        ssl_ca_certs: Optional[str] = None,
        ssl_ca_data: Optional[str] = None,
        ssl_ca_path: Optional[str] = None,
        ssl_check_hostname: bool = True,
        ssl_min_version: Optional[TLSVersion] = None,
        ssl_ciphers: Optional[str] = None,
        ssl_password: Optional[str] = None,
        **kwargs,
    ):
        if not SSL_AVAILABLE:
            raise RedisError("Python wasn't built with SSL support")

        self.ssl_context: RedisSSLContext = RedisSSLContext(
            keyfile=ssl_keyfile,
            certfile=ssl_certfile,
            cert_reqs=ssl_cert_reqs,
            include_verify_flags=ssl_include_verify_flags,
            exclude_verify_flags=ssl_exclude_verify_flags,
            ca_certs=ssl_ca_certs,
            ca_data=ssl_ca_data,
            ca_path=ssl_ca_path,
            check_hostname=ssl_check_hostname,
            min_version=ssl_min_version,
            ciphers=ssl_ciphers,
            password=ssl_password,
        )
        super().__init__(**kwargs)

    def _connection_arguments(self) -> Mapping:
        kwargs = super()._connection_arguments()
        kwargs["ssl"] = self.ssl_context.get()
        return kwargs

    @property
    def keyfile(self):
        return self.ssl_context.keyfile

    @property
    def certfile(self):
        return self.ssl_context.certfile

    @property
    def cert_reqs(self):
        return self.ssl_context.cert_reqs

    @property
    def include_verify_flags(self):
        return self.ssl_context.include_verify_flags

    @property
    def exclude_verify_flags(self):
        return self.ssl_context.exclude_verify_flags

    @property
    def ca_certs(self):
        return self.ssl_context.ca_certs

    @property
    def ca_data(self):
        return self.ssl_context.ca_data

    @property
    def check_hostname(self):
        return self.ssl_context.check_hostname

    @property
    def min_version(self):
        return self.ssl_context.min_version


class RedisSSLContext:
    __slots__ = (
        "keyfile",
        "certfile",
        "cert_reqs",
        "include_verify_flags",
        "exclude_verify_flags",
        "ca_certs",
        "ca_data",
        "ca_path",
        "context",
        "check_hostname",
        "min_version",
        "ciphers",
        "password",
    )

    def __init__(
        self,
        keyfile: Optional[str] = None,
        certfile: Optional[str] = None,
        cert_reqs: Optional[Union[str, ssl.VerifyMode]] = None,
        include_verify_flags: Optional[List["ssl.VerifyFlags"]] = None,
        exclude_verify_flags: Optional[List["ssl.VerifyFlags"]] = None,
        ca_certs: Optional[str] = None,
        ca_data: Optional[str] = None,
        ca_path: Optional[str] = None,
        check_hostname: bool = False,
        min_version: Optional[TLSVersion] = None,
        ciphers: Optional[str] = None,
        password: Optional[str] = None,
    ):
        if not SSL_AVAILABLE:
            raise RedisError("Python wasn't built with SSL support")

        self.keyfile = keyfile
        self.certfile = certfile
        if cert_reqs is None:
            cert_reqs = ssl.CERT_NONE
        elif isinstance(cert_reqs, str):
            CERT_REQS = {  # noqa: N806
                "none": ssl.CERT_NONE,
                "optional": ssl.CERT_OPTIONAL,
                "required": ssl.CERT_REQUIRED,
            }
            if cert_reqs not in CERT_REQS:
                raise RedisError(
                    f"Invalid SSL Certificate Requirements Flag: {cert_reqs}"
                )
            cert_reqs = CERT_REQS[cert_reqs]
        self.cert_reqs = cert_reqs
        self.include_verify_flags = include_verify_flags
        self.exclude_verify_flags = exclude_verify_flags
        self.ca_certs = ca_certs
        self.ca_data = ca_data
        self.ca_path = ca_path
        self.check_hostname = (
            check_hostname if self.cert_reqs != ssl.CERT_NONE else False
        )
        self.min_version = min_version
        self.ciphers = ciphers
        self.password = password
        self.context: Optional[SSLContext] = None

    def get(self) -> SSLContext:
        if not self.context:
            context = ssl.create_default_context()
            context.check_hostname = self.check_hostname
            context.verify_mode = self.cert_reqs
            if self.include_verify_flags:
                for flag in self.include_verify_flags:
                    context.verify_flags |= flag
            if self.exclude_verify_flags:
                for flag in self.exclude_verify_flags:
                    context.verify_flags &= ~flag
            if self.certfile or self.keyfile:
                context.load_cert_chain(
                    certfile=self.certfile,
                    keyfile=self.keyfile,
                    password=self.password,
                )
            if self.ca_certs or self.ca_data or self.ca_path:
                context.load_verify_locations(
                    cafile=self.ca_certs, capath=self.ca_path, cadata=self.ca_data
                )
            if self.min_version is not None:
                context.minimum_version = self.min_version
            if self.ciphers is not None:
                context.set_ciphers(self.ciphers)
            self.context = context
        return self.context


class UnixDomainSocketConnection(AbstractConnection):
    "Manages UDS communication to and from a Redis server"

    def __init__(self, *, path: str = "", **kwargs):
        self.path = path
        super().__init__(**kwargs)

    def repr_pieces(self) -> Iterable[Tuple[str, Union[str, int]]]:
        pieces = [("path", self.path), ("db", self.db)]
        if self.client_name:
            pieces.append(("client_name", self.client_name))
        return pieces

    async def _connect(self):
        async with async_timeout(self.socket_connect_timeout):
            reader, writer = await asyncio.open_unix_connection(path=self.path)
        self._reader = reader
        self._writer = writer

    def _host_error(self) -> str:
        return self.path


FALSE_STRINGS = ("0", "F", "FALSE", "N", "NO", "OFF")


def to_bool(value) -> Optional[bool]:
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


URL_QUERY_ARGUMENT_PARSERS: Mapping[str, Callable[..., object]] = MappingProxyType(
    {
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
        "ssl_include_verify_flags": parse_ssl_verify_flags,
        "ssl_exclude_verify_flags": parse_ssl_verify_flags,
        "ssl_min_version": int,
        "timeout": float,
        "protocol": int,
        "legacy_responses": to_bool,
    }
)


class ConnectKwargs(TypedDict, total=False):
    username: str
    password: str
    connection_class: Type[AbstractConnection]
    host: str
    port: int
    db: int
    path: str


def parse_url(url: str) -> ConnectKwargs:
    # Scheme names are case-insensitive (RFC 3986), so normalize before the
    # prefix check; the "://" is required so a URL like "redis:foo" (which
    # urlparse would still report as the "redis" scheme) is rejected.
    if not url.lower().startswith(("redis://", "rediss://", "unix://")):
        raise ValueError(
            "Redis URL must specify one of the following schemes "
            "(redis://, rediss://, unix://)"
        )

    parsed: ParseResult = urlparse(url)
    kwargs: ConnectKwargs = {}

    for name, value_list in parse_qs(parsed.query).items():
        if value_list and len(value_list) > 0:
            # parse_qs() already percent-decodes query values, so use the value
            # as-is; unquoting again here would double-decode (e.g. "%2520" ->
            # "%20" -> " "). See issue #4208.
            value = value_list[0]
            parser = URL_QUERY_ARGUMENT_PARSERS.get(name)
            if parser:
                try:
                    kwargs[name] = parser(value)
                except (TypeError, ValueError):
                    raise ValueError(f"Invalid value for '{name}' in connection URL.")
            else:
                kwargs[name] = value

    if parsed.username:
        kwargs["username"] = unquote(parsed.username)
    if parsed.password:
        kwargs["password"] = unquote(parsed.password)

    # We only support redis://, rediss:// and unix:// schemes.
    if parsed.scheme == "unix":
        if parsed.path:
            kwargs["path"] = unquote(parsed.path)
        kwargs["connection_class"] = UnixDomainSocketConnection

    else:  # implied:  parsed.scheme in ("redis", "rediss")
        if parsed.hostname:
            kwargs["host"] = unquote(parsed.hostname)
        if parsed.port is not None:
            kwargs["port"] = int(parsed.port)

        # If there's a path argument, use it as the db argument if a
        # querystring value wasn't specified
        if parsed.path and "db" not in kwargs:
            try:
                kwargs["db"] = int(unquote(parsed.path).replace("/", ""))
            except (AttributeError, ValueError):
                pass

        if parsed.scheme == "rediss":
            kwargs["connection_class"] = SSLConnection

    return kwargs


_CP = TypeVar("_CP", bound="ConnectionPool")


class ConnectionPoolInterface(ABC):
    @abstractmethod
    def get_protocol(self):
        pass

    @abstractmethod
    def reset(self) -> None:
        pass

    @abstractmethod
    @deprecated_args(
        args_to_warn=["*"],
        reason="Use get_connection() without args instead",
        version="5.3.0",
    )
    async def get_connection(
        self, command_name: Optional[str] = None, *keys: Any, **options: Any
    ) -> "AbstractConnection":
        pass

    @abstractmethod
    def get_encoder(self) -> "Encoder":
        pass

    @abstractmethod
    async def release(self, connection: "AbstractConnection") -> None:
        pass

    @abstractmethod
    async def disconnect(self, inuse_connections: bool = True) -> None:
        pass

    @abstractmethod
    async def aclose(self) -> None:
        pass

    @abstractmethod
    def set_retry(self, retry: "Retry") -> None:
        pass

    @abstractmethod
    async def re_auth_callback(self, token: TokenInterface) -> None:
        pass

    @abstractmethod
    def get_connection_count(self) -> List[Tuple[int, dict]]:
        """
        Returns a connection count (both idle and in use).
        """
        pass


class AsyncMaintNotificationsAbstractConnectionPool:
    """
    Internal mixin for async maintenance notification pool wiring.

    The handler owns notification policy.
    This mixin owns pool state mutation because `_available_connections`,
    `_in_use_connections`, `connection_kwargs`, and the non-reentrant `asyncio.Lock`
    all live on the pool.
    """

    def __init__(
        self,
        maint_notifications_config: MaintNotificationsConfig | None = None,
        oss_cluster_maint_notifications_handler: (
            "AsyncOSSMaintNotificationsHandler | None"
        ) = None,
        **kwargs: Any,
    ) -> None:
        protocol = kwargs.get("protocol")
        is_protocol_supported = check_protocol_version(protocol, 3)
        is_connection_supported = self._maintenance_notifications_supported()

        if (
            maint_notifications_config is None
            and is_protocol_supported
            and is_connection_supported
        ):
            maint_notifications_config = MaintNotificationsConfig()

        if maint_notifications_config and maint_notifications_config.enabled:
            if not is_connection_supported:
                if maint_notifications_config.enabled is True:
                    # Unix sockets do not have a host endpoint for CLIENT
                    # MAINT_NOTIFICATIONS to describe.
                    if "path" in self.connection_kwargs:
                        raise RedisError(
                            "Maintenance notifications are not supported for "
                            "Unix domain socket connections"
                        )

                    # Custom connection classes must inherit the async maintenance
                    # mixin so handlers can update connection state safely.
                    if not self._maintenance_notifications_connection_class_supported():
                        connection_class = getattr(self, "connection_class", None)
                        connection_class_name = getattr(
                            connection_class, "__name__", connection_class
                        )
                        raise RedisError(
                            "Maintenance notifications are not supported for "
                            f"connection class {connection_class_name}"
                        )

                    # TCP-like connections still need a host to identify the
                    # endpoint that can move during maintenance.
                    raise RedisError(
                        "Maintenance notifications are not supported for connections "
                        "without a host"
                    )
                self._maint_notifications_pool_handler = None
                self._oss_cluster_maint_notifications_handler = None
                return

            if not is_protocol_supported:
                raise RedisError(
                    "Maintenance notifications handlers on connection are only supported with RESP version 3"
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
                self._maint_notifications_pool_handler = (
                    AsyncMaintNotificationsPoolHandler(self, maint_notifications_config)
                )
                self._update_connection_kwargs_for_maint_notifications(
                    maint_notifications_pool_handler=self._maint_notifications_pool_handler
                )
        else:
            self._maint_notifications_pool_handler = None
            self._oss_cluster_maint_notifications_handler = None

    async def _on_close(self) -> None:
        """Hook invoked from the pool's ``aclose()`` before the pool is shut down."""
        if self._maint_notifications_pool_handler is not None:
            await self._maint_notifications_pool_handler.cancel_scheduled_tasks()

    @property
    @abstractmethod
    def connection_kwargs(self) -> dict[str, Any]:
        pass

    @connection_kwargs.setter
    @abstractmethod
    def connection_kwargs(self, value: dict[str, Any]) -> None:
        pass

    @abstractmethod
    def _get_pool_lock(self) -> asyncio.Lock:
        pass

    @abstractmethod
    def _get_free_connections(self) -> Iterable["AbstractConnection"]:
        pass

    @abstractmethod
    def _get_in_use_connections(self) -> Iterable["AbstractConnection"]:
        pass

    def _maintenance_notifications_supported(self) -> bool:
        if "path" in self.connection_kwargs:
            return False
        if not self._maintenance_notifications_connection_class_supported():
            return False
        return bool(self.connection_kwargs.get("host"))

    def _maintenance_notifications_connection_class_supported(self) -> bool:
        connection_class = getattr(self, "connection_class", None)
        if connection_class is None:
            return False
        try:
            return issubclass(
                connection_class, AsyncMaintNotificationsAbstractConnection
            )
        except TypeError:
            return False

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

    async def update_maint_notifications_config(
        self,
        maint_notifications_config: MaintNotificationsConfig,
        oss_cluster_maint_notifications_handler: (
            AsyncOSSMaintNotificationsHandler | None
        ) = None,
    ) -> None:
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
            if (
                maint_notifications_config.enabled
                and not self._maintenance_notifications_supported()
            ):
                if maint_notifications_config.enabled is True:
                    # Unix sockets do not have a host endpoint for CLIENT
                    # MAINT_NOTIFICATIONS to describe.
                    if "path" in self.connection_kwargs:
                        raise RedisError(
                            "Maintenance notifications are not supported for "
                            "Unix domain socket connections"
                        )

                    # Custom connection classes must inherit the async maintenance
                    # mixin so handlers can update connection state safely.
                    if not self._maintenance_notifications_connection_class_supported():
                        connection_class = getattr(self, "connection_class", None)
                        connection_class_name = getattr(
                            connection_class, "__name__", connection_class
                        )
                        raise RedisError(
                            "Maintenance notifications are not supported for "
                            f"connection class {connection_class_name}"
                        )

                    # TCP-like connections still need a host to identify the
                    # endpoint that can move during maintenance.
                    raise RedisError(
                        "Maintenance notifications are not supported for connections "
                        "without a host"
                    )
                self._maint_notifications_pool_handler = None
                return

            if self._oss_cluster_maint_notifications_handler:
                # Pool already in OSS cluster mode; update the OSS handler config
                # instead of creating a mutually-exclusive pool handler (which
                # would be silently ignored because the OSS handler wins priority
                # in both update helpers below).
                self._oss_cluster_maint_notifications_handler.config = (
                    maint_notifications_config
                )
            elif not self._maint_notifications_pool_handler:
                self._maint_notifications_pool_handler = (
                    AsyncMaintNotificationsPoolHandler(self, maint_notifications_config)
                )
            else:
                self._maint_notifications_pool_handler.config = (
                    maint_notifications_config
                )

        self._update_connection_kwargs_for_maint_notifications(
            maint_notifications_pool_handler=self._maint_notifications_pool_handler,
            oss_cluster_maint_notifications_handler=self._oss_cluster_maint_notifications_handler,
        )
        await self._update_maint_notifications_configs_for_connections(
            maint_notifications_pool_handler=self._maint_notifications_pool_handler,
            oss_cluster_maint_notifications_handler=self._oss_cluster_maint_notifications_handler,
        )

    def _update_connection_kwargs_for_maint_notifications(
        self,
        maint_notifications_pool_handler: (
            AsyncMaintNotificationsPoolHandler | None
        ) = None,
        oss_cluster_maint_notifications_handler: (
            AsyncOSSMaintNotificationsHandler | None
        ) = None,
    ) -> None:
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

    async def _update_maint_notifications_configs_for_connections(
        self,
        maint_notifications_pool_handler: (
            AsyncMaintNotificationsPoolHandler | None
        ) = None,
        oss_cluster_maint_notifications_handler: (
            AsyncOSSMaintNotificationsHandler | None
        ) = None,
    ) -> None:
        """Update the maintenance notifications config for all connections in the pool."""
        async with self._get_pool_lock():
            for conn in list(self._get_free_connections()):
                if oss_cluster_maint_notifications_handler:
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
                        "Either maint_notifications_pool_handler or "
                        "oss_cluster_maint_notifications_handler must be set"
                    )
                await conn.disconnect()

            for conn in list(self._get_in_use_connections()):
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
                        "Either maint_notifications_pool_handler or "
                        "oss_cluster_maint_notifications_handler must be set"
                    )
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "Marking active connection for reconnect after config update "
                        f"config update: {conn}, {conn.extract_connection_details()}"
                    )
                conn.mark_for_reconnect()

    def _should_update_connection(
        self,
        conn: "AbstractConnection",
        matching_pattern: str = "connected_address",
        matching_address: str | None = None,
        matching_notification_hash: int | None = None,
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
        conn: "AsyncMaintNotificationsAbstractConnection",
        state: MaintenanceState | None = None,
        maintenance_notification_hash: int | None = None,
        host_address: str | None = None,
        relaxed_timeout: float | None = None,
        update_notification_hash: bool = False,
        reset_host_address: bool = False,
        reset_relaxed_timeout: bool = False,
    ) -> None:
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
        self, conn: "AsyncMaintNotificationsAbstractConnection", state: MaintenanceState
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

    async def update_connections_settings(
        self,
        state: MaintenanceState | None = None,
        maintenance_notification_hash: int | None = None,
        host_address: str | None = None,
        relaxed_timeout: float | None = None,
        matching_address: str | None = None,
        matching_notification_hash: int | None = None,
        matching_pattern: Literal[
            "connected_address", "configured_address", "notification_hash"
        ] = "connected_address",
        update_notification_hash: bool = False,
        reset_host_address: bool = False,
        reset_relaxed_timeout: bool = False,
        include_free_connections: bool = True,
    ) -> None:
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
        async with self._get_pool_lock():
            self._update_connections_settings_without_locking(
                state=state,
                maintenance_notification_hash=maintenance_notification_hash,
                host_address=host_address,
                relaxed_timeout=relaxed_timeout,
                matching_address=matching_address,
                matching_notification_hash=matching_notification_hash,
                matching_pattern=matching_pattern,
                update_notification_hash=update_notification_hash,
                reset_host_address=reset_host_address,
                reset_relaxed_timeout=reset_relaxed_timeout,
                include_free_connections=include_free_connections,
            )

    def _update_connections_settings_without_locking(
        self,
        state: MaintenanceState | None = None,
        maintenance_notification_hash: int | None = None,
        host_address: str | None = None,
        relaxed_timeout: float | None = None,
        matching_address: str | None = None,
        matching_notification_hash: int | None = None,
        matching_pattern: Literal[
            "connected_address", "configured_address", "notification_hash"
        ] = "connected_address",
        update_notification_hash: bool = False,
        reset_host_address: bool = False,
        reset_relaxed_timeout: bool = False,
        include_free_connections: bool = True,
    ) -> None:
        """
        Update matching connections while the caller already holds the pool lock.

        This helper intentionally does not acquire the pool lock so callers can
        compose several pool mutations inside one critical section without
        deadlocking the non-reentrant `asyncio.Lock`.
        """
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

    def update_connection_kwargs(self, **kwargs: Any) -> None:
        """
        Update the connection kwargs for all future connections.

        This method updates the connection kwargs for all future connections created by the pool.
        Existing connections are not affected.
        """
        self.connection_kwargs.update(kwargs)

    async def apply_moving_notification(
        self,
        notification: NodeMovingNotification,
        config: MaintNotificationsConfig,
        moving_address_src: str | None,
        run_proactive_reconnect: bool = False,
    ) -> None:
        """
        Apply the pool state transition for a MOVING notification atomically.

        Async pools use a non-reentrant `asyncio.Lock`, so the handler cannot
        safely compose several separately locked calls. Existing connection
        updates, optional proactive reconnect, and future `connection_kwargs`
        changes must happen under one pool-owned lock; otherwise a connection
        can move between active/free lists and escape handling.
        """
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Applying MOVING notification to pool: {notification}, "
                f"moving address src: {moving_address_src}, "
                f"proactive reconnect: {run_proactive_reconnect}"
            )
        async with self._get_pool_lock():
            # Record maintenance state for pools that expose the hook.
            self._set_in_maintenance(True)
            try:
                self._update_connections_settings_without_locking(
                    state=MaintenanceState.MOVING,
                    maintenance_notification_hash=hash(notification),
                    relaxed_timeout=config.relaxed_timeout,
                    host_address=notification.new_node_host,
                    matching_address=moving_address_src,
                    matching_pattern="connected_address",
                    update_notification_hash=True,
                    include_free_connections=True,
                )

                if run_proactive_reconnect:
                    await self._run_proactive_reconnect_without_locking(
                        moving_address_src
                    )

                self.update_connection_kwargs(
                    **_build_moving_connection_kwargs(notification, config)
                )
            finally:
                self._set_in_maintenance(False)

    async def run_proactive_reconnect(
        self,
        moving_address_src: str | None = None,
    ) -> None:
        """
        Mark active connections and disconnect free connections atomically.

        This operation is pool-owned because the active/free lists can change
        while tasks acquire or release connections. Keeping the mark/disconnect
        pass under one lock avoids a connection moving between lists between
        separately locked calls.
        """
        async with self._get_pool_lock():
            await self._run_proactive_reconnect_without_locking(moving_address_src)

    async def _run_proactive_reconnect_without_locking(
        self,
        moving_address_src: str | None = None,
    ) -> None:
        """
        Mark and disconnect matching connections while the caller holds the pool lock.

        This helper intentionally does not acquire the pool lock so it can be
        reused by larger atomic operations that already hold the non-reentrant
        `asyncio.Lock`.
        """
        debug = logger.isEnabledFor(logging.DEBUG)
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

        free_connections = [
            conn
            for conn in self._get_free_connections()
            if self._should_update_connection(
                conn, "connected_address", moving_address_src
            )
        ]
        if debug:
            for conn in free_connections:
                logger.debug(
                    f"Disconnecting free connection: {conn}, "
                    f"{conn.extract_connection_details()}"
                )
        await self._disconnect_connections(free_connections)

    async def cleanup_moving_notification(
        self,
        notification_hash: int,
        reset_relaxed_timeout: bool,
        reset_host_address: bool,
    ) -> bool:
        """
        Revert MOVING pool state atomically after the notification TTL.

        Future connection kwargs and existing connection state must be cleaned
        up in the same critical section. Splitting the cleanup lets an
        acquire/release interleave, which can leave stale MOVING state or undo a
        newer overlapping MOVING notification.

        Returns True when the pool's connection kwargs were reverted, False when
        a newer MOVING notification had superseded this one and they were left
        for it to revert.
        """
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Cleaning up MOVING pool state for notification hash "
                f"{notification_hash}, reset_relaxed_timeout="
                f"{reset_relaxed_timeout}, reset_host_address={reset_host_address}"
            )
        async with self._get_pool_lock():
            kwargs = _build_moving_cleanup_connection_kwargs(
                self.connection_kwargs, notification_hash
            )
            if kwargs is not None:
                self.update_connection_kwargs(**kwargs)

            self._update_connections_settings_without_locking(
                relaxed_timeout=-1,
                state=MaintenanceState.NONE,
                maintenance_notification_hash=None,
                matching_notification_hash=notification_hash,
                matching_pattern="notification_hash",
                update_notification_hash=True,
                reset_relaxed_timeout=reset_relaxed_timeout,
                reset_host_address=reset_host_address,
                include_free_connections=True,
            )
            return kwargs is not None

    async def _disconnect_connections(
        self, connections: Iterable["AbstractConnection"]
    ) -> None:
        connections = tuple(connections)
        if not connections:
            return
        results = await asyncio.gather(
            *(connection.disconnect() for connection in connections),
            return_exceptions=True,
        )
        exc = next(
            (result for result in results if isinstance(result, BaseException)), None
        )
        if exc:
            raise exc

    def _set_in_maintenance(self, in_maintenance: bool) -> None:
        """Flip the pool's maintenance flag if it exposes one (BlockingConnectionPool)."""
        set_in_maintenance = getattr(self, "set_in_maintenance", None)
        if callable(set_in_maintenance):
            set_in_maintenance(in_maintenance)


class ConnectionPool(
    AsyncMaintNotificationsAbstractConnectionPool, ConnectionPoolInterface
):
    """
    Create a connection pool. ``If max_connections`` is set, then this
    object raises :py:class:`~redis.ConnectionError` when the pool's
    limit is reached.

    By default, TCP connections are created unless ``connection_class``
    is specified. Use :py:class:`~redis.UnixDomainSocketConnection` for
    unix sockets.
    :py:class:`~redis.SSLConnection` can be used for SSL enabled connections.

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
        connection_class: Type[AbstractConnection] = Connection,
        max_connections: Optional[int] = None,
        maint_notifications_config: MaintNotificationsConfig | None = None,
        **connection_kwargs,
    ):
        max_connections = max_connections or 100
        if not isinstance(max_connections, int) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')

        retry = connection_kwargs.get("retry")
        if retry is not None:
            connection_kwargs["retry"] = _to_async_retry(retry)

        self.connection_class = connection_class
        self._connection_kwargs = connection_kwargs
        self.max_connections = max_connections

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

        self._available_connections: List[AbstractConnection] = []
        self._in_use_connections: Set[AbstractConnection] = set()
        self.encoder_class = self.connection_kwargs.get("encoder_class", Encoder)
        self._lock = asyncio.Lock()
        self._event_dispatcher = self.connection_kwargs.get("event_dispatcher", None)
        if self._event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()

        AsyncMaintNotificationsAbstractConnectionPool.__init__(
            self,
            maint_notifications_config=maint_notifications_config,
            **connection_kwargs,
        )

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

    def __repr__(self):
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
    def connection_kwargs(self) -> dict[str, Any]:
        return self._connection_kwargs

    @connection_kwargs.setter
    def connection_kwargs(self, value: dict[str, Any]) -> None:
        self._connection_kwargs = value

    def _get_pool_lock(self) -> asyncio.Lock:
        return self._lock

    def _get_free_connections(self) -> Iterable[AbstractConnection]:
        return self._available_connections

    def _get_in_use_connections(self) -> Iterable[AbstractConnection]:
        return self._in_use_connections

    def get_protocol(self):
        """
        Returns:
            The RESP protocol version, or ``None`` if the protocol is not specified,
            in which case the server default will be used.
        """
        return self.connection_kwargs.get("protocol", None)

    def reset(self):
        # Record metrics for connections being removed before clearing
        # (only if attributes exist - they won't during __init__)
        if hasattr(self, "_available_connections") and hasattr(
            self, "_in_use_connections"
        ):
            idle_count = len(self._available_connections)
            in_use_count = len(self._in_use_connections)
            if idle_count > 0 or in_use_count > 0:
                pool_name = get_pool_name(self)
                # Note: Using sync version since reset() is sync
                from redis.observability.recorder import (
                    record_connection_count as sync_record_connection_count,
                )

                if idle_count > 0:
                    sync_record_connection_count(
                        pool_name=pool_name,
                        connection_state=ConnectionState.IDLE,
                        counter=-idle_count,
                    )
                if in_use_count > 0:
                    sync_record_connection_count(
                        pool_name=pool_name,
                        connection_state=ConnectionState.USED,
                        counter=-in_use_count,
                    )

        self._available_connections = []
        self._in_use_connections = weakref.WeakSet()

    def __del__(self) -> None:
        """Clean up connection pool and record metrics when garbage collected."""
        try:
            if not hasattr(self, "_available_connections") or not hasattr(
                self, "_in_use_connections"
            ):
                return
            idle_count = len(self._available_connections)
            in_use_count = len(self._in_use_connections)
            if idle_count > 0 or in_use_count > 0:
                pool_name = get_pool_name(self)
                # Note: Using sync version since __del__ is sync
                from redis.observability.recorder import (
                    record_connection_count as sync_record_connection_count,
                )

                if idle_count > 0:
                    sync_record_connection_count(
                        pool_name=pool_name,
                        connection_state=ConnectionState.IDLE,
                        counter=-idle_count,
                    )
                if in_use_count > 0:
                    sync_record_connection_count(
                        pool_name=pool_name,
                        connection_state=ConnectionState.USED,
                        counter=-in_use_count,
                    )
        except Exception:
            pass

    def can_get_connection(self) -> bool:
        """Return True if a connection can be retrieved from the pool."""
        return (
            self._available_connections
            or len(self._in_use_connections) < self.max_connections
        )

    @deprecated_args(
        args_to_warn=["*"],
        reason="Use get_connection() without args instead",
        version="5.3.0",
    )
    async def get_connection(self, command_name=None, *keys, **options):
        """Get a connected connection from the pool"""
        # Track connection count before to detect if a new connection is created
        async with self._lock:
            connections_before = len(self._available_connections) + len(
                self._in_use_connections
            )
            start_time_created = time.monotonic()
            connection = self.get_available_connection()
            connections_after = len(self._available_connections) + len(
                self._in_use_connections
            )
            is_created = connections_after > connections_before

        # Record state transition for observability
        # This ensures counters stay balanced if ensure_connection() fails and release() is called
        pool_name = get_pool_name(self)
        if is_created:
            # New connection created and acquired: just USED +1
            await record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.USED,
                counter=1,
            )
        else:
            # Existing connection acquired from pool: IDLE -> USED
            await record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.IDLE,
                counter=-1,
            )
            await record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.USED,
                counter=1,
            )

        # We now perform the connection check outside of the lock.
        try:
            await self.ensure_connection(connection)

            if is_created:
                await record_connection_create_time(
                    connection_pool=self,
                    duration_seconds=time.monotonic() - start_time_created,
                )

            return connection
        except BaseException:
            await self.release(connection)
            raise

    def get_available_connection(self):
        """Get a connection from the pool, without making sure it is connected"""
        try:
            connection = self._available_connections.pop()
        except IndexError:
            if len(self._in_use_connections) >= self.max_connections:
                raise MaxConnectionsError("Too many connections") from None
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        return connection

    def get_encoder(self):
        """Return an encoder based on encoding settings"""
        kwargs = self.connection_kwargs
        return self.encoder_class(
            encoding=kwargs.get("encoding", "utf-8"),
            encoding_errors=kwargs.get("encoding_errors", "strict"),
            decode_responses=kwargs.get("decode_responses", False),
        )

    def make_connection(self):
        """Create a new connection.  Can be overridden by child classes."""
        # Note: We don't record IDLE here because async uses a sync make_connection
        # but async record_connection_count. The recording is handled in get_connection.
        return self.connection_class(**self.connection_kwargs)

    async def ensure_connection(self, connection: AbstractConnection):
        """Ensure that the connection object is connected and valid"""
        await connection.connect()
        # connections that the pool provides should be ready to send
        # a command. if not, the connection was either returned to the
        # pool before all data has been read or the socket has been
        # closed. either way, reconnect and verify everything is good.
        # With maintenance notifications enabled the pending data may be
        # push notifications that have to be applied rather than a reason
        # to reconnect; reading them is what tells the two apart, and it
        # raises for a socket the server has closed in the meantime.
        try:
            if await connection.can_read():
                if self.maint_notifications_enabled():
                    await connection.handle_pending_push_notifications()
                else:
                    raise ConnectionError("Connection has data") from None
        except (ConnectionError, TimeoutError, OSError):
            await connection.disconnect()
            await connection.connect()
            if await connection.can_read() and not self.maint_notifications_enabled():
                raise ConnectionError("Connection not ready") from None

    async def release(self, connection: AbstractConnection):
        """Releases the connection back to the pool"""
        # Connections should always be returned to the correct pool,
        # not doing so is an error that will cause an exception here.
        async with self._lock:
            self._in_use_connections.remove(connection)

            if connection.should_reconnect():
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "Disconnecting released connection marked for reconnect: "
                        f"{connection}, {connection.extract_connection_details()}"
                    )
                await connection.disconnect()

            self._available_connections.append(connection)

        await self._event_dispatcher.dispatch_async(
            AsyncAfterConnectionReleasedEvent(connection)
        )

        # Record state transition: USED -> IDLE
        pool_name = get_pool_name(self)
        await record_connection_count(
            pool_name=pool_name,
            connection_state=ConnectionState.USED,
            counter=-1,
        )
        await record_connection_count(
            pool_name=pool_name,
            connection_state=ConnectionState.IDLE,
            counter=1,
        )

    async def disconnect(self, inuse_connections: bool = True):
        """
        Disconnects connections in the pool

        If ``inuse_connections`` is True, disconnect connections that are
        current in use, potentially by other tasks. Otherwise only disconnect
        connections that are idle in the pool.
        """
        if inuse_connections:
            connections: Iterable[AbstractConnection] = chain(
                self._available_connections, self._in_use_connections
            )
        else:
            connections = self._available_connections
        resp = await asyncio.gather(
            *(connection.disconnect() for connection in connections),
            return_exceptions=True,
        )

        exc = next((r for r in resp if isinstance(r, BaseException)), None)
        if exc:
            raise exc

    async def update_active_connections_for_reconnect(self):
        """
        Mark all active connections for reconnect.
        """
        debug = logger.isEnabledFor(logging.DEBUG)
        async with self._lock:
            for conn in self._in_use_connections:
                if debug:
                    logger.debug(
                        f"Marking active connection for reconnect: {conn}, "
                        f"{conn.extract_connection_details()}"
                    )
                conn.mark_for_reconnect()

    async def aclose(self) -> None:
        """Close the pool, disconnecting all connections"""
        try:
            await self._on_close()
        finally:
            await self.disconnect()

    async def __aenter__(self: _CP) -> _CP:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.aclose()

    def set_retry(self, retry: "Retry") -> None:
        retry = _to_async_retry(retry)
        self.connection_kwargs["retry"] = retry
        for conn in self._available_connections:
            conn.retry = retry
        for conn in self._in_use_connections:
            conn.retry = retry

    async def re_auth_callback(self, token: TokenInterface):
        first_error = None
        async with self._lock:
            for conn in self._available_connections:
                try:
                    await conn.retry.call_with_retry(
                        lambda: conn.send_command(
                            "AUTH", token.try_get("oid"), token.get_value()
                        ),
                        lambda error: self._mock(error),
                    )
                    await conn.retry.call_with_retry(
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

    async def _mock(self, error: RedisError):
        """
        Dummy functions, needs to be passed as error callback to retry object.
        :param error:
        :return:
        """
        pass

    def get_connection_count(self) -> List[tuple[int, dict]]:
        """
        Returns a connection count (both idle and in use).
        """
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
            (len(self._available_connections), free_connections_attributes),
            (len(self._in_use_connections), in_use_connections_attributes),
        ]


class BlockingConnectionPool(ConnectionPool):
    """
    A blocking connection pool::

        >>> from redis.asyncio import Redis, BlockingConnectionPool
        >>> client = Redis.from_pool(BlockingConnectionPool())

    It performs the same function as the default
    :py:class:`~redis.asyncio.connection.ConnectionPool` implementation, in that,
    it maintains a pool of reusable connections that can be shared by
    multiple async redis clients.

    The difference is that, in the event that a client tries to get a
    connection from the pool when all of connections are in use, rather than
    raising a :py:class:`~redis.exceptions.MaxConnectionsError` (as the default
    :py:class:`~redis.asyncio.connection.ConnectionPool` implementation does), it
    blocks the current `Task` for a specified number of seconds until
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
        max_connections: int = 50,
        timeout: Optional[float] = 20,
        connection_class: Type[AbstractConnection] = Connection,
        queue_class: Type[asyncio.Queue] = asyncio.LifoQueue,  # deprecated
        **connection_kwargs,
    ):
        super().__init__(
            connection_class=connection_class,
            max_connections=max_connections,
            **connection_kwargs,
        )
        self._condition = asyncio.Condition()
        self.timeout = timeout
        self._in_maintenance = False

    def set_in_maintenance(self, in_maintenance: bool) -> None:
        """
        Record the pool's maintenance state.

        ``get_connection`` and ``release`` always serialize their pool mutations
        through ``self._lock``, independently of this flag, so they also wait for
        reauthentication and maintenance handlers.
        """
        self._in_maintenance = in_maintenance

    @deprecated_args(
        args_to_warn=["*"],
        reason="Use get_connection() without args instead",
        version="5.3.0",
    )
    async def get_connection(self, command_name=None, *keys, **options):
        """Gets a connection from the pool, blocking until one is available"""
        # Start timing for wait time observability
        start_time_acquired = time.monotonic()

        try:
            async with self._condition:
                async with async_timeout(self.timeout):
                    await self._condition.wait_for(self.can_get_connection)
                    async with self._lock:
                        # Track connection count before to detect if a new connection is created
                        connections_before = len(self._available_connections) + len(
                            self._in_use_connections
                        )
                        start_time_created = time.monotonic()
                        connection = super().get_available_connection()
                        connections_after = len(self._available_connections) + len(
                            self._in_use_connections
                        )
                        is_created = connections_after > connections_before
        except asyncio.TimeoutError as err:
            await record_connection_timeout(pool_name=get_pool_name(self))
            raise ConnectionError("No connection available.") from err

        # Record state transition for observability.
        pool_name = get_pool_name(self)
        if is_created:
            # New connection created and acquired: just USED +1
            await record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.USED,
                counter=1,
            )
        else:
            # Existing connection acquired from pool: IDLE -> USED
            await record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.IDLE,
                counter=-1,
            )
            await record_connection_count(
                pool_name=pool_name,
                connection_state=ConnectionState.USED,
                counter=1,
            )

        # We now perform the connection check outside of the lock.
        try:
            await self.ensure_connection(connection)

            if is_created:
                await record_connection_create_time(
                    connection_pool=self,
                    duration_seconds=time.monotonic() - start_time_created,
                )

            await record_connection_wait_time(
                pool_name=get_pool_name(self),
                duration_seconds=time.monotonic() - start_time_acquired,
            )

            return connection
        except BaseException:
            await self.release(connection)
            raise

    async def release(self, connection: AbstractConnection):
        """Releases the connection back to the pool."""
        async with self._condition:
            await super().release(connection)
            self._condition.notify()
