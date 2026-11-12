import asyncio
import threading
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional, Type, Union

from redis.auth.token import TokenInterface
from redis.credentials import CredentialProvider, StreamingCredentialProvider
from redis.observability.recorder import (
    init_connection_count,
    register_pools_connection_count,
)
from redis.utils import check_protocol_version, deprecated_function

if TYPE_CHECKING:
    from redis.maint_notifications import (
        MaintenanceNotification,
        MaintenanceState,
        MaintNotificationsConfig,
    )


class EventListenerInterface(ABC):
    """
    Represents a listener for given event object.
    """

    @abstractmethod
    def listen(self, event: object):
        pass


class AsyncEventListenerInterface(ABC):
    """
    Represents an async listener for given event object.
    """

    @abstractmethod
    async def listen(self, event: object):
        pass


class EventDispatcherInterface(ABC):
    """
    Represents a dispatcher that dispatches events to listeners
    associated with given event.
    """

    @abstractmethod
    def dispatch(self, event: object):
        pass

    @abstractmethod
    async def dispatch_async(self, event: object):
        pass

    @abstractmethod
    def register_listeners(
        self,
        mappings: Dict[
            Type[object],
            List[Union[EventListenerInterface, AsyncEventListenerInterface]],
        ],
    ):
        """Register additional listeners."""
        pass

    @abstractmethod
    def unregister_listeners(
        self,
        mappings: Dict[
            Type[object],
            List[Union[EventListenerInterface, AsyncEventListenerInterface]],
        ],
    ):
        """Remove previously registered listeners by identity."""
        pass


class EventException(Exception):
    """
    Exception wrapper that adds an event object into exception context.
    """

    def __init__(self, exception: Exception, event: object):
        self.exception = exception
        self.event = event
        super().__init__(exception)


class EventDispatcher(EventDispatcherInterface):
    # TODO: Make dispatcher to accept external mappings.
    def __init__(
        self,
        event_listeners: Optional[
            Dict[Type[object], List[EventListenerInterface]]
        ] = None,
    ):
        """
        Dispatcher that dispatches events to listeners associated with given event.
        """
        self._event_listeners_mapping: Dict[
            Type[object], List[EventListenerInterface]
        ] = {
            AfterConnectionReleasedEvent: [
                ReAuthConnectionListener(),
            ],
            AfterPooledConnectionsInstantiationEvent: [
                RegisterReAuthForPooledConnections(),
            ],
            AfterSingleConnectionInstantiationEvent: [
                RegisterReAuthForSingleConnection()
            ],
            AfterPubSubConnectionInstantiationEvent: [RegisterReAuthForPubSub()],
            AfterAsyncClusterInstantiationEvent: [RegisterReAuthForAsyncClusterNodes()],
            AsyncAfterConnectionReleasedEvent: [
                AsyncReAuthConnectionListener(),
            ],
        }

        # Reentrant so a finalizer/listener that runs on the same thread
        # while the lock is held (e.g. a weakref.finalize callback fired
        # from cyclic GC during an allocation inside register_listeners /
        # unregister_listeners) can re-enter without deadlocking.
        self._lock = threading.RLock()
        self._async_lock = None

        if event_listeners:
            self.register_listeners(event_listeners)

    def dispatch(self, event: object):
        # Snapshot listeners under the lock, then release it before invoking
        # them. Holding the lock across listener execution would turn any
        # listener that calls register_listeners / unregister_listeners /
        # dispatch back into the dispatcher into a deadlock.
        with self._lock:
            listeners = list(self._event_listeners_mapping.get(type(event), []))
        for listener in listeners:
            listener.listen(event)

    async def dispatch_async(self, event: object):
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()

        # Snapshot listeners under the lock, then release it before awaiting
        # them. See the note in dispatch(); the same rationale applies here
        # for dispatch_async re-entry from within a listener.
        async with self._async_lock:
            listeners = list(self._event_listeners_mapping.get(type(event), []))
        for listener in listeners:
            await listener.listen(event)

    def register_listeners(
        self,
        mappings: Dict[
            Type[object],
            List[Union[EventListenerInterface, AsyncEventListenerInterface]],
        ],
    ):
        with self._lock:
            for event_type in mappings:
                if event_type in self._event_listeners_mapping:
                    self._event_listeners_mapping[event_type] = list(
                        set(
                            self._event_listeners_mapping[event_type]
                            + mappings[event_type]
                        )
                    )
                else:
                    self._event_listeners_mapping[event_type] = mappings[event_type]

    def unregister_listeners(
        self,
        mappings: Dict[
            Type[object],
            List[Union[EventListenerInterface, AsyncEventListenerInterface]],
        ],
    ):
        with self._lock:
            for event_type, to_remove in mappings.items():
                current = self._event_listeners_mapping.get(event_type)
                if not current:
                    continue
                # Remove by identity to match register semantics and to avoid
                # reliance on listener __eq__ implementations.
                self._event_listeners_mapping[event_type] = [
                    listener
                    for listener in current
                    if all(listener is not target for target in to_remove)
                ]


class AfterConnectionReleasedEvent:
    """
    Event that will be fired before each command execution.
    """

    def __init__(self, connection):
        self._connection = connection

    @property
    def connection(self):
        return self._connection


class AsyncAfterConnectionReleasedEvent(AfterConnectionReleasedEvent):
    pass


class AfterSlotsCacheRefreshEvent:
    """
    Event fired after NodesManager's slots cache is refreshed, either via a
    full re-initialization or a MOVED-driven slot re-mapping. Signal-only;
    carries no payload. Listeners typically reconcile per-node bookkeeping
    (e.g. ClusterPubSub shard subscriptions).
    """

    pass


class AsyncAfterSlotsCacheRefreshEvent(AfterSlotsCacheRefreshEvent):
    pass


class MaintenanceEvent:
    """
    Base class of the events a maintenance notification pushed by the server
    produces while it relaxes timeouts: on a connection pool for a node handoff
    (``MaintenanceState.MOVING``), or on a single connection for a shard
    migration or failover (``MaintenanceState.MAINTENANCE``).

    Dispatched synchronously, by both the sync and the async client, to the
    ``event_dispatcher`` of the ``MaintNotificationsConfig`` the handler acted
    with; listeners implement ``EventListenerInterface``.

    :param connection_pool: The pool whose connections are affected. ``None``
        when the notification was handled by a connection without a pool
        handler, such as an OSS cluster node connection.
    :param connection: The connection that received the notification. ``None``
        when a pool-level handler had no connection to attribute it to.
    :param state: The ``MaintenanceState`` the notification puts its source in.
    :param notification: The ``MaintenanceNotification`` that was handled.
    :param config: The ``MaintNotificationsConfig`` the handler acted with.
    """

    def __init__(
        self,
        connection_pool: Optional[object],
        connection: Optional[object],
        state: "MaintenanceState",
        notification: Optional["MaintenanceNotification"],
        config: "MaintNotificationsConfig",
    ):
        self._connection_pool = connection_pool
        self._connection = connection
        self._state = state
        self._notification = notification
        self._config = config

    @property
    def connection_pool(self) -> Optional[object]:
        return self._connection_pool

    @property
    def connection(self) -> Optional[object]:
        return self._connection

    @property
    def state(self) -> "MaintenanceState":
        return self._state

    @property
    def notification(self) -> Optional["MaintenanceNotification"]:
        return self._notification

    @property
    def config(self) -> "MaintNotificationsConfig":
        return self._config


class MaintenanceStartedEvent(MaintenanceEvent):
    """
    Event fired when a maintenance notification starts relaxing the timeouts of
    its source. Paired with a ``MaintenanceCompletedEvent`` carrying the same
    ``state`` and source.
    """

    pass


class MaintenanceCompletedEvent(MaintenanceEvent):
    """
    Event fired when the timeout relaxation announced by a
    ``MaintenanceStartedEvent`` is reverted: the completion notification
    arrived, the handoff TTL expired, or the connection was closed while
    still under maintenance. Also fired, with ``MaintenanceState.MAINTENANCE``,
    when a handoff takes over a connection under maintenance: the connection
    stays relaxed, but under the handoff's ``MOVING`` pair from then on. In
    the last two cases ``notification`` is ``None``.

    Not fired when a newer handoff superseded the one that completed; the
    source stays relaxed until the newer one completes.
    """

    pass


class ClientType(Enum):
    SYNC = ("sync",)
    ASYNC = ("async",)


class AfterPooledConnectionsInstantiationEvent:
    """
    Event that will be fired after pooled connection instances was created.
    """

    def __init__(
        self,
        connection_pools: List,
        client_type: ClientType,
        credential_provider: Optional[CredentialProvider] = None,
    ):
        self._connection_pools = connection_pools
        self._client_type = client_type
        self._credential_provider = credential_provider

    @property
    def connection_pools(self):
        return self._connection_pools

    @property
    def client_type(self) -> ClientType:
        return self._client_type

    @property
    def credential_provider(self) -> Union[CredentialProvider, None]:
        return self._credential_provider


class AfterSingleConnectionInstantiationEvent:
    """
    Event that will be fired after single connection instances was created.

    :param connection_lock: For sync client thread-lock should be provided,
    for async asyncio.Lock
    """

    def __init__(
        self,
        connection,
        client_type: ClientType,
        connection_lock: Union[threading.RLock, asyncio.Lock],
    ):
        self._connection = connection
        self._client_type = client_type
        self._connection_lock = connection_lock

    @property
    def connection(self):
        return self._connection

    @property
    def client_type(self) -> ClientType:
        return self._client_type

    @property
    def connection_lock(self) -> Union[threading.RLock, asyncio.Lock]:
        return self._connection_lock


class AfterPubSubConnectionInstantiationEvent:
    def __init__(
        self,
        pubsub_connection,
        connection_pool,
        client_type: ClientType,
        connection_lock: Union[threading.RLock, asyncio.Lock],
    ):
        self._pubsub_connection = pubsub_connection
        self._connection_pool = connection_pool
        self._client_type = client_type
        self._connection_lock = connection_lock

    @property
    def pubsub_connection(self):
        return self._pubsub_connection

    @property
    def connection_pool(self):
        return self._connection_pool

    @property
    def client_type(self) -> ClientType:
        return self._client_type

    @property
    def connection_lock(self) -> Union[threading.RLock, asyncio.Lock]:
        return self._connection_lock


class AfterAsyncClusterInstantiationEvent:
    """
    Event that will be fired after async cluster instance was created.

    Async cluster doesn't use connection pools,
    instead ClusterNode object manages connections.
    """

    def __init__(
        self,
        nodes: dict,
        credential_provider: Optional[CredentialProvider] = None,
    ):
        self._nodes = nodes
        self._credential_provider = credential_provider

    @property
    def nodes(self) -> dict:
        return self._nodes

    @property
    def credential_provider(self) -> Union[CredentialProvider, None]:
        return self._credential_provider


class OnCommandsFailEvent:
    """
    Event fired whenever a command fails during the execution.
    """

    def __init__(
        self,
        commands: tuple,
        exception: Exception,
    ):
        self._commands = commands
        self._exception = exception

    @property
    def commands(self) -> tuple:
        return self._commands

    @property
    def exception(self) -> Exception:
        return self._exception


class AsyncOnCommandsFailEvent(OnCommandsFailEvent):
    pass


class ReAuthConnectionListener(EventListenerInterface):
    """
    Listener that performs re-authentication of given connection.
    """

    def listen(self, event: AfterConnectionReleasedEvent):
        event.connection.re_auth()


class AsyncReAuthConnectionListener(AsyncEventListenerInterface):
    """
    Async listener that performs re-authentication of given connection.
    """

    async def listen(self, event: AsyncAfterConnectionReleasedEvent):
        await event.connection.re_auth()


class RegisterReAuthForPooledConnections(EventListenerInterface):
    """
    Listener that registers a re-authentication callback for pooled connections.
    Required by :class:`StreamingCredentialProvider`.
    """

    def __init__(self):
        self._event = None

    def listen(self, event: AfterPooledConnectionsInstantiationEvent):
        if isinstance(event.credential_provider, StreamingCredentialProvider):
            self._event = event

            if event.client_type == ClientType.SYNC:
                event.credential_provider.on_next(self._re_auth)
                event.credential_provider.on_error(self._raise_on_error)
            else:
                event.credential_provider.on_next(self._re_auth_async)
                event.credential_provider.on_error(self._raise_on_error_async)

    def _re_auth(self, token):
        first_error = None
        for pool in self._event.connection_pools:
            try:
                pool.re_auth_callback(token)
            except Exception as e:
                if first_error is None:
                    first_error = e
        if first_error is not None:
            raise first_error

    async def _re_auth_async(self, token):
        first_error = None
        for pool in self._event.connection_pools:
            try:
                await pool.re_auth_callback(token)
            except Exception as e:
                if first_error is None:
                    first_error = e
        if first_error is not None:
            raise first_error

    def _raise_on_error(self, error: Exception):
        raise EventException(error, self._event)

    async def _raise_on_error_async(self, error: Exception):
        raise EventException(error, self._event)


class RegisterReAuthForSingleConnection(EventListenerInterface):
    """
    Listener that registers a re-authentication callback for single connection.
    Required by :class:`StreamingCredentialProvider`.
    """

    def __init__(self):
        self._event = None

    def listen(self, event: AfterSingleConnectionInstantiationEvent):
        if isinstance(
            event.connection.credential_provider, StreamingCredentialProvider
        ):
            self._event = event

            if event.client_type == ClientType.SYNC:
                event.connection.credential_provider.on_next(self._re_auth)
                event.connection.credential_provider.on_error(self._raise_on_error)
            else:
                event.connection.credential_provider.on_next(self._re_auth_async)
                event.connection.credential_provider.on_error(
                    self._raise_on_error_async
                )

    def _re_auth(self, token):
        with self._event.connection_lock:
            self._event.connection.send_command(
                "AUTH", token.try_get("oid"), token.get_value()
            )
            self._event.connection.read_response()

    async def _re_auth_async(self, token):
        async with self._event.connection_lock:
            await self._event.connection.send_command(
                "AUTH", token.try_get("oid"), token.get_value()
            )
            await self._event.connection.read_response()

    def _raise_on_error(self, error: Exception):
        raise EventException(error, self._event)

    async def _raise_on_error_async(self, error: Exception):
        raise EventException(error, self._event)


class RegisterReAuthForAsyncClusterNodes(EventListenerInterface):
    def __init__(self):
        self._event = None

    def listen(self, event: AfterAsyncClusterInstantiationEvent):
        if isinstance(event.credential_provider, StreamingCredentialProvider):
            self._event = event
            event.credential_provider.on_next(self._re_auth)
            event.credential_provider.on_error(self._raise_on_error)

    async def _re_auth(self, token: TokenInterface):
        nodes = self._event.nodes
        first_error = None
        # Iterate a snapshot: a topology refresh can add and remove nodes while
        # a node is being re-authenticated. A node that is no longer the one cached
        # under its key is skipped: it is being closed, even if a later refresh
        # re-added its address as a new node. Nodes added in the meantime connect
        # with the current credentials and need no AUTH.
        for key, node in tuple(nodes.items()):
            if nodes.get(key) is node:
                try:
                    await node.re_auth_callback(token)
                except Exception as e:
                    if first_error is None:
                        first_error = e
        if first_error is not None:
            raise first_error

    async def _raise_on_error(self, error: Exception):
        raise EventException(error, self._event)


class RegisterReAuthForPubSub(EventListenerInterface):
    def __init__(self):
        self._connection = None
        self._connection_pool = None
        self._client_type = None
        self._connection_lock = None
        self._event = None

    def listen(self, event: AfterPubSubConnectionInstantiationEvent):
        if isinstance(
            event.pubsub_connection.credential_provider, StreamingCredentialProvider
        ) and check_protocol_version(event.pubsub_connection.get_protocol(), 3):
            self._event = event
            self._connection = event.pubsub_connection
            self._connection_pool = event.connection_pool
            self._client_type = event.client_type
            self._connection_lock = event.connection_lock

            if self._client_type == ClientType.SYNC:
                self._connection.credential_provider.on_next(self._re_auth)
                self._connection.credential_provider.on_error(self._raise_on_error)
            else:
                self._connection.credential_provider.on_next(self._re_auth_async)
                self._connection.credential_provider.on_error(
                    self._raise_on_error_async
                )

    def _re_auth(self, token: TokenInterface):
        with self._connection_lock:
            self._connection.send_command(
                "AUTH", token.try_get("oid"), token.get_value()
            )
            self._connection.read_response()

        self._connection_pool.re_auth_callback(token)

    async def _re_auth_async(self, token: TokenInterface):
        async with self._connection_lock:
            await self._connection.send_command(
                "AUTH", token.try_get("oid"), token.get_value()
            )
            await self._connection.read_response()

        await self._connection_pool.re_auth_callback(token)

    def _raise_on_error(self, error: Exception):
        raise EventException(error, self._event)

    async def _raise_on_error_async(self, error: Exception):
        raise EventException(error, self._event)


class InitializeConnectionCountObservability(EventListenerInterface):
    """
    Listener that initializes connection count observability.
    """

    @deprecated_function(
        reason="Connection count is now tracked via record_connection_count(). "
        "This functionality will be removed in the next major version",
        version="7.4.0",
    )
    def listen(self, event: AfterPooledConnectionsInstantiationEvent):
        # Initialize gauge only once, subsequent calls won't have an affect.
        # Note: init_connection_count() and register_pools_connection_count()
        # are deprecated and will emit their own warnings.
        init_connection_count()

        # Register pools for connection count observability.
        register_pools_connection_count(event.connection_pools)
