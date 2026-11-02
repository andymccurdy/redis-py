import asyncio
import copy
import inspect
import logging
import math
import re
import time
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
    Sequence,
    Set,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

from redis._defaults import (
    DEFAULT_RETRY_BASE,
    DEFAULT_RETRY_CAP,
    DEFAULT_RETRY_COUNT,
    DEFAULT_SOCKET_CONNECT_TIMEOUT,
    DEFAULT_SOCKET_READ_SIZE,
    DEFAULT_SOCKET_TIMEOUT,
)
from redis._parsers.helpers import bool_ok, decode_info, get_response_callbacks
from redis.asyncio import _himport_exec
from redis.asyncio.connection import (
    AbstractConnection,
    Connection,
    ConnectionPool,
    SSLConnection,
    UnixDomainSocketConnection,
)
from redis.asyncio.lock import Lock
from redis.asyncio.observability.recorder import (
    record_error_count,
    record_operation_duration,
    record_pubsub_message,
)
from redis.asyncio.retry import Retry, _to_async_retry
from redis.backoff import ExponentialWithJitterBackoff
from redis.client import (
    EMPTY_RESPONSE,
    NEVER_DECODE,
    AbstractRedis,
    CaseInsensitiveDict,
    _decode_transaction_replies,
    _transaction_never_decode,
)
from redis.commands import (
    AsyncCoreCommands,
    AsyncRedisModuleCommands,
    AsyncSentinelCommands,
    list_or_args,
)
from redis.commands.helpers import parse_pubsub_subscriptions, pubsub_subscription_args
from redis.credentials import CredentialProvider
from redis.driver_info import DriverInfo, resolve_driver_info
from redis.event import (
    AfterPooledConnectionsInstantiationEvent,
    AfterPubSubConnectionInstantiationEvent,
    AfterSingleConnectionInstantiationEvent,
    ClientType,
    EventDispatcher,
)
from redis.exceptions import (
    ConnectionError,
    ExecAbortError,
    PubSubError,
    RedisError,
    ResponseError,
    WatchError,
)
from redis.himport import HImportRegistry, parse_himport_set_args
from redis.maint_notifications import MaintNotificationsConfig
from redis.observability.attributes import PubSubDirection
from redis.typing import (
    ChannelT,
    EncodableT,
    FieldT,
    KeyT,
    PubSubHandler,
    Subscription,
)
from redis.utils import (
    SENTINEL,
    SSL_AVAILABLE,
    _set_info_logger,
    check_protocol_version,
    command_text,
    deprecated_args,
    deprecated_function,
    experimental_method,
    safe_str,
    str_if_bytes,
    truncate_text,
)

if TYPE_CHECKING and SSL_AVAILABLE:
    from ssl import TLSVersion, VerifyFlags, VerifyMode
else:
    TLSVersion = None
    VerifyMode = None
    VerifyFlags = None

_KeyT = TypeVar("_KeyT", bound=KeyT)
_ArgT = TypeVar("_ArgT", KeyT, EncodableT)
_RedisT = TypeVar("_RedisT", bound="Redis")
_NormalizeKeysT = TypeVar("_NormalizeKeysT", bound=Mapping[ChannelT, object])
if TYPE_CHECKING:
    from redis.asyncio.keyspace_notifications import AsyncKeyspaceNotifications
    from redis.commands.core import Script


logger = logging.getLogger(__name__)


def is_debug_log_enabled():
    return logger.isEnabledFor(logging.DEBUG)


def add_debug_log_for_operation_failure(
    connection: AbstractConnection,
    error: BaseException | None = None,
    args: Sequence[Any] | None = None,
):
    details = connection.extract_connection_details() if connection else "no connection"
    prefix = (
        f"{type(error).__name__} received" if error is not None else "Operation failed"
    )
    # Log only the command name - argument values can carry secrets
    # (AUTH, CONFIG SET requirepass, ACL SETUSER) or user data.
    command = f" for command {safe_str(args[0])}" if args else ""
    suffix = f", error: {error}" if error is not None else ""
    logger.debug(
        f"{prefix}{command}, with connection: {connection}, details: {details}{suffix}",
    )


class ResponseCallbackProtocol(Protocol):
    def __call__(self, response: Any, **kwargs): ...


class AsyncResponseCallbackProtocol(Protocol):
    async def __call__(self, response: Any, **kwargs): ...


ResponseCallbackT = Union[ResponseCallbackProtocol, AsyncResponseCallbackProtocol]


class Redis(
    AbstractRedis, AsyncRedisModuleCommands, AsyncCoreCommands, AsyncSentinelCommands
):
    """
    Implementation of the Redis protocol.

    This abstract class provides a Python interface to all Redis commands
    and an implementation of the Redis protocol.

    Pipelines derive from this, implementing how
    the commands are sent and received to the Redis server. Based on
    configuration, an instance will either use a ConnectionPool, or
    Connection object to talk to redis.
    """

    # Type discrimination marker for @overload self-type pattern
    _is_async_client: Literal[True] = True

    response_callbacks: MutableMapping[Union[str, bytes], ResponseCallbackT]

    @classmethod
    def from_url(
        cls: Type["Redis"],
        url: str,
        single_connection_client: bool = False,
        auto_close_connection_pool: Optional[bool] = None,
        **kwargs,
    ) -> "Redis":
        """
        Return a Redis client object configured from the given URL

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
        connection_pool = ConnectionPool.from_url(url, **kwargs)
        client = cls(
            connection_pool=connection_pool,
            single_connection_client=single_connection_client,
        )
        if auto_close_connection_pool is not None:
            warnings.warn(
                DeprecationWarning(
                    '"auto_close_connection_pool" is deprecated '
                    "since version 5.0.1. "
                    "Please create a ConnectionPool explicitly and "
                    "provide to the Redis() constructor instead."
                )
            )
        else:
            auto_close_connection_pool = True
        client.auto_close_connection_pool = auto_close_connection_pool
        return client

    @classmethod
    def from_pool(
        cls: Type["Redis"],
        connection_pool: ConnectionPool,
    ) -> "Redis":
        """
        Return a Redis client from the given connection pool.
        The Redis client will take ownership of the connection pool and
        close it when the Redis client is closed.

        Because the client closes (disconnects all connections in) the pool
        when it is closed or garbage-collected, the pool must not be shared
        with other clients. Constructing multiple clients from the same pool
        via ``from_pool`` -- for example one per request across tasks -- is
        not safe: when one client is closed it will disconnect connections
        still in use by the others.

        To share a single pool across clients, construct the pool explicitly
        and manage its lifecycle instead. Unlike ``from_pool``, the plain
        ``Redis(connection_pool=pool)`` constructor does not take ownership of
        the pool and will not close it, so a pool created this way can be
        safely shared across clients. ``ConnectionPool`` supports the async
        context manager protocol for this::

            async with ConnectionPool.from_url(url) as pool:
                r = Redis(connection_pool=pool)
        """
        client = cls(
            connection_pool=connection_pool,
        )
        client.auto_close_connection_pool = True
        return client

    @deprecated_args(
        args_to_warn=["retry_on_timeout"],
        reason="TimeoutError is included by default.",
        version="6.0.0",
    )
    @deprecated_args(
        args_to_warn=["lib_name", "lib_version"],
        reason="Use 'driver_info' parameter instead. "
        "lib_name and lib_version will be removed in a future version.",
    )
    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 6379,
        db: str | int = 0,
        password: str | None = None,
        socket_timeout: float | None = DEFAULT_SOCKET_TIMEOUT,
        socket_connect_timeout: float | None = DEFAULT_SOCKET_CONNECT_TIMEOUT,
        socket_read_size: int = DEFAULT_SOCKET_READ_SIZE,
        socket_keepalive: bool | None = True,
        socket_keepalive_options: Mapping[int, int | bytes] | object | None = SENTINEL,
        connection_pool: ConnectionPool | None = None,
        unix_socket_path: str | None = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        retry_on_timeout: bool = False,
        retry: Retry = Retry(
            backoff=ExponentialWithJitterBackoff(
                base=DEFAULT_RETRY_BASE, cap=DEFAULT_RETRY_CAP
            ),
            retries=DEFAULT_RETRY_COUNT,
        ),
        retry_on_error: list | None = None,
        ssl: bool = False,
        ssl_keyfile: str | None = None,
        ssl_certfile: str | None = None,
        ssl_cert_reqs: "str | VerifyMode" = "required",
        ssl_include_verify_flags: List["VerifyFlags"] | None = None,
        ssl_exclude_verify_flags: List["VerifyFlags"] | None = None,
        ssl_ca_certs: str | None = None,
        ssl_ca_data: str | None = None,
        ssl_ca_path: str | None = None,
        ssl_check_hostname: bool = True,
        ssl_min_version: "TLSVersion | None" = None,
        ssl_ciphers: str | None = None,
        ssl_password: str | None = None,
        max_connections: int | None = None,
        single_connection_client: bool = False,
        health_check_interval: int = 0,
        client_name: str | None = None,
        lib_name: str | object | None = SENTINEL,
        lib_version: str | object | None = SENTINEL,
        driver_info: DriverInfo | object | None = SENTINEL,
        username: str | None = None,
        auto_close_connection_pool: bool | None = None,
        redis_connect_func=None,
        credential_provider: CredentialProvider | None = None,
        protocol: int | None = None,
        legacy_responses: bool = True,
        event_dispatcher: EventDispatcher | None = None,
        maint_notifications_config: MaintNotificationsConfig | None = None,
    ):
        """
        Initialize a new Redis client.

        To specify a retry policy for specific errors, you have two options:

        1. Set the `retry_on_error` to a list of the error/s to retry on, and
        you can also set `retry` to a valid `Retry` object(in case the default
        one is not appropriate) - with this approach the retries will be triggered
        on the default errors specified in the Retry object enriched with the
        errors specified in `retry_on_error`.

        2. Define a `Retry` object with configured 'supported_errors' and set
        it to the `retry` parameter - with this approach you completely redefine
        the errors on which retries will happen.

        `retry_on_timeout` is deprecated - please include the TimeoutError
        either in the Retry object or in the `retry_on_error` list.

        When 'connection_pool' is provided - the retry configuration of the
        provided pool will be used.

        Args:

        socket_keepalive:
            if `True`, TCP keepalive is enabled for TCP socket connections.
            Argument is ignored when connection_pool is provided.
        socket_keepalive_options:
            mapping of TCP keepalive socket option constants to values, for
            example `{socket.TCP_KEEPIDLE: 30}`. If left unspecified, redis-py
            uses TCP keepalive defaults when `socket_keepalive` is enabled:
            idle 30 seconds, interval 5 seconds, and 3 probes. Platform-specific
            options that are not available are skipped. Pass `None` or `{}` to
            avoid setting additional TCP keepalive options. Argument is ignored
            when connection_pool is provided.
        maint_notifications_config:
            configures the pool to support maintenance notifications - see
            `redis.maint_notifications.MaintNotificationsConfig` for details.
            Only supported with RESP3
            If not provided and protocol is RESP3, the maintenance notifications
            will be enabled by default (logic is included in the connection pool
            initialization).
            Argument is ignored when connection_pool is provided.
        """
        kwargs: Dict[str, Any]
        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher
        # auto_close_connection_pool only has an effect if connection_pool is
        # None. It is assumed that if connection_pool is not None, the user
        # wants to manage the connection pool themselves.
        if auto_close_connection_pool is not None:
            warnings.warn(
                DeprecationWarning(
                    '"auto_close_connection_pool" is deprecated '
                    "since version 5.0.1. "
                    "Please create a ConnectionPool explicitly and "
                    "provide to the Redis() constructor instead."
                )
            )
        else:
            auto_close_connection_pool = True

        if not connection_pool:
            # Create internal connection pool, expected to be closed by Redis instance
            if not retry_on_error:
                retry_on_error = []

            # Handle driver_info: if provided, use it; otherwise create from lib_name/lib_version.
            computed_driver_info = resolve_driver_info(
                driver_info, lib_name, lib_version
            )

            kwargs = {
                "db": db,
                "username": username,
                "password": password,
                "credential_provider": credential_provider,
                "socket_timeout": socket_timeout,
                "socket_read_size": socket_read_size,
                "encoding": encoding,
                "encoding_errors": encoding_errors,
                "decode_responses": decode_responses,
                "retry_on_error": retry_on_error,
                "retry": copy.deepcopy(retry),
                "max_connections": max_connections,
                "health_check_interval": health_check_interval,
                "client_name": client_name,
                "driver_info": computed_driver_info,
                "redis_connect_func": redis_connect_func,
                "protocol": protocol,
                "legacy_responses": legacy_responses,
            }
            # based on input, setup appropriate connection args
            if unix_socket_path is not None:
                if (
                    maint_notifications_config
                    and maint_notifications_config.enabled is True
                ):
                    raise RedisError(
                        "Maintenance notifications are not supported with Unix "
                        "domain socket connections"
                    )
                kwargs.update(
                    {
                        "path": unix_socket_path,
                        "connection_class": UnixDomainSocketConnection,
                        "maint_notifications_config": MaintNotificationsConfig(
                            enabled=False
                        ),
                    }
                )
            else:
                # TCP specific options
                kwargs.update(
                    {
                        "host": host,
                        "port": port,
                        "socket_connect_timeout": socket_connect_timeout,
                        "socket_keepalive": socket_keepalive,
                        "socket_keepalive_options": socket_keepalive_options,
                    }
                )

                if ssl:
                    kwargs.update(
                        {
                            "connection_class": SSLConnection,
                            "ssl_keyfile": ssl_keyfile,
                            "ssl_certfile": ssl_certfile,
                            "ssl_cert_reqs": ssl_cert_reqs,
                            "ssl_include_verify_flags": ssl_include_verify_flags,
                            "ssl_exclude_verify_flags": ssl_exclude_verify_flags,
                            "ssl_ca_certs": ssl_ca_certs,
                            "ssl_ca_data": ssl_ca_data,
                            "ssl_ca_path": ssl_ca_path,
                            "ssl_check_hostname": ssl_check_hostname,
                            "ssl_min_version": ssl_min_version,
                            "ssl_ciphers": ssl_ciphers,
                            "ssl_password": ssl_password,
                        }
                    )
            maint_notifications_enabled = (
                maint_notifications_config and maint_notifications_config.enabled
            )
            if maint_notifications_enabled and not check_protocol_version(protocol, 3):
                raise RedisError(
                    "Maintenance notifications handlers on connection are only supported with RESP version 3"
                )
            if maint_notifications_config:
                kwargs.update(
                    {
                        "maint_notifications_config": maint_notifications_config,
                    }
                )
            # This arg only used if no pool is passed in
            self.auto_close_connection_pool = auto_close_connection_pool
            connection_pool = ConnectionPool(**kwargs)
            self._event_dispatcher.dispatch(
                AfterPooledConnectionsInstantiationEvent(
                    [connection_pool], ClientType.ASYNC, credential_provider
                )
            )
        else:
            # If a pool is passed in, do not close it
            self.auto_close_connection_pool = False
            self._event_dispatcher.dispatch(
                AfterPooledConnectionsInstantiationEvent(
                    [connection_pool], ClientType.ASYNC, credential_provider
                )
            )

        self.connection_pool = connection_pool
        self.single_connection_client = single_connection_client
        self.connection: Optional[Connection] = None

        connection_kwargs = self.connection_pool.connection_kwargs
        self.response_callbacks = CaseInsensitiveDict(
            get_response_callbacks(
                user_protocol=connection_kwargs.get("protocol"),
                legacy_responses=connection_kwargs.get("legacy_responses", True),
            )
        )

        # If using a single connection client, we need to lock creation-of and use-of
        # the client in order to avoid race conditions such as using asyncio.gather
        # on a set of redis commands
        self._single_conn_lock = asyncio.Lock()

        # When used as an async context manager, we need to increment and decrement
        # a usage counter so that we can close the connection pool when no one is
        # using the client.
        self._usage_counter = 0
        self._usage_lock = asyncio.Lock()

    def __repr__(self):
        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__}"
            f"({self.connection_pool!r})>"
        )

    def __await__(self):
        return self.initialize().__await__()

    async def initialize(self: _RedisT) -> _RedisT:
        if self.single_connection_client:
            async with self._single_conn_lock:
                if self.connection is None:
                    self.connection = await self.connection_pool.get_connection()

            self._event_dispatcher.dispatch(
                AfterSingleConnectionInstantiationEvent(
                    self.connection, ClientType.ASYNC, self._single_conn_lock
                )
            )
        return self

    def set_response_callback(self, command: str, callback: ResponseCallbackT):
        """Set a custom Response Callback"""
        self.response_callbacks[command] = callback

    def get_encoder(self):
        """Get the connection pool's encoder"""
        return self.connection_pool.get_encoder()

    def get_connection_kwargs(self):
        """Get the connection's key-word arguments"""
        return self.connection_pool.connection_kwargs

    @property
    def himport_registry(self) -> HImportRegistry:
        """The client's HIMPORT fieldset registry (empty if none was declared).

        Read-only: the registry is mutated only through the HIMPORT command methods.
        """
        return self.connection_pool.himport_registry

    def get_retry(self) -> Optional[Retry]:
        return self.get_connection_kwargs().get("retry")

    def set_retry(self, retry: Retry) -> None:
        retry = _to_async_retry(retry)
        self.get_connection_kwargs().update({"retry": retry})
        self.connection_pool.set_retry(retry)

    def load_external_module(self, funcname, func):
        """
        This function can be used to add externally defined redis modules,
        and their namespaces to the redis client.

        funcname - A string containing the name of the function to create
        func - The function, being added to this class.

        ex: Assume that one has a custom redis module named foomod that
        creates command named 'foo.dothing' and 'foo.anotherthing' in redis.
        To load function functions into this namespace:

        from redis import Redis
        from foomodule import F
        r = Redis()
        r.load_external_module("foo", F)
        r.foo().dothing('your', 'arguments')

        For a concrete example see the reimport of the redisjson module in
        tests/test_connection.py::test_loading_external_modules
        """
        setattr(self, funcname, func)

    def pipeline(
        self, transaction: bool = True, shard_hint: Optional[str] = None
    ) -> "Pipeline":
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        return Pipeline(
            self.connection_pool, self.response_callbacks, transaction, shard_hint
        )

    async def transaction(
        self,
        func: Callable[["Pipeline"], Union[Any, Awaitable[Any]]],
        *watches: KeyT,
        shard_hint: Optional[str] = None,
        value_from_callable: bool = False,
        watch_delay: Optional[float] = None,
    ):
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single argument which is a Pipeline object.
        """
        pipe: Pipeline
        async with self.pipeline(True, shard_hint) as pipe:
            while True:
                try:
                    if watches:
                        await pipe.watch(*watches)
                    func_value = func(pipe)
                    if inspect.isawaitable(func_value):
                        func_value = await func_value
                    exec_value = await pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        await asyncio.sleep(watch_delay)
                    continue

    def lock(
        self,
        name: KeyT,
        timeout: Optional[float] = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: Optional[float] = None,
        lock_class: Optional[Type[Lock]] = None,
        thread_local: bool = True,
        raise_on_release_error: bool = True,
    ) -> Lock:
        """
        Return a new Lock object using key ``name`` that mimics
        the behavior of threading.Lock.

        If specified, ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        ``blocking`` indicates whether calling ``acquire`` should block until
        the lock has been acquired or to fail immediately, causing ``acquire``
        to return False and the lock not being acquired. Defaults to True.
        Note this value can be overridden by passing a ``blocking``
        argument to ``acquire``.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``lock_class`` forces the specified lock implementation. Note that as
        of redis-py 3.0, the only lock class we implement is ``Lock`` (which is
        a Lua-based lock). So, it's unlikely you'll need this parameter, unless
        you have created your own custom lock class.

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. Consider the following timeline:

            time: 0, thread-1 acquires `my-lock`, with a timeout of 5 seconds.
                     thread-1 sets the token to "abc"
            time: 1, thread-2 blocks trying to acquire `my-lock` using the
                     Lock instance.
            time: 5, thread-1 has not yet completed. redis expires the lock
                     key.
            time: 5, thread-2 acquired `my-lock` now that it's available.
                     thread-2 sets the token to "xyz"
            time: 6, thread-1 finishes its work and calls release(). if the
                     token is *not* stored in thread local storage, then
                     thread-1 would see the token value as "xyz" and would be
                     able to successfully release the thread-2's lock.

        ``raise_on_release_error`` indicates whether to raise an exception when
        the lock is no longer owned when exiting the context manager. By default,
        this is True, meaning an exception will be raised. If False, the warning
        will be logged and the exception will be suppressed.

        In some use cases it's necessary to disable thread local storage. For
        example, if you have code where one thread acquires a lock and passes
        that lock instance to a worker thread to release later. If thread
        local storage isn't disabled in this case, the worker thread won't see
        the token set by the thread that acquired the lock. Our assumption
        is that these cases aren't common and as such default to using
        thread local storage."""
        if lock_class is None:
            lock_class = Lock
        return lock_class(
            self,
            name,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
            raise_on_release_error=raise_on_release_error,
        )

    def pubsub(self, **kwargs) -> "PubSub":
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        them.
        """
        return PubSub(
            self.connection_pool, event_dispatcher=self._event_dispatcher, **kwargs
        )

    def keyspace_notifications(
        self,
        key_prefix: Union[str, bytes, None] = None,
        ignore_subscribe_messages: bool = True,
    ) -> "AsyncKeyspaceNotifications":
        """
        Return an :class:`~redis.asyncio.keyspace_notifications.AsyncKeyspaceNotifications`
        object for subscribing to keyspace and keyevent notifications.

        Note: Keyspace notifications must be enabled on the Redis server via
        the ``notify-keyspace-events`` configuration option.

        Args:
            key_prefix: Optional prefix to filter and strip from keys in
                        notifications.
            ignore_subscribe_messages: If True, subscribe/unsubscribe
                                      confirmations are not returned by
                                      get_message/listen.
        """
        from redis.asyncio.keyspace_notifications import AsyncKeyspaceNotifications

        return AsyncKeyspaceNotifications(
            self,
            key_prefix=key_prefix,
            ignore_subscribe_messages=ignore_subscribe_messages,
        )

    def monitor(self) -> "Monitor":
        return Monitor(self.connection_pool)

    def client(self) -> "Redis":
        return self.__class__(
            connection_pool=self.connection_pool, single_connection_client=True
        )

    async def __aenter__(self: _RedisT) -> _RedisT:
        """
        Async context manager entry. Increments a usage counter so that the
        connection pool is only closed (via aclose()) when no context is using
        the client.
        """
        await self._increment_usage()
        try:
            # Initialize the client (i.e. establish connection, etc.)
            return await self.initialize()
        except (Exception, asyncio.CancelledError):
            # If initialization fails, decrement the counter to keep it in sync
            await asyncio.shield(self._decrement_usage())
            raise

    async def _increment_usage(self) -> int:
        """
        Helper coroutine to increment the usage counter while holding the lock.
        Returns the new value of the usage counter.
        """
        async with self._usage_lock:
            self._usage_counter += 1
            return self._usage_counter

    async def _decrement_usage(self) -> int:
        """
        Helper coroutine to decrement the usage counter while holding the lock.
        Returns the new value of the usage counter.
        """
        async with self._usage_lock:
            self._usage_counter -= 1
            return self._usage_counter

    async def _release_usage(self) -> None:
        """
        Decrement the usage counter and close the client on the last exit.

        Hold the lock through closing so a new context cannot use connections
        that are still being disconnected by the previous context.
        """
        async with self._usage_lock:
            self._usage_counter -= 1
            if self._usage_counter == 0:
                await self.aclose()

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Async context manager exit. Decrements a usage counter. If this is the
        last exit (counter becomes zero), the client closes its connection pool.
        """
        await asyncio.shield(self._release_usage())

    _DEL_MESSAGE = "Unclosed Redis client"

    # passing _warnings and _grl as argument default since they may be gone
    # by the time __del__ is called at shutdown
    def __del__(
        self,
        _warn: Any = warnings.warn,
        _grl: Any = asyncio.get_running_loop,
    ) -> None:
        if hasattr(self, "connection") and (self.connection is not None):
            _warn(f"Unclosed client session {self!r}", ResourceWarning, source=self)
            try:
                context = {"client": self, "message": self._DEL_MESSAGE}
                _grl().call_exception_handler(context)
            except RuntimeError:
                pass
            self.connection._close()

    async def aclose(self, close_connection_pool: Optional[bool] = None) -> None:
        """
        Closes Redis client connection

        Args:
            close_connection_pool:
                decides whether to close the connection pool used by this Redis client,
                overriding Redis.auto_close_connection_pool.
                By default, let Redis.auto_close_connection_pool decide
                whether to close the connection pool.
        """
        conn = self.connection
        if conn:
            self.connection = None
            await self.connection_pool.release(conn)
        if close_connection_pool or (
            close_connection_pool is None and self.auto_close_connection_pool
        ):
            await self.connection_pool.aclose()

    @deprecated_function(version="5.0.1", reason="Use aclose() instead", name="close")
    async def close(self, close_connection_pool: Optional[bool] = None) -> None:
        """
        Alias for aclose(), for backwards compatibility
        """
        await self.aclose(close_connection_pool)

    async def _send_command_parse_response(self, conn, command_name, *args, **options):
        """
        Send a command and parse the response
        """
        # HIMPORT SET is the one command whose wire form depends on per-connection
        # state: the fieldset must be PREPAREd on this connection first, and any
        # fieldset discarded since this connection last reconciled must be dropped.
        # Handling it here (rather than in himport_set) lets himport_set reuse the
        # full execute_command machinery — retry, disconnect-on-error, pooling — so
        # a failed HIMPORT SET disconnects the connection like any other command.
        # This per-command branch in the hot dispatch path is deliberate and has no
        # cleaner alternative: this is the only seam where the concrete borrowed
        # connection is known, and connection-scoped session setup can only happen
        # once that connection is chosen. The overhead is one string compare per
        # command.
        himport_set = parse_himport_set_args(args)
        if himport_set is not None:
            # ``args`` is an HIMPORT SET in either the joined ("HIMPORT SET", key,
            # ...) or split ("HIMPORT", "SET", key, ...) raw form; the operands come
            # back at the right offsets for the form. A command with too few operands
            # returns None and falls through to the normal send path so the server
            # returns its arity error instead of a client-side IndexError here.
            key, fieldset_name, values = himport_set
            return await self._himport_execute_set(conn, key, fieldset_name, values)
        await conn.send_command(*args)
        return await self.parse_response(conn, command_name, **options)

    async def _himport_reconcile_discards(self, conn):
        """Delegate to the shared async HIMPORT executor."""
        return await _himport_exec.reconcile_discards(self, conn)

    async def _himport_prepare_and_set(
        self, conn, key, fieldset_name, values, fieldset
    ):
        """Delegate to the shared async HIMPORT executor."""
        return await _himport_exec.prepare_and_set(
            self, conn, key, fieldset_name, values, fieldset
        )

    async def _himport_execute_set(self, conn, key, fieldset_name, values):
        """Delegate to the shared async HIMPORT executor."""
        return await _himport_exec.execute_set(self, conn, key, fieldset_name, values)

    async def _close_connection(
        self,
        conn: Connection,
        error: Optional[BaseException] = None,
        failure_count: Optional[int] = None,
        start_time: Optional[float] = None,
        command_name: Optional[str] = None,
    ):
        """
        Close the connection before retrying.

        The supported exceptions are already checked in the
        retry object so we don't need to do it here.

        After we disconnect the connection, it will try to reconnect and
        do a health check as part of the send_command logic(on connection level).
        """
        if (
            error
            and failure_count is not None
            and failure_count <= conn.retry.get_retries()
        ):
            await record_operation_duration(
                command_name=command_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
                error=error,
                retry_attempts=failure_count,
            )

        await conn.disconnect(error=error, failure_count=failure_count)

    # COMMAND EXECUTION AND PROTOCOL PARSING
    async def execute_command(self, *args, **options):
        """Execute a command and return a parsed response"""
        await self.initialize()
        pool = self.connection_pool
        command_name = args[0]
        conn = self.connection or await pool.get_connection()

        # Start timing for observability
        start_time = time.monotonic()
        # Track actual retry attempts for error reporting
        actual_retry_attempts = 0

        def failure_callback(error, failure_count):
            if is_debug_log_enabled():
                add_debug_log_for_operation_failure(conn, error, args)
            nonlocal actual_retry_attempts
            actual_retry_attempts = failure_count
            return self._close_connection(
                conn, error, failure_count, start_time, command_name
            )

        if self.single_connection_client:
            await self._single_conn_lock.acquire()
        try:
            result = await conn.retry.call_with_retry(
                lambda: self._send_command_parse_response(
                    conn, command_name, *args, **options
                ),
                failure_callback,
                with_failure_count=True,
            )

            await record_operation_duration(
                command_name=command_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
            )
            return result
        except Exception as e:
            await record_error_count(
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                network_peer_address=getattr(conn, "host", None),
                network_peer_port=getattr(conn, "port", None),
                error_type=e,
                retry_attempts=actual_retry_attempts,
                is_internal=False,
            )
            raise
        finally:
            try:
                if self.single_connection_client and conn and conn.should_reconnect():
                    await self._close_connection(conn)
                    await conn.connect()
            finally:
                if self.single_connection_client:
                    self._single_conn_lock.release()
                if not self.connection:
                    await pool.release(conn)

    async def parse_response(
        self, connection: Connection, command_name: Union[str, bytes], **options
    ):
        """Parses a response from the Redis server"""
        try:
            if NEVER_DECODE in options:
                response = await connection.read_response(disable_decoding=True)
                options.pop(NEVER_DECODE)
                if command_name == "INFO":
                    response = decode_info(response, self.get_encoder())
            else:
                response = await connection.read_response()
        except ResponseError:
            if EMPTY_RESPONSE in options:
                return options[EMPTY_RESPONSE]
            raise

        if EMPTY_RESPONSE in options:
            options.pop(EMPTY_RESPONSE)

        # Remove keys entry, it needs only for cache.
        options.pop("keys", None)

        if command_name in self.response_callbacks:
            # Mypy bug: https://github.com/python/mypy/issues/10977
            command_name = cast(str, command_name)
            retval = self.response_callbacks[command_name](response, **options)
            return await retval if inspect.isawaitable(retval) else retval
        return response

    # HIMPORT orchestration (async mirror of redis.client.Redis). See
    # ``.agents/himport_client_support_spec.md``.

    @experimental_method()
    async def himport_prepare(
        self, fieldset_name: str, fields: Iterable[FieldT]
    ) -> bool:
        """Declare an HIMPORT fieldset for use by :meth:`himport_set`."""
        await self.initialize()
        fieldset = self.himport_registry.prepare(fieldset_name, fields)
        conn = self.connection
        if self.single_connection_client and conn is not None and conn.is_connected:
            await self.himport_prepare_internal(fieldset_name, fieldset.fields)
            conn._himport_prepared[fieldset_name] = fieldset.version
        return True

    @experimental_method()
    async def himport_discard(self, fieldset_name: str) -> int:
        """Remove a fieldset from the registry."""
        await self.initialize()
        removed = self.himport_registry.discard(fieldset_name)
        conn = self.connection
        if self.single_connection_client and conn is not None and conn.is_connected:
            if removed:
                await self.himport_discard_internal(fieldset_name)
            conn._himport_prepared.pop(fieldset_name, None)
            conn._himport_reconciled_revision = self.himport_registry.revision
        return 1 if removed else 0

    @experimental_method()
    async def himport_discard_all(self) -> int:
        """Remove all fieldsets from the registry."""
        await self.initialize()
        count = self.himport_registry.discard_all()
        conn = self.connection
        if self.single_connection_client and conn is not None and conn.is_connected:
            if count:
                await self.himport_discard_all_internal()
            conn._himport_prepared.clear()
            conn._himport_reconciled_revision = self.himport_registry.revision
        return count


StrictRedis = Redis


class MonitorCommandInfo(TypedDict):
    time: float
    db: int
    client_address: str
    client_port: str
    client_type: str
    command: str


class Monitor:
    """
    Monitor is useful for handling the MONITOR command to the redis server.
    next_command() method returns one command from monitor
    listen() method yields commands from monitor.
    """

    monitor_re = re.compile(r"\[(\d+) (.*?)\] (.*)")
    command_re = re.compile(r'"(.*?)(?<!\\)"')

    def __init__(self, connection_pool: ConnectionPool):
        self.connection_pool = connection_pool
        self.connection: Optional[Connection] = None

    async def connect(self):
        if self.connection is None:
            self.connection = await self.connection_pool.get_connection()

    async def __aenter__(self):
        await self.connect()
        await self.connection.send_command("MONITOR")
        # check that monitor returns 'OK', but don't return it to user
        response = await self.connection.read_response()
        if not bool_ok(response):
            raise RedisError(f"MONITOR failed: {response}")
        return self

    async def __aexit__(self, *args):
        await self.connection.disconnect()
        await self.connection_pool.release(self.connection)

    async def next_command(self) -> MonitorCommandInfo:
        """Parse the response from a monitor command"""
        await self.connect()
        response = await self.connection.read_response()
        if isinstance(response, bytes):
            response = self.connection.encoder.decode(response, force=True)
        command_time, command_data = response.split(" ", 1)
        m = self.monitor_re.match(command_data)
        db_id, client_info, command = m.groups()
        command = " ".join(self.command_re.findall(command))
        # Redis escapes double quotes because each piece of the command
        # string is surrounded by double quotes. We don't have that
        # requirement so remove the escaping and leave the quote.
        command = command.replace('\\"', '"')

        if client_info == "lua":
            client_address = "lua"
            client_port = ""
            client_type = "lua"
        elif client_info.startswith("unix"):
            client_address = "unix"
            client_port = client_info[5:]
            client_type = "unix"
        else:
            # use rsplit as ipv6 addresses contain colons
            client_address, client_port = client_info.rsplit(":", 1)
            client_type = "tcp"
        return {
            "time": float(command_time),
            "db": int(db_id),
            "client_address": client_address,
            "client_port": client_port,
            "client_type": client_type,
            "command": command,
        }

    async def listen(self) -> AsyncIterator[MonitorCommandInfo]:
        """Listen for commands coming to the server."""
        while True:
            yield await self.next_command()


class PubSub:
    """
    PubSub provides publish, subscribe and listen support to Redis channels.

    After subscribing to one or more channels, the listen() method will block
    until a message arrives on one of the subscribed channels. That message
    will be returned and it's safe to start listening again.
    """

    PUBLISH_MESSAGE_TYPES = ("message", "pmessage", "smessage")
    UNSUBSCRIBE_MESSAGE_TYPES = ("unsubscribe", "punsubscribe", "sunsubscribe")
    HEALTH_CHECK_MESSAGE = "redis-py-health-check"

    def __init__(
        self,
        connection_pool: ConnectionPool,
        shard_hint: Optional[str] = None,
        ignore_subscribe_messages: bool = False,
        encoder=None,
        push_handler_func: Optional[Callable] = None,
        event_dispatcher: Optional["EventDispatcher"] = None,
    ):
        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher
        self.connection_pool = connection_pool
        self.shard_hint = shard_hint
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.connection = None
        # we need to know the encoding options for this connection in order
        # to lookup channel and pattern names for callback handlers.
        self.encoder = encoder
        self.push_handler_func = push_handler_func
        if self.encoder is None:
            self.encoder = self.connection_pool.get_encoder()
        if self.encoder.decode_responses:
            self.health_check_response = [
                ["pong", self.HEALTH_CHECK_MESSAGE],
                self.HEALTH_CHECK_MESSAGE,
            ]
        else:
            self.health_check_response = [
                [b"pong", self.encoder.encode(self.HEALTH_CHECK_MESSAGE)],
                self.encoder.encode(self.HEALTH_CHECK_MESSAGE),
            ]
        if self.push_handler_func is None:
            _set_info_logger()
        self.channels = {}
        self.pending_unsubscribe_channels = set()
        self.patterns = {}
        self.pending_unsubscribe_patterns = set()
        self.shard_channels = {}
        self.pending_unsubscribe_shard_channels = set()
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.aclose()

    def __del__(self):
        if self.connection:
            self.connection.deregister_connect_callback(self.on_connect)

    async def aclose(self):
        # In case a connection property does not yet exist
        # (due to a crash earlier in the Redis() constructor), return
        # immediately as there is nothing to clean-up.
        if not hasattr(self, "connection"):
            return
        async with self._lock:
            if self.connection:
                # Use nowait=True to avoid awaiting StreamWriter.wait_closed(),
                # which can deadlock when a concurrent reader task (e.g. one
                # running pubsub.run() or get_message(block=True)) still holds
                # the transport.  See https://github.com/redis/redis-py/issues/3941
                await self.connection.disconnect(nowait=True)
                self.connection.deregister_connect_callback(self.on_connect)
                await self.connection_pool.release(self.connection)
                self.connection = None
            self.channels = {}
            self.pending_unsubscribe_channels = set()
            self.patterns = {}
            self.pending_unsubscribe_patterns = set()
            self.shard_channels = {}
            self.pending_unsubscribe_shard_channels = set()

    @deprecated_function(version="5.0.1", reason="Use aclose() instead", name="close")
    async def close(self) -> None:
        """Alias for aclose(), for backwards compatibility"""
        await self.aclose()

    @deprecated_function(version="5.0.1", reason="Use aclose() instead", name="reset")
    async def reset(self) -> None:
        """Alias for aclose(), for backwards compatibility"""
        await self.aclose()

    async def _resubscribe(self, subscribed, subscribe_fn) -> None:
        # Replay handler-backed subscriptions as positional Subscription objects
        # so binary names never need to be decoded into keyword argument keys.
        subscriptions = pubsub_subscription_args(subscribed)
        if subscriptions:
            await subscribe_fn(*subscriptions)

    async def _resubscribe_shard_channels(self) -> None:
        await self._resubscribe(self.shard_channels, self.ssubscribe)

    async def on_connect(self, connection: Connection):
        """Re-subscribe to any channels and patterns previously subscribed to"""
        self.pending_unsubscribe_channels.clear()
        self.pending_unsubscribe_patterns.clear()
        self.pending_unsubscribe_shard_channels.clear()
        if self.channels:
            await self._resubscribe(self.channels, self.subscribe)
        if self.patterns:
            await self._resubscribe(self.patterns, self.psubscribe)
        if self.shard_channels:
            await self._resubscribe_shard_channels()

    @property
    def subscribed(self):
        """Indicates if there are subscriptions to any channels or patterns"""
        return bool(self.channels or self.patterns or self.shard_channels)

    async def execute_command(self, *args: EncodableT):
        """Execute a publish/subscribe command"""

        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        await self.connect()
        connection = self.connection
        kwargs = {"check_health": not self.subscribed}
        await self._execute(connection, connection.send_command, *args, **kwargs)

    async def connect(self):
        """
        Ensure that the PubSub is connected
        """
        if self.connection is None:
            self.connection = await self.connection_pool.get_connection()
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            self.connection.register_connect_callback(self.on_connect)
        else:
            await self.connection.connect()
        if self.push_handler_func is not None:
            self.connection._parser.set_pubsub_push_handler(self.push_handler_func)

        self._event_dispatcher.dispatch(
            AfterPubSubConnectionInstantiationEvent(
                self.connection, self.connection_pool, ClientType.ASYNC, self._lock
            )
        )

    async def _reconnect(
        self,
        conn,
        error: Optional[BaseException] = None,
        failure_count: Optional[int] = None,
        start_time: Optional[float] = None,
        command_name: Optional[str] = None,
    ):
        """
        The supported exceptions are already checked in the
        retry object so we don't need to do it here.

        In this error handler we are trying to reconnect to the server.
        """
        if (
            error
            and failure_count is not None
            and failure_count <= conn.retry.get_retries()
        ):
            if command_name:
                await record_operation_duration(
                    command_name=command_name,
                    duration_seconds=time.monotonic() - start_time,
                    server_address=getattr(conn, "host", None),
                    server_port=getattr(conn, "port", None),
                    db_namespace=str(conn.db),
                    error=error,
                    retry_attempts=failure_count,
                )
        await conn.disconnect(error=error, failure_count=failure_count)
        await conn.connect()

    async def _execute(self, conn, command, *args, **kwargs):
        """
        Connect manually upon disconnection. If the Redis server is down,
        this will fail and raise a ConnectionError as desired.
        After reconnection, the ``on_connect`` callback should have been
        called by the # connection to resubscribe us to any channels and
        patterns we were previously listening to
        """
        if not len(args) == 0:
            command_name = args[0]
        else:
            command_name = None

        # Start timing for observability
        start_time = time.monotonic()
        # Track actual retry attempts for error reporting
        actual_retry_attempts = 0

        def failure_callback(error, failure_count):
            if is_debug_log_enabled():
                add_debug_log_for_operation_failure(conn, error, args)
            nonlocal actual_retry_attempts
            actual_retry_attempts = failure_count
            return self._reconnect(conn, error, failure_count, start_time, command_name)

        try:
            response = await conn.retry.call_with_retry(
                lambda: command(*args, **kwargs),
                failure_callback,
                with_failure_count=True,
            )

            if command_name:
                await record_operation_duration(
                    command_name=command_name,
                    duration_seconds=time.monotonic() - start_time,
                    server_address=getattr(conn, "host", None),
                    server_port=getattr(conn, "port", None),
                    db_namespace=str(conn.db),
                )

            return response
        except Exception as e:
            await record_error_count(
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                network_peer_address=getattr(conn, "host", None),
                network_peer_port=getattr(conn, "port", None),
                error_type=e,
                retry_attempts=actual_retry_attempts,
                is_internal=False,
            )
            raise

    async def parse_response(self, block: bool = True, timeout: float = 0):
        """
        Parse the response from a publish/subscribe command.

        Args:
            block: If True, block indefinitely until a message is available.
                   If False, return immediately if no message is available.
                   Default: True
            timeout: The timeout in seconds for reading a response when block=False.
                     This parameter is ignored when block=True.
                     Default: 0 (return immediately if no data available)

        Returns:
            The parsed response from the server, or None if no message is available
            within the timeout period (when block=False).

        Important:
            The block and timeout parameters work together:
            - When block=True: timeout is IGNORED, method blocks indefinitely
            - When block=False: timeout is USED, method returns after timeout expires

            Typically, you should use get_message(timeout=X) instead of calling
            parse_response() directly. The get_message() method automatically sets
            block=False when a timeout is provided, and block=True when timeout=None.

        Example:
            # Block indefinitely (timeout is ignored)
            response = await pubsub.parse_response(block=True, timeout=0.1)

            # Non-blocking with 0.1 second timeout
            response = await pubsub.parse_response(block=False, timeout=0.1)

            # Non-blocking, return immediately
            response = await pubsub.parse_response(block=False, timeout=0)

            # Recommended: use get_message() instead
            msg = await pubsub.get_message(timeout=0.1)  # automatically sets block=False
            msg = await pubsub.get_message(timeout=None)  # automatically sets block=True
        """
        conn = self.connection
        if conn is None:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
            )

        await self.check_health()

        if not conn.is_connected:
            await conn.connect()

        # Block=True: signal "no timeout" to conn.read_response via
        # math.inf. The connection treats math.inf as the per-read
        # opt-in for blocking indefinitely without falling back to
        # self.socket_timeout. Reconnect/AUTH/HELLO/resubscribe
        # operations performed by the retry layer continue to honor
        # self.socket_timeout because they do not pass math.inf.
        #
        # TODO(next-major): when the async Connection.read_response
        # default for ``timeout`` is changed to SENTINEL, passing
        # ``timeout=None`` from this method will become the natural
        # "no timeout" signal and the math.inf hand-off can be
        # removed. That swap is a breaking change to the
        # Connection.read_response signature so it must wait for a
        # major release.
        read_timeout = math.inf if block else timeout
        response = await self._execute(
            conn,
            conn.read_response,
            timeout=read_timeout,
            disconnect_on_error=False,
            push_request=True,
        )

        if conn.health_check_interval and response in self.health_check_response:
            # ignore the health check message as user might not expect it
            return None
        return response

    async def check_health(self):
        conn = self.connection
        if conn is None:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
            )

        if (
            conn.health_check_interval
            and asyncio.get_running_loop().time() > conn.next_health_check
        ):
            await conn.send_command(
                "PING", self.HEALTH_CHECK_MESSAGE, check_health=False
            )

    def _normalize_keys(self, data: _NormalizeKeysT) -> _NormalizeKeysT:
        """
        normalize channel/pattern names to be either bytes or strings
        based on whether responses are automatically decoded. this saves us
        from coercing the value for each message coming in.
        """
        encode = self.encoder.encode
        decode = self.encoder.decode
        return {decode(encode(k)): v for k, v in data.items()}  # type: ignore[return-value]  # noqa: E501

    async def psubscribe(
        self, *args: ChannelT | Subscription, **kwargs: PubSubHandler
    ) -> None:
        """
        Subscribe to channel patterns.
        Patterns supplied as keyword arguments expect a pattern name as the
        key and a callable as the value.
        ``Subscription`` objects can also be supplied positionally with an
        optional handler.
        A pattern's callable will be invoked automatically
        when a message is received on that pattern rather than producing a
        message via ``listen()``.
        """
        new_patterns = parse_pubsub_subscriptions(args, kwargs)
        ret_val = await self.execute_command("PSUBSCRIBE", *new_patterns.keys())
        # update the patterns dict AFTER we send the command. we don't want to
        # subscribe twice to these patterns, once for the command and again
        # for the reconnection.
        new_patterns = self._normalize_keys(new_patterns)
        self.patterns.update(new_patterns)
        self.pending_unsubscribe_patterns.difference_update(new_patterns)
        return ret_val

    def punsubscribe(self, *args: ChannelT) -> Awaitable:
        """
        Unsubscribe from the supplied patterns. If empty, unsubscribe from
        all patterns.
        """
        patterns: Iterable[ChannelT]
        if args:
            parsed_args = list_or_args(args[0], args[1:])
            patterns = self._normalize_keys(dict.fromkeys(parsed_args)).keys()
        else:
            parsed_args = []
            patterns = self.patterns
        self.pending_unsubscribe_patterns.update(patterns)
        return self.execute_command("PUNSUBSCRIBE", *parsed_args)

    async def subscribe(
        self, *args: ChannelT | Subscription, **kwargs: PubSubHandler
    ) -> None:
        """
        Subscribe to channels.
        Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value.
        ``Subscription`` objects can also be supplied positionally with an
        optional handler.
        A channel's callable will be invoked automatically
        when a message is received on that channel rather than producing a
        message via ``listen()`` or ``get_message()``.
        """
        new_channels = parse_pubsub_subscriptions(args, kwargs)
        ret_val = await self.execute_command("SUBSCRIBE", *new_channels.keys())
        # update the channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        new_channels = self._normalize_keys(new_channels)
        self.channels.update(new_channels)
        self.pending_unsubscribe_channels.difference_update(new_channels)
        return ret_val

    def unsubscribe(self, *args) -> Awaitable:
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        if args:
            parsed_args = list_or_args(args[0], args[1:])
            channels = self._normalize_keys(dict.fromkeys(parsed_args))
        else:
            parsed_args = []
            channels = self.channels
        self.pending_unsubscribe_channels.update(channels)
        return self.execute_command("UNSUBSCRIBE", *parsed_args)

    async def ssubscribe(
        self,
        *args: ChannelT | Subscription,
        target_node: Any = None,
        **kwargs: PubSubHandler,
    ) -> None:
        """
        Subscribes the client to the specified shard channels.
        Channels supplied as keyword arguments expect a channel name as the key
        and a callable as the value.
        ``Subscription`` objects can also be supplied positionally
        with an optional handler.
        A channel's callable will be invoked automatically when a message
        is received on that channel rather than producing a message
        via ``listen()`` or ``get_sharded_message()``.
        """
        new_s_channels = parse_pubsub_subscriptions(args, kwargs)
        ret_val = await self.execute_command("SSUBSCRIBE", *new_s_channels.keys())
        # update the s_channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        new_s_channels = self._normalize_keys(new_s_channels)
        self.shard_channels.update(new_s_channels)
        self.pending_unsubscribe_shard_channels.difference_update(new_s_channels)
        return ret_val

    def sunsubscribe(self, *args, target_node=None) -> Awaitable:
        """
        Unsubscribe from the supplied shard_channels. If empty, unsubscribe from
        all shard_channels
        """
        if args:
            args = list_or_args(args[0], args[1:])
            s_channels = self._normalize_keys(dict.fromkeys(args))
        else:
            s_channels = self.shard_channels
        self.pending_unsubscribe_shard_channels.update(s_channels)
        return self.execute_command("SUNSUBSCRIBE", *args)

    async def listen(self) -> AsyncIterator:
        """Listen for messages on channels this client has been subscribed to.

        Iteration ends once every channel and pattern has been unsubscribed
        from. If nothing is subscribed when iteration begins it ends
        immediately rather than waiting, so subscribe first: a listener
        started before any subscription finishes without yielding anything.
        """
        while self.subscribed:
            response = await self.handle_message(await self.parse_response(block=True))
            if response is not None:
                yield response

    async def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: Optional[float] = 0.0
    ):
        """
        Get the next message if one is available, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number or None to wait indefinitely.
        """
        response = await self.parse_response(block=(timeout is None), timeout=timeout)
        if response:
            return await self.handle_message(response, ignore_subscribe_messages)
        return None

    get_sharded_message = get_message

    def ping(self, message=None) -> Awaitable[bool]:
        """
        Ping the Redis server to test connectivity.

        Sends a PING command to the Redis server and returns True if the server
        responds with "PONG".
        """
        args = ["PING", message] if message is not None else ["PING"]
        return self.execute_command(*args)

    async def handle_message(self, response, ignore_subscribe_messages=False):
        """
        Parses a pub/sub message. If the channel or pattern was subscribed to
        with a message handler, the handler is invoked instead of a parsed
        message being returned.
        """
        if response is None:
            return None
        if isinstance(response, bytes):
            response = [b"pong", response] if response != b"PONG" else [b"pong", b""]
        elif isinstance(response, str):
            response = ["pong", response] if response != "PONG" else ["pong", ""]
        message_type = str_if_bytes(response[0])
        if message_type == "pmessage":
            message = {
                "type": message_type,
                "pattern": response[1],
                "channel": response[2],
                "data": response[3],
            }
        elif message_type == "pong":
            message = {
                "type": message_type,
                "pattern": None,
                "channel": None,
                "data": response[1],
            }
        else:
            message = {
                "type": message_type,
                "pattern": None,
                "channel": response[1],
                "data": response[2],
            }

        if message_type in ["message", "pmessage"]:
            channel = str_if_bytes(message["channel"])
            await record_pubsub_message(
                direction=PubSubDirection.RECEIVE,
                channel=channel,
            )
        elif message_type == "smessage":
            channel = str_if_bytes(message["channel"])
            await record_pubsub_message(
                direction=PubSubDirection.RECEIVE,
                channel=channel,
                sharded=True,
            )

        # if this is an unsubscribe message, remove it from memory.
        # ``discard`` rather than ``remove``: the guard above already makes the
        # removal conditional, so the two are equivalent for a single caller -
        # but another writer can drop the same entry between the check and the
        # removal, and ``remove`` would then raise ``KeyError`` out of a pubsub
        # read that no caller catches. ``ClusterPubSub._detach_shard_channel``
        # is such a writer: it forgets a migrating shard channel locally,
        # deliberately without the per-node I/O lock this bookkeeping runs
        # under, because waiting for that lock stalls reconciliation behind a
        # poll's whole retry budget on the node being migrated away from.
        if message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            if message_type == "punsubscribe":
                pattern = response[1]
                if pattern in self.pending_unsubscribe_patterns:
                    self.pending_unsubscribe_patterns.discard(pattern)
                    self.patterns.pop(pattern, None)
            elif message_type == "sunsubscribe":
                s_channel = response[1]
                if s_channel in self.pending_unsubscribe_shard_channels:
                    self.shard_channels.pop(s_channel, None)
                    self.pending_unsubscribe_shard_channels.discard(s_channel)
            else:
                channel = response[1]
                if channel in self.pending_unsubscribe_channels:
                    self.pending_unsubscribe_channels.discard(channel)
                    self.channels.pop(channel, None)

        if message_type in self.PUBLISH_MESSAGE_TYPES:
            # if there's a message handler, invoke it
            if message_type == "pmessage":
                handler = self.patterns.get(message["pattern"], None)
            elif message_type == "smessage":
                handler = self.shard_channels.get(message["channel"], None)
            else:
                handler = self.channels.get(message["channel"], None)
            if handler:
                if inspect.iscoroutinefunction(handler):
                    await handler(message)
                else:
                    handler(message)
                return None
        elif message_type != "pong":
            # this is a subscribe/unsubscribe message. ignore if we don't
            # want them
            if ignore_subscribe_messages or self.ignore_subscribe_messages:
                return None

        return message

    async def run(
        self,
        *,
        exception_handler: Optional["PSWorkerThreadExcHandlerT"] = None,
        poll_timeout: float = 1.0,
        pubsub=None,
    ) -> None:
        """Process pub/sub messages using registered callbacks.

        This is the equivalent of :py:meth:`redis.PubSub.run_in_thread` in
        redis-py, but it is a coroutine. To launch it as a separate task, use
        ``asyncio.create_task``:

            >>> task = asyncio.create_task(pubsub.run())

        To shut it down, use asyncio cancellation:

            >>> task.cancel()
            >>> await task
        """
        for channel, handler in self.channels.items():
            if handler is None:
                raise PubSubError(f"Channel: '{channel}' has no handler registered")
        for pattern, handler in self.patterns.items():
            if handler is None:
                raise PubSubError(f"Pattern: '{pattern}' has no handler registered")
        for s_channel, handler in self.shard_channels.items():
            if handler is None:
                raise PubSubError(
                    f"Shard Channel: '{s_channel}' has no handler registered"
                )

        await self.connect()
        while True:
            try:
                if pubsub is None:
                    await self.get_message(
                        ignore_subscribe_messages=True, timeout=poll_timeout
                    )
                else:
                    await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=poll_timeout
                    )
            except asyncio.CancelledError:
                raise
            except BaseException as e:
                if exception_handler is None:
                    raise
                res = exception_handler(e, self)
                if inspect.isawaitable(res):
                    await res
            # Ensure that other tasks on the event loop get a chance to run
            # if we didn't have to block for I/O anywhere.
            await asyncio.sleep(0)


class PubsubWorkerExceptionHandler(Protocol):
    def __call__(self, e: BaseException, pubsub: PubSub): ...


class AsyncPubsubWorkerExceptionHandler(Protocol):
    async def __call__(self, e: BaseException, pubsub: PubSub): ...


PSWorkerThreadExcHandlerT = Union[
    PubsubWorkerExceptionHandler, AsyncPubsubWorkerExceptionHandler
]


CommandT = Tuple[Tuple[Union[str, bytes], ...], Mapping[str, Any]]
CommandStackT = List[CommandT]


class Pipeline(Redis):  # lgtm [py/init-calls-subclass]
    """
    Pipelines provide a way to transmit multiple commands to the Redis server
    in one transmission.  This is convenient for batch processing, such as
    saving all the values in a list to Redis.

    All commands executed within a pipeline(when running in transactional mode,
    which is the default behavior) are wrapped with MULTI and EXEC
    calls. This guarantees all commands executed in the pipeline will be
    executed atomically.

    Any command raising an exception does *not* halt the execution of
    subsequent commands in the pipeline. Instead, the exception is caught
    and its instance is placed into the response list returned by execute().
    Code iterating over the response list should be able to deal with an
    instance of an exception as a potential value. In general, these will be
    ResponseError exceptions, such as those raised when issuing a command
    on a key of a different datatype.
    """

    UNWATCH_COMMANDS = {"DISCARD", "EXEC", "UNWATCH"}

    def __init__(
        self,
        connection_pool: ConnectionPool,
        response_callbacks: MutableMapping[Union[str, bytes], ResponseCallbackT],
        transaction: bool,
        shard_hint: Optional[str],
    ):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        self.is_transaction = transaction
        self.shard_hint = shard_hint
        self.watching = False
        self.command_stack: CommandStackT = []
        self.scripts: Set[Script] = set()
        self.explicit_transaction = False

    async def __aenter__(self: _RedisT) -> _RedisT:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.reset()

    def __await__(self):
        return self._async_self().__await__()

    _DEL_MESSAGE = "Unclosed Pipeline client"

    def __len__(self):
        return len(self.command_stack)

    def __bool__(self):
        """Pipeline instances should always evaluate to True"""
        return True

    async def _async_self(self):
        return self

    async def reset(self):
        self.command_stack = []
        self.scripts = set()
        try:
            # make sure to reset the connection state in the event that we were
            # watching something
            if self.watching and self.connection:
                try:
                    # call this manually since our unwatch or
                    # immediate_execute_command methods can call reset()
                    await self.connection.send_command("UNWATCH")
                    await self.connection.read_response()
                except ConnectionError:
                    # disconnect will also remove any previous WATCHes
                    if self.connection:
                        await self.connection.disconnect()
                except asyncio.CancelledError:
                    # Disconnect so any unread UNWATCH reply does not get
                    # served to the next caller that takes the connection.
                    if self.connection:
                        await self.connection.disconnect()
                    raise
        finally:
            self.watching = False
            self.explicit_transaction = False
            # We can safely return the connection to the pool here since we're
            # sure we're no longer WATCHing anything. Detach self.connection
            # before awaiting release: if a second cancel aborts the await,
            # the pipeline must not be left holding a reference to a
            # connection that is being returned to the pool. Shield the
            # release itself so a second cancel cannot split the pool's
            # internal in-use/available bookkeeping mid-update.
            if self.connection:
                connection, self.connection = self.connection, None
                await asyncio.shield(self.connection_pool.release(connection))

    async def aclose(self) -> None:
        """Alias for reset(), a standard method name for cleanup"""
        await self.reset()

    def multi(self):
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        if self.explicit_transaction:
            raise RedisError("Cannot issue nested calls to MULTI")
        if self.command_stack:
            raise RedisError(
                "Commands without an initial WATCH have already been issued"
            )
        self.explicit_transaction = True

    def execute_command(
        self, *args, **kwargs
    ) -> Union["Pipeline", Awaitable["Pipeline"]]:
        if (self.watching or args[0] == "WATCH") and not self.explicit_transaction:
            return self.immediate_execute_command(*args, **kwargs)
        return self.pipeline_execute_command(*args, **kwargs)

    async def _disconnect_reset_raise_on_watching(
        self,
        conn: Connection,
        error: Exception,
        failure_count: Optional[int] = None,
        start_time: Optional[float] = None,
        command_name: Optional[str] = None,
    ) -> None:
        """
        Close the connection reset watching state and
        raise an exception if we were watching.

        The supported exceptions are already checked in the
        retry object so we don't need to do it here.

        After we disconnect the connection, it will try to reconnect and
        do a health check as part of the send_command logic(on connection level).
        """
        if (
            error
            and failure_count is not None
            and failure_count <= conn.retry.get_retries()
        ):
            await record_operation_duration(
                command_name=command_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
                error=error,
                retry_attempts=failure_count,
            )
        await conn.disconnect(error=error, failure_count=failure_count)
        # if we were already watching a variable, the watch is no longer
        # valid since this connection has died. raise a WatchError, which
        # indicates the user should retry this transaction.
        if self.watching:
            await self.reset()
            raise WatchError(
                f"A {type(error).__name__} occurred while watching one or more keys"
            )

    async def immediate_execute_command(self, *args, **options):
        """
        Execute a command immediately, but don't auto-retry on the supported
        errors for retry if we're already WATCHing a variable.
        Used when issuing WATCH or subsequent commands retrieving their values but before
        MULTI is called.
        """
        command_name = args[0]
        conn = self.connection
        # if this is the first call, we need a connection
        if not conn:
            conn = await self.connection_pool.get_connection()
            self.connection = conn

        # Start timing for observability
        start_time = time.monotonic()
        # Track actual retry attempts for error reporting
        actual_retry_attempts = 0

        def failure_callback(error, failure_count):
            if is_debug_log_enabled():
                add_debug_log_for_operation_failure(conn, error, args)
            nonlocal actual_retry_attempts
            actual_retry_attempts = failure_count
            return self._disconnect_reset_raise_on_watching(
                conn, error, failure_count, start_time, command_name
            )

        try:
            response = await conn.retry.call_with_retry(
                lambda: self._send_command_parse_response(
                    conn, command_name, *args, **options
                ),
                failure_callback,
                with_failure_count=True,
            )

            await record_operation_duration(
                command_name=command_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
            )

            return response
        except Exception as e:
            await record_error_count(
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                network_peer_address=getattr(conn, "host", None),
                network_peer_port=getattr(conn, "port", None),
                error_type=e,
                retry_attempts=actual_retry_attempts,
                is_internal=False,
            )
            raise

    def pipeline_execute_command(self, *args, **options):
        """
        Stage a command to be executed when execute() is next called

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.
        """
        self.command_stack.append((args, options))
        return self

    async def _himport_prepare_pipeline(self, conn, commands):
        """Delegate to the shared async HIMPORT executor."""
        await _himport_exec.prepare_pipeline(self, conn, [args for args, _ in commands])

    async def _execute_transaction(  # noqa: C901
        self, connection: Connection, commands: CommandStackT, raise_on_error
    ):
        # Ensure fieldsets referenced by buffered HIMPORT SETs are prepared on this
        # connection before the MULTI/EXEC block (session state, not transactional).
        await self._himport_prepare_pipeline(connection, commands)
        pre: CommandT = (("MULTI",), {})
        post: CommandT = (("EXEC",), {})
        cmds = (pre, *commands, post)
        all_cmds = connection.pack_commands(
            args for args, options in cmds if EMPTY_RESPONSE not in options
        )
        await connection.send_packed_command(all_cmds)
        errors = []

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            await self.parse_response(connection, "_")
        except ResponseError as err:
            errors.append((0, err))

        # and all the other commands
        for i, command in enumerate(commands):
            if EMPTY_RESPONSE in command[1]:
                errors.append((i, command[1][EMPTY_RESPONSE]))
            else:
                try:
                    await self.parse_response(connection, "_")
                except ResponseError as err:
                    self.annotate_exception(err, i + 1, command[0])
                    errors.append((i, err))

        # parse the EXEC.
        never_decode = _transaction_never_decode(self.get_connection_kwargs(), commands)
        try:
            if never_decode:
                response = await self.parse_response(
                    connection, "_", **{NEVER_DECODE: []}
                )
            else:
                response = await self.parse_response(connection, "_")
        except ExecAbortError as err:
            if errors:
                raise errors[0][1] from err
            raise

        # EXEC clears any watched keys
        self.watching = False

        if response is None:
            raise WatchError("Watched variable changed.") from None

        # put any parse errors into the response
        for i, e in errors:
            response.insert(i, e)

        if len(response) != len(commands):
            if self.connection:
                await self.connection.disconnect()
            raise ResponseError(
                "Wrong number of response items from pipeline execution"
            ) from None

        if never_decode:
            response = _decode_transaction_replies(
                self.get_encoder(), response, commands
            )

        # find any errors in the response and raise if necessary
        if raise_on_error:
            self.raise_first_error(commands, response)

        # We have to run response callbacks manually
        data = []
        for r, cmd in zip(response, commands):
            if not isinstance(r, Exception):
                args, options = cmd
                command_name = args[0]

                # Remove keys entry, it needs only for cache.
                options.pop("keys", None)

                if command_name in self.response_callbacks:
                    r = self.response_callbacks[command_name](r, **options)
                    if inspect.isawaitable(r):
                        r = await r
            data.append(r)
        return data

    async def _execute_pipeline(
        self, connection: Connection, commands: CommandStackT, raise_on_error: bool
    ):
        # Fold any first-use HIMPORT PREPAREs for referenced fieldsets into the same
        # packed write as the queued commands, so a pipeline that lands on a fresh or
        # reconnected connection stays a single round trip (the batched write bypasses
        # the per-command lazy PREPARE path). Deferred-discard reconciliation happens
        # inside pipeline_prepares and only touches the socket when discards are
        # actually pending.
        fieldsets = await _himport_exec.pipeline_prepares(
            self, connection, [args for args, _ in commands]
        )
        preflight = _himport_exec.prepare_wire_commands(fieldsets)
        # build up all commands into a single request to increase network perf
        all_cmds = connection.pack_commands(preflight + [args for args, _ in commands])
        await connection.send_packed_command(all_cmds)

        # Drain the leading PREPARE replies (bookkeeping + capture the first error)
        # before the queued replies. Everything on the wire is read before raising so
        # the pooled socket never desyncs.
        prep_error = await _himport_exec.drain_pipeline_prepares(
            self, connection, fieldsets
        )

        response = []
        try:
            for args, options in commands:
                try:
                    response.append(
                        await self.parse_response(connection, args[0], **options)
                    )
                except ResponseError as e:
                    response.append(e)
        except BaseException:
            # A response callback raised, so the replies of the rest of the batch
            # are still on the socket. Drop the connection rather than hand them to
            # the next caller that borrows it from the pool.
            await connection.disconnect(nowait=True)
            raise

        # A PREPARE failure (rare: an invalid fieldset definition) is a hard error,
        # raised regardless of raise_on_error as it was before folding -- only now
        # every reply has already been drained.
        if prep_error is not None:
            raise prep_error
        if raise_on_error:
            self.raise_first_error(commands, response)
        return response

    def raise_first_error(self, commands: CommandStackT, response: Iterable[Any]):
        for i, r in enumerate(response):
            if isinstance(r, ResponseError):
                self.annotate_exception(r, i + 1, commands[i][0])
                raise r

    def annotate_exception(
        self, exception: Exception, number: int, command: Iterable[object]
    ) -> None:
        cmd = command_text(command)
        msg = (
            f"Command # {number} ({truncate_text(cmd)}) "
            f"of pipeline caused error: {exception.args[0]}"
        )
        exception.args = (msg,) + exception.args[1:]

    async def parse_response(
        self, connection: Connection, command_name: Union[str, bytes], **options
    ):
        result = await super().parse_response(connection, command_name, **options)
        if command_name in self.UNWATCH_COMMANDS:
            self.watching = False
        elif command_name == "WATCH":
            self.watching = True
        return result

    async def load_scripts(self):
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        # we can't use the normal script_* methods because they would just
        # get buffered in the pipeline.
        exists = await immediate("SCRIPT EXISTS", *shas)
        if not all(exists):
            for s, exist in zip(scripts, exists):
                if not exist:
                    s.sha = await immediate("SCRIPT LOAD", s.script)

    async def _disconnect_raise_on_watching(
        self,
        conn: Connection,
        error: Exception,
        failure_count: Optional[int] = None,
        start_time: Optional[float] = None,
        command_name: Optional[str] = None,
    ):
        """
        Close the connection, raise an exception if we were watching.

        The supported exceptions are already checked in the
        retry object so we don't need to do it here.

        After we disconnect the connection, it will try to reconnect and
        do a health check as part of the send_command logic(on connection level).
        """
        if (
            error
            and failure_count is not None
            and failure_count <= conn.retry.get_retries()
        ):
            await record_operation_duration(
                command_name=command_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
                error=error,
                retry_attempts=failure_count,
            )
        await conn.disconnect(error=error, failure_count=failure_count)
        # if we were watching a variable, the watch is no longer valid
        # since this connection has died. raise a WatchError, which
        # indicates the user should retry this transaction.
        if self.watching:
            raise WatchError(
                f"A {type(error).__name__} occurred while watching one or more keys"
            )

    async def execute(self, raise_on_error: bool = True) -> List[Any]:
        """Execute all the commands in the current pipeline"""
        stack = self.command_stack
        if not stack and not self.watching:
            return []
        if self.scripts:
            await self.load_scripts()
        if self.is_transaction or self.explicit_transaction:
            execute = self._execute_transaction
            operation_name = "MULTI"
        else:
            execute = self._execute_pipeline
            operation_name = "PIPELINE"

        conn = self.connection
        if not conn:
            conn = await self.connection_pool.get_connection()
            # assign to self.connection so reset() releases the connection
            # back to the pool after we're done
            self.connection = conn
        conn = cast(Connection, conn)

        # Start timing for observability
        start_time = time.monotonic()
        # Track actual retry attempts for error reporting
        actual_retry_attempts = 0

        def failure_callback(error, failure_count):
            if is_debug_log_enabled():
                add_debug_log_for_operation_failure(conn, error, (operation_name,))
            nonlocal actual_retry_attempts
            actual_retry_attempts = failure_count
            return self._disconnect_raise_on_watching(
                conn, error, failure_count, start_time, operation_name
            )

        try:
            response = await conn.retry.call_with_retry(
                lambda: execute(conn, stack, raise_on_error),
                failure_callback,
                with_failure_count=True,
            )

            await record_operation_duration(
                command_name=operation_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
            )
            return response
        except Exception as e:
            await record_error_count(
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                network_peer_address=getattr(conn, "host", None),
                network_peer_port=getattr(conn, "port", None),
                error_type=e,
                retry_attempts=actual_retry_attempts,
                is_internal=False,
            )
            raise
        finally:
            await self.reset()

    async def discard(self):
        """Flushes all previously queued commands
        See: https://redis.io/commands/DISCARD
        """
        await self.execute_command("DISCARD")

    async def watch(self, *names: KeyT):
        """Watches the values at keys ``names``"""
        if self.explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")
        return await self.execute_command("WATCH", *names)

    async def unwatch(self):
        """Unwatches all previously specified keys"""
        return self.watching and await self.execute_command("UNWATCH") or True
