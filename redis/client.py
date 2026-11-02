import copy
import logging
import re
import threading
import time
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Set,
    Type,
    Union,
)

from redis import _himport_exec
from redis._defaults import (
    DEFAULT_RETRY_BASE,
    DEFAULT_RETRY_CAP,
    DEFAULT_RETRY_COUNT,
    DEFAULT_SOCKET_CONNECT_TIMEOUT,
    DEFAULT_SOCKET_READ_SIZE,
    DEFAULT_SOCKET_TIMEOUT,
)
from redis._parsers.encoders import Encoder
from redis._parsers.helpers import bool_ok, decode_info, get_response_callbacks
from redis.backoff import ExponentialWithJitterBackoff
from redis.cache import CacheConfig, CacheInterface
from redis.commands import (
    CoreCommands,
    RedisModuleCommands,
    SentinelCommands,
    list_or_args,
)
from redis.commands.core import Script
from redis.commands.helpers import parse_pubsub_subscriptions, pubsub_subscription_args
from redis.commands.metadata import MetadataResolver
from redis.connection import (
    AbstractConnection,
    Connection,
    ConnectionPool,
    SSLConnection,
    UnixDomainSocketConnection,
)
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
from redis.lock import Lock
from redis.maint_notifications import (
    MaintNotificationsConfig,
    OSSMaintNotificationsHandler,
)
from redis.observability.attributes import PubSubDirection
from redis.observability.recorder import (
    record_error_count,
    record_operation_duration,
    record_pubsub_message,
)
from redis.retry import Retry
from redis.typing import (
    ChannelT,
    FieldT,
    PubSubHandler,
    Subscription,
)
from redis.utils import (
    SENTINEL,
    _set_info_logger,
    check_protocol_version,
    command_text,
    deprecated_args,
    experimental_method,
    safe_str,
    str_if_bytes,
    truncate_text,
)

if TYPE_CHECKING:
    import ssl

    import OpenSSL

    from redis.keyspace_notifications import KeyspaceNotifications

SYM_EMPTY = b""
EMPTY_RESPONSE = "EMPTY_RESPONSE"

# some responses (ie. dump) are binary, and just meant to never be decoded
NEVER_DECODE = "NEVER_DECODE"


def _decode_nested(encoder, value):
    """Decode every bytes value in a parsed reply, as the parser would have."""
    if isinstance(value, (bytes, memoryview)):
        return encoder.decode(value)
    if isinstance(value, list):
        return [_decode_nested(encoder, item) for item in value]
    if isinstance(value, dict):
        return {
            _decode_nested(encoder, k): _decode_nested(encoder, v)
            for k, v in value.items()
        }
    return value


# Transactional pipelines have always returned FT.SEARCH and FT.HYBRID replies
# decoded, so their NEVER_DECODE option is not applied inside MULTI/EXEC.
_TRANSACTION_DECODED_COMMANDS = frozenset(("FT.SEARCH", "FT.HYBRID"))


def _keeps_raw_reply(args, options) -> bool:
    return NEVER_DECODE in options and args[0] not in _TRANSACTION_DECODED_COMMANDS


def _transaction_never_decode(connection_kwargs, commands) -> bool:
    """
    Whether the EXEC reply must be read without decoding.

    EXEC returns the replies of all queued commands as one array, so the
    per-command ``NEVER_DECODE`` option cannot be applied while parsing it.
    When a queued command needs raw bytes, EXEC is read undecoded and the
    other replies are decoded afterwards with ``_decode_transaction_replies``.
    ``commands`` yields ``(args, options)`` pairs. They are only scanned when
    the client decodes responses.
    """
    return bool(connection_kwargs.get("decode_responses", False)) and any(
        _keeps_raw_reply(args, options) for args, options in commands
    )


def _decode_transaction_replies(encoder, response, commands):
    """Decode the EXEC replies of the commands that do not keep raw replies."""
    return [
        r
        if isinstance(r, Exception) or _keeps_raw_reply(args, options)
        else _decode_nested(encoder, r)
        for r, (args, options) in zip(response, commands)
    ]


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


class CaseInsensitiveDict(dict):
    "Case insensitive dict implementation. Assumes string keys only."

    def __init__(self, data: Dict[str, str]) -> None:
        for k, v in data.items():
            self[k.upper()] = v

    def __contains__(self, k):
        return super().__contains__(k.upper())

    def __delitem__(self, k):
        super().__delitem__(k.upper())

    def __getitem__(self, k):
        return super().__getitem__(k.upper())

    def get(self, k, default=None):
        return super().get(k.upper(), default)

    def __setitem__(self, k, v):
        super().__setitem__(k.upper(), v)

    def update(self, data):
        data = CaseInsensitiveDict(data)
        super().update(data)


class AbstractRedis:
    pass


class Redis(RedisModuleCommands, CoreCommands, SentinelCommands):
    """
    Implementation of the Redis protocol.

    This abstract class provides a Python interface to all Redis commands
    and an implementation of the Redis protocol.

    Pipelines derive from this, implementing how
    the commands are sent and received to the Redis server. Based on
    configuration, an instance will either use a ConnectionPool, or
    Connection object to talk to redis.

    It is not safe to pass PubSub or Pipeline objects between threads.
    """

    # Type discrimination marker for @overload self-type pattern
    _is_async_client: Literal[False] = False

    @classmethod
    def from_url(cls, url: str, **kwargs) -> "Redis":
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
        single_connection_client = kwargs.pop("single_connection_client", False)
        connection_pool = ConnectionPool.from_url(url, **kwargs)
        client = cls(
            connection_pool=connection_pool,
            single_connection_client=single_connection_client,
        )
        client.auto_close_connection_pool = True
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
        via ``from_pool`` -- for example one per request across threads -- is
        not thread safe: when one client is closed it will disconnect
        connections still in use by the others.

        To share a single pool across clients, construct the pool explicitly
        and manage its lifecycle instead. Unlike ``from_pool``, the plain
        ``Redis(connection_pool=pool)`` constructor does not take ownership of
        the pool and will not close it, so a pool created this way can be
        safely shared across clients. ``ConnectionPool`` supports the context
        manager protocol for this::

            with ConnectionPool.from_url(url) as pool:
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
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
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
        retry_on_error: List[Type[Exception]] | None = None,
        ssl: bool = False,
        ssl_keyfile: str | None = None,
        ssl_certfile: str | None = None,
        ssl_cert_reqs: "str | ssl.VerifyMode" = "required",
        ssl_include_verify_flags: List["ssl.VerifyFlags"] | None = None,
        ssl_exclude_verify_flags: List["ssl.VerifyFlags"] | None = None,
        ssl_ca_certs: str | None = None,
        ssl_ca_path: str | None = None,
        ssl_ca_data: str | None = None,
        ssl_check_hostname: bool = True,
        ssl_password: str | None = None,
        ssl_validate_ocsp: bool = False,
        ssl_validate_ocsp_stapled: bool = False,
        ssl_ocsp_context: "OpenSSL.SSL.Context | None" = None,
        ssl_ocsp_expected_cert: str | None = None,
        ssl_min_version: "ssl.TLSVersion | None" = None,
        ssl_ciphers: str | None = None,
        max_connections: int | None = None,
        single_connection_client: bool = False,
        health_check_interval: int = 0,
        client_name: str | None = None,
        lib_name: str | object | None = SENTINEL,
        lib_version: str | object | None = SENTINEL,
        driver_info: DriverInfo | object | None = SENTINEL,
        username: str | None = None,
        redis_connect_func: Callable[[], None] | None = None,
        credential_provider: CredentialProvider | None = None,
        protocol: int | None = None,
        legacy_responses: bool = True,
        cache: CacheInterface | None = None,
        cache_config: CacheConfig | None = None,
        event_dispatcher: EventDispatcher | None = None,
        maint_notifications_config: MaintNotificationsConfig | None = None,
        oss_cluster_maint_notifications_handler: OSSMaintNotificationsHandler
        | None = None,
        metadata_resolver: MetadataResolver | None = None,
    ) -> None:
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
        single_connection_client:
            if `True`, connection pool is not used. In that case `Redis`
            instance use is not thread safe.
        decode_responses:
            if `True`, the response will be decoded to utf-8.
            Argument is ignored when connection_pool is provided.
        driver_info:
            Optional DriverInfo object to identify upstream libraries.
            If provided, lib_name and lib_version are ignored.
            If not provided, a DriverInfo will be created from lib_name and lib_version.
            Explicit None disables CLIENT SETINFO.
            Argument is ignored when connection_pool is provided.
        lib_name:
            **Deprecated.** Use driver_info instead. Library name for CLIENT SETINFO.
        lib_version:
            **Deprecated.** Use driver_info instead. Library version for CLIENT SETINFO.
        maint_notifications_config:
            configures the pool to support maintenance notifications - see
            `redis.maint_notifications.MaintNotificationsConfig` for details.
            Only supported with RESP3
            If not provided and protocol is RESP3, the maintenance notifications
            will be enabled by default (logic is included in the connection pool
            initialization).
            Argument is ignored when connection_pool is provided.
        oss_cluster_maint_notifications_handler:
            handler for OSS cluster notifications - see
            `redis.maint_notifications.OSSMaintNotificationsHandler` for details.
            Only supported with RESP3
            Argument is ignored when connection_pool is provided.
        metadata_resolver:
            decides which commands are eligible for client-side caching - see
            `redis.commands.metadata.MetadataResolver`. Defaults to the static command
            metadata this library ships. Resolvers chain through `with_fallback`, first match
            wins, so one placed in front of `StaticMetadataResolver` overrides the commands it
            carries while the static records answer for everything else.
            The library never reads `COMMAND` on its own behalf for this, so the default adds
            no round trips. To decide eligibility from the connected server, pass a
            `DynamicMetadataResolver` built from a live `COMMAND` reply - use it with care,
            because reading that reply relies on a class in the private `redis._parsers`
            package.
            Argument is ignored when connection_pool is provided - configure it on the pool
            instead.
        """
        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher
        if not connection_pool:
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
                "credential_provider": credential_provider,
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
                            "ssl_check_hostname": ssl_check_hostname,
                            "ssl_password": ssl_password,
                            "ssl_ca_path": ssl_ca_path,
                            "ssl_validate_ocsp_stapled": ssl_validate_ocsp_stapled,
                            "ssl_validate_ocsp": ssl_validate_ocsp,
                            "ssl_ocsp_context": ssl_ocsp_context,
                            "ssl_ocsp_expected_cert": ssl_ocsp_expected_cert,
                            "ssl_min_version": ssl_min_version,
                            "ssl_ciphers": ssl_ciphers,
                        }
                    )
                maint_notifications_enabled = (
                    maint_notifications_config and maint_notifications_config.enabled
                )
                if maint_notifications_enabled and not check_protocol_version(
                    protocol, 3
                ):
                    raise RedisError(
                        "Maintenance notifications handlers on connection are only supported with RESP version 3"
                    )
                if maint_notifications_config:
                    kwargs.update(
                        {
                            "maint_notifications_config": maint_notifications_config,
                        }
                    )
                if oss_cluster_maint_notifications_handler:
                    kwargs.update(
                        {
                            "oss_cluster_maint_notifications_handler": oss_cluster_maint_notifications_handler,
                        }
                    )
            if (cache_config or cache) and check_protocol_version(protocol, 3):
                kwargs.update(
                    {
                        "cache": cache,
                        "cache_config": cache_config,
                    }
                )
            if metadata_resolver is not None:
                # A named ``ConnectionPool`` parameter, not a connection kwarg, so it
                # reaches the pool without also being forwarded to every connection.
                kwargs.update(
                    {
                        "metadata_resolver": metadata_resolver,
                    }
                )
            connection_pool = ConnectionPool(**kwargs)
            self._event_dispatcher.dispatch(
                AfterPooledConnectionsInstantiationEvent(
                    [connection_pool], ClientType.SYNC, credential_provider
                )
            )
            self.auto_close_connection_pool = True
        else:
            self.auto_close_connection_pool = False
            self._event_dispatcher.dispatch(
                AfterPooledConnectionsInstantiationEvent(
                    [connection_pool], ClientType.SYNC, credential_provider
                )
            )

        self.connection_pool = connection_pool

        if (cache_config or cache) and not check_protocol_version(
            self.connection_pool.get_protocol(), 3
        ):
            raise RedisError("Client caching is only supported with RESP version 3")

        self.single_connection_lock = threading.RLock()
        self.connection = None
        self._single_connection_client = single_connection_client
        if self._single_connection_client:
            self.connection = self.connection_pool.get_connection()
            self._event_dispatcher.dispatch(
                AfterSingleConnectionInstantiationEvent(
                    self.connection, ClientType.SYNC, self.single_connection_lock
                )
            )

        connection_kwargs = self.connection_pool.connection_kwargs
        self.response_callbacks = CaseInsensitiveDict(
            get_response_callbacks(
                user_protocol=connection_kwargs.get("protocol"),
                legacy_responses=connection_kwargs.get("legacy_responses", True),
            )
        )

    def __repr__(self) -> str:
        return (
            f"<{type(self).__module__}.{type(self).__name__}"
            f"({repr(self.connection_pool)})>"
        )

    def get_encoder(self) -> "Encoder":
        """Get the connection pool's encoder"""
        return self.connection_pool.get_encoder()

    def get_connection_kwargs(self) -> Dict:
        """Get the connection's key-word arguments"""
        return self.connection_pool.connection_kwargs

    @property
    def himport_registry(self) -> HImportRegistry:
        """The client's HIMPORT fieldset registry (contains empty
        schema registry if none was declared).

        Read-only: the registry is mutated only through the HIMPORT command methods.
        """
        return self.connection_pool.himport_registry

    def get_retry(self) -> Optional[Retry]:
        return self.get_connection_kwargs().get("retry")

    def set_retry(self, retry: Retry) -> None:
        self.get_connection_kwargs().update({"retry": retry})
        self.connection_pool.set_retry(retry)

    def set_response_callback(self, command: str, callback: Callable) -> None:
        """Set a custom Response Callback"""
        self.response_callbacks[command] = callback

    def load_external_module(self, funcname, func) -> None:
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

    def pipeline(self, transaction=True, shard_hint=None) -> "Pipeline":
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

    def transaction(
        self, func: Callable[["Pipeline"], None], *watches, **kwargs
    ) -> Union[List[Any], Any, None]:
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single argument which is a Pipeline object.
        """
        shard_hint = kwargs.pop("shard_hint", None)
        value_from_callable = kwargs.pop("value_from_callable", False)
        watch_delay = kwargs.pop("watch_delay", None)
        with self.pipeline(True, shard_hint) as pipe:
            while True:
                try:
                    if watches:
                        pipe.watch(*watches)
                    func_value = func(pipe)
                    exec_value = pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        time.sleep(watch_delay)
                    continue

    def lock(
        self,
        name: str,
        timeout: Optional[float] = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: Optional[float] = None,
        lock_class: Union[None, Any] = None,
        thread_local: bool = True,
        raise_on_release_error: bool = True,
    ):
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

    def pubsub(self, **kwargs):
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
    ) -> "KeyspaceNotifications":
        """
        Return a :class:`~redis.keyspace_notifications.KeyspaceNotifications`
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
        from redis.keyspace_notifications import KeyspaceNotifications

        return KeyspaceNotifications(
            self,
            key_prefix=key_prefix,
            ignore_subscribe_messages=ignore_subscribe_messages,
        )

    def monitor(self):
        return Monitor(self.connection_pool)

    def client(self):
        return self.__class__(
            connection_pool=self.connection_pool,
            single_connection_client=True,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def close(self) -> None:
        # In case a connection property does not yet exist
        # (due to a crash earlier in the Redis() constructor), return
        # immediately as there is nothing to clean-up.
        if not hasattr(self, "connection"):
            return

        conn = self.connection
        if conn:
            self.connection = None
            self.connection_pool.release(conn)

        if self.auto_close_connection_pool:
            self.connection_pool.close()

    def _send_command_parse_response(self, conn, command_name, *args, **options):
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
            return self._himport_execute_set(conn, key, fieldset_name, values)
        conn.send_command(*args, **options)
        return self.parse_response(conn, command_name, **options)

    def _himport_reconcile_discards(self, conn):
        """Delegate to the shared sync HIMPORT executor."""
        return _himport_exec.reconcile_discards(self, conn)

    def _himport_prepare_and_set(self, conn, key, fieldset_name, values, fieldset):
        """Delegate to the shared sync HIMPORT executor."""
        return _himport_exec.prepare_and_set(
            self, conn, key, fieldset_name, values, fieldset
        )

    def _himport_execute_set(self, conn, key, fieldset_name, values):
        """Delegate to the shared sync HIMPORT executor."""
        return _himport_exec.execute_set(self, conn, key, fieldset_name, values)

    def _close_connection(
        self,
        conn,
        error: Optional[Exception] = None,
        failure_count: Optional[int] = None,
        start_time: Optional[float] = None,
        command_name: Optional[str] = None,
    ) -> None:
        """
        Close the connection before retrying.

        The supported exceptions are already checked in the
        retry object so we don't need to do it here.

        After we disconnect the connection, it will try to reconnect and
        do a health check as part of the send_command logic(on connection level).
        """
        if error and failure_count <= conn.retry.get_retries():
            record_operation_duration(
                command_name=command_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
                error=error,
                retry_attempts=failure_count,
            )

        conn.disconnect()

    # COMMAND EXECUTION AND PROTOCOL PARSING
    def execute_command(self, *args, **options):
        return self._execute_command(*args, **options)

    def _execute_command(self, *args, **options):
        """Execute a command and return a parsed response"""
        pool = self.connection_pool
        command_name = args[0]
        conn = self.connection or pool.get_connection()

        # Start timing for observability
        start_time = time.monotonic()
        # Track actual retry attempts for error reporting
        actual_retry_attempts = [0]

        def failure_callback(error, failure_count):
            if is_debug_log_enabled():
                add_debug_log_for_operation_failure(conn, error, args)
            actual_retry_attempts[0] = failure_count
            self._close_connection(conn, error, failure_count, start_time, command_name)

        if self._single_connection_client:
            self.single_connection_lock.acquire()
        try:
            result = conn.retry.call_with_retry(
                lambda: self._send_command_parse_response(
                    conn, command_name, *args, **options
                ),
                failure_callback,
                with_failure_count=True,
            )

            record_operation_duration(
                command_name=command_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
            )
            return result
        except Exception as e:
            record_error_count(
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                network_peer_address=getattr(conn, "host", None),
                network_peer_port=getattr(conn, "port", None),
                error_type=e,
                retry_attempts=actual_retry_attempts[0],
                is_internal=False,
            )
            raise

        finally:
            try:
                if conn and conn.should_reconnect():
                    self._close_connection(conn)
                    conn.connect()
            finally:
                if self._single_connection_client:
                    self.single_connection_lock.release()
                if not self.connection:
                    pool.release(conn)

    def parse_response(self, connection, command_name, **options):
        """Parses a response from the Redis server"""
        try:
            if NEVER_DECODE in options:
                response = connection.read_response(disable_decoding=True)
                options.pop(NEVER_DECODE)
                if command_name == "INFO":
                    response = decode_info(response, self.get_encoder())
            else:
                response = connection.read_response()
        except ResponseError:
            if EMPTY_RESPONSE in options:
                return options[EMPTY_RESPONSE]
            raise

        if EMPTY_RESPONSE in options:
            options.pop(EMPTY_RESPONSE)

        # Remove keys entry, it needs only for cache.
        options.pop("keys", None)

        if command_name in self.response_callbacks:
            return self.response_callbacks[command_name](response, **options)
        return response

    def get_cache(self) -> Optional[CacheInterface]:
        return self.connection_pool.cache

    # HIMPORT orchestration. The registry lives on the shared HImportRegistry; the
    # server-side effect is applied lazily per connection (PREPARE bundled into the
    # first himport_set; DISCARD reconciled when a connection is next borrowed for a
    # himport_set). The connection carries the per-connection HIMPORT state; a
    # CacheProxyConnection transparently delegates it to the wrapped connection, so
    # this code never needs to know which connection type it holds.

    @experimental_method()
    def himport_prepare(self, fieldset_name: str, fields: Iterable[FieldT]) -> bool:
        """Declare an HIMPORT fieldset for use by :meth:`himport_set`.

        Registers ``fieldset_name`` (ordered ``fields``, verbatim) in the client's
        shared registry. On a pooled client the server-side ``PREPARE`` is deferred
        and bundled into the next ``himport_set`` per connection. On a single
        connection client it is run immediately when the pinned connection is live;
        while that connection is not connected there is no session state to prepare,
        so the next ``himport_set`` prepares it lazily instead.
        """
        fieldset = self.himport_registry.prepare(fieldset_name, fields)
        conn = self.connection
        if self._single_connection_client and conn is not None and conn.is_connected:
            self.himport_prepare_internal(fieldset_name, fieldset.fields)
            conn._himport_prepared[fieldset_name] = fieldset.version
        return True

    @experimental_method()
    def himport_discard(self, fieldset_name: str) -> int:
        """Remove a fieldset from the registry.

        Returns ``1`` if it was registered, ``0`` otherwise. On a pooled client the
        server-side ``DISCARD`` is reconciled lazily when each connection is next
        used for ``himport_set``. On a single connection client it runs immediately
        on the pinned connection when it is live; while that connection is not
        connected there is nothing prepared on the server to discard (its tracking is
        reset on connect), so no server call is made.
        """
        removed = self.himport_registry.discard(fieldset_name)
        conn = self.connection
        if self._single_connection_client and conn is not None and conn.is_connected:
            if removed:
                self.himport_discard_internal(fieldset_name)
            conn._himport_prepared.pop(fieldset_name, None)
            conn._himport_reconciled_revision = self.himport_registry.revision
        return 1 if removed else 0

    @experimental_method()
    def himport_discard_all(self) -> int:
        """Remove all fieldsets from the registry.

        Returns the number removed from the registry. Server-side removal follows the
        same live/lazy rule as :meth:`himport_discard`.
        """
        count = self.himport_registry.discard_all()
        conn = self.connection
        if self._single_connection_client and conn is not None and conn.is_connected:
            if count:
                self.himport_discard_all_internal()
            conn._himport_prepared.clear()
            conn._himport_reconciled_revision = self.himport_registry.revision
        return count


StrictRedis = Redis


class Monitor:
    """
    Monitor is useful for handling the MONITOR command to the redis server.
    next_command() method returns one command from monitor
    listen() method yields commands from monitor.
    """

    monitor_re = re.compile(r"\[(\d+) (.*?)\] (.*)")
    command_re = re.compile(r'"(.*?)(?<!\\)"')

    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.connection = self.connection_pool.get_connection()

    def __enter__(self):
        self._start_monitor()
        return self

    def __exit__(self, *args):
        self.connection.disconnect()
        self.connection_pool.release(self.connection)

    def next_command(self):
        """Parse the response from a monitor command"""
        response = self.connection.read_response()

        if response is None:
            return None

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
            if client_info == "":
                client_address = ""
                client_port = ""
                client_type = "unknown"
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

    def listen(self):
        """Listen for commands coming to the server."""
        while True:
            yield self.next_command()

    def _start_monitor(self):
        self.connection.send_command("MONITOR")
        # check that monitor returns 'OK', but don't return it to user
        response = self.connection.read_response()

        if not bool_ok(response):
            raise RedisError(f"MONITOR failed: {response}")


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
        connection_pool,
        shard_hint=None,
        ignore_subscribe_messages: bool = False,
        encoder: Optional["Encoder"] = None,
        push_handler_func: Union[None, Callable[[str], None]] = None,
        event_dispatcher: Optional["EventDispatcher"] = None,
    ):
        self.connection_pool = connection_pool
        self.shard_hint = shard_hint
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.connection = None
        self.subscribed_event = threading.Event()
        # we need to know the encoding options for this connection in order
        # to lookup channel and pattern names for callback handlers.
        self.encoder = encoder
        self.push_handler_func = push_handler_func
        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher

        self._lock = threading.RLock()
        if self.encoder is None:
            self.encoder = self.connection_pool.get_encoder()
        self.health_check_response_b = self.encoder.encode(self.HEALTH_CHECK_MESSAGE)
        if self.encoder.decode_responses:
            self.health_check_response = ["pong", self.HEALTH_CHECK_MESSAGE]
        else:
            self.health_check_response = [b"pong", self.health_check_response_b]
        if self.push_handler_func is None:
            _set_info_logger()
        self.reset()

    def __enter__(self) -> "PubSub":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.reset()

    def __del__(self) -> None:
        try:
            # if this object went out of scope prior to shutting down
            # subscriptions, close the connection manually before
            # returning it to the connection pool
            self.reset()
        except Exception:
            pass

    def reset(self) -> None:
        if self.connection:
            self.connection.disconnect()
            self.connection.deregister_connect_callback(self.on_connect)
            self.connection_pool.release(self.connection)
            self.connection = None
        self.health_check_response_counter = 0
        self.channels = {}
        self.pending_unsubscribe_channels = set()
        self.shard_channels = {}
        self.pending_unsubscribe_shard_channels = set()
        self.patterns = {}
        self.pending_unsubscribe_patterns = set()
        self.subscribed_event.clear()

    def close(self) -> None:
        self.reset()

    def _resubscribe(self, subscribed, subscribe_fn) -> None:
        # Replay handler-backed subscriptions as positional Subscription objects
        # so binary names never need to be decoded into keyword argument keys.
        subscriptions = pubsub_subscription_args(subscribed)
        if subscriptions:
            subscribe_fn(*subscriptions)

    def _resubscribe_shard_channels(self) -> None:
        self._resubscribe(self.shard_channels, self.ssubscribe)

    def on_connect(self, connection) -> None:
        "Re-subscribe to any channels and patterns previously subscribed to"
        self.pending_unsubscribe_channels.clear()
        self.pending_unsubscribe_patterns.clear()
        self.pending_unsubscribe_shard_channels.clear()
        if self.channels:
            self._resubscribe(self.channels, self.subscribe)
        if self.patterns:
            self._resubscribe(self.patterns, self.psubscribe)
        if self.shard_channels:
            self._resubscribe_shard_channels()

    @property
    def subscribed(self) -> bool:
        """Indicates if there are subscriptions to any channels or patterns"""
        return self.subscribed_event.is_set()

    def execute_command(self, *args):
        """Execute a publish/subscribe command"""

        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        if self.connection is None:
            self.connection = self.connection_pool.get_connection()
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            self.connection.register_connect_callback(self.on_connect)
            if self.push_handler_func is not None:
                self.connection._parser.set_pubsub_push_handler(self.push_handler_func)
            self._event_dispatcher.dispatch(
                AfterPubSubConnectionInstantiationEvent(
                    self.connection, self.connection_pool, ClientType.SYNC, self._lock
                )
            )
        connection = self.connection
        kwargs = {"check_health": not self.subscribed}
        if not self.subscribed:
            self.clean_health_check_responses()
        with self._lock:
            self._execute(connection, connection.send_command, *args, **kwargs)

    def clean_health_check_responses(self) -> None:
        """
        If any health check responses are present, clean them
        """
        ttl = 10
        conn = self.connection
        while conn and self.health_check_response_counter > 0 and ttl > 0:
            if self._execute(conn, conn.can_read, timeout=conn.socket_timeout):
                response = self._execute(conn, conn.read_response)
                if self.is_health_check_response(response):
                    self.health_check_response_counter -= 1
                else:
                    raise PubSubError(
                        "A non health check response was cleaned by "
                        "execute_command: {}".format(response)
                    )
            ttl -= 1

    def _reconnect(
        self,
        conn,
        error: Optional[Exception] = None,
        failure_count: Optional[int] = None,
        start_time: Optional[float] = None,
        command_name: Optional[str] = None,
    ) -> None:
        """
        The supported exceptions are already checked in the
        retry object so we don't need to do it here.

        In this error handler we are trying to reconnect to the server.
        """
        if error and failure_count <= conn.retry.get_retries():
            if command_name:
                record_operation_duration(
                    command_name=command_name,
                    duration_seconds=time.monotonic() - start_time,
                    server_address=getattr(conn, "host", None),
                    server_port=getattr(conn, "port", None),
                    db_namespace=str(conn.db),
                    error=error,
                    retry_attempts=failure_count,
                )
        conn.disconnect()
        conn.connect()

    def _execute(self, conn, command, *args, **kwargs):
        """
        Connect manually upon disconnection. If the Redis server is down,
        this will fail and raise a ConnectionError as desired.
        After reconnection, the ``on_connect`` callback should have been
        called by the # connection to resubscribe us to any channels and
        patterns we were previously listening to
        """

        if conn.should_reconnect():
            self._reconnect(conn)

        if not len(args) == 0:
            command_name = args[0]
        else:
            command_name = None

        # Start timing for observability
        start_time = time.monotonic()
        # Track actual retry attempts for error reporting
        actual_retry_attempts = [0]

        def failure_callback(error, failure_count):
            if is_debug_log_enabled():
                add_debug_log_for_operation_failure(conn, error, args)
            actual_retry_attempts[0] = failure_count
            self._reconnect(conn, error, failure_count, start_time, command_name)

        try:
            response = conn.retry.call_with_retry(
                lambda: command(*args, **kwargs),
                failure_callback,
                with_failure_count=True,
            )

            if command_name:
                record_operation_duration(
                    command_name=command_name,
                    duration_seconds=time.monotonic() - start_time,
                    server_address=getattr(conn, "host", None),
                    server_port=getattr(conn, "port", None),
                    db_namespace=str(conn.db),
                )

            return response
        except Exception as e:
            record_error_count(
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                network_peer_address=getattr(conn, "host", None),
                network_peer_port=getattr(conn, "port", None),
                error_type=e,
                retry_attempts=actual_retry_attempts[0],
                is_internal=False,
            )
            raise

    def parse_response(self, block=True, timeout=0):
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
            response = pubsub.parse_response(block=True, timeout=0.1)

            # Non-blocking with 0.1 second timeout
            response = pubsub.parse_response(block=False, timeout=0.1)

            # Non-blocking, return immediately
            response = pubsub.parse_response(block=False, timeout=0)

            # Recommended: use get_message() instead
            msg = pubsub.get_message(timeout=0.1)  # automatically sets block=False
            msg = pubsub.get_message(timeout=None)  # automatically sets block=True
        """
        conn = self.connection
        if conn is None:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
            )

        self.check_health()

        def try_read():
            if not block:
                if not conn.can_read(timeout=timeout):
                    return None
                read_timeout = timeout
            else:
                conn.connect()
                # Block indefinitely waiting for a pubsub message. timeout=None
                # makes the socket layer call sock.settimeout(None) for this read
                # (and restore the original socket_timeout afterwards), so the
                # configured socket_timeout does not abort the read.
                read_timeout = None
            return conn.read_response(
                disconnect_on_error=False, push_request=True, timeout=read_timeout
            )

        response = self._execute(conn, try_read)

        if self.is_health_check_response(response):
            # ignore the health check message as user might not expect it
            self.health_check_response_counter -= 1
            return None
        return response

    def is_health_check_response(self, response) -> bool:
        """
        Check if the response is a health check response.
        If there are no subscriptions redis responds to PING command with a
        bulk response, instead of a multi-bulk with "pong" and the response.
        """
        if self.encoder.decode_responses:
            return (
                response
                in [
                    self.health_check_response,  # If there is a subscription
                    self.HEALTH_CHECK_MESSAGE,  # If there are no subscriptions and decode_responses=True
                ]
            )
        else:
            return (
                response
                in [
                    self.health_check_response,  # If there is a subscription
                    self.health_check_response_b,  # If there isn't a subscription and decode_responses=False
                ]
            )

    def check_health(self) -> None:
        conn = self.connection
        if conn is None:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
            )

        if conn.health_check_interval and time.monotonic() > conn.next_health_check:
            conn.send_command("PING", self.HEALTH_CHECK_MESSAGE, check_health=False)
            self.health_check_response_counter += 1

    def _normalize_keys(self, data) -> Dict:
        """
        normalize channel/pattern names to be either bytes or strings
        based on whether responses are automatically decoded. this saves us
        from coercing the value for each message coming in.
        """
        encode = self.encoder.encode
        decode = self.encoder.decode
        return {decode(encode(k)): v for k, v in data.items()}

    def psubscribe(
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
        ret_val = self.execute_command("PSUBSCRIBE", *new_patterns.keys())
        # update the patterns dict AFTER we send the command. we don't want to
        # subscribe twice to these patterns, once for the command and again
        # for the reconnection.
        new_patterns = self._normalize_keys(new_patterns)
        self.patterns.update(new_patterns)
        if not self.subscribed:
            # Set the subscribed_event flag to True
            self.subscribed_event.set()
            # Clear the health check counter
            self.health_check_response_counter = 0
        self.pending_unsubscribe_patterns.difference_update(new_patterns)
        return ret_val

    def punsubscribe(self, *args):
        """
        Unsubscribe from the supplied patterns. If empty, unsubscribe from
        all patterns.
        """
        if args:
            args = list_or_args(args[0], args[1:])
            patterns = self._normalize_keys(dict.fromkeys(args))
        else:
            patterns = self.patterns
        self.pending_unsubscribe_patterns.update(patterns)
        return self.execute_command("PUNSUBSCRIBE", *args)

    def subscribe(
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
        ret_val = self.execute_command("SUBSCRIBE", *new_channels.keys())
        # update the channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        new_channels = self._normalize_keys(new_channels)
        self.channels.update(new_channels)
        if not self.subscribed:
            # Set the subscribed_event flag to True
            self.subscribed_event.set()
            # Clear the health check counter
            self.health_check_response_counter = 0
        self.pending_unsubscribe_channels.difference_update(new_channels)
        return ret_val

    def unsubscribe(self, *args):
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        if args:
            args = list_or_args(args[0], args[1:])
            channels = self._normalize_keys(dict.fromkeys(args))
        else:
            channels = self.channels
        self.pending_unsubscribe_channels.update(channels)
        return self.execute_command("UNSUBSCRIBE", *args)

    def ssubscribe(
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
        ret_val = self.execute_command("SSUBSCRIBE", *new_s_channels.keys())
        # update the s_channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        new_s_channels = self._normalize_keys(new_s_channels)
        self.shard_channels.update(new_s_channels)
        if not self.subscribed:
            # Set the subscribed_event flag to True
            self.subscribed_event.set()
            # Clear the health check counter
            self.health_check_response_counter = 0
        self.pending_unsubscribe_shard_channels.difference_update(new_s_channels)
        return ret_val

    def sunsubscribe(self, *args, target_node=None):
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

    def listen(self):
        """Listen for messages on channels this client has been subscribed to.

        Iteration ends once every channel and pattern has been unsubscribed
        from. If nothing is subscribed when iteration begins it ends
        immediately rather than waiting, so subscribe first: a listener
        started before any subscription finishes without yielding anything.
        """
        while self.subscribed:
            response = self.handle_message(self.parse_response(block=True))
            if response is not None:
                yield response

    def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float = 0.0
    ):
        """
        Get the next message if one is available, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number, or None, to wait indefinitely.
        """
        if not self.subscribed:
            # Wait for subscription
            start_time = time.monotonic()
            if self.subscribed_event.wait(timeout) is True:
                # The connection was subscribed during the timeout time frame.
                # The timeout should be adjusted based on the time spent
                # waiting for the subscription. timeout=None means "block
                # indefinitely" and has nothing to charge against, so leave it
                # alone rather than raising TypeError on the subtraction.
                if timeout is not None:
                    time_spent = time.monotonic() - start_time
                    timeout = max(0.0, timeout - time_spent)
            else:
                # The connection isn't subscribed to any channels or patterns,
                # so no messages are available
                return None

        response = self.parse_response(block=(timeout is None), timeout=timeout)

        if response:
            return self.handle_message(response, ignore_subscribe_messages)
        return None

    get_sharded_message = get_message

    def ping(self, message: Union[str, None] = None) -> bool:
        """
        Ping the Redis server to test connectivity.

        Sends a PING command to the Redis server and returns True if the server
        responds with "PONG".
        """
        args = ["PING", message] if message is not None else ["PING"]
        return self.execute_command(*args)

    def handle_message(self, response, ignore_subscribe_messages=False):
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
            record_pubsub_message(
                direction=PubSubDirection.RECEIVE,
                channel=channel,
            )
        elif message_type == "smessage":
            channel = str_if_bytes(message["channel"])
            record_pubsub_message(
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
            if not self.channels and not self.patterns and not self.shard_channels:
                # There are no subscriptions anymore, set subscribed_event flag
                # to false
                self.subscribed_event.clear()

        if message_type in self.PUBLISH_MESSAGE_TYPES:
            # if there's a message handler, invoke it
            if message_type == "pmessage":
                handler = self.patterns.get(message["pattern"], None)
            elif message_type == "smessage":
                handler = self.shard_channels.get(message["channel"], None)
            else:
                handler = self.channels.get(message["channel"], None)
            if handler:
                handler(message)
                return None
        elif message_type != "pong":
            # this is a subscribe/unsubscribe message. ignore if we don't
            # want them
            if ignore_subscribe_messages or self.ignore_subscribe_messages:
                return None

        return message

    def run_in_thread(
        self,
        sleep_time: float = 0.0,
        daemon: bool = False,
        exception_handler: Optional[Callable] = None,
        pubsub=None,
        sharded_pubsub: bool = False,
    ) -> "PubSubWorkerThread":
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

        pubsub = self if pubsub is None else pubsub
        thread = PubSubWorkerThread(
            pubsub,
            sleep_time,
            daemon=daemon,
            exception_handler=exception_handler,
            sharded_pubsub=sharded_pubsub,
        )
        thread.start()
        return thread


class PubSubWorkerThread(threading.Thread):
    def __init__(
        self,
        pubsub,
        sleep_time: float,
        daemon: bool = False,
        exception_handler: Union[
            Callable[[Exception, "PubSub", "PubSubWorkerThread"], None], None
        ] = None,
        sharded_pubsub: bool = False,
    ):
        super().__init__()
        self.daemon = daemon
        self.pubsub = pubsub
        self.sleep_time = sleep_time
        self.exception_handler = exception_handler
        self.sharded_pubsub = sharded_pubsub
        self._running = threading.Event()
        # Set before start() so run() cannot undo an early stop() request.
        self._running.set()

    def run(self) -> None:
        pubsub = self.pubsub
        sleep_time = self.sleep_time
        while self._running.is_set():
            try:
                if not self.sharded_pubsub:
                    pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=sleep_time
                    )
                else:
                    pubsub.get_sharded_message(
                        ignore_subscribe_messages=True, timeout=sleep_time
                    )
            except BaseException as e:
                if self.exception_handler is None:
                    raise
                self.exception_handler(e, pubsub, self)
        pubsub.close()

    def stop(self, timeout: Optional[float] = None) -> None:
        """Wait for the worker to finish after clearing the running flag.

        timeout bounds that wait. None waits until the worker exits.
        The wait is skipped when stop() runs on the worker thread.
        """
        # trip the flag so the run loop exits. the run loop will
        # close the pubsub connection, which disconnects the socket
        # and returns the connection to the pool.
        self._running.clear()
        # wait for that close before returning, so a caller that
        # follows stop() with pubsub.close() does not disconnect the
        # socket while get_message is still reading it. the exception
        # handler calls stop() from this thread; joining there deadlocks.
        if threading.current_thread() is not self and self.is_alive():
            self.join(timeout)


class Pipeline(Redis):
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
        response_callbacks,
        transaction,
        shard_hint,
    ):
        self.connection_pool = connection_pool
        self.connection: Optional[Connection] = None
        self.response_callbacks = response_callbacks
        self.transaction = transaction
        self.shard_hint = shard_hint
        self.watching = False
        self.command_stack = []
        self.scripts: Set[Script] = set()
        self.explicit_transaction = False

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self) -> int:
        return len(self.command_stack)

    def __bool__(self) -> bool:
        """Pipeline instances should always evaluate to True"""
        return True

    def reset(self) -> None:
        self.command_stack = []
        self.scripts = set()
        # make sure to reset the connection state in the event that we were
        # watching something
        if self.watching and self.connection:
            try:
                # call this manually since our unwatch or
                # immediate_execute_command methods can call reset()
                self.connection.send_command("UNWATCH")
                self.connection.read_response()
            except ConnectionError:
                # disconnect will also remove any previous WATCHes
                self.connection.disconnect()
        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False

        # we can safely return the connection to the pool here since we're
        # sure we're no longer WATCHing anything
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    def close(self) -> None:
        """Close the pipeline"""
        self.reset()

    def multi(self) -> None:
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

    def execute_command(self, *args, **kwargs):
        if (self.watching or args[0] == "WATCH") and not self.explicit_transaction:
            return self.immediate_execute_command(*args, **kwargs)
        return self.pipeline_execute_command(*args, **kwargs)

    def _disconnect_reset_raise_on_watching(
        self,
        conn: AbstractConnection,
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
        if error and failure_count <= conn.retry.get_retries():
            record_operation_duration(
                command_name=command_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
                error=error,
                retry_attempts=failure_count,
            )
        conn.disconnect()

        # if we were already watching a variable, the watch is no longer
        # valid since this connection has died. raise a WatchError, which
        # indicates the user should retry this transaction.
        if self.watching:
            self.reset()
            raise WatchError(
                f"A {type(error).__name__} occurred while watching one or more keys"
            )

    def immediate_execute_command(self, *args, **options):
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
            conn = self.connection_pool.get_connection()
            self.connection = conn

        # Start timing for observability
        start_time = time.monotonic()
        # Track actual retry attempts for error reporting
        actual_retry_attempts = [0]

        def failure_callback(error, failure_count):
            if is_debug_log_enabled():
                add_debug_log_for_operation_failure(conn, error, args)
            actual_retry_attempts[0] = failure_count
            self._disconnect_reset_raise_on_watching(
                conn, error, failure_count, start_time, command_name
            )

        try:
            response = conn.retry.call_with_retry(
                lambda: self._send_command_parse_response(
                    conn, command_name, *args, **options
                ),
                failure_callback,
                with_failure_count=True,
            )

            record_operation_duration(
                command_name=command_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
            )

            return response
        except Exception as e:
            record_error_count(
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                network_peer_address=getattr(conn, "host", None),
                network_peer_port=getattr(conn, "port", None),
                error_type=e,
                retry_attempts=actual_retry_attempts[0],
                is_internal=False,
            )
            raise

    def pipeline_execute_command(self, *args, **options) -> "Pipeline":
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

    def _himport_prepare_pipeline(self, conn, commands):
        """Delegate to the shared sync HIMPORT executor."""
        _himport_exec.prepare_pipeline(self, conn, [args for args, _ in commands])

    def _execute_transaction(
        self, connection: Connection, commands, raise_on_error
    ) -> List:
        # Ensure fieldsets referenced by buffered HIMPORT SETs are prepared on this
        # connection before the MULTI/EXEC block (session state, not transactional).
        self._himport_prepare_pipeline(connection, commands)
        cmds = chain([(("MULTI",), {})], commands, [(("EXEC",), {})])
        all_cmds = connection.pack_commands(
            [args for args, options in cmds if EMPTY_RESPONSE not in options]
        )
        connection.send_packed_command(all_cmds)
        errors = []

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            self.parse_response(connection, "_")
        except ResponseError as e:
            errors.append((0, e))

        # and all the other commands
        for i, command in enumerate(commands):
            if EMPTY_RESPONSE in command[1]:
                errors.append((i, command[1][EMPTY_RESPONSE]))
            else:
                try:
                    self.parse_response(connection, "_")
                except ResponseError as e:
                    self.annotate_exception(e, i + 1, command[0])
                    errors.append((i, e))

        # parse the EXEC.
        never_decode = _transaction_never_decode(self.get_connection_kwargs(), commands)
        try:
            if never_decode:
                response = self.parse_response(connection, "_", **{NEVER_DECODE: []})
            else:
                response = self.parse_response(connection, "_")
        except ExecAbortError:
            if errors:
                raise errors[0][1]
            raise

        # EXEC clears any watched keys
        self.watching = False

        if response is None:
            raise WatchError("Watched variable changed.")

        # put any parse errors into the response
        for i, e in errors:
            response.insert(i, e)

        if len(response) != len(commands):
            self.connection.disconnect()
            raise ResponseError(
                "Wrong number of response items from pipeline execution"
            )

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
                # Remove keys entry, it needs only for cache.
                options.pop("keys", None)
                command_name = args[0]
                if command_name in self.response_callbacks:
                    r = self.response_callbacks[command_name](r, **options)
            data.append(r)

        return data

    def _execute_pipeline(self, connection, commands, raise_on_error):
        # Fold any first-use HIMPORT PREPAREs for referenced fieldsets into the same
        # packed write as the queued commands, so a pipeline that lands on a fresh or
        # reconnected connection stays a single round trip (the batched write bypasses
        # the per-command lazy PREPARE path). Deferred-discard reconciliation happens
        # inside pipeline_prepares and only touches the socket when discards are
        # actually pending.
        fieldsets = _himport_exec.pipeline_prepares(
            self, connection, [args for args, _ in commands]
        )
        preflight = _himport_exec.prepare_wire_commands(fieldsets)
        # build up all commands into a single request to increase network perf
        all_cmds = connection.pack_commands(preflight + [args for args, _ in commands])
        connection.send_packed_command(all_cmds)

        # Drain the leading PREPARE replies (bookkeeping + capture the first error)
        # before the queued replies. Everything on the wire is read before raising so
        # the pooled socket never desyncs.
        prep_error = _himport_exec.drain_pipeline_prepares(self, connection, fieldsets)

        responses = []
        try:
            for args, options in commands:
                try:
                    responses.append(
                        self.parse_response(connection, args[0], **options)
                    )
                except ResponseError as e:
                    responses.append(e)
        except BaseException:
            # A response callback raised, so the replies of the rest of the batch
            # are still on the socket. Drop the connection rather than hand them to
            # the next caller that borrows it from the pool.
            connection.disconnect()
            raise

        # A PREPARE failure (rare: an invalid fieldset definition) is a hard error,
        # raised regardless of raise_on_error as it was before folding -- only now
        # every reply has already been drained.
        if prep_error is not None:
            raise prep_error
        if raise_on_error:
            self.raise_first_error(commands, responses)

        return responses

    def raise_first_error(self, commands, response):
        for i, r in enumerate(response):
            if isinstance(r, ResponseError):
                self.annotate_exception(r, i + 1, commands[i][0])
                raise r

    def annotate_exception(self, exception, number, command):
        cmd = command_text(command)
        msg = (
            f"Command # {number} ({truncate_text(cmd)}) of pipeline "
            f"caused error: {exception.args[0]}"
        )
        exception.args = (msg,) + exception.args[1:]

    def parse_response(self, connection, command_name, **options):
        result = Redis.parse_response(self, connection, command_name, **options)
        if command_name in self.UNWATCH_COMMANDS:
            self.watching = False
        elif command_name == "WATCH":
            self.watching = True
        return result

    def load_scripts(self):
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        # we can't use the normal script_* methods because they would just
        # get buffered in the pipeline.
        exists = immediate("SCRIPT EXISTS", *shas)
        if not all(exists):
            for s, exist in zip(scripts, exists):
                if not exist:
                    s.sha = immediate("SCRIPT LOAD", s.script)

    def _disconnect_raise_on_watching(
        self,
        conn: AbstractConnection,
        error: Exception,
        failure_count: Optional[int] = None,
        start_time: Optional[float] = None,
        command_name: Optional[str] = None,
    ) -> None:
        """
        Close the connection, raise an exception if we were watching.

        The supported exceptions are already checked in the
        retry object so we don't need to do it here.

        After we disconnect the connection, it will try to reconnect and
        do a health check as part of the send_command logic(on connection level).
        """
        if error and failure_count <= conn.retry.get_retries():
            record_operation_duration(
                command_name=command_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
                error=error,
                retry_attempts=failure_count,
            )
        conn.disconnect()
        # if we were watching a variable, the watch is no longer valid
        # since this connection has died. raise a WatchError, which
        # indicates the user should retry this transaction.
        if self.watching:
            raise WatchError(
                f"A {type(error).__name__} occurred while watching one or more keys"
            )

    def execute(self, raise_on_error: bool = True) -> List[Any]:
        """Execute all the commands in the current pipeline"""
        stack = self.command_stack
        if not stack and not self.watching:
            return []
        if self.scripts:
            self.load_scripts()
        if self.transaction or self.explicit_transaction:
            execute = self._execute_transaction
            operation_name = "MULTI"
        else:
            execute = self._execute_pipeline
            operation_name = "PIPELINE"

        conn = self.connection
        if not conn:
            conn = self.connection_pool.get_connection()
            # assign to self.connection so reset() releases the connection
            # back to the pool after we're done
            self.connection = conn

        # Start timing for observability
        start_time = time.monotonic()
        # Track actual retry attempts for error reporting
        actual_retry_attempts = [0]

        def failure_callback(error, failure_count):
            if is_debug_log_enabled():
                add_debug_log_for_operation_failure(conn, error, (operation_name,))
            actual_retry_attempts[0] = failure_count
            self._disconnect_raise_on_watching(
                conn, error, failure_count, start_time, operation_name
            )

        try:
            response = conn.retry.call_with_retry(
                lambda: execute(conn, stack, raise_on_error),
                failure_callback,
                with_failure_count=True,
            )

            record_operation_duration(
                command_name=operation_name,
                duration_seconds=time.monotonic() - start_time,
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                db_namespace=str(conn.db),
            )
            return response
        except Exception as e:
            record_error_count(
                server_address=getattr(conn, "host", None),
                server_port=getattr(conn, "port", None),
                network_peer_address=getattr(conn, "host", None),
                network_peer_port=getattr(conn, "port", None),
                error_type=e,
                retry_attempts=actual_retry_attempts[0],
                is_internal=False,
            )
            raise

        finally:
            # in reset() the connection is disconnected before returned to the pool if
            # it is marked for reconnect.
            self.reset()

    def discard(self):
        """
        Flushes all previously queued commands
        See: https://redis.io/commands/DISCARD
        """
        self.execute_command("DISCARD")

    def watch(self, *names):
        """Watches the values at keys ``names``"""
        if self.explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")
        return self.execute_command("WATCH", *names)

    def unwatch(self) -> bool:
        """Unwatches all previously specified keys"""
        return self.watching and self.execute_command("UNWATCH") or True
