import asyncio
import collections
import logging
import random
import threading
import time
import warnings
import weakref
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import asynccontextmanager, nullcontext
from copy import copy
from itertools import chain
from types import MethodType
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    Deque,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    from redis.asyncio.keyspace_notifications import (
        AsyncClusterKeyspaceNotifications,
    )

from redis._defaults import (
    DEFAULT_RETRY_BASE,
    DEFAULT_RETRY_CAP,
    DEFAULT_RETRY_COUNT,
    DEFAULT_SOCKET_CONNECT_TIMEOUT,
    DEFAULT_SOCKET_READ_SIZE,
    DEFAULT_SOCKET_TIMEOUT,
)
from redis._parsers import AsyncCommandsParser, Encoder
from redis._parsers.helpers import decode_info, get_response_callbacks
from redis.asyncio import _himport_exec
from redis.asyncio.client import PubSub, ResponseCallbackT
from redis.asyncio.connection import (
    AbstractConnection,
    Connection,
    ConnectionPoolInterface,
    SSLConnection,
    parse_url,
)
from redis.asyncio.lock import Lock
from redis.asyncio.maint_notifications import AsyncOSSMaintNotificationsHandler
from redis.asyncio.observability.recorder import (
    record_error_count,
    record_operation_duration,
)
from redis.asyncio.retry import Retry, _to_async_retry
from redis.auth.token import TokenInterface
from redis.backoff import ExponentialWithJitterBackoff, NoBackoff
from redis.client import (
    EMPTY_RESPONSE,
    NEVER_DECODE,
    AbstractRedis,
    _decode_transaction_replies,
    _transaction_never_decode,
)
from redis.cluster import (
    _REPLICAS_ONLY_STRATEGIES,
    PIPELINE_BLOCKED_COMMANDS,
    PRIMARY,
    REPLICA,
    SLOT_ID,
    AbstractRedisCluster,
    LoadBalancer,
    LoadBalancingStrategy,
    _error_to_raise,
    _resolve_localhost,
    _should_request_refresh,
    _topology_refresh_delay,
    block_pipeline_command,
    get_node_name,
    is_zero_key_eval_command,
    parse_cluster_shards,
    parse_cluster_shards_unified,
    parse_cluster_shards_with_str_keys,
    parse_cluster_slots,
)
from redis.commands import AsyncRedisClusterCommands
from redis.commands.helpers import list_or_args, parse_pubsub_subscriptions
from redis.commands.metadata import (
    _DEFAULT_KEYED_METADATA,
    _DEFAULT_KEYLESS_METADATA,
    _METADATA_BY_REQUEST_POLICY,
    AsyncMetadataResolver,
    AsyncStaticMetadataResolver,
    CommandMetadata,
    CommandPolicies,
    RequestPolicy,
    ResponsePolicy,
)
from redis.commands.policies import AsyncPolicyResolver, AsyncStaticPolicyResolver
from redis.crc import REDIS_CLUSTER_HASH_SLOTS, key_slot
from redis.credentials import CredentialProvider
from redis.driver_info import DriverInfo, resolve_driver_info
from redis.event import (
    AfterAsyncClusterInstantiationEvent,
    AsyncAfterSlotsCacheRefreshEvent,
    AsyncEventListenerInterface,
    EventDispatcher,
)
from redis.exceptions import (
    AskError,
    AuthenticationError,
    AuthorizationError,
    BusyLoadingError,
    ClusterDownError,
    ClusterError,
    ConnectionError,
    CrossSlotTransactionError,
    DataError,
    ExecAbortError,
    InvalidPipelineStack,
    MaxClientsReachedError,
    MaxConnectionsError,
    MovedError,
    RedisClusterException,
    RedisClusterUnreachableError,
    RedisError,
    ResponseError,
    SlotNotCoveredError,
    TimeoutError,
    TryAgainError,
    WatchError,
)
from redis.himport import HImportRegistry, parse_himport_set_args
from redis.maint_notifications import MaintNotificationsConfig
from redis.typing import (
    AnyKeyT,
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
    check_protocol_version,
    command_text,
    deprecated_args,
    deprecated_function,
    experimental_method,
    str_if_bytes,
    truncate_text,
)

if SSL_AVAILABLE:
    from ssl import TLSVersion, VerifyFlags, VerifyMode
else:
    TLSVersion = None
    VerifyMode = None
    VerifyFlags = None

logger = logging.getLogger(__name__)

TargetNodesT = TypeVar(
    "TargetNodesT", str, "ClusterNode", List["ClusterNode"], Dict[Any, "ClusterNode"]
)

_T = TypeVar("_T")


def _run_coroutine_in_thread(coro: Coroutine[Any, Any, _T]) -> _T:
    """
    Runs ``coro`` to completion on a private event loop in a worker thread.

    The bridge for the places where a synchronous entry point has to drive an awaitable
    body. The caller may already be running an event loop, which rules out both awaiting
    the coroutine and running it here with ``asyncio.run``, so it gets a loop of its own
    and the calling thread blocks until it finishes. Whatever the coroutine raised is
    re-raised to the caller, so the entry point fails the way it would have had the body
    run in place.

    .. warning::

       **Deadlock hazard. Pass only a coroutine that touches nothing bound to the
       caller's loop.** The ``thread.join()`` below blocks the calling thread for the
       whole run, so when that thread is the one running an event loop, the loop stops
       making progress for the duration. A body that then awaits anything owned by that
       loop - a future, a task, a lock, or I/O over a pooled connection - is waiting on a
       loop that is itself blocked on this very ``join()``. The result is a permanent
       stall, not an exception: nothing times out and nothing is raised. Loop-bound
       futures may instead fail with a cross-loop ``RuntimeError``, which is the luckier
       outcome because at least it is visible.

    Pass only a body whose awaits are self-contained, and prefer restructuring the entry
    point to be ``async`` over reaching for this.
    """
    result: Any = None
    error: BaseException | None = None

    def runner() -> None:
        nonlocal result
        nonlocal error
        try:
            result = asyncio.run(coro)
        except BaseException as e:
            error = e

    thread = threading.Thread(target=runner)
    thread.start()
    # Unconditional and untimed by design: the caller expects a value, so there is no
    # partial result to return early with. If the caller's thread is running an event
    # loop, that loop is blocked here, which is why the coroutine must never await
    # anything owned by it. See the deadlock hazard in the docstring above.
    thread.join()

    if error is not None:
        raise error

    return result


class AsyncMaintNotificationsAbstractRedisCluster:
    """
    Mixin for async cluster maintenance notifications handling.

    Intended to be used with multiple inheritance alongside RedisCluster.
    All logic related to cluster-level maintenance notifications is encapsulated here.
    """

    def __init__(
        self,
        maint_notifications_config: MaintNotificationsConfig | None,
        **kwargs,
    ) -> None:
        # The RESP3 requirement is validated in RedisCluster.__init__ before the
        # NodesManager is constructed; this mixin is only ever run from there, so
        # the config it receives has already been validated.
        is_protocol_supported = check_protocol_version(kwargs.get("protocol"), 3)

        if maint_notifications_config is None and is_protocol_supported:
            maint_notifications_config = MaintNotificationsConfig()

        self.maint_notifications_config = maint_notifications_config

        if self.maint_notifications_config and self.maint_notifications_config.enabled:
            self._oss_cluster_maint_notifications_handler = (
                AsyncOSSMaintNotificationsHandler(self, self.maint_notifications_config)
            )
            self._update_connection_kwargs_for_maint_notifications(
                self._oss_cluster_maint_notifications_handler
            )
            # Connections are created lazily via ClusterNode.acquire_connection()
            # during nodes_manager.initialize() (which runs after __init__), so
            # injecting into the shared connection_kwargs covers nodes discovered
            # later. Startup nodes are the exception — they were built before this
            # runs with their own kwargs snapshot — so the helper above also
            # updates them directly.
        else:
            self._oss_cluster_maint_notifications_handler = None

    def _update_connection_kwargs_for_maint_notifications(
        self,
        oss_cluster_maint_notifications_handler: AsyncOSSMaintNotificationsHandler,
    ) -> None:
        maint_kwargs = {
            "oss_cluster_maint_notifications_handler": oss_cluster_maint_notifications_handler,
            "maint_notifications_config": oss_cluster_maint_notifications_handler.config,
        }
        # Shared template used for every node created from now on (e.g. nodes
        # discovered during nodes_manager.initialize()).
        self.nodes_manager.connection_kwargs.update(maint_kwargs)
        # Startup nodes were constructed before this mixin ran, so each one
        # snapshotted connection_kwargs without the handler. Their connections
        # are created lazily, so updating their per-node kwargs now is in time —
        # otherwise initialize() opens the topology-discovery connection (CLUSTER
        # SLOTS) on a startup node with no push handler wired and silently drops
        # the maintenance notifications carried on that connection.
        for node in self.nodes_manager.startup_nodes.values():
            node.connection_kwargs.update(maint_kwargs)


class RedisCluster(
    AbstractRedis,
    AbstractRedisCluster,
    AsyncMaintNotificationsAbstractRedisCluster,
    AsyncRedisClusterCommands,
):
    """
    Create a new RedisCluster client.

    Pass one of parameters:

      - `host` & `port`
      - `startup_nodes`

    | Use ``await`` :meth:`initialize` to find cluster nodes & create connections.
    | Use ``await`` :meth:`close` to disconnect connections & close client.

    Many commands support the target_nodes kwarg. It can be one of the
    :attr:`NODE_FLAGS`:

      - :attr:`PRIMARIES`
      - :attr:`REPLICAS`
      - :attr:`ALL_NODES`
      - :attr:`RANDOM`
      - :attr:`DEFAULT_NODE`

    Note: This client is not thread/process/fork safe.

    :param host:
        | Can be used to point to a startup node
    :param port:
        | Port used if **host** is provided
    :param startup_nodes:
        | :class:`~.ClusterNode` to used as a startup node
    :param require_full_coverage:
        | When set to ``False``: the client will not require a full coverage of
          the slots. However, if not all slots are covered, and at least one node
          has ``cluster-require-full-coverage`` set to ``yes``, the server will throw
          a :class:`~.ClusterDownError` for some key-based commands.
        | When set to ``True``: all slots must be covered to construct the cluster
          client. If not all slots are covered, :class:`~.RedisClusterException` will be
          thrown.
        | See:
          https://redis.io/docs/manual/scaling/#redis-cluster-configuration-parameters
    :param read_from_replicas:
        | @deprecated - please use load_balancing_strategy instead
        | Enable read from replicas in READONLY mode.
          When set to true, read commands will be assigned between the primary and
          its replications in a Round-Robin manner.
          The data read from replicas is eventually consistent with the data in primary nodes.
    :param load_balancing_strategy:
        | Enable read from replicas in READONLY mode and defines the load balancing
          strategy that will be used for cluster node selection.
          The data read from replicas is eventually consistent with the data in primary nodes.
    :param metadata_resolver:
        | Optional :class:`~.AsyncMetadataResolver` instance used to map command names
          to replica-safe routing rules. If not provided, an AsyncStaticMetadataResolver
          is used by default. The routing view of it is derived into the default
          ``policy_resolver``; an explicit ``policy_resolver`` supersedes that view, but
          replica safety keeps resolving through this argument either way.
    :param dynamic_startup_nodes:
        | Set the RedisCluster's startup nodes to all the discovered nodes.
          If true (default value), the cluster's discovered nodes will be used to
          determine the cluster nodes-slots mapping in the next topology refresh.
          It will remove the initial passed startup nodes if their endpoints aren't
          listed in the CLUSTER SLOTS output.
          If you use dynamic DNS endpoints for startup nodes but CLUSTER SLOTS lists
          specific IP addresses, it is best to set it to false.
    :param reinitialize_steps:
        | Specifies the number of MOVED errors that need to occur before reinitializing
          the whole cluster topology. If a MOVED error occurs and the cluster does not
          need to be reinitialized on this current error handling, only the MOVED slot
          will be patched with the redirected node.
          To reinitialize the cluster on every MOVED error, set reinitialize_steps to 1.
          To avoid reinitializing the cluster on moved errors, set reinitialize_steps to
          0. A negative value is not the same as 0: it works like its absolute value
          (-3 is the same as 3).
          Every MOVED error patches its slot, so the redirect goes to the new owner;
          the refresh is only asked for, and like every refresh triggered by an
          error, it is run by the next command attempt once due, at most once per
          second.
    :param cluster_error_retry_attempts:
        | @deprecated - Please configure the 'retry' object instead
          In case 'retry' object is set - this argument is ignored!

          Number of times to retry before raising an error when :class:`~.TimeoutError`,
          :class:`~.ConnectionError`, :class:`~.SlotNotCoveredError`
          or :class:`~.ClusterDownError` are encountered
    :param retry:
        | A retry object that defines the retry strategy and the number of
          retries for the cluster client. Its backoff sets the delay before each
          retry of a command or a pipeline; one that failed with
          :class:`~.ClusterDownError` or :class:`~.SlotNotCoveredError` also
          waits a fixed 0.25 seconds before it. A topology refresh triggered by an
          error (connection errors, the ``reinitialize_steps`` MOVED threshold) is
          run by the first command attempt to start once it is due, at most once
          per second; after refreshes that failed in a row, the next one waits as
          long as the backoff gives for that many failures, but at least one
          second. A command that failed with a connection error or a timeout
          gives up with :class:`~.RedisClusterUnreachableError` once a refresh
          that ended since reached no node, or when it runs out of retries while
          the last refresh did.
          Backoffs that keep state between calls
          (:class:`~.DecorrelatedJitterBackoff`) are not fully supported: the
          retry object is shared by every command and pipeline of the client, so
          concurrent retries reset and advance each other's backoff, and the
          delays can come out shorter or longer than configured.
    :param max_connections:
        | Maximum number of connections per node. If there are no free connections & the
          maximum number of connections are already created, a
          :class:`~.MaxConnectionsError` is raised.
    :param socket_keepalive:
        | If ``True``, TCP keepalive is enabled for TCP socket connections.
    :param socket_keepalive_options:
        | Mapping of TCP keepalive socket option constants to values, for
          example ``{socket.TCP_KEEPIDLE: 30}``. If left unspecified, redis-py
          uses TCP keepalive defaults when ``socket_keepalive`` is enabled:
          idle 30 seconds, interval 5 seconds, and 3 probes.
          Platform-specific options that are not available are skipped.
          Pass ``None`` or ``{}`` to avoid setting additional TCP keepalive
          options.
    :param address_remap:
        | An optional callable which, when provided with an internal network
          address of a node, e.g. a `(host, port)` tuple, will return the address
          where the node is reachable.  This can be used to map the addresses at
          which the nodes _think_ they are, to addresses at which a client may
          reach them, such as when they sit behind a proxy.

    | Rest of the arguments will be passed to the
      :class:`~redis.asyncio.connection.Connection` instances when created

    :raises RedisClusterException:
        if any arguments are invalid or unknown. Eg:

        - `db` != 0 or None
        - `path` argument for unix socket connection
        - none of the `host`/`port` & `startup_nodes` were provided

    """

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> "RedisCluster":
        """
        Return a Redis client object configured from the given URL.

        For example::

            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0

        Three URL schemes are supported:

        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>

        The username, password, hostname and path are passed through
        ``urllib.parse.unquote`` in order to replace any percent-encoded values with
        their corresponding characters. Querystring values are decoded by
        ``urllib.parse.parse_qs`` and are not unquoted again.

        All querystring options are cast to their appropriate Python types. Boolean
        arguments can be specified with string values "True"/"False" or "Yes"/"No".
        Values that cannot be properly cast cause a ``ValueError`` to be raised. Once
        parsed, the querystring arguments and keyword arguments are passed to
        :class:`~redis.asyncio.connection.Connection` when created.
        In the case of conflicting arguments, querystring arguments are used.
        """
        kwargs.update(parse_url(url))
        if kwargs.pop("connection_class", None) is SSLConnection:
            kwargs["ssl"] = True
        return cls(**kwargs)

    # Type discrimination marker for @overload self-type pattern
    _is_async_client: Literal[True] = True

    __slots__ = (
        "_initialize",
        "_lock",
        "_topology_ready",
        "maint_notifications_config",
        "_oss_cluster_maint_notifications_handler",
        "_himport_registry",
        "retry",
        "command_flags",
        "commands_parser",
        "connection_kwargs",
        "encoder",
        "node_flags",
        "nodes_manager",
        "read_from_replicas",
        "reinitialize_counter",
        "reinitialize_steps",
        "response_callbacks",
        "result_callbacks",
    )

    @deprecated_args(
        args_to_warn=["read_from_replicas"],
        reason="Please configure the 'load_balancing_strategy' instead",
        version="5.3.0",
    )
    @deprecated_args(
        args_to_warn=[
            "cluster_error_retry_attempts",
        ],
        reason="Please configure the 'retry' object instead",
        version="6.0.0",
    )
    @deprecated_args(
        args_to_warn=["lib_name", "lib_version"],
        reason="Use 'driver_info' parameter instead. "
        "lib_name and lib_version will be removed in a future version.",
    )
    def __init__(
        self,
        host: str | None = None,
        port: str | int = 6379,
        # Cluster related kwargs
        startup_nodes: List["ClusterNode"] | None = None,
        require_full_coverage: bool = True,
        read_from_replicas: bool = False,
        load_balancing_strategy: LoadBalancingStrategy | None = None,
        dynamic_startup_nodes: bool = True,
        reinitialize_steps: int = 5,
        cluster_error_retry_attempts: int = DEFAULT_RETRY_COUNT,
        max_connections: int = 100,
        retry: Retry | None = None,
        retry_on_error: List[Type[Exception]] | None = None,
        # Client related kwargs
        db: str | int = 0,
        path: str | None = None,
        credential_provider: CredentialProvider | None = None,
        username: str | None = None,
        password: str | None = None,
        client_name: str | None = None,
        lib_name: str | object | None = SENTINEL,
        lib_version: str | object | None = SENTINEL,
        driver_info: DriverInfo | object | None = SENTINEL,
        # Encoding related kwargs
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        # Connection related kwargs
        health_check_interval: float = 0,
        socket_timeout: float | None = DEFAULT_SOCKET_TIMEOUT,
        socket_connect_timeout: float | None = DEFAULT_SOCKET_CONNECT_TIMEOUT,
        socket_read_size: int = DEFAULT_SOCKET_READ_SIZE,
        socket_keepalive: bool = True,
        socket_keepalive_options: Mapping[int, int | bytes] | object | None = SENTINEL,
        # SSL related kwargs
        ssl: bool = False,
        ssl_ca_certs: str | None = None,
        ssl_ca_data: str | None = None,
        ssl_cert_reqs: "str | VerifyMode" = "required",
        ssl_include_verify_flags: List["VerifyFlags"] | None = None,
        ssl_exclude_verify_flags: List["VerifyFlags"] | None = None,
        ssl_certfile: str | None = None,
        ssl_check_hostname: bool = True,
        ssl_keyfile: str | None = None,
        ssl_min_version: "TLSVersion | None" = None,
        ssl_ciphers: str | None = None,
        protocol: int | None = None,
        legacy_responses: bool = True,
        address_remap: Callable[[Tuple[str, int]], Tuple[str, int]] | None = None,
        event_dispatcher: EventDispatcher | None = None,
        policy_resolver: AsyncPolicyResolver | None = None,
        maint_notifications_config: MaintNotificationsConfig | None = None,
        metadata_resolver: AsyncMetadataResolver | None = None,
    ) -> None:
        if db:
            raise RedisClusterException(
                "Argument 'db' must be 0 or None in cluster mode"
            )

        if path:
            raise RedisClusterException(
                "Unix domain socket is not supported in cluster mode"
            )

        port_is_provided = bool(port) or type(port) is int
        if (not host or not port_is_provided) and not startup_nodes:
            raise RedisClusterException(
                "RedisCluster requires at least one node to discover the cluster.\n"
                "Please provide one of the following or use RedisCluster.from_url:\n"
                '   - host and port: RedisCluster(host="localhost", port=6379)\n'
                "   - startup_nodes: RedisCluster(startup_nodes=["
                'ClusterNode("localhost", 6379), ClusterNode("localhost", 6380)])'
            )

        computed_driver_info = resolve_driver_info(driver_info, lib_name, lib_version)

        kwargs: Dict[str, Any] = {
            "max_connections": max_connections,
            "connection_class": Connection,
            # Client related kwargs
            "credential_provider": credential_provider,
            "username": username,
            "password": password,
            "client_name": client_name,
            "driver_info": computed_driver_info,
            # Encoding related kwargs
            "encoding": encoding,
            "encoding_errors": encoding_errors,
            "decode_responses": decode_responses,
            # Connection related kwargs
            "health_check_interval": health_check_interval,
            "socket_connect_timeout": socket_connect_timeout,
            "socket_keepalive": socket_keepalive,
            "socket_keepalive_options": socket_keepalive_options,
            "socket_read_size": socket_read_size,
            "socket_timeout": socket_timeout,
            "protocol": protocol,
            "legacy_responses": legacy_responses,
        }

        if ssl:
            # SSL related kwargs
            kwargs.update(
                {
                    "connection_class": SSLConnection,
                    "ssl_ca_certs": ssl_ca_certs,
                    "ssl_ca_data": ssl_ca_data,
                    "ssl_cert_reqs": ssl_cert_reqs,
                    "ssl_include_verify_flags": ssl_include_verify_flags,
                    "ssl_exclude_verify_flags": ssl_exclude_verify_flags,
                    "ssl_certfile": ssl_certfile,
                    "ssl_check_hostname": ssl_check_hostname,
                    "ssl_keyfile": ssl_keyfile,
                    "ssl_min_version": ssl_min_version,
                    "ssl_ciphers": ssl_ciphers,
                }
            )

        if read_from_replicas or load_balancing_strategy:
            # Call our on_connect function to configure READONLY mode
            kwargs["redis_connect_func"] = self.on_connect

        if retry:
            self.retry = _to_async_retry(retry)
        else:
            self.retry = Retry(
                backoff=ExponentialWithJitterBackoff(
                    base=DEFAULT_RETRY_BASE, cap=DEFAULT_RETRY_CAP
                ),
                retries=cluster_error_retry_attempts,
            )
        if retry_on_error:
            self.retry.update_supported_errors(retry_on_error)

        kwargs["response_callbacks"] = get_response_callbacks(
            user_protocol=kwargs.get("protocol"),
            legacy_responses=kwargs.get("legacy_responses", True),
        )
        if not kwargs.get("legacy_responses", True):
            kwargs["response_callbacks"]["CLUSTER SHARDS"] = (
                parse_cluster_shards_unified
            )
        elif kwargs.get("protocol") is None:
            kwargs["response_callbacks"]["CLUSTER SHARDS"] = (
                parse_cluster_shards_with_str_keys
            )
        else:
            kwargs["response_callbacks"]["CLUSTER SHARDS"] = parse_cluster_shards

        # Build the client-level HIMPORT registry once (always empty at construction)
        # and share the same object with every node connection. It rides in
        # connection_kwargs -> ClusterNode -> each node's Connection, so the registry is
        # shared cluster-wide and runtime himport_prepare mutates one object. (Async has
        # no per-node Redis client, so the object flows via connection_kwargs directly to
        # the Connection, which is internal plumbing, not a public param.)
        self._himport_registry = HImportRegistry()
        kwargs["himport_registry"] = self._himport_registry

        self.connection_kwargs = kwargs

        # Validate maint_notifications_config before NodesManager is constructed
        # so that a bad config doesn't leak an open NodesManager.
        if (
            maint_notifications_config
            and maint_notifications_config.enabled
            and not check_protocol_version(protocol, 3)
        ):
            raise RedisError(
                "Maintenance notifications are only supported with RESP version 3"
            )
        if check_protocol_version(protocol, 3) and maint_notifications_config is None:
            maint_notifications_config = MaintNotificationsConfig()
        # Initialize to None so aclose() and any error-path code never sees an
        # unset slot, even if __init__ raises before the mixin runs.
        self._oss_cluster_maint_notifications_handler = None

        if startup_nodes:
            passed_nodes = []
            for node in startup_nodes:
                passed_nodes.append(
                    ClusterNode(node.host, node.port, **self.connection_kwargs)
                )
            startup_nodes = passed_nodes
        else:
            startup_nodes = []
        if host and port_is_provided:
            startup_nodes.append(ClusterNode(host, port, **self.connection_kwargs))

        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher

        self.startup_nodes = startup_nodes
        self.nodes_manager = NodesManager(
            startup_nodes,
            require_full_coverage,
            kwargs,
            dynamic_startup_nodes=dynamic_startup_nodes,
            address_remap=address_remap,
            event_dispatcher=self._event_dispatcher,
        )
        AsyncMaintNotificationsAbstractRedisCluster.__init__(
            self,
            maint_notifications_config=maint_notifications_config,
            protocol=protocol,
        )
        self.encoder = Encoder(encoding, encoding_errors, decode_responses)
        self.read_from_replicas = read_from_replicas
        self.load_balancing_strategy = load_balancing_strategy
        self.reinitialize_steps = reinitialize_steps
        self.reinitialize_counter = 0

        # For backward compatibility, mapping from existing policies to new one
        self._command_flags_mapping: dict[str, Union[RequestPolicy, ResponsePolicy]] = {
            self.__class__.RANDOM: RequestPolicy.DEFAULT_KEYLESS,
            self.__class__.PRIMARIES: RequestPolicy.ALL_SHARDS,
            self.__class__.ALL_NODES: RequestPolicy.ALL_NODES,
            self.__class__.REPLICAS: RequestPolicy.ALL_REPLICAS,
            self.__class__.DEFAULT_NODE: RequestPolicy.DEFAULT_NODE,
            SLOT_ID: RequestPolicy.DEFAULT_KEYED,
        }

        self._policies_callback_mapping: dict[
            Union[RequestPolicy, ResponsePolicy], Callable
        ] = {
            RequestPolicy.DEFAULT_KEYLESS: lambda command_name: (
                self.get_keyless_target_node(command_name)
            ),
            RequestPolicy.DEFAULT_KEYED: self.get_nodes_from_slot,
            RequestPolicy.DEFAULT_NODE: lambda: [self.get_default_node()],
            RequestPolicy.ALL_SHARDS: self.get_primaries,
            RequestPolicy.ALL_NODES: self.get_nodes,
            RequestPolicy.ALL_REPLICAS: self.get_replicas,
            RequestPolicy.SPECIAL: self.get_special_nodes,
            ResponsePolicy.DEFAULT_KEYLESS: lambda res: res,
            ResponsePolicy.DEFAULT_KEYED: lambda res: res,
        }

        # Built here rather than defaulted in the signature, so that each client owns its
        # resolver and the memos it accumulates are released with the client. The async
        # ClusterPipeline holds the client and reads this attribute, so a pipeline routes by
        # whatever the client routes by without any propagation of its own.
        if metadata_resolver is None:
            self._metadata_resolver: AsyncMetadataResolver = (
                AsyncStaticMetadataResolver()
            )
        else:
            self._metadata_resolver = metadata_resolver

        # ``policy_resolver`` is the routing view of a metadata resolver, so the two
        # arguments overlap. Resolved by precedence rather than by rejecting the
        # combination, because a user migrating from one to the other will legitimately pass
        # both: an explicit ``policy_resolver`` - the extension point that shipped in 7.1.0
        # - keeps deciding which nodes a command targets, and otherwise those policies are
        # derived from the metadata resolver.
        #
        # The precedence covers the routing view only. Replica safety is read from
        # ``_metadata_resolver`` either way, because the projection a policy resolver serves
        # drops the flag it is decided from - a ``CommandPolicies`` record has no
        # ``is_readonly``. Mirrors the sync stack, including the log below.
        if policy_resolver is None:
            self._policy_resolver: AsyncPolicyResolver = AsyncStaticPolicyResolver(
                metadata_resolver=self._metadata_resolver
            )
        else:
            self._policy_resolver = policy_resolver
            if metadata_resolver is not None:
                logger.debug(
                    "Both policy_resolver and metadata_resolver were given; the nodes a "
                    "command targets resolve through policy_resolver and ignore "
                    "metadata_resolver. Replica safety still resolves through "
                    "metadata_resolver."
                )
        self.commands_parser = AsyncCommandsParser()
        self._aggregate_nodes = None
        self.node_flags = self.__class__.NODE_FLAGS.copy()
        self.command_flags = self.__class__.COMMAND_FLAGS.copy()
        self.response_callbacks = kwargs["response_callbacks"]
        self.result_callbacks = self.__class__.RESULT_CALLBACKS.copy()
        self.result_callbacks["CLUSTER SLOTS"] = lambda cmd, res, **kwargs: (
            parse_cluster_slots(list(res.values())[0], **kwargs)
        )

        self._initialize = True
        self._lock: Optional[asyncio.Lock] = None
        # Set once a refresh has finished, commands parser included, and cleared
        # by aclose(): the default node is published before the first refresh
        # finishes, so it does not tell whether the topology can serve yet
        self._topology_ready = False

        # When used as an async context manager, we need to increment and decrement
        # a usage counter so that we can close the connection pool when no one is
        # using the client.
        self._usage_counter = 0
        self._usage_lock = asyncio.Lock()

    def _initialize_due(self) -> bool:
        """
        Whether initialize() has work to do now: the client has no topology
        yet, or a refresh was asked for, is due and no other task is running
        it. The command, pipeline and transaction attempts check this before
        awaiting initialize(), so an attempt made while a requested refresh is
        not due yet costs no coroutine.
        """
        return self._initialize and not (
            self._topology_ready
            and (
                not self.nodes_manager._refresh_due(self.retry)
                or (self._lock is not None and self._lock.locked())
            )
        )

    async def initialize(
        self,
        additional_startup_nodes_info: Optional[List[Tuple[str, int]]] = None,
        last_failed_node_name: Optional[str] = None,
    ) -> "RedisCluster":
        """
        Get all nodes from startup nodes & creates connections if not initialized.

        Once initialized, this refreshes the topology only when one was asked
        for (``_initialize`` is set by the error paths), and no sooner than
        ``_TOPOLOGY_REFRESH_MIN_INTERVAL_SECONDS`` after the previous refresh,
        or longer after refreshes that failed (see ``_topology_refresh_delay``).
        Until then it returns at once and the current topology keeps serving.
        It also returns at once while another task runs the refresh: only the
        first initialize, which has no topology to serve yet, makes the other
        callers wait for it.
        A refresh that reached no node, or whose nodes answered with a topology
        that cannot replace the current one (slots not covered, or nodes that
        disagree on them), is logged and asked for again instead of raised; the
        current topology keeps serving until a refresh succeeds.
        """
        if self._initialize:
            if not self._initialize_due():
                # A refresh was asked for but is not due yet, or another task
                # is running it; the current topology keeps serving. The flag
                # stays until a refresh succeeds, so a later call runs it
                return self
            if not self._lock:
                self._lock = asyncio.Lock()
            async with self._lock:
                if self._initialize:
                    # A client that never initialized (or was closed since)
                    # has no topology to serve yet
                    first_initialize = not self._topology_ready
                    if not first_initialize and not self.nodes_manager._refresh_due(
                        self.retry
                    ):
                        # A refresh that failed while this task waited for the
                        # lock moved the next one out; the current topology
                        # keeps serving until it is due
                        return self
                    requested_at_epoch = self.nodes_manager._refresh_requested_at_epoch
                    if (
                        not first_initialize
                        and requested_at_epoch is not None
                        and requested_at_epoch != self.nodes_manager._epoch
                    ):
                        # A refresh completed after the last request (for
                        # example one SMIGRATED ran) and counts as answering
                        # it, the nodes it named included
                        self.nodes_manager._refresh_requested_at_epoch = None
                        self.nodes_manager._requested_last_failed_node_name = None
                        self.nodes_manager._requested_additional_startup_nodes = ()
                        self._initialize = False
                        return self
                    try:
                        await self._refresh_topology(
                            additional_startup_nodes_info=additional_startup_nodes_info,
                            last_failed_node_name=last_failed_node_name,
                        )
                        self._initialize = False
                        self._topology_ready = True
                        self.nodes_manager._refresh_requested_at_epoch = None
                    except BaseException as e:
                        # Only a client that never initialized is torn down. A
                        # failed or cancelled refresh keeps the connections of
                        # the current topology, which every other task is using,
                        # and leaves _initialize set so the next command retries.
                        if first_initialize:
                            await self.nodes_manager.aclose()
                            await self.nodes_manager.aclose("startup_nodes")
                            raise
                        if isinstance(e, RedisClusterException):
                            # No node answered, or nodes answered with a
                            # topology that cannot replace the current one. The
                            # current topology keeps serving until a refresh
                            # succeeds: this may be a command for a healthy
                            # shard, which must not fail for a refresh another
                            # error asked for, and the nodes queried may be only
                            # seeds (dynamic_startup_nodes=False) while the
                            # nodes of the current topology still answer. The
                            # failure is counted, so the next refresh runs once
                            # the backoff allows.
                            logger.error(
                                f"Topology refresh failed, keeping the current one: {e}"
                            )
                            return self
                        raise
        return self

    async def _refresh_topology(
        self,
        additional_startup_nodes_info: Optional[List[Tuple[str, int]]] = None,
        last_failed_node_name: Optional[str] = None,
    ) -> None:
        """
        Refresh the nodes and slots caches, and point the commands parser at a
        node of the current topology.
        """
        await self.nodes_manager.initialize(
            additional_startup_nodes_info=additional_startup_nodes_info,
            last_failed_node_name=last_failed_node_name,
        )
        if self.commands_parser.commands:
            # The command table is fetched once, as on the sync client. A command
            # missing from it (e.g. added by a server upgrade) makes get_keys
            # fetch it again, from this node, so the node must be one that is
            # still part of the cluster; it also serves COMMAND GETKEYS
            self.commands_parser.node = self.nodes_manager.default_node
        else:
            await self.commands_parser.initialize(self.nodes_manager.default_node)

    def _request_refresh(
        self,
        additional_startup_nodes_info: Optional[List[Tuple[str, int]]] = None,
        last_failed_node_name: Optional[str] = None,
    ) -> None:
        """
        Ask for a topology refresh after an error. Requests are merged until
        initialize() runs them, once due. The nodes they name are kept until a
        refresh succeeds, so the refresh uses them whichever command runs it.
        """
        nodes_manager = self.nodes_manager
        if additional_startup_nodes_info:
            requested = nodes_manager._requested_additional_startup_nodes
            new_nodes = tuple(
                node_info
                for node_info in additional_startup_nodes_info
                if node_info not in requested
            )
            if new_nodes:
                nodes_manager._requested_additional_startup_nodes = (
                    *requested,
                    *new_nodes,
                )
        if last_failed_node_name is not None:
            nodes_manager._requested_last_failed_node_name = last_failed_node_name
        # The epoch of the newest request. A refresh that completes after it
        # counts as answering every merged request, even one that started
        # before the error and read a slot map from before it: an error that
        # outlives it asks again
        nodes_manager._refresh_requested_at_epoch = nodes_manager._epoch
        self._initialize = True

    async def aclose(self) -> None:
        """Close all connections & client if initialized."""
        # Initialized means the client has a topology (a default node), not that
        # _initialize is clear: the error paths set _initialize to ask for a
        # refresh, and a client closed before that refresh ran must still close
        if self.nodes_manager.default_node is not None:
            if not self._lock:
                self._lock = asyncio.Lock()
            async with self._lock:
                if self.nodes_manager.default_node is not None:
                    self._initialize = True
                    self._topology_ready = False
                    if self._oss_cluster_maint_notifications_handler:
                        tasks = list(
                            self._oss_cluster_maint_notifications_handler._background_tasks
                        )
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                    await self.nodes_manager.aclose()
                    await self.nodes_manager.aclose("startup_nodes")

    @deprecated_function(version="5.0.0", reason="Use aclose() instead", name="close")
    async def close(self) -> None:
        """alias for aclose() for backwards compatibility"""
        await self.aclose()

    async def __aenter__(self) -> "RedisCluster":
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

    def __await__(self) -> Generator[Any, None, "RedisCluster"]:
        return self.initialize().__await__()

    _DEL_MESSAGE = "Unclosed RedisCluster client"

    def __del__(
        self,
        _warn: Any = warnings.warn,
        _grl: Any = asyncio.get_running_loop,
    ) -> None:
        # Initialized means the client has a topology, as in aclose(): an error
        # path can leave _initialize set on a client that is still open
        nodes_manager = getattr(self, "nodes_manager", None)
        if nodes_manager is not None and nodes_manager.default_node is not None:
            _warn(f"{self._DEL_MESSAGE} {self!r}", ResourceWarning, source=self)
            try:
                context = {"client": self, "message": self._DEL_MESSAGE}
                _grl().call_exception_handler(context)
            except RuntimeError:
                pass

    async def on_connect(self, connection: Connection) -> None:
        await connection.on_connect()

        # Sending READONLY command to server to configure connection as
        # readonly. Since each cluster node may change its server type due
        # to a failover, we should establish a READONLY connection
        # regardless of the server type. If this is a primary connection,
        # READONLY would not affect executing write commands.
        await connection.send_command("READONLY")
        if str_if_bytes(await connection.read_response()) != "OK":
            raise ConnectionError("READONLY command failed")

    def get_nodes(self) -> List["ClusterNode"]:
        """Get all nodes of the cluster."""
        return list(self.nodes_manager.nodes_cache.values())

    def get_primaries(self) -> List["ClusterNode"]:
        """Get the primary nodes of the cluster."""
        return self.nodes_manager.get_nodes_by_server_type(PRIMARY)

    def get_replicas(self) -> List["ClusterNode"]:
        """Get the replica nodes of the cluster."""
        return self.nodes_manager.get_nodes_by_server_type(REPLICA)

    def get_random_node(self) -> "ClusterNode":
        """Get a random node of the cluster."""
        return random.choice(list(self.nodes_manager.nodes_cache.values()))

    def get_default_node(self) -> "ClusterNode":
        """Get the default node of the client."""
        return self.nodes_manager.default_node

    def set_default_node(self, node: "ClusterNode") -> None:
        """
        Set the default node of the client.

        :raises DataError: if None is passed or node does not exist in cluster.
        """
        if not node or not self.get_node(node_name=node.name):
            raise DataError("The requested node does not exist in the cluster.")

        self.nodes_manager.default_node = node

    def get_node(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        node_name: Optional[str] = None,
    ) -> Optional["ClusterNode"]:
        """Get node by (host, port) or node_name."""
        return self.nodes_manager.get_node(host, port, node_name)

    def get_node_from_key(
        self, key: str, replica: bool = False
    ) -> Optional["ClusterNode"]:
        """
        Get the cluster node corresponding to the provided key.

        :param key:
        :param replica:
            | Indicates if a replica should be returned
            |
              None will returned if no replica holds this key

        :raises SlotNotCoveredError: if the key is not covered by any slot.
        """
        slot = self.keyslot(key)
        slot_cache = self.nodes_manager.slots_cache.get(slot)
        if not slot_cache:
            raise SlotNotCoveredError(f'Slot "{slot}" is not covered by the cluster.')

        if replica:
            if len(self.nodes_manager.slots_cache[slot]) < 2:
                return None
            node_idx = 1
        else:
            node_idx = 0

        return slot_cache[node_idx]

    async def get_keyless_target_node(self, command_name: str) -> "ClusterNode":
        """
        Returns the node a keyless command is routed to: a random node when replica reads
        are enabled and the command is safe to serve from a replica, a random primary
        otherwise.

        A replicas-only ``load_balancing_strategy`` is honored by picking from the replicas
        alone, so a strategy that asks for replicas cannot land on a primary here. The
        strategy is not applied any further than that: the rest of it is an index into one
        shard's node list and a round-robin counter kept per primary name, and a keyless
        command has no shard to index - so the pick is uniform over the eligible nodes.

        Falls back to the whole node set when the cluster has no replicas to pick from,
        which is every primary. That is also the answer for the two strategies that
        include the primary, and for ``read_from_replicas`` on its own, which is what this
        method has returned for a replica-safe command since 7.1.0.
        """
        replica_safe = (
            self.read_from_replicas or self.load_balancing_strategy is not None
        ) and await self._is_replica_safe(command_name)
        if replica_safe:
            if self.load_balancing_strategy in _REPLICAS_ONLY_STRATEGIES:
                replicas = self.get_replicas()
                if replicas:
                    return random.choice(replicas)

            return self.get_random_node()

        return self.get_random_primary_node()

    @deprecated_function(
        version="8.2.0",
        reason="Use get_keyless_target_node() instead.",
    )
    def get_random_primary_or_all_nodes(self, command_name: str) -> "ClusterNode":
        """
        Returns random primary or all nodes depends on READONLY mode.

        Deprecated wrapper over :meth:`get_keyless_target_node`, kept so the name that
        has been public since 7.1.0 keeps working. Replica safety is resolved through the
        metadata resolver, which is awaitable, so the coroutine runs on its own event loop
        in a worker thread to keep this entry point synchronous.

        .. warning::

           **Do not extend this method, and do not call it from a running event loop.**
           Keeping the 7.1.0 signature synchronous costs a thread bridge
           (``_run_coroutine_in_thread``) that blocks the calling thread until the
           resolver answers. Two rules follow, and breaking either one stalls the caller
           permanently rather than raising:

           1. **Override the right method.** A subclass that customizes keyless routing
              must override :meth:`get_keyless_target_node`, the coroutine the client
              actually awaits. Overriding this name changes nothing, because no code path
              inside the client calls it.
           2. **Keep a custom resolver synchronous in effect.** A caller-supplied
              ``AsyncMetadataResolver`` must implement ``is_replica_safe`` as a
              self-contained in-memory lookup. One that awaits work owned by the caller's
              loop - I/O over a pooled connection, say - waits on a loop this bridge has
              already blocked, and hangs.

        That bridge is what confines this method to the deprecation window. It is sound
        for the resolvers the library ships, whose ``is_replica_safe`` suspends on nothing,
        and nothing inside the client reaches this path: keyless routing goes through
        :meth:`get_keyless_target_node`, which is awaited normally. So the hazard is
        confined to callers of this deprecated name.
        """
        return _run_coroutine_in_thread(self.get_keyless_target_node(command_name))

    async def _is_replica_safe(self, command_name: str) -> bool:
        return await self._metadata_resolver.is_replica_safe(command_name)

    def get_random_primary_node(self) -> "ClusterNode":
        """
        Returns a random primary node
        """
        return random.choice(self.get_primaries())

    async def get_nodes_from_slot(self, command: str, *args):
        """
        Returns a list of nodes that hold the specified keys' slots.
        """
        # get the node that holds the key's slot
        replica_safe = (
            self.read_from_replicas or self.load_balancing_strategy is not None
        ) and await self._is_replica_safe(command)
        return [
            self.nodes_manager.get_node_from_slot(
                await self._determine_slot(command, *args),
                replica_safe,
                self.load_balancing_strategy if replica_safe else None,
            )
        ]

    def get_special_nodes(self) -> Optional[list["ClusterNode"]]:
        """
        Returns a list of nodes for commands with a special policy.
        """
        if not self._aggregate_nodes:
            raise RedisClusterException(
                "Cannot execute FT.CURSOR commands without FT.AGGREGATE"
            )

        return self._aggregate_nodes

    def keyslot(self, key: EncodableT) -> int:
        """
        Find the keyslot for a given key.

        See: https://redis.io/docs/manual/scaling/#redis-cluster-data-sharding
        """
        return key_slot(self.encoder.encode(key))

    # HIMPORT orchestration (async mirror of redis.cluster.RedisCluster). The one
    # shared HImportRegistry is mutated once by PREPARE/DISCARD/DISCARDALL and applied
    # lazily per node; SET routes by key slot to the owning primary's ClusterNode.
    # See ``.agents/himport_client_support_spec.md``.

    @property
    def himport_registry(self) -> HImportRegistry:
        """The cluster-wide HIMPORT fieldset registry (empty if none was declared).

        Read-only: the registry is mutated only through the HIMPORT command methods.
        """
        return self._himport_registry

    @experimental_method()
    async def himport_prepare(
        self, fieldset_name: str, fields: Iterable[FieldT]
    ) -> bool:
        """Declare an HIMPORT fieldset cluster-wide (shared registry, applied lazily)."""
        await self.initialize()
        self._himport_registry.prepare(fieldset_name, fields)
        return True

    @experimental_method()
    async def himport_discard(self, fieldset_name: str) -> int:
        """Remove an HIMPORT fieldset cluster-wide (shared registry, applied lazily)."""
        await self.initialize()
        return 1 if self._himport_registry.discard(fieldset_name) else 0

    @experimental_method()
    async def himport_discard_all(self) -> int:
        """Remove all HIMPORT fieldsets cluster-wide (shared registry, applied lazily)."""
        await self.initialize()
        return self._himport_registry.discard_all()

    def get_encoder(self) -> Encoder:
        """Get the encoder object of the client."""
        return self.encoder

    def get_connection_kwargs(self) -> Dict[str, Optional[Any]]:
        """Get the kwargs passed to :class:`~redis.asyncio.connection.Connection`."""
        return self.connection_kwargs

    def set_retry(self, retry: Retry) -> None:
        self.retry = _to_async_retry(retry)

    def set_response_callback(self, command: str, callback: ResponseCallbackT) -> None:
        """Set a custom response callback."""
        self.response_callbacks[command] = callback

    async def _resolve_command_policies(
        self, *args: Any, target_nodes_specified: bool = False
    ) -> Tuple[str, Union[CommandPolicies, CommandMetadata]]:
        """
        Resolves the policies a command routes and aggregates by.

        Returns the name the policies were decided by along with the record, because the
        name a command is known by is not always ``args[0]``: a container command is
        keyed by both of its words, and the flag tables are keyed in upper case. Callers
        that look the command up elsewhere - result callbacks, observability - use the
        returned name so they agree with the routing decision.

        The name is normalized before any branch, so one command answers with one name
        however it got here. The result callbacks are keyed in upper case, so a name that
        kept the caller's spelling on only some paths would fire them on only some paths -
        ``execute_command("dbsize")`` would be summed and ``execute_command("dbsize",
        target_nodes=...)`` would not.

        First choice is the policy resolver. When it does not know the command, the
        fallbacks are, in order: nothing to route at all because the caller named its
        targets, the command's ``COMMAND_FLAGS`` entry, and finally whether the command
        carries a key.
        """
        command = args[0].upper()
        if len(args) >= 2 and f"{args[0]} {args[1]}".upper() in self.command_flags:
            command = f"{args[0]} {args[1]}".upper()

        if target_nodes_specified:
            # The caller named its targets, so nothing is routed from here - and the
            # command's own aggregation must not apply either. A response policy resolved
            # for the whole cluster (``ONE_SUCCEEDED``, an ``AGG_*``) would short-circuit
            # the loop over the nodes the caller picked, or fold their replies into one.
            # Answer with the record that aggregates nothing, and skip the resolver: with
            # the targets given, neither of its answers is used.
            return command, _DEFAULT_KEYLESS_METADATA

        policies = await self._policy_resolver.resolve(args[0].lower())
        if policies:
            return command, policies

        command_flag = self.command_flags.get(command)
        if command_flag:
            if command_flag in self._command_flags_mapping:
                return command, _METADATA_BY_REQUEST_POLICY[
                    self._command_flags_mapping[command_flag]
                ]
            return command, _DEFAULT_KEYLESS_METADATA

        # Unflagged and unresolved, so the command routes by its key. Without a default
        # node the topology is not known yet and there is no slot to route by.
        if not self.get_default_node():
            return command, _DEFAULT_KEYLESS_METADATA

        slot = await self._determine_slot(*args)
        if slot is None:
            return command, _DEFAULT_KEYLESS_METADATA

        return command, _DEFAULT_KEYED_METADATA

    async def _determine_nodes(
        self,
        command: str,
        *args: Any,
        request_policy: Optional[RequestPolicy] = None,
        node_flag: Optional[str] = None,
    ) -> List["ClusterNode"]:
        # Determine which nodes should be executed the command on.
        # Returns a list of target nodes.
        # The caller resolves the command's own policy - see
        # ``_resolve_command_policies`` - so the only decision left here is an explicit
        # node flag, which overrides it.
        if node_flag and self._is_node_flag(node_flag):
            if node_flag in self._command_flags_mapping:
                request_policy = self._command_flags_mapping[node_flag]

        if request_policy is None:
            raise RedisClusterException(
                f"No targets were found to execute {command} command on"
            )

        policy_callback = self._policies_callback_mapping[request_policy]

        if request_policy == RequestPolicy.DEFAULT_KEYED:
            nodes = await policy_callback(command, *args)
        elif request_policy == RequestPolicy.DEFAULT_KEYLESS:
            nodes = [await policy_callback(command)]
        else:
            nodes = policy_callback()

        if command.lower() == "ft.aggregate":
            self._aggregate_nodes = nodes

        return nodes

    async def _determine_slot(self, command: str, *args: Any) -> int:
        if self.command_flags.get(command.upper()) == SLOT_ID:
            # The command contains the slot ID
            return int(args[0])

        # Get the keys in the command

        # EVAL and EVALSHA are common enough that it's wasteful to go to the
        # redis server to parse the keys. Besides, there is a bug in redis<7.0
        # where `self._get_command_keys()` fails anyway. So, we special case
        # EVAL/EVALSHA.
        # - issue: https://github.com/redis/redis/issues/9493
        # - fix: https://github.com/redis/redis/pull/9733
        if command.upper() in ("EVAL", "EVALSHA"):
            # command syntax: EVAL "script body" num_keys ...
            if len(args) < 2:
                raise RedisClusterException(
                    f"Invalid args in command: {command, *args}"
                )
            keys = args[2 : 2 + int(args[1])]
            # if there are 0 keys, that means the script can be run on any node
            # so we can just return a random slot
            if not keys:
                return random.randrange(0, REDIS_CLUSTER_HASH_SLOTS)
        else:
            keys = await self.commands_parser.get_keys(command, *args)
            if not keys:
                # FCALL can call a function with 0 keys, that means the function
                #  can be run on any node so we can just return a random slot
                if command.upper() in ("FCALL", "FCALL_RO"):
                    return random.randrange(0, REDIS_CLUSTER_HASH_SLOTS)
                raise RedisClusterException(
                    "No way to dispatch this command to Redis Cluster. "
                    "Missing key.\nYou can execute the command by specifying "
                    f"target nodes.\nCommand: {args}"
                )

        # single key command
        if len(keys) == 1:
            return self.keyslot(keys[0])

        # multi-key command; we need to make sure all keys are mapped to
        # the same slot
        slots = {self.keyslot(key) for key in keys}
        if len(slots) != 1:
            raise RedisClusterException(
                f"{command} - all keys must map to the same key slot"
            )

        return slots.pop()

    def _is_node_flag(self, target_nodes: Any) -> bool:
        return isinstance(target_nodes, str) and target_nodes in self.node_flags

    def _parse_target_nodes(self, target_nodes: Any) -> List["ClusterNode"]:
        if isinstance(target_nodes, list):
            nodes = target_nodes
        elif isinstance(target_nodes, ClusterNode):
            # Supports passing a single ClusterNode as a variable
            nodes = [target_nodes]
        elif isinstance(target_nodes, dict):
            # Supports dictionaries of the format {node_name: node}.
            # It enables to execute commands with multi nodes as follows:
            # rc.cluster_save_config(rc.get_primaries())
            nodes = list(target_nodes.values())
        else:
            raise TypeError(
                "target_nodes type can be one of the following: "
                "node_flag (PRIMARIES, REPLICAS, RANDOM, ALL_NODES),"
                "ClusterNode, list<ClusterNode>, or dict<any, ClusterNode>. "
                f"The passed type is {type(target_nodes)}"
            )
        return nodes

    async def _record_error_metric(
        self,
        error: Exception,
        connection: Union[Connection, "ClusterNode"],
        is_internal: bool = True,
        retry_attempts: Optional[int] = None,
    ):
        """
        Records error count metric directly.
        Accepts either a Connection or ClusterNode object.
        """
        await record_error_count(
            server_address=connection.host,
            server_port=connection.port,
            network_peer_address=connection.host,
            network_peer_port=connection.port,
            error_type=error,
            retry_attempts=retry_attempts if retry_attempts is not None else 0,
            is_internal=is_internal,
        )

    async def _record_command_metric(
        self,
        command_name: str,
        duration_seconds: float,
        connection: Union[Connection, "ClusterNode"],
        error: Optional[Exception] = None,
    ):
        """
        Records operation duration metric directly.
        Accepts either a Connection or ClusterNode object.
        """
        # Connection has db attribute, ClusterNode has connection_kwargs
        if hasattr(connection, "db"):
            db = connection.db
        else:
            db = connection.connection_kwargs.get("db", 0)
        await record_operation_duration(
            command_name=command_name,
            duration_seconds=duration_seconds,
            server_address=connection.host,
            server_port=connection.port,
            db_namespace=str(db) if db is not None else None,
            error=error,
        )

    async def execute_command(self, *args: EncodableT, **kwargs: Any) -> Any:
        """
        Execute a raw command on the appropriate cluster node or target_nodes.

        It will retry the command as specified by the retries property of
        the :attr:`retry` & then raise an exception.

        :param args:
            | Raw command args
        :param kwargs:

            - target_nodes: :attr:`NODE_FLAGS` or :class:`~.ClusterNode`
              or List[:class:`~.ClusterNode`] or Dict[Any, :class:`~.ClusterNode`]
            - Rest of the kwargs are passed to the Redis connection

        :raises RedisClusterException: if target_nodes is not provided & the command
            can't be mapped to a slot
        """
        target_nodes = []
        target_nodes_specified = False
        retry_attempts = self.retry.get_retries()

        passed_targets = kwargs.pop("target_nodes", None)
        if (
            passed_targets is not None
            and not self._is_node_flag(passed_targets)
            and not (
                isinstance(passed_targets, (list, dict, str)) and not passed_targets
            )
        ):
            target_nodes = self._parse_target_nodes(passed_targets)
            target_nodes_specified = True
            retry_attempts = 0

        command, command_policies = await self._resolve_command_policies(
            *args, target_nodes_specified=target_nodes_specified
        )

        # Add one for the first execution
        execute_attempts = 1 + retry_attempts
        failure_count = 0

        # Start timing for observability
        start_time = time.monotonic()

        # The error the previous attempt failed with, and when the last refresh
        # had ended by then: a refresh since that reached no node ends the
        # retries (see NodesManager._error_instead_of_retry)
        last_error: Optional[Exception] = None
        last_refresh_at: Optional[float] = None

        # Whether the attempt targets the default node alone: only an attempt
        # that failed on it replaces it, not one made while a refresh another
        # error asked for is pending
        is_default_node = False

        for _ in range(execute_attempts):
            if self._initialize_due():
                # The node that failed is kept on the nodes manager, so the
                # refresh tries it last even when it runs on a later command
                await self.initialize()
            if last_error is not None:
                outage_error = self.nodes_manager._error_instead_of_retry(
                    last_error, last_refresh_at
                )
                if outage_error is not None:
                    raise outage_error
            try:
                if not target_nodes_specified:
                    # Determine the nodes to execute the command on
                    target_nodes = await self._determine_nodes(
                        *args,
                        request_policy=command_policies.request_policy,
                        node_flag=passed_targets,
                    )
                    if not target_nodes:
                        raise RedisClusterException(
                            f"No targets were found to execute {args} command on"
                        )
                    is_default_node = (
                        len(target_nodes) == 1
                        and target_nodes[0] == self.get_default_node()
                    )

                if len(target_nodes) == 1:
                    # Return the processed result
                    ret = await self._execute_command(target_nodes[0], *args, **kwargs)
                    if command in self.result_callbacks:
                        ret = self.result_callbacks[command](
                            command, {target_nodes[0].name: ret}, **kwargs
                        )
                    return self._policies_callback_mapping[
                        command_policies.response_policy
                    ](ret)
                else:
                    keys = [node.name for node in target_nodes]
                    values = await asyncio.gather(
                        *(
                            asyncio.create_task(
                                self._execute_command(node, *args, **kwargs)
                            )
                            for node in target_nodes
                        )
                    )
                    if command in self.result_callbacks:
                        return self.result_callbacks[command](
                            command, dict(zip(keys, values)), **kwargs
                        )
                    return self._policies_callback_mapping[
                        command_policies.response_policy
                    ](dict(zip(keys, values)))
            except Exception as e:
                if retry_attempts > 0 and type(e) in self.__class__.ERRORS_ALLOW_RETRY:
                    if is_default_node:
                        # Replace the default cluster node
                        self.replace_default_node()
                    # The nodes and slots cache were should be reinitialized.
                    # Try again with the new cluster setup.
                    retry_attempts -= 1
                    failure_count += 1
                    last_error = e
                    last_refresh_at = self.nodes_manager._last_refresh_at

                    if hasattr(e, "connection"):
                        # ``args[0]``, not the resolved ``command``: every other metric in
                        # this class - including the per-command one ``_execute_command``
                        # records on success - names the command the caller spelled. Using
                        # the routed name only here would report one command under two
                        # names depending on whether it was retried.
                        await self._record_command_metric(
                            command_name=args[0],
                            duration_seconds=time.monotonic() - start_time,
                            connection=e.connection,
                            error=e,
                        )
                        await self._record_error_metric(
                            error=e,
                            connection=e.connection,
                            retry_attempts=failure_count,
                        )
                    # Space the attempts out as the retry backoff says; the
                    # refresh the error asked for runs after this delay
                    delay = self._retry_backoff_delay(self.retry, failure_count)
                    if delay > 0:
                        await asyncio.sleep(delay)
                    continue
                else:
                    # raise the exception
                    if hasattr(e, "connection"):
                        await self._record_error_metric(
                            error=e,
                            connection=e.connection,
                            retry_attempts=failure_count,
                            is_internal=False,
                        )
                    raise _error_to_raise(
                        self.nodes_manager._last_refresh_unreachable_args, e
                    )

    async def _execute_command(
        self, target_node: "ClusterNode", *args: Union[KeyT, EncodableT], **kwargs: Any
    ) -> Any:
        asking = moved = False
        redirect_addr = None
        ttl = self.RedisClusterRequestTTL
        command = args[0]
        start_time = time.monotonic()

        while ttl > 0:
            ttl -= 1
            ask_himport = False
            try:
                if asking:
                    target_node = self.get_node(*redirect_addr)
                    if parse_himport_set_args(args) is not None:
                        # ASKING must sit on the same connection as the SET,
                        # immediately before it. HIMPORT SET's own executor folds
                        # ASKING into the SET's packed write after the session setup,
                        # so don't send it here as a separately pooled command.
                        ask_himport = True
                    else:
                        await target_node.execute_command("ASKING")
                    asking = False
                elif moved:
                    # MOVED occurred and the slots cache was updated,
                    # refresh the target node
                    slot = await self._determine_slot(*args)
                    replica_safe = (
                        self.read_from_replicas
                        or self.load_balancing_strategy is not None
                    ) and await self._is_replica_safe(args[0])
                    target_node = self.nodes_manager.get_node_from_slot(
                        slot,
                        replica_safe,
                        self.load_balancing_strategy if replica_safe else None,
                    )
                    moved = False

                response = await target_node.execute_command(
                    *args, asking=ask_himport, **kwargs
                )
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                )
                return response
            except BusyLoadingError as e:
                e.connection = target_node
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                    error=e,
                )
                raise
            except AuthenticationError as e:
                # Rejected or expired credentials are not a node failure:
                # recycling the node's connections or refreshing the topology
                # cannot repair them, so re-raise without either.
                e.connection = target_node
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                    error=e,
                )
                raise
            except MaxConnectionsError as e:
                # MaxConnectionsError indicates client-side resource exhaustion
                # (too many connections in the pool), not a node failure.
                # Don't treat this as a node failure - just re-raise the error
                # without reinitializing the cluster.
                e.connection = target_node
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                    error=e,
                )
                raise
            except MaxClientsReachedError as e:
                # The server refused a new connection because it is at its
                # maxclients limit: server-side capacity, not a node failure.
                # Recycling the node's working connections or refreshing the
                # topology would only open more connections the server refuses,
                # so re-raise without either. The connection that read the error
                # has already disconnected itself: read_response drops the
                # connection on any error it raises.
                e.connection = target_node
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                    error=e,
                )
                raise
            except (ConnectionError, TimeoutError) as e:
                # Connection retries are being handled in the node's
                # Retry object. The failed connection has disconnected itself;
                # the node's in-use connections are kept, and a timeout keeps the
                # free ones too: closing them all would reconnect every caller of
                # a node that may be healthy. After a connection error the free
                # ones are reconnected lazily on acquire, since the server may
                # have closed them all and the retries would take them one by one.
                target_node._mark_free_connections_for_reconnect_on_error(e)

                # Move the failed node to the end of the cached nodes. The
                # refresh keeps that order, so it does not pick this node as the
                # default node while it is still a primary. The order the
                # refresh queries nodes in comes from last_failed_node_name
                # below: it shuffles the startup nodes and tries this one last.
                self.nodes_manager.move_node_to_end_of_cached_nodes(target_node.name)
                e.last_failed_node_name = target_node.name

                # Signal that reinitialization is needed
                # The retry loop will handle initialize() AND replace_default_node()
                self._request_refresh(last_failed_node_name=target_node.name)
                e.connection = target_node
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                    error=e,
                )
                raise
            except (ClusterDownError, SlotNotCoveredError) as e:
                # ClusterDownError can occur during a failover and to get
                # self-healed, we will try to reinitialize the cluster layout
                # and retry executing the command

                # SlotNotCoveredError can occur when the cluster is not fully
                # initialized or can be temporary issue.
                # We will try to reinitialize the cluster topology
                # and retry executing the command

                await asyncio.sleep(0.25)
                # Refreshed here, in place and without closing the connections,
                # not paced or asked for: this command raises the refresh error
                # if it fails. Under the lock, as in initialize(), so aclose()
                # waits for it instead of closing a client this refresh then
                # reopens. A client closed meanwhile is left to the next
                # attempt's initialize(), and a refresh another task completed
                # meanwhile already answers this error.
                epoch = self.nodes_manager._epoch
                async with self._lock:
                    if self._topology_ready and self.nodes_manager._epoch == epoch:
                        await self._refresh_topology()
                        self._initialize = False
                e.connection = target_node
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                    error=e,
                )
                raise
            except MovedError as e:
                # Patch the slots/nodes cache with the redirected node and try
                # again. Every 'reinitialize_steps' MOVED errors, also ask for a
                # refresh of the whole topology, run by the next attempt of any
                # command once due: the redirect needs only this slot, which the
                # patch already points to the node that owns it.
                # 'reinitialize_steps' counter will increase faster when
                # the same client object is shared between multiple tasks. To
                # reduce the frequency you can set this variable in the
                # RedisCluster constructor.
                self.reinitialize_counter += 1
                await self.nodes_manager.move_slot(e)
                if _should_request_refresh(
                    self.reinitialize_steps, self.reinitialize_counter
                ):
                    # The refresh also queries the node the error pointed to
                    self._request_refresh(
                        additional_startup_nodes_info=[
                            self.nodes_manager.remap_host_port(e.host, e.port)
                        ]
                    )
                    # Reset the counter
                    self.reinitialize_counter = 0
                moved = True
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                    error=e,
                )
                await self._record_error_metric(
                    error=e,
                    connection=target_node,
                )
            except AskError as e:
                redirect_addr = self.nodes_manager.remap_host_port(e.host, e.port)
                asking = True
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                    error=e,
                )
                await self._record_error_metric(
                    error=e,
                    connection=target_node,
                )
            except TryAgainError as e:
                if ttl < self.RedisClusterRequestTTL / 2:
                    await asyncio.sleep(0.05)
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                    error=e,
                )
                await self._record_error_metric(
                    error=e,
                    connection=target_node,
                )
            except ResponseError as e:
                e.connection = target_node
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                    error=e,
                )
                raise
            except Exception as e:
                e.connection = target_node
                await self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=target_node,
                    error=e,
                )
                raise

        e = ClusterError("TTL exhausted.")
        e.connection = target_node
        await self._record_command_metric(
            command_name=command,
            duration_seconds=time.monotonic() - start_time,
            connection=target_node,
            error=e,
        )
        raise e

    def pipeline(
        self, transaction: Optional[Any] = None, shard_hint: Optional[Any] = None
    ) -> "ClusterPipeline":
        """
        Create & return a new :class:`~.ClusterPipeline` object.

        Cluster implementation of pipeline does not support transaction or shard_hint.

        :raises RedisClusterException: if transaction or shard_hint are truthy values
        """
        if shard_hint:
            raise RedisClusterException("shard_hint is deprecated in cluster mode")

        return ClusterPipeline(self, transaction)

    def pubsub(
        self,
        node: Optional["ClusterNode"] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        **kwargs: Any,
    ) -> "ClusterPubSub":
        """
        Create and return a ClusterPubSub instance.

        Allows passing a ClusterNode, or host&port, to get a pubsub instance
        connected to the specified node

        :param node: ClusterNode to connect to
        :param host: Host of the node to connect to
        :param port: Port of the node to connect to
        :param kwargs: Additional keyword arguments
        :return: ClusterPubSub instance
        """
        return ClusterPubSub(self, node=node, host=host, port=port, **kwargs)

    def keyspace_notifications(
        self,
        key_prefix: Union[str, bytes, None] = None,
        ignore_subscribe_messages: bool = True,
    ) -> "AsyncClusterKeyspaceNotifications":
        """
        Return an
        :class:`~redis.asyncio.keyspace_notifications.AsyncClusterKeyspaceNotifications`
        object for subscribing to keyspace and keyevent notifications across
        all primary nodes in the cluster.

        Note: Keyspace notifications must be enabled on all Redis cluster nodes
        via the ``notify-keyspace-events`` configuration option.

        Args:
            key_prefix: Optional prefix to filter and strip from keys in
                        notifications.
            ignore_subscribe_messages: If True, subscribe/unsubscribe
                                      confirmations are not returned by
                                      get_message/listen.
        """
        from redis.asyncio.keyspace_notifications import (
            AsyncClusterKeyspaceNotifications,
        )

        return AsyncClusterKeyspaceNotifications(
            self,
            key_prefix=key_prefix,
            ignore_subscribe_messages=ignore_subscribe_messages,
        )

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

    async def transaction(
        self, func: Coroutine[None, "ClusterPipeline", Any], *watches, **kwargs
    ):
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single argument which is a Pipeline object.
        """
        shard_hint = kwargs.pop("shard_hint", None)
        value_from_callable = kwargs.pop("value_from_callable", False)
        watch_delay = kwargs.pop("watch_delay", None)
        async with self.pipeline(True, shard_hint) as pipe:
            while True:
                try:
                    if watches:
                        await pipe.watch(*watches)
                    func_value = await func(pipe)
                    exec_value = await pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        await asyncio.sleep(watch_delay)
                    continue


class ClusterNode:
    """
    Create a new ClusterNode.

    Each ClusterNode manages multiple :class:`~redis.asyncio.connection.Connection`
    objects for the (host, port).
    """

    __slots__ = (
        "_background_tasks",
        "_connections",
        "_free",
        "_lock",
        "_event_dispatcher",
        "connection_class",
        "connection_kwargs",
        "host",
        "max_connections",
        "name",
        "port",
        "response_callbacks",
        "server_type",
    )

    def __init__(
        self,
        host: str,
        port: Union[str, int],
        server_type: Optional[str] = None,
        *,
        max_connections: int = 100,
        connection_class: Type[Connection] = Connection,
        **connection_kwargs: Any,
    ) -> None:
        if host == "localhost":
            host = _resolve_localhost()

        connection_kwargs["host"] = host
        connection_kwargs["port"] = port
        self.host = host
        self.port = port
        self.name = get_node_name(host, port)
        self.server_type = server_type

        self.max_connections = max_connections
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.response_callbacks = connection_kwargs.pop("response_callbacks", {})

        self._connections: List[Connection] = []
        self._free: Deque[Connection] = collections.deque(maxlen=self.max_connections)
        self._background_tasks: Set[asyncio.Task] = set()
        self._event_dispatcher = self.connection_kwargs.get("event_dispatcher", None)
        if self._event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()

    def __repr__(self) -> str:
        return (
            f"[host={self.host}, port={self.port}, "
            f"name={self.name}, server_type={self.server_type}]"
        )

    def __eq__(self, obj: Any) -> bool:
        return isinstance(obj, ClusterNode) and obj.name == self.name

    def __hash__(self) -> int:
        return hash(self.name)

    _DEL_MESSAGE = "Unclosed ClusterNode object"

    def __del__(
        self,
        _warn: Any = warnings.warn,
        _grl: Any = asyncio.get_running_loop,
    ) -> None:
        for connection in self._connections:
            if connection.is_connected:
                _warn(f"{self._DEL_MESSAGE} {self!r}", ResourceWarning, source=self)

                try:
                    context = {"client": self, "message": self._DEL_MESSAGE}
                    _grl().call_exception_handler(context)
                except RuntimeError:
                    pass
                break

    async def disconnect(self) -> None:
        ret = await asyncio.gather(
            *(
                asyncio.create_task(connection.disconnect())
                for connection in self._connections
            ),
            return_exceptions=True,
        )
        exc = next((res for res in ret if isinstance(res, Exception)), None)
        if exc:
            raise exc

    def acquire_connection(self) -> Connection:
        try:
            return self._free.popleft()
        except IndexError:
            if len(self._connections) < self.max_connections:
                # We are configuring the connection pool not to retry
                # connections on lower level clients to avoid retrying
                # connections to nodes that are not reachable
                # and to avoid blocking the connection pool.
                # The only error that will have some handling in the lower
                # level clients is ConnectionError which will trigger disconnection
                # of the socket.
                # The retries will be handled on cluster client level
                # where we will have proper handling of the cluster topology
                retry = Retry(
                    backoff=NoBackoff(),
                    retries=0,
                    supported_errors=(ConnectionError,),
                )
                connection_kwargs = self.connection_kwargs.copy()
                connection_kwargs["retry"] = retry
                connection = self.connection_class(**connection_kwargs)
                self._connections.append(connection)
                return connection

            raise MaxConnectionsError()

    async def disconnect_if_needed(self, connection: Connection) -> None:
        """
        Disconnect a connection if it's marked for reconnect.
        This implements lazy disconnection to avoid race conditions.
        The connection will auto-reconnect on next use.
        """
        if connection.should_reconnect():
            # Render the connection before disconnecting: extract_connection_details()
            # reads the local port and the in-flight read deadline off the transport, so
            # after disconnect() it can only report "not connected". This line is what
            # records the maintenance state and relaxed timeout at the moment they are
            # discarded, so a maintenance-driven recycle is attributable in the logs.
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Disconnecting acquired connection marked for reconnect: "
                    f"{connection}, {connection.extract_connection_details()}"
                )
            await connection.disconnect()

    def release(self, connection: Connection) -> None:
        """
        Release connection back to free queue.
        If a connected connection is marked for reconnect, disconnect it before
        returning it to the free queue. An already-closed connection can be
        returned immediately after clearing its reconnect flag.
        """
        if connection.should_reconnect():
            if connection.is_connected:
                # Logged here rather than in _disconnect_and_release: that runs as a
                # task after the fact, by which point extract_connection_details()
                # may already have lost the transport it reads from.
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "Disconnecting released connection marked for reconnect: "
                        f"{connection}, {connection.extract_connection_details()}"
                    )
                task = asyncio.create_task(self._disconnect_and_release(connection))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
                return
            # It may have been re-marked while its own disconnect was in progress.
            connection.reset_should_reconnect()
        self._free.append(connection)

    async def _disconnect_and_release(self, connection: Connection) -> None:
        try:
            await connection.disconnect()
        except Exception as exc:
            logger.debug(
                "disconnecting released cluster connection failed: %r",
                exc,
                exc_info=True,
            )
            try:
                self._connections.remove(connection)
            except ValueError:
                pass
            return

        self._free.append(connection)

    def get_encoder(self) -> Encoder:
        """Return an :class:`Encoder` derived from this node's connection kwargs."""
        kwargs = self.connection_kwargs
        encoder_class = kwargs.get("encoder_class", Encoder)
        return encoder_class(
            encoding=kwargs.get("encoding", "utf-8"),
            encoding_errors=kwargs.get("encoding_errors", "strict"),
            decode_responses=kwargs.get("decode_responses", False),
        )

    def update_active_connections_for_reconnect(self) -> None:
        """
        Mark all in-use (active) connections for reconnect.
        In-use connections are those in _connections but not currently in _free.
        They will be disconnected after their current operation completes.
        """
        free_set = set(self._free)
        for connection in self._connections:
            if connection not in free_set:
                connection.mark_for_reconnect()

    async def disconnect_free_connections(self) -> None:
        """
        Disconnect all free/idle connections in the pool.
        This is useful after topology changes (e.g., failover) to clear
        stale connection state like READONLY mode.
        The connections remain in the pool and will reconnect on next use.
        """
        if self._free:
            # Take the connections out of _free while they close, so
            # acquire_connection cannot hand one out mid-disconnect, and put
            # them back behind any connection released meanwhile.
            connections = tuple(self._free)
            self._free.clear()
            try:
                await asyncio.gather(
                    *(connection.disconnect() for connection in connections),
                    return_exceptions=True,
                )
            finally:
                self._free.extend(connections)

    def _mark_free_connections_for_reconnect_on_error(self, error: Exception) -> bool:
        """
        Mark the free connections for reconnect after a connection error.

        A connection error (not a timeout) on one connection means the server
        may have closed the others too: a restart, an idle ``timeout``, a
        ``CLIENT KILL``. Unlike the sync pool, ``acquire_connection`` does not
        check whether a free connection is still alive, so without this each
        retry would take another dead one. Marking is lazy: a connection is
        reconnected by ``disconnect_if_needed`` only when it is acquired, so the
        connections that are in use, and the idle ones never used again, are
        left alone. Errors that are not node failures (``MaxClientsReachedError``,
        ``AuthenticationError``) mark nothing.

        Returns whether the error was one that marks them.
        """
        if isinstance(error, ConnectionError) and not isinstance(
            error, RedisCluster.NOT_NODE_FAILURE_ERRORS
        ):
            for connection in self._free:
                connection.mark_for_reconnect()
            return True
        return False

    async def parse_response(
        self, connection: Connection, command: str, **kwargs: Any
    ) -> Any:
        try:
            if NEVER_DECODE in kwargs:
                response = await connection.read_response(disable_decoding=True)
                kwargs.pop(NEVER_DECODE)
                if command == "INFO":
                    response = decode_info(response, self.get_encoder())
            else:
                response = await connection.read_response()
        except ResponseError:
            if EMPTY_RESPONSE in kwargs:
                return kwargs[EMPTY_RESPONSE]
            raise

        if EMPTY_RESPONSE in kwargs:
            kwargs.pop(EMPTY_RESPONSE)

        # Remove keys entry, it needs only for cache.
        kwargs.pop("keys", None)

        # Return response
        if command in self.response_callbacks:
            return self.response_callbacks[command](response, **kwargs)

        return response

    async def execute_command(
        self, *args: Any, asking: bool = False, **kwargs: Any
    ) -> Any:
        # Acquire connection
        connection = self.acquire_connection()
        try:
            # Handle lazy disconnect for connections marked for reconnect
            await self.disconnect_if_needed(connection)

            # HIMPORT SET is the one command whose wire form depends on
            # per-connection state: the fieldset must be PREPAREd on this
            # connection first, and any fieldset discarded since this connection
            # last reconciled must be dropped. Doing it here (rather than in
            # RedisCluster.himport_set) reuses the caller's full retry, MOVED/ASK
            # and disconnect-on-error handling for HIMPORT SET too.
            # This per-command branch in the hot dispatch path is deliberate and has
            # no cleaner alternative: this is the only seam where the concrete routed
            # connection is known, and connection-scoped session setup can only happen
            # once that connection is chosen. The overhead is one comparison per
            # command.
            # On an ASK redirect ``asking`` is passed here rather than sent as a
            # separate ASKING command so the allowance sits on this same connection,
            # folded into the SET's own write immediately before the SET.
            himport_set = parse_himport_set_args(args)
            if himport_set is not None:
                # HIMPORT SET in the joined ("HIMPORT SET", key, ...) or split
                # ("HIMPORT", "SET", key, ...) raw form; operands at the right
                # offsets. Too few operands returns None and falls through to the
                # normal send path below so the server returns its arity error
                # instead of a client-side IndexError.
                key, fieldset_name, values = himport_set
                return await self._himport_execute_set(
                    connection, key, fieldset_name, values, asking=asking
                )

            # Execute command
            await connection.send_packed_command(connection.pack_command(*args))

            # Read response
            return await self.parse_response(connection, args[0], **kwargs)
        finally:
            try:
                await self.disconnect_if_needed(connection)
            finally:
                # Release connection
                self.release(connection)

    async def _himport_reconcile_discards(self, conn: "Connection") -> None:
        """Delegate to the shared async HIMPORT executor."""
        return await _himport_exec.reconcile_discards(self, conn)

    async def _himport_prepare_and_set(
        self,
        conn: "Connection",
        key: KeyT,
        fieldset_name: str,
        values: List,
        fieldset,
        asking: bool = False,
    ) -> Any:
        """Delegate to the shared async HIMPORT executor."""
        return await _himport_exec.prepare_and_set(
            self, conn, key, fieldset_name, values, fieldset, asking=asking
        )

    async def _himport_execute_set(
        self,
        conn: "Connection",
        key: KeyT,
        fieldset_name: str,
        values: List,
        asking: bool = False,
    ) -> Any:
        """Delegate to the shared async HIMPORT executor."""
        return await _himport_exec.execute_set(
            self, conn, key, fieldset_name, values, asking=asking
        )

    async def _himport_prepare_pipeline(
        self, conn: "Connection", commands: List["PipelineCommand"]
    ) -> None:
        """Delegate to the shared async HIMPORT executor."""
        await _himport_exec.prepare_pipeline(self, conn, [cmd.args for cmd in commands])

    async def execute_pipeline(self, commands: List["PipelineCommand"]) -> bool:
        # Acquire connection
        connection = self.acquire_connection()
        try:
            # Handle lazy disconnect for connections marked for reconnect
            await self.disconnect_if_needed(connection)

            try:
                # PREPARE fieldsets referenced by buffered HIMPORT SETs before the
                # batched write (it bypasses the per-command lazy prepare path).
                await self._himport_prepare_pipeline(connection, commands)

                # Execute command
                await connection.send_packed_command(
                    connection.pack_commands(cmd.args for cmd in commands)
                )
            except Exception as e:
                self._mark_free_connections_for_reconnect_on_error(e)
                raise

            # Read responses
            ret = False
            marked = False
            for cmd in commands:
                try:
                    cmd.result = await self.parse_response(
                        connection, cmd.args[0], **cmd.kwargs
                    )
                except Exception as e:
                    if not marked:
                        # Once per pipeline: after a connection error every
                        # later read fails on the same closed connection
                        marked = self._mark_free_connections_for_reconnect_on_error(e)
                    cmd.result = e
                    ret = True

            return ret
        finally:
            try:
                await self.disconnect_if_needed(connection)
            finally:
                # Release connection
                self.release(connection)

    async def re_auth_callback(self, token: TokenInterface):
        first_error = None
        tmp_queue = collections.deque()
        try:
            while self._free:
                conn = self._free.popleft()
                tmp_queue.append(conn)
                if conn.should_reconnect() or not conn.is_connected:
                    # Not connected, or marked after a connection error so its
                    # socket may be dead: it authenticates with fresh
                    # credentials when it (re)connects on acquire, and opening
                    # a socket here only to authenticate could raise and stop
                    # the token renewal
                    continue
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
        finally:
            # Return every popped connection to the free queue, also when a
            # re-authentication raised
            while tmp_queue:
                self._free.append(tmp_queue.popleft())
        if first_error is not None:
            raise first_error

    async def _mock(self, error: RedisError):
        """
        Dummy functions, needs to be passed as error callback to retry object.
        :param error:
        :return:
        """
        pass


class NodesManager:
    __slots__ = (
        "_dynamic_startup_nodes",
        "_event_dispatcher",
        "_background_tasks",
        "connection_kwargs",
        "default_node",
        "nodes_cache",
        "_epoch",
        "read_load_balancer",
        "_initialize_lock",
        "require_full_coverage",
        "slots_cache",
        "startup_nodes",
        "address_remap",
        "_last_refresh_at",
        "_failed_refreshes",
        "_refresh_due_at",
        "_last_refresh_unreachable_args",
        "_refresh_requested_at_epoch",
        "_requested_last_failed_node_name",
        "_requested_additional_startup_nodes",
    )

    def __init__(
        self,
        startup_nodes: List["ClusterNode"],
        require_full_coverage: bool,
        connection_kwargs: Dict[str, Any],
        dynamic_startup_nodes: bool = True,
        address_remap: Optional[Callable[[Tuple[str, int]], Tuple[str, int]]] = None,
        event_dispatcher: Optional[EventDispatcher] = None,
    ) -> None:
        self.startup_nodes = {node.name: node for node in startup_nodes}
        self.require_full_coverage = require_full_coverage
        self.connection_kwargs = connection_kwargs
        self.address_remap = address_remap

        self.default_node: "ClusterNode" = None
        self.nodes_cache: Dict[str, "ClusterNode"] = {}
        self.slots_cache: Dict[int, List["ClusterNode"]] = {}
        self._epoch: int = 0
        self.read_load_balancer = LoadBalancer()
        self._initialize_lock: asyncio.Lock = asyncio.Lock()
        # When the last refresh ended, how many failed in a row, and when the
        # next error-triggered refresh is due. Every refresh that ends clears
        # the due time and the next check computes it, once, from the backoff
        # of the caller's retry (a jittered backoff returns a new value on
        # every call). See _refresh_due.
        self._last_refresh_at: Optional[float] = None
        self._failed_refreshes = 0
        self._refresh_due_at: Optional[float] = None
        # The message (args) of the error of the last refresh if it reached no
        # node, cleared by a refresh that ends otherwise; only the message, so
        # the error does not keep the frames of that refresh alive. See
        # _error_to_raise.
        self._last_refresh_unreachable_args: Optional[Tuple[Any, ...]] = None
        # The epoch of the newest refresh request, until a refresh answers it:
        # a refresh that completes after it, such as one SMIGRATED runs,
        # already does
        self._refresh_requested_at_epoch: Optional[int] = None
        # The node whose connection error asked for the pending refresh: the
        # refresh tries it last when no other name is passed in
        self._requested_last_failed_node_name: Optional[str] = None
        # The nodes a MOVED error that asked for the pending refresh pointed
        # to: the refresh also queries them, after the startup nodes
        self._requested_additional_startup_nodes: Tuple[Tuple[str, int], ...] = ()

        self._background_tasks: Set[asyncio.Task] = set()
        self._dynamic_startup_nodes: bool = dynamic_startup_nodes
        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher

    def get_node(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        node_name: Optional[str] = None,
    ) -> Optional["ClusterNode"]:
        if host and port:
            # the user passed host and port
            if host == "localhost":
                host = _resolve_localhost()
            return self.nodes_cache.get(get_node_name(host=host, port=port))
        elif node_name:
            return self.nodes_cache.get(node_name)
        else:
            raise DataError(
                "get_node requires one of the following: 1. node name 2. host and port"
            )

    def set_nodes(
        self,
        old: Dict[str, "ClusterNode"],
        new: Dict[str, "ClusterNode"],
        remove_old: bool = False,
    ) -> None:
        if remove_old:
            for name in list(old.keys()):
                if name not in new:
                    # Node is removed from cache before disconnect starts,
                    # so it won't be found in lookups during disconnect
                    # Mark active connections so in-flight commands can
                    # finish, then disconnect them when their current
                    # operation completes. Free connections can be
                    # disconnected immediately.
                    #
                    # TODO: Make this method async in the next major release to
                    # disconnect the free connections inline instead of in a
                    # background task.
                    removed_node = old.pop(name)
                    removed_node.update_active_connections_for_reconnect()
                    task = asyncio.create_task(
                        removed_node.disconnect_free_connections()
                    )
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)

        for name, node in new.items():
            if name in old:
                # Preserve the existing node and its connections, whatever its
                # role. The server keeps the session state of a connection
                # across a role change, READONLY is sent on every connection
                # when reads from replicas are enabled, and a command that
                # reaches a node that no longer serves its slot gets MOVED.
                old[name].server_type = node.server_type
                continue
            # New node is detected and should be added to the pool
            old[name] = node

    def move_node_to_end_of_cached_nodes(self, node_name: str) -> None:
        """
        Move a failing node to the end of startup_nodes and nodes_cache so it's
        tried last during reinitialization and when selecting the default node.
        If the node is not in the respective list, nothing is done.
        """
        # Move in startup_nodes
        if node_name in self.startup_nodes and len(self.startup_nodes) > 1:
            node = self.startup_nodes.pop(node_name)
            self.startup_nodes[node_name] = node  # Re-insert at end

        # Move in nodes_cache - this affects get_nodes_by_server_type ordering
        # which is used to select the default_node during initialize()
        if node_name in self.nodes_cache and len(self.nodes_cache) > 1:
            node = self.nodes_cache.pop(node_name)
            self.nodes_cache[node_name] = node  # Re-insert at end

    async def move_slot(self, e: AskError | MovedError):
        node_changed = False
        # The redirect carries the address the node advertises, like
        # CLUSTER SLOTS does, so it needs the same remapping to be reachable
        host, port = self.remap_host_port(e.host, e.port)
        redirected_node = self.get_node(host=host, port=port)
        if redirected_node:
            # The node already exists
            if redirected_node.server_type != PRIMARY:
                # Update the node's server type
                redirected_node.server_type = PRIMARY
        else:
            # This is a new node, we will add it to the nodes cache
            redirected_node = ClusterNode(host, port, PRIMARY, **self.connection_kwargs)
            self.set_nodes(self.nodes_cache, {redirected_node.name: redirected_node})
        slot_nodes = self.slots_cache[e.slot_id]
        if redirected_node not in slot_nodes:
            # The new slot owner is a new server, or a server from a different
            # shard. We need to remove all current nodes from the slot's list
            # (including replications) and add just the new node.
            self.slots_cache[e.slot_id] = [redirected_node]
            node_changed = True
        elif redirected_node is not slot_nodes[0]:
            # The MOVED error resulted from a failover, and the new slot owner
            # had previously been a replica.
            old_primary = slot_nodes[0]
            # Update the old primary to be a replica and add it to the end of
            # the slot's node list
            old_primary.server_type = REPLICA
            slot_nodes.append(old_primary)
            # Remove the old replica, which is now a primary, from the slot's
            # node list
            slot_nodes.remove(redirected_node)
            # Override the old primary with the new one
            slot_nodes[0] = redirected_node
            if self.default_node == old_primary:
                # Update the default node with the new primary
                self.default_node = redirected_node
            node_changed = True
        # else: circular MOVED to current primary -> no-op
        # Dispatch so listeners can run shard-pubsub reconciliation; skipped on
        # the no-op branch to avoid needless walks under MOVED storms. A
        # listener must not break slots-cache refresh; log and continue so a
        # single buggy listener cannot starve the rest.
        if node_changed:
            try:
                await self._event_dispatcher.dispatch_async(
                    AsyncAfterSlotsCacheRefreshEvent()
                )
            except Exception as exc:
                # Don't shadow the method parameter ``e``: ``except as`` binds
                # the listener exception in the function scope and ``del``s
                # the name on block exit (PEP 3134), which would also wipe
                # out the original AskError/MovedError parameter.
                logger.exception(
                    "listener raised during slots-cache refresh: %s: %s",
                    type(exc).__name__,
                    exc,
                )

    def get_node_from_slot(
        self,
        slot: int,
        read_from_replicas: bool = False,
        load_balancing_strategy=None,
    ) -> "ClusterNode":
        if read_from_replicas is True and load_balancing_strategy is None:
            load_balancing_strategy = LoadBalancingStrategy.ROUND_ROBIN

        try:
            if len(self.slots_cache[slot]) > 1 and load_balancing_strategy:
                # get the server index using the strategy defined in load_balancing_strategy
                primary_name = self.slots_cache[slot][0].name
                node_idx = self.read_load_balancer.get_server_index(
                    primary_name, len(self.slots_cache[slot]), load_balancing_strategy
                )
                return self.slots_cache[slot][node_idx]
            return self.slots_cache[slot][0]
        except (IndexError, KeyError, TypeError):
            raise SlotNotCoveredError(
                f'Slot "{slot}" not covered by the cluster. '
                f'"require_full_coverage={self.require_full_coverage}"'
            )

    def get_nodes_by_server_type(self, server_type: str) -> List["ClusterNode"]:
        return [
            node
            for node in self.nodes_cache.values()
            if node.server_type == server_type
        ]

    @asynccontextmanager
    async def _counting_failed_refresh(self) -> AsyncGenerator[None, None]:
        """
        Record a refresh that raised, so error-triggered refreshes back off.

        A refresh cancelled before it ended (for example by a timeout around
        the command that ran it) is not counted as failed, but the next one
        still waits the delay, so callers that keep timing out do not run one
        refresh after another. Its outcome is unknown, so it clears the record
        of a refresh that reached no node.
        """
        try:
            yield
        except BaseException as e:
            if isinstance(e, Exception):
                self._failed_refreshes += 1
            self._last_refresh_unreachable_args = (
                e.args if isinstance(e, RedisClusterUnreachableError) else None
            )
            self._last_refresh_at = time.monotonic()
            self._refresh_due_at = None
            raise

    @asynccontextmanager
    async def _closing_temporary_nodes(
        self,
    ) -> AsyncGenerator[List["ClusterNode"], None]:
        """
        Yield a list to fill with the ClusterNode objects built only to query
        CLUSTER SLOTS during a refresh (the additional startup nodes), and
        close the connections they opened once the refresh ends, whether it
        succeeded or raised. The topology keeps its own ClusterNode objects.
        """
        temporary_nodes: List["ClusterNode"] = []
        try:
            yield temporary_nodes
        finally:
            if temporary_nodes:
                await asyncio.gather(
                    *(node.disconnect() for node in temporary_nodes),
                    return_exceptions=True,
                )

    def _refresh_due(self, retry: Optional[Retry] = None) -> bool:
        """
        Whether a topology refresh triggered by an error may run now: not
        before the delay of _topology_refresh_delay has passed since the
        previous refresh ended.
        """
        due_at = self._refresh_due_at
        if due_at is None:
            # The first check since a refresh ended
            last_refresh_at = self._last_refresh_at
            if last_refresh_at is None:
                return True
            due_at = last_refresh_at + _topology_refresh_delay(
                retry, self._failed_refreshes
            )
            self._refresh_due_at = due_at
        return time.monotonic() >= due_at

    def _error_instead_of_retry(
        self, error: Exception, last_refresh_at: Optional[float]
    ) -> Optional[Exception]:
        """
        The error to raise instead of retrying :error:, the connection or
        timeout error of the previous attempt, or None to retry. A refresh that
        ended since that attempt failed (``_last_refresh_at`` moved from
        :last_refresh_at:, read when the error was raised) and reached no node
        leaves the retry the slot map that attempt failed on, so the caller
        gives up now with the :class:`~.RedisClusterUnreachableError` that
        _error_to_raise builds.
        """
        if (
            self._last_refresh_unreachable_args is None
            or self._last_refresh_at == last_refresh_at
        ):
            return None
        outage_error = _error_to_raise(self._last_refresh_unreachable_args, error)
        return None if outage_error is error else outage_error

    async def initialize(
        self,
        additional_startup_nodes_info: Optional[List[Tuple[str, int]]] = None,
        last_failed_node_name: Optional[str] = None,
    ) -> None:
        self.read_load_balancer.reset()
        tmp_nodes_cache: Dict[str, "ClusterNode"] = {}
        tmp_slots: Dict[int, List["ClusterNode"]] = {}
        disagreements = []
        startup_nodes_reachable = False
        fully_covered = False
        exception = None
        epoch = self._epoch
        if additional_startup_nodes_info is None:
            additional_startup_nodes_info = []

        # The temporary nodes are closed after the lock is released
        async with (
            self._closing_temporary_nodes() as temporary_nodes,
            self._initialize_lock,
            self._counting_failed_refresh(),
        ):
            if self._epoch != epoch:
                # another initialize call has already reinitialized the
                # nodes since we started waiting for the lock;
                # we don't need to do it again.
                return

            # Register the nodes for re-authentication before the first
            # connection is opened: that connection's handshake starts the
            # streaming credential provider, whose first renewal is cancelled
            # for good if no callback is registered yet. The event holds the
            # nodes_cache dict itself, which set_nodes updates in place, so the
            # nodes this refresh adds are covered without another dispatch.
            self._event_dispatcher.dispatch(
                AfterAsyncClusterInstantiationEvent(
                    self.nodes_cache,
                    self.connection_kwargs.get("credential_provider", None),
                )
            )

            requested_last_failed_node_name = self._requested_last_failed_node_name
            if last_failed_node_name is None:
                last_failed_node_name = requested_last_failed_node_name
            requested_additional_startup_nodes = (
                self._requested_additional_startup_nodes
            )
            if requested_additional_startup_nodes:
                # A new list: the one passed in belongs to the caller
                additional_startup_nodes_info = additional_startup_nodes_info + [
                    node_info
                    for node_info in requested_additional_startup_nodes
                    if node_info not in additional_startup_nodes_info
                ]

            # Copy to a list to prevent RuntimeError if self.startup_nodes
            # is modified during iteration, then shuffle the iteration order.
            startup_nodes = list(self.startup_nodes.values())
            deferred_failed_nodes = []
            if last_failed_node_name is not None:
                for index, node in enumerate(startup_nodes):
                    if node.name == last_failed_node_name:
                        deferred_failed_nodes.append(startup_nodes.pop(index))
                        break
            if len(startup_nodes) > 1:
                # Vary which startup node is queried first so clients do not
                # all reinitialize through the same node.
                random.shuffle(startup_nodes)
            # An address that is already a startup node is queried through that
            # node, so no temporary node is built for it. Startup nodes are
            # named after the resolved localhost, like ClusterNode names them.
            additional_startup_nodes = [
                ClusterNode(host, port, **self.connection_kwargs)
                for host, port in additional_startup_nodes_info
                if get_node_name(
                    _resolve_localhost() if host == "localhost" else host, port
                )
                not in self.startup_nodes
            ]
            temporary_nodes.extend(additional_startup_nodes)
            if last_failed_node_name is not None:
                for index, node in enumerate(additional_startup_nodes):
                    if node.name == last_failed_node_name:
                        if not deferred_failed_nodes:
                            deferred_failed_nodes.append(node)
                        additional_startup_nodes.pop(index)
                        break
            for startup_node in chain(
                startup_nodes,
                additional_startup_nodes,
                deferred_failed_nodes,
            ):
                try:
                    # Make sure cluster mode is enabled on this node
                    try:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                "Topology refresh: querying CLUSTER SLOTS on "
                                f"{startup_node.name}"
                            )
                        cluster_slots = await startup_node.execute_command(
                            "CLUSTER SLOTS"
                        )
                    except ResponseError:
                        raise RedisClusterException(
                            "Cluster mode is not enabled on this node"
                        )
                    startup_nodes_reachable = True
                    # An error from an earlier startup node is only raised when
                    # no node answers, so drop it. Its traceback refers to this
                    # frame: keeping it forms a reference cycle that holds
                    # tmp_slots and tmp_nodes_cache until the GC runs.
                    exception = None
                except Exception as e:
                    # Try the next startup node.
                    # The last error is kept while no node has answered, and is
                    # raised only if none does.
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            "Topology refresh: CLUSTER SLOTS failed on "
                            f"{startup_node.name}: {type(e).__name__}: {e}"
                        )
                    # Once a node has answered, later errors are not stored: they
                    # would bring the cycle back.
                    if not startup_nodes_reachable:
                        exception = e
                    continue

                # CLUSTER SLOTS command results in the following output:
                # [[slot_section[from_slot,to_slot,master,replica1,...,replicaN]]]
                # where each node contains the following list: [IP, port, node_id]
                # Therefore, cluster_slots[0][2][0] will be the IP address of the
                # primary node of the first slot section.
                # If there's only one server in the cluster, its ``host`` is ''
                # Fix it to the host in startup_nodes
                if (
                    len(cluster_slots) == 1
                    and not cluster_slots[0][2][0]
                    and len(self.startup_nodes) == 1
                ):
                    cluster_slots[0][2][0] = startup_node.host

                for slot in cluster_slots:
                    for i in range(2, len(slot)):
                        slot[i] = [str_if_bytes(val) for val in slot[i]]
                    primary_node = slot[2]
                    host = primary_node[0]
                    if host == "":
                        host = startup_node.host
                    port = int(primary_node[1])
                    host, port = self.remap_host_port(host, port)

                    nodes_for_slot = []

                    target_node = tmp_nodes_cache.get(get_node_name(host, port))
                    if not target_node:
                        target_node = ClusterNode(
                            host, port, PRIMARY, **self.connection_kwargs
                        )
                    # add this node to the nodes cache
                    tmp_nodes_cache[target_node.name] = target_node
                    nodes_for_slot.append(target_node)

                    replica_nodes = slot[3:]
                    for replica_node in replica_nodes:
                        host = replica_node[0]
                        port = replica_node[1]
                        host, port = self.remap_host_port(host, port)

                        target_replica_node = tmp_nodes_cache.get(
                            get_node_name(host, port)
                        )
                        if not target_replica_node:
                            target_replica_node = ClusterNode(
                                host, port, REPLICA, **self.connection_kwargs
                            )
                        # add this node to the nodes cache
                        tmp_nodes_cache[target_replica_node.name] = target_replica_node
                        nodes_for_slot.append(target_replica_node)

                    for i in range(int(slot[0]), int(slot[1]) + 1):
                        if i not in tmp_slots:
                            tmp_slots[i] = nodes_for_slot
                        else:
                            # Validate that 2 nodes want to use the same slot cache
                            # setup
                            tmp_slot = tmp_slots[i][0]
                            if tmp_slot.name != target_node.name:
                                disagreements.append(
                                    f"{tmp_slot.name} vs {target_node.name} on slot: {i}"
                                )

                                if len(disagreements) > 5:
                                    raise RedisClusterException(
                                        f"startup_nodes could not agree on a valid "
                                        f"slots cache: {', '.join(disagreements)}"
                                    )

                # Validate if all slots are covered or if we should try next startup node
                fully_covered = True
                for i in range(REDIS_CLUSTER_HASH_SLOTS):
                    if i not in tmp_slots:
                        fully_covered = False
                        break
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Topology refresh: CLUSTER SLOTS from {startup_node.name} "
                        f"reported nodes {sorted(tmp_nodes_cache)}; "
                        f"slots fully covered: {fully_covered}"
                    )
                if fully_covered:
                    break

            if not startup_nodes_reachable:
                # The unreachable subtype is reserved for connectivity failures:
                # MultiDB registers it as retryable, so a deterministic
                # server/configuration error (e.g. cluster mode disabled or
                # invalid credentials - AuthenticationError and
                # AuthorizationError subclass ConnectionError but cannot be
                # repaired by a failover) must keep surfacing as a plain
                # RedisClusterException.
                try:
                    if isinstance(
                        exception, (ConnectionError, TimeoutError, OSError)
                    ) and not isinstance(
                        exception, (AuthenticationError, AuthorizationError)
                    ):
                        raise RedisClusterUnreachableError(
                            f"Redis Cluster cannot be connected. Please provide at least "
                            f"one reachable node: {str(exception)}"
                        ) from exception
                    raise RedisClusterException(
                        f"Redis Cluster cannot be connected. Please provide at least "
                        f"one reachable node: {str(exception)}"
                    ) from exception
                finally:
                    # Ensure we clear local references to caught exceptions
                    exception = None

            # Check if the slots are not fully covered
            if not fully_covered and self.require_full_coverage:
                # Despite the requirement that the slots be covered, there
                # isn't a full coverage
                raise RedisClusterException(
                    f"All slots are not covered after query all startup_nodes. "
                    f"{len(tmp_slots)} of {REDIS_CLUSTER_HASH_SLOTS} "
                    f"covered..."
                )

            # Set the tmp variables to the real variables
            self.set_nodes(self.nodes_cache, tmp_nodes_cache, remove_old=True)
            # tmp_slots was built from CLUSTER SLOTS responses and can contain
            # newly-created ClusterNode objects for nodes we already know about.
            # Rebuild the slots cache with the preserved nodes_cache instances
            # so existing per-node connection pools stay in use after refresh.
            # Keep the shared node-list-per-slot-range shape from tmp_slots to
            # avoid allocating a separate list for every slot.
            node_lists_by_id: Dict[int, List["ClusterNode"]] = {}
            new_slots_cache: Dict[int, List["ClusterNode"]] = {}
            for slot, nodes in tmp_slots.items():
                node_list_id = id(nodes)
                slot_nodes = node_lists_by_id.get(node_list_id)
                if slot_nodes is None:
                    slot_nodes = [self.nodes_cache[node.name] for node in nodes]
                    node_lists_by_id[node_list_id] = slot_nodes
                new_slots_cache[slot] = slot_nodes
            self.slots_cache = new_slots_cache

            if self._dynamic_startup_nodes:
                # Populate the startup nodes with all discovered nodes
                self.set_nodes(self.startup_nodes, self.nodes_cache, remove_old=True)

            # Set the default node
            self.default_node = self.get_nodes_by_server_type(PRIMARY)[0]
            self._last_refresh_at = time.monotonic()
            self._failed_refreshes = 0
            if self._requested_last_failed_node_name == requested_last_failed_node_name:
                # Answered by this refresh; another node that failed while it
                # ran stays for the next one
                self._requested_last_failed_node_name = None
            if (
                self._requested_additional_startup_nodes
                == requested_additional_startup_nodes
            ):
                self._requested_additional_startup_nodes = ()
            self._refresh_due_at = None
            self._last_refresh_unreachable_args = None
            self._epoch += 1
        # Dispatch so listeners (e.g. ClusterPubSub) can reconcile per-node
        # state after slot ownership may have changed. A listener must not
        # break slots-cache refresh; log and continue so a single buggy
        # listener cannot starve the rest.
        try:
            await self._event_dispatcher.dispatch_async(
                AsyncAfterSlotsCacheRefreshEvent()
            )
        except Exception as e:
            logger.exception(
                "listener raised during slots-cache refresh: %s: %s",
                type(e).__name__,
                e,
            )

    async def aclose(self, attr: str = "nodes_cache") -> None:
        self.default_node = None
        await asyncio.gather(
            *(
                asyncio.create_task(node.disconnect())
                for node in getattr(self, attr).values()
            )
        )

    def remap_host_port(self, host: str, port: int) -> Tuple[str, int]:
        """
        Remap the host and port returned from the cluster to a different
        internal value.  Useful if the client is not connecting directly
        to the cluster.
        """
        if self.address_remap:
            return self.address_remap((host, port))
        return host, port


class ClusterPipeline(AbstractRedis, AbstractRedisCluster, AsyncRedisClusterCommands):
    """
    Create a new ClusterPipeline object.

    Usage::

        result = await (
            rc.pipeline()
            .set("A", 1)
            .get("A")
            .hset("K", "F", "V")
            .hgetall("K")
            .mset_nonatomic({"A": 2, "B": 3})
            .get("A")
            .get("B")
            .delete("A", "B", "K")
            .execute()
        )
        # result = [True, "1", 1, {"F": "V"}, True, True, "2", "3", 1, 1, 1]

    Note: For commands `DELETE`, `EXISTS`, `TOUCH`, `mset_nonatomic`, which are split
    across multiple nodes, you'll get multiple results for them in the array.

    Which methods must be awaited:

    Staging a command returns the pipeline itself, so commands chain and are not
    awaited, as in the example above. The coroutines defined on this class must be
    awaited individually::

        watch, unwatch, unlink, discard, reset, execute, initialize,
        himport_prepare, himport_discard, himport_discard_all

    Of those, only ``initialize`` returns the pipeline, so it is the only one that
    chains once awaited. ``reset``, ``discard``, ``watch``, ``unwatch`` and ``unlink``
    return ``None``, ``execute`` returns a ``list``, ``himport_prepare`` a ``bool`` and
    the two ``himport_discard`` methods an ``int``.

    That list covers only the methods defined on this class. Inherited command
    coroutines are not pipeline steps. ``client_tracking_on`` and
    ``client_tracking_off`` await the pipeline itself, which re-initializes it and
    discards everything staged so far. Others cannot run on a pipeline at all:
    ``command_info`` and the ``hotkeys_*`` family raise ``NotImplementedError``, and
    ``cluster_delslots`` raises ``TypeError``.

    `unlink` is the surprising one, because the inherited `delete` behaves the other
    way::

        pipe = rc.pipeline()
        pipe.delete("A")          # stages, chainable
        await pipe.unlink("B")    # stages, must be awaited, returns None
        await pipe.execute()

    Between :meth:`watch` and :meth:`multi` a command returns a coroutine rather than
    the pipeline. It is executed only when awaited and resolves to the response, so it
    must be awaited. After :meth:`multi` commands are queued again and chain as usual::

        async with rc.pipeline(transaction=True) as pipe:
            await pipe.watch("a")
            a = await pipe.get("a")     # executed when awaited
            pipe.multi()
            pipe.set("a", int(a) + 1)   # queued, chains
            await pipe.execute()

    The split commands from the note above and ``unlink`` are the exception: between
    :meth:`watch` and :meth:`multi`, ``delete``, ``exists`` and ``touch`` return the
    pipeline and ``unlink`` returns ``None``, and all four drop the command unsent, so
    do not use them there. ``mset_nonatomic`` raises ``NotImplementedError`` in a
    transactional pipeline.

    Outside a transactional pipeline ``watch``, ``unwatch`` and ``discard`` raise
    :class:`~.RedisClusterException`, and the ``himport_*`` methods act on the shared
    fieldset registry rather than staging a command.

    Retryable errors:
        - :class:`~.ClusterDownError`
        - :class:`~.ConnectionError`
        - :class:`~.TimeoutError`

    Redirection errors:
        - :class:`~.TryAgainError`
        - :class:`~.MovedError`
        - :class:`~.AskError`

    :param client:
        | Existing :class:`~.RedisCluster` client
    """

    __slots__ = (
        "cluster_client",
        "_transaction",
        "_execution_strategy",
    )

    # Type discrimination marker for @overload self-type pattern
    _is_async_client: Literal[True] = True

    def __init__(
        self, client: RedisCluster, transaction: Optional[bool] = None
    ) -> None:
        self.cluster_client = client
        self._transaction = transaction
        self._execution_strategy: ExecutionStrategy = (
            PipelineStrategy(self)
            if not self._transaction
            else TransactionStrategy(self)
        )

    @property
    def nodes_manager(self) -> "NodesManager":
        """Get the nodes manager from the cluster client."""
        return self.cluster_client.nodes_manager

    def get_connection_kwargs(self) -> Dict[str, Optional[Any]]:
        """Get the connection kwargs from the cluster client."""
        return self.cluster_client.get_connection_kwargs()

    # HIMPORT lifecycle on a cluster pipeline delegates to the parent client, mutating
    # the one shared registry that every node pool references. A fieldset declared here
    # is therefore visible to the batched himport_set pre-flight, mirroring the sync
    # ClusterPipeline (which inherits these from RedisCluster over the shared registry).

    @property
    def himport_registry(self) -> HImportRegistry:
        """The cluster-wide HIMPORT fieldset registry (empty if none was declared).

        Read-only: the registry is mutated only through the HIMPORT command methods.
        """
        return self.cluster_client.himport_registry

    async def himport_prepare(
        self, fieldset_name: str, fields: Iterable[FieldT]
    ) -> bool:
        """Declare an HIMPORT fieldset cluster-wide (shared registry, applied lazily)."""
        return await self.cluster_client.himport_prepare(fieldset_name, fields)

    async def himport_discard(self, fieldset_name: str) -> int:
        """Remove an HIMPORT fieldset cluster-wide (shared registry, applied lazily)."""
        return await self.cluster_client.himport_discard(fieldset_name)

    async def himport_discard_all(self) -> int:
        """Remove all HIMPORT fieldsets cluster-wide (shared registry, applied lazily)."""
        return await self.cluster_client.himport_discard_all()

    def set_response_callback(self, command: str, callback: ResponseCallbackT) -> None:
        """Set a custom response callback on the cluster client."""
        self.cluster_client.set_response_callback(command, callback)

    async def initialize(self) -> "ClusterPipeline":
        await self._execution_strategy.initialize()
        return self

    async def __aenter__(self) -> "ClusterPipeline":
        return await self.initialize()

    async def __aexit__(self, exc_type: None, exc_value: None, traceback: None) -> None:
        await self.reset()

    def __await__(self) -> Generator[Any, None, "ClusterPipeline"]:
        return self.initialize().__await__()

    def __bool__(self) -> bool:
        "Pipeline instances should  always evaluate to True on Python 3+"
        return True

    def __len__(self) -> int:
        return len(self._execution_strategy)

    def execute_command(
        self, *args: Union[KeyT, EncodableT], **kwargs: Any
    ) -> "ClusterPipeline":
        """
        Append a raw command to the pipeline.

        :param args:
            | Raw command args
        :param kwargs:

            - target_nodes: :attr:`NODE_FLAGS` or :class:`~.ClusterNode`
              or List[:class:`~.ClusterNode`] or Dict[Any, :class:`~.ClusterNode`]
            - Rest of the kwargs are passed to the Redis connection
        """
        return self._execution_strategy.execute_command(*args, **kwargs)

    async def execute(
        self, raise_on_error: bool = True, allow_redirections: bool = True
    ) -> List[Any]:
        """
        Execute the pipeline.

        It will retry the commands as specified by retries specified in :attr:`retry`
        & then raise an exception.

        :param raise_on_error:
            | Raise the first error if there are any errors
        :param allow_redirections:
            | Whether to retry each failed command individually in case of redirection
              errors

        :raises RedisClusterException: if target_nodes is not provided & the command
            can't be mapped to a slot
        """
        try:
            return await self._execution_strategy.execute(
                raise_on_error, allow_redirections
            )
        finally:
            await self.reset()

    def _split_command_across_slots(
        self, command: str, *keys: KeyT
    ) -> "ClusterPipeline":
        for slot_keys in self.cluster_client._partition_keys_by_slot(keys).values():
            self.execute_command(command, *slot_keys)

        return self

    async def reset(self):
        """
        Reset back to empty pipeline.
        """
        await self._execution_strategy.reset()

    def multi(self):
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        self._execution_strategy.multi()

    async def discard(self):
        """Discards the transactional block. Must be awaited."""
        await self._execution_strategy.discard()

    async def watch(self, *names):
        """Watches the values at keys ``names``"""
        await self._execution_strategy.watch(*names)

    async def unwatch(self):
        """Unwatches all previously specified keys"""
        await self._execution_strategy.unwatch()

    async def unlink(self, *names):
        """
        Stages an `UNLINK` for keys ``names``. Must be awaited, and returns ``None``
        rather than the pipeline, so unlike `delete` it does not chain::

            await pipe.unlink("A")

        Outside a transactional block only one key is accepted; more raises
        :class:`~.RedisClusterException`. Between :meth:`watch` and :meth:`multi`
        nothing is sent, so do not use it there.
        """
        await self._execution_strategy.unlink(*names)

    def mset_nonatomic(
        self, mapping: Mapping[AnyKeyT, EncodableT]
    ) -> "ClusterPipeline":
        return self._execution_strategy.mset_nonatomic(mapping)


for command in PIPELINE_BLOCKED_COMMANDS:
    command = command.replace(" ", "_").lower()
    if command == "mset_nonatomic":
        continue

    setattr(ClusterPipeline, command, block_pipeline_command(command))

# client_list_iter has no wire command of its own to add to
# PIPELINE_BLOCKED_COMMANDS - it sends CLIENT LIST, blocked above under its
# own name - so block it explicitly here too, or it would fall through to
# the inherited implementation and queue CLIENT LIST like a real pipelined
# command instead of raising.
setattr(ClusterPipeline, "client_list_iter", block_pipeline_command("client_list_iter"))


class PipelineCommand:
    def __init__(self, position: int, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs
        self.position = position
        self.result: Union[Any, Exception] = None
        # Either record type: a policy resolver serves ``CommandPolicies``, while the
        # fallbacks below reuse the shared ``CommandMetadata`` defaults. Only the two routing
        # policies, which both carry, are ever read.
        self.command_policies: Optional[Union[CommandPolicies, CommandMetadata]] = None

    def __repr__(self) -> str:
        return f"[{self.position}] {self.args} ({self.kwargs})"


class ExecutionStrategy(ABC):
    @abstractmethod
    async def initialize(self) -> "ClusterPipeline":
        """
        Initialize the execution strategy.

        See ClusterPipeline.initialize()
        """
        pass

    @abstractmethod
    def execute_command(
        self, *args: Union[KeyT, EncodableT], **kwargs: Any
    ) -> "ClusterPipeline":
        """
        Append a raw command to the pipeline.

        See ClusterPipeline.execute_command()
        """
        pass

    @abstractmethod
    async def execute(
        self, raise_on_error: bool = True, allow_redirections: bool = True
    ) -> List[Any]:
        """
        Execute the pipeline.

        It will retry the commands as specified by retries specified in :attr:`retry`
        & then raise an exception.

        See ClusterPipeline.execute()
        """
        pass

    @abstractmethod
    def mset_nonatomic(
        self, mapping: Mapping[AnyKeyT, EncodableT]
    ) -> "ClusterPipeline":
        """
        Executes multiple MSET commands according to the provided slot/pairs mapping.

        See ClusterPipeline.mset_nonatomic()
        """
        pass

    @abstractmethod
    async def reset(self):
        """
        Resets current execution strategy.

        See: ClusterPipeline.reset()
        """
        pass

    @abstractmethod
    def multi(self):
        """
        Starts transactional context.

        See: ClusterPipeline.multi()
        """
        pass

    @abstractmethod
    async def watch(self, *names):
        """
        Watch given keys.

        See: ClusterPipeline.watch()
        """
        pass

    @abstractmethod
    async def unwatch(self):
        """
        Unwatches all previously specified keys

        See: ClusterPipeline.unwatch()
        """
        pass

    @abstractmethod
    async def discard(self):
        pass

    @abstractmethod
    async def unlink(self, *names):
        """
        "Unlink a key specified by ``names``"

        See: ClusterPipeline.unlink()
        """
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass


class AbstractStrategy(ExecutionStrategy):
    def __init__(self, pipe: ClusterPipeline) -> None:
        self._pipe: ClusterPipeline = pipe
        self._command_queue: List["PipelineCommand"] = []

    async def initialize(self) -> "ClusterPipeline":
        if self._pipe.cluster_client._initialize_due():
            await self._pipe.cluster_client.initialize()
        self._command_queue = []
        return self._pipe

    def execute_command(
        self, *args: Union[KeyT, EncodableT], **kwargs: Any
    ) -> "ClusterPipeline":
        self._command_queue.append(
            PipelineCommand(len(self._command_queue), *args, **kwargs)
        )
        return self._pipe

    def _annotate_exception(self, exception, number, command):
        """
        Provides extra context to the exception prior to it being handled
        """
        cmd = command_text(command)
        msg = (
            f"Command # {number} ({truncate_text(cmd)}) of pipeline "
            f"caused error: {exception.args[0]}"
        )
        exception.args = (msg,) + exception.args[1:]

    @abstractmethod
    def mset_nonatomic(
        self, mapping: Mapping[AnyKeyT, EncodableT]
    ) -> "ClusterPipeline":
        pass

    @abstractmethod
    async def execute(
        self, raise_on_error: bool = True, allow_redirections: bool = True
    ) -> List[Any]:
        pass

    @abstractmethod
    async def reset(self):
        pass

    @abstractmethod
    def multi(self):
        pass

    @abstractmethod
    async def watch(self, *names):
        pass

    @abstractmethod
    async def unwatch(self):
        pass

    @abstractmethod
    async def discard(self):
        pass

    @abstractmethod
    async def unlink(self, *names):
        pass

    def __len__(self) -> int:
        return len(self._command_queue)


class PipelineStrategy(AbstractStrategy):
    def __init__(self, pipe: ClusterPipeline) -> None:
        super().__init__(pipe)

    def mset_nonatomic(
        self, mapping: Mapping[AnyKeyT, EncodableT]
    ) -> "ClusterPipeline":
        encoder = self._pipe.cluster_client.encoder

        slots_pairs = {}
        for pair in mapping.items():
            slot = key_slot(encoder.encode(pair[0]))
            slots_pairs.setdefault(slot, []).extend(pair)

        for pairs in slots_pairs.values():
            self.execute_command("MSET", *pairs)

        return self._pipe

    async def execute(
        self, raise_on_error: bool = True, allow_redirections: bool = True
    ) -> List[Any]:
        """
        Send the commands, retrying on RedisCluster.ERRORS_ALLOW_RETRY as many
        times as ``retry`` allows, with the backoff delay before each attempt
        and a fixed 0.25 seconds first after CLUSTERDOWN or an uncovered slot.
        Every retried error asks for a topology refresh, which the next attempt
        runs once due. NOT_NODE_FAILURE_ERRORS are raised at once. Out of
        retries, the last error is raised, as RedisClusterUnreachableError when
        the last refresh reached no node; a refresh that reached no node since
        an attempt failed ends the retries the same way.
        """
        if not self._command_queue:
            return []

        try:
            retry_attempts = self._pipe.cluster_client.retry.get_retries()
            failure_count = 0
            nodes_manager = self._pipe.cluster_client.nodes_manager
            # The error the previous attempt failed with, and when the last
            # refresh had ended by then: a refresh since that reached no node
            # ends the retries (see NodesManager._error_instead_of_retry)
            last_error: Optional[Exception] = None
            last_refresh_at: Optional[float] = None
            while True:
                try:
                    if self._pipe.cluster_client._initialize_due():
                        await self._pipe.cluster_client.initialize()
                    if last_error is not None:
                        outage_error = nodes_manager._error_instead_of_retry(
                            last_error, last_refresh_at
                        )
                        if outage_error is not None:
                            raise outage_error
                    return await self._execute(
                        self._pipe.cluster_client,
                        self._command_queue,
                        raise_on_error=raise_on_error,
                        allow_redirections=allow_redirections,
                    )

                except RedisCluster.NOT_NODE_FAILURE_ERRORS:
                    raise
                except RedisCluster.ERRORS_ALLOW_RETRY as e:
                    if retry_attempts > 0:
                        # Try again with the new cluster setup. All other errors
                        # should be raised.
                        retry_attempts -= 1
                        # Refresh the topology on the next attempt, without
                        # closing the connections of the shared client
                        self._pipe.cluster_client._request_refresh()
                        failure_count += 1
                        last_error = e
                        last_refresh_at = nodes_manager._last_refresh_at
                        if type(e) in (ClusterDownError, SlotNotCoveredError):
                            # Give the cluster time to recover first, as a
                            # command that failed so does
                            await asyncio.sleep(0.25)
                        # Space the attempts out as the retry backoff says
                        delay = self._pipe.cluster_client._retry_backoff_delay(
                            self._pipe.cluster_client.retry, failure_count
                        )
                        if delay > 0:
                            await asyncio.sleep(delay)
                    else:
                        # All other errors should be raised.
                        raise _error_to_raise(
                            nodes_manager._last_refresh_unreachable_args, e
                        )
        finally:
            await self.reset()

    async def _execute(
        self,
        client: "RedisCluster",
        stack: List["PipelineCommand"],
        raise_on_error: bool = True,
        allow_redirections: bool = True,
    ) -> List[Any]:
        todo = [
            cmd for cmd in stack if not cmd.result or isinstance(cmd.result, Exception)
        ]

        nodes = {}
        for cmd in todo:
            passed_targets = cmd.kwargs.pop("target_nodes", None)
            target_nodes_specified = bool(passed_targets) and not client._is_node_flag(
                passed_targets
            )
            _, command_policies = await client._resolve_command_policies(
                *cmd.args, target_nodes_specified=target_nodes_specified
            )

            if target_nodes_specified:
                target_nodes = client._parse_target_nodes(passed_targets)
            else:
                target_nodes = await client._determine_nodes(
                    *cmd.args,
                    request_policy=command_policies.request_policy,
                    node_flag=passed_targets,
                )
                if not target_nodes:
                    raise RedisClusterException(
                        f"No targets were found to execute {cmd.args} command on"
                    )
            cmd.command_policies = command_policies
            if len(target_nodes) > 1:
                raise RedisClusterException(f"Too many targets for command {cmd.args}")
            node = target_nodes[0]
            if node.name not in nodes:
                nodes[node.name] = (node, [])
            nodes[node.name][1].append(cmd)

        # Start timing for observability
        start_time = time.monotonic()

        tasks = [
            asyncio.create_task(node[0].execute_pipeline(node[1]))
            for node in nodes.values()
        ]
        # Let every node task settle before raising, so a retry never resends
        # commands that a sibling task is still executing
        errors = await asyncio.gather(*tasks, return_exceptions=True)
        for (node, _), e in zip(nodes.values(), errors):
            if isinstance(e, BaseException):
                # The write failed (the connection connects lazily on it): the
                # error carries no node, so take it from the task that raised it
                if isinstance(e, (ConnectionError, TimeoutError)) and not isinstance(
                    e, RedisCluster.NOT_NODE_FAILURE_ERRORS
                ):
                    self._try_failed_node_last(client, node)
                raise e

        # Record operation duration for each node
        for (node, commands), failed in zip(nodes.values(), errors):
            # Find the first error in this node's commands, if any
            node_error = None
            for cmd in commands:
                if isinstance(cmd.result, Exception):
                    node_error = cmd.result
                    break

            if failed:
                # A read failed. Commands before it may have been redirected,
                # so look past the first error for a connection error
                for cmd in commands:
                    if isinstance(
                        cmd.result, (ConnectionError, TimeoutError)
                    ) and not isinstance(
                        cmd.result, RedisCluster.NOT_NODE_FAILURE_ERRORS
                    ):
                        self._try_failed_node_last(client, node)
                        break

            db = node.connection_kwargs.get("db", 0)
            await record_operation_duration(
                command_name="PIPELINE",
                duration_seconds=time.monotonic() - start_time,
                server_address=node.host,
                server_port=node.port,
                db_namespace=str(db) if db is not None else None,
                error=node_error,
            )

        if any(errors):
            if allow_redirections:
                # send each errored command individually
                for cmd in todo:
                    if isinstance(cmd.result, (TryAgainError, MovedError, AskError)):
                        try:
                            cmd.result = client._policies_callback_mapping[
                                cmd.command_policies.response_policy
                            ](await client.execute_command(*cmd.args, **cmd.kwargs))
                        except Exception as e:
                            cmd.result = e

            if raise_on_error:
                for cmd in todo:
                    result = cmd.result
                    if isinstance(result, Exception):
                        command = command_text(cmd.args)
                        # Errors from the redirect retry may carry no args,
                        # e.g. MaxConnectionsError() from acquire_connection().
                        error = result.args[0] if result.args else type(result).__name__
                        msg = (
                            f"Command # {cmd.position + 1} "
                            f"({truncate_text(command)}) "
                            f"of pipeline caused error: {error}"
                        )
                        result.args = (msg,) + result.args[1:]
                        raise result

            default_cluster_node = client.get_default_node()

            # Check whether the default node was used. In some cases,
            # 'client.get_default_node()' may return None. The check below
            # prevents a potential AttributeError.
            if default_cluster_node is not None:
                default_node = nodes.get(default_cluster_node.name)
                if default_node is not None:
                    # This pipeline execution used the default node, check if we need
                    # to replace it.
                    # Note: when the error is raised we'll reset the default node in the
                    # caller function.
                    for cmd in default_node[1]:
                        # Check if it has a command that failed with a relevant
                        # exception
                        if type(cmd.result) in RedisCluster.ERRORS_ALLOW_RETRY:
                            client.replace_default_node()
                            break

        return [cmd.result for cmd in stack]

    @staticmethod
    def _try_failed_node_last(client: "RedisCluster", node: "ClusterNode") -> None:
        # As on the command path: move the failed node to the end of the cached
        # nodes, so the refresh does not pick it as the default node while it
        # is still a primary, and ask for a refresh that tries it last
        client.nodes_manager.move_node_to_end_of_cached_nodes(node.name)
        client._request_refresh(last_failed_node_name=node.name)

    async def reset(self):
        """
        Reset back to empty pipeline.
        """
        self._command_queue = []

    def multi(self):
        raise RedisClusterException(
            "method multi() is not supported outside of transactional context"
        )

    async def watch(self, *names):
        raise RedisClusterException(
            "method watch() is not supported outside of transactional context"
        )

    async def unwatch(self):
        raise RedisClusterException(
            "method unwatch() is not supported outside of transactional context"
        )

    async def discard(self):
        raise RedisClusterException(
            "method discard() is not supported outside of transactional context"
        )

    async def unlink(self, *names):
        if len(names) != 1:
            raise RedisClusterException(
                "unlinking multiple keys is not implemented in pipeline command"
            )

        return self.execute_command("UNLINK", names[0])


class TransactionStrategy(AbstractStrategy):
    NO_SLOTS_COMMANDS = {"UNWATCH"}
    IMMEDIATE_EXECUTE_COMMANDS = {"WATCH", "UNWATCH"}
    UNWATCH_COMMANDS = {"DISCARD", "EXEC", "UNWATCH"}
    SLOT_REDIRECT_ERRORS = (AskError, MovedError)
    CONNECTION_ERRORS = (
        ConnectionError,
        TimeoutError,
        OSError,
        ClusterDownError,
        SlotNotCoveredError,
    )

    def __init__(self, pipe: ClusterPipeline) -> None:
        super().__init__(pipe)
        self._explicit_transaction = False
        self._watching = False
        self._watch_acknowledged = False
        self._pipeline_slots: Set[int] = set()
        # True once a keyed (non-slot-agnostic) command has fixed the slot
        self._transaction_has_keyed_slot = False
        self._transaction_node: Optional[ClusterNode] = None
        self._transaction_connection: Optional[Connection] = None
        self._executing = False
        # The error the running retry is retrying, and when the last refresh
        # had ended by then: a refresh since that reached no node ends the
        # retries (see NodesManager._error_instead_of_retry)
        self._retrying: Optional[Tuple[Exception, Optional[float]]] = None
        self._retry = copy(self._pipe.cluster_client.retry)
        self._retry.update_supported_errors(
            RedisCluster.ERRORS_ALLOW_RETRY + self.SLOT_REDIRECT_ERRORS
        )

    async def _resolve_transaction_slot(self, *args) -> Optional[int]:
        """
        Pick a slot for a transactional pipeline command.

        Zero-key EVAL/EVALSHA can run on any primary. Reuse an existing
        transaction slot when present so multiple zero-key scripts (or a
        mix with keyed commands) stay single-slot.
        """
        if args[0] in self.NO_SLOTS_COMMANDS:
            return None

        if is_zero_key_eval_command(*args):
            if self._pipeline_slots:
                return next(iter(self._pipeline_slots))
            return await self._pipe.cluster_client._determine_slot(*args)

        slot_number = await self._pipe.cluster_client._determine_slot(*args)
        if (
            slot_number is not None
            and self._pipeline_slots
            and slot_number not in self._pipeline_slots
            and not self._transaction_has_keyed_slot
        ):
            # Prior slots came only from zero-key scripts; retarget.
            self._pipeline_slots.clear()
        if slot_number is not None:
            self._transaction_has_keyed_slot = True
        return slot_number

    def _get_client_and_connection_for_transaction(
        self,
    ) -> Tuple[ClusterNode, Connection]:
        """
        Find a connection for a pipeline transaction.

        For running an atomic transaction, watch keys ensure that contents have not been
        altered as long as the watch commands for those keys were sent over the same
        connection. So once we start watching a key, we fetch a connection to the
        node that owns that slot and reuse it.

        ``_transaction_node`` is only updated when a new connection is acquired, so
        it always names the node that owns the held connection, even if the slot
        map changes while the connection is held.
        """
        if not self._pipeline_slots:
            raise RedisClusterException(
                "At least a command with a key is needed to identify a node"
            )

        if not self._transaction_connection:
            node: ClusterNode = (
                self._pipe.cluster_client.nodes_manager.get_node_from_slot(
                    list(self._pipeline_slots)[0], False
                )
            )
            self._transaction_connection = node.acquire_connection()
            self._transaction_node = node

        return self._transaction_node, self._transaction_connection

    def execute_command(self, *args: Union[KeyT, EncodableT], **kwargs: Any) -> "Any":
        # Given the limitation of ClusterPipeline sync API, we have to run it in thread.
        return _run_coroutine_in_thread(self._execute_command(*args, **kwargs))

    async def _execute_command(
        self, *args: Union[KeyT, EncodableT], **kwargs: Any
    ) -> Any:
        cluster_client = self._pipe.cluster_client
        # This runs on a private loop in a worker thread (execute_command), so
        # a topology refresh asked for by an error is left to the next command
        # on the caller's loop: running it here would do I/O on connections
        # bound to that loop. Only a client that never initialized has no
        # topology to queue against.
        initialize_here = (
            cluster_client._initialize
            and cluster_client.nodes_manager.default_node is None
        )
        startup_nodes = (
            tuple(cluster_client.nodes_manager.startup_nodes.values())
            if initialize_here
            else ()
        )
        parser_node = (
            None
            if initialize_here or args[0] in self.NO_SLOTS_COMMANDS
            else cluster_client.commands_parser.node
        )
        if parser_node is not None:
            # Parser I/O during slot resolution belongs to this private loop,
            # not to the caller's node or its connection pool.
            cluster_client.commands_parser.node = ClusterNode(
                parser_node.host,
                parser_node.port,
                **cluster_client.nodes_manager.connection_kwargs,
            )
        slot_number: Optional[int] = None
        try:
            if initialize_here:
                await cluster_client.initialize()
            if args[0] not in self.NO_SLOTS_COMMANDS:
                slot_number = await self._resolve_transaction_slot(*args)
        finally:
            if initialize_here:
                # Keep the discovered topology and metadata, but not sockets bound
                # to the private loop that closes when this command is queued.
                # Same-address nodes can have distinct pools; deduplicate by identity.
                nodes = {
                    id(node): node
                    for node in chain(startup_nodes, cluster_client.get_nodes())
                }
                await asyncio.gather(
                    *(node.disconnect() for node in nodes.values()),
                    return_exceptions=True,
                )
            elif parser_node is not None:
                temporary_node = cluster_client.commands_parser.node
                cluster_client.commands_parser.node = parser_node
                await asyncio.gather(
                    temporary_node.disconnect(), return_exceptions=True
                )

        if (
            self._watching or args[0] in self.IMMEDIATE_EXECUTE_COMMANDS
        ) and not self._explicit_transaction:
            if args[0] == "WATCH":
                self._validate_watch()

            if slot_number is not None:
                if self._pipeline_slots and slot_number not in self._pipeline_slots:
                    raise CrossSlotTransactionError(
                        "Cannot watch or send commands on different slots"
                    )

                self._pipeline_slots.add(slot_number)
            elif args[0] not in self.NO_SLOTS_COMMANDS:
                raise RedisClusterException(
                    f"Cannot identify slot number for command: {args[0]},"
                    "it cannot be triggered in a transaction"
                )

            return self._immediate_execute_command(*args, **kwargs)
        else:
            if slot_number is not None:
                self._pipeline_slots.add(slot_number)

            return super().execute_command(*args, **kwargs)

    def _validate_watch(self):
        if self._explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")

        self._watching = True

    async def _immediate_execute_command(self, *args, **options):
        try:
            return await self._retry.call_with_retry(
                lambda: self._get_connection_and_send_command(*args, **options),
                self._reinitialize_on_error,
                is_retryable=RedisCluster._is_retryable_cluster_error,
                with_failure_count=True,
            )
        except (ConnectionError, TimeoutError) as e:
            nodes_manager = self._pipe.cluster_client.nodes_manager
            raise _error_to_raise(nodes_manager._last_refresh_unreachable_args, e)
        finally:
            # The error of a retried attempt serves the run's next attempt only
            self._retrying = None

    def _raise_instead_of_retry(self) -> None:
        """
        Give up the running retry when a refresh that ended since its error
        reached no node (see NodesManager._error_instead_of_retry).
        """
        if self._retrying is not None:
            nodes_manager = self._pipe.cluster_client.nodes_manager
            outage_error = nodes_manager._error_instead_of_retry(*self._retrying)
            if outage_error is not None:
                raise outage_error

    async def _get_connection_and_send_command(self, *args, **options):
        # Run a topology refresh an earlier error asked for, once due. This runs
        # on the caller's loop, before every attempt: execute_command returns
        # this coroutine from its private loop unawaited.
        if self._pipe.cluster_client._initialize_due():
            await self._pipe.cluster_client.initialize()
        self._raise_instead_of_retry()
        held_connection = self._transaction_connection
        redis_node, connection = self._get_client_and_connection_for_transaction()
        # Only disconnect if not watching - disconnecting would lose WATCH state.
        # A connection acquired just now holds no WATCH state yet, so it is
        # reconnected if an earlier connection error marked it.
        if not self._watching or connection is not held_connection:
            await redis_node.disconnect_if_needed(connection)

        # Start timing for observability
        start_time = time.monotonic()

        try:
            response = await self._send_command_parse_response(
                connection, redis_node, args[0], *args, **options
            )

            await record_operation_duration(
                command_name=args[0],
                duration_seconds=time.monotonic() - start_time,
                server_address=connection.host,
                server_port=connection.port,
                db_namespace=str(connection.db),
            )

            return response
        except Exception as e:
            e.connection = connection
            await record_operation_duration(
                command_name=args[0],
                duration_seconds=time.monotonic() - start_time,
                server_address=connection.host,
                server_port=connection.port,
                db_namespace=str(connection.db),
                error=e,
            )
            raise

    async def _send_command_parse_response(
        self,
        connection: Connection,
        redis_node: ClusterNode,
        command_name,
        *args,
        **options,
    ):
        """
        Send a command and parse the response
        """

        # HIMPORT SET's wire form depends on per-connection state: the fieldset
        # must be PREPAREd on this connection first, and any fieldset discarded
        # since this connection last reconciled must be dropped. The
        # immediate/watched path (commands issued after WATCH, before MULTI)
        # would otherwise send a bare HIMPORT SET and fail with "no such
        # fieldset". Route it through the node's HIMPORT executor, the same way
        # the normal cluster path, the batched MULTI/EXEC path, and standalone
        # watched pipelines all do.
        himport_set = parse_himport_set_args(args)
        if himport_set is not None:
            # HIMPORT SET in the joined or split raw form; operands at the right
            # offsets. Too few operands returns None and falls through to the bare
            # send so the server returns its arity error.
            key, fieldset_name, values = himport_set
            output = await redis_node._himport_execute_set(
                connection, key, fieldset_name, values
            )
        else:
            await connection.send_command(*args)
            output = await redis_node.parse_response(
                connection, command_name, **options
            )

        if command_name == "WATCH":
            self._watch_acknowledged = True
        elif command_name in self.UNWATCH_COMMANDS:
            self._watching = False
            self._watch_acknowledged = False
        return output

    async def _reinitialize_on_error(self, error, failure_count):
        self._retrying = (
            error,
            self._pipe.cluster_client.nodes_manager._last_refresh_at,
        )
        if hasattr(error, "connection"):
            await record_error_count(
                server_address=error.connection.host,
                server_port=error.connection.port,
                network_peer_address=error.connection.host,
                network_peer_port=error.connection.port,
                error_type=error,
                retry_attempts=failure_count,
                is_internal=True,
            )

        # WATCH is requested before the first attempt, but only a reply
        # establishes the protection that a retry must not silently discard.
        if self._watch_acknowledged or isinstance(
            error, self.SLOT_REDIRECT_ERRORS + self.CONNECTION_ERRORS
        ):
            if self._transaction_node is not None:
                # As on the command path: after a connection error the server
                # may have closed the node's free connections too, and the
                # retries would take them one by one
                self._transaction_node._mark_free_connections_for_reconnect_on_error(
                    error
                )
            if self._transaction_connection and self._transaction_node:
                # Disconnect and release back to pool
                await self._transaction_connection.disconnect()
                self._transaction_node.release(self._transaction_connection)
                self._transaction_connection = None

            self._pipe.cluster_client.reinitialize_counter += 1
            should_request_refresh = _should_request_refresh(
                self._pipe.cluster_client.reinitialize_steps,
                self._pipe.cluster_client.reinitialize_counter,
            )
            if isinstance(error, AskError):
                # MovedError is an AskError, so a MOVED slot is patched too, and
                # the retry goes to the node that owns it now. The refresh is
                # only asked for, run by the next command once due.
                await self._pipe.cluster_client.nodes_manager.move_slot(error)
                if should_request_refresh:
                    self._pipe.cluster_client._request_refresh()
                    self._pipe.cluster_client.reinitialize_counter = 0
            elif should_request_refresh:
                # A connection error: ask for a refresh that tries the failed
                # node last. The next attempt runs it once due, through
                # initialize(), which keeps the current topology if the refresh
                # fails.
                cluster_client = self._pipe.cluster_client
                failed_node_name = None
                if self._transaction_node is not None:
                    failed_node_name = self._transaction_node.name
                    cluster_client.nodes_manager.move_node_to_end_of_cached_nodes(
                        failed_node_name
                    )
                cluster_client._request_refresh(last_failed_node_name=failed_node_name)
                cluster_client.reinitialize_counter = 0

        self._executing = False
        if self._watch_acknowledged:
            await self.reset()
            if isinstance(error, self.SLOT_REDIRECT_ERRORS):
                raise WatchError(
                    "Slot rebalancing occurred while watching keys"
                ) from error
            raise WatchError("An error occurred while watching keys") from error

    async def _raise_first_error(self, responses, stack, start_time):
        """
        Raise the first exception on the stack
        """
        for r, cmd in zip(responses, stack):
            if isinstance(r, Exception):
                self._annotate_exception(r, cmd.position + 1, cmd.args)

                await record_operation_duration(
                    command_name="TRANSACTION",
                    duration_seconds=time.monotonic() - start_time,
                    server_address=self._transaction_connection.host,
                    server_port=self._transaction_connection.port,
                    db_namespace=str(self._transaction_connection.db),
                    error=r,
                )

                raise r

    def mset_nonatomic(
        self, mapping: Mapping[AnyKeyT, EncodableT]
    ) -> "ClusterPipeline":
        raise NotImplementedError("Method is not supported in transactional context.")

    async def execute(
        self, raise_on_error: bool = True, allow_redirections: bool = True
    ) -> List[Any]:
        stack = self._command_queue
        if not stack and (not self._watching or not self._pipeline_slots):
            return []

        return await self._execute_transaction_with_retries(stack, raise_on_error)

    async def _execute_transaction_with_retries(
        self, stack: List["PipelineCommand"], raise_on_error: bool
    ):
        try:
            return await self._retry.call_with_retry(
                lambda: self._execute_transaction(stack, raise_on_error),
                lambda error, failure_count: self._reinitialize_on_error(
                    error, failure_count
                ),
                is_retryable=RedisCluster._is_retryable_cluster_error,
                with_failure_count=True,
            )
        except (ConnectionError, TimeoutError) as e:
            nodes_manager = self._pipe.cluster_client.nodes_manager
            raise _error_to_raise(nodes_manager._last_refresh_unreachable_args, e)
        finally:
            # The error of a retried attempt serves the run's next attempt only
            self._retrying = None

    async def _execute_transaction(
        self, stack: List["PipelineCommand"], raise_on_error: bool
    ):
        if len(self._pipeline_slots) > 1:
            raise CrossSlotTransactionError(
                "All keys involved in a cluster transaction must map to the same slot"
            )

        # Run a topology refresh an earlier error asked for, once due. This runs
        # on the caller's loop, before every attempt.
        if self._pipe.cluster_client._initialize_due():
            await self._pipe.cluster_client.initialize()
        self._raise_instead_of_retry()

        self._executing = True

        held_connection = self._transaction_connection
        redis_node, connection = self._get_client_and_connection_for_transaction()
        # Only disconnect if not watching - disconnecting would lose WATCH state.
        # A connection acquired just now holds no WATCH state yet, so it is
        # reconnected if an earlier connection error marked it.
        if not self._watching or connection is not held_connection:
            await redis_node.disconnect_if_needed(connection)

        # Ensure fieldsets referenced by buffered HIMPORT SETs are prepared on this
        # node's connection before the MULTI/EXEC block (session state, not
        # transactional). All keys share one slot here, so it is a single node.
        await redis_node._himport_prepare_pipeline(connection, stack)

        stack = chain(
            [PipelineCommand(0, "MULTI")],
            stack,
            [PipelineCommand(0, "EXEC")],
        )
        commands = [c.args for c in stack if EMPTY_RESPONSE not in c.kwargs]
        packed_commands = connection.pack_commands(commands)

        # Start timing for observability
        start_time = time.monotonic()

        await connection.send_packed_command(packed_commands)
        errors = []

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            await redis_node.parse_response(connection, "MULTI")
        except ResponseError as e:
            self._annotate_exception(e, 0, "MULTI")
            errors.append(e)
        except self.CONNECTION_ERRORS as cluster_error:
            self._annotate_exception(cluster_error, 0, "MULTI")
            cluster_error.connection = connection
            raise

        # and all the other commands
        for i, command in enumerate(self._command_queue):
            if EMPTY_RESPONSE in command.kwargs:
                errors.append((i, command.kwargs[EMPTY_RESPONSE]))
            else:
                try:
                    _ = await redis_node.parse_response(connection, "_")
                except self.SLOT_REDIRECT_ERRORS as slot_error:
                    self._annotate_exception(slot_error, i + 1, command.args)
                    errors.append(slot_error)
                except self.CONNECTION_ERRORS as cluster_error:
                    self._annotate_exception(cluster_error, i + 1, command.args)
                    cluster_error.connection = connection
                    raise
                except ResponseError as e:
                    self._annotate_exception(e, i + 1, command.args)
                    errors.append(e)

        response = None
        # parse the EXEC.
        never_decode = _transaction_never_decode(
            redis_node.connection_kwargs,
            ((c.args, c.kwargs) for c in self._command_queue),
        )
        try:
            if never_decode:
                response = await redis_node.parse_response(
                    connection, "EXEC", **{NEVER_DECODE: []}
                )
            else:
                response = await redis_node.parse_response(connection, "EXEC")
        except ExecAbortError:
            if errors:
                raise errors[0]
            raise

        self._executing = False

        # EXEC clears any watched keys
        self._watching = False
        self._watch_acknowledged = False

        if response is None:
            raise WatchError("Watched variable changed.")

        # put any parse errors into the response
        for i, e in errors:
            response.insert(i, e)

        if len(response) != len(self._command_queue):
            raise InvalidPipelineStack(
                "Unexpected response length for cluster pipeline EXEC."
                " Command stack was {} but response had length {}".format(
                    [c.args[0] for c in self._command_queue], len(response)
                )
            )

        if never_decode:
            response = _decode_transaction_replies(
                redis_node.get_encoder(),
                response,
                [(c.args, c.kwargs) for c in self._command_queue],
            )

        # find any errors in the response and raise if necessary
        if raise_on_error or len(errors) > 0:
            await self._raise_first_error(
                response,
                self._command_queue,
                start_time,
            )

        # We have to run response callbacks manually
        data = []
        for r, cmd in zip(response, self._command_queue):
            if not isinstance(r, Exception):
                command_name = cmd.args[0]
                if command_name in self._pipe.cluster_client.response_callbacks:
                    # The keys option is only for the client cache and is
                    # not an option of the callbacks
                    cmd.kwargs.pop("keys", None)
                    r = self._pipe.cluster_client.response_callbacks[command_name](
                        r, **cmd.kwargs
                    )
            data.append(r)

        await record_operation_duration(
            command_name="TRANSACTION",
            duration_seconds=time.monotonic() - start_time,
            server_address=connection.host,
            server_port=connection.port,
            db_namespace=str(connection.db),
        )

        return data

    async def reset(self):
        self._command_queue = []

        try:
            # make sure to reset the connection state in the event that we
            # were watching something
            if self._transaction_connection:
                try:
                    if self._watching:
                        # call this manually since our unwatch or
                        # immediate_execute_command methods can call reset()
                        await self._transaction_connection.send_command("UNWATCH")
                        await self._transaction_connection.read_response()
                except self.CONNECTION_ERRORS:
                    # disconnect will also remove any previous WATCHes
                    if self._transaction_connection:
                        await self._transaction_connection.disconnect()
                except asyncio.CancelledError:
                    # Disconnect so any unread UNWATCH reply does not get
                    # served to the next caller that takes the connection.
                    if self._transaction_connection:
                        await self._transaction_connection.disconnect()
                    raise
                else:
                    # On the happy path, honor lazy reconnect before release.
                    await self._transaction_node.disconnect_if_needed(
                        self._transaction_connection
                    )
        finally:
            # Always return the connection to the node's free queue, even on
            # cancellation, so cancelled resets do not leak pooled
            # connections. Detach the reference before releasing so the
            # strategy never holds a pointer to a returned connection.
            # ClusterNode.release is synchronous, so no shield is required.
            if self._transaction_connection and self._transaction_node:
                connection, self._transaction_connection = (
                    self._transaction_connection,
                    None,
                )
                self._transaction_node.release(connection)
            # clean up the other instance attributes
            self._transaction_connection = None
            self._transaction_node = None
            self._watching = False
            self._watch_acknowledged = False
            self._explicit_transaction = False
            self._pipeline_slots = set()
            self._transaction_has_keyed_slot = False
            self._executing = False
            self._retrying = None

    def multi(self):
        if self._explicit_transaction:
            raise RedisError("Cannot issue nested calls to MULTI")
        if self._command_queue:
            raise RedisError(
                "Commands without an initial WATCH have already been issued"
            )
        self._explicit_transaction = True

    async def watch(self, *names):
        if self._explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")

        return await self.execute_command("WATCH", *names)

    async def unwatch(self):
        if self._watching:
            return await self.execute_command("UNWATCH")

        return True

    async def discard(self):
        await self.reset()

    async def unlink(self, *names):
        return self.execute_command("UNLINK", *names)


class _ClusterNodePoolAdapter(ConnectionPoolInterface):
    """Thin adapter exposing the :class:`ConnectionPoolInterface` that
    :class:`PubSub` requires, backed by a :class:`ClusterNode`'s own
    connection pool.

    Connections are acquired from the node via
    :meth:`ClusterNode.acquire_connection` and returned via
    :meth:`ClusterNode.release`.  :meth:`PubSub.aclose` already
    disconnects the connection *before* calling :meth:`release`, so the
    connection is returned to the node's free-queue in a disconnected
    state — guaranteeing that a subscribed socket is never silently
    reused for regular commands.

    Methods that do not apply to this adapter (the underlying node's
    lifecycle is managed by the cluster, not by individual PubSub
    instances) are implemented as no-ops so the adapter remains a valid
    :class:`ConnectionPoolInterface`.
    """

    def __init__(self, node: "ClusterNode") -> None:
        self._node = node
        self.connection_kwargs = node.connection_kwargs

    # -- methods used by PubSub ------------------------------------------------

    def get_encoder(self) -> Encoder:
        return self._node.get_encoder()

    async def get_connection(
        self, command_name: Optional[str] = None, *keys: Any, **options: Any
    ) -> AbstractConnection:
        connection = self._node.acquire_connection()
        try:
            await connection.connect()
        except BaseException:
            # connect() may fail mid-handshake (e.g. after the TCP socket
            # is established but before AUTH/HELLO completes) leaving the
            # connection in a partially-connected state.  Disconnect before
            # returning it to the node's free queue so it is not reused.
            await connection.disconnect()
            self._node.release(connection)
            raise
        return connection

    async def release(self, connection: AbstractConnection) -> None:
        # PubSub.aclose() disconnects the connection before calling
        # release(), so it is safe to put it back in the node's free
        # queue – it will reconnect lazily on next use.
        await self._node.disconnect_if_needed(connection)
        self._node.release(connection)

    # -- no-op stubs for the rest of ConnectionPoolInterface -------------------
    # The node's connections are shared with regular cluster traffic and its
    # lifecycle is managed by RedisCluster / NodesManager, so the adapter must
    # not reset, disconnect, retry-configure or re-auth them on behalf of a
    # single PubSub instance.

    def get_protocol(self):
        return self.connection_kwargs.get("protocol", None)

    def reset(self) -> None:
        pass

    async def disconnect(self, inuse_connections: bool = True) -> None:
        pass

    async def aclose(self) -> None:
        pass

    def set_retry(self, retry: "Retry") -> None:
        pass

    async def re_auth_callback(self, token: TokenInterface) -> None:
        pass

    def get_connection_count(self) -> List[Tuple[int, dict]]:
        return []


def _unregister_slots_cache_listener(
    dispatcher_ref: "weakref.ref[EventDispatcher]",
    listener: AsyncEventListenerInterface,
    event_type: Type[object],
) -> None:
    # Module-level finalizer callback. Kept free of strong references to the
    # owning ClusterPubSub so attaching it via weakref.finalize does not
    # extend the pubsub's lifetime.
    dispatcher = dispatcher_ref()
    if dispatcher is not None:
        dispatcher.unregister_listeners({event_type: [listener]})


class ClusterPubSubSlotsCacheListener(AsyncEventListenerInterface):
    """
    Async listener that forwards AsyncAfterSlotsCacheRefreshEvent to a
    ClusterPubSub.

    Holds a weak reference to the pubsub so it does not keep the instance
    alive. Deterministic cleanup of the dispatcher's strong reference to this
    listener is performed by a ``weakref.finalize`` attached to the owning
    ClusterPubSub in ``ClusterPubSub.__init__``.
    """

    def __init__(self, pubsub: "ClusterPubSub") -> None:
        self._pubsub_ref: "weakref.ref[ClusterPubSub]" = weakref.ref(pubsub)

    async def listen(self, event: object) -> None:
        pubsub = self._pubsub_ref()
        if pubsub is None:
            # Race window between pubsub GC and the finalizer running; safe
            # no-op, finalizer will remove this listener shortly.
            return
        try:
            await pubsub.on_slots_changed()
        except Exception as e:
            # Listeners must not break slots-cache refresh; log and continue so
            # a single buggy pubsub cannot starve the rest.
            logger.exception(
                "pubsub %r raised during slots-cache change: %s: %s",
                pubsub,
                type(e).__name__,
                e,
            )


# How long a per-node sharded-pubsub connection is skipped by the round robin
# after a failed poll. PubSub._execute reconnects and retries through the
# connection's own Retry, so one poll on an unreachable node can cost its whole
# retry budget rather than the timeout the caller asked for; a cool-off keeps
# the single reader from spending every pass on that node while its healthy
# siblings hold undelivered messages.
SHARD_POLL_COOL_OFF_SECONDS = 1.0

# How often a failed sharded-pubsub poll may trigger a slots-cache refresh.
# Reconciliation is otherwise purely event-driven, and a node that has left the
# deployment answers ECONNREFUSED rather than MOVED - so without this the reader
# would cool off against the departed node forever and the shard channels pinned
# to it would never move to their new owner. Throttled because the refresh costs
# a CLUSTER SLOTS round trip and a failing node fails every poll.
SHARD_TOPOLOGY_REPAIR_INTERVAL_SECONDS = 5.0


class ClusterPubSub(PubSub):
    """
    Async cluster implementation for pub/sub.

    IMPORTANT: before using ClusterPubSub, read about the known limitations
    with pubsub in Cluster mode and learn how to workaround them:
    https://redis.readthedocs.io/en/stable/clustering.html#known-pubsub-limitations
    """

    def __init__(
        self,
        redis_cluster: "RedisCluster",
        node: Optional["ClusterNode"] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        push_handler_func: Optional[Callable] = None,
        event_dispatcher: Optional[EventDispatcher] = None,
        **kwargs: Any,
    ) -> None:
        """
        When a pubsub instance is created without specifying a node, a single
        node will be transparently chosen for the pubsub connection on the
        first command execution. The node will be determined by:
         1. Hashing the channel name in the request to find its keyslot
         2. Selecting a node that handles the keyslot: If read_from_replicas is
            set to true or load_balancing_strategy is set, a replica can be selected.

        :param redis_cluster: RedisCluster instance
        :param node: ClusterNode to connect to
        :param host: Host of the node to connect to
        :param port: Port of the node to connect to
        :param push_handler_func: Optional push handler function
        :param event_dispatcher: Optional event dispatcher
        :param kwargs: Additional keyword arguments
        """
        self.node = None
        self.set_pubsub_node(redis_cluster, node, host, port)

        # Borrow the node's own connection pool via an adapter rather than
        # creating a second, detached ConnectionPool for pubsub.
        if self.node is not None:
            connection_pool = _ClusterNodePoolAdapter(self.node)
        else:
            connection_pool = None

        self.cluster = redis_cluster
        self.node_pubsub_mapping: Dict[str, PubSub] = {}
        # Reverse index: shard channel (normalized) -> owning node.name. Used to
        # route sunsubscribe calls and reconcile subscriptions after slot
        # migration / failover.
        self._shard_channel_to_node: Dict[Any, str] = {}
        # Per-node poll cool-off deadlines (monotonic). Weak-keyed so a
        # per-node pubsub dropped from node_pubsub_mapping takes its entry with
        # it instead of leaking one per migration.
        self._poll_cool_off: "weakref.WeakKeyDictionary[PubSub, float]" = (
            weakref.WeakKeyDictionary()
        )
        # Node names whose last poll failed to connect. Read by
        # _migrate_shard_channel to skip a wire SUNSUBSCRIBE that cannot
        # succeed, and cleared as soon as a poll on that node works again.
        self._unreachable_nodes: Set[str] = set()
        # Monotonic deadline before which a failed poll must not trigger
        # another slots-cache refresh. 0.0 means "never refreshed".
        self._next_topology_repair: float = 0.0
        # Dedicated lock for shard-subscription bookkeeping. Distinct from
        # PubSub.self._lock (which serializes wire I/O on the cluster-level
        # connection used by aclose / send_command / regular subscribe) so
        # that reconciliation cannot starve those unrelated coroutines
        # during long per-channel migrations.
        self._shard_state_lock: asyncio.Lock = asyncio.Lock()
        # Background tasks created by on_slots_changed; kept to prevent GC.
        self._reconcile_tasks: Set[asyncio.Task] = set()
        # Background NodesManager.initialize() tasks created by
        # _schedule_topology_repair; kept to prevent GC, and kept apart from
        # _reconcile_tasks because aclose() cancels that set and this work is
        # not ours to cancel - see _schedule_topology_repair.
        self._topology_repair_tasks: Set[asyncio.Task] = set()
        self._pubsubs_generator = self._pubsubs_generator()
        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher
        super().__init__(
            connection_pool=connection_pool,
            encoder=redis_cluster.encoder,
            push_handler_func=push_handler_func,
            event_dispatcher=self._event_dispatcher,
            **kwargs,
        )
        # Subscribe to slots-cache change notifications so shard subscriptions
        # can be reconciled automatically after topology refreshes.
        nm_dispatcher = redis_cluster.nodes_manager._event_dispatcher
        self._slots_cache_listener = ClusterPubSubSlotsCacheListener(self)
        nm_dispatcher.register_listeners(
            {AsyncAfterSlotsCacheRefreshEvent: [self._slots_cache_listener]}
        )
        # Deterministic GC-time cleanup so short-lived pubsubs do not leak
        # listeners in the dispatcher when no slots-refresh event ever fires.
        weakref.finalize(
            self,
            _unregister_slots_cache_listener,
            weakref.ref(nm_dispatcher),
            self._slots_cache_listener,
            AsyncAfterSlotsCacheRefreshEvent,
        )

    async def _ensure_cluster_initialized(self) -> None:
        if self.cluster._initialize:
            await self.cluster.initialize()

    def set_pubsub_node(
        self,
        cluster: "RedisCluster",
        node: Optional["ClusterNode"] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        """
        The pubsub node will be set according to the passed node, host and port
        When none of the node, host, or port are specified - the node is set
        to None and will be determined by the keyslot of the channel in the
        first command to be executed.
        RedisClusterException will be thrown if the passed node does not exist
        in the cluster.
        If host is passed without port, or vice versa, a DataError will be
        thrown.
        """
        if node is not None:
            # node is passed by the user
            self._raise_on_invalid_node(cluster, node, node.host, node.port)
            pubsub_node = node
        elif host is not None and port is not None:
            # host and port passed by the user
            node = cluster.get_node(host=host, port=port)
            self._raise_on_invalid_node(cluster, node, host, port)
            pubsub_node = node
        elif host is not None or port is not None:
            # only one of host and port is specified
            raise DataError("Specify both host and port")
        else:
            # nothing specified by the user
            pubsub_node = None
        self.node = pubsub_node

    def get_pubsub_node(self) -> Optional["ClusterNode"]:
        """
        Get the node that is being used as the pubsub connection.

        :return: The ClusterNode being used for pubsub, or None if not yet determined
        """
        return self.node

    async def _resubscribe_shard_channels(self) -> None:
        # A single node can own multiple slot ranges, so a batched
        # ``SSUBSCRIBE`` covering every tracked channel would be rejected by
        # Redis with a ``CROSSSLOT`` error. Group by hash slot and emit one
        # ``SSUBSCRIBE`` per slot.
        by_slot: defaultdict[int, dict] = defaultdict(dict)
        for k, v in self.shard_channels.items():
            by_slot[key_slot(self.encoder.encode(k))][k] = v
        for subscriptions in by_slot.values():
            await self._resubscribe(subscriptions, self.ssubscribe)

    def _get_node_pubsub(self, node: "ClusterNode") -> PubSub:
        """Get or create a PubSub instance for the given node."""
        try:
            return self.node_pubsub_mapping[node.name]
        except KeyError:
            pubsub = PubSub(
                connection_pool=_ClusterNodePoolAdapter(node),
                encoder=self.cluster.encoder,
                push_handler_func=self.push_handler_func,
                event_dispatcher=self._event_dispatcher,
            )
            # Replay shard subscriptions on reconnect with slot-aware grouping
            # so that channels spanning multiple slots owned by this node do
            # not trigger a CROSSSLOT error.
            pubsub._resubscribe_shard_channels = MethodType(
                ClusterPubSub._resubscribe_shard_channels, pubsub
            )
            self._pubsub_io_lock(pubsub)
            self.node_pubsub_mapping[node.name] = pubsub
            return pubsub

    def _find_node_name_for_pubsub(self, pubsub: PubSub) -> Optional[str]:
        for name, candidate in self.node_pubsub_mapping.items():
            if candidate is pubsub:
                return name
        return None

    @staticmethod
    def _pubsub_io_lock(pubsub: PubSub) -> asyncio.Lock:
        """Return the per-node pubsub's wire I/O lock, creating it on first use.

        A per-node ``PubSub`` is read by whichever task polls
        ``get_sharded_message`` and written by the reconciliation task
        (``_migrate_shard_channel``) and by any caller of ``ssubscribe`` /
        ``sunsubscribe``. ``PubSub`` guards writes with its own ``_lock``
        (``PubSub.execute_command``) but reads take no lock at all, so without
        this the reader can be awaiting ``read_response`` while another task's
        ``_execute`` disconnects and reconnects the same socket underneath it -
        which loses the reply to the handshake and surfaces as a read timeout
        followed by a broken connection.

        Kept on the pubsub rather than in a dict keyed by node name so it
        travels with the object through ``node_pubsub_mapping`` and cannot go
        stale when a per-node pubsub is dropped and recreated.
        """
        lock = getattr(pubsub, "_shard_io_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            pubsub._shard_io_lock = lock
        return lock

    @staticmethod
    def _detach_shard_channel(pubsub: PubSub, channel: Any) -> None:
        """Forget a shard channel on a per-node pubsub without a wire round trip.

        ``PubSub.sunsubscribe`` only records the intent in
        ``pending_unsubscribe_shard_channels``; the channel leaves
        ``shard_channels`` when the server confirmation is read. So if the
        ``SUNSUBSCRIBE`` never reaches the server, ``on_connect`` clears the
        pending set and replays ``SSUBSCRIBE`` for the channel - on the node it
        is being migrated away from, on every reconnect. Once the caller has
        decided the channel belongs to a different node, the local intent is
        the only truth left, so drop it here.

        Unlike the sync counterpart there is no ``subscribed_event`` to clear
        once the last subscription is gone: the async ``PubSub`` has no such
        event, and its ``subscribed`` is a plain property derived from the
        subscription dicts - including the ``shard_channels`` entry just
        dropped here.

        Nor is the per-node I/O lock taken, as in the sync counterpart - and
        here it is not even needed for correctness: this runs to completion
        without an ``await``, so the event loop already makes it atomic against
        ``handle_message``'s unsubscribe bookkeeping. Should an ``await`` ever
        land in this body, that bookkeeping still discards rather than removes
        (see ``PubSub.handle_message``), so a racing detach cannot make it raise
        ``KeyError`` into a poll no caller catches.
        """
        pubsub.shard_channels.pop(channel, None)
        pubsub.pending_unsubscribe_shard_channels.discard(channel)

    async def _drop_node_pubsub(self, name: str, pubsub: PubSub) -> None:
        """Retire a per-node pubsub and drop it from ``node_pubsub_mapping``.

        Callers hold ``_shard_state_lock``, the lock that every mutation of
        that mapping observes. ``aclose()`` runs under the per-node I/O lock so
        the socket is not torn down beneath a concurrent bounded poll parked in
        ``parse_response``, and its errors are swallowed: retiring one node's
        pubsub must not abort the caller's pass.

        Every caller must leave nothing subscribed on ``pubsub`` (or have lost
        the node itself). An empty per-node pubsub left in the mapping keeps a
        live connection with nothing subscribed on it, so ``_poll_node_pubsub``
        passes its ``connection is None`` guard and blocks reading a socket no
        message can arrive on - for the whole timeout of every pass, and until
        the connection is torn down when the caller passed ``timeout=None``.

        Popping it from the mapping is not enough to keep it out of a poll; the
        rebind below is what stops the round robin from handing it out.
        """
        try:
            async with self._pubsub_io_lock(pubsub):
                await pubsub.aclose()
        except Exception:
            pass
        self.node_pubsub_mapping.pop(name, None)
        self._unreachable_nodes.discard(name)
        # Same snapshot reason ``aclose()`` recreates this: ``_pubsubs_generator``
        # captures node_pubsub_mapping.values() into a local list inside
        # ``yield from``, which the pop above does not reach - so a generator
        # suspended mid-yield-from would still hand the object we just retired
        # to the next poll. That cannot stall the reader the way it can in the
        # sync stack (there is no subscription wait to park on, and
        # ``_poll_node_pubsub``'s ``connection is None`` guard skips it), but it
        # still spends one of the pass's ``range(len(node_pubsub_mapping))``
        # slots on a dead entry - so a healthy sibling loses its turn until the
        # captured snapshot drains. ``type(self)`` bypasses the instance-level
        # self-shadow established at __init__. Costs nothing: constructing a
        # generator runs no frame, so the per-node collection loop in
        # reinitialize_shard_subscriptions can rebind once per dropped node.
        self._pubsubs_generator = type(self)._pubsubs_generator(  # type: ignore[method-assign]
            self
        )

    async def _sharded_message_generator(
        self, timeout: float = 0.0
    ) -> Tuple[Optional[PubSub], Optional[Dict[str, Any]]]:
        """Generate messages from shard channels across all nodes."""
        first_error: Optional[BaseException] = None
        polled = 0
        failed = 0
        next_ready: Optional[float] = None
        for _ in range(len(self.node_pubsub_mapping)):
            pubsub = next(self._pubsubs_generator)
            if pubsub is None:
                # node_pubsub_mapping was emptied between the len() above and
                # here; nothing left to poll in this pass.
                break
            if self._poll_cool_off:
                deadline = self._poll_cool_off.get(pubsub, 0.0)
                if time.monotonic() < deadline:
                    # In cool-off after a failed poll: skip it so the reader
                    # spends this pass on the nodes that can still deliver.
                    if next_ready is None or deadline < next_ready:
                        next_ready = deadline
                    continue
            polled += 1
            try:
                message = await self._poll_node_pubsub(pubsub, timeout)
            except MovedError as e:
                # Handled, not failed: _handle_moved_on_read re-routes the
                # offending channels and schedules reconciliation, so the next
                # pass recovers. Re-raising a MovedError out of a pubsub read
                # would only hand the caller an error it cannot act on. Still
                # cool off: if the slots cache cannot be corrected the repair
                # would otherwise re-run on every poll.
                self._poll_cool_off[pubsub] = (
                    time.monotonic() + SHARD_POLL_COOL_OFF_SECONDS
                )
                await self._handle_moved_on_read(pubsub, e)
                continue
            except (ConnectionError, TimeoutError, OSError) as e:
                # One unhealthy node must not starve its healthy siblings. A
                # single reader serves every per-node pubsub, so aborting the
                # pass here stops delivery cluster-wide for as long as this one
                # node stays unreachable - even though the slots it no longer
                # serves are the only ones affected. Keep polling the rest and
                # surface an error only if nothing in the pass worked, the same
                # made-progress rule reinitialize_shard_subscriptions applies.
                failed += 1
                if first_error is None:
                    first_error = e
                # Cool off before polling this one again. PubSub._execute
                # reconnects and then retries through the connection's own
                # Retry, so a single "bounded" poll on an unreachable node can
                # cost its whole retry budget - far longer than the timeout the
                # caller asked for. Without a cool-off the reader goes straight
                # back to that node on the next pass and pays it again, which
                # is what turns one sick node into a cluster-wide delivery
                # stall.
                self._poll_cool_off[pubsub] = (
                    time.monotonic() + SHARD_POLL_COOL_OFF_SECONDS
                )
                # Mutated without _shard_state_lock, mirroring the sync stack:
                # this is an advisory hint for _migrate_shard_channel's fast
                # path, and both misread directions are handled there and
                # self-heal - a stale entry only skips a SUNSUBSCRIBE to a dead
                # node, a missing one only pays a reconnect before the same
                # local forget.
                node_name = self._find_node_name_for_pubsub(pubsub)
                if node_name is not None:
                    self._unreachable_nodes.add(node_name)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "sharded pubsub poll failed on %s: %s: %s",
                        node_name,
                        type(e).__name__,
                        e,
                    )
                # A node that has left the deployment never answers MOVED, so
                # this branch is the only signal that its shard channels may
                # need a new owner. Ask for a slots-cache refresh; its dispatch
                # reaches on_slots_changed and reconciles.
                self._schedule_topology_repair()
                continue
            # Emptiness check first: this is the per-message hot path, and the
            # weakref lookup a WeakKeyDictionary pop needs is pure overhead
            # while no node is in cool-off, which is the normal case.
            if self._poll_cool_off:
                self._poll_cool_off.pop(pubsub, None)
            if self._unreachable_nodes:
                node_name = self._find_node_name_for_pubsub(pubsub)
                if node_name is not None:
                    self._unreachable_nodes.discard(node_name)
            if message is not None:
                return pubsub, message
        if first_error is not None and failed == polled:
            raise first_error
        if polled == 0 and next_ready is not None:
            await self._wait_out_cool_off(next_ready, timeout)
        return None, None

    @staticmethod
    async def _wait_out_cool_off(next_ready: float, timeout: Optional[float]) -> None:
        """Wait out a pass in which every node was skipped for cool-off.

        Such a pass does no wire read at all, so returning straight away
        ignores the timeout the caller asked to block for - and a reader loop
        on ``get_sharded_message`` polls back immediately, spinning until the
        cool-off expires instead of blocking. Sleep instead: until the
        earliest cool-off is over, never longer than the caller's timeout, and
        not at all for a non-blocking poll.
        """
        if timeout is not None and timeout <= 0:
            return
        delay = next_ready - time.monotonic()
        if delay <= 0:
            return
        if timeout is not None:
            delay = min(delay, timeout)
        await asyncio.sleep(delay)

    def _poll_io_lock(self, pubsub: PubSub, timeout: Optional[float]):
        """Guard a per-node poll against concurrent writers on the same socket.

        ``timeout=None`` makes ``_poll_node_pubsub``'s read wait indefinitely,
        so holding the lock across it would block reconciliation for as long as
        no message arrives. Such a caller drives the pubsub itself and gets the
        pre-existing unguarded behavior; every bounded poll - which is what
        ``ClusterPubSub``'s own callers use - is serialized.
        """
        if timeout is None:
            return nullcontext()
        return self._pubsub_io_lock(pubsub)

    async def _poll_node_pubsub(
        self, pubsub: PubSub, timeout: Optional[float]
    ) -> Optional[Dict[str, Any]]:
        """Read one message from a per-node pubsub, dispatching outside the lock.

        Splits ``PubSub.get_message`` so the per-node I/O lock covers the wire
        read only. ``handle_message`` awaits a subscribed channel's user
        handler inline, and a handler is free to await ``ssubscribe`` /
        ``sunsubscribe`` on this ``ClusterPubSub`` - which takes
        ``_shard_state_lock`` and then the same I/O lock. ``asyncio.Lock`` is
        not reentrant, so holding the I/O lock across the handler hangs the
        reader task permanently and silently on the re-acquire; even a
        task-reentrant lock would still deadlock against the reconciliation
        task, which holds ``_shard_state_lock`` and awaits that I/O lock.

        The two halves of ``handle_message`` are mutually exclusive:
        ``UNSUBSCRIBE_MESSAGE_TYPES`` does subscription bookkeeping and never
        reaches a handler, ``PUBLISH_MESSAGE_TYPES`` only dispatches. So
        bookkeeping stays inside the lock - it mutates the very
        ``shard_channels`` / ``pending_unsubscribe_shard_channels`` that
        ``ssubscribe`` / ``sunsubscribe`` mutate under this lock - and only the
        dispatch moves out. The cost is a narrow race: a reconciliation pass
        that detaches the channel between the read and the dispatch makes the
        handler lookup miss, so the message is returned to the caller instead
        of dispatched. That is the same in-flight-during-unsubscribe race
        ``PubSub`` itself has, and closing it would mean duplicating
        ``handle_message``'s dispatch here.

        The async ``PubSub`` has no ``subscribed_event`` for the sync
        counterpart's subscription wait to mirror, but the connectionless state
        that wait guards against still has to be handled: a per-node pubsub
        enters ``node_pubsub_mapping`` before its first ``SSUBSCRIBE``
        (``_get_node_pubsub``) and is left connectionless by ``aclose()`` (the
        GC in ``reinitialize_shard_subscriptions``), while ``parse_response``
        raises ``RuntimeError`` on a ``None`` connection - which neither poll
        site catches. ``_pubsubs_generator`` yields from a snapshot of the
        mapping, so it can hand out a pubsub the GC has just dropped. Checking
        under the I/O lock rather than before it closes that window for every
        bounded poll, because the GC ``aclose()``s under the same lock.
        """
        async with self._poll_io_lock(pubsub, timeout):
            if pubsub.connection is None:
                # Not connected yet, or closed by a concurrent teardown.
                # Reading would raise RuntimeError; skip this node instead.
                return None
            response = await pubsub.parse_response(
                block=(timeout is None), timeout=timeout
            )
            # get_message's truthiness test, not "is None": a health check
            # reply filtered out by parse_response, or an empty bulk, is "no
            # message" rather than a message to parse.
            if not response:
                return None
            if not self._is_publish_response(response):
                # Don't pass ignore_subscribe_messages here - let
                # get_sharded_message handle the filtering after processing
                # subscription state changes
                return await pubsub.handle_message(
                    response, ignore_subscribe_messages=False
                )
        return await pubsub.handle_message(response, ignore_subscribe_messages=False)

    @staticmethod
    def _is_publish_response(response: Any) -> bool:
        """Whether a raw pubsub reply can make ``handle_message`` dispatch.

        ``handle_message`` awaits a user handler only for
        ``PUBLISH_MESSAGE_TYPES``; every other reply either does subscription
        bookkeeping (``UNSUBSCRIBE_MESSAGE_TYPES``) or is a pong, and the two
        branches are mutually exclusive. A non-sequence reply is the bare-PING
        shape ``handle_message`` rewrites into a pong, so it cannot dispatch
        either.
        """
        if not isinstance(response, (list, tuple)):
            return False
        return str_if_bytes(response[0]) in PubSub.PUBLISH_MESSAGE_TYPES

    def _schedule_topology_repair(self) -> None:
        """Ask for a slots-cache refresh after a poll could not reach a node.

        ``reinitialize_shard_subscriptions`` only ever runs from a slots-cache
        change notification, and a node that has been rebooted or taken out of
        the deployment answers ``ECONNREFUSED`` rather than ``MOVED`` - so the
        read path itself has to ask, or the shard channels pinned to that node
        stay there for the lifetime of the pubsub.

        The refresh is asked for on the cluster client like every other
        error-triggered one, and ``RedisCluster.initialize`` runs it once due:
        it serializes concurrent callers, keeps the request when the refresh
        fails, and clears it when it succeeds. ``NodesManager.initialize`` drops
        nodes that have left the topology and dispatches
        ``AsyncAfterSlotsCacheRefreshEvent``, which reaches
        ``on_slots_changed``. Run it as a task so a bounded poll does not pay
        for a ``CLUSTER SLOTS`` round trip, and throttle it because a node that
        is down fails every poll.
        """
        if not self.shard_channels:
            return
        now = time.monotonic()
        if now < self._next_topology_repair:
            return
        self._next_topology_repair = now + SHARD_TOPOLOGY_REPAIR_INTERVAL_SECONDS
        # Not tracked in _reconcile_tasks: that set is cancelled by aclose(),
        # and ``initialize`` refreshes the slots cache of the whole cluster
        # client, which every other command on it reads - closing one pubsub
        # must not abort a refresh in flight, leaving the shared cache stale
        # until some later MOVED or failed poll asks again. The sync
        # counterpart has the same property for free: ``reset()`` retires the
        # reconciliation executor with ``cancel_futures=True``, which drops
        # queued work but lets a running ``initialize`` finish.
        # Ask for the refresh on the cluster client: a refresh that fails
        # leaves the request for the next command or repair, as on sync, and
        # one that succeeds answers it, so no command repeats it
        self.cluster._request_refresh()
        if not self.cluster.nodes_manager._refresh_due(self.cluster.retry):
            # Subject to _TOPOLOGY_REFRESH_MIN_INTERVAL_SECONDS like every other
            # error-triggered refresh: the next command, or the next repair,
            # runs it once it is due
            return
        task = asyncio.create_task(self.cluster.initialize())
        self._topology_repair_tasks.add(task)
        task.add_done_callback(self._topology_repair_tasks.discard)
        task.add_done_callback(self._log_reconcile_task_exception)

    async def _handle_moved_on_read(self, pubsub: PubSub, error: MovedError) -> None:
        """Re-route shard channels pinned to a node that lost their slot.

        ``PubSub.on_connect`` replays ``SSUBSCRIBE`` to the node its connection
        is bound to, so after a slot migration that node answers ``MOVED``.
        ``MovedError`` is not in ``Retry.supported_errors`` and no other code on
        the read path refreshes the slots cache, so a shard channel left on a
        former owner could never recover. Drop the offending channels from this
        pubsub so the replay stops, forget their recorded owner so
        ``reinitialize_shard_subscriptions`` does not short-circuit on an
        already-advanced reverse index, then apply the redirect and reconcile.
        """
        node_name = self._find_node_name_for_pubsub(pubsub)
        logger.debug(
            "sharded pubsub: %s no longer owns slot %s; re-routing its shard channels",
            node_name,
            error.slot_id,
        )
        async with self._shard_state_lock:
            for channel in list(pubsub.shard_channels):
                if key_slot(self.encoder.encode(channel)) != error.slot_id:
                    continue
                self._detach_shard_channel(pubsub, channel)
                if self._shard_channel_to_node.get(channel) == node_name:
                    del self._shard_channel_to_node[channel]
            # The detach above can leave this pubsub with nothing subscribed -
            # a node that lost its only slot answers MOVED for every channel it
            # held. Retire it here rather than leave it in the mapping for a
            # collector elsewhere: no SUNSUBSCRIBE confirmation will arrive for
            # a channel forgotten locally, so get_sharded_message's collector
            # cannot reach it, and the reconciliation pass scheduled below only
            # GCs it once its task gets to run - a whole poll cool-off later,
            # at best, while a poll that reaches the empty pubsub first blocks
            # on a socket no message can arrive on (see _drop_node_pubsub).
            if node_name is not None and not pubsub.subscribed:
                await self._drop_node_pubsub(node_name, pubsub)
        # move_slot applies the redirect to the slots cache and dispatches
        # AsyncAfterSlotsCacheRefreshEvent, which reaches on_slots_changed. Call
        # on_slots_changed unconditionally too: move_slot skips the dispatch on
        # a circular MOVED, and a duplicate reconciliation pass is a no-op.
        # move_slot indexes slots_cache by the redirected slot, so an
        # as-yet-uncovered slot raises: log and still reconcile rather than let
        # a repair attempt break a pubsub read.
        try:
            await self.cluster.nodes_manager.move_slot(error)
        except Exception as exc:
            logger.debug(
                "sharded pubsub: could not apply the redirect for slot %s: %s: %s",
                error.slot_id,
                type(exc).__name__,
                exc,
            )
        await self.on_slots_changed()

    def _pubsubs_generator(self) -> Generator[Optional[PubSub], None, None]:
        """Generator that yields PubSub instances in round-robin fashion.

        Never returns: a generator that returns is exhausted for good and only
        ``reset`` recreates this one, so a momentarily empty
        ``node_pubsub_mapping`` - reconciliation drops a per-node pubsub before
        creating its replacement - would stop the round robin permanently.
        Yields ``None`` for an empty mapping instead, which lets the caller skip
        the slot without this loop spinning on an empty list.
        """
        while True:
            current_nodes = list(self.node_pubsub_mapping.values())
            if not current_nodes:
                yield None
            else:
                yield from current_nodes

    async def get_sharded_message(
        self,
        ignore_subscribe_messages: bool = False,
        timeout: float = 0.0,
        target_node: Optional["ClusterNode"] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Get the next sharded pubsub message, or ``None`` if none is available.

        Polls the per-node connections in round robin unless ``target_node`` is
        given, and keeps shard channels attached to the node that currently
        owns their slot: a failed poll cools that node off and asks for a
        slots-cache refresh, and a ``MOVED`` reply re-routes the affected
        channels to their new owner. Neither reaches the caller. A connection
        failure is surfaced only when every node polled in the pass failed, so
        one unreachable node does not stop delivery from its healthy siblings.

        ``target_node`` opts out of that shielding: a caller that names a
        single node has no sibling to protect, so connection errors propagate.
        A ``MOVED`` reply is still handled rather than raised.

        :param ignore_subscribe_messages: Whether to ignore subscribe messages
        :param timeout: Timeout for message retrieval
        :param target_node: Specific node to get message from
        :return: Message dictionary or None
        """
        pubsub: Optional[PubSub]
        if target_node:
            pubsub = self.node_pubsub_mapping.get(target_node.name)
            if pubsub:
                try:
                    message = await self._poll_node_pubsub(pubsub, timeout)
                except MovedError as e:
                    # Same handling as the round-robin path: the caller cannot
                    # act on a MovedError raised out of a pubsub read, and the
                    # channels this node no longer owns have to be re-routed or
                    # they never recover. Cool off too, so a slots cache that
                    # cannot be corrected does not re-run the repair on every
                    # poll. Unlike that path, connectivity errors still
                    # propagate: they are swallowed there only to keep one sick
                    # node from starving its healthy siblings, and a caller that
                    # named a single node has no sibling to protect.
                    self._poll_cool_off[pubsub] = (
                        time.monotonic() + SHARD_POLL_COOL_OFF_SECONDS
                    )
                    await self._handle_moved_on_read(pubsub, e)
                    message = None
            else:
                message = None
        else:
            pubsub, message = await self._sharded_message_generator(timeout=timeout)

        if message is None:
            return None
        # Only sunsubscribe mutates cluster-level shard state; bypassing the
        # lock on the data-message hot path keeps smessage delivery from
        # competing with the reconciliation task for _shard_state_lock.
        if str_if_bytes(message["type"]) == "sunsubscribe":
            # Serialize state mutation against reinitialize_shard_subscriptions
            # (background task). The blocking _poll_node_pubsub above
            # intentionally runs outside the lock so reconciliation is not
            # stalled by long polls.
            async with self._shard_state_lock:
                if (
                    message["channel"] in self.pending_unsubscribe_shard_channels
                    and message["channel"] not in pubsub.shard_channels
                ):
                    # User-initiated sunsubscribe: drop from cluster-level tracking.
                    self.pending_unsubscribe_shard_channels.remove(message["channel"])
                    self.shard_channels.pop(message["channel"], None)
                    self._shard_channel_to_node.pop(message["channel"], None)
                # Drop the per-node pubsub that delivered the confirmation once
                # it no longer holds any shard subscriptions, regardless of
                # whether the sunsubscribe was user-initiated or driven by
                # slot-migration reconciliation (_migrate_shard_channel, which
                # intentionally does not add the channel to
                # pending_unsubscribe_shard_channels). This releases the
                # dedicated connection that would otherwise linger.
                # Identifying the receiving pubsub directly (rather than via
                # the cluster's current slot map) is required after slot
                # migration, where the channel's owner is no longer the node
                # that received our original SSUBSCRIBE.
                if pubsub is not None and not pubsub.subscribed:
                    name = self._find_node_name_for_pubsub(pubsub)
                    if name is not None:
                        await self._drop_node_pubsub(name, pubsub)

        # Only suppress subscribe/unsubscribe messages, not data messages (smessage)
        if str_if_bytes(message["type"]) in ("ssubscribe", "sunsubscribe"):
            if self.ignore_subscribe_messages or ignore_subscribe_messages:
                return None
        return message

    async def ssubscribe(
        self, *args: ChannelT | Subscription, **kwargs: PubSubHandler
    ) -> None:
        """
        Subscribe to shard channels.

        :param args: Channel names or ``Subscription`` objects
        :param kwargs: Channel names with handlers
        """
        s_channels = parse_pubsub_subscriptions(args, kwargs)

        if not s_channels:
            return
        await self._ensure_cluster_initialized()

        # Serialize against reinitialize_shard_subscriptions (background
        # task) so the reverse index, shard_channels, and node_pubsub_mapping
        # are not mutated concurrently. _migrate_shard_channel below does not
        # re-acquire this lock (asyncio.Lock is non-reentrant).
        async with self._shard_state_lock:
            for s_channel, handler in s_channels.items():
                node = self.cluster.get_node_from_key(s_channel)
                if not node:
                    continue
                # Lazy re-route: if this channel is already tracked against a
                # different node (e.g. after a slot migration), migrate it now
                # so the caller's intent is applied on the current owner.
                normalized_key = next(iter(self._normalize_keys({s_channel: None})))
                old_name = self._shard_channel_to_node.get(normalized_key)
                if old_name and old_name != node.name:
                    # Match PubSub.ssubscribe() dict.update() semantics: the
                    # caller's newly supplied handler (including None) always
                    # overrides any previously registered handler.
                    await self._migrate_shard_channel(
                        normalized_key,
                        handler,
                        old_name,
                        node,
                    )
                    continue
                pubsub = self._get_node_pubsub(node)
                async with self._pubsub_io_lock(pubsub):
                    if handler:
                        await pubsub.ssubscribe(Subscription(s_channel, handler))
                    else:
                        await pubsub.ssubscribe(s_channel)
                self.shard_channels.update(pubsub.shard_channels)
                self._shard_channel_to_node[normalized_key] = node.name
                self.pending_unsubscribe_shard_channels.difference_update(
                    self._normalize_keys({s_channel: None})
                )

    async def sunsubscribe(self, *args: Any) -> None:
        """
        Unsubscribe from shard channels.

        :param args: Channel names to unsubscribe from. If empty, unsubscribe from all.
        """
        if args:
            args = list_or_args(args[0], args[1:])
        else:
            args = list(self.shard_channels.keys())

        if not self.node_pubsub_mapping:
            return
        # Keep initialization outside the shard-state lock: it can dispatch a
        # topology refresh whose reconciler must remain able to make progress.
        await self._ensure_cluster_initialized()

        # Serialize against reinitialize_shard_subscriptions: the reverse
        # index and node_pubsub_mapping must not change between the lookup
        # and the per-node sunsubscribe call below.
        async with self._shard_state_lock:
            for s_channel in args:
                normalized_key = next(iter(self._normalize_keys({s_channel: None})))
                # Route via the reverse index so we unsubscribe on the node
                # that actually holds the subscription. After a slot migration
                # the cluster's current owner may no longer be that node.
                name = self._shard_channel_to_node.get(normalized_key)
                if name and name in self.node_pubsub_mapping:
                    pubsub = self.node_pubsub_mapping[name]
                else:
                    node = self.cluster.get_node_from_key(s_channel)
                    if not node or node.name not in self.node_pubsub_mapping:
                        continue
                    pubsub = self.node_pubsub_mapping[node.name]
                async with self._pubsub_io_lock(pubsub):
                    await pubsub.sunsubscribe(s_channel)
                self.pending_unsubscribe_shard_channels.update(
                    pubsub.pending_unsubscribe_shard_channels
                )

    async def reinitialize_shard_subscriptions(self) -> None:
        """
        Reconcile per-node shard subscriptions against the cluster's current
        slot ownership map. For each tracked shard channel whose owning node
        has changed (e.g. after CLUSTER SETSLOT / failover), sunsubscribe on
        the old node's pubsub and ssubscribe on the new owner's pubsub,
        preserving any registered handler.
        """
        uncovered: list = []
        made_progress = False
        first_migrate_error: Optional[BaseException] = None
        async with self._shard_state_lock:
            for channel, handler in list(self.shard_channels.items()):
                old_name = self._shard_channel_to_node.get(channel)
                if channel in self.pending_unsubscribe_shard_channels:
                    # A reconnect clears the per-node pending set and replays
                    # SSUBSCRIBE, so a channel the node still holds without a
                    # pending unsubscribe is subscribed again, not leaving, and
                    # its cluster-level entry is stale. Check the pending set
                    # first: handle_message drops the channel before its
                    # pending entry.
                    old_pubsub = self.node_pubsub_mapping.get(old_name)
                    if (
                        old_pubsub is None
                        or channel in old_pubsub.pending_unsubscribe_shard_channels
                        or channel not in old_pubsub.shard_channels
                    ):
                        continue
                    self.pending_unsubscribe_shard_channels.discard(channel)
                try:
                    new_node = self.cluster.get_node_from_key(channel)
                except SlotNotCoveredError:
                    # Slot is transiently uncovered (mid-migration / partial
                    # topology refresh). Defer this channel so coverable
                    # siblings still reconcile this pass; we surface the
                    # error below so the caller (and logs) know not every
                    # channel was reconciled. Retry happens on the next
                    # slots-cache change notification.
                    uncovered.append(channel)
                    continue
                if old_name == new_node.name:
                    owner = self.node_pubsub_mapping.get(new_node.name)
                    if owner is not None and channel in owner.shard_channels:
                        continue
                    # The reverse index names this node but the subscription is
                    # not there. _migrate_shard_channel detaches from the old
                    # owner before it advances the index, so a pass that failed
                    # to attach leaves the channel subscribed nowhere - and once
                    # ownership moves back, this short-circuit would skip it for
                    # the lifetime of the pubsub. Re-attach instead of trusting
                    # the index; there is nothing to sunsubscribe from.
                    old_name = None
                try:
                    await self._migrate_shard_channel(
                        channel, handler, old_name, new_node
                    )
                    made_progress = True
                except (ConnectionError, TimeoutError, OSError) as e:
                    # Transient connectivity error while subscribing on the
                    # new owner (or unsubscribing on the old owner if its
                    # handler chose to re-raise). Do not abort reconciliation
                    # for sibling channels: _shard_channel_to_node was not
                    # advanced for this channel, so the next slots-cache
                    # change notification will retry it.
                    logger.warning(
                        "shard channel %r migration deferred: %s: %s",
                        channel,
                        type(e).__name__,
                        e,
                    )
                    if first_migrate_error is None:
                        first_migrate_error = e
                    continue
            # Garbage-collect per-node pubsubs that no longer hold any
            # subscription so their connections are released.
            for name, pubsub in list(self.node_pubsub_mapping.items()):
                if not pubsub.subscribed:
                    await self._drop_node_pubsub(name, pubsub)
        if uncovered:
            # Surface the uncovered channels so the caller (and observer
            # notification path) knows reconciliation was incomplete. All
            # coverable siblings have already been migrated above.
            raise SlotNotCoveredError(
                f"{len(uncovered)} shard channel(s) left unreconciled; "
                f"slot(s) not covered by the cluster: {uncovered!r}"
            )
        if first_migrate_error is not None and not made_progress:
            # Every migration attempted in this pass failed transiently and
            # nothing else made progress. Re-raise the first caught error
            # (typically the root cause; later failures are often downstream
            # symptoms of the same unreachable node) so the task's done-
            # callback surfaces a single representative failure through the
            # same logger channel used for SlotNotCoveredError. Per-channel
            # WARNINGs above preserve the full forensic detail.
            raise first_migrate_error

    async def _forget_shard_channel_on_old_node(
        self, old_pubsub: PubSub, channel: Any, old_name: str
    ) -> None:
        """Drop a migrating shard channel from a node we could not tell about it.

        Forget the channel locally: the caller advances the reverse index to the
        new owner, so reconciliation will never revisit this channel, while
        ``on_connect`` would keep replaying ``SSUBSCRIBE`` for it to this very
        node on every reconnect - the server would answer ``MOVED`` and the
        subscription would never work again.
        """
        self._detach_shard_channel(old_pubsub, channel)
        # Drop the per-node pubsub when either the old node has left the cluster
        # topology - no reconnect target, so the round-robin generator must stop
        # yielding a dead one, and any sibling subscription it still holds
        # recovers through ``PubSub._execute``'s reconnect and ``on_connect``
        # replay - or the detach above left it with nothing subscribed.
        #
        # The empty case cannot be deferred to a collector elsewhere, because
        # neither of the other two can reach it. ``get_sharded_message``'s
        # unsubscribe branch needs a ``SUNSUBSCRIBE`` confirmation, and none will
        # arrive for a channel this method forgot locally - that is the whole
        # reason it is forgotten. ``reinitialize_shard_subscriptions``'s
        # end-of-pass GC only runs for the reconciliation caller, while
        # ``ssubscribe``'s lazy re-route reaches here without it. An empty pubsub
        # left in the mapping keeps a live connection with nothing subscribed on
        # it, so ``_poll_node_pubsub`` passes its ``connection is None`` guard and
        # blocks reading a socket no message can arrive on: forever when the
        # caller passed ``timeout=None``, and for the whole timeout of every pass
        # otherwise, before a single healthy node is read.
        if (
            self.cluster.get_node(node_name=old_name) is None
            or not old_pubsub.subscribed
        ):
            await self._drop_node_pubsub(old_name, old_pubsub)

    async def _migrate_shard_channel(
        self,
        channel: Any,
        handler: Optional[Callable],
        old_name: Optional[str],
        new_node: "ClusterNode",
    ) -> None:
        # Detach from the old per-node pubsub, best-effort: the old node may
        # already be unreachable during migration / failover.
        if old_name and old_name in self.node_pubsub_mapping:
            old_pubsub = self.node_pubsub_mapping[old_name]
            if old_name in self._unreachable_nodes:
                # The reader has just failed to reach this node, so a
                # ``SUNSUBSCRIBE`` cannot arrive. Skip it: the attempt would pay
                # a full reconnect (and the client's whole retry budget) behind
                # the reader on the same per-node io lock, all while this pass
                # holds ``_shard_state_lock`` - which is what turns one departed
                # node into a migration slow enough to look like a permanent
                # delivery stall.
                await self._forget_shard_channel_on_old_node(
                    old_pubsub, channel, old_name
                )
            else:
                try:
                    async with self._pubsub_io_lock(old_pubsub):
                        await old_pubsub.sunsubscribe(channel)
                except (ConnectionError, TimeoutError, OSError):
                    # redis-py's Connection has already called ``disconnect()``
                    # before raising (see Connection.read_response /
                    # send_packed_command with ``disconnect_on_error=True``), so
                    # ``old_pubsub``'s dedicated socket is gone and the
                    # ``SUNSUBSCRIBE`` never reached the server.
                    await self._forget_shard_channel_on_old_node(
                        old_pubsub, channel, old_name
                    )
        # Attach to the new per-node pubsub, preserving the handler. Decode to
        # a text key only when we must pass it as a kwarg (handler present).
        new_pubsub = self._get_node_pubsub(new_node)
        async with self._pubsub_io_lock(new_pubsub):
            if handler:
                await new_pubsub.ssubscribe(Subscription(channel, handler))
            else:
                await new_pubsub.ssubscribe(channel)
        self.shard_channels.update(new_pubsub.shard_channels)
        normalized_key = next(iter(self._normalize_keys({channel: None})))
        self._shard_channel_to_node[normalized_key] = new_node.name
        self.pending_unsubscribe_shard_channels.difference_update(
            self._normalize_keys({channel: None})
        )

    async def on_slots_changed(self) -> None:
        # Observer hook invoked by NodesManager after a slots-cache refresh.
        # Schedule reconciliation as a separate task so the caller's code
        # path (typically MovedError handling in _execute_command) is not
        # blocked on the network I/O performed by reinitialize_shard_
        # subscriptions. No-op when there are no shard subscriptions to
        # reconcile.
        if not self.shard_channels:
            return
        task = asyncio.create_task(self.reinitialize_shard_subscriptions())
        self._reconcile_tasks.add(task)
        task.add_done_callback(self._reconcile_tasks.discard)
        # Consume the task's exception (if any) so Python does not emit a
        # "Task exception was never retrieved" warning. reinitialize_shard_
        # subscriptions surfaces SlotNotCoveredError when a slot is still
        # transiently uncovered; route it through the same logger channel
        # as sync ClusterPubSubSlotsCacheListener for consistent observability.
        task.add_done_callback(self._log_reconcile_task_exception)

    @staticmethod
    def _log_reconcile_task_exception(task: "asyncio.Task") -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(
                "shard subscription reconciliation failed: %r", exc, exc_info=exc
            )

    def get_redis_connection(self) -> Optional["AbstractConnection"]:
        """
        Get the Redis connection of the pubsub connected node.

        Returns the pubsub's dedicated connection (acquired from its own
        connection pool), not from the ClusterNode's connection pool.
        This avoids the connection pool resource leak that would occur
        if we called node.acquire_connection() without releasing.
        """
        # Return the pubsub's own dedicated connection, which is acquired
        # from self.connection_pool when executing pubsub commands.
        # This is safe because it's the connection dedicated to this pubsub
        # instance, not a shared pool connection from the ClusterNode.
        return self.connection

    async def aclose(self) -> None:
        """
        Disconnect the pubsub connection.
        """
        # Cancel and gather in-flight reconciliation tasks BEFORE acquiring
        # _shard_state_lock. The tasks themselves take that lock inside
        # reinitialize_shard_subscriptions; since asyncio.Lock is non-
        # reentrant, gathering while holding it would deadlock. Awaiting
        # each task with suppressed CancelledError also avoids unhandled-
        # exception warnings if the task was created but not yet scheduled.
        # _topology_repair_tasks is deliberately left alone: it holds
        # NodesManager.initialize() calls that refresh the whole client's
        # slots cache, which is not this pubsub's to abort (see
        # _schedule_topology_repair). The set keeps them referenced until
        # they finish and discard themselves.
        if self._reconcile_tasks:
            tasks = list(self._reconcile_tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        # Hold _shard_state_lock across the rest of the teardown so it
        # observes the same mutual-exclusion discipline as ssubscribe /
        # sunsubscribe / get_sharded_message / reinitialize_shard_
        # subscriptions, which all mutate shard_channels,
        # _shard_channel_to_node, and node_pubsub_mapping under this lock.
        # Without it, super().aclose() rebinds shard_channels and
        # pending_unsubscribe_shard_channels in parallel with a concurrent
        # user-coroutine mutation that resumes during one of the awaits
        # below, silently dropping subscription intent.
        async with self._shard_state_lock:
            self._reconcile_tasks.clear()
            # Close all shard pubsub instances first, under the per-node I/O
            # lock so the socket is not torn down beneath a concurrent bounded
            # poll parked in parse_response. A bounded poll holds the lock only
            # for its timeout, and an unbounded one holds nullcontext() (see
            # _poll_io_lock), so teardown never waits indefinitely here.
            for pubsub in self.node_pubsub_mapping.values():
                async with self._pubsub_io_lock(pubsub):
                    await pubsub.aclose()
            # Drop the now-dead per-node pubsubs from the mapping so the
            # round-robin in _pubsubs_generator / _sharded_message_generator
            # cannot yield them between teardown and re-subscription.
            self.node_pubsub_mapping.clear()
            self._unreachable_nodes.clear()
            # Drop the throttle window too: a reused pubsub that keeps a
            # deadline armed before the teardown would skip the first repair
            # after resubscribing, delaying the move of its shard channels
            # off a node that is already gone.
            self._next_topology_repair = 0.0
            # _pubsubs_generator captures node_pubsub_mapping.values() into
            # a local list inside ``yield from``; clearing the mapping does
            # not reach references already held by that captured snapshot,
            # so a generator suspended mid-yield-from would still surface
            # the now-aclose()'d per-node pubsubs after re-subscription.
            # Recreate it to drop the captured list. type(self) bypasses
            # the instance-level self-shadow established at __init__
            # (self._pubsubs_generator = self._pubsubs_generator()).
            self._pubsubs_generator = type(self)._pubsubs_generator(  # type: ignore[method-assign]
                self
            )
            # Let parent handle self.connection disconnect under the lock
            # (includes disconnect, release to pool, and clearing
            # self.connection)
            await super().aclose()
            # Clear the reverse index so a reused instance doesn't route
            # against stale mappings. super().aclose() has already cleared
            # shard_channels.
            self._shard_channel_to_node.clear()

    def _raise_on_invalid_node(
        self,
        redis_cluster: "RedisCluster",
        node: Optional["ClusterNode"],
        host: Optional[str],
        port: Optional[int],
    ) -> None:
        """
        Raise a RedisClusterException if the node is None or doesn't exist in
        the cluster.
        """
        if node is None or redis_cluster.get_node(node_name=node.name) is None:
            raise RedisClusterException(
                f"Node {host}:{port} doesn't exist in the cluster"
            )

    async def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        """
        Execute a command on the appropriate cluster node.

        Taken code from redis-py and tweaked to make it work within a cluster.
        """
        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        await self._ensure_cluster_initialized()

        # For shard commands, route to appropriate node
        command = args[0].upper() if args else ""
        if command in ("SSUBSCRIBE", "SUNSUBSCRIBE", "SPUBLISH"):
            if len(args) > 1:
                # ssubscribe / sunsubscribe own both the per-node I/O lock and
                # the shard_channels / _shard_channel_to_node bookkeeping, so
                # delegate to them instead of dispatching raw. A raw dispatch
                # writes the socket unguarded against a concurrent poll and
                # records nothing, leaving the channel invisible to the reader
                # loop and to on_connect's replay.
                if command == "SSUBSCRIBE":
                    return await self.ssubscribe(*args[1:])
                if command == "SUNSUBSCRIBE":
                    return await self.sunsubscribe(*args[1:])
                channel = args[1]
                node = self.cluster.get_node_from_key(channel)
                if node:
                    pubsub = self._get_node_pubsub(node)
                    async with self._pubsub_io_lock(pubsub):
                        return await pubsub.execute_command(*args, **kwargs)

        # For other commands, use the set node or lazily discover one
        if self.connection is None:
            if self.connection_pool is None:
                if len(args) > 1:
                    # Hash the first channel and get one of the nodes holding
                    # this slot
                    channel = args[1]
                    slot = self.cluster.keyslot(channel)
                    node = self.cluster.nodes_manager.get_node_from_slot(
                        slot,
                        self.cluster.read_from_replicas,
                        self.cluster.load_balancing_strategy,
                    )
                else:
                    # Get a random node
                    node = self.cluster.get_random_node()
                self.node = node
                self.connection_pool = _ClusterNodePoolAdapter(node)

        # Now we have a connection_pool, use parent's execute_command
        return await super().execute_command(*args, **kwargs)
