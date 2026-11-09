import logging
import random
import socket
import sys
import threading
import time
import weakref
from abc import ABC, abstractmethod
from collections import OrderedDict, defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from copy import copy
from enum import Enum
from functools import cache, partial
from itertools import chain
from types import MethodType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

if TYPE_CHECKING:
    from redis.keyspace_notifications import ClusterKeyspaceNotifications

from redis import _himport_exec
from redis._defaults import DEFAULT_RETRY_BASE, DEFAULT_RETRY_CAP, DEFAULT_RETRY_COUNT
from redis._parsers import CommandsParser, Encoder
from redis._parsers.helpers import parse_scan
from redis.backoff import ExponentialWithJitterBackoff, NoBackoff
from redis.cache import CacheConfig, CacheFactory, CacheFactoryInterface, CacheInterface
from redis.client import EMPTY_RESPONSE, CaseInsensitiveDict, PubSub, Redis
from redis.commands import RedisClusterCommands
from redis.commands.helpers import list_or_args, parse_pubsub_subscriptions
from redis.commands.metadata import (
    _DEFAULT_KEYED_METADATA,
    _DEFAULT_KEYLESS_METADATA,
    _METADATA_BY_REQUEST_POLICY,
    CommandMetadata,
    CommandPolicies,
    MetadataResolver,
    RequestPolicy,
    ResponsePolicy,
    StaticMetadataResolver,
)
from redis.commands.policies import PolicyResolver, StaticPolicyResolver
from redis.connection import (
    Connection,
    ConnectionPool,
    SSLConnection,
    parse_url,
)
from redis.crc import REDIS_CLUSTER_HASH_SLOTS, key_slot
from redis.event import (
    AfterPooledConnectionsInstantiationEvent,
    AfterPubSubConnectionInstantiationEvent,
    AfterSlotsCacheRefreshEvent,
    ClientType,
    EventDispatcher,
    EventListenerInterface,
)
from redis.exceptions import (
    AskError,
    AuthenticationError,
    AuthorizationError,
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
from redis.lock import Lock
from redis.maint_notifications import (
    MaintNotificationsConfig,
    OSSMaintNotificationsHandler,
)
from redis.observability.recorder import (
    record_error_count,
    record_operation_duration,
)
from redis.retry import AbstractRetry, Retry
from redis.typing import (
    ChannelT,
    FieldT,
    PubSubHandler,
    Subscription,
)
from redis.utils import (
    check_protocol_version,
    command_text,
    deprecated_args,
    deprecated_function,
    dict_merge,
    experimental_method,
    list_keys_to_dict,
    merge_result,
    safe_str,
    str_if_bytes,
    truncate_text,
)

logger = logging.getLogger(__name__)


def is_debug_log_enabled():
    return logger.isEnabledFor(logging.DEBUG)


def get_node_name(host: str, port: Union[str, int]) -> str:
    return f"{host}:{port}"


@cache
def _resolve_localhost() -> str:
    """
    The address a node given as ``localhost`` is cached under. Resolved once,
    so lookups such as the one on every MOVED or ASK redirect don't each pay
    for a resolver call.
    """
    return socket.gethostbyname("localhost")


def _fill_missing_redirect_host(error: AskError, origin_host: str) -> None:
    """
    Give a MOVED or ASK redirect that came without a host the host of the node
    that sent it. A node whose endpoint is unknown
    (``cluster-preferred-endpoint-type unknown-endpoint``) redirects with only a
    port, as in ``MOVED 3999 :6381``, and expects the client to reach out on
    the endpoint it sent the command to.

    ``origin_host`` is the host the client connected to, which has already been
    through ``address_remap``, so the redirect is remapped a second time when it
    is followed. That is harmless for a remap that only picks the port, as for a
    proxy, but not for one that changes the host.
    """
    if not error.host:
        error.host = origin_host
        error.node_addr = error.host, error.port


@deprecated_args(
    allowed_args=["redis_node"],
    reason="Use get_connection(redis_node) instead",
    version="5.3.0",
)
def get_connection(redis_node: Redis, *args, **options) -> Connection:
    return redis_node.connection or redis_node.connection_pool.get_connection()


def parse_scan_result(command, res, **options):
    cursors = {}
    ret = []
    for node_name, response in res.items():
        cursor, r = parse_scan(response, **options)
        cursors[node_name] = cursor
        ret += r

    return cursors, ret


def parse_pubsub_numsub(command, res, **options):
    numsub_d = OrderedDict()
    for numsub_tups in res.values():
        for channel, numsubbed in numsub_tups:
            try:
                numsub_d[channel] += numsubbed
            except KeyError:
                numsub_d[channel] = numsubbed

    ret_numsub = [(channel, numsub) for channel, numsub in numsub_d.items()]
    return ret_numsub


def _first_successful_reply(res: Dict[str, Any]) -> Any:
    """
    Any reply will do: the errors were already raised or dropped, so every reply left is
    an equal success.
    """
    return next(iter(res.values()))


def _aggregate_replies(res: Dict[str, Any], aggregate: Callable[[Any], Any]) -> Any:
    """
    Fold the replies of several nodes into one, the way an ``agg_*`` response policy asks.

    An array reply is folded element-wise - SCRIPT EXISTS answers one flag per SHA1, WAITAOF
    a pair of counts - and any other reply is folded as a whole.
    """
    replies = list(res.values())
    if isinstance(replies[0], (list, tuple)):
        return [aggregate(column) for column in zip(*replies)]
    return aggregate(replies)


def _logical_and(values: Any) -> Any:
    # Keeps the type the replies were parsed into: integer flags stay integers, and the
    # booleans a response callback produced stay booleans.
    return type(values[0])(all(values))


def _logical_or(values: Any) -> Any:
    return type(values[0])(any(values))


# How the nodes a command is sent to are picked, by the command's request policy. Each
# callback is handed the cluster client, the name the command is routed by and the command's
# arguments, ``args[0]`` included. A MULTI_SHARD command routed here was not split - its keys
# share a slot, or are not at fixed positions - so it is routed by its keys as a whole.
_REQUEST_POLICY_CALLBACKS: Dict[RequestPolicy, Callable[..., List["ClusterNode"]]] = {
    RequestPolicy.DEFAULT_KEYLESS: lambda cluster, command, *args: [
        cluster.get_keyless_target_node(args[0])
    ],
    RequestPolicy.DEFAULT_KEYED: lambda cluster, command, *args: (
        cluster.get_nodes_from_slot(command, *args)
    ),
    RequestPolicy.DEFAULT_NODE: lambda cluster, command, *args: [
        cluster.get_default_node()
    ],
    RequestPolicy.ALL_SHARDS: lambda cluster, command, *args: cluster.get_primaries(),
    RequestPolicy.ALL_NODES: lambda cluster, command, *args: cluster.get_nodes(),
    RequestPolicy.ALL_REPLICAS: lambda cluster, command, *args: cluster.get_replicas(),
    RequestPolicy.MULTI_SHARD: lambda cluster, command, *args: (
        cluster.get_nodes_from_slot(command, *args)
    ),
    RequestPolicy.SPECIAL: lambda cluster, command, *args: cluster.get_special_nodes(),
}


# How the replies of a command sent to several nodes are folded into the one reply the caller
# gets, by the command's response policy. Each callback is handed the replies keyed by node
# name, and only once the command has no result callback of its own to merge them with - see
# ``RedisCluster._process_result``. The keyless and keyed defaults, and SPECIAL, leave the
# replies as they are: a SPECIAL command needs a result callback that knows its reply.
_RESPONSE_POLICY_CALLBACKS: Dict[ResponsePolicy, Callable[[Dict[str, Any]], Any]] = {
    ResponsePolicy.ONE_SUCCEEDED: _first_successful_reply,
    ResponsePolicy.ALL_SUCCEEDED: _first_successful_reply,
    ResponsePolicy.AGG_LOGICAL_AND: partial(_aggregate_replies, aggregate=_logical_and),
    ResponsePolicy.AGG_LOGICAL_OR: partial(_aggregate_replies, aggregate=_logical_or),
    ResponsePolicy.AGG_MIN: partial(_aggregate_replies, aggregate=min),
    ResponsePolicy.AGG_MAX: partial(_aggregate_replies, aggregate=max),
    ResponsePolicy.AGG_SUM: partial(_aggregate_replies, aggregate=sum),
    ResponsePolicy.SPECIAL: lambda res: res,
    ResponsePolicy.DEFAULT_KEYLESS: lambda res: res,
    ResponsePolicy.DEFAULT_KEYED: lambda res: res,
}


def _is_expected_node_error(error: BaseException) -> bool:
    """
    Whether the error is the answer a ``ONE_SUCCEEDED`` command expects from the shards
    that do not succeed: SCRIPT KILL is answered NOTBUSY by each shard that runs no script.
    An error that leaves the command in doubt - a connection error, CLUSTERDOWN, a
    redirection - is not.
    """
    return isinstance(error, ResponseError) and not isinstance(
        error, (ClusterDownError, AskError, TryAgainError)
    )


def _is_idle_node_error(error: BaseException) -> bool:
    """
    Whether the error only says the node had nothing to do: NOTBUSY, the answer of a shard
    that runs no script or function, which SCRIPT KILL and FUNCTION KILL get from all of
    them but the one running it.
    """
    return str(error).startswith("NOTBUSY")


def _one_succeeded_error_rank(error: BaseException) -> int:
    """
    How well an error explains why no node of a ``ONE_SUCCEEDED`` command succeeded, lowest
    first. An error that leaves the command in doubt comes first, as it keeps the command
    eligible for the retry it would otherwise get. An error from the node that runs the
    script - UNKILLABLE, as the script already wrote - comes next, as it says why the command
    failed. NOTBUSY comes last: it says only that the node had nothing to stop.
    """
    if not _is_expected_node_error(error):
        return 0
    return 2 if _is_idle_node_error(error) else 1


def _split_successes_and_failure(
    res: Dict[str, Any], response_policy: ResponsePolicy
) -> Tuple[Dict[str, Any], Optional[BaseException]]:
    """
    Separate the replies of a command sent to several nodes from the errors some answered with.

    Returns the replies to process and the error that fails the command instead, if any. Any
    error fails it, except under ``ONE_SUCCEEDED``, which fails only when no node succeeded,
    as the server's ``one_succeeded`` tip asks: there the Redis errors of the other nodes are
    dropped, even one that leaves its node in doubt, and when nothing else is left the error
    that best explains the failure is reported - see ``_one_succeeded_error_rank``. An error
    that is not a Redis error is a client fault, and fails the command either way.
    """
    replies = {}
    reported_error = None
    one_succeeded = response_policy is ResponsePolicy.ONE_SUCCEEDED
    for node_name, reply in res.items():
        if not isinstance(reply, BaseException):
            replies[node_name] = reply
        elif not one_succeeded or not isinstance(reply, RedisError):
            return replies, reply
        elif reported_error is None or _one_succeeded_error_rank(
            reply
        ) < _one_succeeded_error_rank(reported_error):
            reported_error = reply

    return replies, None if replies else reported_error


def _random_reply(res: Dict[str, Any]) -> Any:
    """
    RANDOMKEY: a key from any shard that has one, so the reply is None only when every
    shard is empty - which a single shard's reply cannot tell.
    """
    keys = [key for key in res.values() if key is not None]
    return random.choice(keys) if keys else None


# How the replies of a ``response_policy:special`` command are merged, by the routed command
# name. A SPECIAL command without an entry here keeps its replies keyed by node. INFO and the
# diagnostics commands - FUNCTION STATS, MEMORY DOCTOR, the LATENCY reports - rely on that:
# each node describes itself, and a section such as ``replication`` or ``server`` is about one
# process, so no field merges meaningfully across nodes.
_SPECIAL_RESPONSE_CALLBACKS: Dict[str, Callable[[Dict[str, Any]], Any]] = {
    "RANDOMKEY": _random_reply,
}


# One command of a ``MULTI_SHARD`` command split by hash slot: the slot, the arguments of the
# command that carries that slot's keys, the ordinals of those keys among the keys of the whole
# command, and the keys themselves.
_MultiShardSplit = Tuple[int, Tuple[Any, ...], List[int], List[Any]]


def _split_by_slot(
    args: Tuple[Any, ...],
    command_details: Optional[Dict[str, Any]],
    keyslot: Callable[[Any], int],
) -> Optional[List[_MultiShardSplit]]:
    """
    Split a ``MULTI_SHARD`` command into one command per hash slot its keys map to.

    The keys are located by the command's fixed key positions - ``first_key_pos``,
    ``last_key_pos`` and ``step_count`` of its ``COMMAND`` entry - so a key travels with the
    arguments of its step (MSET's value), and the arguments before the first key and after
    the last are repeated in every command. Each command keeps its keys in the order the
    caller gave them, and the commands are ordered by the slot's first key.

    When every key maps to one slot, the one command returned is the command as the caller
    sent it, and the caller routes it by that slot, so its keys are not hashed again.

    Returns None when the keys cannot be located that way - a ``movablekeys`` command, whose
    ``first_key_pos`` is 0, or a command the parser does not know - which leaves the command
    to be routed by its keys as a whole. Returns None for a single key too, which has nothing
    to split, before its slot is computed.
    """
    if not command_details:
        return None

    first = command_details["first_key_pos"]
    last = command_details["last_key_pos"]
    step = command_details["step_count"]
    if first <= 0 or step <= 0:
        return None

    last_index = last if last >= 0 else len(args) + last
    positions = range(first, last_index + 1, step)
    if len(positions) < 2 or positions[-1] + step > len(args):
        return None

    slots = [keyslot(args[position]) for position in positions]
    if slots.count(slots[0]) == len(slots):
        return [
            (
                slots[0],
                args,
                list(range(len(positions))),
                [args[position] for position in positions],
            )
        ]

    prefix = args[:first]
    suffix = args[positions[-1] + step :]
    by_slot: Dict[int, Tuple[List[Any], List[int], List[Any]]] = {}
    for ordinal, (position, slot) in enumerate(zip(positions, slots)):
        key = args[position]
        slot_args, ordinals, keys = by_slot.setdefault(slot, ([], [], []))
        slot_args.extend(args[position : position + step])
        ordinals.append(ordinal)
        keys.append(key)

    return [
        (slot, (*prefix, *slot_args, *suffix), ordinals, keys)
        for slot, (slot_args, ordinals, keys) in by_slot.items()
    ]


def _merge_multi_shard_replies(
    res: Dict[int, Any],
    splits: List[_MultiShardSplit],
    response_policy: ResponsePolicy,
) -> Any:
    """
    Merge the per-slot replies of a ``MULTI_SHARD`` command into the reply of the command
    the caller sent.

    A command with an aggregating response policy is folded by it - EXISTS and TOUCH sum
    their counts, MSET answers with one of its successes. Without one, the command answers
    with one value per key, as MGET does, and the values are put back in the caller's key
    order.
    """
    if response_policy not in (
        ResponsePolicy.DEFAULT_KEYED,
        ResponsePolicy.DEFAULT_KEYLESS,
    ):
        return _RESPONSE_POLICY_CALLBACKS[response_policy](res)

    merged: List[Any] = [None] * sum(len(ordinals) for _, _, ordinals, _ in splits)
    for slot, _, ordinals, _ in splits:
        for ordinal, value in zip(ordinals, res[slot]):
            merged[ordinal] = value
    return merged


def _record_replies(
    kept: Dict[Any, Any], replies: Iterable[Tuple[Any, Any]]
) -> Optional[BaseException]:
    """
    Record the replies that succeeded - keyed by slot for a split ``MULTI_SHARD`` command,
    by node name for a command sent to several nodes - and return the first error another
    one answered with, if any.

    A success is kept even when another slot or node fails, so a retry sends the command
    only where it has not been answered: sending it again would run a write twice and
    change what a summed reply counts.
    """
    error = None
    for key, reply in replies:
        if not isinstance(reply, BaseException):
            kept[key] = reply
        elif error is None:
            error = reply
    return error


def _keep_target_replies(
    replies: Dict[str, Any], target_nodes: Iterable["ClusterNode"]
) -> None:
    """
    Drop the kept replies of the nodes that are no longer among the targets of a command
    sent to several nodes, so a retry merges the replies of the current targets only - as
    the main path does. A node the topology refresh replaced has answered for a shard its
    replacement is asked about again.
    """
    names = {node.name for node in target_nodes}
    for name in [name for name in replies if name not in names]:
        del replies[name]


def _with_keys(options: Dict[str, Any], keys: List[Any]) -> Dict[str, Any]:
    """
    The options of one command of a split, which must carry that command's keys: the
    client-side cache stores a reply under the ``keys`` option it was sent with.
    """
    if "keys" not in options:
        return options
    return {**options, "keys": keys}


def parse_cluster_slots(
    resp: Any, **options: Any
) -> Dict[Tuple[int, int], Dict[str, Any]]:
    current_host = options.get("current_host", "")

    def fix_server(*args: Any) -> Tuple[str, Any]:
        return str_if_bytes(args[0]) or current_host, args[1]

    slots = {}
    for slot in resp:
        start, end, primary = slot[:3]
        replicas = slot[3:]
        slots[start, end] = {
            "primary": fix_server(*primary),
            "replicas": [fix_server(*replica) for replica in replicas],
        }

    return slots


def _pair_cluster_slots(slots):
    """Pair a CLUSTER SHARDS flat ``[start, end, start, end, ...]`` slot array
    into ``[(start, end), ...]``.

    CLUSTER SHARDS returns each shard's slots as a flat integer array in both
    RESP2 and RESP3; only the outer shard container differs between protocols.
    """
    return [(slots[i], slots[i + 1]) for i in range(0, len(slots), 2)]


def parse_cluster_shards(resp, **options):
    """
    Parse CLUSTER SHARDS response.
    """
    if not resp:
        return resp
    if isinstance(resp[0], dict):
        return resp
    shards = []
    for x in resp:
        shard = {"slots": [], "nodes": []}
        for i in range(0, len(x[1]), 2):
            shard["slots"].append((x[1][i], (x[1][i + 1])))
        nodes = x[3]
        for node in nodes:
            dict_node = {}
            for i in range(0, len(node), 2):
                dict_node[node[i]] = node[i + 1]
            shard["nodes"].append(dict_node)
        shards.append(shard)

    return shards


def parse_cluster_shards_with_str_keys(resp, **options):
    """
    Parse CLUSTER SHARDS with string top-level structural keys.

    RESP2 parsing exposes top-level shard keys as ``"slots"``/``"nodes"``
    while node attribute keys keep the connection's decoded/raw form. RESP3 can
    return top-level shard dictionaries directly, so normalize only the
    structural shard keys and preserve nested node dictionaries as delivered.
    """
    if not resp:
        return resp
    if not isinstance(resp[0], dict):
        return parse_cluster_shards(resp, **options)

    shards = []
    for shard_resp in resp:
        slots = shard_resp.get(b"slots", shard_resp.get("slots", []))
        nodes = shard_resp.get(b"nodes", shard_resp.get("nodes", []))
        shard = {
            "slots": _pair_cluster_slots(slots),
            "nodes": [dict(node) if isinstance(node, dict) else node for node in nodes],
        }
        shards.append(shard)
    return shards


def parse_cluster_shards_unified(resp, **options):
    """
    Parse CLUSTER SHARDS into the approved unified shape.

    Top-level shard keys and nested node attribute keys are strings for both
    RESP2 and RESP3 wire responses.
    """
    if not resp:
        return resp
    if isinstance(resp[0], dict):
        shards = []
        for shard_resp in resp:
            slots = shard_resp.get(b"slots", shard_resp.get("slots", []))
            nodes = shard_resp.get(b"nodes", shard_resp.get("nodes", []))
            shard = {
                "slots": _pair_cluster_slots(slots),
                "nodes": [
                    {str_if_bytes(k): v for k, v in node.items()}
                    if isinstance(node, dict)
                    else node
                    for node in nodes
                ],
            }
            shards.append(shard)
        return shards

    shards = []
    for x in resp:
        shard = {"slots": [], "nodes": []}
        for i in range(0, len(x[1]), 2):
            shard["slots"].append((x[1][i], x[1][i + 1]))
        nodes = x[3]
        for node in nodes:
            dict_node = {}
            for i in range(0, len(node), 2):
                dict_node[str_if_bytes(node[i])] = node[i + 1]
            shard["nodes"].append(dict_node)
        shards.append(shard)
    return shards


def parse_cluster_myshardid(resp, **options):
    """
    Parse CLUSTER MYSHARDID response.
    """
    return str_if_bytes(resp)


PRIMARY = "primary"
REPLICA = "replica"
SLOT_ID = "slot-id"

REDIS_ALLOWED_KEYS = (
    "connection_class",
    "connection_pool",
    "connection_pool_class",
    "client_name",
    "credential_provider",
    "db",
    "decode_responses",
    "encoding",
    "encoding_errors",
    "host",
    "driver_info",
    "lib_name",
    "lib_version",
    "max_connections",
    "nodes_flag",
    "redis_connect_func",
    "password",
    "port",
    "timeout",
    "queue_class",
    "retry",
    "retry_on_timeout",
    "protocol",
    "legacy_responses",
    "socket_connect_timeout",
    "socket_keepalive",
    "socket_keepalive_options",
    "socket_read_size",
    "socket_timeout",
    "ssl",
    "ssl_ca_certs",
    "ssl_ca_data",
    "ssl_ca_path",
    "ssl_certfile",
    "ssl_cert_reqs",
    "ssl_include_verify_flags",
    "ssl_exclude_verify_flags",
    "ssl_keyfile",
    "ssl_password",
    "ssl_check_hostname",
    "ssl_validate_ocsp",
    "ssl_validate_ocsp_stapled",
    "ssl_ocsp_context",
    "ssl_ocsp_expected_cert",
    "ssl_min_version",
    "ssl_ciphers",
    "unix_socket_path",
    "username",
    "cache",
    "cache_config",
    "maint_notifications_config",
)
KWARGS_DISABLED_KEYS = ("host", "port", "retry")


def cleanup_kwargs(**kwargs):
    """
    Remove unsupported or disabled keys from kwargs
    """
    connection_kwargs = {
        k: v
        for k, v in kwargs.items()
        if k in REDIS_ALLOWED_KEYS and k not in KWARGS_DISABLED_KEYS
    }

    return connection_kwargs


class MaintNotificationsAbstractRedisCluster:
    """
    Abstract class for handling maintenance notifications logic.
    This class is expected to be used as base class together with RedisCluster.

    This class is intended to be used with multiple inheritance!

    All logic related to maintenance notifications is encapsulated in this class.
    """

    def __init__(
        self,
        maint_notifications_config: Optional[MaintNotificationsConfig],
        **kwargs,
    ):
        # Initialize maintenance notifications.
        # The RESP3 requirement is validated in RedisCluster.__init__ before the
        # NodesManager is constructed; this mixin is only ever run from there, so
        # the config it receives has already been validated.
        is_protocol_supported = check_protocol_version(kwargs.get("protocol"), 3)

        if maint_notifications_config is None and is_protocol_supported:
            maint_notifications_config = MaintNotificationsConfig()

        self.maint_notifications_config = maint_notifications_config

        if self.maint_notifications_config and self.maint_notifications_config.enabled:
            self._oss_cluster_maint_notifications_handler = (
                OSSMaintNotificationsHandler(self, self.maint_notifications_config)
            )
            # Update connection kwargs for all future nodes connections
            self._update_connection_kwargs_for_maint_notifications(
                self._oss_cluster_maint_notifications_handler
            )
            # Update existing nodes connections - they are created as part of the RedisCluster constructor
            for node in self.get_nodes():
                if node.redis_connection is None:
                    continue
                node.redis_connection.connection_pool.update_maint_notifications_config(
                    self.maint_notifications_config,
                    oss_cluster_maint_notifications_handler=self._oss_cluster_maint_notifications_handler,
                )
        else:
            self._oss_cluster_maint_notifications_handler = None

    def _update_connection_kwargs_for_maint_notifications(
        self, oss_cluster_maint_notifications_handler: OSSMaintNotificationsHandler
    ):
        """
        Update the connection kwargs for all future connections.
        """
        self.nodes_manager.connection_kwargs.update(
            {
                "oss_cluster_maint_notifications_handler": oss_cluster_maint_notifications_handler,
            }
        )


# Topology refreshes triggered by errors (connection errors, the MOVED
# threshold, pipeline failures, pubsub repair) run at most once per this many
# seconds per client. Requests in between are collapsed into one refresh, run by
# the next command once the interval has passed. After refreshes that failed in
# a row, the next one waits as long as the client's retry backoff says for that
# many failures, but never less than this. Server-pushed refreshes (SMIGRATED),
# the first initialize and direct ``NodesManager.initialize()`` calls, such as
# the one a CLUSTERDOWN or an uncovered slot runs, are not limited.
_TOPOLOGY_REFRESH_MIN_INTERVAL_SECONDS = 1.0


def _topology_refresh_delay(
    retry: Optional[AbstractRetry], failed_refreshes: int
) -> float:
    """
    How long an error-triggered topology refresh waits after the previous one:
    the minimum interval, or what the retry backoff gives for that many failed
    refreshes in a row, whichever is longer.
    """
    delay = _TOPOLOGY_REFRESH_MIN_INTERVAL_SECONDS
    # A duck-typed retry policy may have no backoff to compute a delay from
    backoff = getattr(retry, "_backoff", None)
    if failed_refreshes and backoff is not None:
        delay = max(delay, backoff.compute(failed_refreshes))
    return delay


def _should_request_refresh(reinitialize_steps: int, reinitialize_counter: int) -> bool:
    """
    Whether the error just counted reached the ``reinitialize_steps`` threshold,
    so a topology refresh should be asked for. With ``reinitialize_steps`` 1 a
    refresh is asked for on every MOVED error, with 0 never on MOVED errors. A
    negative value works like its absolute value.
    """
    return bool(reinitialize_steps) and reinitialize_counter % reinitialize_steps == 0


def _error_to_raise(
    last_refresh_unreachable_args: Optional[Tuple[Any, ...]],
    error: Exception,
) -> Exception:
    """
    The error a command, pipeline or transaction that gave up on :error:
    raises. While the last topology refresh reached no node (it raised a
    :class:`~.RedisClusterUnreachableError` with
    :last_refresh_unreachable_args:), a connection or timeout error is raised
    as a new :class:`~.RedisClusterUnreachableError` with those args and
    :error: as its cause, so a cluster that cannot be reached fails as a
    :class:`~.RedisClusterException`, whichever command ran the refresh.
    """
    if not isinstance(last_refresh_unreachable_args, tuple) or (
        type(error) not in (ConnectionError, TimeoutError)
    ):
        return error
    outage_error = RedisClusterUnreachableError(*last_refresh_unreachable_args)
    outage_error.__cause__ = error
    outage_error.__suppress_context__ = True
    return outage_error


class AbstractRedisCluster:
    RedisClusterRequestTTL = 16

    PRIMARIES = "primaries"
    REPLICAS = "replicas"
    ALL_NODES = "all"
    RANDOM = "random"
    DEFAULT_NODE = "default-node"

    NODE_FLAGS = {PRIMARIES, REPLICAS, ALL_NODES, RANDOM, DEFAULT_NODE}

    COMMAND_FLAGS = dict_merge(
        list_keys_to_dict(
            [
                "ACL CAT",
                "ACL DELUSER",
                "ACL DRYRUN",
                "ACL GENPASS",
                "ACL GETUSER",
                "ACL HELP",
                "ACL LIST",
                "ACL LOG",
                "ACL LOAD",
                "ACL SAVE",
                "ACL SETUSER",
                "ACL USERS",
                "ACL WHOAMI",
                "AUTH",
                "CLIENT LIST",
                "CLIENT SETINFO",
                "CLIENT SETNAME",
                "CLIENT GETNAME",
                "CONFIG SET",
                "CONFIG REWRITE",
                "CONFIG RESETSTAT",
                "TIME",
                "PUBSUB CHANNELS",
                "PUBSUB NUMPAT",
                "PUBSUB NUMSUB",
                "PUBSUB SHARDCHANNELS",
                "PUBSUB SHARDNUMSUB",
                "PING",
                "INFO",
                "SHUTDOWN",
                "KEYS",
                "DBSIZE",
                "BGSAVE",
                "SLOWLOG GET",
                "SLOWLOG LEN",
                "SLOWLOG RESET",
                "WAIT",
                "WAITAOF",
                "SAVE",
                "MEMORY PURGE",
                "MEMORY MALLOC-STATS",
                "MEMORY STATS",
                "LASTSAVE",
                "CLIENT TRACKINGINFO",
                "CLIENT PAUSE",
                "CLIENT UNPAUSE",
                "CLIENT UNBLOCK",
                "CLIENT ID",
                "CLIENT REPLY",
                "CLIENT GETREDIR",
                "CLIENT INFO",
                "CLIENT KILL",
                "READONLY",
                "CLUSTER INFO",
                "CLUSTER MEET",
                "CLUSTER MYSHARDID",
                "CLUSTER NODES",
                "CLUSTER REPLICAS",
                "CLUSTER RESET",
                "CLUSTER SET-CONFIG-EPOCH",
                "CLUSTER SLOTS",
                "CLUSTER SHARDS",
                "CLUSTER COUNT-FAILURE-REPORTS",
                "CLUSTER KEYSLOT",
                "COMMAND",
                "COMMAND COUNT",
                "COMMAND LIST",
                "COMMAND GETKEYS",
                "COMMAND GETKEYSANDFLAGS",
                "CONFIG GET",
                "DEBUG",
                "RANDOMKEY",
                "READONLY",
                "READWRITE",
                "TIME",
                "TFUNCTION LOAD",
                "TFUNCTION DELETE",
                "TFUNCTION LIST",
                "TFCALL",
                "TFCALLASYNC",
                "LATENCY HISTORY",
                "LATENCY LATEST",
                "LATENCY RESET",
                "MODULE LIST",
                "MODULE LOAD",
                "MODULE UNLOAD",
                "MODULE LOADEX",
            ],
            DEFAULT_NODE,
        ),
        list_keys_to_dict(
            [
                "BLESS SCAN",
                "FLUSHALL",
                "FLUSHDB",
                "FUNCTION DELETE",
                "FUNCTION FLUSH",
                "FUNCTION LIST",
                "FUNCTION LOAD",
                "FUNCTION RESTORE",
                "SCAN",
                "SCRIPT EXISTS",
                "SCRIPT FLUSH",
                "SCRIPT LOAD",
            ],
            PRIMARIES,
        ),
        list_keys_to_dict(["FUNCTION DUMP"], RANDOM),
        list_keys_to_dict(
            [
                "CLUSTER COUNTKEYSINSLOT",
                "CLUSTER DELSLOTS",
                "CLUSTER DELSLOTSRANGE",
                "CLUSTER GETKEYSINSLOT",
                "CLUSTER SETSLOT",
            ],
            SLOT_ID,
        ),
    )

    SEARCH_COMMANDS = (
        [
            "FT.CREATE",
            "FT.SEARCH",
            "FT.AGGREGATE",
            "FT.EXPLAIN",
            "FT.EXPLAINCLI",
            "FT,PROFILE",
            "FT.ALTER",
            "FT.DROPINDEX",
            "FT.ALIASADD",
            "FT.ALIASUPDATE",
            "FT.ALIASDEL",
            "FT.ALIASLIST",
            "FT.TAGVALS",
            "FT.SUGADD",
            "FT.SUGGET",
            "FT.SUGDEL",
            "FT.SUGLEN",
            "FT.SYNUPDATE",
            "FT.SYNDUMP",
            "FT.SPELLCHECK",
            "FT.DICTADD",
            "FT.DICTDEL",
            "FT.DICTDUMP",
            "FT.INFO",
            "FT._LIST",
            "FT.CONFIG",
            "FT.ADD",
            "FT.DEL",
            "FT.DROP",
            "FT.GET",
            "FT.MGET",
            "FT.SYNADD",
        ],
    )

    CLUSTER_COMMANDS_RESPONSE_CALLBACKS = {
        "CLUSTER SLOTS": parse_cluster_slots,
        "CLUSTER SHARDS": parse_cluster_shards,
        "CLUSTER MYSHARDID": parse_cluster_myshardid,
    }

    RESULT_CALLBACKS = dict_merge(
        list_keys_to_dict(["PUBSUB NUMSUB", "PUBSUB SHARDNUMSUB"], parse_pubsub_numsub),
        list_keys_to_dict(
            ["PUBSUB NUMPAT"], lambda command, res: sum(list(res.values()))
        ),
        list_keys_to_dict(
            ["KEYS", "PUBSUB CHANNELS", "PUBSUB SHARDCHANNELS"], merge_result
        ),
        list_keys_to_dict(
            [
                "PING",
                "CONFIG SET",
                "CONFIG REWRITE",
                "CONFIG RESETSTAT",
                "CLIENT SETNAME",
                "BGSAVE",
                "SLOWLOG RESET",
                "SAVE",
                "MEMORY PURGE",
                "CLIENT PAUSE",
                "CLIENT UNPAUSE",
            ],
            lambda command, res: all(res.values()) if isinstance(res, dict) else res,
        ),
        list_keys_to_dict(
            ["DBSIZE", "WAIT"],
            lambda command, res: sum(res.values()) if isinstance(res, dict) else res,
        ),
        list_keys_to_dict(
            ["CLIENT UNBLOCK"], lambda command, res: 1 if sum(res.values()) > 0 else 0
        ),
        list_keys_to_dict(["BLESS SCAN", "SCAN"], parse_scan_result),
        list_keys_to_dict(
            ["SCRIPT LOAD"], lambda command, res: list(res.values()).pop()
        ),
        list_keys_to_dict(
            ["SCRIPT EXISTS"], lambda command, res: [all(k) for k in zip(*res.values())]
        ),
        list_keys_to_dict(["SCRIPT FLUSH"], lambda command, res: all(res.values())),
    )

    ERRORS_ALLOW_RETRY = (
        ConnectionError,
        TimeoutError,
        ClusterDownError,
        SlotNotCoveredError,
    )

    # Connection errors that do not mean the node failed: the client's own pool
    # is exhausted, the server is at its maxclients limit, or the credentials
    # were rejected. Recycling connections or refreshing the topology cannot
    # repair them, so the cluster clients and their pipelines re-raise them
    # without either, and without a retry.
    NOT_NODE_FAILURE_ERRORS = (
        AuthenticationError,
        MaxConnectionsError,
        MaxClientsReachedError,
    )

    @staticmethod
    def _is_retryable_cluster_error(error: Exception) -> bool:
        """
        ``is_retryable`` for a cluster retry: never retry one of
        ``NOT_NODE_FAILURE_ERRORS``.
        """
        return not isinstance(error, AbstractRedisCluster.NOT_NODE_FAILURE_ERRORS)

    @staticmethod
    def _retry_backoff_delay(retry: AbstractRetry, failures: int) -> float:
        """
        The delay before the next attempt of a cluster retry loop, after
        ``failures`` failed attempts, as computed by the retry's backoff. A
        duck-typed retry policy with no backoff gives no delay.
        """
        backoff = getattr(retry, "_backoff", None)
        if backoff is None:
            return 0
        if failures == 1:
            # A new run of failures starts, as in Retry.call_with_retry
            backoff.reset()
        return backoff.compute(failures)

    def replace_default_node(self, target_node: "ClusterNode" = None) -> None:
        """Replace the default cluster node.
        A random cluster node will be chosen if target_node isn't passed, and primaries
        will be prioritized. The default node will not be changed if there are no other
        nodes in the cluster.

        Args:
            target_node (ClusterNode, optional): Target node to replace the default
            node. Defaults to None.
        """
        if target_node:
            self.nodes_manager.default_node = target_node
        else:
            curr_node = self.get_default_node()
            primaries = [node for node in self.get_primaries() if node != curr_node]
            if primaries:
                # Choose a primary if the cluster contains different primaries
                self.nodes_manager.default_node = random.choice(primaries)
            else:
                # Otherwise, choose a primary if the cluster contains different primaries
                replicas = [node for node in self.get_replicas() if node != curr_node]
                if replicas:
                    self.nodes_manager.default_node = random.choice(replicas)


class RedisCluster(
    AbstractRedisCluster, MaintNotificationsAbstractRedisCluster, RedisClusterCommands
):
    # Type discrimination marker for @overload self-type pattern
    _is_async_client: Literal[False] = False

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> "RedisCluster":
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
        return cls(url=url, **kwargs)

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
    def __init__(
        self,
        host: Optional[str] = None,
        port: int = 6379,
        startup_nodes: Optional[List["ClusterNode"]] = None,
        cluster_error_retry_attempts: int = DEFAULT_RETRY_COUNT,
        retry: Optional["Retry"] = None,
        require_full_coverage: bool = True,
        reinitialize_steps: int = 5,
        read_from_replicas: bool = False,
        load_balancing_strategy: Optional["LoadBalancingStrategy"] = None,
        dynamic_startup_nodes: bool = True,
        url: Optional[str] = None,
        address_remap: Optional[Callable[[Tuple[str, int]], Tuple[str, int]]] = None,
        cache: Optional[CacheInterface] = None,
        cache_config: Optional[CacheConfig] = None,
        event_dispatcher: Optional[EventDispatcher] = None,
        policy_resolver: Optional[PolicyResolver] = None,
        maint_notifications_config: Optional[MaintNotificationsConfig] = None,
        metadata_resolver: Optional[MetadataResolver] = None,
        **kwargs,
    ):
        """
        Initialize a new RedisCluster client.

        :param startup_nodes:
            List of nodes from which initial bootstrapping can be done
        :param host:
            Can be used to point to a startup node
        :param port:
            Can be used to point to a startup node
        :param require_full_coverage:
            When set to False (default value): the client will not require a
            full coverage of the slots. However, if not all slots are covered,
            and at least one node has 'cluster-require-full-coverage' set to
            'yes,' the server will throw a ClusterDownError for some key-based
            commands. See -
            https://redis.io/topics/cluster-tutorial#redis-cluster-configuration-parameters
            When set to True: all slots must be covered to construct the
            cluster client. If not all slots are covered, RedisClusterException
            will be thrown.
        :param read_from_replicas:
            @deprecated - please use load_balancing_strategy instead
            Enable read from replicas in READONLY mode. You can read possibly
            stale data.
            When set to true, read commands will be assigned between the
            primary and its replications in a Round-Robin manner.
        :param load_balancing_strategy:
            Enable read from replicas in READONLY mode and defines the load balancing
            strategy that will be used for cluster node selection.
            The data read from replicas is eventually consistent with the data in primary nodes.
        :param dynamic_startup_nodes:
            Set the RedisCluster's startup nodes to all of the discovered nodes.
            If true (default value), the cluster's discovered nodes will be used to
            determine the cluster nodes-slots mapping in the next topology refresh.
            It will remove the initial passed startup nodes if their endpoints aren't
            listed in the CLUSTER SLOTS output.
            If you use dynamic DNS endpoints for startup nodes but CLUSTER SLOTS lists
            specific IP addresses, it is best to set it to false.
        :param cluster_error_retry_attempts:
            @deprecated - Please configure the 'retry' object instead
            In case 'retry' object is set - this argument is ignored!

            Number of times to retry before raising an error when
            :class:`~.TimeoutError` or :class:`~.ConnectionError`, :class:`~.SlotNotCoveredError` or
            :class:`~.ClusterDownError` are encountered
        :param retry:
            A retry object that defines the retry strategy and the number of
            retries for the cluster client. Its backoff sets the delay before
            each retry of a command or a pipeline; one that failed with
            :class:`~.ClusterDownError` or :class:`~.SlotNotCoveredError` also
            waits a fixed 0.25 seconds before it. A topology refresh triggered
            by an error (connection errors, the ``reinitialize_steps`` MOVED
            threshold) is run by the first command attempt to start once it is
            due, at most once per second; after refreshes that failed in a row,
            the next one waits as long as the backoff gives for that many
            failures, but at least one second. A command that failed with a
            connection error or a timeout gives up with
            :class:`~.RedisClusterUnreachableError` once a refresh that ended
            since reached no node, or when it runs out of retries while the
            last refresh did.
            Backoffs that keep state between calls
            (:class:`~.DecorrelatedJitterBackoff`) are not fully supported: the
            retry object is shared by every command and pipeline of the client,
            so concurrent retries reset and advance each other's backoff, and
            the delays can come out shorter or longer than configured.
        :param reinitialize_steps:
            Specifies the number of MOVED errors that need to occur before
            reinitializing the whole cluster topology. If a MOVED error occurs
            and the cluster does not need to be reinitialized on this current
            error handling, only the MOVED slot will be patched with the
            redirected node.
            To reinitialize the cluster on every MOVED error, set
            reinitialize_steps to 1.
            To avoid reinitializing the cluster on moved errors, set
            reinitialize_steps to 0. A negative value is not the same as 0: it
            works like its absolute value (-3 is the same as 3).
            Every MOVED error patches its slot, so the redirect goes to the new
            owner; the refresh is only asked for, and like every refresh
            triggered by an error, it is run by the next command attempt once
            due, at most once per second.
        :param address_remap:
            An optional callable which, when provided with an internal network
            address of a node, e.g. a `(host, port)` tuple, will return the address
            where the node is reachable.  This can be used to map the addresses at
            which the nodes _think_ they are, to addresses at which a client may
            reach them, such as when they sit behind a proxy.

        :param policy_resolver:
            Decides the request/response policies each command is routed by - see
            `redis.commands.policies.PolicyResolver`. Defaults to a
            `StaticPolicyResolver` built for this client, which resolves the command
            metadata this library ships. A resolver built from a live `COMMAND` reply is a
            snapshot of the server it was read from, so give each client its own rather
            than sharing one across clients on different servers.
            This is the narrow routing view of `metadata_resolver`, which supersedes it:
            prefer `metadata_resolver`, which serves routing and every other
            command-metadata consumer from one object. When both are given this one still
            decides which nodes a command targets, so that its 7.1.0 behavior does not
            move. It does not decide anything the routing view cannot express: replica
            safety and client-side-cache eligibility keep coming from `metadata_resolver`,
            because a `CommandPolicies` record carries no `is_readonly` flag to answer them
            with.
        :param metadata_resolver:
            Serves the command metadata this client reads - see
            `redis.commands.metadata.MetadataResolver`. Routing is derived from it, and it
            is handed to every node's client, where it also decides which commands are
            eligible for client-side caching. Defaults to a `StaticMetadataResolver` built
            for this client, which resolves the command metadata this library ships; the
            library never reads `COMMAND` on its own behalf for this, so the default adds no
            round trips. Resolvers chain through `with_fallback`, first match wins, so one
            placed in front of `StaticMetadataResolver` overrides the commands it carries
            while the static records answer for everything else. To decide eligibility and
            routing from the connected server, pass a `DynamicMetadataResolver` built from a
            live `COMMAND` reply - use it with care, because reading that reply relies on a
            class in the private `redis._parsers` package. Note that a server-derived resolver
            decides routing here too, and the live tips route some commands differently from
            the shipped records. `MGET` is split by hash slot instead of raising for keys in
            different slots. `MSET`, `DEL` and `UNLINK` are split by hash slot too. A split
            `MSET` is not atomic: if one slot fails, the keys of the other slots are still
            written. `DBSIZE` and `KEYS` are sent to every primary instead of the default
            node. So pair such a resolver with an explicit
            `policy_resolver=StaticPolicyResolver()` to keep routing on the shipped records.
        :param maint_notifications_config:
            Configures the nodes connections to support maintenance notifications - see
            `redis.maint_notifications.MaintNotificationsConfig` for details.
            Only supported with RESP3.
            If not provided and protocol is RESP3, the maintenance notifications
            will be enabled by default (logic is included in the NodesManager
            initialization).
        :**kwargs:
            Extra arguments that will be sent into Redis instance when created
            (See Official redis-py doc for supported kwargs - the only limitation
            is that you can't provide 'retry' object as part of kwargs.
            [https://github.com/andymccurdy/redis-py/blob/master/redis/client.py])
            Some kwargs are not supported and will raise a
            RedisClusterException:
                - db (Redis do not support database SELECT in cluster mode)

        """
        if startup_nodes is None:
            startup_nodes = []

        if "db" in kwargs:
            # Argument 'db' is not possible to use in cluster mode
            raise RedisClusterException(
                "Argument 'db' is not possible to use in cluster mode"
            )

        if "retry" in kwargs:
            # Argument 'retry' is not possible to be used in kwargs when in cluster mode
            # the kwargs are set to the lower level connections to the cluster nodes
            # and there we provide retry configuration without retries allowed.
            # The retries should be handled on cluster client level.
            raise RedisClusterException(
                "The 'retry' argument cannot be used in kwargs when running in cluster mode."
            )

        if kwargs.get("ssl") and not issubclass(
            kwargs.get("connection_class", SSLConnection), SSLConnection
        ):
            raise RedisClusterException(
                "The 'ssl' argument requires 'connection_class' to be an "
                "SSLConnection subclass"
            )

        if kwargs.pop("unix_socket_path", None) is not None:
            raise RedisClusterException(
                "RedisCluster does not currently support Unix Domain Socket connections"
            )

        # Get the startup node/s
        from_url = False
        if url is not None:
            from_url = True
            url_options = parse_url(url)
            if "path" in url_options:
                raise RedisClusterException(
                    "RedisCluster does not currently support Unix Domain "
                    "Socket connections"
                )
            if "db" in url_options and url_options["db"] != 0:
                # Argument 'db' is not possible to use in cluster mode
                raise RedisClusterException(
                    "A ``db`` querystring option can only be 0 in cluster mode"
                )
            kwargs.update(url_options)
            host = kwargs.get("host")
            port = kwargs.get("port", port)
            startup_nodes.append(ClusterNode(host, port))
        elif host is not None and port is not None:
            startup_nodes.append(ClusterNode(host, port))
        elif len(startup_nodes) == 0:
            # No startup node was provided
            raise RedisClusterException(
                "RedisCluster requires at least one node to discover the "
                "cluster. Please provide one of the following:\n"
                "1. host and port, for example:\n"
                " RedisCluster(host='localhost', port=6379)\n"
                "2. list of startup nodes, for example:\n"
                " RedisCluster(startup_nodes=[ClusterNode('localhost', 6379),"
                " ClusterNode('localhost', 6378)])"
            )
        # Update the connection arguments
        # Whenever a new connection is established, RedisCluster's on_connect
        # method should be run
        # If the user passed on_connect function we'll save it and run it
        # inside the RedisCluster.on_connect() function
        self.user_on_connect_func = kwargs.pop("redis_connect_func", None)
        kwargs.update({"redis_connect_func": self.on_connect})
        kwargs = cleanup_kwargs(**kwargs)
        if retry:
            self.retry = retry
        else:
            self.retry = Retry(
                backoff=ExponentialWithJitterBackoff(
                    base=DEFAULT_RETRY_BASE, cap=DEFAULT_RETRY_CAP
                ),
                retries=cluster_error_retry_attempts,
            )

        self.encoder = Encoder(
            kwargs.get("encoding", "utf-8"),
            kwargs.get("encoding_errors", "strict"),
            kwargs.get("decode_responses", False),
        )
        protocol = kwargs.get("protocol", None)
        if (cache_config or cache) and not check_protocol_version(protocol, 3):
            raise RedisError("Client caching is only supported with RESP version 3")

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

        # Build the client-level HIMPORT registry once (always empty at construction)
        # and share the same object with every node pool, so the fieldset registry is
        # shared cluster-wide and runtime himport_prepare mutates one object. It is
        # handed to the NodesManager and injected onto each node's pool in
        # create_redis_node; it is deliberately NOT forwarded through connection_kwargs,
        # so nodes reuse the one shared object rather than each rebuilding their own.
        self._himport_registry = HImportRegistry()

        self.command_flags = self.__class__.COMMAND_FLAGS.copy()
        self.node_flags = self.__class__.NODE_FLAGS.copy()
        self.read_from_replicas = read_from_replicas
        self.load_balancing_strategy = load_balancing_strategy
        self.reinitialize_counter = 0
        self.reinitialize_steps = reinitialize_steps
        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher
        self.startup_nodes = startup_nodes

        # Built here rather than defaulted in the signature, so that each client owns its
        # resolver and the memos it accumulates are released with the client. The one object
        # is shared with every node's client below, so a cluster resolves command metadata -
        # routing and cache eligibility both - from a single source of truth.
        if metadata_resolver is None:
            self._metadata_resolver: MetadataResolver = StaticMetadataResolver()
        else:
            self._metadata_resolver = metadata_resolver

        self.nodes_manager = NodesManager(
            startup_nodes=startup_nodes,
            from_url=from_url,
            require_full_coverage=require_full_coverage,
            dynamic_startup_nodes=dynamic_startup_nodes,
            address_remap=address_remap,
            cache=cache,
            cache_config=cache_config,
            metadata_resolver=self._metadata_resolver,
            event_dispatcher=self._event_dispatcher,
            maint_notifications_config=maint_notifications_config,
            himport_registry=self._himport_registry,
            **kwargs,
        )

        cluster_response_callbacks = dict(
            self.__class__.CLUSTER_COMMANDS_RESPONSE_CALLBACKS
        )
        legacy_responses = kwargs.get("legacy_responses", True)
        protocol = kwargs.get("protocol")
        if not legacy_responses:
            cluster_response_callbacks["CLUSTER SHARDS"] = parse_cluster_shards_unified
        elif protocol is None:
            cluster_response_callbacks["CLUSTER SHARDS"] = (
                parse_cluster_shards_with_str_keys
            )
        self.cluster_response_callbacks = CaseInsensitiveDict(
            cluster_response_callbacks
        )
        self.result_callbacks = CaseInsensitiveDict(self.__class__.RESULT_CALLBACKS)

        # For backward compatibility, mapping from existing policies to new one
        self._command_flags_mapping: dict[str, Union[RequestPolicy, ResponsePolicy]] = {
            self.__class__.RANDOM: RequestPolicy.DEFAULT_KEYLESS,
            self.__class__.PRIMARIES: RequestPolicy.ALL_SHARDS,
            self.__class__.ALL_NODES: RequestPolicy.ALL_NODES,
            self.__class__.REPLICAS: RequestPolicy.ALL_REPLICAS,
            self.__class__.DEFAULT_NODE: RequestPolicy.DEFAULT_NODE,
            SLOT_ID: RequestPolicy.DEFAULT_KEYED,
        }

        # ``policy_resolver`` is the routing view of a metadata resolver, so the two
        # arguments overlap. Resolved by precedence rather than by rejecting the
        # combination, because a user migrating from one to the other will legitimately pass
        # both: an explicit ``policy_resolver`` - the extension point that shipped in 7.1.0
        # - keeps deciding which nodes a command targets, and otherwise those policies are
        # derived from the metadata resolver.
        #
        # The precedence covers the routing view only. Replica safety and cache eligibility
        # are read from ``_metadata_resolver`` either way, because the projection a policy
        # resolver serves drops the flags they are decided from - a ``CommandPolicies``
        # record has no ``is_readonly``. So "ignores metadata_resolver" below means for the
        # target-node decision, not for the record as a whole.
        if policy_resolver is None:
            self._policy_resolver: PolicyResolver = StaticPolicyResolver(
                metadata_resolver=self._metadata_resolver
            )
        else:
            self._policy_resolver = policy_resolver
            if metadata_resolver is not None:
                logger.debug(
                    "Both policy_resolver and metadata_resolver were given; the nodes a "
                    "command targets resolve through policy_resolver and ignore "
                    "metadata_resolver. Replica safety and client-side-cache eligibility "
                    "still resolve through metadata_resolver."
                )
        self.commands_parser = CommandsParser(self)

        # Node where FT.AGGREGATE command is executed.
        self._aggregate_nodes = None
        self._lock = threading.RLock()

        MaintNotificationsAbstractRedisCluster.__init__(
            self, maint_notifications_config, **kwargs
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

    def disconnect_connection_pools(self):
        for node in self.get_nodes():
            if node.redis_connection:
                try:
                    node.redis_connection.connection_pool.disconnect()
                except OSError:
                    # Client was already disconnected. do nothing
                    pass

    def on_connect(self, connection):
        """
        Initialize the connection, authenticate and select a database and send
         READONLY if it is set during object initialization.
        """
        connection.on_connect()

        if self.read_from_replicas or self.load_balancing_strategy:
            # Sending READONLY command to server to configure connection as
            # readonly. Since each cluster node may change its server type due
            # to a failover, we should establish a READONLY connection
            # regardless of the server type. If this is a primary connection,
            # READONLY would not affect executing write commands.
            connection.send_command("READONLY")
            if str_if_bytes(connection.read_response()) != "OK":
                raise ConnectionError("READONLY command failed")

        if self.user_on_connect_func is not None:
            self.user_on_connect_func(connection)

    def get_redis_connection(self, node: "ClusterNode") -> Redis:
        if not node.redis_connection:
            with self._lock:
                if not node.redis_connection:
                    self.nodes_manager.create_redis_connections([node])
        return node.redis_connection

    def get_node(self, host=None, port=None, node_name=None):
        return self.nodes_manager.get_node(host, port, node_name)

    def get_primaries(self):
        return self.nodes_manager.get_nodes_by_server_type(PRIMARY)

    def get_replicas(self):
        return self.nodes_manager.get_nodes_by_server_type(REPLICA)

    def get_random_node(self):
        return random.choice(list(self.nodes_manager.nodes_cache.values()))

    def get_keyless_target_node(self, command_name: str) -> "ClusterNode":
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
        ) and self._is_replica_safe(command_name)
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

        Deprecated alias of :meth:`get_keyless_target_node`. Kept so the name that has
        been public since 7.1.0 keeps working; it answers from the metadata resolver just
        as the new name does.
        """
        return self.get_keyless_target_node(command_name)

    def _is_replica_safe(self, command_name: str) -> bool:
        return self._metadata_resolver.is_replica_safe(command_name)

    def get_nodes(self):
        return list(self.nodes_manager.nodes_cache.values())

    def get_node_from_key(self, key, replica=False):
        """
        Get the node that holds the key's slot.
        If replica set to True but the slot doesn't have any replicas, None is
        returned.
        """
        slot = self.keyslot(key)
        slot_cache = self.nodes_manager.slots_cache.get(slot)
        if slot_cache is None or len(slot_cache) == 0:
            raise SlotNotCoveredError(f'Slot "{slot}" is not covered by the cluster.')
        if replica and len(self.nodes_manager.slots_cache[slot]) < 2:
            return None
        elif replica:
            node_idx = 1
        else:
            # primary
            node_idx = 0

        return slot_cache[node_idx]

    def get_default_node(self):
        """
        Get the cluster's default node
        """
        return self.nodes_manager.default_node

    def get_nodes_from_slot(self, command: str, *args):
        """
        Returns a list of nodes that hold the specified keys' slots.
        """
        # get the node that holds the key's slot
        slot = self.determine_slot(*args)
        replica_safe = (
            self.read_from_replicas or self.load_balancing_strategy is not None
        ) and self._is_replica_safe(command)
        return [
            self.nodes_manager.get_node_from_slot(
                slot,
                replica_safe,
                self.load_balancing_strategy if replica_safe else None,
            )
        ]

    def _get_node_from_slot(self, command: str, slot: int) -> "ClusterNode":
        """
        Returns the node a command for the given slot is sent to, a replica when the
        command is replica safe and the client reads from replicas - for a command whose
        slot is already known, so its keys are not hashed again.
        """
        replica_safe = (
            self.read_from_replicas or self.load_balancing_strategy is not None
        ) and self._is_replica_safe(command)
        return self.nodes_manager.get_node_from_slot(
            slot,
            replica_safe,
            self.load_balancing_strategy if replica_safe else None,
        )

    def _split_multi_shard_command(self, *args) -> Optional[List[_MultiShardSplit]]:
        """
        Splits a command with the MULTI_SHARD request policy into one command per hash
        slot of its keys - see ``_split_by_slot``.
        """
        return _split_by_slot(
            args, self.commands_parser.commands.get(args[0].lower()), self.keyslot
        )

    def _execute_multi_shard_command(
        self,
        command: str,
        splits: List[_MultiShardSplit],
        response_policy: ResponsePolicy,
        **kwargs,
    ):
        """
        Sends the commands of a split MULTI_SHARD command in one pipeline and merges the
        replies - see ``_merge_multi_shard_replies``.

        The pipeline batches the commands per node, so the command costs one round trip
        per node rather than one per slot. Each command names the node holding its slot,
        so the pipeline does not split it again, and a command that fails is retried on its
        own, routed by its keys: the pipeline retries a connection error or a redirection,
        and the errors only the client retries - CLUSTERDOWN, or a slot the client knows
        no node for - are resent here, with the retries of a command sent on its own. A
        slot that succeeded is therefore not sent again, which would run a write twice
        and change what a summed reply counts. The failed slots are resent one at a time.
        """
        # Run a topology refresh an earlier error asked for first, as the retry loop of a
        # command sent on its own does, so the slots are routed by the refreshed topology
        if self.nodes_manager._refresh_requested:
            self.nodes_manager._run_requested_refresh(self.retry)
        replica_safe = (
            self.read_from_replicas or self.load_balancing_strategy is not None
        ) and self._is_replica_safe(command)
        load_balancing_strategy = self.load_balancing_strategy if replica_safe else None
        pipe = self.pipeline()
        replies: List[Any] = [None] * len(splits)
        # The node each slot's command is queued for. None for a slot that is not queued
        # or is resent: its error is recorded by the command that raised it.
        nodes: List[Optional[ClusterNode]] = [None] * len(splits)
        for index, (slot, slot_args, _, keys) in enumerate(splits):
            try:
                nodes[index] = self.nodes_manager.get_node_from_slot(
                    slot, replica_safe, load_balancing_strategy
                )
            except SlotNotCoveredError as e:
                replies[index] = e
                continue
            pipe.execute_command(
                *slot_args, target_nodes=[nodes[index]], **_with_keys(kwargs, keys)
            )

        queued = [index for index, node in enumerate(nodes) if node is not None]
        for index, reply in zip(queued, pipe.execute(raise_on_error=False)):
            replies[index] = reply
        for index, ((_, slot_args, _, keys), reply) in enumerate(zip(splits, replies)):
            if isinstance(reply, self.ERRORS_ALLOW_RETRY) and not isinstance(
                reply, ClusterPipeline.ERRORS_ALLOW_RETRY
            ):
                nodes[index] = None
                try:
                    replies[index] = self.execute_command(
                        *slot_args, **_with_keys(kwargs, keys)
                    )
                except RedisError as e:
                    replies[index] = e

        slot_replies: Dict[int, Any] = {}
        for index, ((slot, _, _, _), reply) in enumerate(zip(splits, replies)):
            if isinstance(reply, BaseException):
                # A command sent on its own counts the error it fails with. The pipeline
                # counts none, so an error read from it is counted here; one a retry
                # raised carries the connection its command already counted it on.
                node = nodes[index]
                if node is not None and not hasattr(reply, "connection"):
                    self._record_error_metric(
                        error=reply, connection=node, is_internal=False
                    )
                raise reply
            slot_replies[slot] = reply
        return _merge_multi_shard_replies(slot_replies, splits, response_policy)

    def _execute_multi_shard_command_per_slot(
        self,
        command: str,
        splits: List[_MultiShardSplit],
        response_policy: ResponsePolicy,
        slot_replies: Dict[int, Any],
        **kwargs,
    ):
        """
        Sends each command of a split MULTI_SHARD command to the node holding its slot on
        its own, and merges the replies - see ``_merge_multi_shard_replies``. Used instead
        of ``_execute_multi_shard_command`` when the client caches replies: a pipeline
        never reads the client-side cache, while a command sent on its own may be answered
        from it.

        ``slot_replies`` holds the replies of the slots that already succeeded. The caller
        keeps it across its retry attempts, so a retry resends only the slots that failed:
        resending one that succeeded would run a write twice and change what a summed reply
        counts. A slot is fixed by its keys, so it names the same command on every attempt,
        while its node is looked up again in case the topology changed.
        """
        replica_safe = (
            self.read_from_replicas or self.load_balancing_strategy is not None
        ) and self._is_replica_safe(command)
        load_balancing_strategy = self.load_balancing_strategy if replica_safe else None
        for slot, slot_args, _, keys in splits:
            if slot in slot_replies:
                continue
            node = self.nodes_manager.get_node_from_slot(
                slot, replica_safe, load_balancing_strategy
            )
            slot_replies[slot] = self._execute_command(
                node, *slot_args, **_with_keys(kwargs, keys)
            )

        return _merge_multi_shard_replies(slot_replies, splits, response_policy)

    def get_special_nodes(self) -> Optional[list["ClusterNode"]]:
        """
        Returns a list of nodes for commands with a special policy.
        """
        if not self._aggregate_nodes:
            raise RedisClusterException(
                "Cannot execute FT.CURSOR commands without FT.AGGREGATE"
            )

        return self._aggregate_nodes

    def get_random_primary_node(self) -> "ClusterNode":
        """
        Returns a random primary node
        """
        return random.choice(self.get_primaries())

    def set_default_node(self, node):
        """
        Set the default node of the cluster.
        :param node: 'ClusterNode'
        :return True if the default node was set, else False
        """
        if node is None or self.get_node(node_name=node.name) is None:
            return False
        self.nodes_manager.default_node = node
        return True

    def set_retry(self, retry: Retry) -> None:
        self.retry = retry

    def monitor(self, target_node=None):
        """
        Returns a Monitor object for the specified target node.
        The default cluster node will be selected if no target node was
        specified.
        Monitor is useful for handling the MONITOR command to the redis server.
        next_command() method returns one command from monitor
        listen() method yields commands from monitor.
        """
        if target_node is None:
            target_node = self.get_default_node()
        if target_node.redis_connection is None:
            raise RedisClusterException(
                f"Cluster Node {target_node.name} has no redis_connection"
            )
        return target_node.redis_connection.monitor()

    def pubsub(self, node=None, host=None, port=None, **kwargs):
        """
        Allows passing a ClusterNode, or host&port, to get a pubsub instance
        connected to the specified node
        """
        return ClusterPubSub(self, node=node, host=host, port=port, **kwargs)

    def keyspace_notifications(
        self,
        key_prefix: Union[str, bytes, None] = None,
        ignore_subscribe_messages: bool = True,
    ) -> "ClusterKeyspaceNotifications":
        """
        Return a :class:`~redis.keyspace_notifications.ClusterKeyspaceNotifications`
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
        from redis.keyspace_notifications import ClusterKeyspaceNotifications

        return ClusterKeyspaceNotifications(
            self,
            key_prefix=key_prefix,
            ignore_subscribe_messages=ignore_subscribe_messages,
        )

    def pipeline(self, transaction=None, shard_hint=None):
        """
        Cluster impl:
            Pipelines do not work in cluster mode the same way they
            do in normal mode. Create a clone of this object so
            that simulating pipelines will work correctly. Each
            command will be called directly when used and
            when calling execute() will only return the result stack.
        """
        if shard_hint:
            raise RedisClusterException("shard_hint is deprecated in cluster mode")

        return ClusterPipeline(
            nodes_manager=self.nodes_manager,
            commands_parser=self.commands_parser,
            startup_nodes=self.nodes_manager.startup_nodes,
            result_callbacks=self.result_callbacks,
            cluster_response_callbacks=self.cluster_response_callbacks,
            read_from_replicas=self.read_from_replicas,
            load_balancing_strategy=self.load_balancing_strategy,
            reinitialize_steps=self.reinitialize_steps,
            retry=self.retry,
            lock=self._lock,
            transaction=transaction,
            # Routing must not change just because the commands go through a pipeline, so the
            # pipeline resolves through the same objects the client does.
            policy_resolver=self._policy_resolver,
            metadata_resolver=self._metadata_resolver,
            event_dispatcher=self._event_dispatcher,
        )

    def lock(
        self,
        name,
        timeout=None,
        sleep=0.1,
        blocking=True,
        blocking_timeout=None,
        lock_class=None,
        thread_local=True,
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

    def set_response_callback(self, command, callback):
        """Set a custom Response Callback"""
        self.cluster_response_callbacks[command] = callback

    def _resolve_command_policies(
        self, *args, target_nodes_specified: bool = False
    ) -> Tuple[str, Union[CommandPolicies, CommandMetadata]]:
        """
        Resolves the policies a command routes and aggregates by.

        Returns the name the policies were decided by along with the record, because the
        name a command is known by is not always ``args[0]``: a container command is
        keyed by both of its words, and the flag tables are keyed in upper case.

        The name is normalized before any branch, so one command answers with one name
        however it got here. The result callbacks are keyed in upper case, so a name that
        kept the caller's spelling on only some paths would fire them on only some paths -
        ``execute_command("dbsize")`` would be summed and ``execute_command("dbsize",
        target_nodes=...)`` would not.

        First choice is the policy resolver. When it does not know the command, the
        fallbacks are the command's ``COMMAND_FLAGS`` entry and then whether the command
        carries a key. ``target_nodes_specified`` says the caller picked the targets, as
        named nodes or as a nodes flag.
        """
        command = args[0].upper()
        if len(args) >= 2 and f"{args[0]} {args[1]}".upper() in self.command_flags:
            command = f"{args[0]} {args[1]}".upper()

        if target_nodes_specified:
            # The caller picked its targets - named nodes, or a nodes flag that
            # ``_determine_nodes`` resolves - so nothing is routed from here, and the
            # command's own aggregation must not apply either. A response policy resolved
            # for the whole cluster (``ONE_SUCCEEDED``, an ``AGG_*``) would short-circuit
            # the loop over the nodes the caller picked, or fold their replies into one.
            # Answer with the record that aggregates nothing, and skip the resolver: with
            # the targets given, neither of its answers is used.
            return command, _DEFAULT_KEYLESS_METADATA

        policies = self._policy_resolver.resolve(args[0].lower())
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

        slot = self.determine_slot(*args)
        if slot is None:
            return command, _DEFAULT_KEYLESS_METADATA

        return command, _DEFAULT_KEYED_METADATA

    def _determine_nodes(
        self, *args, request_policy: Optional[RequestPolicy] = None, **kwargs
    ) -> List["ClusterNode"]:
        """
        Determines a nodes the command should be executed on.

        The caller resolves the command's own policy - see
        ``_resolve_command_policies`` - so the only decision left here is an explicit
        nodes flag, which overrides it.
        """
        command = args[0].upper()
        if len(args) >= 2 and f"{args[0]} {args[1]}".upper() in self.command_flags:
            command = f"{args[0]} {args[1]}".upper()

        nodes_flag = kwargs.pop("nodes_flag", None)
        if nodes_flag and self._is_nodes_flag(nodes_flag):
            # nodes flag passed by the user
            if nodes_flag in self._command_flags_mapping:
                request_policy = self._command_flags_mapping[nodes_flag]

        if request_policy is None:
            raise RedisClusterException(
                f"No targets were found to execute {args} command on"
            )

        nodes = _REQUEST_POLICY_CALLBACKS[request_policy](self, command, *args)

        if args[0].lower() == "ft.aggregate":
            self._aggregate_nodes = nodes

        return nodes

    def keyslot(self, key):
        """
        Calculate keyslot for a given key.
        See Keys distribution model in https://redis.io/topics/cluster-spec
        """
        k = self.encoder.encode(key)
        return key_slot(k)

    # HIMPORT orchestration. PREPARE/DISCARD/DISCARDALL mutate the one shared
    # HImportRegistry exactly once (every node pool references the same object, so the
    # change is visible cluster-wide and applied lazily per node). SET routes by key
    # slot to the owning primary and reuses that node's standalone himport_set (lazy
    # PREPARE bundled with SET). See ``.agents/himport_client_support_spec.md``.

    @property
    def himport_registry(self) -> HImportRegistry:
        """The cluster-wide HIMPORT fieldset registry (empty if none was declared).

        Read-only: the registry is mutated only through the HIMPORT command methods.
        """
        return self._himport_registry

    @experimental_method()
    def himport_prepare(self, fieldset_name: str, fields: Iterable[FieldT]) -> bool:
        """Declare an HIMPORT fieldset cluster-wide (shared registry, applied lazily)."""
        self._himport_registry.prepare(fieldset_name, fields)
        return True

    @experimental_method()
    def himport_discard(self, fieldset_name: str) -> int:
        """Remove an HIMPORT fieldset cluster-wide (shared registry, applied lazily)."""
        return 1 if self._himport_registry.discard(fieldset_name) else 0

    @experimental_method()
    def himport_discard_all(self) -> int:
        """Remove all HIMPORT fieldsets cluster-wide (shared registry, applied lazily)."""
        return self._himport_registry.discard_all()

    def _get_command_keys(self, *args):
        """
        Get the keys in the command. If the command has no keys in it, None is
        returned.

        NOTE: Due to a bug in redis<7.0, this function does not work properly
        for EVAL or EVALSHA when the `numkeys` arg is 0.
         - issue: https://github.com/redis/redis/issues/9493
         - fix: https://github.com/redis/redis/pull/9733

        So, don't use this function with EVAL or EVALSHA.

        Raises:
            RedisClusterException: If the cluster has no default node to resolve
                the keys against, which is the case before the slots cache is
                first populated and after the client is closed.
        """
        default_node = self.get_default_node()
        if default_node is None:
            # The keys are unknown rather than absent: there is no node to resolve them
            # against. Say that, rather than reporting a missing key the caller did
            # supply, or raising AttributeError from in here.
            #
            # The async stack needs no counterpart: its parser holds the node it
            # was initialized with and never reads the default node.
            raise RedisClusterException(
                "The cluster has no default node to resolve the keys of this "
                "command against. The client may be closed, or not initialized "
                f"yet.\nCommand: {args}"
            )

        return self.commands_parser.get_keys(default_node.redis_connection, *args)

    def determine_slot(self, *args) -> Optional[int]:
        """
        Figure out what slot to use based on args.

        Raises a RedisClusterException if there's a missing key and we can't
            determine what slots to map the command to; or, if the keys don't
            all map to the same key slot; or, when the keys have to be resolved
            through ``_get_command_keys``, if the cluster has no default node to
            resolve them against.

        Returns the slot as an ``int``, which is what the declared return type has always
        promised and what the slot map is keyed by. A command carrying its slot as an
        argument - the ``SLOT_ID`` group - is therefore cast rather than returned verbatim,
        so a caller that spelled the slot as a string gets a usable slot instead of a
        ``KeyError`` from ``get_node_from_slot``.
        """
        command = args[0]
        if self.command_flags.get(command.upper()) == SLOT_ID:
            # The command contains the slot ID. The flag table is keyed in upper case, so
            # the lookup is normalized - a raw ``execute_command("cluster countkeysinslot",
            # ...)`` names the same command as the spelling the command method sends.
            return int(args[1])

        # Get the keys in the command

        # CLIENT TRACKING is a special case.
        # It doesn't have any keys, it needs to be sent to the provided nodes
        # By default it will be sent to all nodes.
        if command.upper() == "CLIENT TRACKING":
            return None

        # EVAL and EVALSHA are common enough that it's wasteful to go to the
        # redis server to parse the keys. Besides, there is a bug in redis<7.0
        # where `self._get_command_keys()` fails anyway. So, we special case
        # EVAL/EVALSHA, and their read-only variants, which take the same arguments.
        if command.upper() in _EVAL_COMMANDS:
            # command syntax: EVAL "script body" num_keys ...
            if len(args) <= 2:
                raise RedisClusterException(f"Invalid args in command: {args}")
            num_actual_keys = int(args[2])
            eval_keys = args[3 : 3 + num_actual_keys]
            # if there are 0 keys, that means the script can be run on any node
            # so we can just return a random slot
            if len(eval_keys) == 0:
                return random.randrange(0, REDIS_CLUSTER_HASH_SLOTS)
            keys = eval_keys
        else:
            keys = self._get_command_keys(*args)
            if keys is None or len(keys) == 0:
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

    def get_encoder(self):
        """
        Get the connections' encoder
        """
        return self.encoder

    def get_connection_kwargs(self):
        """
        Get the connections' key-word arguments
        """
        return self.nodes_manager.connection_kwargs

    def _is_nodes_flag(self, target_nodes):
        return isinstance(target_nodes, str) and target_nodes in self.node_flags

    def _parse_target_nodes(self, target_nodes):
        if isinstance(target_nodes, list):
            nodes = target_nodes
        elif isinstance(target_nodes, ClusterNode):
            # Supports passing a single ClusterNode as a variable
            nodes = [target_nodes]
        elif isinstance(target_nodes, dict):
            # Supports dictionaries of the format {node_name: node}.
            # It enables to execute commands with multi nodes as follows:
            # rc.cluster_save_config(rc.get_primaries())
            nodes = target_nodes.values()
        else:
            raise TypeError(
                "target_nodes type can be one of the following: "
                "node_flag (PRIMARIES, REPLICAS, RANDOM, ALL_NODES),"
                "ClusterNode, list<ClusterNode>, or dict<any, ClusterNode>. "
                f"The passed type is {type(target_nodes)}"
            )
        return nodes

    def execute_command(self, *args, **kwargs):
        return self._internal_execute_command(*args, **kwargs)

    def _internal_execute_command(self, *args, **kwargs):
        """
        Wrapper for ERRORS_ALLOW_RETRY error handling.

        It will try the number of times specified by the retries property from
        config option "self.retry" which defaults to 10 unless manually
        configured.

        If it reaches the number of times, the command will raise the exception

        Key argument :target_nodes: can be passed with the following types:
            nodes_flag: PRIMARIES, REPLICAS, ALL_NODES, RANDOM
            ClusterNode
            list<ClusterNode>
            dict<Any, ClusterNode>
        """
        target_nodes_specified = False
        is_default_node = False
        target_nodes = None
        passed_targets = kwargs.pop("target_nodes", None)
        nodes_flag = False
        if passed_targets is not None:
            nodes_flag = self._is_nodes_flag(passed_targets)
            if not nodes_flag and not (
                isinstance(passed_targets, (list, dict, str)) and not passed_targets
            ):
                target_nodes = self._parse_target_nodes(passed_targets)
                target_nodes_specified = True

        # A nodes flag picks the targets too: ``_determine_nodes`` resolves it, and the
        # command's own aggregation must not apply to the replies
        command, command_policies = self._resolve_command_policies(
            *args, target_nodes_specified=target_nodes_specified or nodes_flag
        )

        # If an error that allows retrying was thrown, a refresh of the nodes
        # and slots cache was requested; it is run by the next attempt, of any
        # command, once due. We will retry executing the command with
        # the updated cluster setup only when the target nodes can be
        # determined again with the new cache tables. Therefore, when target
        # nodes were passed to this function, we cannot retry the command
        # execution since the nodes may not be valid anymore after the tables
        # were reinitialized. So in case of passed target nodes,
        # retry_attempts will be set to 0.
        retry_attempts = 0 if target_nodes_specified else self.retry.get_retries()
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

        # A MULTI_SHARD command whose keys span several slots is split once, as a slot is
        # fixed by its keys. Without a client-side cache the split is sent in one pipeline,
        # which retries the failed slots itself, so it is not retried here as well.
        splits = None
        if (
            not target_nodes_specified
            and command_policies.request_policy == RequestPolicy.MULTI_SHARD
        ):
            splits = self._split_multi_shard_command(*args)
            if (
                splits is not None
                and len(splits) > 1
                and self.nodes_manager._cache is None
            ):
                return self._execute_multi_shard_command(
                    command, splits, command_policies.response_policy, **kwargs
                )

        # The replies already got, kept across the attempts so a retry sends the command
        # only where it has not been answered: sending it again would run a write twice,
        # and change what a summed reply counts. Keyed by slot for a split MULTI_SHARD
        # command, by node name for a command sent to several nodes. Allocated only for
        # those: a command sent to one node is retried whole.
        replies: Optional[Dict[Any, Any]] = None

        for _ in range(execute_attempts):
            # Run a topology refresh an earlier error asked for, once due. The
            # flag is read inline: this runs before every attempt.
            if self.nodes_manager._refresh_requested:
                self.nodes_manager._run_requested_refresh(self.retry)
            if last_error is not None:
                outage_error = self.nodes_manager._error_instead_of_retry(
                    last_error, last_refresh_at
                )
                if outage_error is not None:
                    raise outage_error
            try:
                res = {}
                if not target_nodes_specified:
                    if splits is None:
                        # Determine the nodes to execute the command on
                        target_nodes = self._determine_nodes(
                            *args,
                            request_policy=command_policies.request_policy,
                            nodes_flag=passed_targets,
                        )
                    elif len(splits) > 1:
                        if replies is None:
                            replies = {}
                        return self._execute_multi_shard_command_per_slot(
                            command,
                            splits,
                            command_policies.response_policy,
                            replies,
                            **kwargs,
                        )
                    else:
                        # One slot: sent as it is, to the node of the slot the split
                        # already computed
                        target_nodes = [self._get_node_from_slot(command, splits[0][0])]

                    if not target_nodes:
                        raise RedisClusterException(
                            f"No targets were found to execute {args} command on"
                        )
                    # Per attempt: only an attempt that failed on the default
                    # node replaces it
                    is_default_node = (
                        len(target_nodes) == 1
                        and target_nodes[0] == self.get_default_node()
                    )
                one_succeeded = (
                    command_policies.response_policy == ResponsePolicy.ONE_SUCCEEDED
                )
                fan_out = len(target_nodes) > 1
                if fan_out and replies is None:
                    replies = {}
                for node in target_nodes:
                    if fan_out and node.name in replies:
                        # Answered on an earlier attempt
                        res[node.name] = replies[node.name]
                        continue
                    try:
                        reply = self._execute_command(node, *args, **kwargs)
                    except RedisError as e:
                        # Every node is asked: the one that succeeds is not known up front.
                        # SCRIPT KILL succeeds only on the shard running the script.
                        if not one_succeeded:
                            raise
                        res[node.name] = e
                        continue
                    res[node.name] = reply
                    if fan_out:
                        replies[node.name] = reply

                if one_succeeded:
                    res, error = _split_successes_and_failure(
                        res, command_policies.response_policy
                    )
                    if error is not None:
                        raise error

                # Return the processed result
                # ``command``, not ``args[0]``: the result callbacks are keyed by the name
                # the policies were decided by, so a container command passed as two words
                # - ``execute_command("command", "count")`` - is dispatched as the command
                # that was actually routed. Telemetry deliberately does not follow; see the
                # note on the metric in the retry branch below.
                return self._process_result(
                    command,
                    res,
                    response_policy=command_policies.response_policy,
                    **kwargs,
                )
            except Exception as e:
                if retry_attempts > 0 and type(e) in self.__class__.ERRORS_ALLOW_RETRY:
                    if is_default_node:
                        # Replace the default cluster node
                        self.replace_default_node()
                    # The nodes and slots cache were reinitialized.
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
                        self._record_command_metric(
                            command_name=args[0],
                            duration_seconds=time.monotonic() - start_time,
                            connection=e.connection,
                            error=e,
                        )

                        self._record_error_metric(
                            error=e,
                            connection=e.connection,
                            retry_attempts=failure_count,
                        )
                    # Space the attempts out as the retry backoff says; the
                    # refresh the error asked for runs after this delay
                    delay = self._retry_backoff_delay(self.retry, failure_count)
                    if delay > 0:
                        time.sleep(delay)
                    continue
                else:
                    # raise the exception
                    if hasattr(e, "connection"):
                        self._record_error_metric(
                            error=e,
                            connection=e.connection,
                            retry_attempts=failure_count,
                            is_internal=False,
                        )
                    raise _error_to_raise(
                        self.nodes_manager._last_refresh_unreachable_args, e
                    )

    def _himport_reconcile_discards(self, redis_node, connection):
        """Delegate to the shared sync HIMPORT executor."""
        return _himport_exec.reconcile_discards(redis_node, connection)

    def _himport_prepare_and_set(
        self,
        redis_node,
        connection,
        key,
        fieldset_name,
        values,
        fieldset,
        asking: bool = False,
    ):
        """Delegate to the shared sync HIMPORT executor."""
        return _himport_exec.prepare_and_set(
            redis_node,
            connection,
            key,
            fieldset_name,
            values,
            fieldset,
            asking=asking,
        )

    def _himport_execute_set(
        self,
        redis_node,
        connection,
        key,
        fieldset_name,
        values,
        asking: bool = False,
    ):
        """Delegate to the shared sync HIMPORT executor."""
        return _himport_exec.execute_set(
            redis_node, connection, key, fieldset_name, values, asking=asking
        )

    def _execute_command(self, target_node, *args, **kwargs):
        """
        Send a command to a node in the cluster
        """
        command = args[0]
        redis_node = None
        connection = None
        redirect_addr = None
        asking = False
        moved = False
        ttl = int(self.RedisClusterRequestTTL)

        # Start timing for observability
        start_time = time.monotonic()

        while ttl > 0:
            ttl -= 1
            # The previous attempt's connection was released by its finally and
            # may already belong to another caller. Forget it, so a failed
            # checkout below does not disconnect or release it a second time.
            connection = None
            try:
                if asking:
                    target_node = self.get_node(*redirect_addr)
                elif moved:
                    # MOVED occurred and the slots cache was updated,
                    # refresh the target node
                    slot = self.determine_slot(*args)
                    replica_safe = (
                        self.read_from_replicas
                        or self.load_balancing_strategy is not None
                    ) and self._is_replica_safe(command)
                    target_node = self.nodes_manager.get_node_from_slot(
                        slot,
                        replica_safe,
                        self.load_balancing_strategy if replica_safe else None,
                    )
                    moved = False

                redis_node = self.get_redis_connection(target_node)
                connection = get_connection(redis_node)
                himport_set = parse_himport_set_args(args)
                if asking and himport_set is None:
                    connection.send_command("ASKING")
                    redis_node.parse_response(connection, "ASKING", **kwargs)
                    asking = False
                if himport_set is not None:
                    # args == (HIMPORT_SET, key, fieldset_name, *values). A raw
                    # ``execute_command`` with too few args falls through to the
                    # normal send path below so the server returns its arity error
                    # instead of a client-side IndexError.
                    # The cluster
                    # executor lazily PREPAREs the fieldset on this connection and
                    # reconciles deferred DISCARDs, then SETs; it already applies the
                    # HIMPORT SET response callback, so it bypasses the cluster callback
                    # block below.
                    # This per-command branch in the hot dispatch path is deliberate
                    # and has no cleaner alternative: this is the only seam where the
                    # concrete routed connection is known, and connection-scoped
                    # session setup can only happen once that connection is chosen.
                    # On an ASK redirect ``asking`` is folded into the SET's own packed
                    # write (see the guard above that suppresses the standalone ASKING
                    # for HIMPORT SET) so the allowance sits immediately before the SET.
                    # Clear ``asking`` first and carry the allowance in a dedicated
                    # local: ``_himport_execute_set`` can raise a retriable MOVED/TRYAGAIN
                    # mid-exchange, and a stale ``asking`` would shadow the moved-retry
                    # branch on the next loop iteration (mirrors the async client).
                    key, fieldset_name, values = himport_set
                    ask_himport = asking
                    asking = False
                    response = self._himport_execute_set(
                        redis_node,
                        connection,
                        key,
                        fieldset_name,
                        values,
                        asking=ask_himport,
                    )
                    kwargs.pop("keys", None)
                else:
                    connection.send_command(*args, **kwargs)
                    response = redis_node.parse_response(connection, command, **kwargs)

                    # Remove keys entry, it needs only for cache.
                    kwargs.pop("keys", None)

                    if command in self.cluster_response_callbacks:
                        response = self.cluster_response_callbacks[command](
                            response, **kwargs
                        )

                self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=connection,
                )
                return response
            except AuthenticationError as e:
                e.connection = connection if connection is not None else target_node
                self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=e.connection,
                    error=e,
                )
                raise
            except MaxConnectionsError as e:
                # MaxConnectionsError indicates client-side resource exhaustion
                # (too many connections in the pool), not a node failure.
                # Don't treat this as a node failure - just re-raise the error
                # without reinitializing the cluster.
                # The connection in the error is used to report the metrics based on host and port info
                # so we use the target node object which contains the host and port info
                # because we did not get the connection yet
                e.connection = target_node
                self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=e.connection,
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
                e.connection = connection if connection is not None else target_node
                self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=e.connection,
                    error=e,
                )
                raise
            except (ConnectionError, TimeoutError) as e:
                if is_debug_log_enabled():
                    connection_details = (
                        connection.extract_connection_details()
                        if connection
                        else "no connection"
                    )
                    # Log only the command name - argument values can carry
                    # secrets or user data.
                    args_log_str = safe_str(args[0])
                    logger.debug(
                        f"{type(e).__name__} received for command {args_log_str}, on node {target_node.name}, "
                        f"and connection: {connection}, {connection_details}, error: {e}"
                    )
                # this is used to report the metrics based on host and port info
                e.connection = connection if connection else target_node

                # ConnectionError can also be raised if we couldn't get a
                # connection from the pool before timing out, so check that
                # this is an actual connection before attempting to disconnect.
                if connection is not None:
                    connection.disconnect()
                # The node's other connections are kept: a timeout or a dropped
                # connection does not mean they are broken, and closing them all
                # would reconnect every caller of a node that may be healthy.
                # One the server closed is reconnected when it is checked out
                # (ConnectionPool.ensure_connection).

                # Move the failed node to the end of the cached nodes. This does
                # not set the order the refresh queries nodes in (it shuffles the
                # startup nodes and tries the failed one last through
                # last_failed_node_name below), and the refresh replaces the
                # nodes cache, so the new order only lasts until then.
                self.nodes_manager.move_node_to_end_of_cached_nodes(target_node.name)

                # DON'T set redis_connection = None - keep the pool for reuse
                # Ask for a topology refresh, run by the next attempt, of any
                # command, once due; provide the name of the failed node so we can
                # try it last
                self.nodes_manager._request_refresh(
                    last_failed_node_name=target_node.name
                )
                self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=e.connection,
                    error=e,
                )
                raise e
            except MovedError as e:
                if is_debug_log_enabled():
                    connection_details = (
                        connection.extract_connection_details()
                        if connection
                        else "no connection"
                    )
                    # Log only the command name - argument values can carry
                    # secrets or user data.
                    args_log_str = safe_str(args[0])
                    logger.debug(
                        f"MOVED error received for command {args_log_str}, on node {target_node.name}, "
                        f"and connection: {connection}, {connection_details}, error: {e}"
                    )
                _fill_missing_redirect_host(e, target_node.host)
                # Patch the slots/nodes cache with the redirected node and try
                # again. Every 'reinitialize_steps' MOVED errors, also ask for a
                # refresh of the whole topology, run by the next attempt of any
                # command once due: the redirect needs only this slot, which the
                # patch already points to the node that owns it.
                # 'reinitialize_steps' counter will increase faster when
                # the same client object is shared between multiple threads. To
                # reduce the frequency you can set this variable in the
                # RedisCluster constructor.
                self.reinitialize_counter += 1
                self.nodes_manager.move_slot(e)
                if _should_request_refresh(
                    self.reinitialize_steps, self.reinitialize_counter
                ):
                    # The refresh also queries the node the error pointed to
                    self.nodes_manager._request_refresh(
                        additional_startup_nodes_info=[
                            self.nodes_manager.remap_host_port(e.host, e.port)
                        ]
                    )
                    # Reset the counter
                    self.reinitialize_counter = 0
                moved = True
                self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=connection,
                    error=e,
                )
                self._record_error_metric(
                    error=e,
                    connection=connection,
                )
            except TryAgainError as e:
                if is_debug_log_enabled():
                    connection_details = (
                        connection.extract_connection_details()
                        if connection
                        else "no connection"
                    )
                    # Log only the command name - argument values can carry
                    # secrets or user data.
                    args_log_str = safe_str(args[0])
                    logger.debug(
                        f"TRYAGAIN error received for command {args_log_str}, on node {target_node.name}, "
                        f"and connection: {connection}, {connection_details}"
                    )
                if ttl < self.RedisClusterRequestTTL / 2:
                    time.sleep(0.05)

                self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=connection,
                    error=e,
                )
                self._record_error_metric(
                    error=e,
                    connection=connection,
                )
            except AskError as e:
                if is_debug_log_enabled():
                    connection_details = (
                        connection.extract_connection_details()
                        if connection
                        else "no connection"
                    )
                    # Log only the command name - argument values can carry
                    # secrets or user data.
                    args_log_str = safe_str(args[0])
                    logger.debug(
                        f"ASK error received for command {args_log_str}, on node {target_node.name}, "
                        f"and connection: {connection}, {connection_details}, error: {e}"
                    )
                _fill_missing_redirect_host(e, target_node.host)
                redirect_addr = self.nodes_manager.remap_host_port(e.host, e.port)
                asking = True

                self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=connection,
                    error=e,
                )
                self._record_error_metric(
                    error=e,
                    connection=connection,
                )
            except (ClusterDownError, SlotNotCoveredError) as e:
                # ClusterDownError can occur during a failover and to get
                # self-healed, we will try to reinitialize the cluster layout
                # and retry executing the command

                # SlotNotCoveredError can occur when the cluster is not fully
                # initialized or can be temporary issue.
                # We will try to reinitialize the cluster topology
                # and retry executing the command

                time.sleep(0.25)
                # Refreshed here, not paced or asked for: this command raises the
                # refresh error if it fails
                self.nodes_manager.initialize()

                # if we have a connection, use it, otherwise use the target node
                # object which contains the host and port info
                # this is used to report the metrics based on host and port info
                e.connection = connection if connection else target_node
                self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=e.connection,
                    error=e,
                )
                raise
            except ResponseError as e:
                # this is used to report the metrics based on host and port info
                # ResponseError typically happens after get_connection() succeeds,
                # so connection should be available
                e.connection = connection if connection else target_node
                self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=e.connection,
                    error=e,
                )
                raise
            except Exception as e:
                if connection:
                    connection.disconnect()

                # if we have a connection, use it, otherwise use the target node
                # object which contains the host and port info
                # this is used to report the metrics based on host and port info
                e.connection = connection if connection else target_node
                self._record_command_metric(
                    command_name=command,
                    duration_seconds=time.monotonic() - start_time,
                    connection=e.connection,
                    error=e,
                )
                raise e
            finally:
                if connection is not None:
                    redis_node.connection_pool.release(connection)

        e = ClusterError("TTL exhausted.")
        # In this case we should have an active connection.
        # If we are here, we have received many MOVED or ASK errors and finally exhausted the TTL.
        # This means that we used an active connection to read from the socket.
        # This is used to report metrics based on the host and port information.
        e.connection = connection
        self._record_command_metric(
            command_name=command,
            duration_seconds=time.monotonic() - start_time,
            connection=connection,
            error=e,
        )
        raise e

    def _record_command_metric(
        self,
        command_name: str,
        duration_seconds: float,
        connection: Connection,
        error=None,
    ):
        """
        Records operation duration metric directly.
        """
        host = connection.host if connection else "unknown"
        port = connection.port if connection else 0
        db = str(connection.db) if connection and hasattr(connection, "db") else "0"

        record_operation_duration(
            command_name=command_name,
            duration_seconds=duration_seconds,
            server_address=host,
            server_port=port,
            db_namespace=db,
            error=error,
        )

    def _record_error_metric(
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
        record_error_count(
            server_address=connection.host,
            server_port=connection.port,
            network_peer_address=connection.host,
            network_peer_port=connection.port,
            error_type=error,
            retry_attempts=retry_attempts if retry_attempts is not None else 0,
            is_internal=is_internal,
        )

    def close(self) -> None:
        try:
            with self._lock:
                if self.nodes_manager:
                    self.nodes_manager.close()
        except AttributeError:
            # RedisCluster's __init__ can fail before nodes_manager is set
            pass

    def _process_result(self, command, res, response_policy: ResponsePolicy, **kwargs):
        """
        Process the result of the executed command.
        The function would return a dict or a single value.

        :type command: str
        :type res: dict

        `res` should be in the following format:
            Dict<node_name, command_result>

        A command's own result callback merges the replies when it has one. Otherwise a
        single reply is returned as it is, and the replies of several nodes are folded by
        the command's response policy.
        """
        if command in self.result_callbacks:
            return self.result_callbacks[command](command, res, **kwargs)
        elif len(res) == 1:
            # When we execute the command on a single node, we can
            # remove the dictionary and return a single response
            return list(res.values())[0]
        elif response_policy == ResponsePolicy.SPECIAL:
            special = _SPECIAL_RESPONSE_CALLBACKS.get(command)
            if special is not None:
                return special(res)

        return _RESPONSE_POLICY_CALLBACKS[response_policy](res)

    def load_external_module(self, funcname, func):
        """
        This function can be used to add externally defined redis modules,
        and their namespaces to the redis client.

        ``funcname`` - A string containing the name of the function to create
        ``func`` - The function, being added to this class.
        """
        setattr(self, funcname, func)

    def transaction(self, func, *watches, **kwargs):
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


class ClusterNode:
    def __init__(self, host, port, server_type=None, redis_connection=None):
        if host == "localhost":
            host = _resolve_localhost()

        self.host = host
        self.port = port
        self.name = get_node_name(host, port)
        self.server_type = server_type
        self.redis_connection = redis_connection

    def __repr__(self):
        return (
            f"[host={self.host},"
            f"port={self.port},"
            f"name={self.name},"
            f"server_type={self.server_type},"
            f"redis_connection={self.redis_connection}]"
        )

    def __eq__(self, obj):
        return isinstance(obj, ClusterNode) and obj.name == self.name

    def __hash__(self):
        return hash(self.name)


class LoadBalancingStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    ROUND_ROBIN_REPLICAS = "round_robin_replicas"
    RANDOM = "random"
    RANDOM_REPLICA = "random_replica"


# The strategies that exclude the primary. ``LoadBalancer`` derives the same thing per
# strategy as the ``replicas_only`` bit it passes to its index helpers below, but it can
# only express it as an index into one shard's node list. Named here so the keyless
# routing path - which has no shard, and so no list to index - can ask the same question.
# A strategy added to the enum has to be classified here too.
_REPLICAS_ONLY_STRATEGIES = frozenset(
    {
        LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
        LoadBalancingStrategy.RANDOM_REPLICA,
    }
)


class LoadBalancer:
    """
    Round-Robin Load Balancing
    """

    def __init__(self, start_index: int = 0) -> None:
        self.primary_to_idx: dict[str, int] = {}
        self.start_index: int = start_index
        self._lock: threading.Lock = threading.Lock()

    def get_server_index(
        self,
        primary: str,
        list_size: int,
        load_balancing_strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN,
    ) -> int:
        if load_balancing_strategy == LoadBalancingStrategy.RANDOM_REPLICA:
            return self._get_random_server_index(
                list_size,
                replicas_only=True,
            )
        elif load_balancing_strategy == LoadBalancingStrategy.RANDOM:
            return self._get_random_server_index(
                list_size,
                replicas_only=False,
            )
        else:
            return self._get_round_robin_index(
                primary,
                list_size,
                load_balancing_strategy == LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
            )

    def reset(self) -> None:
        with self._lock:
            self.primary_to_idx.clear()

    def _get_random_server_index(self, list_size: int, replicas_only: bool) -> int:
        return random.randint(1 if replicas_only else 0, list_size - 1)

    def _get_round_robin_index(
        self, primary: str, list_size: int, replicas_only: bool
    ) -> int:
        with self._lock:
            server_index = self.primary_to_idx.setdefault(primary, self.start_index)
            if replicas_only and server_index == 0:
                # skip the primary node index
                server_index = 1
            # Update the index for the next round
            self.primary_to_idx[primary] = (server_index + 1) % list_size
            return server_index


class NodesManager:
    def __init__(
        self,
        startup_nodes: list[ClusterNode],
        from_url=False,
        require_full_coverage=False,
        lock: Optional[threading.RLock] = None,
        dynamic_startup_nodes=True,
        connection_pool_class=ConnectionPool,
        address_remap: Optional[Callable[[Tuple[str, int]], Tuple[str, int]]] = None,
        cache: Optional[CacheInterface] = None,
        cache_config: Optional[CacheConfig] = None,
        cache_factory: Optional[CacheFactoryInterface] = None,
        event_dispatcher: Optional[EventDispatcher] = None,
        maint_notifications_config: Optional[MaintNotificationsConfig] = None,
        himport_registry: HImportRegistry | None = None,
        metadata_resolver: Optional[MetadataResolver] = None,
        **kwargs,
    ):
        # Shared, cluster-wide HIMPORT registry object, injected onto every node's pool
        # in create_redis_node (not forwarded through connection_kwargs, so all nodes
        # reuse the one object rather than rebuilding it per node).
        self.himport_registry = himport_registry
        self.nodes_cache: dict[str, ClusterNode] = {}
        self.slots_cache: dict[int, list[ClusterNode]] = {}
        self.startup_nodes: dict[str, ClusterNode] = {n.name: n for n in startup_nodes}
        self.default_node: Optional[ClusterNode] = None
        self._epoch: int = 0
        self.from_url = from_url
        self._require_full_coverage = require_full_coverage
        self._dynamic_startup_nodes = dynamic_startup_nodes
        self.connection_pool_class = connection_pool_class
        self.address_remap = address_remap
        # Shared, cluster-wide metadata resolver, injected onto every node's client in
        # create_redis_node for the same reason the cache and the HIMPORT registry are:
        # every node must resolve command metadata - and therefore cache eligibility -
        # through the one object the cluster client was configured with.
        self._metadata_resolver = metadata_resolver

        self._cache: Optional[CacheInterface] = None
        if cache:
            self._cache = cache
        elif cache_factory is not None:
            self._cache = cache_factory.get_cache()
        elif cache_config is not None:
            # Injected here, on a copy, rather than left to the node pools: the cluster hands
            # every node the one cache built below, so each pool sees a ``cache=`` and would
            # set the resolver on the configuration inside it - which is the caller's object,
            # since ``CacheFactory`` holds it by reference. Copying keeps a ``CacheConfig``
            # reused across clients from picking up whichever resolver was injected last,
            # exactly as ``ConnectionPool.__init__`` does for the standalone client.
            #
            # Only this branch needs it. ``cache=`` and ``cache_factory=`` hand over a whole
            # cache whose configuration the caller owns, and the node pools set the resolver
            # on it in place - the same thing they do for a standalone client given one.
            if metadata_resolver is not None and isinstance(cache_config, CacheConfig):
                cache_config = copy(cache_config)
                cache_config.set_metadata_resolver(metadata_resolver)

            self._cache = CacheFactory(cache_config).get_cache()
        self.connection_kwargs = kwargs
        self.read_load_balancer = LoadBalancer()

        # nodes_cache / slots_cache / startup_nodes / default_node are protected by _lock
        if lock is None:
            self._lock = threading.RLock()
        else:
            self._lock = lock

        # initialize holds _initialization_lock to dedup multiple calls to reinitialize;
        # note that if we hold both _lock and _initialization_lock, we _must_ acquire
        # _initialization_lock first (ie: to have a consistent order) to avoid deadlock.
        #
        # The same ordering rule extends to OSSMaintNotificationsHandler._lock, which
        # is a third lock in this graph: initialize runs a CLUSTER SLOTS round trip
        # while holding _initialization_lock, and the response can carry an SMIGRATED
        # push that is handled inline on that thread and needs the handler's _lock. The
        # full order is therefore
        #     _initialization_lock -> OSSMaintNotificationsHandler._lock
        #         -> NodesManager._lock / connection pool locks
        # ie: a thread holding the handler's _lock must never wait for
        # _initialization_lock.
        self._initialization_lock: threading.RLock = threading.RLock()
        # Ident of the thread currently running initialize, or None. Written only
        # under _initialization_lock; see the re-entrancy guard in initialize.
        self._initializing_thread_id: Optional[int] = None
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
        # A refresh requested by an error path and not run yet; the next
        # command runs it once it is due. All written under _lock.
        self._refresh_requested = False
        self._refresh_requested_at_epoch = 0
        self._requested_additional_startup_nodes: List[Tuple[str, int]] = []
        self._requested_last_failed_node_name: Optional[str] = None
        self._requested_disconnect_startup_nodes_pools = True

        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher
        self._credential_provider = self.connection_kwargs.get(
            "credential_provider", None
        )
        self.maint_notifications_config = maint_notifications_config

        self.initialize()

    def get_node(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        node_name: Optional[str] = None,
    ) -> Optional[ClusterNode]:
        """
        Get the requested node from the cluster's nodes.
        nodes.
        :return: ClusterNode if the node exists, else None
        """
        if host and port:
            # the user passed host and port
            if host == "localhost":
                host = _resolve_localhost()
            with self._lock:
                return self.nodes_cache.get(get_node_name(host=host, port=port))
        elif node_name:
            with self._lock:
                return self.nodes_cache.get(node_name)
        else:
            return None

    def move_slot(self, e: Union[AskError, MovedError]):
        """
        Update the slot's node with the redirected one
        """
        node_changed = False
        # The redirect carries the address the node advertises, like
        # CLUSTER SLOTS does, so it needs the same remapping to be reachable
        host, port = self.remap_host_port(e.host, e.port)
        with self._lock:
            redirected_node = self.get_node(host=host, port=port)
            if redirected_node is not None:
                # The node already exists
                if redirected_node.server_type is not PRIMARY:
                    # Update the node's server type
                    redirected_node.server_type = PRIMARY
            else:
                # This is a new node, we will add it to the nodes cache
                redirected_node = ClusterNode(host, port, PRIMARY)
                self.nodes_cache[redirected_node.name] = redirected_node

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
        # Dispatch outside the lock so listeners can acquire their own locks
        # without risk of deadlock. Skipped on the no-op branch to avoid
        # needless reconciliation walks under MOVED storms. A listener must
        # not break slots-cache refresh; log and continue so a single buggy
        # listener cannot starve the rest.
        if node_changed:
            try:
                self._event_dispatcher.dispatch(AfterSlotsCacheRefreshEvent())
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

    @deprecated_args(
        args_to_warn=["server_type"],
        reason=(
            "In case you need select some load balancing strategy "
            "that will use replicas, please set it through 'load_balancing_strategy'"
        ),
        version="5.3.0",
    )
    def get_node_from_slot(
        self,
        slot: int,
        read_from_replicas: bool = False,
        load_balancing_strategy: Optional[LoadBalancingStrategy] = None,
        server_type: Optional[Literal["primary", "replica"]] = None,
    ) -> ClusterNode:
        """
        Gets a node that servers this hash slot
        """

        if read_from_replicas is True and load_balancing_strategy is None:
            load_balancing_strategy = LoadBalancingStrategy.ROUND_ROBIN

        with self._lock:
            if self.slots_cache.get(slot) is None or len(self.slots_cache[slot]) == 0:
                raise SlotNotCoveredError(
                    f'Slot "{slot}" not covered by the cluster. '
                    + f'"require_full_coverage={self._require_full_coverage}"'
                )

            if len(self.slots_cache[slot]) > 1 and load_balancing_strategy:
                # get the server index using the strategy defined in load_balancing_strategy
                primary_name = self.slots_cache[slot][0].name
                node_idx = self.read_load_balancer.get_server_index(
                    primary_name, len(self.slots_cache[slot]), load_balancing_strategy
                )
            elif (
                server_type is None
                or server_type == PRIMARY
                or len(self.slots_cache[slot]) == 1
            ):
                # return a primary
                node_idx = 0
            else:
                # return a replica
                # randomly choose one of the replicas
                node_idx = random.randint(1, len(self.slots_cache[slot]) - 1)

            return self.slots_cache[slot][node_idx]

    def get_nodes_by_server_type(self, server_type: Literal["primary", "replica"]):
        """
        Get all nodes with the specified server type
        :param server_type: 'primary' or 'replica'
        :return: list of ClusterNode
        """
        with self._lock:
            return [
                node
                for node in self.nodes_cache.values()
                if node.server_type == server_type
            ]

    @deprecated_function(
        reason="This method is not used anymore internally. The startup nodes are populated automatically.",
        version="7.0.2",
    )
    def populate_startup_nodes(self, nodes):
        """
        Populate all startup nodes and filters out any duplicates
        """
        with self._lock:
            for n in nodes:
                self.startup_nodes[n.name] = n

    def move_node_to_end_of_cached_nodes(self, node_name: str) -> None:
        """
        Move a failing node to the end of startup_nodes and nodes_cache so it's
        tried last during reinitialization and when selecting the default node.
        If the node is not in the respective list, nothing is done.

        Runs under the lock: the error paths of many threads call it at once
        when a node fails, and with dynamic startup nodes both names point to
        the same dict.
        """
        with self._lock:
            # Move in startup_nodes
            if node_name in self.startup_nodes and len(self.startup_nodes) > 1:
                node = self.startup_nodes.pop(node_name)
                self.startup_nodes[node_name] = node  # Re-insert at end

            # Move in nodes_cache - this affects get_nodes_by_server_type
            # ordering which is used to select the default_node during
            # initialize()
            if node_name in self.nodes_cache and len(self.nodes_cache) > 1:
                node = self.nodes_cache.pop(node_name)
                self.nodes_cache[node_name] = node  # Re-insert at end

    def check_slots_coverage(self, slots_cache):
        # Validate if all slots are covered or if we should try next
        # startup node
        for i in range(0, REDIS_CLUSTER_HASH_SLOTS):
            if i not in slots_cache:
                return False
        return True

    def create_redis_connections(self, nodes):
        """
        This function will create a redis connection to all nodes in :nodes:
        """
        nodes = list(nodes)
        self._create_redis_connections(nodes)
        with self._lock:
            current_nodes = list(self.nodes_cache.values())
        self._dispatch_pools_instantiated(chain(current_nodes, nodes))

    def _create_redis_connections(self, nodes) -> None:
        """
        Create a redis connection for every node in :nodes: that has none,
        without registering them for re-authentication.
        """
        for node in nodes:
            if node.redis_connection is None:
                node.redis_connection = self.create_redis_node(
                    host=node.host,
                    port=node.port,
                    maint_notifications_config=self.maint_notifications_config,
                    **self.connection_kwargs,
                )

    def _dispatch_pools_instantiated(self, nodes) -> None:
        """
        Register the connection pools of :nodes: for re-authentication.

        A streaming credential provider keeps only the last callback registered,
        so the event must list the pool of every node that is still serving the
        cluster, not just the pools created last.
        """
        connection_pools = []
        seen = set()
        for node in nodes:
            if node.redis_connection is None:
                continue
            pool = node.redis_connection.connection_pool
            if id(pool) not in seen:
                seen.add(id(pool))
                connection_pools.append(pool)

        self._event_dispatcher.dispatch(
            AfterPooledConnectionsInstantiationEvent(
                connection_pools, ClientType.SYNC, self._credential_provider
            )
        )

    @staticmethod
    def _free_pool_connections(pool) -> None:
        """
        Mark the in-use connections of :pool: for reconnect, so they are
        closed once their current command completes, and disconnect its free
        connections.
        """
        with pool._lock:
            pool.update_active_connections_for_reconnect()
            pool.disconnect_free_connections()

    def _release_removed_nodes(self, old_nodes_cache, new_node_names) -> None:
        """
        Release the connections of the nodes that left the topology.

        Nodes that stay keep their connections whatever their role: the server
        keeps the session state of a connection across a role change, READONLY
        is sent on every connection when reads from replicas are enabled, and a
        command that reaches a node that no longer serves its slot gets MOVED.

        :new_node_names: are the names of the new topology taken when it was
        published, under the lock: the published cache itself can be halfway
        through a move_node_to_end_of_cached_nodes on another thread.
        """
        for name, node in old_nodes_cache.items():
            if name not in new_node_names and node.redis_connection is not None:
                if is_debug_log_enabled():
                    logger.debug(
                        f"Topology refresh: releasing connections of removed node {name}"
                    )
                self._free_pool_connections(node.redis_connection.connection_pool)

    def create_redis_node(
        self,
        host,
        port,
        **kwargs,
    ):
        # We are configuring the connection pool not to retry
        # connections on lower level clients to avoid retrying
        # connections to nodes that are not reachable
        # and to avoid blocking the connection pool.
        # The only error that will have some handling in the lower
        # level clients is ConnectionError which will trigger disconnection
        # of the socket.
        # The retries will be handled on cluster client level
        # where we will have proper handling of the cluster topology
        node_retry_config = Retry(
            backoff=NoBackoff(), retries=0, supported_errors=(ConnectionError,)
        )

        connection_pool = kwargs.pop("connection_pool", None)
        if connection_pool is not None:
            r = Redis(connection_pool=connection_pool)
        else:
            # parse_url already turns rediss:// into SSLConnection, do the same
            # for ssl=True
            if not self.from_url and kwargs.pop("ssl", False):
                kwargs.setdefault("connection_class", SSLConnection)
            # Create a redis node with a custom connection pool
            kwargs.update({"host": host})
            kwargs.update({"port": port})
            kwargs.update({"cache": self._cache})
            kwargs.update({"metadata_resolver": self._metadata_resolver})
            kwargs.update({"retry": node_retry_config})
            pool = self.connection_pool_class(**kwargs)
            if not self.from_url and not issubclass(
                pool.connection_class, SSLConnection
            ):
                # a plain connection rejects the TLS options Redis() used to ignore
                for key in [k for k in pool.connection_kwargs if k.startswith("ssl_")]:
                    del pool.connection_kwargs[key]
            r = Redis.from_pool(pool)
        # Share the one cluster-wide HIMPORT registry with this node's pool. Injected
        # here (rather than forwarded via connection_kwargs) so every node reuses the
        # same object; the node has no connections yet, so this is safe.
        if self.himport_registry is not None:
            r.connection_pool.himport_registry = self.himport_registry
            r.connection_pool.connection_kwargs["himport_registry"] = (
                self.himport_registry
            )
        return r

    def _get_or_create_cluster_node(self, host, port, role, tmp_nodes_cache):
        node_name = get_node_name(host, port)
        # check if we already have this node in the tmp_nodes_cache
        target_node = tmp_nodes_cache.get(node_name)
        if target_node is None:
            # before creating a new cluster node, check if the cluster node already
            # exists in the current nodes cache and has a valid connection so we can
            # reuse it
            redis_connection: Optional[Redis] = None
            with self._lock:
                previous_node = self.nodes_cache.get(node_name)
                if previous_node:
                    redis_connection = previous_node.redis_connection
            # don't update the old ClusterNode, so we don't update its role
            # outside of the lock
            target_node = ClusterNode(host, port, role, redis_connection)
            # add this node to the nodes cache
            tmp_nodes_cache[target_node.name] = target_node

        return target_node

    def _get_epoch(self) -> int:
        """
        Get the current epoch value. This method exists primarily to allow
        tests to mock the epoch fetch and control race condition timing.
        """
        with self._lock:
            return self._epoch

    @contextmanager
    def _initializing_on_this_thread(self):
        """
        Mark the calling thread as the one currently running ``initialize``.

        Must be entered while holding ``_initialization_lock`` - that is what
        makes the calling thread the only writer of ``_initializing_thread_id``.
        """
        self._initializing_thread_id = threading.get_ident()
        try:
            yield
        finally:
            self._initializing_thread_id = None

    @contextmanager
    def _counting_failed_refresh(self):
        """
        Record a refresh that raised, so error-triggered refreshes back off,
        and register the pools of the current nodes for re-authentication
        again: every client built during the refresh registered its own pool
        with the credential provider, which keeps only the last callback.

        A refresh interrupted before it ended (``BaseException``, such as a
        gevent timeout) is not counted as failed, but the next one still waits
        the delay, so callers that keep timing out do not run one refresh
        after another. Its outcome is unknown, so it clears the record of a
        refresh that reached no node.
        """
        try:
            yield
        except BaseException as e:
            with self._lock:
                if isinstance(e, Exception):
                    self._failed_refreshes += 1
                self._last_refresh_unreachable_args = (
                    e.args if isinstance(e, RedisClusterUnreachableError) else None
                )
                self._last_refresh_at = time.monotonic()
                self._refresh_due_at = None
                current_nodes = list(self.nodes_cache.values())
            if current_nodes:
                self._dispatch_pools_instantiated(current_nodes)
            raise

    @contextmanager
    def _releasing_queried_clients(self, disconnect_startup_nodes_pools: bool):
        """
        Collect the clients queried for CLUSTER SLOTS during a refresh, whether
        they answered or not, and release the ones that serve no node of the
        topology once the refresh ends, whether it succeeded or raised.

        Yields the two lists to fill: (node name, client) of the startup nodes,
        and the clients built for the additional startup nodes, which exist
        only for this refresh.
        """
        queried_startup_nodes: List[Tuple[str, Redis]] = []
        queried_additional_clients: List[Redis] = []
        try:
            yield queried_startup_nodes, queried_additional_clients
        finally:
            if disconnect_startup_nodes_pools and queried_startup_nodes:
                with self._lock:
                    # A copy: another thread can be halfway through
                    # move_node_to_end_of_cached_nodes on the published cache
                    nodes_cache = dict(self.nodes_cache)
                for name, r in queried_startup_nodes:
                    node = nodes_cache.get(name)
                    if node is None or node.redis_connection is not r:
                        # This client serves no node of the topology (e.g. a
                        # load balancer or DNS seed, or a startup node whose
                        # node got its own client), so don't keep its
                        # connections open
                        self._free_pool_connections(r.connection_pool)
            for r in queried_additional_clients:
                # Never a client of the topology, whatever the flag says
                self._free_pool_connections(r.connection_pool)

    def _refresh_due(self, retry: Optional[AbstractRetry] = None) -> bool:
        """
        Whether a topology refresh triggered by an error may run now: not
        before the delay of _topology_refresh_delay has passed since the
        previous refresh ended. The due time is read without the lock; a stale
        read only moves one refresh by a call. The first check after a refresh
        ended computes it under the lock, so a due time computed for an older
        refresh never replaces a newer one.
        """
        due_at = self._refresh_due_at
        if due_at is None:
            # The first check since a refresh ended
            with self._lock:
                due_at = self._refresh_due_at
                if due_at is None:
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
        _error_to_raise builds. The two values are read together under the
        lock: a refresh ending between two reads could pair the outcome of one
        refresh with the end time of the next.
        """
        with self._lock:
            unreachable_args = self._last_refresh_unreachable_args
            refresh_at = self._last_refresh_at
        if unreachable_args is None or refresh_at == last_refresh_at:
            return None
        outage_error = _error_to_raise(unreachable_args, error)
        return None if outage_error is error else outage_error

    def _request_refresh(
        self,
        additional_startup_nodes_info: Optional[List[Tuple[str, int]]] = None,
        last_failed_node_name: Optional[str] = None,
        disconnect_startup_nodes_pools: bool = True,
    ) -> None:
        """
        Ask for a topology refresh after an error. Requests are merged until
        _run_requested_refresh runs them, once due.
        """
        with self._lock:
            if not self._refresh_requested:
                self._refresh_requested = True
                self._requested_additional_startup_nodes = []
                self._requested_last_failed_node_name = None
                self._requested_disconnect_startup_nodes_pools = True
            # The epoch of the newest request. A refresh that completes after
            # it counts as answering every merged request, even one that
            # started before the error and read a slot map from before it: an
            # error that outlives it asks again
            self._refresh_requested_at_epoch = self._epoch
            for node_info in additional_startup_nodes_info or ():
                if node_info not in self._requested_additional_startup_nodes:
                    self._requested_additional_startup_nodes.append(node_info)
            if last_failed_node_name is not None:
                self._requested_last_failed_node_name = last_failed_node_name
            if not disconnect_startup_nodes_pools:
                self._requested_disconnect_startup_nodes_pools = False

    def _run_requested_refresh(self, retry: Optional[AbstractRetry] = None) -> None:
        """
        Run the requested topology refresh, if there is one and it is due.
        The command and pipeline loops check ``_refresh_requested`` themselves
        before calling this, to keep the attempts that have nothing to run free
        of a call.

        A refresh that fails is logged and asked for again; the current
        topology keeps serving. Any other error is raised, and the refresh is
        still asked for again.
        """
        if not self._refresh_requested or not self._refresh_due(retry):
            return
        with self._lock:
            if not self._refresh_requested:
                # Another caller took it
                return
            self._refresh_requested = False
            if self._refresh_requested_at_epoch != self._epoch:
                # A refresh already completed after the last request
                return
            additional_startup_nodes_info = self._requested_additional_startup_nodes
            last_failed_node_name = self._requested_last_failed_node_name
            disconnect_startup_nodes_pools = (
                self._requested_disconnect_startup_nodes_pools
            )
        try:
            self.initialize(
                additional_startup_nodes_info=additional_startup_nodes_info,
                disconnect_startup_nodes_pools=disconnect_startup_nodes_pools,
                last_failed_node_name=last_failed_node_name,
            )
        except RedisClusterException as e:
            # The failure is counted, so the request made again here runs once
            # the backoff allows, still trying the failed node last
            self._request_refresh(
                additional_startup_nodes_info=additional_startup_nodes_info,
                last_failed_node_name=last_failed_node_name,
                disconnect_startup_nodes_pools=disconnect_startup_nodes_pools,
            )
            # No node answered, or nodes answered with a topology that cannot
            # replace the current one (slots not covered, or nodes that
            # disagree on them). The current topology keeps serving until a
            # refresh succeeds: this may be a command for a healthy shard, which
            # must not fail for a refresh another error asked for, and the
            # nodes queried may be only seeds (dynamic_startup_nodes=False)
            # while the nodes of the current topology still answer.
            logger.error(f"Topology refresh failed, keeping the current one: {e}")
        except BaseException:
            # Any other error is raised, but the request is kept: the flag was
            # cleared above, and no later attempt would run it otherwise
            self._request_refresh(
                additional_startup_nodes_info=additional_startup_nodes_info,
                last_failed_node_name=last_failed_node_name,
                disconnect_startup_nodes_pools=disconnect_startup_nodes_pools,
            )
            raise

    def initialize(
        self,
        additional_startup_nodes_info: Optional[List[Tuple[str, int]]] = None,
        disconnect_startup_nodes_pools: bool = True,
        last_failed_node_name: Optional[str] = None,
    ):
        """
        Initializes the nodes cache, slots cache and redis connections.
        :startup_nodes:
            Responsible for discovering other nodes in the cluster
        :disconnect_startup_nodes_pools:
            Whether to disconnect the connection pool of the startup nodes
            after the initialization is complete. This is useful when the
            startup nodes are not part of the cluster and we want to avoid
            keeping the connection open. Only the pools that serve no node
            of the new topology are disconnected; the nodes of the topology
            keep their connections.
        :additional_startup_nodes_info:
            Additional nodes to add temporarily to the startup nodes.
            The additional nodes will be used just in the process of extraction of the slots
            and nodes information from the cluster.
            This is useful when we want to add new nodes to the cluster
            and initialize the client
            with them.
            The format of the list is a list of tuples, where each tuple contains
            the host and port of the node. The connections they open are
            released once the refresh is done.
        :last_failed_node_name:
            Name of the node that just failed and should be tried only after
            other startup and additional startup nodes during this refresh.
        """
        if self._initializing_thread_id == threading.get_ident():
            # Re-entrant call on the thread that is already refreshing the
            # topology: a push notification (SMIGRATED) arrived on the CLUSTER
            # SLOTS response below and was handled inline on this thread. The
            # outer call is mid-refresh and will publish its own result, so
            # running a nested refresh here would reset() and swap the caches
            # underneath it, only for the outer call to overwrite them again
            # with its older snapshot. Skip instead; the outer refresh reads the
            # authoritative slot map anyway, and anything it still misses is
            # recovered through MOVED redirection.
            #
            # Reading the attribute without the lock is safe: while this thread
            # holds _initialization_lock it is the only writer, so a match can
            # only ever mean "this thread set it". A stale ident belonging to
            # another thread simply fails the comparison and falls through to
            # the normal blocking acquire below.
            if is_debug_log_enabled():
                logger.debug(
                    "Topology refresh: skipping re-entrant initialize on thread "
                    f"{threading.get_ident()}"
                )
            return
        self.reset()
        tmp_nodes_cache = {}
        tmp_slots = {}
        disagreements = []
        startup_nodes_reachable = False
        fully_covered = False
        kwargs = self.connection_kwargs
        exception = None
        epoch = self._get_epoch()
        if additional_startup_nodes_info is None:
            additional_startup_nodes_info = []

        with (
            self._initialization_lock,
            self._initializing_on_this_thread(),
            self._counting_failed_refresh(),
            self._releasing_queried_clients(disconnect_startup_nodes_pools) as (
                queried_startup_nodes,
                queried_additional_clients,
            ),
        ):
            with self._lock:
                if epoch != self._epoch:
                    # another thread has already re-initialized the nodes; don't
                    # bother running again
                    return

            with self._lock:
                startup_nodes = list(self.startup_nodes.values())
            startup_node_names = {node.name for node in startup_nodes}
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
            # node's client, so a second client never replaces a live node's one.
            # Startup nodes are named after the resolved localhost, like
            # ClusterNode names them.
            additional_startup_nodes = [
                ClusterNode(host, port)
                for host, port in additional_startup_nodes_info
                if get_node_name(
                    _resolve_localhost() if host == "localhost" else host, port
                )
                not in startup_node_names
            ]
            if last_failed_node_name is not None:
                for index, node in enumerate(additional_startup_nodes):
                    if node.name == last_failed_node_name:
                        if not deferred_failed_nodes:
                            deferred_failed_nodes.append(node)
                        additional_startup_nodes.pop(index)
                        break
            if is_debug_log_enabled():
                logger.debug(
                    f"Topology refresh: using additional nodes: {[node.name for node in additional_startup_nodes]}; "
                    f"and startup nodes: {[node.name for node in startup_nodes]}"
                )

            for startup_node in chain(
                startup_nodes,
                additional_startup_nodes,
                deferred_failed_nodes,
            ):
                try:
                    if startup_node.redis_connection:
                        r = startup_node.redis_connection

                    else:
                        # Create a new Redis connection
                        if is_debug_log_enabled():
                            socket_timeout = kwargs.get("socket_timeout", "not set")
                            socket_connect_timeout = kwargs.get(
                                "socket_connect_timeout", "not set"
                            )
                            maint_enabled = (
                                self.maint_notifications_config.enabled
                                if self.maint_notifications_config
                                else False
                            )
                            logger.debug(
                                "Topology refresh: Creating new Redis connection to "
                                f"{startup_node.host}:{startup_node.port}; "
                                f"with socket_timeout: {socket_timeout}, and "
                                f"socket_connect_timeout: {socket_connect_timeout}, "
                                "and maint_notifications enabled: "
                                f"{maint_enabled}"
                            )
                        r = self.create_redis_node(
                            startup_node.host,
                            startup_node.port,
                            maint_notifications_config=self.maint_notifications_config,
                            **kwargs,
                        )
                        if startup_node.name in self.startup_nodes:
                            self.startup_nodes[startup_node.name].redis_connection = r
                        else:
                            startup_node.redis_connection = r
                    # Collected before the query, so a client is released also
                    # when its CLUSTER SLOTS raises
                    if startup_node.name in self.startup_nodes:
                        queried_startup_nodes.append((startup_node.name, r))
                    else:
                        queried_additional_clients.append(r)
                    try:
                        if is_debug_log_enabled():
                            logger.debug(
                                "Topology refresh: querying CLUSTER SLOTS on "
                                f"{startup_node.name}"
                            )
                        # Make sure cluster mode is enabled on this node
                        cluster_slots = str_if_bytes(r.execute_command("CLUSTER SLOTS"))
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
                    if is_debug_log_enabled():
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
                    primary_node = slot[2]
                    host = str_if_bytes(primary_node[0])
                    # A node that does not know its own address sends an empty
                    # host, and one whose endpoint is unknown
                    # (cluster-preferred-endpoint-type unknown-endpoint) a null
                    # one: both are reached on the host that answered
                    if not host:
                        host = startup_node.host
                    port = int(primary_node[1])
                    host, port = self.remap_host_port(host, port)

                    nodes_for_slot = []

                    target_node = self._get_or_create_cluster_node(
                        host, port, PRIMARY, tmp_nodes_cache
                    )
                    nodes_for_slot.append(target_node)

                    replica_nodes = slot[3:]
                    for replica_node in replica_nodes:
                        host = str_if_bytes(replica_node[0])
                        if not host:
                            host = startup_node.host
                        port = int(replica_node[1])
                        host, port = self.remap_host_port(host, port)
                        target_replica_node = self._get_or_create_cluster_node(
                            host, port, REPLICA, tmp_nodes_cache
                        )
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

                fully_covered = self.check_slots_coverage(tmp_slots)
                if is_debug_log_enabled():
                    logger.debug(
                        f"Topology refresh: CLUSTER SLOTS from {startup_node.name} "
                        f"reported nodes {sorted(tmp_nodes_cache)}; "
                        f"slots fully covered: {fully_covered}"
                    )
                if fully_covered:
                    # Don't need to continue to the next startup node if all
                    # slots are covered
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
            if not fully_covered and self._require_full_coverage:
                # Despite the requirement that the slots be covered, there
                # isn't a full coverage
                raise RedisClusterException(
                    f"All slots are not covered after query all startup_nodes. "
                    f"{len(tmp_slots)} of {REDIS_CLUSTER_HASH_SLOTS} "
                    f"covered..."
                )

            # Create Redis connections to all new nodes. They are registered for
            # re-authentication once they replace the current nodes below.
            self._create_redis_connections(tmp_nodes_cache.values())

            # Set the tmp variables to the real variables
            with self._lock:
                old_nodes_cache = self.nodes_cache
                self.nodes_cache = tmp_nodes_cache
                self.slots_cache = tmp_slots
                # Set the default node
                self.default_node = self.get_nodes_by_server_type(PRIMARY)[0]
                if self._dynamic_startup_nodes:
                    # Populate the startup nodes with all discovered nodes
                    self.startup_nodes = tmp_nodes_cache
                self._last_refresh_at = time.monotonic()
                self._failed_refreshes = 0
                self._refresh_due_at = None
                self._last_refresh_unreachable_args = None
                # Increment the epoch to signal that initialization has completed
                self._epoch += 1
                # The published cache can change under other threads from now
                # on (move_slot, move_node_to_end_of_cached_nodes)
                new_nodes = tuple(tmp_nodes_cache.values())
                new_node_names = frozenset(tmp_nodes_cache)

            # Register the new pools for re-authentication before releasing the
            # removed ones, so a token refresh never targets a released pool.
            # Connection pool locks are taken outside self._lock.
            self._dispatch_pools_instantiated(new_nodes)
            self._release_removed_nodes(old_nodes_cache, new_node_names)
            # Dispatch so listeners (e.g. ClusterPubSub) can reconcile per-node
            # state after slot ownership may have changed. A listener must not
            # break slots-cache refresh; log and continue so a single buggy
            # listener cannot starve the rest.
            try:
                self._event_dispatcher.dispatch(AfterSlotsCacheRefreshEvent())
            except Exception as e:
                logger.exception(
                    "listener raised during slots-cache refresh: %s: %s",
                    type(e).__name__,
                    e,
                )

    def close(self) -> None:
        with self._lock:
            self.default_node = None
            nodes = tuple(self.nodes_cache.values())
        for node in nodes:
            if node.redis_connection:
                node.redis_connection.close()

    def reset(self):
        try:
            self.read_load_balancer.reset()
        except TypeError:
            # The read_load_balancer is None, do nothing
            pass

    def remap_host_port(self, host: str, port: int) -> Tuple[str, int]:
        """
        Remap the host and port returned from the cluster to a different
        internal value.  Useful if the client is not connecting directly
        to the cluster.
        """
        if self.address_remap:
            return self.address_remap((host, port))
        return host, port

    def find_connection_owner(self, connection: Connection) -> Optional[ClusterNode]:
        node_name = get_node_name(connection.host, connection.port)
        with self._lock:
            for node in tuple(self.nodes_cache.values()):
                if node.redis_connection:
                    conn_args = node.redis_connection.connection_pool.connection_kwargs
                    if node_name == get_node_name(
                        conn_args.get("host"), conn_args.get("port")
                    ):
                        return node
        return None


def _unregister_slots_cache_listener(
    dispatcher_ref: "weakref.ref[EventDispatcher]",
    listener: EventListenerInterface,
    event_type: Type[object],
) -> None:
    # Module-level finalizer callback. Kept free of strong references to the
    # owning ClusterPubSub so attaching it via weakref.finalize does not
    # extend the pubsub's lifetime.
    dispatcher = dispatcher_ref()
    if dispatcher is not None:
        dispatcher.unregister_listeners({event_type: [listener]})


class ClusterPubSubSlotsCacheListener(EventListenerInterface):
    """
    Listener that forwards AfterSlotsCacheRefreshEvent to a ClusterPubSub.

    Holds a weak reference to the pubsub so it does not keep the instance
    alive. Deterministic cleanup of the dispatcher's strong reference to this
    listener is performed by a ``weakref.finalize`` attached to the owning
    ClusterPubSub in ``ClusterPubSub.__init__``.
    """

    def __init__(self, pubsub: "ClusterPubSub") -> None:
        self._pubsub_ref: "weakref.ref[ClusterPubSub]" = weakref.ref(pubsub)

    def listen(self, event: object) -> None:
        pubsub = self._pubsub_ref()
        if pubsub is None:
            # Race window between pubsub GC and the finalizer running; safe
            # no-op, finalizer will remove this listener shortly.
            return
        try:
            pubsub.on_slots_changed()
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

# How long an unbounded sharded-pubsub poll waits for a not-yet-subscribed
# per-node pubsub before re-checking that the pubsub is still in
# node_pubsub_mapping. A retired one is never resubscribed and its
# subscribed_event is never set again, so an uninterrupted wait would park the
# single reader for good and stop delivery from every healthy sibling too.
# Only a poll that would otherwise block indefinitely ticks at all.
SHARD_SUBSCRIBE_WAIT_TICK_SECONDS = 1.0

# How often a failed sharded-pubsub poll may trigger a slots-cache refresh.
# Reconciliation is otherwise purely event-driven, and a node that has left the
# deployment answers ECONNREFUSED rather than MOVED - so without this the reader
# would cool off against the departed node forever and the shard channels pinned
# to it would never move to their new owner. Throttled because the refresh costs
# a CLUSTER SLOTS round trip and a failing node fails every poll.
SHARD_TOPOLOGY_REPAIR_INTERVAL_SECONDS = 5.0


class ClusterPubSub(PubSub):
    """
    Wrapper for PubSub class.

    IMPORTANT: before using ClusterPubSub, read about the known limitations
    with pubsub in Cluster mode and learn how to workaround them:
    https://redis.readthedocs.io/en/stable/clustering.html#known-pubsub-limitations
    """

    def __init__(
        self,
        redis_cluster,
        node=None,
        host=None,
        port=None,
        push_handler_func=None,
        event_dispatcher: Optional["EventDispatcher"] = None,
        **kwargs,
    ):
        """
        When a pubsub instance is created without specifying a node, a single
        node will be transparently chosen for the pubsub connection on the
        first command execution. The node will be determined by:
         1. Hashing the channel name in the request to find its keyslot
         2. Selecting a node that handles the keyslot: If read_from_replicas is
            set to true or load_balancing_strategy is set, a replica can be selected.

        :type redis_cluster: RedisCluster
        :type node: ClusterNode
        :type host: str
        :type port: int
        """
        self.node = None
        self.set_pubsub_node(redis_cluster, node, host, port)
        connection_pool = (
            None
            if self.node is None
            else redis_cluster.get_redis_connection(self.node).connection_pool
        )
        self.cluster = redis_cluster
        self.node_pubsub_mapping = {}
        # Reverse index: shard channel (normalized) -> owning node.name. Used to
        # route sunsubscribe calls and reconcile subscriptions after slot
        # migration / failover.
        self._shard_channel_to_node: dict = {}
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
        # that reconciliation cannot starve those unrelated paths during
        # long per-channel migrations.
        self._shard_state_lock: threading.RLock = threading.RLock()
        # Worker executor for off-loading slot-migration reconciliation from
        # the dispatch call site (mirrors async's asyncio.create_task model so
        # the thread that triggered MovedError / topology refresh is not
        # blocked on per-channel sunsubscribe / ssubscribe network I/O).
        #
        # Installed by reset(), which PubSub.__init__ calls below, and replaced
        # by every later reset() - so it is never None once construction has
        # finished and the scheduling sites need no lock to create it. That
        # matters: on_slots_changed runs inline on whichever thread refreshed
        # the topology, which can be a thread that handled an SMIGRATED push and
        # still holds OSSMaintNotificationsHandler._lock while a third thread
        # waits for that handler lock holding a per-node pubsub I/O lock the
        # reconciliation worker needs - so blocking there on _shard_state_lock
        # closes a three-thread deadlock cycle.
        #
        # Constructing an executor starts no thread; ThreadPoolExecutor spawns
        # its worker on the first submit(), so pubsubs that never see a slot
        # migration never pay for one. Declared before super().__init__()
        # because reset() reads it.
        self._reconcile_executor: Optional[ThreadPoolExecutor] = None
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
            {AfterSlotsCacheRefreshEvent: [self._slots_cache_listener]}
        )
        # Deterministic GC-time cleanup so short-lived pubsubs do not leak
        # listeners in the dispatcher when no slots-refresh event ever fires.
        weakref.finalize(
            self,
            _unregister_slots_cache_listener,
            weakref.ref(nm_dispatcher),
            self._slots_cache_listener,
            AfterSlotsCacheRefreshEvent,
        )

    def set_pubsub_node(self, cluster, node=None, host=None, port=None):
        """
        The pubsub node will be set according to the passed node, host and port
        When none of the node, host, or port are specified - the node is set
        to None and will be determined by the keyslot of the channel in the
        first command to be executed.
        RedisClusterException will be thrown if the passed node does not exist
        in the cluster.
        If host is passed without port, or vice versa, a DataError will be
        thrown.
        :type cluster: RedisCluster
        :type node: ClusterNode
        :type host: str
        :type port: int
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
        elif any([host, port]) is True:
            # only 'host' or 'port' passed
            raise DataError("Passing a host requires passing a port, and vice versa")
        else:
            # nothing passed by the user. set node to None
            pubsub_node = None

        self.node = pubsub_node

    def get_pubsub_node(self):
        """
        Get the node that is being used as the pubsub connection
        """
        return self.node

    def _raise_on_invalid_node(self, redis_cluster, node, host, port):
        """
        Raise a RedisClusterException if the node is None or doesn't exist in
        the cluster.
        """
        if node is None or redis_cluster.get_node(node_name=node.name) is None:
            raise RedisClusterException(
                f"Node {host}:{port} doesn't exist in the cluster"
            )

    def execute_command(self, *args):
        """
        Execute a subscribe/unsubscribe command.

        Taken code from redis-py and tweak to make it work within a cluster.
        """
        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

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
                    return self.ssubscribe(*args[1:])
                if command == "SUNSUBSCRIBE":
                    return self.sunsubscribe(*args[1:])
                channel = args[1]
                node = self.cluster.get_node_from_key(channel)
                if node:
                    pubsub = self._get_node_pubsub(node)
                    with self._pubsub_io_lock(pubsub):
                        return pubsub.execute_command(*args)

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
                redis_connection = self.cluster.get_redis_connection(node)
                self.connection_pool = redis_connection.connection_pool
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
        self._execute(connection, connection.send_command, *args)

    def _resubscribe_shard_channels(self) -> None:
        # A single node can own multiple slot ranges, so a batched
        # ``SSUBSCRIBE`` covering every tracked channel would be rejected by
        # Redis with a ``CROSSSLOT`` error. Group by hash slot and emit one
        # ``SSUBSCRIBE`` per slot.
        by_slot: defaultdict[int, dict] = defaultdict(dict)
        for k, v in self.shard_channels.items():
            by_slot[key_slot(self.encoder.encode(k))][k] = v
        for subscriptions in by_slot.values():
            self._resubscribe(subscriptions, self.ssubscribe)

    def _get_node_pubsub(self, node):
        try:
            return self.node_pubsub_mapping[node.name]
        except KeyError:
            redis_connection = self.cluster.get_redis_connection(node)
            pubsub = redis_connection.pubsub(
                push_handler_func=self.push_handler_func,
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

    def _find_node_name_for_pubsub(self, pubsub):
        # Snapshot the items: every caller but one runs without
        # _shard_state_lock, so iterating the mapping directly would raise
        # "dictionary changed size during iteration" whenever a concurrent
        # migration adds or retires a per-node pubsub - which is exactly when
        # these lookups happen. list() of a dict view is atomic under the GIL.
        for node_name, node_pubsub in list(self.node_pubsub_mapping.items()):
            if node_pubsub is pubsub:
                return node_name
        return None

    @staticmethod
    def _pubsub_io_lock(pubsub) -> threading.RLock:
        """Return the per-node pubsub's wire I/O lock, creating it on first use.

        A per-node ``PubSub`` is read by whichever thread polls
        ``get_sharded_message`` and written by the reconciliation worker
        (``_migrate_shard_channel``) and by any caller of ``ssubscribe`` /
        ``sunsubscribe``. ``PubSub`` guards writes with its own ``_lock``
        (``PubSub.execute_command``) but reads take no lock at all, so without
        this the reader can be inside ``read_response`` while another thread's
        ``_execute`` disconnects and reconnects the same socket underneath it -
        which loses the reply to the handshake and surfaces as a read timeout
        followed by ``EBADF``.

        Kept on the pubsub rather than in a dict keyed by node name so it
        travels with the object through ``node_pubsub_mapping`` and cannot go
        stale when a per-node pubsub is dropped and recreated.
        """
        lock = getattr(pubsub, "_shard_io_lock", None)
        if lock is None:
            lock = threading.RLock()
            pubsub._shard_io_lock = lock
        return lock

    @staticmethod
    def _detach_shard_channel(pubsub, channel) -> None:
        """Forget a shard channel on a per-node pubsub without a wire round trip.

        ``PubSub.sunsubscribe`` only records the intent in
        ``pending_unsubscribe_shard_channels``; the channel leaves
        ``shard_channels`` when the server confirmation is read. So if the
        ``SUNSUBSCRIBE`` never reaches the server, ``on_connect`` clears the
        pending set and replays ``SSUBSCRIBE`` for the channel - on the node it
        is being migrated away from, on every reconnect. Once the caller has
        decided the channel belongs to a different node, the local intent is
        the only truth left, so drop it here.

        Deliberately without the per-node I/O lock, unlike every other writer
        on a per-node pubsub. ``_forget_shard_channel_on_old_node`` calls this
        for a node the reader has just failed to reach, and a bounded poll holds
        that lock across ``PubSub._execute``'s reconnect and its whole retry
        budget - far longer than the timeout the poll was given. Waiting for it
        here, while this pass holds ``_shard_state_lock``, is the migration
        slow enough to look like a permanent delivery stall that the
        ``_unreachable_nodes`` fast path exists to avoid.

        Nothing needs the lock: each mutation below is individually atomic, and
        every interleaving with ``handle_message``'s unsubscribe bookkeeping (the
        only concurrent writer of the same state, and one that runs under that
        lock) converges on the same end state - the channel gone from both the
        subscription dict and the pending set. That bookkeeping discards rather
        than removes precisely so this detach cannot make it raise ``KeyError``
        into a poll no caller catches.
        """
        pubsub.shard_channels.pop(channel, None)
        pubsub.pending_unsubscribe_shard_channels.discard(channel)
        if not pubsub.channels and not pubsub.patterns and not pubsub.shard_channels:
            pubsub.subscribed_event.clear()

    def _drop_node_pubsub(self, name: str, pubsub) -> None:
        """Retire a per-node pubsub and drop it from ``node_pubsub_mapping``.

        Callers hold ``_shard_state_lock``, the lock that every mutation of
        that mapping observes. ``reset()`` runs under the per-node I/O lock so
        the socket is not torn down beneath a concurrent bounded poll parked in
        ``parse_response``, and its errors are swallowed: retiring one node's
        pubsub must not abort the caller's pass, and this also runs from the
        ``__del__`` fallback path through ``reset()``.

        Every caller must leave nothing subscribed on ``pubsub`` (or have lost
        the node itself). An empty per-node pubsub left in the mapping is what
        ``_poll_node_pubsub`` stalls on: its ``subscribed_event`` is cleared, so
        the prelude waits out the whole timeout of every pass - indefinitely when
        the caller passed ``timeout=None``, since the ``reset()`` below only
        clears that event again and no message can arrive on a subscription that
        is gone.

        Popping it from the mapping is also what makes the retirement
        *observable*: that prelude gives up on an unbounded wait once the pubsub
        it was handed is no longer mapped, which is the only thing standing
        between a reader that held the object across this call and a permanent
        park. The rebind below keeps it from being handed out to begin with.
        """
        try:
            with self._pubsub_io_lock(pubsub):
                pubsub.reset()
        except Exception:
            pass
        self.node_pubsub_mapping.pop(name, None)
        self._unreachable_nodes.discard(name)
        # Same snapshot reason ``reset()`` recreates this: ``_pubsubs_generator``
        # captures node_pubsub_mapping.values() into a local list inside
        # ``yield from``, which the pop above does not reach - so a generator
        # suspended mid-yield-from would still hand the object we just retired
        # to the next poll. ``type(self)`` bypasses the instance-level
        # self-shadow established at __init__. Costs nothing: constructing a
        # generator runs no frame, so the per-node collection loop in
        # reinitialize_shard_subscriptions can rebind once per dropped node.
        self._pubsubs_generator = type(self)._pubsubs_generator(self)

    def _sharded_message_generator(self, timeout=0.0):
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
                message = self._poll_node_pubsub(pubsub, timeout)
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
                self._handle_moved_on_read(pubsub, e)
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
                # Deliberately outside _shard_state_lock: this is an advisory
                # hint for _migrate_shard_channel's fast path, mutated with
                # single atomic set operations, and both misread directions are
                # handled there and self-heal - a stale entry only skips a
                # SUNSUBSCRIBE to a dead node, a missing one only pays a
                # reconnect before the same local forget.
                node_name = self._find_node_name_for_pubsub(pubsub)
                if node_name is not None:
                    self._unreachable_nodes.add(node_name)
                if is_debug_log_enabled():
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
            self._wait_out_cool_off(next_ready, timeout)
        return None, None

    @staticmethod
    def _wait_out_cool_off(next_ready: float, timeout) -> None:
        """Wait out a pass in which every node was skipped for cool-off.

        Such a pass does no wire read at all, so returning straight away
        ignores the timeout the caller asked to block for - and a reader loop
        (``PubSubWorkerThread.run`` with ``sharded_pubsub=True``, or a user
        loop on ``get_sharded_message``) polls back immediately, spinning
        until the cool-off expires instead of blocking. Sleep instead: until
        the earliest cool-off is over, never longer than the caller's timeout,
        and not at all for a non-blocking poll.
        """
        if timeout is not None and timeout <= 0:
            return
        delay = next_ready - time.monotonic()
        if delay <= 0:
            return
        if timeout is not None:
            delay = min(delay, timeout)
        time.sleep(delay)

    def _poll_io_lock(self, pubsub, timeout):
        """Guard a per-node poll against concurrent writers on the same socket.

        ``timeout=None`` makes ``_poll_node_pubsub``'s read block indefinitely,
        so holding the lock across it would block reconciliation for as long as
        no message arrives. Such a caller drives the pubsub itself and gets the
        pre-existing unguarded behavior; every bounded poll - which is what
        ``PubSubWorkerThread`` and ``ClusterPubSub``'s own callers use - is
        serialized.
        """
        if timeout is None:
            return nullcontext()
        return self._pubsub_io_lock(pubsub)

    def _poll_node_pubsub(self, pubsub, timeout):
        """Read one message from a per-node pubsub, dispatching outside the lock.

        Splits ``PubSub.get_message`` so the per-node I/O lock covers the wire
        read only. ``handle_message`` invokes a subscribed channel's user
        handler inline, and a handler is free to call ``ssubscribe`` /
        ``sunsubscribe`` on this ``ClusterPubSub`` - which takes
        ``_shard_state_lock`` and then the same I/O lock. Holding the I/O lock
        across the handler therefore deadlocks against the reconciliation
        worker, which holds ``_shard_state_lock`` and waits for that I/O lock:
        an ABBA cycle between two threads that the ``RLock``'s self-reentrancy
        cannot break. The async counterpart's non-reentrant ``asyncio.Lock``
        hangs on the re-acquire alone, before any reconciliation is involved.

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
        """
        # ``PubSub.get_message``'s prelude, which bypassing it would drop: a
        # per-node pubsub enters node_pubsub_mapping before its first
        # SSUBSCRIBE (_get_node_pubsub) and is left connectionless by reset()
        # (the GC in reinitialize_shard_subscriptions), while parse_response
        # raises RuntimeError on a None connection - which neither poll site
        # catches. Deliberately kept outside the I/O lock, unlike the
        # get_message call it replaces: waiting here is not wire I/O, and the
        # ssubscribe that sets this event needs the I/O lock itself, so waiting
        # under it stalls the very subscribe being waited for.
        if not pubsub.subscribed:
            start_time = time.monotonic()
            # An unbounded caller must not wait on this event uninterrupted.
            # _drop_node_pubsub retires a per-node pubsub whose event is cleared
            # and will never be set again - it is never resubscribed - and the
            # reader can be holding that object across the drop: the round robin
            # yields from a _pubsubs_generator snapshot the drop's pop does not
            # reach, and get_sharded_message's target_node lookup takes no lock.
            # So wake up periodically and re-check the mapping rather than park
            # the single reader for good, which would withhold the messages of
            # every healthy sibling in the pass too.
            if timeout is None:
                wait_for = SHARD_SUBSCRIBE_WAIT_TICK_SECONDS
            else:
                wait_for = timeout
            while not pubsub.subscribed_event.wait(wait_for):
                if timeout is not None:
                    # The connection isn't subscribed to any channels or
                    # patterns, so no messages are available
                    return None
                if self._find_node_name_for_pubsub(pubsub) is None:
                    # Retired by a concurrent drop while we held it. Nothing can
                    # arrive on a subscription that is gone; let the pass move on
                    # to the nodes that can still deliver.
                    return None
            # The connection was subscribed during the timeout time frame.
            # The timeout should be adjusted based on the time spent
            # waiting for the subscription. Only the bounded case needs it: an
            # unbounded wait leaves ``timeout`` at None for the read below.
            if timeout is not None:
                timeout = max(0.0, timeout - (time.monotonic() - start_time))
        with self._poll_io_lock(pubsub, timeout):
            # Re-check now that no writer can be mid-flight: the prelude above
            # can pass and the GC then close this pubsub while holding this very
            # lock, which would leave parse_response with the None connection it
            # raises RuntimeError on. Only the connection is re-checked, not
            # ``subscribed``: a pubsub whose last channel was detached locally
            # may still have an in-flight SUNSUBSCRIBE confirmation to read.
            if pubsub.connection is None:
                return None
            response = pubsub.parse_response(block=(timeout is None), timeout=timeout)
            # get_message's truthiness test, not "is None": a health check
            # reply filtered out by parse_response, or an empty bulk, is "no
            # message" rather than a message to parse.
            if not response:
                return None
            if not self._is_publish_response(response):
                # Don't pass ignore_subscribe_messages here - let
                # get_sharded_message handle the filtering after processing
                # subscription state changes
                return pubsub.handle_message(response, ignore_subscribe_messages=False)
        return pubsub.handle_message(response, ignore_subscribe_messages=False)

    @staticmethod
    def _is_publish_response(response) -> bool:
        """Whether a raw pubsub reply can make ``handle_message`` dispatch.

        ``handle_message`` invokes a user handler only for
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

        The repair asks the nodes manager for a refresh and runs it on the
        reconciliation worker, once due, so a bounded poll does not pay for a
        ``CLUSTER SLOTS`` round trip; it is throttled because a node that is
        down fails every poll. The refresh drops nodes that have left the
        topology and dispatches ``AfterSlotsCacheRefreshEvent``, which reaches
        ``on_slots_changed``.

        ``disconnect_startup_nodes_pools=False``, unlike every other caller: a
        fresh slot map is all this repair asks for, so it also keeps the idle
        connections of a startup node pool that serves no node of the topology.
        Pools of the nodes that stay in the topology, which per-node pubsubs
        draw their connections from, are not recycled by a refresh either way.
        The async counterpart has no such parameter, so this is also the shape
        parity asks for.
        """
        if not self.shard_channels:
            return
        now = time.monotonic()
        if now < self._next_topology_repair:
            return
        # Unsynchronized on purpose: the read path must not queue behind a
        # reconciliation pass doing per-channel network I/O, because the
        # cluster-wide delivery stall that would cause is the very thing this
        # repair exists to end. Two readers racing the throttle window can both
        # submit, but their requests merge into one: the single-worker executor
        # runs them sequentially, the first run claims the request, and the
        # second finds nothing left to run (or a refresh that already completed
        # after its request) and returns without a CLUSTER SLOTS round trip.
        self._next_topology_repair = now + SHARD_TOPOLOGY_REPAIR_INTERVAL_SECONDS
        # The refresh is subject to _TOPOLOGY_REFRESH_MIN_INTERVAL_SECONDS like
        # every other error-triggered one: if it is not due yet, the request
        # stays and the next command, or the next repair, runs it.
        nodes_manager = self.cluster.nodes_manager
        nodes_manager._request_refresh(disconnect_startup_nodes_pools=False)
        self._submit_reconcile_work(
            partial(nodes_manager._run_requested_refresh, self.cluster.retry)
        )

    def _handle_moved_on_read(self, pubsub, error: MovedError) -> None:
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
        with self._shard_state_lock:
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
            # GCs it once the worker gets to run - a whole poll cool-off later,
            # at best, while an unbounded poll that reaches the empty pubsub
            # first parks on it for good (see _drop_node_pubsub).
            if node_name is not None and not pubsub.subscribed:
                self._drop_node_pubsub(node_name, pubsub)
        # move_slot applies the redirect to the slots cache and dispatches
        # AfterSlotsCacheRefreshEvent, which reaches on_slots_changed. Call
        # on_slots_changed unconditionally too: move_slot skips the dispatch on
        # a circular MOVED, and a duplicate reconciliation pass is a no-op.
        # move_slot indexes slots_cache by the redirected slot, so an
        # as-yet-uncovered slot raises: log and still reconcile rather than let
        # a repair attempt break a pubsub read.
        if node_name is not None:
            # A redirect without a host is relative to the node that sent it,
            # and a node's name is its host and port
            _fill_missing_redirect_host(error, node_name.rsplit(":", 1)[0])
        try:
            self.cluster.nodes_manager.move_slot(error)
        except Exception as exc:
            logger.debug(
                "sharded pubsub: could not apply the redirect for slot %s: %s: %s",
                error.slot_id,
                type(exc).__name__,
                exc,
            )
        self.on_slots_changed()

    def _pubsubs_generator(self):
        # Never return: a generator that returns is exhausted for good and only
        # reset() recreates this one, so a momentarily empty
        # node_pubsub_mapping - reconciliation drops a per-node pubsub before
        # creating its replacement - would stop the round robin permanently.
        # Yield None for an empty mapping instead, which lets the caller skip
        # the slot without this loop spinning on an empty list.
        while True:
            current_nodes = list(self.node_pubsub_mapping.values())
            if not current_nodes:
                yield None
            else:
                yield from current_nodes

    def get_sharded_message(
        self, ignore_subscribe_messages=False, timeout=0.0, target_node=None
    ):
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
        if target_node:
            # Use .get(): migration-driven cleanup in the sunsubscribe branch
            # below and reset() both remove entries from node_pubsub_mapping,
            # so a caller polling with target_node may race the cleanup. Match
            # the async counterpart's None-handling rather than raising
            # KeyError. None pubsub falls through to "no message available".
            pubsub = self.node_pubsub_mapping.get(target_node.name)
            if pubsub is not None:
                try:
                    message = self._poll_node_pubsub(pubsub, timeout)
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
                    self._handle_moved_on_read(pubsub, e)
                    message = None
            else:
                message = None
        else:
            pubsub, message = self._sharded_message_generator(timeout=timeout)
        if message is None:
            return None
        # Only sunsubscribe mutates cluster-level shard state; bypassing the
        # lock on the data-message hot path keeps smessage delivery from
        # competing with the reconciliation worker for _shard_state_lock.
        if str_if_bytes(message["type"]) == "sunsubscribe":
            # Serialize state mutation against reinitialize_shard_subscriptions
            # (worker thread). The blocking _poll_node_pubsub above
            # intentionally runs outside the lock so reconciliation is not
            # stalled by long polls.
            with self._shard_state_lock:
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
                        self._drop_node_pubsub(name, pubsub)
                # Mirror PubSub.handle_message: the empty-check belongs in the
                # unsubscribe branch since that is the only path that can
                # reduce shard_channels here.
                if not self.channels and not self.patterns and not self.shard_channels:
                    self.subscribed_event.clear()
        # Only suppress subscribe/unsubscribe messages, not data messages (smessage)
        if str_if_bytes(message["type"]) in ("ssubscribe", "sunsubscribe"):
            if self.ignore_subscribe_messages or ignore_subscribe_messages:
                return None
        return message

    def ssubscribe(
        self, *args: ChannelT | Subscription, **kwargs: PubSubHandler
    ) -> None:
        """
        Subscribe to shard channels.

        Channels supplied as keyword arguments expect a channel name as the key
        and a callable as the value. ``Subscription`` objects can also be
        supplied positionally with an optional handler.
        """
        s_channels = parse_pubsub_subscriptions(args, kwargs)
        # Serialize against reinitialize_shard_subscriptions (worker thread)
        # so the reverse index, shard_channels, and node_pubsub_mapping are
        # not mutated concurrently.
        with self._shard_state_lock:
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
                    self._migrate_shard_channel(
                        normalized_key,
                        handler,
                        old_name,
                        node,
                    )
                    continue
                pubsub = self._get_node_pubsub(node)
                with self._pubsub_io_lock(pubsub):
                    if handler:
                        pubsub.ssubscribe(Subscription(s_channel, handler))
                    else:
                        pubsub.ssubscribe(s_channel)
                self.shard_channels.update(pubsub.shard_channels)
                self._shard_channel_to_node[normalized_key] = node.name
                self.pending_unsubscribe_shard_channels.difference_update(
                    self._normalize_keys({s_channel: None})
                )
                if pubsub.subscribed and not self.subscribed:
                    self.subscribed_event.set()
                    self.health_check_response_counter = 0

    def sunsubscribe(self, *args):
        if args:
            args = list_or_args(args[0], args[1:])
        else:
            args = list(self.shard_channels)

        # Serialize against reinitialize_shard_subscriptions: the reverse
        # index and node_pubsub_mapping must not change between the lookup
        # and the per-node sunsubscribe call below.
        with self._shard_state_lock:
            for s_channel in args:
                normalized_key = next(iter(self._normalize_keys({s_channel: None})))
                # Route via the reverse index so we unsubscribe on the node
                # that actually holds the subscription. After a slot migration
                # the cluster's current owner may no longer be that node.
                name = self._shard_channel_to_node.get(normalized_key)
                if name and name in self.node_pubsub_mapping:
                    p = self.node_pubsub_mapping[name]
                else:
                    node = self.cluster.get_node_from_key(s_channel)
                    if not node or node.name not in self.node_pubsub_mapping:
                        continue
                    p = self.node_pubsub_mapping[node.name]
                with self._pubsub_io_lock(p):
                    p.sunsubscribe(s_channel)
                self.pending_unsubscribe_shard_channels.update(
                    p.pending_unsubscribe_shard_channels
                )

    def reinitialize_shard_subscriptions(self):
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
        with self._shard_state_lock:
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
                    self._migrate_shard_channel(channel, handler, old_name, new_node)
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
                    self._drop_node_pubsub(name, pubsub)
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
            # symptoms of the same unreachable node) so the worker's done-
            # callback surfaces a single representative failure through the
            # same logger channel used for SlotNotCoveredError. Per-channel
            # WARNINGs above preserve the full forensic detail.
            raise first_migrate_error

    def _forget_shard_channel_on_old_node(self, old_pubsub, channel, old_name):
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
        # left in the mapping has had its ``subscribed_event`` cleared by the
        # detach, so ``_poll_node_pubsub`` waits on an event nothing will ever
        # set: forever when the caller passed ``timeout=None``, and for the whole
        # timeout of every pass otherwise, before a single healthy node is read.
        if (
            self.cluster.get_node(node_name=old_name) is None
            or not old_pubsub.subscribed
        ):
            self._drop_node_pubsub(old_name, old_pubsub)

    def _migrate_shard_channel(self, channel, handler, old_name, new_node):
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
                self._forget_shard_channel_on_old_node(old_pubsub, channel, old_name)
            else:
                try:
                    with self._pubsub_io_lock(old_pubsub):
                        old_pubsub.sunsubscribe(channel)
                except (ConnectionError, TimeoutError, OSError):
                    # redis-py's Connection has already called ``disconnect()``
                    # before raising (see Connection.read_response /
                    # send_packed_command with ``disconnect_on_error=True``), so
                    # ``old_pubsub``'s dedicated socket is gone and the
                    # ``SUNSUBSCRIBE`` never reached the server.
                    self._forget_shard_channel_on_old_node(
                        old_pubsub, channel, old_name
                    )
        # Attach to the new per-node pubsub, preserving the handler. Decode to
        # a text key only when we must pass it as a kwarg (handler present).
        new_pubsub = self._get_node_pubsub(new_node)
        with self._pubsub_io_lock(new_pubsub):
            if handler:
                new_pubsub.ssubscribe(Subscription(channel, handler))
            else:
                new_pubsub.ssubscribe(channel)
        self.shard_channels.update(new_pubsub.shard_channels)
        normalized_key = next(iter(self._normalize_keys({channel: None})))
        self._shard_channel_to_node[normalized_key] = new_node.name
        self.pending_unsubscribe_shard_channels.difference_update(
            self._normalize_keys({channel: None})
        )
        if new_pubsub.subscribed and not self.subscribed:
            self.subscribed_event.set()
            self.health_check_response_counter = 0

    def on_slots_changed(self):
        # Observer hook invoked by NodesManager after a slots-cache refresh.
        # Schedule reconciliation on a dedicated worker thread so the caller
        # (typically MovedError handling in _execute_command or the topology
        # refresh thread in initialize()) is not blocked on the network I/O
        # performed by reinitialize_shard_subscriptions. Mirrors the async
        # path's asyncio.create_task model. No-op when there are no shard
        # subscriptions to reconcile.
        if not self.shard_channels:
            return
        # Takes no lock. This hook runs inline on whichever thread refreshed the
        # topology - including a command or pubsub thread that handled an
        # SMIGRATED push and still holds OSSMaintNotificationsHandler._lock.
        # Waiting here for a reconciliation pass that is itself waiting for a
        # per-node pubsub I/O lock held by a third thread blocked on that
        # handler lock is a deadlock cycle. The async counterpart takes no lock
        # here either (the event loop serializes it), so this is also the
        # shape sync/async parity asks for.
        self._submit_reconcile_work(self.reinitialize_shard_subscriptions)

    def _submit_reconcile_work(self, work: Callable[[], Any]) -> Optional["Future"]:
        """Run ``work`` on the reconciliation worker, without taking a lock.

        The executor is installed by ``reset()`` and never None afterwards, so
        there is nothing to create here and no creation race to serialize - see
        the ``_reconcile_executor`` comment in ``__init__`` for why the callers
        must not block. Only ``reset()`` swaps it, and a submit that loses that
        race raises ``RuntimeError``: read the attribute once so the swap cannot
        be observed half-done, and treat the rejection as "nothing to do", which
        it is - ``reset()`` has already dropped the shard channels this pass
        would have reconciled.
        """
        executor = self._reconcile_executor
        if executor is None:
            # Defensive: reset() runs from PubSub.__init__, so by the time any
            # caller can get here an executor is installed.
            return None
        try:
            future = executor.submit(work)
        except RuntimeError:
            return None
        # Consume the future's exception (if any) so it is not silently lost.
        # reinitialize_shard_subscriptions surfaces SlotNotCoveredError when a
        # slot is still transiently uncovered; route it through the same logger
        # channel as the async path for consistent observability.
        future.add_done_callback(self._log_reconcile_future_exception)
        return future

    @staticmethod
    def _log_reconcile_future_exception(future: "Future") -> None:
        if future.cancelled():
            return
        exc = future.exception()
        if exc is not None:
            logger.error(
                "shard subscription reconciliation failed: %r", exc, exc_info=exc
            )

    def reset(self) -> None:
        # Hold _shard_state_lock across the entire teardown so it observes
        # the same mutual-exclusion discipline as ssubscribe / sunsubscribe /
        # get_sharded_message / reinitialize_shard_subscriptions, which all
        # mutate shard_channels, _shard_channel_to_node, and
        # node_pubsub_mapping under this lock. Without it, super().reset()
        # rebinds shard_channels and pending_unsubscribe_shard_channels in
        # parallel with a concurrent user-thread mutation, silently dropping
        # subscription intent. The reconciliation executor is swapped out at the
        # end of that critical section; see the comment there for why it goes
        # last.
        with self._shard_state_lock:
            # Tear down per-node pubsubs (parity with async aclose) so they
            # don't leak their dedicated connections and don't replay stale
            # shard_channels via PubSub.on_connect on a subsequent reconnect.
            # Errors are swallowed because reset() is also a fallback path
            # from __del__; we cannot let one buggy per-node pubsub mask the
            # rest of the teardown.
            # The per-node I/O lock keeps the socket from being torn down
            # beneath a concurrent bounded poll parked in parse_response. It is
            # an RLock, so re-entry from this thread is fine; contention is with
            # another thread's bounded poll and is bounded by that poll's
            # timeout (an unbounded poll holds nullcontext() - see
            # _poll_io_lock). reset() is also the __del__ fallback path, where
            # the try below does not cover a blocking acquire - that boundedness
            # is what makes taking the lock here safe.
            for pubsub in self.node_pubsub_mapping.values():
                try:
                    with self._pubsub_io_lock(pubsub):
                        pubsub.reset()
                except Exception:
                    pass
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
            # the now-reset() per-node pubsubs after re-subscription.
            # Recreate it to drop the captured list. type(self) bypasses
            # the instance-level self-shadow established at __init__
            # (self._pubsubs_generator = self._pubsubs_generator()).
            self._pubsubs_generator = type(self)._pubsubs_generator(self)
            super().reset()
            self._shard_channel_to_node = {}
            # Swap in a fresh reconciliation executor and retire the old one.
            # Installing the replacement is what lets the scheduling sites stay
            # lock-free (see __init__); it costs nothing until something is
            # submitted, because a ThreadPoolExecutor spawns its worker on the
            # first submit(). Done last, after super().reset() has emptied
            # shard_channels, so a concurrent on_slots_changed either loses the
            # swap and is rejected or finds nothing left to reconcile.
            # cancel_futures drops queued reconciliation work; a pass already
            # running is serialized against us by _shard_state_lock, and
            # shutdown(wait=False) avoids waiting on the worker thread's join.
            retired = self._reconcile_executor
            self._reconcile_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="redis-cluster-pubsub-reconcile",
            )
            if retired is not None:
                retired.shutdown(wait=False, cancel_futures=True)

    def get_redis_connection(self):
        """
        Get the Redis connection of the pubsub connected node.
        """
        if self.node is not None:
            return self.node.redis_connection

    def disconnect(self):
        """
        Disconnect the pubsub connection.
        """
        if self.connection:
            self.connection.disconnect()
        for pubsub in self.node_pubsub_mapping.values():
            if pubsub.connection:
                pubsub.connection.disconnect()


class ClusterPipeline(RedisCluster):
    """
    Support for Redis pipeline
    in cluster mode
    """

    ERRORS_ALLOW_RETRY = (
        ConnectionError,
        TimeoutError,
        MovedError,
        AskError,
        TryAgainError,
    )

    NO_SLOTS_COMMANDS = {"UNWATCH"}
    IMMEDIATE_EXECUTE_COMMANDS = {"WATCH", "UNWATCH"}
    UNWATCH_COMMANDS = {"DISCARD", "EXEC", "UNWATCH"}

    @deprecated_args(
        args_to_warn=[
            "cluster_error_retry_attempts",
        ],
        reason="Please configure the 'retry' object instead",
        version="6.0.0",
    )
    def __init__(
        self,
        nodes_manager: "NodesManager",
        commands_parser: "CommandsParser",
        result_callbacks: Optional[Dict[str, Callable]] = None,
        cluster_response_callbacks: Optional[Dict[str, Callable]] = None,
        startup_nodes: Optional[List["ClusterNode"]] = None,
        read_from_replicas: bool = False,
        load_balancing_strategy: Optional[LoadBalancingStrategy] = None,
        cluster_error_retry_attempts: int = DEFAULT_RETRY_COUNT,
        reinitialize_steps: int = 5,
        retry: Optional[Retry] = None,
        lock=None,
        transaction=False,
        policy_resolver: Optional[PolicyResolver] = None,
        event_dispatcher: Optional["EventDispatcher"] = None,
        metadata_resolver: Optional[MetadataResolver] = None,
        **kwargs,
    ):
        """ """
        self.command_stack = []
        self.nodes_manager = nodes_manager
        # Share the parent cluster's HIMPORT registry (held on the NodesManager and
        # referenced by every node pool). The inherited himport_prepare/discard/
        # discard_all mutate this one object, so a fieldset declared on the pipeline is
        # visible to the batched himport_set pre-flight exactly as on the parent client.
        self._himport_registry = nodes_manager.himport_registry
        self.commands_parser = commands_parser
        self.refresh_table_asap = False
        self.result_callbacks = (
            result_callbacks or self.__class__.RESULT_CALLBACKS.copy()
        )
        self.startup_nodes = startup_nodes if startup_nodes else []
        self.read_from_replicas = read_from_replicas
        self.load_balancing_strategy = load_balancing_strategy
        self.command_flags = self.__class__.COMMAND_FLAGS.copy()
        self.node_flags = self.__class__.NODE_FLAGS.copy()
        self.cluster_response_callbacks = cluster_response_callbacks
        self.reinitialize_counter = 0
        self.reinitialize_steps = reinitialize_steps
        if retry is not None:
            self.retry = retry
        else:
            self.retry = Retry(
                backoff=ExponentialWithJitterBackoff(
                    base=DEFAULT_RETRY_BASE, cap=DEFAULT_RETRY_CAP
                ),
                retries=cluster_error_retry_attempts,
            )

        self.encoder = Encoder(
            kwargs.get("encoding", "utf-8"),
            kwargs.get("encoding_errors", "strict"),
            kwargs.get("decode_responses", False),
        )
        if lock is None:
            lock = threading.RLock()
        self._lock = lock
        self.parent_execute_command = super().execute_command
        self._execution_strategy: ExecutionStrategy = (
            PipelineStrategy(self) if not transaction else TransactionStrategy(self)
        )

        # For backward compatibility, mapping from existing policies to new one
        self._command_flags_mapping: dict[str, Union[RequestPolicy, ResponsePolicy]] = {
            self.__class__.RANDOM: RequestPolicy.DEFAULT_KEYLESS,
            self.__class__.PRIMARIES: RequestPolicy.ALL_SHARDS,
            self.__class__.ALL_NODES: RequestPolicy.ALL_NODES,
            self.__class__.REPLICAS: RequestPolicy.ALL_REPLICAS,
            self.__class__.DEFAULT_NODE: RequestPolicy.DEFAULT_NODE,
            SLOT_ID: RequestPolicy.DEFAULT_KEYED,
        }

        # ``RedisCluster.pipeline`` passes the client's own resolvers, so a pipeline routes by
        # whatever the client routes by. Only a pipeline built directly, without either, falls
        # back to the static default. Precedence between the two mirrors ``RedisCluster`` -
        # see the note there.
        if metadata_resolver is None:
            self._metadata_resolver: MetadataResolver = StaticMetadataResolver()
        else:
            self._metadata_resolver = metadata_resolver

        if policy_resolver is None:
            self._policy_resolver: PolicyResolver = StaticPolicyResolver(
                metadata_resolver=self._metadata_resolver
            )
        else:
            self._policy_resolver = policy_resolver

        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher

    def __repr__(self):
        """ """
        return f"{type(self).__name__}"

    def __enter__(self):
        """ """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ """
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self):
        """ """
        return len(self._execution_strategy.command_queue)

    def __bool__(self):
        "Pipeline instances should  always evaluate to True on Python 3+"
        return True

    def execute_command(self, *args, **kwargs):
        """
        Wrapper function for pipeline_execute_command
        """
        return self._execution_strategy.execute_command(*args, **kwargs)

    def pipeline_execute_command(self, *args, **options):
        """
        Stage a command to be executed when execute() is next called

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.
        """
        return self._execution_strategy.execute_command(*args, **options)

    def annotate_exception(self, exception, number, command):
        """
        Provides extra context to the exception prior to it being handled
        """
        self._execution_strategy.annotate_exception(exception, number, command)

    def execute(self, raise_on_error: bool = True) -> List[Any]:
        """
        Execute all the commands in the current pipeline
        """

        try:
            return self._execution_strategy.execute(raise_on_error)
        finally:
            self.reset()

    def reset(self):
        """
        Reset back to empty pipeline.
        """
        self._execution_strategy.reset()

    def send_cluster_commands(
        self, stack, raise_on_error=True, allow_redirections=True
    ):
        return self._execution_strategy.send_cluster_commands(
            stack, raise_on_error=raise_on_error, allow_redirections=allow_redirections
        )

    def exists(self, *keys):
        return self._execution_strategy.exists(*keys)

    def touch(self, *keys):
        return self._execution_strategy.touch(*keys)

    def eval(self):
        """ """
        return self._execution_strategy.eval()

    def multi(self):
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        self._execution_strategy.multi()

    def load_scripts(self):
        """ """
        self._execution_strategy.load_scripts()

    def discard(self):
        """Discards the transactional block."""
        self._execution_strategy.discard()

    def watch(self, *names):
        """Watches the values at keys ``names``"""
        self._execution_strategy.watch(*names)

    def unwatch(self):
        """Unwatches all previously specified keys"""
        self._execution_strategy.unwatch()

    def script_load_for_pipeline(self, *args, **kwargs):
        self._execution_strategy.script_load_for_pipeline(*args, **kwargs)

    def delete(self, *names):
        self._execution_strategy.delete(*names)

    def unlink(self, *names):
        """
        Stages an `UNLINK` for keys ``names``. Like `delete`, it returns ``None``
        rather than the pipeline, so it does not chain.

        Outside a transactional block only one key is accepted, as with `delete`; more
        raises :class:`~.RedisClusterException`. Between :meth:`watch` and
        :meth:`multi` it is not staged: it is sent at once, outside the eventual
        transaction, and the response is discarded.
        """
        self._execution_strategy.unlink(*names)


def block_pipeline_command(name: str) -> Callable[..., Any]:
    """
    Prints error because some pipelined commands should
    be blocked when running in cluster-mode
    """

    def inner(*args, **kwargs):
        raise RedisClusterException(
            f"ERROR: Calling pipelined function {name} is blocked "
            f"when running redis in cluster mode..."
        )

    return inner


# The script runners whose keys follow a ``numkeys`` count right after the script, so they are
# found without asking the server for them.
_EVAL_COMMANDS = ("EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO")


def is_zero_key_eval_command(*args) -> bool:
    """
    True for EVAL/EVALSHA, or their read-only variants, with numkeys=0 (any primary).
    """
    if len(args) < 3:
        return False
    if str(args[0]).upper() not in _EVAL_COMMANDS:
        return False
    try:
        return int(args[2]) == 0
    except (TypeError, ValueError):
        return False


# Blocked pipeline commands
PIPELINE_BLOCKED_COMMANDS = (
    "BGREWRITEAOF",
    "BGSAVE",
    "BITOP",
    "BLESS SCAN",
    "BRPOPLPUSH",
    "CLIENT GETNAME",
    "CLIENT KILL",
    "CLIENT LIST",
    "CLIENT SETNAME",
    "CLIENT",
    "CONFIG GET",
    "CONFIG RESETSTAT",
    "CONFIG REWRITE",
    "CONFIG SET",
    "CONFIG",
    "DBSIZE",
    "ECHO",
    "FLUSHALL",
    "FLUSHDB",
    "INFO",
    "KEYS",
    "LASTSAVE",
    "MGET",
    "MGET NONATOMIC",
    "MOVE",
    "MSET",
    "MSETEX",
    "MSET NONATOMIC",
    "MSETNX",
    "PFCOUNT",
    "PFMERGE",
    "PING",
    "PUBLISH",
    "RANDOMKEY",
    "READONLY",
    "READWRITE",
    "RENAME",
    "RENAMENX",
    "RPOPLPUSH",
    "SAVE",
    "SCAN",
    "SCRIPT EXISTS",
    "SCRIPT FLUSH",
    "SCRIPT KILL",
    "SCRIPT LOAD",
    "SCRIPT",
    "SDIFF",
    "SDIFFSTORE",
    "SENTINEL GET MASTER ADDR BY NAME",
    "SENTINEL MASTER",
    "SENTINEL MASTERS",
    "SENTINEL MONITOR",
    "SENTINEL REMOVE",
    "SENTINEL SENTINELS",
    "SENTINEL SET",
    "SENTINEL SLAVES",
    "SENTINEL",
    "SHUTDOWN",
    "SINTER",
    "SINTERSTORE",
    "SLAVEOF",
    "SLOWLOG GET",
    "SLOWLOG LEN",
    "SLOWLOG RESET",
    "SLOWLOG",
    "SMOVE",
    "SORT",
    "SUNION",
    "SUNIONSTORE",
    "TIME",
)
for command in PIPELINE_BLOCKED_COMMANDS:
    command = command.replace(" ", "_").lower()

    setattr(ClusterPipeline, command, block_pipeline_command(command))

# client_list_iter has no wire command of its own to add to
# PIPELINE_BLOCKED_COMMANDS - it sends CLIENT LIST, blocked above under its
# own name - so block it explicitly here too, or it would fall through to
# the inherited implementation and queue CLIENT LIST like a real pipelined
# command instead of raising.
setattr(ClusterPipeline, "client_list_iter", block_pipeline_command("client_list_iter"))


# The result of a pipeline command no node has answered yet. A reply can be None, so None
# cannot tell an unanswered command from one a node answered.
_UNANSWERED = object()


class PipelineCommand:
    """ """

    def __init__(self, args, options=None, position=None):
        self.args = args
        if options is None:
            options = {}
        self.options = options
        self.position = position
        self.result = _UNANSWERED
        self.node = None
        self.asking = False
        # Either record type: a policy resolver serves ``CommandPolicies``, while the
        # fallbacks below reuse the shared ``CommandMetadata`` defaults. Only the two routing
        # policies, which both carry, are ever read.
        self.command_policies: Optional[Union[CommandPolicies, CommandMetadata]] = None
        # The replies the command already got, kept across the attempts so a retry sends
        # it only where it has not been answered - keyed by slot for a split MULTI_SHARD
        # command, by node name for a command sent to several nodes
        self.replies: Optional[Dict[Any, Any]] = None
        # The ``target_nodes`` a retry must route by again - see
        # ``PipelineStrategy._send_cluster_commands``
        self.target_nodes: Any = None


class NodeCommands:
    """ """

    def __init__(
        self, parse_response, connection_pool: ConnectionPool, connection: Connection
    ):
        """ """
        self.parse_response = parse_response
        self.connection_pool = connection_pool
        self.connection = connection
        self.commands = []

    def append(self, c):
        """ """
        self.commands.append(c)

    def write(self):
        """
        Code borrowed from Redis so it can be fixed
        """
        connection = self.connection
        commands = self.commands

        # We are going to clobber the commands with the write, so go ahead
        # and ensure that nothing is sitting there from a previous run.
        for c in commands:
            c.result = _UNANSWERED

        # build up all commands into a single request to increase network perf
        # send all the commands and catch connection and timeout errors.
        try:
            connection.send_packed_command(
                connection.pack_commands([c.args for c in commands])
            )
        except (ConnectionError, TimeoutError) as e:
            for c in commands:
                c.result = e

    def read(self):
        """ """
        connection = self.connection
        for c in self.commands:
            # if there is a result on this command,
            # it means we ran into an exception
            # like a connection error. Trying to parse
            # a response on a connection that
            # is no longer open will result in a
            # connection error raised by redis-py.
            # but redis-py doesn't check in parse_response
            # that the sock object is
            # still set and if you try to
            # read from a closed connection, it will
            # result in an AttributeError because
            # it will do a readline() call on None.
            # This can have all kinds of nasty side-effects.
            # Treating this case as a connection error
            # is fine because it will dump
            # the connection object back into the
            # pool and on the next write, it will
            # explicitly open the connection and all will be well.
            if c.result is _UNANSWERED:
                try:
                    c.result = self.parse_response(connection, c.args[0], **c.options)
                except (ConnectionError, TimeoutError) as e:
                    for c in self.commands:
                        c.result = e
                    return
                except RedisError:
                    c.result = sys.exc_info()[1]


class ExecutionStrategy(ABC):
    @property
    @abstractmethod
    def command_queue(self):
        pass

    @abstractmethod
    def execute_command(self, *args, **kwargs):
        """
        Execution flow for current execution strategy.

        See: ClusterPipeline.execute_command()
        """
        pass

    @abstractmethod
    def annotate_exception(self, exception, number, command):
        """
        Annotate exception according to current execution strategy.

        See: ClusterPipeline.annotate_exception()
        """
        pass

    @abstractmethod
    def pipeline_execute_command(self, *args, **options):
        """
        Pipeline execution flow for current execution strategy.

        See: ClusterPipeline.pipeline_execute_command()
        """
        pass

    @abstractmethod
    def execute(self, raise_on_error: bool = True) -> List[Any]:
        """
        Executes current execution strategy.

        See: ClusterPipeline.execute()
        """
        pass

    @abstractmethod
    def send_cluster_commands(
        self, stack, raise_on_error=True, allow_redirections=True
    ):
        """
        Sends commands according to current execution strategy.

        See: ClusterPipeline.send_cluster_commands()
        """
        pass

    @abstractmethod
    def reset(self):
        """
        Resets current execution strategy.

        See: ClusterPipeline.reset()
        """
        pass

    @abstractmethod
    def exists(self, *keys):
        pass

    def touch(self, *keys):
        return self.execute_command("TOUCH", *keys)

    @abstractmethod
    def eval(self):
        pass

    @abstractmethod
    def multi(self):
        """
        Starts transactional context.

        See: ClusterPipeline.multi()
        """
        pass

    @abstractmethod
    def load_scripts(self):
        pass

    @abstractmethod
    def watch(self, *names):
        pass

    @abstractmethod
    def unwatch(self):
        """
        Unwatches all previously specified keys

        See: ClusterPipeline.unwatch()
        """
        pass

    @abstractmethod
    def script_load_for_pipeline(self, *args, **kwargs):
        pass

    @abstractmethod
    def delete(self, *names):
        """
        "Delete a key specified by ``names``"

        See: ClusterPipeline.delete()
        """
        pass

    @abstractmethod
    def unlink(self, *names):
        """
        "Unlink a key specified by ``names``"

        See: ClusterPipeline.unlink()
        """
        pass

    @abstractmethod
    def discard(self):
        pass


class AbstractStrategy(ExecutionStrategy):
    def __init__(
        self,
        pipe: ClusterPipeline,
    ):
        self._command_queue: List[PipelineCommand] = []
        self._pipe = pipe
        self._nodes_manager = self._pipe.nodes_manager

    @property
    def command_queue(self):
        return self._command_queue

    @command_queue.setter
    def command_queue(self, queue: List[PipelineCommand]):
        self._command_queue = queue

    @abstractmethod
    def execute_command(self, *args, **kwargs):
        pass

    def pipeline_execute_command(self, *args, **options):
        self._command_queue.append(
            PipelineCommand(args, options, len(self._command_queue))
        )
        return self._pipe

    def _himport_prepare_pipeline(self, redis_node, conn, commands):
        """Delegate to the shared sync HIMPORT executor."""
        _himport_exec.prepare_pipeline(redis_node, conn, [args for args, _ in commands])

    @abstractmethod
    def execute(self, raise_on_error: bool = True) -> List[Any]:
        pass

    @abstractmethod
    def send_cluster_commands(
        self, stack, raise_on_error=True, allow_redirections=True
    ):
        pass

    @abstractmethod
    def reset(self):
        pass

    def exists(self, *keys):
        return self.execute_command("EXISTS", *keys)

    def eval(self):
        """ """
        raise RedisClusterException("method eval() is not implemented")

    def load_scripts(self):
        """ """
        raise RedisClusterException("method load_scripts() is not implemented")

    def script_load_for_pipeline(self, *args, **kwargs):
        """ """
        raise RedisClusterException(
            "method script_load_for_pipeline() is not implemented"
        )

    def annotate_exception(self, exception, number, command):
        """
        Provides extra context to the exception prior to it being handled
        """
        cmd = command_text(command)
        msg = (
            f"Command # {number} ({truncate_text(cmd)}) of pipeline "
            f"caused error: {exception.args[0]}"
        )
        exception.args = (msg,) + exception.args[1:]


class PipelineStrategy(AbstractStrategy):
    def __init__(self, pipe: ClusterPipeline):
        super().__init__(pipe)
        self.command_flags = pipe.command_flags

    def execute_command(self, *args, **kwargs):
        return self.pipeline_execute_command(*args, **kwargs)

    def _raise_first_error(self, stack, start_time):
        """
        Raise the first exception on the stack
        """
        for c in stack:
            r = c.result
            if isinstance(r, Exception):
                self.annotate_exception(r, c.position + 1, c.args)

                record_operation_duration(
                    command_name="PIPELINE",
                    duration_seconds=time.monotonic() - start_time,
                    error=r,
                )

                raise r

    def execute(self, raise_on_error: bool = True) -> List[Any]:
        stack = self._command_queue
        if not stack:
            return []

        try:
            return self.send_cluster_commands(stack, raise_on_error)
        finally:
            self.reset()

    def reset(self):
        """
        Reset back to empty pipeline.
        """
        self._command_queue = []

    def send_cluster_commands(
        self, stack, raise_on_error=True, allow_redirections=True
    ):
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
        if not stack:
            return []
        retry_attempts = self._pipe.retry.get_retries()
        failure_count = 0
        # The error the previous attempt failed with, and when the last refresh
        # had ended by then: a refresh since that reached no node ends the
        # retries (see NodesManager._error_instead_of_retry)
        last_error: Optional[Exception] = None
        last_refresh_at: Optional[float] = None
        while True:
            # Run a topology refresh an earlier error asked for, once due
            if self._nodes_manager._refresh_requested:
                self._nodes_manager._run_requested_refresh(self._pipe.retry)
            if last_error is not None:
                outage_error = self._nodes_manager._error_instead_of_retry(
                    last_error, last_refresh_at
                )
                if outage_error is not None:
                    raise outage_error
            try:
                return self._send_cluster_commands(
                    stack,
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
                    failure_count += 1
                    last_error = e
                    last_refresh_at = self._nodes_manager._last_refresh_at
                    # Ask for a refresh of the topology, run by the next attempt
                    # once due; a request a connection error already made,
                    # naming its node, is kept
                    self._nodes_manager._request_refresh()
                    if type(e) in (ClusterDownError, SlotNotCoveredError):
                        # Give the cluster time to recover first, as a command
                        # that failed so does
                        time.sleep(0.25)
                    # Space the attempts out as the retry backoff says
                    delay = self._pipe._retry_backoff_delay(
                        self._pipe.retry, failure_count
                    )
                    if delay > 0:
                        time.sleep(delay)
                else:
                    raise _error_to_raise(
                        self._nodes_manager._last_refresh_unreachable_args, e
                    )

    def _send_cluster_commands(
        self, stack, raise_on_error=True, allow_redirections=True
    ):
        """
        Send a bunch of cluster commands to the redis cluster.

        `allow_redirections` If the pipeline should follow
        `ASK` & `MOVED` responses automatically. If set
        to false it will raise RedisClusterException.
        """
        # the first time sending the commands we send all of
        # the commands that were queued up.
        # if we have to run through it again, we only retry
        # the commands that failed, or that no node answered:
        # resending one that succeeded would run a write twice.
        attempt = sorted(
            (
                c
                for c in stack
                if c.result is _UNANSWERED or isinstance(c.result, Exception)
            ),
            key=lambda x: x.position,
        )
        # The commands whose reply is read raw below, so their cluster response callback
        # is applied once at the end. A command answered on an earlier attempt was
        # parsed then, and one resent on its own is parsed by ``_execute_command``.
        parsed_here = {c.position for c in attempt}
        is_default_node = False
        # build a list of node objects based on node names we need to
        nodes: dict[str, NodeCommands] = {}
        # node objects keyed by name, so each node's connection can be pre-flighted
        # for HIMPORT SET (PREPARE) before the batched write.
        node_objs: dict = {}
        # the commands sent to several nodes, each with its copies - keyed by node name, or
        # by slot for a split MULTI_SHARD command - and the split, if it was one.
        fanned_out: List[
            Tuple[
                PipelineCommand,
                Dict[Any, PipelineCommand],
                Optional[List[_MultiShardSplit]],
            ]
        ] = []
        nodes_written = 0
        nodes_read = 0

        try:
            # as we move through each command that still needs to be processed,
            # we figure out the slot number that command maps to, then from
            # the slot determine the node.
            for c in attempt:
                # refer to our internal node -> slot table that
                # tells us where a given command should route to.
                # (it might be possible we have a cached node that no longer
                # exists in the cluster, which is why we do this in a loop)
                passed_targets = c.options.pop("target_nodes", c.target_nodes)
                splits = None
                if passed_targets and not self._is_nodes_flag(passed_targets):
                    target_nodes = self._parse_target_nodes(passed_targets)

                    # The caller named its targets, so the command's own aggregation must
                    # not apply to their replies - see
                    # ``RedisCluster._resolve_command_policies``.
                    command_policies = _DEFAULT_KEYLESS_METADATA
                else:
                    if self._is_nodes_flag(passed_targets):
                        # A nodes flag picks the targets too, in ``_determine_nodes``
                        # below, and the command's own aggregation must not apply either
                        command_policies = _DEFAULT_KEYLESS_METADATA
                    else:
                        command_policies = self._pipe._policy_resolver.resolve(
                            c.args[0].lower()
                        )
                    if not command_policies:
                        command = c.args[0].upper()
                        if (
                            len(c.args) >= 2
                            and f"{c.args[0]} {c.args[1]}".upper()
                            in self._pipe.command_flags
                        ):
                            command = f"{c.args[0]} {c.args[1]}".upper()

                        # We only could resolve key properties if command is not
                        # in a list of pre-defined request policies
                        command_flag = self.command_flags.get(command)
                        if not command_flag:
                            # Fallback to default policy.
                            # EVAL/EVALSHA must not use _get_command_keys(): Redis
                            # <7 breaks on COMMAND GETKEYS when numkeys is 0.
                            # Other unflagged commands keep the keyless fallback.
                            if command in ("EVAL", "EVALSHA"):
                                command_policies = _DEFAULT_KEYED_METADATA
                            else:
                                if not self._pipe.get_default_node():
                                    keys = None
                                else:
                                    keys = self._pipe._get_command_keys(*c.args)
                                if not keys or len(keys) == 0:
                                    command_policies = _DEFAULT_KEYLESS_METADATA
                                else:
                                    command_policies = _DEFAULT_KEYED_METADATA
                        else:
                            if command_flag in self._pipe._command_flags_mapping:
                                command_policies = _METADATA_BY_REQUEST_POLICY[
                                    self._pipe._command_flags_mapping[command_flag]
                                ]
                            else:
                                command_policies = _DEFAULT_KEYLESS_METADATA

                    request_policy = command_policies.request_policy
                    if request_policy == RequestPolicy.MULTI_SHARD:
                        splits = self._pipe._split_multi_shard_command(*c.args)

                    if splits is not None and len(splits) < 2:
                        # One slot: sent as it is, to the node of the slot the split
                        # already computed
                        target_nodes = [
                            self._pipe._get_node_from_slot(c.args[0], splits[0][0])
                        ]
                        splits = None
                    elif splits is None:
                        target_nodes = self._determine_nodes(
                            *c.args,
                            request_policy=request_policy,
                            nodes_flag=passed_targets,
                        )
                        if not target_nodes:
                            raise RedisClusterException(
                                f"No targets were found to execute {c.args} command on"
                            )
                c.command_policies = command_policies
                # Kept for a retry, which would otherwise route the command by its own
                # policy: a nodes flag is resolved again, and a command sent to several
                # named nodes is sent to them again. A single named node is not kept, so a
                # retry routes a keyed command by its keys - which the per-slot commands of
                # ``mget_nonatomic``, ``delete`` and the other multi-key helpers rely on
                # after a failover.
                if passed_targets and (
                    self._is_nodes_flag(passed_targets) or len(target_nodes) > 1
                ):
                    c.target_nodes = passed_targets

                # A command sent to several nodes - or split into one command per slot of
                # its keys - is queued as copies that each carry one reply, and the replies
                # are folded back into ``c`` once they are read, so the pipeline still
                # answers with one result per queued command.
                if splits is not None:
                    # A slot that succeeded on an earlier attempt is not sent again.
                    if c.replies is None:
                        c.replies = {}
                    copies: Dict[Any, PipelineCommand] = {}
                    fanned_out.append((c, copies, splits))
                    queued = []
                    replica_safe = (
                        self._pipe.read_from_replicas
                        or self._pipe.load_balancing_strategy is not None
                    ) and self._pipe._is_replica_safe(c.args[0])
                    load_balancing_strategy = (
                        self._pipe.load_balancing_strategy if replica_safe else None
                    )
                    for slot, slot_args, _, keys in splits:
                        if slot in c.replies:
                            continue
                        copies[slot] = PipelineCommand(
                            slot_args, _with_keys(c.options, keys), c.position
                        )
                        queued.append(
                            (
                                self._nodes_manager.get_node_from_slot(
                                    slot, replica_safe, load_balancing_strategy
                                ),
                                copies[slot],
                            )
                        )
                elif len(target_nodes) > 1:
                    # A node that answered on an earlier attempt is not asked again. One
                    # the topology refresh dropped from the targets is dropped from the
                    # replies too, as on the main path.
                    if c.replies is None:
                        c.replies = {}
                    else:
                        _keep_target_replies(c.replies, target_nodes)
                    copies = {}
                    fanned_out.append((c, copies, None))
                    queued = []
                    for node in target_nodes:
                        if node.name in c.replies:
                            continue
                        copies[node.name] = PipelineCommand(
                            c.args, c.options, c.position
                        )
                        queued.append((node, copies[node.name]))
                else:
                    queued = [(target_nodes[0], c)]

                for node, queued_command in queued:
                    if node == self._pipe.get_default_node():
                        is_default_node = True

                    # now that we know the name of the node
                    # ( it's just a string in the form of host:port )
                    # we can build a list of commands for each node.
                    node_name = node.name
                    if node_name not in nodes:
                        redis_node = self._pipe.get_redis_connection(node)
                        try:
                            connection = get_connection(redis_node)
                        except (ConnectionError, TimeoutError) as e:
                            # Release any connections we've already acquired before clearing nodes
                            for n in nodes.values():
                                n.connection_pool.release(n.connection)
                            if not isinstance(e, RedisCluster.NOT_NODE_FAILURE_ERRORS):
                                # Connection retries are being handled in the node's
                                # Retry object. Ask for a refresh of the node -> slot
                                # table, run by the next attempt, of any command,
                                # once due. The failed node moves to the end of the
                                # cached nodes, so the refresh does not pick it as
                                # the default node while it is still a primary.
                                self._nodes_manager.move_node_to_end_of_cached_nodes(
                                    node.name
                                )
                                self._nodes_manager._request_refresh(
                                    last_failed_node_name=node.name
                                )
                                if is_default_node:
                                    self._pipe.replace_default_node()
                            nodes = {}
                            raise
                        nodes[node_name] = NodeCommands(
                            redis_node.parse_response,
                            redis_node.connection_pool,
                            connection,
                        )
                        node_objs[node_name] = node
                    nodes[node_name].append(queued_command)

            # send the commands in sequence.
            # we  write to all the open sockets for each node first,
            # before reading anything
            # this allows us to flush all the requests out across the
            # network
            # so that we can read them from different sockets as they come back.
            # we don't multiplex on the sockets as they come available,
            # but that shouldn't make too much difference.

            # HIMPORT SETs in the batch need their fieldsets prepared on each
            # node's connection first; the packed write bypasses the per-command
            # lazy prepare, so pre-flight the PREPARE (once per node) here.
            for node_name, n in nodes.items():
                redis_node = self._pipe.get_redis_connection(node_objs[node_name])
                self._himport_prepare_pipeline(
                    redis_node, n.connection, [(c.args, c.options) for c in n.commands]
                )

            # Start timing for observability
            start_time = time.monotonic()

            node_commands = nodes.values()
            for n in node_commands:
                nodes_written += 1
                n.write()

            for n in node_commands:
                n.read()

                # Find the first error in this node's commands, if any
                node_error = None
                for cmd in n.commands:
                    if isinstance(cmd.result, Exception):
                        node_error = cmd.result
                        break

                record_operation_duration(
                    command_name="PIPELINE",
                    duration_seconds=time.monotonic() - start_time,
                    server_address=n.connection.host,
                    server_port=n.connection.port,
                    db_namespace=str(n.connection.db),
                    error=node_error,
                )
                nodes_read += 1
        finally:
            # release all the redis connections we allocated earlier
            # back into the connection pool.
            # if the connection is dirty (that is: we've written
            # commands to it, but haven't read the responses), we need
            # to close the connection before returning it to the pool.
            # otherwise, the next caller to use this connection will
            # read the response from _this_ request, not its own request.
            # disconnecting discards the dirty state & forces the next
            # caller to reconnect.
            # NOTE: dicts have a consistent ordering; we're iterating
            # through nodes.values() in the same order as we are when
            # reading / writing to the connections above, which is critical
            # for how we're using the nodes_written/nodes_read offsets.
            for i, n in enumerate(nodes.values()):
                if i < nodes_written and i >= nodes_read:
                    n.connection.disconnect()
                n.connection_pool.release(n.connection)

        for c, copies, splits in fanned_out:
            c.result = self._merge_node_results(c, copies, splits)

        # if the response isn't an exception it is a
        # valid response from the node
        # we're all done with that command, YAY!
        # if we have more commands to attempt, we've run into problems.
        # collect all the commands we are allowed to retry.
        # (MOVED, ASK, or connection errors or timeout errors)
        attempt = sorted(
            (
                c
                for c in attempt
                if isinstance(c.result, ClusterPipeline.ERRORS_ALLOW_RETRY)
                and not isinstance(c.result, ClusterPipeline.NOT_NODE_FAILURE_ERRORS)
            ),
            key=lambda x: x.position,
        )
        parsed_here.difference_update(c.position for c in attempt)
        if attempt and allow_redirections:
            # RETRY MAGIC HAPPENS HERE!
            # send these remaining commands one at a time using `execute_command`
            # in the main client. This keeps our retry logic
            # in one place mostly,
            # and allows us to be more confident in correctness of behavior.
            # at this point any speed gains from pipelining have been lost
            # anyway, so we might as well make the best
            # attempt to get the correct behavior.
            #
            # The client command will handle retries for each
            # individual command sequentially as we pass each
            # one into `execute_command`. Any exceptions
            # that bubble out should only appear once all
            # retries have been exhausted.
            #
            # If a lot of commands have failed, we'll be setting the
            # flag to rebuild the slots table from scratch.
            # So MOVED errors should correct themselves fairly quickly.
            self._pipe.reinitialize_counter += 1
            if _should_request_refresh(
                self._pipe.reinitialize_steps, self._pipe.reinitialize_counter
            ):
                # Ask for a refresh of the topology and run it before the
                # commands are sent again, once due. When it is not due it
                # stays asked for, and a later command runs it.
                self._nodes_manager._request_refresh()
                self._nodes_manager._run_requested_refresh(self._pipe.retry)
                if is_default_node:
                    self._pipe.replace_default_node()
            # A split MULTI_SHARD command resends only the slots that failed, and a
            # command sent to several nodes asks only the nodes that have not answered
            fanned_out_by_position = {
                c.position: (copies, splits) for c, copies, splits in fanned_out
            }
            for c in attempt:
                fan_out = fanned_out_by_position.get(c.position)
                if fan_out is not None:
                    copies, splits = fan_out
                    if splits is not None:
                        c.result = self._retry_failed_slots(c, copies, splits)
                    else:
                        c.result = self._retry_failed_nodes(c)
                    continue
                try:
                    # send each command individually like we
                    # do in the main client.
                    if c.target_nodes is None:
                        c.result = self._pipe.parent_execute_command(
                            *c.args, **c.options
                        )
                    else:
                        c.result = self._pipe.parent_execute_command(
                            *c.args, target_nodes=c.target_nodes, **c.options
                        )
                except RedisError as e:
                    c.result = e

        # turn the response back into a simple flat array that corresponds
        # to the sequence of commands issued in the stack in pipeline.execute()
        # A fanned-out command had its per-node replies parsed when they were merged.
        parsed_here.difference_update(c.position for c, _, _ in fanned_out)
        response = []
        for c in sorted(stack, key=lambda x: x.position):
            if (
                c.position in parsed_here
                and not isinstance(c.result, Exception)
                and c.args[0] in self._pipe.cluster_response_callbacks
            ):
                # Remove keys entry, it needs only for cache.
                c.options.pop("keys", None)
                c.result = self._pipe.cluster_response_callbacks[c.args[0]](
                    c.result, **c.options
                )
            response.append(c.result)

        if raise_on_error:
            self._raise_first_error(stack, start_time)

        return response

    def _merge_node_results(
        self,
        command: PipelineCommand,
        copies: Dict[Any, PipelineCommand],
        splits: Optional[List[_MultiShardSplit]] = None,
    ) -> Any:
        """
        Fold the per-node replies of a command the pipeline sent to several nodes into the
        one result the command answers with, the way the main client does: each reply is
        parsed by the command's cluster response callback, an error fails the command
        unless its response policy expects one, and the replies are then merged by the
        command's result callback or folded by its response policy. The replies of a split
        MULTI_SHARD command are merged per slot instead - see
        ``_merge_multi_shard_replies`` - once every slot has succeeded, and the slots that
        did are kept on ``command`` so a retry resends only the ones that failed.
        """
        name = command.args[0]
        # Remove keys entry, it needs only for cache.
        options = {
            key: value for key, value in command.options.items() if key != "keys"
        }
        parse = self._pipe.cluster_response_callbacks.get(name)
        res = {
            node_name: (
                parse(c.result, **options)
                if parse is not None and not isinstance(c.result, Exception)
                else c.result
            )
            for node_name, c in copies.items()
        }

        response_policy = command.command_policies.response_policy
        if splits is not None:
            error = _record_replies(command.replies, res.items())
            if error is not None:
                return error
            return _merge_multi_shard_replies(command.replies, splits, response_policy)

        # The nodes that answered on an earlier attempt are merged with this one's
        _record_replies(command.replies, res.items())
        res, error = _split_successes_and_failure(
            {**command.replies, **res}, response_policy
        )
        if error is not None:
            return error

        return self._pipe._process_result(
            self._routed_name(command), res, response_policy=response_policy, **options
        )

    def _routed_name(self, command: PipelineCommand) -> str:
        """
        The name a command's result callback is keyed by: the routed name, which joins a
        container command's two words - see ``RedisCluster._resolve_command_policies``.
        """
        name = command.args[0]
        if (
            len(command.args) >= 2
            and f"{name} {command.args[1]}".upper() in self._pipe.command_flags
        ):
            return f"{name} {command.args[1]}".upper()
        return name.upper()

    def _retry_failed_nodes(self, command: PipelineCommand) -> Any:
        """
        Ask each node of a command sent to several nodes that has not answered, one node
        at a time, like the main client does, and merge the replies once every node has
        answered. A node that already answered is not asked again - see
        ``_record_replies``.

        The targets are resolved again, the way the first attempt resolved them, so a
        node the topology refresh replaced is asked in its place.
        """
        if command.target_nodes is not None and not self._is_nodes_flag(
            command.target_nodes
        ):
            target_nodes = self._parse_target_nodes(command.target_nodes)
        else:
            target_nodes = self._determine_nodes(
                *command.args,
                request_policy=command.command_policies.request_policy,
                nodes_flag=command.target_nodes,
            )

        _keep_target_replies(command.replies, target_nodes)
        res = dict(command.replies)
        for node in target_nodes:
            if node.name in command.replies:
                continue
            try:
                reply = self._pipe.parent_execute_command(
                    *command.args, target_nodes=[node], **command.options
                )
            except RedisError as e:
                res[node.name] = e
                continue
            command.replies[node.name] = reply
            res[node.name] = reply

        response_policy = command.command_policies.response_policy
        res, error = _split_successes_and_failure(res, response_policy)
        if error is not None:
            return error
        # Remove keys entry, it needs only for cache.
        options = {
            key: value for key, value in command.options.items() if key != "keys"
        }
        return self._pipe._process_result(
            self._routed_name(command), res, response_policy=response_policy, **options
        )

    def _retry_failed_slots(
        self,
        command: PipelineCommand,
        copies: Dict[Any, PipelineCommand],
        splits: List[_MultiShardSplit],
    ) -> Any:
        """
        Send each slot of a split MULTI_SHARD command that failed individually, like the
        main client does, and merge the replies once every slot has succeeded. A slot that
        already succeeded is not sent again - see ``_record_replies``.
        """
        for slot, slot_command in copies.items():
            if slot in command.replies:
                continue
            try:
                command.replies[slot] = self._pipe.parent_execute_command(
                    *slot_command.args, **slot_command.options
                )
            except RedisError as e:
                return e

        return _merge_multi_shard_replies(
            command.replies, splits, command.command_policies.response_policy
        )

    def _is_nodes_flag(self, target_nodes):
        return isinstance(target_nodes, str) and target_nodes in self._pipe.node_flags

    def _parse_target_nodes(self, target_nodes):
        if isinstance(target_nodes, list):
            nodes = target_nodes
        elif isinstance(target_nodes, ClusterNode):
            # Supports passing a single ClusterNode as a variable
            nodes = [target_nodes]
        elif isinstance(target_nodes, dict):
            # Supports dictionaries of the format {node_name: node}.
            # It enables to execute commands with multi nodes as follows:
            # rc.cluster_save_config(rc.get_primaries())
            nodes = target_nodes.values()
        else:
            raise TypeError(
                "target_nodes type can be one of the following: "
                "node_flag (PRIMARIES, REPLICAS, RANDOM, ALL_NODES),"
                "ClusterNode, list<ClusterNode>, or dict<any, ClusterNode>. "
                f"The passed type is {type(target_nodes)}"
            )
        return nodes

    def _determine_nodes(
        self, *args, request_policy: Optional[RequestPolicy] = None, **kwargs
    ) -> List["ClusterNode"]:
        # Determine which nodes should be executed the command on.
        # Returns a list of target nodes.
        command = args[0].upper()
        if (
            len(args) >= 2
            and f"{args[0]} {args[1]}".upper() in self._pipe.command_flags
        ):
            command = f"{args[0]} {args[1]}".upper()

        # The caller resolves the command's own policy - see
        # ``RedisCluster._resolve_command_policies`` - so the only decision left here is
        # an explicit nodes flag, which overrides it.
        nodes_flag = kwargs.pop("nodes_flag", None)
        if nodes_flag and self._is_nodes_flag(nodes_flag):
            # nodes flag passed by the user
            if nodes_flag in self._pipe._command_flags_mapping:
                request_policy = self._pipe._command_flags_mapping[nodes_flag]

        if request_policy is None:
            raise RedisClusterException(
                f"No targets were found to execute {args} command on"
            )

        nodes = _REQUEST_POLICY_CALLBACKS[request_policy](self._pipe, command, *args)

        if args[0].lower() == "ft.aggregate":
            self._aggregate_nodes = nodes

        return nodes

    def multi(self):
        raise RedisClusterException(
            "method multi() is not supported outside of transactional context"
        )

    def discard(self):
        raise RedisClusterException(
            "method discard() is not supported outside of transactional context"
        )

    def watch(self, *names):
        raise RedisClusterException(
            "method watch() is not supported outside of transactional context"
        )

    def unwatch(self, *names):
        raise RedisClusterException(
            "method unwatch() is not supported outside of transactional context"
        )

    def delete(self, *names):
        if len(names) != 1:
            raise RedisClusterException(
                "deleting multiple keys is not implemented in pipeline command"
            )

        return self.execute_command("DEL", names[0])

    def unlink(self, *names):
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

    def __init__(self, pipe: ClusterPipeline):
        super().__init__(pipe)
        self._explicit_transaction = False
        self._watching = False
        self._watch_acknowledged = False
        self._pipeline_slots: Set[int] = set()
        # True once a keyed (non-slot-agnostic) command has fixed the slot
        self._transaction_has_keyed_slot = False
        self._transaction_connection: Optional[Connection] = None
        self._transaction_node: Optional[ClusterNode] = None
        self._executing = False
        # The error the running retry is retrying, and when the last refresh
        # had ended by then: a refresh since that reached no node ends the
        # retries (see NodesManager._error_instead_of_retry)
        self._retrying: Optional[Tuple[Exception, Optional[float]]] = None
        self._retry = copy(self._pipe.retry)
        self._retry.update_supported_errors(
            RedisCluster.ERRORS_ALLOW_RETRY + self.SLOT_REDIRECT_ERRORS
        )

    def _resolve_transaction_slot(self, *args) -> Optional[int]:
        """
        Pick a slot for a transactional pipeline command.

        Zero-key EVAL/EVALSHA can run on any primary. Reuse an existing
        transaction slot when present so multiple zero-key scripts (or a
        mix with keyed commands) stay single-slot.
        """
        if args[0] in ClusterPipeline.NO_SLOTS_COMMANDS:
            return None

        if is_zero_key_eval_command(*args):
            if self._pipeline_slots:
                return next(iter(self._pipeline_slots))
            return self._pipe.determine_slot(*args)

        slot_number = self._pipe.determine_slot(*args)
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

    def _get_client_and_connection_for_transaction(self) -> Tuple[Redis, Connection]:
        """
        Find a connection for a pipeline transaction.

        For running an atomic transaction, watch keys ensure that contents have not been
        altered as long as the watch commands for those keys were sent over the same
        connection. So once we start watching a key, we fetch a connection to the
        node that owns that slot and reuse it.
        """
        if not self._pipeline_slots:
            raise RedisClusterException(
                "At least a command with a key is needed to identify a node"
            )

        if self._transaction_connection:
            # Keep the node the held connection belongs to, even if a refresh
            # moved the slot: the server reports the move (MOVED), and the error
            # path releases the connection to its own node before retargeting
            return (
                self._pipe.get_redis_connection(self._transaction_node),
                self._transaction_connection,
            )

        node: ClusterNode = self._nodes_manager.get_node_from_slot(
            list(self._pipeline_slots)[0], False
        )
        redis_node: Redis = self._pipe.get_redis_connection(node)
        self._transaction_connection = get_connection(redis_node)
        self._transaction_node = node

        return redis_node, self._transaction_connection

    def execute_command(self, *args, **kwargs):
        slot_number: Optional[int] = None
        if args[0] not in ClusterPipeline.NO_SLOTS_COMMANDS:
            slot_number = self._resolve_transaction_slot(*args)

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

            return self.pipeline_execute_command(*args, **kwargs)

    def _validate_watch(self):
        if self._explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")

        self._watching = True

    def _immediate_execute_command(self, *args, **options):
        try:
            return self._retry.call_with_retry(
                lambda: self._get_connection_and_send_command(*args, **options),
                self._reinitialize_on_error,
                is_retryable=RedisCluster._is_retryable_cluster_error,
                with_failure_count=True,
            )
        except (ConnectionError, TimeoutError) as e:
            raise _error_to_raise(self._nodes_manager._last_refresh_unreachable_args, e)
        finally:
            # The error of a retried attempt serves the run's next attempt only
            self._retrying = None

    def _raise_instead_of_retry(self) -> None:
        """
        Give up the running retry when a refresh that ended since its error
        reached no node (see NodesManager._error_instead_of_retry).
        """
        if self._retrying is not None:
            outage_error = self._nodes_manager._error_instead_of_retry(*self._retrying)
            if outage_error is not None:
                raise outage_error

    def _get_connection_and_send_command(self, *args, **options):
        # Run a topology refresh an earlier error asked for, once due. The flag
        # is read inline: this runs before every attempt.
        if self._nodes_manager._refresh_requested:
            self._nodes_manager._run_requested_refresh(self._pipe.retry)
        self._raise_instead_of_retry()
        redis_node, connection = self._get_client_and_connection_for_transaction()

        # Start timing for observability
        start_time = time.monotonic()

        try:
            response = self._send_command_parse_response(
                connection, redis_node, args[0], *args, **options
            )

            record_operation_duration(
                command_name=args[0],
                duration_seconds=time.monotonic() - start_time,
                server_address=connection.host,
                server_port=connection.port,
                db_namespace=str(connection.db),
            )

            return response
        except Exception as e:
            if connection:
                # this is used to report the metrics based on host and port info
                e.connection = connection
            record_operation_duration(
                command_name=args[0],
                duration_seconds=time.monotonic() - start_time,
                server_address=connection.host,
                server_port=connection.port,
                db_namespace=str(connection.db),
                error=e,
            )
            raise

    def _send_command_parse_response(
        self, conn, redis_node: Redis, command_name, *args, **options
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
            output = redis_node._himport_execute_set(conn, key, fieldset_name, values)
        else:
            conn.send_command(*args)
            output = redis_node.parse_response(conn, command_name, **options)

        if command_name == "WATCH":
            self._watch_acknowledged = True
        elif command_name in self.UNWATCH_COMMANDS:
            self._watching = False
            self._watch_acknowledged = False
        return output

    def _reinitialize_on_error(self, error, failure_count):
        self._retrying = (error, self._nodes_manager._last_refresh_at)
        if hasattr(error, "connection"):
            record_error_count(
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
            node = None
            if self._transaction_connection:
                if is_debug_log_enabled():
                    logger.debug(
                        f"Operation failed, "
                        f"with connection: {self._transaction_connection}, "
                        f"details: {self._transaction_connection.extract_connection_details()}",
                    )
                # Disconnect and release back to pool
                self._transaction_connection.disconnect()
                node = self._transaction_node
                self._release_transaction_connection()

            self._pipe.reinitialize_counter += 1
            should_request_refresh = _should_request_refresh(
                self._pipe.reinitialize_steps, self._pipe.reinitialize_counter
            )
            if isinstance(error, AskError):
                # MovedError is an AskError, so a MOVED slot is patched too, and
                # the retry goes to the node that owns it now. The refresh is
                # only asked for, run by the next command once due.
                if self._transaction_node is not None:
                    _fill_missing_redirect_host(error, self._transaction_node.host)
                self._nodes_manager.move_slot(error)
                if should_request_refresh:
                    self._nodes_manager._request_refresh()
                    self._pipe.reinitialize_counter = 0
            elif should_request_refresh:
                # A connection error: ask for a refresh that tries the failed
                # node last. The next attempt runs it once due, through
                # _run_requested_refresh, which keeps the current topology if
                # the refresh fails.
                if node is None and self._pipeline_slots:
                    # Connecting failed, so no connection names the node: it is
                    # the one that owns the slot the attempt targeted
                    slot_nodes = self._nodes_manager.slots_cache.get(
                        list(self._pipeline_slots)[0]
                    )
                    if slot_nodes:
                        node = slot_nodes[0]
                failed_node_name = node.name if node is not None else None
                if failed_node_name is not None:
                    self._nodes_manager.move_node_to_end_of_cached_nodes(
                        failed_node_name
                    )
                self._nodes_manager._request_refresh(
                    last_failed_node_name=failed_node_name
                )
                self._pipe.reinitialize_counter = 0

        self._executing = False
        if self._watch_acknowledged:
            self.reset()
            if isinstance(error, self.SLOT_REDIRECT_ERRORS):
                raise WatchError(
                    "Slot rebalancing occurred while watching keys"
                ) from error
            raise WatchError("An error occurred while watching keys") from error

    def _raise_first_error(self, responses, stack, start_time):
        """
        Raise the first exception on the stack
        """
        for r, cmd in zip(responses, stack):
            if isinstance(r, Exception):
                self.annotate_exception(r, cmd.position + 1, cmd.args)

                record_operation_duration(
                    command_name="TRANSACTION",
                    duration_seconds=time.monotonic() - start_time,
                    server_address=self._transaction_connection.host,
                    server_port=self._transaction_connection.port,
                    db_namespace=str(self._transaction_connection.db),
                )

                raise r

    def execute(self, raise_on_error: bool = True) -> List[Any]:
        stack = self._command_queue
        if not stack and (not self._watching or not self._pipeline_slots):
            return []

        return self._execute_transaction_with_retries(stack, raise_on_error)

    def _execute_transaction_with_retries(
        self, stack: List["PipelineCommand"], raise_on_error: bool
    ):
        try:
            return self._retry.call_with_retry(
                lambda: self._execute_transaction(stack, raise_on_error),
                lambda error, failure_count: self._reinitialize_on_error(
                    error, failure_count
                ),
                is_retryable=RedisCluster._is_retryable_cluster_error,
                with_failure_count=True,
            )
        except (ConnectionError, TimeoutError) as e:
            raise _error_to_raise(self._nodes_manager._last_refresh_unreachable_args, e)
        finally:
            # The error of a retried attempt serves the run's next attempt only
            self._retrying = None

    def _execute_transaction(
        self, stack: List["PipelineCommand"], raise_on_error: bool
    ):
        if len(self._pipeline_slots) > 1:
            raise CrossSlotTransactionError(
                "All keys involved in a cluster transaction must map to the same slot"
            )

        # Run a topology refresh an earlier error asked for, once due. The flag
        # is read inline: this runs before every attempt.
        if self._nodes_manager._refresh_requested:
            self._nodes_manager._run_requested_refresh(self._pipe.retry)
        self._raise_instead_of_retry()

        self._executing = True

        redis_node, connection = self._get_client_and_connection_for_transaction()

        # Ensure fieldsets referenced by buffered HIMPORT SETs are prepared on this
        # node's connection before the MULTI/EXEC block (session state, not
        # transactional). All keys share one slot here, so it is a single node.
        self._himport_prepare_pipeline(
            redis_node, connection, [(c.args, c.options) for c in stack]
        )

        stack = chain(
            [PipelineCommand(("MULTI",))],
            stack,
            [PipelineCommand(("EXEC",))],
        )
        commands = [c.args for c in stack if EMPTY_RESPONSE not in c.options]
        packed_commands = connection.pack_commands(commands)

        # Start timing for observability
        start_time = time.monotonic()

        connection.send_packed_command(packed_commands)
        errors = []

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            redis_node.parse_response(connection, "MULTI")
        except ResponseError as e:
            self.annotate_exception(e, 0, "MULTI")
            errors.append(e)
        except self.CONNECTION_ERRORS as cluster_error:
            self.annotate_exception(cluster_error, 0, "MULTI")
            raise

        # and all the other commands
        for i, command in enumerate(self._command_queue):
            if EMPTY_RESPONSE in command.options:
                errors.append((i, command.options[EMPTY_RESPONSE]))
            else:
                try:
                    _ = redis_node.parse_response(connection, "_")
                except self.SLOT_REDIRECT_ERRORS as slot_error:
                    self.annotate_exception(slot_error, i + 1, command.args)
                    errors.append(slot_error)
                except self.CONNECTION_ERRORS as cluster_error:
                    self.annotate_exception(cluster_error, i + 1, command.args)
                    raise
                except ResponseError as e:
                    self.annotate_exception(e, i + 1, command.args)
                    errors.append(e)

        response = None
        # parse the EXEC.
        try:
            response = redis_node.parse_response(connection, "EXEC")
        except ExecAbortError:
            if errors:
                raise errors[0]
            raise

        self._executing = False

        record_operation_duration(
            command_name="TRANSACTION",
            duration_seconds=time.monotonic() - start_time,
            server_address=connection.host,
            server_port=connection.port,
            db_namespace=str(connection.db),
        )

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

        # find any errors in the response and raise if necessary
        if raise_on_error or len(errors) > 0:
            self._raise_first_error(
                response,
                self._command_queue,
                start_time,
            )

        # We have to run response callbacks manually
        data = []
        for r, cmd in zip(response, self._command_queue):
            if not isinstance(r, Exception):
                command_name = cmd.args[0]
                if command_name in self._pipe.cluster_response_callbacks:
                    r = self._pipe.cluster_response_callbacks[command_name](
                        r, **cmd.options
                    )
            data.append(r)
        return data

    def _release_transaction_connection(self) -> None:
        """
        Return the held connection to the pool of the node it was taken from.

        That node, not the one ``nodes_cache`` holds under its name now: a
        refresh may have removed the node, or replaced it with a new instance,
        and the connection belongs to the old node's pool either way.
        """
        connection, self._transaction_connection = self._transaction_connection, None
        node = self._transaction_node
        if connection and node and node.redis_connection:
            node.redis_connection.connection_pool.release(connection)

    def reset(self):
        self._command_queue = []

        # make sure to reset the connection state in the event that we were
        # watching something
        if self._transaction_connection:
            try:
                if self._watching:
                    # call this manually since our unwatch or
                    # immediate_execute_command methods can call reset()
                    self._transaction_connection.send_command("UNWATCH")
                    self._transaction_connection.read_response()
                # we can safely return the connection to the pool here since we're
                # sure we're no longer WATCHing anything
                self._release_transaction_connection()
            except self.CONNECTION_ERRORS:
                # disconnect will also remove any previous WATCHes
                if self._transaction_connection:
                    self._transaction_connection.disconnect()
                    self._release_transaction_connection()

        # clean up the other instance attributes
        self._transaction_node = None
        self._watching = False
        self._watch_acknowledged = False
        self._explicit_transaction = False
        self._pipeline_slots = set()
        self._transaction_has_keyed_slot = False
        self._executing = False
        self._retrying = None

    def send_cluster_commands(
        self, stack, raise_on_error=True, allow_redirections=True
    ):
        raise NotImplementedError(
            "send_cluster_commands cannot be executed in transactional context."
        )

    def multi(self):
        if self._explicit_transaction:
            raise RedisError("Cannot issue nested calls to MULTI")
        if self._command_queue:
            raise RedisError(
                "Commands without an initial WATCH have already been issued"
            )
        self._explicit_transaction = True

    def watch(self, *names):
        if self._explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")

        return self.execute_command("WATCH", *names)

    def unwatch(self):
        if self._watching:
            return self.execute_command("UNWATCH")

        return True

    def discard(self):
        self.reset()

    def delete(self, *names):
        return self.execute_command("DEL", *names)

    def unlink(self, *names):
        return self.execute_command("UNLINK", *names)
