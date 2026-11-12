import threading
import warnings
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from redis.commands.metadata import MetadataResolver, StaticMetadataResolver
from redis.observability.attributes import CSCReason


class CacheEntryStatus(Enum):
    VALID = "VALID"
    IN_PROGRESS = "IN_PROGRESS"


class EvictionPolicyType(Enum):
    time_based = "time_based"
    frequency_based = "frequency_based"


class TrackingMode(Enum):
    """
    How a cache-managed connection enables server-side tracking.

    The mode is a connection-setup property: the server refuses to switch a live connection
    between ``OPTIN`` and ``OPTOUT``, so a configuration change applies to new connections
    only.

    ``PLAIN`` is the default, and was the only mode before 8.2, which added ``OPTIN`` and
    ``OPTOUT``.

    The two other modes reduce what the server has to remember, from the two ends,
    by pairing one ``CLIENT CACHING YES|NO`` immediately in front of a single read.
    """

    PLAIN = "plain"
    """``CLIENT TRACKING ON`` - every trackable read is tracked."""

    OPTIN = "optin"
    """``CLIENT TRACKING ON OPTIN`` - tracked only after ``CLIENT CACHING YES``."""

    OPTOUT = "optout"
    """``CLIENT TRACKING ON OPTOUT`` - tracked unless ``CLIENT CACHING NO``."""


CachePredicate = Callable[[str, tuple], bool]
"""Decides whether the application wants an eligible reply cached.

Receives the command name as the command method spells it (``"GET"``, ``"FT.SEARCH"``) and the
key tuple exactly as the invocation supplied it in ``keys=``, so its elements are ``str`` or
``bytes`` depending on what the caller passed. Consulted under ``optin`` and ``optout`` only,
and only for a command that already passed eligibility and carried keys.
"""


@dataclass(frozen=True)
class CacheKey:
    """
    Represents a unique key for a cache entry.

    Attributes:
        command (str): The Redis command being cached.
        redis_keys (tuple): The Redis keys involved in the command.
        redis_args (tuple): Additional arguments for the Redis command.
            This field is included in the cache key to ensure uniqueness
            when commands have the same keys but different arguments.
            Changing this field will affect cache key uniqueness.
    """

    command: str
    redis_keys: tuple
    redis_args: tuple = ()  # Additional arguments for the Redis command; affects cache key uniqueness.


class CacheEntry:
    def __init__(
        self,
        cache_key: CacheKey,
        cache_value: bytes,
        status: CacheEntryStatus,
        connection_ref,
    ):
        self.cache_key = cache_key
        self.cache_value = cache_value
        self.status = status
        self.connection_ref = connection_ref

    def __hash__(self):
        return hash(
            (self.cache_key, self.cache_value, self.status, self.connection_ref)
        )

    def __eq__(self, other):
        return hash(self) == hash(other)


class EvictionPolicyInterface(ABC):
    @property
    @abstractmethod
    def cache(self):
        pass

    @cache.setter
    @abstractmethod
    def cache(self, value):
        pass

    @property
    @abstractmethod
    def type(self) -> EvictionPolicyType:
        pass

    @abstractmethod
    def evict_next(self) -> CacheKey:
        pass

    @abstractmethod
    def evict_many(self, count: int) -> list[CacheKey]:
        pass

    @abstractmethod
    def touch(self, cache_key: CacheKey) -> None:
        pass


class CacheConfigurationInterface(ABC):
    @abstractmethod
    def get_cache_class(self):
        pass

    @abstractmethod
    def get_max_size(self) -> int:
        pass

    @abstractmethod
    def get_eviction_policy(self):
        pass

    @abstractmethod
    def is_exceeds_max_size(self, count: int) -> bool:
        pass

    @abstractmethod
    def is_allowed_to_cache(self, command: str) -> bool:
        pass

    # The three tracking-mode decisions are concrete, not abstract: this ABC is public and
    # implemented by third parties, so a configuration written against the previous version of
    # it must keep working. The defaults reproduce the existing behaviour exactly - plain tracking,
    # every eligible reply stored, no ``CLIENT CACHING NO`` ever paired. Same reasoning as
    # ``CacheConfig.set_metadata_resolver``, which was deliberately kept off this ABC.

    def get_tracking_mode(self) -> TrackingMode:
        return TrackingMode.PLAIN

    def should_cache(self, command: str, keys: tuple) -> bool:
        return True

    def is_trackable_read(self, command: str) -> bool:
        return False


class CacheInterface(ABC):
    @property
    @abstractmethod
    def collection(self) -> OrderedDict:
        pass

    @property
    @abstractmethod
    def config(self) -> CacheConfigurationInterface:
        pass

    @property
    @abstractmethod
    def eviction_policy(self) -> EvictionPolicyInterface:
        pass

    @property
    @abstractmethod
    def size(self) -> int:
        pass

    @abstractmethod
    def get(self, key: CacheKey) -> CacheEntry | None:
        pass

    @abstractmethod
    def set(self, entry: CacheEntry) -> bool:
        pass

    @abstractmethod
    def delete_by_cache_keys(self, cache_keys: list[CacheKey]) -> list[bool]:
        pass

    @abstractmethod
    def delete_by_redis_keys(self, redis_keys: list[bytes]) -> list[bool]:
        pass

    @abstractmethod
    def flush(self) -> int:
        pass

    @abstractmethod
    def is_cachable(self, key: CacheKey) -> bool:
        pass


class _IndexedCacheEntries(OrderedDict):
    """
    The cache's entry map, carrying a reverse index from Redis key to the entries holding it.

    Invalidation is the hot path: the server names a key and the cache has to find every entry
    whose invocation touched it. Doing that by scanning the whole map costs O(entries) per
    message, under the lock, for every message - and opt-in only reduces how many messages
    arrive, not what each one costs.

    The index lives on the mapping rather than in :class:`DefaultCache` because entries do not
    only leave through that class's methods: an eviction policy pops straight off
    ``cache.collection`` (see :meth:`LRUPolicy.evict_next`), so an index maintained one level
    up would go stale on every eviction, and a stale index is the one thing it must never be -
    a missed entry is a reply that is never invalidated. Overriding the mutating methods here
    means every insertion and removal path keeps it exact, whoever calls it.

    Most Redis keys have exactly one holder, and a ``set`` costs over 200 bytes, so a lone
    holder is stored bare and promoted to a ``set`` only when a second one arrives - and
    demoted back when it drops to one again. That trades an ``isinstance`` check on each
    index update for most of the index's memory.

    Every mutation holds ``_lock`` across both the map update and its index update. The map is
    shared by a whole pool, while each :class:`~redis.connection.CacheProxyConnection` guards it
    with a lock of its own, so two connections can mutate it at once - and an index update is a
    read-modify-write that would otherwise lose a holder, leaving an entry no invalidation can
    find. The lock is a leaf: nothing is acquired under it, so it cannot join a lock cycle with
    the connection and pool locks. Plain reads of the map - the cache-hit path - do not take it.
    """

    def __init__(self, *args, **kwargs) -> None:
        # Assigned before delegating: ``OrderedDict.__init__`` may populate, which routes
        # through the ``__setitem__`` below. The lock is re-entrant in case an ``OrderedDict``
        # implementation (PyPy's, say) routes one overridden method through another.
        self._lock = threading.RLock()
        self._by_redis_key: dict[Any, CacheKey | set[CacheKey]] = {}
        super().__init__(*args, **kwargs)

    def __setitem__(self, key: CacheKey, value: "CacheEntry") -> None:
        with self._lock:
            index = self._by_redis_key
            for redis_key in key.redis_keys:
                holders = index.get(redis_key)
                if holders is None:
                    index[redis_key] = key
                elif isinstance(holders, set):
                    holders.add(key)
                elif holders != key:
                    index[redis_key] = {holders, key}
            super().__setitem__(key, value)

    def __delitem__(self, key: CacheKey) -> None:
        with self._lock:
            super().__delitem__(key)
            self._unindex(key)

    def pop(self, key: CacheKey, *args):
        with self._lock:
            value = super().pop(key, *args)
            self._unindex(key)
            return value

    def popitem(self, last: bool = True):
        with self._lock:
            key, value = super().popitem(last=last)
            self._unindex(key)
            return key, value

    def clear(self) -> None:
        with self._lock:
            super().clear()
            self._by_redis_key.clear()

    def holders_of(self, redis_key) -> frozenset:
        """
        The cache keys of every entry whose invocation named ``redis_key``.

        Returned as a snapshot, because the caller deletes what it finds.
        """
        with self._lock:
            holders = self._by_redis_key.get(redis_key)
            if holders is None:
                return frozenset()
            if isinstance(holders, set):
                return frozenset(holders)
            return frozenset((holders,))

    def _unindex(self, key: CacheKey) -> None:
        # Called with ``_lock`` held.
        index = self._by_redis_key
        for redis_key in key.redis_keys:
            holders = index.get(redis_key)
            if holders is None:
                continue
            if isinstance(holders, set):
                holders.discard(key)
                if len(holders) == 1:
                    index[redis_key] = next(iter(holders))
                elif not holders:
                    del index[redis_key]
            elif holders == key:
                del index[redis_key]


class DefaultCache(CacheInterface):
    def __init__(
        self,
        cache_config: CacheConfigurationInterface,
    ) -> None:
        self._cache = _IndexedCacheEntries()
        self._cache_config = cache_config
        self._eviction_policy = self._cache_config.get_eviction_policy().value()
        self._eviction_policy.cache = self

    @property
    def collection(self) -> OrderedDict:
        return self._cache

    @property
    def config(self) -> CacheConfigurationInterface:
        return self._cache_config

    @property
    def eviction_policy(self) -> EvictionPolicyInterface:
        return self._eviction_policy

    @property
    def size(self) -> int:
        return len(self._cache)

    def set(self, entry: CacheEntry) -> bool:
        if not self.is_cachable(entry.cache_key):
            return False

        self._cache[entry.cache_key] = entry
        self._eviction_policy.touch(entry.cache_key)

        return True

    def get(self, key: CacheKey) -> CacheEntry | None:
        entry = self._cache.get(key, None)

        if entry is None:
            return None

        self._eviction_policy.touch(key)
        return entry

    def delete_by_cache_keys(self, cache_keys: list[CacheKey]) -> list[bool]:
        response = []

        for key in cache_keys:
            if self.get(key) is not None:
                self._cache.pop(key)
                response.append(True)
            else:
                response.append(False)

        return response

    def delete_by_redis_keys(self, redis_keys: list[bytes] | list[str]) -> list[bool]:
        response = []
        keys_to_delete = []

        for redis_key in redis_keys:
            # Prepare both versions for lookup
            candidates = [redis_key]
            if isinstance(redis_key, str):
                candidates.append(redis_key.encode("utf-8"))
            elif isinstance(redis_key, bytes):
                try:
                    candidates.append(redis_key.decode("utf-8"))
                except UnicodeDecodeError:
                    pass  # Non-UTF-8 bytes, skip str version

            # The reverse index answers this without walking the map. Both spellings are
            # looked up because an entry is indexed under its keys exactly as the invocation
            # supplied them, while the server names them in its own encoding. The two results
            # are unioned, so an entry indexed under both spellings of this one key is
            # collected once.
            holders: set[CacheKey] = set()
            for candidate in candidates:
                holders |= self._cache.holders_of(candidate)

            # An invalidation message never carries more than one key, so an entry holding
            # several keys (MGET) cannot be collected twice by one call. A duplicate pop for
            # a multi-key batch is not a reachable case - do not "fix" it.
            for cache_key in holders:
                keys_to_delete.append(cache_key)
                response.append(True)

        for key in keys_to_delete:
            self._cache.pop(key)

        return response

    def flush(self) -> int:
        elem_count = len(self._cache)
        self._cache.clear()
        return elem_count

    def is_cachable(self, key: CacheKey) -> bool:
        return self._cache_config.is_allowed_to_cache(key.command)


class CacheProxy(CacheInterface):
    """
    Proxy object that wraps cache implementations to enable additional logic on top
    """

    def __init__(self, cache: CacheInterface):
        self._cache = cache

    @property
    def collection(self) -> OrderedDict:
        return self._cache.collection

    @property
    def config(self) -> CacheConfigurationInterface:
        return self._cache.config

    @property
    def eviction_policy(self) -> EvictionPolicyInterface:
        return self._cache.eviction_policy

    @property
    def size(self) -> int:
        return self._cache.size

    def get(self, key: CacheKey) -> CacheEntry | None:
        return self._cache.get(key)

    def set(self, entry: CacheEntry) -> bool:
        is_set = self._cache.set(entry)

        if self.config.is_exceeds_max_size(self.size):
            # Lazy import to avoid circular dependency
            from redis.observability.recorder import record_csc_eviction

            record_csc_eviction(
                count=1,
                reason=CSCReason.FULL,
            )
            self.eviction_policy.evict_next()

        return is_set

    def delete_by_cache_keys(self, cache_keys: list[CacheKey]) -> list[bool]:
        return self._cache.delete_by_cache_keys(cache_keys)

    def delete_by_redis_keys(self, redis_keys: list[bytes]) -> list[bool]:
        return self._cache.delete_by_redis_keys(redis_keys)

    def flush(self) -> int:
        return self._cache.flush()

    def is_cachable(self, key: CacheKey) -> bool:
        return self._cache.is_cachable(key)


class LRUPolicy(EvictionPolicyInterface):
    def __init__(self):
        self.cache = None

    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache: CacheInterface):
        self._cache = cache

    @property
    def type(self) -> EvictionPolicyType:
        return EvictionPolicyType.time_based

    def evict_next(self) -> CacheKey:
        self._assert_cache()
        popped_entry = self._cache.collection.popitem(last=False)
        return popped_entry[0]

    def evict_many(self, count: int) -> list[CacheKey]:
        self._assert_cache()
        if count > len(self._cache.collection):
            raise ValueError("Evictions count is above cache size")

        popped_keys = []

        for _ in range(count):
            popped_entry = self._cache.collection.popitem(last=False)
            popped_keys.append(popped_entry[0])

        return popped_keys

    def touch(self, cache_key: CacheKey) -> None:
        self._assert_cache()

        if self._cache.collection.get(cache_key) is None:
            raise ValueError("Given entry does not belong to the cache")

        self._cache.collection.move_to_end(cache_key)

    def _assert_cache(self):
        if self.cache is None or not isinstance(self.cache, CacheInterface):
            raise ValueError("Eviction policy should be associated with valid cache.")


class EvictionPolicy(Enum):
    LRU = LRUPolicy


class CacheConfig(CacheConfigurationInterface):
    DEFAULT_CACHE_CLASS = DefaultCache
    DEFAULT_EVICTION_POLICY = EvictionPolicy.LRU
    DEFAULT_MAX_SIZE = 10000

    # DEPRECATED - no longer consulted, and it will be removed in a future release.
    #
    # Command eligibility is now decided from command metadata by the metadata resolver this
    # config holds, so this list no longer describes what gets cached. It is kept as a public
    # attribute only so an external caller reading it keeps working; editing it changes
    # nothing. The effective set it is replaced by differs from it by ``+FT.SUGGET``,
    # ``+FT.SUGLEN``, ``+DIGEST``, ``+EXPIRETIME``, ``+PEXPIRETIME``, ``+HEXPIRETIME``,
    # ``+HPEXPIRETIME``, ``+SDIFFCARD``, ``+SUNIONCARD`` and ``-XPENDING``, ``-TS.INFO``,
    # ``-XREAD`` - the three removals being commands the server itself reports as not cacheable.
    #
    # To change eligibility, edit ``redis.commands.metadata._STATIC_COMMAND_METADATA`` or pass
    # a ``metadata_resolver`` to the client. Nothing here.
    DEFAULT_ALLOW_LIST = [
        "BITCOUNT",
        "BITFIELD_RO",
        "BITPOS",
        "EXISTS",
        "GEODIST",
        "GEOHASH",
        "GEOPOS",
        "GEORADIUSBYMEMBER_RO",
        "GEORADIUS_RO",
        "GEOSEARCH",
        "GET",
        "GETBIT",
        "GETRANGE",
        "HEXISTS",
        "HGET",
        "HGETALL",
        "HKEYS",
        "HLEN",
        "HMGET",
        "HSTRLEN",
        "HVALS",
        "JSON.ARRINDEX",
        "JSON.ARRLEN",
        "JSON.GET",
        "JSON.MGET",
        "JSON.OBJKEYS",
        "JSON.OBJLEN",
        "JSON.RESP",
        "JSON.STRLEN",
        "JSON.TYPE",
        "LCS",
        "LINDEX",
        "LLEN",
        "LPOS",
        "LRANGE",
        "MGET",
        "SCARD",
        "SDIFF",
        "SINTER",
        "SINTERCARD",
        "SISMEMBER",
        "SMEMBERS",
        "SMISMEMBER",
        "SORT_RO",
        "STRLEN",
        "SUBSTR",
        "SUNION",
        "TS.GET",
        "TS.INFO",
        "TS.RANGE",
        "TS.REVRANGE",
        "TYPE",
        "XLEN",
        "XPENDING",
        "XRANGE",
        "XREAD",
        "XREVRANGE",
        "ZCARD",
        "ZCOUNT",
        "ZDIFF",
        "ZINTER",
        "ZINTERCARD",
        "ZLEXCOUNT",
        "ZMSCORE",
        "ZRANGE",
        "ZRANGEBYLEX",
        "ZRANGEBYSCORE",
        "ZRANK",
        "ZREVRANGE",
        "ZREVRANGEBYLEX",
        "ZREVRANGEBYSCORE",
        "ZREVRANK",
        "ZSCORE",
        "ZUNION",
    ]

    def __init__(
        self,
        max_size: int = DEFAULT_MAX_SIZE,
        cache_class: Any = DEFAULT_CACHE_CLASS,
        eviction_policy: EvictionPolicy = DEFAULT_EVICTION_POLICY,
        tracking_mode: TrackingMode = TrackingMode.PLAIN,
        cache_predicate: CachePredicate | None = None,
    ):
        # A bare string here - ``tracking_mode="optin"`` - would compare equal to no
        # ``TrackingMode`` member and so silently behave as plain mode, and the failure mode
        # of a mis-configured cache is a wrongly-cached reply. Refused instead, which is the
        # one thing this configuration validates.
        if not isinstance(tracking_mode, TrackingMode):
            raise TypeError(
                "tracking_mode must be a redis.cache.TrackingMode member, got "
                f"{tracking_mode!r}"
            )

        if cache_predicate is not None and tracking_mode is TrackingMode.PLAIN:
            warnings.warn(
                "cache_predicate is only consulted in optin and optout modes and is "
                "ignored with tracking_mode=plain.",
                UserWarning,
                stacklevel=2,
            )

        if cache_predicate is None and tracking_mode is TrackingMode.OPTIN:
            warnings.warn(
                "tracking_mode=optin with no cache_predicate stores nothing; every read is "
                "sent alone and left untracked. Configure cache_predicate to select what to "
                "cache.",
                UserWarning,
                stacklevel=2,
            )

        self._cache_class = cache_class
        self._max_size = max_size
        self._eviction_policy = eviction_policy
        self._tracking_mode = tracking_mode
        self._cache_predicate = cache_predicate
        # Defaulted here rather than taken as a constructor argument: eligibility is
        # configured at client level, through the ``metadata_resolver`` of the client or the
        # pool, which injects it below. Defaulting it means a config built standalone - in a
        # test, or by a user who configures nothing else - is fully functional, and decides
        # eligibility from the command metadata this library ships.
        self._metadata_resolver: MetadataResolver = StaticMetadataResolver()

    def set_metadata_resolver(self, metadata_resolver: MetadataResolver) -> None:
        """
        Set the metadata resolver that decides which commands may be cached.

        Called by the connection pool with the client-level resolver, so that one object
        serves both cluster routing and cache eligibility. Deliberately not part of
        :class:`CacheConfigurationInterface`: that ABC is public and implemented by third
        parties, so a custom configuration keeps whatever eligibility logic it has.

        Which object the pool calls this on depends on how the cache was supplied, and the
        difference is observable to a caller who reuses one configuration:

        - Given ``cache_config=``, the pool copies the configuration first, so the caller's
          object keeps the resolver it had and two clients sharing it stay independent. A
          later call to this method on the caller's object does not reach a pool already
          built from it.
        - Given ``cache=`` or ``cache_factory=``, the caller supplied a whole cache that
          reads its configuration on every lookup and cannot be handed a different one, so
          the pool calls this on the configuration inside it. One configuration reused that
          way therefore ends up with whichever resolver was injected last.

        Args:
            metadata_resolver: The resolver to decide eligibility through.
        """
        self._metadata_resolver = metadata_resolver

    def get_cache_class(self):
        return self._cache_class

    def get_max_size(self) -> int:
        return self._max_size

    def get_eviction_policy(self) -> EvictionPolicy:
        return self._eviction_policy

    def is_exceeds_max_size(self, count: int) -> bool:
        return count > self._max_size

    def get_tracking_mode(self) -> TrackingMode:
        return self._tracking_mode

    def get_cache_predicate(self) -> CachePredicate | None:
        return self._cache_predicate

    def is_allowed_to_cache(self, command: str) -> bool:
        # Fails closed on everything the resolver cannot decide: an unknown command, a name
        # the record tables cannot be keyed by, and a record built from incomplete metadata
        # all resolve to False. The verdict is memoized per command name, so this is a dict
        # hit on the command execution path.
        return self._metadata_resolver.is_cacheable(command)

    def should_cache(self, command: str, keys: tuple) -> bool:
        """
        Intent: does the application want this eligible reply stored?

        The second of the three decisions a cached read passes, and the only one the
        application configures directly.
        Eligibility - whether the reply is safe to cache at all - is answered before this
        by :meth:`is_allowed_to_cache` from the command metadata, and is never widened here.

        Asked only after eligibility said yes and the invocation supplied keys, so the
        predicate never sees a command it could not affect and never sees an empty key tuple.
        One function call per eligible read that reaches the cache layer.

        Args:
            command: The command name as the command method spells it.
            keys: The Redis keys of this invocation.

        Returns:
            bool: True when the reply may be stored.
        """
        if self._tracking_mode is TrackingMode.PLAIN:
            return True

        if self._cache_predicate is None:
            return self._tracking_mode is TrackingMode.OPTOUT

        return bool(self._cache_predicate(command, keys))

    def is_trackable_read(self, command: str) -> bool:
        """
        Whether the server would remember this command's keys - the readonly flag alone.

        Never affects what may be stored. It decides only whether a ``CLIENT CACHING NO`` in
        front of a read is worth sending under ``optout`` tracking, so it fails closed in the
        cheap direction: skipping the ``NO`` wastes invalidation-table entries, where storing
        an untracked reply is stale forever.

        Args:
            command: The command name as the command method spells it.

        Returns:
            bool: True only when the command carries the ``readonly`` command flag.
        """
        return self._metadata_resolver.is_trackable_read(command)


class CacheFactoryInterface(ABC):
    @abstractmethod
    def get_cache(self) -> CacheInterface:
        pass


class CacheFactory(CacheFactoryInterface):
    def __init__(self, cache_config: CacheConfig | None = None):
        self._config = cache_config

        if self._config is None:
            self._config = CacheConfig()

    def get_cache(self) -> CacheInterface:
        cache_class = self._config.get_cache_class()
        return CacheProxy(cache_class(cache_config=self._config))
