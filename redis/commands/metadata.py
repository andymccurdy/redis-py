"""
Normalized Redis command metadata.

This module owns the record types for everything the client derives from a ``COMMAND``
reply. ``CommandPolicies`` carries the request/response policies used for cluster routing;
``CommandMetadata`` is the superset record that adds the inputs to the
client-side-caching (CSC) eligibility rules.

A command is cacheable only when it is ``readonly``, is not ``blocking``, takes at least one
key name argument, and carries none of the ``nondeterministic_output``, ``script_runner`` or
``dont_cache`` markers - see :func:`_is_client_side_cacheable`, which is the one normative
implementation of those rules and also requires the source metadata to have been complete.
Every boolean below therefore defaults to the fail-closed value, so a partially populated
record is never mistaken for a cacheable one.

``MetadataResolver`` serves those records by command name. Resolvers chain through
``with_fallback``, first match wins, and an exhausted chain resolves to None - so an
unknown command fails closed. A cluster client reads the routing policies off the resolved
record; a client-side cache asks ``is_cacheable``, which applies the rules above to it. Either
way a consumer needs a reference to nothing but the resolver.

A record may also withhold its routing policies by leaving them None, which says that the
metadata does not describe how to route the command and the client must resolve the target
itself - see :func:`_to_command_policies`. The cacheability inputs of such a record are still
authoritative, so withholding routing costs nothing on the caching side.

``_STATIC_COMMAND_METADATA`` is the static resolver table, and the single source of truth for
what this client routes by: the commands eligible for client-side caching plus the commands the
7.1.0 ``redis.commands.policies.STATIC_POLICIES`` table carried. That table is now a frozen,
unread copy of its 7.1.0 self - edit routing here, never there. Values here are validated
against a live
``COMMAND`` reply - see the provenance note on the constant, which records the server it
was checked against and the entries that diverge on purpose, before editing it by hand.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, replace
from enum import Enum
from types import MappingProxyType

__all__ = [
    "AsyncBaseMetadataResolver",
    "AsyncDynamicMetadataResolver",
    "AsyncMetadataResolver",
    "AsyncStaticMetadataResolver",
    "BaseMetadataResolver",
    "CommandMetadata",
    "CommandMetadataRecordsCache",
    "CommandPolicies",
    "DynamicMetadataResolver",
    "MetadataResolver",
    "PolicyRecords",
    "RequestPolicy",
    "ResponsePolicy",
    "StaticMetadataResolver",
]


class RequestPolicy(Enum):
    ALL_NODES = "all_nodes"
    ALL_SHARDS = "all_shards"
    ALL_REPLICAS = "all_replicas"
    MULTI_SHARD = "multi_shard"
    SPECIAL = "special"
    DEFAULT_KEYLESS = "default_keyless"
    DEFAULT_KEYED = "default_keyed"
    DEFAULT_NODE = "default_node"


class ResponsePolicy(Enum):
    ONE_SUCCEEDED = "one_succeeded"
    ALL_SUCCEEDED = "all_succeeded"
    AGG_LOGICAL_AND = "agg_logical_and"
    AGG_LOGICAL_OR = "agg_logical_or"
    AGG_MIN = "agg_min"
    AGG_MAX = "agg_max"
    AGG_SUM = "agg_sum"
    SPECIAL = "special"
    DEFAULT_KEYLESS = "default_keyless"
    DEFAULT_KEYED = "default_keyed"


class CommandPolicies:
    """
    The routing view of a command's metadata: how to dispatch it, how to aggregate replies.

    Kept as a mutable class rather than folded into :class:`CommandMetadata` because this is
    the shape that shipped in 7.1.0.

    Compared by value, because a policy resolver serves the projection of the record it
    proxies rather than a record a caller handed it - so identity does not hold, and a caller
    matching what a resolver served against what it supplied has only the values to go by.
    """

    def __init__(
        self,
        request_policy: RequestPolicy = RequestPolicy.DEFAULT_KEYLESS,
        response_policy: ResponsePolicy = ResponsePolicy.DEFAULT_KEYLESS,
    ):
        self.request_policy = request_policy
        self.response_policy = response_policy

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CommandPolicies):
            return NotImplemented

        return (
            self.request_policy == other.request_policy
            and self.response_policy == other.response_policy
        )

    def __hash__(self) -> int:
        # Defined alongside ``__eq__`` because Python would otherwise set it to None, which
        # would make a type that shipped hashable in 7.1.0 unhashable.
        return hash((self.request_policy, self.response_policy))

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(request_policy={self.request_policy!r}, "
            f"response_policy={self.response_policy!r})"
        )


PolicyRecords = dict[str, dict[str, CommandPolicies]]


@dataclass(frozen=True, slots=True)
class CommandMetadata:
    """
    Normalized metadata for a single Redis command or container subcommand.

    Instances are immutable so the static table below can be shared without copying. A
    resolver that refines a record builds a new one with :func:`dataclasses.replace`.

    Attributes:
        request_policy:
            How the cluster client should route the command. Derived from the
            ``request_policy`` command tip, defaulting to keyed/keyless based on whether
            the command takes keys. None means the record withholds its routing opinion:
            the metadata does not describe how to route this command, so the client resolves
            the target itself. That is distinct from ``DEFAULT_KEYLESS``, which is a routing
            decision - send the command to any one node.
        response_policy:
            How the cluster client should aggregate replies from several nodes. Derived
            from the ``response_policy`` command tip, with the same default. None has the
            same meaning as it does for ``request_policy``; the two are withheld together,
            because a policy resolver serves them as one record.
        is_readonly:
            Whether the command has the ``readonly`` command flag. Required for
            cacheability.
        is_blocking:
            Whether the command has the ``blocking`` command flag, which marks a command
            that may block the connection until data arrives. A blocking read serves the
            reply its caller waited for rather than a snapshot of keyspace state, so
            re-serving it from a cache would break the command's execution-time contract:
            ``XREAD BLOCK`` in a loop would stop blocking once a timed-out reply was cached.
            The exclusion is by command name, so ``XREAD`` is ineligible even when a
            particular call omits ``BLOCK``. It is the only readonly, keyed command the flag
            excludes.
        has_key_argument:
            Whether the command accepts at least one Redis key name argument. True when
            ``key_specifications`` holds at least one spec not flagged ``not_key``,
            falling back to ``first_key_pos > 0 and step_count > 0`` when key specs are
            absent. ``last_key_pos`` is never consulted: it is ``-1`` for variadic
            commands and must not disqualify one.

            This is not the inverse of ``_is_keyless_command``, which tests
            ``first_key_pos`` alone and so reports every ``movablekeys`` command as
            keyless. It is also recorded independently of ``request_policy``, because a
            command tip overwrites the keyed/keyless default, after which the resolved
            policy no longer indicates whether the command takes keys.
        has_nondeterministic_output:
            Whether the command has the ``nondeterministic_output`` command tip, which
            marks a command whose reply may differ between calls over the same keyspace
            state. Matched exactly rather than by prefix: ``nondeterministic_output_order``
            is a distinct tip, denoting only that element order varies.
        is_script_runner:
            Whether the command has the ``script_runner`` command flag, which marks a
            command that executes a user-supplied script or function. Reported by Redis
            8.10 and later; earlier servers do not report the flag at all.
        is_dont_cache:
            Whether the command has the ``dont_cache`` command tip, which marks a command
            whose reply the server states must not be cached client-side. It is a negative
            override: it decides on its own, even when every positive rule matches.
        has_complete_metadata:
            Whether the source metadata was complete enough to trust, meaning the command
            flags were present *and* the ``tips`` key was present. ``COMMAND`` replies
            older than Redis 7.0 carry neither ``tips`` nor ``key_specifications``, which
            makes ``nondeterministic_output`` and ``dont_cache`` undetectable. An empty
            tips list is a real answer; a missing tips key is not. An incomplete record
            must not be treated as authoritative.
    """

    request_policy: RequestPolicy | None = RequestPolicy.DEFAULT_KEYLESS
    response_policy: ResponsePolicy | None = ResponsePolicy.DEFAULT_KEYLESS
    is_readonly: bool = False
    is_blocking: bool = False
    has_key_argument: bool = False
    has_nondeterministic_output: bool = False
    is_script_runner: bool = False
    is_dont_cache: bool = False
    has_complete_metadata: bool = False


CommandMetadataRecordsCache = Mapping[str, Mapping[str, CommandMetadata]]
"""Command metadata keyed by module name, then by command name.

Mirrors the shape of ``PolicyRecords``: non-module commands live under ``"core"``, module
commands under their lowercased prefix (``"json"``, ``"ts"``, ``"ft"``), and container
subcommands under their space-joined name (``"memory usage"``), which is the form
``execute_command`` receives as ``args[0]``.
"""


def _lowercase_keyed(
    records: CommandMetadataRecordsCache,
) -> CommandMetadataRecordsCache:
    """
    Key the given records lowercase, which is how :func:`_split_command_name` looks one up.

    Records built from a ``COMMAND`` reply already are - both parsers lowercase every command
    name as they read it - so the common case returns the argument untouched rather than
    rebuilding it. Caller-supplied records carry whatever spelling the caller chose, and a
    resolver that silently answers nothing for a table keyed ``"ZCOUNT"`` would be worse than
    one that normalizes it.
    """
    if all(
        module_name == module_name.lower()
        and all(command == command.lower() for command in commands)
        for module_name, commands in records.items()
    ):
        return records

    return {
        module_name.lower(): {
            command.lower(): metadata for command, metadata in commands.items()
        }
        for module_name, commands in records.items()
    }


def _split_command_name(command_name: str) -> tuple[str, str]:
    """
    Split a command name into the ``(module, command)`` pair the record tables are keyed by.

    Non-module commands resolve under ``"core"``. Container subcommands are not split here:
    they arrive space-joined (``"memory usage"``) and are looked up under that whole name.

    The name is lowercased, because the record tables are keyed lowercase while callers
    spell a command the way they send it: the cluster client lowercases before resolving,
    but the client-side cache is handed the command name as the command methods spell it
    (``"GET"``, ``"JSON.GET"``, ``"MEMORY USAGE"``).

    Raises:
        ValueError: If the name carries more than one module prefix.
    """
    parts = command_name.lower().split(".")

    if len(parts) > 2:
        raise ValueError(f"Wrong command or module name: {command_name}")

    if len(parts) == 2:
        return parts[0], parts[1]

    return "core", parts[0]


# Upper bound on each of a resolver's memos. Far above the number of distinct command names
# an application issues - the whole core surface plus every module's is a few hundred - but
# bounded, because the default resolver a client gets is a single instance evaluated at import
# and shared by every client in the process, ``execute_command`` accepts an arbitrary command
# name, and a name that resolves to nothing is memoized too. Past the cap the views still
# answer correctly; they just recompute.
_MEMO_MAX_ENTRIES = 4096

# Commands whose ``readonly`` flag is not sufficient to make replica routing safe. TOUCH
# changes key idle-time state even though Redis reports it readonly, so it must continue to
# reach a primary. Keep this exception narrow: caller-supplied metadata remains authoritative
# for ordinary core and module reads.
#
# Loaded into every resolver's ``_replica_safe`` memo at construction, which is the whole
# mechanism: the entry is in place before any command is looked up, so these answer from the
# same single dict lookup as every other command and ``is_replica_safe`` needs no test of its
# own. Nothing clears or evicts from that memo - the cap only declines to add - so a seeded
# entry cannot be lost. Names are lower case, the form command names are keyed by throughout
# this module.
_REPLICA_UNSAFE_COMMANDS = frozenset({"touch"})


class MetadataResolver(ABC):
    @abstractmethod
    def resolve(self, command_name: str) -> CommandMetadata | None:
        """
        Resolves the command name and determines the associated command metadata.

        Args:
            command_name: The name of the command to resolve, in any case.

        Returns:
            CommandMetadata: The metadata associated with the specified command, or None
            when no resolver in the chain knows the command.
        """
        pass

    @abstractmethod
    def resolve_policies(self, command_name: str) -> CommandPolicies | None:
        """
        Resolves the command name and determines the associated routing policies.

        The routing view of :meth:`resolve`: the request/response policies the resolved
        record carries, and nothing else about it.

        Args:
            command_name: The name of the command to resolve, in any case.

        Returns:
            CommandPolicies: The policies associated with the specified command, or None
            when no resolver in the chain knows the command.
        """
        pass

    @abstractmethod
    def is_cacheable(self, command_name: str) -> bool:
        """
        Determines whether the reply of a command may be served from a client-side cache.

        The client-side-caching view of :meth:`resolve`, decided by
        :func:`_is_client_side_cacheable`. Fails closed: an unknown command, a name the
        record tables cannot be keyed by, and a record built from incomplete metadata all
        resolve to False.

        Args:
            command_name: The name of the command to check, in any case.

        Returns:
            bool: True only when every eligibility rule is satisfied.
        """
        pass

    @abstractmethod
    def is_replica_safe(self, command_name: str) -> bool:
        """
        Determines whether a command is safe to execute on a replica.

        Args:
            command_name: The name of the command to check, in any case.

        Returns:
            bool: True if the command is replica safe.
        """
        pass

    @abstractmethod
    def is_trackable_read(self, command_name: str) -> bool:
        """
        Determines whether the server would remember this command's keys while tracking.

        The server-side-tracking view of :meth:`resolve`, decided by
        :func:`_is_trackable_read`. Never affects what may be stored: it only decides whether
        a ``CLIENT CACHING NO`` in front of a read is worth sending under ``optout`` tracking.
        When in doubt it returns False, so the ``NO`` is skipped and the server keeps
        tracking the keys. That is the safe side: an extra tracked key only costs an entry in
        the server's invalidation table, but a stored reply that the server does not track is
        never invalidated and can go stale.

        Args:
            command_name: The name of the command to check, in any case.

        Returns:
            bool: True only when the command carries the ``readonly`` command flag.
        """
        pass

    @abstractmethod
    def with_fallback(self, fallback: "MetadataResolver") -> "MetadataResolver":
        """
        Factory method to instantiate a metadata resolver with a fallback resolver.

        Args:
            fallback: Fallback resolver

        Returns:
            MetadataResolver: Returns a new metadata resolver with the specified fallback resolver.
        """
        pass


class AsyncMetadataResolver(ABC):
    @abstractmethod
    async def resolve(self, command_name: str) -> CommandMetadata | None:
        """
        Resolves the command name and determines the associated command metadata.

        Args:
            command_name: The name of the command to resolve, in any case.

        Returns:
            CommandMetadata: The metadata associated with the specified command, or None
            when no resolver in the chain knows the command.
        """
        pass

    @abstractmethod
    async def resolve_policies(self, command_name: str) -> CommandPolicies | None:
        """
        Resolves the command name and determines the associated routing policies.

        The routing view of :meth:`resolve`: the request/response policies the resolved
        record carries, and nothing else about it.

        Args:
            command_name: The name of the command to resolve, in any case.

        Returns:
            CommandPolicies: The policies associated with the specified command, or None
            when no resolver in the chain knows the command.
        """
        pass

    @abstractmethod
    async def is_cacheable(self, command_name: str) -> bool:
        """
        Determines whether the reply of a command may be served from a client-side cache.

        The client-side-caching view of :meth:`resolve`, decided by
        :func:`_is_client_side_cacheable`. Fails closed: an unknown command, a name the
        record tables cannot be keyed by, and a record built from incomplete metadata all
        resolve to False.

        Args:
            command_name: The name of the command to check, in any case.

        Returns:
            bool: True only when every eligibility rule is satisfied.
        """
        pass

    @abstractmethod
    async def is_replica_safe(self, command_name: str) -> bool:
        """
        Determines whether a command is safe to execute on a replica.

        Args:
            command_name: The name of the command to check, in any case.

        Returns:
            bool: True if the command is replica safe.
        """
        pass

    @abstractmethod
    async def is_trackable_read(self, command_name: str) -> bool:
        """
        Determines whether the server would remember this command's keys while tracking.

        The server-side-tracking view of :meth:`resolve`, decided by
        :func:`_is_trackable_read`. Never affects what may be stored: it only decides whether
        a ``CLIENT CACHING NO`` in front of a read is worth sending under ``optout`` tracking.
        When in doubt it returns False, so the ``NO`` is skipped and the server keeps
        tracking the keys. That is the safe side: an extra tracked key only costs an entry in
        the server's invalidation table, but a stored reply that the server does not track is
        never invalidated and can go stale.

        Args:
            command_name: The name of the command to check, in any case.

        Returns:
            bool: True only when the command carries the ``readonly`` command flag.
        """
        pass

    @abstractmethod
    def with_fallback(
        self, fallback: "AsyncMetadataResolver"
    ) -> "AsyncMetadataResolver":
        """
        Factory method to instantiate an async metadata resolver with a fallback resolver.

        Args:
            fallback: Fallback resolver

        Returns:
            AsyncMetadataResolver: Returns a new metadata resolver with the specified fallback resolver.
        """
        pass


class BaseMetadataResolver(MetadataResolver):
    """
    Base class for metadata resolvers.

    Lookup is first-match-wins: a command the records do not carry falls through to the
    fallback resolver, and a chain that ends without a match resolves to None. Besides the
    whole record, each consumer gets the view it needs of the same resolved metadata:
    ``resolve_policies`` for cluster routing, ``is_cacheable`` for client-side caching.

    Both views are memoized under the lowercased command name, so every spelling of one
    command - ``GET`` as the command methods write it, ``get`` as the cluster client lowers it
    - shares a single entry instead of taking one each. The metadata a resolver serves is a
    snapshot taken when the resolver is built, so a command's view cannot change, and the memo
    keeps the record lookup, the projection and the walk down the fallback chain off the
    command execution path. Concurrent resolves of the same command may each compute it once;
    the memo is idempotent, so the duplicated work is harmless. The memos grow with the set of
    distinct commands a caller asks about, and are capped at ``_MEMO_MAX_ENTRIES`` so that a
    caller asking about unbounded many names - a name that resolves to nothing is memoized
    too - cannot grow them without bound.
    """

    def __init__(
        self,
        metadata: CommandMetadataRecordsCache,
        fallback: MetadataResolver | None = None,
    ) -> None:
        self._metadata = metadata
        self._fallback = fallback
        self._policies: dict[str, CommandPolicies | None] = {}
        self._cacheable: dict[str, bool] = {}
        self._replica_safe: dict[str, bool] = {
            cmd: False for cmd in _REPLICA_UNSAFE_COMMANDS
        }
        # No ``_REPLICA_UNSAFE_COMMANDS`` pre-seed: trackability is the readonly flag alone,
        # and TOUCH - the one command that seed excludes - is precisely the trackable read
        # ``optout`` most wants to exempt.
        self._trackable_read: dict[str, bool] = {}

    def resolve(self, command_name: str) -> CommandMetadata | None:
        module, command = _split_command_name(command_name)

        commands = self._metadata.get(module)
        metadata = commands.get(command) if commands is not None else None

        if metadata is None:
            if self._fallback is not None:
                return self._fallback.resolve(command_name)
            return None

        return metadata

    def resolve_policies(self, command_name: str) -> CommandPolicies | None:
        # Memoized under the lowercased name, so the spellings of one command share an
        # entry rather than taking one each. ``resolve`` is still asked with the name as
        # given, so an unresolvable one is reported the way the caller spelled it.
        memo_key = command_name.lower()

        try:
            return self._policies[memo_key]
        except KeyError:
            pass

        metadata = self.resolve(command_name)
        policies = _to_command_policies(metadata) if metadata is not None else None

        if len(self._policies) < _MEMO_MAX_ENTRIES:
            self._policies[memo_key] = policies

        return policies

    def is_cacheable(self, command_name: str) -> bool:
        # A name the record tables cannot be keyed by is not a command this client can decide
        # on. Fail closed rather than raise into the command execution path, where a raw
        # command with such a name must still reach the server and come back with the
        # server's own error. ``execute_command`` accepts an arbitrary first argument, and a
        # non-str one - ``bytes``, which the request encoder accepts - reaches here
        # unchanged; it is refused rather than decoded, because deciding it would start
        # caching a command whose name this client never resolved.
        if not isinstance(command_name, str):
            return False

        # Memoized under the lowercased name, so the spellings of one command share an
        # entry rather than taking one each.
        memo_key = command_name.lower()

        try:
            return self._cacheable[memo_key]
        except KeyError:
            pass

        try:
            metadata = self.resolve(command_name)
        except ValueError:
            # More than one module prefix, which ``_split_command_name`` refuses.
            metadata = None

        cacheable = _is_client_side_cacheable(metadata)

        if len(self._cacheable) < _MEMO_MAX_ENTRIES:
            self._cacheable[memo_key] = cacheable

        return cacheable

    def is_replica_safe(self, command_name: str) -> bool:
        if not isinstance(command_name, str):
            return False

        memo_key = command_name.lower()

        try:
            return self._replica_safe[memo_key]
        except KeyError:
            pass

        try:
            metadata = self.resolve(command_name)
        except ValueError:
            metadata = None

        replica_safe = _is_replica_safe(metadata)

        if len(self._replica_safe) < _MEMO_MAX_ENTRIES:
            self._replica_safe[memo_key] = replica_safe

        return replica_safe

    def is_trackable_read(self, command_name: str) -> bool:
        if not isinstance(command_name, str):
            return False

        memo_key = command_name.lower()

        try:
            return self._trackable_read[memo_key]
        except KeyError:
            pass

        try:
            metadata = self.resolve(command_name)
        except ValueError:
            metadata = None

        trackable_read = _is_trackable_read(metadata)

        if len(self._trackable_read) < _MEMO_MAX_ENTRIES:
            self._trackable_read[memo_key] = trackable_read

        return trackable_read

    @abstractmethod
    def with_fallback(self, fallback: "MetadataResolver") -> "MetadataResolver":
        pass


class AsyncBaseMetadataResolver(AsyncMetadataResolver):
    """
    Async base class for metadata resolvers.

    Lookup is first-match-wins: a command the records do not carry falls through to the
    fallback resolver, and a chain that ends without a match resolves to None. Besides the
    whole record, each consumer gets the view it needs of the same resolved metadata:
    ``resolve_policies`` for cluster routing, ``is_cacheable`` for client-side caching.

    Both views are memoized under the lowercased command name, so every spelling of one
    command - ``GET`` as the command methods write it, ``get`` as the cluster client lowers it
    - shares a single entry instead of taking one each. The metadata a resolver serves is a
    snapshot taken when the resolver is built, so a command's view cannot change, and the memo
    keeps the record lookup, the projection and the walk down the fallback chain off the
    command execution path. Concurrent resolves of the same command may each compute it once;
    the memo is idempotent, so the duplicated work is harmless. The memos grow with the set of
    distinct commands a caller asks about, and are capped at ``_MEMO_MAX_ENTRIES`` so that a
    caller asking about unbounded many names - a name that resolves to nothing is memoized
    too - cannot grow them without bound.
    """

    def __init__(
        self,
        metadata: CommandMetadataRecordsCache,
        fallback: AsyncMetadataResolver | None = None,
    ) -> None:
        self._metadata = metadata
        self._fallback = fallback
        self._policies: dict[str, CommandPolicies | None] = {}
        self._cacheable: dict[str, bool] = {}
        self._replica_safe: dict[str, bool] = {
            cmd: False for cmd in _REPLICA_UNSAFE_COMMANDS
        }
        # No ``_REPLICA_UNSAFE_COMMANDS`` pre-seed: trackability is the readonly flag alone,
        # and TOUCH - the one command that seed excludes - is precisely the trackable read
        # ``optout`` most wants to exempt.
        self._trackable_read: dict[str, bool] = {}

    async def resolve(self, command_name: str) -> CommandMetadata | None:
        module, command = _split_command_name(command_name)

        commands = self._metadata.get(module)
        metadata = commands.get(command) if commands is not None else None

        if metadata is None:
            if self._fallback is not None:
                return await self._fallback.resolve(command_name)
            return None

        return metadata

    async def resolve_policies(self, command_name: str) -> CommandPolicies | None:
        # Memoized under the lowercased name, so the spellings of one command share an
        # entry rather than taking one each. ``resolve`` is still asked with the name as
        # given, so an unresolvable one is reported the way the caller spelled it.
        memo_key = command_name.lower()

        try:
            return self._policies[memo_key]
        except KeyError:
            pass

        metadata = await self.resolve(command_name)
        policies = _to_command_policies(metadata) if metadata is not None else None

        if len(self._policies) < _MEMO_MAX_ENTRIES:
            self._policies[memo_key] = policies

        return policies

    async def is_cacheable(self, command_name: str) -> bool:
        # A name the record tables cannot be keyed by is not a command this client can decide
        # on. Fail closed rather than raise into the command execution path, where a raw
        # command with such a name must still reach the server and come back with the
        # server's own error. ``execute_command`` accepts an arbitrary first argument, and a
        # non-str one - ``bytes``, which the request encoder accepts - reaches here
        # unchanged; it is refused rather than decoded, because deciding it would start
        # caching a command whose name this client never resolved.
        if not isinstance(command_name, str):
            return False

        # Memoized under the lowercased name, so the spellings of one command share an
        # entry rather than taking one each.
        memo_key = command_name.lower()

        try:
            return self._cacheable[memo_key]
        except KeyError:
            pass

        try:
            metadata = await self.resolve(command_name)
        except ValueError:
            # More than one module prefix, which ``_split_command_name`` refuses.
            metadata = None

        cacheable = _is_client_side_cacheable(metadata)

        if len(self._cacheable) < _MEMO_MAX_ENTRIES:
            self._cacheable[memo_key] = cacheable

        return cacheable

    async def is_replica_safe(self, command_name: str) -> bool:
        if not isinstance(command_name, str):
            return False

        memo_key = command_name.lower()

        try:
            return self._replica_safe[memo_key]
        except KeyError:
            pass

        try:
            metadata = await self.resolve(command_name)
        except ValueError:
            metadata = None

        replica_safe = _is_replica_safe(metadata)

        if len(self._replica_safe) < _MEMO_MAX_ENTRIES:
            self._replica_safe[memo_key] = replica_safe

        return replica_safe

    async def is_trackable_read(self, command_name: str) -> bool:
        if not isinstance(command_name, str):
            return False

        memo_key = command_name.lower()

        try:
            return self._trackable_read[memo_key]
        except KeyError:
            pass

        try:
            metadata = await self.resolve(command_name)
        except ValueError:
            metadata = None

        trackable_read = _is_trackable_read(metadata)

        if len(self._trackable_read) < _MEMO_MAX_ENTRIES:
            self._trackable_read[memo_key] = trackable_read

        return trackable_read

    @abstractmethod
    def with_fallback(
        self, fallback: "AsyncMetadataResolver"
    ) -> "AsyncMetadataResolver":
        pass


class DynamicMetadataResolver(BaseMetadataResolver):
    """
    Resolves metadata dynamically based on the provided metadata records
    (they can be extracted either from COMMAND output, or provided by user).

    Note: Takes the records rather than the parser that produced them, so that this and
    ``AsyncDynamicMetadataResolver`` accept the same argument: the async parser's
    ``get_commands_metadata_cache`` is a coroutine and cannot be awaited in a constructor, so records
    are the one shape both stacks can be built from. :func:`_load_commands_metadata_cache` turns a
    parser into records at the one place that owns one, ``DynamicPolicyResolver``.
    """

    def __init__(
        self,
        metadata_records: CommandMetadataRecordsCache,
        fallback: MetadataResolver | None = None,
    ) -> None:
        """
        Parameters:
            metadata_records (CommandMetadataRecordsCache): Command metadata records,
                keyed the way ``CommandMetadataRecordsCache`` documents. Keys are
                lowercased if they are not already, because that is how a resolved
                command name is looked up.
            fallback (Optional[MetadataResolver]): An optional resolver to be used when the
                primary metadata cannot handle a specific request.
        """
        super().__init__(_lowercase_keyed(metadata_records), fallback)

    def with_fallback(self, fallback: "MetadataResolver") -> "MetadataResolver":
        return DynamicMetadataResolver(self._metadata, fallback)


class StaticMetadataResolver(BaseMetadataResolver):
    """
    Resolves metadata from a static list, provided by the library,
    containing command metadata records.
    """

    def __init__(self, fallback: MetadataResolver | None = None) -> None:
        """
        Parameters:
            fallback (Optional[MetadataResolver]): An optional fallback metadata resolver
            used for resolving metadata if static metadata is inadequate.
        """
        super().__init__(_STATIC_COMMAND_METADATA, fallback)

    def with_fallback(self, fallback: "MetadataResolver") -> "MetadataResolver":
        return StaticMetadataResolver(fallback)


class AsyncDynamicMetadataResolver(AsyncBaseMetadataResolver):
    """
    Async version of DynamicMetadataResolver.

    Takes the records rather than the parser that produced them, because
    ``AsyncCommandsParser.get_commands_metadata_cache`` is a coroutine and cannot be awaited in a
    constructor.
    """

    def __init__(
        self,
        metadata_records: CommandMetadataRecordsCache,
        fallback: AsyncMetadataResolver | None = None,
    ) -> None:
        """
        Parameters:
            metadata_records (CommandMetadataRecordsCache): Command metadata records,
                keyed the way ``CommandMetadataRecordsCache`` documents. Keys are
                lowercased if they are not already, because that is how a resolved
                command name is looked up.
            fallback (Optional[AsyncMetadataResolver]): An optional resolver to be used when the
                primary metadata cannot handle a specific request.
        """
        super().__init__(_lowercase_keyed(metadata_records), fallback)

    def with_fallback(
        self, fallback: "AsyncMetadataResolver"
    ) -> "AsyncMetadataResolver":
        return AsyncDynamicMetadataResolver(self._metadata, fallback)


class AsyncStaticMetadataResolver(AsyncBaseMetadataResolver):
    """
    Async version of StaticMetadataResolver.
    """

    def __init__(self, fallback: AsyncMetadataResolver | None = None) -> None:
        """
        Parameters:
            fallback (Optional[AsyncMetadataResolver]): An optional fallback metadata resolver
            used for resolving metadata if static metadata is inadequate.
        """
        super().__init__(_STATIC_COMMAND_METADATA, fallback)

    def with_fallback(
        self, fallback: "AsyncMetadataResolver"
    ) -> "AsyncMetadataResolver":
        return AsyncStaticMetadataResolver(fallback)


def _to_command_policies(metadata: CommandMetadata) -> CommandPolicies | None:
    """
    Project a single metadata record down to the routing policies a policy resolver serves.

    Drops every field a ``CommandPolicies`` record does not carry, so the policies a
    command routes by stay derived from its metadata rather than tracked beside it.

    A record that withholds its routing policies projects to None, which a policy resolver
    reports the same way it reports a command it does not carry. Withholding therefore
    reproduces, exactly, what a command absent from the records resolves to: a policy-level
    fallback still gets its turn, and a resolver with none leaves the cluster client to resolve
    the target itself - which for a ``movablekeys`` command is the only path that finds its
    keys. The *metadata* chain is not re-walked, though: the record was found, so a metadata
    resolver behind this one is never asked, and cannot answer with the very policies the
    record withholds.
    """
    if metadata.request_policy is None or metadata.response_policy is None:
        return None

    return CommandPolicies(
        request_policy=metadata.request_policy,
        response_policy=metadata.response_policy,
    )


def _is_client_side_cacheable(metadata: CommandMetadata | None) -> bool:
    """
    Decide whether the reply of the command a record describes may be cached client-side.

    The one normative implementation of the eligibility rules, applied in the order they are
    specified. Every rule is a veto, so the verdict does not depend on the order; the order
    is kept to match the specification.

    Takes ``None`` - what an exhausted resolver chain resolves to - so the unknown-command
    case is decided here as well, rather than by every caller.

    An incomplete record is refused for the same reason an unknown command is: a ``COMMAND``
    reply that carries no tips cannot express ``nondeterministic_output`` or ``dont_cache``,
    so a record built from one does not prove the command is cacheable. Note that a resolver
    answers from the first record it finds, so an incomplete record decides the command even
    when a complete one sits behind it in the chain - an override table therefore belongs in
    front of the resolver that may serve incomplete records, not behind it.
    """
    if metadata is None or not metadata.has_complete_metadata:
        return False

    # A negative override: the server states the reply must not be cached, which decides on
    # its own even when every positive rule matches.
    if metadata.is_dont_cache:
        return False

    if not metadata.is_readonly:
        return False

    if metadata.is_blocking:
        return False

    if not metadata.has_key_argument:
        return False

    if metadata.has_nondeterministic_output:
        return False

    if metadata.is_script_runner:
        return False

    return True


def _is_replica_safe(metadata: CommandMetadata | None) -> bool:
    """
    Decide whether a command is safe to execute on a replica based on the readonly flag.

    Takes ``None`` - what an exhausted resolver chain resolves to - so the
    unknown-command case is decided here as well.
    """
    if metadata is None:
        return False

    return metadata.is_readonly


def _is_trackable_read(metadata: CommandMetadata | None) -> bool:
    """
    Decide whether the server would remember the keys of the command a record describes.

    The ``readonly`` command flag alone, because that plus the key names of the invocation is
    the whole gate the server applies before it records a read against a tracking client. The
    negative signals that make a command ineligible to store do not enter it: ``XPENDING`` is
    tipped ``nondeterministic_output`` and ``TOUCH`` is tipped ``dont_cache``, and the server
    tracks both.

    Deliberately does not require ``has_complete_metadata``, unlike
    :func:`_is_client_side_cacheable`: the readonly flag comes from the command flags, which
    every ``COMMAND`` reply carries, and only the tips can be missing.

    Takes ``None`` - what an exhausted resolver chain resolves to - so the unknown-command
    case is decided here as well.
    """
    if metadata is None:
        return False

    return metadata.is_readonly


def _build_commands_metadata_cache_from_policies(
    policy_records: PolicyRecords,
) -> CommandMetadataRecordsCache:
    """
    Build the metadata records cache from policy records.

    Unlike ``_build_commands_metadata_cache``, which builds the cache from a raw ``COMMAND``
    reply, this lifts the narrower 7.1.0 routing view into the same shape.

    Only the routing policies of each command are known, so every other field keeps its
    fail-closed default. This backs the ``PolicyRecords`` arguments of the policy resolvers:
    a resolver built from policy records serves exactly the policies it was given, and
    reports no command as client-side-cacheable, which is the conservative answer for
    metadata that was never supplied.

    Keys are lowercased on the way through - the records are rebuilt here anyway - because
    that is how a resolved command name is looked up.
    """
    return {
        module_name.lower(): {
            command_name.lower(): CommandMetadata(
                request_policy=policies.request_policy,
                response_policy=policies.response_policy,
            )
            for command_name, policies in commands.items()
        }
        for module_name, commands in policy_records.items()
    }


def _load_commands_metadata_cache(
    commands_parser: object,
) -> CommandMetadataRecordsCache:
    """
    Load the metadata records cache of a ``COMMAND`` parser.

    ``get_commands_metadata_cache`` is what a parser serves, and is what this loads.

    The ``get_command_policies`` branch is a backwards-compatibility shim, not a second
    supported shape. ``DynamicPolicyResolver`` called nothing but that method in 7.1.0, so code
    that passed an object duck-typing it would otherwise break at construction; the branch keeps
    that code working and nothing more. It is deliberately not advertised as an extension point:
    ``CommandsParser`` lives in the private ``redis._parsers`` package, and the supported way to
    decide routing yourself is to implement the public ``PolicyResolver`` ABC and pass it as the
    ``policy_resolver`` of a cluster client. Policy records also carry no cacheability metadata,
    so everything a parser reaching this branch serves reports as non-cacheable.

    The async stack needs no counterpart: ``AsyncDynamicMetadataResolver`` takes records rather
    than a parser, and ``AsyncDynamicPolicyResolver`` still accepts ``policy_records`` directly.

    Raises:
        TypeError: If the parser serves neither method.
    """
    get_metadata = getattr(commands_parser, "get_commands_metadata_cache", None)
    if get_metadata is not None:
        return get_metadata()

    get_policies = getattr(commands_parser, "get_command_policies", None)
    if get_policies is None:
        raise TypeError(
            f"{type(commands_parser).__name__} serves neither get_commands_metadata_cache() nor "
            "get_command_policies(); a commands parser must serve one of them"
        )

    return _build_commands_metadata_cache_from_policies(get_policies())


# The records the cluster clients fall back to when the policy resolver does not know a
# command, which is the common case: the default resolver is backed by the static table
# below, so every write and every read outside it falls through on every execution. Reused
# rather than constructed there, both to keep an allocation off that path and because a
# frozen record is safe to share.
#
# Only the routing policies are known for a command the client had to fall back on, so
# every other field is recorded at its fail-closed value and none of these reports as
# cacheable. Spelled out rather than left to the dataclass defaults, for the reason given
# on the shared table shapes below.
_DEFAULT_KEYLESS_METADATA = CommandMetadata(
    request_policy=RequestPolicy.DEFAULT_KEYLESS,
    response_policy=ResponsePolicy.DEFAULT_KEYLESS,
    is_readonly=False,
    is_blocking=False,
    has_key_argument=False,
    has_nondeterministic_output=False,
    is_script_runner=False,
    is_dont_cache=False,
    has_complete_metadata=False,
)
_DEFAULT_KEYED_METADATA = CommandMetadata(
    request_policy=RequestPolicy.DEFAULT_KEYED,
    response_policy=ResponsePolicy.DEFAULT_KEYED,
    is_readonly=False,
    is_blocking=False,
    has_key_argument=False,
    has_nondeterministic_output=False,
    is_script_runner=False,
    is_dont_cache=False,
    has_complete_metadata=False,
)

# Same, for the node-flag fallback, which resolves a request policy and leaves the response
# policy at its keyless default.
_METADATA_BY_REQUEST_POLICY: Mapping[RequestPolicy, CommandMetadata] = MappingProxyType(
    {
        policy: CommandMetadata(
            request_policy=policy,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
            is_readonly=False,
            is_blocking=False,
            has_key_argument=False,
            has_nondeterministic_output=False,
            is_script_runner=False,
            is_dont_cache=False,
            has_complete_metadata=False,
        )
        for policy in RequestPolicy
    }
)


# Every record in this module spells out all nine fields, including the ones that happen to
# equal the dataclass default. The nine are not interchangeable: seven of them
# (``is_readonly``, ``is_blocking``, ``has_key_argument``, ``has_nondeterministic_output``,
# ``is_script_runner``, ``is_dont_cache``, ``has_complete_metadata``) are the inputs to
# ``_is_client_side_cacheable``, so a shape that leaves one implicit lets a command added
# for an unrelated reason - replica-safe read routing, say, which needs only ``is_readonly``
# - become client-side cacheable as a side effect, with nothing in the diff saying so.
# Listing every field makes each new entry state its own cacheability.
#
# Shapes derived with ``dataclasses.replace`` below inherit from a fully explicit base, so
# they name only the field that differs and still leave nothing to a default.
#
# Record shared by every command the client-side cache may serve: readonly, takes a key
# name argument, and reports nothing that forbids caching. Spelled out once because the
# vast majority of the table below is one of these shapes.
_CACHEABLE_KEYED = CommandMetadata(
    request_policy=RequestPolicy.DEFAULT_KEYED,
    response_policy=ResponsePolicy.DEFAULT_KEYED,
    is_readonly=True,
    is_blocking=False,
    has_key_argument=True,
    has_nondeterministic_output=False,
    is_script_runner=False,
    is_dont_cache=False,
    has_complete_metadata=True,
)

# Same, for the ``movablekeys`` commands, with the routing policies withheld. Their keys are
# only discoverable through key specs, so ``first_key_pos`` is 0 and the derived policies come
# out keyless even though the command genuinely takes keys. This divergence is exactly why
# ``has_key_argument`` is recorded separately from ``request_policy``.
#
# Recording those derived policies here would route the command to an arbitrary node, so they
# are withheld: a policy resolver then reports the command as unresolved and the cluster client
# resolves the target itself through ``determine_slot``, which asks the server for the keys with
# ``COMMAND GETKEYS``. That is the only path that finds the keys of a ``movablekeys`` command,
# and it is what the routing fallback did before this table backed the static resolver.
# TODO(pslavova): record the real request/response policies once routing derives keyed/keyless
# from the key specs (``has_key_argument``) rather than from ``first_key_pos``.
_CACHEABLE_MOVABLE_KEYS = CommandMetadata(
    request_policy=None,
    response_policy=None,
    is_readonly=True,
    is_blocking=False,
    has_key_argument=True,
    has_nondeterministic_output=False,
    is_script_runner=False,
    is_dont_cache=False,
    has_complete_metadata=True,
)

# Same, for a command that also carries the ``blocking`` flag, which makes it ineligible:
# its reply is what the caller waited for rather than a snapshot the cache may re-serve.
# XREAD is the only readonly, keyed command the flag excludes. The withheld routing policies
# are inherited: XREAD is ``movablekeys`` too, so it must be routed by its resolved keys.
_BLOCKING_MOVABLE_KEYS = replace(_CACHEABLE_MOVABLE_KEYS, is_blocking=True)

# Same, for the read-only script runners - EVAL_RO, EVALSHA_RO, FCALL_RO. The
# ``script_runner`` flag makes them ineligible: the reply is whatever the script computed,
# which the client cannot tie to the keys the script happened to touch. They are
# ``movablekeys`` too - their keys are counted by ``numkeys`` - so the withheld routing
# policies are inherited for the same reason.
#
# Recorded rather than left absent because the flag only reaches the client from Redis 8.10
# on: on 7.4.x-8.8.x all three report readonly and keyed with no flag that excludes them,
# i.e. cacheable, so a resolver reading a live COMMAND reply from one of those servers would
# admit them. This record states what the command is, independently of what a given server
# version reports. Removable once the minimum CSC-supported server reports ``script_runner``.
_SCRIPT_RUNNER_MOVABLE_KEYS = replace(_CACHEABLE_MOVABLE_KEYS, is_script_runner=True)

# Readonly but keyless, so not cacheable: the command observes keyspace state without
# taking a key name argument. Note that this is what makes a search or aggregation query
# uncacheable - being a module command is not itself a reason to exclude one.
_READONLY_KEYLESS = CommandMetadata(
    request_policy=RequestPolicy.DEFAULT_KEYLESS,
    response_policy=ResponsePolicy.DEFAULT_KEYLESS,
    is_readonly=True,
    is_blocking=False,
    has_key_argument=False,
    has_nondeterministic_output=False,
    is_script_runner=False,
    is_dont_cache=False,
    has_complete_metadata=True,
)

# Readonly and keyless with routing policies withheld, so the cluster client keeps
# resolving its target node via COMMAND_FLAGS (e.g. DEFAULT_NODE for DBSIZE/KEYS/RANDOMKEY
# or PRIMARIES for SCAN).
_READONLY_KEYLESS_WITHHELD_ROUTING = CommandMetadata(
    request_policy=None,
    response_policy=None,
    is_readonly=True,
    is_blocking=False,
    has_key_argument=False,
    has_nondeterministic_output=False,
    is_script_runner=False,
    is_dont_cache=False,
    has_complete_metadata=True,
)

# Readonly and keyed, but tipped nondeterministic_output, which makes it ineligible
# for client-side caching.
_NONDETERMINISTIC_KEYED = replace(_CACHEABLE_KEYED, has_nondeterministic_output=True)

# Same tip on the keyless reads that withhold their routing. Carries no cacheability
# consequence of its own - a keyless command is already ineligible for want of a key
# argument - but the table records what the server reports, and both RANDOMKEY and SCAN are
# tipped ``nondeterministic_output``: one returns an arbitrary key, the other an arbitrary
# page of the keyspace.
_NONDETERMINISTIC_KEYLESS_WITHHELD_ROUTING = replace(
    _READONLY_KEYLESS_WITHHELD_ROUTING, has_nondeterministic_output=True
)

# Write commands. Rule 1 excludes them, so nothing else about them matters to the cache.
_WRITE_KEYLESS = CommandMetadata(
    request_policy=RequestPolicy.DEFAULT_KEYLESS,
    response_policy=ResponsePolicy.DEFAULT_KEYLESS,
    is_readonly=False,
    is_blocking=False,
    has_key_argument=False,
    has_nondeterministic_output=False,
    is_script_runner=False,
    is_dont_cache=False,
    has_complete_metadata=True,
)

_WRITE_KEYLESS_WITHHELD_ROUTING = CommandMetadata(
    request_policy=None,
    response_policy=None,
    is_readonly=False,
    is_blocking=False,
    has_key_argument=False,
    has_nondeterministic_output=False,
    is_script_runner=False,
    is_dont_cache=False,
    has_complete_metadata=True,
)

_WRITE_KEYED = CommandMetadata(
    request_policy=RequestPolicy.DEFAULT_KEYED,
    response_policy=ResponsePolicy.DEFAULT_KEYED,
    is_readonly=False,
    is_blocking=False,
    has_key_argument=True,
    has_nondeterministic_output=False,
    is_script_runner=False,
    is_dont_cache=False,
    has_complete_metadata=True,
)

# The same three shapes for the commands the server tips ``dont_cache``. The search module
# tips nearly its whole surface that way, including the write commands, where the marker is
# redundant - rule 1 already excludes them. It is recorded anyway so the table keeps
# reporting what the server reports.
_DONT_CACHE_READONLY_KEYLESS = replace(_READONLY_KEYLESS, is_dont_cache=True)
_DONT_CACHE_WRITE_KEYLESS = replace(_WRITE_KEYLESS, is_dont_cache=True)
_DONT_CACHE_WRITE_KEYED = replace(_WRITE_KEYED, is_dont_cache=True)


# =============================================================================
# Static command metadata table
# =============================================================================
# This table is seeded from old ``CacheConfig.DEFAULT_ALLOW_LIST`` and ``redis.commands.policies.STATIC_POLICIES``.
#
# Every entry below was validated against a live ``COMMAND`` reply from Redis 8.10.0 with
# search, timeseries, ReJSON, bf and vectorset loaded (the ``redislabs/client-libs-test``
# stack image), with the core entries also compared against 7.4.2 to ensure the minimum
# supported version does not disagree on the fields that decide cacheability. The
# exceptions are the ``bless`` container, which first ships in 8.11 and was validated against
# that release's ``COMMAND INFO`` reply, and the set member expiration reads (``sexpiretime``,
# ``spexpiretime``, ``sttl``, ``spttl``), which first ship in 8.12 and were validated against
# the ``COMMAND INFO`` reply of an 8.11.240 build.
#
# The table is not exhaustive: on 8.10.0 the server reports 127 cacheable commands and this
# covers 80 of them. The count is version-qualified because it moves between releases - a
# later server reports more. The uncovered ones are whole module surfaces the CSC allow-list
# never carried - ``bf.*``, ``cf.*``, ``cms.*``, ``topk.*``, ``tdigest.*``, the vectorset
# reads - plus core reads such as ``pfcount``. A command absent from the table fails closed,
# so the uncovered commands are simply treated as not cacheable.
#
# Every command on the default allow-list of the 7.1.0 client-side cache is present.
# So are all of their module counterparts, plus a selection of write commands.
# Write commands carry the metadata that makes them *ineligible*, so the reason
# is documented in one place rather than inferred from an absence - and, because
# a resolver answers from the first record it finds, so that a live resolver
# chained behind this one cannot re-admit them. ``xpending``, ``ts.info`` and
# ``xread`` are on today's ``CacheConfig.DEFAULT_ALLOW_LIST``, and the server
# confirms all three are defects in that list: ``xpending`` is tipped
# ``nondeterministic_output``, ``ts.info`` ``dont_cache``, and ``xread`` carries
# the ``blocking`` command flag.
#
# Five entries diverge from the live reply, each for a reason spelled out where it occurs:
# ``exists`` and ``mget`` stand in the keyed defaults for the unimplemented ``multi_shard``
# tips, ``ft.cursor`` is SPECIAL, a client-side decision the server does not tip, ``touch`` is
# recorded ``dont_cache`` where the server reports it cacheable, and ``vrandmember`` is
# recorded ``nondeterministic_output`` where the server tips it nothing. The ``movablekeys``
# reads - ``eval_ro``, ``evalsha_ro``, ``fcall_ro``, ``sdiffcard``, ``sintercard``,
# ``sunioncard``, ``xread``, ``zdiff``, ``zinter``, ``zintercard`` and ``zunion`` - plus
# ``bless scan``, ``command``, ``dbsize``, ``keys``, ``randomkey``, ``scan``, ``touch`` and
# ``vrandmember`` withhold their routing policies entirely, so the cluster client keeps
# resolving their targets itself. Their cacheability inputs are unaffected.
_STATIC_COMMAND_METADATA: CommandMetadataRecordsCache = MappingProxyType(
    {
        "core": MappingProxyType(
            {
                "bitcount": _CACHEABLE_KEYED,
                "bitfield_ro": _CACHEABLE_KEYED,
                "bitpos": _CACHEABLE_KEYED,
                # The BLESS container, keyed by subcommand the way ``execute_command``
                # receives it. Validated against the 8.11 ``COMMAND INFO`` reply, the first
                # release that reports it.
                "bless clear": _WRITE_KEYED,
                # Not cacheable and not replica-eligible: the server flags BLESS GET ``fast``
                # only - neither ``readonly`` nor ``write`` - and ``is_readonly`` records the
                # command flag, not the ``RO`` key spec, so it fails closed on both counts.
                # Recorded with the write shape for the same reason ``command`` is below.
                "bless get": _WRITE_KEYED,
                # Keyless, tipped nondeterministic_output and request_policy:special /
                # response_policy:special, exactly like SCAN. Routing policies are withheld
                # for the same reason as SCAN: the cluster client routes BLESS SCAN to all
                # primary nodes (PRIMARIES) through its COMMAND_FLAGS entry and merges the
                # per-node cursors. Unlike SCAN the server reports no flags at all, so it is
                # not readonly.
                "bless scan": CommandMetadata(
                    request_policy=None,
                    response_policy=None,
                    is_readonly=False,
                    is_blocking=False,
                    has_key_argument=False,
                    has_nondeterministic_output=True,
                    is_script_runner=False,
                    is_dont_cache=False,
                    has_complete_metadata=True,
                ),
                "bless set": _WRITE_KEYED,
                # From STATIC_POLICIES. COMMAND is flagged loading/stale, not readonly,
                # and takes no keys. Routing policies are withheld so the cluster client preserves
                # the 2-word COMMAND COUNT, COMMAND LIST, COMMAND GETKEYS default-node flags.
                "command": _WRITE_KEYLESS_WITHHELD_ROUTING,
                "dbsize": _READONLY_KEYLESS_WITHHELD_ROUTING,
                "digest": _CACHEABLE_KEYED,
                "dump": _NONDETERMINISTIC_KEYED,
                # Not cacheable: script_runner. See the shape's note for why all three are
                # recorded rather than left to the server to report.
                "eval_ro": _SCRIPT_RUNNER_MOVABLE_KEYS,
                "evalsha_ro": _SCRIPT_RUNNER_MOVABLE_KEYS,
                # The server tips EXISTS request_policy:multi_shard and
                # response_policy:agg_sum, but ``RequestPolicy.MULTI_SHARD`` is not
                # implemented in either client stack: ``_split_multi_shard_command``
                # returns per-key command descriptors where every caller of
                # ``_determine_nodes`` expects ``ClusterNode``, and the async client has no
                # MULTI_SHARD entry in ``_policies_callback_mapping`` at all. Recording the
                # real tips here therefore breaks the command, so the keyed defaults stand
                # in - which is what the routing fallback resolved to before this table
                # backed the static resolver.
                # TODO(pslavova): record the real tips once MULTI_SHARD is implemented
                # across both stacks (main path and pipeline).
                "exists": _CACHEABLE_KEYED,
                "expiretime": _CACHEABLE_KEYED,
                "fcall_ro": _SCRIPT_RUNNER_MOVABLE_KEYS,
                "geodist": _CACHEABLE_KEYED,
                "geohash": _CACHEABLE_KEYED,
                "geopos": _CACHEABLE_KEYED,
                "georadius_ro": _CACHEABLE_KEYED,
                "georadiusbymember_ro": _CACHEABLE_KEYED,
                "geosearch": _CACHEABLE_KEYED,
                "get": _CACHEABLE_KEYED,
                "getbit": _CACHEABLE_KEYED,
                "getrange": _CACHEABLE_KEYED,
                "hexists": _CACHEABLE_KEYED,
                "hexpiretime": _CACHEABLE_KEYED,
                "hget": _CACHEABLE_KEYED,
                # HGETALL, HKEYS, HVALS, SDIFF, SINTER, SMEMBERS and SUNION all carry
                # nondeterministic_output_order, which is a different tip from
                # nondeterministic_output and does not prevent caching. Matching tips by
                # prefix would silently drop all seven.
                "hgetall": _CACHEABLE_KEYED,
                "hkeys": _CACHEABLE_KEYED,
                "hlen": _CACHEABLE_KEYED,
                "hmget": _CACHEABLE_KEYED,
                "hpexpiretime": _CACHEABLE_KEYED,
                "hpttl": _NONDETERMINISTIC_KEYED,
                "hrandfield": _NONDETERMINISTIC_KEYED,
                "hscan": _NONDETERMINISTIC_KEYED,
                "hstrlen": _CACHEABLE_KEYED,
                "httl": _NONDETERMINISTIC_KEYED,
                "hvals": _CACHEABLE_KEYED,
                "keys": _READONLY_KEYLESS_WITHHELD_ROUTING,
                "lcs": _CACHEABLE_KEYED,
                "lindex": _CACHEABLE_KEYED,
                "llen": _CACHEABLE_KEYED,
                "lpos": _CACHEABLE_KEYED,
                "lrange": _CACHEABLE_KEYED,
                # Tipped request_policy:multi_shard by the server, withheld for the reason
                # spelled out on ``exists`` above.
                # TODO: record the real tip once MULTI_SHARD is implemented.
                "mget": _CACHEABLE_KEYED,
                "pexpiretime": _CACHEABLE_KEYED,
                "pttl": _NONDETERMINISTIC_KEYED,
                "randomkey": _NONDETERMINISTIC_KEYLESS_WITHHELD_ROUTING,
                # Readonly and keyless. Routing policies are withheld so the cluster client keeps
                # routing SCAN to all primary nodes (PRIMARIES) rather than a single random node.
                "scan": _NONDETERMINISTIC_KEYLESS_WITHHELD_ROUTING,
                "scard": _CACHEABLE_KEYED,
                "sdiff": _CACHEABLE_KEYED,
                "sdiffcard": _CACHEABLE_MOVABLE_KEYS,
                "sexpiretime": _CACHEABLE_KEYED,
                "sinter": _CACHEABLE_KEYED,
                "sintercard": _CACHEABLE_MOVABLE_KEYS,
                "sismember": _CACHEABLE_KEYED,
                "smembers": _CACHEABLE_KEYED,
                "smismember": _CACHEABLE_KEYED,
                # Unreachable today: ``CoreCommands.sort_ro`` delegates to ``sort()``, which
                # sends SORT rather than SORT_RO, so nothing resolves this entry. Recorded
                # because SORT_RO is genuinely cacheable, and the old allow-list carried it
                # just as ineffectively.
                # TODO: drop this note once the command method sends its own name.
                "sort_ro": _CACHEABLE_KEYED,
                "spexpiretime": _CACHEABLE_KEYED,
                "spttl": _NONDETERMINISTIC_KEYED,
                "srandmember": _NONDETERMINISTIC_KEYED,
                "sscan": _NONDETERMINISTIC_KEYED,
                "strlen": _CACHEABLE_KEYED,
                "sttl": _NONDETERMINISTIC_KEYED,
                "substr": _CACHEABLE_KEYED,
                "sunion": _CACHEABLE_KEYED,
                "sunioncard": _CACHEABLE_MOVABLE_KEYS,
                # Not cacheable, and the one entry in this table that records a client-side
                # judgement instead of what the server reports: TOUCH has a server-side
                # effect - it refreshes each key's idle time, which is what LRU/LFU
                # eviction and OBJECT IDLETIME read - so it must reach the server on every
                # call. No server flag or tip expresses that; measured on 8.10.0 the server
                # reports it readonly and keyed with no ``dont_cache`` tip, i.e. cacheable.
                # Recorded here as ``dont_cache`` so the reason is stated once rather than
                # left to every resolver in a chain. Removable once the server tips it
                # ``dont_cache``.
                #
                # Routing policies are withheld, not defaulted: the server tips TOUCH
                # request_policy:multi_shard / response_policy:agg_sum, and MULTI_SHARD is
                # unimplemented for the reason spelled out on ``exists`` above. Withholding
                # keeps the cluster client resolving the target itself, exactly as it does
                # today for a command this table does not carry.
                "touch": CommandMetadata(
                    request_policy=None,
                    response_policy=None,
                    is_readonly=True,
                    is_blocking=False,
                    has_key_argument=True,
                    has_nondeterministic_output=False,
                    is_script_runner=False,
                    is_dont_cache=True,
                    has_complete_metadata=True,
                ),
                "ttl": _NONDETERMINISTIC_KEYED,
                "type": _CACHEABLE_KEYED,
                # Not cacheable: its reply is a random sample of the vector set, so re-serving
                # it from a cache would stop it varying. Every core random read - SRANDMEMBER,
                # ZRANDMEMBER, HRANDFIELD, RANDOMKEY - is tipped ``nondeterministic_output``
                # by the server; measured on 8.10.0 VRANDMEMBER reports no tips at all, so the
                # algorithm would admit it. Recorded as nondeterministic because that is what
                # it is, and it is the one other command node-redis hard-codes ineligible
                # besides TOUCH. Removable once the server tips it like its core siblings.
                #
                # Routing policies are withheld for the same reason as ``touch`` above: the
                # record exists for its cacheability inputs, and must not start routing a
                # command the cluster client resolves for itself today.
                "vrandmember": CommandMetadata(
                    request_policy=None,
                    response_policy=None,
                    is_readonly=True,
                    is_blocking=False,
                    has_key_argument=True,
                    has_nondeterministic_output=True,
                    is_script_runner=False,
                    is_dont_cache=False,
                    has_complete_metadata=True,
                ),
                "xlen": _CACHEABLE_KEYED,
                # Not cacheable: nondeterministic_output. It is on today's allow-list,
                # which is a defect in that list rather than a reason to keep caching it.
                "xpending": _NONDETERMINISTIC_KEYED,
                "xrange": _CACHEABLE_KEYED,
                "xread": _BLOCKING_MOVABLE_KEYS,
                "xrevrange": _CACHEABLE_KEYED,
                "zcard": _CACHEABLE_KEYED,
                "zcount": _CACHEABLE_KEYED,
                "zdiff": _CACHEABLE_MOVABLE_KEYS,
                "zinter": _CACHEABLE_MOVABLE_KEYS,
                "zintercard": _CACHEABLE_MOVABLE_KEYS,
                "zlexcount": _CACHEABLE_KEYED,
                "zmscore": _CACHEABLE_KEYED,
                "zrange": _CACHEABLE_KEYED,
                "zrangebylex": _CACHEABLE_KEYED,
                "zrangebyscore": _CACHEABLE_KEYED,
                "zrank": _CACHEABLE_KEYED,
                "zrandmember": _NONDETERMINISTIC_KEYED,
                "zrevrange": _CACHEABLE_KEYED,
                "zrevrangebylex": _CACHEABLE_KEYED,
                "zrevrangebyscore": _CACHEABLE_KEYED,
                "zrevrank": _CACHEABLE_KEYED,
                "zscan": _NONDETERMINISTIC_KEYED,
                "zscore": _CACHEABLE_KEYED,
                "zunion": _CACHEABLE_MOVABLE_KEYS,
            }
        ),
        "json": MappingProxyType(
            {
                "arrindex": _CACHEABLE_KEYED,
                "arrlen": _CACHEABLE_KEYED,
                "get": _CACHEABLE_KEYED,
                "mget": _CACHEABLE_KEYED,
                "objkeys": _CACHEABLE_KEYED,
                "objlen": _CACHEABLE_KEYED,
                "resp": _CACHEABLE_KEYED,
                "strlen": _CACHEABLE_KEYED,
                "type": _CACHEABLE_KEYED,
            }
        ),
        "ts": MappingProxyType(
            {
                "get": _CACHEABLE_KEYED,
                # Not cacheable: the server reports dont_cache for TS.INFO, contradicting
                # today's allow-list, which caches it.
                "info": CommandMetadata(
                    request_policy=RequestPolicy.DEFAULT_KEYED,
                    response_policy=ResponsePolicy.DEFAULT_KEYED,
                    is_readonly=True,
                    is_blocking=False,
                    has_key_argument=True,
                    has_nondeterministic_output=False,
                    is_script_runner=False,
                    is_dont_cache=True,
                    has_complete_metadata=True,
                ),
                "range": _CACHEABLE_KEYED,
                "revrange": _CACHEABLE_KEYED,
            }
        ),
        # From STATIC_POLICIES. Only the suggestion-dictionary commands take a key name
        # argument, so FT.SUGGET and FT.SUGLEN are the only cacheable entries here -
        # FT.SEARCH and FT.AGGREGATE are readonly but keyless.
        #
        # The search module tips almost its whole surface ``dont_cache``. The exceptions
        # are FT.CURSOR, FT.DROP, FT.SUGGET and FT.SUGLEN, which report no tips at all -
        # which is what keeps the two suggestion-dictionary reads cacheable.
        "ft": MappingProxyType(
            {
                "aggregate": _DONT_CACHE_READONLY_KEYLESS,
                "aliasadd": _DONT_CACHE_WRITE_KEYLESS,
                "aliasdel": _DONT_CACHE_WRITE_KEYLESS,
                "aliaslist": _DONT_CACHE_READONLY_KEYLESS,
                "aliasupdate": _DONT_CACHE_WRITE_KEYLESS,
                "alter": _DONT_CACHE_WRITE_KEYLESS,
                "create": _DONT_CACHE_WRITE_KEYLESS,
                # SPECIAL is a client-side routing decision, not a server tip: FT.CURSOR
                # must reach the node that ran the FT.AGGREGATE it continues, which
                # ``get_special_nodes`` resolves. The server reports no tips for it, so a
                # generated table would say DEFAULT_KEYLESS and break cursor routing.
                "cursor": CommandMetadata(
                    request_policy=RequestPolicy.SPECIAL,
                    response_policy=ResponsePolicy.DEFAULT_KEYLESS,
                    is_readonly=True,
                    is_blocking=False,
                    has_key_argument=False,
                    has_nondeterministic_output=False,
                    is_script_runner=False,
                    is_dont_cache=False,
                    has_complete_metadata=True,
                ),
                "dictadd": _DONT_CACHE_WRITE_KEYLESS,
                "dictdel": _DONT_CACHE_WRITE_KEYLESS,
                "dictdump": _DONT_CACHE_READONLY_KEYLESS,
                "drop": _WRITE_KEYLESS,
                "dropindex": _DONT_CACHE_WRITE_KEYLESS,
                "explain": _DONT_CACHE_READONLY_KEYLESS,
                "explaincli": _DONT_CACHE_READONLY_KEYLESS,
                "info": _DONT_CACHE_READONLY_KEYLESS,
                "profile": _DONT_CACHE_READONLY_KEYLESS,
                "search": _DONT_CACHE_READONLY_KEYLESS,
                "spellcheck": _DONT_CACHE_READONLY_KEYLESS,
                "sugadd": _DONT_CACHE_WRITE_KEYED,
                "sugdel": _DONT_CACHE_WRITE_KEYED,
                "sugget": _CACHEABLE_KEYED,
                "suglen": _CACHEABLE_KEYED,
                "syndump": _DONT_CACHE_READONLY_KEYLESS,
                "synupdate": _DONT_CACHE_WRITE_KEYLESS,
                "tagvals": _DONT_CACHE_READONLY_KEYLESS,
            }
        ),
    }
)
