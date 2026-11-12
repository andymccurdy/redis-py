from abc import ABC, abstractmethod
from typing import Optional

from redis._parsers.commands import CommandsParser
from redis.commands.metadata import (
    AsyncDynamicMetadataResolver,
    AsyncMetadataResolver,
    AsyncStaticMetadataResolver,
    CommandPolicies,
    DynamicMetadataResolver,
    MetadataResolver,
    PolicyRecords,
    RequestPolicy,
    ResponsePolicy,
    StaticMetadataResolver,
    _build_commands_metadata_cache_from_policies,
    _load_commands_metadata_cache,
)
from redis.utils import warn_deprecated

# ``STATIC_POLICIES`` is named here because it is served by the module ``__getattr__`` below
# rather than bound in the module namespace, and a wildcard import only reaches a lazy
# attribute through ``__all__``. The metadata resolvers this module imports are deliberately
# left out: their home is ``redis.commands.metadata``.
__all__ = [
    "AsyncBasePolicyResolver",
    "AsyncDynamicPolicyResolver",
    "AsyncPolicyResolver",
    "AsyncStaticPolicyResolver",
    "BasePolicyResolver",
    "CommandPolicies",
    "CommandsParser",
    "DynamicPolicyResolver",
    "PolicyRecords",
    "PolicyResolver",
    "RequestPolicy",
    "ResponsePolicy",
    "STATIC_POLICIES",
    "StaticPolicyResolver",
]

# =====================================================================================
# DEPRECATED - DO NOT USE, DO NOT EDIT, DO NOT ADD TO.
#
# Nothing in this library reads this table. It is a frozen verbatim copy of the table that
# shipped in 7.1.0, kept only so that an external caller importing ``STATIC_POLICIES`` keeps
# working, and it will be removed in a future release. It is bound to a private name and
# served through the module ``__getattr__`` below, so that reading it warns.
#
# It is deliberately NOT derived from ``redis.commands.metadata._STATIC_COMMAND_METADATA`` and
# NOT kept in sync with it: the metadata table is the single source of truth, and
# ``StaticPolicyResolver`` resolves it through ``StaticMetadataResolver``, projecting one record
# at a time. Deriving this table instead would build ~100 throwaway objects on every import of
# ``redis`` for a table no code path consumes, and would silently change what an existing caller
# reads. So expect the two to disagree: this one answers "what did 7.1.0 route by", the metadata
# table answers "what does this client route by".
#
# To change routing, edit ``_STATIC_COMMAND_METADATA``. Nothing here.
# =====================================================================================
#
# Declared without a value so that linters and type checkers see the name that ``__all__``
# exports while the module ``__getattr__`` still serves it: an annotation alone binds nothing
# at runtime, whereas assigning here would bypass the deprecation warning.
STATIC_POLICIES: PolicyRecords

_DEPRECATED_STATIC_POLICIES: PolicyRecords = {
    "ft": {
        "explaincli": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "suglen": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYED,
            response_policy=ResponsePolicy.DEFAULT_KEYED,
        ),
        "profile": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "dropindex": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "aliasupdate": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "alter": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "aggregate": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "syndump": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "create": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "explain": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "sugget": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYED,
            response_policy=ResponsePolicy.DEFAULT_KEYED,
        ),
        "dictdel": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "aliasadd": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "dictadd": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "synupdate": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "drop": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "info": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "sugadd": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYED,
            response_policy=ResponsePolicy.DEFAULT_KEYED,
        ),
        "dictdump": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "cursor": CommandPolicies(
            request_policy=RequestPolicy.SPECIAL,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "search": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "tagvals": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "aliasdel": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "aliaslist": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
        "sugdel": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYED,
            response_policy=ResponsePolicy.DEFAULT_KEYED,
        ),
        "spellcheck": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
    },
    "core": {
        "command": CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        ),
    },
}


class PolicyResolver(ABC):
    @abstractmethod
    def resolve(self, command_name: str) -> Optional[CommandPolicies]:
        """
        Resolves the command name and determines the associated command policies.

        Args:
            command_name: The name of the command to resolve.

        Returns:
            CommandPolicies: The policies associated with the specified command.
        """
        pass

    @abstractmethod
    def with_fallback(self, fallback: "PolicyResolver") -> "PolicyResolver":
        """
        Factory method to instantiate a policy resolver with a fallback resolver.

        Args:
            fallback: Fallback resolver

        Returns:
            PolicyResolver: Returns a new policy resolver with the specified fallback resolver.
        """
        pass


class AsyncPolicyResolver(ABC):
    @abstractmethod
    async def resolve(self, command_name: str) -> Optional[CommandPolicies]:
        """
        Resolves the command name and determines the associated command policies.

        Args:
            command_name: The name of the command to resolve.

        Returns:
            CommandPolicies: The policies associated with the specified command.
        """
        pass

    @abstractmethod
    def with_fallback(self, fallback: "AsyncPolicyResolver") -> "AsyncPolicyResolver":
        """
        Factory method to instantiate an async policy resolver with a fallback resolver.

        Args:
            fallback: Fallback resolver

        Returns:
            AsyncPolicyResolver: Returns a new policy resolver with the specified fallback resolver.
        """
        pass


class BasePolicyResolver(PolicyResolver):
    """
    Base class for policy resolvers.

    A policy resolver is the routing view of a metadata resolver: the metadata resolver it
    holds owns the records and the lookup, and a caller here reads only the request/response
    policies of a resolved record. Fallback stays at this layer rather than being delegated
    to the metadata resolver, so a chain may include resolvers that are not metadata-backed.

    A resolved record may withhold its routing policies, in which case this resolver reports
    the command exactly the way it reports one the records do not carry: unresolved, so the
    fallback gets its turn and a resolver without one leaves the cluster client to resolve the
    target itself.

    Memoization of resolved policies belongs to the metadata resolver, which serves the
    routing projection through ``resolve_policies``, so a command is projected once no matter
    which layer it is resolved through.
    """

    def __init__(
        self, policies: PolicyRecords, fallback: Optional[PolicyResolver] = None
    ) -> None:
        """
        Parameters:
            policies (PolicyRecords): Policy records to serve. Lifted into metadata records
                where every other field keeps its fail-closed default, so a resolver built
                this way reports no command as client-side-cacheable - the conservative answer
                for metadata that was never supplied. Keys are lowercased, because that is how
                a resolved command name is looked up.
            fallback (Optional[PolicyResolver]): An optional resolver to be used when the
                primary policies cannot handle a specific request.
        """
        self._init_from_metadata_resolver(
            DynamicMetadataResolver(
                _build_commands_metadata_cache_from_policies(policies)
            ),
            fallback,
        )

    def _init_from_metadata_resolver(
        self,
        metadata_resolver: MetadataResolver,
        fallback: Optional[PolicyResolver] = None,
    ) -> None:
        """
        Initialize from a metadata resolver instead of from policy records.

        The purpose of this method is to allow the subclasses, whose own constructor carries
        no policy records to hand up, like ``StaticPolicyResolver`` and ``DynamicPolicyResolver``,
        to initialize using metadata_resolver.
        They resolve through a metadata resolver so that one object can serve routing
        and every other command-metadata consumer.
        """
        self._metadata_resolver = metadata_resolver
        self._fallback = fallback

    def resolve(self, command_name: str) -> Optional[CommandPolicies]:
        policies = self._metadata_resolver.resolve_policies(command_name)

        if policies is None and self._fallback is not None:
            return self._fallback.resolve(command_name)

        return policies

    @abstractmethod
    def with_fallback(self, fallback: "PolicyResolver") -> "PolicyResolver":
        pass


class AsyncBasePolicyResolver(AsyncPolicyResolver):
    """
    Async base class for policy resolvers.

    A policy resolver is the routing view of a metadata resolver: the metadata resolver it
    holds owns the records and the lookup, and a caller here reads only the request/response
    policies of a resolved record. Fallback stays at this layer rather than being delegated
    to the metadata resolver, so a chain may include resolvers that are not metadata-backed.

    A resolved record may withhold its routing policies, in which case this resolver reports
    the command exactly the way it reports one the records do not carry: unresolved, so the
    fallback gets its turn and a resolver without one leaves the cluster client to resolve the
    target itself.

    Memoization of resolved policies belongs to the metadata resolver, which serves the
    routing projection through ``resolve_policies``, so a command is projected once no matter
    which layer it is resolved through.
    """

    def __init__(
        self, policies: PolicyRecords, fallback: Optional[AsyncPolicyResolver] = None
    ) -> None:
        """
        Parameters:
            policies (PolicyRecords): Policy records to serve. Mirrors
                sync ``BasePolicyResolver`` - see its note.
            fallback (Optional[AsyncPolicyResolver]): An optional resolver to be used when
                the primary policies cannot handle a specific request.
        """
        self._init_from_metadata_resolver(
            AsyncDynamicMetadataResolver(
                _build_commands_metadata_cache_from_policies(policies)
            ),
            fallback,
        )

    def _init_from_metadata_resolver(
        self,
        metadata_resolver: AsyncMetadataResolver,
        fallback: Optional[AsyncPolicyResolver] = None,
    ) -> None:
        """Async mirror of ``BasePolicyResolver._init_from_metadata_resolver`` - see its note."""
        self._metadata_resolver = metadata_resolver
        self._fallback = fallback

    async def resolve(self, command_name: str) -> Optional[CommandPolicies]:
        policies = await self._metadata_resolver.resolve_policies(command_name)

        if policies is None and self._fallback is not None:
            return await self._fallback.resolve(command_name)

        return policies

    @abstractmethod
    def with_fallback(self, fallback: "AsyncPolicyResolver") -> "AsyncPolicyResolver":
        pass


class DynamicPolicyResolver(BasePolicyResolver):
    """
    Resolves policy dynamically based on the COMMAND output.
    """

    def __init__(
        self, commands_parser: CommandsParser, fallback: Optional[PolicyResolver] = None
    ) -> None:
        """
        Parameters:
            commands_parser (CommandsParser): COMMAND output parser.
            fallback (Optional[PolicyResolver]): An optional resolver to be used when the
                primary policies cannot handle a specific request.
        """
        self._commands_parser = commands_parser
        self._init_from_metadata_resolver(
            DynamicMetadataResolver(_load_commands_metadata_cache(commands_parser)),
            fallback,
        )

    def with_fallback(self, fallback: "PolicyResolver") -> "PolicyResolver":
        return DynamicPolicyResolver(self._commands_parser, fallback)


class StaticPolicyResolver(BasePolicyResolver):
    """
    Resolves policy from a static list of command metadata records.
    """

    def __init__(
        self,
        fallback: Optional[PolicyResolver] = None,
        metadata_resolver: Optional[MetadataResolver] = None,
    ) -> None:
        """
        Parameters:
            fallback (Optional[PolicyResolver]): An optional fallback policy resolver
                used for resolving policies if static policies are inadequate.
            metadata_resolver (Optional[MetadataResolver]): The metadata resolver to project
                the routing view of. Defaults to a ``StaticMetadataResolver``. Pass one to
                route by the same records another consumer - the client-side cache, say -
                resolves through, so a client configured with a single metadata resolver has
                a single source of truth. A chain that starts with a static resolver keeps
                serving the static records first, which is what this class promises.
        """
        if metadata_resolver is None:
            metadata_resolver = StaticMetadataResolver()

        self._init_from_metadata_resolver(metadata_resolver, fallback)

    def with_fallback(self, fallback: "PolicyResolver") -> "PolicyResolver":
        return StaticPolicyResolver(fallback, self._metadata_resolver)


class AsyncDynamicPolicyResolver(AsyncBasePolicyResolver):
    """
    Async version of DynamicPolicyResolver.

    Takes records rather than the parser that produced them, because
    ``AsyncCommandsParser.get_commands_metadata_cache`` is a coroutine and cannot be awaited in a
    constructor. That is why this class and its sync counterpart differ in what they accept,
    and the difference is forced rather than an oversight: the sync parser can be read in
    ``__init__`` and the async one cannot. To serve full command metadata asynchronously,
    build an ``AsyncDynamicMetadataResolver`` from the records and pass it to
    ``AsyncBasePolicyResolver`` through ``AsyncStaticPolicyResolver(metadata_resolver=...)``.
    """

    def __init__(
        self,
        policy_records: PolicyRecords,
        fallback: Optional[AsyncPolicyResolver] = None,
    ) -> None:
        """
        Parameters:
            policy_records (PolicyRecords): Policy records, lifted into metadata
                records where every other field keeps its fail-closed default - so a resolver
                built this way reports no command as client-side-cacheable, which is the
                conservative answer for metadata it was never given. Keys are lowercased,
                because that is how a resolved command name is looked up.
            fallback (Optional[AsyncPolicyResolver]): An optional resolver to be used when the
                primary policies cannot handle a specific request.
        """
        self._policy_records = policy_records
        super().__init__(policy_records, fallback)

    def with_fallback(self, fallback: "AsyncPolicyResolver") -> "AsyncPolicyResolver":
        return AsyncDynamicPolicyResolver(self._policy_records, fallback)


class AsyncStaticPolicyResolver(AsyncBasePolicyResolver):
    """
    Async version of StaticPolicyResolver.
    """

    def __init__(
        self,
        fallback: Optional[AsyncPolicyResolver] = None,
        metadata_resolver: Optional[AsyncMetadataResolver] = None,
    ) -> None:
        """
        Parameters:
            fallback (Optional[AsyncPolicyResolver]): An optional fallback policy resolver
                used for resolving policies if static policies are inadequate.
            metadata_resolver (Optional[AsyncMetadataResolver]): The metadata resolver to
                project the routing view of. Defaults to an ``AsyncStaticMetadataResolver``.
                Mirrors ``StaticPolicyResolver`` - see its note.
        """
        if metadata_resolver is None:
            metadata_resolver = AsyncStaticMetadataResolver()

        self._init_from_metadata_resolver(metadata_resolver, fallback)

    def with_fallback(self, fallback: "AsyncPolicyResolver") -> "AsyncPolicyResolver":
        return AsyncStaticPolicyResolver(fallback, self._metadata_resolver)


def __getattr__(name: str):
    """
    Serve the deprecated ``STATIC_POLICIES`` table, warning on the way.

    A module attribute cannot be deprecated by decoration, so the table is bound to a private
    name above and resolved here instead - which is only reached because no module attribute
    of that name exists.
    """
    if name == "STATIC_POLICIES":
        warn_deprecated(
            "STATIC_POLICIES",
            reason=(
                "Nothing in this library reads this table any more. It is a frozen copy of "
                "the 7.1.0 routing table, kept for backwards compatibility only, and it does "
                "not describe what this client routes by"
            ),
            version="8.1.0",
            stacklevel=3,
        )
        return _DEPRECATED_STATIC_POLICIES

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
