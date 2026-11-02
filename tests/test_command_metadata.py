from dataclasses import replace
from typing import Any, Iterator, Optional
from unittest.mock import Mock

import pytest

from redis._parsers import CommandsParser
from redis._parsers.commands import (
    _build_commands_metadata_cache,
    _build_policy_records,
    _parse_subcommand,
)
from redis.cache import CacheConfig
from redis.cluster import RedisCluster
from redis.commands.metadata import (
    _DEFAULT_KEYED_METADATA,
    _DEFAULT_KEYLESS_METADATA,
    _MEMO_MAX_ENTRIES,
    _is_replica_safe,
    _is_trackable_read,
    _METADATA_BY_REQUEST_POLICY,
    _STATIC_COMMAND_METADATA,
    PolicyRecords,
    CommandMetadata,
    CommandMetadataRecordsCache,
    CommandPolicies,
    DynamicMetadataResolver,
    MetadataResolver,
    RequestPolicy,
    ResponsePolicy,
    StaticMetadataResolver,
    _build_commands_metadata_cache_from_policies,
    _is_client_side_cacheable,
    _load_commands_metadata_cache,
    _split_command_name,
    _to_command_policies,
)
from redis.commands.policies import (
    BasePolicyResolver,
    DynamicPolicyResolver,
    PolicyResolver,
    StaticPolicyResolver,
)
from redis.utils import str_if_bytes
from tests.conftest import skip_if_server_version_lt

# The server the static table was generated from.
# 8.11 is the first release that reports every command the
# table carries (FT.ALIASLIST from 8.10, the BLESS container from 8.11); 8.10 is the first
# that reports the ``script_runner`` flag. The guards that compare the whole table against
# the live reply cannot run below it.
STATIC_TABLE_SERVER_VERSION = "8.11.0"

# Shape of a plain cacheable keyed read, used to stand in for live metadata in the
# resolver unit tests below.
CACHEABLE_KEYED = CommandMetadata(
    request_policy=RequestPolicy.DEFAULT_KEYED,
    response_policy=ResponsePolicy.DEFAULT_KEYED,
    is_readonly=True,
    has_key_argument=True,
    has_complete_metadata=True,
)


KEYED_POLICIES = (RequestPolicy.DEFAULT_KEYED, ResponsePolicy.DEFAULT_KEYED)

# The ``movablekeys`` reads whose records withhold their routing policies. Asserted as one set
# rather than command by command, because a table edit that gives any of them a routing policy
# sends it to an arbitrary node instead of the one holding its keys.
WITHHELD_ROUTING_COMMANDS = (
    "sdiffcard",
    "sintercard",
    "sunioncard",
    "xread",
    "zdiff",
    "zinter",
    "zintercard",
    "zunion",
)

# The records that carry the metadata making a command ineligible for client-side caching on
# a server that does not report it - or does not report it yet. They withhold their routing
# policies too, which is what makes adding them a no-op for cluster routing.
INELIGIBLE_RECORD_COMMANDS = (
    "eval_ro",
    "evalsha_ro",
    "fcall_ro",
    "touch",
    "vrandmember",
)

# The records whose cacheability inputs contradict the live reply on purpose, and the one field
# each diverges on. TOUCH reports readonly and keyed with no ``dont_cache`` tip; VRANDMEMBER
# reports no tips at all where every core random read is tipped ``nondeterministic_output``.
#
# Only the named field is exempted from the drift guards - every other field of these records
# is still compared against the live reply - and the divergence itself is pinned in both
# directions by ``test_the_recorded_divergence_from_the_live_reply_is_still_needed``, so a
# server that starts agreeing surfaces as a failure rather than as silent dead weight.
LIVE_CACHEABILITY_DIVERGENCE = {
    "touch": "is_dont_cache",
    "vrandmember": "has_nondeterministic_output",
}

# What the static table makes client-side cacheable that the legacy
# ``CacheConfig.DEFAULT_ALLOW_LIST`` never carried. One constant, because two tests pin this
# delta - the default path and the live-chained path - and a table edit that updated only one
# of them is exactly how they drifted apart before.
NEWLY_ELIGIBLE_VS_LEGACY_ALLOW_LIST = frozenset(
    {
        "DIGEST",
        "EXPIRETIME",
        "FT.SUGGET",
        "FT.SUGLEN",
        "HEXPIRETIME",
        "HPEXPIRETIME",
        "PEXPIRETIME",
        "SDIFFCARD",
        "SEXPIRETIME",
        "SPEXPIRETIME",
        "SUNIONCARD",
    }
)

# Keyless readonly commands whose routing policies are withheld so the cluster client routes
# them via COMMAND_FLAGS (e.g. DEFAULT_NODE or PRIMARIES).
WITHHELD_KEYLESS_READS = (
    "dbsize",
    "keys",
    "randomkey",
    "scan",
)

# Every entry of the static table whose routing view is None. BLESS SCAN and COMMAND are
# keyless and withhold their routing like the reads above, but neither is flagged readonly.
ALL_WITHHELD_ROUTING_COMMANDS = (
    *WITHHELD_ROUTING_COMMANDS,
    *INELIGIBLE_RECORD_COMMANDS,
    *WITHHELD_KEYLESS_READS,
    "bless scan",
    "command",
)


def policy_pair(
    policies: CommandPolicies,
) -> tuple[RequestPolicy, ResponsePolicy]:
    """
    The policies a resolver served, as a comparable pair.

    A policy resolver projects a fresh record out of the metadata it proxies, so its policies
    are compared by value rather than by identity. ``CommandPolicies`` compares by value too
    (see ``TestCommandPoliciesEquality``); this spells the comparison out where the expected
    value is more readable as a pair than as a record.
    """
    return policies.request_policy, policies.response_policy


class RecordsPolicyResolver(BasePolicyResolver):
    """A policy resolver built from policy records rather than from a metadata resolver."""

    def __init__(
        self, policies: PolicyRecords, fallback: Optional[PolicyResolver] = None
    ) -> None:
        self._policy_records = policies
        super().__init__(policies, fallback)

    def with_fallback(self, fallback: PolicyResolver) -> PolicyResolver:
        return RecordsPolicyResolver(self._policy_records, fallback)


def metadata_parser(metadata_records: CommandMetadataRecordsCache) -> CommandsParser:
    """A ``CommandsParser`` stand-in that reports the given metadata records."""
    commands_parser = Mock(spec=CommandsParser)
    commands_parser.get_commands_metadata_cache.return_value = metadata_records
    return commands_parser


def legacy_policy_parser(policy_records: PolicyRecords) -> CommandsParser:
    """
    A stand-in for the 7.1.0 parser shape: ``get_command_policies`` and nothing else.

    Reproduces a shape the library only keeps a compatibility shim for, so the shim has a
    regression test. It is not a pattern to copy: ``CommandsParser`` is private to
    ``redis._parsers``, and the supported way to decide routing is the public
    ``PolicyResolver`` ABC.

    The metadata method is deleted rather than left unset, because ``Mock(spec=CommandsParser)``
    would otherwise serve it from the spec and the shim would never be reached.
    """
    commands_parser = Mock(spec=CommandsParser)
    commands_parser.get_command_policies.return_value = policy_records
    del commands_parser.get_commands_metadata_cache
    return commands_parser


# Policy records for the shim's regression tests: DBSIZE fanned out and summed, which no
# keyed/keyless default resolves to, so serving them is observable.
LEGACY_POLICIES = CommandPolicies(
    request_policy=RequestPolicy.ALL_SHARDS,
    response_policy=ResponsePolicy.AGG_SUM,
)
LEGACY_POLICY_RECORDS: PolicyRecords = {"core": {"dbsize": LEGACY_POLICIES}}


def cacheability_fields(metadata: CommandMetadata) -> tuple[bool, ...]:
    """Everything a record carries except the routing policies."""
    return (
        metadata.is_readonly,
        metadata.is_blocking,
        metadata.has_key_argument,
        metadata.has_nondeterministic_output,
        metadata.is_script_runner,
        metadata.is_dont_cache,
        metadata.has_complete_metadata,
    )


def static_table_names() -> Iterator[tuple[str, str, str]]:
    """
    Yield ``(module, command, resolver_name)`` for every entry of the static table.

    ``resolver_name`` is the name a caller passes to ``resolve``, which is also the name
    the command is reported under in a lowercased ``COMMAND`` reply.
    """
    for module, commands in _STATIC_COMMAND_METADATA.items():
        for command in commands:
            yield (
                module,
                command,
                command if module == "core" else f"{module}.{command}",
            )


def slot_routed_static_commands() -> Iterator[str]:
    """
    The names of every static-table entry the cluster client must route by its keys.

    That is the entries recorded ``DEFAULT_KEYED`` plus the keyed entries that withhold their
    routing policies: the first route by slot directly, the second send the client down its own
    slot resolution, and both must land on the node holding the key. Derived from the table so the
    routing tests in ``tests/test_cluster.py`` and its async mirror cannot drift from it.
    """
    for _, _, name in static_table_names():
        module, command = _split_command_name(name)
        metadata = _STATIC_COMMAND_METADATA[module][command]

        if (
            metadata.request_policy is None
            or metadata.request_policy is RequestPolicy.DEFAULT_KEYED
        ) and metadata.has_key_argument:
            yield name


def live_command_details(commands: dict[str, Any]) -> dict[str, Any]:
    """
    Key a ``COMMAND`` reply the way the record tables are keyed.

    Module commands are reported in upper case (``FT.SEARCH``), so the names are
    lowercased the way ``CommandsParser.initialize`` lowercases them. Container
    subcommands are reported nested under their container as ``bless|scan`` and are
    flattened under their space-joined name (``bless scan``), the form the tables key them
    by and ``execute_command`` receives.
    """
    details_by_name = {}
    for name, details in commands.items():
        details_by_name[name.lower()] = details
        for subcommand in details.get("subcommands") or ():
            subcommand_details = _parse_subcommand(subcommand)
            subcommand_name = subcommand_details["name"].lower().replace("|", " ")
            details_by_name[subcommand_name] = subcommand_details
    return details_by_name


def command_flags(details: dict[str, Any]) -> set[str]:
    # RESP2 reports flags as a list, RESP3 as a set; both decode to the same names.
    return {str_if_bytes(flag) for flag in details["flags"]}


def command_tips(details: dict[str, Any]) -> set[str]:
    return {str_if_bytes(tip) for tip in details.get("tips") or ()}


def live_has_key_argument(details: dict[str, Any]) -> bool:
    """
    Whether the live reply says the command takes a key name argument.

    ``movablekeys`` commands report ``first_key_pos == 0``, so the answer is only in the
    key specs. This asserts the specs are there rather than normalizing them: the
    ``not_key`` filter belongs to the resolver, and the key-spec reply shape differs
    between RESP2 and RESP3.
    """
    if "movablekeys" in command_flags(details):
        return bool(details.get("key_specifications"))

    return details["first_key_pos"] > 0 and details["step_count"] > 0


class CountingStaticMetadataResolver(StaticMetadataResolver):
    """The shipped static resolver, counting how often the record lookup runs."""

    def __init__(self) -> None:
        super().__init__()
        self.resolve_calls = 0

    def resolve(self, command_name: str) -> Optional[CommandMetadata]:
        self.resolve_calls += 1
        return super().resolve(command_name)


@pytest.mark.fixed_client
class TestDefaultRecords:
    """
    The records the cluster clients fall back to when the policy resolver does not know a
    command. They are ``CommandMetadata`` rather than ``CommandPolicies`` precisely because
    they are shared: a frozen record is safe to hand out on every command, and only the two
    routing policies - which both record types carry - are ever read from them.

    Only the sync suite covers them: they are module-level constants shared by both stacks.
    """

    def test_the_default_records_hold_the_fallback_shapes(self):
        assert _DEFAULT_KEYLESS_METADATA == CommandMetadata(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        )
        assert _DEFAULT_KEYED_METADATA == CommandMetadata(
            request_policy=RequestPolicy.DEFAULT_KEYED,
            response_policy=ResponsePolicy.DEFAULT_KEYED,
        )

    def test_every_request_policy_has_a_record(self):
        """
        The node-flag fallback looks a record up by the request policy it resolved, so the
        table must cover every member - including any added later.
        """
        for policy in RequestPolicy:
            record = _METADATA_BY_REQUEST_POLICY[policy]

            assert record.request_policy is policy
            # The node flags decide routing only; the response policy stays at its default.
            assert record.response_policy is ResponsePolicy.DEFAULT_KEYLESS

    def test_the_default_records_are_not_cacheable(self):
        """
        A command the resolver did not know is a command whose metadata is unknown, so the
        record the client falls back to must not claim its reply may be cached.
        """
        assert _is_client_side_cacheable(_DEFAULT_KEYLESS_METADATA) is False
        assert _is_client_side_cacheable(_DEFAULT_KEYED_METADATA) is False
        assert all(
            _is_client_side_cacheable(record) is False
            for record in _METADATA_BY_REQUEST_POLICY.values()
        )


@pytest.mark.fixed_client
class TestIsClientSideCacheable:
    """
    The one normative implementation of the client-side-caching eligibility rules.

    Only the sync suite covers it: it is a module-level function over an immutable record,
    shared by both stacks, so an async mirror would assert the same call twice.
    """

    def test_a_readonly_keyed_command_is_cacheable(self):
        assert _is_client_side_cacheable(CACHEABLE_KEYED) is True

    def test_an_unknown_command_is_not_cacheable(self):
        """What an exhausted resolver chain resolves to."""
        assert _is_client_side_cacheable(None) is False

    def test_incomplete_metadata_is_not_cacheable(self):
        """
        A reply that carries no tips cannot express the negative markers, so a record built
        from one does not prove the command is cacheable - however positive it looks.
        """
        metadata = replace(CACHEABLE_KEYED, has_complete_metadata=False)

        assert _is_client_side_cacheable(metadata) is False

    def test_dont_cache_overrides_every_positive_rule(self):
        metadata = replace(CACHEABLE_KEYED, is_dont_cache=True)

        assert _is_client_side_cacheable(metadata) is False

    def test_a_write_command_is_not_cacheable(self):
        metadata = replace(CACHEABLE_KEYED, is_readonly=False)

        assert _is_client_side_cacheable(metadata) is False

    def test_a_blocking_command_is_not_cacheable(self):
        """The XREAD shape: readonly and keyed, but its reply is one the caller waited for."""
        metadata = replace(CACHEABLE_KEYED, is_blocking=True)

        assert _is_client_side_cacheable(metadata) is False

    def test_a_keyless_command_is_not_cacheable(self):
        """The KEYS shape: readonly, but it takes no key name argument."""
        metadata = replace(CACHEABLE_KEYED, has_key_argument=False)

        assert _is_client_side_cacheable(metadata) is False

    def test_a_nondeterministic_command_is_not_cacheable(self):
        """The XPENDING shape."""
        metadata = replace(CACHEABLE_KEYED, has_nondeterministic_output=True)

        assert _is_client_side_cacheable(metadata) is False

    def test_a_script_runner_is_not_cacheable(self):
        """The EVAL_RO shape."""
        metadata = replace(CACHEABLE_KEYED, is_script_runner=True)

        assert _is_client_side_cacheable(metadata) is False

    def test_nondeterministic_output_order_does_not_disqualify(self):
        """
        HGETALL, HKEYS, HVALS, SDIFF, SINTER, SMEMBERS and SUNION carry
        ``nondeterministic_output_order``, a different tip that only says element order
        varies. Matching tips by prefix would silently drop all seven.
        """
        static_resolver = StaticMetadataResolver()

        for name in (
            "hgetall",
            "hkeys",
            "hvals",
            "sdiff",
            "sinter",
            "smembers",
            "sunion",
        ):
            assert static_resolver.is_cacheable(name) is True, name


@pytest.mark.fixed_client
class TestIsTrackableRead:
    """
    Whether the server would remember a command's keys: the ``readonly`` flag alone.

    A different question from eligibility, and deliberately so. The server's own gate reads
    the readonly flag and the key arguments of the invocation and nothing else, so every
    negative signal that makes a command ineligible to *store* is irrelevant here. The verdict
    never affects storage: it decides only whether an ``optout`` ``CLIENT CACHING NO`` in
    front of a read is worth sending.

    Only the sync suite covers it, for the reason given on ``TestIsClientSideCacheable``.
    """

    def test_a_readonly_command_is_trackable(self):
        assert _is_trackable_read(CACHEABLE_KEYED) is True

    def test_a_write_command_is_not_trackable(self):
        assert _is_trackable_read(replace(CACHEABLE_KEYED, is_readonly=False)) is False

    def test_an_unknown_command_is_not_trackable(self):
        """What an exhausted resolver chain resolves to."""
        assert _is_trackable_read(None) is False

    def test_incomplete_metadata_is_still_trackable(self):
        """
        Unlike eligibility, which refuses an incomplete record. The readonly flag comes from
        the command flags, which every ``COMMAND`` reply carries; only the tips can be
        missing, and no tip enters the server's tracking gate.
        """
        metadata = replace(CACHEABLE_KEYED, has_complete_metadata=False)

        assert _is_client_side_cacheable(metadata) is False
        assert _is_trackable_read(metadata) is True

    @pytest.mark.parametrize(
        "field",
        ["is_dont_cache", "has_nondeterministic_output", "is_script_runner"],
    )
    def test_the_negative_eligibility_signals_do_not_untrack(self, field):
        """The server tracks XPENDING, TOUCH and EVAL_RO's own reads all the same."""
        metadata = replace(CACHEABLE_KEYED, **{field: True})

        assert _is_client_side_cacheable(metadata) is False
        assert _is_trackable_read(metadata) is True

    def test_a_keyless_read_is_trackable(self):
        """
        The server extracts the keys from the invocation itself, so a ``NO`` in front of a
        keyless read is consumed with no effect. Approximating in this direction wastes one
        command; approximating the other way would risk a stored, untracked reply.
        """
        assert _is_trackable_read(replace(CACHEABLE_KEYED, has_key_argument=False)) is (
            True
        )


@pytest.mark.fixed_client
class TestWithheldRoutingPolicies:
    """
    A record may carry its cacheability inputs while withholding its routing policies.

    The ``movablekeys`` reads need that. Their keys live only in the key specs, so the policies
    derived from ``first_key_pos`` come out keyless, and recording them would send the command
    to an arbitrary node. Withholding them makes a policy resolver report the command the way it
    reports one the records do not carry, which is what sends the cluster client back to
    ``determine_slot`` -> ``COMMAND GETKEYS`` - the only path that finds those keys.

    Only the sync suite covers the projection function and the shipped table: both are
    module-level and shared by the two stacks. The resolver behaviour is mirrored in the async
    suite.
    """

    def test_a_withheld_record_projects_to_no_policies(self):
        assert (
            _to_command_policies(
                replace(CACHEABLE_KEYED, request_policy=None, response_policy=None)
            )
            is None
        )
        # Withholding one of the two is enough: a policy resolver serves them as one record.
        assert (
            _to_command_policies(replace(CACHEABLE_KEYED, request_policy=None)) is None
        )
        assert (
            _to_command_policies(replace(CACHEABLE_KEYED, response_policy=None)) is None
        )
        # A record that carries both still projects to them.
        assert policy_pair(_to_command_policies(CACHEABLE_KEYED)) == KEYED_POLICIES

    def test_the_movablekeys_reads_withhold_their_policies(self):
        static_resolver = StaticMetadataResolver()

        for name in WITHHELD_ROUTING_COMMANDS:
            metadata = static_resolver.resolve(name)

            assert metadata is not None, name
            assert metadata.request_policy is None, name
            assert metadata.response_policy is None, name
            # Withholding the routing view leaves the cacheability inputs untouched.
            assert metadata.is_readonly is True, name
            assert metadata.has_key_argument is True, name
            assert metadata.has_complete_metadata is True, name

    def test_cacheability_is_unaffected_by_the_withheld_routing(self):
        static_resolver = StaticMetadataResolver()

        for name in WITHHELD_ROUTING_COMMANDS:
            # XREAD is the one exclusion here, and the blocking flag is what excludes it.
            assert static_resolver.is_cacheable(name) is (name != "xread"), name

    def test_the_ineligible_records_withhold_their_policies(self):
        """
        The four records that state their own ineligibility must not state a routing policy.

        The table backs both the metadata resolver and ``StaticPolicyResolver``, so a record
        added for its cacheability inputs alone would otherwise start routing a command the
        cluster client resolves for itself today.
        """
        static_resolver = StaticMetadataResolver()

        for name in INELIGIBLE_RECORD_COMMANDS:
            metadata = static_resolver.resolve(name)

            assert metadata is not None, name
            assert metadata.request_policy is None, name
            assert metadata.response_policy is None, name
            # Ineligible for the reason the record states, not for want of metadata.
            assert metadata.is_readonly is True, name
            assert metadata.has_key_argument is True, name
            assert metadata.has_complete_metadata is True, name
            assert static_resolver.is_cacheable(name) is False, name

        # Each record states *why*, so the reason survives a regeneration of the table.
        assert static_resolver.resolve("touch").is_dont_cache is True
        assert (
            static_resolver.resolve("vrandmember").has_nondeterministic_output is True
        )
        for name in ("eval_ro", "evalsha_ro", "fcall_ro"):
            assert static_resolver.resolve(name).is_script_runner is True, name

    def test_keyless_reads_withhold_routing_and_are_replica_safe(self):
        static_resolver = StaticMetadataResolver()
        for name in WITHHELD_KEYLESS_READS:
            metadata = static_resolver.resolve(name)

            assert metadata is not None, name
            assert metadata.request_policy is None, name
            assert metadata.response_policy is None, name
            assert metadata.is_readonly is True, name
            assert metadata.has_key_argument is False, name
            assert metadata.has_complete_metadata is True, name
            assert static_resolver.is_cacheable(name) is False, name
            assert static_resolver.is_replica_safe(name) is True, name

    def test_command_withholds_routing_and_is_not_replica_safe(self):
        static_resolver = StaticMetadataResolver()
        metadata = static_resolver.resolve("command")

        assert metadata is not None
        assert metadata.request_policy is None
        assert metadata.response_policy is None
        assert metadata.is_readonly is False
        assert metadata.has_key_argument is False
        assert metadata.has_complete_metadata is True
        assert static_resolver.is_cacheable("command") is False
        assert static_resolver.is_replica_safe("command") is False

    def test_bless_scan_withholds_routing_and_is_not_replica_safe(self):
        """
        The SCAN-shaped container subcommand: keyless, nondeterministic and routed to every
        primary through COMMAND_FLAGS, so its record must withhold routing like SCAN's does.
        Unlike SCAN the server reports no flags for it at all, so it is not readonly.
        """
        static_resolver = StaticMetadataResolver()
        metadata = static_resolver.resolve("bless scan")

        assert metadata is not None
        assert metadata.request_policy is None
        assert metadata.response_policy is None
        assert metadata.is_readonly is False
        assert metadata.has_key_argument is False
        assert metadata.has_nondeterministic_output is True
        assert metadata.has_complete_metadata is True
        assert static_resolver.is_cacheable("bless scan") is False
        assert static_resolver.is_replica_safe("bless scan") is False

    @pytest.mark.parametrize("name", ["bless clear", "bless get", "bless set"])
    def test_the_keyed_bless_subcommands_route_by_key_and_are_not_cacheable(self, name):
        """
        BLESS GET is included: the server flags it ``fast`` only, with no ``readonly``, so it
        fails closed exactly like the two writes.
        """
        static_resolver = StaticMetadataResolver()
        metadata = static_resolver.resolve(name)

        assert metadata is not None, name
        assert metadata.request_policy is RequestPolicy.DEFAULT_KEYED, name
        assert metadata.response_policy is ResponsePolicy.DEFAULT_KEYED, name
        assert metadata.is_readonly is False, name
        assert metadata.has_key_argument is True, name
        assert metadata.has_complete_metadata is True, name
        assert static_resolver.is_cacheable(name) is False, name
        assert static_resolver.is_replica_safe(name) is False, name

    def test_all_static_entries_in_cluster_command_flags_withhold_routing(self):
        """
        Verify that every command appearing in RedisCluster.command_flags that is present
        in _STATIC_COMMAND_METADATA has its routing view withheld (request_policy=None and
        response_policy=None).

        _determine_nodes gives precedence to resolved metadata request_policy over COMMAND_FLAGS,
        so any static table record for a COMMAND_FLAGS command must withhold its routing policies
        to ensure the legacy COMMAND_FLAGS routing (e.g. DEFAULT_NODE, PRIMARIES) is consulted.
        """
        static_resolver = StaticMetadataResolver()
        table_core = _STATIC_COMMAND_METADATA["core"]

        for flag_cmd in RedisCluster.COMMAND_FLAGS:
            # A container command like "COMMAND COUNT" or "SLOWLOG GET" may be recorded under
            # its first token ("command", "slowlog") or, like "BLESS SCAN", under its full
            # space-joined name - which is the name the resolver is asked for.
            for table_cmd in {flag_cmd.split()[0].lower(), flag_cmd.lower()}:
                if table_cmd not in table_core:
                    continue
                metadata = static_resolver.resolve(table_cmd)
                assert metadata is not None
                assert metadata.request_policy is None, (
                    f"Command '{table_cmd}' (from COMMAND_FLAGS '{flag_cmd}') must withhold request_policy in _STATIC_COMMAND_METADATA"
                )
                assert metadata.response_policy is None, (
                    f"Command '{table_cmd}' (from COMMAND_FLAGS '{flag_cmd}') must withhold response_policy in _STATIC_COMMAND_METADATA"
                )

    @pytest.mark.parametrize(
        "cmd",
        [
            "ft.aggregate",
            "ft.spellcheck",
            "ft.tagvals",
            "ft.syndump",
            "ft.dictdump",
            "ft.explaincli",
            "ts.get",
            "ts.range",
            "ts.revrange",
            "sort_ro",
            "georadius_ro",
            "georadiusbymember_ro",
            "substr",
        ],
    )
    def test_newly_replica_eligible_commands_are_replica_safe(self, cmd):
        resolver = StaticMetadataResolver()
        assert resolver.is_replica_safe(cmd) is True

    def test_the_ineligible_records_decide_ahead_of_a_live_layer(self):
        """
        Why the documented chain order is static-first, pinned in both directions and offline.

        The records exist to override a server that reports these commands as cacheable -
        which every supported server does for TOUCH and VRANDMEMBER, and servers before 8.9 do
        for the script runners. A resolver answers from the first record it finds, so only a
        static-first chain reaches them.
        """
        live_says_cacheable = DynamicMetadataResolver(
            {"core": {name: CACHEABLE_KEYED for name in INELIGIBLE_RECORD_COMMANDS}}
        )

        static_first = StaticMetadataResolver().with_fallback(live_says_cacheable)
        dynamic_first = live_says_cacheable.with_fallback(StaticMetadataResolver())

        for name in INELIGIBLE_RECORD_COMMANDS:
            assert static_first.is_cacheable(name) is False, name
            # Reversed, the live layer wins and the command becomes cacheable - which is
            # exactly what static-first exists to prevent, and what dynamic-first forfeits.
            assert dynamic_first.is_cacheable(name) is True, name

    def test_a_policy_resolver_reports_them_unresolved(self):
        """
        What sends the cluster client back to its own slot resolution: the routing view is None,
        exactly as it is for a command the table does not carry at all.
        """
        static_resolver = StaticPolicyResolver()

        for name in ALL_WITHHELD_ROUTING_COMMANDS:
            assert static_resolver.resolve(name) is None, name

        # A keyed read that is not movablekeys still resolves, so the None above is the withheld
        # record rather than a table-wide miss.
        assert policy_pair(static_resolver.resolve("get")) == KEYED_POLICIES

    def test_no_entry_that_takes_a_key_is_routed_keyless(self):
        """
        The invariant behind every withheld record, asserted over the whole table.

        A command that takes a key name argument must be routed to the node holding that key.
        Recording ``DEFAULT_KEYLESS`` for one sends it to an arbitrary node, which only a MOVED
        redirection repairs - so a keyed entry may say ``DEFAULT_KEYED`` or say nothing, and
        nothing else. This is what generalizes beyond the ``movablekeys`` reads: it fails for any
        future entry whose policies are taken from a ``COMMAND`` reply without checking whether
        the derived keyed/keyless answer is right.
        """
        for module, commands in _STATIC_COMMAND_METADATA.items():
            for command, metadata in commands.items():
                if not metadata.has_key_argument:
                    continue

                name = command if module == "core" else f"{module}.{command}"
                assert metadata.request_policy in (
                    RequestPolicy.DEFAULT_KEYED,
                    None,
                ), name

    def test_a_withheld_record_does_not_re_walk_the_metadata_chain(self):
        """
        The record was found, so a metadata resolver behind this one is never asked - it would
        answer with the very policies the record withholds.
        """
        resolver = StaticMetadataResolver().with_fallback(
            DynamicMetadataResolver({"core": {"zdiff": CACHEABLE_KEYED}})
        )

        assert resolver.resolve("zdiff").request_policy is None
        assert resolver.resolve_policies("zdiff") is None

    def test_a_policy_level_fallback_still_gets_its_turn(self):
        """
        Withholding must be indistinguishable from a table miss at the policy layer, so that a
        caller-supplied chain behaves exactly as it did before the table carried these commands.
        """
        resolver = StaticPolicyResolver(
            fallback=DynamicPolicyResolver(
                metadata_parser({"core": {"zdiff": CACHEABLE_KEYED}})
            )
        )

        assert policy_pair(resolver.resolve("zdiff")) == KEYED_POLICIES


@pytest.mark.fixed_client
class TestCommandPoliciesEquality:
    """
    ``CommandPolicies`` compares by value.

    A policy resolver serves the projection of the record it proxies rather than a record a
    caller handed it, so identity does not hold and a caller matching what a resolver served
    against what it configured has only the values to go by.
    """

    def test_records_carrying_the_same_policies_are_equal(self):
        assert CommandPolicies(
            request_policy=RequestPolicy.ALL_SHARDS,
            response_policy=ResponsePolicy.AGG_SUM,
        ) == CommandPolicies(
            request_policy=RequestPolicy.ALL_SHARDS,
            response_policy=ResponsePolicy.AGG_SUM,
        )

    def test_a_differing_policy_is_not_equal(self):
        keyed = CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYED,
            response_policy=ResponsePolicy.DEFAULT_KEYED,
        )

        assert keyed != CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYLESS,
            response_policy=ResponsePolicy.DEFAULT_KEYED,
        )
        assert keyed != CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYED,
            response_policy=ResponsePolicy.DEFAULT_KEYLESS,
        )

    def test_comparison_against_another_type_is_unequal_rather_than_an_error(self):
        assert CommandPolicies() != KEYED_POLICIES
        assert CommandPolicies() is not None

    def test_records_stay_hashable(self):
        """
        ``__hash__`` is defined alongside ``__eq__`` because Python would otherwise set it to
        None, which would make a type that shipped hashable in 7.1.0 unhashable.
        """
        assert len({CommandPolicies(), CommandPolicies()}) == 1
        assert len({CommandPolicies(), _to_command_policies(CACHEABLE_KEYED)}) == 2

    def test_a_resolver_serves_a_record_equal_to_the_configured_one(self):
        """
        The regression this exists for: the 7.1.0 comparison against the caller's own record.
        """
        zcount_policies = CommandPolicies(
            request_policy=RequestPolicy.DEFAULT_KEYED,
            response_policy=ResponsePolicy.DEFAULT_KEYED,
        )
        resolver = DynamicPolicyResolver(
            metadata_parser({"core": {"zcount": CACHEABLE_KEYED}})
        )

        served = resolver.resolve("zcount")

        assert served == zcount_policies
        # It is the projection, not the record the table holds - which is exactly why value
        # equality is needed.
        assert served is not zcount_policies

    def test_repr_names_both_policies(self):
        assert repr(
            CommandPolicies(
                request_policy=RequestPolicy.ALL_NODES,
                response_policy=ResponsePolicy.AGG_MIN,
            )
        ) == (
            "CommandPolicies(request_policy=<RequestPolicy.ALL_NODES: 'all_nodes'>, "
            "response_policy=<ResponsePolicy.AGG_MIN: 'agg_min'>)"
        )


@pytest.mark.fixed_client
class TestMemoBounds:
    """
    Neither memo may grow without bound.

    The default resolver a client gets is a single instance evaluated at import and shared by
    every client in the process, ``execute_command`` accepts an arbitrary command name, and a
    name that resolves to nothing is memoized too - so a caller that asks about unbounded many
    names would otherwise retain all of them for the life of the process.
    """

    @staticmethod
    def _fill_beyond_the_cap(ask) -> None:
        for index in range(_MEMO_MAX_ENTRIES + 100):
            ask(f"nosuchmodule.nosuchcommand{index}")

    def test_the_policy_memo_stops_at_the_cap(self):
        resolver = StaticMetadataResolver()

        self._fill_beyond_the_cap(resolver.resolve_policies)

        assert len(resolver._policies) == _MEMO_MAX_ENTRIES

    def test_the_cacheable_memo_stops_at_the_cap(self):
        resolver = StaticMetadataResolver()

        self._fill_beyond_the_cap(resolver.is_cacheable)

        assert len(resolver._cacheable) == _MEMO_MAX_ENTRIES

    def test_the_trackable_read_memo_stops_at_the_cap(self):
        resolver = StaticMetadataResolver()

        self._fill_beyond_the_cap(resolver.is_trackable_read)

        assert len(resolver._trackable_read) == _MEMO_MAX_ENTRIES

    def test_the_views_stay_correct_past_the_cap(self):
        """A capped memo recomputes; it never answers wrongly."""
        resolver = StaticMetadataResolver()

        self._fill_beyond_the_cap(resolver.resolve_policies)
        self._fill_beyond_the_cap(resolver.is_cacheable)
        self._fill_beyond_the_cap(resolver.is_trackable_read)

        assert policy_pair(resolver.resolve_policies("get")) == KEYED_POLICIES
        assert resolver.is_cacheable("get") is True
        assert resolver.is_trackable_read("get") is True
        assert resolver.resolve_policies("nosuchmodule.nosuchcommand") is None
        assert resolver.is_cacheable("nosuchmodule.nosuchcommand") is False
        assert resolver.is_trackable_read("nosuchmodule.nosuchcommand") is False


@pytest.mark.fixed_client
class TestDeprecations:
    """
    The 7.1.0 routing surface warns rather than disappearing.

    ``get_command_policies`` is superseded by ``get_commands_metadata_cache``, and
    ``STATIC_POLICIES`` is a frozen copy of the 7.1.0 table that no code path reads.
    """

    def test_get_command_policies_warns(self):
        commands_parser = CommandsParser.__new__(CommandsParser)
        commands_parser.commands = {}

        with pytest.warns(DeprecationWarning, match="get_commands_metadata_cache"):
            assert commands_parser.get_command_policies() == {}

    def test_static_policies_warns_on_access(self):
        import redis.commands.policies as policies_module

        with pytest.warns(DeprecationWarning, match="STATIC_POLICIES"):
            table = policies_module.STATIC_POLICIES

        assert table is policies_module._DEPRECATED_STATIC_POLICIES

    def test_an_unknown_module_attribute_still_raises(self):
        """The module ``__getattr__`` must not swallow a genuine typo."""
        import redis.commands.policies as policies_module

        with pytest.raises(AttributeError, match="nosuchattribute"):
            policies_module.nosuchattribute

    def test_the_frozen_table_is_unchanged(self):
        """
        Nothing reads ``STATIC_POLICIES`` and nothing derives it, so pin it: it answers "what
        did 7.1.0 route by", and an edit to it would silently change what an external importer
        reads.
        """
        from redis.commands.policies import _DEPRECATED_STATIC_POLICIES

        table = _DEPRECATED_STATIC_POLICIES

        assert table.keys() == {"ft", "core"}
        assert len(table["ft"]) == 26
        assert table["core"].keys() == {"command"}

        assert policy_pair(table["ft"]["cursor"]) == (
            RequestPolicy.SPECIAL,
            ResponsePolicy.DEFAULT_KEYLESS,
        )
        assert policy_pair(table["ft"]["sugget"]) == KEYED_POLICIES
        assert policy_pair(table["ft"]["search"]) == (
            RequestPolicy.DEFAULT_KEYLESS,
            ResponsePolicy.DEFAULT_KEYLESS,
        )


@pytest.mark.fixed_client
class TestBasePolicyResolver:
    def test_resolve(self):
        mock_command_parser = metadata_parser(
            {
                "core": {
                    "zcount": CACHEABLE_KEYED,
                    "rpoplpush": replace(CACHEABLE_KEYED, is_readonly=False),
                }
            }
        )

        dynamic_resolver = DynamicPolicyResolver(mock_command_parser)
        assert policy_pair(dynamic_resolver.resolve("zcount")) == KEYED_POLICIES
        assert policy_pair(dynamic_resolver.resolve("rpoplpush")) == KEYED_POLICIES

        with pytest.raises(
            ValueError, match="Wrong command or module name: foo.bar.baz"
        ):
            dynamic_resolver.resolve("foo.bar.baz")

        assert dynamic_resolver.resolve("foo.bar") is None
        assert dynamic_resolver.resolve("core.foo") is None

        # Test that policy fallback correctly
        static_resolver = StaticPolicyResolver()
        with_fallback_dynamic_resolver = dynamic_resolver.with_fallback(static_resolver)

        assert (
            with_fallback_dynamic_resolver.resolve("ft.aggregate").request_policy
            == RequestPolicy.DEFAULT_KEYLESS
        )
        assert (
            with_fallback_dynamic_resolver.resolve("ft.aggregate").response_policy
            == ResponsePolicy.DEFAULT_KEYLESS
        )

        # Extended chain with one more resolver
        mock_command_parser = metadata_parser(
            {
                "foo": {
                    "bar": replace(
                        CACHEABLE_KEYED, request_policy=RequestPolicy.SPECIAL
                    )
                }
            }
        )
        another_dynamic_resolver = DynamicPolicyResolver(mock_command_parser)
        with_fallback_static_resolver = static_resolver.with_fallback(
            another_dynamic_resolver
        )
        with_double_fallback_dynamic_resolver = dynamic_resolver.with_fallback(
            with_fallback_static_resolver
        )

        assert policy_pair(
            with_double_fallback_dynamic_resolver.resolve("foo.bar")
        ) == (
            RequestPolicy.SPECIAL,
            ResponsePolicy.DEFAULT_KEYED,
        )

    def test_resolve_serves_the_metadata_it_proxies(self):
        """
        A policy resolver is the routing view of a metadata resolver, so the policies it
        serves are the ones the metadata record carries.
        """
        commands_parser = metadata_parser(
            {"core": {"pfcount": replace(CACHEABLE_KEYED, is_dont_cache=True)}}
        )

        dynamic_resolver = DynamicPolicyResolver(commands_parser)

        # The metadata is read through the metadata resolver, not through the policy view
        # of the parser.
        commands_parser.get_commands_metadata_cache.assert_called_once_with()
        commands_parser.get_command_policies.assert_not_called()

        assert policy_pair(dynamic_resolver.resolve("pfcount")) == KEYED_POLICIES

        # FT.CURSOR is the static entry whose policies are not the keyed/keyless defaults,
        # so it shows that whatever the record says is what is served.
        assert policy_pair(StaticPolicyResolver().resolve("ft.cursor")) == (
            RequestPolicy.SPECIAL,
            ResponsePolicy.DEFAULT_KEYLESS,
        )

    def test_resolve_from_policy_records(self):
        """
        The ``policies`` argument serves exactly the policies it is given. Every field such
        a record does not carry keeps its fail-closed default, so nothing but the routing view
        is claimed for them.
        """
        policy_records = {
            "core": {
                "zcount": CommandPolicies(
                    request_policy=RequestPolicy.DEFAULT_KEYED,
                    response_policy=ResponsePolicy.DEFAULT_KEYED,
                ),
                "keys": CommandPolicies(
                    request_policy=RequestPolicy.ALL_SHARDS,
                    response_policy=ResponsePolicy.SPECIAL,
                ),
            }
        }

        resolver = RecordsPolicyResolver(policy_records)

        assert policy_pair(resolver.resolve("zcount")) == KEYED_POLICIES
        assert policy_pair(resolver.resolve("keys")) == (
            RequestPolicy.ALL_SHARDS,
            ResponsePolicy.SPECIAL,
        )
        assert resolver.resolve("pfcount") is None

        # Chaining keeps the records, and the fallback answers what they do not carry.
        chained_resolver = resolver.with_fallback(StaticPolicyResolver())

        assert policy_pair(chained_resolver.resolve("keys")) == (
            RequestPolicy.ALL_SHARDS,
            ResponsePolicy.SPECIAL,
        )
        assert policy_pair(chained_resolver.resolve("ft.cursor")) == (
            RequestPolicy.SPECIAL,
            ResponsePolicy.DEFAULT_KEYLESS,
        )

    def test_the_policy_records_are_required_at_the_call_site(self):
        """A resolver cannot be built without records to resolve through."""
        with pytest.raises(TypeError, match="policies"):
            RecordsPolicyResolver()

    def test_resolve_memoizes_the_walk_down_the_chain(self):
        static_resolver = StaticPolicyResolver()
        resolver = DynamicPolicyResolver(
            metadata_parser({"core": {"pfcount": CACHEABLE_KEYED}}), static_resolver
        )

        # The walk down the fallback chain is memoized, so a repeated resolve stays off
        # the command execution path.
        assert resolver.resolve("pfcount") is resolver.resolve("pfcount")
        assert resolver.resolve("ft.cursor") is resolver.resolve("ft.cursor")
        # A command no resolver in the chain carries is memoized as unresolved.
        assert resolver.resolve("cms.query") is None
        assert resolver.resolve("cms.query") is None


@pytest.mark.fixed_client
class TestBaseMetadataResolver:
    def test_resolve(self):
        zcount_metadata = CACHEABLE_KEYED
        rpoplpush_metadata = replace(CACHEABLE_KEYED, is_readonly=False)

        dynamic_resolver = DynamicMetadataResolver(
            {
                "core": {
                    "zcount": zcount_metadata,
                    "rpoplpush": rpoplpush_metadata,
                }
            }
        )

        # The record is served as-is, not copied: consumers share one immutable record.
        assert dynamic_resolver.resolve("zcount") is zcount_metadata
        assert dynamic_resolver.resolve("rpoplpush") is rpoplpush_metadata

        with pytest.raises(
            ValueError, match="Wrong command or module name: foo.bar.baz"
        ):
            dynamic_resolver.resolve("foo.bar.baz")

        # Unknown module and unknown command in a known module both fail closed when
        # there is no fallback behind the resolver.
        assert dynamic_resolver.resolve("foo.bar") is None
        assert dynamic_resolver.resolve("core.foo") is None

    def test_resolve_policies_projects_the_routing_view(self):
        resolver = DynamicMetadataResolver(
            {"core": {"pfcount": replace(CACHEABLE_KEYED, is_dont_cache=True)}}
        )

        # Nothing but the routing policies of the record leaks into the projection.
        assert policy_pair(resolver.resolve_policies("pfcount")) == KEYED_POLICIES

        # FT.CURSOR is the static entry whose policies are not the keyed/keyless defaults,
        # so it shows that the projection carries whatever the record says.
        assert policy_pair(StaticMetadataResolver().resolve_policies("ft.cursor")) == (
            RequestPolicy.SPECIAL,
            ResponsePolicy.DEFAULT_KEYLESS,
        )

    def test_resolve_policies_memoizes_the_projection(self):
        resolver = DynamicMetadataResolver(
            {"core": {"pfcount": CACHEABLE_KEYED}},
            StaticMetadataResolver(),
        )

        # Both the projected record and the walk down the fallback chain are memoized, so a
        # repeated resolve stays off the command execution path.
        assert resolver.resolve_policies("pfcount") is resolver.resolve_policies(
            "pfcount"
        )
        assert resolver.resolve_policies("ft.cursor") is resolver.resolve_policies(
            "ft.cursor"
        )
        # A command no resolver in the chain carries is memoized as unresolved.
        assert resolver.resolve_policies("cms.query") is None
        assert resolver.resolve_policies("cms.query") is None

    def test_is_cacheable_decides_from_the_shipped_table(self):
        static_resolver = StaticMetadataResolver()

        assert static_resolver.is_cacheable("get") is True
        assert static_resolver.is_cacheable("json.get") is True

        # The entries the table carries precisely because they are *not* cacheable.
        assert static_resolver.is_cacheable("xpending") is False
        assert static_resolver.is_cacheable("ts.info") is False
        assert static_resolver.is_cacheable("xread") is False
        # Readonly but keyless.
        assert static_resolver.is_cacheable("ft.search") is False
        # A write command.
        assert static_resolver.is_cacheable("set") is False

        # Not covered by the table, and nothing behind it: fails closed.
        assert static_resolver.is_cacheable("pfcount") is False
        assert static_resolver.is_cacheable("bf.exists") is False
        # Covered, by records that state their own ineligibility rather than relying on the
        # absence above - which is what keeps them out when a live layer sits behind.
        assert static_resolver.is_cacheable("touch") is False
        assert static_resolver.is_cacheable("eval_ro") is False

    def test_custom_metadata_decides_replica_safety_for_known_module_reads(self):
        resolver = DynamicMetadataResolver(
            {
                "ts": {"get": CACHEABLE_KEYED},
                "ft": {"aggregate": CACHEABLE_KEYED},
            }
        )

        assert resolver.is_replica_safe("ts.get") is True
        assert resolver.is_replica_safe("ft.aggregate") is True

    def test_touch_remains_replica_unsafe_despite_readonly_metadata(self):
        resolver = DynamicMetadataResolver({"core": {"touch": CACHEABLE_KEYED}})

        assert resolver.is_replica_safe("touch") is False

    def test_the_replica_unsafe_commands_are_seeded_into_the_memo(self):
        """
        Seeding the memo at construction is the whole mechanism, so pin it: the entry is in
        place before any command is looked up, which is what lets these answer from the same
        single dict lookup as every other command with no per-call test of their own. The
        record TOUCH would otherwise resolve to reports it readonly, so the seed is the only
        thing keeping it off a replica.
        """
        resolver = DynamicMetadataResolver({"core": {"touch": CACHEABLE_KEYED}})

        assert resolver._replica_safe == {"touch": False}
        assert _is_replica_safe(resolver.resolve("touch")) is True
        assert resolver.is_replica_safe("TOUCH") is False

    def test_static_resolver_decides_replica_safety(self):
        resolver = StaticMetadataResolver()

        # Replica safe commands
        assert resolver.is_replica_safe("get") is True
        assert resolver.is_replica_safe("GET") is True
        assert resolver.is_replica_safe("dbsize") is True
        assert resolver.is_replica_safe("ttl") is True
        assert resolver.is_replica_safe("eval_ro") is True
        assert resolver.is_replica_safe("xread") is True
        assert resolver.is_replica_safe("json.get") is True
        assert resolver.is_replica_safe("ft.search") is True

        # Replica unsafe commands
        assert resolver.is_replica_safe("set") is False
        assert resolver.is_replica_safe("SET") is False
        assert resolver.is_replica_safe("touch") is False
        assert resolver.is_replica_safe("TOUCH") is False
        assert resolver.is_replica_safe("hset") is False
        assert resolver.is_replica_safe("ft.create") is False

        # Non-string or unknown commands
        assert resolver.is_replica_safe(None) is False
        assert resolver.is_replica_safe(123) is False
        assert resolver.is_replica_safe("nosuchcommand") is False

    def test_the_default_eligible_set_differs_from_the_legacy_allow_list_by_exactly_this(
        self,
    ):
        """
        The behaviour delta a user gets by default, pinned so it cannot move silently.

        ``CacheConfig.DEFAULT_ALLOW_LIST`` is what CSC eligibility used to be, and this table
        is what it is now. A table edit that changes what the default path caches has to change
        this test with it.
        """
        static_resolver = StaticMetadataResolver()

        eligible = {
            resolver_name.upper()
            for _, _, resolver_name in static_table_names()
            if static_resolver.is_cacheable(resolver_name)
        }
        allow_list = set(CacheConfig.DEFAULT_ALLOW_LIST)

        # Newly eligible: suggestion-dictionary reads and newly added core/module reads
        # the legacy allow-list never carried.
        assert eligible - allow_list == set(NEWLY_ELIGIBLE_VS_LEGACY_ALLOW_LIST)
        # No longer eligible, and all three server-confirmed defects in the allow-list.
        assert allow_list - eligible == {"XPENDING", "TS.INFO", "XREAD"}

    def test_is_cacheable_fails_closed_for_an_unresolvable_name(self):
        """
        A name the record tables cannot be keyed by is not a raise on the command execution
        path: a raw command carrying such a name must still reach the server. ``resolve``
        keeps reporting the error, because its caller asked about the records themselves.
        """
        static_resolver = StaticMetadataResolver()

        assert static_resolver.is_cacheable("foo.bar.baz") is False

        with pytest.raises(
            ValueError, match="Wrong command or module name: foo.bar.baz"
        ):
            static_resolver.resolve("foo.bar.baz")

        with pytest.raises(
            ValueError, match="Wrong command or module name: foo.bar.baz"
        ):
            static_resolver.resolve_policies("foo.bar.baz")

    @pytest.mark.parametrize(
        "command,trackable",
        [
            # Readonly and keyed: the server remembers every key of the invocation.
            ("GET", True),
            ("MGET", True),
            # Readonly and keyed, but never eligible to store - which is exactly why optout
            # wants to exempt them. TOUCH would be skipped if this read ``is_replica_safe``,
            # which is pre-seeded with ``touch -> False``.
            ("TOUCH", True),
            ("XPENDING", True),
            # Readonly but keyless, so the ``NO`` is a harmless no-op. Reported honestly
            # rather than approximated, because trackability never affects storage.
            ("KEYS", True),
            # Readonly; the server does not remember a script runner's own key arguments,
            # only the reads the script performs. Over-reporting costs one wasted command.
            ("EVAL_RO", True),
            # Absent from the shipped table, so undecidable: fails closed, which skips the
            # ``NO``. Waste, never staleness.
            ("SET", False),
            ("DEL", False),
            ("XREADGROUP", False),
            ("NOSUCHCOMMAND", False),
            ("NOSUCHMODULE.NOSUCHCOMMAND", False),
        ],
    )
    def test_is_trackable_read_decides_from_the_readonly_flag(self, command, trackable):
        assert StaticMetadataResolver().is_trackable_read(command) is trackable

    @pytest.mark.parametrize(
        "command",
        ["a.b.c", b"GET", bytearray(b"GET"), memoryview(b"GET"), 1, None],
        ids=["two-dots", "bytes", "bytearray", "memoryview", "int", "none"],
    )
    def test_is_trackable_read_fails_closed_on_an_undecidable_name(self, command):
        """
        Same contract as ``is_cacheable``: a raw command whose name cannot be decided must
        still reach the server, so this never raises into the command execution path.
        """
        assert StaticMetadataResolver().is_trackable_read(command) is False

    def test_is_trackable_read_is_case_insensitive(self):
        resolver = StaticMetadataResolver()

        for command in ("get", "GET", "Get"):
            assert resolver.is_trackable_read(command) is True

    def test_is_trackable_read_is_abstract_on_the_abc(self):
        """
        A ``MetadataResolver`` must implement ``is_trackable_read`` like its other views.
        """

        class ResolverWithoutTheView(MetadataResolver):
            def resolve(self, command_name):
                return None

            def resolve_policies(self, command_name):
                return None

            def is_cacheable(self, command_name):
                return False

            def is_replica_safe(self, command_name):
                return False

            def with_fallback(self, fallback):
                return self

        with pytest.raises(TypeError, match="is_trackable_read"):
            ResolverWithoutTheView()

    def test_the_views_are_case_insensitive(self):
        """
        The client-side cache resolves the command name as the command methods spell it,
        while the cluster client lowercases first.
        """
        resolver = DynamicMetadataResolver(
            {
                "core": {"memory usage": CACHEABLE_KEYED},
                "json": {"get": CACHEABLE_KEYED},
            }
        )

        for name in ("MEMORY USAGE", "memory usage", "JSON.GET", "json.get"):
            assert resolver.resolve(name) is CACHEABLE_KEYED, name
            assert policy_pair(resolver.resolve_policies(name)) == KEYED_POLICIES, name
            assert resolver.is_cacheable(name) is True, name

    def test_caller_supplied_records_do_not_have_to_be_keyed_lowercase(self):
        """
        A lookup is lowercased, so records keyed any other way would silently resolve to
        nothing. Records read from a ``COMMAND`` reply are already lowercase - both parsers
        lowercase every name as they read it - but caller-supplied ones carry whatever
        spelling the caller chose, so they are normalized on the way in.
        """
        resolver = DynamicMetadataResolver(
            {
                "core": {
                    "ZCOUNT": CACHEABLE_KEYED,
                    "MEMORY USAGE": CACHEABLE_KEYED,
                },
                "JSON": {"GET": CACHEABLE_KEYED},
            }
        )

        for name in ("ZCOUNT", "zcount", "MEMORY USAGE", "JSON.GET", "json.get"):
            assert resolver.resolve(name) is CACHEABLE_KEYED, name
            assert policy_pair(resolver.resolve_policies(name)) == KEYED_POLICIES, name
            assert resolver.is_cacheable(name) is True, name

    def test_records_already_keyed_lowercase_are_not_rebuilt(self):
        """
        The common case pays nothing: a ``COMMAND`` reply arrives lowercase, so the records are
        served as given rather than copied per resolver.
        """
        records = {"core": {"zcount": CACHEABLE_KEYED}}

        assert DynamicMetadataResolver(records)._metadata is records

    def test_the_views_are_memoized_independently(self):
        resolver = CountingStaticMetadataResolver()

        assert resolver.is_cacheable("get") is True
        assert resolver.is_cacheable("get") is True
        assert resolver.resolve_calls == 1

        # A second view of the same command is resolved once more, then memoized itself.
        assert policy_pair(resolver.resolve_policies("get")) == KEYED_POLICIES
        assert policy_pair(resolver.resolve_policies("get")) == KEYED_POLICIES
        assert resolver.resolve_calls == 2

    def test_the_memos_are_keyed_by_the_normalized_name(self):
        """
        One command takes one memo entry however the caller spells it: the command methods
        write ``GET``, the cluster client lowercases first, and a raw ``execute_command`` may
        carry anything, so keying by the name as asked would hold the same answer several
        times over and spend the cap on spellings.
        """
        resolver = CountingStaticMetadataResolver()

        for name in ("GET", "get", "Get"):
            assert resolver.is_cacheable(name) is True, name
            assert policy_pair(resolver.resolve_policies(name)) == KEYED_POLICIES, name

        # One record lookup per view, not one per spelling.
        assert resolver.resolve_calls == 2
        assert list(resolver._cacheable) == ["get"]
        assert list(resolver._policies) == ["get"]

    def test_resolve_policies_reports_an_unresolvable_name_as_spelled(self):
        """
        The memo is keyed lowercase, but the error still quotes the caller's spelling: it is
        their input that has to be recognizable in the message.
        """
        with pytest.raises(
            ValueError, match="Wrong command or module name: FOO.BAR.BAZ"
        ):
            StaticMetadataResolver().resolve_policies("FOO.BAR.BAZ")

    def test_is_cacheable_refuses_an_incomplete_record_that_shadows_a_complete_one(
        self,
    ):
        """
        A resolver answers from the first record it finds, so an incomplete record decides
        the command even when a complete one sits behind it. Fail closed rather than let the
        cacheability view disagree with ``resolve`` about which record answered - which is
        why an override table belongs in front of a resolver that may serve incomplete
        records, not behind it.
        """
        dynamic_resolver = DynamicMetadataResolver(
            {"core": {"get": replace(CACHEABLE_KEYED, has_complete_metadata=False)}}
        )
        static_resolver = StaticMetadataResolver()
        resolver = dynamic_resolver.with_fallback(static_resolver)

        assert static_resolver.is_cacheable("get") is True
        assert resolver.resolve("get").has_complete_metadata is False
        assert resolver.is_cacheable("get") is False

    def test_a_resolver_built_from_policy_records_reports_nothing_cacheable(self):
        """
        Policy records carry only the routing view, so every cacheability field of a record
        lifted from them keeps its fail-closed default.
        """
        resolver = DynamicMetadataResolver(
            _build_commands_metadata_cache_from_policies(
                {"core": {"get": CommandPolicies(*KEYED_POLICIES)}}
            )
        )

        assert policy_pair(resolver.resolve_policies("get")) == KEYED_POLICIES
        assert resolver.is_cacheable("get") is False

    def test_the_adapter_reads_the_metadata_from_the_parser(self):
        commands_parser = metadata_parser({"core": {"pfcount": CACHEABLE_KEYED}})

        dynamic_resolver = DynamicMetadataResolver(
            _load_commands_metadata_cache(commands_parser)
        )

        commands_parser.get_commands_metadata_cache.assert_called_once_with()
        assert dynamic_resolver.resolve("pfcount") is CACHEABLE_KEYED

        # A chained resolver serves the snapshot it already holds, so the parser is read once.
        chained_resolver = dynamic_resolver.with_fallback(StaticMetadataResolver())

        assert commands_parser.get_commands_metadata_cache.call_count == 1
        assert chained_resolver.resolve("pfcount") is CACHEABLE_KEYED

    def test_the_policy_resolver_rereads_the_parser_when_chained(self):
        """
        Released behaviour: ``DynamicPolicyResolver`` owns the parser and rebuilds from it, so
        a chained routing resolver reads the server rather than freezing a snapshot.
        """
        commands_parser = metadata_parser({"core": {"pfcount": CACHEABLE_KEYED}})
        resolver = DynamicPolicyResolver(commands_parser)

        resolver.with_fallback(StaticPolicyResolver())

        assert commands_parser.get_commands_metadata_cache.call_count == 2

    def test_a_parser_serving_only_the_deprecated_policy_view_is_accepted(self):
        """
        ``get_command_policies`` was the whole contract a parser had to satisfy in 7.1.0 -
        ``DynamicPolicyResolver`` called nothing else - so a caller-supplied parser-shaped
        object may implement only it, and must keep working.
        """
        resolver = DynamicMetadataResolver(
            _load_commands_metadata_cache(legacy_policy_parser(LEGACY_POLICY_RECORDS))
        )

        assert policy_pair(resolver.resolve_policies("dbsize")) == (
            RequestPolicy.ALL_SHARDS,
            ResponsePolicy.AGG_SUM,
        )

        # The same object reaches the cluster client through the policy resolver.
        assert policy_pair(
            DynamicPolicyResolver(legacy_policy_parser(LEGACY_POLICY_RECORDS)).resolve(
                "dbsize"
            )
        ) == (RequestPolicy.ALL_SHARDS, ResponsePolicy.AGG_SUM)

    def test_a_policy_only_parser_supplies_no_cacheability_metadata(self):
        """
        Policy records carry the routing view alone, so every other field keeps its fail-closed
        default and nothing such a parser serves reports as cacheable - the conservative answer
        for metadata that was never supplied.
        """
        resolver = DynamicMetadataResolver(
            _load_commands_metadata_cache(legacy_policy_parser(LEGACY_POLICY_RECORDS))
        )

        assert resolver.resolve("dbsize").has_complete_metadata is False
        assert resolver.is_cacheable("dbsize") is False

    def test_a_policy_only_parser_does_not_have_to_be_keyed_lowercase(self):
        resolver = DynamicMetadataResolver(
            _load_commands_metadata_cache(
                legacy_policy_parser({"CORE": {"DBSIZE": LEGACY_POLICIES}})
            )
        )

        assert policy_pair(resolver.resolve_policies("dbsize")) == (
            RequestPolicy.ALL_SHARDS,
            ResponsePolicy.AGG_SUM,
        )

    def test_the_metadata_view_wins_when_a_parser_serves_both(self):
        commands_parser = metadata_parser({"core": {"pfcount": CACHEABLE_KEYED}})

        resolver = DynamicMetadataResolver(
            _load_commands_metadata_cache(commands_parser)
        )

        commands_parser.get_commands_metadata_cache.assert_called_once_with()
        commands_parser.get_command_policies.assert_not_called()
        assert resolver.resolve("pfcount") is CACHEABLE_KEYED

    def test_a_parser_serving_neither_view_is_rejected(self):
        with pytest.raises(
            TypeError,
            match="object serves neither get_commands_metadata_cache\\(\\) nor "
            "get_command_policies\\(\\)",
        ):
            _load_commands_metadata_cache(object())

    def test_the_metadata_records_are_required_at_the_call_site(self):
        """A resolver cannot be built without records to resolve through."""
        with pytest.raises(TypeError, match="metadata_records"):
            DynamicMetadataResolver()

    def test_resolve_container_subcommand(self):
        """Container subcommands are keyed space-joined, the form ``args[0]`` carries."""
        memory_usage_metadata = CACHEABLE_KEYED
        resolver = DynamicMetadataResolver(
            {"core": {"memory usage": memory_usage_metadata}}
        )

        assert resolver.resolve("memory usage") is memory_usage_metadata
        assert resolver.resolve("memory") is None

    def test_static_resolver_serves_the_shipped_table(self):
        static_resolver = StaticMetadataResolver()

        get_metadata = static_resolver.resolve("get")
        assert get_metadata.is_readonly is True
        assert get_metadata.has_key_argument is True
        assert get_metadata.is_dont_cache is False
        assert get_metadata.has_complete_metadata is True

        # The two entries the table carries precisely because they are *not* cacheable.
        assert static_resolver.resolve("xpending").has_nondeterministic_output is True
        assert static_resolver.resolve("ts.info").is_dont_cache is True

        # Readonly but keyless, so not cacheable either.
        ft_search_metadata = static_resolver.resolve("ft.search")
        assert ft_search_metadata.is_readonly is True
        assert ft_search_metadata.has_key_argument is False

        # Not covered by the table, and nothing behind it: fails closed.
        assert static_resolver.resolve("pfcount") is None
        assert static_resolver.resolve("bf.exists") is None

    def test_dynamic_first_falls_back_to_static(self):
        dynamic_resolver = DynamicMetadataResolver(
            {"core": {"zcount": CACHEABLE_KEYED}}
        )
        resolver = dynamic_resolver.with_fallback(StaticMetadataResolver())

        resolved_metadata = resolver.resolve("ft.aggregate")
        assert resolved_metadata.request_policy == RequestPolicy.DEFAULT_KEYLESS
        assert resolved_metadata.response_policy == ResponsePolicy.DEFAULT_KEYLESS
        assert resolved_metadata.is_dont_cache is True

    def test_static_first_falls_back_to_dynamic_for_uncovered_command(self):
        """
        Static first, live metadata behind it: the static table is the override layer and
        the server answers for everything the table does not carry.
        """
        dynamic_resolver = DynamicMetadataResolver(
            {
                # PFCOUNT is readonly and keyed, and the static table does not carry
                # it. "core" is a module the static table does know, so this covers the
                # command-level miss: a known module with an unknown command must still
                # fall through.
                "core": {"pfcount": CACHEABLE_KEYED},
                # BF.EXISTS is a whole module surface the static table does not carry,
                # which covers the module-level miss.
                "bf": {"exists": CACHEABLE_KEYED},
                # The server reports TS.INFO with the dont_cache tip. Here it
                # deliberately does not, so that the static entry answering instead is
                # observable.
                "ts": {"info": CACHEABLE_KEYED},
            }
        )
        static_resolver = StaticMetadataResolver()
        resolver = static_resolver.with_fallback(dynamic_resolver)

        assert static_resolver.resolve("pfcount") is None
        assert resolver.resolve("pfcount") is CACHEABLE_KEYED
        assert resolver.resolve("bf.exists") is CACHEABLE_KEYED

        # TS.INFO is carried by both, so the static entry wins and the command stays
        # uncacheable - this is what makes the static layer an override.
        assert resolver.resolve("ts.info").is_dont_cache is True

        # A command neither resolver carries still fails closed.
        assert resolver.resolve("cms.query") is None

    def test_with_fallback_returns_a_new_resolver(self):
        static_resolver = StaticMetadataResolver()
        dynamic_resolver = DynamicMetadataResolver(
            {"core": {"pfcount": CACHEABLE_KEYED}}
        )

        resolver = static_resolver.with_fallback(dynamic_resolver)

        assert resolver is not static_resolver
        # The receiver is left untouched, so a chain can be built from a shared resolver.
        assert static_resolver.resolve("pfcount") is None
        assert resolver.resolve("pfcount") is CACHEABLE_KEYED

    def test_extended_fallback_chain(self):
        foo_bar_metadata = replace(CACHEABLE_KEYED, has_key_argument=False)

        dynamic_resolver = DynamicMetadataResolver(
            {"core": {"zcount": CACHEABLE_KEYED}}
        )
        another_dynamic_resolver = DynamicMetadataResolver(
            {"foo": {"bar": foo_bar_metadata}}
        )
        static_resolver = StaticMetadataResolver()

        resolver = dynamic_resolver.with_fallback(
            static_resolver.with_fallback(another_dynamic_resolver)
        )

        # Resolved by the last resolver in the chain.
        assert resolver.resolve("foo.bar") is foo_bar_metadata
        # Resolved by the first.
        assert resolver.resolve("zcount") is CACHEABLE_KEYED
        # Resolved by the middle one.
        assert resolver.resolve("ts.info").is_dont_cache is True
        # Not resolved by any of them.
        assert resolver.resolve("cms.query") is None


@pytest.mark.fixed_client
class TestCommandReplyNormalization:
    """
    Both views of a ``COMMAND`` reply must read it the same way whether it decodes to bytes or
    to str.

    A reply arrives as bytes on a plain client and as str under ``decode_responses=True``, and
    the traversal that resolves the policy tips is shared by the policy view and the metadata
    view. Both stacks share the traversal, so only the sync suite covers it.
    """

    # One ``COMMAND`` entry, in the shape ``CommandsParser.initialize`` leaves behind. GEODIST
    # is readonly and keyed, and the tips override the keyed default so that the tip traversal
    # is observable rather than reproducing the default it started from.
    @staticmethod
    def command_reply(encode):
        return {
            "geodist": {
                "name": encode("geodist"),
                "arity": -4,
                "flags": [encode("readonly")],
                "first_key_pos": 1,
                "last_key_pos": 1,
                "step_count": 1,
                "tips": [
                    encode("request_policy:all_shards"),
                    encode("response_policy:agg_sum"),
                    encode("nondeterministic_output"),
                ],
                "key_specifications": [
                    {encode("flags"): [encode("RO"), encode("ACCESS")]}
                ],
                "subcommands": [],
            }
        }

    def test_the_metadata_view_reads_bytes_and_str_alike(self):
        as_bytes = _build_commands_metadata_cache(self.command_reply(str.encode))
        as_str = _build_commands_metadata_cache(self.command_reply(str))

        assert as_bytes == as_str

        metadata = as_str["core"]["geodist"]
        # The tips were applied, so this is the traversal's answer and not the keyed default.
        assert metadata.request_policy is RequestPolicy.ALL_SHARDS
        assert metadata.response_policy is ResponsePolicy.AGG_SUM
        assert metadata.is_readonly is True
        assert metadata.has_key_argument is True
        assert metadata.has_nondeterministic_output is True
        assert metadata.has_complete_metadata is True

    def test_the_policy_view_reads_bytes_and_str_alike(self):
        as_bytes = _build_policy_records(self.command_reply(str.encode))
        as_str = _build_policy_records(self.command_reply(str))

        for records in (as_bytes, as_str):
            assert policy_pair(records["core"]["geodist"]) == (
                RequestPolicy.ALL_SHARDS,
                ResponsePolicy.AGG_SUM,
            )

    # Same entry, with every tip buried one array and one map deep. A ``COMMAND`` reply reports
    # tips flat, so this is not a shape a server sends; it is here because the traversal is
    # shared, and a metadata record is the superset of a policy record - it carries the two
    # routing policies plus the cacheability markers, all off this one field. Reading less of
    # the field than the routing view does would make the superset the smaller reader.
    @staticmethod
    def nested_command_reply(encode):
        reply = TestCommandReplyNormalization.command_reply(encode)
        reply["geodist"]["tips"] = [
            [encode("request_policy:all_shards")],
            {encode("wrapped"): [encode("response_policy:agg_sum")]},
            [[encode("nondeterministic_output")]],
        ]
        return reply

    @pytest.mark.parametrize("encode", (str.encode, str), ids=("bytes", "str"))
    def test_both_views_read_nested_tips_alike(self, encode):
        # Neither view may raise, and both must find every tip the fragment carries.
        metadata = _build_commands_metadata_cache(self.nested_command_reply(encode))
        policies = _build_policy_records(self.nested_command_reply(encode))

        record = metadata["core"]["geodist"]
        assert (record.request_policy, record.response_policy) == (
            RequestPolicy.ALL_SHARDS,
            ResponsePolicy.AGG_SUM,
        )
        # The marker the routing view ignores and only the metadata view reads.
        assert record.has_nondeterministic_output is True

        # The routing view agrees, which is what makes the two views one reading of one field.
        assert policy_pair(policies["core"]["geodist"]) == (
            RequestPolicy.ALL_SHARDS,
            ResponsePolicy.AGG_SUM,
        )

    def test_nesting_does_not_change_what_either_view_resolves(self):
        # The nesting is transparent: same answers as the flat reply it was built from.
        assert _build_commands_metadata_cache(
            self.nested_command_reply(str.encode)
        ) == _build_commands_metadata_cache(self.command_reply(str.encode))
        assert policy_pair(
            _build_policy_records(self.nested_command_reply(str.encode))["core"][
                "geodist"
            ]
        ) == policy_pair(
            _build_policy_records(self.command_reply(str.encode))["core"]["geodist"]
        )


@pytest.mark.redismod
class TestCommandsParserMetadata:
    def test_get_command_policies_matches_the_metadata_view(self, stack_r):
        """
        The two views of one reply agree. ``get_command_policies`` is the routing view: same
        traversal, same keys, projected down to the two policies a cluster client routes by.
        """
        commands_parser = CommandsParser(stack_r)

        policy_records = commands_parser.get_command_policies()

        metadata_records = commands_parser.get_commands_metadata_cache()

        # Same commands, and the policies of each are the ones the metadata record carries.
        assert policy_records.keys() == metadata_records.keys()
        for module, commands in policy_records.items():
            assert commands.keys() == metadata_records[module].keys()
            for command, policies in commands.items():
                metadata = metadata_records[module][command]
                assert policy_pair(policies) == (
                    metadata.request_policy,
                    metadata.response_policy,
                )


@pytest.mark.redismod
class TestStaticMetadataAgainstServer:
    """
    Drift guard for ``_STATIC_COMMAND_METADATA``.

    The table is generated from a live ``COMMAND`` reply, so every entry must keep saying
    what the server says. Within a test, an assertion on a tip the running server may not
    report yet is gated on the server reporting that tip for any command at all.

    The tests that read the live reply for the richer flags and tips 8.10 started reporting -
    the ``script_runner`` flag, and the ``dont_cache`` tip on the module surfaces - are gated on
    that release. It is also the server the table was generated from, and the first that reports
    every command the table carries (FT.ALIASLIST, and VRANDMEMBER below 8.0). Nothing those
    commands do changed in the server; it only started describing them accurately, and the static
    table already carries the right data. A test that holds on any server - one that reads no live
    metadata, or only the absence of a tip - is left ungated.

    The one exception is ``LIVE_CACHEABILITY_DIVERGENCE``, whose divergence from the reply is
    the point of the record; it is pinned in both directions, so the record cannot rot and the
    day the server starts agreeing shows up as a failure here.
    """

    @skip_if_server_version_lt(STATIC_TABLE_SERVER_VERSION)
    def test_static_metadata_matches_live_command_metadata(self, stack_r):
        live_commands = live_command_details(stack_r.command())
        static_resolver = StaticMetadataResolver()

        reports_dont_cache = any(
            "dont_cache" in command_tips(details) for details in live_commands.values()
        )
        reports_script_runner = any(
            "script_runner" in command_flags(details)
            for details in live_commands.values()
        )

        checked = 0
        for _, _, name in static_table_names():
            details = live_commands.get(name)
            assert details is not None, (
                f"{name} is in the static table but not in the COMMAND reply"
            )

            metadata = static_resolver.resolve(name)
            flags = command_flags(details)
            tips = command_tips(details)
            diverging_field = LIVE_CACHEABILITY_DIVERGENCE.get(name)

            assert metadata.is_readonly == ("readonly" in flags), name
            # The blocking flag is reported by every server that reports flags at all, so
            # this needs no gate.
            assert metadata.is_blocking == ("blocking" in flags), name
            assert metadata.has_key_argument == live_has_key_argument(details), name
            if diverging_field != "has_nondeterministic_output":
                assert metadata.has_nondeterministic_output == (
                    "nondeterministic_output" in tips
                ), name

            # A missing tips key, unlike an empty one, makes the negative markers
            # undetectable; every entry in the table claims complete metadata.
            assert "tips" in details, name
            assert metadata.has_complete_metadata is True, name

            if reports_script_runner:
                assert metadata.is_script_runner == ("script_runner" in flags), name
            if reports_dont_cache and diverging_field != "is_dont_cache":
                assert metadata.is_dont_cache == ("dont_cache" in tips), name

            # The routing invariant, decided from the live reply rather than from the table:
            # a command the server says takes a key name argument must be routed to the node
            # holding that key, so its entry either says DEFAULT_KEYED or says nothing. This
            # catches a server-side metadata change that a table-only assertion cannot - a
            # command that gains a key argument, or one whose key specs start reporting one.
            if live_has_key_argument(details):
                assert metadata.request_policy in (
                    RequestPolicy.DEFAULT_KEYED,
                    None,
                ), name

            checked += 1

        assert checked == sum(
            len(commands) for commands in _STATIC_COMMAND_METADATA.values()
        )

    @skip_if_server_version_lt(STATIC_TABLE_SERVER_VERSION)
    def test_eligibility_matches_the_specified_rules(self, stack_r):
        """
        The cacheability view, decided from live server metadata rather than the table.

        One case per rule of the specification, so a metadata change on the server surfaces
        here rather than silently changing what the client caches.
        """
        dynamic_resolver = DynamicMetadataResolver(
            _load_commands_metadata_cache(CommandsParser(stack_r))
        )
        live_commands = live_command_details(stack_r.command())
        reports_script_runner = any(
            "script_runner" in command_flags(details)
            for details in live_commands.values()
        )

        # Readonly, keyed, nothing that forbids caching - including module surfaces, which
        # are decided by metadata rather than excluded by prefix.
        for name in (
            "get",
            "hgetall",
            "memory usage",
            "json.get",
            "ts.get",
            "bf.exists",
        ):
            assert dynamic_resolver.is_cacheable(name) is True, name

        # Readonly but keyless.
        assert dynamic_resolver.is_cacheable("keys") is False
        assert dynamic_resolver.is_cacheable("ft.search") is False
        # Nondeterministic output.
        assert dynamic_resolver.is_cacheable("xpending") is False
        # No readonly flag.
        assert dynamic_resolver.is_cacheable("xreadgroup") is False
        # Blocking, even though it is readonly and keyed.
        assert dynamic_resolver.is_cacheable("xread") is False
        # Tipped dont_cache.
        assert dynamic_resolver.is_cacheable("ts.info") is False
        # A shard channel is not a key name.
        assert dynamic_resolver.is_cacheable("ssubscribe") is False
        # Unknown to the server, so unproven and not cached.
        assert dynamic_resolver.is_cacheable("nosuchmodule.nosuchcommand") is False

        if reports_script_runner:
            for name in ("eval_ro", "evalsha_ro", "fcall_ro"):
                assert dynamic_resolver.is_cacheable(name) is False, name

        # TOUCH is readonly and keyed and the server tips it nothing, so server metadata
        # alone cannot exclude it, even though it must reach the server on every call. The
        # static table is what excludes it - see
        # ``test_the_recorded_divergence_from_the_live_reply_is_still_needed`` - as it also
        # does for the script runners above on a server that predates the script_runner flag.
        assert dynamic_resolver.is_cacheable("touch") is True

    @skip_if_server_version_lt(STATIC_TABLE_SERVER_VERSION)
    def test_dynamic_resolver_matches_the_static_table(self, stack_r):
        """
        Both resolver paths must produce the same record for the same command.

        The routing policies are compared apart from the cacheability inputs because the
        table diverges from the server on purpose for four reasons:

        - EXISTS and MGET are tipped ``request_policy:multi_shard``, which neither client
          stack implements; the table records the keyed defaults instead.
        - FT.CURSOR is SPECIAL, a client-side routing decision the server does not tip.
        - The ``movablekeys`` reads withhold their policies entirely. The derived policies come
          out keyless because their keys are only in the key specs, so recording them would
          route the command to an arbitrary node instead of the one holding its keys.
        - The records that state their own ineligibility withhold theirs for the same reason:
          adding one must not start routing a command the cluster client resolves itself.

        ``LIVE_CACHEABILITY_DIVERGENCE`` is the one place the *cacheability* inputs are allowed
        to disagree, and the disagreement is pinned rather than skipped.
        """
        dynamic_resolver = DynamicMetadataResolver(
            _load_commands_metadata_cache(CommandsParser(stack_r))
        )
        static_resolver = StaticMetadataResolver()

        diverged = set()
        for _, _, name in static_table_names():
            live_metadata = dynamic_resolver.resolve(name)
            assert live_metadata is not None, (
                f"{name} is in the static table but not in the COMMAND reply"
            )

            static_metadata = static_resolver.resolve(name)
            diverging_field = LIVE_CACHEABILITY_DIVERGENCE.get(name)
            if diverging_field is None:
                assert cacheability_fields(live_metadata) == cacheability_fields(
                    static_metadata
                ), name
                # The same effective metadata must produce the same cacheability decision, no
                # matter which resolver path answered.
                assert dynamic_resolver.is_cacheable(
                    name
                ) == static_resolver.is_cacheable(name), name
            else:
                # Only the recorded field may differ: mask it and compare the rest, so a
                # deliberate divergence on one field does not stop guarding the other six.
                masked = replace(
                    static_metadata,
                    **{diverging_field: getattr(live_metadata, diverging_field)},
                )
                assert cacheability_fields(live_metadata) == cacheability_fields(
                    masked
                ), name

            if (live_metadata.request_policy, live_metadata.response_policy) != (
                static_metadata.request_policy,
                static_metadata.response_policy,
            ):
                diverged.add(name)

        assert diverged == {
            "exists",
            "mget",
            "ft.cursor",
            *ALL_WITHHELD_ROUTING_COMMANDS,
        }

    # VRANDMEMBER is a vectorset command, so it is only reported from Redis 8.0 on.
    @skip_if_server_version_lt("8.0.0")
    @pytest.mark.parametrize("name,field", LIVE_CACHEABILITY_DIVERGENCE.items())
    def test_the_recorded_divergence_from_the_live_reply_is_still_needed(
        self, stack_r, name, field
    ):
        """
        The hand-maintained divergences, pinned in both directions.

        Each record exists because the server reports the command as cacheable when it is not:
        TOUCH has a server-side effect (it refreshes idle time), and VRANDMEMBER samples
        randomly. If the server ever starts reporting either correctly, this fails and the
        record can be deleted.
        """
        dynamic_resolver = DynamicMetadataResolver(
            _load_commands_metadata_cache(CommandsParser(stack_r))
        )
        static_resolver = StaticMetadataResolver()

        # The server says cacheable...
        assert getattr(dynamic_resolver.resolve(name), field) is False, name
        assert dynamic_resolver.is_cacheable(name) is True, name
        # ...the table says otherwise, and the table is right.
        assert getattr(static_resolver.resolve(name), field) is True, name
        assert static_resolver.is_cacheable(name) is False, name

        # And the static-first chain is what makes the record reachable with a live layer
        # behind it, which is why that ordering is the documented one.
        chain = static_resolver.with_fallback(dynamic_resolver)
        assert chain.is_cacheable(name) is False, name

    @skip_if_server_version_lt(STATIC_TABLE_SERVER_VERSION)
    def test_dynamic_resolver_covers_commands_the_static_table_does_not(self, stack_r):
        """
        The commands the table leaves out, resolved from the server instead. These are the
        normalization paths the static entries do not exercise.
        """
        dynamic_resolver = DynamicMetadataResolver(
            _load_commands_metadata_cache(CommandsParser(stack_r))
        )

        # Readonly, keyed, nothing that forbids caching - and absent from the table.
        pfcount_metadata = dynamic_resolver.resolve("pfcount")
        assert pfcount_metadata.is_readonly is True
        assert pfcount_metadata.has_key_argument is True
        assert pfcount_metadata.is_dont_cache is False

        # A whole module surface the table does not carry.
        assert dynamic_resolver.resolve("bf.exists").is_readonly is True

        # Readonly but keyless, so not cacheable.
        assert dynamic_resolver.resolve("keys").has_key_argument is False

        # The script_runner flag, as the server reports it - the table carries its own
        # record for EVAL_RO, for the servers that do not. EVAL_RO is also movablekeys, so
        # its key argument is only visible in the key specs.
        eval_ro_metadata = dynamic_resolver.resolve("eval_ro")
        assert eval_ro_metadata.is_script_runner is True
        assert eval_ro_metadata.has_key_argument is True

        # SSUBSCRIBE takes a shard channel, not a key: the server reports a key spec
        # flagged not_key while the legacy positions still say first_key_pos 1.
        assert dynamic_resolver.resolve("ssubscribe").has_key_argument is False

        # Container subcommands resolve under their space-joined name.
        assert dynamic_resolver.resolve("memory usage").is_readonly is True

    @skip_if_server_version_lt(STATIC_TABLE_SERVER_VERSION)
    def test_the_live_chain_broadens_eligibility_only_through_its_fallback(
        self, stack_r
    ):
        """
        The guard on the opt-in surface: a user who chains a live resolver behind the static one
        gets more cacheable commands, and every one of them must come from the fallback layer.

        A command the table carries must keep the table's verdict, so a server that starts
        reporting something the table contradicts cannot silently change what is cached. The
        broadening is pinned by size rather than command by command, because it is dominated by
        module surfaces whose metadata moves with the module version rather than with this
        client - a count still moves when the image does, but it moves visibly, which is the
        point. Regenerate it by printing ``sorted(eligible - allow_list)`` here.
        """
        chain = StaticMetadataResolver().with_fallback(
            DynamicMetadataResolver(
                _load_commands_metadata_cache(CommandsParser(stack_r))
            )
        )
        table_names = {resolver_name for _, _, resolver_name in static_table_names()}
        allow_list = {name.lower() for name in CacheConfig.DEFAULT_ALLOW_LIST}

        eligible = {
            name.lower()
            for name in stack_r.command()
            if chain.is_cacheable(name.lower())
        }

        # Nothing the allow-list carried is dropped beyond the three the server itself reports
        # as not cacheable.
        assert allow_list - eligible == {"xpending", "ts.info", "xread"}
        # Everything newly cacheable is either a command the table does not carry - so the
        # broadening is attributable to the fallback rather than to a table edit - or one of
        # the table's own additions over the legacy allow-list, which the default path pins
        # separately.
        assert (eligible - allow_list) & table_names == {
            name.lower() for name in NEWLY_ELIGIBLE_VS_LEGACY_ALLOW_LIST
        }
        # And it is exactly this large, so a server or module version that starts reporting a
        # command as cacheable when it should not be surfaces here rather than silently
        # broadening what an opt-in user caches. Measured against a Redis 8.11.240 stack
        # build: 56 commands - core reads, the probabilistic, t-digest and timeseries read
        # surfaces, and the vectorset reads.
        assert len(eligible - allow_list) == 56
        # And the commands the specification names stay out, whichever layer answers.
        for name in (
            "keys",
            "xpending",
            "eval_ro",
            "evalsha_ro",
            "fcall_ro",
            "touch",
            "vrandmember",
            "xreadgroup",
            "xread",
            "ts.info",
        ):
            assert name not in eligible, name

    def test_static_first_falls_back_to_the_server(self, stack_r):
        """The static-first chain of the unit tests, resolved against a real server."""
        static_resolver = StaticMetadataResolver()
        resolver = static_resolver.with_fallback(
            DynamicMetadataResolver(
                _load_commands_metadata_cache(CommandsParser(stack_r))
            )
        )

        # Not in the table, so the server answers.
        assert static_resolver.resolve("pfcount") is None
        assert resolver.resolve("pfcount").is_readonly is True

        # In the table, so the table answers - and keeps XPENDING and TS.INFO out of the
        # cache even though both are readonly and keyed on the server.
        assert resolver.resolve("xpending").has_nondeterministic_output is True
        assert resolver.resolve("ts.info").is_dont_cache is True

        # Unknown to the table and to the server.
        assert resolver.resolve("nosuchmodule.nosuchcommand") is None
