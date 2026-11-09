from dataclasses import replace

import pytest
import pytest_asyncio

from redis._parsers import AsyncCommandsParser
from redis.asyncio.cluster import RedisCluster
from redis.commands.metadata import (
    _MEMO_MAX_ENTRIES,
    _is_replica_safe,
    _STATIC_COMMAND_METADATA,
    AsyncDynamicMetadataResolver,
    AsyncMetadataResolver,
    AsyncStaticMetadataResolver,
    CommandMetadata,
    CommandMetadataRecordsCache,
    CommandPolicies,
    RequestPolicy,
    ResponsePolicy,
    _build_commands_metadata_cache_from_policies,
)
from redis.commands.policies import (
    AsyncBasePolicyResolver,
    AsyncDynamicPolicyResolver,
    AsyncPolicyResolver,
    AsyncStaticPolicyResolver,
)
from tests.conftest import skip_if_server_version_lt
from tests.test_command_metadata import (
    ALL_WITHHELD_ROUTING_COMMANDS,
    CACHEABLE_KEYED,
    INELIGIBLE_RECORD_COMMANDS,
    KEYED_POLICIES,
    KILL_COMMANDS,
    LIVE_CACHEABILITY_DIVERGENCE,
    MULTI_SHARD_SUMMED_POLICIES,
    MOVABLE_KEYS_READS,
    ONE_SUCCEEDED_POLICIES,
    PER_NODE_DIAGNOSTICS,
    SCRIPT_RUNNER_READS,
    STATIC_TABLE_SERVER_VERSION,
    TABLE_ROUTED_FLAG_COMMANDS,
    WITHHELD_KEYLESS_READS,
    cacheability_fields,
    command_flags,
    command_tips,
    live_command_details,
    live_has_key_argument,
    policy_pair,
    static_table_names,
)


@pytest_asyncio.fixture()
async def stack_client(create_redis, stack_url):
    return await create_redis(url=stack_url)


class MetadataPolicyResolver(AsyncBasePolicyResolver):
    """
    An async policy resolver over a given metadata resolver.

    ``AsyncDynamicPolicyResolver`` takes policy records, which carry no cacheability metadata,
    so it cannot stand in for a live metadata layer. ``AsyncStaticPolicyResolver`` accepts a
    metadata resolver but puts the static table in front of it. This serves the given resolver
    directly, which is what the routing-view tests below need.
    """

    def __init__(
        self,
        metadata_resolver: AsyncStaticMetadataResolver | AsyncDynamicMetadataResolver,
        fallback: AsyncPolicyResolver | None = None,
    ) -> None:
        self._resolver = metadata_resolver
        self._init_from_metadata_resolver(metadata_resolver, fallback)

    def with_fallback(self, fallback: AsyncPolicyResolver) -> AsyncPolicyResolver:
        return MetadataPolicyResolver(self._resolver, fallback)


class CountingAsyncStaticMetadataResolver(AsyncStaticMetadataResolver):
    """The shipped static resolver, counting how often the record lookup runs."""

    def __init__(self) -> None:
        super().__init__()
        self.resolve_calls = 0

    async def resolve(self, command_name: str) -> CommandMetadata | None:
        self.resolve_calls += 1
        return await super().resolve(command_name)


async def live_metadata_records(client) -> CommandMetadataRecordsCache:
    """
    The metadata records of the connected server.

    ``AsyncDynamicMetadataResolver`` takes the records rather than the parser, because
    ``AsyncCommandsParser.get_commands_metadata_cache`` cannot be awaited in a constructor.
    """
    commands_parser = AsyncCommandsParser()
    await commands_parser.initialize(client)

    return await commands_parser.get_commands_metadata_cache()


@pytest.mark.asyncio
@pytest.mark.fixed_client
class TestWithheldRoutingPolicies:
    """
    Async mirror of the resolver behaviour in ``tests.test_command_metadata``.

    The projection function, the shipped table and ``STATIC_POLICIES`` are module-level and
    shared by both stacks, so only the sync suite covers them.
    """

    async def test_the_movablekeys_reads_record_the_keyed_policies(self):
        static_resolver = AsyncStaticMetadataResolver()

        for name in (*MOVABLE_KEYS_READS, *SCRIPT_RUNNER_READS):
            metadata = await static_resolver.resolve(name)

            assert metadata is not None, name
            assert policy_pair(metadata) == KEYED_POLICIES, name
            assert metadata.is_readonly is True, name
            assert metadata.has_key_argument is True, name
            assert metadata.has_complete_metadata is True, name

    async def test_the_kill_commands_ask_every_shard_for_one_success(self):
        static_resolver = AsyncStaticMetadataResolver()

        for name in KILL_COMMANDS:
            metadata = await static_resolver.resolve(name)

            assert metadata is not None, name
            assert policy_pair(metadata) == ONE_SUCCEEDED_POLICIES, name
            assert metadata.has_key_argument is False, name
            assert await static_resolver.is_cacheable(name) is False, name

    async def test_the_diagnostics_commands_record_the_server_tips(self):
        static_resolver = AsyncStaticMetadataResolver()

        for name, policies in PER_NODE_DIAGNOSTICS.items():
            metadata = await static_resolver.resolve(name)

            assert metadata is not None, name
            assert policy_pair(metadata) == policies, name
            assert metadata.has_nondeterministic_output is True, name
            assert await static_resolver.is_cacheable(name) is False, name

    async def test_the_movablekeys_reads_keep_their_cacheability(self):
        static_resolver = AsyncStaticMetadataResolver()

        for name in MOVABLE_KEYS_READS:
            # XREAD is the one exclusion here, and the blocking flag is what excludes it.
            assert await static_resolver.is_cacheable(name) is (name != "xread"), name

    async def test_the_ineligible_records_withhold_their_policies(self):
        """
        The records that state their own ineligibility must not state a routing policy the
        server does not give them.

        The table backs both the metadata resolver and ``AsyncStaticPolicyResolver``, so a
        record added for its cacheability inputs alone would otherwise start routing a command
        the cluster client resolves for itself today. The script runners are the exception:
        they are ``movablekeys`` reads, recorded keyed like the others, and TOUCH carries the
        ``multi_shard`` tip the server gives it.
        """
        static_resolver = AsyncStaticMetadataResolver()

        for name in INELIGIBLE_RECORD_COMMANDS:
            metadata = await static_resolver.resolve(name)

            assert metadata is not None, name
            if name in SCRIPT_RUNNER_READS:
                assert policy_pair(metadata) == KEYED_POLICIES, name
            elif name == "touch":
                assert policy_pair(metadata) == MULTI_SHARD_SUMMED_POLICIES, name
            else:
                assert metadata.request_policy is None, name
                assert metadata.response_policy is None, name
            # Ineligible for the reason the record states, not for want of metadata.
            assert metadata.is_readonly is True, name
            assert metadata.has_key_argument is True, name
            assert metadata.has_complete_metadata is True, name
            assert await static_resolver.is_cacheable(name) is False, name

        # Each record states *why*, so the reason survives a regeneration of the table.
        assert (await static_resolver.resolve("touch")).is_dont_cache is True
        assert (
            await static_resolver.resolve("vrandmember")
        ).has_nondeterministic_output is True
        for name in ("eval_ro", "evalsha_ro", "fcall_ro"):
            assert (await static_resolver.resolve(name)).is_script_runner is True, name

    async def test_keyless_reads_withhold_routing_and_are_replica_safe(self):
        static_resolver = AsyncStaticMetadataResolver()
        for name in WITHHELD_KEYLESS_READS:
            metadata = await static_resolver.resolve(name)

            assert metadata is not None, name
            assert metadata.request_policy is None, name
            assert metadata.response_policy is None, name
            assert metadata.is_readonly is True, name
            assert metadata.has_key_argument is False, name
            assert metadata.has_complete_metadata is True, name
            assert await static_resolver.is_cacheable(name) is False, name
            assert await static_resolver.is_replica_safe(name) is True, name

    async def test_command_withholds_routing_and_is_not_replica_safe(self):
        static_resolver = AsyncStaticMetadataResolver()
        metadata = await static_resolver.resolve("command")

        assert metadata is not None
        assert metadata.request_policy is None
        assert metadata.response_policy is None
        assert metadata.is_readonly is False
        assert metadata.has_key_argument is False
        assert metadata.has_complete_metadata is True
        assert await static_resolver.is_cacheable("command") is False
        assert await static_resolver.is_replica_safe("command") is False

    async def test_bless_scan_withholds_routing_and_is_not_replica_safe(self):
        """
        The SCAN-shaped container subcommand: keyless, nondeterministic and routed to every
        primary through COMMAND_FLAGS, so its record must withhold routing like SCAN's does.
        Unlike SCAN the server reports no flags for it at all, so it is not readonly.
        """
        static_resolver = AsyncStaticMetadataResolver()
        metadata = await static_resolver.resolve("bless scan")

        assert metadata is not None
        assert metadata.request_policy is None
        assert metadata.response_policy is None
        assert metadata.is_readonly is False
        assert metadata.has_key_argument is False
        assert metadata.has_nondeterministic_output is True
        assert metadata.has_complete_metadata is True
        assert await static_resolver.is_cacheable("bless scan") is False
        assert await static_resolver.is_replica_safe("bless scan") is False

    @pytest.mark.parametrize("name", ["bless clear", "bless get", "bless set"])
    async def test_the_keyed_bless_subcommands_route_by_key_and_are_not_cacheable(
        self, name
    ):
        """
        BLESS GET is included: the server flags it ``fast`` only, with no ``readonly``, so it
        fails closed exactly like the two writes.
        """
        static_resolver = AsyncStaticMetadataResolver()
        metadata = await static_resolver.resolve(name)

        assert metadata is not None, name
        assert metadata.request_policy is RequestPolicy.DEFAULT_KEYED, name
        assert metadata.response_policy is ResponsePolicy.DEFAULT_KEYED, name
        assert metadata.is_readonly is False, name
        assert metadata.has_key_argument is True, name
        assert metadata.has_complete_metadata is True, name
        assert await static_resolver.is_cacheable(name) is False, name
        assert await static_resolver.is_replica_safe(name) is False, name

    async def test_all_static_entries_in_cluster_command_flags_withhold_routing(self):
        """
        Verify that every command appearing in RedisCluster.command_flags that is present
        in _STATIC_COMMAND_METADATA has its routing view withheld (request_policy=None and
        response_policy=None), except the ``TABLE_ROUTED_FLAG_COMMANDS``, which record the
        server's tips.
        """
        static_resolver = AsyncStaticMetadataResolver()
        table_core = _STATIC_COMMAND_METADATA["core"]

        for flag_cmd in RedisCluster.COMMAND_FLAGS:
            # A container command like "COMMAND COUNT" or "SLOWLOG GET" may be recorded under
            # its first token ("command", "slowlog") or, like "BLESS SCAN", under its full
            # space-joined name - which is the name the resolver is asked for.
            for table_cmd in {flag_cmd.split()[0].lower(), flag_cmd.lower()}:
                if table_cmd not in table_core:
                    continue
                metadata = await static_resolver.resolve(table_cmd)
                assert metadata is not None
                if table_cmd in TABLE_ROUTED_FLAG_COMMANDS:
                    assert (
                        policy_pair(metadata) == TABLE_ROUTED_FLAG_COMMANDS[table_cmd]
                    ), table_cmd
                    continue
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
    async def test_newly_replica_eligible_commands_are_replica_safe(self, cmd):
        resolver = AsyncStaticMetadataResolver()
        assert await resolver.is_replica_safe(cmd) is True

    async def test_the_ineligible_records_decide_ahead_of_a_live_layer(self):
        """
        Why the documented chain order is static-first, pinned in both directions and offline.

        The records exist to override a server that reports these commands as cacheable -
        which every supported server does for TOUCH and VRANDMEMBER, and servers before 8.10 do
        for the script runners. A resolver answers from the first record it finds, so only a
        static-first chain reaches them.
        """
        live_says_cacheable = AsyncDynamicMetadataResolver(
            {"core": {name: CACHEABLE_KEYED for name in INELIGIBLE_RECORD_COMMANDS}}
        )

        static_first = AsyncStaticMetadataResolver().with_fallback(live_says_cacheable)
        dynamic_first = live_says_cacheable.with_fallback(AsyncStaticMetadataResolver())

        for name in INELIGIBLE_RECORD_COMMANDS:
            assert await static_first.is_cacheable(name) is False, name
            # Reversed, the live layer wins and the command becomes cacheable - which is
            # exactly what static-first exists to prevent, and what dynamic-first forfeits.
            assert await dynamic_first.is_cacheable(name) is True, name

    async def test_a_policy_resolver_reports_them_unresolved(self):
        """
        What sends the cluster client back to its own slot resolution: the routing view is None,
        exactly as it is for a command the table does not carry at all.
        """
        static_resolver = AsyncStaticPolicyResolver()

        for name in ALL_WITHHELD_ROUTING_COMMANDS:
            assert await static_resolver.resolve(name) is None, name

        # Keyed reads still resolve - the movablekeys ones included - so the None above is the
        # withheld record rather than a table-wide miss.
        assert policy_pair(await static_resolver.resolve("get")) == KEYED_POLICIES
        for name in (*MOVABLE_KEYS_READS, *SCRIPT_RUNNER_READS):
            assert policy_pair(await static_resolver.resolve(name)) == KEYED_POLICIES, (
                name
            )

    async def test_a_withheld_record_does_not_re_walk_the_metadata_chain(self):
        """
        The record was found, so a metadata resolver behind this one is never asked - it would
        answer with the very policies the record withholds.
        """
        resolver = AsyncStaticMetadataResolver().with_fallback(
            AsyncDynamicMetadataResolver({"core": {"vrandmember": CACHEABLE_KEYED}})
        )

        vrandmember_metadata = await resolver.resolve("vrandmember")
        assert vrandmember_metadata.request_policy is None
        assert await resolver.resolve_policies("vrandmember") is None

    async def test_a_policy_level_fallback_still_gets_its_turn(self):
        """
        Withholding must be indistinguishable from a table miss at the policy layer, so that a
        caller-supplied chain behaves exactly as it did before the table carried these commands.
        """
        resolver = AsyncStaticPolicyResolver(
            fallback=MetadataPolicyResolver(
                AsyncDynamicMetadataResolver({"core": {"vrandmember": CACHEABLE_KEYED}})
            )
        )

        assert policy_pair(await resolver.resolve("vrandmember")) == KEYED_POLICIES


@pytest.mark.asyncio
@pytest.mark.fixed_client
class TestMemoBounds:
    """
    Async mirror of the memo bounds. ``CommandPolicies`` value equality is stack-agnostic, so
    only the sync suite covers it.
    """

    @staticmethod
    async def _fill_beyond_the_cap(ask) -> None:
        for index in range(_MEMO_MAX_ENTRIES + 100):
            await ask(f"nosuchmodule.nosuchcommand{index}")

    async def test_the_policy_memo_stops_at_the_cap(self):
        resolver = AsyncStaticMetadataResolver()

        await self._fill_beyond_the_cap(resolver.resolve_policies)

        assert len(resolver._policies) == _MEMO_MAX_ENTRIES

    async def test_the_cacheable_memo_stops_at_the_cap(self):
        resolver = AsyncStaticMetadataResolver()

        await self._fill_beyond_the_cap(resolver.is_cacheable)

        assert len(resolver._cacheable) == _MEMO_MAX_ENTRIES

    async def test_the_trackable_read_memo_stops_at_the_cap(self):
        resolver = AsyncStaticMetadataResolver()

        await self._fill_beyond_the_cap(resolver.is_trackable_read)

        assert len(resolver._trackable_read) == _MEMO_MAX_ENTRIES

    async def test_the_views_stay_correct_past_the_cap(self):
        """A capped memo recomputes; it never answers wrongly."""
        resolver = AsyncStaticMetadataResolver()

        await self._fill_beyond_the_cap(resolver.resolve_policies)
        await self._fill_beyond_the_cap(resolver.is_cacheable)
        await self._fill_beyond_the_cap(resolver.is_trackable_read)

        assert policy_pair(await resolver.resolve_policies("get")) == KEYED_POLICIES
        assert await resolver.is_cacheable("get") is True
        assert await resolver.is_trackable_read("get") is True
        assert await resolver.resolve_policies("nosuchmodule.nosuchcommand") is None
        assert await resolver.is_cacheable("nosuchmodule.nosuchcommand") is False
        assert await resolver.is_trackable_read("nosuchmodule.nosuchcommand") is False


@pytest.mark.asyncio
@pytest.mark.fixed_client
class TestDeprecations:
    """
    Async mirror of the deprecation warnings. ``STATIC_POLICIES`` is module-level and shared by
    both stacks, so only the sync suite covers it.
    """

    async def test_get_command_policies_warns(self):
        commands_parser = AsyncCommandsParser()

        with pytest.warns(DeprecationWarning, match="get_commands_metadata_cache"):
            assert await commands_parser.get_command_policies() == {}


@pytest.mark.asyncio
@pytest.mark.fixed_client
class TestBasePolicyResolver:
    async def test_resolve(self):
        dynamic_resolver = MetadataPolicyResolver(
            AsyncDynamicMetadataResolver(
                {
                    "core": {
                        "zcount": CACHEABLE_KEYED,
                        "rpoplpush": replace(CACHEABLE_KEYED, is_readonly=False),
                    }
                }
            )
        )
        assert policy_pair(await dynamic_resolver.resolve("zcount")) == KEYED_POLICIES
        assert (
            policy_pair(await dynamic_resolver.resolve("rpoplpush")) == KEYED_POLICIES
        )

        with pytest.raises(
            ValueError, match="Wrong command or module name: foo.bar.baz"
        ):
            await dynamic_resolver.resolve("foo.bar.baz")

        assert await dynamic_resolver.resolve("foo.bar") is None
        assert await dynamic_resolver.resolve("core.foo") is None

        # Test that policy fallback correctly
        static_resolver = AsyncStaticPolicyResolver()
        with_fallback_dynamic_resolver = dynamic_resolver.with_fallback(static_resolver)
        resolved_policies = await with_fallback_dynamic_resolver.resolve("ft.aggregate")

        assert resolved_policies.request_policy == RequestPolicy.DEFAULT_KEYLESS
        assert resolved_policies.response_policy == ResponsePolicy.DEFAULT_KEYLESS

        # Extended chain with one more resolver
        another_dynamic_resolver = MetadataPolicyResolver(
            AsyncDynamicMetadataResolver(
                {
                    "foo": {
                        "bar": replace(
                            CACHEABLE_KEYED, request_policy=RequestPolicy.SPECIAL
                        ),
                    }
                }
            )
        )
        with_fallback_static_resolver = static_resolver.with_fallback(
            another_dynamic_resolver
        )
        with_double_fallback_dynamic_resolver = dynamic_resolver.with_fallback(
            with_fallback_static_resolver
        )

        assert policy_pair(
            await with_double_fallback_dynamic_resolver.resolve("foo.bar")
        ) == (RequestPolicy.SPECIAL, ResponsePolicy.DEFAULT_KEYED)

    async def test_resolve_serves_the_metadata_it_proxies(self):
        """
        A policy resolver is the routing view of a metadata resolver, so the policies it
        serves are the ones the metadata record carries.
        """
        dynamic_resolver = MetadataPolicyResolver(
            AsyncDynamicMetadataResolver(
                {"core": {"pfcount": replace(CACHEABLE_KEYED, is_dont_cache=True)}}
            )
        )

        assert policy_pair(await dynamic_resolver.resolve("pfcount")) == KEYED_POLICIES

        # FT.CURSOR is the static entry whose policies are not the keyed/keyless defaults,
        # so it shows that whatever the record says is what is served.
        static_policies = await AsyncStaticPolicyResolver().resolve("ft.cursor")
        assert policy_pair(static_policies) == (
            RequestPolicy.SPECIAL,
            ResponsePolicy.DEFAULT_KEYLESS,
        )

    async def test_resolve_from_policy_records(self):
        """
        The ``policy_records`` argument serves exactly the policies it is given. Every field
        such a record does not carry keeps its fail-closed default, so nothing but the routing
        view is claimed for them.
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

        resolver = AsyncDynamicPolicyResolver(policy_records)

        assert policy_pair(await resolver.resolve("zcount")) == KEYED_POLICIES
        assert policy_pair(await resolver.resolve("keys")) == (
            RequestPolicy.ALL_SHARDS,
            ResponsePolicy.SPECIAL,
        )
        assert await resolver.resolve("pfcount") is None

        # Chaining keeps the records, and the fallback answers what they do not carry.
        chained_resolver = resolver.with_fallback(AsyncStaticPolicyResolver())

        assert policy_pair(await chained_resolver.resolve("keys")) == (
            RequestPolicy.ALL_SHARDS,
            ResponsePolicy.SPECIAL,
        )
        assert policy_pair(await chained_resolver.resolve("ft.cursor")) == (
            RequestPolicy.SPECIAL,
            ResponsePolicy.DEFAULT_KEYLESS,
        )

    async def test_the_policy_records_are_required_at_the_call_site(self):
        """A resolver cannot be built without something to resolve through."""
        with pytest.raises(TypeError, match="policy_records"):
            AsyncDynamicPolicyResolver()

    async def test_resolve_memoizes_the_walk_down_the_chain(self):
        resolver = MetadataPolicyResolver(
            AsyncDynamicMetadataResolver({"core": {"pfcount": CACHEABLE_KEYED}}),
            fallback=AsyncStaticPolicyResolver(),
        )

        # The walk down the fallback chain is memoized, so a repeated resolve stays off
        # the command execution path.
        assert await resolver.resolve("pfcount") is await resolver.resolve("pfcount")
        assert await resolver.resolve("ft.cursor") is await resolver.resolve(
            "ft.cursor"
        )
        # A command no resolver in the chain carries is memoized as unresolved.
        assert await resolver.resolve("cms.query") is None
        assert await resolver.resolve("cms.query") is None


@pytest.mark.asyncio
@pytest.mark.fixed_client
class TestAsyncBaseMetadataResolver:
    async def test_resolve(self):
        zcount_metadata = CACHEABLE_KEYED
        rpoplpush_metadata = replace(CACHEABLE_KEYED, is_readonly=False)

        dynamic_resolver = AsyncDynamicMetadataResolver(
            {
                "core": {
                    "zcount": zcount_metadata,
                    "rpoplpush": rpoplpush_metadata,
                }
            }
        )

        # The record is served as-is, not copied: consumers share one immutable record.
        assert await dynamic_resolver.resolve("zcount") is zcount_metadata
        assert await dynamic_resolver.resolve("rpoplpush") is rpoplpush_metadata

        with pytest.raises(
            ValueError, match="Wrong command or module name: foo.bar.baz"
        ):
            await dynamic_resolver.resolve("foo.bar.baz")

        # Unknown module and unknown command in a known module both fail closed when
        # there is no fallback behind the resolver.
        assert await dynamic_resolver.resolve("foo.bar") is None
        assert await dynamic_resolver.resolve("core.foo") is None

    async def test_resolve_container_subcommand(self):
        """Container subcommands are keyed space-joined, the form ``args[0]`` carries."""
        memory_usage_metadata = CACHEABLE_KEYED
        resolver = AsyncDynamicMetadataResolver(
            {"core": {"memory usage": memory_usage_metadata}}
        )

        assert await resolver.resolve("memory usage") is memory_usage_metadata
        assert await resolver.resolve("memory") is None

    async def test_resolve_policies_projects_the_routing_view(self):
        resolver = AsyncDynamicMetadataResolver(
            {"core": {"pfcount": replace(CACHEABLE_KEYED, is_dont_cache=True)}}
        )

        # Nothing but the routing policies of the record leaks into the projection.
        assert policy_pair(await resolver.resolve_policies("pfcount")) == KEYED_POLICIES

        # FT.CURSOR is the static entry whose policies are not the keyed/keyless defaults,
        # so it shows that the projection carries whatever the record says.
        static_policies = await AsyncStaticMetadataResolver().resolve_policies(
            "ft.cursor"
        )
        assert policy_pair(static_policies) == (
            RequestPolicy.SPECIAL,
            ResponsePolicy.DEFAULT_KEYLESS,
        )

    async def test_resolve_policies_memoizes_the_projection(self):
        resolver = AsyncDynamicMetadataResolver(
            {"core": {"pfcount": CACHEABLE_KEYED}}, AsyncStaticMetadataResolver()
        )

        # Both the projected record and the walk down the fallback chain are memoized, so a
        # repeated resolve stays off the command execution path.
        assert await resolver.resolve_policies(
            "pfcount"
        ) is await resolver.resolve_policies("pfcount")
        assert await resolver.resolve_policies(
            "ft.cursor"
        ) is await resolver.resolve_policies("ft.cursor")
        # A command no resolver in the chain carries is memoized as unresolved.
        assert await resolver.resolve_policies("cms.query") is None
        assert await resolver.resolve_policies("cms.query") is None

    async def test_is_cacheable_decides_from_the_shipped_table(self):
        static_resolver = AsyncStaticMetadataResolver()

        assert await static_resolver.is_cacheable("get") is True
        assert await static_resolver.is_cacheable("json.get") is True

        # The entries the table carries precisely because they are *not* cacheable.
        assert await static_resolver.is_cacheable("xpending") is False
        assert await static_resolver.is_cacheable("ts.info") is False
        assert await static_resolver.is_cacheable("xread") is False
        # Readonly but keyless.
        assert await static_resolver.is_cacheable("ft.search") is False
        # A write command.
        assert await static_resolver.is_cacheable("set") is False

        # Not covered by the table, and nothing behind it: fails closed.
        assert await static_resolver.is_cacheable("pfcount") is False
        assert await static_resolver.is_cacheable("bf.exists") is False
        # Covered, by records that state their own ineligibility rather than relying on the
        # absence above - which is what keeps them out when a live layer sits behind.
        assert await static_resolver.is_cacheable("touch") is False
        assert await static_resolver.is_cacheable("eval_ro") is False

    async def test_custom_metadata_decides_replica_safety_for_known_module_reads(self):
        resolver = AsyncDynamicMetadataResolver(
            {
                "ts": {"get": CACHEABLE_KEYED},
                "ft": {"aggregate": CACHEABLE_KEYED},
            }
        )

        assert await resolver.is_replica_safe("ts.get") is True
        assert await resolver.is_replica_safe("ft.aggregate") is True

    async def test_touch_remains_replica_unsafe_despite_readonly_metadata(self):
        resolver = AsyncDynamicMetadataResolver({"core": {"touch": CACHEABLE_KEYED}})

        assert await resolver.is_replica_safe("touch") is False

    async def test_the_replica_unsafe_commands_are_seeded_into_the_memo(self):
        """
        Seeding the memo at construction is the whole mechanism, so pin it: the entry is in
        place before any command is looked up, which is what lets these answer from the same
        single dict lookup as every other command with no per-call test of their own. The
        record TOUCH would otherwise resolve to reports it readonly, so the seed is the only
        thing keeping it off a replica.
        """
        resolver = AsyncDynamicMetadataResolver({"core": {"touch": CACHEABLE_KEYED}})

        assert resolver._replica_safe == {"touch": False}
        assert _is_replica_safe(await resolver.resolve("touch")) is True
        assert await resolver.is_replica_safe("TOUCH") is False

    async def test_static_resolver_decides_replica_safety(self):
        resolver = AsyncStaticMetadataResolver()

        # Replica safe commands
        assert await resolver.is_replica_safe("get") is True
        assert await resolver.is_replica_safe("GET") is True
        assert await resolver.is_replica_safe("dbsize") is True
        assert await resolver.is_replica_safe("ttl") is True
        assert await resolver.is_replica_safe("eval_ro") is True
        assert await resolver.is_replica_safe("xread") is True
        assert await resolver.is_replica_safe("json.get") is True
        assert await resolver.is_replica_safe("ft.search") is True

        # Replica unsafe commands
        assert await resolver.is_replica_safe("set") is False
        assert await resolver.is_replica_safe("SET") is False
        assert await resolver.is_replica_safe("touch") is False
        assert await resolver.is_replica_safe("TOUCH") is False
        assert await resolver.is_replica_safe("hset") is False
        assert await resolver.is_replica_safe("ft.create") is False

        # Non-string or unknown commands
        assert await resolver.is_replica_safe(None) is False
        assert await resolver.is_replica_safe(123) is False
        assert await resolver.is_replica_safe("nosuchcommand") is False

    @pytest.mark.parametrize(
        "command,trackable",
        [
            ("GET", True),
            ("MGET", True),
            # Readonly and keyed, but never eligible to store - which is exactly why optout
            # wants to exempt them.
            ("TOUCH", True),
            ("XPENDING", True),
            # Readonly but keyless, so the ``NO`` is a harmless no-op.
            ("KEYS", True),
            ("EVAL_RO", True),
            # Absent from the shipped table, so undecidable: fails closed.
            ("SET", False),
            ("DEL", False),
            ("XREADGROUP", False),
            ("NOSUCHCOMMAND", False),
            ("NOSUCHMODULE.NOSUCHCOMMAND", False),
        ],
    )
    async def test_is_trackable_read_decides_from_the_readonly_flag(
        self, command, trackable
    ):
        resolver = AsyncStaticMetadataResolver()

        assert await resolver.is_trackable_read(command) is trackable

    @pytest.mark.parametrize(
        "command",
        ["a.b.c", b"GET", bytearray(b"GET"), memoryview(b"GET"), 1, None],
        ids=["two-dots", "bytes", "bytearray", "memoryview", "int", "none"],
    )
    async def test_is_trackable_read_fails_closed_on_an_undecidable_name(self, command):
        resolver = AsyncStaticMetadataResolver()

        assert await resolver.is_trackable_read(command) is False

    async def test_is_trackable_read_is_case_insensitive(self):
        resolver = AsyncStaticMetadataResolver()

        for command in ("get", "GET", "Get"):
            assert await resolver.is_trackable_read(command) is True

    async def test_is_trackable_read_is_abstract_on_the_abc(self):
        """
        An ``AsyncMetadataResolver`` must implement ``is_trackable_read`` like its other views.
        """

        class ResolverWithoutTheView(AsyncMetadataResolver):
            async def resolve(self, command_name):
                return None

            async def resolve_policies(self, command_name):
                return None

            async def is_cacheable(self, command_name):
                return False

            async def is_replica_safe(self, command_name):
                return False

            def with_fallback(self, fallback):
                return self

        with pytest.raises(TypeError, match="is_trackable_read"):
            ResolverWithoutTheView()

    async def test_is_cacheable_fails_closed_for_an_unresolvable_name(self):
        """
        A name the record tables cannot be keyed by is not a raise on the command execution
        path: a raw command carrying such a name must still reach the server. ``resolve``
        keeps reporting the error, because its caller asked about the records themselves.
        """
        static_resolver = AsyncStaticMetadataResolver()

        assert await static_resolver.is_cacheable("foo.bar.baz") is False

        with pytest.raises(
            ValueError, match="Wrong command or module name: foo.bar.baz"
        ):
            await static_resolver.resolve("foo.bar.baz")

        with pytest.raises(
            ValueError, match="Wrong command or module name: foo.bar.baz"
        ):
            await static_resolver.resolve_policies("foo.bar.baz")

    async def test_the_views_are_case_insensitive(self):
        """
        The client-side cache resolves the command name as the command methods spell it,
        while the cluster client lowercases first.
        """
        resolver = AsyncDynamicMetadataResolver(
            {
                "core": {"memory usage": CACHEABLE_KEYED},
                "json": {"get": CACHEABLE_KEYED},
            }
        )

        for name in ("MEMORY USAGE", "memory usage", "JSON.GET", "json.get"):
            assert await resolver.resolve(name) is CACHEABLE_KEYED, name
            assert (
                policy_pair(await resolver.resolve_policies(name)) == KEYED_POLICIES
            ), name
            assert await resolver.is_cacheable(name) is True, name

    async def test_caller_supplied_records_do_not_have_to_be_keyed_lowercase(self):
        """
        A lookup is lowercased, so records keyed any other way would silently resolve to
        nothing. Records read from a ``COMMAND`` reply are already lowercase - both parsers
        lowercase every name as they read it - but caller-supplied ones carry whatever
        spelling the caller chose, so they are normalized on the way in.
        """
        resolver = AsyncDynamicMetadataResolver(
            {
                "core": {"ZCOUNT": CACHEABLE_KEYED, "MEMORY USAGE": CACHEABLE_KEYED},
                "JSON": {"GET": CACHEABLE_KEYED},
            }
        )

        for name in ("ZCOUNT", "zcount", "MEMORY USAGE", "JSON.GET", "json.get"):
            assert await resolver.resolve(name) is CACHEABLE_KEYED, name
            assert (
                policy_pair(await resolver.resolve_policies(name)) == KEYED_POLICIES
            ), name
            assert await resolver.is_cacheable(name) is True, name

    async def test_records_already_keyed_lowercase_are_not_rebuilt(self):
        """
        The common case pays nothing: a ``COMMAND`` reply arrives lowercase, so the records are
        served as given rather than copied per resolver.
        """
        records = {"core": {"zcount": CACHEABLE_KEYED}}

        assert AsyncDynamicMetadataResolver(records)._metadata is records

    async def test_the_views_are_memoized_independently(self):
        resolver = CountingAsyncStaticMetadataResolver()

        assert await resolver.is_cacheable("get") is True
        assert await resolver.is_cacheable("get") is True
        assert resolver.resolve_calls == 1

        # A second view of the same command is resolved once more, then memoized itself.
        assert policy_pair(await resolver.resolve_policies("get")) == KEYED_POLICIES
        assert policy_pair(await resolver.resolve_policies("get")) == KEYED_POLICIES
        assert resolver.resolve_calls == 2

    async def test_the_memos_are_keyed_by_the_normalized_name(self):
        """
        One command takes one memo entry however the caller spells it: the command methods
        write ``GET``, the cluster client lowercases first, and a raw ``execute_command`` may
        carry anything, so keying by the name as asked would hold the same answer several
        times over and spend the cap on spellings.
        """
        resolver = CountingAsyncStaticMetadataResolver()

        for name in ("GET", "get", "Get"):
            assert await resolver.is_cacheable(name) is True, name
            assert (
                policy_pair(await resolver.resolve_policies(name)) == KEYED_POLICIES
            ), name

        # One record lookup per view, not one per spelling.
        assert resolver.resolve_calls == 2
        assert list(resolver._cacheable) == ["get"]
        assert list(resolver._policies) == ["get"]

    async def test_resolve_policies_reports_an_unresolvable_name_as_spelled(self):
        """
        The memo is keyed lowercase, but the error still quotes the caller's spelling: it is
        their input that has to be recognizable in the message.
        """
        with pytest.raises(
            ValueError, match="Wrong command or module name: FOO.BAR.BAZ"
        ):
            await AsyncStaticMetadataResolver().resolve_policies("FOO.BAR.BAZ")

    async def test_is_cacheable_refuses_an_incomplete_record_that_shadows_a_complete_one(
        self,
    ):
        """
        A resolver answers from the first record it finds, so an incomplete record decides
        the command even when a complete one sits behind it. Fail closed rather than let the
        cacheability view disagree with ``resolve`` about which record answered - which is
        why an override table belongs in front of a resolver that may serve incomplete
        records, not behind it.
        """
        dynamic_resolver = AsyncDynamicMetadataResolver(
            {"core": {"get": replace(CACHEABLE_KEYED, has_complete_metadata=False)}}
        )
        static_resolver = AsyncStaticMetadataResolver()
        resolver = dynamic_resolver.with_fallback(static_resolver)

        assert await static_resolver.is_cacheable("get") is True
        resolved_get = await resolver.resolve("get")
        assert resolved_get.has_complete_metadata is False
        assert await resolver.is_cacheable("get") is False

    async def test_a_resolver_built_from_policy_records_reports_nothing_cacheable(self):
        """
        Policy records carry only the routing view, so every cacheability field of a record
        lifted from them keeps its fail-closed default.
        """
        resolver = AsyncDynamicMetadataResolver(
            _build_commands_metadata_cache_from_policies(
                {"core": {"get": CommandPolicies(*KEYED_POLICIES)}}
            )
        )

        assert policy_pair(await resolver.resolve_policies("get")) == KEYED_POLICIES
        assert await resolver.is_cacheable("get") is False

    async def test_static_resolver_serves_the_shipped_table(self):
        static_resolver = AsyncStaticMetadataResolver()

        get_metadata = await static_resolver.resolve("get")
        assert get_metadata.is_readonly is True
        assert get_metadata.has_key_argument is True
        assert get_metadata.is_dont_cache is False
        assert get_metadata.has_complete_metadata is True

        # The two entries the table carries precisely because they are *not* cacheable.
        xpending_metadata = await static_resolver.resolve("xpending")
        assert xpending_metadata.has_nondeterministic_output is True
        ts_info_metadata = await static_resolver.resolve("ts.info")
        assert ts_info_metadata.is_dont_cache is True

        # Readonly but keyless, so not cacheable either.
        ft_search_metadata = await static_resolver.resolve("ft.search")
        assert ft_search_metadata.is_readonly is True
        assert ft_search_metadata.has_key_argument is False

        # Not covered by the table, and nothing behind it: fails closed.
        assert await static_resolver.resolve("pfcount") is None
        assert await static_resolver.resolve("bf.exists") is None

    async def test_dynamic_first_falls_back_to_static(self):
        dynamic_resolver = AsyncDynamicMetadataResolver(
            {"core": {"zcount": CACHEABLE_KEYED}}
        )
        resolver = dynamic_resolver.with_fallback(AsyncStaticMetadataResolver())

        resolved_metadata = await resolver.resolve("ft.aggregate")
        assert resolved_metadata.request_policy == RequestPolicy.DEFAULT_KEYLESS
        assert resolved_metadata.response_policy == ResponsePolicy.DEFAULT_KEYLESS
        assert resolved_metadata.is_dont_cache is True

    async def test_static_first_falls_back_to_dynamic_for_uncovered_command(self):
        """
        Static first, live metadata behind it: the static table is the override layer and
        the server answers for everything the table does not carry.
        """
        dynamic_resolver = AsyncDynamicMetadataResolver(
            {
                # PFCOUNT is readonly and keyed, and the static table does not carry it.
                # "core" is a module the static table does know, so this covers the
                # command-level miss: a known module with an unknown command must still
                # fall through.
                "core": {"pfcount": CACHEABLE_KEYED},
                # BF.EXISTS is a whole module surface the static table does not carry,
                # which covers the module-level miss.
                "bf": {"exists": CACHEABLE_KEYED},
                # The server reports TS.INFO with the dont_cache tip. Here it deliberately
                # does not, so that the static entry answering instead is observable.
                "ts": {"info": CACHEABLE_KEYED},
            }
        )
        static_resolver = AsyncStaticMetadataResolver()
        resolver = static_resolver.with_fallback(dynamic_resolver)

        assert await static_resolver.resolve("pfcount") is None
        assert await resolver.resolve("pfcount") is CACHEABLE_KEYED
        assert await resolver.resolve("bf.exists") is CACHEABLE_KEYED

        # TS.INFO is carried by both, so the static entry wins and the command stays
        # uncacheable - this is what makes the static layer an override.
        resolved_ts_info = await resolver.resolve("ts.info")
        assert resolved_ts_info.is_dont_cache is True

        # A command neither resolver carries still fails closed.
        assert await resolver.resolve("cms.query") is None

    async def test_with_fallback_returns_a_new_resolver(self):
        static_resolver = AsyncStaticMetadataResolver()
        dynamic_resolver = AsyncDynamicMetadataResolver(
            {"core": {"pfcount": CACHEABLE_KEYED}}
        )

        resolver = static_resolver.with_fallback(dynamic_resolver)

        assert resolver is not static_resolver
        # The receiver is left untouched, so a chain can be built from a shared resolver.
        assert await static_resolver.resolve("pfcount") is None
        assert await resolver.resolve("pfcount") is CACHEABLE_KEYED

    async def test_extended_fallback_chain(self):
        foo_bar_metadata = replace(CACHEABLE_KEYED, has_key_argument=False)

        dynamic_resolver = AsyncDynamicMetadataResolver(
            {"core": {"zcount": CACHEABLE_KEYED}}
        )
        another_dynamic_resolver = AsyncDynamicMetadataResolver(
            {"foo": {"bar": foo_bar_metadata}}
        )
        static_resolver = AsyncStaticMetadataResolver()

        resolver = dynamic_resolver.with_fallback(
            static_resolver.with_fallback(another_dynamic_resolver)
        )

        # Resolved by the last resolver in the chain.
        assert await resolver.resolve("foo.bar") is foo_bar_metadata
        # Resolved by the first.
        assert await resolver.resolve("zcount") is CACHEABLE_KEYED
        # Resolved by the middle one.
        ts_info_metadata = await resolver.resolve("ts.info")
        assert ts_info_metadata.is_dont_cache is True
        # Not resolved by any of them.
        assert await resolver.resolve("cms.query") is None


@pytest.mark.asyncio
@pytest.mark.redismod
class TestAsyncCommandsParserMetadata:
    async def test_get_command_policies_matches_the_metadata_view(self, stack_client):
        """
        The two views of one reply agree. ``get_command_policies`` is the routing view: same
        traversal, same keys, projected down to the two policies a cluster client routes by.
        """
        commands_parser = AsyncCommandsParser()
        await commands_parser.initialize(stack_client)

        policy_records = await commands_parser.get_command_policies()

        metadata_records = await commands_parser.get_commands_metadata_cache()

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


@pytest.mark.asyncio
@pytest.mark.redismod
class TestAsyncStaticMetadataAgainstServer:
    """
    Async mirror of the sync drift guard for ``_STATIC_COMMAND_METADATA``.

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
    async def test_static_metadata_matches_live_command_metadata(self, stack_client):
        live_commands = live_command_details(await stack_client.command())
        static_resolver = AsyncStaticMetadataResolver()

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

            metadata = await static_resolver.resolve(name)
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
            # holding that key, so its entry says DEFAULT_KEYED, MULTI_SHARD or nothing. This
            # catches a server-side metadata change that a table-only assertion cannot - a
            # command that gains a key argument, or one whose key specs start reporting one.
            if live_has_key_argument(details):
                assert metadata.request_policy in (
                    RequestPolicy.DEFAULT_KEYED,
                    RequestPolicy.MULTI_SHARD,
                    None,
                ), name

            checked += 1

        assert checked == sum(
            len(commands) for commands in _STATIC_COMMAND_METADATA.values()
        )

    @skip_if_server_version_lt(STATIC_TABLE_SERVER_VERSION)
    async def test_eligibility_matches_the_specified_rules(self, stack_client):
        """
        The cacheability view, decided from live server metadata rather than the table.

        One case per rule of the specification, so a metadata change on the server surfaces
        here rather than silently changing what the client caches. The rules themselves are
        unit-tested once, on the sync side: they are a module-level function over an
        immutable record, shared by both stacks.
        """
        dynamic_resolver = AsyncDynamicMetadataResolver(
            await live_metadata_records(stack_client)
        )
        live_commands = live_command_details(await stack_client.command())
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
            assert await dynamic_resolver.is_cacheable(name) is True, name

        # Readonly but keyless.
        assert await dynamic_resolver.is_cacheable("keys") is False
        assert await dynamic_resolver.is_cacheable("ft.search") is False
        # Nondeterministic output.
        assert await dynamic_resolver.is_cacheable("xpending") is False
        # No readonly flag.
        assert await dynamic_resolver.is_cacheable("xreadgroup") is False
        # Blocking, even though it is readonly and keyed.
        assert await dynamic_resolver.is_cacheable("xread") is False
        # Tipped dont_cache.
        assert await dynamic_resolver.is_cacheable("ts.info") is False
        # A shard channel is not a key name.
        assert await dynamic_resolver.is_cacheable("ssubscribe") is False
        # Unknown to the server, so unproven and not cached.
        assert (
            await dynamic_resolver.is_cacheable("nosuchmodule.nosuchcommand") is False
        )

        if reports_script_runner:
            for name in ("eval_ro", "evalsha_ro", "fcall_ro"):
                assert await dynamic_resolver.is_cacheable(name) is False, name

        # TOUCH is readonly and keyed and the server tips it nothing, so server metadata
        # alone cannot exclude it, even though it must reach the server on every call. The
        # static table is what excludes it - see
        # ``test_the_recorded_divergence_from_the_live_reply_is_still_needed`` - as it also
        # does for the script runners above on a server that predates the script_runner flag.
        assert await dynamic_resolver.is_cacheable("touch") is True

    @skip_if_server_version_lt(STATIC_TABLE_SERVER_VERSION)
    async def test_dynamic_resolver_matches_the_static_table(self, stack_client):
        """
        Both resolver paths must produce the same record for the same command.

        The routing policies are compared apart from the cacheability inputs because the
        table diverges from the server on purpose for four reasons:

        - MGET is tipped ``request_policy:multi_shard``, but the table records the keyed
          defaults: ``mget`` documents that keys in different slots raise on a cluster.
        - FT.CURSOR is SPECIAL, a client-side routing decision the server does not tip.
        - The ``COMMAND_FLAGS`` reads - DBSIZE, KEYS, SCAN - and BLESS SCAN and
          COMMAND withhold their policies entirely, so the cluster client keeps resolving
          their targets itself.
        - VRANDMEMBER, which states its own ineligibility, withholds its policies for the
          same reason: adding it must not start routing a command the client resolves itself.

        The ``movablekeys`` reads agree with the live reply: the parser derives their keyed
        defaults from the key specs.

        ``LIVE_CACHEABILITY_DIVERGENCE`` is the one place the *cacheability* inputs are allowed
        to disagree, and the disagreement is pinned rather than skipped.
        """
        dynamic_resolver = AsyncDynamicMetadataResolver(
            await live_metadata_records(stack_client)
        )
        static_resolver = AsyncStaticMetadataResolver()

        diverged = set()
        for _, _, name in static_table_names():
            live_metadata = await dynamic_resolver.resolve(name)
            assert live_metadata is not None, (
                f"{name} is in the static table but not in the COMMAND reply"
            )

            static_metadata = await static_resolver.resolve(name)
            diverging_field = LIVE_CACHEABILITY_DIVERGENCE.get(name)
            if diverging_field is None:
                assert cacheability_fields(live_metadata) == cacheability_fields(
                    static_metadata
                ), name
                # The same effective metadata must produce the same cacheability decision, no
                # matter which resolver path answered.
                assert await dynamic_resolver.is_cacheable(
                    name
                ) == await static_resolver.is_cacheable(name), name
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
            "mget",
            "ft.cursor",
            *ALL_WITHHELD_ROUTING_COMMANDS,
        }

    # VRANDMEMBER is a vectorset command, so it is only reported from Redis 8.0 on.
    @skip_if_server_version_lt("8.0.0")
    @pytest.mark.parametrize("name,field", LIVE_CACHEABILITY_DIVERGENCE.items())
    async def test_the_recorded_divergence_from_the_live_reply_is_still_needed(
        self, stack_client, name, field
    ):
        """
        The hand-maintained divergences, pinned in both directions.

        Each record exists because the server reports the command as cacheable when it is not:
        TOUCH has a server-side effect (it refreshes idle time), and VRANDMEMBER samples
        randomly. If the server ever starts reporting either correctly, this fails and the
        record can be deleted.
        """
        dynamic_resolver = AsyncDynamicMetadataResolver(
            await live_metadata_records(stack_client)
        )
        static_resolver = AsyncStaticMetadataResolver()

        # The server says cacheable...
        assert getattr(await dynamic_resolver.resolve(name), field) is False, name
        assert await dynamic_resolver.is_cacheable(name) is True, name
        # ...the table says otherwise, and the table is right.
        assert getattr(await static_resolver.resolve(name), field) is True, name
        assert await static_resolver.is_cacheable(name) is False, name

        # And the static-first chain is what makes the record reachable with a live layer
        # behind it, which is why that ordering is the documented one.
        chain = static_resolver.with_fallback(dynamic_resolver)
        assert await chain.is_cacheable(name) is False, name

    @skip_if_server_version_lt(STATIC_TABLE_SERVER_VERSION)
    async def test_dynamic_resolver_covers_commands_the_static_table_does_not(
        self, stack_client
    ):
        """
        The commands the table leaves out, resolved from the server instead. These are the
        normalization paths the static entries do not exercise.
        """
        dynamic_resolver = AsyncDynamicMetadataResolver(
            await live_metadata_records(stack_client)
        )

        # Readonly, keyed, nothing that forbids caching - and absent from the table.
        pfcount_metadata = await dynamic_resolver.resolve("pfcount")
        assert pfcount_metadata.is_readonly is True
        assert pfcount_metadata.has_key_argument is True
        assert pfcount_metadata.is_dont_cache is False

        # A whole module surface the table does not carry.
        bf_exists_metadata = await dynamic_resolver.resolve("bf.exists")
        assert bf_exists_metadata.is_readonly is True

        # Readonly but keyless, so not cacheable.
        keys_metadata = await dynamic_resolver.resolve("keys")
        assert keys_metadata.has_key_argument is False

        # The script_runner flag, as the server reports it - the table carries its own
        # record for EVAL_RO, for the servers that do not. EVAL_RO is also
        # movablekeys, so its key argument is only visible in the key specs.
        eval_ro_metadata = await dynamic_resolver.resolve("eval_ro")
        assert eval_ro_metadata.is_script_runner is True
        assert eval_ro_metadata.has_key_argument is True

        # SSUBSCRIBE takes a shard channel, not a key: the server reports a key spec
        # flagged not_key while the legacy positions still say first_key_pos 1.
        ssubscribe_metadata = await dynamic_resolver.resolve("ssubscribe")
        assert ssubscribe_metadata.has_key_argument is False

        # Container subcommands resolve under their space-joined name.
        memory_usage_metadata = await dynamic_resolver.resolve("memory usage")
        assert memory_usage_metadata.is_readonly is True

    async def test_static_first_falls_back_to_the_server(self, stack_client):
        """The static-first chain of the unit tests, resolved against a real server."""
        static_resolver = AsyncStaticMetadataResolver()
        resolver = static_resolver.with_fallback(
            AsyncDynamicMetadataResolver(await live_metadata_records(stack_client))
        )

        # Not in the table, so the server answers.
        assert await static_resolver.resolve("pfcount") is None
        pfcount_metadata = await resolver.resolve("pfcount")
        assert pfcount_metadata.is_readonly is True

        # In the table, so the table answers - and keeps XPENDING and TS.INFO out of the
        # cache even though both are readonly and keyed on the server.
        xpending_metadata = await resolver.resolve("xpending")
        assert xpending_metadata.has_nondeterministic_output is True
        ts_info_metadata = await resolver.resolve("ts.info")
        assert ts_info_metadata.is_dont_cache is True

        # Unknown to the table and to the server.
        assert await resolver.resolve("nosuchmodule.nosuchcommand") is None
