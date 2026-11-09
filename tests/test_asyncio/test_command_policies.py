import random
from unittest.mock import patch

import pytest

from redis import ResponseError
from redis.exceptions import RedisClusterException
from redis.asyncio import RedisCluster
from redis.commands.metadata import AsyncDynamicMetadataResolver, RequestPolicy
from redis.commands.policies import AsyncStaticPolicyResolver
from redis.commands.search.aggregation import AggregateRequest, Cursor
from redis.commands.search.field import NumericField, TextField
from redis.utils import str_if_bytes
from tests.conftest import (
    expects_resp2_shape,
    expects_unified_shape,
    skip_if_server_version_lt,
)


@pytest.mark.onlycluster
@pytest.mark.asyncio
@skip_if_server_version_lt("8.0.0")
class TestClusterWithPolicies:
    async def test_resolves_correctly_policies(self, r: RedisCluster, monkeypatch):
        # original nodes selection method
        determine_nodes = r._determine_nodes
        determined_nodes = []
        primary_nodes = r.get_primaries()
        calls = iter(list(range(len(primary_nodes))))

        async def wrapper(*args, request_policy: RequestPolicy, **kwargs):
            nonlocal determined_nodes
            determined_nodes = await determine_nodes(
                *args, request_policy=request_policy, **kwargs
            )
            return determined_nodes

        # Mock random.choice to always return a pre-defined sequence of nodes
        monkeypatch.setattr(random, "choice", lambda seq: seq[next(calls)])

        with patch.object(r, "_determine_nodes", side_effect=wrapper, autospec=True):
            # Routed to a random primary node
            await r.ft().create_index(
                [
                    NumericField("random_num"),
                    TextField("title"),
                    TextField("body"),
                    TextField("parent"),
                ]
            )
            assert determined_nodes[0] == primary_nodes[0]

            # Routed to another random primary node
            info = await r.ft().info()

            if expects_resp2_shape(r) or expects_unified_shape(r):
                assert info["index_name"] == "idx"
            else:
                assert info[b"index_name"] == b"idx"

            assert determined_nodes[0] == primary_nodes[1]

            expected_node = await r.get_nodes_from_slot("FT.SUGLEN", *["foo"])
            await r.ft().suglen("foo")
            assert determined_nodes[0] == expected_node[0]

            # Indexing a document
            await r.hset(
                "search",
                mapping={
                    "title": "RediSearch",
                    "body": "Redisearch impements a search engine on top of redis",
                    "parent": "redis",
                    "random_num": 10,
                },
            )
            await r.hset(
                "ai",
                mapping={
                    "title": "RedisAI",
                    "body": "RedisAI executes Deep Learning/Machine Learning models and managing their data.",  # noqa
                    "parent": "redis",
                    "random_num": 3,
                },
            )
            await r.hset(
                "json",
                mapping={
                    "title": "RedisJson",
                    "body": "RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.",  # noqa
                    "parent": "redis",
                    "random_num": 8,
                },
            )

            req = AggregateRequest("redis").group_by("@parent").cursor(1)
            res = await r.ft().aggregate(req)

            if expects_resp2_shape(r) or expects_unified_shape(r):
                cursor = res.cursor
            else:
                cursor = Cursor(res[1])

            # Ensure that aggregate node was cached.
            assert determined_nodes[0] == r._aggregate_nodes[0]

            await r.ft().aggregate(cursor)

            # Verify that FT.CURSOR dispatched to the same node.
            assert determined_nodes[0] == r._aggregate_nodes[0]

            # Error propagates to a user
            with pytest.raises(ResponseError, match="Cursor not found, id:"):
                await r.ft().aggregate(cursor)

            assert determined_nodes[0] == primary_nodes[2]

            # RANDOMKEY is asked of every primary, and the key is picked among their
            # replies, so the pre-defined node sequence no longer applies.
            monkeypatch.undo()
            await r.randomkey()
            assert set(determined_nodes) == set(primary_nodes)

    async def test_dbsize_and_keys_answer_from_the_default_node_unless_fanned_out(
        self, r: RedisCluster
    ) -> None:
        """
        The server tips both ``all_shards``, but the shipped records withhold that tip, so
        they keep answering from the default node. A PRIMARIES flag fans them out.
        """
        keys = {f"all-shards-{index}" for index in range(8)}
        await r.mset_nonatomic({key: 1 for key in keys})
        # The keys must span shards, or the default node alone would answer the same.
        assert len({r.get_node_from_key(key).name for key in keys}) > 1
        default_node = r.get_default_node()

        with patch.object(r, "_execute_command", wraps=r._execute_command) as execute:
            await r.dbsize()
            await r.keys()

        assert [call.args[0] for call in execute.call_args_list] == [default_node] * 2

        assert await r.dbsize(target_nodes=r.PRIMARIES) == len(keys)
        keys_on_primaries = await r.keys(target_nodes=r.PRIMARIES)
        assert {str_if_bytes(key) for key in keys_on_primaries} == keys

    async def test_movablekeys_reads_route_by_their_keys(self, r: RedisCluster) -> None:
        await r.zadd("{movable}a", {"member": 1})
        await r.zadd("{movable}b", {"member": 2})
        expected_node = r.get_node_from_key("{movable}a")

        with patch.object(r, "_execute_command", wraps=r._execute_command) as execute:
            assert await r.zintercard(2, ["{movable}a", "{movable}b"]) == 1
            script = "return redis.call('zcard', KEYS[1])"
            assert await r.eval_ro(script, 1, "{movable}a") == 1

        # Each was sent straight to the node holding its keys, not redirected there.
        assert [call.args[0] for call in execute.call_args_list] == [expected_node] * 2

        # And a read-only script without keys runs on any node.
        assert await r.eval_ro("return 1", 0) == 1

    async def test_a_pipeline_answers_once_per_queued_command(
        self, r: RedisCluster
    ) -> None:
        keys = {f"fan-out-{index}" for index in range(4)}
        await r.mset_nonatomic({key: 1 for key in keys})

        async with r.pipeline() as pipe:
            pipe.execute_command("DBSIZE", target_nodes=r.PRIMARIES)
            pipe.get("fan-out-0")
            pipe.execute_command("KEYS", "fan-out-*", target_nodes=r.PRIMARIES)
            pipe.execute_command("PING", target_nodes=r.PRIMARIES)
            pipe.execute_command("PING", target_nodes=r.get_primaries())
            pipe.eval_ro("return 1", 0)
            results = await pipe.execute()

        assert len(results) == 6
        assert results[0] == len(keys)
        assert str_if_bytes(results[1]) == "1"
        assert {str_if_bytes(key) for key in results[2]} == keys
        assert results[3] is True
        assert results[4] is True
        assert results[5] == 1

    async def test_a_pipeline_reports_a_fanned_out_error_at_its_position(
        self, r: RedisCluster
    ) -> None:
        async with r.pipeline() as pipe:
            pipe.execute_command(
                "CONFIG SET", "no-such-parameter", "1", target_nodes=r.PRIMARIES
            )
            pipe.set("fan-out", 1)
            results = await pipe.execute(raise_on_error=False)

        assert len(results) == 2
        assert isinstance(results[0], ResponseError)
        assert results[1] is True

        async with r.pipeline() as pipe:
            pipe.set("fan-out", 1)
            pipe.execute_command(
                "CONFIG SET", "no-such-parameter", "1", target_nodes=r.PRIMARIES
            )
            with pytest.raises(ResponseError, match="Command # 2"):
                await pipe.execute()

    async def test_live_tips_fold_the_replies_by_their_response_policy(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        records = await r.commands_parser.get_commands_metadata_cache()
        monkeypatch.setattr(
            r,
            "_policy_resolver",
            AsyncStaticPolicyResolver(
                metadata_resolver=AsyncDynamicMetadataResolver(records)
            ),
        )

        # all_shards / agg_min: one [numlocal, numreplicas] pair, folded element-wise.
        waitaof = await r.execute_command("WAITAOF", 0, 0, 0)
        assert isinstance(waitaof, list) and len(waitaof) == 2
        # all_nodes / agg_sum: one count for the whole cluster.
        assert isinstance(await r.execute_command("LATENCY RESET"), int)
        # all_shards / all_succeeded.
        assert await r.execute_command("FUNCTION FLUSH") is True
        # all_shards / one_succeeded: no shard is running a script, so all answer NOTBUSY.
        with pytest.raises(ResponseError, match="NOTBUSY"):
            await r.execute_command("SCRIPT KILL")

    async def test_multi_shard_reads_split_by_slot(self, r: RedisCluster) -> None:
        keys = ["{shard-a}1", "{shard-b}1", "{shard-a}2", "{shard-c}1"]
        assert len({r.keyslot(key) for key in keys}) > 1
        await r.mset_nonatomic({key: 1 for key in keys})

        # EXISTS and TOUCH are split by slot and their counts summed.
        assert await r.execute_command("EXISTS", *keys, "{shard-b}missing") == len(keys)
        assert await r.execute_command("TOUCH", *keys) == len(keys)
        async with r.pipeline() as pipe:
            pipe.execute_command("EXISTS", *keys)
            pipe.get("{shard-a}1")
            results = await pipe.execute()
        assert results[0] == len(keys)
        assert str_if_bytes(results[1]) == "1"

        # MGET keeps the documented error for keys in different slots.
        with pytest.raises(RedisClusterException, match="same key slot"):
            await r.mget(keys)

    async def test_live_multi_shard_and_special_tips(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        records = await r.commands_parser.get_commands_metadata_cache()
        monkeypatch.setattr(
            r,
            "_policy_resolver",
            AsyncStaticPolicyResolver(
                metadata_resolver=AsyncDynamicMetadataResolver(records)
            ),
        )
        keys = ["{shard-a}1", "{shard-b}1", "{shard-a}2", "{shard-c}1"]

        # multi_shard / all_succeeded, then multi_shard with the values in key order.
        assert await r.execute_command(
            "MSET", *(arg for key in keys for arg in (key, key))
        )
        assert [str_if_bytes(value) for value in await r.mget(keys)] == keys
        async with r.pipeline() as pipe:
            pipe.execute_command("MGET", *reversed(keys))
            pipe.execute_command("DEL", *keys)
            results = await pipe.execute()
        assert [str_if_bytes(value) for value in results[0]] == keys[::-1]
        assert results[1] == len(keys)

        # all_shards / special: RANDOMKEY finds a key on whichever shard holds one.
        key = next(
            f"{{only}}{index}"
            for index in range(1000)
            if r.get_node_from_key(f"{{only}}{index}") != r.get_default_node()
        )
        await r.set(key, 1)
        assert str_if_bytes(await r.randomkey()) == key
        # INFO keeps each shard's reply, keyed by node.
        assert set(await r.info()) == {node.name for node in r.get_primaries()}
