import asyncio
from typing import Tuple
from unittest import mock
from unittest.mock import patch, Mock

import pytest

import redis
from redis import CrossSlotTransactionError, RedisClusterException
from redis.asyncio import RedisCluster, Connection
from redis.asyncio.cluster import ClusterNode, NodesManager
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.cluster import PRIMARY
from tests.conftest import skip_if_server_version_lt


def _find_source_and_target_node_for_slot(
    r: RedisCluster, slot: int
) -> Tuple[ClusterNode, ClusterNode]:
    """Returns a pair of ClusterNodes, where the first node is the
    one that owns the slot and the second is a possible target
    for that slot, i.e. a primary node different from the first
    one.
    """
    node_migrating = r.nodes_manager.get_node_from_slot(slot)
    assert node_migrating, f"No node could be found that owns slot #{slot}"

    available_targets = [
        n
        for n in r.nodes_manager.startup_nodes.values()
        if node_migrating.name != n.name and n.server_type == PRIMARY
    ]

    assert available_targets, f"No possible target nodes for slot #{slot}"
    return node_migrating, available_targets[0]


@pytest.mark.onlycluster
class TestClusterTransaction:
    @pytest.mark.onlycluster
    async def test_pipeline_is_true(self, r) -> None:
        "Ensure pipeline instances are not false-y"
        async with r.pipeline(transaction=True) as pipe:
            assert pipe

    @pytest.mark.onlycluster
    async def test_transaction_applies_response_callbacks(self, r) -> None:
        async with r.pipeline(transaction=True) as pipe:
            pipe.hset("{foo}hash", mapping={"a": "1", "b": "2"})
            pipe.hgetall("{foo}hash")
            result = await pipe.execute()

        assert result == [2, {b"a": b"1", b"b": b"2"}]

    @pytest.mark.onlycluster
    async def test_pipeline_empty_transaction(self, r):
        await r.set("a", 0)

        async with r.pipeline(transaction=True) as pipe:
            assert await pipe.execute() == []

    @pytest.mark.onlycluster
    async def test_executes_transaction_against_cluster(self, r) -> None:
        async with r.pipeline(transaction=True) as tx:
            tx.set("{foo}bar", "value1")
            tx.set("{foo}baz", "value2")
            tx.set("{foo}bad", "value3")
            tx.get("{foo}bar")
            tx.get("{foo}baz")
            tx.get("{foo}bad")
            assert await tx.execute() == [
                True,
                True,
                True,
                b"value1",
                b"value2",
                b"value3",
            ]

        await r.flushall()

        tx = r.pipeline(transaction=True)
        tx.set("{foo}bar", "value1")
        tx.set("{foo}baz", "value2")
        tx.set("{foo}bad", "value3")
        tx.get("{foo}bar")
        tx.get("{foo}baz")
        tx.get("{foo}bad")
        assert await tx.execute() == [True, True, True, b"value1", b"value2", b"value3"]

    @pytest.mark.onlycluster
    async def test_throws_exception_on_different_hash_slots(self, r):
        async with r.pipeline(transaction=True) as tx:
            tx.set("{foo}bar", "value1")
            tx.set("{foobar}baz", "value2")

            with pytest.raises(
                CrossSlotTransactionError,
                match="All keys involved in a cluster transaction must map to the same slot",
            ):
                await tx.execute()

    @pytest.mark.onlycluster
    async def test_throws_exception_with_watch_on_different_hash_slots(self, r):
        async with r.pipeline(transaction=True) as tx:
            with pytest.raises(
                RedisClusterException,
                match="WATCH - all keys must map to the same key slot",
            ):
                await tx.watch("key1", "key2")

    @pytest.mark.onlycluster
    async def test_transaction_with_watched_keys(self, r):
        await r.set("a", 0)

        async with r.pipeline(transaction=True) as pipe:
            await pipe.watch("a")
            a = await pipe.get("a")

            pipe.multi()
            pipe.set("a", int(a) + 1)
            assert await pipe.execute() == [True]

    @pytest.mark.onlycluster
    async def test_pipeline_reset_releases_connection_when_cancelled(self, r):
        # Regression test for https://github.com/redis/redis-py/issues/4121
        # (cluster variant): cancelling reset() while it awaits the UNWATCH
        # reply must still return the node-pooled connection.
        await r.set("a", 0)

        pipe = r.pipeline(transaction=True)
        await pipe.watch("a")

        strategy = pipe._execution_strategy
        leased = strategy._transaction_connection
        node = strategy._transaction_node
        assert leased is not None
        assert node is not None
        assert leased not in node._free

        started = asyncio.Event()

        async def hang(*_args, **_kwargs):
            started.set()
            await asyncio.sleep(60)

        with mock.patch.object(leased, "read_response", side_effect=hang):
            task = asyncio.ensure_future(pipe.reset())
            await started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        assert strategy._transaction_connection is None
        assert strategy._transaction_node is None
        assert strategy._watching is False
        assert leased in node._free

    @pytest.mark.onlycluster
    @pytest.mark.parametrize(
        "error_type",
        [
            redis.exceptions.MovedError,
            redis.exceptions.AskError,
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
            redis.exceptions.ClusterDownError,
            redis.exceptions.SlotNotCoveredError,
        ],
    )
    @pytest.mark.parametrize("command", ["GET", "WATCH", "EXEC"])
    @pytest.mark.parametrize("failure_point", ["send", "read"])
    async def test_retry_after_acknowledged_watch_raises(
        self, r, error_type, command, failure_point
    ):
        """A retried command must not silently abandon an acknowledged WATCH."""
        key = "book"
        slot = r.keyslot(key)
        node = r.nodes_manager.get_node_from_slot(slot)
        error = error_type(f"{slot} {node.name}")
        r.set_retry(Retry(NoBackoff(), 1))
        r.reinitialize_steps = 1

        async with r.pipeline(transaction=True) as pipe:
            await pipe.watch(key)
            strategy = pipe._execution_strategy
            connection = strategy._transaction_connection
            # A concurrent write must not be overwritten by an unwatched retry.
            await r.set(key, 100)
            if command == "EXEC":
                pipe.multi()
                pipe.set(key, 1)
                send_method = "send_packed_command"
            else:
                send_method = "send_command"
            original_send = getattr(connection, send_method)
            attempts = []

            async def fail_once(*args, **kwargs):
                attempts.append(args)
                if len(attempts) == 1:
                    raise error
                return await original_send(*args, **kwargs)

            if failure_point == "send":
                patcher = patch.object(connection, send_method, side_effect=fail_once)
            else:
                node_type = ClusterNode
                original_parse = node_type.parse_response

                async def fail_after_read(redis_node, conn, name, **options):
                    result = await original_parse(redis_node, conn, name, **options)
                    if name == command:
                        attempts.append(name)
                        if len(attempts) == 1:
                            raise error
                    return result

                patcher = patch.object(
                    node_type,
                    "parse_response",
                    autospec=True,
                    side_effect=fail_after_read,
                )

            with patcher:
                with pytest.raises(redis.exceptions.WatchError) as exc_info:
                    if command == "EXEC":
                        await pipe.execute()
                    else:
                        await pipe.execute_command(command, key)

            assert exc_info.value.__cause__ is error
            assert len(attempts) == 1
            assert strategy._transaction_connection is None
            assert not connection.is_connected
            assert await r.get(key) == b"100"

            # Reset and reuse must not retain acknowledged WATCH state.
            await pipe.reset()
            assert not strategy._watch_acknowledged
            await pipe.watch(key)
            assert strategy._watch_acknowledged
            await pipe.unwatch()
            assert not strategy._watch_acknowledged
            pipe.set(key, 2)
            assert await pipe.execute() == [True]
            assert not strategy._watch_acknowledged

    @pytest.mark.onlycluster
    @pytest.mark.parametrize(
        "error_type",
        [
            redis.exceptions.MovedError,
            redis.exceptions.AskError,
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
            redis.exceptions.ClusterDownError,
            redis.exceptions.SlotNotCoveredError,
        ],
    )
    async def test_error_before_watch_acknowledgement_is_retried(self, r, error_type):
        key = "book"
        slot = r.keyslot(key)
        node = r.nodes_manager.get_node_from_slot(slot)
        error = error_type(f"{slot} {node.name}")
        r.set_retry(Retry(NoBackoff(), 1))
        node_type = ClusterNode
        original_parse = node_type.parse_response
        attempts = []

        async def fail_first_watch_reply(redis_node, connection, command, **options):
            result = await original_parse(redis_node, connection, command, **options)
            if command == "WATCH":
                attempts.append(connection)
                if len(attempts) == 1:
                    raise error
            return result

        async with r.pipeline(transaction=True) as pipe:
            with patch.object(
                node_type,
                "parse_response",
                autospec=True,
                side_effect=fail_first_watch_reply,
            ):
                await pipe.watch(key)

            assert len(attempts) == 2
            assert pipe._execution_strategy._watch_acknowledged
            # The successful retry must actually install a new WATCH.
            await r.set(key, 100)
            pipe.multi()
            pipe.set(key, 1)
            with pytest.raises(redis.exceptions.WatchError, match="Watched variable"):
                await pipe.execute()
            assert await r.get(key) == b"100"
            assert not pipe._execution_strategy._watch_acknowledged

    @pytest.mark.onlycluster
    async def test_retry_transaction_during_unfinished_slot_migration(self, r):
        """
        When a transaction is triggered during a migration, MovedError
        or AskError may appear (depends on the key being already migrated
        or the key not existing already). The patch on parse_response
        simulates such an error, but the slot cache is not updated
        (meaning the migration is still ongoing) so the pipeline eventually
        fails as if it was retried but the migration is not yet complete.
        """
        key = "book"
        slot = r.keyslot(key)
        node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)

        original_parse_response = ClusterNode.parse_response
        with (
            patch.object(
                ClusterNode, "parse_response", autospec=True
            ) as parse_response,
            patch.object(NodesManager, "move_slot") as manager_move_slot,
        ):

            async def ask_redirect_effect(redis_node, connection, command, **options):
                if command == "MULTI":
                    return
                elif command == "EXEC":
                    raise redis.exceptions.ExecAbortError()
                elif command in ("_", "SET"):
                    raise redis.exceptions.AskError(f"{slot} {node_importing.name}")

                return await original_parse_response(
                    redis_node, connection, command, **options
                )

            parse_response.side_effect = ask_redirect_effect

            # the migration never completes, so one retry is enough;
            # move_slot still runs on the first failure
            r.set_retry(Retry(NoBackoff(), 1))
            async with r.pipeline(transaction=True) as pipe:
                pipe.set(key, "val")
                with pytest.raises(redis.exceptions.AskError) as ex:
                    await pipe.execute()

                assert str(ex.value).startswith(
                    "Command # 1 (SET book val) of pipeline caused error:"
                    f" {slot} {node_importing.name}"
                )

            manager_move_slot.assert_called()

    @pytest.mark.onlycluster
    async def test_retry_transaction_during_slot_migration_successful(
        self, create_redis
    ):
        """
        If a MovedError or AskError appears when calling EXEC and no key is watched,
        the pipeline is retried after updating the node manager slot table. If the
        migration was completed, the transaction may then complete successfully.
        """
        r = await create_redis(flushdb=False)
        key = "book"
        slot = r.keyslot(key)
        node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)

        with (
            patch.object(ClusterNode, "parse_response") as parse_response,
        ):

            def ask_redirect_effect(conn, *args, **options):
                # first call should go here, we trigger an AskError
                if f"{conn.host}:{conn.port}" == node_migrating.name:
                    if "MULTI" in args:
                        return
                    elif "EXEC" in args:
                        raise redis.exceptions.ExecAbortError()

                    raise redis.exceptions.AskError(f"{slot} {node_importing.name}")
                # if the slot table is updated, the next call will go here
                elif f"{conn.host}:{conn.port}" == node_importing.name:
                    if "EXEC" in args:
                        return ["OK"]  # mock value to validate this section was called
                    return
                else:
                    assert False, f"unexpected node {conn.host}:{conn.port} was called"

            parse_response.side_effect = ask_redirect_effect

            result = None
            async with r.pipeline(transaction=True) as pipe:
                pipe.multi()
                pipe.set(key, "val")
                result = await pipe.execute()

            assert result and True in result, "Target node was not called"

    @pytest.mark.onlycluster
    async def test_retry_transaction_with_watch_after_slot_migration(self, r):
        """
        If a MovedError or AskError appears when calling WATCH, the client
        must attempt to recover itself before proceeding and no WatchError
        should appear.
        """
        key = "book"
        slot = r.keyslot(key)
        r.reinitialize_steps = 1

        # force a MovedError on the first call to pipe.watch()
        # by switching the node that owns the slot to another one
        _node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)
        r.nodes_manager.slots_cache[slot] = [node_importing]

        async with r.pipeline(transaction=True) as pipe:
            await pipe.watch(key)
            pipe.multi()
            pipe.set(key, "val")
            assert await pipe.execute() == [True]

    @pytest.mark.onlycluster
    async def test_retry_transaction_with_watch_during_slot_migration(self, r):
        """
        If a MovedError or AskError appears when calling EXEC and keys were
        being watched before the migration started, a WatchError should appear.
        These errors imply resetting the connection and connecting to a new node,
        so watches are lost anyway and the client code must be notified.
        """
        key = "book"
        slot = r.keyslot(key)
        node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)

        with patch.object(ClusterNode, "parse_response") as parse_response:

            def ask_redirect_effect(conn, *args, **options):
                if f"{conn.host}:{conn.port}" == node_migrating.name:
                    # we simulate the watch was sent before the migration started
                    if "WATCH" in args:
                        return b"OK"
                    # but the pipeline was triggered after the migration started
                    elif "MULTI" in args:
                        return
                    elif "EXEC" in args:
                        raise redis.exceptions.ExecAbortError()

                    raise redis.exceptions.AskError(f"{slot} {node_importing.name}")
                # we should not try to connect to any other node
                else:
                    assert False, f"unexpected node {conn.host}:{conn.port} was called"

            parse_response.side_effect = ask_redirect_effect

            async with r.pipeline(transaction=True) as pipe:
                await pipe.watch(key)

                pipe.multi()
                pipe.set(key, "val")
                with pytest.raises(redis.exceptions.WatchError) as ex:
                    await pipe.execute()

                assert str(ex.value).startswith(
                    "Slot rebalancing occurred while watching keys"
                )

    @pytest.mark.usefixtures("no_topology_refresh_interval", "no_cluster_retry_delay")
    @pytest.mark.onlycluster
    async def test_retry_transaction_on_connection_error(self, r):
        key = "book"
        slot = r.keyslot(key)

        _node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)
        original_slots_cache = r.nodes_manager.slots_cache[slot]

        mock_connection = Mock(spec=Connection)
        mock_connection.read_response.side_effect = redis.exceptions.ConnectionError(
            "Conn error"
        )
        mock_connection.retry = Retry(NoBackoff(), 0)
        mock_connection.host = node_importing.host
        mock_connection.port = node_importing.port
        mock_connection.db = 0

        node_importing._free.append(mock_connection)
        r.nodes_manager.slots_cache[slot] = [node_importing]
        r.reinitialize_steps = 1

        try:
            async with r.pipeline(transaction=True) as pipe:
                pipe.set(key, "val")
                assert await pipe.execute() == [True]

            assert mock_connection.read_response.call_count == 1
        finally:
            # Clean up mock connection from node so teardown can work
            if mock_connection in node_importing._free:
                node_importing._free.remove(mock_connection)
            r.nodes_manager.slots_cache[slot] = original_slots_cache

    @pytest.mark.usefixtures("no_topology_refresh_interval", "no_cluster_retry_delay")
    @pytest.mark.onlycluster
    async def test_retry_transaction_on_connection_error_with_watched_keys(self, r):
        key = "book"
        slot = r.keyslot(key)

        _node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)
        original_slots_cache = r.nodes_manager.slots_cache[slot]

        mock_connection = Mock(spec=Connection)
        mock_connection.read_response.side_effect = redis.exceptions.ConnectionError(
            "Conn error"
        )
        mock_connection.retry = Retry(NoBackoff(), 0)
        mock_connection.host = node_importing.host
        mock_connection.port = node_importing.port
        mock_connection.db = 0

        node_importing._free.append(mock_connection)
        r.nodes_manager.slots_cache[slot] = [node_importing]
        r.reinitialize_steps = 1

        try:
            async with r.pipeline(transaction=True) as pipe:
                await pipe.watch(key)

                pipe.multi()
                pipe.set(key, "val")
                assert await pipe.execute() == [True]

            assert mock_connection.read_response.call_count == 1
        finally:
            # Clean up mock connection from node so teardown can work
            if mock_connection in node_importing._free:
                node_importing._free.remove(mock_connection)
            r.nodes_manager.slots_cache[slot] = original_slots_cache

    @pytest.mark.onlycluster
    async def test_watch_connection_stays_with_its_node_after_slot_move(self, r):
        """
        A slot moved by a refresh while keys are watched does not pair the held
        connection with the new owner: commands keep using the node the
        connection belongs to, and the connection goes back to that node's pool.
        """
        key = "book"
        slot = r.keyslot(key)
        node_owning, node_other = _find_source_and_target_node_for_slot(r, slot)
        original_slots_cache = r.nodes_manager.slots_cache[slot]

        try:
            async with r.pipeline(transaction=True) as pipe:
                await pipe.watch(key)
                connection = pipe._execution_strategy._transaction_connection
                # As a refresh that moves the slot to another node would
                r.nodes_manager.slots_cache[slot] = [node_other]
                await pipe.get(key)

                pipe.multi()
                pipe.set(key, "val")
                assert await pipe.execute() == [True]

            assert connection in node_owning._free
            assert connection not in node_other._free
        finally:
            r.nodes_manager.slots_cache[slot] = original_slots_cache

    @pytest.mark.onlycluster
    async def test_exec_error_raised(self, r):
        hashkey = "{key}"
        await r.set(f"{hashkey}:c", "a")

        async with r.pipeline(transaction=True) as pipe:
            pipe.set(f"{hashkey}:a", 1).set(f"{hashkey}:b", 2)
            pipe.lpush(f"{hashkey}:c", 3).set(f"{hashkey}:d", 4)
            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()
            assert str(ex.value).startswith(
                "Command # 3 (LPUSH {key}:c 3) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert await pipe.set(f"{hashkey}:z", "zzz").execute() == [True]
            assert await r.get(f"{hashkey}:z") == b"zzz"

    @pytest.mark.onlycluster
    async def test_parse_error_raised(self, r):
        hashkey = "{key}"
        async with r.pipeline(transaction=True) as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set(f"{hashkey}:a", 1).zrem(f"{hashkey}:b").set(f"{hashkey}:b", 2)
            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()

            assert str(ex.value).startswith(
                "Command # 2 (ZREM {key}:b) of pipeline caused error: wrong number"
            )

            # make sure the pipe was restored to a working state
            assert await pipe.set(f"{hashkey}:z", "zzz").execute() == [True]
            assert await r.get(f"{hashkey}:z") == b"zzz"

    @pytest.mark.onlycluster
    async def test_transaction_callable(self, r):
        hashkey = "{key}"
        await r.set(f"{hashkey}:a", 1)
        await r.set(f"{hashkey}:b", 2)
        has_run = []

        async def my_transaction(pipe):
            a_value = await pipe.get(f"{hashkey}:a")
            assert a_value in (b"1", b"2")
            b_value = await pipe.get(f"{hashkey}:b")
            assert b_value == b"2"

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"
            if not has_run:
                await r.incr(f"{hashkey}:a")
                has_run.append("it has")

            pipe.multi()
            pipe.set(f"{hashkey}:c", int(a_value) + int(b_value))

        result = await r.transaction(my_transaction, f"{hashkey}:a", f"{hashkey}:b")
        assert result == [True]
        assert await r.get(f"{hashkey}:c") == b"4"

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("2.0.0")
    async def test_transaction_discard(self, r):
        hashkey = "{key}"

        # pipelines enabled as transactions can be discarded at any point
        async with r.pipeline(transaction=True) as pipe:
            await pipe.watch(f"{hashkey}:key")
            await pipe.set(f"{hashkey}:key", "someval")
            await pipe.discard()

            assert not pipe._execution_strategy._watching
            assert not len(pipe)
