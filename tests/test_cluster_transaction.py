import threading
from typing import Tuple
from unittest.mock import patch, Mock

import pytest
from redis.exceptions import ResponseError

import redis
from redis import CrossSlotTransactionError, ConnectionPool, RedisClusterException
from redis.backoff import NoBackoff
from redis.client import Redis
from redis.cluster import PRIMARY, ClusterNode, RedisCluster
from redis.observability import recorder
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector
from redis.retry import Retry

from .conftest import skip_if_server_version_lt


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


class TestClusterTransaction:
    @pytest.mark.onlycluster
    def test_pipeline_is_true(self, r):
        "Ensure pipeline instances are not false-y"
        with r.pipeline(transaction=True) as pipe:
            assert pipe

    @pytest.mark.onlycluster
    def test_pipeline_empty_transaction(self, r):
        r["a"] = 0

        with r.pipeline(transaction=True) as pipe:
            assert pipe.execute() == []

    @pytest.mark.onlycluster
    def test_executes_transaction_against_cluster(self, r):
        with r.pipeline(transaction=True) as tx:
            tx.set("{foo}bar", "value1")
            tx.set("{foo}baz", "value2")
            tx.set("{foo}bad", "value3")
            tx.get("{foo}bar")
            tx.get("{foo}baz")
            tx.get("{foo}bad")
            assert tx.execute() == [
                b"OK",
                b"OK",
                b"OK",
                b"value1",
                b"value2",
                b"value3",
            ]

        r.flushall()

        tx = r.pipeline(transaction=True)
        tx.set("{foo}bar", "value1")
        tx.set("{foo}baz", "value2")
        tx.set("{foo}bad", "value3")
        tx.get("{foo}bar")
        tx.get("{foo}baz")
        tx.get("{foo}bad")
        assert tx.execute() == [b"OK", b"OK", b"OK", b"value1", b"value2", b"value3"]

    @pytest.mark.onlycluster
    def test_throws_exception_on_different_hash_slots(self, r):
        with r.pipeline(transaction=True) as tx:
            tx.set("{foo}bar", "value1")
            tx.set("{foobar}baz", "value2")

            with pytest.raises(
                CrossSlotTransactionError,
                match="All keys involved in a cluster transaction must map to the same slot",
            ):
                tx.execute()

    @pytest.mark.onlycluster
    def test_throws_exception_with_watch_on_different_hash_slots(self, r):
        with r.pipeline(transaction=True) as tx:
            with pytest.raises(
                RedisClusterException,
                match="WATCH - all keys must map to the same key slot",
            ):
                tx.watch("key1", "key2")

    @pytest.mark.onlycluster
    def test_transaction_with_watched_keys(self, r):
        r["a"] = 0

        with r.pipeline(transaction=True) as pipe:
            pipe.watch("a")
            a = pipe.get("a")
            pipe.multi()
            pipe.set("a", int(a) + 1)
            assert pipe.execute() == [b"OK"]

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
    def test_retry_after_acknowledged_watch_raises(
        self, r, error_type, command, failure_point
    ):
        """A retried command must not silently abandon an acknowledged WATCH."""
        key = "book"
        slot = r.keyslot(key)
        node = r.nodes_manager.get_node_from_slot(slot)
        error = error_type(f"{slot} {node.name}")
        r.set_retry(Retry(NoBackoff(), 1))
        r.reinitialize_steps = 1

        with r.pipeline(transaction=True) as pipe:
            pipe.watch(key)
            strategy = pipe._execution_strategy
            connection = strategy._transaction_connection
            # A concurrent write must not be overwritten by an unwatched retry.
            r.set(key, 100)
            if command == "EXEC":
                pipe.multi()
                pipe.set(key, 1)
                send_method = "send_packed_command"
            else:
                send_method = "send_command"
            original_send = getattr(connection, send_method)
            attempts = []

            def fail_once(*args, **kwargs):
                attempts.append(args)
                if len(attempts) == 1:
                    raise error
                return original_send(*args, **kwargs)

            if failure_point == "send":
                patcher = patch.object(connection, send_method, side_effect=fail_once)
            else:
                node_type = Redis
                original_parse = node_type.parse_response

                def fail_after_read(redis_node, conn, name, **options):
                    result = original_parse(redis_node, conn, name, **options)
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
                        pipe.execute()
                    else:
                        pipe.execute_command(command, key)

            assert exc_info.value.__cause__ is error
            assert len(attempts) == 1
            assert strategy._transaction_connection is None
            assert not connection.is_connected
            assert r.get(key) == b"100"

            # Reset and reuse must not retain acknowledged WATCH state.
            pipe.reset()
            assert not strategy._watch_acknowledged
            pipe.watch(key)
            assert strategy._watch_acknowledged
            pipe.unwatch()
            assert not strategy._watch_acknowledged
            pipe.set(key, 2)
            assert pipe.execute() == [b"OK"]
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
    def test_error_before_watch_acknowledgement_is_retried(self, r, error_type):
        key = "book"
        slot = r.keyslot(key)
        node = r.nodes_manager.get_node_from_slot(slot)
        error = error_type(f"{slot} {node.name}")
        r.set_retry(Retry(NoBackoff(), 1))
        node_type = Redis
        original_parse = node_type.parse_response
        attempts = []

        def fail_first_watch_reply(redis_node, connection, command, **options):
            result = original_parse(redis_node, connection, command, **options)
            if command == "WATCH":
                attempts.append(connection)
                if len(attempts) == 1:
                    raise error
            return result

        with r.pipeline(transaction=True) as pipe:
            with patch.object(
                node_type,
                "parse_response",
                autospec=True,
                side_effect=fail_first_watch_reply,
            ):
                pipe.watch(key)

            assert len(attempts) == 2
            assert pipe._execution_strategy._watch_acknowledged
            # The successful retry must actually install a new WATCH.
            r.set(key, 100)
            pipe.multi()
            pipe.set(key, 1)
            with pytest.raises(redis.exceptions.WatchError, match="Watched variable"):
                pipe.execute()
            assert r.get(key) == b"100"
            assert not pipe._execution_strategy._watch_acknowledged

    @pytest.mark.onlycluster
    def test_retry_transaction_during_unfinished_slot_migration(self, r):
        """
        When a transaction is triggered during a migration, MovedError
        or AskError may appear (depends on the key being already migrated
        or the key not existing already). The patch on parse_response
        simulates such an error, but the slot cache is not updated
        (meaning the migration is still ongogin) so the pipeline eventually
        fails as if it was retried but the migration is not yet complete.
        """
        key = "book"
        slot = r.keyslot(key)
        node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)

        original_parse_response = Redis.parse_response
        with (
            patch.object(Redis, "parse_response", autospec=True) as parse_response,
        ):

            def ask_redirect_effect(redis_node, connection, command, **options):
                if command == "MULTI":
                    return
                elif command == "EXEC":
                    raise redis.exceptions.ExecAbortError()
                elif command in ("_", "SET"):
                    raise redis.exceptions.AskError(f"{slot} {node_importing.name}")

                return original_parse_response(
                    redis_node, connection, command, **options
                )

            parse_response.side_effect = ask_redirect_effect

            # the migration never completes, so one retry is enough;
            # move_slot still runs on the first failure
            r.set_retry(Retry(NoBackoff(), 1))
            with r.pipeline(transaction=True) as pipe:
                pipe.set(key, "val")
                with pytest.raises(redis.exceptions.AskError) as ex:
                    pipe.execute()

                assert str(ex.value).startswith(
                    "Command # 1 (SET book val) of pipeline caused error:"
                    f" {slot} {node_importing.name}"
                )

    @pytest.mark.onlycluster
    def test_retry_transaction_during_slot_migration_successful(self, r):
        """
        If a MovedError or AskError appears when calling EXEC and no key is watched,
        the pipeline is retried after updating the node manager slot table. If the
        migration was completed, the transaction may then complete successfully.
        """
        key = "book"
        slot = r.keyslot(key)
        node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)

        with (
            patch.object(Redis, "parse_response") as parse_response,
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
                        return [
                            "MOCK_OK"
                        ]  # mock value to validate this section was called
                    return
                else:
                    assert False, f"unexpected node {conn.host}:{conn.port} was called"

            parse_response.side_effect = ask_redirect_effect

            result = None
            with r.pipeline(transaction=True) as pipe:
                pipe.multi()
                pipe.set(key, "val")
                result = pipe.execute()

            assert result and "MOCK_OK" in result, "Target node was not called"

    @pytest.mark.onlycluster
    def test_retry_transaction_with_watch_after_slot_migration(self, r):
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

        with r.pipeline(transaction=True) as pipe:
            pipe.watch(key)
            pipe.multi()
            pipe.set(key, "val")
            assert pipe.execute() == [b"OK"]

    @pytest.mark.onlycluster
    def test_retry_transaction_with_watch_during_slot_migration(self, r):
        """
        If a MovedError or AskError appears when calling EXEC and keys were
        being watched before the migration started, a WatchError should appear.
        These errors imply resetting the connection and connecting to a new node,
        so watches are lost anyway and the client code must be notified.
        """
        key = "book"
        slot = r.keyslot(key)
        node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)

        with patch.object(Redis, "parse_response") as parse_response:

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

            with r.pipeline(transaction=True) as pipe:
                pipe.watch(key)
                pipe.multi()
                pipe.set(key, "val")
                with pytest.raises(redis.exceptions.WatchError) as ex:
                    pipe.execute()

                assert str(ex.value).startswith(
                    "Slot rebalancing occurred while watching keys"
                )

    @pytest.mark.onlycluster
    def test_watch_connection_stays_with_its_node_after_slot_move(self, r):
        """
        A requested refresh that moves the slot while keys are watched does not
        pair the held connection with the new owner: commands keep using the
        node the connection belongs to, and the connection goes back to that
        node's pool.
        """
        key = "book"
        slot = r.keyslot(key)
        node_owning, node_other = _find_source_and_target_node_for_slot(r, slot)
        original_slots_cache = r.nodes_manager.slots_cache[slot]

        def move_slot(retry=None):
            # As a refresh that moves the slot to another node would
            r.nodes_manager._refresh_requested = False
            r.nodes_manager.slots_cache[slot] = [node_other]

        try:
            with patch.object(
                r.nodes_manager, "_run_requested_refresh", side_effect=move_slot
            ):
                with r.pipeline(transaction=True) as pipe:
                    pipe.watch(key)
                    strategy = pipe._execution_strategy
                    connection = strategy._transaction_connection
                    r.nodes_manager._refresh_requested = True
                    pipe.get(key)
                    assert strategy._get_client_and_connection_for_transaction() == (
                        node_owning.redis_connection,
                        connection,
                    )

                    pipe.multi()
                    pipe.set(key, "val")
                    assert len(pipe.execute()) == 1

            owning_pool = node_owning.redis_connection.connection_pool
            assert connection in owning_pool._available_connections
            assert (
                connection
                not in node_other.redis_connection.connection_pool._available_connections
            )
        finally:
            r.nodes_manager._refresh_requested = False
            r.nodes_manager.slots_cache[slot] = original_slots_cache
        assert r.get(key) == b"val"

    @pytest.mark.onlycluster
    @pytest.mark.parametrize("change", ["removed", "replaced"])
    @pytest.mark.parametrize("release", ["reset", "error"])
    def test_watch_connection_returns_to_its_node_after_node_left(
        self, r, change, release
    ):
        """
        A refresh that removes the node of the held WATCH connection, or
        replaces it with a new instance under the same name, does not leak the
        connection: it goes back to the pool of the node it was taken from,
        both when the pipeline is reset and on the error path.
        """
        key = "book"
        node_owning = r.nodes_manager.get_node_from_slot(r.keyslot(key))
        owning_pool = node_owning.redis_connection.connection_pool
        replacement = None
        try:
            with r.pipeline(transaction=True) as pipe:
                pipe.watch(key)
                strategy = pipe._execution_strategy
                connection = strategy._transaction_connection
                assert connection in owning_pool._in_use_connections

                # As a refresh that drops the node, or rebuilds it, would
                if change == "removed":
                    del r.nodes_manager.nodes_cache[node_owning.name]
                else:
                    replacement = ClusterNode(
                        node_owning.host,
                        node_owning.port,
                        PRIMARY,
                        Redis(host=node_owning.host, port=node_owning.port),
                    )
                    r.nodes_manager.nodes_cache[node_owning.name] = replacement

                if release == "error":
                    with pytest.raises(redis.exceptions.WatchError):
                        strategy._reinitialize_on_error(
                            redis.exceptions.ConnectionError("Conn error"), 1
                        )
                    assert strategy._transaction_connection is None
                    assert not connection.is_connected

            assert connection not in owning_pool._in_use_connections
            assert connection in owning_pool._available_connections
            if replacement is not None:
                replacement_pool = replacement.redis_connection.connection_pool
                assert connection not in replacement_pool._available_connections
        finally:
            r.nodes_manager.nodes_cache[node_owning.name] = node_owning
            r.nodes_manager._refresh_requested = False
            if replacement is not None:
                replacement.redis_connection.close()

    @pytest.mark.usefixtures("no_topology_refresh_interval", "no_cluster_retry_delay")
    @pytest.mark.onlycluster
    def test_retry_transaction_on_connection_error(self, r, mock_connection):
        key = "book"
        slot = r.keyslot(key)

        mock_connection.read_response.side_effect = redis.exceptions.ConnectionError(
            "Conn error"
        )
        mock_connection.retry = Retry(NoBackoff(), 0)
        mock_pool = Mock(spec=ConnectionPool)
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [mock_connection]
        mock_pool._lock = threading.RLock()

        _node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)
        # Set mock connection's host/port/db to match the node for find_connection_owner
        mock_connection.host = node_importing.host
        mock_connection.port = node_importing.port
        mock_connection.db = 0
        # Save original pool to restore later
        original_pool = node_importing.redis_connection.connection_pool
        original_slots_cache = r.nodes_manager.slots_cache[slot]
        try:
            node_importing.redis_connection.connection_pool = mock_pool
            r.nodes_manager.slots_cache[slot] = [node_importing]
            r.reinitialize_steps = 1

            with r.pipeline(transaction=True) as pipe:
                pipe.set(key, "val")
                assert pipe.execute() == [b"OK"]
        finally:
            # Restore original pool so teardown can work
            node_importing.redis_connection.connection_pool = original_pool
            r.nodes_manager.slots_cache[slot] = original_slots_cache

    @pytest.mark.usefixtures("no_topology_refresh_interval", "no_cluster_retry_delay")
    @pytest.mark.onlycluster
    def test_retry_transaction_on_connection_error_with_watched_keys(
        self, r, mock_connection
    ):
        key = "book"
        slot = r.keyslot(key)

        mock_connection.read_response.side_effect = redis.exceptions.ConnectionError(
            "Conn error"
        )
        mock_connection.retry = Retry(NoBackoff(), 0)
        mock_pool = Mock(spec=ConnectionPool)
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [mock_connection]
        mock_pool._lock = threading.RLock()
        mock_pool.connection_kwargs = {}

        _node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)
        # Set mock connection's host/port/db to match the node for find_connection_owner
        mock_connection.host = node_importing.host
        mock_connection.port = node_importing.port
        mock_connection.db = 0
        # Save original pool to restore later
        original_pool = node_importing.redis_connection.connection_pool
        original_slots_cache = r.nodes_manager.slots_cache[slot]
        try:
            node_importing.redis_connection.connection_pool = mock_pool
            r.nodes_manager.slots_cache[slot] = [node_importing]
            r.reinitialize_steps = 1

            with r.pipeline(transaction=True) as pipe:
                pipe.watch(key)
                pipe.multi()
                pipe.set(key, "val")
                assert pipe.execute() == [b"OK"]
        finally:
            # Restore original pool so teardown can work
            node_importing.redis_connection.connection_pool = original_pool
            r.nodes_manager.slots_cache[slot] = original_slots_cache

    @pytest.mark.onlycluster
    def test_exec_error_raised(self, r):
        hashkey = "{key}"
        r[f"{hashkey}:c"] = "a"
        with r.pipeline(transaction=True) as pipe:
            pipe.set(f"{hashkey}:a", 1).set(f"{hashkey}:b", 2)
            pipe.lpush(f"{hashkey}:c", 3).set(f"{hashkey}:d", 4)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()
            assert str(ex.value).startswith(
                "Command # 3 (LPUSH {key}:c 3) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert pipe.set(f"{hashkey}:z", "zzz").execute() == [b"OK"]
            assert r[f"{hashkey}:z"] == b"zzz"

    @pytest.mark.onlycluster
    def test_parse_error_raised(self, r):
        hashkey = "{key}"
        with r.pipeline(transaction=True) as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set(f"{hashkey}:a", 1).zrem(f"{hashkey}:b").set(f"{hashkey}:b", 2)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert str(ex.value).startswith(
                "Command # 2 (ZREM {key}:b) of pipeline caused error: wrong number"
            )

            # make sure the pipe was restored to a working state
            assert pipe.set(f"{hashkey}:z", "zzz").execute() == [b"OK"]
            assert r[f"{hashkey}:z"] == b"zzz"

    @pytest.mark.onlycluster
    def test_transaction_callable(self, r):
        hashkey = "{key}"
        r[f"{hashkey}:a"] = 1
        r[f"{hashkey}:b"] = 2
        has_run = []

        def my_transaction(pipe):
            a_value = pipe.get(f"{hashkey}:a")
            assert a_value in (b"1", b"2")
            b_value = pipe.get(f"{hashkey}:b")
            assert b_value == b"2"

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"
            if not has_run:
                r.incr(f"{hashkey}:a")
                has_run.append("it has")

            pipe.multi()
            pipe.set(f"{hashkey}:c", int(a_value) + int(b_value))

        result = r.transaction(my_transaction, f"{hashkey}:a", f"{hashkey}:b")
        assert result == [b"OK"]
        assert r[f"{hashkey}:c"] == b"4"

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("2.0.0")
    def test_transaction_discard(self, r):
        hashkey = "{key}"

        # pipelines enabled as transactions can be discarded at any point
        with r.pipeline(transaction=True) as pipe:
            pipe.watch(f"{hashkey}:key")
            pipe.set(f"{hashkey}:key", "someval")
            pipe.discard()

            assert not pipe._execution_strategy._watching
            assert not pipe.command_stack


@pytest.mark.onlycluster
class TestClusterTransactionMetricsRecording:
    """
    Integration tests that verify metrics are properly recorded
    from ClusterPipeline (transaction mode) and delivered to the Meter through
    the observability recorder.

    These tests use a real Redis cluster connection but mock the OTel Meter
    to verify metrics are correctly recorded.
    """

    @pytest.fixture
    def mock_meter(self):
        """Create a mock Meter that tracks all instrument calls."""
        meter = Mock()

        # Create mock histogram for operation duration
        self.operation_duration = Mock()

        def create_histogram_side_effect(name, **kwargs):
            if name == "db.client.operation.duration":
                return self.operation_duration
            return Mock()

        meter.create_counter.return_value = Mock()
        meter.create_up_down_counter.return_value = Mock()
        meter.create_histogram.side_effect = create_histogram_side_effect

        return meter

    @pytest.fixture
    def cluster_transaction_with_otel(self, r, mock_meter):
        """
        Setup a ClusterPipeline (transaction mode) with real connection
        and mocked OTel collector.
        Returns tuple of (cluster, operation_duration_mock).
        """

        # Reset any existing collector state
        recorder.reset_collector()

        # Create config with COMMAND group enabled
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        # Create collector with mocked meter
        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Patch the recorder to use our collector
        with patch.object(recorder, "_metrics_collector", collector):
            yield r, self.operation_duration

        # Cleanup
        recorder.reset_collector()

    def test_transaction_execute_records_metric(self, cluster_transaction_with_otel):
        """
        Test that transaction execute records operation duration metric to Meter.
        """
        cluster, operation_duration_mock = cluster_transaction_with_otel

        # Execute a transaction
        with cluster.pipeline(transaction=True) as tx:
            tx.set("{tx_key}1", "value1")
            tx.get("{tx_key}1")
            tx.execute()

        # Verify the Meter's histogram.record() was called
        operation_duration_mock.record.assert_called()

        # Find the TRANSACTION event call
        transaction_call = None
        for call_obj in operation_duration_mock.record.call_args_list:
            attrs = call_obj[1]["attributes"]
            if attrs.get("db.operation.name") == "TRANSACTION":
                transaction_call = call_obj
                break

        assert transaction_call is not None

        # Verify duration was recorded
        duration = transaction_call[0][0]
        assert isinstance(duration, float)
        assert duration >= 0

        # Verify attributes
        attrs = transaction_call[1]["attributes"]
        assert attrs["db.operation.name"] == "TRANSACTION"

    def test_transaction_server_attributes_recorded(
        self, cluster_transaction_with_otel
    ):
        """
        Test that server address, port, and db namespace are recorded for transaction.
        """
        cluster, operation_duration_mock = cluster_transaction_with_otel

        with cluster.pipeline(transaction=True) as tx:
            tx.set("{server_attr}key", "value")
            tx.execute()

        # Find the TRANSACTION event call
        transaction_call = None
        for call_obj in operation_duration_mock.record.call_args_list:
            attrs = call_obj[1]["attributes"]
            if attrs.get("db.operation.name") == "TRANSACTION":
                transaction_call = call_obj
                break

        assert transaction_call is not None
        attrs = transaction_call[1]["attributes"]

        # Verify server attributes are present
        assert "server.address" in attrs
        assert isinstance(attrs["server.address"], str)

        assert "server.port" in attrs
        assert isinstance(attrs["server.port"], int)

        assert "db.namespace" in attrs

    def test_transaction_duration_is_positive(self, cluster_transaction_with_otel):
        """
        Test that the recorded duration for transaction is a positive float.
        """
        cluster, operation_duration_mock = cluster_transaction_with_otel

        with cluster.pipeline(transaction=True) as tx:
            tx.set("{duration}key", "value")
            tx.execute()

        # Find the TRANSACTION event call
        transaction_call = None
        for call_obj in operation_duration_mock.record.call_args_list:
            attrs = call_obj[1]["attributes"]
            if attrs.get("db.operation.name") == "TRANSACTION":
                transaction_call = call_obj
                break

        assert transaction_call is not None
        duration = transaction_call[0][0]
        assert isinstance(duration, float)
        assert duration >= 0

    def test_multiple_transaction_executions_record_multiple_metrics(
        self, cluster_transaction_with_otel
    ):
        """
        Test that multiple transaction executions record multiple metrics.
        """
        cluster, operation_duration_mock = cluster_transaction_with_otel

        # Execute first transaction
        with cluster.pipeline(transaction=True) as tx1:
            tx1.set("{multi1}key", "value1")
            tx1.execute()

        # Execute second transaction
        with cluster.pipeline(transaction=True) as tx2:
            tx2.set("{multi2}key", "value2")
            tx2.execute()

        # Count TRANSACTION events
        transaction_count = sum(
            1
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "TRANSACTION"
        )

        assert transaction_count >= 2

    def test_empty_transaction_does_not_record_metric(
        self, cluster_transaction_with_otel
    ):
        """
        Test that an empty transaction does not record TRANSACTION metrics.
        """
        cluster, operation_duration_mock = cluster_transaction_with_otel

        # Execute an empty transaction
        with cluster.pipeline(transaction=True) as tx:
            tx.execute()

        # Count TRANSACTION events - should be 0
        transaction_count = sum(
            1
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "TRANSACTION"
        )

        assert transaction_count == 0

    def test_transaction_with_watch_records_metric(self, cluster_transaction_with_otel):
        """
        Test that transaction with WATCH records metric correctly.
        """
        cluster, operation_duration_mock = cluster_transaction_with_otel

        # Set initial value
        cluster.set("{watch}key", "0")

        with cluster.pipeline(transaction=True) as tx:
            tx.watch("{watch}key")
            val = tx.get("{watch}key")
            tx.multi()
            tx.set("{watch}key", int(val or 0) + 1)
            tx.execute()

        # Find the TRANSACTION event call
        transaction_call = None
        for call_obj in operation_duration_mock.record.call_args_list:
            attrs = call_obj[1]["attributes"]
            if attrs.get("db.operation.name") == "TRANSACTION":
                transaction_call = call_obj
                break

        assert transaction_call is not None
        attrs = transaction_call[1]["attributes"]
        assert attrs["db.operation.name"] == "TRANSACTION"

    # Tests for metrics recording in TransactionStrategy

    def test_watch_command_records_metric(self, cluster_transaction_with_otel):
        """
        Test that WATCH command records operation duration metric.
        """
        cluster, operation_duration_mock = cluster_transaction_with_otel

        # Set initial value
        cluster.set("{watch_event}key", "value")

        with cluster.pipeline(transaction=True) as tx:
            tx.watch("{watch_event}key")
            tx.multi()
            tx.set("{watch_event}key", "new_value")
            tx.execute()

        # Find the WATCH event call
        watch_calls = [
            call_obj
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "WATCH"
        ]

        assert len(watch_calls) >= 1
        attrs = watch_calls[0][1]["attributes"]
        assert attrs["db.operation.name"] == "WATCH"
        assert "server.address" in attrs
        assert "server.port" in attrs

    def test_immediate_command_records_metric_with_server_info(
        self, cluster_transaction_with_otel
    ):
        """
        Test that immediate commands (WATCH, GET before MULTI) record metrics
        with server address and port.
        """
        cluster, operation_duration_mock = cluster_transaction_with_otel

        cluster.set("{immediate}key", "value")

        with cluster.pipeline(transaction=True) as tx:
            tx.watch("{immediate}key")
            tx.get("{immediate}key")
            tx.multi()
            tx.set("{immediate}key", "updated")
            tx.execute()

        # Find WATCH and GET events
        watch_calls = [
            call_obj
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "WATCH"
        ]
        get_calls = [
            call_obj
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "GET"
        ]

        # Verify WATCH event has server info
        assert len(watch_calls) >= 1
        watch_attrs = watch_calls[0][1]["attributes"]
        assert "server.address" in watch_attrs
        assert isinstance(watch_attrs["server.address"], str)
        assert "server.port" in watch_attrs
        assert isinstance(watch_attrs["server.port"], int)

        # Verify GET event has server info
        assert len(get_calls) >= 1
        get_attrs = get_calls[0][1]["attributes"]
        assert "server.address" in get_attrs
        assert "server.port" in get_attrs

    def test_transaction_error_records_metric_with_error_type(
        self, cluster_transaction_with_otel
    ):
        """
        Test that when a transaction fails, the recorded metric includes error.type.
        """
        cluster, operation_duration_mock = cluster_transaction_with_otel

        # Set a string value
        cluster.set("{tx_err_type}key", "string_value")

        try:
            with cluster.pipeline(transaction=True) as tx:
                # Try to use LPUSH on a string key - this will fail
                tx.lpush("{tx_err_type}key", "value")
                tx.execute()
        except ResponseError:
            pass

        # Find the TRANSACTION event call - it should have error.type
        transaction_calls = [
            call_obj
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "TRANSACTION"
        ]

        # There should be at least one TRANSACTION event
        assert len(transaction_calls) >= 1

    def test_watch_and_transaction_record_separate_metrics(
        self, cluster_transaction_with_otel
    ):
        """
        Test that WATCH commands and TRANSACTION record separate metrics.
        """
        cluster, operation_duration_mock = cluster_transaction_with_otel

        cluster.set("{separate}key", "0")

        with cluster.pipeline(transaction=True) as tx:
            tx.watch("{separate}key")
            val = tx.get("{separate}key")
            tx.multi()
            tx.set("{separate}key", int(val or 0) + 1)
            tx.execute()

        # Count WATCH events
        watch_count = sum(
            1
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "WATCH"
        )

        # Count TRANSACTION events
        transaction_count = sum(
            1
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "TRANSACTION"
        )

        # Should have at least 1 WATCH and 1 TRANSACTION event
        assert watch_count >= 1
        assert transaction_count >= 1

    def test_immediate_get_command_records_metric(self, cluster_transaction_with_otel):
        """
        Test that GET command executed immediately (after WATCH, before MULTI)
        records operation duration metric.
        """
        cluster, operation_duration_mock = cluster_transaction_with_otel

        cluster.set("{imm_get}key", "test_value")

        with cluster.pipeline(transaction=True) as tx:
            tx.watch("{imm_get}key")
            # This GET is executed immediately because we're watching
            tx.get("{imm_get}key")
            tx.multi()
            tx.set("{imm_get}key", "new_value")
            tx.execute()

        # Find GET events
        get_calls = [
            call_obj
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "GET"
        ]

        assert len(get_calls) >= 1
        attrs = get_calls[0][1]["attributes"]
        assert attrs["db.operation.name"] == "GET"
        assert "server.address" in attrs
        assert "server.port" in attrs
        assert "db.namespace" in attrs
