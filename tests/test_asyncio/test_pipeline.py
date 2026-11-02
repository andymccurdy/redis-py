import asyncio
from unittest import mock

import pytest
from redis import RedisClusterException
import redis
from tests.conftest import skip_if_server_version_lt

from .compat import aclosing
from .conftest import wait_for_command

from unittest.mock import MagicMock, patch, AsyncMock
from redis.asyncio.client import Pipeline
from redis.asyncio.observability import recorder as async_recorder
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector


class TestPipeline:
    async def test_pipeline_is_true(self, r):
        """Ensure pipeline instances are not false-y"""
        async with r.pipeline() as pipe:
            assert pipe

    async def test_pipeline(self, r):
        async with r.pipeline() as pipe:
            (
                pipe.set("a", "a1")
                .get("a")
                .zadd("z", {"z1": 1})
                .zadd("z", {"z2": 4})
                .zincrby("z", 1, "z1")
            )
            assert await pipe.execute() == [
                True,
                b"a1",
                True,
                True,
                2.0,
            ]

    async def test_pipeline_memoryview(self, r):
        async with r.pipeline() as pipe:
            (pipe.set("a", memoryview(b"a1")).get("a"))
            assert await pipe.execute() == [True, b"a1"]

    async def test_pipeline_length(self, r):
        async with r.pipeline() as pipe:
            # Initially empty.
            assert len(pipe) == 0

            # Fill 'er up!
            pipe.set("a", "a1").set("b", "b1").set("c", "c1")
            assert len(pipe) == 3

            # Execute calls reset(), so empty once again.
            await pipe.execute()
            assert len(pipe) == 0

    async def test_pipeline_no_transaction(self, r):
        async with r.pipeline(transaction=False) as pipe:
            pipe.set("a", "a1").set("b", "b1").set("c", "c1")
            assert await pipe.execute() == [True, True, True]
            assert await r.get("a") == b"a1"
            assert await r.get("b") == b"b1"
            assert await r.get("c") == b"c1"

    @pytest.mark.onlynoncluster
    async def test_info_in_transaction(self, decoded_r):
        async with decoded_r.pipeline() as pipe:
            pipe.info("server")
            (info,) = await pipe.execute()
            assert "redis_version" in info

    @pytest.mark.onlynoncluster
    async def test_info_in_transaction_with_custom_callback(self, decoded_r):
        decoded_r.set_response_callback("INFO", lambda response, **options: response)
        async with decoded_r.pipeline() as pipe:
            pipe.info("server")
            (info,) = await pipe.execute()
            assert "redis_version:" in info

    @pytest.mark.onlynoncluster
    async def test_pipeline_no_transaction_watch(self, r):
        await r.set("a", 0)

        async with r.pipeline(transaction=False) as pipe:
            await pipe.watch("a")
            a = await pipe.get("a")

            pipe.multi()
            pipe.set("a", int(a) + 1)
            assert await pipe.execute() == [True]

    @pytest.mark.onlynoncluster
    async def test_pipeline_no_transaction_watch_failure(self, r):
        await r.set("a", 0)

        async with r.pipeline(transaction=False) as pipe:
            await pipe.watch("a")
            a = await pipe.get("a")

            await r.set("a", "bad")

            pipe.multi()
            pipe.set("a", int(a) + 1)

            with pytest.raises(redis.WatchError):
                await pipe.execute()

            assert await r.get("a") == b"bad"

    @pytest.mark.onlynoncluster
    async def test_pipeline_reset_releases_connection_when_cancelled(self, r):
        # Regression test for https://github.com/redis/redis-py/issues/4121:
        # cancelling reset() while it awaits the UNWATCH reply must still
        # return the pooled connection to the pool.
        await r.set("a", 0)

        pool = r.connection_pool
        pipe = r.pipeline()
        await pipe.watch("a")
        assert pipe.connection is not None
        assert pipe.connection in pool._in_use_connections
        leased = pipe.connection

        # Block read_response on the UNWATCH reply so we can cancel mid-call.
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

        assert pipe.connection is None
        assert leased not in pool._in_use_connections

    async def test_exec_error_in_response(self, r):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        await r.set("c", "a")
        async with r.pipeline() as pipe:
            pipe.set("a", 1).set("b", 2).lpush("c", 3).set("d", 4)
            result = await pipe.execute(raise_on_error=False)

            assert result[0]
            assert await r.get("a") == b"1"
            assert result[1]
            assert await r.get("b") == b"2"

            # we can't lpush to a key that's a string value, so this should
            # be a ResponseError exception
            assert isinstance(result[2], redis.ResponseError)
            assert await r.get("c") == b"a"

            # since this isn't a transaction, the other commands after the
            # error are still executed
            assert result[3]
            assert await r.get("d") == b"4"

            # make sure the pipe was restored to a working state
            assert await pipe.set("z", "zzz").execute() == [True]
            assert await r.get("z") == b"zzz"

    async def test_exec_error_raised(self, r):
        await r.set("c", "a")
        async with r.pipeline() as pipe:
            pipe.set("a", 1).set("b", 2).lpush("c", 3).set("d", 4)
            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()
            assert str(ex.value) == (
                "Command # 3 (LPUSH c 3) of pipeline caused error: "
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )

            # make sure the pipe was restored to a working state
            assert await pipe.set("z", "zzz").execute() == [True]
            assert await r.get("z") == b"zzz"

    @pytest.mark.onlynoncluster
    async def test_transaction_with_empty_error_command(self, r):
        """
        Commands with custom EMPTY_ERROR functionality return their default
        values in the pipeline no matter the raise_on_error preference
        """
        for error_switch in (True, False):
            async with r.pipeline() as pipe:
                pipe.set("a", 1).mget([]).set("c", 3)
                result = await pipe.execute(raise_on_error=error_switch)

                assert result[0]
                assert result[1] == []
                assert result[2]

    @pytest.mark.onlynoncluster
    async def test_pipeline_with_empty_error_command(self, r):
        """
        Commands with custom EMPTY_ERROR functionality return their default
        values in the pipeline no matter the raise_on_error preference
        """
        for error_switch in (True, False):
            async with r.pipeline(transaction=False) as pipe:
                pipe.set("a", 1).mget([]).set("c", 3)
                result = await pipe.execute(raise_on_error=error_switch)

                assert result[0]
                assert result[1] == []
                assert result[2]

    async def test_parse_error_raised(self, r):
        async with r.pipeline() as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set("a", 1).zrem("b").set("b", 2)
            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()

            assert str(ex.value).startswith(
                "Command # 2 (ZREM b) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert await pipe.set("z", "zzz").execute() == [True]
            assert await r.get("z") == b"zzz"

    @pytest.mark.onlynoncluster
    async def test_callback_error_does_not_leave_replies_on_connection(self, r):
        # The score callback raises on the first reply, while the BLPOP reply is
        # still in flight. The next command on the pooled connection must not
        # read it.
        await r.zadd("z", {"a": float("inf")})
        await r.set("b", "b1")
        async with r.pipeline(transaction=False) as pipe:
            pipe.zrange("z", 0, -1, withscores=True, score_cast_func=int)
            pipe.blpop("q", 0.5)
            with pytest.raises(OverflowError):
                await pipe.execute()

        client = redis.asyncio.Redis(connection_pool=r.connection_pool)
        assert await client.get("b") == b"b1"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.0.0")
    async def test_exec_error_in_pipeline_redacts_credentials(self, r):
        try:
            async with r.pipeline(transaction=False) as pipe:
                pipe.acl_setuser(
                    "redis-py-user",
                    enabled=True,
                    passwords=["+pipeline-secret"],
                    commands=["+nosuchcommand"],
                )

                with pytest.raises(redis.ResponseError) as ex:
                    await pipe.execute()
        finally:
            await r.acl_deluser("redis-py-user")

        assert "pipeline-secret" not in str(ex.value)
        assert str(ex.value).startswith(
            "Command # 1 (ACL SETUSER <REDACTED>) of pipeline caused error: "
        )

    @pytest.mark.onlynoncluster
    async def test_parse_error_raised_transaction(self, r):
        async with r.pipeline() as pipe:
            pipe.multi()
            # the zrem is invalid because we don't pass any keys to it
            pipe.set("a", 1).zrem("b").set("b", 2)
            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()

            assert str(ex.value).startswith(
                "Command # 2 (ZREM b) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert await pipe.set("z", "zzz").execute() == [True]
            assert await r.get("z") == b"zzz"

    @pytest.mark.onlynoncluster
    async def test_watch_succeed(self, r):
        await r.set("a", 1)
        await r.set("b", 2)

        async with r.pipeline() as pipe:
            await pipe.watch("a", "b")
            assert pipe.watching
            a_value = await pipe.get("a")
            b_value = await pipe.get("b")
            assert a_value == b"1"
            assert b_value == b"2"
            pipe.multi()

            pipe.set("c", 3)
            assert await pipe.execute() == [True]
            assert not pipe.watching

    @pytest.mark.onlynoncluster
    async def test_watch_failure(self, r):
        await r.set("a", 1)
        await r.set("b", 2)

        async with r.pipeline() as pipe:
            await pipe.watch("a", "b")
            await r.set("b", 3)
            pipe.multi()
            pipe.get("a")
            with pytest.raises(redis.WatchError):
                await pipe.execute()

            assert not pipe.watching

    @pytest.mark.onlynoncluster
    async def test_watch_failure_in_empty_transaction(self, r):
        await r.set("a", 1)
        await r.set("b", 2)

        async with r.pipeline() as pipe:
            await pipe.watch("a", "b")
            await r.set("b", 3)
            pipe.multi()
            with pytest.raises(redis.WatchError):
                await pipe.execute()

            assert not pipe.watching

    @pytest.mark.onlynoncluster
    async def test_unwatch(self, r):
        await r.set("a", 1)
        await r.set("b", 2)

        async with r.pipeline() as pipe:
            await pipe.watch("a", "b")
            await r.set("b", 3)
            await pipe.unwatch()
            assert not pipe.watching
            pipe.get("a")
            assert await pipe.execute() == [b"1"]

    @pytest.mark.onlynoncluster
    async def test_watch_exec_no_unwatch(self, r):
        await r.set("a", 1)
        await r.set("b", 2)

        async with r.monitor() as m:
            async with r.pipeline() as pipe:
                await pipe.watch("a", "b")
                assert pipe.watching
                a_value = await pipe.get("a")
                b_value = await pipe.get("b")
                assert a_value == b"1"
                assert b_value == b"2"
                pipe.multi()
                pipe.set("c", 3)
                assert await pipe.execute() == [True]
                assert not pipe.watching

            unwatch_command = await wait_for_command(r, m, "UNWATCH")
            assert unwatch_command is None, "should not send UNWATCH"

    @pytest.mark.onlynoncluster
    async def test_watch_reset_unwatch(self, r):
        await r.set("a", 1)

        async with r.monitor() as m:
            async with r.pipeline() as pipe:
                await pipe.watch("a")
                assert pipe.watching
                await pipe.reset()
                assert not pipe.watching

            unwatch_command = await wait_for_command(r, m, "UNWATCH")
            assert unwatch_command is not None
            assert unwatch_command["command"] == "UNWATCH"

    @pytest.mark.onlynoncluster
    async def test_aclose_is_reset(self, r):
        async with r.pipeline() as pipe:
            called = 0

            async def mock_reset():
                nonlocal called
                called += 1

            with mock.patch.object(pipe, "reset", mock_reset):
                await pipe.aclose()
                assert called == 1

    @pytest.mark.onlynoncluster
    async def test_aclosing(self, r):
        async with aclosing(r.pipeline()):
            pass

    @pytest.mark.onlynoncluster
    async def test_transaction_callable(self, r):
        await r.set("a", 1)
        await r.set("b", 2)
        has_run = []

        async def my_transaction(pipe):
            a_value = await pipe.get("a")
            assert a_value in (b"1", b"2")
            b_value = await pipe.get("b")
            assert b_value == b"2"

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"
            if not has_run:
                await r.incr("a")
                has_run.append("it has")

            pipe.multi()
            pipe.set("c", int(a_value) + int(b_value))

        result = await r.transaction(my_transaction, "a", "b")
        assert result == [True]
        assert await r.get("c") == b"4"

    @pytest.mark.onlynoncluster
    async def test_transaction_callable_returns_value_from_callable(self, r):
        async def callback(pipe):
            # No need to do anything here since we only want the return value
            return "a"

        res = await r.transaction(callback, "my-key", value_from_callable=True)
        assert res == "a"

    async def test_exec_error_in_no_transaction_pipeline(self, r):
        await r.set("a", 1)
        async with r.pipeline(transaction=False) as pipe:
            pipe.llen("a")
            pipe.expire("a", 100)

            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()

            assert str(ex.value).startswith(
                "Command # 1 (LLEN a) of pipeline caused error: "
            )

        assert await r.get("a") == b"1"

    async def test_exec_error_in_no_transaction_pipeline_unicode_command(self, r):
        key = chr(3456) + "abcd" + chr(3421)
        await r.set(key, 1)
        async with r.pipeline(transaction=False) as pipe:
            pipe.llen(key)
            pipe.expire(key, 100)

            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()

            expected = f"Command # 1 (LLEN {key}) of pipeline caused error: "
            assert str(ex.value).startswith(expected)

        assert await r.get(key) == b"1"

    async def test_exec_error_in_pipeline_truncated(self, r):
        key = "a" * 50
        a_value = "a" * 20
        b_value = "b" * 20

        await r.set(key, 1)
        async with r.pipeline(transaction=False) as pipe:
            pipe.hset(key, mapping={"field_a": a_value, "field_b": b_value})
            pipe.expire(key, 100)

            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()

            expected = f"Command # 1 (HSET {key} field_a {a_value} field_b...) of pipeline caused error: "
            assert str(ex.value).startswith(expected)

    async def test_pipeline_with_bitfield(self, r):
        async with r.pipeline() as pipe:
            pipe.set("a", "1")
            bf = pipe.bitfield("b")
            pipe2 = (
                bf.set("u8", 8, 255)
                .get("u8", 0)
                .get("u4", 8)  # 1111
                .get("u4", 12)  # 1111
                .get("u4", 13)  # 1110
                .execute()
            )
            pipe.get("a")
            response = await pipe.execute()

            assert pipe == pipe2
            assert response == [True, [0, 0, 15, 15, 14], b"1"]

    @pytest.mark.parametrize("transaction", [True, False])
    async def test_pipeline_never_decode_with_decode_responses(
        self, decoded_r, transaction
    ):
        # https://github.com/redis/redis-py/issues/1884
        await decoded_r.set("{k}str", "value")
        await decoded_r.rpush("{k}list", "a", "b")
        await decoded_r.hset("{k}hash", mapping={"f": "v"})
        async with decoded_r.pipeline(transaction=transaction) as pipe:
            pipe.dump("{k}str")
            pipe.get("{k}str")
            pipe.lrange("{k}list", 0, -1)
            pipe.hmget("{k}hash", ["f", "missing"])
            pipe.lpush("{k}str", "x")
            dumped, value, items, fields, error = await pipe.execute(
                raise_on_error=False
            )

        assert isinstance(dumped, bytes)
        assert value == "value"
        assert items == ["a", "b"]
        assert fields == ["v", None]
        assert isinstance(error, redis.ResponseError)
        assert await decoded_r.restore("{k}copy", 0, dumped)
        assert await decoded_r.get("{k}copy") == "value"

    async def test_pipeline_get(self, r):
        await r.set("a", "a1")
        async with r.pipeline() as pipe:
            pipe.get("a")
            assert await pipe.execute() == [b"a1"]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.0.0")
    async def test_pipeline_discard(self, r):
        # empty pipeline should raise an error
        async with r.pipeline() as pipe:
            pipe.set("key", "someval")
            await pipe.discard()
            with pytest.raises(redis.exceptions.ResponseError):
                await pipe.execute()

        # setting a pipeline and discarding should do the same
        async with r.pipeline() as pipe:
            pipe.set("key", "someval")
            pipe.set("someotherkey", "val")
            response = await pipe.execute()
            pipe.set("key", "another value!")
            await pipe.discard()
            pipe.set("key", "another vae!")
            with pytest.raises(redis.exceptions.ResponseError):
                await pipe.execute()

            pipe.set("foo", "bar")
            response = await pipe.execute()
        assert response[0]
        assert await r.get("foo") == b"bar"

    @pytest.mark.onlynoncluster
    async def test_send_set_commands_over_async_pipeline(self, r: redis.asyncio.Redis):
        pipe = r.pipeline()
        pipe.hset("hash:1", "foo", "bar")
        pipe.hset("hash:1", "bar", "foo")
        pipe.hset("hash:1", "baz", "bar")
        pipe.hgetall("hash:1")
        resp = await pipe.execute()
        assert resp == [1, 1, 1, {b"bar": b"foo", b"baz": b"bar", b"foo": b"bar"}]

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    async def test_pipeline_with_msetex(self, r):
        p = r.pipeline()
        with pytest.raises(RedisClusterException):
            p.msetex({"key1": "value1", "key2": "value2"}, ex=1000)

        p_transaction = r.pipeline(transaction=True)
        with pytest.raises(RedisClusterException):
            p_transaction.msetex(
                {"key1_transaction": "value1", "key2_transaction": "value2"}, ex=10
            )

    async def test_pipeline_json_module_access(self, r):
        """
        Test that pipeline can access the json() module.
        The JSON module requires nodes_manager (for cluster) and set_response_callback
        on the client during initialization.

        """
        pipeline = r.pipeline()

        # This should not raise an AttributeError
        json_pipeline = pipeline.json()

        # Verify the JSON module was created successfully
        assert json_pipeline is not None
        assert json_pipeline.client is pipeline

        # Verify that JSON callbacks were registered
        assert "JSON.SET" in r.response_callbacks
        assert "JSON.GET" in r.response_callbacks


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestAsyncPipelineOperationDurationMetricsRecording:
    """
    Unit tests that verify operation duration metrics are properly recorded
    from async Pipeline via record_operation_duration function calls.

    These tests use fully mocked connection and connection pool - no real Redis
    or OTel integration is used.
    """

    @pytest.fixture
    def mock_async_connection(self):
        """Create a mock async connection with required attributes."""
        conn = MagicMock()
        conn.host = "localhost"
        conn.port = 6379
        conn.db = 0

        # Create a real Retry object that just executes the function directly
        async def mock_call_with_retry(
            do, fail, is_retryable=None, with_failure_count=False
        ):
            return await do()

        conn.retry = MagicMock()
        conn.retry.call_with_retry = mock_call_with_retry
        conn.retry.get_retries.return_value = 0

        return conn

    @pytest.fixture
    def mock_async_connection_pool(self, mock_async_connection):
        """Create a mock async connection pool."""
        pool = MagicMock()
        pool.get_connection = AsyncMock(return_value=mock_async_connection)
        pool.release = AsyncMock()
        pool.get_encoder.return_value = MagicMock()
        pool.get_protocol.return_value = 2
        return pool

    @pytest.fixture
    def mock_meter(self):
        """Create a mock Meter that tracks all instrument calls."""
        meter = MagicMock()

        # Create mock histogram for operation duration
        self.operation_duration = MagicMock()
        # Create mock counter for client errors
        self.client_errors = MagicMock()

        def create_histogram_side_effect(name, **kwargs):
            if name == "db.client.operation.duration":
                return self.operation_duration
            return MagicMock()

        def create_counter_side_effect(name, **kwargs):
            if name == "redis.client.errors":
                return self.client_errors
            return MagicMock()

        meter.create_counter.side_effect = create_counter_side_effect
        meter.create_up_down_counter.return_value = MagicMock()
        meter.create_histogram.side_effect = create_histogram_side_effect
        meter.create_observable_gauge.return_value = MagicMock()

        return meter

    @pytest.fixture
    def setup_pipeline_with_otel(
        self, mock_async_connection_pool, mock_async_connection, mock_meter
    ):
        """
        Setup a Pipeline with mocked connection and OTel collector.
        Returns tuple of (pipeline, operation_duration_mock).
        """

        # Reset any existing collector state
        async_recorder.reset_collector()

        # Create config with COMMAND group enabled
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        # Create collector with mocked meter
        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Patch the recorder to use our collector
        # Note: _get_or_create_collector is now sync
        with patch.object(
            async_recorder,
            "_get_or_create_collector",
            return_value=collector,
        ):
            # Create pipeline with mocked connection pool
            pipeline = Pipeline(
                connection_pool=mock_async_connection_pool,
                response_callbacks={},
                transaction=True,
                shard_hint=None,
            )

            yield pipeline, self.operation_duration

        # Cleanup
        async_recorder.reset_collector()

    async def test_pipeline_execute_records_operation_duration(
        self, setup_pipeline_with_otel
    ):
        """
        Test that executing a pipeline records operation duration metric
        which is delivered to the Meter's histogram.record() method.
        """
        pipeline, operation_duration_mock = setup_pipeline_with_otel

        # Mock _execute_transaction to return successful responses
        pipeline._execute_transaction = AsyncMock(return_value=[True, True, b"value1"])

        # Queue commands in the pipeline
        pipeline.command_stack = [
            (("SET", "key1", "value1"), {}),
            (("SET", "key2", "value2"), {}),
            (("GET", "key1"), {}),
        ]

        # Execute the pipeline
        await pipeline.execute()

        # Verify the Meter's histogram.record() was called
        operation_duration_mock.record.assert_called_once()

        # Get the call arguments
        call_args = operation_duration_mock.record.call_args

        # Verify duration was recorded (first positional arg)
        duration = call_args[0][0]
        assert isinstance(duration, float)
        assert duration >= 0

        # Verify attributes
        attrs = call_args[1]["attributes"]
        assert attrs["db.operation.name"] == "MULTI"
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 6379
        assert attrs["db.namespace"] == "0"

    async def test_pipeline_no_transaction_records_pipeline_command_name(
        self, mock_async_connection_pool, mock_async_connection, mock_meter
    ):
        """
        Test that executing a pipeline without transaction
        records metric with command_name='PIPELINE'.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Note: _get_or_create_collector is now sync
        with patch.object(
            async_recorder,
            "_get_or_create_collector",
            return_value=collector,
        ):
            # Create pipeline with transaction=False
            pipeline = Pipeline(
                connection_pool=mock_async_connection_pool,
                response_callbacks={},
                transaction=False,  # Non-transaction mode
                shard_hint=None,
            )

            pipeline._execute_pipeline = AsyncMock(return_value=[True, True])
            pipeline.command_stack = [
                (("SET", "key1", "value1"), {}),
                (("SET", "key2", "value2"), {}),
            ]

            await pipeline.execute()

            # Verify command name is PIPELINE
            call_args = self.operation_duration.record.call_args
            attrs = call_args[1]["attributes"]
            assert attrs["db.operation.name"] == "PIPELINE"

        async_recorder.reset_collector()

    async def test_pipeline_server_attributes_recorded(self, setup_pipeline_with_otel):
        """
        Test that server address, port, and db namespace are correctly recorded.
        """
        pipeline, operation_duration_mock = setup_pipeline_with_otel

        pipeline._execute_transaction = AsyncMock(return_value=[True])
        pipeline.command_stack = [(("PING",), {})]

        await pipeline.execute()

        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]["attributes"]

        # Verify server attributes match mock connection
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 6379
        assert attrs["db.namespace"] == "0"

    async def test_multiple_pipeline_executions_record_multiple_metrics(
        self, mock_async_connection_pool, mock_async_connection, mock_meter
    ):
        """
        Test that each pipeline execution records a separate metric to the Meter.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Note: _get_or_create_collector is now sync
        with patch.object(
            async_recorder,
            "_get_or_create_collector",
            return_value=collector,
        ):
            # First pipeline execution
            pipeline1 = Pipeline(
                connection_pool=mock_async_connection_pool,
                response_callbacks={},
                transaction=True,
                shard_hint=None,
            )
            pipeline1._execute_transaction = AsyncMock(return_value=[True])
            pipeline1.command_stack = [(("SET", "key1", "value1"), {})]
            await pipeline1.execute()

            # Second pipeline execution
            pipeline2 = Pipeline(
                connection_pool=mock_async_connection_pool,
                response_callbacks={},
                transaction=True,
                shard_hint=None,
            )
            pipeline2._execute_transaction = AsyncMock(return_value=[True, True])
            pipeline2.command_stack = [
                (("SET", "key2", "value2"), {}),
                (("SET", "key3", "value3"), {}),
            ]
            await pipeline2.execute()

            # Verify histogram.record() was called twice
            assert self.operation_duration.record.call_count == 2

        async_recorder.reset_collector()

    async def test_empty_pipeline_does_not_record_metric(
        self, mock_async_connection_pool, mock_async_connection, mock_meter
    ):
        """
        Test that an empty pipeline (no commands) does not record a metric.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Note: _get_or_create_collector is now sync
        with patch.object(
            async_recorder,
            "_get_or_create_collector",
            return_value=collector,
        ):
            pipeline = Pipeline(
                connection_pool=mock_async_connection_pool,
                response_callbacks={},
                transaction=True,
                shard_hint=None,
            )

            # Empty command stack
            pipeline.command_stack = []

            # Execute empty pipeline
            result = await pipeline.execute()

            # Should return empty list
            assert result == []

            # No metric should be recorded for empty pipeline
            self.operation_duration.record.assert_not_called()

        async_recorder.reset_collector()
