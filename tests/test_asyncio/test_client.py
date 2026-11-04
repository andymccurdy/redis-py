"""
Unit tests that verify metrics are properly recorded from async Redis client
via record_* function calls.

These tests use fully mocked connection and connection pool - no real Redis
or OTel integration is used.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock

import redis.asyncio as redis
from redis.asyncio.observability import recorder as async_recorder
from redis.observability.attributes import (
    SERVER_ADDRESS,
    SERVER_PORT,
)
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestAsyncRedisClientOperationDurationMetricsRecording:
    """
    Unit tests that verify operation duration metrics are properly recorded
    from async Redis client via record_operation_duration function calls.

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
        conn.should_reconnect.return_value = False

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

    async def test_execute_command_records_operation_duration(
        self, mock_async_connection_pool, mock_async_connection, mock_meter
    ):
        """
        Test that executing a command records operation duration metric
        via the Meter's histogram.record() method.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with patch.object(async_recorder, "_async_metrics_collector", collector):
            client = redis.Redis(connection_pool=mock_async_connection_pool)

            # Mock _send_command_parse_response to return a successful response
            async def mock_send(*args, **kwargs):
                return True

            client._send_command_parse_response = mock_send

            # Execute a command
            await client.execute_command("SET", "key1", "value1")

            # Verify the Meter's histogram.record() was called
            self.operation_duration.record.assert_called_once()

            # Get the call arguments
            call_args = self.operation_duration.record.call_args

            # Verify duration was recorded (first positional arg)
            duration = call_args[0][0]
            assert isinstance(duration, float)
            assert duration >= 0

            # Verify attributes
            attrs = call_args[1]["attributes"]
            assert attrs["db.operation.name"] == "SET"
            assert attrs["server.address"] == "localhost"
            assert attrs["server.port"] == 6379
            assert attrs["db.namespace"] == "0"

        async_recorder.reset_collector()

    async def test_get_command_records_operation_duration(
        self, mock_async_connection_pool, mock_async_connection, mock_meter
    ):
        """
        Test that GET command records operation duration with correct command name.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with patch.object(async_recorder, "_async_metrics_collector", collector):
            client = redis.Redis(connection_pool=mock_async_connection_pool)

            async def mock_send(*args, **kwargs):
                return b"value1"

            client._send_command_parse_response = mock_send

            # Execute GET command
            await client.execute_command("GET", "key1")

            # Verify command name is GET
            call_args = self.operation_duration.record.call_args
            attrs = call_args[1]["attributes"]
            assert attrs["db.operation.name"] == "GET"

        async_recorder.reset_collector()

    async def test_multiple_commands_record_multiple_metrics(
        self, mock_async_connection_pool, mock_async_connection, mock_meter
    ):
        """
        Test that multiple command executions record multiple metrics.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with patch.object(async_recorder, "_async_metrics_collector", collector):
            client = redis.Redis(connection_pool=mock_async_connection_pool)

            async def mock_send(*args, **kwargs):
                return True

            client._send_command_parse_response = mock_send

            # Execute multiple commands
            await client.execute_command("SET", "key1", "value1")
            await client.execute_command("GET", "key1")
            await client.execute_command("DEL", "key1")

            # Verify histogram.record() was called 3 times
            assert self.operation_duration.record.call_count == 3

            # Verify command names
            calls = self.operation_duration.record.call_args_list
            assert calls[0][1]["attributes"]["db.operation.name"] == "SET"
            assert calls[1][1]["attributes"]["db.operation.name"] == "GET"
            assert calls[2][1]["attributes"]["db.operation.name"] == "DEL"

        async_recorder.reset_collector()


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestAsyncRedisClientErrorMetricsRecording:
    """
    Unit tests that verify error metrics are properly recorded from async Redis client
    via record_error_count function calls.

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
        conn.should_reconnect.return_value = False

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
    def mock_error_meter(self):
        """Create a mock Meter that tracks error counter calls."""
        meter = MagicMock()

        # Create mock counter for client errors
        self.client_errors = MagicMock()

        def create_counter_side_effect(name, **kwargs):
            if name == "redis.client.errors":
                return self.client_errors
            return MagicMock()

        meter.create_counter.side_effect = create_counter_side_effect
        meter.create_up_down_counter.return_value = MagicMock()
        meter.create_histogram.return_value = MagicMock()
        meter.create_observable_gauge.return_value = MagicMock()

        return meter

    async def test_execute_command_error_records_error_count(
        self, mock_async_connection_pool, mock_async_connection, mock_error_meter
    ):
        """
        Test that when a command execution raises an exception,
        error count is recorded via record_error_count.
        """
        async_recorder.reset_collector()
        # Enable RESILIENCY metric group for error counting
        config = OTelConfig(metric_groups=[MetricGroup.RESILIENCY])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_error_meter, config)

        with patch.object(async_recorder, "_async_metrics_collector", collector):
            client = redis.Redis(connection_pool=mock_async_connection_pool)

            # Make command raise an exception
            test_error = redis.ResponseError("WRONGTYPE Operation error")

            async def raise_error(*args, **kwargs):
                raise test_error

            client._send_command_parse_response = raise_error

            # Execute should raise the error
            with pytest.raises(redis.ResponseError):
                await client.execute_command("LPUSH", "string_key", "value")

            # Verify record_error_count was called (via client_errors counter)
            self.client_errors.add.assert_called_once()

            # Verify error type is recorded in attributes
            call_args = self.client_errors.add.call_args
            attrs = call_args[1]["attributes"]
            assert "error.type" in attrs
            assert attrs["error.type"] == "ResponseError"

        async_recorder.reset_collector()

    async def test_connection_error_records_error_count(
        self, mock_async_connection_pool, mock_async_connection, mock_error_meter
    ):
        """
        Test that ConnectionError is recorded via record_error_count.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.RESILIENCY])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_error_meter, config)

        with patch.object(async_recorder, "_async_metrics_collector", collector):
            client = redis.Redis(connection_pool=mock_async_connection_pool)

            # Make command raise a ConnectionError
            async def raise_connection_error(*args, **kwargs):
                raise redis.ConnectionError("Connection refused")

            client._send_command_parse_response = raise_connection_error

            # Execute should raise the error
            with pytest.raises(redis.ConnectionError):
                await client.execute_command("GET", "key")

            # Verify record_error_count was called
            self.client_errors.add.assert_called_once()

            # Verify error type is ConnectionError
            call_args = self.client_errors.add.call_args
            attrs = call_args[1]["attributes"]
            assert attrs["error.type"] == "ConnectionError"

        async_recorder.reset_collector()

    async def test_timeout_error_records_error_count(
        self, mock_async_connection_pool, mock_async_connection, mock_error_meter
    ):
        """
        Test that TimeoutError is recorded via record_error_count.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.RESILIENCY])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_error_meter, config)

        with patch.object(async_recorder, "_async_metrics_collector", collector):
            client = redis.Redis(connection_pool=mock_async_connection_pool)

            # Make command raise a TimeoutError
            async def raise_timeout_error(*args, **kwargs):
                raise redis.TimeoutError("Connection timed out")

            client._send_command_parse_response = raise_timeout_error

            # Execute should raise the error
            with pytest.raises(redis.TimeoutError):
                await client.execute_command("GET", "key")

            # Verify record_error_count was called
            self.client_errors.add.assert_called_once()

            # Verify error type is TimeoutError
            call_args = self.client_errors.add.call_args
            attrs = call_args[1]["attributes"]
            assert attrs["error.type"] == "TimeoutError"

        async_recorder.reset_collector()

    async def test_error_count_includes_server_attributes(
        self, mock_async_connection_pool, mock_async_connection, mock_error_meter
    ):
        """
        Test that error count includes server address and port attributes.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.RESILIENCY])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_error_meter, config)

        with patch.object(async_recorder, "_async_metrics_collector", collector):
            client = redis.Redis(connection_pool=mock_async_connection_pool)

            async def raise_error(*args, **kwargs):
                raise redis.ResponseError("Error")

            client._send_command_parse_response = raise_error

            with pytest.raises(redis.ResponseError):
                await client.execute_command("SET", "key", "value")

            # Verify server attributes are recorded
            call_args = self.client_errors.add.call_args
            attrs = call_args[1]["attributes"]
            assert attrs[SERVER_ADDRESS] == "localhost"
            assert attrs[SERVER_PORT] == 6379

        async_recorder.reset_collector()
