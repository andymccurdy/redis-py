from unittest import mock

import pytest

import redis
from redis.event import (
    EventDispatcher,
)
from redis.observability import recorder
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector


@pytest.mark.fixed_client
class TestRedisClientMetricsRecording:
    """
    Unit tests that verify metrics are properly recorded from Redis client
    via direct record_* function calls.

    These tests use fully mocked connection and connection pool - no real Redis
    or OTel integration is used.
    """

    @pytest.fixture
    def mock_connection(self):
        """Create a mock connection with required attributes."""
        conn = mock.MagicMock()
        conn.host = "localhost"
        conn.port = 6379
        conn.db = 0
        conn.should_reconnect.return_value = False

        # Mock retry to just execute the function directly
        def mock_call_with_retry(do, fail, is_retryable=None, with_failure_count=False):
            return do()

        conn.retry.call_with_retry = mock_call_with_retry

        return conn

    @pytest.fixture
    def mock_connection_pool(self, mock_connection):
        """Create a mock connection pool."""
        pool = mock.MagicMock()
        pool.get_connection.return_value = mock_connection
        pool.get_encoder.return_value = mock.MagicMock()
        return pool

    @pytest.fixture
    def mock_meter(self):
        """Create a mock Meter that tracks all instrument calls."""
        meter = mock.MagicMock()

        # Create mock histogram for operation duration
        self.operation_duration = mock.MagicMock()
        # Create mock counter for client errors
        self.client_errors = mock.MagicMock()

        def create_histogram_side_effect(name, **kwargs):
            if name == "db.client.operation.duration":
                return self.operation_duration
            return mock.MagicMock()

        def create_counter_side_effect(name, **kwargs):
            if name == "redis.client.errors":
                return self.client_errors
            return mock.MagicMock()

        meter.create_counter.side_effect = create_counter_side_effect
        meter.create_up_down_counter.return_value = mock.MagicMock()
        meter.create_histogram.side_effect = create_histogram_side_effect

        return meter

    @pytest.fixture
    def setup_redis_client_with_otel(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Setup a Redis client with mocked connection and OTel collector.
        Returns tuple of (redis_client, operation_duration_mock).
        """

        # Reset any existing collector state
        recorder.reset_collector()

        # Create config with COMMAND group enabled
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        # Create collector with mocked meter
        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Patch the recorder to use our collector
        with mock.patch.object(recorder, "_metrics_collector", collector):
            # Create Redis client with mocked connection pool
            client = redis.Redis(
                connection_pool=mock_connection_pool,
            )

            yield client, self.operation_duration

        # Cleanup
        recorder.reset_collector()

    def test_execute_command_records_metrics(self, setup_redis_client_with_otel):
        """
        Test that executing a command records metrics
        via the Meter's histogram.record() method.
        """
        client, operation_duration_mock = setup_redis_client_with_otel

        # Mock _send_command_parse_response to return a successful response
        client._send_command_parse_response = mock.MagicMock(return_value=True)

        # Execute a command
        client.execute_command("SET", "key1", "value1")

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
        assert attrs["db.operation.name"] == "SET"
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 6379
        assert attrs["db.namespace"] == "0"

    def test_get_command_records_metrics(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that GET command records metrics with correct command name.
        """

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, "_metrics_collector", collector):
            client = redis.Redis(
                connection_pool=mock_connection_pool,
            )

            client._send_command_parse_response = mock.MagicMock(return_value=b"value1")

            # Execute GET command
            client.execute_command("GET", "key1")

            # Verify command name is GET
            call_args = self.operation_duration.record.call_args
            attrs = call_args[1]["attributes"]
            assert attrs["db.operation.name"] == "GET"

        recorder.reset_collector()

    def test_command_error_records_error_count(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that when a command execution raises an exception,
        error count is recorded via record_error_count.

        Note: record_operation_duration is NOT called for final errors -
        only record_error_count is called. record_operation_duration is
        only called during retries (in _close_connection) and on success.
        """

        recorder.reset_collector()
        # Enable RESILIENCY metric group for error counting
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND, MetricGroup.RESILIENCY])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, "_metrics_collector", collector):
            client = redis.Redis(
                connection_pool=mock_connection_pool,
            )

            # Make command raise an exception
            test_error = redis.ResponseError("WRONGTYPE Operation error")
            client._send_command_parse_response = mock.MagicMock(side_effect=test_error)

            # Execute should raise the error
            with pytest.raises(redis.ResponseError):
                client.execute_command("LPUSH", "string_key", "value")

            # Verify record_error_count was called (via client_errors counter)
            self.client_errors.add.assert_called_once()

            # Verify error type is recorded in attributes
            call_args = self.client_errors.add.call_args
            attrs = call_args[1]["attributes"]
            assert "error.type" in attrs

            # Verify operation_duration was NOT called (no retries, direct failure)
            self.operation_duration.record.assert_not_called()

        recorder.reset_collector()

    def test_server_attributes_recorded_correctly(self, setup_redis_client_with_otel):
        """
        Test that server address, port, and db namespace are correctly recorded.
        """
        client, operation_duration_mock = setup_redis_client_with_otel

        client._send_command_parse_response = mock.MagicMock(return_value=b"PONG")

        client.execute_command("PING")

        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]["attributes"]

        # Verify server attributes match mock connection
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 6379
        assert attrs["db.namespace"] == "0"

    def test_multiple_commands_record_multiple_metrics(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that each command execution records a separate metric to the Meter.
        """

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, "_metrics_collector", collector):
            client = redis.Redis(
                connection_pool=mock_connection_pool,
            )

            client._send_command_parse_response = mock.MagicMock(return_value=True)

            # Execute multiple commands
            client.execute_command("SET", "key1", "value1")
            client.execute_command("SET", "key2", "value2")
            client.execute_command("GET", "key1")

            # Verify histogram.record() was called three times
            assert self.operation_duration.record.call_count == 3

            # Verify command names in order
            calls = self.operation_duration.record.call_args_list
            assert calls[0][1]["attributes"]["db.operation.name"] == "SET"
            assert calls[1][1]["attributes"]["db.operation.name"] == "SET"
            assert calls[2][1]["attributes"]["db.operation.name"] == "GET"

        recorder.reset_collector()

    def test_different_db_namespace_recorded(self, mock_connection_pool, mock_meter):
        """
        Test that different db namespace values are correctly recorded.
        """

        # Create connection with different db
        mock_connection = mock.MagicMock()
        mock_connection.host = "redis.example.com"
        mock_connection.port = 6380
        mock_connection.db = 5
        mock_connection.should_reconnect.return_value = False

        def mock_call_with_retry(do, fail, is_retryable=None, with_failure_count=False):
            return do()

        mock_connection.retry.call_with_retry = mock_call_with_retry

        mock_connection_pool.get_connection.return_value = mock_connection

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, "_metrics_collector", collector):
            client = redis.Redis(
                connection_pool=mock_connection_pool,
            )

            client._send_command_parse_response = mock.MagicMock(return_value=True)

            client.execute_command("SET", "key", "value")

            call_args = self.operation_duration.record.call_args
            attrs = call_args[1]["attributes"]

            # Verify different server attributes
            assert attrs["server.address"] == "redis.example.com"
            assert attrs["server.port"] == 6380
            assert attrs["db.namespace"] == "5"

        recorder.reset_collector()

    def test_duration_is_positive(self, setup_redis_client_with_otel):
        """
        Test that the recorded duration is a positive float value.
        """
        client, operation_duration_mock = setup_redis_client_with_otel

        client._send_command_parse_response = mock.MagicMock(return_value=True)

        client.execute_command("SET", "key", "value")

        call_args = operation_duration_mock.record.call_args
        duration = call_args[0][0]

        assert isinstance(duration, float)
        assert duration >= 0

    def test_no_batch_size_for_single_command(self, setup_redis_client_with_otel):
        """
        Test that single commands do not include batch_size attribute
        (batch_size is only for pipeline operations).
        """
        client, operation_duration_mock = setup_redis_client_with_otel

        client._send_command_parse_response = mock.MagicMock(return_value=True)

        client.execute_command("SET", "key", "value")

        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]["attributes"]

        # batch_size should not be present for single commands
        assert "db.operation.batch_size" not in attrs

    def test_retry_records_metrics_on_each_attempt(
        self, mock_connection_pool, mock_meter
    ):
        """
        Test that when a command is retried, metrics are recorded
        for each retry attempt with retry_attempts attribute.
        """
        # Create connection with retry behavior
        mock_connection = mock.MagicMock()
        mock_connection.host = "localhost"
        mock_connection.port = 6379
        mock_connection.db = 0
        mock_connection.should_reconnect.return_value = False

        # Track retry attempts
        attempt_count = [0]
        max_retries = 2

        def call_with_retry_impl(do, fail, is_retryable=None, with_failure_count=False):
            """Simulate retry behavior - fail twice, then succeed."""
            for attempt in range(max_retries + 1):
                try:
                    return do()
                except redis.ConnectionError as e:
                    attempt_count[0] += 1
                    if attempt < max_retries:
                        if with_failure_count:
                            fail(e, attempt + 1)
                        else:
                            fail(e)
                    else:
                        raise

        mock_connection.retry.call_with_retry = call_with_retry_impl
        mock_connection.retry.get_retries.return_value = max_retries

        mock_connection_pool.get_connection.return_value = mock_connection

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, "_metrics_collector", collector):
            event_dispatcher = EventDispatcher()

            client = redis.Redis(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )

            # Make command fail twice then succeed
            call_count = [0]

            def send_command_impl(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] <= 2:
                    raise redis.ConnectionError("Connection failed")
                return True

            client._send_command_parse_response = mock.MagicMock(
                side_effect=send_command_impl
            )

            # Execute command - should retry twice then succeed
            client.execute_command("SET", "key", "value")

            # Verify histogram.record() was called 3 times:
            # 2 retry attempts + 1 final success
            assert self.operation_duration.record.call_count == 3

            calls = self.operation_duration.record.call_args_list

            # First two calls should have error.type (retry attempts)
            assert "error.type" in calls[0][1]["attributes"]
            assert "error.type" in calls[1][1]["attributes"]

            # Last call should be success (no error.type)
            assert "error.type" not in calls[2][1]["attributes"]

        recorder.reset_collector()

    def test_retry_exhausted_records_final_error_metrics(
        self, mock_connection_pool, mock_meter
    ):
        """
        Test that when all retries are exhausted, metrics are recorded correctly:
        - record_operation_duration is called for each retry attempt (with error info)
        - record_error_count is called for the final error (after all retries exhausted)
        """
        mock_connection = mock.MagicMock()
        mock_connection.host = "localhost"
        mock_connection.port = 6379
        mock_connection.db = 0
        mock_connection.should_reconnect.return_value = False

        max_retries = 2

        def call_with_retry_impl(do, fail, is_retryable=None, with_failure_count=False):
            """Simulate retry behavior - always fail."""
            for attempt in range(max_retries + 1):
                try:
                    return do()
                except redis.ConnectionError as e:
                    if attempt < max_retries:
                        if with_failure_count:
                            fail(e, attempt + 1)
                        else:
                            fail(e)
                    else:
                        raise

        mock_connection.retry.call_with_retry = call_with_retry_impl
        mock_connection.retry.get_retries.return_value = max_retries

        mock_connection_pool.get_connection.return_value = mock_connection

        recorder.reset_collector()
        # Enable both COMMAND and RESILIENCY metric groups
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND, MetricGroup.RESILIENCY])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, "_metrics_collector", collector):
            event_dispatcher = EventDispatcher()

            client = redis.Redis(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )

            # Make command always fail
            client._send_command_parse_response = mock.MagicMock(
                side_effect=redis.ConnectionError("Connection failed")
            )

            # Execute command - should fail after all retries
            with pytest.raises(redis.ConnectionError):
                client.execute_command("SET", "key", "value")

            # Verify histogram.record() was called 2 times (for retry attempts only)
            # Final error does NOT call record_operation_duration
            assert self.operation_duration.record.call_count == 2

            calls = self.operation_duration.record.call_args_list

            # Both retry calls should have error.type
            for call in calls:
                assert "error.type" in call[1]["attributes"]
                assert call[1]["attributes"]["db.operation.name"] == "SET"

            # Verify record_error_count was called once for the final error
            self.client_errors.add.assert_called_once()
            error_call_args = self.client_errors.add.call_args
            error_attrs = error_call_args[1]["attributes"]
            assert "error.type" in error_attrs

        recorder.reset_collector()
