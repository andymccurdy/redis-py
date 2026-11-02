"""
Unit tests for redis.observability.recorder module.

These tests verify that recorder functions correctly pass arguments through
to the underlying OTel Meter instruments (Counter, Histogram, UpDownCounter).
The MeterProvider is mocked to verify the actual integration point where
metrics are exported to OTel.
"""

import pytest
from unittest.mock import MagicMock, patch

from opentelemetry.metrics import Observation

from redis._parsers.response_callbacks import get_response_callbacks

from redis.observability import recorder
from redis.observability.registry import (
    ObservablesRegistry,
    get_observables_registry_instance,
)
from redis.observability.attributes import (
    ConnectionState,
    GeoFailoverReason,
    PubSubDirection,
    # Connection pool attributes
    DB_CLIENT_CONNECTION_POOL_NAME,
    DB_CLIENT_CONNECTION_STATE,
    SERVER_ADDRESS,
    SERVER_PORT,
    # Database attributes
    DB_NAMESPACE,
    DB_OPERATION_NAME,
    DB_RESPONSE_STATUS_CODE,
    # Error attributes
    ERROR_TYPE,
    # Network attributes
    NETWORK_PEER_ADDRESS,
    NETWORK_PEER_PORT,
    # Redis-specific attributes
    REDIS_CLIENT_OPERATION_RETRY_ATTEMPTS,
    REDIS_CLIENT_CONNECTION_CLOSE_REASON,
    REDIS_CLIENT_CONNECTION_NOTIFICATION,
    REDIS_CLIENT_PUBSUB_MESSAGE_DIRECTION,
    REDIS_CLIENT_PUBSUB_CHANNEL,
    REDIS_CLIENT_PUBSUB_SHARDED,
    # Streaming attributes
    REDIS_CLIENT_STREAM_NAME,
    REDIS_CLIENT_CONSUMER_GROUP,
    DB_CLIENT_GEOFAILOVER_FAIL_FROM,
    DB_CLIENT_GEOFAILOVER_FAIL_TO,
    DB_CLIENT_GEOFAILOVER_REASON,
)
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.providers import ObservabilityInstance
from redis.observability.metrics import RedisMetricsCollector, CloseReason
from redis.observability.recorder import (
    record_connection_closed,
    record_connection_count,
    record_connection_create_time,
    record_connection_handoff,
    record_connection_relaxed_timeout,
    record_connection_timeout,
    record_connection_wait_time,
    record_error_count,
    record_geo_failover,
    record_operation_duration,
    record_pubsub_message,
    record_streaming_lag,
    reset_collector,
)


class MockInstruments:
    """Container for mock OTel instruments."""

    def __init__(self):
        # Counters
        self.client_errors = MagicMock()
        self.maintenance_notifications = MagicMock()
        self.connection_timeouts = MagicMock()
        self.connection_closed = MagicMock()
        self.connection_handoff = MagicMock()
        self.pubsub_messages = MagicMock()
        self.geo_failovers = MagicMock()

        # Gauges
        self.connection_count = MagicMock()

        # UpDownCounters
        self.connection_relaxed_timeout = MagicMock()

        # Histograms
        self.connection_create_time = MagicMock()
        self.connection_wait_time = MagicMock()
        self.connection_use_time = MagicMock()
        self.operation_duration = MagicMock()
        self.stream_lag = MagicMock()


@pytest.fixture
def mock_instruments():
    """Create mock OTel instruments."""
    return MockInstruments()


@pytest.fixture
def mock_meter(mock_instruments):
    """Create a mock Meter that returns our mock instruments."""
    meter = MagicMock()

    def create_counter_side_effect(name, **kwargs):
        instrument_map = {
            "redis.client.errors": mock_instruments.client_errors,
            "redis.client.maintenance.notifications": mock_instruments.maintenance_notifications,
            "db.client.connection.timeouts": mock_instruments.connection_timeouts,
            "redis.client.connection.closed": mock_instruments.connection_closed,
            "redis.client.connection.handoff": mock_instruments.connection_handoff,
            "redis.client.pubsub.messages": mock_instruments.pubsub_messages,
            "redis.client.geofailover.failovers": mock_instruments.geo_failovers,
        }
        return instrument_map.get(name, MagicMock())

    def create_gauge_side_effect(name, **kwargs):
        instrument_map = {
            "db.client.connection.count": mock_instruments.connection_count,
        }
        return instrument_map.get(name, MagicMock())

    def create_up_down_counter_side_effect(name, **kwargs):
        instrument_map = {
            "redis.client.connection.relaxed_timeout": mock_instruments.connection_relaxed_timeout,
        }
        return instrument_map.get(name, MagicMock())

    def create_histogram_side_effect(name, **kwargs):
        instrument_map = {
            "db.client.connection.create_time": mock_instruments.connection_create_time,
            "db.client.connection.wait_time": mock_instruments.connection_wait_time,
            "db.client.connection.use_time": mock_instruments.connection_use_time,
            "db.client.operation.duration": mock_instruments.operation_duration,
            "redis.client.stream.lag": mock_instruments.stream_lag,
        }
        return instrument_map.get(name, MagicMock())

    meter.create_counter.side_effect = create_counter_side_effect
    meter.create_gauge.side_effect = create_gauge_side_effect
    meter.create_observable_gauge.side_effect = create_gauge_side_effect
    meter.create_up_down_counter.side_effect = create_up_down_counter_side_effect
    meter.create_histogram.side_effect = create_histogram_side_effect

    return meter


@pytest.fixture
def mock_config():
    """Create a config with all metric groups enabled."""
    config = OTelConfig(
        metric_groups=[
            MetricGroup.RESILIENCY,
            MetricGroup.CONNECTION_BASIC,
            MetricGroup.CONNECTION_ADVANCED,
            MetricGroup.COMMAND,
            MetricGroup.PUBSUB,
            MetricGroup.STREAMING,
        ]
    )
    return config


@pytest.fixture
def metrics_collector(mock_meter, mock_config):
    """Create a real RedisMetricsCollector with mocked Meter."""
    with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
        from redis.observability.metrics import RedisMetricsCollector

        collector = RedisMetricsCollector(mock_meter, mock_config)
        return collector


@pytest.fixture
def setup_recorder(metrics_collector, mock_instruments):
    """
    Setup the recorder module with our collector that has mocked instruments.
    """
    from redis.observability import recorder

    # Reset the global collector before test
    recorder.reset_collector()

    # Install our collector with mocked instruments directly: the recording helpers read this variable.
    with patch.object(recorder, "_metrics_collector", metrics_collector):
        yield mock_instruments

    # Reset after test
    recorder.reset_collector()


@pytest.mark.fixed_client
class TestRecordOperationDuration:
    """Tests for record_operation_duration - verifies Histogram.record() calls."""

    def test_record_operation_duration_success(self, setup_recorder):
        """Test that operation duration is recorded to the histogram with correct attributes."""

        instruments = setup_recorder

        record_operation_duration(
            command_name="SET",
            duration_seconds=0.005,
            server_address="localhost",
            server_port=6379,
            db_namespace="0",
            error=None,
        )

        # Verify histogram.record() was called
        instruments.operation_duration.record.assert_called_once()
        call_args = instruments.operation_duration.record.call_args

        # Verify duration value
        assert call_args[0][0] == 0.005

        # Verify attributes
        attrs = call_args[1]["attributes"]
        assert attrs[SERVER_ADDRESS] == "localhost"
        assert attrs[SERVER_PORT] == 6379
        assert attrs[DB_NAMESPACE] == "0"
        assert attrs[DB_OPERATION_NAME] == "SET"

    def test_record_operation_duration_with_error(self, setup_recorder):
        """Test that error information is included in attributes."""

        instruments = setup_recorder

        error = ConnectionError("Connection refused")
        record_operation_duration(
            command_name="GET",
            duration_seconds=0.001,
            server_address="localhost",
            server_port=6379,
            error=error,
        )

        instruments.operation_duration.record.assert_called_once()
        call_args = instruments.operation_duration.record.call_args

        attrs = call_args[1]["attributes"]
        assert attrs[DB_OPERATION_NAME] == "GET"
        assert attrs[DB_RESPONSE_STATUS_CODE] == "error"
        assert attrs[ERROR_TYPE] == "ConnectionError"

    def test_record_operation_duration_with_bytes_command_name(self, setup_recorder):
        """Test that bytes command names are converted to strings for OTel exporters."""

        instruments = setup_recorder

        # Pass command_name as bytes (as it can come from args[0])
        record_operation_duration(
            command_name=b"SET",
            duration_seconds=0.005,
            server_address="localhost",
            server_port=6379,
            db_namespace="0",
            error=None,
        )

        instruments.operation_duration.record.assert_called_once()
        call_args = instruments.operation_duration.record.call_args

        attrs = call_args[1]["attributes"]
        # Verify command_name is converted to uppercase string, not bytes
        assert attrs[DB_OPERATION_NAME] == "SET"
        assert isinstance(attrs[DB_OPERATION_NAME], str)


@pytest.mark.fixed_client
class TestRecordConnectionCreateTime:
    """Tests for record_connection_create_time - verifies Histogram.record() calls."""

    def test_record_connection_create_time(self, setup_recorder):
        """Test that connection creation time is recorded with pool name."""
        from unittest.mock import MagicMock

        instruments = setup_recorder

        # Create a mock connection pool
        mock_pool = MagicMock()
        mock_pool.__class__.__name__ = "ConnectionPool"
        mock_pool.connection_kwargs = {"host": "localhost", "port": 6379, "db": 0}
        mock_pool._pool_id = "a1b2c3d4"  # Mock the unique pool ID

        record_connection_create_time(
            connection_pool=mock_pool,
            duration_seconds=0.025,
        )

        instruments.connection_create_time.record.assert_called_once()
        call_args = instruments.connection_create_time.record.call_args

        # Verify duration value
        assert call_args[0][0] == 0.025

        # Verify attributes
        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "localhost:6379_a1b2c3d4"


@pytest.mark.fixed_client
class TestRecordConnectionTimeout:
    """Tests for record_connection_timeout - verifies Counter.add() calls."""

    def test_record_connection_timeout(self, setup_recorder):
        """Test recording connection timeout event."""

        instruments = setup_recorder

        record_connection_timeout(
            pool_name="ConnectionPool<localhost:6379>",
        )

        instruments.connection_timeouts.add.assert_called_once()
        call_args = instruments.connection_timeouts.add.call_args

        # Counter increments by 1
        assert call_args[0][0] == 1

        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "ConnectionPool<localhost:6379>"


@pytest.mark.fixed_client
class TestRecordConnectionWaitTime:
    """Tests for record_connection_wait_time - verifies Histogram.record() calls."""

    def test_record_connection_wait_time(self, setup_recorder):
        """Test recording connection wait time."""

        instruments = setup_recorder

        record_connection_wait_time(
            pool_name="ConnectionPool<localhost:6379>",
            duration_seconds=0.010,
        )

        instruments.connection_wait_time.record.assert_called_once()
        call_args = instruments.connection_wait_time.record.call_args

        assert call_args[0][0] == 0.010
        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "ConnectionPool<localhost:6379>"


@pytest.mark.fixed_client
class TestRecordConnectionClosed:
    """Tests for record_connection_closed - verifies Counter.add() calls."""

    def test_record_connection_closed_with_reason(self, setup_recorder):
        """Test recording connection closed with reason."""

        instruments = setup_recorder

        record_connection_closed(
            close_reason=CloseReason.HEALTHCHECK_FAILED,
        )

        instruments.connection_closed.add.assert_called_once()
        call_args = instruments.connection_closed.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]["attributes"]
        assert (
            attrs[REDIS_CLIENT_CONNECTION_CLOSE_REASON]
            == CloseReason.HEALTHCHECK_FAILED.value
        )

    def test_record_connection_closed_with_error(self, setup_recorder):
        """Test recording connection closed with error type."""

        instruments = setup_recorder

        error = ConnectionResetError("Connection reset by peer")
        record_connection_closed(
            close_reason=CloseReason.ERROR,
            error_type=error,
        )

        instruments.connection_closed.add.assert_called_once()
        attrs = instruments.connection_closed.add.call_args[1]["attributes"]
        assert attrs[REDIS_CLIENT_CONNECTION_CLOSE_REASON] == "error"
        assert attrs[ERROR_TYPE] == "ConnectionResetError"


@pytest.mark.fixed_client
class TestRecordConnectionRelaxedTimeout:
    """Tests for record_connection_relaxed_timeout - verifies UpDownCounter.add() calls."""

    def test_record_connection_relaxed_timeout_relaxed(self, setup_recorder):
        """Test recording relaxed timeout increments counter by 1."""

        instruments = setup_recorder

        record_connection_relaxed_timeout(
            connection_name="localhost:6379_abc123",
            maint_notification="MOVING",
            relaxed=True,
        )

        instruments.connection_relaxed_timeout.add.assert_called_once()
        call_args = instruments.connection_relaxed_timeout.add.call_args

        # relaxed=True means count up (+1)
        assert call_args[0][0] == 1
        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "localhost:6379_abc123"
        assert attrs[REDIS_CLIENT_CONNECTION_NOTIFICATION] == "MOVING"

    def test_record_connection_relaxed_timeout_unrelaxed(self, setup_recorder):
        """Test recording unrelaxed timeout decrements counter by 1."""

        instruments = setup_recorder

        record_connection_relaxed_timeout(
            connection_name="localhost:6379_abc123",
            maint_notification="MIGRATING",
            relaxed=False,
        )

        instruments.connection_relaxed_timeout.add.assert_called_once()
        call_args = instruments.connection_relaxed_timeout.add.call_args

        # relaxed=False means count down (-1)
        assert call_args[0][0] == -1
        attrs = call_args[1]["attributes"]
        assert attrs[REDIS_CLIENT_CONNECTION_NOTIFICATION] == "MIGRATING"


@pytest.mark.fixed_client
class TestRecordConnectionHandoff:
    """Tests for record_connection_handoff - verifies Counter.add() calls."""

    def test_record_connection_handoff(self, setup_recorder):
        """Test recording connection handoff event."""

        instruments = setup_recorder

        record_connection_handoff(
            pool_name="ConnectionPool<localhost:6379>",
        )

        instruments.connection_handoff.add.assert_called_once()
        call_args = instruments.connection_handoff.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "ConnectionPool<localhost:6379>"


@pytest.mark.fixed_client
class TestRecordErrorCount:
    """Tests for record_error_count - verifies Counter.add() calls."""

    def test_record_error_count(self, setup_recorder):
        """Test recording error count with all attributes."""

        instruments = setup_recorder

        error = ConnectionError("Connection refused")
        record_error_count(
            server_address="localhost",
            server_port=6379,
            network_peer_address="127.0.0.1",
            network_peer_port=6379,
            error_type=error,
            retry_attempts=3,
            is_internal=True,
        )

        instruments.client_errors.add.assert_called_once()
        call_args = instruments.client_errors.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]["attributes"]
        assert attrs[SERVER_ADDRESS] == "localhost"
        assert attrs[SERVER_PORT] == 6379
        assert attrs[NETWORK_PEER_ADDRESS] == "127.0.0.1"
        assert attrs[NETWORK_PEER_PORT] == 6379
        assert attrs[ERROR_TYPE] == "ConnectionError"
        assert attrs[REDIS_CLIENT_OPERATION_RETRY_ATTEMPTS] == 3

    def test_record_error_count_with_is_internal_false(self, setup_recorder):
        """Test recording error count with is_internal=False."""

        instruments = setup_recorder

        error = TimeoutError("Connection timed out")
        record_error_count(
            server_address="localhost",
            server_port=6379,
            network_peer_address="127.0.0.1",
            network_peer_port=6379,
            error_type=error,
            retry_attempts=2,
            is_internal=False,
        )

        instruments.client_errors.add.assert_called_once()
        call_args = instruments.client_errors.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]["attributes"]
        assert attrs[ERROR_TYPE] == "TimeoutError"
        assert attrs[REDIS_CLIENT_OPERATION_RETRY_ATTEMPTS] == 2


@pytest.mark.fixed_client
class TestRecordMaintNotificationCount:
    """Tests for record_maint_notification_count - verifies Counter.add() calls."""

    def test_record_maint_notification_count(self, setup_recorder):
        """Test recording maintenance notification count with all attributes."""

        instruments = setup_recorder

        recorder.record_maint_notification_count(
            server_address="localhost",
            server_port=6379,
            network_peer_address="127.0.0.1",
            network_peer_port=6379,
            maint_notification="MOVING",
        )

        instruments.maintenance_notifications.add.assert_called_once()
        call_args = instruments.maintenance_notifications.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]["attributes"]
        assert attrs[SERVER_ADDRESS] == "localhost"
        assert attrs[SERVER_PORT] == 6379
        assert attrs[NETWORK_PEER_ADDRESS] == "127.0.0.1"
        assert attrs[NETWORK_PEER_PORT] == 6379
        assert attrs[REDIS_CLIENT_CONNECTION_NOTIFICATION] == "MOVING"

    def test_record_maint_notification_count_migrating(self, setup_recorder):
        """Test recording maintenance notification count with MIGRATING type."""

        instruments = setup_recorder

        recorder.record_maint_notification_count(
            server_address="redis-primary",
            server_port=6380,
            network_peer_address="10.0.0.1",
            network_peer_port=6380,
            maint_notification="MIGRATING",
        )

        instruments.maintenance_notifications.add.assert_called_once()
        call_args = instruments.maintenance_notifications.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]["attributes"]
        assert attrs[SERVER_ADDRESS] == "redis-primary"
        assert attrs[SERVER_PORT] == 6380
        assert attrs[REDIS_CLIENT_CONNECTION_NOTIFICATION] == "MIGRATING"


@pytest.mark.fixed_client
class TestRecordGeoFailover:
    """Tests for record_geo_failover - verifies Counter.add() calls."""

    @pytest.fixture
    def mock_database(self):
        """Create a mock database with required attributes."""
        mock_db = MagicMock()
        mock_db.client.get_connection_kwargs.return_value = {
            "host": "localhost",
            "port": 6379,
        }
        mock_db.weight = 1.0
        return mock_db

    @pytest.fixture
    def mock_database_secondary(self):
        """Create a secondary mock database with different attributes."""
        mock_db = MagicMock()
        mock_db.client.get_connection_kwargs.return_value = {
            "host": "redis-secondary",
            "port": 6380,
        }
        mock_db.weight = 0.5
        return mock_db

    def test_record_geo_failover_automatic(
        self, setup_recorder, mock_database, mock_database_secondary
    ):
        """Test recording automatic geo failover."""
        instruments = setup_recorder

        record_geo_failover(
            fail_from=mock_database,
            fail_to=mock_database_secondary,
            reason=GeoFailoverReason.AUTOMATIC,
        )

        instruments.geo_failovers.add.assert_called_once()
        call_args = instruments.geo_failovers.add.call_args

        # Counter increments by 1
        assert call_args[0][0] == 1

        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_GEOFAILOVER_FAIL_FROM] == "localhost:6379/1.0"
        assert attrs[DB_CLIENT_GEOFAILOVER_FAIL_TO] == "redis-secondary:6380/0.5"
        assert attrs[DB_CLIENT_GEOFAILOVER_REASON] == "automatic"

    def test_record_geo_failover_manual(
        self, setup_recorder, mock_database, mock_database_secondary
    ):
        """Test recording manual geo failover."""
        instruments = setup_recorder

        record_geo_failover(
            fail_from=mock_database_secondary,
            fail_to=mock_database,
            reason=GeoFailoverReason.MANUAL,
        )

        instruments.geo_failovers.add.assert_called_once()
        call_args = instruments.geo_failovers.add.call_args

        assert call_args[0][0] == 1

        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_GEOFAILOVER_FAIL_FROM] == "redis-secondary:6380/0.5"
        assert attrs[DB_CLIENT_GEOFAILOVER_FAIL_TO] == "localhost:6379/1.0"
        assert attrs[DB_CLIENT_GEOFAILOVER_REASON] == "manual"


@pytest.mark.fixed_client
class TestRecordPubsubMessage:
    """Tests for record_pubsub_message - verifies Counter.add() calls."""

    def test_record_pubsub_message_publish(self, setup_recorder):
        """Test recording published message."""

        instruments = setup_recorder

        record_pubsub_message(
            direction=PubSubDirection.PUBLISH,
            channel="my-channel",
            sharded=False,
        )

        instruments.pubsub_messages.add.assert_called_once()
        call_args = instruments.pubsub_messages.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]["attributes"]
        assert (
            attrs[REDIS_CLIENT_PUBSUB_MESSAGE_DIRECTION]
            == PubSubDirection.PUBLISH.value
        )
        assert attrs[REDIS_CLIENT_PUBSUB_CHANNEL] == "my-channel"
        assert attrs[REDIS_CLIENT_PUBSUB_SHARDED] is False

    def test_record_pubsub_message_receive_sharded(self, setup_recorder):
        """Test recording received message on sharded channel."""

        instruments = setup_recorder

        record_pubsub_message(
            direction=PubSubDirection.RECEIVE,
            channel="sharded-channel",
            sharded=True,
        )

        instruments.pubsub_messages.add.assert_called_once()
        attrs = instruments.pubsub_messages.add.call_args[1]["attributes"]
        assert (
            attrs[REDIS_CLIENT_PUBSUB_MESSAGE_DIRECTION]
            == PubSubDirection.RECEIVE.value
        )
        assert attrs[REDIS_CLIENT_PUBSUB_CHANNEL] == "sharded-channel"
        assert attrs[REDIS_CLIENT_PUBSUB_SHARDED] is True


@pytest.mark.fixed_client
class TestRecordStreamingLag:
    """Tests for record_streaming_lag - verifies Histogram.record() calls."""

    def test_record_streaming_lag_with_all_attributes(self, setup_recorder):
        """Test recording streaming lag with all attributes."""

        instruments = setup_recorder

        record_streaming_lag(
            lag_seconds=0.150,
            stream_name="my-stream",
            consumer_group="my-group",
        )

        instruments.stream_lag.record.assert_called_once()
        call_args = instruments.stream_lag.record.call_args

        # Verify lag value
        assert call_args[0][0] == 0.150

        # Verify attributes
        attrs = call_args[1]["attributes"]
        assert attrs[REDIS_CLIENT_STREAM_NAME] == "my-stream"
        assert attrs[REDIS_CLIENT_CONSUMER_GROUP] == "my-group"

    def test_record_streaming_lag_minimal(self, setup_recorder):
        """Test recording streaming lag with only required attributes."""

        instruments = setup_recorder

        record_streaming_lag(
            lag_seconds=0.025,
        )

        instruments.stream_lag.record.assert_called_once()
        call_args = instruments.stream_lag.record.call_args

        # Verify lag value
        assert call_args[0][0] == 0.025

    def test_record_streaming_lag_with_stream_only(self, setup_recorder):
        """Test recording streaming lag with stream name only."""

        instruments = setup_recorder

        record_streaming_lag(
            lag_seconds=0.500,
            stream_name="events-stream",
        )

        instruments.stream_lag.record.assert_called_once()
        attrs = instruments.stream_lag.record.call_args[1]["attributes"]
        assert attrs[REDIS_CLIENT_STREAM_NAME] == "events-stream"

    @pytest.mark.parametrize("protocol", [None, 2, 3])
    @pytest.mark.parametrize("legacy_responses", [False, True])
    @pytest.mark.parametrize("command", ["XREAD", "XREADGROUP"])
    @pytest.mark.parametrize("decode_responses", [False, True])
    def test_streaming_lag_response_modes(
        self, setup_recorder, protocol, legacy_responses, command, decode_responses
    ):
        def encode(value):
            return value if decode_responses else value.encode()

        entries = [
            [encode("1000-0"), [encode("field"), encode("first")]],
            [encode("2000-0"), [encode("field"), encode("second")]],
        ]
        stream = encode("events")
        raw = [[stream, entries]] if protocol == 2 else {stream: entries}
        response = get_response_callbacks(protocol, legacy_responses)[command](raw)
        group = "workers" if command == "XREADGROUP" else None
        recorder.record_streaming_lag_from_response(response, consumer_group=group)
        instrument = setup_recorder.stream_lag
        assert instrument.record.call_count == 2
        for call in instrument.record.call_args_list:
            assert call.kwargs["attributes"][REDIS_CLIENT_STREAM_NAME] == "events"
            assert call.args[0] >= 0
            if group is not None:
                assert call.kwargs["attributes"][REDIS_CLIENT_CONSUMER_GROUP] == group


@pytest.mark.fixed_client
class TestHidePubSubChannelNames:
    """Tests for hide_pubsub_channel_names configuration option."""

    @pytest.fixture
    def setup_recorder_with_hidden_channels(self, mock_meter, mock_instruments):
        """Setup recorder with hide_pubsub_channel_names=True."""
        config = OTelConfig(
            metric_groups=[MetricGroup.PUBSUB],
            hide_pubsub_channel_names=True,
        )

        recorder.reset_collector()

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with patch.object(recorder, "_metrics_collector", collector):
            with patch.object(recorder, "_get_config", return_value=config):
                yield mock_instruments

        recorder.reset_collector()

    def test_channel_name_hidden_when_configured(
        self, setup_recorder_with_hidden_channels
    ):
        """Test that channel name is hidden when hide_pubsub_channel_names=True."""
        instruments = setup_recorder_with_hidden_channels

        record_pubsub_message(
            direction=PubSubDirection.PUBLISH,
            channel="secret-channel",
            sharded=False,
        )

        instruments.pubsub_messages.add.assert_called_once()
        attrs = instruments.pubsub_messages.add.call_args[1]["attributes"]
        assert (
            attrs[REDIS_CLIENT_PUBSUB_MESSAGE_DIRECTION]
            == PubSubDirection.PUBLISH.value
        )
        # Channel should NOT be in attributes when hidden
        assert REDIS_CLIENT_PUBSUB_CHANNEL not in attrs
        assert attrs[REDIS_CLIENT_PUBSUB_SHARDED] is False

    def test_channel_name_visible_when_not_configured(self, setup_recorder):
        """Test that channel name is visible when hide_pubsub_channel_names=False (default)."""
        instruments = setup_recorder

        record_pubsub_message(
            direction=PubSubDirection.PUBLISH,
            channel="visible-channel",
            sharded=False,
        )

        instruments.pubsub_messages.add.assert_called_once()
        attrs = instruments.pubsub_messages.add.call_args[1]["attributes"]
        assert attrs[REDIS_CLIENT_PUBSUB_CHANNEL] == "visible-channel"


@pytest.mark.fixed_client
class TestHideStreamNames:
    """Tests for hide_stream_names configuration option."""

    @pytest.fixture
    def setup_recorder_with_hidden_streams(self, mock_meter, mock_instruments):
        """Setup recorder with hide_stream_names=True."""
        config = OTelConfig(
            metric_groups=[MetricGroup.STREAMING],
            hide_stream_names=True,
        )

        recorder.reset_collector()

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with patch.object(recorder, "_metrics_collector", collector):
            with patch.object(recorder, "_get_config", return_value=config):
                yield mock_instruments

        recorder.reset_collector()

    def test_stream_name_hidden_when_configured(
        self, setup_recorder_with_hidden_streams
    ):
        """Test that stream name is hidden when hide_stream_names=True."""
        instruments = setup_recorder_with_hidden_streams

        record_streaming_lag(
            lag_seconds=0.150,
            stream_name="secret-stream",
            consumer_group="my-group",
        )

        instruments.stream_lag.record.assert_called_once()
        attrs = instruments.stream_lag.record.call_args[1]["attributes"]
        # Stream name should NOT be in attributes when hidden
        assert REDIS_CLIENT_STREAM_NAME not in attrs
        assert attrs[REDIS_CLIENT_CONSUMER_GROUP] == "my-group"

    def test_stream_name_visible_when_not_configured(self, setup_recorder):
        """Test that stream name is visible when hide_stream_names=False (default)."""
        instruments = setup_recorder

        record_streaming_lag(
            lag_seconds=0.150, stream_name="visible-stream", consumer_group="my-group"
        )

        instruments.stream_lag.record.assert_called_once()
        attrs = instruments.stream_lag.record.call_args[1]["attributes"]
        assert attrs[REDIS_CLIENT_STREAM_NAME] == "visible-stream"

    def test_stream_name_hidden_in_record_streaming_lag_from_response_resp3(
        self, setup_recorder_with_hidden_streams
    ):
        """Test that stream names are hidden in record_streaming_lag_from_response for RESP3 format."""
        instruments = setup_recorder_with_hidden_streams

        # RESP3 format: dict with stream name as key
        # Message ID format: timestamp-sequence (e.g., "1234567890123-0")
        import time

        current_time_ms = int(time.time() * 1000)
        message_id = f"{current_time_ms}-0"

        response = {
            "secret-stream": [
                [
                    (message_id, {"field": "value"}),
                ]
            ]
        }

        from redis.observability.recorder import record_streaming_lag_from_response

        record_streaming_lag_from_response(response=response, consumer_group="my-group")

        instruments.stream_lag.record.assert_called_once()
        attrs = instruments.stream_lag.record.call_args[1]["attributes"]
        # Stream name should NOT be in attributes when hidden
        assert REDIS_CLIENT_STREAM_NAME not in attrs

    def test_stream_name_hidden_in_record_streaming_lag_from_response_resp2(
        self, setup_recorder_with_hidden_streams
    ):
        """Test that stream names are hidden in record_streaming_lag_from_response for RESP2 format."""
        instruments = setup_recorder_with_hidden_streams

        # RESP2 format: list of [stream_name, messages]
        import time

        current_time_ms = int(time.time() * 1000)
        message_id = f"{current_time_ms}-0"

        response = [
            [
                b"secret-stream",
                [
                    (message_id, {"field": "value"}),
                ],
            ]
        ]

        from redis.observability.recorder import record_streaming_lag_from_response

        record_streaming_lag_from_response(response=response, consumer_group="my-group")

        instruments.stream_lag.record.assert_called_once()
        attrs = instruments.stream_lag.record.call_args[1]["attributes"]
        # Stream name should NOT be in attributes when hidden
        assert REDIS_CLIENT_STREAM_NAME not in attrs


@pytest.mark.fixed_client
class TestRecorderDisabled:
    """Tests for recorder behavior when observability is disabled."""

    def test_record_operation_duration_when_disabled(self):
        """Test that recording does nothing when collector is None."""

        reset_collector()

        with (
            patch.object(recorder, "_metrics_collector", None),
            patch.object(recorder, "_collector_resolved", True),
        ):
            # Should not raise any exception
            record_operation_duration(
                command_name="SET",
                duration_seconds=0.005,
                server_address="localhost",
                server_port=6379,
            )

        reset_collector()

    def test_is_enabled_returns_false_when_disabled(self):
        """Test is_enabled returns False when collector is None."""
        reset_collector()

        with (
            patch.object(recorder, "_metrics_collector", None),
            patch.object(recorder, "_collector_resolved", True),
        ):
            assert recorder.is_enabled() is False

        recorder.reset_collector()

    def test_all_record_functions_safe_when_disabled(self):
        """Test that all record functions are safe to call when disabled."""

        reset_collector()

        with (
            patch.object(recorder, "_metrics_collector", None),
            patch.object(recorder, "_collector_resolved", True),
        ):
            # None of these should raise
            recorder.record_connection_create_time("pool", 0.1)
            recorder.record_connection_timeout("pool")
            recorder.record_connection_wait_time("pool", 0.1)
            recorder.record_connection_closed("pool")
            recorder.record_connection_relaxed_timeout("pool", "MOVING", True)
            recorder.record_connection_handoff("pool")
            recorder.record_error_count("host", 6379, "127.0.0.1", 6379, Exception(), 0)
            recorder.record_maint_notification_count(
                "host", 6379, "127.0.0.1", 6379, "MOVING"
            )
            recorder.record_pubsub_message(PubSubDirection.PUBLISH)
            recorder.record_streaming_lag(0.1, "stream", "group", "consumer")

        recorder.reset_collector()


@pytest.mark.fixed_client
class TestResetCollector:
    """Tests for reset_collector function."""

    def test_reset_collector_clears_global(self):
        """Test that reset_collector clears the global collector."""

        reset_collector()
        assert recorder._metrics_collector is None


class _Config:
    enabled_telemetry = True


class _MeterProvider:
    def __init__(self, hook):
        self._hook = hook

    def get_meter(self, *_args, **_kwargs):
        # The read a concurrent reset can land in, after the "resolved" flag has
        # been set and before the collector is published.
        self._hook()
        return MagicMock(name="meter")


class _ProviderManager:
    def __init__(self, hook):
        self.config = _Config()
        self._provider = _MeterProvider(hook)

    def get_meter_provider(self):
        return self._provider


class _Observability:
    def __init__(self, manager):
        self._manager = manager

    def get_provider_manager(self):
        return self._manager


class _StandInCollector:
    """Keeps the class attributes the resolver reads off the collector class.

    Replacing the collector class with a plain callable raises AttributeError on
    the ``METER_NAME`` read, which the resolver's broad ``except`` swallows, and
    the resolution then looks like a miss for the wrong reason.
    """

    METER_NAME = "redis-py"
    METER_VERSION = "1"

    def __init__(self, *_args, **_kwargs):
        pass

    def record_operation_duration(self, **_kwargs):
        pass


def _record_with_the_real_resolver(hook):
    """One recording, with ``hook`` called during the meter read."""
    manager = _ProviderManager(hook)
    with (
        patch.object(
            recorder, "get_observability_instance", lambda: _Observability(manager)
        ),
        patch.object(recorder, "RedisMetricsCollector", _StandInCollector),
    ):
        recorder.reset_collector()
        record_operation_duration("GET", 0.001, "localhost", 6379)


@pytest.mark.fixed_client
class TestCollectorResolution:
    """The collector lookup runs once per observability configuration.

    The recording helpers run on every command, so a miss - observability is
    disabled - is cached as well. ObservabilityInstance.init() and
    reset_collector() drop that cache so a later enable still collects.
    """

    def test_disabled_path_resolves_the_collector_once(self, monkeypatch):
        recorder.reset_collector()
        lookups = []
        original = recorder.get_observability_instance

        def counting(*args, **kwargs):
            lookups.append(1)
            return original(*args, **kwargs)

        monkeypatch.setattr(recorder, "get_observability_instance", counting)

        for _ in range(5):
            record_operation_duration("GET", 0.001, "localhost", 6379)
            record_connection_count("pool", ConnectionState.IDLE, 1)

        assert len(lookups) == 1
        reset_collector()

    def test_reset_collector_resolves_again(self, monkeypatch):
        recorder.reset_collector()
        lookups = []
        original = recorder.get_observability_instance

        def counting(*args, **kwargs):
            lookups.append(1)
            return original(*args, **kwargs)

        monkeypatch.setattr(recorder, "get_observability_instance", counting)

        record_operation_duration("GET", 0.001, "localhost", 6379)
        assert len(lookups) == 1

        reset_collector()
        record_operation_duration("GET", 0.001, "localhost", 6379)
        assert len(lookups) == 2
        reset_collector()

    def test_init_drops_a_cached_miss(
        self, monkeypatch, metrics_collector, mock_instruments
    ):
        """init() must invalidate the recorder's cached collector.

        Both recorders cache the resolved collector, including the answer
        "observability is disabled", so that a disabled recording stays cheap.
        Once cached, that answer is only ever replaced by dropping the cache, so
        if init() did not drop it a recorder that ran before init() would stay
        silent for the rest of the process.
        """
        recorder.reset_collector()

        # Prime the cache the way a recording call before init() would: a miss
        # is cached by setting the "resolved" flag with no collector.
        recorder._metrics_collector = None
        recorder._collector_resolved = True

        ObservabilityInstance().init(OTelConfig())

        assert recorder._collector_resolved is False, (
            "init() must drop the recorder's cached miss, or the recorder "
            "stays silent after observability is enabled"
        )
        assert recorder._metrics_collector is None
        reset_collector()

    def test_resolution_spanning_a_reset_does_not_cache_its_answer(self):
        """A reset during the resolution must leave the cache unresolved.

        This drives the real resolver, with the reset injected during the
        provider-manager read: the resolution starts before the reset and
        finishes after it, so the collector it builds belongs to the
        configuration the reset replaced. Publishing it would pin a stale value
        into the cache; leaving the flag set would be worse, because the next
        caller would read a cached miss and metrics would stay off until the
        next init.
        """
        recorder.reset_collector()
        resets = []

        def inject_reset():
            if not resets:
                resets.append(1)
                recorder.reset_collector()

        _record_with_the_real_resolver(inject_reset)

        assert resets == [1], "the reset must land inside the resolution"
        assert recorder._metrics_collector is None, (
            "the collector was built against the configuration the reset "
            "replaced, so it must not be published as the cache"
        )
        assert recorder._collector_resolved is False, (
            "the cache must be left unresolved; a cached miss here is what "
            "silences metrics until the next init"
        )
        recorder.reset_collector()

    def test_resolution_without_a_reset_does_cache_its_answer(self):
        """The control: with no reset the resolution publishes normally.

        Without this, the test above would also pass if the resolver simply
        never cached anything.
        """
        recorder.reset_collector()
        _record_with_the_real_resolver(lambda: None)

        assert recorder._metrics_collector is not None, (
            "an uninterrupted resolution must cache the collector it built"
        )
        assert recorder._collector_resolved is True

        recorder.reset_collector()


@pytest.mark.fixed_client
class TestMetricGroupsDisabled:
    """Tests for verifying metrics are not sent to Meter when their MetricGroup is disabled.

    These tests call recorder.record_*() functions and verify that no calls
    are made to the underlying Meter instruments (.add() or .record()).
    """

    def _create_collector_with_disabled_groups(self, mock_instruments, enabled_groups):
        """Helper to create a collector with specific metric groups enabled."""
        mock_meter = MagicMock()

        def create_counter_side_effect(name, **kwargs):
            instrument_map = {
                "redis.client.errors": mock_instruments.client_errors,
                "redis.client.maintenance.notifications": mock_instruments.maintenance_notifications,
                "db.client.connection.timeouts": mock_instruments.connection_timeouts,
                "redis.client.connection.closed": mock_instruments.connection_closed,
                "redis.client.connection.handoff": mock_instruments.connection_handoff,
                "redis.client.pubsub.messages": mock_instruments.pubsub_messages,
            }
            return instrument_map.get(name, MagicMock())

        def create_gauge_side_effect(name, **kwargs):
            instrument_map = {
                "db.client.connection.count": mock_instruments.connection_count,
            }
            return instrument_map.get(name, MagicMock())

        def create_up_down_counter_side_effect(name, **kwargs):
            instrument_map = {
                "redis.client.connection.relaxed_timeout": mock_instruments.connection_relaxed_timeout,
            }
            return instrument_map.get(name, MagicMock())

        def create_histogram_side_effect(name, **kwargs):
            instrument_map = {
                "db.client.connection.create_time": mock_instruments.connection_create_time,
                "db.client.connection.wait_time": mock_instruments.connection_wait_time,
                "db.client.connection.use_time": mock_instruments.connection_use_time,
                "db.client.operation.duration": mock_instruments.operation_duration,
                "redis.client.stream.lag": mock_instruments.stream_lag,
            }
            return instrument_map.get(name, MagicMock())

        mock_meter.create_counter.side_effect = create_counter_side_effect
        # The RedisMetricsCollector uses create_observable_gauge in the implementation,
        # so we need to mock that here to ensure the tests observe the correct behavior.
        mock_meter.create_observable_gauge.side_effect = create_gauge_side_effect
        # Keep create_gauge mocked as well in case it is used elsewhere.
        mock_meter.create_gauge.side_effect = create_gauge_side_effect
        mock_meter.create_up_down_counter.side_effect = (
            create_up_down_counter_side_effect
        )
        mock_meter.create_histogram.side_effect = create_histogram_side_effect

        config = OTelConfig(metric_groups=enabled_groups)

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            return RedisMetricsCollector(mock_meter, config)

    def test_record_operation_duration_no_meter_call_when_command_disabled(self):
        """Test that record_operation_duration makes no Meter calls when COMMAND group is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.RESILIENCY],  # No COMMAND
        )

        recorder.reset_collector()
        with patch.object(recorder, "_metrics_collector", collector):
            record_operation_duration(
                command_name="SET",
                duration_seconds=0.005,
                server_address="localhost",
                server_port=6379,
            )

        # Verify no call to the histogram's record method
        instruments.operation_duration.record.assert_not_called()

    def test_record_connection_create_time_no_meter_call_when_connection_basic_disabled(
        self,
    ):
        """Test that record_connection_create_time makes no Meter calls when CONNECTION_BASIC is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND],  # No CONNECTION_BASIC
        )

        recorder.reset_collector()
        with patch.object(recorder, "_metrics_collector", collector):
            record_connection_create_time(
                connection_pool="test-pool",
                duration_seconds=0.050,
            )

        # Verify no call to the histogram's record method
        instruments.connection_create_time.record.assert_not_called()

    def test_record_connection_wait_time_no_meter_call_when_connection_advanced_disabled(
        self,
    ):
        """Test that record_connection_wait_time makes no Meter calls when CONNECTION_ADVANCED is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND],  # No CONNECTION_ADVANCED
        )

        recorder.reset_collector()
        with patch.object(recorder, "_metrics_collector", collector):
            record_connection_wait_time(
                pool_name="test-pool",
                duration_seconds=0.010,
            )

        # Verify no call to the histogram's record method
        instruments.connection_wait_time.record.assert_not_called()

    def test_record_connection_closed_no_meter_call_when_connection_advanced_disabled(
        self,
    ):
        """Test that record_connection_closed makes no Meter calls when CONNECTION_ADVANCED is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND],  # No CONNECTION_ADVANCED
        )

        recorder.reset_collector()
        with patch.object(recorder, "_metrics_collector", collector):
            record_connection_closed(
                close_reason=CloseReason.APPLICATION_CLOSE,
            )

        # Verify no call to the counter's add method
        instruments.connection_closed.add.assert_not_called()

    def test_record_connection_relaxed_timeout_no_meter_call_when_connection_basic_disabled(
        self,
    ):
        """Test that record_connection_relaxed_timeout makes no Meter calls when CONNECTION_BASIC is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND],  # No CONNECTION_BASIC
        )

        recorder.reset_collector()
        with patch.object(recorder, "_metrics_collector", collector):
            record_connection_relaxed_timeout(
                connection_name="test-pool",
                maint_notification="MOVING",
                relaxed=True,
            )

        # Verify no call to the up_down_counter's add method
        instruments.connection_relaxed_timeout.add.assert_not_called()

    def test_record_pubsub_message_no_meter_call_when_pubsub_disabled(self):
        """Test that record_pubsub_message makes no Meter calls when PUBSUB group is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND],  # No PUBSUB
        )

        recorder.reset_collector()
        with patch.object(recorder, "_metrics_collector", collector):
            record_pubsub_message(
                direction=PubSubDirection.PUBLISH,
                channel="test-channel",
            )

        # Verify no call to the counter's add method
        instruments.pubsub_messages.add.assert_not_called()

    def test_record_streaming_lag_no_meter_call_when_streaming_disabled(self):
        """Test that record_streaming_lag makes no Meter calls when STREAMING group is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND],  # No STREAMING
        )

        recorder.reset_collector()
        with patch.object(recorder, "_metrics_collector", collector):
            record_streaming_lag(
                lag_seconds=0.150,
                stream_name="test-stream",
                consumer_group="test-group",
            )

        # Verify no call to the histogram's record method
        instruments.stream_lag.record.assert_not_called()

    def test_record_error_count_no_meter_call_when_resiliency_disabled(self):
        """Test that record_error_count makes no Meter calls when RESILIENCY group is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND],  # No RESILIENCY
        )

        recorder.reset_collector()
        with patch.object(recorder, "_metrics_collector", collector):
            record_error_count(
                server_address="localhost",
                server_port=6379,
                network_peer_address="127.0.0.1",
                network_peer_port=6379,
                error_type=Exception("test error"),
                retry_attempts=0,
            )

        # Verify no call to the counter's add method
        instruments.client_errors.add.assert_not_called()

    def test_record_maint_notification_count_no_meter_call_when_resiliency_disabled(
        self,
    ):
        """Test that record_maint_notification_count makes no Meter calls when RESILIENCY group is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND],  # No RESILIENCY
        )

        recorder.reset_collector()
        with patch.object(recorder, "_metrics_collector", collector):
            recorder.record_maint_notification_count(
                server_address="localhost",
                server_port=6379,
                network_peer_address="127.0.0.1",
                network_peer_port=6379,
                maint_notification="MOVING",
            )

        # Verify no call to the counter's add method
        instruments.maintenance_notifications.add.assert_not_called()

    def test_all_record_functions_no_meter_calls_when_all_groups_disabled(self):
        """Test that all record_* functions make no Meter calls when all groups are disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [],  # No metric groups enabled
        )

        recorder.reset_collector()
        with patch.object(recorder, "_metrics_collector", collector):
            # Call all record functions
            record_operation_duration("GET", 0.001, "localhost", 6379)
            record_connection_create_time("pool", 0.050)
            record_connection_timeout("pool")
            record_connection_wait_time("pool", 0.010)
            record_connection_closed("pool", "shutdown")
            record_connection_relaxed_timeout("pool", "MOVING", True)
            record_connection_handoff("pool")
            record_error_count(
                "localhost", 6379, "127.0.0.1", 6379, Exception("err"), 0
            )
            recorder.record_maint_notification_count(
                "localhost", 6379, "127.0.0.1", 6379, "MOVING"
            )
            record_pubsub_message(PubSubDirection.PUBLISH, "channel")
            record_streaming_lag(0.150, "stream", "group", "consumer")

        # Verify no Meter instrument methods were called
        instruments.operation_duration.record.assert_not_called()
        instruments.connection_create_time.record.assert_not_called()
        instruments.connection_count.set.assert_not_called()
        instruments.connection_timeouts.add.assert_not_called()
        instruments.connection_wait_time.record.assert_not_called()
        instruments.connection_closed.add.assert_not_called()
        instruments.connection_relaxed_timeout.add.assert_not_called()
        instruments.connection_handoff.add.assert_not_called()
        instruments.client_errors.add.assert_not_called()
        instruments.maintenance_notifications.add.assert_not_called()
        instruments.pubsub_messages.add.assert_not_called()
        instruments.stream_lag.record.assert_not_called()

    def test_enabled_group_receives_meter_calls_disabled_group_does_not(self):
        """Test that only enabled groups receive Meter calls."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [
                MetricGroup.COMMAND,
                MetricGroup.PUBSUB,
            ],  # Only COMMAND and PUBSUB enabled
        )

        recorder.reset_collector()
        with patch.object(recorder, "_metrics_collector", collector):
            # Call functions from enabled groups
            record_operation_duration("GET", 0.001, "localhost", 6379)
            record_pubsub_message(PubSubDirection.PUBLISH, "channel")

            # Call functions from disabled groups
            record_error_count(
                "localhost", 6379, "127.0.0.1", 6379, Exception("err"), 0
            )
            recorder.record_maint_notification_count(
                "localhost", 6379, "127.0.0.1", 6379, "MOVING"
            )
            record_streaming_lag(0.150, "stream", "group", "consumer")

        # Enabled groups should have received Meter calls
        instruments.operation_duration.record.assert_called_once()
        instruments.pubsub_messages.add.assert_called_once()

        # Disabled groups should NOT have received Meter calls
        instruments.connection_count.set.assert_not_called()
        instruments.client_errors.add.assert_not_called()
        instruments.maintenance_notifications.add.assert_not_called()
        instruments.stream_lag.record.assert_not_called()


@pytest.mark.fixed_client
class TestObservablesRegistry:
    """Tests for ObservablesRegistry singleton and callback registration."""

    def test_registry_singleton_returns_same_instance(self):
        """Test that get_observables_registry_instance returns the same instance."""
        registry1 = get_observables_registry_instance()
        registry2 = get_observables_registry_instance()
        assert registry1 is registry2

    def test_registry_register_and_get_callbacks(self):
        """Test registering and retrieving callbacks from the registry."""
        registry = ObservablesRegistry()

        callback1 = MagicMock(return_value=[])
        callback2 = MagicMock(return_value=[])

        registry.register("test_metric", callback1)
        registry.register("test_metric", callback2)

        callbacks = registry.get("test_metric")
        assert len(callbacks) == 2
        assert callback1 in callbacks
        assert callback2 in callbacks

    def test_registry_get_returns_empty_list_for_unknown_key(self):
        """Test that get returns empty list for unknown metric key."""
        registry = ObservablesRegistry()
        callbacks = registry.get("unknown_metric")
        assert callbacks == []

    def test_registry_clear_removes_all_callbacks(self):
        """Test that clear removes all registered callbacks."""
        registry = ObservablesRegistry()

        registry.register("metric1", MagicMock())
        registry.register("metric2", MagicMock())

        registry.clear()

        assert registry.get("metric1") == []
        assert registry.get("metric2") == []
        assert len(registry) == 0


@pytest.mark.fixed_client
class TestRecordConnectionCount:
    """Tests for record_connection_count (UpDownCounter)."""

    @pytest.fixture
    def mock_up_down_counter(self):
        """Create a mock UpDownCounter."""
        return MagicMock()

    @pytest.fixture
    def mock_meter_with_counter(self, mock_up_down_counter):
        """Create a mock meter that returns our mock UpDownCounter."""
        meter = MagicMock()
        meter.create_up_down_counter.return_value = mock_up_down_counter
        meter.create_counter.return_value = MagicMock()
        meter.create_histogram.return_value = MagicMock()
        meter.create_observable_gauge.return_value = MagicMock()
        return meter

    @pytest.fixture
    def mock_config_with_connection_basic(self):
        """Create a config with CONNECTION_BASIC enabled."""
        return OTelConfig(metric_groups=[MetricGroup.CONNECTION_BASIC])

    @staticmethod
    def _calls_for(mock_counter, pool_name):
        """The add() calls for ``pool_name`` only.

        Other code in the process (a pool from an earlier test being collected,
        for instance) can record connection counts on the patched collector while
        a test runs, so a test counts only the calls for the pool it uses.
        """
        return [
            c
            for c in mock_counter.add.call_args_list
            if c[1]["attributes"].get(DB_CLIENT_CONNECTION_POOL_NAME) == pool_name
        ]

    @pytest.fixture
    def setup_connection_count_recorder(
        self,
        mock_meter_with_counter,
        mock_config_with_connection_basic,
        mock_up_down_counter,
    ):
        """Setup recorder with mocked meter for connection count tests."""
        recorder.reset_collector()

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(
                mock_meter_with_counter, mock_config_with_connection_basic
            )

        with patch.object(recorder, "_metrics_collector", collector):
            # Reset mock call counts to avoid pollution from external code
            # paths (e.g., connection pool operations in other tests) that may
            # have called record_connection_count on our mock collector.
            mock_up_down_counter.reset_mock()
            yield mock_meter_with_counter

        recorder.reset_collector()

    def test_record_connection_count_increment(
        self, setup_connection_count_recorder, mock_up_down_counter
    ):
        """Test recording connection count increment."""
        recorder.record_connection_count(
            pool_name="localhost:6379_abc123",
            connection_state=ConnectionState.IDLE,
            counter=1,
        )

        calls = self._calls_for(mock_up_down_counter, "localhost:6379_abc123")
        assert len(calls) == 1
        assert calls[0][0][0] == 1
        assert (
            calls[0][1]["attributes"][DB_CLIENT_CONNECTION_POOL_NAME]
            == "localhost:6379_abc123"
        )
        assert calls[0][1]["attributes"][DB_CLIENT_CONNECTION_STATE] == "idle"

    def test_record_connection_count_decrement(
        self, setup_connection_count_recorder, mock_up_down_counter
    ):
        """Test recording connection count decrement."""
        recorder.record_connection_count(
            pool_name="localhost:6379_abc123",
            connection_state=ConnectionState.USED,
            counter=-1,
        )

        calls = self._calls_for(mock_up_down_counter, "localhost:6379_abc123")
        assert len(calls) == 1
        assert calls[0][0][0] == -1
        assert (
            calls[0][1]["attributes"][DB_CLIENT_CONNECTION_POOL_NAME]
            == "localhost:6379_abc123"
        )
        assert calls[0][1]["attributes"][DB_CLIENT_CONNECTION_STATE] == "used"

    def test_record_connection_count_batch_decrement(
        self, setup_connection_count_recorder, mock_up_down_counter
    ):
        """Test recording batch connection count decrement (e.g., pool disconnect)."""
        recorder.record_connection_count(
            pool_name="localhost:6379_abc123",
            connection_state=ConnectionState.IDLE,
            counter=-5,
        )

        calls = self._calls_for(mock_up_down_counter, "localhost:6379_abc123")
        assert len(calls) == 1
        assert calls[0][0][0] == -5
        assert calls[0][1]["attributes"][DB_CLIENT_CONNECTION_STATE] == "idle"

    def test_record_connection_count_lifecycle_scenario(
        self, setup_connection_count_recorder, mock_up_down_counter
    ):
        """Test realistic lifecycle: create connection, acquire, release, re-acquire, disconnect."""
        # 1. New connection created (goes to IDLE)
        recorder.record_connection_count(
            pool_name="pool1",
            connection_state=ConnectionState.IDLE,
            counter=1,
        )

        # 2. Connection acquired from pool (transition: IDLE -> USED)
        recorder.record_connection_count(
            pool_name="pool1",
            connection_state=ConnectionState.IDLE,
            counter=-1,
        )
        recorder.record_connection_count(
            pool_name="pool1",
            connection_state=ConnectionState.USED,
            counter=1,
        )

        # 3. Connection released to pool (transition: USED -> IDLE)
        recorder.record_connection_count(
            pool_name="pool1",
            connection_state=ConnectionState.USED,
            counter=-1,
        )
        recorder.record_connection_count(
            pool_name="pool1",
            connection_state=ConnectionState.IDLE,
            counter=1,
        )

        # 4. Pool disconnect (IDLE -> destroyed)
        recorder.record_connection_count(
            pool_name="pool1",
            connection_state=ConnectionState.IDLE,
            counter=-1,
        )

        # Total calls: 6
        calls = self._calls_for(mock_up_down_counter, "pool1")
        assert len(calls) == 6

        # Step 1: IDLE +1 (creation)
        assert calls[0][0][0] == 1
        assert calls[0][1]["attributes"][DB_CLIENT_CONNECTION_STATE] == "idle"

        # Step 2: IDLE -1, USED +1 (acquire)
        assert calls[1][0][0] == -1
        assert calls[1][1]["attributes"][DB_CLIENT_CONNECTION_STATE] == "idle"
        assert calls[2][0][0] == 1
        assert calls[2][1]["attributes"][DB_CLIENT_CONNECTION_STATE] == "used"

        # Step 3: USED -1, IDLE +1 (release)
        assert calls[3][0][0] == -1
        assert calls[3][1]["attributes"][DB_CLIENT_CONNECTION_STATE] == "used"
        assert calls[4][0][0] == 1
        assert calls[4][1]["attributes"][DB_CLIENT_CONNECTION_STATE] == "idle"

        # Step 4: IDLE -1 (disconnect)
        assert calls[5][0][0] == -1
        assert calls[5][1]["attributes"][DB_CLIENT_CONNECTION_STATE] == "idle"


@pytest.mark.fixed_client
class TestInitCSCItems:
    """Tests for init_csc_items and register_csc_items_callback."""

    @pytest.fixture
    def mock_observable_gauge(self):
        """Create a mock observable gauge."""
        return MagicMock()

    @pytest.fixture
    def mock_meter_with_observable(self, mock_observable_gauge):
        """Create a mock meter that returns our mock observable gauge."""
        meter = MagicMock()
        meter.create_observable_gauge.return_value = mock_observable_gauge
        return meter

    @pytest.fixture
    def mock_config_with_csc(self):
        """Create a config with CSC metric group enabled."""
        return OTelConfig(metric_groups=[MetricGroup.CSC])

    @pytest.fixture
    def setup_csc_recorder(self, mock_meter_with_observable, mock_config_with_csc):
        """Setup recorder with mocked meter for CSC tests."""
        recorder.reset_collector()
        get_observables_registry_instance().clear()

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(
                mock_meter_with_observable, mock_config_with_csc
            )

        with patch.object(recorder, "_metrics_collector", collector):
            yield mock_meter_with_observable

        recorder.reset_collector()
        get_observables_registry_instance().clear()

    def test_init_csc_items_creates_observable_gauge(self, setup_csc_recorder):
        """Test that init_csc_items creates an observable gauge."""
        mock_meter = setup_csc_recorder

        recorder.init_csc_items()

        mock_meter.create_observable_gauge.assert_called_once()
        call_args = mock_meter.create_observable_gauge.call_args
        assert call_args[1]["name"] == "redis.client.csc.items"

    def test_init_csc_items_callback_aggregates_registry_callbacks(
        self, setup_csc_recorder
    ):
        """Test that the CSC observable callback aggregates all registered callbacks."""
        mock_meter = setup_csc_recorder

        recorder.init_csc_items()

        # Get the callback that was passed to create_observable_gauge
        call_args = mock_meter.create_observable_gauge.call_args
        observable_callback = call_args[1]["callbacks"][0]

        # Register some mock CSC callbacks
        mock_observation1 = Observation(100, attributes={"db": 0})
        mock_observation2 = Observation(50, attributes={"db": 1})

        csc_callback1 = MagicMock(return_value=[mock_observation1])
        csc_callback2 = MagicMock(return_value=[mock_observation2])

        registry = get_observables_registry_instance()
        registry.register(recorder.CSC_ITEMS_REGISTRY_KEY, csc_callback1)
        registry.register(recorder.CSC_ITEMS_REGISTRY_KEY, csc_callback2)

        # Call the observable callback
        observations = observable_callback(None)

        # Verify both callbacks were called and observations aggregated
        csc_callback1.assert_called_once()
        csc_callback2.assert_called_once()
        assert len(observations) == 2
        assert mock_observation1 in observations
        assert mock_observation2 in observations

    def test_register_csc_items_callback_adds_callback_to_registry(
        self, setup_csc_recorder
    ):
        """Test that register_csc_items_callback adds a callback to the registry."""
        # Create a mock cache size callback
        cache_size_callback = MagicMock(return_value=42)

        recorder.register_csc_items_callback(
            cache_size_callback, pool_name="TestPool(localhost:6379/0)"
        )

        registry = get_observables_registry_instance()
        callbacks = registry.get(recorder.CSC_ITEMS_REGISTRY_KEY)

        assert len(callbacks) == 1

        # Call the registered callback and verify it returns an observation
        observations = callbacks[0]()

        assert len(observations) == 1
        assert observations[0].value == 42
        cache_size_callback.assert_called_once()

    def test_register_csc_items_callback_multiple_registrations(
        self, setup_csc_recorder
    ):
        """Test registering multiple CSC callbacks."""
        callback1 = MagicMock(return_value=10)
        callback2 = MagicMock(return_value=20)

        recorder.register_csc_items_callback(
            callback1, pool_name="TestPool(localhost:6379/0)"
        )
        recorder.register_csc_items_callback(
            callback2, pool_name="TestPool(localhost:6379/1)"
        )

        registry = get_observables_registry_instance()
        callbacks = registry.get(recorder.CSC_ITEMS_REGISTRY_KEY)

        assert len(callbacks) == 2

        # Verify each callback returns correct observation
        obs1 = callbacks[0]()
        obs2 = callbacks[1]()

        assert obs1[0].value == 10
        assert obs2[0].value == 20


@pytest.mark.fixed_client
class TestHistogramBucketBoundaries:
    """Tests for custom histogram bucket boundaries configuration."""

    def test_custom_operation_duration_buckets_passed_to_meter(self):
        """Test that custom operation duration buckets are passed to create_histogram."""
        custom_buckets = [0.001, 0.01, 0.1, 1.0]
        config = OTelConfig(
            metric_groups=[MetricGroup.COMMAND],
            buckets_operation_duration=custom_buckets,
        )

        mock_meter = MagicMock()
        mock_meter.create_histogram.return_value = MagicMock()

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            RedisMetricsCollector(mock_meter, config)

        # Verify create_histogram was called with the custom buckets
        mock_meter.create_histogram.assert_called_once()
        call_kwargs = mock_meter.create_histogram.call_args[1]
        assert call_kwargs["name"] == "db.client.operation.duration"
        assert call_kwargs["explicit_bucket_boundaries_advisory"] == custom_buckets

    def test_custom_connection_create_time_buckets_passed_to_meter(self):
        """Test that custom connection create time buckets are passed to create_histogram."""
        custom_buckets = [0.0001, 0.001, 0.01, 0.1]
        config = OTelConfig(
            metric_groups=[MetricGroup.CONNECTION_BASIC],
            buckets_connection_create_time=custom_buckets,
        )

        mock_meter = MagicMock()
        mock_meter.create_histogram.return_value = MagicMock()
        mock_meter.create_up_down_counter.return_value = MagicMock()
        mock_meter.create_counter.return_value = MagicMock()

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            RedisMetricsCollector(mock_meter, config)

        # Find the call for connection_create_time
        histogram_calls = mock_meter.create_histogram.call_args_list
        create_time_call = None
        for c in histogram_calls:
            if c[1].get("name") == "db.client.connection.create_time":
                create_time_call = c
                break

        assert create_time_call is not None
        assert (
            create_time_call[1]["explicit_bucket_boundaries_advisory"] == custom_buckets
        )

    def test_custom_connection_wait_time_buckets_passed_to_meter(self):
        """Test that custom connection wait time buckets are passed to create_histogram."""
        custom_buckets = [0.00001, 0.0001, 0.001, 0.01]
        config = OTelConfig(
            metric_groups=[MetricGroup.CONNECTION_ADVANCED],
            buckets_connection_wait_time=custom_buckets,
        )

        mock_meter = MagicMock()
        mock_meter.create_histogram.return_value = MagicMock()
        mock_meter.create_counter.return_value = MagicMock()

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            RedisMetricsCollector(mock_meter, config)

        # Find the call for connection_wait_time
        histogram_calls = mock_meter.create_histogram.call_args_list
        wait_time_call = None
        for c in histogram_calls:
            if c[1].get("name") == "db.client.connection.wait_time":
                wait_time_call = c
                break

        assert wait_time_call is not None
        assert (
            wait_time_call[1]["explicit_bucket_boundaries_advisory"] == custom_buckets
        )

    def test_custom_stream_lag_buckets_passed_to_meter(self):
        """Test that custom stream processing duration buckets are passed to create_histogram."""
        custom_buckets = [0.01, 0.1, 1.0, 10.0]
        config = OTelConfig(
            metric_groups=[MetricGroup.STREAMING],
            buckets_stream_processing_duration=custom_buckets,
        )

        mock_meter = MagicMock()
        mock_meter.create_histogram.return_value = MagicMock()

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            RedisMetricsCollector(mock_meter, config)

        # Verify create_histogram was called with the custom buckets
        mock_meter.create_histogram.assert_called_once()
        call_kwargs = mock_meter.create_histogram.call_args[1]
        assert call_kwargs["name"] == "redis.client.stream.lag"
        assert call_kwargs["explicit_bucket_boundaries_advisory"] == custom_buckets

    def test_default_buckets_used_when_not_specified(self):
        """Test that default bucket boundaries are used when not specified."""
        from redis.observability.config import default_operation_duration_buckets

        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        mock_meter = MagicMock()
        mock_meter.create_histogram.return_value = MagicMock()

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            RedisMetricsCollector(mock_meter, config)

        call_kwargs = mock_meter.create_histogram.call_args[1]
        assert (
            call_kwargs["explicit_bucket_boundaries_advisory"]
            == default_operation_duration_buckets()
        )
