"""
Unit tests for redis.asyncio.observability.recorder module.

These tests verify that async recorder functions correctly pass arguments through
to the underlying OTel Meter instruments (Counter, Histogram, UpDownCounter).
The MeterProvider is mocked to verify the actual integration point where
metrics are exported to OTel.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from redis._parsers.response_callbacks import get_response_callbacks

from redis.asyncio.observability import recorder
from redis.observability.attributes import (
    ConnectionState,
    GeoFailoverReason,
    PubSubDirection,
    SERVER_ADDRESS,
    SERVER_PORT,
    DB_NAMESPACE,
    DB_OPERATION_NAME,
    DB_RESPONSE_STATUS_CODE,
    DB_CLIENT_CONNECTION_STATE,
    ERROR_TYPE,
    NETWORK_PEER_ADDRESS,
    NETWORK_PEER_PORT,
    DB_CLIENT_CONNECTION_POOL_NAME,
    DB_CLIENT_GEOFAILOVER_FAIL_FROM,
    DB_CLIENT_GEOFAILOVER_FAIL_TO,
    DB_CLIENT_GEOFAILOVER_REASON,
    REDIS_CLIENT_OPERATION_RETRY_ATTEMPTS,
    REDIS_CLIENT_STREAM_NAME,
    REDIS_CLIENT_CONSUMER_GROUP,
    REDIS_CLIENT_PUBSUB_CHANNEL,
    REDIS_CLIENT_PUBSUB_MESSAGE_DIRECTION,
    REDIS_CLIENT_PUBSUB_SHARDED,
)
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector, CloseReason
from redis.observability.providers import ObservabilityInstance
from redis.observability.registry import get_observables_registry_instance


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
    return OTelConfig(
        metric_groups=[
            MetricGroup.RESILIENCY,
            MetricGroup.CONNECTION_BASIC,
            MetricGroup.CONNECTION_ADVANCED,
            MetricGroup.COMMAND,
            MetricGroup.PUBSUB,
            MetricGroup.STREAMING,
        ]
    )


@pytest.fixture
def metrics_collector(mock_meter, mock_config):
    """Create a real RedisMetricsCollector with mocked Meter."""
    with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
        collector = RedisMetricsCollector(mock_meter, mock_config)
        return collector


@pytest.fixture
def setup_async_recorder(metrics_collector, mock_instruments):
    """
    Setup the async recorder module with our collector that has mocked instruments.
    """
    # Reset the global collector before test
    recorder.reset_collector()
    get_observables_registry_instance().clear()

    # Install our collector with mocked instruments directly: the recording helpers read this variable.
    with patch.object(recorder, "_async_metrics_collector", metrics_collector):
        yield mock_instruments

    # Reset after test
    recorder.reset_collector()
    get_observables_registry_instance().clear()


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestRecordOperationDuration:
    """Tests for record_operation_duration - verifies Histogram.record() calls."""

    async def test_record_operation_duration_success(self, setup_async_recorder):
        """Test that operation duration is recorded to the histogram with correct attributes."""
        instruments = setup_async_recorder

        await recorder.record_operation_duration(
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

    async def test_record_operation_duration_with_error(self, setup_async_recorder):
        """Test that error information is included in attributes."""
        instruments = setup_async_recorder

        error = ConnectionError("Connection refused")
        await recorder.record_operation_duration(
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


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestRecordConnectionCreateTime:
    """Tests for record_connection_create_time - verifies Histogram.record() calls."""

    async def test_record_connection_create_time(self, setup_async_recorder):
        """Test that connection create time is recorded correctly."""
        instruments = setup_async_recorder

        mock_pool = MagicMock()
        mock_pool.__class__.__name__ = "ConnectionPool"
        mock_pool.connection_kwargs = {"host": "localhost", "port": 6379, "db": 0}
        mock_pool._pool_id = "a1b2c3d4"  # Mock the unique pool ID

        await recorder.record_connection_create_time(
            connection_pool=mock_pool,
            duration_seconds=0.050,
        )

        instruments.connection_create_time.record.assert_called_once()
        call_args = instruments.connection_create_time.record.call_args

        # Verify duration value
        assert call_args[0][0] == 0.050

        # Verify attributes
        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "localhost:6379_a1b2c3d4"


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestRecordConnectionTimeout:
    """Tests for record_connection_timeout - verifies Counter.add() calls."""

    async def test_record_connection_timeout(self, setup_async_recorder):
        """Test that connection timeout is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_connection_timeout(pool_name="test_pool")

        instruments.connection_timeouts.add.assert_called_once()
        call_args = instruments.connection_timeouts.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "test_pool"


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestRecordConnectionWaitTime:
    """Tests for record_connection_wait_time - verifies Histogram.record() calls."""

    async def test_record_connection_wait_time(self, setup_async_recorder):
        """Test that connection wait time is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_connection_wait_time(
            pool_name="test_pool",
            duration_seconds=0.010,
        )

        instruments.connection_wait_time.record.assert_called_once()
        call_args = instruments.connection_wait_time.record.call_args

        assert call_args[0][0] == 0.010
        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "test_pool"


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestRecordConnectionClosed:
    """Tests for record_connection_closed - verifies Counter.add() calls."""

    async def test_record_connection_closed(self, setup_async_recorder):
        """Test that connection closed is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_connection_closed(
            close_reason=CloseReason.ERROR,
            error_type=ConnectionError("Connection lost"),
        )

        instruments.connection_closed.add.assert_called_once()


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestRecordConnectionRelaxedTimeout:
    """Tests for record_connection_relaxed_timeout - verifies UpDownCounter calls."""

    async def test_record_connection_relaxed_timeout_relaxed(
        self, setup_async_recorder
    ):
        """Test that relaxed timeout is recorded correctly when relaxed=True."""
        instruments = setup_async_recorder

        await recorder.record_connection_relaxed_timeout(
            connection_name="localhost:6379_abc123",
            maint_notification="MOVING",
            relaxed=True,
        )

        instruments.connection_relaxed_timeout.add.assert_called_once()
        call_args = instruments.connection_relaxed_timeout.add.call_args
        assert call_args[0][0] == 1  # +1 for relaxed

    async def test_record_connection_relaxed_timeout_unrelaxed(
        self, setup_async_recorder
    ):
        """Test that relaxed timeout is recorded correctly when relaxed=False."""
        instruments = setup_async_recorder

        await recorder.record_connection_relaxed_timeout(
            connection_name="localhost:6379_abc123",
            maint_notification="MOVING",
            relaxed=False,
        )

        instruments.connection_relaxed_timeout.add.assert_called_once()
        call_args = instruments.connection_relaxed_timeout.add.call_args
        assert call_args[0][0] == -1  # -1 for unrelaxed


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestRecordConnectionHandoff:
    """Tests for record_connection_handoff - verifies Counter.add() calls."""

    async def test_record_connection_handoff(self, setup_async_recorder):
        """Test that connection handoff is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_connection_handoff(pool_name="test_pool")

        instruments.connection_handoff.add.assert_called_once()
        call_args = instruments.connection_handoff.add.call_args
        assert call_args[0][0] == 1


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestRecordErrorCount:
    """Tests for record_error_count - verifies Counter.add() calls."""

    async def test_record_error_count(self, setup_async_recorder):
        """Test recording error count with all attributes."""
        instruments = setup_async_recorder

        error = ConnectionError("Connection refused")
        await recorder.record_error_count(
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

    async def test_record_error_count_with_is_internal_false(
        self, setup_async_recorder
    ):
        """Test recording error count with is_internal=False."""
        instruments = setup_async_recorder

        error = TimeoutError("Connection timed out")
        await recorder.record_error_count(
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
@pytest.mark.asyncio
class TestRecordPubsubMessage:
    """Tests for record_pubsub_message - verifies Counter.add() calls."""

    async def test_record_pubsub_message_publish(self, setup_async_recorder):
        """Test that pubsub publish message is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_pubsub_message(
            direction=PubSubDirection.PUBLISH,
            channel="test_channel",
            sharded=False,
        )

        instruments.pubsub_messages.add.assert_called_once()

    async def test_record_pubsub_message_receive(self, setup_async_recorder):
        """Test that pubsub receive message is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_pubsub_message(
            direction=PubSubDirection.RECEIVE,
            channel="test_channel",
            sharded=True,
        )

        instruments.pubsub_messages.add.assert_called_once()


@pytest.mark.fixed_client
@pytest.mark.asyncio
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

    async def test_record_geo_failover_automatic(
        self, setup_async_recorder, mock_database, mock_database_secondary
    ):
        """Test recording automatic geo failover."""
        instruments = setup_async_recorder

        await recorder.record_geo_failover(
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

    async def test_record_geo_failover_manual(
        self, setup_async_recorder, mock_database, mock_database_secondary
    ):
        """Test recording manual geo failover."""
        instruments = setup_async_recorder

        await recorder.record_geo_failover(
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
@pytest.mark.asyncio
class TestHidePubSubChannelNames:
    """Tests for hide_pubsub_channel_names configuration option."""

    @pytest.fixture
    def setup_async_recorder_with_hidden_channels(self, mock_meter, mock_instruments):
        """Setup async recorder with hide_pubsub_channel_names=True."""
        config = OTelConfig(
            metric_groups=[MetricGroup.PUBSUB],
            hide_pubsub_channel_names=True,
        )

        recorder.reset_collector()
        get_observables_registry_instance().clear()

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Note: _get_config is still async
        with patch.object(recorder, "_async_metrics_collector", collector):
            with patch.object(
                recorder,
                "_get_config",
                new_callable=lambda: AsyncMock(return_value=config),
            ):
                yield mock_instruments

        recorder.reset_collector()
        get_observables_registry_instance().clear()

    async def test_channel_name_hidden_when_configured(
        self, setup_async_recorder_with_hidden_channels
    ):
        """Test that channel name is hidden when hide_pubsub_channel_names=True."""
        instruments = setup_async_recorder_with_hidden_channels

        await recorder.record_pubsub_message(
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

    async def test_channel_name_visible_when_not_configured(self, setup_async_recorder):
        """Test that channel name is visible when hide_pubsub_channel_names=False (default)."""
        instruments = setup_async_recorder

        await recorder.record_pubsub_message(
            direction=PubSubDirection.PUBLISH,
            channel="visible-channel",
            sharded=False,
        )

        instruments.pubsub_messages.add.assert_called_once()
        attrs = instruments.pubsub_messages.add.call_args[1]["attributes"]
        assert attrs[REDIS_CLIENT_PUBSUB_CHANNEL] == "visible-channel"

    async def test_bytes_channel_normalized_to_str(self, setup_async_recorder):
        """Test that bytes channel names are normalized to str."""
        instruments = setup_async_recorder

        await recorder.record_pubsub_message(
            direction=PubSubDirection.RECEIVE,
            channel=b"bytes-channel",
            sharded=True,
        )

        instruments.pubsub_messages.add.assert_called_once()
        attrs = instruments.pubsub_messages.add.call_args[1]["attributes"]
        # Channel should be normalized from bytes to str
        assert attrs[REDIS_CLIENT_PUBSUB_CHANNEL] == "bytes-channel"


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestRecordStreamingLag:
    """Tests for record_streaming_lag - verifies Histogram.record() calls."""

    async def test_record_streaming_lag(self, setup_async_recorder):
        """Test that streaming lag is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_streaming_lag(
            lag_seconds=0.150,
            stream_name="test_stream",
            consumer_group="test_group",
        )

        instruments.stream_lag.record.assert_called_once()
        call_args = instruments.stream_lag.record.call_args
        assert call_args[0][0] == 0.150

    @pytest.mark.parametrize("protocol", [None, 2, 3])
    @pytest.mark.parametrize("legacy_responses", [False, True])
    @pytest.mark.parametrize("command", ["XREAD", "XREADGROUP"])
    @pytest.mark.parametrize("decode_responses", [False, True])
    async def test_streaming_lag_response_modes(
        self,
        setup_async_recorder,
        protocol,
        legacy_responses,
        command,
        decode_responses,
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
        await recorder.record_streaming_lag_from_response(
            response, consumer_group=group
        )
        instrument = setup_async_recorder.stream_lag
        assert instrument.record.call_count == 2
        for call in instrument.record.call_args_list:
            assert call.kwargs["attributes"][REDIS_CLIENT_STREAM_NAME] == "events"
            assert call.args[0] >= 0
            if group is not None:
                assert call.kwargs["attributes"][REDIS_CLIENT_CONSUMER_GROUP] == group

    @pytest.mark.parametrize("protocol", [None, 2, 3])
    @pytest.mark.parametrize("legacy_responses", [False, True])
    @pytest.mark.parametrize("decode_responses", [False, True])
    @pytest.mark.parametrize("claim", [False, True])
    async def test_streaming_lag_reads_deleted_and_claimed_entries(
        self,
        setup_async_recorder,
        protocol,
        legacy_responses,
        decode_responses,
        claim,
    ):
        def encode(value):
            return value if decode_responses else value.encode()

        def entry(message_id):
            fields = [encode("field"), encode("value")]
            if claim:
                return [encode(message_id), fields, 100, 2]
            return [encode(message_id), fields]

        first = [entry("1000-0")]
        second = [entry("2000-0")]
        expected = ["first", "second"]
        if not claim:
            # A deleted pending entry keeps its ID and has nil fields. CLAIM
            # leaves deleted entries out of the reply.
            first.insert(0, [encode("500-0"), None])
            expected.insert(0, "first")
        first_stream, second_stream = encode("first"), encode("second")
        if protocol == 2:
            raw = [[first_stream, first], [second_stream, second]]
        else:
            raw = {first_stream: first, second_stream: second}
        options = {"claim_min_idle_time": 100} if claim else {}
        response = get_response_callbacks(protocol, legacy_responses)["XREADGROUP"](
            raw, **options
        )
        await recorder.record_streaming_lag_from_response(
            response, consumer_group="workers"
        )
        instrument = setup_async_recorder.stream_lag
        assert [
            call.kwargs["attributes"][REDIS_CLIENT_STREAM_NAME]
            for call in instrument.record.call_args_list
        ] == expected


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestRecordStreamingLagFromResponse:
    """Tests for record_streaming_lag_from_response - RESP2/RESP3 parsing and timestamp extraction."""

    @pytest.fixture
    def setup_async_recorder_with_hidden_streams(self, mock_meter, mock_instruments):
        """Setup async recorder with hide_stream_names=True."""
        config = OTelConfig(
            metric_groups=[MetricGroup.STREAMING],
            hide_stream_names=True,
        )

        recorder.reset_collector()
        get_observables_registry_instance().clear()

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Note: _get_config is still async
        with patch.object(recorder, "_async_metrics_collector", collector):
            with patch.object(
                recorder,
                "_get_config",
                new_callable=lambda: AsyncMock(return_value=config),
            ):
                yield mock_instruments

        recorder.reset_collector()
        get_observables_registry_instance().clear()

    async def test_record_streaming_lag_from_response_resp3_format(
        self, setup_async_recorder
    ):
        """Test RESP3 format parsing (dict with stream name as key)."""
        instruments = setup_async_recorder

        import time

        current_time_ms = int(time.time() * 1000)
        message_id = f"{current_time_ms}-0"

        # RESP3 format: dict with stream name as key
        response = {
            "test-stream": [
                [
                    (message_id, {"field": "value"}),
                ]
            ]
        }

        await recorder.record_streaming_lag_from_response(
            response=response,
            consumer_group="my-group",
        )

        instruments.stream_lag.record.assert_called_once()
        call_args = instruments.stream_lag.record.call_args
        attrs = call_args[1]["attributes"]
        assert attrs[REDIS_CLIENT_STREAM_NAME] == "test-stream"
        assert attrs[REDIS_CLIENT_CONSUMER_GROUP] == "my-group"
        # Lag should be non-negative and small (just created)
        assert call_args[0][0] >= 0.0
        assert call_args[0][0] < 1.0  # Should be less than 1 second

    async def test_record_streaming_lag_from_response_resp2_format(
        self, setup_async_recorder
    ):
        """Test RESP2 format parsing (list with bytes stream name)."""
        instruments = setup_async_recorder

        import time

        current_time_ms = int(time.time() * 1000)
        message_id = f"{current_time_ms}-0"

        # RESP2 format: list of [stream_name, messages] with bytes stream name
        response = [
            [
                b"test-stream",
                [
                    (message_id, {"field": "value"}),
                ],
            ]
        ]

        await recorder.record_streaming_lag_from_response(
            response=response,
            consumer_group="my-group",
        )

        instruments.stream_lag.record.assert_called_once()
        call_args = instruments.stream_lag.record.call_args
        attrs = call_args[1]["attributes"]
        # Stream name should be normalized from bytes to str
        assert attrs[REDIS_CLIENT_STREAM_NAME] == "test-stream"
        assert attrs[REDIS_CLIENT_CONSUMER_GROUP] == "my-group"

    async def test_record_streaming_lag_from_response_multiple_messages(
        self, setup_async_recorder
    ):
        """Test that multiple messages are processed correctly."""
        instruments = setup_async_recorder

        import time

        current_time_ms = int(time.time() * 1000)
        message_id_1 = f"{current_time_ms}-0"
        message_id_2 = f"{current_time_ms}-1"

        response = {
            "test-stream": [
                [
                    (message_id_1, {"field": "value1"}),
                    (message_id_2, {"field": "value2"}),
                ]
            ]
        }

        await recorder.record_streaming_lag_from_response(
            response=response,
            consumer_group="my-group",
        )

        # Should record lag for each message
        assert instruments.stream_lag.record.call_count == 2

    async def test_record_streaming_lag_from_response_multiple_streams(
        self, setup_async_recorder
    ):
        """Test that multiple streams are processed correctly."""
        instruments = setup_async_recorder

        import time

        current_time_ms = int(time.time() * 1000)
        message_id = f"{current_time_ms}-0"

        response = {
            "stream-1": [[(message_id, {"field": "value1"})]],
            "stream-2": [[(message_id, {"field": "value2"})]],
        }

        await recorder.record_streaming_lag_from_response(
            response=response,
            consumer_group="my-group",
        )

        # Should record lag for each stream
        assert instruments.stream_lag.record.call_count == 2

    async def test_stream_name_hidden_in_record_streaming_lag_from_response_resp3(
        self, setup_async_recorder_with_hidden_streams
    ):
        """Test that stream names are hidden in record_streaming_lag_from_response for RESP3 format."""
        instruments = setup_async_recorder_with_hidden_streams

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

        await recorder.record_streaming_lag_from_response(
            response=response,
            consumer_group="my-group",
        )

        instruments.stream_lag.record.assert_called_once()
        attrs = instruments.stream_lag.record.call_args[1]["attributes"]
        # Stream name should NOT be in attributes when hidden
        assert REDIS_CLIENT_STREAM_NAME not in attrs

    async def test_stream_name_hidden_in_record_streaming_lag_from_response_resp2(
        self, setup_async_recorder_with_hidden_streams
    ):
        """Test that stream names are hidden in record_streaming_lag_from_response for RESP2 format."""
        instruments = setup_async_recorder_with_hidden_streams

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

        await recorder.record_streaming_lag_from_response(
            response=response,
            consumer_group="my-group",
        )

        instruments.stream_lag.record.assert_called_once()
        attrs = instruments.stream_lag.record.call_args[1]["attributes"]
        # Stream name should NOT be in attributes when hidden
        assert REDIS_CLIENT_STREAM_NAME not in attrs

    async def test_record_streaming_lag_from_response_empty_response(
        self, setup_async_recorder
    ):
        """Test that empty response is handled gracefully."""
        instruments = setup_async_recorder

        await recorder.record_streaming_lag_from_response(
            response=None,
            consumer_group="my-group",
        )

        # Should not record anything for empty response
        instruments.stream_lag.record.assert_not_called()

    async def test_record_streaming_lag_from_response_bytes_message_id(
        self, setup_async_recorder
    ):
        """Test that bytes message IDs are handled correctly."""
        instruments = setup_async_recorder

        import time

        current_time_ms = int(time.time() * 1000)
        message_id = f"{current_time_ms}-0".encode()  # bytes message ID

        response = [
            [
                b"test-stream",
                [
                    (message_id, {"field": "value"}),
                ],
            ]
        ]

        await recorder.record_streaming_lag_from_response(
            response=response,
            consumer_group="my-group",
        )

        instruments.stream_lag.record.assert_called_once()
        # Should not raise - bytes message ID should be handled


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestRecorderDisabled:
    """Tests for recorder behavior when observability is disabled."""

    async def test_record_operation_duration_when_disabled(self):
        """Test that recording does nothing when collector is None."""
        recorder.reset_collector()

        with (
            patch.object(recorder, "_async_metrics_collector", None),
            patch.object(recorder, "_async_collector_resolved", True),
        ):
            # Should not raise any exception
            await recorder.record_operation_duration(
                command_name="SET",
                duration_seconds=0.005,
                server_address="localhost",
                server_port=6379,
            )

        recorder.reset_collector()

    async def test_is_enabled_returns_false_when_disabled(self):
        """Test is_enabled returns False when collector is None."""
        recorder.reset_collector()

        with (
            patch.object(recorder, "_async_metrics_collector", None),
            patch.object(recorder, "_async_collector_resolved", True),
        ):
            assert await recorder.is_enabled() is False

        recorder.reset_collector()

    async def test_all_record_functions_safe_when_disabled(self):
        """Test that all record functions are safe to call when disabled."""
        recorder.reset_collector()

        with (
            patch.object(recorder, "_async_metrics_collector", None),
            patch.object(recorder, "_async_collector_resolved", True),
        ):
            # None of these should raise
            mock_pool = MagicMock()
            mock_pool.pool_name = "test_pool"

            await recorder.record_connection_create_time(mock_pool, 0.1)
            await recorder.record_connection_timeout("pool")
            await recorder.record_connection_wait_time("pool", 0.1)
            await recorder.record_connection_closed()
            await recorder.record_connection_relaxed_timeout("pool", "MOVING", True)
            await recorder.record_connection_handoff("pool")
            await recorder.record_error_count(
                "host", 6379, "127.0.0.1", 6379, Exception(), 0
            )
            await recorder.record_maint_notification_count(
                "host", 6379, "127.0.0.1", 6379, "MOVING"
            )
            await recorder.record_pubsub_message(PubSubDirection.PUBLISH)
            await recorder.record_streaming_lag(0.1, "stream", "group")

        recorder.reset_collector()


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


async def _record_with_the_real_resolver(hook):
    """One recording, with ``hook`` called during the meter read."""
    manager = _ProviderManager(hook)
    with (
        patch.object(
            recorder, "get_observability_instance", lambda: _Observability(manager)
        ),
        patch.object(recorder, "RedisMetricsCollector", _StandInCollector),
    ):
        recorder.reset_collector()
        await recorder.record_operation_duration("GET", 0.001, "localhost", 6379)


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestCollectorResolution:
    """The collector lookup runs once per observability configuration.

    The recording helpers run on every command, so a miss - observability is
    disabled - is cached as well. ObservabilityInstance.init() and
    reset_collector() drop that cache so a later enable still collects.
    """

    async def test_disabled_path_resolves_the_collector_once(self, monkeypatch):
        recorder.reset_collector()
        lookups = []
        original = recorder.get_observability_instance

        def counting(*args, **kwargs):
            lookups.append(1)
            return original(*args, **kwargs)

        monkeypatch.setattr(recorder, "get_observability_instance", counting)

        for _ in range(5):
            await recorder.record_operation_duration("GET", 0.001, "localhost", 6379)
            await recorder.record_connection_count("pool", ConnectionState.IDLE, 1)

        assert len(lookups) == 1
        recorder.reset_collector()

    async def test_reset_collector_resolves_again(self, monkeypatch):
        recorder.reset_collector()
        lookups = []
        original = recorder.get_observability_instance

        def counting(*args, **kwargs):
            lookups.append(1)
            return original(*args, **kwargs)

        monkeypatch.setattr(recorder, "get_observability_instance", counting)

        await recorder.record_operation_duration("GET", 0.001, "localhost", 6379)
        assert len(lookups) == 1

        recorder.reset_collector()
        await recorder.record_operation_duration("GET", 0.001, "localhost", 6379)
        assert len(lookups) == 2
        recorder.reset_collector()

    async def test_init_drops_a_cached_miss(self, monkeypatch):
        """init() must invalidate the recorder's cached collector.

        Both recorders cache the resolved collector, including the answer
        "observability is disabled", so that a disabled recording stays cheap.
        Once cached, that answer is only ever replaced by dropping the cache,
        so if init() did not drop it a recorder that ran before init() would
        stay silent for the rest of the process.
        """
        recorder.reset_collector()

        # Prime both caches the way a recording call before init() would: a
        # miss is cached by setting the "resolved" flag with no collector.
        recorder._async_metrics_collector = None
        recorder._async_collector_resolved = True

        # A fresh instance, not the global singleton: initializing the singleton
        # here would leave observability enabled for every later async test.
        ObservabilityInstance().init(OTelConfig())

        assert recorder._async_collector_resolved is False, (
            "init() must drop the recorder's cached miss, or the recorder "
            "stays silent after observability is enabled"
        )
        assert recorder._async_metrics_collector is None
        recorder.reset_collector()

    async def test_resolution_spanning_a_reset_does_not_cache_its_answer(self):
        """A reset during the resolution must leave the cache unresolved.

        The async twin of the sync test: this drives the real resolver, with the
        reset injected during the provider-manager read. The resolution starts
        before the reset and finishes after it, so the collector it builds
        belongs to the configuration the reset replaced. Publishing it would pin
        a stale value into the cache; leaving the flag set would be worse,
        because the next caller would read a cached miss and metrics would stay
        off until the next init.
        """
        recorder.reset_collector()
        resets = []

        def inject_reset():
            if not resets:
                resets.append(1)
                recorder.reset_collector()

        await _record_with_the_real_resolver(inject_reset)

        assert resets == [1], "the reset must land inside the resolution"
        assert recorder._async_metrics_collector is None, (
            "the collector was built against the configuration the reset "
            "replaced, so it must not be published as the cache"
        )
        assert recorder._async_collector_resolved is False, (
            "the cache must be left unresolved; a cached miss here is what "
            "silences metrics until the next init"
        )
        recorder.reset_collector()

    async def test_resolution_without_a_reset_does_cache_its_answer(self):
        """The control: with no reset the resolution publishes normally.

        Without this, the test above would also pass if the resolver simply
        never cached anything.
        """
        recorder.reset_collector()
        await _record_with_the_real_resolver(lambda: None)

        assert recorder._async_metrics_collector is not None, (
            "an uninterrupted resolution must cache the collector it built"
        )
        assert recorder._async_collector_resolved is True

        recorder.reset_collector()


@pytest.mark.fixed_client
@pytest.mark.asyncio
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

        with patch.object(recorder, "_async_metrics_collector", collector):
            # Reset mock call counts to avoid pollution from external code
            # paths (e.g., connection pool operations in other tests) that may
            # have called record_connection_count on our mock collector.
            mock_up_down_counter.reset_mock()
            yield mock_meter_with_counter

        recorder.reset_collector()

    async def test_record_connection_count_increment(
        self, setup_connection_count_recorder, mock_up_down_counter
    ):
        """Test recording connection count increment."""
        await recorder.record_connection_count(
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

    async def test_record_connection_count_decrement(
        self, setup_connection_count_recorder, mock_up_down_counter
    ):
        """Test recording connection count decrement."""
        await recorder.record_connection_count(
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

    async def test_record_connection_count_batch_decrement(
        self, setup_connection_count_recorder, mock_up_down_counter
    ):
        """Test recording batch connection count decrement (e.g., pool disconnect)."""
        await recorder.record_connection_count(
            pool_name="localhost:6379_abc123",
            connection_state=ConnectionState.IDLE,
            counter=-5,
        )

        calls = self._calls_for(mock_up_down_counter, "localhost:6379_abc123")
        assert len(calls) == 1
        assert calls[0][0][0] == -5
        assert calls[0][1]["attributes"][DB_CLIENT_CONNECTION_STATE] == "idle"

    async def test_record_connection_count_lifecycle_scenario(
        self, setup_connection_count_recorder, mock_up_down_counter
    ):
        """Test realistic lifecycle: create connection, acquire, release, re-acquire, disconnect."""
        # 1. New connection created (goes to IDLE)
        await recorder.record_connection_count(
            pool_name="pool1",
            connection_state=ConnectionState.IDLE,
            counter=1,
        )

        # 2. Connection acquired from pool (transition: IDLE -> USED)
        await recorder.record_connection_count(
            pool_name="pool1",
            connection_state=ConnectionState.IDLE,
            counter=-1,
        )
        await recorder.record_connection_count(
            pool_name="pool1",
            connection_state=ConnectionState.USED,
            counter=1,
        )

        # 3. Connection released to pool (transition: USED -> IDLE)
        await recorder.record_connection_count(
            pool_name="pool1",
            connection_state=ConnectionState.USED,
            counter=-1,
        )
        await recorder.record_connection_count(
            pool_name="pool1",
            connection_state=ConnectionState.IDLE,
            counter=1,
        )

        # 4. Pool disconnect (IDLE -> destroyed)
        await recorder.record_connection_count(
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
