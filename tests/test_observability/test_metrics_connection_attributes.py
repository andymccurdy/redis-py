"""
Unit tests for metrics recording with connections that don't have host/port attributes.

These tests verify that the changes to use getattr() for accessing host and port
attributes work correctly with connections that don't have these attributes.
"""

import pytest
import os
from unittest.mock import MagicMock, patch
from redis.connection import ConnectionInterface, ConnectionPool
from redis.observability import recorder
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector
from redis import Redis
from redis.retry import Retry
from redis.backoff import NoBackoff


class MockConnectionWithoutHostPort(ConnectionInterface):
    """
    A mock connection class that implements ConnectionInterface but doesn't have
    host and port attributes. This simulates connections like UnixDomainSocketConnection
    or other custom connection types.
    """

    def __init__(self, db=0, **kwargs):
        self.db = db
        self._sock = None
        self.kwargs = kwargs
        # Add required attributes that connections need
        self.pid = os.getpid()
        self.retry = Retry(NoBackoff(), 0)  # No retries for testing
        self.encoder = None
        self.client_name = None

    def repr_pieces(self):
        return [("db", self.db)]

    def register_connect_callback(self, callback):
        pass

    def deregister_connect_callback(self, callback):
        pass

    def set_parser(self, parser_class):
        pass

    def get_protocol(self):
        return 2

    def connect(self):
        pass

    def on_connect(self):
        pass

    def disconnect(self, *args, **kwargs):
        pass

    def check_health(self):
        return True

    def send_packed_command(self, command, check_health=True):
        pass

    def send_command(self, *args, **kwargs):
        pass

    def can_read(self, timeout: float = 0) -> bool:
        return False

    def read_response(
        self, disable_decoding=False, *, disconnect_on_error=True, push_request=False
    ):
        return "OK"

    def pack_command(self, *args):
        return b""

    def pack_commands(self, commands):
        return b""

    @property
    def handshake_metadata(self):
        return {}

    def set_re_auth_token(self, token):
        pass

    def re_auth(self):
        pass

    def mark_for_reconnect(self):
        pass

    def should_reconnect(self):
        return False

    def reset_should_reconnect(self):
        pass

    @property
    def is_connected(self) -> bool:
        return self._sock is not None


@pytest.mark.fixed_client
class TestConnectionAttributesWithoutHostPort:
    """Tests for metrics recording with connections lacking host/port attributes."""

    @pytest.fixture
    def mock_meter(self):
        """Create a mock meter for testing."""
        return MagicMock()

    @pytest.fixture
    def setup_client_and_pool(self, mock_meter):
        """Setup common test infrastructure: pool, client, collector."""
        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND, MetricGroup.PUBSUB])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Create a ConnectionPool with MockConnectionWithoutHostPort as connection_class
        # Similar to how UnixDomainSocketConnection is used
        pool = ConnectionPool(
            connection_class=MockConnectionWithoutHostPort,
            db=0,
        )

        with patch.object(recorder, "_metrics_collector", collector):
            client = Redis(connection_pool=pool)
            yield client, pool

        # Cleanup
        pool.disconnect()

    def test_client_execute_command_with_connection_without_host_port(
        self, setup_client_and_pool
    ):
        """Test Redis client execute_command with connection without host/port."""
        client, pool = setup_client_and_pool

        # Get a connection from the pool
        conn = pool.get_connection()

        try:
            # Mock the connection's methods to simulate a successful command
            with patch.object(conn, "read_response", return_value=b"OK"):
                with patch.object(conn, "send_packed_command"):
                    # This should not raise an AttributeError even though connection has no host/port
                    # The getattr() calls in client.py should handle missing attributes gracefully
                    client.execute_command("PING")
        finally:
            pool.release(conn)

    def test_pubsub_execute_command_with_connection_without_host_port(
        self, setup_client_and_pool
    ):
        """Test PubSub execute_command with connection without host/port."""
        client, pool = setup_client_and_pool
        pubsub = client.pubsub()

        # Get a connection from the pool
        conn = pool.get_connection()
        pubsub.connection = conn

        try:
            # Mock the connection's methods to simulate a successful subscribe
            with patch.object(
                conn, "read_response", return_value=[b"subscribe", b"test", 1]
            ):
                with patch.object(conn, "send_command"):
                    # This should not raise an AttributeError even though connection has no host/port
                    pubsub.execute_command("SUBSCRIBE", "test")
        finally:
            pool.release(conn)
            pubsub.close()

    def test_pipeline_execute_with_connection_without_host_port(
        self, setup_client_and_pool
    ):
        """Test Pipeline execute with connection without host/port."""
        client, pool = setup_client_and_pool
        pipe = client.pipeline()

        # Get a connection from the pool
        conn = pool.get_connection()

        try:
            # Add some commands to the pipeline
            pipe.set("key", "value")
            pipe.get("key")

            # Mock the connection's methods to simulate successful pipeline execution
            # Use a callable to avoid StopIteration when the mock is called multiple times
            call_count = [0]

            def mock_read_response(*args, **kwargs):
                call_count[0] += 1
                # First call is for MULTI, second is for EXEC which returns results
                if call_count[0] == 1:
                    return b"OK"  # MULTI response
                else:
                    return [b"OK", b"value"]  # EXEC response with command results

            with patch.object(conn, "read_response", side_effect=mock_read_response):
                with patch.object(conn, "send_packed_command"):
                    with patch.object(pool, "get_connection", return_value=conn):
                        # This should not raise an AttributeError even though connection has no host/port
                        pipe.execute()
        finally:
            pool.release(conn)
