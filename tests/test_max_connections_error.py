import pytest
import redis
from unittest import mock
from redis._parsers import BaseParser
from redis.connection import ConnectionInterface


class DummyConnection(ConnectionInterface):
    """A dummy connection class for testing that doesn't actually connect to Redis"""

    def __init__(self, *args, **kwargs):
        self.connected = False

    @property
    def is_connected(self) -> bool:
        return self.connected

    def connect(self):
        self.connected = True

    def disconnect(self):
        self.connected = False

    def register_connect_callback(self, callback):
        pass

    def deregister_connect_callback(self, callback):
        pass

    def set_parser(self, parser_class):
        pass

    def get_protocol(self):
        return 2

    def on_connect(self):
        pass

    def check_health(self):
        return True

    def send_packed_command(self, command, check_health=True):
        pass

    def send_command(self, *args, **kwargs):
        pass

    def can_read(self, timeout: float = 0) -> bool:
        return False

    def read_response(self, disable_decoding=False, **kwargs):
        return "PONG"


@pytest.mark.fixed_client
def test_max_connections_error_inheritance():
    """Test that MaxConnectionsError is a subclass of ConnectionError"""
    assert issubclass(redis.MaxConnectionsError, redis.ConnectionError)


@pytest.mark.fixed_client
def test_connection_pool_raises_max_connections_error():
    """Test that ConnectionPool raises MaxConnectionsError and not ConnectionError"""
    # Use a dummy connection class that doesn't try to connect to a real Redis server
    pool = redis.ConnectionPool(max_connections=1, connection_class=DummyConnection)
    pool.get_connection()

    with pytest.raises(redis.MaxConnectionsError):
        pool.get_connection()


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(redis, "RedisCluster"), reason="RedisCluster not available"
)
def test_cluster_handles_max_connections_error():
    """
    Test that RedisCluster doesn't reinitialize when MaxConnectionsError is raised
    """
    # Create a more complete mock cluster
    cluster = mock.MagicMock(spec=redis.RedisCluster)
    cluster.cluster_response_callbacks = {}
    cluster.RedisClusterRequestTTL = 3  # Set the TTL to avoid infinite loops
    cluster.nodes_manager = mock.MagicMock()
    node = mock.MagicMock()

    # Mock get_redis_connection to return a mock Redis client
    redis_conn = mock.MagicMock()
    cluster.get_redis_connection.return_value = redis_conn

    # Setup get_connection to be called and return a connection that will raise
    connection = mock.MagicMock()

    # Patch the get_connection function in the cluster module
    with mock.patch("redis.cluster.get_connection", return_value=connection):
        # Test MaxConnectionsError
        connection.send_command.side_effect = redis.MaxConnectionsError(
            "Too many connections"
        )

        # Call the method and check that the exception is raised
        with pytest.raises(redis.MaxConnectionsError):
            redis.RedisCluster._execute_command(cluster, node, "GET", "key")

        # Verify no topology refresh was run or asked for
        cluster.nodes_manager.initialize.assert_not_called()
        cluster.nodes_manager._request_refresh.assert_not_called()

        # Now test with regular ConnectionError to ensure it DOES ask for a
        # refresh; it is run by the next attempt, of any command, once due
        connection.send_command.side_effect = redis.ConnectionError("Connection lost")

        with pytest.raises(redis.ConnectionError):
            redis.RedisCluster._execute_command(cluster, node, "GET", "key")

        cluster.nodes_manager._request_refresh.assert_called_once()


def test_max_clients_reached_error_inheritance():
    """MaxClientsReachedError stays a ConnectionError for existing handlers"""
    assert issubclass(redis.MaxClientsReachedError, redis.ConnectionError)


def test_max_clients_reply_maps_to_max_clients_reached_error():
    """The server's maxclients refusal is told apart from other errors"""
    exc = BaseParser.parse_error("ERR max number of clients reached")
    assert type(exc) is redis.MaxClientsReachedError


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "error",
    [
        redis.MaxClientsReachedError("max number of clients reached"),
        redis.AuthenticationError("invalid password"),
    ],
    ids=["max_clients_reached", "authentication"],
)
def test_cluster_does_not_treat_capacity_or_auth_errors_as_node_failure(error):
    """
    A maxclients refusal or an authentication error is not a node failure:
    the node's connections are not recycled and the topology is not refreshed.
    """
    cluster = mock.MagicMock(spec=redis.RedisCluster)
    cluster.cluster_response_callbacks = {}
    cluster.RedisClusterRequestTTL = 3
    cluster.nodes_manager = mock.MagicMock()
    node = mock.MagicMock()
    redis_conn = mock.MagicMock()
    cluster.get_redis_connection.return_value = redis_conn
    node.redis_connection = redis_conn

    # Raised while opening the connection, as the server sends the refusal
    # right after accepting it
    with mock.patch("redis.cluster.get_connection", side_effect=error):
        with pytest.raises(type(error)):
            redis.RedisCluster._execute_command(cluster, node, "GET", "key")

    cluster.nodes_manager.initialize.assert_not_called()
    cluster.nodes_manager._request_refresh.assert_not_called()
    cluster.nodes_manager.move_node_to_end_of_cached_nodes.assert_not_called()
    redis_conn.connection_pool.update_active_connections_for_reconnect.assert_not_called()
    redis_conn.connection_pool.disconnect_free_connections.assert_not_called()


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "error",
    [redis.ConnectionError("connection lost"), redis.TimeoutError("timed out")],
    ids=["connection_error", "timeout"],
)
def test_cluster_node_error_closes_only_the_failed_connection(error):
    """
    A connection error or a timeout closes the connection it happened on and
    asks for a topology refresh, but keeps the node's other connections: one
    failed command does not mean they are broken.
    """
    cluster = mock.MagicMock(spec=redis.RedisCluster)
    cluster.cluster_response_callbacks = {}
    cluster.RedisClusterRequestTTL = 3
    cluster.nodes_manager = mock.MagicMock()
    node = mock.MagicMock()
    redis_conn = mock.MagicMock()
    cluster.get_redis_connection.return_value = redis_conn
    node.redis_connection = redis_conn
    connection = mock.MagicMock()
    connection.send_command.side_effect = error

    with mock.patch("redis.cluster.get_connection", return_value=connection):
        with pytest.raises(type(error)):
            redis.RedisCluster._execute_command(cluster, node, "GET", "key")

    connection.disconnect.assert_called_once()
    redis_conn.connection_pool.update_active_connections_for_reconnect.assert_not_called()
    redis_conn.connection_pool.disconnect_free_connections.assert_not_called()
    cluster.nodes_manager._request_refresh.assert_called_once()
