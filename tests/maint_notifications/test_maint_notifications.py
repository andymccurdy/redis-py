import threading
from unittest.mock import Mock, call, patch, MagicMock
import pytest

from redis._parsers.base import MaintenanceNotificationsParser
from redis.connection import (
    CacheProxyConnection,
    Connection,
    ConnectionInterface,
    ConnectionPool,
    MaintNotificationsAbstractConnection,
)
from redis.event import (
    EventDispatcher,
    EventListenerInterface,
    MaintenanceCompletedEvent,
    MaintenanceStartedEvent,
)

from redis.maint_notifications import (
    MaintenanceNotification,
    NodeMovingNotification,
    NodeMigratingNotification,
    NodeMigratedNotification,
    NodeFailingOverNotification,
    NodeFailedOverNotification,
    OSSNodeMigratingNotification,
    OSSNodeMigratedNotification,
    MaintNotificationsConfig,
    MaintNotificationsPoolHandler,
    MaintNotificationsConnectionHandler,
    OSSMaintNotificationsHandler,
    MaintenanceState,
    EndpointType,
)


@pytest.mark.fixed_client
class TestMaintenanceNotification:
    """Test the base MaintenanceNotification class functionality through concrete subclasses."""

    def test_abstract_class_cannot_be_instantiated(self):
        """Test that MaintenanceNotification cannot be instantiated directly."""
        with patch("time.monotonic", return_value=1000):
            with pytest.raises(TypeError):
                MaintenanceNotification(id=1, ttl=10)  # type: ignore

    def test_init_through_subclass(self):
        """Test MaintenanceNotification initialization through concrete subclass."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeMovingNotification(
                id=1, new_node_host="localhost", new_node_port=6379, ttl=10
            )
            assert notification.id == 1
            assert notification.ttl == 10
            assert notification.creation_time == 1000
            assert notification.expire_at == 1010

    @pytest.mark.parametrize(
        ("current_time", "expected_expired_state"),
        [
            (1005, False),
            (1015, True),
        ],
    )
    def test_is_expired(self, current_time, expected_expired_state):
        """Test is_expired returns False for non-expired notification."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeMovingNotification(
                id=1, new_node_host="localhost", new_node_port=6379, ttl=10
            )

        with patch("time.monotonic", return_value=current_time):
            assert notification.is_expired() == expected_expired_state

    def test_is_expired_exact_boundary(self):
        """Test is_expired at exact expiration boundary."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeMovingNotification(
                id=1, new_node_host="localhost", new_node_port=6379, ttl=10
            )

        with patch("time.monotonic", return_value=1010):  # Exactly at expiration
            assert not notification.is_expired()

        with patch("time.monotonic", return_value=1011):  # 1 second past expiration
            assert notification.is_expired()


@pytest.mark.fixed_client
class TestNodeMovingNotification:
    """Test the NodeMovingNotification class."""

    def test_init(self):
        """Test NodeMovingNotification initialization."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeMovingNotification(
                id=1, new_node_host="localhost", new_node_port=6379, ttl=10
            )
            assert notification.id == 1
            assert notification.new_node_host == "localhost"
            assert notification.new_node_port == 6379
            assert notification.ttl == 10
            assert notification.creation_time == 1000

    def test_repr(self):
        """Test NodeMovingNotification string representation."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeMovingNotification(
                id=1, new_node_host="localhost", new_node_port=6379, ttl=10
            )

        with patch("time.monotonic", return_value=1005):  # 5 seconds later
            repr_str = repr(notification)
            assert "NodeMovingNotification" in repr_str
            assert "id=1" in repr_str
            assert "new_node_host='localhost'" in repr_str
            assert "new_node_port=6379" in repr_str
            assert "ttl=10" in repr_str
            assert "remaining=5.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_none_id_none_port(self):
        """Test equality for notifications with same id and host and port - None."""
        notification1 = NodeMovingNotification(
            id=1, new_node_host=None, new_node_port=None, ttl=10
        )
        notification2 = NodeMovingNotification(
            id=1, new_node_host=None, new_node_port=None, ttl=20
        )  # Different TTL
        assert notification1 == notification2

    def test_equality_same_id_host_port(self):
        """Test equality for notifications with same id, host, and port."""
        notification1 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        notification2 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=20
        )  # Different TTL
        assert notification1 == notification2

    def test_equality_same_id_different_host(self):
        """Test inequality for notifications with same id but different host."""
        notification1 = NodeMovingNotification(
            id=1, new_node_host="host1", new_node_port=6379, ttl=10
        )
        notification2 = NodeMovingNotification(
            id=1, new_node_host="host2", new_node_port=6379, ttl=10
        )
        assert notification1 != notification2

    def test_equality_same_id_different_port(self):
        """Test inequality for notifications with same id but different port."""
        notification1 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        notification2 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6380, ttl=10
        )
        assert notification1 != notification2

    def test_equality_different_id(self):
        """Test inequality for notifications with different id."""
        notification1 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        notification2 = NodeMovingNotification(
            id=2, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        assert notification1 != notification2

    def test_equality_different_type(self):
        """Test inequality for notifications of different types."""
        notification1 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        notification2 = NodeMigratingNotification(id=1, ttl=10)
        assert notification1 != notification2

    def test_hash_same_id_host_port(self):
        """Test hash consistency for notifications with same id, host, and port."""
        notification1 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        notification2 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=20
        )  # Different TTL
        assert hash(notification1) == hash(notification2)

    def test_hash_different_host(self):
        """Test hash difference for notifications with different host."""
        notification1 = NodeMovingNotification(
            id=1, new_node_host="host1", new_node_port=6379, ttl=10
        )
        notification2 = NodeMovingNotification(
            id=1, new_node_host="host2", new_node_port=6379, ttl=10
        )
        assert hash(notification1) != hash(notification2)

    def test_hash_different_port(self):
        """Test hash difference for notifications with different port."""
        notification1 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        notification2 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6380, ttl=10
        )
        assert hash(notification1) != hash(notification2)

    def test_hash_different_id(self):
        """Test hash difference for notifications with different id."""
        notification1 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        notification2 = NodeMovingNotification(
            id=2, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        assert hash(notification1) != hash(notification2)

    def test_set_functionality(self):
        """Test that notifications can be used in sets correctly."""
        notification1 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        notification2 = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=20
        )  # Same id, host, port - should be considered the same
        notification3 = NodeMovingNotification(
            id=1, new_node_host="host2", new_node_port=6380, ttl=10
        )  # Same id but different host/port - should be different
        notification4 = NodeMovingNotification(
            id=2, new_node_host="localhost", new_node_port=6379, ttl=10
        )  # Different id - should be different

        notification_set = {notification1, notification2, notification3, notification4}
        assert (
            len(notification_set) == 3
        )  # notification1 and notification2 should be considered the same


@pytest.mark.fixed_client
class TestNodeMigratingNotification:
    """Test the NodeMigratingNotification class."""

    def test_init(self):
        """Test NodeMigratingNotification initialization."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeMigratingNotification(id=1, ttl=5)
            assert notification.id == 1
            assert notification.ttl == 5
            assert notification.creation_time == 1000

    def test_repr(self):
        """Test NodeMigratingNotification string representation."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeMigratingNotification(id=1, ttl=5)

        with patch("time.monotonic", return_value=1002):  # 2 seconds later
            repr_str = repr(notification)
            assert "NodeMigratingNotification" in repr_str
            assert "id=1" in repr_str
            assert "ttl=5" in repr_str
            assert "remaining=3.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_and_hash(self):
        """Test equality and hash for NodeMigratingNotification."""
        notification1 = NodeMigratingNotification(id=1, ttl=5)
        notification2 = NodeMigratingNotification(
            id=1, ttl=10
        )  # Same id, different ttl
        notification3 = NodeMigratingNotification(id=2, ttl=5)  # Different id

        assert notification1 == notification2
        assert notification1 != notification3
        assert hash(notification1) == hash(notification2)
        assert hash(notification1) != hash(notification3)


@pytest.mark.fixed_client
class TestNodeMigratedNotification:
    """Test the NodeMigratedNotification class."""

    def test_init(self):
        """Test NodeMigratedNotification initialization."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeMigratedNotification(id=1)
            assert notification.id == 1
            assert notification.ttl == NodeMigratedNotification.DEFAULT_TTL
            assert notification.creation_time == 1000

    def test_default_ttl(self):
        """Test that DEFAULT_TTL is used correctly."""
        assert NodeMigratedNotification.DEFAULT_TTL == 5
        notification = NodeMigratedNotification(id=1)
        assert notification.ttl == 5

    def test_repr(self):
        """Test NodeMigratedNotification string representation."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeMigratedNotification(id=1)

        with patch("time.monotonic", return_value=1001):  # 1 second later
            repr_str = repr(notification)
            assert "NodeMigratedNotification" in repr_str
            assert "id=1" in repr_str
            assert "ttl=5" in repr_str
            assert "remaining=4.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_and_hash(self):
        """Test equality and hash for NodeMigratedNotification."""
        notification1 = NodeMigratedNotification(id=1)
        notification2 = NodeMigratedNotification(id=1)  # Same id
        notification3 = NodeMigratedNotification(id=2)  # Different id

        assert notification1 == notification2
        assert notification1 != notification3
        assert hash(notification1) == hash(notification2)
        assert hash(notification1) != hash(notification3)


@pytest.mark.fixed_client
class TestNodeFailingOverNotification:
    """Test the NodeFailingOverNotification class."""

    def test_init(self):
        """Test NodeFailingOverNotification initialization."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeFailingOverNotification(id=1, ttl=5)
            assert notification.id == 1
            assert notification.ttl == 5
            assert notification.creation_time == 1000

    def test_repr(self):
        """Test NodeFailingOverNotification string representation."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeFailingOverNotification(id=1, ttl=5)

        with patch("time.monotonic", return_value=1002):  # 2 seconds later
            repr_str = repr(notification)
            assert "NodeFailingOverNotification" in repr_str
            assert "id=1" in repr_str
            assert "ttl=5" in repr_str
            assert "remaining=3.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_and_hash(self):
        """Test equality and hash for NodeFailingOverNotification."""
        notification1 = NodeFailingOverNotification(id=1, ttl=5)
        notification2 = NodeFailingOverNotification(
            id=1, ttl=10
        )  # Same id, different ttl
        notification3 = NodeFailingOverNotification(id=2, ttl=5)  # Different id

        assert notification1 == notification2
        assert notification1 != notification3
        assert hash(notification1) == hash(notification2)
        assert hash(notification1) != hash(notification3)


@pytest.mark.fixed_client
class TestNodeFailedOverNotification:
    """Test the NodeFailedOverNotification class."""

    def test_init(self):
        """Test NodeFailedOverNotification initialization."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeFailedOverNotification(id=1)
            assert notification.id == 1
            assert notification.ttl == NodeFailedOverNotification.DEFAULT_TTL
            assert notification.creation_time == 1000

    def test_default_ttl(self):
        """Test that DEFAULT_TTL is used correctly."""
        assert NodeFailedOverNotification.DEFAULT_TTL == 5
        notification = NodeFailedOverNotification(id=1)
        assert notification.ttl == 5

    def test_repr(self):
        """Test NodeFailedOverNotification string representation."""
        with patch("time.monotonic", return_value=1000):
            notification = NodeFailedOverNotification(id=1)

        with patch("time.monotonic", return_value=1001):  # 1 second later
            repr_str = repr(notification)
            assert "NodeFailedOverNotification" in repr_str
            assert "id=1" in repr_str
            assert "ttl=5" in repr_str
            assert "remaining=4.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_and_hash(self):
        """Test equality and hash for NodeFailedOverNotification."""
        notification1 = NodeFailedOverNotification(id=1)
        notification2 = NodeFailedOverNotification(id=1)  # Same id
        notification3 = NodeFailedOverNotification(id=2)  # Different id

        assert notification1 == notification2
        assert notification1 != notification3
        assert hash(notification1) == hash(notification2)
        assert hash(notification1) != hash(notification3)


@pytest.mark.fixed_client
class TestOSSNodeMigratingNotification:
    """Test the OSSNodeMigratingNotification class."""

    def test_init_with_defaults(self):
        """Test OSSNodeMigratingNotification initialization with default values."""
        with patch("time.monotonic", return_value=1000):
            notification = OSSNodeMigratingNotification(id=1)
            assert notification.id == 1
            assert notification.ttl == OSSNodeMigratingNotification.DEFAULT_TTL
            assert notification.creation_time == 1000
            assert notification.slots is None

    def test_init_with_all_parameters(self):
        """Test OSSNodeMigratingNotification initialization with all parameters."""
        with patch("time.monotonic", return_value=1000):
            slots = "1,2,3,4,5"
            notification = OSSNodeMigratingNotification(
                id=1,
                slots=slots,
            )
            assert notification.id == 1
            assert notification.ttl == OSSNodeMigratingNotification.DEFAULT_TTL
            assert notification.creation_time == 1000
            assert notification.slots == slots

    def test_default_ttl(self):
        """Test that DEFAULT_TTL is used correctly."""
        assert OSSNodeMigratingNotification.DEFAULT_TTL == 30
        notification = OSSNodeMigratingNotification(id=1)
        assert notification.ttl == 30

    def test_repr(self):
        """Test OSSNodeMigratingNotification string representation."""
        with patch("time.monotonic", return_value=1000):
            notification = OSSNodeMigratingNotification(
                id=1,
                slots="1,2,3",
            )

        with patch("time.monotonic", return_value=1005):  # 5 seconds later
            repr_str = repr(notification)
            assert "OSSNodeMigratingNotification" in repr_str
            assert "id=1" in repr_str
            assert "ttl=30" in repr_str
            assert "remaining=25.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_same_id_and_type(self):
        """Test equality for notifications with same id and type."""
        notification1 = OSSNodeMigratingNotification(
            id=1,
            slots="1,2,3",
        )
        notification2 = OSSNodeMigratingNotification(
            id=1,
            slots="4,5,6",
        )
        # Should be equal because id and type are the same
        assert notification1 == notification2

    def test_equality_different_id(self):
        """Test inequality for notifications with different id."""
        notification1 = OSSNodeMigratingNotification(id=1)
        notification2 = OSSNodeMigratingNotification(id=2)
        assert notification1 != notification2

    def test_equality_different_type(self):
        """Test inequality for notifications of different types."""
        notification1 = OSSNodeMigratingNotification(id=1)
        notification2 = NodeMigratingNotification(id=1, ttl=30)
        assert notification1 != notification2

    def test_hash_same_id_and_type(self):
        """Test hash for notifications with same id and type."""
        notification1 = OSSNodeMigratingNotification(
            id=1,
            slots="1,2,3",
        )
        notification2 = OSSNodeMigratingNotification(
            id=1,
            slots="4,5,6",
        )
        # Should have same hash because id and type are the same
        assert hash(notification1) == hash(notification2)

    def test_hash_different_id(self):
        """Test hash for notifications with different id."""
        notification1 = OSSNodeMigratingNotification(id=1)
        notification2 = OSSNodeMigratingNotification(id=2)
        assert hash(notification1) != hash(notification2)

    def test_in_set(self):
        """Test that notifications can be used in sets."""
        notification1 = OSSNodeMigratingNotification(id=1)
        notification2 = OSSNodeMigratingNotification(id=1)
        notification3 = OSSNodeMigratingNotification(id=2)
        notification4 = OSSNodeMigratingNotification(id=2)

        notification_set = {notification1, notification2, notification3, notification4}
        assert (
            len(notification_set) == 2
        )  # notification1 and notification2 should be the same


@pytest.mark.fixed_client
class TestOSSNodeMigratedNotification:
    """Test the OSSNodeMigratedNotification class."""

    def test_init_with_defaults(self):
        """Test OSSNodeMigratedNotification initialization with default values."""
        with patch("time.monotonic", return_value=1000):
            nodes_to_slots_mapping = {"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]}
            notification = OSSNodeMigratedNotification(
                id=1, nodes_to_slots_mapping=nodes_to_slots_mapping
            )
            assert notification.id == 1
            assert notification.ttl == OSSNodeMigratedNotification.DEFAULT_TTL
            assert notification.creation_time == 1000
            assert notification.nodes_to_slots_mapping == nodes_to_slots_mapping

    def test_init_with_all_parameters(self):
        """Test OSSNodeMigratedNotification initialization with all parameters."""
        with patch("time.monotonic", return_value=1000):
            nodes_to_slots_mapping = {
                "127.0.0.1:6379": [
                    {"127.0.0.1:6380": "1-100"},
                    {"127.0.0.1:6381": "101-200"},
                ]
            }
            notification = OSSNodeMigratedNotification(
                id=1,
                nodes_to_slots_mapping=nodes_to_slots_mapping,
            )
            assert notification.id == 1
            assert notification.ttl == OSSNodeMigratedNotification.DEFAULT_TTL
            assert notification.creation_time == 1000
            assert notification.nodes_to_slots_mapping == nodes_to_slots_mapping

    def test_default_ttl(self):
        """Test that DEFAULT_TTL is used correctly."""
        assert OSSNodeMigratedNotification.DEFAULT_TTL == 120
        notification = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
        )
        assert notification.ttl == 120

    def test_repr(self):
        """Test OSSNodeMigratedNotification string representation."""
        with patch("time.monotonic", return_value=1000):
            nodes_to_slots_mapping = {"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]}
            notification = OSSNodeMigratedNotification(
                id=1,
                nodes_to_slots_mapping=nodes_to_slots_mapping,
            )

        with patch("time.monotonic", return_value=1010):  # 10 seconds later
            repr_str = repr(notification)
            assert "OSSNodeMigratedNotification" in repr_str
            assert "id=1" in repr_str
            assert "ttl=120" in repr_str
            assert "remaining=110.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_same_id_and_type(self):
        """Test equality for notifications with same id and type."""
        notification1 = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
        )
        notification2 = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6381": "101-200"}]},
        )
        # Should be equal because id and type are the same
        assert notification1 == notification2

    def test_equality_different_id(self):
        """Test inequality for notifications with different id."""
        notification1 = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
        )
        notification2 = OSSNodeMigratedNotification(
            id=2,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
        )
        assert notification1 != notification2

    def test_equality_different_type(self):
        """Test inequality for notifications of different types."""
        notification1 = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
        )
        notification2 = NodeMigratedNotification(id=1)
        assert notification1 != notification2

    def test_hash_same_id_and_type(self):
        """Test hash for notifications with same id and type."""
        notification1 = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
        )
        notification2 = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6381": "101-200"}]},
        )
        # Should have same hash because id and type are the same
        assert hash(notification1) == hash(notification2)

    def test_hash_different_id(self):
        """Test hash for notifications with different id."""
        notification1 = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
        )
        notification2 = OSSNodeMigratedNotification(
            id=2,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
        )
        assert hash(notification1) != hash(notification2)

    def test_in_set(self):
        """Test that notifications can be used in sets."""
        notification1 = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
        )
        notification2 = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
        )
        notification3 = OSSNodeMigratedNotification(
            id=2,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6381": "101-200"}]},
        )
        notification4 = OSSNodeMigratedNotification(
            id=2,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6381": "101-200"}]},
        )

        notification_set = {notification1, notification2, notification3, notification4}
        assert (
            len(notification_set) == 2
        )  # notification1 and notification2 should be the same


@pytest.mark.fixed_client
class TestOSSMaintNotificationsHandler:
    """Test address parsing in the OSS SMIGRATED handler."""

    def _make_handler(self):
        cluster_client = MagicMock()
        # Keep the affected-nodes set and the node-reconnect loop empty so the
        # test focuses purely on how src/dest addresses are parsed.
        cluster_client.nodes_manager.get_node.return_value = None
        cluster_client.nodes_manager.nodes_cache.values.return_value = []
        config = MaintNotificationsConfig(enabled=True)
        return OSSMaintNotificationsHandler(cluster_client, config), cluster_client

    def test_handle_smigrated_parses_ipv4_addresses(self):
        handler, cluster_client = self._make_handler()
        notification = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
        )

        handler.handle_oss_maintenance_completed_notification(notification)

        cluster_client.nodes_manager.get_node.assert_called_once_with(
            host="127.0.0.1", port=6379
        )
        cluster_client.nodes_manager.initialize.assert_called_once_with(
            disconnect_startup_nodes_pools=False,
            additional_startup_nodes_info=[("127.0.0.1", 6380)],
        )

    def test_handle_smigrated_parses_ipv6_addresses(self):
        handler, cluster_client = self._make_handler()
        # IPv6 literals contain multiple colons; a naive split(":") would raise
        # ValueError on the host/port unpack.
        notification = OSSNodeMigratedNotification(
            id=1,
            nodes_to_slots_mapping={
                "2001:db8::1:6379": [{"2001:db8::2:6380": "1-100"}]
            },
        )

        handler.handle_oss_maintenance_completed_notification(notification)

        cluster_client.nodes_manager.get_node.assert_called_once_with(
            host="2001:db8::1", port=6379
        )
        cluster_client.nodes_manager.initialize.assert_called_once_with(
            disconnect_startup_nodes_pools=False,
            additional_startup_nodes_info=[("2001:db8::2", 6380)],
        )


@pytest.mark.fixed_client
class TestMaintenanceNotificationsParser:
    """Test endpoint parsing in the shared MOVING push parser."""

    def test_parse_moving_msg_ipv4_endpoint(self):
        notification = MaintenanceNotificationsParser.parse_moving_msg(
            ["MOVING", 1, 10, "1.2.3.4:6379"]
        )
        assert notification.new_node_host == "1.2.3.4"
        assert notification.new_node_port == 6379

    def test_parse_moving_msg_ipv6_endpoint(self):
        # IPv6 literals contain multiple colons; a naive split(":") would raise
        # ValueError on the host/port unpack.
        notification = MaintenanceNotificationsParser.parse_moving_msg(
            ["MOVING", 1, 10, "2001:db8::1:6379"]
        )
        assert notification.new_node_host == "2001:db8::1"
        assert notification.new_node_port == 6379

    def test_parse_moving_msg_none_endpoint(self):
        notification = MaintenanceNotificationsParser.parse_moving_msg(
            ["MOVING", 1, 10, None]
        )
        assert notification.new_node_host is None
        assert notification.new_node_port is None


@pytest.mark.fixed_client
class TestMaintNotificationsConfig:
    """Test the MaintNotificationsConfig class."""

    def test_init_defaults(self):
        """Test MaintNotificationsConfig initialization with defaults."""
        config = MaintNotificationsConfig()
        assert config.enabled == "auto"
        assert config.proactive_reconnect is True
        assert config.relaxed_timeout == 10

    def test_init_custom_values(self):
        """Test MaintNotificationsConfig initialization with custom values."""
        config = MaintNotificationsConfig(
            enabled=True, proactive_reconnect=False, relaxed_timeout=30
        )
        assert config.enabled is True
        assert config.proactive_reconnect is False
        assert config.relaxed_timeout == 30

    def test_repr(self):
        """Test MaintNotificationsConfig string representation."""
        config = MaintNotificationsConfig(
            enabled=True, proactive_reconnect=False, relaxed_timeout=30
        )
        repr_str = repr(config)
        assert "MaintNotificationsConfig" in repr_str
        assert "enabled=True" in repr_str
        assert "proactive_reconnect=False" in repr_str
        assert "relaxed_timeout=30" in repr_str

    def test_is_relaxed_timeouts_enabled_true(self):
        """Test is_relaxed_timeouts_enabled returns True for positive timeout."""
        config = MaintNotificationsConfig(relaxed_timeout=20)
        assert config.is_relaxed_timeouts_enabled() is True

    def test_is_relaxed_timeouts_enabled_false(self):
        """Test is_relaxed_timeouts_enabled returns False for -1 timeout."""
        config = MaintNotificationsConfig(relaxed_timeout=-1)
        assert config.is_relaxed_timeouts_enabled() is False

    def test_is_relaxed_timeouts_enabled_zero(self):
        """Test is_relaxed_timeouts_enabled returns True for zero timeout."""
        config = MaintNotificationsConfig(relaxed_timeout=0)
        assert config.is_relaxed_timeouts_enabled() is True

    def test_is_relaxed_timeouts_enabled_none(self):
        """Test is_relaxed_timeouts_enabled returns True for None timeout."""
        config = MaintNotificationsConfig(relaxed_timeout=None)
        assert config.is_relaxed_timeouts_enabled() is True

    def test_relaxed_timeout_none_is_saved_as_none(self):
        """Test that None value for relaxed_timeout is saved as None."""
        config = MaintNotificationsConfig(relaxed_timeout=None)
        assert config.relaxed_timeout is None

    def test_event_dispatcher_defaults_to_none(self):
        """No events are dispatched unless a dispatcher is configured."""
        config = MaintNotificationsConfig()
        assert config.event_dispatcher is None
        assert "event_dispatcher=None" in repr(config)

    def test_event_dispatcher_is_saved(self):
        dispatcher = EventDispatcher()
        config = MaintNotificationsConfig(event_dispatcher=dispatcher)
        assert config.event_dispatcher is dispatcher
        assert "event_dispatcher=" in repr(config)


class _RecordingListener(EventListenerInterface):
    def __init__(self):
        self.events = []

    def listen(self, event):
        self.events.append(event)


class _RaisingListener(EventListenerInterface):
    def listen(self, event):
        raise RuntimeError("listener failed")


def _config_with_listener(listener, **kwargs):
    dispatcher = EventDispatcher(
        {
            MaintenanceStartedEvent: [listener],
            MaintenanceCompletedEvent: [listener],
        }
    )
    kwargs.setdefault("enabled", True)
    kwargs.setdefault("relaxed_timeout", 20)
    return MaintNotificationsConfig(event_dispatcher=dispatcher, **kwargs)


@pytest.mark.fixed_client
class TestMaintNotificationsConnectionHandlerEvents:
    """The connection handler reports the relaxation it applies as events."""

    def setup_method(self):
        self.mock_connection = Mock()
        self.mock_connection._sock.getsockname.return_value = ("127.0.0.1", 12345)
        self.mock_connection.maintenance_state = MaintenanceState.NONE
        self.listener = _RecordingListener()
        self.config = _config_with_listener(self.listener)
        self.handler = MaintNotificationsConnectionHandler(
            self.mock_connection, self.config
        )

    def test_start_notification_dispatches_started_event(self):
        notification = NodeMigratingNotification(id=1, ttl=5)

        self.handler.handle_maintenance_start_notification(
            MaintenanceState.MAINTENANCE, notification
        )

        assert len(self.listener.events) == 1
        event = self.listener.events[0]
        assert isinstance(event, MaintenanceStartedEvent)
        assert event.state == MaintenanceState.MAINTENANCE
        assert event.connection is self.mock_connection
        assert (
            event.connection_pool
            is self.mock_connection._maint_notifications_pool_handler.pool
        )
        assert event.notification is notification
        assert event.config is self.config

    def test_completed_notification_dispatches_completed_event(self):
        self.mock_connection.maintenance_state = MaintenanceState.MAINTENANCE
        notification = NodeMigratedNotification(id=1)

        self.handler.handle_maintenance_completed_notification(
            notification=notification
        )

        assert len(self.listener.events) == 1
        event = self.listener.events[0]
        assert isinstance(event, MaintenanceCompletedEvent)
        assert event.state == MaintenanceState.MAINTENANCE
        assert event.connection is self.mock_connection
        assert event.notification is notification
        assert event.config is self.config

    def test_completed_without_notification_dispatches_completed_event(self):
        self.mock_connection.maintenance_state = MaintenanceState.MAINTENANCE

        self.handler.handle_maintenance_completed_notification()

        assert len(self.listener.events) == 1
        assert isinstance(self.listener.events[0], MaintenanceCompletedEvent)
        assert self.listener.events[0].notification is None

    def test_completed_without_prior_start_dispatches_nothing(self):
        """The start may have been routed to another connection of the cluster."""
        self.mock_connection.maintenance_state = MaintenanceState.NONE

        self.handler.handle_maintenance_completed_notification(
            notification=NodeMigratedNotification(id=1)
        )

        assert self.listener.events == []

    def test_start_and_completed_pair_through_handle_notification(self):
        self.handler.handle_notification(NodeFailingOverNotification(id=1, ttl=5))
        self.handler.handle_notification(NodeFailedOverNotification(id=1))

        assert [type(e) for e in self.listener.events] == [
            MaintenanceStartedEvent,
            MaintenanceCompletedEvent,
        ]

    def test_no_event_when_relaxed_timeout_disabled(self):
        config = _config_with_listener(self.listener, relaxed_timeout=-1)
        handler = MaintNotificationsConnectionHandler(self.mock_connection, config)

        handler.handle_maintenance_start_notification(
            MaintenanceState.MAINTENANCE, NodeMigratingNotification(id=1, ttl=5)
        )
        handler.handle_maintenance_completed_notification()

        assert self.listener.events == []

    def test_no_event_when_connection_is_moving(self):
        self.mock_connection.maintenance_state = MaintenanceState.MOVING

        self.handler.handle_maintenance_start_notification(
            MaintenanceState.MAINTENANCE, NodeMigratingNotification(id=1, ttl=5)
        )
        self.handler.handle_maintenance_completed_notification()

        assert self.listener.events == []

    def test_no_dispatcher_is_a_noop(self):
        config = MaintNotificationsConfig(enabled=True, relaxed_timeout=20)
        handler = MaintNotificationsConnectionHandler(self.mock_connection, config)

        handler.handle_maintenance_start_notification(
            MaintenanceState.MAINTENANCE, NodeMigratingNotification(id=1, ttl=5)
        )

        assert self.mock_connection.maintenance_state == MaintenanceState.MAINTENANCE

    def test_raising_listener_does_not_prevent_state_change(self):
        config = _config_with_listener(_RaisingListener())
        handler = MaintNotificationsConnectionHandler(self.mock_connection, config)

        handler.handle_maintenance_start_notification(
            MaintenanceState.MAINTENANCE, NodeMigratingNotification(id=1, ttl=5)
        )
        assert self.mock_connection.maintenance_state == MaintenanceState.MAINTENANCE

        handler.handle_maintenance_completed_notification()
        assert self.mock_connection.maintenance_state == MaintenanceState.NONE


@pytest.mark.fixed_client
class TestMaintNotificationsPoolHandlerEvents:
    """The pool handler reports a handoff as a MOVING event pair."""

    def setup_method(self):
        self.mock_pool = Mock()
        self.mock_pool._lock = MagicMock()
        self.mock_pool._lock.__enter__.return_value = None
        self.mock_pool._lock.__exit__.return_value = None
        self.listener = _RecordingListener()
        self.config = _config_with_listener(self.listener)
        self.handler = MaintNotificationsPoolHandler(self.mock_pool, self.config)

    def _moving(self, id=1):
        return NodeMovingNotification(
            id=id, new_node_host="localhost", new_node_port=6379, ttl=10
        )

    def test_moving_dispatches_one_started_event_per_notification(self):
        notification = self._moving()

        with patch("threading.Timer"):
            self.handler.handle_node_moving_notification(notification)
            # Delivered again on another connection: already processed
            self.handler.handle_node_moving_notification(notification)

        assert len(self.listener.events) == 1
        event = self.listener.events[0]
        assert isinstance(event, MaintenanceStartedEvent)
        assert event.state == MaintenanceState.MOVING
        assert event.connection_pool is self.mock_pool
        assert event.notification is notification
        assert event.config is self.config

    def test_started_event_precedes_relaxing_the_pool(self):
        """
        Taking over a connection under maintenance completes that connection's
        pair; the handoff's pair must already be open for a listener by then.
        """
        seen = []
        self.mock_pool.update_connections_settings.side_effect = (
            lambda **kwargs: seen.extend(type(e) for e in self.listener.events)
        )

        with patch("threading.Timer"):
            self.handler.handle_node_moving_notification(self._moving())

        assert seen == [MaintenanceStartedEvent]

    def test_failed_handoff_closes_its_pair(self):
        """A pool mutation that fails leaves no relaxation to end later."""
        notification = self._moving()
        self.mock_pool.update_connections_settings.side_effect = RuntimeError("boom")

        with patch("threading.Timer") as timer, pytest.raises(RuntimeError):
            self.handler.handle_node_moving_notification(notification)

        assert [type(e) for e in self.listener.events] == [
            MaintenanceStartedEvent,
            MaintenanceCompletedEvent,
        ]
        assert self.listener.events[1].notification is notification
        timer.assert_not_called()
        assert notification not in self.handler._processed_notifications

    def test_moved_dispatches_completed_event(self):
        notification = self._moving()
        self.mock_pool.connection_kwargs = {
            "maintenance_notification_hash": hash(notification)
        }

        self.handler.handle_node_moved_notification(notification)

        assert len(self.listener.events) == 1
        event = self.listener.events[0]
        assert isinstance(event, MaintenanceCompletedEvent)
        assert event.state == MaintenanceState.MOVING
        assert event.connection_pool is self.mock_pool
        assert event.notification is notification

    def test_moved_superseded_by_newer_moving_dispatches_nothing(self):
        first = self._moving(id=1)
        second = self._moving(id=2)
        self.mock_pool.connection_kwargs = {
            "maintenance_notification_hash": hash(second)
        }

        self.handler.handle_node_moved_notification(first)

        assert self.listener.events == []
        # The connections matching the expired notification are still reverted
        self.mock_pool.update_connections_settings.assert_called_once()

    def test_no_event_when_handoff_handling_disabled(self):
        config = _config_with_listener(
            self.listener, proactive_reconnect=False, relaxed_timeout=-1
        )
        handler = MaintNotificationsPoolHandler(self.mock_pool, config)

        handler.handle_node_moving_notification(self._moving())

        assert self.listener.events == []

    def test_no_event_when_handoff_relaxes_nothing(self):
        """A handoff handled only for the proactive reconnect relaxes no timeout."""
        config = _config_with_listener(
            self.listener, proactive_reconnect=True, relaxed_timeout=-1
        )
        handler = MaintNotificationsPoolHandler(self.mock_pool, config)
        notification = self._moving()
        self.mock_pool.connection_kwargs = {
            "maintenance_notification_hash": hash(notification)
        }

        with patch("threading.Timer"):
            handler.handle_node_moving_notification(notification)
        handler.handle_node_moved_notification(notification)

        # The handoff itself is still handled
        assert self.mock_pool.update_connections_settings.call_count == 2
        assert self.listener.events == []


@pytest.mark.fixed_client
class TestCacheProxyConnectionMaintenanceEvents:
    """With client-side caching the proxy is the source of both events."""

    def test_disconnect_completes_with_the_proxy_as_source(self):
        listener = _RecordingListener()
        config = _config_with_listener(listener)
        connection = Connection(protocol=3, maint_notifications_config=config)
        proxy = CacheProxyConnection(connection, Mock(), threading.RLock())

        proxy._maint_notifications_connection_handler.handle_maintenance_start_notification(
            MaintenanceState.MAINTENANCE, NodeMigratingNotification(id=1, ttl=5)
        )
        assert proxy.maintenance_state == MaintenanceState.MAINTENANCE

        proxy.disconnect()

        assert proxy.maintenance_state == MaintenanceState.NONE
        assert [type(e) for e in listener.events] == [
            MaintenanceStartedEvent,
            MaintenanceCompletedEvent,
        ]
        assert all(event.connection is proxy for event in listener.events)


@pytest.mark.fixed_client
class TestConnectionDisconnectMaintenanceEvent:
    """Closing a connection under maintenance ends its relaxation with an event."""

    def _connection(self, listener):
        config = _config_with_listener(listener)
        connection = Connection(protocol=3, maint_notifications_config=config)
        # disconnect() is a no-op on a connection that was never connected
        connection._sock = Mock()
        return connection

    def test_disconnect_in_maintenance_dispatches_completed_event(self):
        listener = _RecordingListener()
        connection = self._connection(listener)
        connection.maintenance_state = MaintenanceState.MAINTENANCE

        connection.disconnect()

        assert connection.maintenance_state == MaintenanceState.NONE
        assert len(listener.events) == 1
        event = listener.events[0]
        assert isinstance(event, MaintenanceCompletedEvent)
        assert event.state == MaintenanceState.MAINTENANCE
        assert event.connection is connection
        assert event.notification is None

    @pytest.mark.parametrize("state", [MaintenanceState.NONE, MaintenanceState.MOVING])
    def test_disconnect_outside_maintenance_dispatches_nothing(self, state):
        listener = _RecordingListener()
        connection = self._connection(listener)
        connection.maintenance_state = state

        connection.disconnect()

        assert listener.events == []


@pytest.mark.fixed_client
class TestMaintNotificationsPoolHandler:
    """Test the MaintNotificationsPoolHandler class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_pool = Mock()
        self.mock_pool._lock = MagicMock()
        self.mock_pool._lock.__enter__.return_value = None
        self.mock_pool._lock.__exit__.return_value = None
        self.config = MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=20
        )
        self.handler = MaintNotificationsPoolHandler(self.mock_pool, self.config)

    def test_init(self):
        """Test MaintNotificationsPoolHandler initialization."""
        assert self.handler.pool == self.mock_pool
        assert self.handler.config == self.config
        assert isinstance(self.handler._processed_notifications, set)
        assert isinstance(self.handler._lock, type(threading.RLock()))

    def test_remove_expired_notifications(self):
        """Test removal of expired notifications."""
        with patch("time.monotonic", return_value=1000):
            notification1 = NodeMovingNotification(
                id=1, new_node_host="host1", new_node_port=6379, ttl=10
            )
            notification2 = NodeMovingNotification(
                id=2, new_node_host="host2", new_node_port=6380, ttl=5
            )
            self.handler._processed_notifications.add(notification1)
            self.handler._processed_notifications.add(notification2)

        # Move time forward but not enough to expire notification2 (expires at 1005)
        with patch("time.monotonic", return_value=1003):
            self.handler.remove_expired_notifications()
            assert notification1 in self.handler._processed_notifications
            assert (
                notification2 in self.handler._processed_notifications
            )  # Not expired yet

        # Move time forward to expire notification2 but not notification1
        with patch("time.monotonic", return_value=1006):
            self.handler.remove_expired_notifications()
            assert notification1 in self.handler._processed_notifications
            assert (
                notification2 not in self.handler._processed_notifications
            )  # Now expired

    def test_handle_notification_node_moving(self):
        """Test handling of NodeMovingNotification."""
        notification = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )

        with patch.object(
            self.handler, "handle_node_moving_notification"
        ) as mock_handle:
            self.handler.handle_notification(notification)
            mock_handle.assert_called_once_with(notification)

    def test_handle_notification_unknown_type(self):
        """Test handling of unknown notification type."""
        notification = NodeMigratingNotification(
            id=1, ttl=5
        )  # Not handled by pool handler

        result = self.handler.handle_notification(notification)
        assert result is None

    def test_handle_node_moving_notification_disabled_config(self):
        """Test node moving notification handling when both features are disabled."""
        config = MaintNotificationsConfig(proactive_reconnect=False, relaxed_timeout=-1)
        handler = MaintNotificationsPoolHandler(self.mock_pool, config)
        notification = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )

        result = handler.handle_node_moving_notification(notification)
        assert result is None
        assert notification not in handler._processed_notifications

    def test_handle_node_moving_notification_already_processed(self):
        """Test node moving notification handling when notification already processed."""
        notification = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        self.handler._processed_notifications.add(notification)

        result = self.handler.handle_node_moving_notification(notification)
        assert result is None

    def test_handle_node_moving_notification_success(self):
        """Test successful node moving notification handling."""
        notification = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )

        with (
            patch.object(self.handler, "_schedule") as mock_schedule,
            patch("time.monotonic", return_value=1000),
        ):
            self.handler.handle_node_moving_notification(notification)

            # Verify the cleanup was scheduled
            mock_schedule.assert_called_once_with(
                notification.ttl,
                self.handler.handle_node_moved_notification,
                notification,
            )

            # Verify notification was added to processed set
            assert notification in self.handler._processed_notifications

            # Verify pool methods were called
            self.mock_pool.update_connections_settings.assert_called_once()

    def test_handle_node_moving_notification_with_no_host_and_port(self):
        """Test successful node moving notification handling."""
        notification = NodeMovingNotification(
            id=1, new_node_host=None, new_node_port=None, ttl=2
        )

        with (
            patch.object(self.handler, "_schedule") as mock_schedule,
            patch("time.monotonic", return_value=1000),
        ):
            self.handler.handle_node_moving_notification(notification)

            # Verify the proactive reconnect and the cleanup were scheduled
            assert mock_schedule.call_args_list == [
                call(notification.ttl / 2, self.handler.run_proactive_reconnect, None),
                call(
                    notification.ttl,
                    self.handler.handle_node_moved_notification,
                    notification,
                ),
            ]

            # Verify notification was added to processed set
            assert notification in self.handler._processed_notifications

            # Verify pool methods were called
            self.mock_pool.update_connections_settings.assert_called_once()

    def test_handle_node_moved_notification(self):
        """Test handling of node moved notification (cleanup)."""
        notification = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        self.mock_pool.connection_kwargs = {"host": "localhost"}
        self.handler.handle_node_moved_notification(notification)

        # Verify cleanup methods were called
        self.mock_pool.update_connections_settings.assert_called_once()


@pytest.mark.fixed_client
class TestMaintNotificationsConnectionHandler:
    """Test the MaintNotificationsConnectionHandler class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_connection = Mock()
        # Configure _sock.getsockname() to return a proper tuple (host, port)
        self.mock_connection._sock.getsockname.return_value = ("127.0.0.1", 12345)
        self.config = MaintNotificationsConfig(enabled=True, relaxed_timeout=20)
        self.handler = MaintNotificationsConnectionHandler(
            self.mock_connection, self.config
        )

    def test_init(self):
        """Test MaintNotificationsConnectionHandler initialization."""
        assert self.handler.connection == self.mock_connection
        assert self.handler.config == self.config

    def test_handle_notification_migrating(self):
        """Test handling of NodeMigratingNotification."""
        notification = NodeMigratingNotification(id=1, ttl=5)

        with patch.object(
            self.handler, "handle_maintenance_start_notification"
        ) as mock_handle:
            self.handler.handle_notification(notification)
            mock_handle.assert_called_once_with(
                MaintenanceState.MAINTENANCE, notification
            )

    def test_handle_notification_migrated(self):
        """Test handling of NodeMigratedNotification."""
        notification = NodeMigratedNotification(id=1)

        with patch.object(
            self.handler, "handle_maintenance_completed_notification"
        ) as mock_handle:
            self.handler.handle_notification(notification)
            mock_handle.assert_called_once_with(notification=notification)

    def test_handle_notification_failing_over(self):
        """Test handling of NodeFailingOverNotification."""
        notification = NodeFailingOverNotification(id=1, ttl=5)

        with patch.object(
            self.handler, "handle_maintenance_start_notification"
        ) as mock_handle:
            self.handler.handle_notification(notification)
            mock_handle.assert_called_once_with(
                MaintenanceState.MAINTENANCE, notification
            )

    def test_handle_notification_failed_over(self):
        """Test handling of NodeFailedOverNotification."""
        notification = NodeFailedOverNotification(id=1)

        with patch.object(
            self.handler, "handle_maintenance_completed_notification"
        ) as mock_handle:
            self.handler.handle_notification(notification)
            mock_handle.assert_called_once_with(notification=notification)

    def test_handle_notification_unknown_type(self):
        """Test handling of unknown notification type."""
        notification = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )

        result = self.handler.handle_notification(notification)
        assert result is None

    def test_handle_maintenance_start_notification_disabled(self):
        """Test maintenance start notification handling when relaxed timeouts are disabled."""
        config = MaintNotificationsConfig(relaxed_timeout=-1)
        handler = MaintNotificationsConnectionHandler(self.mock_connection, config)

        result = handler.handle_maintenance_start_notification(
            MaintenanceState.MAINTENANCE, NodeMigratingNotification(id=1, ttl=5)
        )

        assert result is None
        self.mock_connection.update_current_socket_timeout.assert_not_called()

    def test_handle_maintenance_start_notification_moving_state(self):
        """Test maintenance start notification handling when connection is in MOVING state."""
        self.mock_connection.maintenance_state = MaintenanceState.MOVING

        result = self.handler.handle_maintenance_start_notification(
            MaintenanceState.MAINTENANCE, NodeMigratingNotification(id=1, ttl=5)
        )
        assert result is None
        self.mock_connection.update_current_socket_timeout.assert_not_called()

    def test_handle_maintenance_start_notification_success(self):
        """Test successful maintenance start notification handling for migrating."""
        self.mock_connection.maintenance_state = MaintenanceState.NONE

        self.handler.handle_maintenance_start_notification(
            MaintenanceState.MAINTENANCE, NodeMigratingNotification(id=1, ttl=5)
        )

        assert self.mock_connection.maintenance_state == MaintenanceState.MAINTENANCE
        self.mock_connection.update_current_socket_timeout.assert_called_once_with(20)
        self.mock_connection.set_tmp_settings.assert_called_once_with(
            tmp_relaxed_timeout=20
        )

    def test_handle_maintenance_completed_notification_disabled(self):
        """Test maintenance completed notification handling when relaxed timeouts are disabled."""
        config = MaintNotificationsConfig(relaxed_timeout=-1)
        handler = MaintNotificationsConnectionHandler(self.mock_connection, config)

        result = handler.handle_maintenance_completed_notification()
        assert result is None
        self.mock_connection.update_current_socket_timeout.assert_not_called()

    def test_handle_maintenance_completed_notification_moving_state(self):
        """Test maintenance completed notification handling when connection is in MOVING state."""
        self.mock_connection.maintenance_state = MaintenanceState.MOVING

        result = self.handler.handle_maintenance_completed_notification()
        assert result is None
        self.mock_connection.update_current_socket_timeout.assert_not_called()

    def test_handle_maintenance_completed_notification_success(self):
        """Test successful maintenance completed notification handling."""
        self.mock_connection.maintenance_state = MaintenanceState.MAINTENANCE

        self.handler.handle_maintenance_completed_notification()

        assert self.mock_connection.maintenance_state == MaintenanceState.NONE

        self.mock_connection.update_current_socket_timeout.assert_called_once_with(-1)
        self.mock_connection.reset_tmp_settings.assert_called_once_with(
            reset_relaxed_timeout=True
        )


@pytest.mark.fixed_client
class TestEndpointType:
    """Test the EndpointType class functionality."""

    def test_endpoint_type_constants(self):
        """Test that the EndpointType constants are correct."""
        assert EndpointType.INTERNAL_IP.value == "internal-ip"
        assert EndpointType.INTERNAL_FQDN.value == "internal-fqdn"
        assert EndpointType.EXTERNAL_IP.value == "external-ip"
        assert EndpointType.EXTERNAL_FQDN.value == "external-fqdn"
        assert EndpointType.NONE.value == "none"


@pytest.mark.fixed_client
class TestMaintNotificationsConfigEndpointType:
    """Test MaintNotificationsConfig endpoint type functionality."""

    def setup_method(self):
        """Set up common mock classes for all tests."""

        class MockSocket:
            def __init__(self, resolved_ip):
                self.resolved_ip = resolved_ip

            def getpeername(self):
                return (self.resolved_ip, 6379)

        class MockConnection(MaintNotificationsAbstractConnection, ConnectionInterface):
            def __init__(self, host, resolved_ip=None, is_ssl=False):
                self.host = host
                self.port = 6379
                self._sock = MockSocket(resolved_ip) if resolved_ip else None
                self.__class__.__name__ = "SSLConnection" if is_ssl else "Connection"

            @property
            def is_connected(self):
                return self._sock is not None

            def _get_socket(self):
                return self._sock

            def get_resolved_ip(self):
                # Call the actual method from AbstractConnection
                from redis.connection import AbstractConnection

                return AbstractConnection.get_resolved_ip(self)  # type: ignore

        self.MockSocket = MockSocket
        self.MockConnection = MockConnection

    def test_config_validation_valid_endpoint_types(self):
        """Test that MaintNotificationsConfig accepts valid endpoint types."""
        for endpoint_type in EndpointType:
            config = MaintNotificationsConfig(endpoint_type=endpoint_type)
            assert config.endpoint_type == endpoint_type

    def test_config_validation_none_endpoint_type(self):
        """Test that MaintNotificationsConfig accepts None as endpoint type."""
        config = MaintNotificationsConfig(endpoint_type=None)
        assert config.endpoint_type is None

    def test_endpoint_type_detection_ip_addresses(self):
        """Test endpoint type detection for IP addresses."""
        config = MaintNotificationsConfig()

        # Test private IPv4 addresses
        conn1 = self.MockConnection("192.168.1.1", resolved_ip="192.168.1.1")
        assert (
            config.get_endpoint_type("192.168.1.1", conn1) == EndpointType.INTERNAL_IP
        )

        # Test public IPv4 addresses
        conn2 = self.MockConnection("8.8.8.8", resolved_ip="8.8.8.8")
        assert config.get_endpoint_type("8.8.8.8", conn2) == EndpointType.EXTERNAL_IP

        # Test IPv6 loopback
        conn3 = self.MockConnection("::1")
        assert config.get_endpoint_type("::1", conn3) == EndpointType.INTERNAL_IP

        # Test IPv6 public address
        conn4 = self.MockConnection("2001:4860:4860::8888")
        assert (
            config.get_endpoint_type("2001:4860:4860::8888", conn4)
            == EndpointType.EXTERNAL_IP
        )

    def test_endpoint_type_detection_fqdn_with_resolved_ip(self):
        """Test endpoint type detection for FQDNs with resolved IP addresses."""
        config = MaintNotificationsConfig()

        # Test FQDN resolving to private IP
        conn1 = self.MockConnection(
            "redis.internal.company.com", resolved_ip="192.168.1.1"
        )
        assert (
            config.get_endpoint_type("redis.internal.company.com", conn1)
            == EndpointType.INTERNAL_FQDN
        )

        # Test FQDN resolving to public IP
        conn2 = self.MockConnection("db123.redis.com", resolved_ip="8.8.8.8")
        assert (
            config.get_endpoint_type("db123.redis.com", conn2)
            == EndpointType.EXTERNAL_FQDN
        )

        # Test internal FQDN resolving to public IP (should use resolved IP)
        conn3 = self.MockConnection(
            "redis.internal.company.com", resolved_ip="10.8.8.8"
        )
        assert (
            config.get_endpoint_type("redis.internal.company.com", conn3)
            == EndpointType.INTERNAL_FQDN
        )

        # Test FQDN with TLS
        conn4 = self.MockConnection(
            "redis.internal.company.com", resolved_ip="192.168.1.1", is_ssl=True
        )
        assert (
            config.get_endpoint_type("redis.internal.company.com", conn4)
            == EndpointType.INTERNAL_FQDN
        )

        conn5 = self.MockConnection(
            "db123.redis.com", resolved_ip="8.8.8.8", is_ssl=True
        )
        assert (
            config.get_endpoint_type("db123.redis.com", conn5)
            == EndpointType.EXTERNAL_FQDN
        )

    def test_endpoint_type_detection_fqdn_heuristics(self):
        """Test endpoint type detection using FQDN heuristics when no resolved IP is available."""
        config = MaintNotificationsConfig()

        # Test localhost (should be internal)
        conn1 = self.MockConnection("localhost")
        assert (
            config.get_endpoint_type("localhost", conn1) == EndpointType.INTERNAL_FQDN
        )

        # Test .local domain (should be internal)
        conn2 = self.MockConnection("server.local")
        assert (
            config.get_endpoint_type("server.local", conn2)
            == EndpointType.INTERNAL_FQDN
        )

        # Test public domain (should be external)
        conn3 = self.MockConnection("example.com")
        assert (
            config.get_endpoint_type("example.com", conn3) == EndpointType.EXTERNAL_FQDN
        )

    def test_endpoint_type_override(self):
        """Test that configured endpoint_type overrides detection."""

        # Test with endpoint_type set to NONE
        config = MaintNotificationsConfig(endpoint_type=EndpointType.NONE)
        conn = self.MockConnection("localhost")

        assert config.get_endpoint_type("localhost", conn) == EndpointType.NONE

        # Test with endpoint_type set to EXTERNAL_IP
        config = MaintNotificationsConfig(endpoint_type=EndpointType.EXTERNAL_IP)
        assert config.get_endpoint_type("localhost", conn) == EndpointType.EXTERNAL_IP


@pytest.mark.fixed_client
class TestMaintNotificationsMetricsRecording:
    """
    Tests for metrics recording from maintenance notification handlers.
    These tests verify that the OTel recorder functions are called with correct arguments.
    """

    @patch("redis.maint_notifications.record_maint_notification_count")
    def test_connection_handler_calls_record_maint_notification_count(
        self, mock_record_maint_notification_count
    ):
        """Test that handle_notification calls record_maint_notification_count."""
        mock_connection = Mock()
        mock_connection.maintenance_state = MaintenanceState.NONE
        mock_connection.host = "localhost"
        mock_connection.port = 6379
        mock_connection._sock.getsockname.return_value = ("127.0.0.1", 12345)

        config = MaintNotificationsConfig(enabled=True, relaxed_timeout=20)
        handler = MaintNotificationsConnectionHandler(mock_connection, config)

        notification = NodeMigratingNotification(id=1, ttl=5)
        handler.handle_notification(notification)

        mock_record_maint_notification_count.assert_called_once_with(
            server_address="localhost",
            server_port=6379,
            network_peer_address="localhost",
            network_peer_port=6379,
            maint_notification="MIGRATING",
        )

    @patch("redis.maint_notifications.record_connection_relaxed_timeout")
    @patch("redis.maint_notifications.get_pool_name")
    def test_connection_handler_calls_record_connection_relaxed_timeout_on_start(
        self, mock_get_pool_name, mock_record_connection_relaxed_timeout
    ):
        """Test that handle_notification calls record_connection_relaxed_timeout with relaxed=True."""
        mock_connection = Mock()
        mock_connection.maintenance_state = MaintenanceState.NONE
        mock_connection._maint_notifications_pool_handler = Mock()
        mock_connection._maint_notifications_pool_handler.pool = Mock()
        mock_connection._sock.getsockname.return_value = ("127.0.0.1", 12345)
        mock_get_pool_name.return_value = "localhost:6379_abc123"

        config = MaintNotificationsConfig(enabled=True, relaxed_timeout=20)
        handler = MaintNotificationsConnectionHandler(mock_connection, config)

        notification = NodeMigratingNotification(id=1, ttl=5)
        handler.handle_notification(notification)

        mock_record_connection_relaxed_timeout.assert_called_once_with(
            connection_name="localhost:6379_abc123",
            maint_notification="MIGRATING",
            relaxed=True,
        )

    @patch("redis.maint_notifications.record_connection_relaxed_timeout")
    @patch("redis.maint_notifications.get_pool_name")
    def test_connection_handler_calls_record_connection_relaxed_timeout_on_complete(
        self, mock_get_pool_name, mock_record_connection_relaxed_timeout
    ):
        """Test that handle_notification calls record_connection_relaxed_timeout with relaxed=False."""
        mock_connection = Mock()
        mock_connection.maintenance_state = MaintenanceState.MAINTENANCE
        mock_connection._maint_notifications_pool_handler = Mock()
        mock_connection._maint_notifications_pool_handler.pool = Mock()
        mock_connection._sock.getsockname.return_value = ("127.0.0.1", 12345)
        mock_get_pool_name.return_value = "localhost:6379_abc123"

        config = MaintNotificationsConfig(relaxed_timeout=20)
        handler = MaintNotificationsConnectionHandler(mock_connection, config)

        notification = NodeMigratedNotification(id=1)
        handler.handle_notification(notification)

        mock_record_connection_relaxed_timeout.assert_called_once_with(
            connection_name="localhost:6379_abc123",
            maint_notification="MIGRATED",
            relaxed=False,
        )

    @patch("redis.maint_notifications.record_connection_relaxed_timeout")
    def test_connection_handler_no_relaxed_timeout_call_when_disabled(
        self, mock_record_connection_relaxed_timeout
    ):
        """Test that record_connection_relaxed_timeout is not called when relaxed_timeout is disabled."""
        mock_connection = Mock()
        mock_connection.maintenance_state = MaintenanceState.NONE
        mock_connection.host = "localhost"
        mock_connection.port = 6379
        mock_connection._sock.getsockname.return_value = ("127.0.0.1", 12345)

        config = MaintNotificationsConfig(enabled=True, relaxed_timeout=-1)
        handler = MaintNotificationsConnectionHandler(mock_connection, config)

        notification = NodeMigratingNotification(id=1, ttl=5)
        handler.handle_notification(notification)

        mock_record_connection_relaxed_timeout.assert_not_called()

    @patch("redis.maint_notifications.record_connection_handoff")
    def test_pool_handler_calls_record_connection_handoff(
        self, mock_record_connection_handoff
    ):
        """Test that handle_node_moving_notification calls record_connection_handoff."""
        mock_pool = Mock()
        mock_pool._lock = MagicMock()
        mock_pool._lock.__enter__ = Mock(return_value=None)
        mock_pool._lock.__exit__ = Mock(return_value=None)
        mock_pool.connection_kwargs = {"host": "localhost", "port": 6379, "db": 0}
        mock_pool._pool_id = "a1b2c3d4"  # Mock the unique pool ID

        config = MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=20
        )
        handler = MaintNotificationsPoolHandler(mock_pool, config)

        notification = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )

        with patch("threading.Timer"):
            handler.handle_node_moving_notification(notification)

        mock_record_connection_handoff.assert_called_once_with(
            pool_name="localhost:6379_a1b2c3d4",
        )

    @patch("redis.maint_notifications.record_connection_handoff")
    def test_pool_handler_no_handoff_call_when_already_processed(
        self, mock_record_connection_handoff
    ):
        """Test that record_connection_handoff is not called for already processed notification."""
        mock_pool = Mock()
        mock_pool._lock = MagicMock()
        mock_pool._lock.__enter__ = Mock(return_value=None)
        mock_pool._lock.__exit__ = Mock(return_value=None)

        config = MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=20
        )
        handler = MaintNotificationsPoolHandler(mock_pool, config)

        notification = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        # Add notification to processed set
        handler._processed_notifications.add(notification)

        handler.handle_node_moving_notification(notification)

        mock_record_connection_handoff.assert_not_called()

    @patch("redis.maint_notifications.record_connection_handoff")
    def test_pool_handler_no_handoff_call_when_disabled(
        self, mock_record_connection_handoff
    ):
        """Test that record_connection_handoff is not called when both features are disabled."""
        mock_pool = Mock()
        mock_pool._lock = MagicMock()
        mock_pool._lock.__enter__ = Mock(return_value=None)
        mock_pool._lock.__exit__ = Mock(return_value=None)

        config = MaintNotificationsConfig(
            enabled=True, proactive_reconnect=False, relaxed_timeout=-1
        )
        handler = MaintNotificationsPoolHandler(mock_pool, config)

        notification = NodeMovingNotification(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )

        handler.handle_node_moving_notification(notification)

        mock_record_connection_handoff.assert_not_called()


@pytest.mark.fixed_client
class TestDisconnectCompletesMaintenance:
    def test_disconnect_without_socket_still_completes_maintenance(self):
        """
        The relaxation is restored and reported on every disconnect, including
        one that finds no socket to close and returns early.
        """
        listener = _RecordingListener()
        config = _config_with_listener(listener)
        connection = Connection(protocol=3, maint_notifications_config=config)
        connection.maintenance_state = MaintenanceState.MAINTENANCE
        assert connection._sock is None

        connection.disconnect()

        assert connection.maintenance_state == MaintenanceState.NONE
        assert [type(e) for e in listener.events] == [MaintenanceCompletedEvent]


@pytest.mark.fixed_client
class TestHandoffTakesOverMaintenance:
    """
    A connection under maintenance that a MOVING handoff takes over has its
    MAINTENANCE pair closed: its completion notification is skipped while it
    is MOVING, and the handoff's cleanup does not know what it superseded.
    """

    def setup_method(self):
        self.listener = _RecordingListener()
        self.config = _config_with_listener(self.listener)
        self.pool = ConnectionPool(
            host="localhost",
            port=6379,
            protocol=3,
            maint_notifications_config=self.config,
        )

    def _connection(self, state):
        conn = Mock()
        conn.maintenance_state = state
        conn.maint_notifications_config = self.config
        return conn

    def test_moving_over_maintenance_dispatches_completed_event(self):
        conn = self._connection(MaintenanceState.MAINTENANCE)

        self.pool.update_connection_settings(conn, state=MaintenanceState.MOVING)

        assert conn.maintenance_state == MaintenanceState.MOVING
        assert len(self.listener.events) == 1
        event = self.listener.events[0]
        assert isinstance(event, MaintenanceCompletedEvent)
        assert event.state == MaintenanceState.MAINTENANCE
        assert event.connection is conn
        assert event.connection_pool is self.pool
        assert event.notification is None

    @pytest.mark.parametrize(
        "state, new_state",
        [
            (MaintenanceState.NONE, MaintenanceState.MOVING),
            (MaintenanceState.MOVING, MaintenanceState.MOVING),
            (MaintenanceState.MOVING, MaintenanceState.NONE),
        ],
    )
    def test_other_state_transitions_dispatch_nothing(self, state, new_state):
        conn = self._connection(state)

        self.pool.update_connection_settings(conn, state=new_state)

        assert conn.maintenance_state == new_state
        assert self.listener.events == []
