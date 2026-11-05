"""
Tests for Redis keyspace notifications support.
"""

import pytest
import time
from unittest.mock import MagicMock, Mock, PropertyMock

from redis.exceptions import ConnectionError
from tests.conftest import skip_if_server_version_lt
from redis.keyspace_notifications import (
    SubkeyeventChannel,
    SubkeyspaceChannel,
    SubkeyspaceeventChannel,
    SubkeyspaceitemChannel,
    _parse_length_prefixed_subkeys,
)
from redis.keyspace_notifications import (
    ChannelType,
    ClusterKeyspaceNotifications,
    EventType,
    KeyeventChannel,
    KeyNotification,
    KeyspaceChannel,
    KeyspaceNotifications,
    _is_pattern,
    get_channel_type,
)


@pytest.mark.fixed_client
class TestEventType:
    """Tests for EventType constants."""

    def test_common_event_types(self):
        """Test that common event type constants are defined."""
        assert EventType.SET == "set"
        assert EventType.DEL == "del"
        assert EventType.EXPIRE == "expire"
        assert EventType.EXPIRED == "expired"
        assert EventType.LPUSH == "lpush"


@pytest.mark.fixed_client
class TestPatternDetection:
    """Tests for pattern detection function."""

    def test_is_pattern_with_asterisk(self):
        """Test detection of asterisk wildcard."""
        assert _is_pattern("user:*") is True
        assert _is_pattern("*") is True
        assert _is_pattern("__keyspace@0__:user:*") is True

    def test_is_pattern_with_question_mark(self):
        """Test detection of question mark wildcard."""
        assert _is_pattern("user:?") is True
        assert _is_pattern("key?") is True

    def test_is_pattern_with_brackets(self):
        """Test detection of bracket character class."""
        assert _is_pattern("user:[abc]") is True
        assert _is_pattern("key[0-9]") is True

    def test_is_pattern_exact_channel(self):
        """Test that exact channels are not detected as patterns."""
        assert _is_pattern("user:123") is False
        assert _is_pattern("__keyspace@0__:mykey") is False
        assert _is_pattern("simple_key") is False

    def test_is_pattern_escaped_wildcards(self):
        """Test that escaped wildcards are not detected as patterns."""
        assert _is_pattern(r"user:\*") is False
        assert _is_pattern(r"key\?name") is False
        assert _is_pattern(r"test\[bracket") is False

    def test_is_pattern_with_bytes(self):
        """Test pattern detection with bytes input."""
        assert _is_pattern(b"user:*") is True
        assert _is_pattern(b"user:123") is False


@pytest.mark.fixed_client
class TestKeyspaceChannelClass:
    """Tests for KeyspaceChannel class."""

    def test_basic_channel(self):
        """Test basic keyspace channel creation."""
        channel = KeyspaceChannel("mykey", db=0)
        assert str(channel) == "__keyspace@0__:mykey"
        assert channel.key_or_pattern == "mykey"
        assert channel.db == 0

    def test_channel_default_db(self):
        """Test keyspace channel defaults to database 0."""
        channel = KeyspaceChannel("mykey")
        assert str(channel) == "__keyspace@0__:mykey"
        assert channel.db == 0

    def test_pattern_channel(self):
        """Test keyspace channel with pattern."""
        channel = KeyspaceChannel("user:*", db=0)
        assert str(channel) == "__keyspace@0__:user:*"
        assert channel.is_pattern is True

    def test_equality_with_string(self):
        """Test equality comparison with string."""
        channel = KeyspaceChannel("mykey", db=0)
        assert channel == "__keyspace@0__:mykey"
        assert channel != "__keyspace@1__:mykey"

    def test_equality_with_channel(self):
        """Test equality comparison with another channel."""
        channel1 = KeyspaceChannel("mykey", db=0)
        channel2 = KeyspaceChannel("mykey", db=0)
        channel3 = KeyspaceChannel("otherkey", db=0)
        assert channel1 == channel2
        assert channel1 != channel3


@pytest.mark.fixed_client
class TestKeyeventChannelClass:
    """Tests for KeyeventChannel class."""

    def test_basic_channel(self):
        """Test basic keyevent channel creation."""
        channel = KeyeventChannel(EventType.SET, db=0)
        assert str(channel) == "__keyevent@0__:set"
        assert channel.event == "set"
        assert channel.db == 0

    def test_channel_with_string_event(self):
        """Test keyevent channel with string event type."""
        channel = KeyeventChannel("del", db=0)
        assert str(channel) == "__keyevent@0__:del"

    def test_channel_default_db(self):
        """Test keyevent channel defaults to database 0."""
        channel = KeyeventChannel(EventType.SET)
        assert str(channel) == "__keyevent@0__:set"
        assert channel.db == 0

    def test_pattern_channel(self):
        """Test keyevent channel with pattern."""
        # Pattern for all GET-related events (get, getex, getdel, getset)
        channel = KeyeventChannel("get*")
        assert str(channel) == "__keyevent@0__:get*"
        assert channel.is_pattern is True

        # Pattern for all list operations
        channel = KeyeventChannel("l*")
        assert str(channel) == "__keyevent@0__:l*"

    def test_all_events_factory(self):
        """Test KeyeventChannel.all_events() factory method."""
        channel = KeyeventChannel.all_events()
        assert str(channel) == "__keyevent@0__:*"
        assert channel.is_pattern is True

    def test_is_pattern_property(self):
        """Test is_pattern property."""
        exact = KeyeventChannel(EventType.SET)
        assert exact.is_pattern is False

        pattern = KeyeventChannel.all_events()
        assert pattern.is_pattern is True

        pattern2 = KeyeventChannel("get*")
        assert pattern2.is_pattern is True

    def test_equality_with_string(self):
        """Test equality comparison with string."""
        channel = KeyeventChannel(EventType.SET, db=0)
        assert channel == "__keyevent@0__:set"
        assert channel != "__keyevent@1__:set"

    def test_equality_with_channel(self):
        """Test equality comparison with another channel."""
        channel1 = KeyeventChannel(EventType.SET, db=0)
        channel2 = KeyeventChannel("set", db=0)
        channel3 = KeyeventChannel(EventType.DEL, db=0)
        assert channel1 == channel2
        assert channel1 != channel3


@pytest.mark.fixed_client
class TestChannelDetection:
    """Tests for channel type detection using get_channel_type()."""

    def test_keyspace_channel_detection(self):
        """Test keyspace channel detection."""
        assert get_channel_type("__keyspace@0__:mykey") == ChannelType.KEYSPACE
        assert get_channel_type("__keyspace@5__:user:123") == ChannelType.KEYSPACE
        assert get_channel_type("__keyevent@0__:set") != ChannelType.KEYSPACE
        assert get_channel_type("regular_channel") is None

    def test_keyevent_channel_detection(self):
        """Test keyevent channel detection."""
        assert get_channel_type("__keyevent@0__:set") == ChannelType.KEYEVENT
        assert get_channel_type("__keyevent@5__:del") == ChannelType.KEYEVENT
        assert get_channel_type("__keyspace@0__:mykey") != ChannelType.KEYEVENT
        assert get_channel_type("regular_channel") is None

    def test_non_notification_channel(self):
        """Test that non-notification channels return None."""
        assert get_channel_type("regular_channel") is None
        assert get_channel_type("some:other:channel") is None
        assert get_channel_type("__other@0__:something") is None

    def test_bytes_input(self):
        """Test that bytes input is handled correctly."""
        assert get_channel_type(b"__keyspace@0__:mykey") == ChannelType.KEYSPACE
        assert get_channel_type(b"__keyevent@0__:set") == ChannelType.KEYEVENT
        assert get_channel_type(b"regular_channel") is None


@pytest.mark.fixed_client
class TestKeyNotification:
    """Tests for KeyNotification class."""

    def test_from_message_keyspace(self):
        """Test parsing a keyspace notification message."""
        message = {
            "type": "pmessage",
            "pattern": "__keyspace@0__:user:*",
            "channel": "__keyspace@0__:user:123",
            "data": "set",
        }
        notification = KeyNotification.from_message(message)

        assert notification is not None
        assert notification.key == "user:123"
        assert notification.event_type == "set"
        assert notification.event_type == EventType.SET  # Can compare with constant
        assert notification.database == 0
        assert notification.is_keyspace is True

    def test_from_message_keyevent(self):
        """Test parsing a keyevent notification message."""
        message = {
            "type": "message",
            "pattern": None,
            "channel": "__keyevent@0__:set",
            "data": "user:123",
        }
        notification = KeyNotification.from_message(message)

        assert notification is not None
        assert notification.key == "user:123"
        assert notification.event_type == "set"
        assert notification.database == 0
        assert notification.is_keyspace is False

    def test_from_message_with_bytes(self):
        """Test parsing a message with bytes channel and data."""
        message = {
            "type": "pmessage",
            "pattern": b"__keyspace@0__:user:*",
            "channel": b"__keyspace@0__:user:456",
            "data": b"del",
        }
        notification = KeyNotification.from_message(message)

        assert notification is not None
        assert notification.key == "user:456"
        assert notification.event_type == "del"
        assert notification.database == 0
        assert notification.is_keyspace is True

    def test_from_message_with_key_prefix(self):
        """Test parsing with key prefix filtering and stripping."""
        message = {
            "type": "pmessage",
            "pattern": "__keyspace@0__:user:*",
            "channel": "__keyspace@0__:user:123",
            "data": "set",
        }

        # With matching prefix - should strip it
        notification = KeyNotification.from_message(message, key_prefix="user:")
        assert notification is not None
        assert notification.key == "123"

        # With non-matching prefix - should return None
        notification = KeyNotification.from_message(message, key_prefix="cache:")
        assert notification is None

    def test_from_message_with_bytes_key_prefix(self):
        """Test parsing with bytes key prefix."""
        message = {
            "type": "pmessage",
            "pattern": "__keyspace@0__:user:*",
            "channel": "__keyspace@0__:user:123",
            "data": "set",
        }
        notification = KeyNotification.from_message(message, key_prefix=b"user:")
        assert notification is not None
        assert notification.key == "123"

    def test_from_message_non_notification(self):
        """Test that non-notification messages return None."""
        # Subscribe message
        message = {
            "type": "subscribe",
            "pattern": None,
            "channel": "__keyspace@0__:user:*",
            "data": 1,
        }
        assert KeyNotification.from_message(message) is None

        # Regular pubsub message
        message = {
            "type": "message",
            "pattern": None,
            "channel": "regular_channel",
            "data": "some data",
        }
        assert KeyNotification.from_message(message) is None

    def test_from_message_none(self):
        """Test that None message returns None."""
        assert KeyNotification.from_message(None) is None

    def test_try_parse(self):
        """Test the try_parse class method."""
        notification = KeyNotification.try_parse("__keyspace@0__:mykey", "set")
        assert notification is not None
        assert notification.key == "mykey"
        assert notification.event_type == "set"

    def test_try_parse_with_bytes(self):
        """Test try_parse with bytes input."""
        notification = KeyNotification.try_parse(b"__keyevent@0__:del", b"mykey")
        assert notification is not None
        assert notification.key == "mykey"
        assert notification.event_type == "del"

    def test_key_starts_with(self):
        """Test the key_starts_with method."""
        message = {
            "type": "pmessage",
            "pattern": "__keyspace@0__:*",
            "channel": "__keyspace@0__:user:123",
            "data": "set",
        }
        notification = KeyNotification.from_message(message)

        assert notification is not None
        assert notification.key_starts_with("user:") is True
        assert notification.key_starts_with("cache:") is False
        assert notification.key_starts_with(b"user:") is True

    @pytest.mark.parametrize("as_bytes", [False, True])
    @pytest.mark.parametrize("key", ["user:alice\n", "user:al\nice", "\n"])
    def test_keyspace_key_with_newline(self, key, as_bytes):
        """A newline in the key is part of the key, not the end of the channel."""
        channel = f"__keyspace@0__:{key}"
        notification = KeyNotification.try_parse(
            channel.encode() if as_bytes else channel,
            b"set" if as_bytes else "set",
        )
        assert notification is not None
        assert notification.key == key

    @pytest.mark.parametrize("as_bytes", [False, True])
    @pytest.mark.parametrize(
        ("channel", "data", "subkeys"),
        [
            ("__keyspace@0__:", "set", []),
            ("__subkeyspace@0__:", "hset|5:field", ["field"]),
        ],
    )
    def test_empty_keyspace_key(self, channel, data, subkeys, as_bytes):
        notification = KeyNotification.try_parse(
            channel.encode() if as_bytes else channel,
            data.encode() if as_bytes else data,
        )
        assert notification is not None
        assert notification.key == ""
        assert notification.subkeys == subkeys
        assert notification.is_keyspace is True

    def test_keyspace_key_with_trailing_newline_and_prefix(self):
        notification = KeyNotification.try_parse(
            "__keyspace@0__:user:alice\n", "set", key_prefix="user:"
        )
        assert notification is not None
        assert notification.key == "alice\n"

    def test_database_wildcard(self):
        """Test parsing channel with wildcard database."""
        notification = KeyNotification.try_parse("__keyspace@*__:mykey", "set")
        assert notification is not None
        assert notification.database == -1  # -1 indicates wildcard

    def test_future_event_type(self):
        """Test that future/unknown event types work as plain strings."""
        notification = KeyNotification.try_parse(
            "__keyspace@0__:mykey", "some_future_event"
        )
        assert notification is not None
        # Event type is just the string - no UNKNOWN enum needed
        assert notification.event_type == "some_future_event"
        assert isinstance(notification.event_type, str)


@pytest.mark.fixed_client
class TestClusterKeyspaceNotificationsMocked:
    """
    Mock-based unit tests for ClusterKeyspaceNotifications.

    These tests use mocks to simulate cluster behavior without requiring
    a running Redis Cluster, allowing us to test slot migration scenarios
    that are difficult to reproduce in a real cluster environment.
    """

    def _create_mock_node(self, name, host, port, server_type="primary"):
        """Create a mock ClusterNode."""
        node = Mock()
        node.name = name
        node.host = host
        node.port = port
        node.server_type = server_type

        # Create a mock redis connection with pubsub
        redis_conn = Mock()
        mock_pubsub = MagicMock()
        mock_pubsub.get_message = Mock(return_value=None)
        redis_conn.pubsub = Mock(return_value=mock_pubsub)
        node.redis_connection = redis_conn

        return node, mock_pubsub

    def _create_mock_cluster(self, nodes):
        """Create a mock RedisCluster with the given nodes."""

        cluster = Mock()
        cluster.get_nodes = Mock(return_value=nodes)
        # Filter to only primary nodes for get_primaries()
        primary_nodes = [n for n in nodes if n.server_type == "primary"]
        cluster.get_primaries = Mock(return_value=primary_nodes)
        cluster.get_redis_connection = Mock(
            side_effect=lambda node: node.redis_connection
        )

        # Mock nodes_manager
        nodes_manager = Mock()
        nodes_manager.initialize = Mock()
        cluster.nodes_manager = nodes_manager

        return cluster

    def test_receives_notification_from_any_primary_node(self):
        """
        Test that notifications can be received from any primary node.

        Since ClusterKeyspaceNotifications subscribes to ALL primary nodes,
        it receives notifications regardless of which node sends them.
        This makes slot ownership transparent to the caller.
        """

        # Create two mock primary nodes
        node1, pubsub1 = self._create_mock_node(
            "127.0.0.1:7000", "127.0.0.1", 7000, "primary"
        )
        node2, pubsub2 = self._create_mock_node(
            "127.0.0.1:7001", "127.0.0.1", 7001, "primary"
        )

        # Create mock cluster
        cluster = self._create_mock_cluster([node1, node2])

        # Create ClusterKeyspaceNotifications
        notifications = ClusterKeyspaceNotifications(cluster)

        # Subscribe to a key's notifications using KeyspaceChannel class
        test_key = "mykey"
        channel = KeyspaceChannel(test_key)
        notifications.subscribe(channel)

        # Verify we subscribed to both nodes
        assert len(notifications._node_pubsubs) == 2
        assert node1.name in notifications._node_pubsubs
        assert node2.name in notifications._node_pubsubs

        # Verify subscribe was called on both pubsubs
        pubsub1.subscribe.assert_called_once()
        pubsub2.subscribe.assert_called_once()

        # --- Notification from node1 ---
        msg_from_node1 = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"set",
            "pattern": None,
        }

        pubsub1.get_message.return_value = msg_from_node1
        pubsub2.get_message.return_value = None

        notification = notifications.get_message(
            ignore_subscribe_messages=True, timeout=1.0
        )

        assert notification is not None
        assert notification.key == test_key
        assert notification.event_type == EventType.SET

        # --- Notification from node2 ---
        pubsub1.get_message.return_value = None

        msg_from_node2 = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"del",
            "pattern": None,
        }
        pubsub2.get_message.return_value = msg_from_node2

        notification = notifications.get_message(
            ignore_subscribe_messages=True, timeout=1.0
        )

        assert notification is not None
        assert notification.key == test_key
        assert notification.event_type == EventType.DEL

        # Cleanup
        notifications.close()

    def test_subscribes_to_all_primary_nodes(self):
        """
        Test that ClusterKeyspaceNotifications subscribes to ALL primary nodes,
        not just the node that owns the key's slot.

        This is the key design decision that makes slot migrations transparent.
        """

        # Create three mock primary nodes
        node1, pubsub1 = self._create_mock_node(
            "127.0.0.1:7000", "127.0.0.1", 7000, "primary"
        )
        node2, pubsub2 = self._create_mock_node(
            "127.0.0.1:7001", "127.0.0.1", 7001, "primary"
        )
        node3, pubsub3 = self._create_mock_node(
            "127.0.0.1:7002", "127.0.0.1", 7002, "primary"
        )

        # Create mock cluster
        cluster = self._create_mock_cluster([node1, node2, node3])

        # Create ClusterKeyspaceNotifications
        notifications = ClusterKeyspaceNotifications(cluster)

        # Subscribe to a single key's notifications using KeyspaceChannel class
        # Even though this key would only be on ONE node in a real cluster,
        # we subscribe to ALL nodes
        test_key = "mykey"
        channel = KeyspaceChannel(test_key)
        notifications.subscribe(channel)

        # Verify we subscribed to ALL three nodes
        assert len(notifications._node_pubsubs) == 3
        assert node1.name in notifications._node_pubsubs
        assert node2.name in notifications._node_pubsubs
        assert node3.name in notifications._node_pubsubs

        # Verify subscribe was called on all pubsubs
        pubsub1.subscribe.assert_called_once()
        pubsub2.subscribe.assert_called_once()
        pubsub3.subscribe.assert_called_once()

        # Cleanup
        notifications.close()

    def test_pattern_subscription_on_all_nodes(self):
        """
        Test that pattern subscriptions are created on all primary nodes.
        """

        # Create two mock primary nodes
        node1, pubsub1 = self._create_mock_node(
            "127.0.0.1:7000", "127.0.0.1", 7000, "primary"
        )
        node2, pubsub2 = self._create_mock_node(
            "127.0.0.1:7001", "127.0.0.1", 7001, "primary"
        )

        cluster = self._create_mock_cluster([node1, node2])
        notifications = ClusterKeyspaceNotifications(cluster)

        # Subscribe to a pattern using KeyspaceChannel
        pattern = KeyspaceChannel("user:*")
        notifications.subscribe(pattern)

        # Verify psubscribe was called on both nodes (patterns use psubscribe)
        pubsub1.psubscribe.assert_called_once()
        pubsub2.psubscribe.assert_called_once()

        # Cleanup
        notifications.close()

    def test_uses_get_primaries_api(self):
        """
        Test that _get_all_primary_nodes uses the cluster's get_primaries() API
        rather than manually filtering get_nodes() by server_type.

        This ensures thread-safe access via NodesManager.get_nodes_by_server_type.
        """
        # Create mock nodes including a replica
        node1, pubsub1 = self._create_mock_node(
            "127.0.0.1:7000", "127.0.0.1", 7000, "primary"
        )
        node2, pubsub2 = self._create_mock_node(
            "127.0.0.1:7001", "127.0.0.1", 7001, "primary"
        )
        replica, _ = self._create_mock_node(
            "127.0.0.1:7002", "127.0.0.1", 7002, "replica"
        )

        cluster = self._create_mock_cluster([node1, node2, replica])
        notifications = ClusterKeyspaceNotifications(cluster)

        # Call _get_all_primary_nodes
        primaries = notifications._get_all_primary_nodes()

        # Verify get_primaries was called (not get_nodes filtered manually)
        cluster.get_primaries.assert_called_once()

        # Should only return primary nodes
        assert len(primaries) == 2
        assert node1 in primaries
        assert node2 in primaries
        assert replica not in primaries

        notifications.close()

    def test_refresh_subscriptions_recovers_broken_connections(self):
        """
        Test that refresh_subscriptions detects and re-creates broken pubsub connections.

        When an existing primary node's pubsub connection breaks (e.g., transient
        network issue) but the node remains a primary, the broken pubsub should be
        re-created during refresh.
        """
        # Create mock nodes
        node1, pubsub1 = self._create_mock_node(
            "127.0.0.1:7000", "127.0.0.1", 7000, "primary"
        )
        node2, pubsub2 = self._create_mock_node(
            "127.0.0.1:7001", "127.0.0.1", 7001, "primary"
        )

        cluster = self._create_mock_cluster([node1, node2])
        notifications = ClusterKeyspaceNotifications(cluster)

        # Subscribe to establish connections
        channel = KeyspaceChannel("mykey", db=0)
        notifications.subscribe(channel)

        # Verify both nodes are subscribed
        assert len(notifications._node_pubsubs) == 2
        original_pubsub1 = notifications._node_pubsubs[node1.name]

        # Simulate broken connection on node1
        mock_connection = Mock()
        mock_connection.is_connected = False  # Broken connection
        original_pubsub1.connection = mock_connection

        # Create a new pubsub for the re-creation
        new_pubsub1 = MagicMock()
        new_pubsub1.get_message = Mock(return_value=None)
        node1.redis_connection.pubsub.return_value = new_pubsub1

        # Call refresh_subscriptions
        notifications.refresh_subscriptions()

        # Verify node1's pubsub was replaced (re-created)
        assert notifications._node_pubsubs[node1.name] is new_pubsub1

        # Verify the new pubsub was subscribed
        new_pubsub1.subscribe.assert_called()

        notifications.close()

    def test_is_pubsub_connected_returns_false_for_broken_connection(self):
        """
        Test that _is_pubsub_connected correctly detects broken connections.
        """
        node1, pubsub1 = self._create_mock_node(
            "127.0.0.1:7000", "127.0.0.1", 7000, "primary"
        )

        cluster = self._create_mock_cluster([node1])
        notifications = ClusterKeyspaceNotifications(cluster)

        # Test with None connection
        pubsub1.connection = None
        assert notifications._is_pubsub_connected(pubsub1) is False

        # Test with connection that is not connected
        mock_connection = Mock()
        mock_connection.is_connected = False
        pubsub1.connection = mock_connection
        assert notifications._is_pubsub_connected(pubsub1) is False

        # Test with connected connection
        mock_connection.is_connected = True
        assert notifications._is_pubsub_connected(pubsub1) is True

        notifications.close()

    def test_refresh_subscriptions_handles_mixed_scenarios(self):
        """
        Test refresh_subscriptions handling:
        - New nodes (added to cluster)
        - Removed nodes (no longer in cluster)
        - Existing nodes with broken connections
        """
        # Initial nodes
        node1, pubsub1 = self._create_mock_node(
            "127.0.0.1:7000", "127.0.0.1", 7000, "primary"
        )
        node2, pubsub2 = self._create_mock_node(
            "127.0.0.1:7001", "127.0.0.1", 7001, "primary"
        )

        cluster = self._create_mock_cluster([node1, node2])
        notifications = ClusterKeyspaceNotifications(cluster)

        # Subscribe
        notifications.subscribe(KeyspaceChannel("mykey"))
        assert len(notifications._node_pubsubs) == 2

        # Simulate node2 connection broken
        mock_connection = Mock()
        mock_connection.is_connected = False
        notifications._node_pubsubs[node2.name].connection = mock_connection

        # Add a new node and remove node1 from cluster
        node3, pubsub3 = self._create_mock_node(
            "127.0.0.1:7002", "127.0.0.1", 7002, "primary"
        )

        # Update cluster to return new topology: node2 (broken), node3 (new)
        # node1 is removed
        cluster.get_primaries.return_value = [node2, node3]

        # Create new pubsubs for re-creation
        new_pubsub2 = MagicMock()
        new_pubsub2.get_message = Mock(return_value=None)
        node2.redis_connection.pubsub.return_value = new_pubsub2

        # Call refresh
        notifications.refresh_subscriptions()

        # node1 should be removed
        assert node1.name not in notifications._node_pubsubs

        # node2 should have new pubsub (was broken)
        assert notifications._node_pubsubs[node2.name] is new_pubsub2
        new_pubsub2.subscribe.assert_called()

        # node3 should be added
        assert node3.name in notifications._node_pubsubs
        pubsub3.subscribe.assert_called()

        notifications.close()

    def test_node_failure_during_pattern_subscribe_does_not_lose_patterns(self):
        """
        Test that a node which fails during the pattern subscribe call and
        is re-created during the exact-channel subscribe call still receives
        both the patterns and the exact channels.

        Regression test for the bug where _execute_subscribe called
        _subscribe_to_all_nodes twice sequentially.  A node cleaned up
        during the first (pattern) call would be treated as "new" in the
        second (exact-channel) call and caught up from
        _subscribed_patterns — which had not been updated yet, so the
        patterns were silently lost.
        """
        node1, pubsub1 = self._create_mock_node(
            "127.0.0.1:7000", "127.0.0.1", 7000, "primary"
        )
        node2, pubsub2 = self._create_mock_node(
            "127.0.0.1:7001", "127.0.0.1", 7001, "primary"
        )

        cluster = self._create_mock_cluster([node1, node2])
        notifications = ClusterKeyspaceNotifications(cluster)

        # Make node2's pubsub raise on the first psubscribe call
        # (simulating a transient failure during pattern subscription)
        pubsub2.psubscribe.side_effect = ConnectionError("connection lost")

        # Subscribe with both a pattern and an exact channel in one call
        pattern = KeyspaceChannel("user:*")  # pattern → psubscribe
        exact = KeyspaceChannel("mykey")  # exact   → subscribe
        notifications.subscribe(pattern, exact)

        # node1 should have received both psubscribe and subscribe
        pubsub1.psubscribe.assert_called_once()
        pubsub1.subscribe.assert_called_once()

        # node2 failed during psubscribe, so it should have been cleaned
        # up entirely — it must NOT appear in _node_pubsubs with only the
        # exact channel subscribed.
        assert node2.name not in notifications._node_pubsubs

        # The tracking state should still record both subscriptions so that
        # refresh_subscriptions can fully re-subscribe node2 later.
        assert "__keyspace@0__:user:*" in notifications._subscribed_patterns
        assert "__keyspace@0__:mykey" in notifications._subscribed_channels

        notifications.close()


@pytest.mark.onlycluster
class TestClusterKeyspaceNotifications:
    """
    A very basic usability test for subscribing to keyspace notifications in a cluster.

    Note: These tests require keyspace notifications to be enabled on the Redis server.
    The r_with_keyspace_notifications fixture handles this configuration automatically.
    """

    def test_keyspace_subscribe(self, r_with_keyspace_notifications):
        r = r_with_keyspace_notifications
        notifications = ClusterKeyspaceNotifications(r)
        notifications.subscribe(KeyspaceChannel("test:*"))
        r.set("test:key", "value")
        r.set("test:key2", "value2")
        r.delete("test:key2")

        # In a cluster, keys may live on different nodes and round-robin
        # polling does not guarantee the order messages are received.
        # Collect all three and verify by counts instead.
        messages = []
        for _ in range(3):
            msg = notifications.get_message(timeout=1.0)
            assert msg is not None
            assert msg.key.startswith("test:")
            messages.append(msg.event_type)

        assert sorted(messages) == ["del", "set", "set"]

        notifications.close()

    def test_keyspace_subscribe_with_handler(self, r_with_keyspace_notifications):
        """Use a handler in a background thread with run_in_thread()."""
        r = r_with_keyspace_notifications
        received = []

        def handler(msg):
            received.append(msg)
            assert msg.key.startswith("test:")
            if msg.event_type == "del":
                assert msg.key == "test:key2"
            else:
                assert msg.event_type == "set"

        notifications = ClusterKeyspaceNotifications(r)
        notifications.subscribe(KeyspaceChannel("test:*"), handler=handler)

        # Start background thread that polls for messages and triggers handlers
        thread = notifications.run_in_thread(poll_timeout=0.1, daemon=True)

        time.sleep(0.1)  # Allow subscription to complete

        r.set("test:key", "value")
        r.set("test:key2", "value2")
        r.delete("test:key2")

        # Wait for handlers to be called
        deadline = time.monotonic() + 5.0
        while len(received) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)

        thread.stop()
        thread.join(timeout=1.0)  # Wait for thread to actually stop

        # Verify we received all 3 notifications
        assert len(received) == 3

    def test_keyevent_subscribe(self, r_with_keyspace_notifications):
        """Test subscribing to keyevent notifications in a cluster."""
        r = r_with_keyspace_notifications
        notifications = ClusterKeyspaceNotifications(r)
        notifications.subscribe_keyevent(EventType.SET)
        time.sleep(0.1)  # Allow subscription to complete

        # Only SET operations will trigger notifications (not DELETE)
        r.set("test:key", "value")
        r.set("test:key2", "value2")
        r.delete("test:key2")  # This won't trigger a SET event

        # Expect exactly 2 SET notifications
        for _ in range(2):
            msg = notifications.get_message(ignore_subscribe_messages=True, timeout=2.0)
            assert msg is not None
            assert msg.key.startswith("test:")
            assert msg.event_type == "set"

        notifications.close()


@pytest.mark.fixed_client
class TestStandaloneClientKeyspaceNotificationsMocked:
    """
    Mock-based unit tests for KeyspaceNotifications (standalone Redis).

    These tests use mocks to simulate Redis behavior without requiring
    a running Redis server.
    """

    def _create_mock_redis(self):
        """Create a mock Redis client with pubsub."""
        redis_client = Mock()
        mock_pubsub = MagicMock()
        mock_pubsub.get_message = Mock(return_value=None)
        mock_pubsub.close = Mock()
        # Default subscribed to False; tests that need True should set it
        type(mock_pubsub).subscribed = PropertyMock(return_value=False)
        redis_client.pubsub = Mock(return_value=mock_pubsub)
        return redis_client, mock_pubsub

    def test_subscribe_exact_channel(self):
        """Test subscribing to an exact keyspace channel."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = KeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("mykey", db=0)
        notifications.subscribe(channel)

        # Verify subscribe was called with kwargs (exact channel, not pattern)
        mock_pubsub.subscribe.assert_called_once()
        assert "__keyspace@0__:mykey" in mock_pubsub.subscribe.call_args.kwargs

        notifications.close()

    def test_subscribe_pattern_channel(self):
        """Test subscribing to a pattern keyspace channel."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = KeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("user:*", db=0)
        notifications.subscribe(channel)

        # Verify psubscribe was called with kwargs (pattern subscription)
        mock_pubsub.psubscribe.assert_called_once()
        assert "__keyspace@0__:user:*" in mock_pubsub.psubscribe.call_args.kwargs

        notifications.close()

    def test_subscribe_keyevent(self):
        """Test subscribing to a keyevent channel."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = KeyspaceNotifications(redis_client)
        notifications.subscribe_keyevent(EventType.SET, db=0)

        # KeyeventChannel with "set" is exact, not pattern
        mock_pubsub.subscribe.assert_called_once()
        assert "__keyevent@0__:set" in mock_pubsub.subscribe.call_args.kwargs

        notifications.close()

    def test_subscribe_keyspace_convenience(self):
        """Test the subscribe_keyspace convenience method."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = KeyspaceNotifications(redis_client)
        notifications.subscribe_keyspace("cache:*", db=0)

        # Pattern should use psubscribe with kwargs
        mock_pubsub.psubscribe.assert_called_once()
        assert "__keyspace@0__:cache:*" in mock_pubsub.psubscribe.call_args.kwargs

        notifications.close()

    def test_unsubscribe(self):
        """Test unsubscribing from channels."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = KeyspaceNotifications(redis_client)

        # Subscribe to exact and pattern channels
        exact_channel = KeyspaceChannel("mykey", db=0)
        pattern_channel = KeyspaceChannel("user:*", db=0)
        notifications.subscribe(exact_channel)
        notifications.subscribe(pattern_channel)

        # Unsubscribe
        notifications.unsubscribe(exact_channel)
        notifications.unsubscribe(pattern_channel)

        mock_pubsub.unsubscribe.assert_called_once()
        mock_pubsub.punsubscribe.assert_called_once()

        notifications.close()

    def test_get_message_returns_notification(self):
        """Test that get_message returns KeyNotification objects."""
        redis_client, mock_pubsub = self._create_mock_redis()

        # Setup mock to return a keyspace message
        mock_pubsub.get_message.return_value = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"set",
            "pattern": None,
        }

        notifications = KeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("mykey", db=0)
        notifications.subscribe(channel)

        notification = notifications.get_message(timeout=1.0)

        assert notification is not None
        assert notification.key == "mykey"
        assert notification.event_type == "set"
        assert notification.database == 0

        notifications.close()

    def test_get_message_with_pattern(self):
        """Test get_message with pattern subscription."""
        redis_client, mock_pubsub = self._create_mock_redis()

        # Setup mock to return a pmessage
        mock_pubsub.get_message.return_value = {
            "type": "pmessage",
            "pattern": b"__keyspace@0__:user:*",
            "channel": b"__keyspace@0__:user:123",
            "data": b"set",
        }

        notifications = KeyspaceNotifications(redis_client)
        notifications.subscribe(KeyspaceChannel("user:*", db=0))

        notification = notifications.get_message(timeout=1.0)

        assert notification is not None
        assert notification.key == "user:123"
        assert notification.event_type == "set"

        notifications.close()

    def test_get_message_returns_none_when_closed(self):
        """Test that get_message returns None when notifications are closed."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = KeyspaceNotifications(redis_client)
        notifications.close()

        notification = notifications.get_message(timeout=1.0)
        assert notification is None

    def test_handler_callback(self):
        """Test that handlers are called with KeyNotification objects."""
        redis_client, mock_pubsub = self._create_mock_redis()

        received = []

        def handler(notification):
            received.append(notification)

        # Message to be delivered
        test_message = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"set",
            "pattern": None,
        }

        # Track the wrapped handler that will be registered
        registered_handlers = {}

        def capture_subscribe(**kwargs):
            registered_handlers.update(kwargs)

        mock_pubsub.subscribe.side_effect = capture_subscribe

        # When get_message is called, simulate pubsub behavior:
        # call the handler and return None (like real pubsub does)
        def mock_get_message(**_kwargs):
            channel_key = "__keyspace@0__:mykey"
            if channel_key in registered_handlers and registered_handlers[channel_key]:
                registered_handlers[channel_key](test_message)
                return None  # Handler consumed the message
            return test_message

        mock_pubsub.get_message.side_effect = mock_get_message

        notifications = KeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("mykey", db=0)
        notifications.subscribe(channel, handler=handler)

        # get_message should return None because handler consumed it
        result = notifications.get_message(timeout=1.0)
        assert result is None

        # Handler should have been called with KeyNotification
        assert len(received) == 1
        assert received[0].key == "mykey"
        assert received[0].event_type == "set"

        notifications.close()

    def test_context_manager(self):
        """Test KeyspaceNotifications as context manager."""
        redis_client, mock_pubsub = self._create_mock_redis()

        with KeyspaceNotifications(redis_client) as notifications:
            type(mock_pubsub).subscribed = PropertyMock(return_value=True)
            notifications.subscribe(KeyspaceChannel("mykey"))
            assert notifications.subscribed

        # Should be closed after exiting context
        mock_pubsub.close.assert_called_once()

    def test_subscribed_property(self):
        """Test the subscribed property delegates to the underlying PubSub."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = KeyspaceNotifications(redis_client)
        # Initially not subscribed
        assert not notifications.subscribed

        # After subscribing, PubSub reports subscribed
        type(mock_pubsub).subscribed = PropertyMock(return_value=True)
        notifications.subscribe(KeyspaceChannel("mykey"))
        assert notifications.subscribed

        # After unsubscribing, PubSub reports not subscribed
        type(mock_pubsub).subscribed = PropertyMock(return_value=False)
        notifications.unsubscribe(KeyspaceChannel("mykey"))
        assert not notifications.subscribed

        notifications.close()

    def test_key_prefix_filtering(self):
        """Test that key_prefix filters notifications."""
        redis_client, mock_pubsub = self._create_mock_redis()

        # Setup mock to return a notification for a key without the prefix
        mock_pubsub.get_message.return_value = {
            "type": "message",
            "channel": b"__keyspace@0__:other:key",
            "data": b"set",
            "pattern": None,
        }

        notifications = KeyspaceNotifications(redis_client, key_prefix="myapp:")
        notifications.subscribe(KeyspaceChannel("*"))

        # Should be filtered out (returns None)
        notification = notifications.get_message(timeout=0.1)
        assert notification is None

        notifications.close()


@pytest.mark.fixed_client
class TestParseLengthPrefixedSubkeys:
    """Tests for _parse_length_prefixed_subkeys helper."""

    def test_single_subkey(self):
        assert _parse_length_prefixed_subkeys("5:field") == ["field"]

    def test_multiple_subkeys(self):
        assert _parse_length_prefixed_subkeys("5:field,6:field2") == [
            "field",
            "field2",
        ]

    def test_subkey_with_special_chars(self):
        assert _parse_length_prefixed_subkeys("7:foo:bar") == ["foo:bar"]

    def test_subkey_with_comma_in_value(self):
        assert _parse_length_prefixed_subkeys("5:a,b,c,1:x") == ["a,b,c", "x"]

    def test_empty_subkey(self):
        assert _parse_length_prefixed_subkeys("0:") == [""]

    def test_multiple_with_varying_lengths(self):
        assert _parse_length_prefixed_subkeys("1:a,2:bb,3:ccc") == [
            "a",
            "bb",
            "ccc",
        ]

    def test_multibyte_subkeys_use_byte_lengths(self):
        # the server prefixes each subkey with its size in bytes (sdslen):
        # each 键 is 3 bytes in UTF-8, so "键键" is prefixed with 6
        assert _parse_length_prefixed_subkeys("6:键键,3:abc") == ["键键", "abc"]

    def test_multibyte_subkeys_from_bytes(self):
        assert _parse_length_prefixed_subkeys("6:键键,3:abc".encode("utf-8")) == [
            "键键",
            "abc",
        ]


@pytest.mark.fixed_client
class TestSubkeyspaceChannelClass:
    """Tests for SubkeyspaceChannel class."""

    def test_basic_channel(self):
        channel = SubkeyspaceChannel("myhash", db=0)
        assert str(channel) == "__subkeyspace@0__:myhash"
        assert channel.key_or_pattern == "myhash"
        assert channel.db == 0

    def test_channel_default_db(self):
        channel = SubkeyspaceChannel("myhash")
        assert str(channel) == "__subkeyspace@0__:myhash"
        assert channel.db == 0

    def test_pattern_channel(self):
        channel = SubkeyspaceChannel("hash:*", db=0)
        assert str(channel) == "__subkeyspace@0__:hash:*"
        assert channel.is_pattern is True

    def test_exact_channel(self):
        channel = SubkeyspaceChannel("myhash")
        assert channel.is_pattern is False

    def test_equality_with_string(self):
        channel = SubkeyspaceChannel("myhash", db=0)
        assert channel == "__subkeyspace@0__:myhash"
        assert channel != "__subkeyspace@1__:myhash"

    def test_equality_with_channel(self):
        ch1 = SubkeyspaceChannel("myhash", db=0)
        ch2 = SubkeyspaceChannel("myhash", db=0)
        ch3 = SubkeyspaceChannel("other", db=0)
        assert ch1 == ch2
        assert ch1 != ch3

    def test_hash(self):
        ch1 = SubkeyspaceChannel("myhash", db=0)
        ch2 = SubkeyspaceChannel("myhash", db=0)
        assert hash(ch1) == hash(ch2)

    def test_repr(self):
        channel = SubkeyspaceChannel("myhash", db=2)
        assert repr(channel) == "SubkeyspaceChannel('myhash', db=2)"

    def test_different_db(self):
        channel = SubkeyspaceChannel("myhash", db=5)
        assert str(channel) == "__subkeyspace@5__:myhash"


@pytest.mark.fixed_client
class TestSubkeyeventChannelClass:
    """Tests for SubkeyeventChannel class."""

    def test_basic_channel(self):
        channel = SubkeyeventChannel("hset", db=0)
        assert str(channel) == "__subkeyevent@0__:hset"
        assert channel.event == "hset"
        assert channel.db == 0

    def test_channel_default_db(self):
        channel = SubkeyeventChannel("hdel")
        assert str(channel) == "__subkeyevent@0__:hdel"

    def test_pattern_channel(self):
        channel = SubkeyeventChannel("h*")
        assert str(channel) == "__subkeyevent@0__:h*"
        assert channel.is_pattern is True

    def test_exact_channel(self):
        channel = SubkeyeventChannel("hset")
        assert channel.is_pattern is False

    def test_all_events_factory(self):
        channel = SubkeyeventChannel.all_events()
        assert str(channel) == "__subkeyevent@0__:*"
        assert channel.is_pattern is True

    def test_all_events_with_db(self):
        channel = SubkeyeventChannel.all_events(db=3)
        assert str(channel) == "__subkeyevent@3__:*"

    def test_equality_with_string(self):
        channel = SubkeyeventChannel("hset", db=0)
        assert channel == "__subkeyevent@0__:hset"

    def test_equality_with_channel(self):
        ch1 = SubkeyeventChannel("hset", db=0)
        ch2 = SubkeyeventChannel("hset", db=0)
        ch3 = SubkeyeventChannel("hdel", db=0)
        assert ch1 == ch2
        assert ch1 != ch3

    def test_repr(self):
        channel = SubkeyeventChannel("hset", db=1)
        assert repr(channel) == "SubkeyeventChannel('hset', db=1)"


@pytest.mark.fixed_client
class TestSubkeyspaceitemChannelClass:
    """Tests for SubkeyspaceitemChannel class."""

    def test_basic_channel(self):
        channel = SubkeyspaceitemChannel("myhash", "myfield", db=0)
        assert str(channel) == "__subkeyspaceitem@0__:myhash\nmyfield"
        assert channel.key_or_pattern == "myhash"
        assert channel.subkey_or_pattern == "myfield"
        assert channel.db == 0

    def test_channel_default_db(self):
        channel = SubkeyspaceitemChannel("myhash", "myfield")
        assert str(channel) == "__subkeyspaceitem@0__:myhash\nmyfield"

    def test_pattern_in_key(self):
        channel = SubkeyspaceitemChannel("hash:*", "myfield")
        assert channel.is_pattern is True

    def test_pattern_in_subkey(self):
        channel = SubkeyspaceitemChannel("myhash", "field:*")
        assert channel.is_pattern is True

    def test_exact_channel(self):
        channel = SubkeyspaceitemChannel("myhash", "myfield")
        assert channel.is_pattern is False

    def test_equality_with_string(self):
        channel = SubkeyspaceitemChannel("myhash", "myfield", db=0)
        assert channel == "__subkeyspaceitem@0__:myhash\nmyfield"

    def test_equality_with_channel(self):
        ch1 = SubkeyspaceitemChannel("myhash", "myfield", db=0)
        ch2 = SubkeyspaceitemChannel("myhash", "myfield", db=0)
        ch3 = SubkeyspaceitemChannel("myhash", "other", db=0)
        assert ch1 == ch2
        assert ch1 != ch3

    def test_repr(self):
        channel = SubkeyspaceitemChannel("myhash", "myfield", db=2)
        assert repr(channel) == "SubkeyspaceitemChannel('myhash', 'myfield', db=2)"


@pytest.mark.fixed_client
class TestSubkeyspaceeventChannelClass:
    """Tests for SubkeyspaceeventChannel class."""

    def test_basic_channel(self):
        channel = SubkeyspaceeventChannel("hset", "myhash", db=0)
        assert str(channel) == "__subkeyspaceevent@0__:hset|myhash"
        assert channel.event == "hset"
        assert channel.key_or_pattern == "myhash"
        assert channel.db == 0

    def test_channel_default_db(self):
        channel = SubkeyspaceeventChannel("hset", "myhash")
        assert str(channel) == "__subkeyspaceevent@0__:hset|myhash"

    def test_pattern_in_event(self):
        channel = SubkeyspaceeventChannel("h*", "myhash")
        assert channel.is_pattern is True

    def test_pattern_in_key(self):
        channel = SubkeyspaceeventChannel("hset", "hash:*")
        assert channel.is_pattern is True

    def test_exact_channel(self):
        channel = SubkeyspaceeventChannel("hset", "myhash")
        assert channel.is_pattern is False

    def test_equality_with_string(self):
        channel = SubkeyspaceeventChannel("hset", "myhash", db=0)
        assert channel == "__subkeyspaceevent@0__:hset|myhash"

    def test_equality_with_channel(self):
        ch1 = SubkeyspaceeventChannel("hset", "myhash", db=0)
        ch2 = SubkeyspaceeventChannel("hset", "myhash", db=0)
        ch3 = SubkeyspaceeventChannel("hdel", "myhash", db=0)
        assert ch1 == ch2
        assert ch1 != ch3

    def test_repr(self):
        channel = SubkeyspaceeventChannel("hset", "myhash", db=1)
        assert repr(channel) == "SubkeyspaceeventChannel('hset', 'myhash', db=1)"


@pytest.mark.fixed_client
class TestSubkeyChannelDetection:
    """Tests for get_channel_type() with subkey channel types."""

    def test_subkeyspace_detection(self):
        assert get_channel_type("__subkeyspace@0__:myhash") == ChannelType.SUBKEYSPACE

    def test_subkeyevent_detection(self):
        assert get_channel_type("__subkeyevent@0__:hset") == ChannelType.SUBKEYEVENT

    def test_subkeyspaceitem_detection(self):
        channel = "__subkeyspaceitem@0__:myhash\nmyfield"
        assert get_channel_type(channel) == ChannelType.SUBKEYSPACEITEM

    def test_subkeyspaceevent_detection(self):
        channel = "__subkeyspaceevent@0__:hset|myhash"
        assert get_channel_type(channel) == ChannelType.SUBKEYSPACEEVENT

    def test_subkeyspace_does_not_match_subkeyspaceitem(self):
        """Ensure __subkeyspaceitem@ is not mistakenly detected as SUBKEYSPACE."""
        channel = "__subkeyspaceitem@0__:myhash\nfield"
        assert get_channel_type(channel) != ChannelType.SUBKEYSPACE

    def test_subkeyspace_does_not_match_subkeyspaceevent(self):
        """Ensure __subkeyspaceevent@ is not mistakenly detected as SUBKEYSPACE."""
        channel = "__subkeyspaceevent@0__:hset|myhash"
        assert get_channel_type(channel) != ChannelType.SUBKEYSPACE

    def test_bytes_input(self):
        assert get_channel_type(b"__subkeyspace@0__:myhash") == ChannelType.SUBKEYSPACE
        assert get_channel_type(b"__subkeyevent@0__:hset") == ChannelType.SUBKEYEVENT

    def test_regular_keyspace_still_works(self):
        assert get_channel_type("__keyspace@0__:mykey") == ChannelType.KEYSPACE
        assert get_channel_type("__keyevent@0__:set") == ChannelType.KEYEVENT


@pytest.mark.fixed_client
class TestSubkeyNotificationParsing:
    """Tests for KeyNotification parsing of subkey channels."""

    @pytest.mark.parametrize("as_bytes", [False, True])
    @pytest.mark.parametrize(
        ("channel", "data"),
        [
            ("__subkeyspace@0__:myhash", "hset"),
            ("__subkeyspace@0__:myhash", "hset|4:abc"),
            ("__subkeyspace@0__:myhash", "hset|3:abc;3:def"),
            ("__subkeyspace@0__:myhash", "hset|x:abc"),
            ("__subkeyevent@0__:hset", "1:k"),
            ("__subkeyevent@0__:hset", "4:abc|3:foo"),
            ("__subkeyspaceitem@0__:myhash", "hset"),
            ("__subkeyspaceevent@0__:hset", "3:foo"),
        ],
    )
    def test_malformed_payload_returns_none(self, channel, data, as_bytes):
        payload = data.encode() if as_bytes else data
        assert KeyNotification.try_parse(channel, payload) is None
        assert (
            KeyNotification.from_message(
                {"type": "pmessage", "channel": channel, "data": payload}
            )
            is None
        )

    # --- subkeyspace ---

    def test_subkeyspace_parse(self):
        n = KeyNotification.try_parse(
            "__subkeyspace@0__:myhash", "hset|5:field,6:field2"
        )
        assert n is not None
        assert n.key == "myhash"
        assert n.event_type == "hset"
        assert n.database == 0
        assert n.is_keyspace is True
        assert n.subkeys == ["field", "field2"]

    def test_subkeyspace_multibyte_subkeys_bytes_payload(self):
        # payload lengths are byte counts: "键键" is 6 bytes in UTF-8
        n = KeyNotification.try_parse(
            "__subkeyspace@0__:myhash", "hset|6:键键,3:abc".encode("utf-8")
        )
        assert n is not None
        assert n.event_type == "hset"
        assert n.subkeys == ["键键", "abc"]

    def test_subkeyspace_multibyte_subkeys_str_payload(self):
        # decode_responses=True delivers the payload as str; the byte lengths
        # must still be honored after re-encoding
        n = KeyNotification.try_parse("__subkeyspace@0__:myhash", "hset|6:键键,3:abc")
        assert n is not None
        assert n.subkeys == ["键键", "abc"]

    def test_subkeyspace_single_subkey(self):
        n = KeyNotification.try_parse("__subkeyspace@0__:myhash", "hdel|3:foo")
        assert n.subkeys == ["foo"]
        assert n.event_type == "hdel"

    def test_subkeyspace_with_key_prefix(self):
        n = KeyNotification.try_parse(
            "__subkeyspace@0__:prefix:myhash",
            "hset|5:field",
            key_prefix="prefix:",
        )
        assert n is not None
        assert n.key == "myhash"
        assert n.subkeys == ["field"]

    def test_subkeyspace_filtered_by_key_prefix(self):
        n = KeyNotification.try_parse(
            "__subkeyspace@0__:other:myhash",
            "hset|5:field",
            key_prefix="prefix:",
        )
        assert n is None

    def test_subkeyspace_wildcard_db(self):
        n = KeyNotification.try_parse("__subkeyspace@*__:myhash", "hset|5:field")
        assert n is not None
        assert n.database == -1

    # --- subkeyevent ---

    def test_subkeyevent_parse(self):
        n = KeyNotification.try_parse(
            "__subkeyevent@0__:hset", "6:myhash|5:field,6:field2"
        )
        assert n is not None
        assert n.key == "myhash"
        assert n.event_type == "hset"
        assert n.database == 0
        assert n.is_keyspace is False
        assert n.subkeys == ["field", "field2"]

    def test_subkeyevent_multibyte_key_bytes_payload(self):
        # key length prefix is a byte count: "键键" is 6 bytes in UTF-8
        n = KeyNotification.try_parse(
            "__subkeyevent@0__:hset", "6:键键|3:abc".encode("utf-8")
        )
        assert n is not None
        assert n.key == "键键"
        assert n.event_type == "hset"
        assert n.subkeys == ["abc"]

    def test_subkeyevent_single_subkey(self):
        n = KeyNotification.try_parse("__subkeyevent@0__:hdel", "6:myhash|3:foo")
        assert n.key == "myhash"
        assert n.subkeys == ["foo"]

    def test_subkeyevent_with_key_prefix(self):
        n = KeyNotification.try_parse(
            "__subkeyevent@0__:hset",
            "13:prefix:myhash|5:field",
            key_prefix="prefix:",
        )
        assert n is not None
        assert n.key == "myhash"

    def test_subkeyevent_filtered_by_key_prefix(self):
        n = KeyNotification.try_parse(
            "__subkeyevent@0__:hset",
            "6:myhash|5:field",
            key_prefix="prefix:",
        )
        assert n is None

    # --- subkeyspaceitem ---

    def test_subkeyspaceitem_parse(self):
        n = KeyNotification.try_parse("__subkeyspaceitem@0__:myhash\nmyfield", "hset")
        assert n is not None
        assert n.key == "myhash"
        assert n.event_type == "hset"
        assert n.database == 0
        assert n.is_keyspace is True
        assert n.subkeys == ["myfield"]

    def test_subkeyspaceitem_with_key_prefix(self):
        n = KeyNotification.try_parse(
            "__subkeyspaceitem@0__:prefix:myhash\nmyfield",
            "hset",
            key_prefix="prefix:",
        )
        assert n is not None
        assert n.key == "myhash"
        assert n.subkeys == ["myfield"]

    def test_subkeyspaceitem_filtered_by_key_prefix(self):
        n = KeyNotification.try_parse(
            "__subkeyspaceitem@0__:other:myhash\nmyfield",
            "hset",
            key_prefix="prefix:",
        )
        assert n is None

    def test_subkeyspaceitem_wildcard_db(self):
        n = KeyNotification.try_parse("__subkeyspaceitem@*__:myhash\nmyfield", "hset")
        assert n is not None
        assert n.database == -1

    # --- subkeyspaceevent ---

    def test_subkeyspaceevent_parse(self):
        n = KeyNotification.try_parse(
            "__subkeyspaceevent@0__:hset|myhash", "5:field,6:field2"
        )
        assert n is not None
        assert n.key == "myhash"
        assert n.event_type == "hset"
        assert n.database == 0
        assert n.is_keyspace is False
        assert n.subkeys == ["field", "field2"]

    def test_subkeyspaceevent_single_subkey(self):
        n = KeyNotification.try_parse("__subkeyspaceevent@0__:hdel|myhash", "3:foo")
        assert n.key == "myhash"
        assert n.event_type == "hdel"
        assert n.subkeys == ["foo"]

    def test_subkeyspaceevent_with_key_prefix(self):
        n = KeyNotification.try_parse(
            "__subkeyspaceevent@0__:hset|prefix:myhash",
            "5:field",
            key_prefix="prefix:",
        )
        assert n is not None
        assert n.key == "myhash"

    def test_subkeyspaceevent_filtered_by_key_prefix(self):
        n = KeyNotification.try_parse(
            "__subkeyspaceevent@0__:hset|other:myhash",
            "5:field",
            key_prefix="prefix:",
        )
        assert n is None

    @pytest.mark.parametrize("key", ["myhash\n", "my\nhash"])
    def test_subkey_channels_key_with_newline(self, key):
        n = KeyNotification.try_parse(f"__subkeyspace@0__:{key}", "hset|5:field")
        assert n is not None
        assert n.key == key
        assert n.subkeys == ["field"]

        n = KeyNotification.try_parse(f"__subkeyspaceevent@0__:hset|{key}", "5:field")
        assert n is not None
        assert n.key == key
        assert n.event_type == "hset"
        assert n.subkeys == ["field"]

    # --- from_message integration ---

    def test_from_message_subkeyspace(self):
        message = {
            "type": "message",
            "pattern": None,
            "channel": "__subkeyspace@0__:myhash",
            "data": "hset|5:field",
        }
        n = KeyNotification.from_message(message)
        assert n is not None
        assert n.key == "myhash"
        assert n.event_type == "hset"
        assert n.subkeys == ["field"]

    def test_from_message_subkeyspaceitem(self):
        message = {
            "type": "message",
            "pattern": None,
            "channel": "__subkeyspaceitem@0__:myhash\nmyfield",
            "data": "hset",
        }
        n = KeyNotification.from_message(message)
        assert n is not None
        assert n.key == "myhash"
        assert n.event_type == "hset"
        assert n.subkeys == ["myfield"]


@skip_if_server_version_lt("8.7.2")
class TestSubkeyNotifications:
    """
    Integration tests for subkey keyspace notifications.

    These tests require a Redis server with subkey notification support
    (Redis >= 8.7.2) and keyspace/subkey notifications enabled via the
    ``r_with_subkey_notifications`` fixture.

    Works for both standalone Redis and RedisCluster.
    """

    @staticmethod
    def _drain_subscribe_messages(notifications):
        """Drain subscribe/psubscribe confirmation messages so the next
        ``get_message`` call returns a real notification.

        Works for both standalone (``_pubsub``) and cluster
        (``_node_pubsubs``) notification managers.
        """
        pubsubs = (
            list(notifications._node_pubsubs.values())
            if hasattr(notifications, "_node_pubsubs")
            else [notifications._pubsub]
        )
        for pubsub in pubsubs:
            # One ack per subscribed channel or pattern. Reading exactly that
            # many, each with a generous timeout, cannot stop early on a slow
            # runner and leave an ack for the test to read.
            for _ in range(len(pubsub.channels) + len(pubsub.patterns)):
                msg = pubsub.get_message(timeout=2.0)
                assert msg is not None, "subscribe ack not received"
                assert msg["type"] in ("subscribe", "psubscribe"), msg

    def test_create_hash_field_subkeyspace_notification(
        self, r_with_subkey_notifications
    ):
        """Create a hash field and verify that the Subkeyspace notification
        contains the field that was created."""
        r = r_with_subkey_notifications
        notifications = r.keyspace_notifications()
        notifications.subscribe_subkeyspace("test:hash1")
        self._drain_subscribe_messages(notifications)

        r.hset("test:hash1", "field1", "value1")

        msg = notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash1"
        assert msg.event_type == "hset"
        assert "field1" in msg.subkeys

        notifications.close()
        r.delete("test:hash1")

    def test_update_hash_field_subkeyspace_notification(
        self, r_with_subkey_notifications
    ):
        """Update an existing hash field and verify the Subkeyspace notification."""
        r = r_with_subkey_notifications
        r.hset("test:hash2", "field1", "initial")

        notifications = r.keyspace_notifications()
        notifications.subscribe_subkeyspace("test:hash2")
        self._drain_subscribe_messages(notifications)

        r.hset("test:hash2", "field1", "updated")

        msg = notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash2"
        assert msg.event_type == "hset"
        assert "field1" in msg.subkeys

        notifications.close()
        r.delete("test:hash2")

    def test_update_hash_field_subkeyspaceitem_notification(
        self, r_with_subkey_notifications
    ):
        """Update a hash field and verify the Subkeyspaceitem notification
        is received for the specific field."""
        r = r_with_subkey_notifications
        r.hset("test:hash3", "myfield", "initial")

        notifications = r.keyspace_notifications()
        notifications.subscribe_subkeyspaceitem("test:hash3", "myfield")
        self._drain_subscribe_messages(notifications)

        r.hset("test:hash3", "myfield", "updated")

        msg = notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash3"
        assert msg.event_type == "hset"
        assert msg.subkeys == ["myfield"]

        notifications.close()
        r.delete("test:hash3")

    def test_delete_hash_field_subkeyevent_notification(
        self, r_with_subkey_notifications
    ):
        """Delete a hash field and verify the Subkeyevent notification
        is received for the hdel event."""
        r = r_with_subkey_notifications
        r.hset("test:hash4", "field1", "value1")

        notifications = r.keyspace_notifications()
        notifications.subscribe_subkeyevent("hdel")
        self._drain_subscribe_messages(notifications)

        r.hdel("test:hash4", "field1")

        msg = notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash4"
        assert msg.event_type == "hdel"
        assert "field1" in msg.subkeys

        notifications.close()
        r.delete("test:hash4")

    def test_delete_hash_field_subkeyspaceevent_notification(
        self, r_with_subkey_notifications
    ):
        """Delete a hash field and verify the Subkeyspaceevent notification
        is received for the specific key and event."""
        r = r_with_subkey_notifications
        r.hset("test:hash5", "field1", "value1")

        notifications = r.keyspace_notifications()
        notifications.subscribe_subkeyspaceevent("hdel", "test:hash5")
        self._drain_subscribe_messages(notifications)

        r.hdel("test:hash5", "field1")

        msg = notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash5"
        assert msg.event_type == "hdel"
        assert "field1" in msg.subkeys

        notifications.close()
        r.delete("test:hash5")

    def test_multiple_hash_fields_subkeyspace_notification(
        self, r_with_subkey_notifications
    ):
        """Create multiple hash fields at once and verify subkeyspace
        notification contains all affected fields."""
        r = r_with_subkey_notifications
        notifications = r.keyspace_notifications()
        notifications.subscribe_subkeyspace("test:hash6")
        self._drain_subscribe_messages(notifications)

        r.hset("test:hash6", mapping={"f1": "v1", "f2": "v2", "f3": "v3"})

        msg = notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash6"
        assert msg.event_type == "hset"
        assert len(msg.subkeys) == 3
        assert set(msg.subkeys) == {"f1", "f2", "f3"}

        notifications.close()
        r.delete("test:hash6")

    def test_subkeyspace_pattern_subscription(self, r_with_subkey_notifications):
        """Subscribe to a subkeyspace pattern and verify notifications are
        received for matching keys."""
        r = r_with_subkey_notifications
        notifications = r.keyspace_notifications()
        notifications.subscribe_subkeyspace("test:pattern:*")
        self._drain_subscribe_messages(notifications)

        r.hset("test:pattern:hash1", "field1", "value1")
        r.hset("test:pattern:hash2", "field2", "value2")

        messages = []
        for _ in range(2):
            msg = notifications.get_message(timeout=2.0)
            assert msg is not None
            messages.append(msg)

        keys = {m.key for m in messages}
        assert "test:pattern:hash1" in keys
        assert "test:pattern:hash2" in keys

        notifications.close()
        r.delete("test:pattern:hash1", "test:pattern:hash2")

    def test_subkeyevent_pattern_subscription(self, r_with_subkey_notifications):
        """Subscribe to a subkeyevent pattern for all hash events and
        verify notifications are received."""
        r = r_with_subkey_notifications
        notifications = r.keyspace_notifications()
        notifications.subscribe_subkeyevent("h*")
        self._drain_subscribe_messages(notifications)

        r.hset("test:hash7", "field1", "value1")
        r.hdel("test:hash7", "field1")

        messages = []
        for _ in range(2):
            msg = notifications.get_message(timeout=2.0)
            assert msg is not None
            messages.append(msg)

        event_types = {m.event_type for m in messages}
        assert "hset" in event_types
        assert "hdel" in event_types

        notifications.close()
        r.delete("test:hash7")

    def test_subkeyspaceitem_does_not_receive_other_fields(
        self, r_with_subkey_notifications
    ):
        """Subscribe to a specific subkeyspaceitem and verify that
        modifications to other fields do not trigger notifications."""
        r = r_with_subkey_notifications
        r.hset("test:hash8", "watched_field", "initial")

        notifications = r.keyspace_notifications()
        notifications.subscribe_subkeyspaceitem("test:hash8", "watched_field")
        self._drain_subscribe_messages(notifications)

        # Modify a different field, then the watched one. Notifications arrive
        # in order, so if the first write had notified, it would be read first.
        r.hset("test:hash8", "other_field", "value")
        r.hset("test:hash8", "watched_field", "updated")
        msg = notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.subkeys == ["watched_field"]

        notifications.close()
        r.delete("test:hash8")

    def test_combined_keyspace_and_subkeyspace(self, r_with_subkey_notifications):
        """Subscribe to both keyspace and subkeyspace on the same key and
        verify that both types of notifications are received."""
        r = r_with_subkey_notifications

        notifications = r.keyspace_notifications()
        notifications.subscribe_keyspace("test:hash9")
        notifications.subscribe_subkeyspace("test:hash9")
        self._drain_subscribe_messages(notifications)

        r.hset("test:hash9", "field1", "value1")

        messages = []
        for _ in range(2):
            msg = notifications.get_message(timeout=2.0)
            if msg is not None:
                messages.append(msg)

        # Should receive both keyspace (hset event, no subkeys) and
        # subkeyspace (hset event, with subkeys) notifications
        assert len(messages) == 2
        has_subkeys = any(len(m.subkeys) > 0 for m in messages)
        has_no_subkeys = any(len(m.subkeys) == 0 for m in messages)
        assert has_subkeys
        assert has_no_subkeys

        notifications.close()
        r.delete("test:hash9")

    def test_subkeyspaceitem_pattern_receives_matching_fields(
        self, r_with_subkey_notifications
    ):
        """Subscribe to subkeyspaceitem with a pattern subkey (field*) and
        verify that notifications are received for field1, field2, and field3
        but not for unrelated fields."""
        r = r_with_subkey_notifications

        notifications = r.keyspace_notifications()
        notifications.subscribe_subkeyspaceitem("test:hash10", "field*")
        self._drain_subscribe_messages(notifications)

        # These should all match the pattern
        r.hset("test:hash10", "field1", "value1")
        r.hset("test:hash10", "field2", "value2")
        r.hset("test:hash10", "field3", "value3")

        messages = []
        for _ in range(3):
            msg = notifications.get_message(timeout=2.0)
            assert msg is not None
            messages.append(msg)

        received_subkeys = [m.subkeys[0] for m in messages]
        assert "field1" in received_subkeys
        assert "field2" in received_subkeys
        assert "field3" in received_subkeys

        for msg in messages:
            assert msg.key == "test:hash10"
            assert msg.event_type == "hset"

        # A non-matching field should NOT produce a notification. Write it
        # before a matching one: if it notified, it would be read first.
        r.hset("test:hash10", "other", "value")
        r.hset("test:hash10", "field4", "value4")
        msg = notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.subkeys == ["field4"]

        notifications.close()
        r.delete("test:hash10")
