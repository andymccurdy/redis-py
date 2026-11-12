"""
Tests for async Redis keyspace notifications support.
"""

import asyncio

import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, PropertyMock

from redis.asyncio.keyspace_notifications import (
    AsyncClusterKeyspaceNotifications,
    AsyncKeyspaceNotifications,
    _ClusterNodePoolAdapter,
)
from redis.exceptions import ConnectionError
from tests.conftest import skip_if_server_version_lt
from redis.keyspace_notifications import (
    EventType,
    KeyspaceChannel,
)


@pytest.mark.fixed_client
class TestAsyncStandaloneKeyspaceNotificationsMocked:
    """
    Mock-based unit tests for AsyncKeyspaceNotifications (standalone Redis).
    """

    def _create_mock_redis(self):
        """Create a mock async Redis client with pubsub."""
        redis_client = Mock()
        mock_pubsub = MagicMock()
        mock_pubsub.get_message = AsyncMock(return_value=None)
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.psubscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.punsubscribe = AsyncMock()
        mock_pubsub.aclose = AsyncMock()
        # Configure subscribed as a PropertyMock since standalone delegates to it
        type(mock_pubsub).subscribed = PropertyMock(return_value=False)
        redis_client.pubsub = Mock(return_value=mock_pubsub)
        return redis_client, mock_pubsub

    @pytest.mark.asyncio
    async def test_subscribe_exact_channel(self):
        """Test subscribing to an exact keyspace channel."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel)

        # Verify subscribe was called with kwargs (exact channel, not pattern)
        mock_pubsub.subscribe.assert_called_once()
        assert "__keyspace@0__:mykey" in mock_pubsub.subscribe.call_args.kwargs

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_pattern_channel(self):
        """Test subscribing to a pattern keyspace channel."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("user:*", db=0)
        await notifications.subscribe(channel)

        # Verify psubscribe was called with kwargs (pattern subscription)
        mock_pubsub.psubscribe.assert_called_once()
        assert "__keyspace@0__:user:*" in mock_pubsub.psubscribe.call_args.kwargs

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_keyevent(self):
        """Test subscribing to a keyevent channel."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        await notifications.subscribe_keyevent(EventType.SET, db=0)

        # KeyeventChannel with "set" is exact, not pattern
        mock_pubsub.subscribe.assert_called_once()
        assert "__keyevent@0__:set" in mock_pubsub.subscribe.call_args.kwargs

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_keyspace_convenience(self):
        """Test the subscribe_keyspace convenience method."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        await notifications.subscribe_keyspace("cache:*", db=0)

        # Pattern should use psubscribe with kwargs
        mock_pubsub.psubscribe.assert_called_once()
        assert "__keyspace@0__:cache:*" in mock_pubsub.psubscribe.call_args.kwargs

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_unsubscribe(self):
        """Test unsubscribing from channels."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)

        # Subscribe to exact and pattern channels
        exact_channel = KeyspaceChannel("mykey", db=0)
        pattern_channel = KeyspaceChannel("user:*", db=0)
        await notifications.subscribe(exact_channel)
        await notifications.subscribe(pattern_channel)

        # Unsubscribe
        await notifications.unsubscribe(exact_channel)
        await notifications.unsubscribe(pattern_channel)

        mock_pubsub.unsubscribe.assert_called_once()
        mock_pubsub.punsubscribe.assert_called_once()

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_get_message_returns_notification(self):
        """Test that get_message returns KeyNotification objects."""
        redis_client, mock_pubsub = self._create_mock_redis()

        # Setup mock to return a keyspace message
        mock_pubsub.get_message.return_value = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"set",
            "pattern": None,
        }

        notifications = AsyncKeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel)

        notification = await notifications.get_message(timeout=1.0)

        assert notification is not None
        assert notification.key == "mykey"
        assert notification.event_type == "set"
        assert notification.database == 0

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_get_message_with_pattern(self):
        """Test get_message with pattern subscription."""
        redis_client, mock_pubsub = self._create_mock_redis()

        # Setup mock to return a pmessage
        mock_pubsub.get_message.return_value = {
            "type": "pmessage",
            "pattern": b"__keyspace@0__:user:*",
            "channel": b"__keyspace@0__:user:123",
            "data": b"set",
        }

        notifications = AsyncKeyspaceNotifications(redis_client)
        await notifications.subscribe(KeyspaceChannel("user:*", db=0))

        notification = await notifications.get_message(timeout=1.0)

        assert notification is not None
        assert notification.key == "user:123"
        assert notification.event_type == "set"

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_get_message_returns_none_when_closed(self):
        """Test that get_message returns None when notifications are closed."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        await notifications.aclose()

        notification = await notifications.get_message(timeout=1.0)
        assert notification is None

    @pytest.mark.asyncio
    async def test_handler_callback(self):
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

        async def capture_subscribe(**kwargs):
            registered_handlers.update(kwargs)

        mock_pubsub.subscribe.side_effect = capture_subscribe

        # When get_message is called, simulate pubsub behavior:
        # call the handler and return None (like real pubsub does)
        async def mock_get_message(**_kwargs):
            channel_key = "__keyspace@0__:mykey"
            if channel_key in registered_handlers and registered_handlers[channel_key]:
                registered_handlers[channel_key](test_message)
                return None  # Handler consumed the message
            return test_message

        mock_pubsub.get_message.side_effect = mock_get_message

        notifications = AsyncKeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel, handler=handler)

        # get_message should return None because handler consumed it
        result = await notifications.get_message(timeout=1.0)
        assert result is None

        # Handler should have been called with KeyNotification
        assert len(received) == 1
        assert received[0].key == "mykey"
        assert received[0].event_type == "set"

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_async_handler_callback(self):
        """Test that async handlers are properly awaited."""
        redis_client, mock_pubsub = self._create_mock_redis()

        received = []

        async def async_handler(notification):
            await asyncio.sleep(0)  # Simulate async work
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

        async def capture_subscribe(**kwargs):
            registered_handlers.update(kwargs)

        mock_pubsub.subscribe.side_effect = capture_subscribe

        # When get_message is called, simulate pubsub behavior:
        # call the handler and return None (like real pubsub does)
        async def mock_get_message(**_kwargs):
            channel_key = "__keyspace@0__:mykey"
            if channel_key in registered_handlers and registered_handlers[channel_key]:
                handler = registered_handlers[channel_key]
                result = handler(test_message)
                # Await if the handler is async
                if hasattr(result, "__await__"):
                    await result
                return None  # Handler consumed the message
            return test_message

        mock_pubsub.get_message.side_effect = mock_get_message

        notifications = AsyncKeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel, handler=async_handler)

        # get_message should return None because handler consumed it
        result = await notifications.get_message(timeout=1.0)
        assert result is None

        # Handler should have been called with KeyNotification
        assert len(received) == 1
        assert received[0].key == "mykey"
        assert received[0].event_type == "set"

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test AsyncKeyspaceNotifications as async context manager."""
        redis_client, mock_pubsub = self._create_mock_redis()

        async with AsyncKeyspaceNotifications(redis_client) as notifications:
            type(mock_pubsub).subscribed = PropertyMock(return_value=True)
            await notifications.subscribe(KeyspaceChannel("mykey"))
            assert notifications.subscribed

        # Should be closed after exiting context
        mock_pubsub.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribed_property(self):
        """Test the subscribed property."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        assert not notifications.subscribed

        type(mock_pubsub).subscribed = PropertyMock(return_value=True)
        await notifications.subscribe(KeyspaceChannel("mykey"))
        assert notifications.subscribed

        type(mock_pubsub).subscribed = PropertyMock(return_value=False)
        await notifications.unsubscribe(KeyspaceChannel("mykey"))
        assert not notifications.subscribed

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_key_prefix_filtering(self):
        """Test that key_prefix filters notifications."""
        redis_client, mock_pubsub = self._create_mock_redis()

        # Setup mock to return a notification for a key without the prefix
        mock_pubsub.get_message.return_value = {
            "type": "message",
            "channel": b"__keyspace@0__:other:key",
            "data": b"set",
            "pattern": None,
        }

        notifications = AsyncKeyspaceNotifications(redis_client, key_prefix="myapp:")
        await notifications.subscribe(KeyspaceChannel("*"))

        # Should be filtered out (returns None)
        notification = await notifications.get_message(timeout=0.1)
        assert notification is None

        await notifications.aclose()


@pytest.mark.fixed_client
class TestAsyncClusterKeyspaceNotificationsMocked:
    """
    Mock-based unit tests for AsyncClusterKeyspaceNotifications.
    """

    def _create_mock_node(self, name, host, port, server_type="primary"):
        """Create a mock ClusterNode."""
        node = Mock()
        node.name = name
        node.host = host
        node.port = port
        node.server_type = server_type

        # Create a mock pubsub
        mock_pubsub = MagicMock()
        mock_pubsub.get_message = AsyncMock(return_value=None)
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.psubscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.punsubscribe = AsyncMock()
        mock_pubsub.aclose = AsyncMock()

        return node, mock_pubsub

    def _create_mock_cluster(self, nodes, pubsubs_by_node):
        """Create a mock async RedisCluster with the given nodes."""
        cluster = Mock()
        # Filter to only primary nodes
        primary_nodes = [n for n in nodes if n.server_type == "primary"]
        cluster.get_nodes = Mock(return_value=nodes)
        cluster.get_primaries = Mock(return_value=primary_nodes)

        # Mock connection_kwargs for creating standalone clients
        cluster.connection_kwargs = {}

        # Mock nodes_manager
        nodes_manager = Mock()
        cluster.nodes_manager = nodes_manager

        return cluster, pubsubs_by_node

    @pytest.mark.asyncio
    async def test_subscribe_exact_channel_on_all_nodes(self):
        """Test subscribing to an exact channel creates subscriptions on all nodes."""
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)
        node2, pubsub2 = self._create_mock_node("127.0.0.1:7001", "127.0.0.1", 7001)

        pubsubs_by_node = {node1.name: pubsub1, node2.name: pubsub2}
        cluster, _ = self._create_mock_cluster([node1, node2], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)

        # Manually inject the mock pubsubs
        notifications._node_pubsubs = pubsubs_by_node.copy()

        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel)

        # Verify subscribe was called on both pubsubs
        pubsub1.subscribe.assert_called_once()
        pubsub2.subscribe.assert_called_once()

        assert "__keyspace@0__:mykey" in pubsub1.subscribe.call_args.kwargs
        assert "__keyspace@0__:mykey" in pubsub2.subscribe.call_args.kwargs

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_pattern_on_all_nodes(self):
        """Test subscribing to a pattern creates psubscriptions on all nodes."""
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)
        node2, pubsub2 = self._create_mock_node("127.0.0.1:7001", "127.0.0.1", 7001)

        pubsubs_by_node = {node1.name: pubsub1, node2.name: pubsub2}
        cluster, _ = self._create_mock_cluster([node1, node2], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)

        # Manually inject the mock pubsubs
        notifications._node_pubsubs = pubsubs_by_node.copy()

        channel = KeyspaceChannel("user:*", db=0)
        await notifications.subscribe(channel)

        # Verify psubscribe was called on both pubsubs (patterns use psubscribe)
        pubsub1.psubscribe.assert_called_once()
        pubsub2.psubscribe.assert_called_once()

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_receives_notification_from_any_node(self):
        """Test that notifications can be received from any primary node."""
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)
        node2, pubsub2 = self._create_mock_node("127.0.0.1:7001", "127.0.0.1", 7001)

        pubsubs_by_node = {node1.name: pubsub1, node2.name: pubsub2}
        cluster, _ = self._create_mock_cluster([node1, node2], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)
        notifications._node_pubsubs = pubsubs_by_node.copy()

        # Subscribe first
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel)

        # --- Notification from node1 ---
        msg_from_node1 = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"set",
            "pattern": None,
        }
        pubsub1.get_message.return_value = msg_from_node1
        pubsub2.get_message.return_value = None

        notification = await notifications.get_message(timeout=1.0)

        assert notification is not None
        assert notification.key == "mykey"
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

        notification = await notifications.get_message(timeout=1.0)

        assert notification is not None
        assert notification.key == "mykey"
        assert notification.event_type == EventType.DEL

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test AsyncClusterKeyspaceNotifications as async context manager."""
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)

        pubsubs_by_node = {node1.name: pubsub1}
        cluster, _ = self._create_mock_cluster([node1], pubsubs_by_node)

        async with AsyncClusterKeyspaceNotifications(cluster) as notifications:
            # Manually inject the mock pubsub for testing
            notifications._node_pubsubs = pubsubs_by_node.copy()

            await notifications.subscribe(KeyspaceChannel("mykey"))
            assert notifications.subscribed

        # Should have closed pubsub
        pubsub1.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribed_property(self):
        """Test the subscribed property for cluster notifications."""
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)

        pubsubs_by_node = {node1.name: pubsub1}
        cluster, _ = self._create_mock_cluster([node1], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)
        notifications._node_pubsubs = pubsubs_by_node.copy()

        assert not notifications.subscribed

        await notifications.subscribe(KeyspaceChannel("mykey"))
        assert notifications.subscribed

        await notifications.unsubscribe(KeyspaceChannel("mykey"))
        assert not notifications.subscribed

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_ensure_node_pubsub_uses_adapter(self):
        """
        Test that _ensure_node_pubsub creates a PubSub backed by a
        _ClusterNodePoolAdapter wrapping the ClusterNode, instead of
        creating a standalone Redis client.
        """
        node, _ = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)

        # Set up connection_kwargs as ClusterNode does
        node.connection_kwargs = {
            "host": "127.0.0.1",
            "port": 7000,
            "password": "secret",
        }
        node.acquire_connection = Mock(return_value=MagicMock())
        node.release = Mock()

        cluster = Mock()
        cluster.get_primaries = Mock(return_value=[node])
        cluster.connection_kwargs = {}

        notifications = AsyncClusterKeyspaceNotifications(cluster)
        pubsub = await notifications._ensure_node_pubsub(node)

        # The PubSub's connection_pool should be a _ClusterNodePoolAdapter
        assert isinstance(pubsub.connection_pool, _ClusterNodePoolAdapter)
        # The adapter should wrap the original node
        assert pubsub.connection_pool._node is node
        # The adapter should expose the node's connection_kwargs
        assert pubsub.connection_pool.connection_kwargs is node.connection_kwargs

        # Calling again returns the same PubSub (cached)
        pubsub2 = await notifications._ensure_node_pubsub(node)
        assert pubsub2 is pubsub

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_refresh_subscriptions_recovers_broken_connections(self):
        """
        Test that refresh_subscriptions detects and re-creates broken pubsub connections.

        When an existing primary node's pubsub connection breaks (e.g., transient
        network issue) but the node remains a primary, the broken pubsub should be
        re-created during refresh.
        """
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)
        node2, pubsub2 = self._create_mock_node("127.0.0.1:7001", "127.0.0.1", 7001)

        pubsubs_by_node = {node1.name: pubsub1, node2.name: pubsub2}
        cluster, _ = self._create_mock_cluster([node1, node2], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)

        # Manually inject the mock pubsubs
        notifications._node_pubsubs = pubsubs_by_node.copy()

        # Subscribe first
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel)
        notifications._subscribed_channels = {"__keyspace@0__:mykey": None}

        # Simulate broken connection on node1 by setting is_connected to False
        mock_connection = Mock()
        mock_connection.is_connected = False
        pubsub1.connection = mock_connection

        # node2 has a working connection
        mock_connection2 = Mock()
        mock_connection2.is_connected = True
        pubsub2.connection = mock_connection2

        # Create a new pubsub for the re-creation
        new_pubsub1 = MagicMock()
        new_pubsub1.get_message = AsyncMock(return_value=None)
        new_pubsub1.subscribe = AsyncMock()
        new_pubsub1.psubscribe = AsyncMock()
        new_pubsub1.aclose = AsyncMock()

        # Mock the _ensure_node_pubsub to return new pubsub for node1
        original_ensure = notifications._ensure_node_pubsub

        async def mock_ensure(node):
            if node.name == node1.name:
                notifications._node_pubsubs[node.name] = new_pubsub1
                return new_pubsub1
            return await original_ensure(node)

        notifications._ensure_node_pubsub = mock_ensure

        # Call refresh_subscriptions
        await notifications.refresh_subscriptions()

        # Verify node1's pubsub was replaced
        assert notifications._node_pubsubs[node1.name] is new_pubsub1

        # Verify the new pubsub was subscribed
        new_pubsub1.subscribe.assert_called()

        # node2 should remain unchanged (connection was working)
        assert notifications._node_pubsubs[node2.name] is pubsub2

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_is_pubsub_connected_returns_false_for_broken_connection(self):
        """
        Test that _is_pubsub_connected correctly detects broken connections.
        """
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)

        pubsubs_by_node = {node1.name: pubsub1}
        cluster, _ = self._create_mock_cluster([node1], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)

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

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_node_failure_during_pattern_subscribe_does_not_lose_patterns(self):
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
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)
        node2, pubsub2 = self._create_mock_node("127.0.0.1:7001", "127.0.0.1", 7001)

        pubsubs_by_node = {node1.name: pubsub1, node2.name: pubsub2}
        cluster, _ = self._create_mock_cluster([node1, node2], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)

        # Mock _ensure_node_pubsub to return our mock pubsubs and
        # populate _node_pubsubs like the real implementation does
        async def mock_ensure(node):
            notifications._node_pubsubs[node.name] = pubsubs_by_node[node.name]
            return pubsubs_by_node[node.name]

        notifications._ensure_node_pubsub = mock_ensure

        # Make node2's pubsub raise on the first psubscribe call
        # (simulating a transient failure during pattern subscription)
        pubsub2.psubscribe.side_effect = ConnectionError("connection lost")

        # Subscribe with both a pattern and an exact channel in one call
        pattern = KeyspaceChannel("user:*")  # pattern → psubscribe
        exact = KeyspaceChannel("mykey")  # exact   → subscribe
        await notifications.subscribe(pattern, exact)

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

        await notifications.aclose()


@skip_if_server_version_lt("8.7.2")
class TestAsyncSubkeyNotifications:
    """
    Integration tests for subkey keyspace notifications with an async
    Redis client.

    These tests require a Redis server with subkey notification support
    (Redis >= 8.7.2) and keyspace notifications enabled.

    Works for both standalone Redis and RedisCluster.
    """

    @staticmethod
    async def _drain_subscribe_messages(notifications):
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
                msg = await pubsub.get_message(timeout=2.0)
                assert msg is not None, "subscribe ack not received"
                assert msg["type"] in ("subscribe", "psubscribe"), msg

    @pytest.mark.asyncio
    async def test_create_hash_field_subkeyspace_notification(
        self, async_r_with_subkey_notifications
    ):
        """Create a hash field and verify that the Subkeyspace notification
        contains the field that was created."""
        r = async_r_with_subkey_notifications
        notifications = r.keyspace_notifications()
        await notifications.subscribe_subkeyspace("test:hash1")
        await self._drain_subscribe_messages(notifications)

        await r.hset("test:hash1", "field1", "value1")

        msg = await notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash1"
        assert msg.event_type == "hset"
        assert "field1" in msg.subkeys

        await notifications.aclose()
        await r.delete("test:hash1")

    @pytest.mark.asyncio
    async def test_update_hash_field_subkeyspace_notification(
        self, async_r_with_subkey_notifications
    ):
        """Update an existing hash field and verify the Subkeyspace notification."""
        r = async_r_with_subkey_notifications
        await r.hset("test:hash2", "field1", "initial")

        notifications = r.keyspace_notifications()
        await notifications.subscribe_subkeyspace("test:hash2")
        await self._drain_subscribe_messages(notifications)

        await r.hset("test:hash2", "field1", "updated")

        msg = await notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash2"
        assert msg.event_type == "hset"
        assert "field1" in msg.subkeys

        await notifications.aclose()
        await r.delete("test:hash2")

    @pytest.mark.asyncio
    async def test_update_hash_field_subkeyspaceitem_notification(
        self, async_r_with_subkey_notifications
    ):
        """Update a hash field and verify the Subkeyspaceitem notification
        is received for the specific field."""
        r = async_r_with_subkey_notifications
        await r.hset("test:hash3", "myfield", "initial")

        notifications = r.keyspace_notifications()
        await notifications.subscribe_subkeyspaceitem("test:hash3", "myfield")
        await self._drain_subscribe_messages(notifications)

        await r.hset("test:hash3", "myfield", "updated")

        msg = await notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash3"
        assert msg.event_type == "hset"
        assert msg.subkeys == ["myfield"]

        await notifications.aclose()
        await r.delete("test:hash3")

    @pytest.mark.asyncio
    async def test_delete_hash_field_subkeyevent_notification(
        self, async_r_with_subkey_notifications
    ):
        """Delete a hash field and verify the Subkeyevent notification
        is received for the hdel event."""
        r = async_r_with_subkey_notifications
        await r.hset("test:hash4", "field1", "value1")

        notifications = r.keyspace_notifications()
        await notifications.subscribe_subkeyevent("hdel")
        await self._drain_subscribe_messages(notifications)

        await r.hdel("test:hash4", "field1")

        msg = await notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash4"
        assert msg.event_type == "hdel"
        assert "field1" in msg.subkeys

        await notifications.aclose()
        await r.delete("test:hash4")

    @pytest.mark.asyncio
    async def test_delete_hash_field_subkeyspaceevent_notification(
        self, async_r_with_subkey_notifications
    ):
        """Delete a hash field and verify the Subkeyspaceevent notification
        is received for the specific key and event."""
        r = async_r_with_subkey_notifications
        await r.hset("test:hash5", "field1", "value1")

        notifications = r.keyspace_notifications()
        await notifications.subscribe_subkeyspaceevent("hdel", "test:hash5")
        await self._drain_subscribe_messages(notifications)

        await r.hdel("test:hash5", "field1")

        msg = await notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash5"
        assert msg.event_type == "hdel"
        assert "field1" in msg.subkeys

        await notifications.aclose()
        await r.delete("test:hash5")

    @pytest.mark.asyncio
    async def test_create_key_keyspace_notification_still_works(
        self, async_r_with_subkey_notifications
    ):
        """Create a key and verify that a regular keyspace notification
        is received (backward compatibility)."""
        r = async_r_with_subkey_notifications
        notifications = r.keyspace_notifications()
        await notifications.subscribe_keyspace("test:simple1")
        await self._drain_subscribe_messages(notifications)

        await r.set("test:simple1", "value1")

        msg = await notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:simple1"
        assert msg.event_type == "set"
        assert msg.subkeys == []

        await notifications.aclose()
        await r.delete("test:simple1")

    @pytest.mark.asyncio
    async def test_update_key_keyspace_notification(
        self, async_r_with_subkey_notifications
    ):
        """Update a key and verify the keyspace notification is received."""
        r = async_r_with_subkey_notifications
        await r.set("test:simple2", "initial")

        notifications = r.keyspace_notifications()
        await notifications.subscribe_keyspace("test:simple2")
        await self._drain_subscribe_messages(notifications)

        await r.set("test:simple2", "updated")

        msg = await notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:simple2"
        assert msg.event_type == "set"

        await notifications.aclose()
        await r.delete("test:simple2")

    @pytest.mark.asyncio
    async def test_delete_key_keyspace_notification(
        self, async_r_with_subkey_notifications
    ):
        """Delete a key and verify that the deletion notification is received."""
        r = async_r_with_subkey_notifications
        await r.set("test:simple3", "value")

        notifications = r.keyspace_notifications()
        await notifications.subscribe_keyspace("test:simple3")
        await self._drain_subscribe_messages(notifications)

        await r.delete("test:simple3")

        msg = await notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:simple3"
        assert msg.event_type == "del"

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_multiple_hash_fields_subkeyspace_notification(
        self, async_r_with_subkey_notifications
    ):
        """Create multiple hash fields at once and verify subkeyspace
        notification contains all affected fields."""
        r = async_r_with_subkey_notifications
        notifications = r.keyspace_notifications()
        await notifications.subscribe_subkeyspace("test:hash6")
        await self._drain_subscribe_messages(notifications)

        await r.hset("test:hash6", mapping={"f1": "v1", "f2": "v2", "f3": "v3"})

        msg = await notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.key == "test:hash6"
        assert msg.event_type == "hset"
        assert len(msg.subkeys) == 3
        assert set(msg.subkeys) == {"f1", "f2", "f3"}

        await notifications.aclose()
        await r.delete("test:hash6")

    @pytest.mark.asyncio
    async def test_subkeyspace_pattern_subscription(
        self, async_r_with_subkey_notifications
    ):
        """Subscribe to a subkeyspace pattern and verify notifications are
        received for matching keys."""
        r = async_r_with_subkey_notifications
        notifications = r.keyspace_notifications()
        await notifications.subscribe_subkeyspace("test:pattern:*")
        await self._drain_subscribe_messages(notifications)

        await r.hset("test:pattern:hash1", "field1", "value1")
        await r.hset("test:pattern:hash2", "field2", "value2")

        messages = []
        for _ in range(2):
            msg = await notifications.get_message(timeout=2.0)
            assert msg is not None
            messages.append(msg)

        keys = {m.key for m in messages}
        assert "test:pattern:hash1" in keys
        assert "test:pattern:hash2" in keys

        await notifications.aclose()
        await r.delete("test:pattern:hash1", "test:pattern:hash2")

    @pytest.mark.asyncio
    async def test_subkeyevent_pattern_subscription(
        self, async_r_with_subkey_notifications
    ):
        """Subscribe to a subkeyevent pattern for all hash events and
        verify notifications are received."""
        r = async_r_with_subkey_notifications
        notifications = r.keyspace_notifications()
        await notifications.subscribe_subkeyevent("h*")
        await self._drain_subscribe_messages(notifications)

        await r.hset("test:hash7", "field1", "value1")
        await r.hdel("test:hash7", "field1")

        messages = []
        for _ in range(2):
            msg = await notifications.get_message(timeout=2.0)
            assert msg is not None
            messages.append(msg)

        event_types = {m.event_type for m in messages}
        assert "hset" in event_types
        assert "hdel" in event_types

        await notifications.aclose()
        await r.delete("test:hash7")

    @pytest.mark.asyncio
    async def test_subkeyspaceitem_does_not_receive_other_fields(
        self, async_r_with_subkey_notifications
    ):
        """Subscribe to a specific subkeyspaceitem and verify that
        modifications to other fields do not trigger notifications."""
        r = async_r_with_subkey_notifications
        await r.hset("test:hash8", "watched_field", "initial")

        notifications = r.keyspace_notifications()
        await notifications.subscribe_subkeyspaceitem("test:hash8", "watched_field")
        await self._drain_subscribe_messages(notifications)

        # Modify a different field, then the watched one. Notifications arrive
        # in order, so if the first write had notified, it would be read first.
        await r.hset("test:hash8", "other_field", "value")
        await r.hset("test:hash8", "watched_field", "updated")
        msg = await notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.subkeys == ["watched_field"]

        await notifications.aclose()
        await r.delete("test:hash8")

    @pytest.mark.asyncio
    async def test_combined_keyspace_and_subkeyspace(
        self, async_r_with_subkey_notifications
    ):
        """Subscribe to both keyspace and subkeyspace on the same key and
        verify that both types of notifications are received."""
        r = async_r_with_subkey_notifications

        notifications = r.keyspace_notifications()
        await notifications.subscribe_keyspace("test:hash9")
        await notifications.subscribe_subkeyspace("test:hash9")
        await self._drain_subscribe_messages(notifications)

        await r.hset("test:hash9", "field1", "value1")

        messages = []
        for _ in range(2):
            msg = await notifications.get_message(timeout=2.0)
            if msg is not None:
                messages.append(msg)

        # Should receive both keyspace (hset event, no subkeys) and
        # subkeyspace (hset event, with subkeys) notifications
        assert len(messages) == 2
        has_subkeys = any(len(m.subkeys) > 0 for m in messages)
        has_no_subkeys = any(len(m.subkeys) == 0 for m in messages)
        assert has_subkeys
        assert has_no_subkeys

        await notifications.aclose()
        await r.delete("test:hash9")

    @pytest.mark.asyncio
    async def test_subkeyspaceitem_pattern_receives_matching_fields(
        self, async_r_with_subkey_notifications
    ):
        """Subscribe to subkeyspaceitem with a pattern subkey (field*) and
        verify that notifications are received for field1, field2, and field3
        but not for unrelated fields."""
        r = async_r_with_subkey_notifications

        notifications = r.keyspace_notifications()
        await notifications.subscribe_subkeyspaceitem("test:hash10", "field*")
        await self._drain_subscribe_messages(notifications)

        # These should all match the pattern
        await r.hset("test:hash10", "field1", "value1")
        await r.hset("test:hash10", "field2", "value2")
        await r.hset("test:hash10", "field3", "value3")

        messages = []
        for _ in range(3):
            msg = await notifications.get_message(timeout=2.0)
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
        await r.hset("test:hash10", "other", "value")
        await r.hset("test:hash10", "field4", "value4")
        msg = await notifications.get_message(timeout=2.0)
        assert msg is not None
        assert msg.subkeys == ["field4"]

        await notifications.aclose()
        await r.delete("test:hash10")
