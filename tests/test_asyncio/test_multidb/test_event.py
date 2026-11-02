from unittest.mock import AsyncMock, MagicMock, call

import pytest

from redis.asyncio import Redis
from redis.asyncio.multidb.client import PubSub
from redis.asyncio.multidb.event import (
    AsyncActiveDatabaseChanged,
    CloseConnectionOnActiveDatabaseChanged,
    ResubscribeOnActiveDatabaseChanged,
)


@pytest.mark.fixed_client
class TestResubscribeOnActiveDatabaseChanged:
    @pytest.mark.asyncio
    async def test_resubscribes_channels_patterns_and_shard_channels(self):
        old_pubsub = MagicMock()
        old_pubsub.channels = {b"channel": None}
        old_pubsub.patterns = {b"pattern*": None}
        old_pubsub.shard_channels = {b"shard-channel": None}
        old_pubsub.aclose = AsyncMock()
        new_pubsub = MagicMock()
        new_pubsub.on_connect = AsyncMock()
        new_database = MagicMock()
        new_database.client.pubsub.return_value = new_pubsub
        command_executor = MagicMock()
        command_executor.active_pubsub = old_pubsub

        await ResubscribeOnActiveDatabaseChanged().listen(
            AsyncActiveDatabaseChanged(
                MagicMock(),
                new_database,
                command_executor,
                ignore_subscribe_messages=True,
            )
        )

        new_database.client.pubsub.assert_called_once_with(
            ignore_subscribe_messages=True
        )
        assert new_pubsub.channels == {b"channel": None}
        assert new_pubsub.patterns == {b"pattern*": None}
        assert new_pubsub.shard_channels == {b"shard-channel": None}
        new_pubsub.on_connect.assert_awaited_once_with(None)
        assert command_executor.active_pubsub is new_pubsub
        old_pubsub.aclose.assert_awaited_once()


@pytest.mark.fixed_client
class TestCloseConnectionOnActiveDatabaseChanged:
    @pytest.mark.asyncio
    async def test_standalone_keeps_in_use_connections(self):
        old_database = MagicMock()
        old_database.client = MagicMock(spec=Redis)
        pool = old_database.client.connection_pool = MagicMock()
        pool._run_proactive_reconnect_without_locking = AsyncMock()

        await CloseConnectionOnActiveDatabaseChanged().listen(
            AsyncActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        old_database.client.aclose.assert_not_called()
        pool.disconnect.assert_not_called()
        # Mark and disconnect under the pool lock, resetting maintenance state
        # before releasing it.
        assert pool.mock_calls == [
            call._get_pool_lock(),
            call._get_pool_lock().__aenter__(),
            call._set_in_maintenance(True),
            call._run_proactive_reconnect_without_locking(),
            call._set_in_maintenance(False),
            call._get_pool_lock().__aexit__(None, None, None),
        ]

    @pytest.mark.asyncio
    async def test_cluster_keeps_in_use_connections(self):
        old_database = MagicMock()
        nodes = [MagicMock(), MagicMock()]
        for node in nodes:
            node.disconnect_free_connections = AsyncMock()
        old_database.client.nodes_manager.nodes_cache = {
            f"node{i}": node for i, node in enumerate(nodes)
        }

        await CloseConnectionOnActiveDatabaseChanged().listen(
            AsyncActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        old_database.client.aclose.assert_not_called()
        for node in nodes:
            node.update_active_connections_for_reconnect.assert_called_once_with()
            node.disconnect_free_connections.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_cluster_includes_startup_nodes_once(self):
        old_database = MagicMock()
        cached, seed = MagicMock(), MagicMock()
        for node in (cached, seed):
            node.disconnect_free_connections = AsyncMock()
        nodes_manager = old_database.client.nodes_manager
        nodes_manager.nodes_cache = {"node": cached}
        # A seed kept as a separate instance with the same name, and a seed that
        # is the very instance of the topology cache
        nodes_manager.startup_nodes = {"node": seed, "shared": cached}

        await CloseConnectionOnActiveDatabaseChanged().listen(
            AsyncActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        for node in (cached, seed):
            node.update_active_connections_for_reconnect.assert_called_once_with()
            node.disconnect_free_connections.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_cluster_tolerates_nodes_cache_change(self):
        old_database = MagicMock()
        nodes_cache = old_database.client.nodes_manager.nodes_cache = {}
        nodes = [MagicMock(), MagicMock()]
        for i, node in enumerate(nodes):
            node.disconnect_free_connections = AsyncMock()
            nodes_cache[f"node{i}"] = node

        # A topology refresh adds a node while the listener awaits
        async def add_node():
            nodes_cache["new"] = MagicMock()

        nodes[0].disconnect_free_connections.side_effect = add_node

        await CloseConnectionOnActiveDatabaseChanged().listen(
            AsyncActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        for node in nodes:
            node.disconnect_free_connections.assert_awaited_once_with()


@pytest.mark.fixed_client
class TestMultiDbPubSubShardedMethods:
    @staticmethod
    def _pubsub():
        client = MagicMock()
        client.command_executor.execute_pubsub_method = AsyncMock(return_value="ok")
        return PubSub(client), client.command_executor.execute_pubsub_method

    @pytest.mark.asyncio
    async def test_ssubscribe(self):
        pubsub, execute = self._pubsub()
        handler = MagicMock()

        assert await pubsub.ssubscribe("a", b=handler) == "ok"
        execute.assert_awaited_once_with("ssubscribe", "a", b=handler)

    @pytest.mark.asyncio
    async def test_sunsubscribe(self):
        pubsub, execute = self._pubsub()

        assert await pubsub.sunsubscribe("a", "b") == "ok"
        execute.assert_awaited_once_with("sunsubscribe", "a", "b")

    @pytest.mark.asyncio
    async def test_get_sharded_message(self):
        pubsub, execute = self._pubsub()

        assert (
            await pubsub.get_sharded_message(
                ignore_subscribe_messages=True, timeout=1.0
            )
            == "ok"
        )
        execute.assert_awaited_once_with(
            "get_sharded_message", ignore_subscribe_messages=True, timeout=1.0
        )
