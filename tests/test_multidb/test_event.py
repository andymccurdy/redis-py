from unittest.mock import MagicMock, call

import pytest

from redis.client import Redis
from redis.multidb.event import (
    ActiveDatabaseChanged,
    CloseConnectionOnActiveDatabaseChanged,
)

# Mark and disconnect must both happen under the pool lock, with the blocking
# pool's maintenance flag raised so its get_connection honours that lock.
EXPECTED_POOL_CALLS = [
    call._get_pool_lock(),
    call._get_pool_lock().__enter__(),
    call.set_in_maintenance(True),
    call.update_active_connections_for_reconnect(),
    call.disconnect_free_connections(),
    call.set_in_maintenance(False),
    call._get_pool_lock().__exit__(None, None, None),
]


@pytest.mark.fixed_client
class TestCloseConnectionOnActiveDatabaseChanged:
    def test_standalone_keeps_in_use_connections(self):
        old_database = MagicMock()
        old_database.client = MagicMock(spec=Redis)
        pool = old_database.client.connection_pool = MagicMock()

        CloseConnectionOnActiveDatabaseChanged().listen(
            ActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        old_database.client.close.assert_not_called()
        pool.disconnect.assert_not_called()
        assert pool.mock_calls == EXPECTED_POOL_CALLS

    def test_standalone_pool_without_maintenance_flag(self):
        old_database = MagicMock()
        old_database.client = MagicMock(spec=Redis)
        pool = old_database.client.connection_pool = MagicMock(
            spec=[
                "_get_pool_lock",
                "update_active_connections_for_reconnect",
                "disconnect_free_connections",
            ]
        )

        CloseConnectionOnActiveDatabaseChanged().listen(
            ActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        assert pool.mock_calls == [
            c for c in EXPECTED_POOL_CALLS if "set_in_maintenance" not in str(c)
        ]

    def test_cluster_keeps_in_use_connections(self):
        old_database = MagicMock()
        nodes = [MagicMock(), MagicMock()]
        old_database.client.nodes_manager.nodes_cache = {
            f"node{i}": node for i, node in enumerate(nodes)
        }

        CloseConnectionOnActiveDatabaseChanged().listen(
            ActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        old_database.client.close.assert_not_called()
        for node in nodes:
            pool = node.redis_connection.connection_pool
            pool.disconnect.assert_not_called()
            assert pool.mock_calls == EXPECTED_POOL_CALLS

    def test_cluster_tolerates_nodes_cache_change(self):
        old_database = MagicMock()
        nodes_cache = old_database.client.nodes_manager.nodes_cache = {}
        nodes = [MagicMock(), MagicMock()]
        for i, node in enumerate(nodes):
            nodes_cache[f"node{i}"] = node

        # A topology refresh on another thread adds a node meanwhile
        def add_node():
            nodes_cache["new"] = MagicMock()

        pool = nodes[0].redis_connection.connection_pool
        pool.disconnect_free_connections.side_effect = add_node

        CloseConnectionOnActiveDatabaseChanged().listen(
            ActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        for node in nodes:
            pool = node.redis_connection.connection_pool
            pool.disconnect_free_connections.assert_called_once_with()
