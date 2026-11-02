"""
Unit tests for async cluster metrics recording during error handling.

These tests verify that the async cluster error handling correctly records metrics
when exceptions occur, even when using ClusterNode objects instead of Connection objects.
"""

import asyncio

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from redis.asyncio.cluster import RedisCluster, ClusterNode
from redis.exceptions import (
    AuthenticationError,
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
    ClusterDownError,
    SlotNotCoveredError,
    MaxConnectionsError,
    ResponseError,
)


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestAsyncClusterMetricsRecordingDuringErrorHandling:
    """
    Tests for async cluster metrics recording during error handling.

    These tests verify that when exceptions occur during command execution,
    metrics are recorded correctly using either the Connection object (when
    available) or the ClusterNode (as fallback when connection is None).
    """

    async def test_authentication_error_uses_target_node_for_metrics(self):
        """
        Test that AuthenticationError uses target_node for metrics when connection is None.

        AuthenticationError typically occurs during connection establishment.
        Since the error is raised before connection is established, we use
        target_node for metrics.
        """
        target_node = ClusterNode(host="127.0.0.1", port=7000, server_type="primary")

        with patch.object(RedisCluster, "__init__", return_value=None):
            cluster = RedisCluster.__new__(RedisCluster)
            cluster.nodes_manager = MagicMock()
            cluster._initialize = False
            cluster.RedisClusterRequestTTL = 3
            cluster.retry = MagicMock()
            cluster.retry.get_retries.return_value = 0
            cluster._parse_target_nodes = MagicMock(return_value=[target_node])
            cluster._policy_resolver = MagicMock()
            cluster._policy_resolver.resolve = AsyncMock(return_value=None)
            cluster.command_flags = {}

            with patch.object(
                ClusterNode,
                "execute_command",
                new_callable=AsyncMock,
                side_effect=AuthenticationError("Auth failed"),
            ):
                with patch(
                    "redis.asyncio.cluster.record_operation_duration",
                    new_callable=AsyncMock,
                ) as mock_record:
                    with pytest.raises(AuthenticationError) as exc_info:
                        await cluster.execute_command(
                            "GET", "key", target_nodes=target_node
                        )

                    assert "Auth failed" in str(exc_info.value)

                    mock_record.assert_called_once()
                    call_kwargs = mock_record.call_args.kwargs
                    assert call_kwargs["command_name"] == "GET"
                    assert call_kwargs["server_address"] == "127.0.0.1"
                    assert call_kwargs["server_port"] == 7000
                    assert call_kwargs["db_namespace"] == "0"
                    assert isinstance(call_kwargs["error"], AuthenticationError)

    async def test_connection_error_uses_target_node_when_no_connection(self):
        """
        Test that ConnectionError records metrics with target_node.

        This validates the async implementation handles the case where connection
        fails and metrics are recorded using ClusterNode data.
        """
        target_node = ClusterNode(host="10.0.0.50", port=7001, server_type="primary")

        with patch.object(RedisCluster, "__init__", return_value=None):
            cluster = RedisCluster.__new__(RedisCluster)
            cluster.nodes_manager = MagicMock()
            cluster.nodes_manager.move_node_to_end_of_cached_nodes = MagicMock()
            cluster._initialize = False
            cluster.RedisClusterRequestTTL = 3
            cluster.retry = MagicMock()
            cluster.retry.get_retries.return_value = 0
            cluster._parse_target_nodes = MagicMock(return_value=[target_node])
            cluster._policy_resolver = MagicMock()
            cluster._policy_resolver.resolve = AsyncMock(return_value=None)
            cluster.command_flags = {}

            with patch.object(
                ClusterNode,
                "execute_command",
                new_callable=AsyncMock,
                side_effect=RedisConnectionError("Connection refused"),
            ):
                with patch.object(
                    ClusterNode, "update_active_connections_for_reconnect"
                ):
                    with patch.object(
                        ClusterNode,
                        "disconnect_free_connections",
                        new_callable=AsyncMock,
                    ):
                        with patch(
                            "redis.asyncio.cluster.record_operation_duration",
                            new_callable=AsyncMock,
                        ) as mock_record:
                            with pytest.raises(RedisConnectionError) as exc_info:
                                await cluster.execute_command(
                                    "GET", "key", target_nodes=target_node
                                )

                            assert "Connection refused" in str(exc_info.value)

                            mock_record.assert_called_once()
                            call_kwargs = mock_record.call_args.kwargs
                            assert call_kwargs["command_name"] == "GET"
                            assert call_kwargs["server_address"] == "10.0.0.50"
                            assert call_kwargs["server_port"] == 7001
                            assert call_kwargs["db_namespace"] == "0"
                            assert isinstance(
                                call_kwargs["error"], RedisConnectionError
                            )

    async def test_response_error_uses_target_node(self):
        """
        Test that ResponseError uses target_node for metrics.

        When a command succeeds in reaching the server but gets an error response,
        we use the target_node for metrics since async cluster doesn't have a
        persistent connection object in the same way sync does.
        """
        target_node = ClusterNode(host="172.16.0.10", port=6380, server_type="primary")

        with patch.object(RedisCluster, "__init__", return_value=None):
            cluster = RedisCluster.__new__(RedisCluster)
            cluster.nodes_manager = MagicMock()
            cluster._initialize = False
            cluster.RedisClusterRequestTTL = 3
            cluster.retry = MagicMock()
            cluster.retry.get_retries.return_value = 0
            cluster._parse_target_nodes = MagicMock(return_value=[target_node])
            cluster._policy_resolver = MagicMock()
            cluster._policy_resolver.resolve = AsyncMock(return_value=None)
            cluster.command_flags = {}

            with patch.object(
                ClusterNode,
                "execute_command",
                new_callable=AsyncMock,
                side_effect=ResponseError("WRONGTYPE Operation against a key"),
            ):
                with patch(
                    "redis.asyncio.cluster.record_operation_duration",
                    new_callable=AsyncMock,
                ) as mock_record:
                    with pytest.raises(ResponseError) as exc_info:
                        await cluster.execute_command(
                            "GET", "key", target_nodes=target_node
                        )

                    assert "WRONGTYPE" in str(exc_info.value)

                    mock_record.assert_called_once()
                    call_kwargs = mock_record.call_args.kwargs
                    assert call_kwargs["command_name"] == "GET"
                    assert call_kwargs["server_address"] == "172.16.0.10"
                    assert call_kwargs["server_port"] == 6380
                    assert call_kwargs["db_namespace"] == "0"
                    assert isinstance(call_kwargs["error"], ResponseError)

    async def test_max_connections_error_records_metrics_with_cluster_node(self):
        """
        Test that MaxConnectionsError records metrics using ClusterNode info.

        When MaxConnectionsError occurs, connection is None because we couldn't
        get a connection from the pool. Metrics should be recorded using the
        ClusterNode's host/port.
        """
        target_node = ClusterNode(
            host="192.168.1.100", port=7005, server_type="primary"
        )

        with patch.object(RedisCluster, "__init__", return_value=None):
            cluster = RedisCluster.__new__(RedisCluster)
            cluster.nodes_manager = MagicMock()
            cluster._initialize = False
            cluster.RedisClusterRequestTTL = 3
            cluster.retry = MagicMock()
            cluster.retry.get_retries.return_value = 0
            cluster._parse_target_nodes = MagicMock(return_value=[target_node])
            cluster._policy_resolver = MagicMock()
            cluster._policy_resolver.resolve = AsyncMock(return_value=None)
            cluster.command_flags = {}

            with patch.object(
                ClusterNode,
                "execute_command",
                new_callable=AsyncMock,
                side_effect=MaxConnectionsError("Pool exhausted"),
            ):
                with patch(
                    "redis.asyncio.cluster.record_operation_duration",
                    new_callable=AsyncMock,
                ) as mock_record:
                    with pytest.raises(MaxConnectionsError):
                        await cluster.execute_command(
                            "GET", "key", target_nodes=target_node
                        )

                    mock_record.assert_called_once()
                    call_kwargs = mock_record.call_args.kwargs
                    assert call_kwargs["server_address"] == "192.168.1.100"
                    assert call_kwargs["server_port"] == 7005
                    assert call_kwargs["db_namespace"] == "0"
                    assert isinstance(call_kwargs["error"], MaxConnectionsError)

    async def test_timeout_error_uses_target_node_for_metrics(self):
        """
        Test that TimeoutError records metrics with target_node data.
        """
        target_node = ClusterNode(host="10.0.0.100", port=7003, server_type="primary")

        with patch.object(RedisCluster, "__init__", return_value=None):
            cluster = RedisCluster.__new__(RedisCluster)
            cluster.nodes_manager = MagicMock()
            cluster.nodes_manager.move_node_to_end_of_cached_nodes = MagicMock()
            cluster._initialize = False
            cluster.RedisClusterRequestTTL = 3
            cluster.retry = MagicMock()
            cluster.retry.get_retries.return_value = 0
            cluster._parse_target_nodes = MagicMock(return_value=[target_node])
            cluster._policy_resolver = MagicMock()
            cluster._policy_resolver.resolve = AsyncMock(return_value=None)
            cluster.command_flags = {}

            with patch.object(
                ClusterNode,
                "execute_command",
                new_callable=AsyncMock,
                side_effect=RedisTimeoutError("Timeout connecting"),
            ):
                with patch.object(
                    ClusterNode, "update_active_connections_for_reconnect"
                ):
                    with patch.object(
                        ClusterNode,
                        "disconnect_free_connections",
                        new_callable=AsyncMock,
                    ):
                        with patch(
                            "redis.asyncio.cluster.record_operation_duration",
                            new_callable=AsyncMock,
                        ) as mock_record:
                            with pytest.raises(RedisTimeoutError) as exc_info:
                                await cluster.execute_command(
                                    "GET", "key", target_nodes=target_node
                                )

                            assert "Timeout" in str(exc_info.value)

                            mock_record.assert_called_once()
                            call_kwargs = mock_record.call_args.kwargs
                            assert call_kwargs["server_address"] == "10.0.0.100"
                            assert call_kwargs["server_port"] == 7003
                            assert call_kwargs["db_namespace"] == "0"
                            assert isinstance(call_kwargs["error"], RedisTimeoutError)

    async def test_cluster_down_error_with_cluster_node_metrics(self):
        """
        Test that ClusterDownError records metrics correctly with target_node data.
        """
        target_node = ClusterNode(host="172.20.0.10", port=7006, server_type="primary")

        with patch.object(RedisCluster, "__init__", return_value=None):
            cluster = RedisCluster.__new__(RedisCluster)
            cluster.nodes_manager = MagicMock()
            cluster._initialize = False
            cluster.RedisClusterRequestTTL = 3
            cluster.aclose = AsyncMock()
            cluster.retry = MagicMock()
            cluster.retry.get_retries.return_value = 0
            cluster._parse_target_nodes = MagicMock(return_value=[target_node])
            cluster._policy_resolver = MagicMock()
            cluster._policy_resolver.resolve = AsyncMock(return_value=None)
            cluster.command_flags = {}
            cluster._refresh_topology = AsyncMock()
            # The refresh runs under the lock of an initialized client
            cluster._lock = asyncio.Lock()
            cluster._topology_ready = True

            with patch.object(
                ClusterNode,
                "execute_command",
                new_callable=AsyncMock,
                side_effect=ClusterDownError("CLUSTERDOWN"),
            ):
                with patch(
                    "redis.asyncio.cluster.record_operation_duration",
                    new_callable=AsyncMock,
                ) as mock_record:
                    with patch("asyncio.sleep", new_callable=AsyncMock):
                        with pytest.raises(ClusterDownError):
                            await cluster.execute_command(
                                "GET", "key", target_nodes=target_node
                            )

                    cluster._refresh_topology.assert_awaited_once_with()

                    mock_record.assert_called_once()
                    call_kwargs = mock_record.call_args.kwargs
                    assert call_kwargs["server_address"] == "172.20.0.10"
                    assert call_kwargs["server_port"] == 7006
                    assert call_kwargs["db_namespace"] == "0"
                    assert isinstance(call_kwargs["error"], ClusterDownError)

    async def test_slot_not_covered_error_with_cluster_node_metrics(self):
        """
        Test that SlotNotCoveredError records metrics correctly with target_node data.
        """
        target_node = ClusterNode(host="172.20.0.20", port=7007, server_type="primary")

        with patch.object(RedisCluster, "__init__", return_value=None):
            cluster = RedisCluster.__new__(RedisCluster)
            cluster.nodes_manager = MagicMock()
            cluster._initialize = False
            cluster.RedisClusterRequestTTL = 3
            cluster.aclose = AsyncMock()
            cluster.retry = MagicMock()
            cluster.retry.get_retries.return_value = 0
            cluster._parse_target_nodes = MagicMock(return_value=[target_node])
            cluster._policy_resolver = MagicMock()
            cluster._policy_resolver.resolve = AsyncMock(return_value=None)
            cluster.command_flags = {}
            cluster._refresh_topology = AsyncMock()
            # The refresh runs under the lock of an initialized client
            cluster._lock = asyncio.Lock()
            cluster._topology_ready = True

            with patch.object(
                ClusterNode,
                "execute_command",
                new_callable=AsyncMock,
                side_effect=SlotNotCoveredError("Slot 1234 not covered"),
            ):
                with patch(
                    "redis.asyncio.cluster.record_operation_duration",
                    new_callable=AsyncMock,
                ) as mock_record:
                    with patch("asyncio.sleep", new_callable=AsyncMock):
                        with pytest.raises(SlotNotCoveredError):
                            await cluster.execute_command(
                                "GET", "key", target_nodes=target_node
                            )

                    cluster._refresh_topology.assert_awaited_once_with()

                    mock_record.assert_called_once()
                    call_kwargs = mock_record.call_args.kwargs
                    assert call_kwargs["server_address"] == "172.20.0.20"
                    assert call_kwargs["server_port"] == 7007
                    assert call_kwargs["db_namespace"] == "0"
                    assert isinstance(call_kwargs["error"], SlotNotCoveredError)

    async def test_successful_command_records_metrics_with_connection_db(self):
        """
        Test that successful command execution records metrics with proper db value.

        In async cluster, the execute_command is called on target_node directly,
        so we use target_node's connection_kwargs for db lookup.
        """
        from redis.commands.metadata import ResponsePolicy

        target_node = ClusterNode(
            host="192.168.50.10", port=7008, server_type="primary", db=3
        )

        with patch.object(RedisCluster, "__init__", return_value=None):
            cluster = RedisCluster.__new__(RedisCluster)
            cluster.nodes_manager = MagicMock()
            cluster._initialize = False
            cluster.RedisClusterRequestTTL = 3
            cluster.retry = MagicMock()
            cluster.retry.get_retries.return_value = 0
            cluster._parse_target_nodes = MagicMock(return_value=[target_node])
            cluster._policy_resolver = MagicMock()
            cluster._policy_resolver.resolve = AsyncMock(return_value=None)
            cluster.command_flags = {}
            cluster.result_callbacks = {}
            cluster._policies_callback_mapping = {
                ResponsePolicy.DEFAULT_KEYLESS: lambda x: x,
                ResponsePolicy.DEFAULT_KEYED: lambda x: x,
            }

            with patch.object(
                ClusterNode,
                "execute_command",
                new_callable=AsyncMock,
                return_value=b"value",
            ):
                with patch(
                    "redis.asyncio.cluster.record_operation_duration",
                    new_callable=AsyncMock,
                ) as mock_record:
                    result = await cluster.execute_command(
                        "GET", "key", target_nodes=target_node
                    )

                    assert result == b"value"

                    mock_record.assert_called_once()
                    call_kwargs = mock_record.call_args.kwargs
                    assert call_kwargs["command_name"] == "GET"
                    assert call_kwargs["server_address"] == "192.168.50.10"
                    assert call_kwargs["server_port"] == 7008
                    assert call_kwargs["db_namespace"] == "3"
                    assert call_kwargs.get("error") is None
