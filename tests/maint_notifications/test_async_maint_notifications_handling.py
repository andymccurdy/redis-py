import asyncio
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest

import redis.asyncio as redis
from redis._defaults import DEFAULT_SOCKET_CONNECT_TIMEOUT, DEFAULT_SOCKET_TIMEOUT
from redis._parsers.resp2 import _AsyncRESP2Parser
from redis._parsers.resp3 import _AsyncRESP3Parser
from redis.asyncio.connection import (
    BlockingConnectionPool,
    Connection,
    ConnectionPool,
    UnixDomainSocketConnection,
)
from redis.asyncio.maint_notifications import (
    AsyncMaintNotificationsConnectionHandler,
    AsyncMaintNotificationsPoolHandler,
    AsyncOSSMaintNotificationsHandler,
)
from redis.event import (
    EventDispatcher,
    EventListenerInterface,
    MaintenanceCompletedEvent,
    MaintenanceStartedEvent,
)
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError, ResponseError
from redis.maint_notifications import (
    EndpointType,
    MaintenanceState,
    MaintNotificationsConfig,
    NodeFailedOverNotification,
    NodeFailingOverNotification,
    NodeMigratedNotification,
    NodeMigratingNotification,
    NodeMovingNotification,
    OSSNodeMigratedNotification,
)
from redis.utils import SENTINEL


DEFAULT_HOST = "12.45.34.56"
DEFAULT_PORT = 6379
MOVED_HOST = "1.2.3.4"
MOVED_PORT = 6380
RELAXED_TIMEOUT = 30


class DummyAsyncConnection:
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, peer=DEFAULT_HOST):
        self.host = host
        self.port = port
        self.peer = peer
        self.socket_timeout = DEFAULT_SOCKET_TIMEOUT
        self.socket_connect_timeout = DEFAULT_SOCKET_CONNECT_TIMEOUT
        self.orig_host_address = host
        self.orig_socket_timeout = DEFAULT_SOCKET_TIMEOUT
        self.orig_socket_connect_timeout = DEFAULT_SOCKET_CONNECT_TIMEOUT
        self.maintenance_state = MaintenanceState.NONE
        self.maintenance_notification_hash = None
        self.connected = True
        self.disconnect_calls = 0
        self.timeout_updates = []
        self.reset_notifications_calls = 0
        self._should_reconnect = False

    def __repr__(self):
        return f"<DummyAsyncConnection(host={self.host},port={self.port})>"

    def getpeername(self):
        return self.peer

    def get_resolved_ip(self):
        return self.peer

    def set_tmp_settings(
        self,
        tmp_host_address: str | object | None = SENTINEL,
        tmp_relaxed_timeout: float | None = -1,
    ):
        if tmp_host_address and tmp_host_address != SENTINEL:
            self.host = str(tmp_host_address)
        if tmp_relaxed_timeout != -1:
            self.socket_timeout = tmp_relaxed_timeout
            self.socket_connect_timeout = tmp_relaxed_timeout

    def reset_tmp_settings(
        self,
        reset_host_address=False,
        reset_relaxed_timeout=False,
    ):
        if reset_host_address:
            self.host = self.orig_host_address
        if reset_relaxed_timeout:
            self.socket_timeout = self.orig_socket_timeout
            self.socket_connect_timeout = self.orig_socket_connect_timeout

    def update_current_socket_timeout(self, relaxed_timeout=None):
        self.timeout_updates.append(relaxed_timeout)

    def reset_received_notifications(self):
        self.reset_notifications_calls += 1

    def mark_for_reconnect(self):
        self._should_reconnect = True

    def should_reconnect(self):
        return self._should_reconnect

    async def disconnect(self, *args, **kwargs):
        self.disconnect_calls += 1
        self.connected = False


class UnsupportedAsyncConnection:
    def __init__(self, host=DEFAULT_HOST, **kwargs):
        self.host = host
        self.kwargs = kwargs


def _assert_connection_in_moving_state(
    connection,
    *,
    expected_host=MOVED_HOST,
    expected_timeout=RELAXED_TIMEOUT,
    expected_notification_hash=None,
):
    assert connection.maintenance_state == MaintenanceState.MOVING
    if expected_notification_hash is not None:
        assert connection.maintenance_notification_hash == expected_notification_hash
    assert connection.host == expected_host
    assert connection.socket_timeout == expected_timeout
    assert connection.socket_connect_timeout == expected_timeout


def _assert_connection_in_default_state(connection, *, expected_host=DEFAULT_HOST):
    assert connection.maintenance_state == MaintenanceState.NONE
    assert connection.maintenance_notification_hash is None
    assert connection.host == expected_host
    assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT
    assert connection.socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_connection_handler_applies_start_and_end_notifications():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    connection = DummyAsyncConnection()
    handler = AsyncMaintNotificationsConnectionHandler(connection, config)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ) as notification_count,
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await handler.handle_notification(NodeMigratingNotification(id=1, ttl=5))
        await handler.handle_notification(NodeMigratedNotification(id=1))

    assert connection.maintenance_state == MaintenanceState.NONE
    assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT
    assert connection.socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
    assert connection.timeout_updates == [RELAXED_TIMEOUT, -1]
    assert connection.reset_notifications_calls == 1
    assert notification_count.await_count == 2
    assert relaxed_timeout_metric.await_count == 2
    assert relaxed_timeout_metric.await_args_list[0].kwargs["relaxed"] is True
    assert relaxed_timeout_metric.await_args_list[1].kwargs["relaxed"] is False


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_connection_handler_skips_timeout_changes_when_moving():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    connection = DummyAsyncConnection()
    connection.maintenance_state = MaintenanceState.MOVING
    handler = AsyncMaintNotificationsConnectionHandler(connection, config)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await handler.handle_notification(NodeFailingOverNotification(id=1, ttl=5))

    assert connection.maintenance_state == MaintenanceState.MOVING
    assert connection.timeout_updates == []
    relaxed_timeout_metric.assert_not_awaited()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_connection_handler_handles_failover_start_and_end():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    connection = DummyAsyncConnection()
    handler = AsyncMaintNotificationsConnectionHandler(connection, config)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await handler.handle_notification(NodeFailingOverNotification(id=2, ttl=5))
        await handler.handle_notification(NodeFailedOverNotification(id=2))

    assert connection.maintenance_state == MaintenanceState.NONE
    assert connection.timeout_updates == [RELAXED_TIMEOUT, -1]
    assert relaxed_timeout_metric.await_count == 2


@pytest.mark.fixed_client
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "start_notification,end_notification",
    [
        (NodeMigratingNotification(id=1, ttl=5), NodeMigratedNotification(id=1)),
        (
            NodeFailingOverNotification(id=2, ttl=5),
            NodeFailedOverNotification(id=2),
        ),
    ],
)
async def test_async_connection_handler_ignores_relaxed_timeout_when_disabled(
    start_notification, end_notification
):
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=-1)
    connection = DummyAsyncConnection()
    handler = AsyncMaintNotificationsConnectionHandler(connection, config)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ) as notification_count,
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await handler.handle_notification(start_notification)
        await handler.handle_notification(end_notification)

    _assert_connection_in_default_state(connection)
    assert connection.timeout_updates == []
    assert notification_count.await_count == 2
    relaxed_timeout_metric.assert_not_awaited()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pool_handler_logs_non_moving_notifications():
    config = MaintNotificationsConfig(enabled=True)
    pool = MagicMock()
    handler = AsyncMaintNotificationsPoolHandler(pool, config)

    with (
        mock.patch.object(handler, "remove_expired_notifications", new=AsyncMock()),
        mock.patch.object(
            handler, "handle_node_moving_notification", new=AsyncMock()
        ) as handle_moving,
        mock.patch("redis.asyncio.maint_notifications.logger.error") as log_error,
    ):
        await handler.handle_notification(NodeMigratingNotification(id=1, ttl=5))

    handle_moving.assert_not_awaited()
    log_error.assert_called_once()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_parser_routes_maintenance_push_notifications_to_connection_handler():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    connection = Connection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ),
    ):
        await connection._parser.handle_push_response(["MIGRATING", 1, 5])
        assert connection.maintenance_state == MaintenanceState.MAINTENANCE
        assert connection.socket_timeout == RELAXED_TIMEOUT

        await connection._parser.handle_push_response(["MIGRATED", 1])
        assert connection.maintenance_state == MaintenanceState.NONE
        assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT

        await connection._parser.handle_push_response(["FAILING_OVER", 2, 5])
        assert connection.maintenance_state == MaintenanceState.MAINTENANCE
        assert connection.socket_timeout == RELAXED_TIMEOUT

        await connection._parser.handle_push_response(["FAILED_OVER", 2])
        assert connection.maintenance_state == MaintenanceState.NONE
        assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_parser_routes_smigrated_to_cluster_handler_only():
    """
    SMIGRATED goes to the cluster handler alone: its topology refresh marks the
    node's connections for reconnect, and the relaxation SMIGRATING applied to
    a connection ends - and is reported as completed - when the connection is
    disconnected, not when this connection happens to read the SMIGRATED.
    """
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    connection = _connected_async_connection(config)
    cluster_handler = AsyncMock()
    connection._parser.set_oss_cluster_maint_push_handler(cluster_handler)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ),
    ):
        await connection._parser.handle_push_response(["SMIGRATING", 1, "1-100"])
        assert connection.maintenance_state == MaintenanceState.MAINTENANCE
        assert connection.socket_timeout == RELAXED_TIMEOUT

        await connection._parser.handle_push_response(
            ["SMIGRATED", 1, [["127.0.0.1:6379", "127.0.0.1:6380", "1-100"]]]
        )
        cluster_handler.assert_awaited_once()
        assert connection.maintenance_state == MaintenanceState.MAINTENANCE
        assert connection.socket_timeout == RELAXED_TIMEOUT
        assert [type(e) for e in listener.events] == [MaintenanceStartedEvent]

        await connection.disconnect()

    assert connection.maintenance_state == MaintenanceState.NONE
    assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT
    assert [type(e) for e in listener.events] == [
        MaintenanceStartedEvent,
        MaintenanceCompletedEvent,
    ]


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_parser_routes_moving_push_notification_to_pool_handler():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = pool.make_connection()
    pool._in_use_connections.add(connection)
    connection._maint_notifications_pool_handler._schedule = MagicMock()

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await connection._parser.handle_push_response(
            ["MOVING", 1, 5, f"{MOVED_HOST}:{MOVED_PORT}"]
        )

    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.MOVING
    assert pool.connection_kwargs["maintenance_notification_hash"] == hash(
        NodeMovingNotification(1, MOVED_HOST, MOVED_PORT, 5)
    )
    assert pool.connection_kwargs["host"] == MOVED_HOST


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_parser_routes_moving_push_without_target():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = pool.make_connection()
    pool._in_use_connections.add(connection)
    connection._maint_notifications_pool_handler._schedule = MagicMock()

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await connection._parser.handle_push_response(["MOVING", 1, 5, None])

    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.MOVING
    assert pool.connection_kwargs["host"] == DEFAULT_HOST
    connection._maint_notifications_pool_handler._schedule.assert_has_calls(
        [
            mock.call(
                2.5,
                connection._maint_notifications_pool_handler.run_proactive_reconnect,
                None,
            ),
            mock.call(
                5,
                connection._maint_notifications_pool_handler.handle_node_moved_notification,
                mock.ANY,
            ),
        ]
    )


@pytest.mark.fixed_client
def test_async_connection_installs_maintenance_parser_handlers():
    config = MaintNotificationsConfig(enabled=True)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )

    connection = pool.make_connection()
    pool_handler = pool._maint_notifications_pool_handler

    node_moving_handler = connection._parser.node_moving_push_handler_func
    maintenance_handler = connection._parser.maintenance_push_handler_func

    assert node_moving_handler is not None
    assert node_moving_handler.__self__.connection is connection
    assert node_moving_handler.__self__.pool is pool
    assert node_moving_handler.__self__._lock is pool_handler._lock
    assert (
        node_moving_handler.__self__._processed_notifications
        is pool_handler._processed_notifications
    )

    assert maintenance_handler is not None
    assert (
        maintenance_handler.__self__
        is connection._maint_notifications_connection_handler
    )
    assert connection._maint_notifications_connection_handler.config is config


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pool_update_config_wires_existing_connections():
    disabled_config = MaintNotificationsConfig(enabled=False)
    enabled_config = MaintNotificationsConfig(enabled=True)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=disabled_config,
    )
    free_connection = pool.make_connection()
    in_use_connection = pool.make_connection()
    pool._available_connections.append(free_connection)
    pool._in_use_connections.add(in_use_connection)

    await pool.update_maint_notifications_config(enabled_config)

    assert free_connection._parser.node_moving_push_handler_func is not None
    assert free_connection._parser.maintenance_push_handler_func is not None
    assert free_connection.maint_notifications_config is enabled_config
    assert not free_connection.should_reconnect()

    assert in_use_connection._parser.node_moving_push_handler_func is not None
    assert in_use_connection._parser.maintenance_push_handler_func is not None
    assert in_use_connection.maint_notifications_config is enabled_config
    assert in_use_connection.should_reconnect()

    assert pool.connection_kwargs["maint_notifications_config"] is enabled_config
    assert pool.connection_kwargs["maint_notifications_pool_handler"] is (
        pool._maint_notifications_pool_handler
    )


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pool_update_oss_handler_wires_existing_connections():
    """update_maint_notifications_config wires an OSS cluster handler onto every
    existing connection in the pool.

    Each connection's parser receives the OSS cluster and maintenance push
    handlers and the connection references the handler. In-use connections are
    marked for reconnect; free connections are wired then disconnected.
    """
    config = MaintNotificationsConfig(enabled=True)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    free_connection = pool.make_connection()
    in_use_connection = pool.make_connection()
    pool._available_connections.append(free_connection)
    pool._in_use_connections.add(in_use_connection)

    oss_handler = AsyncOSSMaintNotificationsHandler(MagicMock(), config)

    await pool.update_maint_notifications_config(
        config, oss_cluster_maint_notifications_handler=oss_handler
    )

    # In-use connection: parser has the OSS + maintenance push handlers wired
    # and the connection is marked for reconnect.
    assert in_use_connection._oss_cluster_maint_notifications_handler is oss_handler
    in_use_oss_func = in_use_connection._parser.oss_cluster_maint_push_handler_func
    assert in_use_oss_func is not None
    assert in_use_oss_func.__func__ is oss_handler.handle_notification.__func__
    assert in_use_connection._parser.maintenance_push_handler_func is not None
    assert in_use_connection.should_reconnect()

    # Free connection — the idle OSS path, wired then disconnected.
    assert free_connection._oss_cluster_maint_notifications_handler is oss_handler
    assert free_connection._parser.oss_cluster_maint_push_handler_func is not None
    assert free_connection._parser.maintenance_push_handler_func is not None

    # Existing connections must not retain the orphaned pool-handler binding.
    # A default (enabled) pool wires node_moving on each connection at __init__;
    # switching to OSS mode must clear it so no existing connection is configured
    # with both the node-moving and OSS cluster handlers.
    assert in_use_connection._parser.node_moving_push_handler_func is None
    assert in_use_connection._maint_notifications_pool_handler is None
    assert free_connection._parser.node_moving_push_handler_func is None
    assert free_connection._maint_notifications_pool_handler is None

    # OSS cluster mode and pool-handler mode are mutually exclusive. The pool
    # was created with the default (enabled) config, which wires a pool handler
    # in __init__; switching to OSS mode must clear it from both the pool
    # attribute and the shared connection kwargs so future connections are not
    # configured with both handlers.
    assert pool._maint_notifications_pool_handler is None
    assert "maint_notifications_pool_handler" not in pool.connection_kwargs
    assert (
        pool.connection_kwargs.get("oss_cluster_maint_notifications_handler")
        is oss_handler
    )

    # A connection created after the switch gets only the OSS cluster handler.
    new_connection = pool.make_connection()
    assert new_connection._maint_notifications_pool_handler is None
    assert new_connection._parser.node_moving_push_handler_func is None
    assert new_connection._oss_cluster_maint_notifications_handler is oss_handler
    assert new_connection._parser.oss_cluster_maint_push_handler_func is not None


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_update_config_on_oss_pool_updates_oss_handler():
    """A config-only update on a pool already in OSS cluster mode must update
    the OSS handler's config instead of being silently discarded.

    OSS cluster mode and pool-handler mode are mutually exclusive, so the update
    must not create a pool handler; it must apply the new config to the existing
    OSS handler and propagate it to connection kwargs and existing connections.
    """
    config = MaintNotificationsConfig(enabled=True)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    free_connection = pool.make_connection()
    pool._available_connections.append(free_connection)

    oss_handler = AsyncOSSMaintNotificationsHandler(MagicMock(), config)
    await pool.update_maint_notifications_config(
        config, oss_cluster_maint_notifications_handler=oss_handler
    )

    # Now issue a config-only update (no OSS handler passed).
    new_config = MaintNotificationsConfig(
        enabled=True, proactive_reconnect=True, relaxed_timeout=30
    )
    await pool.update_maint_notifications_config(new_config)

    # The new config must land on the existing OSS handler, and the pool must
    # remain in OSS mode without an orphaned pool handler.
    assert oss_handler.config is new_config
    assert pool._oss_cluster_maint_notifications_handler is oss_handler
    assert pool._maint_notifications_pool_handler is None
    assert "maint_notifications_pool_handler" not in pool.connection_kwargs
    assert pool.connection_kwargs.get("maint_notifications_config") is new_config

    # Existing free connections must receive the new config, not the stale one.
    assert free_connection.maint_notifications_config is new_config

    # Disabling an already-enabled OSS pool is not allowed and must not be
    # silently accepted by the config-only update path.
    with pytest.raises(ValueError, match="Cannot disable maintenance notifications"):
        await pool.update_maint_notifications_config(
            MaintNotificationsConfig(enabled=False)
        )
    assert oss_handler.config is new_config


def _make_async_oss_handler():
    cluster_client = MagicMock()
    # Keep the affected-nodes set and the node-reconnect loop empty so the test
    # focuses purely on how src/dest addresses are parsed.
    cluster_client.nodes_manager.get_node.return_value = None
    cluster_client.nodes_manager.nodes_cache.values.return_value = []
    cluster_client.nodes_manager.initialize = AsyncMock()
    config = MaintNotificationsConfig(enabled=True)
    return AsyncOSSMaintNotificationsHandler(cluster_client, config), cluster_client


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_handle_smigrated_parses_ipv4_addresses():
    handler, cluster_client = _make_async_oss_handler()
    notification = OSSNodeMigratedNotification(
        id=1,
        nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
    )

    await handler.handle_oss_maintenance_completed_notification(notification)

    cluster_client.nodes_manager.get_node.assert_called_once_with(
        host="127.0.0.1", port=6379
    )
    cluster_client.nodes_manager.initialize.assert_called_once_with(
        additional_startup_nodes_info=[("127.0.0.1", 6380)],
    )


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_handle_smigrated_parses_ipv6_addresses():
    handler, cluster_client = _make_async_oss_handler()
    # IPv6 literals contain multiple colons; a naive split(":") would raise
    # ValueError on the host/port unpack.
    notification = OSSNodeMigratedNotification(
        id=1,
        nodes_to_slots_mapping={"2001:db8::1:6379": [{"2001:db8::2:6380": "1-100"}]},
    )

    await handler.handle_oss_maintenance_completed_notification(notification)

    cluster_client.nodes_manager.get_node.assert_called_once_with(
        host="2001:db8::1", port=6379
    )
    cluster_client.nodes_manager.initialize.assert_called_once_with(
        additional_startup_nodes_info=[("2001:db8::2", 6380)],
    )


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_handshake_sends_maint_notifications_command():
    config = MaintNotificationsConfig(
        enabled=True,
        endpoint_type=EndpointType.INTERNAL_FQDN,
    )
    connection = Connection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection.send_command = AsyncMock()
    connection.read_response = AsyncMock(return_value=b"OK")

    await connection.activate_maint_notifications_handling_if_enabled(
        check_health=False
    )

    connection.send_command.assert_awaited_once_with(
        "CLIENT",
        "MAINT_NOTIFICATIONS",
        "ON",
        "moving-endpoint-type",
        "internal-fqdn",
        check_health=False,
    )


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_handshake_auto_suppresses_response_error():
    config = MaintNotificationsConfig(enabled="auto")
    connection = Connection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection.send_command = AsyncMock()
    connection.read_response = AsyncMock(side_effect=ResponseError("unsupported"))

    await connection.activate_maint_notifications_handling_if_enabled()

    connection.send_command.assert_awaited_once()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_handshake_enabled_raises_response_error():
    config = MaintNotificationsConfig(enabled=True)
    connection = Connection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection.send_command = AsyncMock()
    connection.read_response = AsyncMock(side_effect=ResponseError("unsupported"))

    with pytest.raises(ResponseError):
        await connection.activate_maint_notifications_handling_if_enabled()


@pytest.mark.fixed_client
def test_async_pool_auto_enables_maint_notifications_for_default_resp3():
    pool = ConnectionPool(host=DEFAULT_HOST, port=DEFAULT_PORT)

    assert pool.maint_notifications_enabled() == "auto"
    assert isinstance(
        pool.connection_kwargs["maint_notifications_config"],
        MaintNotificationsConfig,
    )
    assert pool.connection_kwargs["maint_notifications_pool_handler"] is (
        pool._maint_notifications_pool_handler
    )


@pytest.mark.fixed_client
def test_async_pool_does_not_auto_enable_maint_notifications_without_valid_host():
    pool = ConnectionPool(host=None, port=DEFAULT_PORT, protocol=3)

    assert pool._maint_notifications_pool_handler is None
    assert not pool.maint_notifications_enabled()
    assert "maint_notifications_config" not in pool.connection_kwargs
    assert "maint_notifications_pool_handler" not in pool.connection_kwargs


@pytest.mark.fixed_client
def test_async_pool_rejects_enabled_maint_notifications_without_valid_host():
    config = MaintNotificationsConfig(enabled=True)

    with pytest.raises(
        RedisError,
        match="Maintenance notifications are not supported for connections without a host",
    ):
        ConnectionPool(
            host=None,
            port=DEFAULT_PORT,
            protocol=3,
            maint_notifications_config=config,
        )


@pytest.mark.fixed_client
def test_async_custom_connection_does_not_auto_enable_maint_notifications():
    pool = ConnectionPool(
        connection_class=UnsupportedAsyncConnection,
        host=DEFAULT_HOST,
        protocol=3,
    )

    assert pool._maint_notifications_pool_handler is None
    assert not pool.maint_notifications_enabled()
    assert "maint_notifications_config" not in pool.connection_kwargs
    assert "maint_notifications_pool_handler" not in pool.connection_kwargs


@pytest.mark.fixed_client
def test_async_custom_connection_rejects_enabled_maint_notifications_config():
    config = MaintNotificationsConfig(enabled=True)

    with pytest.raises(
        RedisError,
        match=(
            "Maintenance notifications are not supported for connection class "
            "UnsupportedAsyncConnection"
        ),
    ):
        ConnectionPool(
            connection_class=UnsupportedAsyncConnection,
            host=DEFAULT_HOST,
            protocol=3,
            maint_notifications_config=config,
        )


@pytest.mark.fixed_client
def test_async_unix_socket_pool_does_not_auto_enable_maint_notifications():
    pool = ConnectionPool(
        connection_class=UnixDomainSocketConnection,
        path="/tmp/redis.sock",
        protocol=3,
    )

    assert pool._maint_notifications_pool_handler is None
    assert not pool.maint_notifications_enabled()
    assert "maint_notifications_config" not in pool.connection_kwargs
    assert "maint_notifications_pool_handler" not in pool.connection_kwargs
    assert "orig_host_address" not in pool.connection_kwargs


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_unix_socket_pool_auto_maint_notifications_noop():
    config = MaintNotificationsConfig(enabled="auto")
    pool = ConnectionPool(
        connection_class=UnixDomainSocketConnection,
        path="/tmp/redis.sock",
        protocol=3,
    )

    await pool.update_maint_notifications_config(config)

    assert pool._maint_notifications_pool_handler is None
    assert not pool.maint_notifications_enabled()
    assert "maint_notifications_config" not in pool.connection_kwargs
    assert "maint_notifications_pool_handler" not in pool.connection_kwargs


@pytest.mark.fixed_client
def test_async_unix_socket_pool_rejects_enabled_maint_notifications():
    config = MaintNotificationsConfig(enabled=True)

    with pytest.raises(
        RedisError,
        match="Maintenance notifications are not supported for Unix domain socket connections",
    ):
        ConnectionPool(
            connection_class=UnixDomainSocketConnection,
            path="/tmp/redis.sock",
            protocol=3,
            maint_notifications_config=config,
        )


@pytest.mark.fixed_client
def test_async_redis_unix_socket_rejects_enabled_maint_notifications():
    config = MaintNotificationsConfig(enabled=True)

    with pytest.raises(
        RedisError,
        match="Maintenance notifications are not supported with Unix domain socket connections",
    ):
        redis.Redis(
            unix_socket_path="/tmp/redis.sock",
            protocol=3,
            maint_notifications_config=config,
        )


@pytest.mark.fixed_client
@pytest.mark.asyncio
@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
async def test_async_pool_applies_and_cleans_up_moving_notification(pool_class):
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = pool_class(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    active_matching = DummyAsyncConnection(peer=DEFAULT_HOST)
    active_other = DummyAsyncConnection(peer="8.8.8.8")
    free_matching = DummyAsyncConnection(peer=DEFAULT_HOST)
    free_other = DummyAsyncConnection(peer="8.8.8.8")
    pool._in_use_connections.update({active_matching, active_other})
    pool._available_connections.extend([free_matching, free_other])
    notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=True,
    )

    for connection in (active_matching, free_matching):
        assert connection.maintenance_state == MaintenanceState.MOVING
        assert connection.maintenance_notification_hash == hash(notification)
        assert connection.host == MOVED_HOST
        assert connection.socket_timeout == RELAXED_TIMEOUT
        assert connection.socket_connect_timeout == RELAXED_TIMEOUT
        assert connection.timeout_updates == [RELAXED_TIMEOUT]

    assert active_matching.should_reconnect()
    assert free_matching.disconnect_calls == 1
    assert active_other.maintenance_state == MaintenanceState.NONE
    assert free_other.maintenance_state == MaintenanceState.NONE

    assert pool.connection_kwargs["host"] == MOVED_HOST
    assert pool.connection_kwargs["port"] == DEFAULT_PORT
    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.MOVING
    assert pool.connection_kwargs["maintenance_notification_hash"] == hash(notification)

    await pool.cleanup_moving_notification(
        notification_hash=hash(notification),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    for connection in (active_matching, free_matching):
        assert connection.maintenance_state == MaintenanceState.NONE
        assert connection.maintenance_notification_hash is None
        assert connection.host == DEFAULT_HOST
        assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT
        assert connection.socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
        assert connection.timeout_updates == [RELAXED_TIMEOUT, -1]

    assert pool.connection_kwargs["host"] == DEFAULT_HOST
    assert pool.connection_kwargs["port"] == DEFAULT_PORT
    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.NONE
    assert pool.connection_kwargs["maintenance_notification_hash"] is None


@pytest.mark.fixed_client
@pytest.mark.asyncio
@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
async def test_async_pool_moving_with_disabled_relaxed_timeout_preserves_timeouts(
    pool_class,
):
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=-1,
    )
    pool = pool_class(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = DummyAsyncConnection(peer=DEFAULT_HOST)
    pool._in_use_connections.add(connection)
    notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=True,
    )

    assert connection.maintenance_state == MaintenanceState.MOVING
    assert connection.maintenance_notification_hash == hash(notification)
    assert connection.host == MOVED_HOST
    assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT
    assert connection.socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
    assert connection.timeout_updates == [-1]


@pytest.mark.fixed_client
@pytest.mark.asyncio
@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
async def test_async_new_connections_inherit_moving_settings_until_cleanup(pool_class):
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = pool_class(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=False,
    )

    moving_connection = pool.make_connection()
    _assert_connection_in_moving_state(
        moving_connection,
        expected_notification_hash=hash(notification),
    )
    assert moving_connection.orig_host_address == DEFAULT_HOST

    await pool.cleanup_moving_notification(
        notification_hash=hash(notification),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    restored_connection = pool.make_connection()
    _assert_connection_in_default_state(restored_connection)
    assert restored_connection.orig_host_address == DEFAULT_HOST


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_migrated_after_moving_does_not_clear_moving_state():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    connection = DummyAsyncConnection()
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    pool._in_use_connections.add(connection)
    moving_notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(
        notification=moving_notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=False,
    )

    handler = AsyncMaintNotificationsConnectionHandler(connection, config)
    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await handler.handle_notification(NodeMigratedNotification(id=2))

    _assert_connection_in_moving_state(
        connection,
        expected_notification_hash=hash(moving_notification),
    )
    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.MOVING
    relaxed_timeout_metric.assert_not_awaited()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_overlapping_moving_cleanup_only_reverts_latest_notification():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = DummyAsyncConnection(peer=DEFAULT_HOST)
    pool._in_use_connections.add(connection)
    first = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )
    second_host = "5.6.7.8"
    second = NodeMovingNotification(
        id=2,
        new_node_host=second_host,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(first, config, DEFAULT_HOST)
    await pool.apply_moving_notification(second, config, DEFAULT_HOST)

    assert pool.connection_kwargs["host"] == second_host
    _assert_connection_in_moving_state(
        connection,
        expected_host=second_host,
        expected_notification_hash=hash(second),
    )

    await pool.cleanup_moving_notification(
        notification_hash=hash(first),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    assert pool.connection_kwargs["host"] == second_host
    assert pool.connection_kwargs["maintenance_notification_hash"] == hash(second)
    _assert_connection_in_moving_state(
        connection,
        expected_host=second_host,
        expected_notification_hash=hash(second),
    )

    await pool.cleanup_moving_notification(
        notification_hash=hash(second),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    assert pool.connection_kwargs["host"] == DEFAULT_HOST
    _assert_connection_in_default_state(connection)


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_moving_without_target_marks_for_reconnect_on_delayed_run():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    active = DummyAsyncConnection(peer=DEFAULT_HOST)
    free = DummyAsyncConnection(peer=DEFAULT_HOST)
    pool._in_use_connections.add(active)
    pool._available_connections.append(free)
    notification = NodeMovingNotification(
        id=1,
        new_node_host=None,
        new_node_port=None,
        ttl=5,
    )

    await pool.apply_moving_notification(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=False,
    )

    assert not active.should_reconnect()
    assert free.disconnect_calls == 0
    _assert_connection_in_moving_state(
        active,
        expected_host=DEFAULT_HOST,
        expected_notification_hash=hash(notification),
    )

    await pool.run_proactive_reconnect(DEFAULT_HOST)

    assert active.should_reconnect()
    assert free.disconnect_calls == 1


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_moving_notifications_only_update_matching_address_connections():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = ConnectionPool(
        host="test.address.com",
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    key1_connections = [
        DummyAsyncConnection(host="test.address.com", peer="1.2.3.4"),
        DummyAsyncConnection(host="test.address.com", peer="1.2.3.4"),
    ]
    key2_connections = [
        DummyAsyncConnection(host="test.address.com", peer="5.6.7.8"),
        DummyAsyncConnection(host="test.address.com", peer="5.6.7.8"),
    ]
    key3_connection = DummyAsyncConnection(host="test.address.com", peer="9.10.11.12")
    pool._in_use_connections.update(
        {key1_connections[0], key2_connections[0], key3_connection}
    )
    pool._available_connections.extend([key1_connections[1], key2_connections[1]])
    first = NodeMovingNotification(
        id=1,
        new_node_host="13.14.15.16",
        new_node_port=DEFAULT_PORT,
        ttl=5,
    )
    second = NodeMovingNotification(
        id=2,
        new_node_host="17.18.19.20",
        new_node_port=DEFAULT_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(first, config, "1.2.3.4")

    for connection in key1_connections:
        _assert_connection_in_moving_state(
            connection,
            expected_host="13.14.15.16",
            expected_notification_hash=hash(first),
        )
    for connection in (*key2_connections, key3_connection):
        _assert_connection_in_default_state(
            connection, expected_host="test.address.com"
        )

    await pool.apply_moving_notification(second, config, "5.6.7.8")

    for connection in key1_connections:
        _assert_connection_in_moving_state(
            connection,
            expected_host="13.14.15.16",
            expected_notification_hash=hash(first),
        )
    for connection in key2_connections:
        _assert_connection_in_moving_state(
            connection,
            expected_host="17.18.19.20",
            expected_notification_hash=hash(second),
        )
    _assert_connection_in_default_state(
        key3_connection, expected_host="test.address.com"
    )

    key2_handler = AsyncMaintNotificationsConnectionHandler(key2_connections[0], config)
    key3_handler = AsyncMaintNotificationsConnectionHandler(key3_connection, config)
    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ),
    ):
        await key2_handler.handle_notification(NodeMigratingNotification(id=3, ttl=5))
        await key2_handler.handle_notification(NodeMigratedNotification(id=3))
        await key3_handler.handle_notification(NodeMigratingNotification(id=4, ttl=5))
        await key3_handler.handle_notification(NodeMigratedNotification(id=4))

    _assert_connection_in_moving_state(
        key2_connections[0],
        expected_host="17.18.19.20",
        expected_notification_hash=hash(second),
    )
    _assert_connection_in_default_state(
        key3_connection, expected_host="test.address.com"
    )

    await pool.cleanup_moving_notification(
        notification_hash=hash(first),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    for connection in key1_connections:
        _assert_connection_in_default_state(
            connection, expected_host="test.address.com"
        )
    for connection in key2_connections:
        _assert_connection_in_moving_state(
            connection,
            expected_host="17.18.19.20",
            expected_notification_hash=hash(second),
        )

    await pool.cleanup_moving_notification(
        notification_hash=hash(second),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    for connection in (*key1_connections, *key2_connections, key3_connection):
        _assert_connection_in_default_state(
            connection, expected_host="test.address.com"
        )


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_moving_migrating_migrated_failed_over_state_transitions():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = DummyAsyncConnection(peer=DEFAULT_HOST)
    free_connection = DummyAsyncConnection(peer=DEFAULT_HOST)
    pool._in_use_connections.add(connection)
    pool._available_connections.append(free_connection)
    moving = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(
        moving,
        config,
        DEFAULT_HOST,
        run_proactive_reconnect=True,
    )
    _assert_connection_in_moving_state(
        connection,
        expected_notification_hash=hash(moving),
    )
    _assert_connection_in_moving_state(
        free_connection,
        expected_notification_hash=hash(moving),
    )

    connection_handler = AsyncMaintNotificationsConnectionHandler(connection, config)
    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await connection_handler.handle_notification(
            NodeMigratingNotification(id=2, ttl=5)
        )
        await connection_handler.handle_notification(NodeMigratedNotification(id=2))
        await connection_handler.handle_notification(
            NodeFailingOverNotification(id=3, ttl=5)
        )
        await connection_handler.handle_notification(NodeFailedOverNotification(id=3))

    _assert_connection_in_moving_state(
        connection,
        expected_notification_hash=hash(moving),
    )
    relaxed_timeout_metric.assert_not_awaited()

    await pool.cleanup_moving_notification(
        notification_hash=hash(moving),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    _assert_connection_in_default_state(connection)
    _assert_connection_in_default_state(free_connection)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ),
    ):
        await connection_handler.handle_notification(
            NodeFailingOverNotification(id=4, ttl=5)
        )
        assert connection.maintenance_state == MaintenanceState.MAINTENANCE
        await connection_handler.handle_notification(NodeFailedOverNotification(id=4))

    _assert_connection_in_default_state(connection)


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_concurrent_moving_notification_handling_is_deduplicated():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = DummyAsyncConnection(peer=DEFAULT_HOST)
    pool._in_use_connections.add(connection)
    handler = AsyncMaintNotificationsPoolHandler(pool, config)
    handler.set_connection(connection)
    handler._schedule = MagicMock()
    notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ) as handoff_metric:
        await asyncio.gather(
            *(handler.handle_notification(notification) for _ in range(5))
        )

    _assert_connection_in_moving_state(
        connection,
        expected_notification_hash=hash(notification),
    )
    assert len(handler._processed_notifications) == 1
    handler._schedule.assert_called_once_with(
        notification.ttl,
        handler.handle_node_moved_notification,
        notification,
    )
    handoff_metric.assert_awaited_once()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pool_handler_deduplicates_moving_notifications():
    config = MaintNotificationsConfig(enabled=True, proactive_reconnect=True)
    pool = MagicMock()
    pool.apply_moving_notification = AsyncMock()
    handler = AsyncMaintNotificationsPoolHandler(pool, config)
    handler.set_connection(DummyAsyncConnection())
    handler._schedule = MagicMock()
    notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ) as handoff_metric:
        await handler.handle_notification(notification)
        await handler.handle_notification(notification)

    pool.apply_moving_notification.assert_awaited_once_with(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=True,
    )
    handler._schedule.assert_called_once_with(
        notification.ttl,
        handler.handle_node_moved_notification,
        notification,
    )
    handoff_metric.assert_awaited_once()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pool_handler_schedules_delayed_reconnect_for_unknown_target():
    config = MaintNotificationsConfig(enabled=True, proactive_reconnect=True)
    pool = MagicMock()
    pool.apply_moving_notification = AsyncMock()
    handler = AsyncMaintNotificationsPoolHandler(pool, config)
    handler.set_connection(DummyAsyncConnection())
    handler._schedule = MagicMock()
    notification = NodeMovingNotification(
        id=1,
        new_node_host=None,
        new_node_port=None,
        ttl=10,
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await handler.handle_notification(notification)

    pool.apply_moving_notification.assert_awaited_once_with(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=False,
    )
    handler._schedule.assert_has_calls(
        [
            mock.call(
                notification.ttl / 2, handler.run_proactive_reconnect, DEFAULT_HOST
            ),
            mock.call(
                notification.ttl, handler.handle_node_moved_notification, notification
            ),
        ]
    )


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_blocking_pool_locks_get_and_release_regardless_of_maintenance():
    pool = BlockingConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True
        ),
    )

    real_lock = pool._lock
    pool._lock = MagicMock(wraps=real_lock)

    fake_conn = MagicMock()
    fake_conn.connect = AsyncMock()
    fake_conn.can_read = AsyncMock(return_value=False)
    fake_conn.should_reconnect = MagicMock(return_value=False)
    fake_conn.re_auth = AsyncMock()
    pool._available_connections.append(fake_conn)

    # Both checkout and release acquire _lock outside maintenance.
    conn = await pool.get_connection()
    await pool.release(conn)
    assert pool._lock.__aenter__.call_count == 2

    # Maintenance does not change the locking behavior.
    pool.set_in_maintenance(True)
    try:
        conn = await pool.get_connection()
        await pool.release(conn)
    finally:
        pool.set_in_maintenance(False)
    assert pool._lock.__aenter__.call_count == 4


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_apply_moving_notification_toggles_set_in_maintenance():
    pool = BlockingConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True
        ),
    )
    observed = []
    original = pool.set_in_maintenance

    def spy(value: bool) -> None:
        observed.append(value)
        original(value)

    pool.set_in_maintenance = spy  # type: ignore[assignment]

    notification = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )
    await pool.apply_moving_notification(
        notification=notification,
        config=pool._maint_notifications_pool_handler.config,
        moving_address_src=None,
        run_proactive_reconnect=False,
    )

    assert observed == [True, False]


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_cancel_scheduled_tasks_cancels_pending_and_awaits():
    config = MaintNotificationsConfig(enabled=True, proactive_reconnect=True)
    handler = AsyncMaintNotificationsPoolHandler(MagicMock(), config)

    callback = AsyncMock()
    handler._schedule(60, callback)
    handler._schedule(60, callback)
    tasks = tuple(handler._scheduled_tasks)
    assert len(tasks) == 2

    await handler.cancel_scheduled_tasks()

    for task in tasks:
        assert task.done()
        assert task.cancelled()
    callback.assert_not_awaited()
    assert handler._scheduled_tasks == set()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_cancel_scheduled_tasks_noop_when_empty():
    config = MaintNotificationsConfig(enabled=True, proactive_reconnect=True)
    handler = AsyncMaintNotificationsPoolHandler(MagicMock(), config)
    await handler.cancel_scheduled_tasks()
    assert handler._scheduled_tasks == set()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_pool_aclose_cancels_handler_scheduled_tasks():
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True
        ),
    )
    handler = pool._maint_notifications_pool_handler
    assert handler is not None

    callback = AsyncMock()
    handler._schedule(60, callback)
    handler._schedule(60, callback)
    tasks = tuple(handler._scheduled_tasks)
    assert len(tasks) == 2

    await pool.aclose()

    for task in tasks:
        assert task.cancelled()
    callback.assert_not_awaited()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_pool_aclose_completes_superseded_handoff_event():
    """
    Closing the pool ends the relaxation of every pending handoff, also of the
    one a newer MOVING superseded, so each started event gets its pair.
    """
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    handler = pool._maint_notifications_pool_handler
    handler.set_connection(DummyAsyncConnection(peer=DEFAULT_HOST))
    first = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=60
    )
    second = NodeMovingNotification(
        id=2, new_node_host="10.9.8.7", new_node_port=MOVED_PORT, ttl=60
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await handler.handle_node_moving_notification(first)
        await handler.handle_node_moving_notification(second)
        assert [type(e) for e in listener.events] == [MaintenanceStartedEvent] * 2

        await pool.aclose()

    completed = [e for e in listener.events if isinstance(e, MaintenanceCompletedEvent)]
    assert {e.notification for e in completed} == {first, second}
    assert len(completed) == 2


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_pool_aclose_completes_pending_handoff_event():
    """
    Closing the pool cancels the cleanup that would have ended the handoff's
    relaxation; the completion it would have reported is reported instead.
    """
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    handler = pool._maint_notifications_pool_handler
    handler.set_connection(DummyAsyncConnection(peer=DEFAULT_HOST))
    notification = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=60
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await handler.handle_node_moving_notification(notification)
        assert [type(e) for e in listener.events] == [MaintenanceStartedEvent]

        await pool.aclose()

    assert [type(e) for e in listener.events] == [
        MaintenanceStartedEvent,
        MaintenanceCompletedEvent,
    ]
    completed = listener.events[1]
    assert completed.state == MaintenanceState.MOVING
    assert completed.notification is notification
    assert completed.connection_pool is pool
    assert handler._pending_handoffs == set()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_concurrent_aclose_completes_pending_handoff_once():
    """
    Two aclose() calls racing over the same pending handoff report its
    completion once: the second closer must not take the cleanup the first
    one already ran for a superseded handoff.
    """
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    handler = pool._maint_notifications_pool_handler
    handler.set_connection(DummyAsyncConnection(peer=DEFAULT_HOST))
    notification = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=60
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await handler.handle_node_moving_notification(notification)
        await asyncio.gather(pool.aclose(), pool.aclose())

    assert [type(e) for e in listener.events] == [
        MaintenanceStartedEvent,
        MaintenanceCompletedEvent,
    ]
    assert handler._pending_handoffs == set()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_pool_aclose_disconnects_even_when_moving_cleanup_fails():
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=10
        ),
    )
    handler = pool._maint_notifications_pool_handler
    notification = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=60
    )
    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await handler.handle_node_moving_notification(notification)
    disconnect = AsyncMock()

    failing_cleanup = mock.patch.object(
        pool,
        "cleanup_moving_notification",
        side_effect=OSError(9, "Bad file descriptor"),
    )
    with failing_cleanup, mock.patch.object(pool, "disconnect", new=disconnect):
        with pytest.raises(OSError):
            await pool.aclose()

    # the pool is closed regardless; the handoff stays for a retried aclose()
    disconnect.assert_awaited_once_with()
    assert handler._pending_handoffs == {notification}


@pytest.mark.fixed_client
@pytest.mark.asyncio
@pytest.mark.parametrize("new_node_host", [MOVED_HOST, None])
async def test_pool_reused_after_aclose_mid_moving_has_reverted_kwargs(new_node_host):
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        socket_timeout=3,
        socket_connect_timeout=4,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=10
        ),
    )
    handler = pool._maint_notifications_pool_handler
    original = dict(pool.connection_kwargs)
    notification = NodeMovingNotification(
        id=1, new_node_host=new_node_host, new_node_port=MOVED_PORT, ttl=60
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await handler.handle_node_moving_notification(notification)
        assert pool.connection_kwargs["socket_timeout"] == 10
        if new_node_host is not None:
            assert pool.connection_kwargs["host"] == new_node_host
        await pool.aclose()

    for key in ("host", "socket_timeout", "socket_connect_timeout"):
        assert pool.connection_kwargs[key] == original[key]
    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.NONE
    assert pool.connection_kwargs["maintenance_notification_hash"] is None
    assert handler._pending_handoffs == set()
    # A connection made after the pool is reused does not inherit the MOVING state
    connection = pool.make_connection()
    assert connection.host == DEFAULT_HOST
    assert connection.socket_timeout == 3
    assert connection.socket_connect_timeout == 4


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_moving_redelivered_after_aclose_is_applied_again():
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        socket_timeout=3,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=10
        ),
    )
    handler = pool._maint_notifications_pool_handler
    notification = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=60
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await handler.handle_node_moving_notification(notification)
        await pool.aclose()
        assert pool.connection_kwargs["host"] == DEFAULT_HOST

        # The server redelivers the still-active MOVING to the reused pool
        await handler.handle_notification(notification)

    assert pool.connection_kwargs["host"] == MOVED_HOST
    assert pool.connection_kwargs["socket_timeout"] == 10
    assert handler._pending_handoffs == {notification}
    await pool.aclose()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_pool_aclose_waits_for_moving_being_handled():
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        socket_timeout=3,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=10
        ),
    )
    handler = pool._maint_notifications_pool_handler
    notification = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=60
    )
    applying = asyncio.Event()
    release = asyncio.Event()
    apply_moving_notification = pool.apply_moving_notification

    async def blocked_apply(**kwargs):
        applying.set()
        await release.wait()
        await apply_moving_notification(**kwargs)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_handoff",
            new=AsyncMock(),
        ),
        mock.patch.object(pool, "apply_moving_notification", new=blocked_apply),
    ):
        handling = asyncio.create_task(
            handler.handle_node_moving_notification(notification)
        )
        await applying.wait()
        closing = asyncio.create_task(pool.aclose())
        await asyncio.sleep(0)
        release.set()
        await asyncio.wait_for(asyncio.gather(handling, closing), timeout=5)

    # aclose() waited for the handler and ran the cleanup it scheduled
    assert handler._scheduled_tasks == set()
    assert handler._pending_handoffs == set()
    assert pool.connection_kwargs["host"] == DEFAULT_HOST
    assert pool.connection_kwargs["socket_timeout"] == 3


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_pool_aclose_retried_after_cancellation_reverts_moving():
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        socket_timeout=3,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=10
        ),
    )
    handler = pool._maint_notifications_pool_handler
    notification = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=60
    )
    cleaning = asyncio.Event()
    release = asyncio.Event()
    cleanup_moving_notification = pool.cleanup_moving_notification

    async def blocked_cleanup(**kwargs):
        cleaning.set()
        await release.wait()
        return await cleanup_moving_notification(**kwargs)

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await handler.handle_node_moving_notification(notification)

    with mock.patch.object(pool, "cleanup_moving_notification", new=blocked_cleanup):
        # aclose() is cancelled after cancelling the timer tasks, while the
        # MOVING cleanup it runs is still waiting
        closing = asyncio.create_task(pool.aclose())
        await asyncio.wait_for(cleaning.wait(), timeout=5)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        release.set()

    assert handler._scheduled_tasks == set()
    await pool.aclose()

    assert pool.connection_kwargs["host"] == DEFAULT_HOST
    assert pool.connection_kwargs["socket_timeout"] == 3
    assert handler._pending_handoffs == set()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_pool_disconnect_does_not_cancel_scheduled_tasks():
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True
        ),
    )
    handler = pool._maint_notifications_pool_handler
    assert handler is not None

    handler._schedule(60, AsyncMock())
    tasks = tuple(handler._scheduled_tasks)

    try:
        await pool.disconnect()
        for task in tasks:
            assert not task.done()
    finally:
        await handler.cancel_scheduled_tasks()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pool_ensure_connection_applies_pending_push_when_enabled():
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(enabled=True),
    )
    connection = MagicMock()
    connection.connect = AsyncMock()
    connection.can_read = AsyncMock(return_value=True)
    connection.disconnect = AsyncMock()
    connection.handle_pending_push_notifications = AsyncMock()

    await pool.ensure_connection(connection)

    connection.handle_pending_push_notifications.assert_awaited_once()
    connection.disconnect.assert_not_awaited()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pool_ensure_connection_reconnects_when_pending_pushes_are_stale():
    """A drain that finds the socket closed makes the pool reconnect silently."""
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(enabled=True),
    )
    connection = MagicMock()
    connection.connect = AsyncMock()
    connection.can_read = AsyncMock(side_effect=[True, False])
    connection.disconnect = AsyncMock()
    connection.handle_pending_push_notifications = AsyncMock(
        side_effect=RedisConnectionError("closed")
    )

    await pool.ensure_connection(connection)

    connection.disconnect.assert_awaited_once()
    assert connection.connect.await_count == 2


class ScriptedParser(_AsyncRESP3Parser):
    """
    Parser that replays a script of buffered push notifications the way the
    real one does: each is handed to the handler installed at read time, and
    an exception in the script is the server's close being reached.
    """

    def __init__(self, socket_read_size):
        super().__init__(socket_read_size)
        self.script = []

    async def can_read(self):
        return bool(self.script)

    async def can_read_destructive(self):
        return bool(self.script)

    async def read_response(self, disable_decoding=False, push_request=False):
        item = self.script.pop(0)
        if item == "hang":
            # A frame only partly received: the rest never arrives
            await asyncio.sleep(10)
        if isinstance(item, tuple):
            # A frame whose rest takes this long to arrive
            delay, item = item
            await asyncio.sleep(delay)
        if isinstance(item, Exception):
            raise item
        if isinstance(item, NodeMovingNotification):
            await self.node_moving_push_handler_func(item)
        elif isinstance(item, NodeMigratingNotification):
            await self.maintenance_push_handler_func(item)
        else:
            # A reply that is not a push notification is returned as it is
            return item
        return None


def _scripted_connection(script):
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    connection = Connection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
        parser_class=ScriptedParser,
    )
    parser = connection._parser
    parser.script = list(script)
    moving_handler = AsyncMock()
    maintenance_handler = AsyncMock()
    parser.set_node_moving_push_handler(moving_handler)
    parser.set_maintenance_push_handler(maintenance_handler)
    return connection, parser, moving_handler, maintenance_handler


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pending_pushes_on_alive_connection_are_applied_in_order():
    migrating = NodeMigratingNotification(id=1, ttl=5)
    moving = NodeMovingNotification(
        id=2, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )
    connection, parser, moving_handler, maintenance_handler = _scripted_connection(
        [migrating, moving]
    )
    order = []
    moving_handler.side_effect = lambda n: order.append(n)
    maintenance_handler.side_effect = lambda n: order.append(n)

    await connection.handle_pending_push_notifications()

    assert order == [migrating, moving]
    assert parser.node_moving_push_handler_func is moving_handler
    assert parser.maintenance_push_handler_func is maintenance_handler


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pending_pushes_on_closed_connection_are_discarded():
    """A closed socket means the maintenance is over: nothing is applied."""
    migrating = NodeMigratingNotification(id=1, ttl=5)
    moving = NodeMovingNotification(
        id=2, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )
    connection, parser, moving_handler, maintenance_handler = _scripted_connection(
        [migrating, moving, RedisConnectionError("Connection closed by server.")]
    )

    with pytest.raises(RedisConnectionError):
        await connection.handle_pending_push_notifications()

    moving_handler.assert_not_awaited()
    maintenance_handler.assert_not_awaited()
    # The handlers are back in place for the pushes the next command reads
    assert parser.node_moving_push_handler_func is moving_handler
    assert parser.maintenance_push_handler_func is maintenance_handler


@pytest.mark.fixed_client
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stale_reply",
    [b"OK", None, ResponseError("MOVED 1337 1.2.3.4:6379")],
    ids=["reply", "null_reply", "error_reply"],
)
async def test_async_stale_reply_on_idle_connection_is_a_connection_error(stale_reply):
    """A reply left unread by an earlier command marks the connection dirty."""
    moving = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )
    connection, parser, moving_handler, _ = _scripted_connection([moving, stale_reply])

    with pytest.raises(RedisConnectionError, match="Connection has data"):
        await connection.handle_pending_push_notifications()

    moving_handler.assert_not_awaited()
    assert parser.node_moving_push_handler_func is moving_handler


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_partial_push_frame_is_a_bounded_connection_error():
    """A frame only partly received must not block the checkout."""
    moving = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )
    connection, parser, moving_handler, _ = _scripted_connection([moving, "hang"])

    with mock.patch(
        "redis.asyncio.connection.PENDING_PUSH_NOTIFICATIONS_READ_TIMEOUT", 0.05
    ):
        with pytest.raises(RedisConnectionError, match="Timed out"):
            await connection.handle_pending_push_notifications()

    moving_handler.assert_not_awaited()
    assert parser.node_moving_push_handler_func is moving_handler


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_drain_deadline_spans_all_pending_pushes():
    """The deadline is for the whole drain, not renewed for every frame."""
    moving = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )
    # Each frame arrives within the deadline on its own; together they do not
    connection, parser, moving_handler, _ = _scripted_connection(
        [(0.03, moving), (0.03, moving)]
    )

    with mock.patch(
        "redis.asyncio.connection.PENDING_PUSH_NOTIFICATIONS_READ_TIMEOUT", 0.05
    ):
        with pytest.raises(RedisConnectionError, match="Timed out"):
            await connection.handle_pending_push_notifications()

    moving_handler.assert_not_awaited()
    assert parser.node_moving_push_handler_func is moving_handler


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pending_pushes_without_handler_are_not_deferred():
    """An OSS cluster connection has no pool handler; its slot stays unset."""
    connection, parser, _, _ = _scripted_connection([])
    parser.set_node_moving_push_handler(None)

    await connection.handle_pending_push_notifications()

    assert parser.node_moving_push_handler_func is None


@pytest.mark.fixed_client
@pytest.mark.asyncio
@pytest.mark.parametrize("enabled, raises", [(True, False), (False, True)])
async def test_async_pool_ensure_connection_handles_pending_push_after_reconnect(
    enabled, raises
):
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(enabled=enabled),
    )
    connection = MagicMock()
    connection.connect = AsyncMock()
    connection.can_read = AsyncMock(side_effect=[RedisConnectionError("closed"), True])
    connection.disconnect = AsyncMock()

    if raises:
        with pytest.raises(RedisConnectionError, match="Connection not ready"):
            await pool.ensure_connection(connection)
    else:
        await pool.ensure_connection(connection)

    assert connection.connect.await_count == 2
    connection.disconnect.assert_awaited_once()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_read_response_reschedules_active_socket_timeout():
    connection = Connection(socket_timeout=0.01, socket_read_size=16384, protocol=2)
    parser = _AsyncRESP2Parser(16384)
    connection._parser = parser
    reader = asyncio.StreamReader()
    connection._reader = reader
    connection._writer = mock.Mock()
    parser.on_connect(connection)
    connection.disconnect = mock.AsyncMock()

    task = asyncio.create_task(connection.read_response())
    for _ in range(1000):
        if parser._active_read_timeout is not None:
            break
        await asyncio.sleep(0.001)
    assert parser._active_read_timeout is not None

    connection.update_current_socket_timeout(0.2)
    # Wait well past the original 0.01 deadline: if the reschedule had not
    # reached the in-flight read, it would have timed out by now.
    await asyncio.sleep(0.05)
    reader.feed_data(b"$2\r\nOK\r\n")
    reader.feed_eof()

    assert await task == b"OK"
    assert parser._active_read_timeout is None


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_read_response_reschedules_active_socket_timeout_to_blocking():
    connection = Connection(socket_timeout=0.01, socket_read_size=16384, protocol=2)
    parser = _AsyncRESP2Parser(16384)
    connection._parser = parser
    reader = asyncio.StreamReader()
    connection._reader = reader
    connection._writer = mock.Mock()
    parser.on_connect(connection)
    connection.disconnect = mock.AsyncMock()

    task = asyncio.create_task(connection.read_response())
    for _ in range(1000):
        if parser._active_read_timeout is not None:
            break
        await asyncio.sleep(0.001)
    assert parser._active_read_timeout is not None

    connection.update_current_socket_timeout(None)
    # Wait well past the original 0.01 deadline: if it had not been
    # cleared, the in-flight read would have timed out by now.
    await asyncio.sleep(0.05)
    reader.feed_data(b"$2\r\nOK\r\n")
    reader.feed_eof()

    assert await task == b"OK"
    assert parser._active_read_timeout is None


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_read_response_does_not_reschedule_explicit_timeout():
    connection = Connection(socket_timeout=0.2)

    class RelaxingParser:
        async def read_response(self, disable_decoding=False, push_request=False):
            connection.update_current_socket_timeout(0.2)
            await asyncio.sleep(0.05)
            return b"OK"

    connection._parser = RelaxingParser()

    assert await connection.read_response(timeout=0.01) is None


@pytest.mark.fixed_client
def test_async_redis_maint_notifications_config_forwarded_to_internal_pool():
    config = MaintNotificationsConfig(enabled=True)
    client = redis.Redis(protocol=3, maint_notifications_config=config)

    assert (
        client.connection_pool.connection_kwargs["maint_notifications_config"] is config
    )
    assert client.connection_pool._maint_notifications_pool_handler.config is config


@pytest.mark.fixed_client
def test_async_redis_maint_notifications_config_requires_resp3():
    config = MaintNotificationsConfig(enabled=True)

    with pytest.raises(
        RedisError,
        match="Maintenance notifications handlers on connection are only supported",
    ):
        redis.Redis(protocol=2, maint_notifications_config=config)


@pytest.mark.fixed_client
def test_async_redis_maint_notifications_config_ignored_with_explicit_pool():
    pool = MagicMock()
    pool.connection_kwargs = {"protocol": 2, "legacy_responses": True}
    pool.get_encoder.return_value = MagicMock()
    config = MaintNotificationsConfig(enabled=True)

    client = redis.Redis(connection_pool=pool, maint_notifications_config=config)

    assert client.connection_pool is pool
    assert "maint_notifications_config" not in pool.connection_kwargs


async def _make_retry_call(do, fail, is_retryable=None, with_failure_count=False):
    return await do()


def _mock_single_connection_pool(connection):
    pool = MagicMock()
    pool.get_connection = AsyncMock(return_value=connection)
    pool.release = AsyncMock()
    pool.get_encoder.return_value = MagicMock()
    pool.get_protocol.return_value = 2
    return pool


def _mock_single_connection(should_reconnect):
    connection = MagicMock()
    connection.host = "localhost"
    connection.port = DEFAULT_PORT
    connection.db = 0
    connection.should_reconnect.return_value = should_reconnect
    connection.disconnect = AsyncMock()
    connection.connect = AsyncMock()
    connection.retry = MagicMock()
    connection.retry.call_with_retry = _make_retry_call
    connection.retry.get_retries.return_value = 0
    return connection


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_single_connection_client_reconnects_marked_connection():
    connection = _mock_single_connection(should_reconnect=True)
    pool = _mock_single_connection_pool(connection)
    client = redis.Redis(connection_pool=pool, single_connection_client=True)

    async def mock_send(*args, **kwargs):
        return b"PONG"

    client._send_command_parse_response = mock_send

    assert await client.execute_command("PING") == b"PONG"
    connection.disconnect.assert_awaited_once_with(error=None, failure_count=None)
    connection.connect.assert_awaited_once()
    pool.release.assert_not_awaited()

    await client.aclose(close_connection_pool=False)


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_single_connection_client_does_not_reconnect_unmarked_connection():
    connection = _mock_single_connection(should_reconnect=False)
    pool = _mock_single_connection_pool(connection)
    client = redis.Redis(connection_pool=pool, single_connection_client=True)

    async def mock_send(*args, **kwargs):
        return b"PONG"

    client._send_command_parse_response = mock_send

    assert await client.execute_command("PING") == b"PONG"
    connection.disconnect.assert_not_awaited()
    connection.connect.assert_not_awaited()
    pool.release.assert_not_awaited()

    await client.aclose(close_connection_pool=False)


class _RecordingListener(EventListenerInterface):
    def __init__(self):
        self.events = []

    def listen(self, event):
        self.events.append(event)


def _config_with_listener(listener, **kwargs):
    dispatcher = EventDispatcher(
        {
            MaintenanceStartedEvent: [listener],
            MaintenanceCompletedEvent: [listener],
        }
    )
    kwargs.setdefault("enabled", True)
    kwargs.setdefault("relaxed_timeout", RELAXED_TIMEOUT)
    return MaintNotificationsConfig(event_dispatcher=dispatcher, **kwargs)


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_connection_handler_dispatches_maintenance_event_pair():
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    connection = DummyAsyncConnection()
    handler = AsyncMaintNotificationsConnectionHandler(connection, config)
    migrating = NodeMigratingNotification(id=1, ttl=5)
    migrated = NodeMigratedNotification(id=1)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ),
    ):
        await handler.handle_notification(migrating)
        assert connection.maintenance_state == MaintenanceState.MAINTENANCE
        await handler.handle_notification(migrated)

    assert [type(e) for e in listener.events] == [
        MaintenanceStartedEvent,
        MaintenanceCompletedEvent,
    ]
    started, completed = listener.events
    assert started.state == MaintenanceState.MAINTENANCE
    assert started.connection is connection
    assert started.connection_pool is None
    assert started.notification is migrating
    assert started.config is config
    assert completed.state == MaintenanceState.MAINTENANCE
    assert completed.connection is connection
    assert completed.notification is migrated


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_connection_handler_completion_without_start_dispatches_nothing():
    """The start may have been routed to another connection of the cluster."""
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    connection = DummyAsyncConnection()
    handler = AsyncMaintNotificationsConnectionHandler(connection, config)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ),
    ):
        await handler.handle_notification(NodeMigratedNotification(id=1))

    assert listener.events == []


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_connection_handler_dispatches_nothing_when_relaxation_disabled():
    listener = _RecordingListener()
    config = _config_with_listener(listener, relaxed_timeout=-1)
    connection = DummyAsyncConnection()
    handler = AsyncMaintNotificationsConnectionHandler(connection, config)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ),
    ):
        await handler.handle_notification(NodeMigratingNotification(id=1, ttl=5))
        await handler.handle_notification(NodeMigratedNotification(id=1))

    assert listener.events == []


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pool_handler_started_event_precedes_relaxing_the_pool():
    """
    Taking over a connection under maintenance completes that connection's
    pair; the handoff's pair must already be open for a listener by then.
    """
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    handler = AsyncMaintNotificationsPoolHandler(pool, config)
    seen = []

    async def apply(**kwargs):
        seen.extend(type(e) for e in listener.events)

    pool.apply_moving_notification = apply

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        try:
            await handler.handle_node_moving_notification(
                NodeMovingNotification(
                    id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
                )
            )
        finally:
            await handler.cancel_scheduled_tasks()

    assert seen == [MaintenanceStartedEvent]


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_failed_handoff_closes_its_pair():
    """A pool mutation that fails leaves no relaxation to end later."""
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    handler = AsyncMaintNotificationsPoolHandler(pool, config)
    pool.apply_moving_notification = AsyncMock(side_effect=RuntimeError("boom"))
    notification = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )

    with pytest.raises(RuntimeError):
        await handler.handle_node_moving_notification(notification)

    assert [type(e) for e in listener.events] == [
        MaintenanceStartedEvent,
        MaintenanceCompletedEvent,
    ]
    assert listener.events[1].notification is notification
    assert handler._scheduled_tasks == set()
    assert handler._pending_handoffs == set()
    assert notification not in handler._processed_notifications


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pool_handler_dispatches_moving_event_pair():
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    handler = AsyncMaintNotificationsPoolHandler(pool, config)
    connection = DummyAsyncConnection(peer=DEFAULT_HOST)
    handler.set_connection(connection)
    notification = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        try:
            await handler.handle_node_moving_notification(notification)
            # The same notification delivered on another connection is a no-op
            await handler.handle_node_moving_notification(notification)

            assert len(listener.events) == 1
            started = listener.events[0]
            assert isinstance(started, MaintenanceStartedEvent)
            assert started.state == MaintenanceState.MOVING
            assert started.connection_pool is pool
            assert started.connection is connection
            assert started.notification is notification
            assert started.config is config

            await handler.handle_node_moved_notification(notification)
        finally:
            await handler.cancel_scheduled_tasks()

    assert len(listener.events) == 2
    completed = listener.events[1]
    assert isinstance(completed, MaintenanceCompletedEvent)
    assert completed.state == MaintenanceState.MOVING
    assert completed.connection_pool is pool
    assert completed.notification is notification


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pool_handler_dispatches_nothing_when_handoff_relaxes_nothing():
    """A handoff handled only for the proactive reconnect relaxes no timeout."""
    listener = _RecordingListener()
    config = _config_with_listener(
        listener, proactive_reconnect=True, relaxed_timeout=-1
    )
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    handler = AsyncMaintNotificationsPoolHandler(pool, config)
    notification = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        try:
            await handler.handle_node_moving_notification(notification)
            # The handoff itself is still handled
            assert pool.connection_kwargs["host"] == MOVED_HOST
            await handler.handle_node_moved_notification(notification)
        finally:
            await handler.cancel_scheduled_tasks()

    assert pool.connection_kwargs["host"] == DEFAULT_HOST
    assert listener.events == []


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_pool_handler_superseded_moving_completes_once():
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    handler = AsyncMaintNotificationsPoolHandler(pool, config)
    first = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )
    second = NodeMovingNotification(
        id=2, new_node_host="5.6.7.8", new_node_port=MOVED_PORT, ttl=5
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        try:
            await handler.handle_node_moving_notification(first)
            await handler.handle_node_moving_notification(second)
            assert [type(e) for e in listener.events] == [
                MaintenanceStartedEvent,
                MaintenanceStartedEvent,
            ]

            # The first handoff expires while the second still owns the pool
            await handler.handle_node_moved_notification(first)
            assert len(listener.events) == 2

            await handler.handle_node_moved_notification(second)
        finally:
            await handler.cancel_scheduled_tasks()

    assert len(listener.events) == 3
    completed = listener.events[2]
    assert isinstance(completed, MaintenanceCompletedEvent)
    assert completed.notification is second


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_cleanup_moving_notification_reports_whether_it_reverted():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    first = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )
    second = NodeMovingNotification(
        id=2, new_node_host="5.6.7.8", new_node_port=MOVED_PORT, ttl=5
    )
    await pool.apply_moving_notification(first, config, DEFAULT_HOST)
    await pool.apply_moving_notification(second, config, DEFAULT_HOST)

    assert (
        await pool.cleanup_moving_notification(
            notification_hash=hash(first),
            reset_relaxed_timeout=True,
            reset_host_address=True,
        )
        is False
    )
    assert (
        await pool.cleanup_moving_notification(
            notification_hash=hash(second),
            reset_relaxed_timeout=True,
            reset_host_address=True,
        )
        is True
    )


def _connected_async_connection(config):
    connection = Connection(protocol=3, maint_notifications_config=config)
    # disconnect() is a no-op on a connection that was never connected
    connection._reader = MagicMock()
    connection._writer = MagicMock()
    connection._writer.wait_closed = AsyncMock()
    return connection


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_disconnect_without_socket_still_completes_maintenance():
    """
    The relaxation is restored and reported on every disconnect, including
    one that finds no socket to close and returns early.
    """
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    connection = Connection(protocol=3, maint_notifications_config=config)
    connection.maintenance_state = MaintenanceState.MAINTENANCE
    assert not connection.is_connected

    await connection.disconnect()

    assert connection.maintenance_state == MaintenanceState.NONE
    assert [type(e) for e in listener.events] == [MaintenanceCompletedEvent]


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_disconnect_in_maintenance_dispatches_completed_event():
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    connection = _connected_async_connection(config)
    connection.maintenance_state = MaintenanceState.MAINTENANCE

    await connection.disconnect()

    assert connection.maintenance_state == MaintenanceState.NONE
    assert len(listener.events) == 1
    event = listener.events[0]
    assert isinstance(event, MaintenanceCompletedEvent)
    assert event.state == MaintenanceState.MAINTENANCE
    assert event.connection is connection
    assert event.connection_pool is None
    assert event.notification is None
    assert event.config is config


@pytest.mark.fixed_client
@pytest.mark.asyncio
@pytest.mark.parametrize("state", [MaintenanceState.NONE, MaintenanceState.MOVING])
async def test_async_disconnect_outside_maintenance_dispatches_nothing(state):
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    connection = _connected_async_connection(config)
    connection.maintenance_state = state

    await connection.disconnect()

    assert listener.events == []


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_handoff_taking_over_maintenance_dispatches_completed_event():
    """
    A connection under maintenance that a MOVING handoff takes over has its
    MAINTENANCE pair closed: its completion notification is skipped while it
    is MOVING, and the handoff's cleanup does not know what it superseded.
    """
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = DummyAsyncConnection()
    connection.maint_notifications_config = config
    connection.maintenance_state = MaintenanceState.MAINTENANCE

    pool.update_connection_settings(connection, state=MaintenanceState.MOVING)

    assert connection.maintenance_state == MaintenanceState.MOVING
    assert len(listener.events) == 1
    event = listener.events[0]
    assert isinstance(event, MaintenanceCompletedEvent)
    assert event.state == MaintenanceState.MAINTENANCE
    assert event.connection is connection
    assert event.connection_pool is pool
    assert event.notification is None


@pytest.mark.fixed_client
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state, new_state",
    [
        (MaintenanceState.NONE, MaintenanceState.MOVING),
        (MaintenanceState.MOVING, MaintenanceState.MOVING),
        (MaintenanceState.MOVING, MaintenanceState.NONE),
    ],
)
async def test_async_other_state_transitions_dispatch_nothing(state, new_state):
    listener = _RecordingListener()
    config = _config_with_listener(listener)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = DummyAsyncConnection()
    connection.maint_notifications_config = config
    connection.maintenance_state = state

    pool.update_connection_settings(connection, state=new_state)

    assert connection.maintenance_state == new_state
    assert listener.events == []
