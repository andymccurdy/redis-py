"""Tests for Redis Enterprise maintenance push notifications — async client."""

import asyncio
import json
import logging
import random
import time
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import pytest
import pytest_asyncio

from redis.asyncio import Redis, RedisCluster
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.maint_notifications import (
    EndpointType,
    MaintenanceState,
)
from tests.test_asyncio.test_scenario.conftest import (
    _get_async_client_maint_notifications,
    get_async_cluster_client_maint_notifications,
    get_async_standalone_client_maint_notifications,
)
from tests.test_asyncio.test_scenario.maint_notifications_helpers import (
    AsyncClientValidations,
)
from tests.test_scenario.conftest import (
    CLIENT_TIMEOUT,
    RELAXED_TIMEOUT,
    _FAULT_INJECTOR_CLIENT_OSS_API,
    _FAULT_INJECTOR_STANDALONE_CLIENT,
    use_mock_proxy,
)
from tests.test_asyncio.test_scenario.async_fault_injector_client import (
    AsyncFaultInjectorClient,
    AsyncProxyServerFaultInjector,
)
from tests.test_scenario.fault_injector_client import (
    SlotMigrateEffects,
    TopologyChangeStandaloneEffects,
)
from tests.test_scenario.maint_notifications_helpers import (
    KeyGenerationHelpers,
    generate_params,
    is_endpoint_configured_correctly,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S:%f",
)

logging.getLogger("redis.asyncio.maint_notifications").setLevel(logging.DEBUG)
logging.getLogger("redis.asyncio.cluster").setLevel(logging.DEBUG)
logging.getLogger("redis.asyncio.client").setLevel(logging.DEBUG)
logging.getLogger("redis.asyncio.connection").setLevel(logging.DEBUG)

STANDALONE_MAINT_TIMEOUT = 60

SMIGRATED_TIMEOUT = 120
SLOT_SHUFFLE_TIMEOUT = 300
RESHARD_SMIGRATING_TIMEOUT = int(SLOT_SHUFFLE_TIMEOUT / 2)
RESHARD_OP_TIMEOUT = 600
DEFAULT_TRIGGER_OP_TIMEOUT = 180
RESHARD_TEST_TIMEOUT = RESHARD_OP_TIMEOUT + SMIGRATED_TIMEOUT + 180

DEFAULT_BIND_TTL = 15
DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT = 1
DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT = 1

# The fault injector's create_database can fail on transient cluster-side infrastructure
# problems - a DNS blip while it polls the cluster REST API, for example - that say nothing
# about the client under test. Retrying the setup keeps such a blip from being reported as a
# test failure.
DB_CREATE_ATTEMPTS = 3


class TestAsyncPushNotificationsBase:
    def _drain_command_errors(self, errors, source: str) -> list[str]:
        """Drain and log the collected command errors without failing yet.

        Callers that also await the trigger task or assert on connection state
        must drain *before* that: otherwise the first of those raises and the
        command errors - usually the more informative signal - are never
        reported.

        Every error is logged individually: pytest truncates long assertion
        messages, so a run with many failures would otherwise hide most of them
        behind "...Full output truncated".
        """
        collected = []
        while not errors.empty():
            collected.append(errors.get_nowait())
        if collected:
            logging.error(f"{len(collected)} error(s) collected from {source}:")
            for index, error in enumerate(collected, start=1):
                logging.error(f"  [{index}/{len(collected)}] {error}")
        return collected

    def _fail_on_collected_command_errors(self, collected: list[str], source: str):
        """Fail on errors already drained by ``_drain_command_errors``.

        The failure message keeps a short preview and points at the log for the
        rest.
        """
        if not collected:
            return
        preview = "; ".join(collected[:5])
        if len(collected) > 5:
            preview += f"; ... ({len(collected) - 5} more, see the log above)"
        pytest.fail(f"{len(collected)} error(s) occurred in {source}: {preview}")

    def _fail_on_command_errors(self, errors, source: str):
        """Drain the collected command errors and fail with all of them printed."""
        self._fail_on_collected_command_errors(
            self._drain_command_errors(errors, source), source
        )

    async def _get_all_connections_in_pool(self, client: Redis) -> List:
        connections = []
        async with client.connection_pool._get_pool_lock():
            for conn in client.connection_pool._get_free_connections():
                connections.append(conn)
            for conn in client.connection_pool._get_in_use_connections():
                connections.append(conn)
        return connections

    async def _validate_maintenance_state(
        self, client: Redis, expected_matching_conns_count: int
    ):
        matching_conns_count = 0
        connections = await self._get_all_connections_in_pool(client)

        for conn in connections:
            if (
                conn.is_connected
                and conn.socket_timeout == RELAXED_TIMEOUT
                and conn.maintenance_state == MaintenanceState.MAINTENANCE
            ):
                matching_conns_count += 1
        assert matching_conns_count == expected_matching_conns_count

    async def _validate_moving_state(
        self,
        client: Redis,
        configured_endpoint_type: EndpointType,
        expected_matching_connected_conns_count: int,
        expected_matching_disconnected_conns_count: int,
        fault_injector_client: AsyncFaultInjectorClient,
    ):
        matching_connected_conns_count = 0
        matching_disconnected_conns_count = 0
        # No outer lock here: asyncio.Lock is non-reentrant and
        # _get_all_connections_in_pool already holds the lock for its snapshot.
        # Single-threaded event loop guarantees no interleaving after the await.
        connections = await self._get_all_connections_in_pool(client)
        for conn in connections:
            endpoint_configured_correctly = is_endpoint_configured_correctly(
                conn, configured_endpoint_type, fault_injector_client
            )

            if (
                conn.is_connected
                and conn.socket_timeout == RELAXED_TIMEOUT
                and conn.maintenance_state == MaintenanceState.MOVING
                and endpoint_configured_correctly
            ):
                matching_connected_conns_count += 1
            elif (
                not conn.is_connected
                and conn.maintenance_state == MaintenanceState.MOVING
                and conn.socket_timeout == RELAXED_TIMEOUT
                and endpoint_configured_correctly
            ):
                matching_disconnected_conns_count += 1

        assert matching_connected_conns_count == expected_matching_connected_conns_count
        assert (
            matching_disconnected_conns_count
            == expected_matching_disconnected_conns_count
        )

    async def _validate_default_state(
        self,
        client: Redis,
        expected_matching_conns_count: Union[int, Literal["all"]],
        configured_timeout: float = CLIENT_TIMEOUT,
    ):
        matching_conns_count = 0
        mismatched: list[str] = []
        connections = await self._get_all_connections_in_pool(client)
        logging.info(f"Validating {len(connections)} connections")

        for conn in connections:
            if not conn.is_connected:
                if (
                    conn.maintenance_state == MaintenanceState.NONE
                    and conn.socket_timeout == configured_timeout
                    and conn.host == getattr(conn, "orig_host_address", conn.host)
                ):
                    matching_conns_count += 1
                else:
                    detail = (
                        f"{conn}: maintenance_state={conn.maintenance_state}, "
                        f"socket_timeout={conn.socket_timeout}, host={conn.host}, "
                        f"orig_host_address={getattr(conn, 'orig_host_address', None)}, "
                        f"{conn.extract_connection_details()}"
                    )
                    mismatched.append(detail)
                    logging.debug(f"Connection not matching default state: {detail}")
            elif (
                conn.socket_timeout == configured_timeout
                and conn.maintenance_state == MaintenanceState.NONE
                and conn.host == getattr(conn, "orig_host_address", conn.host)
            ):
                matching_conns_count += 1
            else:
                detail = (
                    f"{conn}: maintenance_state={conn.maintenance_state}, "
                    f"socket_timeout={conn.socket_timeout}, host={conn.host}, "
                    f"orig_host_address={getattr(conn, 'orig_host_address', None)}, "
                    f"{conn.extract_connection_details()}"
                )
                mismatched.append(detail)
                logging.debug(f"Connection not matching default state: {detail}")

        conn_kwargs = client.connection_pool.connection_kwargs
        client_host = conn_kwargs.get("host", "unknown")
        client_port = conn_kwargs.get("port", "unknown")

        if expected_matching_conns_count == "all":
            expected_matching_conns_count = len(connections)

        assert matching_conns_count == expected_matching_conns_count, (
            f"Default state validation failed. "
            f"Client: host={client_host}, port={client_port}, "
            f"configured_timeout={configured_timeout}. "
            f"Expected {expected_matching_conns_count} matching connections, "
            f"but found {matching_conns_count} out of {len(connections)} total connections. "
            f"Non-matching connections: {'; '.join(mismatched) if mismatched else 'none'}"
        )

    async def _validate_default_notif_disabled_state(
        self, client: Redis, expected_matching_conns_count: int
    ):
        matching_conns_count = 0
        connections = await self._get_all_connections_in_pool(client)

        for conn in connections:
            if not conn.is_connected:
                if (
                    conn.maintenance_state == MaintenanceState.NONE
                    and conn.socket_timeout == CLIENT_TIMEOUT
                    and not hasattr(conn, "orig_host_address")
                ):
                    matching_conns_count += 1
            elif (
                conn.socket_timeout == CLIENT_TIMEOUT
                and conn.maintenance_state == MaintenanceState.NONE
                and not hasattr(conn, "orig_host_address")
            ):
                matching_conns_count += 1
        assert matching_conns_count == expected_matching_conns_count

    async def delete_prev_db(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        db_name: str,
    ):
        try:
            logging.info(f"Deleting database if exists: {db_name}")
            existing_db_id = await fault_injector_client.find_database_id_by_name(
                db_name
            )
            if existing_db_id:
                await fault_injector_client.delete_database(existing_db_id)
                logging.info(f"Deleted database: {db_name}")
            else:
                logging.info(f"Database {db_name} does not exist.")
        # _get_cluster_nodes_info reports failures through pytest.fail, which does not
        # derive from Exception - catch it too, so deleting a leftover DB stays best
        # effort and never aborts the caller.
        except (Exception, pytest.fail.Exception) as e:
            logging.error(f"Failed to delete database {db_name}: {e}")

    async def create_db(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        bdb_config: Dict[str, Any],
    ):
        """Create the test database, retrying transient infrastructure failures.

        A create that fails midway can still leave the BDB on the cluster, so any
        leftover is deleted by name before the next attempt - otherwise the retry
        collides with it on the fixed port from the config.
        """
        logging.info(f"Creating database: \n{json.dumps(bdb_config, indent=2)}")
        for attempt in range(1, DB_CREATE_ATTEMPTS + 1):
            try:
                return await fault_injector_client.create_database(bdb_config)
            # get_operation_result reports a failed action through pytest.fail, which
            # does not derive from Exception.
            except (Exception, pytest.fail.Exception) as e:
                if attempt == DB_CREATE_ATTEMPTS:
                    pytest.fail(
                        f"Failed to create database after {attempt} attempts: {e}"
                    )
                logging.warning(
                    f"Attempt {attempt}/{DB_CREATE_ATTEMPTS} to create database "
                    f"{bdb_config['name']} failed: {e}"
                )
                await self.delete_prev_db(fault_injector_client, bdb_config["name"])

    async def _trigger_effect(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        endpoints_config: Dict[str, Any],
        effect_name: SlotMigrateEffects | TopologyChangeStandaloneEffects,
        trigger_name: Optional[str] = None,
        target_node: Optional[str] = None,
        empty_node: Optional[str] = None,
        skip_end_notification: bool = False,
        timeout: int = DEFAULT_TRIGGER_OP_TIMEOUT,
    ):
        action_id = await fault_injector_client.trigger_effect(
            endpoint_config=endpoints_config,
            effect_name=effect_name,
            trigger_name=trigger_name,
            source_node=target_node,
            target_node=empty_node,
            skip_end_notification=skip_end_notification,
        )
        result = await fault_injector_client.get_operation_result(
            action_id,
            timeout=timeout,
        )
        logging.debug(f"Action execution result: {result}")


class TestAsyncStandaloneClientPushNotificationsWithEffectTriggerBase(
    TestAsyncPushNotificationsBase
):
    async def setup_env(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        db_config: Dict[str, Any],
        endpoint_type: Optional[EndpointType] = None,
    ):
        self._fault_injector = fault_injector_client
        await self.delete_prev_db(fault_injector_client, db_config["name"])

        # Recorded before the create so teardown removes a partially created DB.
        self._bdb_name = db_config["name"]
        db_endpoint_config = await self.create_db(fault_injector_client, db_config)

        auth_ssl_client_certs_config_info = db_config.get(
            "authentication_ssl_client_certs", None
        )
        auth_ssl_client_certs = (
            True
            if auth_ssl_client_certs_config_info
            and auth_ssl_client_certs_config_info[0].get("client_cert") is not None
            else False
        )

        self._client = get_async_standalone_client_maint_notifications(
            endpoints_config=db_endpoint_config,
            disable_retries=True,
            socket_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
            enable_maintenance_notifications=True,
            endpoint_type=endpoint_type,
            auth_ssl_client_certs=auth_ssl_client_certs,
        )
        return self._client, db_endpoint_config

    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_cleanup(self):
        self.maintenance_ops_tasks = []
        self._bdb_name = None
        self._fault_injector = None
        self._client: Redis | None = None

        yield

        logging.info("Starting cleanup...")

        logging.info("Waiting for maintenance operation tasks to finish...")
        if self.maintenance_ops_tasks:
            await asyncio.gather(*self.maintenance_ops_tasks, return_exceptions=True)

        if self._client is not None:
            await self._client.aclose()

        if self._bdb_name and self._fault_injector:
            await self.delete_prev_db(self._fault_injector, self._bdb_name)

        logging.info("Cleanup finished")


class TestAsyncStandaloneClientPushNotificationsHandlingWithEffectTrigger(
    TestAsyncStandaloneClientPushNotificationsWithEffectTriggerBase
):
    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_NO_CONN_DROP],
        ),
    )
    async def test_notification_handling_during_data_movements_no_conn_drop(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config
        )

        logging.info("Creating one connection in the pool.")
        conn = await client.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for opening push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=conn,
        )

        logging.info("Validating connection maintenance state...")
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE
        assert conn.socket_timeout == RELAXED_TIMEOUT

        logging.info("Waiting for closing push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=conn,
        )

        logging.info("Validating connection default state is restored...")
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn.socket_timeout == DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT

        logging.info("Releasing connection back to the pool...")
        await client.connection_pool.release(conn)

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
        ),
    )
    async def test_notification_handling_during_data_movements_with_conn_drop(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config
        )

        conn = await client.connection_pool.get_connection()
        await client.connection_pool.release(conn)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )

        logging.info("Validating connection migrating state...")
        conn = await client.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE
        assert conn.socket_timeout == RELAXED_TIMEOUT
        await client.connection_pool.release(conn)

        logging.info("Waiting for MIGRATED push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )

        logging.info("Validating connection states...")
        conn = await client.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn.socket_timeout == DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT
        await client.connection_pool.release(conn)

        logging.info("Waiting for MOVING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            expected_state=MaintenanceState.MOVING,
        )

        logging.info("Validating connection states...")
        conn = await client.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.MOVING
        assert conn.socket_timeout == RELAXED_TIMEOUT

        logging.info("Waiting for moving ttl to expire")
        await asyncio.sleep(DEFAULT_BIND_TTL)

        logging.info("Validating connection states...")
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn.socket_timeout == DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT

        await client.connection_pool.release(conn)

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name, endpoint_type",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
            endpoint_types=[
                EndpointType.EXTERNAL_FQDN,
                EndpointType.EXTERNAL_IP,
                EndpointType.NONE,
            ],
        ),
    )
    async def test_timeout_handling_during_data_movements_with_conn_drop(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
        endpoint_type: EndpointType,
    ):
        logging.info(f"DB name: {db_name}")
        logging.info(f"Testing timeout handling for endpoint type: {endpoint_type}")
        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        logging.info("Creating three connections in the pool.")
        conns = []
        for _ in range(3):
            conns.append(await client.connection_pool.get_connection())
        for conn in conns:
            await client.connection_pool.release(conn)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )

        await self._validate_maintenance_state(client, expected_matching_conns_count=1)
        await self._validate_default_state(
            client,
            expected_matching_conns_count=2,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        logging.info("Waiting for MIGRATED push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )

        logging.info("Validating connection states after MIGRATED ...")
        await self._validate_default_state(
            client,
            expected_matching_conns_count=3,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        logging.info("Waiting for MOVING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            expected_state=MaintenanceState.MOVING,
        )

        if endpoint_type == EndpointType.NONE:
            logging.info(
                "Waiting for moving ttl/2 to expire to validate proactive reconnection"
            )
            await asyncio.sleep(fault_injector_client.get_moving_ttl() / 2 + 1)

        logging.info("Validating connections states...")
        await self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=0,
            expected_matching_disconnected_conns_count=3,
            fault_injector_client=fault_injector_client,
        )
        conn = await client.connection_pool.get_connection()
        await self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=1,
            expected_matching_disconnected_conns_count=2,
            fault_injector_client=fault_injector_client,
        )
        await client.connection_pool.release(conn)

        logging.info("Waiting for moving ttl to expire")
        await asyncio.sleep(DEFAULT_BIND_TTL)

        logging.info("Validating connection states...")
        await self._validate_default_state(
            client,
            expected_matching_conns_count=3,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name, endpoint_type",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
            endpoint_types=[
                EndpointType.EXTERNAL_FQDN,
                EndpointType.EXTERNAL_IP,
                EndpointType.NONE,
            ],
        ),
    )
    async def test_connection_handling_during_data_movements_with_conn_drop(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
        endpoint_type: EndpointType,
    ):
        logging.info(f"DB name: {db_name}")
        logging.info(f"Testing with endpoint type: {endpoint_type}")
        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        logging.info("Creating one connection in the pool.")
        first_conn = await client.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        await self._validate_maintenance_state(client, expected_matching_conns_count=1)

        logging.info("Waiting for MIGRATED push notification ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        await client.connection_pool.release(first_conn)

        logging.info("Waiting for MOVING push notifications on random connection ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            expected_state=MaintenanceState.MOVING,
        )

        if endpoint_type == EndpointType.NONE:
            logging.info(
                "Waiting for moving ttl/2 to expire to validate proactive reconnection"
            )
            await asyncio.sleep(fault_injector_client.get_moving_ttl() / 2 + 1)

        connections = []
        for _ in range(3):
            connections.append(await client.connection_pool.get_connection())
        for conn in connections:
            logging.debug(f"Releasing connection {conn}. {conn.maintenance_state}")
            await client.connection_pool.release(conn)

        logging.info("Validating connections states during MOVING ...")
        await self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=3,
            expected_matching_disconnected_conns_count=0,
            fault_injector_client=fault_injector_client,
        )

        logging.info("Waiting for moving ttl to expire")
        await asyncio.sleep(fault_injector_client.get_moving_ttl())

        logging.info("Validating connection states after MOVING has expired ...")
        await self._validate_default_state(
            client,
            expected_matching_conns_count=3,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_NO_CONN_DROP],
        ),
    )
    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support sending notifications to new connections.",
    )
    async def test_new_connections_receive_notifications_no_conn_drop(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config
        )

        logging.info("Creating one connection in the pool.")
        first_conn = await client.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for opening push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )
        await self._validate_maintenance_state(client, expected_matching_conns_count=1)

        logging.info(
            "Creating second connection that should receive the opening notification as well."
        )
        second_conn = await client.connection_pool.get_connection()
        await self._validate_maintenance_state(client, expected_matching_conns_count=2)

        logging.info("Waiting for closing push notifications on both connections ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=second_conn,
        )

        await self._validate_default_state(
            client,
            expected_matching_conns_count=2,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        await client.connection_pool.release(first_conn)
        await client.connection_pool.release(second_conn)

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
        ),
    )
    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support sending notifications to new connections.",
    )
    async def test_old_connection_shutdown_during_moving(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        endpoint_type = EndpointType.EXTERNAL_IP
        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        conn = await client.connection_pool.get_connection()
        await client.connection_pool.release(conn)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )
        await self._validate_maintenance_state(client, expected_matching_conns_count=1)

        logging.info("Waiting for MIGRATED push notification ...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )
        await self._validate_default_state(
            client,
            expected_matching_conns_count=1,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        moving_event = asyncio.Event()
        errors = asyncio.Queue()

        async def execute_commands():
            while not moving_event.is_set():
                try:
                    await client.set("key", "value")
                    await client.get("key")
                except Exception as e:
                    await errors.put(
                        f"Command failed in task {asyncio.current_task().get_name()}: {e}"
                    )
                await asyncio.sleep(0.001)

        conn_to_check_moving = await client.connection_pool.get_connection()

        threads_count = 10
        tasks = [
            asyncio.create_task(execute_commands(), name=f"cmd_task_{i}")
            for i in range(threads_count)
        ]

        logging.info("Waiting for MOVING push notification ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=conn_to_check_moving,
            expected_state=MaintenanceState.MOVING,
        )

        logging.info("Setting moving event...")
        moving_event.set()
        await client.connection_pool.release(conn_to_check_moving)

        await asyncio.gather(*tasks)

        logging.info("All command tasks finished. Validating connections states...")
        connections = await self._get_all_connections_in_pool(client)
        for conn in connections:
            if conn.is_connected:
                assert conn.get_resolved_ip() == conn.host
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn.socket_timeout == RELAXED_TIMEOUT
                if not isinstance(fault_injector_client, AsyncProxyServerFaultInjector):
                    assert conn.host != conn.orig_host_address
                assert not conn.should_reconnect()
            else:
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn.socket_timeout == RELAXED_TIMEOUT
                if not isinstance(fault_injector_client, AsyncProxyServerFaultInjector):
                    assert conn.host != conn.orig_host_address
                assert not conn.should_reconnect()

        self._fail_on_command_errors(errors, "command execution tasks")

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
        ),
    )
    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support sending notifications to new connections.",
    )
    async def test_new_connections_receive_moving(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        endpoint_type = EndpointType.EXTERNAL_IP
        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        logging.info("Creating one connection in the pool.")
        first_conn = await client.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        await self._validate_maintenance_state(client, expected_matching_conns_count=1)

        logging.info("Waiting for MIGRATED push notifications ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        logging.info("Waiting for MOVING push notifications on random connection ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
            expected_state=MaintenanceState.MOVING,
        )

        assert first_conn.is_connected, (
            "Connection must be active after receiving MOVING notification"
        )
        old_address = first_conn.getpeername()
        logging.info(f"The node address before bind: {old_address}")

        auth_ssl_client_certs_config_info = db_config.get(
            "authentication_ssl_client_certs", None
        )
        auth_ssl_client_certs = (
            True
            if auth_ssl_client_certs_config_info
            and auth_ssl_client_certs_config_info[0].get("client_cert") is not None
            else False
        )

        logging.info(
            "Creating new client pointing at old address — "
            "new connections should receive the moving notification..."
        )
        new_client = _get_async_client_maint_notifications(
            endpoints_config=db_endpoint_config,
            endpoint_type=endpoint_type,
            host_config=old_address,
            socket_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
            auth_ssl_client_certs=auth_ssl_client_certs,
        )

        logging.info(
            "Creating one connection in the new pool that should receive the moving notification."
        )
        new_client_conn = await new_client.connection_pool.get_connection()

        logging.info("Validating connections states during MOVING ...")
        await self._validate_moving_state(
            new_client,
            endpoint_type,
            expected_matching_connected_conns_count=1,
            expected_matching_disconnected_conns_count=0,
            fault_injector_client=fault_injector_client,
        )

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

        await new_client.connection_pool.release(new_client_conn)
        await new_client.aclose()

        await client.connection_pool.release(first_conn)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
        ),
    )
    async def test_disabled_handling_during_migrating_and_moving(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        self._fault_injector = fault_injector_client
        await self.delete_prev_db(fault_injector_client, db_config["name"])
        # Recorded before the create so teardown removes a partially created DB.
        self._bdb_name = db_config["name"]
        db_endpoint_config = await self.create_db(fault_injector_client, db_config)

        logging.info("Creating client with disabled notifications.")
        self._client = client = get_async_standalone_client_maint_notifications(
            endpoints_config=db_endpoint_config,
            disable_retries=True,
            enable_maintenance_notifications=False,
        )

        logging.info("Creating one connection in the pool.")
        first_conn = await client.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=first_conn
        )

        await self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=1
        )

        logging.info(
            "Creating second connection — expect it not to receive MIGRATING either."
        )
        second_conn = await client.connection_pool.get_connection()
        await AsyncClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=second_conn
        )

        logging.info(
            "Validating connection states after MIGRATING for both connections ..."
        )
        await self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=2
        )

        logging.info("Waiting for MIGRATED push notifications on both connections ...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=first_conn
        )
        await AsyncClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=second_conn
        )

        await client.connection_pool.release(first_conn)
        await client.connection_pool.release(second_conn)

        logging.info("Waiting for MOVING push notifications on random connection ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=10,
            fail_on_timeout=False,
        )

        connections = []
        for _ in range(3):
            connections.append(await client.connection_pool.get_connection())
        for conn in connections:
            await client.connection_pool.release(conn)

        logging.info("Validating connections states during MOVING ...")
        await self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=3
        )

        logging.info("Waiting for moving ttl to expire")
        await asyncio.sleep(DEFAULT_BIND_TTL)

        logging.info("Validating connection states after MOVING has expired ...")
        await self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=3
        )

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)


class TestAsyncStandaloneClientCommandsExecutionWithPushNotificationsWithEffectTrigger(
    TestAsyncStandaloneClientPushNotificationsWithEffectTriggerBase
):
    # TODO: re-enable TopologyChangeStandaloneEffects.DNS_RESOLUTION_CHANGE once the
    # effect is troubleshooted and fixed.
    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name, endpoint_type",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [
                TopologyChangeStandaloneEffects.DATA_MOVEMENT_NO_CONN_DROP,
                TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP,
                TopologyChangeStandaloneEffects.CONN_DROP,
                # TopologyChangeStandaloneEffects.DNS_RESOLUTION_CHANGE,
            ],
            endpoint_types=[
                EndpointType.EXTERNAL_FQDN,
                EndpointType.EXTERNAL_IP,
                EndpointType.NONE,
            ],
        ),
    )
    async def test_command_execution_during_maintenance(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
        endpoint_type: EndpointType,
    ):
        errors = asyncio.Queue()
        if isinstance(fault_injector_client, AsyncProxyServerFaultInjector):
            execution_duration = 20
        else:
            execution_duration = 60

        logging.info(f"DB name: {db_name}")
        logging.info(f"Testing timeout handling for endpoint type: {endpoint_type}")
        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        async def execute_commands(duration: int):
            start = time.time()
            while time.time() - start < duration:
                try:
                    await client.set("key", "value")
                    await client.get("key")
                except Exception as e:
                    logging.error(
                        f"Error in task {asyncio.current_task().get_name()}: {e}"
                    )
                    await errors.put(
                        f"Command failed in task "
                        f"{asyncio.current_task().get_name()}: {e}"
                    )
                await asyncio.sleep(0.001)
            logging.debug(f"{asyncio.current_task().get_name()}: task ended")

        tasks = [
            asyncio.create_task(
                execute_commands(execution_duration), name=f"cmd_task_{i}"
            )
            for i in range(10)
        ]

        logging.info("Waiting for tasks to start and have a few cycles executed ...")
        await asyncio.sleep(3)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        await asyncio.gather(*tasks)

        # Drain first so the command errors are logged even if awaiting the
        # trigger task or the state assertion below raises.
        collected_errors = self._drain_command_errors(errors, "command execution tasks")

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

        # "all" rather than a hard-coded 10: the pool size is emergent from the
        # task count, so pinning the number fails on an extra connection even
        # when every connection is in the expected state - and passes when one
        # of eleven is not.
        await self._validate_default_state(
            client,
            expected_matching_conns_count="all",
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        self._fail_on_collected_command_errors(
            collected_errors, "command execution tasks"
        )


class TestAsyncClusterClientPushNotificationsWithEffectTriggerBase(
    TestAsyncPushNotificationsBase
):
    async def setup_env(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        db_config: Dict[str, Any],
    ):
        self._fault_injector = fault_injector_client
        await self.delete_prev_db(fault_injector_client, db_config["name"])

        # Recorded before the create so teardown removes a partially created DB.
        self._bdb_name = db_config["name"]
        cluster_endpoint_config = await self.create_db(fault_injector_client, db_config)

        socket_timeout = DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT
        socket_connect_timeout = DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT

        auth_ssl_client_certs_config_info = db_config.get(
            "authentication_ssl_client_certs", None
        )
        auth_ssl_client_certs = (
            True
            if auth_ssl_client_certs_config_info
            and auth_ssl_client_certs_config_info[0].get("client_cert") is not None
            else False
        )

        self._client = get_async_cluster_client_maint_notifications(
            endpoints_config=cluster_endpoint_config,
            disable_retries=True,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            enable_maintenance_notifications=True,
            auth_ssl_client_certs=auth_ssl_client_certs,
        )
        # The async cluster discovers its topology lazily.
        await self._client.initialize()
        return self._client, cluster_endpoint_config

    async def _acquire_connected_connection(self, node):
        """Acquire a connection from an async ClusterNode and connect it.

        The async ClusterNode is its own pool. ``acquire_connection`` returns a
        (possibly fresh) connection that is not yet connected; we connect it so
        the maintenance push handlers are installed and the handshake is sent.
        """
        conn = node.acquire_connection()
        await conn.connect()
        return conn

    async def _drain_maint_notification_tasks(self, client: RedisCluster):
        """Wait for in-flight async OSS maintenance handling tasks to finish.

        Unlike the sync client (where SMIGRATED handling completes inline while
        reading the command response), the async client schedules the topology
        update in a background task. Tests must drain these before asserting on
        the resulting topology / connection state.
        """
        handler = client._oss_cluster_maint_notifications_handler
        if handler is None:
            return
        for _ in range(100):
            tasks = [t for t in handler._background_tasks if not t.done()]
            if not tasks:
                break
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0)

    async def _wait_for_node_cache_reconciliation(
        self,
        client: RedisCluster,
        connections: List,
        is_reconciled: Callable[[Dict[str, Any]], bool],
        description: str,
        timeout: int = SMIGRATED_TIMEOUT,
    ):
        """Drain pending notifications until the client node cache reconciles.

        Async counterpart of the sync suite's ``_wait_for_node_cache_delta``,
        generalized to a predicate over the node cache so it also covers the
        net-zero node-replace case (one node removed and one added leaves the
        cache size unchanged).

        A topology change may arrive as several push notifications - in particular an
        ASM scale add/remove is a cascade of SMIGRATING/SMIGRATED as the decision
        engine moves slots - so the client reconciles its node cache over multiple
        notifications after the server-side operation finishes. Reading a single
        SMIGRATED frame off a single connection is therefore not enough; poll,
        draining pending pushes on the given connections, until ``is_reconciled``
        holds. Single-step triggers (migrate/failover) reach the target immediately
        and return on the first check.

        Draining pushes alone is not sufficient in the async client: SMIGRATED
        handling is scheduled as a background task instead of completing inline while
        reading the command response, so every round also drains those tasks.
        """
        deadline = time.time() + timeout
        while True:
            await self._drain_maint_notification_tasks(client)
            if is_reconciled(client.nodes_manager.nodes_cache):
                return
            if time.time() >= deadline:
                break
            for conn in connections:
                try:
                    if conn.is_connected and await conn.can_read():
                        await conn.read_response(push_request=True)
                except Exception as e:
                    # A connection to a node being removed may error; that is expected.
                    logging.debug(f"Ignored error while draining notifications: {e}")
            await asyncio.sleep(0.5)
        # Fail explicitly on a drain timeout so it surfaces as a clear "node cache did
        # not reconcile" error, rather than silently returning and letting the caller's
        # exact assertion fail later with a misleading node-count message.
        pytest.fail(
            f"Node cache did not reconcile ({description}) within {timeout}s; "
            f"current nodes={sorted(client.nodes_manager.nodes_cache)}"
        )

    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_cleanup(self):
        self.maintenance_ops_tasks = []
        self._bdb_name = None
        self._fault_injector = None
        self._client: RedisCluster | None = None

        yield

        logging.info("Starting cleanup...")

        logging.info("Waiting for maintenance operation tasks to finish...")
        if self.maintenance_ops_tasks:
            await asyncio.gather(*self.maintenance_ops_tasks, return_exceptions=True)

        if self._client is not None:
            await self._client.aclose()

        if self._bdb_name and self._fault_injector:
            await self.delete_prev_db(self._fault_injector, self._bdb_name)

        logging.info("Cleanup finished")


class TestAsyncClusterClientPushNotificationsHandlingWithEffectTrigger(
    TestAsyncClusterClientPushNotificationsWithEffectTriggerBase
):
    @pytest.mark.timeout(
        RESHARD_TEST_TIMEOUT
    )  # sized for slow ASM reshard, see RESHARD_OP_TIMEOUT
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API, [SlotMigrateEffects.SLOT_SHUFFLE]
        ),
    )
    async def test_notification_handling_during_node_shuffle_no_node_replacement(
        self,
        fault_injector_client_oss_api: AsyncFaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """Slots are moved between existing nodes; no node appears/disappears."""
        logging.info(f"DB name: {db_name}")

        client, cluster_endpoint_config = await self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")
        initial_cluster_nodes = client.nodes_manager.nodes_cache.copy()
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = await self._acquire_connected_connection(node)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
                timeout=RESHARD_OP_TIMEOUT,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conn in in_use_connections.values():
            await AsyncClientValidations.wait_push_notification(
                client,
                timeout=RESHARD_SMIGRATING_TIMEOUT,
                connection=conn,
            )

        logging.info("Validating connection maintenance state...")
        for conn in in_use_connections.values():
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn.socket_timeout == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        assert len(initial_cluster_nodes) == len(client.nodes_manager.nodes_cache)
        for node_key in initial_cluster_nodes.keys():
            assert node_key in client.nodes_manager.nodes_cache

        logging.info("Waiting for SMIGRATED push notifications...")
        con_to_read_smigrated = random.choice(list(in_use_connections.values()))
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=SMIGRATED_TIMEOUT,
            connection=con_to_read_smigrated,
        )
        # SMIGRATED handling runs in a background task in the async client.
        await self._drain_maint_notification_tasks(client)

        logging.info("Validating connection state after SMIGRATED ...")
        updated_cluster_nodes = client.nodes_manager.nodes_cache.copy()
        removed_nodes = set(initial_cluster_nodes.values()) - set(
            updated_cluster_nodes.values()
        )
        assert len(removed_nodes) == 0
        assert len(initial_cluster_nodes) == len(updated_cluster_nodes)

        marked_conns_for_reconnect = 0
        for conn in in_use_connections.values():
            if conn.should_reconnect():
                marked_conns_for_reconnect += 1
        # Async-specific: unlike the sync client (which marks only the src node's
        # connection), the async NodesManager.set_nodes marks ALL preserved nodes'
        # in-use connections for reconnect on every initialize() — the cluster
        # topology refresh triggered while handling SMIGRATED defers disconnection
        # via lazy reconnect since set_nodes is sync. So at least the src node's
        # connection is marked; in practice every held connection is.
        assert marked_conns_for_reconnect >= 1

        logging.info("Releasing connections back to the pool...")
        for node, conn in in_use_connections.items():
            node.release(conn)

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(
        RESHARD_TEST_TIMEOUT
    )  # sized for slow ASM reshard, see RESHARD_OP_TIMEOUT
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API,
            [SlotMigrateEffects.REMOVE_ADD],
        ),
    )
    async def test_notification_handling_with_node_replace(
        self,
        fault_injector_client_oss_api: AsyncFaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """A node is removed and a new node is added during slot movement."""
        logging.info(f"DB name: {db_name}")

        client, cluster_endpoint_config = await self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")
        initial_cluster_nodes = client.nodes_manager.nodes_cache.copy()
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = await self._acquire_connected_connection(node)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
                timeout=RESHARD_OP_TIMEOUT,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conn in in_use_connections.values():
            await AsyncClientValidations.wait_push_notification(
                client,
                timeout=RESHARD_SMIGRATING_TIMEOUT,
                connection=conn,
            )

        logging.info("Validating connection maintenance state...")
        for conn in in_use_connections.values():
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn.socket_timeout == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        assert len(initial_cluster_nodes) == len(client.nodes_manager.nodes_cache)

        logging.info("Waiting for SMIGRATED push notifications...")
        con_to_read_smigrated = random.choice(list(in_use_connections.values()))
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=SMIGRATED_TIMEOUT,
            connection=con_to_read_smigrated,
        )
        logging.info("Waiting for the operation to finish server-side...")
        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

        logging.info("Draining notifications until one node has been replaced...")
        initial_nodes = set(initial_cluster_nodes.values())
        await self._wait_for_node_cache_reconciliation(
            client,
            list(in_use_connections.values()),
            lambda cache: len(initial_nodes - set(cache.values())) == 1
            and len(set(cache.values()) - initial_nodes) == 1,
            "expected exactly one node removed and one added",
        )

        logging.info("Validating connection state after SMIGRATED ...")
        updated_cluster_nodes = client.nodes_manager.nodes_cache.copy()

        removed_nodes = set(initial_cluster_nodes.values()) - set(
            updated_cluster_nodes.values()
        )
        assert len(removed_nodes) == 1
        removed_node = removed_nodes.pop()
        assert removed_node is not None

        added_nodes = set(updated_cluster_nodes.values()) - set(
            initial_cluster_nodes.values()
        )
        assert len(added_nodes) == 1

        conn = in_use_connections.get(removed_node)
        # connection will be dropped, but it is marked for reconnect before
        # being released; timeouts/state are not updated so are not checked
        assert conn is not None
        assert conn.should_reconnect() is True

        logging.info("Releasing connections back to the pool...")
        for node, conn in in_use_connections.items():
            node.release(conn)

    @pytest.mark.timeout(
        RESHARD_TEST_TIMEOUT
    )  # sized for slow ASM reshard, see RESHARD_OP_TIMEOUT
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API,
            [SlotMigrateEffects.REMOVE],
        ),
    )
    async def test_notification_handling_with_node_remove(
        self,
        fault_injector_client_oss_api: AsyncFaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """A node is removed during slot movement (no replacement added)."""
        logging.info(f"DB name: {db_name}")

        client, cluster_endpoint_config = await self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")
        initial_cluster_nodes = client.nodes_manager.nodes_cache.copy()
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = await self._acquire_connected_connection(node)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
                timeout=RESHARD_OP_TIMEOUT,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conn in in_use_connections.values():
            await AsyncClientValidations.wait_push_notification(
                client,
                timeout=RESHARD_SMIGRATING_TIMEOUT,
                connection=conn,
            )

        logging.info("Validating connection maintenance state...")
        for conn in in_use_connections.values():
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn.socket_timeout == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        assert len(initial_cluster_nodes) == len(client.nodes_manager.nodes_cache)

        logging.info("Waiting for SMIGRATED push notifications...")
        con_to_read_smigrated = random.choice(list(in_use_connections.values()))
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=SMIGRATED_TIMEOUT,
            connection=con_to_read_smigrated,
        )
        logging.info("Waiting for the operation to finish server-side...")
        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

        logging.info("Draining notifications until the node cache reflects -1 node...")
        expected_node_count = len(initial_cluster_nodes) - 1
        await self._wait_for_node_cache_reconciliation(
            client,
            list(in_use_connections.values()),
            lambda cache: len(cache) == expected_node_count,
            f"expected {expected_node_count} nodes, one fewer than at start",
        )

        logging.info("Validating connection state after SMIGRATED ...")
        updated_cluster_nodes = client.nodes_manager.nodes_cache.copy()

        removed_nodes = set(initial_cluster_nodes.values()) - set(
            updated_cluster_nodes.values()
        )
        assert len(removed_nodes) == 1
        removed_node = removed_nodes.pop()
        assert removed_node is not None

        assert len(initial_cluster_nodes) == len(updated_cluster_nodes) + 1

        conn = in_use_connections.get(removed_node)
        assert conn is not None
        assert conn.should_reconnect() is True

        # Async-specific: the async NodesManager.set_nodes marks all preserved
        # nodes' in-use connections for reconnect on initialize() (lazy reconnect,
        # since set_nodes is sync), in addition to the removed node's connection.
        # So at least the removed node's connection is marked; in practice all are.
        marked_conns_for_reconnect = 0
        for conn in in_use_connections.values():
            if conn.should_reconnect():
                marked_conns_for_reconnect += 1
        assert marked_conns_for_reconnect >= 1

        logging.info("Releasing connections back to the pool...")
        for node, conn in in_use_connections.items():
            node.release(conn)


class TestAsyncClusterClientCommandsExecutionWithPushNotificationsWithEffectTrigger(
    TestAsyncClusterClientPushNotificationsWithEffectTriggerBase
):
    @pytest.mark.timeout(
        RESHARD_TEST_TIMEOUT
    )  # sized for slow ASM reshard, see RESHARD_OP_TIMEOUT
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API,
            [
                SlotMigrateEffects.SLOT_SHUFFLE,
                SlotMigrateEffects.REMOVE,
                SlotMigrateEffects.ADD,
            ],
        ),
    )
    async def test_command_execution_during_slot_shuffle_no_node_replacement(
        self,
        fault_injector_client_oss_api: AsyncFaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """Commands keep succeeding while slots migrate between nodes."""
        logging.info(f"DB name: {db_name}")

        client, cluster_endpoint_config = await self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        shards_count = db_config["shards_count"]
        logging.info(f"Shards count: {shards_count}")

        errors = asyncio.Queue()
        if isinstance(fault_injector_client_oss_api, AsyncProxyServerFaultInjector):
            execution_duration = 20
        else:
            execution_duration = 40

        # Warm up the per-node connection pools with a single sequential pass over
        # all shards before launching the concurrent load. The async client runs on
        # one event loop, so a burst of concurrent cold connects to the (remote)
        # cluster nodes would starve each other's connect-timeout budget and fail
        # before any migration begins. Connecting sequentially gives each node an
        # uncontended connect window; the concurrent tasks then reuse the pooled
        # connections. (The sync test gets this for free via thread parallelism.)
        logging.info("Warming up connection pools to all shards...")
        warmup_keys = KeyGenerationHelpers.generate_keys_for_all_shards(
            shards_count,
            prefix=f"warmup_{effect_name}_{trigger}_key",
        )
        for key in warmup_keys:
            await client.set(key, "value")
            await client.get(key)

        # Pre-generate each task's key set BEFORE launching the concurrent tasks.
        # generate_keys_for_all_shards brute-forces CRC16 (~16k iterations/key) and
        # is fully synchronous. Calling it inside each task would run that CPU work
        # on the event loop right as the tasks start, stalling the single loop
        # during the connection-establishment burst and causing spurious connect
        # timeouts. (The sync test is immune: each worker is its own OS thread.)
        task_names = [f"cmd_execution_{i}" for i in range(10)]
        keys_by_task = {
            name: KeyGenerationHelpers.generate_keys_for_all_shards(
                shards_count,
                prefix=f"{name}_{effect_name}_{trigger}_key",
            )
            for name in task_names
        }

        async def execute_commands(duration: int, task_name: str):
            start = time.time()
            executed_commands_count = 0
            keys_for_all_shards = keys_by_task[task_name]
            while time.time() - start < duration:
                for key in keys_for_all_shards:
                    try:
                        await client.set(key, "value")
                        await client.get(key)
                        executed_commands_count += 2
                    except Exception as e:
                        logging.error(f"Error in task {task_name}: {e}")
                        await errors.put(f"Command failed in task {task_name}: {e}")
                await asyncio.sleep(0.001)
            logging.debug(f"{task_name}: task ended")

        tasks = [
            asyncio.create_task(
                execute_commands(execution_duration, name),
                name=name,
            )
            for name in task_names
        ]

        logging.info("Waiting for tasks to start and have a few cycles executed ...")
        await asyncio.sleep(3)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
                timeout=RESHARD_OP_TIMEOUT,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        await asyncio.gather(*tasks)

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

        # Drain any in-flight topology-update tasks before consuming buffers.
        await self._drain_maint_notification_tasks(client)

        # Consume all node connection buffers to validate no notifications were
        # left unconsumed.
        logging.info(
            "Consuming all buffers to validate no notifications were left unconsumed..."
        )
        # Snapshot the node list before iterating: the awaits in this loop
        # (sleep + read_response) yield to the event loop, where reading a push
        # notification can schedule a background topology-update task that
        # mutates nodes_cache (set_nodes with remove_old=True), causing
        # "dictionary changed size during iteration". Same reason the inner loop
        # snapshots node._connections.
        for node in list(client.nodes_manager.nodes_cache.values()):
            for conn in list(node._connections):
                if conn.is_connected:
                    # Async can_read() is non-blocking (it only checks the buffer),
                    # unlike the sync can_read(timeout=...). Give any in-flight push
                    # data a moment to land before draining the buffer.
                    await asyncio.sleep(0.2)
                    try:
                        while await conn.can_read():
                            await conn.read_response(push_request=True)
                    except RedisConnectionError:
                        # remove/failover migrations close connections to the
                        # removed/failed-over node server-side. async can_read()
                        # reports buffered data first, then raises on EOF (a
                        # closed connection must not be reused), and
                        # read_response() hits "Connection closed by server" if
                        # the close lands mid-read. A closed connection has
                        # nothing left to drain — this is expected during the
                        # migration, not an unconsumed push.
                        pass
            logging.info(f"Consumed all buffers for node: {node.name}")
        logging.info("All buffers consumed.")

        self._fail_on_command_errors(errors, "command execution tasks")
