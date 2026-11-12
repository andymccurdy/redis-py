"""Tests for Redis Enterprise moving push notifications with real cluster operations."""

from concurrent.futures import ThreadPoolExecutor
import json
import logging
import random
from queue import Queue
from threading import Thread
import threading
import time
from typing import Any, Dict, List, Literal, Optional, Union

import pytest

from redis import Redis
from redis.connection import ConnectionInterface
from redis.maint_notifications import (
    EndpointType,
    MaintenanceState,
)
from tests.test_scenario.conftest import (
    _FAULT_INJECTOR_STANDALONE_CLIENT,
    CLIENT_TIMEOUT,
    RELAXED_TIMEOUT,
    _FAULT_INJECTOR_CLIENT_OSS_API,
    _get_client_maint_notifications,
    get_cluster_client_maint_notifications,
    get_standalone_client_maint_notifications,
    use_mock_proxy,
)
from tests.test_scenario.fault_injector_client import (
    FaultInjectorClient,
    ProxyServerFaultInjector,
    SlotMigrateEffects,
    TopologyChangeStandaloneEffects,
)
from tests.test_scenario.maint_notifications_helpers import (
    ClientValidations,
    ClusterOperations,
    KeyGenerationHelpers,
    generate_params,
    is_endpoint_configured_correctly,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S:%f",
)

# Set DEBUG level for specific redis-py loggers
logging.getLogger("redis.maint_notifications").setLevel(logging.DEBUG)
logging.getLogger("redis.cluster").setLevel(logging.DEBUG)
logging.getLogger("redis.client").setLevel(logging.DEBUG)
logging.getLogger("redis.connection").setLevel(logging.DEBUG)


STANDALONE_MAINT_TIMEOUT = 60
# SMIGRATED / slot-shuffle waits sized for the FI's real reshard budget: the fault injector
# blocks on the reshard up to 300s (slot-shuffle migrate_slots) / 600s (ASM scale). Sizing the
# client below that turned slow-but-successful reshards into false failures. Raising a timeout
# only affects the failure path (it waits longer before failing), so it is free on green runs.
SMIGRATED_TIMEOUT = 120

SLOT_SHUFFLE_TIMEOUT = 300

# Wait budget for the FIRST SMIGRATING of a reshard. It has to cover the ASM decision engine's
# planning time before the first slot actually moves - not just the move itself - so it is sized as
# a fraction of the FI's own slot-shuffle budget rather than as a "notification should be instant"
# value. Every reshard test uses this, including remove_add: that effect keeps its maintenance
# window open the longest, but it is also the heaviest plan, so it is the slowest to open it.
RESHARD_SMIGRATING_TIMEOUT = int(SLOT_SHUFFLE_TIMEOUT / 2)

# The trigger thread must not give up before the FI finishes the reshard, otherwise it dies and
# the command-execution workers are left running (their join then hangs to the pytest cap). Cover
# the FI's slowest reshard (ASM scale = 600s).
RESHARD_OP_TIMEOUT = 600
# Default op-wait for non-reshard (standalone / single-window) triggers: comfortably under the
# standalone tests' 300s pytest cap so a stuck FI poll can't outlive the cap and block teardown on
# thread.join(). The reshard tests pass RESHARD_OP_TIMEOUT explicitly.
DEFAULT_TRIGGER_OP_TIMEOUT = 180
# Per-test cap for the OSS-cluster reshard tests: must exceed the worst-case reshard wall time =
# the trigger op-wait (RESHARD_OP_TIMEOUT) plus the post-join node-cache drain (SMIGRATED_TIMEOUT),
# with margin, so a legitimately slow ASM scale runs to completion instead of being killed mid-way.
# Only bites on the slow path; fast reshards are unaffected.
RESHARD_TEST_TIMEOUT = RESHARD_OP_TIMEOUT + SMIGRATED_TIMEOUT + 180

DEFAULT_BIND_TTL = 15
DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT = 1
DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT = 1

# The fault injector's create_database can fail on transient cluster-side infrastructure
# problems - a DNS blip while it polls the cluster REST API, for example - that say nothing
# about the client under test. Retrying the setup keeps such a blip from being reported as a
# test failure.
DB_CREATE_ATTEMPTS = 3


class TestPushNotificationsBase:
    """
    Test Redis Enterprise maintenance push notifications with real cluster
    operations.
    """

    @pytest.fixture(autouse=True)
    def _reset_trigger_effect_failures(self):
        """Per-test store for failures raised inside the FI trigger thread."""
        self._trigger_effect_failures: list[str] = []
        yield

    def _drain_command_errors(self, errors, source: str) -> list[str]:
        """Drain and log the collected command errors without failing yet.

        Callers that also assert on connection state must drain *before* that
        assertion: otherwise a state mismatch raises first and the command
        errors - usually the more informative signal - are never reported.

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

    def _fail_on_trigger_effect_failures(self):
        """Surface a failure raised inside the FI trigger thread.

        ``pytest.fail()`` on a spawned thread only kills that thread - it does
        not fail the test - so the main thread has to re-report it after join().
        """
        failures = self._trigger_effect_failures
        if not failures:
            return
        pytest.fail(
            f"{len(failures)} failure(s) in the trigger thread: {'; '.join(failures)}"
        )

    def delete_prev_db(
        self,
        fault_injector_client: FaultInjectorClient,
        db_name: str,
    ):
        try:
            logging.info(f"Deleting database if exists: {db_name}")
            existing_db_id = None
            existing_db_id = ClusterOperations.find_database_id_by_name(
                fault_injector_client, db_name
            )

            if existing_db_id:
                fault_injector_client.delete_database(existing_db_id)
                logging.info(f"Deleted database: {db_name}")
            else:
                logging.info(f"Database {db_name} does not exist.")
        # get_cluster_nodes_info reports failures through pytest.fail, which does not
        # derive from Exception - catch it too, so deleting a leftover DB stays best
        # effort and never aborts the caller.
        except (Exception, pytest.fail.Exception) as e:
            logging.error(f"Failed to delete database {db_name}: {e}")

    def create_db(
        self,
        fault_injector_client: FaultInjectorClient,
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
                return fault_injector_client.create_database(bdb_config)
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
                self.delete_prev_db(fault_injector_client, bdb_config["name"])

    def _trigger_effect(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
        effect_name: SlotMigrateEffects | TopologyChangeStandaloneEffects,
        trigger_name: Optional[str] = None,
        target_node: Optional[str] = None,
        empty_node: Optional[str] = None,
        skip_end_notification: bool = False,
        timeout: int = DEFAULT_TRIGGER_OP_TIMEOUT,
    ):
        try:
            trigger_effect_action_id = ClusterOperations.trigger_effect(
                fault_injector=fault_injector_client,
                endpoint_config=endpoints_config,
                effect_name=effect_name,
                trigger_name=trigger_name,
                source_node=target_node,
                target_node=empty_node,
                skip_end_notification=skip_end_notification,
            )

            trigger_effect_result = fault_injector_client.get_operation_result(
                trigger_effect_action_id,
                timeout=timeout,
            )
            logging.debug(f"Action execution result: {trigger_effect_result}")
        except BaseException as e:
            # This usually runs on a spawned thread, where pytest.fail() only
            # kills the thread and leaves the test looking like it merely saw
            # command errors. Record it for _fail_on_trigger_effect_failures().
            logging.error(f"Trigger effect failed: {type(e).__name__}: {e}")
            self._trigger_effect_failures.append(f"{type(e).__name__}: {e}")
            raise

    def _get_all_connections_in_pool(self, client: Redis) -> List[ConnectionInterface]:
        connections = []
        with client.connection_pool._lock:
            for conn in client.connection_pool._get_free_connections():
                connections.append(conn)
            for conn in client.connection_pool._get_in_use_connections():
                connections.append(conn)
        return connections

    def _validate_maintenance_state(
        self, client: Redis, expected_matching_conns_count: int
    ):
        """Validate the client connections are in the expected state after migration."""
        matching_conns_count = 0
        connections = self._get_all_connections_in_pool(client)

        for conn in connections:
            if (
                conn._sock is not None
                and conn._sock.gettimeout() == RELAXED_TIMEOUT
                and conn.maintenance_state == MaintenanceState.MAINTENANCE
            ):
                matching_conns_count += 1
        assert matching_conns_count == expected_matching_conns_count

    def _validate_moving_state(
        self,
        client: Redis,
        configured_endpoint_type: EndpointType,
        expected_matching_connected_conns_count: int,
        expected_matching_disconnected_conns_count: int,
        fault_injector_client: FaultInjectorClient,
    ):
        """Validate the client connections are in the expected state after migration."""
        matching_connected_conns_count = 0
        matching_disconnected_conns_count = 0
        with client.connection_pool._lock:
            connections = self._get_all_connections_in_pool(client)
            for conn in connections:
                endpoint_configured_correctly = is_endpoint_configured_correctly(
                    conn, configured_endpoint_type, fault_injector_client
                )

                if (
                    conn._sock is not None
                    and conn._sock.gettimeout() == RELAXED_TIMEOUT
                    and conn.maintenance_state == MaintenanceState.MOVING
                    and endpoint_configured_correctly
                ):
                    matching_connected_conns_count += 1
                elif (
                    conn._sock is None
                    and conn.maintenance_state == MaintenanceState.MOVING
                    and conn.socket_timeout == RELAXED_TIMEOUT
                    and endpoint_configured_correctly
                ):
                    matching_disconnected_conns_count += 1
                else:
                    pass
        assert matching_connected_conns_count == expected_matching_connected_conns_count
        assert (
            matching_disconnected_conns_count
            == expected_matching_disconnected_conns_count
        )

    def _validate_default_state(
        self,
        client: Redis,
        expected_matching_conns_count: Union[int, Literal["all"]],
        configured_timeout: float = CLIENT_TIMEOUT,
    ):
        """Validate the client connections are in the expected state after migration."""
        matching_conns_count = 0
        mismatched: list[str] = []
        connections = self._get_all_connections_in_pool(client)
        logging.info(f"Validating {len(connections)} connections")
        logging.info(f"Expected matching conns count: {expected_matching_conns_count}")

        for conn in connections:
            if conn._sock is None:
                if (
                    conn.maintenance_state == MaintenanceState.NONE
                    and conn.socket_timeout == configured_timeout
                    and conn.host == conn.orig_host_address
                ):
                    matching_conns_count += 1
                else:
                    detail = (
                        f"{conn}: maintenance_state={conn.maintenance_state}, "
                        f"socket_timeout={conn.socket_timeout}, host={conn.host}, "
                        f"orig_host_address={conn.orig_host_address}, "
                        f"{conn.extract_connection_details()}"
                    )
                    mismatched.append(detail)
                    logging.debug(f"Connection not matching default state: {detail}")
            elif (
                conn._sock.gettimeout() == configured_timeout
                and conn.maintenance_state == MaintenanceState.NONE
                and conn.host == conn.orig_host_address
            ):
                matching_conns_count += 1
            else:
                detail = (
                    f"{conn}: maintenance_state={conn.maintenance_state}, "
                    f"socket_timeout={conn.socket_timeout}, host={conn.host}, "
                    f"orig_host_address={conn.orig_host_address}, "
                    f"{conn.extract_connection_details()}"
                )
                mismatched.append(detail)
                logging.debug(f"Connection not matching default state: {detail}")

        # Get client configuration details for error message
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

    def _validate_default_notif_disabled_state(
        self, client: Redis, expected_matching_conns_count: int
    ):
        """Validate the client connections are in the expected state after migration."""
        matching_conns_count = 0
        connections = self._get_all_connections_in_pool(client)

        for conn in connections:
            if conn._sock is None:
                if (
                    conn.maintenance_state == MaintenanceState.NONE
                    and conn.socket_timeout == CLIENT_TIMEOUT
                    and not hasattr(conn, "orig_host_address")
                ):
                    matching_conns_count += 1
            elif (
                conn._sock.gettimeout() == CLIENT_TIMEOUT
                and conn.maintenance_state == MaintenanceState.NONE
                and not hasattr(conn, "orig_host_address")
            ):
                matching_conns_count += 1
        assert matching_conns_count == expected_matching_conns_count


class TestStanaloneClientPushNotificationsWithEffectTriggerBase(
    TestPushNotificationsBase
):
    def setup_env(
        self,
        fault_injector_client: FaultInjectorClient,
        db_config: Dict[str, Any],
        endpoint_type: EndpointType | None = None,
    ):
        self.delete_prev_db(fault_injector_client, db_config["name"])

        # Recorded before the create so teardown removes a partially created DB.
        self._bdb_name = db_config["name"]
        db_endpoint_config = self.create_db(fault_injector_client, db_config)

        socket_timeout = DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT

        auth_ssl_client_certs_config_info = db_config.get(
            "authentication_ssl_client_certs", None
        )

        auth_ssl_client_certs = (
            True
            if auth_ssl_client_certs_config_info
            and auth_ssl_client_certs_config_info[0].get("client_cert") is not None
            else False
        )

        self._client = get_standalone_client_maint_notifications(
            endpoints_config=db_endpoint_config,
            disable_retries=True,
            socket_timeout=socket_timeout,
            enable_maintenance_notifications=True,
            endpoint_type=endpoint_type,
            auth_ssl_client_certs=auth_ssl_client_certs,
        )
        return self._client, db_endpoint_config

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(
        self,
    ):
        self.maintenance_ops_threads = []
        self._bdb_name = None
        self._client = None

        # Yield control to the test
        yield

        # Cleanup code - this will run even if the test fails
        logging.info("Starting cleanup...")

        if self._client is not None:
            self._client.close()

        logging.info("Waiting for maintenance operations threads to finish...")
        for thread in self.maintenance_ops_threads:
            thread.join()

        if self._bdb_name:
            self.delete_prev_db(_FAULT_INJECTOR_STANDALONE_CLIENT, self._bdb_name)

        logging.info("Cleanup finished")


class TestStandaloneClientPushNotificationsHandlingWithEffectTrigger(
    TestStanaloneClientPushNotificationsWithEffectTriggerBase
):
    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_NO_CONN_DROP],
        ),
    )
    def test_notification_handling_during_data_movements_no_conn_drop(
        self,
        fault_injector_client: FaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing cluster operations
        that don't lead to dropping connections.

        """
        logging.info(f"DB name: {db_name}")

        client_maint_notifications, db_endpoint_config = self.setup_env(
            fault_injector_client, db_config
        )

        logging.info("Creating one connection in the pool.")
        conn = client_maint_notifications.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for opening push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=conn,
        )

        logging.info("Validating connection maintenance state...")
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE
        assert conn._sock.gettimeout() == RELAXED_TIMEOUT

        logging.info("Waiting for closing push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=conn,
        )

        logging.info("Validating connection default states is restored...")
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn._sock.gettimeout() == DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT

        logging.info("Releasing connection back to the pool...")
        client_maint_notifications.connection_pool.release(conn)

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
        ),
    )
    def test_notification_handling_during_data_movements_with_conn_drop(
        self,
        fault_injector_client: FaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing cluster operations
        that lead to dropping connections.

        """
        logging.info(f"DB name: {db_name}")

        client_maint_notifications, db_endpoint_config = self.setup_env(
            fault_injector_client, db_config
        )

        # create one connection and release it back to the pool
        conn = client_maint_notifications.connection_pool.get_connection()
        client_maint_notifications.connection_pool.release(conn)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=STANDALONE_MAINT_TIMEOUT
        )

        logging.info("Validating connection migrating state...")
        conn = client_maint_notifications.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE
        assert conn._sock.gettimeout() == RELAXED_TIMEOUT
        client_maint_notifications.connection_pool.release(conn)

        logging.info("Waiting for MIGRATED push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=STANDALONE_MAINT_TIMEOUT
        )

        logging.info("Validating connection states...")
        conn = client_maint_notifications.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn._sock.gettimeout() == DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT
        client_maint_notifications.connection_pool.release(conn)

        logging.info("Waiting for MOVING push notifications...")
        # There might be multiple migrations, so we wait until we receive a moving notification
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            expected_state=MaintenanceState.MOVING,
        )

        logging.info("Validating connection states...")
        conn = client_maint_notifications.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.MOVING
        assert conn._sock.gettimeout() == RELAXED_TIMEOUT

        logging.info("Waiting for moving ttl to expire")
        time.sleep(DEFAULT_BIND_TTL)

        logging.info("Validating connection states...")
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn.socket_timeout == DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT
        assert conn._sock.gettimeout() == DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT

        client_maint_notifications.connection_pool.release(conn)

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
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
    def test_timeout_handling_during_data_movements_with_conn_drop(
        self,
        fault_injector_client: FaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
        endpoint_type: EndpointType,
    ):
        """
        Test the push notifications are received when executing cluster operations
        that lead to dropping connections.

        """
        logging.info(f"DB name: {db_name}")
        logging.info(f"Testing timeout handling for endpoint type: {endpoint_type}")
        client_maint_notifications, db_endpoint_config = self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        # Create three connections in the pool
        logging.info("Creating three connections in the pool.")
        conns = []
        for _ in range(3):
            conns.append(client_maint_notifications.connection_pool.get_connection())
        # Release the connections
        for conn in conns:
            client_maint_notifications.connection_pool.release(conn)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        # this will consume the notification in one of the connections
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=STANDALONE_MAINT_TIMEOUT
        )

        self._validate_maintenance_state(
            client_maint_notifications, expected_matching_conns_count=1
        )
        self._validate_default_state(
            client_maint_notifications,
            expected_matching_conns_count=2,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        logging.info("Waiting for MIGRATED push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=STANDALONE_MAINT_TIMEOUT
        )

        logging.info("Validating connection states after MIGRATED ...")
        self._validate_default_state(
            client_maint_notifications,
            expected_matching_conns_count=3,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        logging.info("Waiting for MOVING push notifications...")
        # this will consume the notification in one of the connections
        # and will handle the states of the rest
        # the consumed connection will be disconnected during
        # releasing it back to the pool and as a result we will have
        # 3 disconnected connections in the pool
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            expected_state=MaintenanceState.MOVING,
        )

        if endpoint_type == EndpointType.NONE:
            logging.info(
                "Waiting for moving ttl/2 to expire to validate proactive reconnection"
            )
            time.sleep(fault_injector_client.get_moving_ttl() / 2)

        logging.info("Validating connections states...")
        self._validate_moving_state(
            client_maint_notifications,
            endpoint_type,
            expected_matching_connected_conns_count=0,
            expected_matching_disconnected_conns_count=3,
            fault_injector_client=fault_injector_client,
        )
        # during get_connection() the connection will be reconnected
        # either to the address provided in the moving notification or to the original address
        # depending on the configured endpoint type
        # with this call we test if we are able to connect to the new address
        conn = client_maint_notifications.connection_pool.get_connection()
        self._validate_moving_state(
            client_maint_notifications,
            endpoint_type,
            expected_matching_connected_conns_count=1,
            expected_matching_disconnected_conns_count=2,
            fault_injector_client=fault_injector_client,
        )
        client_maint_notifications.connection_pool.release(conn)

        logging.info("Waiting for moving ttl to expire")
        time.sleep(DEFAULT_BIND_TTL)

        logging.info("Validating connection states...")
        self._validate_default_state(
            client_maint_notifications,
            expected_matching_conns_count=3,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
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
    def test_connection_handling_during_data_movements_with_conn_drop(
        self,
        fault_injector_client: FaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
        endpoint_type: EndpointType,
    ):
        """
        Test the push notifications are received when executing cluster operations
        that lead to dropping connections.
        This test validates that the connection is reconnected to the new address
        after the moving notification is received.
        This test also validates that newly created connections will also be
        reconnected to the new address.

        """
        logging.info(f"DB name: {db_name}")
        logging.info(f"Testing with endpoint type: {endpoint_type}")
        client_maint_notifications, db_endpoint_config = self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        logging.info("Creating one connection in the pool.")
        first_conn = client_maint_notifications.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        # this will consume the notification in the provided connection
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        self._validate_maintenance_state(
            client_maint_notifications, expected_matching_conns_count=1
        )

        logging.info("Waiting for MIGRATED push notification ...")
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        client_maint_notifications.connection_pool.release(first_conn)

        logging.info("Waiting for MOVING push notifications on random connection ...")
        # this will consume the notification in one of the connections
        # and will handle the states of the rest
        # the consumed connection will be disconnected during
        # releasing it back to the pool and as a result we will have
        # 3 disconnected connections in the pool
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            expected_state=MaintenanceState.MOVING,
        )

        if endpoint_type == EndpointType.NONE:
            logging.info(
                "Waiting for moving ttl/2 to expire to validate proactive reconnection"
            )
            time.sleep(fault_injector_client.get_moving_ttl() / 2)

        connections = []
        for _ in range(3):
            connections.append(
                client_maint_notifications.connection_pool.get_connection()
            )
        for conn in connections:
            logging.debug(f"Releasing connection {conn}. {conn.maintenance_state}")
            client_maint_notifications.connection_pool.release(conn)

        logging.info("Validating connections states during MOVING ...")
        # during get_connection() the existing connection will be reconnected
        # either to the address provided in the moving notification or to the original address
        # depending on the configured endpoint type
        # with this call we test if we are able to connect to the new address
        # new connection should also be marked as moving
        self._validate_moving_state(
            client_maint_notifications,
            endpoint_type,
            expected_matching_connected_conns_count=3,
            expected_matching_disconnected_conns_count=0,
            fault_injector_client=fault_injector_client,
        )

        logging.info("Waiting for moving ttl to expire")
        time.sleep(fault_injector_client.get_moving_ttl())

        logging.info("Validating connection states after MOVING has expired ...")
        self._validate_default_state(
            client_maint_notifications,
            expected_matching_conns_count=3,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
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
    def test_new_connections_receive_notifications_no_conn_drop(
        self,
        fault_injector_client: FaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing cluster operations
        that lead to dropping connections.

        """
        logging.info(f"DB name: {db_name}")

        client_maint_notifications, db_endpoint_config = self.setup_env(
            fault_injector_client, db_config
        )

        logging.info("Creating one connection in the pool.")
        first_conn = client_maint_notifications.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for opening push notifications...")
        # this will consume the notification in the provided connection
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )
        self._validate_maintenance_state(
            client_maint_notifications, expected_matching_conns_count=1
        )

        # validate that new connections will also receive the opening notification
        # it should be received as part of the client connection setup flow
        logging.info(
            "Creating second connection that should receive the opening notification as well."
        )
        second_connection = client_maint_notifications.connection_pool.get_connection()
        self._validate_maintenance_state(
            client_maint_notifications, expected_matching_conns_count=2
        )

        logging.info("Waiting for closing push notifications on both connections ...")
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=second_connection,
        )

        self._validate_default_state(
            client_maint_notifications,
            expected_matching_conns_count=2,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        client_maint_notifications.connection_pool.release(first_conn)
        client_maint_notifications.connection_pool.release(second_connection)

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
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
    def test_old_connection_shutdown_during_moving(
        self,
        fault_injector_client: FaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing cluster operations
        that lead to dropping connections.

        """
        logging.info(f"DB name: {db_name}")

        # it is better to use ip for this test - enables validation that
        # the connection is disconnected from the original address
        # and connected to the new address
        endpoint_type = EndpointType.EXTERNAL_IP
        logging.info("Testing old connection shutdown during MOVING")

        client, db_endpoint_config = self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        # create one connection and release it back to the pool
        conn = client.connection_pool.get_connection()
        client.connection_pool.release(conn)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        ClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )
        self._validate_maintenance_state(client, expected_matching_conns_count=1)

        logging.info("Waiting for MIGRATED push notification ...")
        ClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )
        self._validate_default_state(
            client,
            expected_matching_conns_count=1,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        moving_event = threading.Event()

        def execute_commands(moving_event: threading.Event, errors: Queue):
            while not moving_event.is_set():
                try:
                    client.set("key", "value")
                    client.get("key")
                except Exception as e:
                    errors.put(
                        f"[mono={time.monotonic():.6f}] Command failed in thread "
                        f"{threading.current_thread().name}: {e}"
                    )

        # get the connection here because in case of proxy server
        # new connections will not receive the notification and there is a chance
        # that the existing connections in the pool that are used in the multiple
        # threads might have already consumed the notification
        # even with re clusters we might end up with an existing connection that has been
        # freed up in the pool that will not receive the notification while we are waiting
        # for it because it has already received and processed it
        conn_to_check_moving = client.connection_pool.get_connection()

        errors = Queue()
        threads_count = 10
        futures = []

        logging.info(f"Starting {threads_count} command execution threads...")
        # Start the worker pool and submit N identical worker tasks
        with ThreadPoolExecutor(
            max_workers=threads_count, thread_name_prefix="command_execution_thread"
        ) as executor:
            futures = [
                executor.submit(execute_commands, moving_event, errors)
                for _ in range(threads_count)
            ]

            logging.info("Waiting for MOVING push notification ...")
            # this will consume the notification in one of the connections
            # and will handle the states of the rest
            ClientValidations.wait_push_notification(
                client,
                timeout=STANDALONE_MAINT_TIMEOUT,
                connection=conn_to_check_moving,
                expected_state=MaintenanceState.MOVING,
            )
            # set the event to stop the command execution threads
            logging.info("Setting moving event...")
            moving_event.set()
            # release the connection back to the pool so that it can be disconnected
            # as part of the flow
            client.connection_pool.release(conn_to_check_moving)

            # Wait for all workers to finish and propagate any exceptions
            for f in futures:
                f.result()

        logging.info(
            "All command execution threads finished. Validating connections states..."
        )
        # validate that all connections are either disconnected
        # or connected to the new address
        connections = self._get_all_connections_in_pool(client)
        for conn in connections:
            if conn._sock is not None:
                assert conn.get_resolved_ip() == conn.host
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn._sock.gettimeout() == RELAXED_TIMEOUT
                if not isinstance(fault_injector_client, ProxyServerFaultInjector):
                    assert conn.host != conn.orig_host_address
                assert not conn.should_reconnect()
            else:
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn.socket_timeout == RELAXED_TIMEOUT
                if not isinstance(fault_injector_client, ProxyServerFaultInjector):
                    assert conn.host != conn.orig_host_address
                assert not conn.should_reconnect()

        # validate no errors were raised in the command execution threads
        self._fail_on_command_errors(errors, "command execution threads")

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

    @pytest.mark.timeout(300)  # 5 minutes timeout
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
    def test_new_connections_receive_moving(
        self,
        fault_injector_client: FaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        endpoint_type = EndpointType.EXTERNAL_IP
        client_maint_notifications, db_endpoint_config = self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        logging.info("Creating one connection in the pool.")
        first_conn = client_maint_notifications.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        # this will consume the notification in the provided connection
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        self._validate_maintenance_state(
            client_maint_notifications, expected_matching_conns_count=1
        )

        logging.info("Waiting for MIGRATED push notifications ...")
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        logging.info("Waiting for MOVING push notifications on random connection ...")
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
            expected_state=MaintenanceState.MOVING,
        )

        assert first_conn._sock is not None, (
            "Connection must be active after receiving MOVING notification"
        )
        old_address = first_conn._sock.getpeername()[0]
        logging.info(f"The node address before bind: {old_address}")
        logging.info(
            "Creating new client to connect to the same node - new connections to this node should receive the moving notification..."
        )

        auth_ssl_client_certs_config_info = db_config.get(
            "authentication_ssl_client_certs", None
        )
        auth_ssl_client_certs = (
            True
            if auth_ssl_client_certs_config_info
            and auth_ssl_client_certs_config_info[0].get("client_cert") is not None
            else False
        )

        # create new client with new pool that should also receive the moving notification
        new_client = _get_client_maint_notifications(
            endpoints_config=db_endpoint_config,
            endpoint_type=endpoint_type,
            host_config=old_address,
            socket_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
            auth_ssl_client_certs=auth_ssl_client_certs,
        )

        # the moving notification will be consumed as
        # part of the client connection setup, so we don't need
        # to wait for it explicitly with wait_push_notification
        logging.info(
            "Creating one connection in the new pool that should receive the moving notification."
        )
        new_client_conn = new_client.connection_pool.get_connection()

        logging.info("Validating connections states during MOVING ...")
        self._validate_moving_state(
            new_client,
            endpoint_type,
            expected_matching_connected_conns_count=1,
            expected_matching_disconnected_conns_count=0,
            fault_injector_client=fault_injector_client,
        )

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

        new_client.connection_pool.release(new_client_conn)
        new_client.close()

        client_maint_notifications.connection_pool.release(first_conn)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
        ),
    )
    def test_disabled_handling_during_migrating_and_moving(
        self,
        fault_injector_client: FaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        self.delete_prev_db(fault_injector_client, db_config["name"])
        # Recorded before the create so teardown removes a partially created DB.
        self._bdb_name = db_config["name"]
        db_endpoint_config = self.create_db(fault_injector_client, db_config)

        logging.info("Creating client with disabled notifications.")
        # self._client for teardown cleanup; local `client` used in the rest of this method.
        self._client = client = get_standalone_client_maint_notifications(
            endpoints_config=db_endpoint_config,
            disable_retries=True,
            enable_maintenance_notifications=False,
        )

        logging.info("Creating one connection in the pool.")
        first_conn = client.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        # this will consume the notification in the provided connection if it arrives
        ClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=first_conn
        )

        self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=1
        )

        # validate that new connections will also not receive the migrating notification
        logging.info(
            "Creating second connection in the pool"
            " and expect it not to receive the migrating as well."
        )

        second_connection = client.connection_pool.get_connection()
        ClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=second_connection
        )

        logging.info(
            "Validating connection states after MIGRATING for both connections ..."
        )
        self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=2
        )

        logging.info("Waiting for MIGRATED push notifications on both connections ...")
        ClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=first_conn
        )
        ClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=second_connection
        )

        client.connection_pool.release(first_conn)
        client.connection_pool.release(second_connection)

        logging.info("Waiting for MOVING push notifications on random connection ...")
        # this will consume the notification if it arrives in one of the connections
        # and will handle the states of the rest
        # the consumed connection will be disconnected during
        # releasing it back to the pool and as a result we will have
        # 3 disconnected connections in the pool
        ClientValidations.wait_push_notification(
            client,
            timeout=10,
            fail_on_timeout=False,
        )

        # validate that new connections will also not receive the moving notification
        connections = []
        for _ in range(3):
            connections.append(client.connection_pool.get_connection())
        for conn in connections:
            client.connection_pool.release(conn)

        logging.info("Validating connections states during MOVING ...")
        self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=3
        )

        logging.info("Waiting for moving ttl to expire")
        time.sleep(DEFAULT_BIND_TTL)

        logging.info("Validating connection states after MOVING has expired ...")
        self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=3
        )

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)


class TestStandaloneClientCommandsExecutionWithPushNotificationsWithEffectTrigger(
    TestStanaloneClientPushNotificationsWithEffectTriggerBase
):
    # TODO: re-enable TopologyChangeStandaloneEffects.DNS_RESOLUTION_CHANGE once the
    # effect is troubleshooted and fixed.
    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
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
    def test_command_execution_during_maintenance(
        self,
        fault_injector_client: FaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
        endpoint_type: EndpointType,
    ):
        """
        Test command execution during migrating and moving notifications.

        This test validates that:
        1. Commands can be executed during MIGRATING and MOVING notifications
        2. Commands are not blocked by the notifications
        3. Commands are executed successfully
        """
        errors = Queue()
        if isinstance(fault_injector_client, ProxyServerFaultInjector):
            execution_duration = 20
        else:
            execution_duration = 60

        logging.info(f"DB name: {db_name}")
        logging.info(f"Testing timeout handling for endpoint type: {endpoint_type}")
        client_maint_notifications, db_endpoint_config = self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        def execute_commands(duration: int, errors: Queue):
            start = time.time()
            while time.time() - start < duration:
                try:
                    client_maint_notifications.set("key", "value")
                    client_maint_notifications.get("key")
                except Exception as e:
                    logging.error(
                        f"Error in thread {threading.current_thread().name}: {e}"
                    )
                    errors.put(
                        f"[mono={time.monotonic():.6f}] Command failed in thread "
                        f"{threading.current_thread().name}: {e}"
                    )
            logging.debug(f"{threading.current_thread().name}: Thread ended")

        threads = []
        for i in range(10):
            thread = Thread(
                target=execute_commands,
                name=f"command_execution_thread_{i}",
                args=(
                    execution_duration,
                    errors,
                ),
            )
            thread.start()
            threads.append(thread)

        logging.info("Waiting for threads to start and have a few cycles executed ...")
        time.sleep(3)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        for thread in threads:
            thread.join()

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

        # Drain first so the command errors are logged even if one of the
        # assertions below raises.
        collected_errors = self._drain_command_errors(
            errors, "command execution threads"
        )

        # A failed trigger voids the premise of the test, so report it before
        # judging the connection state it was supposed to produce.
        self._fail_on_trigger_effect_failures()

        # validate connections settings. "all" rather than a hard-coded 10: the
        # pool size is emergent from the worker count, so pinning the number
        # fails on an extra connection even when every connection is in the
        # expected state - and passes when one of eleven is not.
        self._validate_default_state(
            client_maint_notifications,
            expected_matching_conns_count="all",
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        self._fail_on_collected_command_errors(
            collected_errors, "command execution threads"
        )


class TestClusterClientPushNotificationsWithEffectTriggerBase(
    TestPushNotificationsBase
):
    def setup_env(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        db_config: Dict[str, Any],
    ):
        self.delete_prev_db(fault_injector_client_oss_api, db_config["name"])

        # Recorded before the create so teardown removes a partially created DB.
        self._bdb_name = db_config["name"]
        cluster_endpoint_config = self.create_db(
            fault_injector_client_oss_api, db_config
        )

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

        self._client = get_cluster_client_maint_notifications(
            endpoints_config=cluster_endpoint_config,
            disable_retries=True,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            enable_maintenance_notifications=True,
            auth_ssl_client_certs=auth_ssl_client_certs,
        )
        return self._client, cluster_endpoint_config

    def _wait_for_node_cache_delta(
        self,
        client,
        connections,
        initial_count,
        expected_delta,
        timeout=SMIGRATED_TIMEOUT,
    ):
        """Poll the client node cache until it reflects the expected +/- delta.

        A topology change may arrive as several push notifications (in particular an ASM scale
        add/remove is a cascade of SMIGRATING/SMIGRATED as the decision engine moves slots), so
        the client reconciles its node cache over multiple notifications after the server-side
        operation finishes. Drain pending notifications on the given connections and poll until
        the cache reaches initial_count + expected_delta, or the timeout elapses. Single-step
        triggers (migrate/failover) reach the target immediately and return on the first check.
        """
        expected = initial_count + expected_delta
        deadline = time.time() + timeout
        while time.time() < deadline:
            if len(client.nodes_manager.nodes_cache) == expected:
                return
            for conn in connections:
                try:
                    if conn._sock and conn.can_read(timeout=0.2):
                        conn.read_response(push_request=True)
                except Exception:
                    # A connection to a node being removed may error; that is expected.
                    pass
            time.sleep(0.5)
        # Fail explicitly on a drain timeout so it surfaces as a clear "node cache did not
        # reconcile" error, rather than silently returning and letting the caller's exact-delta
        # assertion fail later with a misleading node-count message.
        pytest.fail(
            f"Node cache did not reach {expected} (delta {expected_delta}) within {timeout}s; "
            f"current size={len(client.nodes_manager.nodes_cache)}"
        )

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(
        self,
    ):
        self.maintenance_ops_threads = []
        self._bdb_name = None
        self._client = None

        # Yield control to the test
        yield

        # Cleanup code - this will run even if the test fails
        logging.info("Starting cleanup...")

        if self._client is not None:
            self._client.close()

        logging.info("Waiting for maintenance operations threads to finish...")
        for thread in self.maintenance_ops_threads:
            thread.join()

        if self._bdb_name:
            self.delete_prev_db(_FAULT_INJECTOR_CLIENT_OSS_API, self._bdb_name)

        logging.info("Cleanup finished")


class TestClusterClientPushNotificationsHandlingWithEffectTrigger(
    TestClusterClientPushNotificationsWithEffectTriggerBase
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
    def test_notification_handling_during_node_shuffle_no_node_replacement(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing re cluster operations.
        The test validates the behavior when during the operations the slots are moved
        between the nodes, but no new nodes are appearing and no nodes are disappearing

        """
        logging.info(f"DB name: {db_name}")

        cluster_client_maint_notifications, cluster_endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")
        initial_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = (
                node.redis_connection.connection_pool.get_connection()
            )

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
            ),
            kwargs={"timeout": RESHARD_OP_TIMEOUT},
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conn in in_use_connections.values():
            ClientValidations.wait_push_notification(
                cluster_client_maint_notifications,
                timeout=RESHARD_SMIGRATING_TIMEOUT,
                connection=conn,
            )

        logging.info("Validating connection maintenance state...")
        for conn in in_use_connections.values():
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn._sock.gettimeout() == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        assert len(initial_cluster_nodes) == len(
            cluster_client_maint_notifications.nodes_manager.nodes_cache
        )

        for node_key in initial_cluster_nodes.keys():
            assert (
                node_key in cluster_client_maint_notifications.nodes_manager.nodes_cache
            )

        logging.info("Waiting for SMIGRATED push notifications...")
        con_to_read_smigrated = random.choice(list(in_use_connections.values()))
        ClientValidations.wait_push_notification(
            cluster_client_maint_notifications,
            timeout=SMIGRATED_TIMEOUT,
            connection=con_to_read_smigrated,
        )

        logging.info("Validating connection state after SMIGRATED ...")

        updated_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )

        removed_nodes = set(initial_cluster_nodes.values()) - set(
            updated_cluster_nodes.values()
        )
        assert len(removed_nodes) == 0
        assert len(initial_cluster_nodes) == len(updated_cluster_nodes)

        marked_conns_for_reconnect = 0
        for conn in in_use_connections.values():
            if conn.should_reconnect():
                marked_conns_for_reconnect += 1
        # only one connection should be marked for reconnect
        # onle the one that belongs to the node that was from
        # the src address of the maintenance
        assert marked_conns_for_reconnect == 1

        logging.info("Releasing connections back to the pool...")
        for node, conn in in_use_connections.items():
            if node.redis_connection is None:
                continue
            node.redis_connection.connection_pool.release(conn)

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

    @pytest.mark.timeout(
        RESHARD_TEST_TIMEOUT
    )  # sized for slow ASM reshard, see RESHARD_OP_TIMEOUT
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API,
            [
                SlotMigrateEffects.REMOVE_ADD,
            ],
        ),
    )
    def test_notification_handling_with_node_replace(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing re cluster operations.
        The test validates the behavior when during the operations the slots are moved
        between the nodes, and as a result a node is removed and a new node is added to the cluster

        """
        logging.info(f"DB name: {db_name}")

        cluster_client_maint_notifications, cluster_endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")

        initial_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = (
                node.redis_connection.connection_pool.get_connection()
            )

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
            ),
            kwargs={"timeout": RESHARD_OP_TIMEOUT},
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conn in in_use_connections.values():
            ClientValidations.wait_push_notification(
                cluster_client_maint_notifications,
                timeout=RESHARD_SMIGRATING_TIMEOUT,
                connection=conn,
            )

        logging.info("Validating connection maintenance state...")
        for conn in in_use_connections.values():
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn._sock.gettimeout() == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        assert len(initial_cluster_nodes) == len(
            cluster_client_maint_notifications.nodes_manager.nodes_cache
        )

        for node_key in initial_cluster_nodes.keys():
            assert (
                node_key in cluster_client_maint_notifications.nodes_manager.nodes_cache
            )

        logging.info("Waiting for SMIGRATED push notifications...")
        con_to_read_smigrated = random.choice(list(in_use_connections.values()))
        ClientValidations.wait_push_notification(
            cluster_client_maint_notifications,
            timeout=SMIGRATED_TIMEOUT,
            connection=con_to_read_smigrated,
        )

        logging.info("Validating connection state after SMIGRATED ...")

        updated_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )

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
        # connection will be dropped, but it is marked
        # to be disconnected before released to the pool
        # we don't waste time to update the timeouts and state
        # so it is pointless to check those configs
        assert conn is not None
        assert conn.should_reconnect() is True

        logging.info("Releasing connections back to the pool...")
        for node, conn in in_use_connections.items():
            if node.redis_connection is None:
                continue
            node.redis_connection.connection_pool.release(conn)

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

    @pytest.mark.timeout(
        RESHARD_TEST_TIMEOUT
    )  # sized for slow ASM reshard, see RESHARD_OP_TIMEOUT
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API,
            [
                SlotMigrateEffects.REMOVE,
            ],
        ),
    )
    def test_notification_handling_with_node_remove(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing re cluster operations.
        The test validates the behavior when during the operations the slots are moved
        between the nodes, and as a result a node is removed.

        """
        logging.info(f"DB name: {db_name}")

        cluster_client_maint_notifications, cluster_endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")

        initial_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = (
                node.redis_connection.connection_pool.get_connection()
            )

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
            ),
            kwargs={"timeout": RESHARD_OP_TIMEOUT},
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conn in in_use_connections.values():
            ClientValidations.wait_push_notification(
                cluster_client_maint_notifications,
                timeout=RESHARD_SMIGRATING_TIMEOUT,
                connection=conn,
            )

        logging.info("Validating connection maintenance state...")
        for conn in in_use_connections.values():
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn._sock.gettimeout() == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        assert len(initial_cluster_nodes) == len(
            cluster_client_maint_notifications.nodes_manager.nodes_cache
        )

        for node_key in initial_cluster_nodes.keys():
            assert (
                node_key in cluster_client_maint_notifications.nodes_manager.nodes_cache
            )

        logging.info("Waiting for SMIGRATED push notifications...")
        con_to_read_smigrated = random.choice(list(in_use_connections.values()))
        ClientValidations.wait_push_notification(
            cluster_client_maint_notifications,
            timeout=SMIGRATED_TIMEOUT,
            connection=con_to_read_smigrated,
        )

        logging.info("Waiting for the operation to finish server-side...")
        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

        logging.info("Draining notifications until the node cache reflects -1 node...")
        self._wait_for_node_cache_delta(
            cluster_client_maint_notifications,
            list(in_use_connections.values()),
            len(initial_cluster_nodes),
            expected_delta=-1,
        )

        logging.info("Validating connection state after SMIGRATED ...")

        updated_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )

        removed_nodes = set(initial_cluster_nodes.values()) - set(
            updated_cluster_nodes.values()
        )
        assert len(removed_nodes) == 1
        removed_node = removed_nodes.pop()
        assert removed_node is not None

        assert len(initial_cluster_nodes) == len(updated_cluster_nodes) + 1

        conn = in_use_connections.get(removed_node)
        # connection will be dropped, but it is marked
        # to be disconnected before released to the pool
        # we don't waste time to update the timeouts and state
        # so it is pointless to check those configs
        assert conn is not None
        assert conn.should_reconnect() is True

        # At least the removed node's connection is marked for reconnect. A single-step migrate
        # marks exactly one; an ASM scale-in cascade may touch more, so accept >= 1.
        marked_conns_for_reconnect = sum(
            1 for conn in in_use_connections.values() if conn.should_reconnect()
        )
        assert marked_conns_for_reconnect >= 1

        logging.info("Releasing connections back to the pool...")
        for node, conn in in_use_connections.items():
            if node.redis_connection is None:
                continue
            node.redis_connection.connection_pool.release(conn)

    @pytest.mark.timeout(
        RESHARD_TEST_TIMEOUT
    )  # sized for slow ASM reshard, see RESHARD_OP_TIMEOUT
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API,
            [
                SlotMigrateEffects.ADD,
            ],
        ),
    )
    def test_notification_handling_with_node_add(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test push notifications when a cluster operation moves slots such that a previously
        empty node gains a master shard / endpoint (the client's node cache grows by one).
        Mirrors test_notification_handling_with_node_remove for the opposite (+1) delta.
        """
        logging.info(f"DB name: {db_name}")

        cluster_client_maint_notifications, cluster_endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")
        initial_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = (
                node.redis_connection.connection_pool.get_connection()
            )

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
            ),
            kwargs={"timeout": RESHARD_OP_TIMEOUT},
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conn in in_use_connections.values():
            ClientValidations.wait_push_notification(
                cluster_client_maint_notifications,
                timeout=RESHARD_SMIGRATING_TIMEOUT,
                connection=conn,
            )

        logging.info("Validating connection maintenance state...")
        for conn in in_use_connections.values():
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn._sock.gettimeout() == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        # During SMIGRATING (before reconciliation) the added node is not in the cache yet: the
        # cache size is unchanged and every initial node key is still present. Mirrors the same
        # invariant asserted by test_notification_handling_with_node_remove.
        assert len(initial_cluster_nodes) == len(
            cluster_client_maint_notifications.nodes_manager.nodes_cache
        )
        for node_key in initial_cluster_nodes.keys():
            assert (
                node_key in cluster_client_maint_notifications.nodes_manager.nodes_cache
            )

        logging.info("Waiting for SMIGRATED push notifications...")
        con_to_read_smigrated = random.choice(list(in_use_connections.values()))
        ClientValidations.wait_push_notification(
            cluster_client_maint_notifications,
            timeout=SMIGRATED_TIMEOUT,
            connection=con_to_read_smigrated,
        )

        logging.info("Waiting for the operation to finish server-side...")
        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

        logging.info("Draining notifications until the node cache reflects +1 node...")
        self._wait_for_node_cache_delta(
            cluster_client_maint_notifications,
            list(in_use_connections.values()),
            len(initial_cluster_nodes),
            expected_delta=1,
        )

        logging.info("Validating a node was added to the client's node cache...")
        updated_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )
        added_nodes = set(updated_cluster_nodes.values()) - set(
            initial_cluster_nodes.values()
        )
        assert len(added_nodes) == 1
        assert len(updated_cluster_nodes) == len(initial_cluster_nodes) + 1

        # An add moves slots FROM an existing shard onto the previously empty node, so the
        # connection to that source shard is marked for reconnect.
        marked_conns_for_reconnect = sum(
            1 for conn in in_use_connections.values() if conn.should_reconnect()
        )
        assert marked_conns_for_reconnect >= 1

        logging.info("Releasing connections back to the pool...")
        for node, conn in in_use_connections.items():
            if node.redis_connection is None:
                continue
            node.redis_connection.connection_pool.release(conn)

    @pytest.mark.timeout(
        RESHARD_TEST_TIMEOUT
    )  # sized for slow ASM reshard, see RESHARD_OP_TIMEOUT
    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support sending notifications to new connections.",
    )
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API,
            [
                SlotMigrateEffects.SLOT_SHUFFLE,
                SlotMigrateEffects.REMOVE_ADD,
                SlotMigrateEffects.REMOVE,
                SlotMigrateEffects.ADD,
            ],
            # Catching the in-flight SMIGRATING on a NEWLY opened connection needs a
            # maintenance window that stays open long enough to open that fresh connection
            # mid-flight. These combinations close too fast for the test to be reliable:
            #   - slot_shuffle + failover: maintenance ends almost immediately;
            #   - reshard + add/remove: a cascade of short SMIGRATING/SMIGRATED windows, so a
            #     connection opened between windows sees no maintenance state;
            #   - reshard + slot_shuffle: a single ASM migrate_slots that, for a small slot
            #     range, closes (SMIGRATED) ~1s after SMIGRATING.
            # remove_add keeps a long enough combined window and is still exercised here.
            skip_combinations=[
                (SlotMigrateEffects.SLOT_SHUFFLE, "failover"),
                (SlotMigrateEffects.SLOT_SHUFFLE, "reshard"),
                (SlotMigrateEffects.REMOVE, "reshard"),
                (SlotMigrateEffects.ADD, "reshard"),
            ],
        ),
    )
    def test_new_connections_receive_last_smigrating_smigrated_notification(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are sent to the newly created connections.

        """
        logging.info(f"DB name: {db_name}")

        cluster_client_maint_notifications, cluster_endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")
        initial_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = [
                node.redis_connection.connection_pool.get_connection()
            ]

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
            ),
            kwargs={"timeout": RESHARD_OP_TIMEOUT},
        )

        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conns_per_node in in_use_connections.values():
            for conn in conns_per_node:
                ClientValidations.wait_push_notification(
                    cluster_client_maint_notifications,
                    timeout=RESHARD_SMIGRATING_TIMEOUT,
                    connection=conn,
                )
                logging.info(
                    f"Validating connection MAINTENANCE state and RELAXED timeout for conn: {conn}..."
                )
                assert conn.maintenance_state == MaintenanceState.MAINTENANCE
                assert conn._sock.gettimeout() == RELAXED_TIMEOUT
                assert conn.should_reconnect() is False

        logging.info(
            "Validating newly created connections will receive the SMIGRATING notification..."
        )
        for node in initial_cluster_nodes.values():
            conn = node.redis_connection.connection_pool.get_connection()
            in_use_connections[node].append(conn)
            ClientValidations.wait_push_notification(
                cluster_client_maint_notifications,
                timeout=1,
                connection=conn,
                fail_on_timeout=False,  # it might get read during handshake
            )
            logging.info(
                f"Validating new connection MAINTENANCE state and RELAXED timeout for conn: {conn}..."
            )
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn._sock.gettimeout() == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        logging.info(
            "Waiting for SMIGRATED push notifications in ALL EXISTING connections..."
        )
        marked_conns_for_reconnect = 0
        for conns_per_node in in_use_connections.values():
            for conn in conns_per_node:
                ClientValidations.wait_push_notification(
                    cluster_client_maint_notifications,
                    timeout=SMIGRATED_TIMEOUT,
                    connection=conn,
                )
                logging.info(
                    f"Validating connection state after SMIGRATED for conn: {conn}, "
                    f"local socket port: {conn._sock.getsockname()[1] if conn._sock else None}..."
                )
                if conn.should_reconnect():
                    logging.info(f"Connection marked for reconnect: {conn}")
                    marked_conns_for_reconnect += 1
                assert conn.maintenance_state == MaintenanceState.NONE
                assert conn.socket_timeout == DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT
                assert (
                    conn.socket_connect_timeout == DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT
                )
        assert (
            marked_conns_for_reconnect >= 1
        )  # at least one should be marked for reconnect

        logging.info("Releasing connections back to the pool...")
        for node, conns in in_use_connections.items():
            if node.redis_connection is None:
                continue
            for conn in conns:
                node.redis_connection.connection_pool.release(conn)

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)


class TestClusterClientCommandsExecutionWithPushNotificationsWithEffectTrigger(
    TestClusterClientPushNotificationsWithEffectTriggerBase
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
    def test_command_execution_during_slot_shuffle_no_node_replacement(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing re cluster operations.
        """
        logging.info(f"DB name: {db_name}")

        cluster_client_maint_notifications, cluster_endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        shards_count = db_config["shards_count"]
        logging.info(f"Shards count: {shards_count}")

        errors = Queue()
        if isinstance(fault_injector_client_oss_api, ProxyServerFaultInjector):
            execution_duration = 20
        else:
            execution_duration = 40

        def execute_commands(duration: int, errors: Queue):
            start = time.time()
            executed_commands_count = 0
            keys_for_all_shards = KeyGenerationHelpers.generate_keys_for_all_shards(
                shards_count,
                prefix=f"{threading.current_thread().name}_{effect_name}_{trigger}_key",
            )

            logging.info("Starting commands execution...")
            while time.time() - start < duration:
                for key in keys_for_all_shards:
                    try:
                        # the slot is covered by the first shard - this one will have slots migrated
                        cluster_client_maint_notifications.set(key, "value")
                        cluster_client_maint_notifications.get(key)
                        executed_commands_count += 2
                    except Exception as e:
                        logging.error(
                            f"Error in thread {threading.current_thread().name}: {e}"
                        )
                        errors.put(
                            f"[mono={time.monotonic():.6f}] Command failed in thread "
                            f"{threading.current_thread().name}: {e}"
                        )
                if executed_commands_count % 500 == 0:
                    logging.debug(
                        f"Executed {executed_commands_count} commands in {threading.current_thread().name}"
                    )
            logging.debug(f"{threading.current_thread().name}: Thread ended")

        threads = []
        for i in range(10):
            thread = Thread(
                target=execute_commands,
                name=f"cmd_execution_{i}",
                args=(
                    execution_duration,
                    errors,
                ),
            )
            thread.start()
            threads.append(thread)

        logging.info("Waiting for threads to start and have a few cycles executed ...")
        time.sleep(3)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
            ),
            kwargs={"timeout": RESHARD_OP_TIMEOUT},
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        for thread in threads:
            thread.join()

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

        # go through all nodes and all their connections and consume the buffers - to validate no
        # notifications were left unconsumed
        logging.info(
            "Consuming all buffers to validate no notifications were left unconsumed..."
        )
        for (
            node
        ) in cluster_client_maint_notifications.nodes_manager.nodes_cache.values():
            if node.redis_connection is None:
                continue
            for conn in self._get_all_connections_in_pool(node.redis_connection):
                if conn._sock:
                    while conn.can_read(timeout=0.2):
                        conn.read_response(push_request=True)
            logging.info(f"Consumed all buffers for node: {node.name}")
        logging.info("All buffers consumed.")

        # Drain first so the command errors are logged even if one of the
        # assertions below raises.
        collected_errors = self._drain_command_errors(
            errors, "command execution threads"
        )

        # A failed trigger voids the premise of the test, so report it before
        # judging the connection state it was supposed to produce.
        self._fail_on_trigger_effect_failures()

        for (
            node
        ) in cluster_client_maint_notifications.nodes_manager.nodes_cache.values():
            # validate connections settings
            self._validate_default_state(
                node.redis_connection,
                expected_matching_conns_count="all",
                configured_timeout=DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT,
            )
            logging.info(
                f"Node successfully validated: {node.name}, "
                f"connections: {len(self._get_all_connections_in_pool(node.redis_connection))}"
            )

        # validate no errors were raised in the command execution threads
        self._fail_on_collected_command_errors(
            collected_errors, "command execution threads"
        )
