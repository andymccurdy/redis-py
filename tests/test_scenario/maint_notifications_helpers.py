import binascii
import logging
import time
from typing import Any, Dict, Optional, Union
import pytest
from redis import RedisCluster

from redis.client import Redis
from redis.connection import Connection
from redis.maint_notifications import EndpointType, MaintenanceState
from tests.test_scenario.fault_injector_client import (
    FaultInjectorClient,
    MockProxyFaultInjector,
    SlotMigrateEffects,
    TopologyChangeStandaloneEffects,
)


class ClientValidations:
    @staticmethod
    def get_default_connection(redis_client: Union[Redis, RedisCluster]) -> Connection:
        """Get a random connection from the pool."""
        if isinstance(redis_client, RedisCluster):
            return redis_client.get_default_node().redis_connection.connection_pool.get_connection()
        if isinstance(redis_client, Redis):
            return redis_client.connection_pool.get_connection()
        raise ValueError(f"Unsupported redis client type: {type(redis_client)}")

    @staticmethod
    def release_connection(
        redis_client: Union[Redis, RedisCluster], connection: Connection
    ):
        """Release a connection back to the pool."""
        if isinstance(redis_client, RedisCluster):
            node_address = connection.host + ":" + str(connection.port)
            node = redis_client.get_node(node_address)
            if node is None:
                raise ValueError(
                    f"Node not found in cluster for address: {node_address}"
                )
            node.redis_connection.connection_pool.release(connection)
        elif isinstance(redis_client, Redis):
            redis_client.connection_pool.release(connection)
        else:
            raise ValueError(f"Unsupported redis client type: {type(redis_client)}")

    @staticmethod
    def wait_push_notification(
        redis_client: Union[Redis, RedisCluster],
        timeout: float = 120,
        fail_on_timeout: bool = True,
        connection: Optional[Connection] = None,
        expected_state: Optional[MaintenanceState] = None,
    ):
        """Wait for a push notification to be received."""
        start_time = time.time()  # returns the time in seconds
        check_interval = 0.2  # Check more frequently during operations
        test_conn = (
            connection
            if connection
            else ClientValidations.get_default_connection(redis_client)
        )
        logging.info(
            f"Waiting for push notification on connection: {test_conn}, "
            f"local socket port: {test_conn._sock.getsockname()[1] if test_conn._sock else None}"
        )

        try:
            if (
                expected_state is not None
                and test_conn.maintenance_state == expected_state
            ):
                logging.debug(
                    f"Connection already in expected state {expected_state}, "
                    f"returning immediately"
                )
                return

            while time.time() - start_time < timeout:
                try:
                    if test_conn.can_read(timeout=0.2):
                        # reading is important, it triggers the push notification
                        push_response = test_conn.read_response(push_request=True)
                        logging.debug(
                            f"Push notification has been received. Response: {push_response}"
                        )
                        if test_conn.should_reconnect():
                            logging.debug("Connection is marked for reconnect")
                        if (
                            expected_state is None
                            or test_conn.maintenance_state == expected_state
                        ):
                            return
                except Exception as e:
                    logging.error(f"Error reading push notification: {e}")
                    break
                time.sleep(check_interval)
            if fail_on_timeout:
                pytest.fail(
                    f"Timeout waiting for push notification: waiting > {time.time() - start_time} seconds."
                )
        finally:
            # Release the connection back to the pool
            try:
                if not connection:
                    ClientValidations.release_connection(redis_client, test_conn)
            except Exception as e:
                logging.error(f"Error releasing connection: {e}")


class ClusterOperations:
    @staticmethod
    def find_database_id_by_name(
        fault_injector: FaultInjectorClient,
        database_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> Optional[int]:
        """Find the database ID by name."""
        return fault_injector.find_database_id_by_name(
            database_name, force_cluster_info_refresh
        )

    @staticmethod
    def get_slot_migrate_triggers(
        fault_injector: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
    ) -> Dict[str, Any]:
        """Get available triggers(trigger name + db example config) for a slot migration effect."""
        return fault_injector.get_slot_migrate_triggers(effect_name)

    @staticmethod
    def get_topology_change_standalone_triggers(
        fault_injector: FaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
    ) -> Dict[str, Any]:
        """
        Get available triggers(trigger name + db example config) for a
        topology change for standalone client effect.
        """
        return fault_injector.get_topology_change_standalone_triggers(effect_name)

    @staticmethod
    def trigger_effect(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects | TopologyChangeStandaloneEffects,
        trigger_name: Optional[str] = None,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
    ) -> str:
        """Execute fault injector action that will trigger the desired effect.

        Args:
            fault_injector: The fault injector client to use
            endpoint_config: Endpoint configuration dictionary
            effect_name: The effect to trigger (e.g., SlotMigrateEffects enum value)
            trigger_name: Optional trigger/variant name
            source_node: Optional source node ID
            target_node: Optional target node ID
            skip_end_notification: Whether to skip end notification

        Returns:
            str: Action ID for tracking the operation
        """
        return fault_injector.trigger_effect(
            endpoint_config=endpoint_config,
            effect_name=effect_name,
            trigger_name=trigger_name,
            source_node=source_node,
            target_node=target_node,
            skip_end_notification=skip_end_notification,
        )

    @staticmethod
    def migrate(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
    ) -> str:
        """Migrate all master shards of the database to another node.

        Args:
            fault_injector: The fault injector client to use
            endpoint_config: Endpoint configuration dictionary

        Returns:
            str: Action ID for tracking the operation
        """
        return fault_injector.migrate(endpoint_config)

    @staticmethod
    def wait_for_database_active(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        active_timeout: Optional[int] = None,
    ) -> str:
        """Wait server-side until the database reports 'active'.

        Args:
            fault_injector: The fault injector client to use
            endpoint_config: Endpoint configuration dictionary
            active_timeout: Optional seconds to wait before the fault injector
                gives up (its own default applies when omitted)

        Returns:
            str: Action ID for tracking the operation
        """
        return fault_injector.wait_for_database_active(
            endpoint_config, active_timeout=active_timeout
        )


class KeyGenerationHelpers:
    TOTAL_SLOTS = 16384

    @staticmethod
    def redis_crc16(data: bytes) -> int:
        """
        Redis-compatible CRC16 (CRC-CCITT)
        """
        return binascii.crc_hqx(data, 0)

    @staticmethod
    def redis_slot(key: str) -> int:
        """
        Compute Redis Cluster hash slot for a key
        """
        start = key.find("{")
        if start != -1:
            end = key.find("}", start + 1)
            if end != -1 and end > start + 1:
                key = key[start + 1 : end]

        return (
            KeyGenerationHelpers.redis_crc16(key.encode("utf-8"))
            % KeyGenerationHelpers.TOTAL_SLOTS
        )

    @staticmethod
    def generate_key(slot_number: int, prefix: str = "key") -> str:
        """
        Generate a Redis key that hashes to the given slot
        """
        if not (0 <= slot_number < KeyGenerationHelpers.TOTAL_SLOTS):
            raise ValueError("slot_number must be between 0 and 16383")

        i = 0
        while True:
            hashtag = f"{slot_number}-{i}"
            candidate = f"{prefix}:{{{hashtag}}}"
            if KeyGenerationHelpers.redis_slot(candidate) == slot_number:
                return candidate
            i += 1

    @staticmethod
    def generate_keys_for_all_shards(
        shards_count: int, prefix: str = "key", keys_per_shard: int = 1
    ) -> list:
        """
        Generate keys for all shards based on slot ranges.

        Divides the total slots (16384) evenly across shards and generates
        keys for each shard range.

        Args:
            shards_count: Number of shards in the cluster
            prefix: Prefix for generated keys
            keys_per_shard: Number of keys to generate per shard

        Returns:
            List of generated keys distributed across all shards
        """
        keys = []
        slots_per_shard = KeyGenerationHelpers.TOTAL_SLOTS // shards_count

        for shard_index in range(shards_count):
            # Calculate slot range for this shard
            start_slot = shard_index * slots_per_shard

            # Last shard gets any remaining slots
            if shard_index == shards_count - 1:
                end_slot = KeyGenerationHelpers.TOTAL_SLOTS - 1
            else:
                end_slot = start_slot + slots_per_shard - 1

            # Generate keys for this shard's slot range
            span = end_slot - start_slot + 1
            for i in range(keys_per_shard):
                # Spread the keys across the shard's slot range instead of clustering
                # them at its start. Slots one apart almost always migrate together, so
                # adjacent keys never produce the partial move that leaves a per-node
                # PubSub holding some but not all of its channels. Identical to the
                # previous formula at keys_per_shard=1, which every other caller uses.
                slot_number = start_slot + (i * span) // keys_per_shard
                keys.append(KeyGenerationHelpers.generate_key(slot_number, prefix))

        return keys


def is_endpoint_configured_correctly(
    conn,
    configured_endpoint_type: EndpointType,
    fault_injector_client,
) -> bool:
    """Return True if conn's host matches the expected endpoint type.

    Always returns True for mock-proxy injectors — the proxy doesn't
    distinguish endpoint types so there is nothing to validate.
    """
    if isinstance(fault_injector_client, MockProxyFaultInjector):
        return True

    if configured_endpoint_type == EndpointType.NONE:
        if conn.host != conn.orig_host_address:
            logging.debug(
                f"Endpoint check failed: configured NONE but "
                f"host={conn.host!r} != orig_host_address={conn.orig_host_address!r}"
            )
            return False
        return True

    if conn.host == conn.orig_host_address:
        logging.debug(
            f"Endpoint check failed: expected non-NONE endpoint type but "
            f"host={conn.host!r} == orig_host_address={conn.orig_host_address!r}"
        )
        return False
    actual_endpoint_type = conn.maint_notifications_config.get_endpoint_type(
        conn.orig_host_address, conn
    )
    if configured_endpoint_type != actual_endpoint_type:
        logging.debug(
            f"Endpoint check failed: "
            f"configured_endpoint_type={configured_endpoint_type!r} != "
            f"actual_endpoint_type={actual_endpoint_type!r} for host={conn.host!r}"
        )
        return False
    return True


def generate_params(
    fault_injector_client: FaultInjectorClient,
    effect_names: list[SlotMigrateEffects | TopologyChangeStandaloneEffects],
    skip_combinations: list[tuple[SlotMigrateEffects, str]] = [],
    endpoint_types: Optional[list[EndpointType]] = None,
    include_tls: bool = True,
):
    """Build parametrize tuples for maint-notification scenario tests.

    Returns a list of (effect_name, trigger, dbconfig, db_name) tuples, or
    (effect_name, trigger, dbconfig, db_name, endpoint_type) when endpoint_types
    is provided.

    Set include_tls=False to drop the TLS and mTLS requirement variants, for suites
    whose client cannot use a rediss:// endpoint.
    """
    params = []
    try:
        logging.info(f"Extracting params for test with effect_names: {effect_names}")
        for effect_name in effect_names:
            if isinstance(effect_name, SlotMigrateEffects):
                triggers_data = ClusterOperations.get_slot_migrate_triggers(
                    fault_injector_client, effect_name
                )
            else:
                triggers_data = (
                    ClusterOperations.get_topology_change_standalone_triggers(
                        fault_injector_client, effect_name
                    )
                )

            for trigger_info in triggers_data["triggers"]:
                trigger = trigger_info["name"]
                if (effect_name, trigger) in skip_combinations:
                    continue
                if trigger == "maintenance_mode":
                    continue
                trigger_requirements = trigger_info["requirements"]
                for requirement in trigger_requirements:
                    if not include_tls and requirement.get("tls"):
                        continue
                    dbconfig = requirement["dbconfig"]
                    if requirement.get("oss_cluster_api"):
                        ip_type = requirement["oss_cluster_api"]["ip_type"]
                        if ip_type == "internal":
                            continue
                    db_name_pattern = dbconfig.get("name").rsplit("-", 2)[0]
                    dbconfig["name"] = db_name_pattern

                    if endpoint_types is not None:
                        for endpoint_type in endpoint_types:
                            params.append(
                                (
                                    effect_name,
                                    trigger,
                                    dbconfig,
                                    db_name_pattern,
                                    endpoint_type,
                                )
                            )
                    else:
                        params.append((effect_name, trigger, dbconfig, db_name_pattern))
    except NotImplementedError as e:
        # Not every backend implements every effect - the mock proxy server
        # rejects the topology-change triggers outright - so an empty
        # parametrization is the correct outcome rather than a failure.
        logging.info(f"No params for test, unsupported by this fault injector: {e}")
    except Exception as e:
        logging.error(f"Failed to extract params for test: {e}")
        if not params:
            # Partial results are still worth running, but an *empty*
            # parametrization makes pytest report success while having executed
            # nothing at all. A fault injector that is unreachable or rejecting
            # requests has to surface as a collection error instead.
            raise

    return params
