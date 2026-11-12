from abc import ABC, abstractmethod
import json
import logging
import time
import urllib.request
import urllib.error
from typing import Dict, Any, Optional, Union
from enum import Enum

import pytest

from tests.maint_notifications.proxy_server_helpers import (
    ProxyInterceptorHelper,
    SlotsRange,
)

DEFAULT_BDB_ID = 1


class TaskStatuses:
    """Class to hold completed statuses constants."""

    FAILED = "failed"
    FINISHED = "finished"
    SUCCESS = "success"
    RUNNING = "running"

    COMPLETED_STATUSES = [FAILED, FINISHED, SUCCESS]


class ActionType(str, Enum):
    DMC_RESTART = "dmc_restart"
    FAILOVER = "failover"
    NODE_FAILURE = "node_failure"
    PROXY_FAILURE = "proxy_failure"
    RESHARD = "reshard"
    SHARD_FAILURE = "shard_failure"
    CREATE_DATABASE = "create_database"
    DELETE_DATABASE = "delete_database"
    SEQUENCE_OF_ACTIONS = "sequence_of_actions"
    NETWORK_FAILURE = "network_failure"
    EXECUTE_RLUTIL_COMMAND = "execute_rlutil_command"
    EXECUTE_RLADMIN_COMMAND = "execute_rladmin_command"
    MIGRATE = "migrate"
    WAIT_FOR_DATABASE_ACTIVE = "wait_for_database_active"
    SLOT_MIGRATE = "slot_migrate"
    TOPOLOGY_CHANGE_STANDALONE = "topology_change_standalone"


class SlotMigrateEffects(str, Enum):
    REMOVE_ADD = "remove-add"
    REMOVE = "remove"
    ADD = "add"
    SLOT_SHUFFLE = "slot-shuffle"


class TopologyChangeStandaloneEffects(str, Enum):
    DATA_MOVEMENT_CONN_DROP = "data_movement_conn_drop"
    DATA_MOVEMENT_NO_CONN_DROP = "data_movement_no_conn_drop"
    CONN_DROP = "conn_drop"
    DNS_RESOLUTION_CHANGE = "dns_resolution_change"


class RestartDmcParams:
    def __init__(self, bdb_id: str):
        self.bdb_id = bdb_id

    def to_dict(self) -> Dict[str, str]:
        return {"bdb_id": self.bdb_id}


class ActionRequest:
    def __init__(
        self,
        action_type: ActionType,
        parameters: Union[Dict[str, Any], RestartDmcParams],
    ):
        self.type = action_type
        self.parameters = parameters

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type.value,  # Use the string value of the enum
            "parameters": self.parameters.to_dict()
            if isinstance(self.parameters, RestartDmcParams)
            else self.parameters,
        }


class FaultInjectorClient(ABC):
    @abstractmethod
    def get_operation_result(
        self,
        action_id: str,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def create_database(
        self,
        bdb_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def delete_database(
        self,
        bdb_id: int,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def find_database_id_by_name(
        self,
        database_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> Optional[int]:
        pass

    @abstractmethod
    def get_moving_ttl(self) -> int:
        pass

    @abstractmethod
    def get_slot_migrate_triggers(
        self,
        effect_name: SlotMigrateEffects,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_topology_change_standalone_triggers(
        self,
        effect_name: TopologyChangeStandaloneEffects,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def trigger_effect(
        self,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects | TopologyChangeStandaloneEffects,
        trigger_name: Optional[str] = None,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
    ) -> str:
        pass

    @abstractmethod
    def migrate(
        self,
        endpoint_config: Dict[str, Any],
    ) -> str:
        pass

    @abstractmethod
    def wait_for_database_active(
        self,
        endpoint_config: Dict[str, Any],
        active_timeout: Optional[int] = None,
    ) -> str:
        pass


class REFaultInjector(FaultInjectorClient):
    """Fault injector client for Redis Enterprise cluster setup."""

    MOVING_TTL = 15

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._cluster_nodes_info = None
        self._current_db_id = None

    def _make_request(
        self, method: str, path: str, data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json"} if data else {}

        request_data = json.dumps(data).encode("utf-8") if data else None

        request = urllib.request.Request(
            url, method=method, data=request_data, headers=headers
        )

        try:
            with urllib.request.urlopen(request) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 422:
                error_body = json.loads(e.read().decode("utf-8"))
                raise ValueError(f"Validation Error: {error_body}")
            raise

    def trigger_action(self, action_request: ActionRequest) -> Dict[str, Any]:
        """Trigger a new action"""
        request_data = action_request.to_dict()
        return self._make_request("POST", "/action", request_data)

    def get_action_status(self, action_id: str) -> Dict[str, Any]:
        """Get the status of a specific action"""
        return self._make_request("GET", f"/action/{action_id}")

    def execute_rladmin_command(
        self, command: str, bdb_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute rladmin command directly as string"""
        url = f"{self.base_url}/rladmin"

        # The fault injector expects the raw command string
        command_string = f"rladmin {command}"
        if bdb_id:
            command_string = f"rladmin -b {bdb_id} {command}"

        headers = {"Content-Type": "text/plain"}

        request = urllib.request.Request(
            url, method="POST", data=command_string.encode("utf-8"), headers=headers
        )

        try:
            with urllib.request.urlopen(request) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 422:
                error_body = json.loads(e.read().decode("utf-8"))
                raise ValueError(f"Validation Error: {error_body}")
            raise

    def get_operation_result(
        self,
        action_id: str,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """Get the result of a specific action"""
        start_time = time.time()  # returns the time in seconds
        check_interval = 0.3

        while time.time() - start_time < timeout:
            try:
                status_result = self.get_action_status(action_id)
                operation_status = status_result.get("status", "unknown")

                if operation_status in TaskStatuses.COMPLETED_STATUSES:
                    logging.debug(
                        f"Operation {action_id} completed with status: "
                        f"{operation_status}"
                    )
                    if operation_status != TaskStatuses.SUCCESS:
                        pytest.fail(f"Operation {action_id} failed: {status_result}")
                    return status_result

                time.sleep(check_interval)
            except Exception as e:
                logging.warning(f"Error checking operation status: {e}")
                time.sleep(check_interval)
        else:
            pytest.fail(
                f"Timeout waiting for operation {action_id}. Start time: {start_time}, current time: {time.time()}"
            )

    def create_database(
        self,
        bdb_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create a new database."""
        # Please provide the config just for the db that will be created
        logging.debug(f"Creating database with config: {bdb_config}")
        params = {"database_config": bdb_config}
        create_db_action = ActionRequest(
            action_type=ActionType.CREATE_DATABASE,
            parameters=params,
        )
        result = self.trigger_action(create_db_action)
        action_id = result.get("action_id")
        if not action_id:
            raise Exception(f"Failed to trigger create database action: {result}")

        action_status_check_response = self.get_operation_result(action_id)
        logging.debug(f"Create database action result: {action_status_check_response}")

        if action_status_check_response.get("status") != TaskStatuses.SUCCESS:
            raise Exception(
                f"Create database action failed: {action_status_check_response}"
            )

        self._current_db_id = action_status_check_response["output"]["bdb_id"]
        return action_status_check_response["output"]

    def delete_database(
        self,
        bdb_id: int,
    ) -> Dict[str, Any]:
        logging.debug(f"Deleting database with id: {bdb_id}")
        params = {"bdb_id": bdb_id}
        delete_db_action = ActionRequest(
            action_type=ActionType.DELETE_DATABASE,
            parameters=params,
        )
        result = self.trigger_action(delete_db_action)
        action_id = result.get("action_id")
        if not action_id:
            raise Exception(f"Failed to trigger delete database action: {result}")

        action_status_check_response = self.get_operation_result(action_id)

        self._current_db_id = None

        if action_status_check_response.get("status") != TaskStatuses.SUCCESS:
            raise Exception(
                f"Delete database action failed: {action_status_check_response}"
            )
        logging.debug(f"Delete database action result: {action_status_check_response}")
        return action_status_check_response

    def get_cluster_nodes_info(self) -> None:
        """Get cluster nodes information from Redis Enterprise."""
        try:
            # Use rladmin status to get node information
            get_status_action = ActionRequest(
                action_type=ActionType.EXECUTE_RLADMIN_COMMAND,
                parameters={
                    "rladmin_command": "status",
                    "bdb_id": DEFAULT_BDB_ID
                    if self._current_db_id is None
                    else self._current_db_id,  # Any database id will do - it just only needs to exist
                },
            )
            trigger_action_result = self.trigger_action(get_status_action)
            action_id = trigger_action_result.get("action_id")
            if not action_id:
                raise ValueError(
                    f"Failed to trigger get cluster status action: {trigger_action_result}"
                )

            action_status_check_response = self.get_operation_result(action_id)

            if action_status_check_response.get("status") != TaskStatuses.SUCCESS:
                raise Exception(
                    f"Get cluster status action failed: {action_status_check_response}"
                )
            self._cluster_nodes_info = action_status_check_response.get(
                "output", {}
            ).get("output", "")

        except Exception as e:
            pytest.fail(f"Failed to get cluster nodes info: {e}")

    def find_database_id_by_name(
        self,
        database_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> Optional[int]:
        """Find the database ID by name."""
        if self._cluster_nodes_info is None or force_cluster_info_refresh:
            self.get_cluster_nodes_info()

        if not self._cluster_nodes_info:
            raise ValueError("No cluster status info found")

        # Parse the DATABASES section to find the database ID
        lines = self._cluster_nodes_info.split("\n")
        databases_section_started = False

        for line in lines:
            line = line.strip()

            # Start of DATABASES section
            if line.startswith("DATABASES:"):
                databases_section_started = True
                continue
            elif databases_section_started and line and not line.startswith("DB:ID"):
                # Parse database line: db:3 m-standard redis:5 node:3 master 8192-16383 1.79MB OK

                parts = line.split()
                if len(parts) >= 2 and parts[1] == database_name:
                    return int(parts[0].replace("db:", ""))

        raise ValueError(f"Database {database_name} not found")

    def get_slot_migrate_triggers(
        self,
        effect_name: SlotMigrateEffects,
    ) -> Dict[str, Any]:
        """Get available triggers(trigger name + db example config) for a slot migration effect."""
        return self._make_request(
            "GET", f"/slot-migrate?effect={effect_name.value}&cluster_index=0"
        )

    def get_topology_change_standalone_triggers(
        self,
        effect_name: TopologyChangeStandaloneEffects,
    ) -> Dict[str, Any]:
        """Get available triggers(trigger name + db example config) for a topology change for standalone client effect."""
        return self._make_request(
            "GET",
            f"/topology-change-standalone?effect={effect_name.value}&cluster_index=0",
        )

    def trigger_effect(
        self,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects | TopologyChangeStandaloneEffects,
        trigger_name: str,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
    ) -> str:
        """Execute FI action that will trigger the desired effect."""

        # Get bdb_id from endpoint configuration
        bdb_id = endpoint_config.get("bdb_id")
        cluster_index = 0

        try:
            # Correct parameter format for fault injector
            parameters = {
                "bdb_id": bdb_id,
                "cluster_index": cluster_index,
                "effect": effect_name,
                "trigger": trigger_name,
            }
            if source_node:
                parameters["source_node"] = source_node
            if target_node:
                parameters["target_node"] = target_node

            action_type = (
                ActionType.SLOT_MIGRATE
                if isinstance(effect_name, SlotMigrateEffects)
                else ActionType.TOPOLOGY_CHANGE_STANDALONE
            )

            logging.debug(
                f"Executing {action_type.value} with parameters: {parameters}"
            )

            action = ActionRequest(action_type=action_type, parameters=parameters)
            result = self.trigger_action(action)

            logging.debug(f"Trigger effect action result: {result}")

            action_id = result.get("action_id")

            if not action_id:
                raise Exception(f"Failed to trigger slot migrate action: {result}")
            return action_id
        except Exception as e:
            raise Exception(f"Failed to execute slot migrate: {e}")

    def migrate(
        self,
        endpoint_config: Dict[str, Any],
    ) -> str:
        """Migrate all master shards of the database to another node.

        The fault injector selects the target node itself - one holding neither a
        master nor a replica of this database - so the caller never inspects the
        cluster topology.
        """
        action = ActionRequest(
            action_type=ActionType.MIGRATE,
            parameters={
                "bdb_id": endpoint_config.get("bdb_id"),
                "cluster_index": 0,
            },
        )
        result = self.trigger_action(action)
        action_id = result.get("action_id")
        if not action_id:
            raise Exception(f"Failed to trigger migrate action: {result}")
        return action_id

    def wait_for_database_active(
        self,
        endpoint_config: Dict[str, Any],
        active_timeout: Optional[int] = None,
    ) -> str:
        """Wait server-side until the database reports 'active'.

        The failure actions can run the same wait inline via 'wait_for_active'; this
        is the standalone form, for when the fault and the wait are separate calls.
        """
        parameters: Dict[str, Any] = {
            "bdb_id": endpoint_config.get("bdb_id"),
            "cluster_index": 0,
        }
        if active_timeout is not None:
            parameters["active_timeout"] = active_timeout

        action = ActionRequest(
            action_type=ActionType.WAIT_FOR_DATABASE_ACTIVE,
            parameters=parameters,
        )
        result = self.trigger_action(action)
        action_id = result.get("action_id")
        if not action_id:
            raise Exception(
                f"Failed to trigger wait_for_database_active action: {result}"
            )
        return action_id

    def get_moving_ttl(self) -> int:
        return self.MOVING_TTL


class MockProxyFaultInjector:
    """Marker mixin for proxy-server-based fault injectors (sync and async).

    Used to skip endpoint-type validation in tests — the mock proxy doesn't
    distinguish endpoint types, so any isinstance check against this class
    means "we're running against a local proxy, skip the check".
    """


class ProxyServerFaultInjector(MockProxyFaultInjector, FaultInjectorClient):
    """Fault injector client for proxy server setup."""

    NODE_PORT_1 = 15379
    NODE_PORT_2 = 15380
    NODE_PORT_3 = 15381

    DEFAULT_CLUSTER_SLOTS = [
        SlotsRange("127.0.0.1", NODE_PORT_1, 0, 8191),
        SlotsRange("127.0.0.1", NODE_PORT_2, 8192, 16383),
    ]

    CLUSTER_SLOTS_INTERCEPTOR_NAME = "test_topology"

    SLEEP_TIME_BETWEEN_START_END_NOTIFICATIONS = 2
    MOVING_TTL = 4

    def __init__(self, oss_cluster: bool = False):
        self.oss_cluster = oss_cluster
        self.proxy_helper = ProxyInterceptorHelper()

        # set the initial state of the proxy server
        logging.info(
            f"Setting up initial cluster slots -> {self.DEFAULT_CLUSTER_SLOTS}"
        )
        self.proxy_helper.set_cluster_slots(
            self.CLUSTER_SLOTS_INTERCEPTOR_NAME, self.DEFAULT_CLUSTER_SLOTS
        )

    def get_operation_result(
        self,
        action_id: str,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        return {"status": "done"}

    def create_database(
        self,
        bdb_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "bdb_id": 1,
            "username": "default",
            "password": "",
            "tls": False,
            "raw_endpoints": [
                {
                    "addr": ["127.0.0.1"],
                    "addr_type": "external",
                    "dns_name": "localhost",
                    "oss_cluster_api_preferred_endpoint_type": "ip",
                    "oss_cluster_api_preferred_ip_type": "internal",
                    "port": 15379,
                    "proxy_policy": "all-master-shards",
                    "uid": "1:1",
                }
            ],
            "endpoints": ["redis://127.0.0.1:15379"],
        }

    def delete_database(
        self,
        endpoint_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {}

    def find_database_id_by_name(
        self,
        database_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> Optional[int]:
        return 1

    def get_moving_ttl(self) -> int:
        return self.MOVING_TTL

    def get_slot_migrate_triggers(
        self,
        effect_name: SlotMigrateEffects,
    ) -> Dict[str, Any]:
        raise NotImplementedError("Not implemented for proxy server")

    def get_topology_change_standalone_triggers(
        self,
        effect_name: TopologyChangeStandaloneEffects,
    ) -> Dict[str, Any]:
        raise NotImplementedError("Not implemented for proxy server")

    def trigger_effect(
        self,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects | TopologyChangeStandaloneEffects,
        trigger_name: Optional[str] = None,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
    ) -> str:
        """
        Trigger the desired effect. For the proxy server,
        this will need to be implemented in next iterations.
        """
        raise NotImplementedError("Not implemented for proxy server")

    def migrate(
        self,
        endpoint_config: Dict[str, Any],
    ) -> str:
        raise NotImplementedError("Not implemented for proxy server")

    def wait_for_database_active(
        self,
        endpoint_config: Dict[str, Any],
        active_timeout: Optional[int] = None,
    ) -> str:
        raise NotImplementedError("Not implemented for proxy server")
