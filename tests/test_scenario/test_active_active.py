import json
import logging
import os
import threading
from time import monotonic, sleep
from typing import Optional
from urllib.parse import urlparse

import pytest

from redis import Redis, RedisCluster
from redis.backoff import ConstantBackoff
from redis.client import Pipeline
from redis.maint_notifications import MaintenanceState, MaintNotificationsConfig
from redis.multidb.circuit import State as CBState
from redis.multidb.exception import TemporaryUnavailableException
from redis.multidb.failover import DEFAULT_FAILOVER_ATTEMPTS, DEFAULT_FAILOVER_DELAY
from redis.asyncio.multidb.healthcheck import LagAwareHealthCheck
from redis.retry import Retry
from redis.utils import dummy_fail
from tests.test_scenario.conftest import RELAXED_TIMEOUT, use_mock_proxy
from tests.test_scenario.fault_injector_client import (
    ActionRequest,
    ActionType,
    TopologyChangeStandaloneEffects,
)

logger = logging.getLogger(__name__)

# The injected network failure is transient - the fault injector restores the link a
# few seconds after the action is triggered. A database is only taken out of service by
# a health check probe that runs while the link is down, and on the default interval a
# probe round is short next to the pause that follows it, so most rounds land after the
# link is already back and no failover is ever initiated. Probing back to back keeps a
# round inside the outage.
FAILOVER_HEALTH_CHECK_INTERVAL = 0.1
# Bounded here rather than left to pytest-timeout so a failover that never happens is
# reported as a failover that never happened, instead of as a stack dump of whatever
# the test was doing when the deadline passed. Kept well inside the per-test timeout so
# the assertion below is what fires.
FAILOVER_TIMEOUT = 60
FAILOVER_TIMEOUT_MESSAGE = (
    f"Active database has not changed within {FAILOVER_TIMEOUT} seconds of the "
    "injected network failure"
)
# The Redis Enterprise REST API credentials LagAwareHealthCheck authenticates with.
# They come from the test environment, and without them every probe gets HTTP 401 and
# both databases are reported unhealthy - which fails the initial health check instead
# of exercising the health check the test is about.
LAG_AWARE_CREDENTIAL_ENV_VARS = ("ENV0_USERNAME", "ENV0_PASSWORD")
# The whole health check - every probe of it - has to finish inside this budget, and
# each probe of this one is two REST calls to the Redis Enterprise API. The default of
# 3 seconds covers a PING, not 3 probes x 2 requests over the public internet with a
# second of it spent in the delay between probes, and running out of it reports the
# database as unhealthy.
LAG_AWARE_HEALTH_CHECK_TIMEOUT = 10


def lag_aware_auth_basic():
    """
    Return the REST API credentials for LagAwareHealthCheck as the environment
    supplies them.
    """
    return tuple(os.getenv(name) for name in LAG_AWARE_CREDENTIAL_ENV_VARS)


def require_lag_aware_credentials():
    """
    Fail the calling test up front when the environment does not supply the REST API
    credentials.

    Deliberately a failure and not a skip: a skipped test is invisible in a scenario
    run's log, and the alternative is every probe returning HTTP 401 and the run
    reporting InitialHealthCheckFailedError, which reads like a client bug.

    Kept apart from lag_aware_auth_basic because the async twin reads the credentials
    at collection time, where failing the individual test is not available.
    """
    missing = ", ".join(
        name for name in LAG_AWARE_CREDENTIAL_ENV_VARS if not os.getenv(name)
    )

    if missing:
        pytest.fail(
            "LagAwareHealthCheck requires the Redis Enterprise REST API credentials "
            f"in {missing}. Set them from the CI secrets of the same name."
        )


def trigger_network_failure_action(
    fault_injector_client,
    config,
    event: Optional[threading.Event] = None,
    failures: Optional[list] = None,
):
    """Inject a network failure on the cluster of the active database and wait for it.

    Meant for a spawned thread: get_operation_result reports a failed or timed-out
    action through pytest.fail, which on another thread only kills that thread. The
    failure is recorded into ``failures`` for the test thread to assert on, and
    ``event`` is set whatever happened, so the test's wait for the injection always
    ends instead of running into its timeout.
    """
    action_request = ActionRequest(
        action_type=ActionType.NETWORK_FAILURE,
        parameters={"bdb_id": config["bdb_id"], "delay": 3, "cluster_index": 0},
    )

    try:
        result = fault_injector_client.trigger_action(action_request)
        status_result = fault_injector_client.get_operation_result(result["action_id"])
        logger.info(f"Action completed. Status: {status_result['status']}")
    except BaseException as e:
        logger.error(f"Network failure injection failed: {type(e).__name__}: {e}")
        if failures is not None:
            failures.append(f"{type(e).__name__}: {e}")
    finally:
        if event:
            event.set()


# The planned maintenance below is a shard migration, optionally followed by an
# endpoint rebind, run by the fault injector on the cluster of the active database. It
# takes as long as the shards take to move, so it gets the wait the maintenance
# notification tests give the same effect triggers.
PLANNED_MAINTENANCE_TIMEOUT = 180
PLANNED_MAINTENANCE_TIMEOUT_MESSAGE = (
    f"Planned maintenance has not completed within {PLANNED_MAINTENANCE_TIMEOUT} "
    "seconds"
)
# A push notification is only processed when the client reads from the connection it
# arrived on, so the command loop is what delivers the notifications. The MOVING state
# stays on the pool for the TTL of the notification, and the loop has to sample it
# inside that window.
MAINTENANCE_COMMAND_INTERVAL = 0.2
# How long past the MOVING TTL the client keeps being exercised after the fault
# injector reports the maintenance complete: by then the handoff has been reverted and
# the pool is expected to be back in its default state.
POST_MAINTENANCE_MARGIN = 10
NO_FAILOVER_MESSAGE = (
    "Active database changed during planned maintenance: the underlying client is "
    "expected to hand off inside its own cluster, without a geo failover"
)
# The maintenance moves the shards of the active database to another node of its
# cluster and rebinds the endpoint there, so the client receives MIGRATING/MIGRATED
# followed by MOVING and reconnects to the new node. The migrate-only trigger of the
# same effect family is deliberately not here: it leaves the endpoint on the old node
# while the shard lives on the new one, and the network failure the other tests of
# this module inject targets the node hosting the shard - the client would keep
# talking to an undisturbed proxy and those tests would stop failing over.
PLANNED_MAINTENANCE_SCENARIOS = [
    pytest.param(
        TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP,
        "endpoint_rebind",
        id="endpoint_rebind",
    ),
]
# Passed to the underlying clients through DatabaseConfig.client_kwargs, which is the
# only way to enable the notifications in a MultiDBClient - MultiDbConfig disables
# them otherwise. The relaxed timeout is what MIGRATING applies to the connections
# for the duration of the maintenance.
MAINT_NOTIFICATIONS_ENABLED = MaintNotificationsConfig(
    enabled=True, relaxed_timeout=RELAXED_TIMEOUT
)
# The r_multi_db parameters shared by the maintenance notification tests. The
# failure threshold is as low as in the failover tests, so an error the handoff
# leaks to the client would show up as a failover; the health check interval is the
# same for the same reason. The health check timeout keeps its default of 3 seconds
# on purpose: a single health check running out of it opens the database's circuit
# no matter the failure threshold, and the shard switch can stall a PING for longer
# than that. The health check client is built from the database client's connection
# kwargs and receives the same MIGRATING notification, which relaxes both its socket
# timeout and the health check budget for the duration of the maintenance - so the
# planned maintenance test passing with the default budget is what proves the
# relaxation, while the unplanned failure test relies on the same default: under
# the network failure it injects, no notification arrives and the health check
# timing out is what fails the database over.
MAINT_NOTIFICATIONS_MULTI_DB_PARAMS = {
    "client_class": Redis,
    "min_num_failures": 2,
    "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
    "maint_notifications_config": MAINT_NOTIFICATIONS_ENABLED,
}


def trigger_planned_maintenance_action(
    fault_injector_client,
    config,
    effect: TopologyChangeStandaloneEffects,
    trigger: str,
    event: threading.Event,
    failures: list,
    results: list,
):
    """Run a planned maintenance on the cluster of the active database and wait for it.

    Meant for a spawned thread: get_operation_result reports a failed or timed-out
    action through pytest.fail, which on another thread only kills that thread. The
    outcome is recorded into ``results`` / ``failures`` for the test thread to assert
    on, and ``event`` is set whatever happened, so the test's wait for the maintenance
    always ends.
    """
    try:
        action_id = fault_injector_client.trigger_effect(config, effect, trigger)
        results.append(
            fault_injector_client.get_operation_result(
                action_id, timeout=PLANNED_MAINTENANCE_TIMEOUT
            )
        )
        logger.info(f"Planned maintenance completed: {results[-1]}")
    except BaseException as e:
        logger.error(f"Planned maintenance failed: {type(e).__name__}: {e}")
        failures.append(f"{type(e).__name__}: {e}")
    finally:
        event.set()


def observe_maintenance_states(r_multi_db) -> set:
    """Sample the maintenance states of the active database's connection pool.

    MOVING is applied to the whole pool through its connection kwargs for the TTL of
    the notification; MIGRATING/MIGRATED only mark the connection they arrive on.
    Both are read so a test can tell a handoff that happened from one that never did.
    """
    pool = r_multi_db.command_executor.active_database.client.connection_pool
    states = {pool.connection_kwargs.get("maintenance_state", MaintenanceState.NONE)}

    with pool._lock:
        for conn in pool._get_free_connections():
            states.add(conn.maintenance_state)
        for conn in pool._get_in_use_connections():
            states.add(conn.maintenance_state)

    return states


def observe_health_check_maintenance_states(r_multi_db) -> set:
    """Sample the maintenance states of the health check clients' connection pools.

    The health checks probe through their own clients, built from the databases'
    connection kwargs, so a maintenance notification a probe receives shows up on
    those pools - the only place it can show up while the client itself sends no
    commands. The pools live on the health check loop, so a collection changing
    under the iteration is skipped and picked up on the next sample.
    """
    states = set()

    for client in list(r_multi_db._health_check_policy._clients.values()):
        pool = client.connection_pool
        states.add(
            pool.connection_kwargs.get("maintenance_state", MaintenanceState.NONE)
        )
        try:
            connections = [
                *pool._get_free_connections(),
                *pool._get_in_use_connections(),
            ]
        except RuntimeError:
            continue
        for conn in connections:
            states.add(conn.maintenance_state)

    return states


def assert_circuits_closed(databases):
    """Assert no health check has opened a circuit."""
    for database in databases:
        assert database.circuit.state == CBState.CLOSED, database


def assert_no_failover(r_multi_db, listener, config):
    """Assert the client stayed on its initial database with every circuit closed."""
    assert not listener.is_changed_flag, NO_FAILOVER_MESSAGE

    # MOVING rewrites `host` for the TTL of the notification; the original address is
    # what tells which cluster the client is on.
    connection_kwargs = (
        r_multi_db.command_executor.active_database.client.get_connection_kwargs()
    )
    active_host = connection_kwargs.get(
        "orig_host_address", connection_kwargs.get("host")
    )
    assert active_host == urlparse(config["endpoints"][0]).hostname, NO_FAILOVER_MESSAGE

    for database, _ in r_multi_db.get_databases():
        assert database.circuit.state == CBState.CLOSED, database


def assert_pool_in_default_state(r_multi_db):
    """Assert the handoff has been reverted on the active database's pool."""
    pool = r_multi_db.command_executor.active_database.client.connection_pool

    assert (
        pool.connection_kwargs.get("maintenance_state", MaintenanceState.NONE)
        == MaintenanceState.NONE
    )

    with pool._lock:
        connections = [
            *pool._get_free_connections(),
            *pool._get_in_use_connections(),
        ]

    for conn in connections:
        assert conn.maintenance_state == MaintenanceState.NONE, conn
        assert conn.host == conn.orig_host_address, conn


class TestActiveActive:
    # No teardown wait for the cluster to recover from the injected network failure: the
    # r_multi_db fixture polls the endpoints until they answer before building the next
    # test's client, which is the condition a blind sleep was approximating. Every test
    # here only advances once the fault injector reports its action complete, so nothing
    # is left in flight for the poll to race.

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_multi_db_client_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        r_multi_db, listener, config = r_multi_db

        # Handle unavailable databases from previous test.
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        failures = []
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, failures),
        )

        # Client initialized on the first command.
        retry.call_with_retry(
            lambda: r_multi_db.set("key", "value"), lambda _: dummy_fail()
        )
        thread.start()

        # Execute commands before network failure
        while not event.is_set():
            assert (
                retry.call_with_retry(
                    lambda: r_multi_db.get("key"), lambda _: dummy_fail()
                )
                == "value"
            )
            sleep(0.5)

        thread.join()
        assert not failures, f"Network failure injection failed: {'; '.join(failures)}"

        # Execute commands until database failover
        deadline = monotonic() + FAILOVER_TIMEOUT
        while not listener.is_changed_flag:
            assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
            assert (
                retry.call_with_retry(
                    lambda: r_multi_db.get("key"), lambda _: dummy_fail()
                )
                == "value"
            )
            sleep(0.5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_multi_db_client_uses_lag_aware_health_check(
        self, r_multi_db, fault_injector_client
    ):
        require_lag_aware_credentials()

        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        failures = []
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, failures),
        )

        # Adding additional health check to the client.
        r_multi_db.add_health_check(
            LagAwareHealthCheck(
                verify_tls=False,
                auth_basic=lag_aware_auth_basic(),
                lag_aware_tolerance=10000,
                health_check_timeout=LAG_AWARE_HEALTH_CHECK_TIMEOUT,
            )
        )

        # Client initialized on the first command.
        retry.call_with_retry(
            lambda: r_multi_db.set("key", "value"), lambda _: dummy_fail()
        )
        thread.start()

        # Execute commands before network failure
        while not event.is_set():
            assert (
                retry.call_with_retry(
                    lambda: r_multi_db.get("key"), lambda _: dummy_fail()
                )
                == "value"
            )
            sleep(0.5)

        thread.join()
        assert not failures, f"Network failure injection failed: {'; '.join(failures)}"

        # Execute commands after network failure
        deadline = monotonic() + FAILOVER_TIMEOUT
        while not listener.is_changed_flag:
            assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
            assert (
                retry.call_with_retry(
                    lambda: r_multi_db.get("key"), lambda _: dummy_fail()
                )
                == "value"
            )
            sleep(0.5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_context_manager_pipeline_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        failures = []
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, failures),
        )

        def callback():
            with r_multi_db.pipeline() as pipe:
                pipe.set("{hash}key1", "value1")
                pipe.set("{hash}key2", "value2")
                pipe.set("{hash}key3", "value3")
                pipe.get("{hash}key1")
                pipe.get("{hash}key2")
                pipe.get("{hash}key3")
                assert pipe.execute() == [
                    True,
                    True,
                    True,
                    "value1",
                    "value2",
                    "value3",
                ]

        # Client initialized on first pipe execution.
        retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
        thread.start()

        # Execute pipeline before network failure
        while not event.is_set():
            retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
            sleep(0.5)

        thread.join()
        assert not failures, f"Network failure injection failed: {'; '.join(failures)}"

        # Execute pipeline until database failover
        deadline = monotonic() + FAILOVER_TIMEOUT
        while not listener.is_changed_flag:
            assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
            retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
            sleep(0.5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_chaining_pipeline_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        failures = []
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, failures),
        )

        def callback():
            pipe = r_multi_db.pipeline()
            pipe.set("{hash}key1", "value1")
            pipe.set("{hash}key2", "value2")
            pipe.set("{hash}key3", "value3")
            pipe.get("{hash}key1")
            pipe.get("{hash}key2")
            pipe.get("{hash}key3")
            assert pipe.execute() == [True, True, True, "value1", "value2", "value3"]

        # Client initialized on first pipe execution.
        retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())

        thread.start()

        # Execute pipeline before network failure
        while not event.is_set():
            retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
            sleep(0.5)

        thread.join()
        assert not failures, f"Network failure injection failed: {'; '.join(failures)}"

        # Execute pipeline until database failover
        deadline = monotonic() + FAILOVER_TIMEOUT
        while not listener.is_changed_flag:
            assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
            retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
            sleep(0.5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_transaction_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        failures = []
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, failures),
        )

        def callback(pipe: Pipeline):
            pipe.set("{hash}key1", "value1")
            pipe.set("{hash}key2", "value2")
            pipe.set("{hash}key3", "value3")
            pipe.get("{hash}key1")
            pipe.get("{hash}key2")
            pipe.get("{hash}key3")

        # Client initialized on first transaction execution.
        retry.call_with_retry(
            lambda: r_multi_db.transaction(callback), lambda _: dummy_fail()
        )
        thread.start()

        # Execute transaction before network failure
        while not event.is_set():
            retry.call_with_retry(
                lambda: r_multi_db.transaction(callback), lambda _: dummy_fail()
            )
            sleep(0.5)

        thread.join()
        assert not failures, f"Network failure injection failed: {'; '.join(failures)}"

        # Execute transaction until database failover
        deadline = monotonic() + FAILOVER_TIMEOUT
        while not listener.is_changed_flag:
            assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
            retry.call_with_retry(
                lambda: r_multi_db.transaction(callback), lambda _: dummy_fail()
            )
            sleep(0.5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_pubsub_failover_to_another_db(self, r_multi_db, fault_injector_client):
        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        failures = []
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, failures),
        )
        data = json.dumps({"message": "test"})
        messages_count = 0

        def handler(message):
            nonlocal messages_count
            messages_count += 1

        pubsub = r_multi_db.pubsub()

        # Assign a handler and run in a separate thread.
        retry.call_with_retry(
            lambda: pubsub.subscribe(**{"test-channel": handler}),
            lambda _: dummy_fail(),
        )
        pubsub_thread = pubsub.run_in_thread(sleep_time=0.1, daemon=True)
        thread.start()

        # Execute publish before network failure
        while not event.is_set():
            retry.call_with_retry(
                lambda: r_multi_db.publish("test-channel", data), lambda _: dummy_fail()
            )
            sleep(0.5)

        thread.join()
        assert not failures, f"Network failure injection failed: {'; '.join(failures)}"

        # Execute publish until database failover
        deadline = monotonic() + FAILOVER_TIMEOUT
        while not listener.is_changed_flag:
            assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
            retry.call_with_retry(
                lambda: r_multi_db.publish("test-channel", data), lambda _: dummy_fail()
            )
            sleep(0.5)

        pubsub_thread.stop()
        assert messages_count > 2

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_sharded_pubsub_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        failures = []
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, failures),
        )
        data = json.dumps({"message": "test"})
        messages_count = 0

        def handler(message):
            nonlocal messages_count
            messages_count += 1

        pubsub = r_multi_db.pubsub()

        # Assign a handler and run in a separate thread.
        retry.call_with_retry(
            lambda: pubsub.ssubscribe(**{"test-channel": handler}),
            lambda _: dummy_fail(),
        )
        pubsub_thread = pubsub.run_in_thread(
            sleep_time=0.1, daemon=True, sharded_pubsub=True
        )
        thread.start()

        # Execute publish before network failure
        while not event.is_set():
            retry.call_with_retry(
                lambda: r_multi_db.spublish("test-channel", data),
                lambda _: dummy_fail(),
            )
            sleep(0.5)

        thread.join()
        assert not failures, f"Network failure injection failed: {'; '.join(failures)}"

        # Execute publish until database failover
        deadline = monotonic() + FAILOVER_TIMEOUT
        while not listener.is_changed_flag:
            assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
            retry.call_with_retry(
                lambda: r_multi_db.spublish("test-channel", data),
                lambda _: dummy_fail(),
            )
            sleep(0.5)

        pubsub_thread.stop()
        assert messages_count > 2

    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support topology change effects.",
    )
    @pytest.mark.parametrize("effect, trigger", PLANNED_MAINTENANCE_SCENARIOS)
    @pytest.mark.parametrize(
        "r_multi_db",
        [MAINT_NOTIFICATIONS_MULTI_DB_PARAMS],
        ids=["standalone"],
        indirect=True,
    )
    @pytest.mark.timeout(300)
    def test_multi_db_client_no_failover_during_planned_maintenance(
        self, r_multi_db, fault_injector_client, effect, trigger
    ):
        """
        With maintenance notifications enabled, a planned maintenance on the cluster of
        the active database is handled by the underlying client - it follows the
        MIGRATING/MOVING notifications inside that cluster - and never turns into a
        geo failover to the other database.
        """
        r_multi_db, listener, config = r_multi_db

        # Handle unavailable databases from previous test.
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        # Client initialized on the first command.
        retry.call_with_retry(
            lambda: r_multi_db.set("key", "value"), lambda _: dummy_fail()
        )

        event = threading.Event()
        failures = []
        results = []
        thread = threading.Thread(
            target=trigger_planned_maintenance_action,
            daemon=True,
            args=(
                fault_injector_client,
                config,
                effect,
                trigger,
                event,
                failures,
                results,
            ),
        )
        thread.start()

        # The commands run without the retry above on purpose: the handoff is
        # expected to be transparent, so an error that reaches this loop is a
        # failure of the test, not something to retry through.
        observed_states = set()
        deadline = monotonic() + PLANNED_MAINTENANCE_TIMEOUT
        while not event.is_set():
            assert monotonic() < deadline, PLANNED_MAINTENANCE_TIMEOUT_MESSAGE
            assert r_multi_db.get("key") == "value"
            observed_states |= observe_maintenance_states(r_multi_db)
            assert not listener.is_changed_flag, NO_FAILOVER_MESSAGE
            sleep(MAINTENANCE_COMMAND_INTERVAL)

        thread.join()
        assert not failures, f"Planned maintenance failed: {'; '.join(failures)}"

        # Keep going past the MOVING TTL, so the handoff is observed and then reverted
        # while the client is in use.
        settle_deadline = (
            monotonic()
            + fault_injector_client.get_moving_ttl()
            + POST_MAINTENANCE_MARGIN
        )
        while monotonic() < settle_deadline:
            assert r_multi_db.get("key") == "value"
            observed_states |= observe_maintenance_states(r_multi_db)
            assert not listener.is_changed_flag, NO_FAILOVER_MESSAGE
            sleep(MAINTENANCE_COMMAND_INTERVAL)

        # Server-side proof that the shards did move, so the test cannot pass on a
        # maintenance that never happened.
        output = results[0]["output"]
        assert output["source_node"] != output["target_node"], output

        # The endpoint rebind is what MOVING announces, and it is on the pool for the
        # whole TTL, so not seeing it means the notification never arrived.
        logger.info(f"Maintenance states observed on the pool: {observed_states}")
        assert MaintenanceState.MOVING in observed_states, (
            f"No MOVING notification observed during {trigger}: {observed_states}"
        )

        assert_no_failover(r_multi_db, listener, config)
        assert_pool_in_default_state(r_multi_db)

    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support topology change effects.",
    )
    @pytest.mark.parametrize("effect, trigger", PLANNED_MAINTENANCE_SCENARIOS)
    @pytest.mark.parametrize(
        "r_multi_db",
        [MAINT_NOTIFICATIONS_MULTI_DB_PARAMS],
        ids=["standalone"],
        indirect=True,
    )
    @pytest.mark.timeout(300)
    def test_health_checks_keep_circuits_closed_during_planned_maintenance(
        self, r_multi_db, fault_injector_client, effect, trigger
    ):
        """
        The health checks on their own never open a circuit during a planned
        maintenance. The client sends no commands while the maintenance runs, so the
        health checks are the only traffic to the databases: a circuit opening here
        could only come from a health check running out of its budget, never from an
        error a command leaked. This isolates the health check budget from the
        connection handling the other planned maintenance test exercises.
        """
        r_multi_db, listener, config = r_multi_db

        # Handle unavailable databases from previous test.
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        # Client initialized on the first command, which also starts the recurring
        # health checks. The last command until the maintenance has settled.
        retry.call_with_retry(
            lambda: r_multi_db.set("key", "value"), lambda _: dummy_fail()
        )
        databases = [database for database, _ in r_multi_db.get_databases()]

        event = threading.Event()
        failures = []
        results = []
        thread = threading.Thread(
            target=trigger_planned_maintenance_action,
            daemon=True,
            args=(
                fault_injector_client,
                config,
                effect,
                trigger,
                event,
                failures,
                results,
            ),
        )
        thread.start()

        # Only the health checks talk to the databases from here on; the test
        # watches the circuits they drive.
        observed_states = set()
        deadline = monotonic() + PLANNED_MAINTENANCE_TIMEOUT
        while not event.is_set():
            assert monotonic() < deadline, PLANNED_MAINTENANCE_TIMEOUT_MESSAGE
            observed_states |= observe_health_check_maintenance_states(r_multi_db)
            assert_circuits_closed(databases)
            assert not listener.is_changed_flag, NO_FAILOVER_MESSAGE
            sleep(MAINTENANCE_COMMAND_INTERVAL)

        thread.join()
        assert not failures, f"Planned maintenance failed: {'; '.join(failures)}"

        # Keep watching past the MOVING TTL: the health checks run through the
        # handoff and its revert as well.
        settle_deadline = (
            monotonic()
            + fault_injector_client.get_moving_ttl()
            + POST_MAINTENANCE_MARGIN
        )
        while monotonic() < settle_deadline:
            observed_states |= observe_health_check_maintenance_states(r_multi_db)
            assert_circuits_closed(databases)
            assert not listener.is_changed_flag, NO_FAILOVER_MESSAGE
            sleep(MAINTENANCE_COMMAND_INTERVAL)

        # Server-side proof that the shards did move, so the test cannot pass on a
        # maintenance that never happened.
        output = results[0]["output"]
        assert output["source_node"] != output["target_node"], output

        # The health checks must have probed through the maintenance for the test
        # to prove anything: the handoff is on their pools for the whole TTL, so not
        # seeing it means the notification never reached a probe.
        logger.info(
            f"Maintenance states observed on the probe pools: {observed_states}"
        )
        assert MaintenanceState.MOVING in observed_states, (
            f"No MOVING notification observed by the health checks during {trigger}: "
            f"{observed_states}"
        )

        # Traffic resumes on the initial database. The idle connection has the
        # maintenance's notifications, and the server's close, waiting in its
        # buffer; the pool must discard them and reconnect rather than surface
        # a connection error the failure detector would count.
        assert r_multi_db.get("key") == "value"
        assert_no_failover(r_multi_db, listener, config)

    @pytest.mark.parametrize(
        "r_multi_db",
        [MAINT_NOTIFICATIONS_MULTI_DB_PARAMS],
        ids=["standalone"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_multi_db_client_failover_on_unplanned_failure_with_maint_notifications(
        self, r_multi_db, fault_injector_client
    ):
        """
        Maintenance notifications only cover planned maintenance: an unplanned failure
        of the active database's cluster still fails the client over to the other one.
        """
        r_multi_db, listener, config = r_multi_db

        # Handle unavailable databases from previous test.
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        failures = []
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, failures),
        )

        # Client initialized on the first command.
        retry.call_with_retry(
            lambda: r_multi_db.set("key", "value"), lambda _: dummy_fail()
        )
        thread.start()

        # Execute commands before network failure
        while not event.is_set():
            assert (
                retry.call_with_retry(
                    lambda: r_multi_db.get("key"), lambda _: dummy_fail()
                )
                == "value"
            )
            sleep(0.5)

        thread.join()
        assert not failures, f"Network failure injection failed: {'; '.join(failures)}"

        # Execute commands until database failover
        deadline = monotonic() + FAILOVER_TIMEOUT
        while not listener.is_changed_flag:
            assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
            assert (
                retry.call_with_retry(
                    lambda: r_multi_db.get("key"), lambda _: dummy_fail()
                )
                == "value"
            )
            sleep(0.5)
