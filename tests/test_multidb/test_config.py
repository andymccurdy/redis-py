from unittest.mock import Mock

import pytest

from redis._defaults import DEFAULT_RETRY_COUNT
from redis.backoff import NoBackoff
from redis.connection import ConnectionPool
from redis.maint_notifications import MaintNotificationsConfig
from redis.multidb.circuit import (
    PBCircuitBreakerAdapter,
    CircuitBreaker,
    DEFAULT_GRACE_PERIOD,
)
from redis.multidb.config import (
    MultiDbConfig,
    DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_AUTO_FALLBACK_INTERVAL,
    DatabaseConfig,
)
from redis.multidb.database import Database
from redis.multidb.failure_detector import CommandFailureDetector, FailureDetector
from redis.asyncio.multidb.healthcheck import PingHealthCheck, HealthCheck
from redis.multidb.failover import WeightBasedFailoverStrategy, FailoverStrategy
from redis.retry import Retry


@pytest.mark.fixed_client
class TestMultiDbConfig:
    def test_default_config(self):
        db_configs = [
            DatabaseConfig(
                client_kwargs={"host": "host1", "port": "port1"}, weight=1.0
            ),
            DatabaseConfig(
                client_kwargs={"host": "host2", "port": "port2"}, weight=0.9
            ),
            DatabaseConfig(
                client_kwargs={"host": "host3", "port": "port3"}, weight=0.8
            ),
        ]

        config = MultiDbConfig(databases_config=db_configs)

        assert config.databases_config == db_configs
        databases = config.databases()
        assert len(databases) == 3

        i = 0
        for db, weight in databases:
            assert isinstance(db, Database)
            assert weight == db_configs[i].weight
            assert db.circuit.grace_period == DEFAULT_GRACE_PERIOD
            assert db.client.get_retry() is not config.command_retry
            i += 1

        assert len(config.default_failure_detectors()) == 1
        assert isinstance(config.default_failure_detectors()[0], CommandFailureDetector)
        assert len(config.default_health_checks()) == 1
        assert isinstance(config.default_health_checks()[0], PingHealthCheck)
        assert config.health_check_interval == DEFAULT_HEALTH_CHECK_INTERVAL
        assert isinstance(
            config.default_failover_strategy(), WeightBasedFailoverStrategy
        )
        assert config.auto_fallback_interval == DEFAULT_AUTO_FALLBACK_INTERVAL
        assert isinstance(config.command_retry, Retry)

    def test_default_command_retry_is_isolated(self):
        first = MultiDbConfig(databases_config=[])
        second = MultiDbConfig(databases_config=[])

        assert first.command_retry is not second.command_retry
        assert first.command_retry._backoff is not second.command_retry._backoff
        assert first.command_retry == second.command_retry

        first.command_retry.update_retries(0)
        first.command_retry.update_supported_errors((ValueError,))

        for config in (second, MultiDbConfig(databases_config=[])):
            assert config.command_retry.get_retries() == DEFAULT_RETRY_COUNT
            assert ValueError not in config.command_retry._supported_errors

    def test_explicit_command_retry_is_preserved(self):
        retry = Retry(backoff=NoBackoff(), retries=2)
        config = MultiDbConfig(databases_config=[], command_retry=retry)

        assert config.command_retry is retry
        assert config.command_retry.get_retries() == 2

    def test_overridden_config(self):
        grace_period = 2
        mock_connection_pools = [
            Mock(spec=ConnectionPool),
            Mock(spec=ConnectionPool),
            Mock(spec=ConnectionPool),
        ]
        mock_connection_pools[0].connection_kwargs = {}
        mock_connection_pools[1].connection_kwargs = {}
        mock_connection_pools[2].connection_kwargs = {}
        mock_cb1 = Mock(spec=CircuitBreaker)
        mock_cb1.grace_period = grace_period
        mock_cb2 = Mock(spec=CircuitBreaker)
        mock_cb2.grace_period = grace_period
        mock_cb3 = Mock(spec=CircuitBreaker)
        mock_cb3.grace_period = grace_period
        mock_failure_detectors = [
            Mock(spec=FailureDetector),
            Mock(spec=FailureDetector),
        ]
        mock_health_checks = [Mock(spec=HealthCheck), Mock(spec=HealthCheck)]
        health_check_interval = 10
        mock_failover_strategy = Mock(spec=FailoverStrategy)
        auto_fallback_interval = 10
        db_configs = [
            DatabaseConfig(
                client_kwargs={"connection_pool": mock_connection_pools[0]},
                weight=1.0,
                circuit=mock_cb1,
            ),
            DatabaseConfig(
                client_kwargs={"connection_pool": mock_connection_pools[1]},
                weight=0.9,
                circuit=mock_cb2,
            ),
            DatabaseConfig(
                client_kwargs={"connection_pool": mock_connection_pools[2]},
                weight=0.8,
                circuit=mock_cb3,
            ),
        ]

        config = MultiDbConfig(
            databases_config=db_configs,
            failure_detectors=mock_failure_detectors,
            health_checks=mock_health_checks,
            health_check_interval=health_check_interval,
            failover_strategy=mock_failover_strategy,
            auto_fallback_interval=auto_fallback_interval,
        )

        assert config.databases_config == db_configs
        databases = config.databases()
        assert len(databases) == 3

        i = 0
        for db, weight in databases:
            assert isinstance(db, Database)
            assert weight == db_configs[i].weight
            assert db.client.connection_pool == mock_connection_pools[i]
            assert db.circuit.grace_period == grace_period
            i += 1

        assert len(config.failure_detectors) == 2
        assert config.failure_detectors[0] == mock_failure_detectors[0]
        assert config.failure_detectors[1] == mock_failure_detectors[1]
        assert len(config.health_checks) == 2
        assert config.health_checks[0] == mock_health_checks[0]
        assert config.health_checks[1] == mock_health_checks[1]
        assert config.health_check_interval == health_check_interval
        assert config.failover_strategy == mock_failover_strategy
        assert config.auto_fallback_interval == auto_fallback_interval

    def test_underlying_clients_have_disabled_retry_and_maint_notifications(self):
        """
        Test that underlying clients have retry disabled (0 retries)
        and maintenance notifications disabled.
        """
        db_configs = [
            DatabaseConfig(
                client_kwargs={"host": "host1", "port": "port1"},
                weight=1.0,
            ),
            DatabaseConfig(
                client_kwargs={"host": "host2", "port": "port2"},
                weight=0.9,
            ),
        ]

        config = MultiDbConfig(databases_config=db_configs)
        databases = config.databases()

        assert len(databases) == 2

        for db, weight in databases:
            # Verify retry is disabled (0 retries)
            retry = db.client.get_retry()
            assert retry is not None
            assert retry.get_retries() == 0

            # Verify maint_notifications_config is disabled
            # When maint_notifications_config.enabled is False, the pool handler is None
            pool = db.client.connection_pool
            assert pool._maint_notifications_pool_handler is None

    def test_user_provided_maint_notifications_config_is_respected(self):
        """
        Test that user-provided maint_notifications_config is not overwritten.
        """
        user_maint_config = MaintNotificationsConfig(enabled=True)
        db_configs = [
            DatabaseConfig(
                client_kwargs={
                    "host": "host1",
                    "port": "port1",
                    "protocol": 3,  # Required for maint notifications
                    "maint_notifications_config": user_maint_config,
                },
                weight=1.0,
            ),
        ]

        config = MultiDbConfig(databases_config=db_configs)
        databases = config.databases()

        assert len(databases) == 1

        db, weight = databases[0]
        # Verify user-provided maint_notifications_config is respected
        pool = db.client.connection_pool
        assert pool._maint_notifications_pool_handler is not None
        assert pool._maint_notifications_pool_handler.config.enabled is True

    def test_supplied_pool_keeps_its_own_maint_notifications_config(self):
        """
        The disabled default only applies to clients built from client_kwargs; a
        pool the user supplies is used as it is, and its kwargs are not touched.
        """
        pool = ConnectionPool(host="host1", port=6379, protocol=3)
        db_config = DatabaseConfig(from_pool=pool, weight=1.0)

        config = MultiDbConfig(databases_config=[db_config])
        databases = config.databases()

        db, weight = databases[0]
        assert db.client.connection_pool is pool
        assert "maint_notifications_config" not in db_config.client_kwargs
        # The pool's own RESP3 default ("auto") is left in place
        assert pool.maint_notifications_enabled()


@pytest.mark.fixed_client
class TestDatabaseConfig:
    def test_default_config(self):
        config = DatabaseConfig(
            client_kwargs={"host": "host1", "port": "port1"}, weight=1.0
        )

        assert config.client_kwargs == {"host": "host1", "port": "port1"}
        assert config.weight == 1.0
        assert isinstance(config.default_circuit_breaker(), PBCircuitBreakerAdapter)

    def test_overridden_config(self):
        mock_connection_pool = Mock(spec=ConnectionPool)
        mock_circuit = Mock(spec=CircuitBreaker)

        config = DatabaseConfig(
            client_kwargs={"connection_pool": mock_connection_pool},
            weight=1.0,
            circuit=mock_circuit,
        )

        assert config.client_kwargs == {"connection_pool": mock_connection_pool}
        assert config.weight == 1.0
        assert config.circuit == mock_circuit
