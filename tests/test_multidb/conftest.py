from unittest.mock import Mock, AsyncMock, MagicMock, patch

import pytest

from redis import Redis, ConnectionPool
from redis.data_structure import WeightedList
from redis.multidb.circuit import State as CBState, CircuitBreaker
from redis.multidb.config import (
    MultiDbConfig,
    DatabaseConfig,
    DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_AUTO_FALLBACK_INTERVAL,
    InitialHealthCheck,
)
from redis.multidb.database import Database, Databases
from redis.multidb.failover import FailoverStrategy
from redis.multidb.failure_detector import FailureDetector
from redis.asyncio.multidb.healthcheck import (
    HealthCheck,
    AbstractHealthCheckPolicy,
    DEFAULT_HEALTH_CHECK_PROBES,
    DEFAULT_HEALTH_CHECK_POLICY,
    DEFAULT_HEALTH_CHECK_TIMEOUT,
)


@pytest.fixture(autouse=True)
def mock_health_check_connections(request):
    """
    Mock clients for health check policies.
    Uses real policy classes but mocks only the client layer.

    Skip this fixture for tests marked with @pytest.mark.no_mock_connections
    (e.g. the integration tests that talk to real Redis deployments).
    """
    # Check if the test is marked to skip connection mocking
    if request.node.get_closest_marker("no_mock_connections"):
        yield
        return

    async def mock_get_client(self, database):
        mock_client = AsyncMock()
        mock_client.ping = AsyncMock(return_value=True)
        mock_client.aclose = AsyncMock()
        return mock_client

    with patch.object(AbstractHealthCheckPolicy, "get_client", mock_get_client):
        yield


@pytest.fixture()
def mock_fd() -> FailureDetector:
    return Mock(spec=FailureDetector)


@pytest.fixture()
def mock_fs() -> FailoverStrategy:
    return Mock(spec=FailoverStrategy)


@pytest.fixture()
def mock_hc() -> HealthCheck:
    from unittest.mock import AsyncMock

    mock = Mock(spec=HealthCheck)
    mock.health_check_probes = DEFAULT_HEALTH_CHECK_PROBES
    # Use minimal delay for faster test execution
    mock.health_check_delay = 0.01
    mock.health_check_timeout = DEFAULT_HEALTH_CHECK_TIMEOUT
    # check_health is now async, so use AsyncMock
    mock.check_health = AsyncMock(return_value=True)
    return mock


def _create_mock_db(request) -> Database:
    """Helper to create a mock Database with proper client setup."""
    db = Mock(spec=Database)
    db.weight = request.param.get("weight", 1.0)
    db.client = Mock(spec=Redis)
    db.client.connection_pool = MagicMock(spec=ConnectionPool)

    cb = request.param.get("circuit", {})
    mock_cb = Mock(spec=CircuitBreaker)
    mock_cb.grace_period = cb.get("grace_period", 1.0)
    mock_cb.state = cb.get("state", CBState.CLOSED)

    db.circuit = mock_cb
    return db


@pytest.fixture()
def mock_db(request) -> Database:
    return _create_mock_db(request)


@pytest.fixture()
def mock_db1(request) -> Database:
    return _create_mock_db(request)


@pytest.fixture()
def mock_db2(request) -> Database:
    return _create_mock_db(request)


@pytest.fixture()
def mock_multi_db_config(request, mock_fd, mock_fs, mock_hc, mock_ed) -> MultiDbConfig:
    hc_interval = request.param.get("hc_interval", DEFAULT_HEALTH_CHECK_INTERVAL)
    auto_fallback_interval = request.param.get(
        "auto_fallback_interval", DEFAULT_AUTO_FALLBACK_INTERVAL
    )
    health_check_policy = request.param.get(
        "health_check_policy", DEFAULT_HEALTH_CHECK_POLICY
    )
    health_check_probes = request.param.get(
        "health_check_probes", DEFAULT_HEALTH_CHECK_PROBES
    )
    initial_health_check_policy = request.param.get(
        "initial_health_check_policy", InitialHealthCheck.ALL_AVAILABLE
    )

    config = MultiDbConfig(
        databases_config=[Mock(spec=DatabaseConfig)],
        failure_detectors=[mock_fd],
        health_check_interval=hc_interval,
        health_check_delay=0.05,
        health_check_policy=health_check_policy,
        health_check_probes=health_check_probes,
        failover_strategy=mock_fs,
        auto_fallback_interval=auto_fallback_interval,
        event_dispatcher=mock_ed,
        initial_health_check_policy=initial_health_check_policy,
    )

    return config


def create_weighted_list(*databases) -> Databases:
    dbs = WeightedList()

    for db in databases:
        dbs.add(db, db.weight)

    return dbs
