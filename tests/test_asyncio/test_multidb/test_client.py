import asyncio
from unittest.mock import patch, AsyncMock, Mock

import pybreaker
import pytest

from redis._parsers import Encoder
from redis.asyncio.multidb.client import MultiDBClient
from redis.asyncio.multidb.config import InitialHealthCheck
from redis.asyncio.multidb.database import AsyncDatabase
from redis.asyncio.multidb.failover import WeightBasedFailoverStrategy
from redis.asyncio.multidb.failure_detector import AsyncFailureDetector
from redis.asyncio.multidb.healthcheck import HealthCheck
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.client import NEVER_DECODE
from redis.event import EventDispatcher, AsyncOnCommandsFailEvent
from redis.exceptions import (
    ConnectionError,
    RedisClusterException,
    RedisClusterUnreachableError,
)
from redis.multidb.circuit import State as CBState, PBCircuitBreakerAdapter
from redis.multidb.config import DatabaseConfig
from redis.multidb.exception import (
    NoValidDatabaseException,
    UnhealthyDatabaseException,
    InitialHealthCheckFailedError,
)
from redis.retry import Retry as SyncRetry
from tests.test_asyncio.helpers import wait_for_condition
from tests.test_asyncio.test_multidb.conftest import create_weighted_list


@pytest.mark.fixed_client
class TestMultiDbClient:
    @pytest.mark.parametrize("mock_multi_db_config,mock_db", [({}, {})], indirect=True)
    def test_sync_retry_is_converted(self, mock_multi_db_config, mock_db):
        mock_multi_db_config.command_retry = SyncRetry(NoBackoff(), 2)
        databases = create_weighted_list(mock_db)

        with pytest.warns(UserWarning, match="synchronous redis.retry.Retry"):
            with patch.object(
                mock_multi_db_config, "databases", return_value=databases
            ):
                client = MultiDBClient(mock_multi_db_config)

        assert isinstance(client._command_retry, Retry)
        assert client._command_retry.get_retries() == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_aclose_closes_every_database_client(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        A failover leaves the previous active database's client open so its
        in-flight commands can complete, so aclose() must close every database
        client, not only the active one.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]
        mock_hc.check_health.return_value = True

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            client = MultiDBClient(mock_multi_db_config)
            await client.initialize()
            await client.set_active_database(mock_db2)
            mock_db1.client.aclose.assert_not_called()

            await client.aclose()

        for db in (mock_db, mock_db1, mock_db2):
            db.client.aclose.assert_awaited_once_with()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_aclose_closes_every_database_client_when_one_fails(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        A client that fails to close must not leave the later ones open, and
        aclose() raises the first error once every client was closed.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]
        mock_hc.check_health.return_value = True
        first_error = ConnectionError("first")
        mock_db1.client.aclose.side_effect = first_error
        mock_db2.client.aclose.side_effect = ConnectionError("second")

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            client = MultiDBClient(mock_multi_db_config)
            await client.initialize()

            with pytest.raises(ConnectionError) as exc_info:
                await client.aclose()

        assert exc_info.value is first_error
        for db in (mock_db, mock_db1, mock_db2):
            db.client.aclose.assert_awaited_once_with()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_aclose_closes_removed_active_database_client(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Removing the active database leaves its client open for in-flight
        commands, so aclose() must still close it although it is no longer in
        the database list.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]
        mock_hc.check_health.return_value = True

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            client = MultiDBClient(mock_multi_db_config)
            await client.initialize()
            assert client.command_executor.active_database is mock_db1

            await client.remove_database(mock_db1)
            assert client.command_executor.active_database is mock_db2
            mock_db1.client.aclose.assert_not_called()

            await client.aclose()
            # The removed database is dropped once closed
            await client.aclose()

        mock_db1.client.aclose.assert_awaited_once_with()
        for db in (mock_db, mock_db2):
            assert db.client.aclose.await_count == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_cluster_unreachable_error_is_registered_as_retryable(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        A cluster database that cannot be reached reports a
        ``RedisClusterUnreachableError`` rather than the ``ConnectionError`` a
        standalone database reports, so the command retry has to support that type
        for a cluster database to fail over. The overloaded ``RedisClusterException``
        base must stay unsupported - it also covers deterministic caller errors such
        as cross-slot commands, which must be raised without retries.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]
        mock_hc.check_health.return_value = True

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            async with MultiDBClient(mock_multi_db_config) as client:
                supported = client._command_retry._supported_errors
                assert RedisClusterUnreachableError in supported
                assert ConnectionError in supported
                assert RedisClusterException not in supported

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"hc_interval": 0.05},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_aclose_stops_recurring_health_checks(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        aclose() has to stop the background scheduler: cancelling
        _recurring_hc_task alone does nothing because run_recurring_async()
        returns right after scheduling the timer. Without stopping the
        scheduler, health checks keep running after close against the
        released health check resources.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]
        mock_hc.close = AsyncMock()

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            async with MultiDBClient(mock_multi_db_config) as client:
                pass  # __aexit__ triggers aclose()

        assert client._bg_scheduler._stopped
        calls_after_close = mock_hc.check_health.call_count
        await asyncio.sleep(0.3)
        assert mock_hc.check_health.call_count == calls_after_close

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_against_correct_db_on_successful_initialization(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")

            mock_hc.check_health.return_value = True

            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )
                assert await client.set("key", "value") == "OK1"
                # 3 databases × 3 probes = 9 calls minimum
                assert len(mock_hc.check_health.call_args_list) >= 9

                assert mock_db.circuit.state == CBState.CLOSED
                assert mock_db1.circuit.state == CBState.CLOSED
                assert mock_db2.circuit.state == CBState.CLOSED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    @pytest.mark.parametrize("decode_responses", [False, True])
    async def test_zrank_with_custom_score_cast(
        self,
        mock_multi_db_config,
        mock_db,
        mock_db1,
        mock_db2,
        mock_hc,
        decode_responses,
    ):
        # zrank/zrevrank with a custom score cast follow the active database's
        # decode_responses setting.
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.get_connection_kwargs.return_value = {
                "decode_responses": decode_responses
            }
            mock_db1.client.execute_command = AsyncMock(return_value=[1, b"2.5"])

            mock_hc.check_health.return_value = True

            async with MultiDBClient(mock_multi_db_config) as client:
                for command in (client.zrank, client.zrevrank):
                    assert await command(
                        "key", "member", withscore=True, score_cast_func=bytes
                    ) == [1, b"2.5"]
                    options = mock_db1.client.execute_command.call_args.kwargs
                    assert options["decode_responses"] is decode_responses

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_connection_kwargs_and_encoder_follow_active_database(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            try:
                # Before initialization: the highest-weighted database.
                assert not client.initialized
                assert (
                    client.get_connection_kwargs()
                    is mock_db1.client.get_connection_kwargs.return_value
                )
                assert client.get_encoder() is mock_db1.client.get_encoder.return_value

                await client.initialize()
                await client.set_active_database(mock_db2)
                pipe = client.pipeline()
                for host in (client, pipe):
                    assert (
                        host.get_connection_kwargs()
                        is mock_db2.client.get_connection_kwargs.return_value
                    )
                    assert (
                        host.get_encoder() is mock_db2.client.get_encoder.return_value
                    )
            finally:
                await client.aclose()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    @pytest.mark.parametrize("decode_responses", [False, True])
    async def test_slowlog_get_before_first_command(
        self,
        mock_multi_db_config,
        mock_db,
        mock_db1,
        mock_db2,
        mock_hc,
        decode_responses,
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.get_connection_kwargs.return_value = {
                "decode_responses": decode_responses
            }
            mock_db1.client.execute_command = AsyncMock(return_value=[])

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            try:
                assert not client.initialized
                assert await client.slowlog_get() == []
                assert client.initialized

                args = mock_db1.client.execute_command.call_args
                assert args.args == ("SLOWLOG GET",)
                if decode_responses:
                    assert args.kwargs == {NEVER_DECODE: []}
                else:
                    assert args.kwargs == {}
            finally:
                await client.aclose()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_acl_setuser_uses_active_database_encoder(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.get_encoder.return_value = Encoder(
                encoding="utf-8", encoding_errors="strict", decode_responses=False
            )
            mock_db1.client.execute_command = AsyncMock(return_value=b"OK")

            mock_hc.check_health.return_value = True

            async with MultiDBClient(mock_multi_db_config) as client:
                assert await client.acl_setuser(
                    "user", enabled=True, passwords=["+pass"]
                )
                args = mock_db1.client.execute_command.call_args.args
                assert args == ("ACL SETUSER", "user", b"on", b">pass")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"initial_health_check_policy": InitialHealthCheck.MAJORITY_AVAILABLE},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_against_correct_db_and_closed_circuit(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Validates that commands are executed against the correct
        database when one database becomes unhealthy during initialization.
        Ensures the client selects the highest-weighted
        healthy database (mock_db1) and executes commands against it
        with a CLOSED circuit.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db.client.execute_command = AsyncMock(
                return_value="NOT_OK-->Response from unexpected db - mock_db"
            )
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")
            mock_db2.client.execute_command = AsyncMock(
                return_value="NOT_OK-->Response from unexpected db - mock_db2"
            )

            async def mock_check_health(database, connection=None):
                if database == mock_db2:
                    return False
                else:
                    return True

            mock_hc.check_health.side_effect = mock_check_health

            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )
                result = await client.set("key", "value")
                assert result == "OK1"
                # 3 databases × 3 probes = 9 calls minimum (but one db fails, so >= 7)
                assert len(mock_hc.check_health.call_args_list) >= 7

                assert mock_db.circuit.state == CBState.CLOSED
                assert mock_db1.circuit.state == CBState.CLOSED
                assert mock_db2.circuit.state == CBState.OPEN

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_against_correct_db_on_background_health_check_determine_active_db_unhealthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        cb = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=0.1))
        cb.database = mock_db
        mock_db.circuit = cb

        cb1 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=0.1))
        cb1.database = mock_db1
        mock_db1.circuit = cb1

        cb2 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=0.1))
        cb2.database = mock_db2
        mock_db2.circuit = cb2

        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        # Track health check rounds per database
        # A round is a complete health check cycle (initial or background)
        db_rounds = {id(mock_db): 0, id(mock_db1): 0, id(mock_db2): 0}
        db_probes_in_round = {id(mock_db): 0, id(mock_db1): 0, id(mock_db2): 0}

        # Create events for each failover scenario
        db1_became_unhealthy = asyncio.Event()
        db2_became_unhealthy = asyncio.Event()
        db_became_unhealthy = asyncio.Event()
        counter_lock = asyncio.Lock()

        async def mock_check_health(database, connection=None):
            async with counter_lock:
                db_probes_in_round[id(database)] += 1
                # After 3 probes, increment the round counter
                if db_probes_in_round[id(database)] > 3:
                    db_rounds[id(database)] += 1
                    db_probes_in_round[id(database)] = 1
                current_round = db_rounds[id(database)]

            # Round 0 (initial health check): All databases healthy
            if current_round == 0:
                return True

            # Round 1: mock_db1 becomes unhealthy, others healthy
            if current_round == 1:
                if database == mock_db1:
                    db1_became_unhealthy.set()
                    return False
                return True

            # Round 2: mock_db2 also becomes unhealthy, mock_db healthy
            if current_round == 2:
                if database == mock_db1:
                    return False
                if database == mock_db2:
                    db2_became_unhealthy.set()
                    return False
                return True

            # Round 3: mock_db also becomes unhealthy, but mock_db1 recovers
            if current_round == 3:
                if database == mock_db:
                    db_became_unhealthy.set()
                    return False
                if database == mock_db2:
                    return False
                return True

            # Round 4+: mock_db1 is healthy, others unhealthy
            if database == mock_db1:
                return True
            return False

        mock_hc.check_health.side_effect = mock_check_health
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db.client.execute_command = AsyncMock(return_value="OK")
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")
            mock_db2.client.execute_command = AsyncMock(return_value="OK2")
            mock_multi_db_config.health_check_interval = 0.05
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()

            async with MultiDBClient(mock_multi_db_config) as client:
                assert await client.set("key", "value") == "OK1"

                # Wait for mock_db1 to become unhealthy
                # Use wait_for_condition to wait for the event with a timeout
                # (instead of just sleeping)
                await wait_for_condition(
                    lambda: cb1.state == CBState.OPEN,
                    timeout=5.0,
                    error_message="Timeout waiting for cb1 to open",
                )

                assert await client.set("key", "value") == "OK2"

                # Wait for mock_db2 to become unhealthy
                # Use wait_for_condition to wait for the event with a timeout
                # (instead of just sleeping)
                await wait_for_condition(
                    lambda: cb2.state == CBState.OPEN,
                    timeout=5.0,
                    error_message="Timeout waiting for cb2 to open",
                )

                assert await client.set("key", "value") == "OK"

                # Wait for mock_db to become unhealthy
                assert await asyncio.wait_for(
                    db_became_unhealthy.wait(), timeout=1.0
                ), "Timeout waiting for mock_db to become unhealthy"
                await wait_for_condition(
                    lambda: cb.state == CBState.OPEN,
                    timeout=5.0,
                    error_message="Timeout waiting for cb to open",
                )

                # Wait for mock_db1 to recover (circuit breaker to close)
                await wait_for_condition(
                    lambda: cb1.state == CBState.CLOSED,
                    timeout=5.0,
                    error_message="Timeout waiting for cb1 to close (mock_db1 to recover)",
                )

                assert await client.set("key", "value") == "OK1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_auto_fallback_to_highest_weight_db(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        # Track health check rounds per database
        db_rounds = {id(mock_db): 0, id(mock_db1): 0, id(mock_db2): 0}
        db_probes_in_round = {id(mock_db): 0, id(mock_db1): 0, id(mock_db2): 0}
        db1_became_unhealthy = asyncio.Event()
        counter_lock = asyncio.Lock()

        async def mock_check_health(database, connection=None):
            async with counter_lock:
                db_probes_in_round[id(database)] += 1
                if db_probes_in_round[id(database)] > 3:
                    db_rounds[id(database)] += 1
                    db_probes_in_round[id(database)] = 1
                current_round = db_rounds[id(database)]

            # Round 0 (initial health check): All databases healthy
            if current_round == 0:
                return True

            # Round 1: mock_db1 becomes unhealthy, others healthy
            if current_round == 1:
                if database == mock_db1:
                    db1_became_unhealthy.set()
                    return False
                return True

            # Round 2+: All databases healthy (mock_db1 recovers)
            return True

        mock_hc.check_health.side_effect = mock_check_health
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db.client.execute_command = AsyncMock(return_value="OK")
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")
            mock_db2.client.execute_command = AsyncMock(return_value="OK2")
            mock_multi_db_config.health_check_interval = 0.05
            mock_multi_db_config.auto_fallback_interval = 0.1
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()

            async with MultiDBClient(mock_multi_db_config) as client:
                assert await client.set("key", "value") == "OK1"

                # Wait for mock_db1 to become unhealthy
                assert await asyncio.wait_for(
                    db1_became_unhealthy.wait(), timeout=1.0
                ), "Timeout waiting for mock_db1 to become unhealthy"

                # Wait for circuit breaker to actually open
                await wait_for_condition(
                    lambda: mock_db1.circuit.state == CBState.OPEN,
                    timeout=5.0,
                    error_message="Timeout waiting for cb1 to open after error event.",
                )

                # Now the failover strategy will select mock_db2
                assert await client.set("key", "value") == "OK2"

                # Wait for auto fallback interval to pass (mock_db1 recovers in round 2+)
                await wait_for_condition(
                    lambda: mock_db1.circuit.state == CBState.CLOSED,
                    timeout=5.0,
                    error_message="Timeout waiting for mock_db1 to be healthy again.",
                )

                # Wait for auto fallback time to pass - this way on next command execution
                # the active database will be re-evaluated
                await asyncio.sleep(0.1)

                # Now the failover strategy will select mock_db1 again
                assert await client.set("key", "value") == "OK1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_do_not_auto_fallback_to_highest_weight_db(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        # Track health check rounds per database
        db_rounds = {id(mock_db): 0, id(mock_db1): 0, id(mock_db2): 0}
        db_probes_in_round = {id(mock_db): 0, id(mock_db1): 0, id(mock_db2): 0}
        db1_became_unhealthy = asyncio.Event()
        counter_lock = asyncio.Lock()

        async def mock_check_health(database, connection=None):
            async with counter_lock:
                db_probes_in_round[id(database)] += 1
                if db_probes_in_round[id(database)] > 3:
                    db_rounds[id(database)] += 1
                    db_probes_in_round[id(database)] = 1
                current_round = db_rounds[id(database)]

            # Round 0 (initial health check): All databases healthy
            if current_round == 0:
                return True

            # Round 1+: mock_db1 stays unhealthy (no auto fallback)
            if database == mock_db1:
                db1_became_unhealthy.set()
                return False

            return True

        mock_hc.check_health.side_effect = mock_check_health
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db.client.execute_command = AsyncMock(return_value="OK")
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")
            mock_db2.client.execute_command = AsyncMock(return_value="OK2")
            mock_multi_db_config.health_check_interval = 0.05
            mock_multi_db_config.auto_fallback_interval = -1
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()

            async with MultiDBClient(mock_multi_db_config) as client:
                assert await client.set("key", "value") == "OK1"

                # Wait for mock_db1 to become unhealthy
                assert await asyncio.wait_for(
                    db1_became_unhealthy.wait(), timeout=1.0
                ), "Timeout waiting for mock_db1 to become unhealthy"

                # Wait for circuit breaker state to actually reflect the unhealthy status
                # (instead of just sleeping)
                await wait_for_condition(
                    lambda: mock_db1.circuit.state == CBState.OPEN,
                    timeout=5.0,
                    error_message="Timeout waiting for cb1 to open after error event.",
                )

                assert await client.set("key", "value") == "OK2"
                await asyncio.sleep(0.5)
                assert await client.set("key", "value") == "OK2"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_add_database_makes_new_database_active(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Test that add_database with a DatabaseConfig creates a new database
        and makes it active if it has the highest weight.
        """
        databases = create_weighted_list(mock_db, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        new_db_config = DatabaseConfig(
            weight=0.8,  # Higher than mock_db2's 0.5
            from_url="redis://localhost:6379",
        )

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db2.client.execute_command.return_value = "OK2"
            mock_hc.check_health = AsyncMock(return_value=True)

            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )

                # Initially mock_db2 is active (highest weight among initial databases)
                assert await client.set("key", "value") == "OK2"
                initial_hc_count = len(mock_hc.check_health.call_args_list)

                # Mock the client class to return a mock client for the new database
                mock_new_client = AsyncMock()
                mock_new_client.execute_command.return_value = "OK_NEW"
                mock_new_client.connection_pool = Mock()

                with patch.object(
                    mock_multi_db_config.client_class,
                    "from_url",
                    return_value=mock_new_client,
                ):
                    await client.add_database(new_db_config)

                # Health check should have been called for the new database
                assert len(mock_hc.check_health.call_args_list) > initial_hc_count

                # New database should be active since it has highest weight
                assert await client.set("key", "value") == "OK_NEW"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_add_database_skip_unhealthy_false_raises_exception(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Test that add_database with skip_unhealthy=False raises an exception
        when the new database fails health check due to an exception.
        """
        databases = create_weighted_list(mock_db, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        new_db_config = DatabaseConfig(
            weight=0.8,
            from_url="redis://localhost:6379",
        )

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db2.client.execute_command.return_value = "OK2"

            # Health check returns True for existing databases, raises exception for new one
            async def mock_check_health(database, connection=None):
                if database in [mock_db, mock_db2]:
                    return True
                # Raise an exception for the new database to trigger UnhealthyDatabaseException
                raise ConnectionError("Connection refused")

            mock_hc.check_health = AsyncMock(side_effect=mock_check_health)

            async with MultiDBClient(mock_multi_db_config) as client:
                # Initially mock_db2 is active
                assert await client.set("key", "value") == "OK2"

                mock_new_client = AsyncMock()
                mock_new_client.execute_command.return_value = "OK_NEW"
                mock_new_client.connection_pool = Mock()

                with patch.object(
                    mock_multi_db_config.client_class,
                    "from_url",
                    return_value=mock_new_client,
                ):
                    # With skip_unhealthy=False, should raise exception
                    with pytest.raises(UnhealthyDatabaseException):
                        await client.add_database(
                            new_db_config, skip_initial_health_check=False
                        )

                # Database list should remain unchanged
                assert len(client.get_databases()) == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_remove_highest_weighted_database(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db2.client.execute_command.return_value = "OK2"

            mock_hc.check_health.return_value = True

            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )

                assert await client.set("key", "value") == "OK1"
                assert len(mock_hc.check_health.call_args_list) == 9

                await client.remove_database(mock_db1)
                assert await client.set("key", "value") == "OK2"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_update_database_weight_to_be_highest(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db2.client.execute_command.return_value = "OK2"

            mock_hc.check_health.return_value = True

            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )

                assert await client.set("key", "value") == "OK1"
                assert len(mock_hc.check_health.call_args_list) == 9

                await client.update_database_weight(mock_db2, 0.8)
                assert mock_db2.weight == 0.8

                assert await client.set("key", "value") == "OK2"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_add_new_failure_detector(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_multi_db_config.event_dispatcher = EventDispatcher()
            mock_fd = mock_multi_db_config.failure_detectors[0]

            # Event fired if command against mock_db1 would fail
            command_fail_event = AsyncOnCommandsFailEvent(
                commands=("SET", "key", "value"),
                exception=Exception(),
            )

            mock_hc.check_health.return_value = True

            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )
                assert await client.set("key", "value") == "OK1"
                assert len(mock_hc.check_health.call_args_list) == 9

                # Simulate failing command events that lead to a failure detection
                for _ in range(5):
                    await mock_multi_db_config.event_dispatcher.dispatch_async(
                        command_fail_event
                    )

                assert mock_fd.register_failure.call_count == 5

                another_fd = Mock(spec=AsyncFailureDetector)
                client.add_failure_detector(another_fd)

                # Simulate failing command events that lead to a failure detection
                for _ in range(5):
                    await mock_multi_db_config.event_dispatcher.dispatch_async(
                        command_fail_event
                    )

                assert mock_fd.register_failure.call_count == 10
                assert another_fd.register_failure.call_count == 5

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_add_new_health_check(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command.return_value = "OK1"

            mock_hc.check_health.return_value = True

            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )
                assert await client.set("key", "value") == "OK1"
                assert len(mock_hc.check_health.call_args_list) >= 9

                another_hc = Mock(spec=HealthCheck)
                another_hc.health_check_probes = 3
                another_hc.health_check_delay = 0.01
                another_hc.health_check_timeout = 3.0
                another_hc.check_health.return_value = True

                await client.add_health_check(another_hc)
                await client._check_db_health(mock_db1)

                assert len(mock_hc.check_health.call_args_list) >= 12
                assert len(another_hc.check_health.call_args_list) >= 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_set_active_database(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db.client.execute_command.return_value = "OK"

            mock_hc.check_health.return_value = True

            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )
                assert await client.set("key", "value") == "OK1"
                assert len(mock_hc.check_health.call_args_list) >= 9

                await client.set_active_database(mock_db)
                assert await client.set("key", "value") == "OK"

                with pytest.raises(
                    ValueError, match="Given database is not a member of database list"
                ):
                    await client.set_active_database(Mock(spec=AsyncDatabase))

                mock_hc.check_health.return_value = False

                with pytest.raises(
                    NoValidDatabaseException,
                    match="Cannot set active database, database is unhealthy",
                ):
                    await client.set_active_database(mock_db1)


@pytest.mark.fixed_client
class TestGeoFailoverMetricRecording:
    """Tests for geo failover metric recording in async MultiDBClient."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_manual_failover_records_metric(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Test that set_active_database records geo failover metric with MANUAL reason.
        """
        from redis.observability.attributes import GeoFailoverReason

        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")
            mock_db.client.execute_command = AsyncMock(return_value="OK")
            mock_hc.check_health = AsyncMock(return_value=True)

            async with MultiDBClient(mock_multi_db_config) as client:
                # Initial active database should be mock_db1 (highest weight)
                assert await client.set("key", "value") == "OK1"

                # Now manually switch to mock_db
                # Patch at the module where it's imported
                with patch(
                    "redis.asyncio.multidb.command_executor.record_geo_failover"
                ) as mock_record_geo_failover:
                    mock_record_geo_failover.return_value = None
                    await client.set_active_database(mock_db)

                    # Verify record_geo_failover was called with correct arguments
                    mock_record_geo_failover.assert_called_once()
                    call_kwargs = mock_record_geo_failover.call_args[1]
                    assert call_kwargs["fail_from"] == mock_db1
                    assert call_kwargs["fail_to"] == mock_db
                    assert call_kwargs["reason"] == GeoFailoverReason.MANUAL

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_automatic_failover_records_metric(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Test that automatic failover records geo failover metric with AUTOMATIC reason.
        """
        from redis.observability.attributes import GeoFailoverReason

        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")
            mock_db2.client.execute_command = AsyncMock(return_value="OK2")
            mock_hc.check_health = AsyncMock(return_value=True)

            async with MultiDBClient(mock_multi_db_config) as client:
                # Initial active database should be mock_db1 (highest weight)
                assert await client.set("key", "value") == "OK1"

                # Simulate mock_db1 becoming unhealthy (circuit open)
                mock_db1.circuit.state = CBState.OPEN

                # Configure the failover strategy to return mock_db2
                mock_multi_db_config.failover_strategy.database.return_value = mock_db2

                # Patch at the module where it's imported
                with patch(
                    "redis.asyncio.multidb.command_executor.record_geo_failover"
                ) as mock_record_geo_failover:
                    mock_record_geo_failover.return_value = None
                    # Execute a command - this should trigger automatic failover
                    assert await client.set("key", "value") == "OK2"

                    # Verify record_geo_failover was called with AUTOMATIC reason
                    mock_record_geo_failover.assert_called_once()
                    call_kwargs = mock_record_geo_failover.call_args[1]
                    assert call_kwargs["fail_from"] == mock_db1
                    assert call_kwargs["fail_to"] == mock_db2
                    assert call_kwargs["reason"] == GeoFailoverReason.AUTOMATIC

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_no_metric_recorded_when_same_database(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Test that no geo failover metric is recorded when active database doesn't change.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")
            mock_hc.check_health = AsyncMock(return_value=True)

            async with MultiDBClient(mock_multi_db_config) as client:
                # Initial active database should be mock_db1 (highest weight)
                assert await client.set("key", "value") == "OK1"

                # Patch at the module where it's imported
                with patch(
                    "redis.asyncio.multidb.command_executor.record_geo_failover"
                ) as mock_record_geo_failover:
                    mock_record_geo_failover.return_value = None
                    # Set active database to the same database
                    await client.set_active_database(mock_db1)

                    # Verify record_geo_failover was NOT called
                    mock_record_geo_failover.assert_not_called()


@pytest.mark.fixed_client
class TestInitialHealthCheckPolicy:
    """Tests for initial health check policy evaluation."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_all_healthy_policy_succeeds_when_all_databases_healthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Test that ALL_HEALTHY policy succeeds when all databases pass health check.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_hc.check_health = AsyncMock(return_value=True)

            async with MultiDBClient(mock_multi_db_config) as client:
                # Should succeed without raising InitialHealthCheckFailedError
                assert await client.set("key", "value") == "OK1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_all_healthy_policy_fails_when_one_database_unhealthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Test that ALL_HEALTHY policy fails when any database fails health check.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):

            async def mock_check_health(database, connection=None):
                # mock_db2 is unhealthy
                return database != mock_db2

            mock_hc.check_health = AsyncMock(side_effect=mock_check_health)

            with pytest.raises(
                InitialHealthCheckFailedError,
                match="Initial health check failed",
            ):
                async with MultiDBClient(mock_multi_db_config) as client:
                    await client.set("key", "value")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"initial_health_check_policy": InitialHealthCheck.MAJORITY_AVAILABLE},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_majority_healthy_policy_succeeds_when_majority_healthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Test that MAJORITY_HEALTHY policy succeeds when more than half of databases are healthy.
        With 3 databases, 2 healthy is a majority.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command.return_value = "OK1"

            async def mock_check_health(database, connection=None):
                # mock_db2 is unhealthy, but 2 out of 3 are healthy (majority)
                return database != mock_db2

            mock_hc.check_health = AsyncMock(side_effect=mock_check_health)

            async with MultiDBClient(mock_multi_db_config) as client:
                # Should succeed - 2 out of 3 healthy is a majority
                assert await client.set("key", "value") == "OK1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"initial_health_check_policy": InitialHealthCheck.MAJORITY_AVAILABLE},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_majority_healthy_policy_fails_when_minority_healthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Test that MAJORITY_HEALTHY policy fails when less than half of databases are healthy.
        With 3 databases, only 1 healthy is not a majority.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):

            async def mock_check_health(database, connection=None):
                # Only mock_db is healthy (1 out of 3 is not a majority)
                return database == mock_db

            mock_hc.check_health = AsyncMock(side_effect=mock_check_health)

            with pytest.raises(
                InitialHealthCheckFailedError,
                match="Initial health check failed",
            ):
                async with MultiDBClient(mock_multi_db_config) as client:
                    await client.set("key", "value")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"initial_health_check_policy": InitialHealthCheck.ONE_AVAILABLE},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_any_healthy_policy_succeeds_when_one_database_healthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Test that ANY_HEALTHY policy succeeds when at least one database is healthy.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db.client.execute_command.return_value = "OK"

            async def mock_check_health(database, connection=None):
                # Only mock_db is healthy
                return database == mock_db

            mock_hc.check_health = AsyncMock(side_effect=mock_check_health)

            async with MultiDBClient(mock_multi_db_config) as client:
                # Should succeed - at least one database is healthy
                assert await client.set("key", "value") == "OK"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"initial_health_check_policy": InitialHealthCheck.ONE_AVAILABLE},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_any_healthy_policy_fails_when_no_database_healthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        """
        Test that ANY_HEALTHY policy fails when no database is healthy.
        """
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_hc.check_health = AsyncMock(return_value=False)

            with pytest.raises(
                InitialHealthCheckFailedError,
                match="Initial health check failed",
            ):
                async with MultiDBClient(mock_multi_db_config) as client:
                    await client.set("key", "value")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_custom_health_check_parameters_are_respected(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        """
        Test that custom health check parameters (probes, delay, timeout)
        override the default values and are properly used during health checks.
        """
        from redis.asyncio.multidb.healthcheck import AbstractHealthCheck
        import asyncio

        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        # Track actual delays between probes
        probe_timestamps = []
        probe_lock = asyncio.Lock()

        class CustomHealthCheck(AbstractHealthCheck):
            """Custom health check with non-default parameters."""

            def __init__(self):
                # Use custom values: 5 probes, 0.02s delay, 2.0s timeout
                super().__init__(
                    health_check_probes=5,
                    health_check_delay=0.02,
                    health_check_timeout=2.0,
                )

            async def check_health(self, database, connection=None) -> bool:
                import time

                async with probe_lock:
                    probe_timestamps.append(time.time())
                return True

        custom_hc = CustomHealthCheck()

        # Verify custom parameters are set correctly
        assert custom_hc.health_check_probes == 5
        assert custom_hc.health_check_delay == 0.02
        assert custom_hc.health_check_timeout == 2.0

        mock_multi_db_config.health_checks = [custom_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")

            async with MultiDBClient(mock_multi_db_config) as client:
                # Client should initialize successfully
                assert await client.set("key", "value") == "OK1"

                # With 3 databases and 5 probes each, we should have 15 probes total
                # (executed in parallel per database, but sequentially within each db)
                assert len(probe_timestamps) == 15

                # Verify delays between probes within each database
                # Since probes run in parallel across databases, we need to check
                # that the minimum delay between consecutive probes is approximately
                # the configured delay (0.02s)
                # Sort timestamps and check that there are gaps of ~0.02s
                sorted_timestamps = sorted(probe_timestamps)

                # With 3 databases running in parallel, each doing 5 probes with 0.02s delay,
                # the total time should be approximately 4 * 0.02 = 0.08s per database
                # (4 delays between 5 probes)
                total_duration = sorted_timestamps[-1] - sorted_timestamps[0]
                # Should be at least 4 delays worth (0.08s) but not too long
                assert total_duration >= 0.04, (
                    f"Total duration {total_duration}s is too short, "
                    f"expected at least 0.04s for 5 probes with 0.02s delay"
                )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_custom_health_check_timeout_triggers_unhealthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        """
        Test that a custom health check timeout is respected and triggers
        UnhealthyDatabaseException when exceeded.
        """
        from redis.asyncio.multidb.healthcheck import AbstractHealthCheck
        from redis.multidb.exception import InitialHealthCheckFailedError
        import asyncio

        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        class SlowHealthCheck(AbstractHealthCheck):
            """Health check that takes longer than the configured timeout."""

            def __init__(self):
                # Short timeout (0.1s) but health check will take longer (0.5s)
                super().__init__(
                    health_check_probes=1,
                    health_check_delay=0.01,
                    health_check_timeout=0.1,
                )

            async def check_health(self, database, connection=None) -> bool:
                # Sleep longer than the timeout
                await asyncio.sleep(0.5)
                return True

        slow_hc = SlowHealthCheck()

        # Verify custom timeout is set
        assert slow_hc.health_check_timeout == 0.1

        mock_multi_db_config.health_checks = [slow_hc]

        with patch.object(mock_multi_db_config, "databases", return_value=databases):
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")

            # Executing a command triggers initialize() which runs health checks
            # The health check should timeout and raise InitialHealthCheckFailedError
            with pytest.raises(InitialHealthCheckFailedError):
                async with MultiDBClient(mock_multi_db_config) as client:
                    await client.set("key", "value")
