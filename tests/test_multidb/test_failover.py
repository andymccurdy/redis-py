from time import sleep

import pytest

from redis.data_structure import WeightedList
from redis.multidb.circuit import State as CBState
from redis.multidb.exception import (
    NoValidDatabaseException,
    TemporaryUnavailableException,
)
from redis.multidb.failover import (
    WeightBasedFailoverStrategy,
    DefaultFailoverStrategyExecutor,
)


@pytest.mark.fixed_client
class TestWeightBasedFailoverStrategy:
    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.OPEN}},
            ),
        ],
        ids=["all closed - highest weight", "highest weight - open"],
        indirect=True,
    )
    def test_get_valid_database(self, mock_db, mock_db1, mock_db2):
        databases = WeightedList()
        databases.add(mock_db, mock_db.weight)
        databases.add(mock_db1, mock_db1.weight)
        databases.add(mock_db2, mock_db2.weight)

        failover_strategy = WeightBasedFailoverStrategy()
        failover_strategy.set_databases(databases)

        assert failover_strategy.database() == mock_db1

    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.OPEN}},
                {"weight": 0.7, "circuit": {"state": CBState.OPEN}},
                {"weight": 0.5, "circuit": {"state": CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    def test_throws_exception_on_empty_databases(self, mock_db, mock_db1, mock_db2):
        failover_strategy = WeightBasedFailoverStrategy()

        with pytest.raises(
            NoValidDatabaseException,
            match="No valid database available for communication",
        ):
            assert failover_strategy.database()


@pytest.mark.fixed_client
class TestDefaultStrategyExecutor:
    @pytest.mark.parametrize(
        "mock_db",
        [
            {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
        ],
        indirect=True,
    )
    def test_execute_returns_valid_database_with_failover_attempts(
        self, mock_db, mock_fs
    ):
        failover_attempts = 3
        mock_fs.database.side_effect = [
            NoValidDatabaseException,
            NoValidDatabaseException,
            NoValidDatabaseException,
            mock_db,
        ]
        executor = DefaultFailoverStrategyExecutor(
            mock_fs, failover_attempts=failover_attempts, failover_delay=0.1
        )

        for i in range(failover_attempts + 1):
            try:
                database = executor.execute()
                assert database == mock_db
            except TemporaryUnavailableException as e:
                assert e.args[0] == (
                    "No database connections currently available. "
                    "This is a temporary condition - please retry the operation."
                )
                sleep(0.11)
                pass

        assert mock_fs.database.call_count == 4

    def test_execute_throws_exception_on_attempts_exceed(self, mock_fs):
        failover_attempts = 3
        mock_fs.database.side_effect = [
            NoValidDatabaseException,
            NoValidDatabaseException,
            NoValidDatabaseException,
            NoValidDatabaseException,
        ]
        executor = DefaultFailoverStrategyExecutor(
            mock_fs, failover_attempts=failover_attempts, failover_delay=0.1
        )

        with pytest.raises(NoValidDatabaseException):
            for i in range(failover_attempts + 1):
                try:
                    executor.execute()
                except TemporaryUnavailableException as e:
                    assert e.args[0] == (
                        "No database connections currently available. "
                        "This is a temporary condition - please retry the operation."
                    )
                    sleep(0.11)
                    pass

        assert mock_fs.database.call_count == 4

    def test_execute_throws_exception_on_attempts_does_not_exceed_delay(self, mock_fs):
        failover_attempts = 3
        mock_fs.database.side_effect = [
            NoValidDatabaseException,
            NoValidDatabaseException,
            NoValidDatabaseException,
            NoValidDatabaseException,
        ]
        executor = DefaultFailoverStrategyExecutor(
            mock_fs, failover_attempts=failover_attempts, failover_delay=0.1
        )

        with pytest.raises(
            TemporaryUnavailableException,
            match=(
                "No database connections currently available. "
                "This is a temporary condition - please retry the operation."
            ),
        ):
            for i in range(failover_attempts + 1):
                try:
                    executor.execute()
                except TemporaryUnavailableException as e:
                    assert e.args[0] == (
                        "No database connections currently available. "
                        "This is a temporary condition - please retry the operation."
                    )
                    if i == failover_attempts:
                        raise e

        assert mock_fs.database.call_count == 4
