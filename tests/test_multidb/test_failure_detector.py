from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from redis.multidb.command_executor import SyncCommandExecutor
from redis.multidb.database import Database
from redis.multidb.failure_detector import CommandFailureDetector
from redis.multidb.circuit import State as CBState
from redis.exceptions import ConnectionError


@pytest.mark.fixed_client
class TestCommandFailureDetector:
    @pytest.mark.parametrize(
        "min_num_failures,failure_rate_threshold,circuit_state",
        [
            (2, 0.4, CBState.OPEN),
            (2, 0, CBState.OPEN),
            (0, 0.4, CBState.OPEN),
            (3, 0.4, CBState.CLOSED),
            (2, 0.41, CBState.CLOSED),
        ],
        ids=[
            "exceeds min num failures AND failures rate",
            "exceeds min num failures AND failures rate == 0",
            "min num failures == 0 AND exceeds failures rate",
            "do not exceeds min num failures",
            "do not exceeds failures rate",
        ],
    )
    def test_failure_detector_correctly_reacts_to_failures(
        self, min_num_failures, failure_rate_threshold, circuit_state
    ):
        fd = CommandFailureDetector(min_num_failures, failure_rate_threshold)
        mock_db = Mock(spec=Database)
        mock_db.circuit.state = CBState.CLOSED
        mock_ce = Mock(spec=SyncCommandExecutor)
        mock_ce.active_database = mock_db
        fd.set_command_executor(mock_ce)

        fd.register_command_execution(("GET", "key"))
        fd.register_command_execution(("GET", "key"))
        fd.register_failure(Exception(), ("GET", "key"))

        fd.register_command_execution(("GET", "key"))
        fd.register_command_execution(("GET", "key"))
        fd.register_command_execution(("GET", "key"))
        fd.register_failure(Exception(), ("GET", "key"))

        assert mock_db.circuit.state == circuit_state

    @pytest.mark.parametrize(
        "min_num_failures,failure_rate_threshold",
        [
            (3, 0.0),
            (3, 0.6),
        ],
        ids=[
            "do not exceeds min num failures, during interval",
            "do not exceeds min num failures AND failure rate, during interval",
        ],
    )
    def test_failure_detector_do_not_open_circuit_on_interval_exceed(
        self, min_num_failures, failure_rate_threshold
    ):
        # Drive the detector's clock, so the 0.3 s window is crossed exactly
        # rather than by sleeps a slow runner can stretch. Each read moves it on
        # by 1 us, as real time would: the window check is strict.
        now = [datetime(2026, 1, 1)]

        def tick():
            now[0] += timedelta(microseconds=1)
            return now[0]

        with patch("redis.multidb.failure_detector.datetime") as mock_datetime:
            mock_datetime.now.side_effect = tick

            fd = CommandFailureDetector(min_num_failures, failure_rate_threshold, 0.3)
            mock_db = Mock(spec=Database)
            mock_db.circuit.state = CBState.CLOSED
            mock_ce = Mock(spec=SyncCommandExecutor)
            mock_ce.active_database = mock_db
            fd.set_command_executor(mock_ce)
            assert mock_db.circuit.state == CBState.CLOSED

            fd.register_command_execution(("GET", "key"))
            fd.register_failure(Exception(), ("GET", "key"))
            now[0] += timedelta(seconds=0.16)
            fd.register_command_execution(("GET", "key"))
            fd.register_command_execution(("GET", "key"))
            fd.register_command_execution(("GET", "key"))
            fd.register_failure(Exception(), ("GET", "key"))
            now[0] += timedelta(seconds=0.16)
            fd.register_command_execution(("GET", "key"))
            fd.register_failure(Exception(), ("GET", "key"))

            assert mock_db.circuit.state == CBState.CLOSED

            # 2 more failure as last one already refreshed timer
            fd.register_command_execution(("GET", "key"))
            fd.register_failure(Exception(), ("GET", "key"))
            fd.register_command_execution(("GET", "key"))
            fd.register_failure(Exception(), ("GET", "key"))

            assert mock_db.circuit.state == CBState.OPEN

    def test_failure_detector_open_circuit_on_specific_exception_threshold_exceed(self):
        fd = CommandFailureDetector(5, 1, error_types=[ConnectionError])
        mock_db = Mock(spec=Database)
        mock_db.circuit.state = CBState.CLOSED
        mock_ce = Mock(spec=SyncCommandExecutor)
        mock_ce.active_database = mock_db
        fd.set_command_executor(mock_ce)
        assert mock_db.circuit.state == CBState.CLOSED

        fd.register_failure(Exception(), ("SET", "key1", "value1"))
        fd.register_failure(ConnectionError(), ("SET", "key1", "value1"))
        fd.register_failure(ConnectionError(), ("SET", "key1", "value1"))
        fd.register_failure(Exception(), ("SET", "key1", "value1"))
        fd.register_failure(Exception(), ("SET", "key1", "value1"))

        assert mock_db.circuit.state == CBState.CLOSED

        fd.register_failure(ConnectionError(), ("SET", "key1", "value1"))
        fd.register_failure(ConnectionError(), ("SET", "key1", "value1"))
        fd.register_failure(ConnectionError(), ("SET", "key1", "value1"))

        assert mock_db.circuit.state == CBState.OPEN
