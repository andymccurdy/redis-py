import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from redis.asyncio.multidb.failure_detector import FailureDetectorAsyncWrapper
from redis.event import EventDispatcher, OnCommandsFailEvent
from redis.exceptions import (
    ConnectionError,
    RedisClusterException,
    RedisClusterUnreachableError,
)
from redis.asyncio.multidb.command_executor import DefaultCommandExecutor
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.multidb.circuit import State as CBState
from redis.multidb.failure_detector import CommandFailureDetector
from redis.observability.attributes import GeoFailoverReason
from tests.test_asyncio.test_multidb.conftest import create_weighted_list


@pytest.mark.fixed_client
class TestDefaultCommandExecutor:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_on_active_database(
        self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        mock_db1.client.execute_command = AsyncMock(return_value="OK1")
        mock_db2.client.execute_command = AsyncMock(return_value="OK2")
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            command_retry=Retry(NoBackoff(), 0),
        )

        await executor.set_active_database(mock_db1, GeoFailoverReason.MANUAL)
        assert await executor.execute_command("SET", "key", "value") == "OK1"

        await executor.set_active_database(mock_db2, GeoFailoverReason.MANUAL)
        assert await executor.execute_command("SET", "key", "value") == "OK2"
        assert mock_ed.register_listeners.call_count == 1
        assert mock_fd.register_command_execution.call_count == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_automatically_select_active_database(
        self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        mock_db1.client.execute_command = AsyncMock(return_value="OK1")
        mock_db2.client.execute_command = AsyncMock(return_value="OK2")
        mock_selector = AsyncMock(side_effect=[mock_db1, mock_db2])
        type(mock_fs).database = mock_selector
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            command_retry=Retry(NoBackoff(), 0),
        )

        assert await executor.execute_command("SET", "key", "value") == "OK1"
        mock_db1.circuit.state = CBState.OPEN

        assert await executor.execute_command("SET", "key", "value") == "OK2"
        assert mock_ed.register_listeners.call_count == 1
        assert mock_selector.call_count == 2
        assert mock_fd.register_command_execution.call_count == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_fallback_to_another_db_after_fallback_interval(
        self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        mock_db1.client.execute_command = AsyncMock(return_value="OK1")
        mock_db2.client.execute_command = AsyncMock(return_value="OK2")
        mock_selector = AsyncMock(side_effect=[mock_db1, mock_db2, mock_db1])
        type(mock_fs).database = mock_selector
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            auto_fallback_interval=0.1,
            command_retry=Retry(NoBackoff(), 0),
        )

        assert await executor.execute_command("SET", "key", "value") == "OK1"
        mock_db1.weight = 0.1
        await asyncio.sleep(0.15)

        assert await executor.execute_command("SET", "key", "value") == "OK2"
        mock_db1.weight = 0.7
        await asyncio.sleep(0.15)

        assert await executor.execute_command("SET", "key", "value") == "OK1"
        assert mock_ed.register_listeners.call_count == 1
        assert mock_selector.call_count == 3
        assert mock_fd.register_command_execution.call_count == 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_fallback_to_another_db_after_failure_detection(
        self, mock_db, mock_db1, mock_db2, mock_fs
    ):
        mock_db1.client.execute_command = AsyncMock(
            side_effect=[
                "OK1",
                ConnectionError,
                ConnectionError,
                ConnectionError,
                "OK1",
            ]
        )
        mock_db2.client.execute_command = AsyncMock(
            side_effect=["OK2", ConnectionError, ConnectionError, ConnectionError]
        )
        mock_selector = AsyncMock(side_effect=[mock_db1, mock_db2, mock_db1])
        type(mock_fs).database = mock_selector
        threshold = 3
        fd = FailureDetectorAsyncWrapper(CommandFailureDetector(threshold, 1))
        ed = EventDispatcher()
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=ed,
            auto_fallback_interval=0.1,
            command_retry=Retry(NoBackoff(), threshold),
        )
        fd.set_command_executor(command_executor=executor)

        assert await executor.execute_command("SET", "key", "value") == "OK1"
        assert await executor.execute_command("SET", "key", "value") == "OK2"
        assert await executor.execute_command("SET", "key", "value") == "OK1"
        assert mock_selector.call_count == 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_fallback_to_another_db_on_cluster_connection_failure(
        self, mock_db, mock_db1, mock_db2, mock_fs
    ):
        """
        A cluster database that cannot be reached reports a ``RedisClusterException``
        instead of the connection error a standalone database reports, so make sure
        it triggers a failover all the same.
        """
        cluster_unreachable = RedisClusterUnreachableError(
            "Redis Cluster cannot be connected. Please provide at least "
            "one reachable node"
        )

        mock_db1.client.execute_command = AsyncMock(
            side_effect=[
                "OK1",
                cluster_unreachable,
                cluster_unreachable,
                cluster_unreachable,
            ]
        )
        mock_db2.client.execute_command = AsyncMock(side_effect=["OK2"])
        mock_selector = AsyncMock(side_effect=[mock_db1, mock_db2])
        type(mock_fs).database = mock_selector
        threshold = 3
        fd = FailureDetectorAsyncWrapper(CommandFailureDetector(threshold, 1))
        ed = EventDispatcher()
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=ed,
            auto_fallback_interval=0.1,
            command_retry=Retry(
                NoBackoff(),
                threshold,
                supported_errors=(RedisClusterUnreachableError,),
            ),
        )
        fd.set_command_executor(command_executor=executor)

        assert await executor.execute_command("SET", "key", "value") == "OK1"
        assert await executor.execute_command("SET", "key", "value") == "OK2"
        assert mock_selector.call_count == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_does_not_fallback_on_cluster_programming_error(
        self, mock_db, mock_db1, mock_db2, mock_fs
    ):
        """
        A plain ``RedisClusterException`` is a deterministic caller error - a
        cross-slot command, an unsupported method - not an unavailable database. It
        must be raised as is, without retries and without counting towards failure
        detection, even while the unreachable subtype is supported.
        """
        error = RedisClusterException("Keys in request don't hash to the same slot")
        mock_db1.client.execute_command = AsyncMock(side_effect=error)
        mock_selector = AsyncMock(return_value=mock_db1)
        type(mock_fs).database = mock_selector
        # A single registered failure would be enough to open the circuit, so the
        # circuit staying closed proves none was registered.
        fd = FailureDetectorAsyncWrapper(
            CommandFailureDetector(
                min_num_failures=1,
                failure_rate_threshold=0.0,
                failure_detection_window=1,
            )
        )
        ed = EventDispatcher()
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=ed,
            auto_fallback_interval=0.1,
            command_retry=Retry(
                NoBackoff(),
                3,
                supported_errors=(RedisClusterUnreachableError,),
            ),
        )
        fd.set_command_executor(command_executor=executor)

        with pytest.raises(RedisClusterException, match="same slot"):
            await executor.execute_command("MGET", "key1", "key2")

        assert mock_db1.client.execute_command.call_count == 1
        assert mock_selector.call_count == 1
        assert mock_db1.circuit.state == CBState.CLOSED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_pubsub_method_forwards_multiple_args(
        self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            command_retry=Retry(NoBackoff(), 0),
        )

        await executor.set_active_database(mock_db1, GeoFailoverReason.MANUAL)
        executor.active_pubsub = AsyncMock()

        # Subscribing to more than one channel used to raise TypeError,
        # because execute_pubsub_method star-unpacked the args tuple into
        # _execute_with_failure_detection, which only accepts one extra arg.
        await executor.execute_pubsub_method("subscribe", "channel-1", "channel-2")

        executor.active_pubsub.subscribe.assert_called_once_with(
            "channel-1", "channel-2"
        )
        mock_fd.register_command_execution.assert_called_once_with(
            ("channel-1", "channel-2")
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    @pytest.mark.parametrize("method", ["unsubscribe", "punsubscribe", "sunsubscribe"])
    async def test_execute_pubsub_method_awaits_returned_awaitable(
        self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed, method
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            command_retry=Retry(NoBackoff(), 0),
        )

        await executor.set_active_database(mock_db1, GeoFailoverReason.MANUAL)
        sent = []

        async def send(*args):
            sent.append(args)
            return "sent"

        # asyncio PubSub.unsubscribe/punsubscribe/sunsubscribe are plain
        # methods that return an awaitable rather than coroutine functions.
        pubsub = Mock()
        setattr(pubsub, method, lambda *args: send(*args))
        executor.active_pubsub = pubsub

        assert await executor.execute_pubsub_method(method, "channel-1") == "sent"
        assert sent == [("channel-1",)]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_pubsub_method_failure_reports_intact_command_tuple(
        self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            command_retry=Retry(NoBackoff(), 0),
        )

        await executor.set_active_database(mock_db1, GeoFailoverReason.MANUAL)
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe.side_effect = ConnectionError("boom")
        executor.active_pubsub = mock_pubsub

        # With a single channel the old code passed a bare string as `cmds`,
        # so the failure event star-unpacked it into individual characters
        # instead of the channel name.
        with pytest.raises(ConnectionError):
            await executor.execute_pubsub_method("subscribe", "channel-1")

        event = mock_ed.dispatch_async.call_args[0][0]
        assert isinstance(event, OnCommandsFailEvent)
        assert event.commands == ("channel-1",)
