import asyncio
import time
from unittest.mock import AsyncMock, Mock

import pytest

from redis.asyncio import Redis
from redis.asyncio.cluster import ClusterNode as AsyncClusterNode
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.asyncio.connection import SSLConnection as AsyncSSLConnection
from redis.asyncio.multidb.database import Database
from redis.asyncio.multidb.healthcheck import (
    PingHealthCheck,
    LagAwareHealthCheck,
    HealthCheck,
    HealthyAllPolicy,
    HealthyMajorityPolicy,
    HealthyAnyPolicy,
    relaxed_health_check_budget,
    database_maint_notifications_config,
    _MaintenanceWindowListener,
    _MaintenanceWindowTracker,
)
from redis.event import MaintenanceCompletedEvent, MaintenanceStartedEvent
from redis.http.http_client import HttpError
from redis.maint_notifications import (
    MaintenanceState,
    MaintNotificationsConfig,
    NodeMigratingNotification,
)
from redis.multidb.circuit import State as CBState
from redis.exceptions import ConnectionError, ResponseError
from redis.multidb.exception import UnhealthyDatabaseException


def _mock_cluster_node(ping_result=True):
    """Mock of an async cluster node, bound to the real ``ClusterNode`` interface."""
    node = Mock(spec=AsyncClusterNode)
    node.execute_command = AsyncMock(return_value=ping_result)
    return node


def _mock_cluster_client(*nodes):
    """Mock of an async cluster client, bound to the real ``RedisCluster`` interface."""
    client = Mock(spec=AsyncRedisCluster)
    client.initialize = AsyncMock(return_value=client)
    client.get_nodes = Mock(return_value=list(nodes))
    return client


def _configure_mock_health_check(mock_hc, probes=3, delay=0.01, timeout=1.0):
    """Helper to configure mock health check with required properties."""
    mock_hc.health_check_probes = probes
    mock_hc.health_check_delay = delay
    mock_hc.health_check_timeout = timeout
    # check_health is async, use AsyncMock
    mock_hc.check_health = AsyncMock(return_value=True)
    return mock_hc


def _maintenance_event(
    event_cls,
    source,
    state=MaintenanceState.MAINTENANCE,
    relaxed_timeout=5.0,
    notification=None,
):
    """
    The event a maintenance handler dispatches for ``source``: a pool for a
    MOVING window, a connection for a MAINTENANCE one.
    """
    if notification is None:
        notification = NodeMigratingNotification(id=1, ttl=5)
    is_moving = state is MaintenanceState.MOVING
    return event_cls(
        connection_pool=source if is_moving else None,
        connection=None if is_moving else source,
        state=state,
        notification=notification,
        config=MaintNotificationsConfig(enabled=True, relaxed_timeout=relaxed_timeout),
    )


def _sleeping_health_check(seconds):
    async def check_health(database, client=None):
        await asyncio.sleep(seconds)
        return True

    return check_health


@pytest.mark.fixed_client
class TestHealthyAllPolicy:
    @pytest.mark.asyncio
    async def test_policy_returns_true_for_all_successful_probes(self):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc1.check_health.return_value = True
        mock_hc2.check_health.return_value = True
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()
        assert await policy.execute([mock_hc1, mock_hc2], mock_db)
        # Both health checks run in parallel, each with 3 probes
        assert mock_hc1.check_health.call_count == 3
        assert mock_hc2.check_health.call_count == 3

    @pytest.mark.asyncio
    async def test_policy_returns_false_on_failed_probe(self):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc1.check_health.side_effect = [True, True, False]
        mock_hc2.check_health.return_value = True
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()
        # Policy returns False because mock_hc1 fails on the third probe
        assert not await policy.execute([mock_hc1, mock_hc2], mock_db)
        # Both health checks run in parallel
        assert mock_hc1.check_health.call_count == 3
        assert mock_hc2.check_health.call_count == 3

    @pytest.mark.asyncio
    async def test_policy_raise_unhealthy_database_exception(self):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc1.check_health.side_effect = [True, True, ConnectionError]
        mock_hc2.check_health.return_value = True
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()
        with pytest.raises(UnhealthyDatabaseException, match="Unhealthy database"):
            await policy.execute([mock_hc1, mock_hc2], mock_db)

    @pytest.mark.asyncio
    async def test_policy_raises_exception_when_health_check_times_out(self):
        """
        Verify that health_check_timeout is respected and raises UnhealthyDatabaseException
        when a health check takes longer than the configured timeout.
        """
        import asyncio

        async def slow_health_check(database, connection=None):
            await asyncio.sleep(0.5)  # Sleep longer than the timeout
            return True

        # Configure with a very short timeout (0.1 seconds)
        mock_hc = _configure_mock_health_check(
            Mock(spec=HealthCheck), probes=1, delay=0.01, timeout=0.1
        )
        mock_hc.check_health.side_effect = slow_health_check
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()
        with pytest.raises(
            UnhealthyDatabaseException, match="Unhealthy database"
        ) as exc_info:
            await policy.execute([mock_hc], mock_db)

        # Verify the original exception is a TimeoutError
        assert isinstance(exc_info.value.original_exception, asyncio.TimeoutError)

    @pytest.mark.asyncio
    async def test_policy_succeeds_when_health_check_completes_within_timeout(self):
        """
        Verify that health checks that complete within the timeout succeed normally.
        """
        import asyncio

        async def fast_health_check(database, connection=None):
            await asyncio.sleep(0.01)  # Sleep much less than the timeout
            return True

        # Configure with a generous timeout (1 second)
        mock_hc = _configure_mock_health_check(
            Mock(spec=HealthCheck), probes=1, delay=0.01, timeout=1.0
        )
        mock_hc.check_health.side_effect = fast_health_check
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()
        result = await policy.execute([mock_hc], mock_db)
        assert result is True


@pytest.mark.fixed_client
class TestMaintenanceAwareBudget:
    """
    The health check budget follows the maintenance windows the probe client
    reports: relaxed while a window is active, plain otherwise.

    Success paths have wide margins, so a stalled runner cannot fail them: a
    probe meant to finish does so seconds inside its relaxed budget. Elapsed-time
    bounds sit about a second above the expected cutoff, well below the probe
    sleeps, so a budget that is not applied still fails them.
    """

    def _policy_with_tracker(self, database):
        policy = HealthyAllPolicy()
        tracker = _MaintenanceWindowTracker()
        policy._maintenance_trackers[id(database)] = tracker
        return policy, _MaintenanceWindowListener(tracker)

    def _slow_check(self, sleep, timeout=0.1):
        mock_hc = _configure_mock_health_check(
            Mock(spec=HealthCheck), probes=1, delay=0.01, timeout=timeout
        )
        mock_hc.check_health.side_effect = _sleeping_health_check(sleep)
        return mock_hc

    def test_relaxed_health_check_budget_holds_every_probe_at_relaxed_length(self):
        mock_hc = _configure_mock_health_check(
            Mock(spec=HealthCheck), probes=3, delay=0.5
        )
        assert relaxed_health_check_budget(mock_hc, 10) == pytest.approx(31.5)

    @pytest.mark.asyncio
    async def test_active_window_extends_budget(self):
        mock_db = Mock(spec=Database)
        policy, listener = self._policy_with_tracker(mock_db)
        listener.listen(
            _maintenance_event(MaintenanceStartedEvent, object(), relaxed_timeout=5.0)
        )

        # Sleeps past the 0.1 s budget, within the relaxed 1 * (5 + 0.01) s
        assert await policy.execute([self._slow_check(0.3)], mock_db) is True

    @pytest.mark.asyncio
    async def test_window_started_during_probe_extends_budget(self):
        """The notification arrives inside the stalled probe, as a push does."""
        mock_db = Mock(spec=Database)
        policy, listener = self._policy_with_tracker(mock_db)
        started = _maintenance_event(
            MaintenanceStartedEvent, object(), relaxed_timeout=5.0
        )

        async def stalled_probe(database, client=None):
            listener.listen(started)
            await asyncio.sleep(0.3)
            return True

        mock_hc = self._slow_check(0.3)
        mock_hc.check_health.side_effect = stalled_probe

        assert await policy.execute([mock_hc], mock_db) is True

    @pytest.mark.asyncio
    async def test_window_started_during_first_probe_is_honoured(self):
        """
        The first check of a database is what creates its client, and the
        tracker with it; a notification arriving during that check must count.
        """
        mock_db = Mock(spec=Database)
        policy = HealthyAllPolicy()
        started = _maintenance_event(
            MaintenanceStartedEvent, object(), relaxed_timeout=5.0
        )

        async def get_client_creating_tracker(database):
            tracker = _MaintenanceWindowTracker()
            policy._maintenance_trackers[id(database)] = tracker
            _MaintenanceWindowListener(tracker).listen(started)
            return AsyncMock()

        policy.get_client = get_client_creating_tracker

        assert await policy.execute([self._slow_check(0.3)], mock_db) is True

    @pytest.mark.asyncio
    async def test_relaxed_budget_is_bounded(self):
        mock_db = Mock(spec=Database)
        policy, listener = self._policy_with_tracker(mock_db)
        listener.listen(
            _maintenance_event(MaintenanceStartedEvent, object(), relaxed_timeout=0.2)
        )

        start = time.monotonic()
        with pytest.raises(UnhealthyDatabaseException) as exc_info:
            # Sleeps past the relaxed 1 * (0.2 + 0.01) s budget as well
            await policy.execute([self._slow_check(10.0)], mock_db)

        assert isinstance(exc_info.value.original_exception, asyncio.TimeoutError)
        assert time.monotonic() - start < 1.5

    @pytest.mark.asyncio
    async def test_blocking_relaxed_timeout_relaxes_until_window_expires(self):
        """With blocking sockets the notification's expiry is the only bound."""
        mock_db = Mock(spec=Database)
        policy, listener = self._policy_with_tracker(mock_db)
        source = object()

        def blocking_window(expires_in):
            notification = NodeMigratingNotification(id=1, ttl=5)
            notification.expire_at = time.monotonic() + expires_in
            return _maintenance_event(
                MaintenanceStartedEvent,
                source,
                relaxed_timeout=None,
                notification=notification,
            )

        # far off, so only the shorter window announced below can end a check early
        listener.listen(blocking_window(30.0))
        assert await policy.execute([self._slow_check(0.2)], mock_db) is True

        # the same source announces a window that is about to expire
        listener.listen(blocking_window(0.5))
        start = time.monotonic()
        with pytest.raises(UnhealthyDatabaseException) as exc_info:
            await policy.execute([self._slow_check(30.0)], mock_db)

        assert isinstance(exc_info.value.original_exception, asyncio.TimeoutError)
        assert time.monotonic() - start < 1.5

    @pytest.mark.asyncio
    async def test_window_outlives_notification_ttl_until_completed(self):
        """
        The connection stays relaxed until the completion arrives, however long
        the maintenance outruns its announced time-to-live; so does the budget.
        """
        mock_db = Mock(spec=Database)
        policy, listener = self._policy_with_tracker(mock_db)
        listener.listen(
            _maintenance_event(
                MaintenanceStartedEvent,
                object(),
                relaxed_timeout=5.0,
                notification=NodeMigratingNotification(id=1, ttl=0),
            )
        )

        assert await policy.execute([self._slow_check(0.3)], mock_db) is True

    @pytest.mark.asyncio
    async def test_window_with_relaxation_disabled_does_not_extend_budget(self):
        """A handoff is still reported with relaxed_timeout=-1, for the reconnect."""
        mock_db = Mock(spec=Database)
        policy, listener = self._policy_with_tracker(mock_db)
        listener.listen(
            _maintenance_event(
                MaintenanceStartedEvent,
                object(),
                state=MaintenanceState.MOVING,
                relaxed_timeout=-1,
            )
        )

        assert policy._maintenance_trackers[id(mock_db)].active_windows() == []
        with pytest.raises(UnhealthyDatabaseException) as exc_info:
            await policy.execute([self._slow_check(0.5)], mock_db)

        assert isinstance(exc_info.value.original_exception, asyncio.TimeoutError)

    @pytest.mark.asyncio
    async def test_completed_window_restores_plain_budget_mid_check(self):
        """
        A probe still hung once the maintenance has completed must not get to
        use up the relaxed budget: the plain one applies again.
        """
        mock_db = Mock(spec=Database)
        policy, listener = self._policy_with_tracker(mock_db)
        source = object()
        listener.listen(
            _maintenance_event(MaintenanceStartedEvent, source, relaxed_timeout=50)
        )

        async def complete_soon():
            await asyncio.sleep(0.3)
            listener.listen(_maintenance_event(MaintenanceCompletedEvent, source))

        completion = asyncio.create_task(complete_soon())
        start = time.monotonic()
        with pytest.raises(UnhealthyDatabaseException) as exc_info:
            # Would be allowed 1 * (50 + 0.01) s while the window is active
            await policy.execute([self._slow_check(30.0)], mock_db)
        await completion

        assert isinstance(exc_info.value.original_exception, asyncio.TimeoutError)
        assert time.monotonic() - start < 1.5

    @pytest.mark.asyncio
    async def test_completed_window_restores_budget(self):
        mock_db = Mock(spec=Database)
        policy, listener = self._policy_with_tracker(mock_db)
        source = object()
        listener.listen(_maintenance_event(MaintenanceStartedEvent, source))
        listener.listen(_maintenance_event(MaintenanceCompletedEvent, source))

        with pytest.raises(UnhealthyDatabaseException) as exc_info:
            await policy.execute([self._slow_check(0.5)], mock_db)

        assert isinstance(exc_info.value.original_exception, asyncio.TimeoutError)

    @pytest.mark.asyncio
    async def test_moving_and_maintenance_windows_are_independent(self):
        mock_db = Mock(spec=Database)
        policy, listener = self._policy_with_tracker(mock_db)
        pool, connection = object(), object()
        listener.listen(
            _maintenance_event(
                MaintenanceStartedEvent, pool, state=MaintenanceState.MOVING
            )
        )
        listener.listen(_maintenance_event(MaintenanceStartedEvent, connection))

        # The handoff completes while the migration is still on
        listener.listen(
            _maintenance_event(
                MaintenanceCompletedEvent, pool, state=MaintenanceState.MOVING
            )
        )
        assert await policy.execute([self._slow_check(0.3)], mock_db) is True

        listener.listen(_maintenance_event(MaintenanceCompletedEvent, connection))
        with pytest.raises(UnhealthyDatabaseException):
            await policy.execute([self._slow_check(0.5)], mock_db)

    @pytest.mark.asyncio
    async def test_database_without_maintenance_notifications_keeps_plain_budget(
        self,
    ):
        mock_db = Mock(spec=Database)
        policy = HealthyAllPolicy()

        start = time.monotonic()
        with pytest.raises(UnhealthyDatabaseException) as exc_info:
            await policy.execute([self._slow_check(5.0)], mock_db)

        assert isinstance(exc_info.value.original_exception, asyncio.TimeoutError)
        # the plain 0.1 s budget applied, not the 5 s the probe would take
        assert time.monotonic() - start < 1.0

    @pytest.mark.asyncio
    async def test_outer_cancellation_cancels_health_check(self):
        """
        Cancelling the policy cancels the running check and waits for its
        cleanup to finish, so the check cannot outlive the policy call.
        """
        mock_db = Mock(spec=Database)
        policy = HealthyAllPolicy()
        cleanup_done = asyncio.Event()

        async def hanging_probe(database, client=None):
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                # Cleanup that takes a moment, as closing a connection would
                await asyncio.sleep(0.05)
                cleanup_done.set()
                raise
            return True

        mock_hc = self._slow_check(10, timeout=5.0)
        mock_hc.check_health.side_effect = hanging_probe

        task = asyncio.create_task(policy.execute([mock_hc], mock_db))
        await asyncio.sleep(0.05)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert cleanup_done.is_set()


@pytest.mark.fixed_client
class TestDatabaseMaintNotificationsConfig:
    """The config is resolved the way each client type carries it."""

    def test_standalone_client_carries_it_in_connection_kwargs(self):
        config = MaintNotificationsConfig(enabled=True)
        client = Mock(spec=Redis)
        client.get_connection_kwargs.return_value = {
            "maint_notifications_config": config
        }
        assert database_maint_notifications_config(client) is config

    def test_cluster_client_carries_it_as_an_attribute(self):
        config = MaintNotificationsConfig(enabled=True)
        client = Mock(spec=AsyncRedisCluster)
        client.maint_notifications_config = config
        client.get_connection_kwargs.return_value = {}
        assert database_maint_notifications_config(client) is config

    def test_standalone_client_carries_it_on_the_pool_handler(self):
        config = MaintNotificationsConfig(enabled=True)
        client = Mock(spec=Redis)
        client.connection_pool = Mock()
        client.connection_pool._maint_notifications_pool_handler.config = config
        client.get_connection_kwargs.return_value = {}
        assert database_maint_notifications_config(client) is config

    def test_client_without_config(self):
        client = Mock(spec=Redis)
        client.get_connection_kwargs.return_value = {}
        assert database_maint_notifications_config(client) is None


@pytest.mark.fixed_client
class TestHealthyMajorityPolicy:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "probes,hc1_side_effect,hc2_side_effect,expected_result",
        [
            (3, [True, False, False], [True, True, True], False),
            (3, [True, True, True], [True, False, False], False),
            (3, [True, False, True], [True, True, True], True),
            (3, [True, True, True], [True, False, True], True),
            (3, [True, True, False], [True, False, True], True),
            (4, [True, True, False, False], [True, True, True, True], False),
            (4, [True, True, True, True], [True, True, False, False], False),
            (4, [False, True, True, True], [True, True, True, True], True),
            (4, [True, True, True, True], [True, False, True, True], True),
            (4, [False, True, True, True], [True, True, False, True], True),
        ],
        ids=[
            "HC1 - no majority - odd",
            "HC2 - no majority - odd",
            "HC1 - majority- odd",
            "HC2 - majority - odd",
            "HC1 + HC2 - majority - odd",
            "HC1 - no majority - even",
            "HC2 - no majority - even",
            "HC1 - majority - even",
            "HC2 - majority - even",
            "HC1 + HC2 - majority - even",
        ],
    )
    async def test_policy_returns_true_for_majority_successful_probes(
        self,
        probes,
        hc1_side_effect,
        hc2_side_effect,
        expected_result,
    ):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=probes)
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=probes)
        mock_hc1.check_health.side_effect = hc1_side_effect
        mock_hc2.check_health.side_effect = hc2_side_effect
        mock_db = Mock(spec=Database)

        policy = HealthyMajorityPolicy()
        assert await policy.execute([mock_hc1, mock_hc2], mock_db) == expected_result
        # Both health checks run in parallel; call counts may vary due to early returns
        assert mock_hc1.check_health.call_count >= 1
        assert mock_hc2.check_health.call_count >= 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "probes,hc1_side_effect,hc2_side_effect",
        [
            (3, [True, ConnectionError, ConnectionError], [True, True, True]),
            (3, [True, True, True], [True, ConnectionError, ConnectionError]),
            (
                4,
                [True, ConnectionError, ConnectionError, True],
                [True, True, True, True],
            ),
            (
                4,
                [True, True, True, True],
                [True, ConnectionError, ConnectionError, False],
            ),
        ],
        ids=[
            "HC1 - majority- odd",
            "HC2 - majority - odd",
            "HC1 - majority - even",
            "HC2 - majority - even",
        ],
    )
    async def test_policy_raise_unhealthy_database_exception_on_majority_probes_exceptions(
        self, probes, hc1_side_effect, hc2_side_effect
    ):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=probes)
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=probes)
        mock_hc1.check_health.side_effect = hc1_side_effect
        mock_hc2.check_health.side_effect = hc2_side_effect
        mock_db = Mock(spec=Database)

        policy = HealthyMajorityPolicy()
        with pytest.raises(UnhealthyDatabaseException, match="Unhealthy database"):
            await policy.execute([mock_hc1, mock_hc2], mock_db)


@pytest.mark.fixed_client
class TestHealthyAnyPolicy:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "hc1_side_effect,hc2_side_effect,expected_result",
        [
            # HC1 fails all probes, HC2 succeeds - overall False (both HCs must pass)
            ([False, False, False], [True, True, True], False),
            # Both fail all probes - overall False
            ([False, False, False], [False, False, False], False),
            # Both succeed on at least one probe - overall True
            ([False, True, True], [False, False, True], True),
            # Both succeed on first probe - overall True
            ([True, True, True], [True, True, True], True),
        ],
        ids=[
            "HC1 fails all, HC2 succeeds",
            "Both fail all probes",
            "Both succeed on at least one",
            "Both succeed on first",
        ],
    )
    async def test_policy_returns_true_for_any_successful_probe(
        self,
        hc1_side_effect,
        hc2_side_effect,
        expected_result,
    ):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc1.check_health.side_effect = hc1_side_effect
        mock_hc2.check_health.side_effect = hc2_side_effect
        mock_db = Mock(spec=Database)

        policy = HealthyAnyPolicy()
        assert await policy.execute([mock_hc1, mock_hc2], mock_db) == expected_result
        # Both health checks run in parallel; call counts depend on when success is found
        assert mock_hc1.check_health.call_count >= 1
        assert mock_hc2.check_health.call_count >= 1

    @pytest.mark.asyncio
    async def test_policy_raise_unhealthy_database_exception_if_exception_occurs_on_failed_health_check(
        self,
    ):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc1.check_health.side_effect = [False, False, ConnectionError]
        mock_hc2.check_health.side_effect = [False, False, False]
        mock_db = Mock(spec=Database)

        policy = HealthyAnyPolicy()
        with pytest.raises(UnhealthyDatabaseException, match="Unhealthy database"):
            await policy.execute([mock_hc1, mock_hc2], mock_db)


@pytest.mark.fixed_client
class TestPingHealthCheck:
    """Tests for PingHealthCheck with standalone and cluster clients."""

    @pytest.mark.asyncio
    async def test_standalone_database_is_healthy_on_ping_true(
        self, mock_client, mock_cb
    ):
        """
        Verify that PingHealthCheck returns True for standalone client when PING succeeds.
        """
        from redis.asyncio import Redis as AsyncRedis

        mock_hc_client = AsyncMock(spec=AsyncRedis)
        mock_hc_client.execute_command = AsyncMock(return_value=True)
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert await hc.check_health(db, mock_hc_client)
        mock_hc_client.execute_command.assert_called_once_with("PING")

    @pytest.mark.asyncio
    async def test_standalone_database_is_unhealthy_on_ping_false(
        self, mock_client, mock_cb
    ):
        """
        Verify that PingHealthCheck returns False for standalone client when PING fails.
        """
        from redis.asyncio import Redis as AsyncRedis

        mock_hc_client = AsyncMock(spec=AsyncRedis)
        mock_hc_client.execute_command = AsyncMock(return_value=False)
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert not await hc.check_health(db, mock_hc_client)
        mock_hc_client.execute_command.assert_called_once_with("PING")

    @pytest.mark.asyncio
    async def test_standalone_database_close_circuit_on_successful_healthcheck(
        self, mock_client, mock_cb
    ):
        """
        Verify health check succeeds for standalone client with HALF_OPEN circuit.
        """
        from redis.asyncio import Redis as AsyncRedis

        mock_hc_client = AsyncMock(spec=AsyncRedis)
        mock_hc_client.execute_command = AsyncMock(return_value=True)
        mock_cb.state = CBState.HALF_OPEN
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert await hc.check_health(db, mock_hc_client)
        mock_hc_client.execute_command.assert_called_once_with("PING")

    @pytest.mark.asyncio
    async def test_cluster_database_is_healthy_when_all_nodes_respond(
        self, mock_client, mock_cb
    ):
        """
        Verify that PingHealthCheck returns True for cluster when all nodes respond.
        """
        mock_node1 = _mock_cluster_node(ping_result=True)
        mock_node2 = _mock_cluster_node(ping_result=True)
        mock_node3 = _mock_cluster_node(ping_result=True)
        mock_hc_client = _mock_cluster_client(mock_node1, mock_node2, mock_node3)

        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert await hc.check_health(db, mock_hc_client)

        # All nodes should have been pinged
        mock_node1.execute_command.assert_awaited_once_with("PING")
        mock_node2.execute_command.assert_awaited_once_with("PING")
        mock_node3.execute_command.assert_awaited_once_with("PING")

    @pytest.mark.asyncio
    async def test_cluster_database_is_unhealthy_when_one_node_fails(
        self, mock_client, mock_cb
    ):
        """
        Verify that PingHealthCheck returns False for cluster when any node fails.
        """
        # Second node fails
        mock_node1 = _mock_cluster_node(ping_result=True)
        mock_node2 = _mock_cluster_node(ping_result=False)
        mock_node3 = _mock_cluster_node(ping_result=True)
        mock_hc_client = _mock_cluster_client(mock_node1, mock_node2, mock_node3)

        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert not await hc.check_health(db, mock_hc_client)

        # Every node is pinged: the pings are issued concurrently, so the failing
        # one cannot spare the nodes behind it.
        mock_node1.execute_command.assert_awaited_once_with("PING")
        mock_node2.execute_command.assert_awaited_once_with("PING")
        mock_node3.execute_command.assert_awaited_once_with("PING")

    @pytest.mark.asyncio
    async def test_cluster_health_check_client_is_initialized_before_ping(
        self, mock_client, mock_cb
    ):
        """
        Verify that the cluster topology is discovered before the nodes are pinged.

        The health check client is created lazily, so without an explicit
        initialization its node cache is empty.
        """
        mock_node = _mock_cluster_node(ping_result=True)
        mock_hc_client = _mock_cluster_client()
        # Empty node cache until the client has been initialized.
        nodes = []
        mock_hc_client.get_nodes = Mock(side_effect=lambda: nodes)
        mock_hc_client.initialize.side_effect = lambda: nodes.append(mock_node)

        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert await hc.check_health(db, mock_hc_client)

        mock_hc_client.initialize.assert_awaited_once()
        mock_node.execute_command.assert_awaited_once_with("PING")

    @pytest.mark.asyncio
    async def test_cluster_database_is_unhealthy_when_no_nodes_are_known(
        self, mock_client, mock_cb
    ):
        """
        Verify that a cluster with an empty node cache is reported as unhealthy.

        Without this, there would be nothing to ping and the database would be
        reported as healthy without a single successful probe.
        """
        mock_hc_client = _mock_cluster_client()

        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert not await hc.check_health(db, mock_hc_client)
        mock_hc_client.initialize.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cluster_nodes_are_pinged_concurrently(self, mock_client, mock_cb):
        """
        Verify that the nodes of a cluster are pinged concurrently.

        A single health_check_timeout covers the whole health check, so pinging
        the nodes one after another would make the budget scale with the size of
        the cluster.
        """
        in_flight = 0
        max_in_flight = 0

        async def tracking_ping(*args, **kwargs):
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            # Long enough for the overlap to be observable, short enough to keep
            # the test fast.
            await asyncio.sleep(0.01)
            in_flight -= 1
            return True

        nodes = [_mock_cluster_node() for _ in range(3)]

        for node in nodes:
            node.execute_command.side_effect = tracking_ping

        mock_hc_client = _mock_cluster_client(*nodes)

        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert await hc.check_health(db, mock_hc_client)
        assert max_in_flight == len(nodes), (
            f"Expected {len(nodes)} concurrent pings, got max {max_in_flight}"
        )

    @pytest.mark.asyncio
    async def test_cluster_ping_error_is_propagated(self, mock_client, mock_cb):
        """
        Verify that a failing PING propagates instead of being reported as an
        unhealthy result, so the policy can wrap it in an
        UnhealthyDatabaseException and the reason stays in the logs.

        The remaining nodes are still awaited: their pings are already in flight
        when the failure surfaces, and dropping them would leave tasks running
        unawaited.
        """
        mock_node1 = _mock_cluster_node(ping_result=True)
        mock_node2 = _mock_cluster_node()
        mock_node2.execute_command.side_effect = ConnectionError("Node is down")
        mock_node3 = _mock_cluster_node(ping_result=True)
        mock_hc_client = _mock_cluster_client(mock_node1, mock_node2, mock_node3)

        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        with pytest.raises(ConnectionError, match="Node is down"):
            await hc.check_health(db, mock_hc_client)

        mock_node1.execute_command.assert_awaited_once_with("PING")
        mock_node2.execute_command.assert_awaited_once_with("PING")
        mock_node3.execute_command.assert_awaited_once_with("PING")

    @pytest.mark.asyncio
    async def test_cluster_client_is_reset_when_ping_fails_with_connection_error(
        self, mock_client, mock_cb
    ):
        """
        Verify that a connectivity failure on a node probe closes the health
        check client. Direct node probes bypass the client's own
        topology-refresh path, so without the reset a node that was removed or
        replaced in the cluster would keep failing every probe against the
        stale topology.
        """
        mock_node1 = _mock_cluster_node(ping_result=True)
        mock_node2 = _mock_cluster_node()
        mock_node2.execute_command.side_effect = ConnectionError("Node is down")
        mock_hc_client = _mock_cluster_client(mock_node1, mock_node2)

        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        with pytest.raises(ConnectionError, match="Node is down"):
            await hc.check_health(db, mock_hc_client)

        mock_hc_client.aclose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cluster_client_is_kept_when_ping_fails_with_server_error(
        self, mock_client, mock_cb
    ):
        """
        Verify that a non-connectivity failure does not reset the health check
        client: the topology is not in question, so re-discovering it would
        only add load without changing the outcome.
        """
        mock_node = _mock_cluster_node()
        mock_node.execute_command.side_effect = ResponseError("some server error")
        mock_hc_client = _mock_cluster_client(mock_node)

        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        with pytest.raises(ResponseError, match="some server error"):
            await hc.check_health(db, mock_hc_client)

        mock_hc_client.aclose.assert_not_awaited()


@pytest.mark.fixed_client
class TestLagAwareHealthCheck:
    @pytest.mark.asyncio
    async def test_close_shuts_down_http_client(self):
        hc = LagAwareHealthCheck()
        hc._http_client = AsyncMock()
        await hc.close()
        hc._http_client.aclose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_database_is_healthy_when_bdb_matches_by_dns_name(
        self, mock_client, mock_cb
    ):
        """
        Ensures health check succeeds when /v1/bdbs contains an endpoint whose dns_name
        matches database host, and availability endpoint returns success.
        """
        host = "db1.example.com"
        mock_client.get_connection_kwargs.return_value = {"host": host}

        # Mock HttpClient used inside LagAwareHealthCheck
        mock_http = AsyncMock()
        mock_http.get.side_effect = [
            # First call: list of bdbs
            [
                {
                    "uid": "bdb-1",
                    "endpoints": [
                        {"dns_name": host, "addr": ["10.0.0.1", "10.0.0.2"]},
                    ],
                }
            ],
            # Second call: availability check (no JSON expected)
            None,
        ]

        hc = LagAwareHealthCheck(rest_api_port=1234, lag_aware_tolerance=150)
        # Inject our mocked http client
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")
        mock_hc_client = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        assert await hc.check_health(db, mock_hc_client) is True
        # Calls: first to list bdbs, then to availability. Both carry the database's
        # own base URL, so a concurrent check on another database cannot redirect
        # them at the wrong cluster.
        assert mock_http.get.call_count == 2
        first_call = mock_http.get.call_args_list[0]
        second_call = mock_http.get.call_args_list[1]
        assert first_call.args[0] == "https://healthcheck.example.com:1234/v1/bdbs"
        assert (
            second_call.args[0]
            == "https://healthcheck.example.com:1234/v1/bdbs/bdb-1/availability"
            "?extend_check=lag&availability_lag_tolerance_ms=150"
        )
        assert second_call.kwargs.get("expect_json") is False

    @pytest.mark.asyncio
    async def test_database_is_healthy_when_bdb_matches_by_addr(
        self, mock_client, mock_cb
    ):
        """
        Ensures health check succeeds when endpoint addr list contains the database host.
        """
        host_ip = "203.0.113.5"
        mock_client.get_connection_kwargs.return_value = {"host": host_ip}

        mock_http = AsyncMock()
        mock_http.get.side_effect = [
            [
                {
                    "uid": "bdb-42",
                    "endpoints": [
                        {"dns_name": "not-matching.example.com", "addr": [host_ip]},
                    ],
                }
            ],
            None,
        ]

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")
        mock_hc_client = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        assert await hc.check_health(db, mock_hc_client) is True
        assert mock_http.get.call_count == 2
        assert (
            mock_http.get.call_args_list[1].args[0]
            == "https://healthcheck.example.com:9443/v1/bdbs/bdb-42/availability"
            "?extend_check=lag&availability_lag_tolerance_ms=5000"
        )

    @pytest.mark.asyncio
    async def test_raises_value_error_when_no_matching_bdb(self, mock_client, mock_cb):
        """
        Ensures health check raises ValueError when there's no bdb matching the database host.
        """
        host = "db2.example.com"
        mock_client.get_connection_kwargs.return_value = {"host": host}

        mock_http = AsyncMock()
        # Return bdbs that do not match host by dns_name nor addr
        mock_http.get.return_value = [
            {
                "uid": "a",
                "endpoints": [{"dns_name": "other.example.com", "addr": ["10.0.0.9"]}],
            },
            {
                "uid": "b",
                "endpoints": [
                    {"dns_name": "another.example.com", "addr": ["10.0.0.10"]}
                ],
            },
        ]

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")
        mock_hc_client = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        with pytest.raises(ValueError, match="Could not find a matching bdb"):
            await hc.check_health(db, mock_hc_client)

        # Only the listing call should have happened, without a per-request
        # timeout override - the client's configured http_timeout applies.
        mock_http.get.assert_called_once_with(
            "https://healthcheck.example.com:9443/v1/bdbs"
        )

    @pytest.mark.asyncio
    async def test_propagates_http_error_from_availability(self, mock_client, mock_cb):
        """
        Ensures that any HTTP error raised by the availability endpoint is propagated.
        """
        host = "db3.example.com"
        mock_client.get_connection_kwargs.return_value = {"host": host}

        mock_http = AsyncMock()
        # First: list bdbs -> match by dns_name
        mock_http.get.side_effect = [
            [{"uid": "bdb-err", "endpoints": [{"dns_name": host, "addr": []}]}],
            # Second: availability -> raise HttpError
            HttpError(
                url=f"https://{host}:9443/v1/bdbs/bdb-err/availability",
                status=503,
                message="busy",
            ),
        ]

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")
        mock_hc_client = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        with pytest.raises(HttpError, match="busy") as e:
            await hc.check_health(db, mock_hc_client)
        assert e.value.status == 503

        # Ensure both calls were attempted
        assert mock_http.get.call_count == 2

    @pytest.mark.asyncio
    async def test_cluster_database_matches_bdb_by_configured_endpoint(self, mock_cb):
        """
        Ensures a cluster database is matched by the endpoint it was configured with,
        and not by the nodes it discovered. CLUSTER SLOTS advertises each node's own
        address, which on a public Redis Enterprise endpoint is neither the endpoint
        DNS name nor an address the bdb lists.
        """
        host = "redis-12000.cluster.example.com"
        client = _mock_cluster_client(_mock_cluster_node())
        client.startup_nodes = [AsyncClusterNode(host, 12000)]
        client.get_nodes.return_value = [
            Mock(spec=AsyncClusterNode, host="203.0.113.9")
        ]

        mock_http = AsyncMock()
        mock_http.get.side_effect = [
            [
                {
                    "uid": "bdb-cluster",
                    "endpoints": [{"dns_name": host, "addr": ["10.0.0.1"]}],
                }
            ],
            None,
        ]

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        db = Database(client, mock_cb, 1.0, "https://healthcheck.example.com")

        assert await hc.check_health(db, AsyncMock()) is True
        assert (
            mock_http.get.call_args_list[1].args[0]
            == "https://healthcheck.example.com:9443/v1/bdbs/bdb-cluster/availability"
            "?extend_check=lag&availability_lag_tolerance_ms=5000"
        )

    @pytest.mark.asyncio
    async def test_dns_name_match_is_preferred_over_addr_match(
        self, mock_client, mock_cb
    ):
        """
        Ensures the bdb owning the host as an endpoint DNS name wins over a bdb that
        only lists the host among its endpoint addresses. Databases hosted by the same
        cluster share the addresses their endpoints resolve to, so an address match is
        ambiguous while a DNS name belongs to a single bdb.
        """
        host = "db4.example.com"
        mock_client.get_connection_kwargs.return_value = {"host": host}

        mock_http = AsyncMock()
        mock_http.get.side_effect = [
            [
                {
                    "uid": "bdb-by-addr",
                    "endpoints": [{"dns_name": "other.example.com", "addr": [host]}],
                },
                {
                    "uid": "bdb-by-dns-name",
                    "endpoints": [{"dns_name": host, "addr": ["10.0.0.1"]}],
                },
            ],
            None,
        ]

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")

        assert await hc.check_health(db, AsyncMock()) is True
        assert (
            mock_http.get.call_args_list[1].args[0]
            == "https://healthcheck.example.com:9443/v1/bdbs/bdb-by-dns-name/availability"
            "?extend_check=lag&availability_lag_tolerance_ms=5000"
        )

    @pytest.mark.asyncio
    async def test_concurrent_checks_do_not_share_one_base_url(self, mock_cb):
        """
        Ensures two databases checked concurrently by the same health check instance each
        talk to their own cluster's REST API. MultiDBClient checks every database at once
        through a single shared LagAwareHealthCheck, so the target must travel with the
        request instead of being stashed on the shared HTTP client - otherwise one
        database's check overwrites the other's while it awaits a response, and the bdb
        listing comes back from the wrong cluster.
        """
        clusters = {
            "https://cluster-a.example.com": ("db-a.example.com", "bdb-a"),
            "https://cluster-b.example.com": ("db-b.example.com", "bdb-b"),
        }
        requested = []

        async def fake_get(path, *args, expect_json=True, timeout=None):
            requested.append(path)
            # Hand control back mid-check so the two checks interleave, the way the
            # thread-pool-backed HTTP client does on every request.
            await asyncio.sleep(0)
            base_url = path.rsplit("/v1/bdbs", 1)[0]
            dns_name, uid = clusters[base_url.rsplit(":", 1)[0]]
            if path.endswith("/v1/bdbs"):
                # A cluster's REST API only lists the bdbs that cluster hosts.
                return [{"uid": uid, "endpoints": [{"dns_name": dns_name, "addr": []}]}]
            return None

        mock_http = AsyncMock()
        mock_http.get = fake_get

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        databases = []
        for base_url, (dns_name, _) in clusters.items():
            client = Mock(spec=Redis)
            client.get_connection_kwargs.return_value = {"host": dns_name}
            databases.append(Database(client, mock_cb, 1.0, base_url))

        results = await asyncio.gather(
            *(hc.check_health(db, AsyncMock()) for db in databases)
        )

        assert results == [True, True]
        assert sorted(requested) == [
            "https://cluster-a.example.com:9443/v1/bdbs",
            "https://cluster-a.example.com:9443/v1/bdbs/bdb-a/availability"
            "?extend_check=lag&availability_lag_tolerance_ms=5000",
            "https://cluster-b.example.com:9443/v1/bdbs",
            "https://cluster-b.example.com:9443/v1/bdbs/bdb-b/availability"
            "?extend_check=lag&availability_lag_tolerance_ms=5000",
        ]


@pytest.mark.fixed_client
@pytest.mark.no_mock_connections
class TestAbstractHealthCheckPolicy:
    """
    Tests for AbstractHealthCheckPolicy public interface methods.
    These tests verify client lifecycle and caching behavior by
    directly manipulating the internal _clients dictionary.

    Uses @pytest.mark.no_mock_connections to disable the autouse fixture
    that mocks get_client.
    """

    @pytest.mark.asyncio
    async def test_get_client_caches_clients_by_database_id(self):
        """
        Verify that clients are cached and reused across multiple calls.
        Uses a real sync Redis client to trigger the standalone path.
        """
        # Create a real sync Redis client (without connecting)
        from redis import Redis as SyncRedis

        sync_client = SyncRedis(host="localhost", port=6379)

        mock_db = Mock(spec=Database)
        mock_db.client = sync_client

        policy = HealthyAllPolicy()

        # Manually inject a mock client to test caching behavior
        mock_redis = AsyncMock()
        db_id = id(mock_db)
        policy._clients[db_id] = mock_redis

        # First call should return cached client
        client1 = await policy.get_client(mock_db)
        # Second call should return same client
        client2 = await policy.get_client(mock_db)

        # Should be the same instance
        assert client1 is client2
        assert client1 is mock_redis

    @pytest.mark.asyncio
    async def test_get_client_creates_separate_clients_for_different_databases(self):
        """
        Verify that different databases get separate clients.
        """
        from redis import Redis as SyncRedis

        sync_client1 = SyncRedis(host="db1.local", port=6379)
        sync_client2 = SyncRedis(host="db2.local", port=6379)

        mock_db1 = Mock(spec=Database)
        mock_db1.client = sync_client1

        mock_db2 = Mock(spec=Database)
        mock_db2.client = sync_client2

        policy = HealthyAllPolicy()

        # Manually inject mock clients
        mock_redis1 = AsyncMock()
        mock_redis2 = AsyncMock()
        policy._clients[id(mock_db1)] = mock_redis1
        policy._clients[id(mock_db2)] = mock_redis2

        client1 = await policy.get_client(mock_db1)
        client2 = await policy.get_client(mock_db2)

        # Should get different clients
        assert client1 is mock_redis1
        assert client2 is mock_redis2
        assert client1 is not client2

    @pytest.mark.asyncio
    async def test_get_client_seeds_cluster_client_with_all_startup_nodes(self):
        """
        Verify the health-check cluster client is constructed from every
        configured startup node, not just the first: if the first seed is down
        while another is healthy, the probe must still be able to discover the
        cluster instead of marking the database unhealthy.
        """
        underlying = AsyncRedisCluster(
            startup_nodes=[
                AsyncClusterNode("node1.example.com", 7000),
                AsyncClusterNode("node2.example.com", 7001),
            ]
        )
        mock_db = Mock(spec=Database)
        mock_db.client = underlying

        policy = HealthyAllPolicy()
        try:
            client = await policy.get_client(mock_db)
            assert {(node.host, node.port) for node in client.startup_nodes} == {
                ("node1.example.com", 7000),
                ("node2.example.com", 7001),
            }
        finally:
            await policy.close()

    @pytest.mark.asyncio
    async def test_get_client_preserves_tls_for_standalone(self):
        """
        Verify the health-check client keeps the underlying client's TLS
        transport: the source connection kwargs carry TLS as
        connection_class=SSLConnection, which the kwargs filter drops, so
        without re-expressing it as ssl=True the probe would speak plaintext
        to a TLS port and mark a healthy database unavailable.
        """
        from redis import Redis as SyncRedis

        underlying = SyncRedis(host="localhost", port=6379, ssl=True)
        mock_db = Mock(spec=Database)
        mock_db.client = underlying

        policy = HealthyAllPolicy()
        try:
            client = await policy.get_client(mock_db)
            assert client.connection_pool.connection_class is AsyncSSLConnection
        finally:
            await policy.close()

    @pytest.mark.asyncio
    async def test_get_client_preserves_tls_for_cluster(self):
        """
        Same as the standalone TLS test, for the cluster path: the async
        cluster also represents TLS as connection_class=SSLConnection in its
        connection kwargs.
        """
        underlying = AsyncRedisCluster(
            startup_nodes=[AsyncClusterNode("node1.example.com", 7000)],
            ssl=True,
        )
        mock_db = Mock(spec=Database)
        mock_db.client = underlying

        policy = HealthyAllPolicy()
        try:
            client = await policy.get_client(mock_db)
            assert (
                client.connection_kwargs.get("connection_class") is AsyncSSLConnection
            )
        finally:
            await policy.close()

    @pytest.mark.asyncio
    async def test_get_client_reports_maintenance_windows_for_standalone(self):
        """
        The probe client gets a copy of the database's maintenance config that
        reports its windows to the policy; the database's config is untouched.
        """
        config = MaintNotificationsConfig(enabled=True, relaxed_timeout=7)
        mock_db = Mock(spec=Database)
        mock_db.client = Redis(protocol=3, maint_notifications_config=config)
        policy = HealthyAllPolicy()

        try:
            client = await policy.get_client(mock_db)
            probe_config = client.connection_pool.connection_kwargs[
                "maint_notifications_config"
            ]

            assert probe_config is not config
            assert probe_config.enabled is True
            assert probe_config.relaxed_timeout == 7
            assert config.event_dispatcher is None
            assert (
                client.connection_pool._maint_notifications_pool_handler.config
                is probe_config
            )

            listeners = probe_config.event_dispatcher._event_listeners_mapping
            assert [type(lst) for lst in listeners[MaintenanceStartedEvent]] == [
                _MaintenanceWindowListener
            ]
            assert (
                listeners[MaintenanceStartedEvent]
                == listeners[MaintenanceCompletedEvent]
            )

            # What the probe's handlers dispatch reaches this database's tracker
            tracker = policy._maintenance_trackers[id(mock_db)]
            probe_config.event_dispatcher.dispatch(
                _maintenance_event(MaintenanceStartedEvent, object(), relaxed_timeout=7)
            )
            assert len(tracker.active_windows()) == 1
        finally:
            await policy.close()

        assert policy._maintenance_trackers == {}

    @pytest.mark.asyncio
    async def test_get_client_reports_maintenance_windows_for_cluster(self):
        config = MaintNotificationsConfig(enabled=True, relaxed_timeout=7)
        mock_db = Mock(spec=Database)
        mock_db.client = AsyncRedisCluster(
            startup_nodes=[
                AsyncClusterNode("localhost", 7000),
                AsyncClusterNode("localhost", 7001),
            ],
            maint_notifications_config=config,
        )
        policy = HealthyAllPolicy()

        try:
            client = await policy.get_client(mock_db)
            probe_config = client.nodes_manager.connection_kwargs[
                "maint_notifications_config"
            ]

            assert probe_config is not config
            assert probe_config.event_dispatcher is not None
            assert config.event_dispatcher is None
            assert client.maint_notifications_config is probe_config
            for node in client.nodes_manager.startup_nodes.values():
                assert (
                    node.connection_kwargs["maint_notifications_config"] is probe_config
                )
            assert id(mock_db) in policy._maintenance_trackers
        finally:
            await policy.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "client_kwargs",
        [
            {"protocol": 2},
            {
                "protocol": 3,
                "maint_notifications_config": MaintNotificationsConfig(enabled=False),
            },
        ],
        ids=["no_config", "disabled"],
    )
    async def test_get_client_without_maintenance_notifications_tracks_nothing(
        self, client_kwargs
    ):
        """
        The probe must not enable notifications the database client has not:
        it gets an explicitly disabled config, not the RESP3 default of "auto".
        """
        mock_db = Mock(spec=Database)
        mock_db.client = Redis(**client_kwargs)
        policy = HealthyAllPolicy()

        try:
            client = await policy.get_client(mock_db)
            assert id(mock_db) not in policy._maintenance_trackers
            assert client.connection_pool._maint_notifications_pool_handler is None
            assert not client.connection_pool.maint_notifications_enabled()
        finally:
            await policy.close()

    @pytest.mark.asyncio
    async def test_get_client_keeps_cluster_notifications_disabled(self):
        """A cluster client keeps a disabled config as an attribute only."""
        mock_db = Mock(spec=Database)
        mock_db.client = AsyncRedisCluster(
            startup_nodes=[AsyncClusterNode("localhost", 7000)],
            maint_notifications_config=MaintNotificationsConfig(enabled=False),
        )
        policy = HealthyAllPolicy()

        try:
            client = await policy.get_client(mock_db)
            assert id(mock_db) not in policy._maintenance_trackers
            assert client.maint_notifications_config.enabled is False
        finally:
            await policy.close()

    @pytest.mark.asyncio
    async def test_close_closes_all_clients(self):
        """
        Verify that close() closes all cached clients.
        """
        policy = HealthyAllPolicy()

        # Manually inject a mock client
        mock_redis = AsyncMock()
        mock_redis.aclose = AsyncMock()
        policy._clients[123] = mock_redis

        # Close the policy
        await policy.close()

        # Client should be closed
        mock_redis.aclose.assert_called_once()

        # Internal clients dict should be cleared
        assert len(policy._clients) == 0

    @pytest.mark.asyncio
    async def test_close_closes_multiple_database_clients(self):
        """
        Verify that close() closes clients for all databases.
        """
        policy = HealthyAllPolicy()

        # Manually inject mock clients
        mock_redis1 = AsyncMock()
        mock_redis1.aclose = AsyncMock()
        mock_redis2 = AsyncMock()
        mock_redis2.aclose = AsyncMock()
        policy._clients[123] = mock_redis1
        policy._clients[456] = mock_redis2

        # Close the policy
        await policy.close()

        # Both clients should be closed
        mock_redis1.aclose.assert_called_once()
        mock_redis2.aclose.assert_called_once()
        assert len(policy._clients) == 0

    @pytest.mark.asyncio
    async def test_close_handles_close_exceptions_gracefully(self):
        """
        Verify that close() completes even if some clients fail to close.
        """
        policy = HealthyAllPolicy()

        # Manually inject mock clients
        mock_redis1 = AsyncMock()
        mock_redis1.aclose = AsyncMock(side_effect=ConnectionError("Close failed"))
        mock_redis2 = AsyncMock()
        mock_redis2.aclose = AsyncMock()
        policy._clients[123] = mock_redis1
        policy._clients[456] = mock_redis2

        # Should not raise even if one client fails to close
        await policy.close()

        # Both close attempts should have been made
        mock_redis1.aclose.assert_called_once()
        mock_redis2.aclose.assert_called_once()
        assert len(policy._clients) == 0

    @pytest.mark.asyncio
    async def test_execute_runs_health_checks_concurrently(self):
        """
        Verify that execute() runs multiple health checks concurrently.
        """

        # Track concurrent execution
        concurrent_count = 0
        max_concurrent = 0

        async def tracking_health_check(database, client=None):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.01)  # Small delay to allow overlap detection
            concurrent_count -= 1
            return True

        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=1)
        mock_hc1.check_health.side_effect = tracking_health_check

        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=1)
        mock_hc2.check_health.side_effect = tracking_health_check

        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()

        # Mock get_client to avoid real connections
        async def mock_get_client(database):
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock(return_value=True)
            return mock_client

        policy.get_client = mock_get_client

        result = await policy.execute([mock_hc1, mock_hc2], mock_db)

        assert result is True
        # Both health checks should have been called
        assert mock_hc1.check_health.call_count == 1
        assert mock_hc2.check_health.call_count == 1
        # If running concurrently, max_concurrent should be 2
        assert max_concurrent == 2, (
            f"Expected 2 concurrent executions, got max {max_concurrent}"
        )

    @pytest.mark.asyncio
    async def test_execute_applies_timeout_per_health_check(self):
        """
        Verify that execute() applies individual timeouts per health check.
        """
        import asyncio

        async def slow_health_check(database, client=None):
            await asyncio.sleep(1.0)  # Takes 1 second
            return True

        async def fast_health_check(database, client=None):
            await asyncio.sleep(0.01)
            return True

        # First health check times out
        mock_hc1 = _configure_mock_health_check(
            Mock(spec=HealthCheck), probes=1, timeout=0.05
        )
        mock_hc1.check_health.side_effect = slow_health_check

        # Second health check completes within timeout
        mock_hc2 = _configure_mock_health_check(
            Mock(spec=HealthCheck), probes=1, timeout=1.0
        )
        mock_hc2.check_health.side_effect = fast_health_check

        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()

        async def mock_get_client(database):
            mock_client = AsyncMock()
            return mock_client

        policy.get_client = mock_get_client

        # Should raise because first health check times out
        with pytest.raises(UnhealthyDatabaseException) as exc_info:
            await policy.execute([mock_hc1, mock_hc2], mock_db)

        assert isinstance(exc_info.value.original_exception, asyncio.TimeoutError)
