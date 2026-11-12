import asyncio
import threading
from time import monotonic, sleep

import pytest

from redis.background import BackgroundScheduler

# The scheduler waits with ``loop.call_later``, whose timer can fire a hair
# before the requested delay on a coarse event loop clock. Far below any
# interval a test asserts on, so it cannot mask a callback that ran early.
EVENT_LOOP_TIMER_SLACK = 0.005


def _wait_until(predicate, timeout=5.0):
    """Poll until ``predicate()`` is true; fail if the deadline passes first.

    The deadline only matters on a stalled runner: a passing run returns as
    soon as the condition holds.
    """
    deadline = monotonic() + timeout
    while not predicate():
        assert monotonic() < deadline, "condition not met before the deadline"
        sleep(0.002)


async def _wait_until_async(predicate, timeout=5.0):
    """Async twin of ``_wait_until``."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        assert loop.time() < deadline, "condition not met before the deadline"
        await asyncio.sleep(0.002)


@pytest.mark.fixed_client
class TestBackgroundScheduler:
    def test_run_once(self):
        execute_counter = 0
        one = "arg1"
        two = 9999

        def callback(arg1: str, arg2: int):
            nonlocal execute_counter
            nonlocal one
            nonlocal two

            execute_counter += 1

            assert arg1 == one
            assert arg2 == two

        scheduler = BackgroundScheduler()
        registered = monotonic()
        scheduler.run_once(0.1, callback, one, two)
        assert execute_counter == 0

        _wait_until(lambda: execute_counter >= 1)
        called = monotonic()

        assert execute_counter == 1
        assert called - registered >= 0.1 - EVENT_LOOP_TIMER_SLACK

    @pytest.mark.parametrize(
        "interval,min_call_count",
        [
            (0.012, 2),
            (0.035, 1),
        ],
    )
    def test_run_recurring(self, interval, min_call_count):
        execute_counter = []
        one = "arg1"
        two = 9999

        def callback(arg1: str, arg2: int):
            nonlocal execute_counter
            nonlocal one
            nonlocal two

            execute_counter.append(monotonic())

            assert arg1 == one
            assert arg2 == two

        scheduler = BackgroundScheduler()
        registered = monotonic()
        scheduler.run_recurring(interval, callback, one, two)
        assert len(execute_counter) == 0

        _wait_until(lambda: len(execute_counter) >= min_call_count)
        # the first call waits out the interval, rather than firing on registration
        assert execute_counter[0] - registered >= interval - EVENT_LOOP_TIMER_SLACK

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "interval,min_call_count",
        [
            (0.012, 2),
            (0.035, 1),
        ],
    )
    async def test_run_recurring_async(self, interval, min_call_count):
        execute_counter = []
        one = "arg1"
        two = 9999

        async def callback(arg1: str, arg2: int):
            nonlocal execute_counter
            nonlocal one
            nonlocal two

            execute_counter.append(monotonic())

            assert arg1 == one
            assert arg2 == two

        scheduler = BackgroundScheduler()
        registered = monotonic()
        await scheduler.run_recurring_async(interval, callback, one, two)
        assert len(execute_counter) == 0

        await _wait_until_async(lambda: len(execute_counter) >= min_call_count)
        # the first call waits out the interval, rather than firing on registration
        assert execute_counter[0] - registered >= interval - EVENT_LOOP_TIMER_SLACK

    @pytest.mark.parametrize(
        "interval,min_call_count",
        [
            (0.012, 2),
            (0.035, 1),
        ],
    )
    def test_run_recurring_coro(self, interval, min_call_count):
        """
        Test that run_recurring_coro executes coroutines in a background thread.
        This is used by sync MultiDBClient for async health checks.
        """
        execute_counter = []
        one = "arg1"
        two = 9999

        async def coro_callback(arg1: str, arg2: int):
            nonlocal execute_counter
            execute_counter.append(monotonic())
            assert arg1 == one
            assert arg2 == two

        scheduler = BackgroundScheduler()
        registered = monotonic()
        scheduler.run_recurring_coro(interval, coro_callback, one, two)
        assert len(execute_counter) == 0

        _wait_until(lambda: len(execute_counter) >= min_call_count)
        scheduler.stop()
        # the first call waits out the interval, rather than firing on registration
        assert execute_counter[0] - registered >= interval - EVENT_LOOP_TIMER_SLACK

    def test_run_recurring_coro_runs_in_background_thread(self):
        """
        Verify that run_recurring_coro executes in a separate thread,
        not blocking the main thread.
        """
        main_thread_id = threading.current_thread().ident
        coro_thread_ids = []

        async def coro_callback():
            coro_thread_ids.append(threading.current_thread().ident)

        scheduler = BackgroundScheduler()
        scheduler.run_recurring_coro(0.01, coro_callback)

        _wait_until(lambda: len(coro_thread_ids) >= 1)

        # Coroutine should run in a different thread than main
        assert all(tid != main_thread_id for tid in coro_thread_ids)
        scheduler.stop()

    def test_run_recurring_coro_supports_concurrent_execution(self):
        """
        Verify that multiple coroutines scheduled in the same background loop
        can run concurrently (not blocking each other).
        """
        execution_order = []
        lock = threading.Lock()

        async def slow_coro(name: str):
            with lock:
                execution_order.append(f"{name}_start")
            await asyncio.sleep(0.02)  # Simulate async I/O
            with lock:
                execution_order.append(f"{name}_end")

        async def fast_coro(name: str):
            with lock:
                execution_order.append(f"{name}_start")
            await asyncio.sleep(0.005)
            with lock:
                execution_order.append(f"{name}_end")

        scheduler = BackgroundScheduler()
        # Schedule both coroutines - they share the same event loop
        scheduler.run_recurring_coro(0.01, slow_coro, "slow")
        scheduler.run_recurring_coro(0.01, fast_coro, "fast")

        # Both coroutines should have executed
        _wait_until(
            lambda: "slow_start" in execution_order and "fast_start" in execution_order
        )
        scheduler.stop()

    def test_run_recurring_coro_with_timeout(self):
        """
        Test that asyncio.wait_for works correctly within run_recurring_coro,
        which is critical for health check timeouts.
        """
        results = []

        async def slow_operation():
            await asyncio.sleep(1.0)  # Takes 1 second
            return "completed"

        async def coro_with_timeout():
            try:
                result = await asyncio.wait_for(slow_operation(), timeout=0.01)
                results.append(("success", result))
            except asyncio.TimeoutError:
                results.append(("timeout", None))

        scheduler = BackgroundScheduler()
        scheduler.run_recurring_coro(0.02, coro_with_timeout)

        # Should have at least one timeout result
        _wait_until(lambda: len(results) >= 1)
        scheduler.stop()
        assert all(r[0] == "timeout" for r in results)

    def test_run_recurring_coro_stops_cleanly(self):
        """
        Verify that stop() properly terminates the background event loop.
        """
        execute_counter = []

        async def coro_callback():
            execute_counter.append(1)

        scheduler = BackgroundScheduler()
        scheduler.run_recurring_coro(0.01, coro_callback)

        _wait_until(lambda: len(execute_counter) >= 1)
        count_before_stop = len(execute_counter)

        scheduler.stop()
        # a negative window: no new execution may start during it
        sleep(0.05)

        # No more executions after stop
        count_after_stop = len(execute_counter)
        # Allow for at most 1 more execution due to timing
        assert count_after_stop <= count_before_stop + 1

    def test_run_recurring_coro_exception_does_not_stop_scheduler(self):
        """
        Verify that exceptions in coroutines don't crash the scheduler.
        """
        success_count = []
        error_count = []

        async def flaky_coro():
            if len(success_count) % 2 == 0:
                error_count.append(1)
                raise ValueError("Simulated error")
            success_count.append(1)

        scheduler = BackgroundScheduler()
        scheduler.run_recurring_coro(0.01, flaky_coro)

        # The scheduler keeps running after a coroutine raises
        _wait_until(lambda: len(success_count) + len(error_count) >= 2)
        scheduler.stop()

    def test_run_recurring_coro_prevents_overlapping_executions(self):
        """
        Verify that the next execution is scheduled only after the current one completes,
        preventing overlapping runs when execution takes longer than the interval.
        """
        execution_log = []
        lock = threading.Lock()

        async def slow_coro():
            with lock:
                execution_log.append(("start", len(execution_log)))
            await asyncio.sleep(0.05)  # Takes 50ms, longer than 10ms interval
            with lock:
                execution_log.append(("end", len(execution_log)))

        scheduler = BackgroundScheduler()
        # Interval is 10ms but execution takes 50ms
        scheduler.run_recurring_coro(0.01, slow_coro)

        # Wait for a few runs, so there is something to check for overlaps
        _wait_until(lambda: sum(1 for event, _ in execution_log if event == "end") >= 3)
        scheduler.stop()

        # Verify no overlapping: each "start" should be followed by "end" before next "start"
        # With overlap prevention, pattern should be: start, end, start, end, ...
        starts = [i for i, (event, _) in enumerate(execution_log) if event == "start"]
        ends = [i for i, (event, _) in enumerate(execution_log) if event == "end"]

        # Each start should have a corresponding end before the next start
        for i, start_idx in enumerate(starts[:-1]):
            # Find the end that follows this start
            corresponding_ends = [e for e in ends if e > start_idx]
            assert len(corresponding_ends) > 0, "Start without corresponding end"
            # The end should come before the next start
            next_start_idx = starts[i + 1]
            assert corresponding_ends[0] < next_start_idx, (
                "Overlapping execution detected"
            )
