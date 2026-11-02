import asyncio
import threading
from datetime import datetime, timezone
from time import monotonic, sleep
from unittest.mock import AsyncMock, Mock

import pytest
from redis.auth.err import RequestTokenErr, TokenRenewalErr
from redis.auth.idp import IdentityProviderInterface
from redis.auth.token import SimpleToken
from redis.auth.token_manager import (
    CredentialsListener,
    RetryPolicy,
    TokenManager,
    TokenManagerConfig,
)


@pytest.fixture
def sync_token_manager():
    now = datetime.now(timezone.utc).timestamp() * 1000
    token = SimpleToken("value", now + 60000, now, {"oid": "test"})
    provider = Mock(spec=IdentityProviderInterface)
    provider.request_token.return_value = token
    config = TokenManagerConfig(0.9, 0, 1000, RetryPolicy(1, 10))
    manager = TokenManager(provider, config)
    workers = []

    def on_next(token):
        workers.append((asyncio.get_running_loop(), threading.current_thread()))

    listener = CredentialsListener()
    listener.on_next = Mock(side_effect=on_next)
    listener.on_error = Mock()
    try:
        yield manager, listener, workers
    finally:
        manager.stop()
        for loop, thread in workers:
            # Also release resources if a shutdown assertion fails.
            if not loop.is_closed():
                loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=1)
            if not thread.is_alive() and not loop.is_closed():
                loop.close()


@pytest.mark.fixed_client
class TestTokenManager:
    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    @pytest.mark.parametrize("skip_initial", [False, True])
    async def test_sync_start_with_running_event_loop(self, skip_initial):
        now = datetime.now(timezone.utc).timestamp() * 1000
        token = SimpleToken("value", now + 60000, now, {"oid": "test"})
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = token
        listener = CredentialsListener()
        listener.on_next = Mock()
        listener.on_error = Mock()
        config = TokenManagerConfig(0.9, 0, 1000, RetryPolicy(1, 10))
        mgr = TokenManager(mock_provider, config)
        heartbeat = asyncio.Event()
        asyncio.get_running_loop().call_soon(heartbeat.set)

        try:
            stop = mgr.start(listener, skip_initial=skip_initial)

            assert stop == mgr.stop
            mock_provider.request_token.assert_called_once_with(True)
            if skip_initial:
                listener.on_next.assert_not_called()
            else:
                listener.on_next.assert_called_once_with(token)
            listener.on_error.assert_not_called()
            await asyncio.wait_for(heartbeat.wait(), timeout=1)
        finally:
            mgr.stop()

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("running_loop", [False, True])
    @pytest.mark.parametrize("stop_between_starts", [False, True])
    def test_sync_stop_closes_background_event_loop(
        self, sync_token_manager, running_loop, stop_between_starts
    ):
        manager, listener, workers = sync_token_manager

        def check_stopped(worker):
            loop, thread = worker
            thread.join(timeout=1)
            assert not thread.is_alive()
            assert loop.is_closed()

        def start_and_stop():
            for _ in range(3):
                stop = manager.start(listener)
                if len(workers) > 1:
                    check_stopped(workers[-2])
                if stop_between_starts:
                    stop()
                    stop()
                    check_stopped(workers[-1])
            stop()
            check_stopped(workers[-1])
            listener.on_error.assert_not_called()
            assert len(workers) == 3

        async def run_in_loop():
            start_and_stop()
            heartbeat = asyncio.Event()
            asyncio.get_running_loop().call_soon(heartbeat.set)
            await asyncio.wait_for(heartbeat.wait(), timeout=1)

        if running_loop:
            asyncio.run(run_in_loop())
        else:
            start_and_stop()

    @pytest.mark.timeout(5)
    @pytest.mark.parametrize("callback", ["on_next", "on_error"])
    def test_sync_stop_from_callback(self, sync_token_manager, callback):
        manager, listener, workers = sync_token_manager
        capture_worker = listener.on_next.side_effect

        def stop_from_callback(value):
            capture_worker(value)
            manager.stop()

        getattr(listener, callback).side_effect = stop_from_callback
        if callback == "on_error":
            listener.on_next.side_effect = ValueError("callback failed")

        stop = manager.start(listener)
        loop, thread = workers[0]
        thread.join(timeout=1)
        assert not thread.is_alive()
        assert loop.is_closed()
        getattr(listener, callback).assert_called_once()
        stop()

    @pytest.mark.timeout(15)
    def test_sync_stop_releases_pending_start(self, sync_token_manager, monkeypatch):
        manager, listener, workers = sync_token_manager
        release_loop = threading.Event()
        init_waiting = threading.Event()
        waiting_events = []
        results = []
        errors = []
        loop = asyncio.new_event_loop()

        def block_loop():
            workers.append((loop, threading.current_thread()))
            release_loop.wait(timeout=5)

        def start_manager():
            try:
                results.append(manager.start(listener))
            except BaseException as error:
                errors.append(error)

        starter = threading.Thread(target=start_manager, daemon=True)
        original_wait = threading.Event.wait

        def wait(event, timeout=None):
            if (
                threading.current_thread() is starter
                and manager._init_timer is not None
            ):
                waiting_events.append(event)
                init_waiting.set()
            return original_wait(event, timeout)

        loop.call_soon(block_loop)
        monkeypatch.setattr(asyncio, "new_event_loop", lambda: loop)
        monkeypatch.setattr(threading.Event, "wait", wait)
        try:
            starter.start()
            assert init_waiting.wait(timeout=5)
            manager.stop()
            release_loop.set()
            starter.join(timeout=5)
            assert not starter.is_alive()
            assert not errors
            assert results == [manager.stop]
            listener.on_next.assert_not_called()
            listener.on_error.assert_not_called()
            worker = workers[0][1]
            worker.join(timeout=1)
            assert not worker.is_alive()
            assert loop.is_closed()
        finally:
            release_loop.set()
            for event in waiting_events:
                event.set()
            starter.join(timeout=1)

    @pytest.mark.timeout(15)
    @pytest.mark.parametrize("skip_initial", [False, True])
    def test_sync_stop_before_initial_scheduling(
        self, sync_token_manager, monkeypatch, skip_initial
    ):
        manager, listener, workers = sync_token_manager
        scheduling = threading.Event()
        resume_start = threading.Event()
        worker_ready = threading.Event()
        results = []
        errors = []
        loop = asyncio.new_event_loop()

        def record_worker():
            workers.append((loop, threading.current_thread()))
            worker_ready.set()

        def start_manager():
            try:
                results.append(manager.start(listener, skip_initial=skip_initial))
            except BaseException as error:
                errors.append(error)

        starter = threading.Thread(target=start_manager, daemon=True)
        original_schedule = loop.call_soon_threadsafe

        def pause_scheduling(callback, *args, **kwargs):
            if threading.current_thread() is starter:
                scheduling.set()
                assert resume_start.wait(timeout=5)
            return original_schedule(callback, *args, **kwargs)

        loop.call_soon(record_worker)
        monkeypatch.setattr(asyncio, "new_event_loop", lambda: loop)
        monkeypatch.setattr(loop, "call_soon_threadsafe", pause_scheduling)
        try:
            starter.start()
            assert scheduling.wait(timeout=5)
            assert manager._init_timer is None
            manager.stop()
            assert worker_ready.wait(timeout=5)
            worker = workers[0][1]
            worker.join(timeout=5)
            assert not worker.is_alive()
            assert loop.is_closed()

            resume_start.set()
            starter.join(timeout=5)
            assert not starter.is_alive()
            assert not errors
            assert results == [manager.stop]
            manager._idp.request_token.assert_not_called()
            listener.on_next.assert_not_called()
            listener.on_error.assert_not_called()
        finally:
            resume_start.set()
            manager.stop()
            starter.join(timeout=1)

    @pytest.mark.timeout(5)
    def test_sync_start_preserves_initial_scheduling_error(
        self, sync_token_manager, monkeypatch
    ):
        manager, listener, workers = sync_token_manager
        loop = asyncio.new_event_loop()
        original_schedule = loop.call_soon_threadsafe
        error = RuntimeError("initial scheduling failed")

        def fail_scheduling(callback, *args, **kwargs):
            if callback == loop.stop:
                return original_schedule(callback, *args, **kwargs)
            raise error

        def record_worker():
            workers.append((loop, threading.current_thread()))

        loop.call_soon(record_worker)
        monkeypatch.setattr(asyncio, "new_event_loop", lambda: loop)
        monkeypatch.setattr(loop, "call_soon_threadsafe", fail_scheduling)
        with pytest.raises(RuntimeError) as exc:
            manager.start(listener)

        assert exc.value is error
        assert not loop.is_closed()
        manager._idp.request_token.assert_not_called()
        listener.on_next.assert_not_called()
        listener.on_error.assert_not_called()

    def test_sync_start_closes_loop_on_thread_start_failure(
        self, sync_token_manager, monkeypatch
    ):
        manager, listener, workers = sync_token_manager
        loop = asyncio.new_event_loop()
        monkeypatch.setattr(asyncio, "new_event_loop", lambda: loop)
        monkeypatch.setattr(
            threading.Thread, "start", Mock(side_effect=RuntimeError("no threads"))
        )

        try:
            with pytest.raises(RuntimeError, match="no threads"):
                manager.start(listener)
            assert loop.is_closed()
            assert not workers
        finally:
            loop.close()

    @pytest.mark.asyncio
    async def test_async_stop_preserves_callers_loop(self, sync_token_manager):
        manager, listener, workers = sync_token_manager
        listener.on_next = AsyncMock()
        listener.on_error = AsyncMock()
        loop = asyncio.get_running_loop()

        stop = await manager.start_async(listener, block_for_initial=True)
        stop()
        heartbeat = asyncio.Event()
        loop.call_soon(heartbeat.set)
        await asyncio.wait_for(heartbeat.wait(), timeout=1)

        assert not workers
        assert loop.is_running()
        assert not loop.is_closed()
        listener.on_next.assert_awaited_once()
        listener.on_error.assert_not_called()

    @pytest.mark.parametrize(
        "exp_refresh_ratio",
        [
            0.9,
            0.28,
        ],
        ids=[
            "Refresh ratio = 0.9",
            "Refresh ratio = 0.28",
        ],
    )
    def test_success_token_renewal(self, exp_refresh_ratio):
        tokens = []
        errors = []

        # Use a function to generate fresh tokens at request time
        # to avoid timing issues on slow CI runners
        def generate_token():
            now = datetime.now(timezone.utc).timestamp() * 1000
            return SimpleToken("value", now + 10000, now, {"oid": "test"})

        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = (
            lambda *args, **kwargs: generate_token()
        )

        def on_next(token):
            nonlocal tokens
            tokens.append(token)

        def on_error(err):
            nonlocal errors
            errors.append(err)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(exp_refresh_ratio, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)

        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(tokens) > 0

    @pytest.mark.parametrize(
        "exp_refresh_ratio",
        [
            (0.9),
            (0.28),
        ],
        ids=[
            "Refresh ratio = 0.9",
            "Refresh ratio = 0.28",
        ],
    )
    @pytest.mark.asyncio
    async def test_async_success_token_renewal(self, exp_refresh_ratio):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 100,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 130,
                (datetime.now(timezone.utc).timestamp() * 1000) + 30,
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 160,
                (datetime.now(timezone.utc).timestamp() * 1000) + 60,
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 190,
                (datetime.now(timezone.utc).timestamp() * 1000) + 90,
                {"oid": "test"},
            ),
        ]

        async def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(exp_refresh_ratio, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)

        assert len(tokens) > 0

    @pytest.mark.parametrize(
        "block_for_initial,tokens_acquired",
        [
            (True, 1),
            (False, 0),
        ],
        ids=[
            "Block for initial, callback will triggered once",
            "Non blocked, callback wont be triggered",
        ],
    )
    @pytest.mark.asyncio
    async def test_async_request_token_blocking_behaviour(
        self, block_for_initial, tokens_acquired
    ):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) + 100,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"oid": "test"},
        )

        async def on_next(token):
            nonlocal tokens
            sleep(0.1)
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=block_for_initial)

        assert len(tokens) == tokens_acquired

    def test_token_renewal_with_skip_initial(self):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 1000,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 1500,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
        ]

        def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(0.5, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener, skip_initial=True)
        assert len(tokens) == 0

        # the renewal is due at ~0.5 s; poll so a stalled runner gets more time
        deadline = monotonic() + 3
        while not tokens and monotonic() < deadline:
            sleep(0.01)

        assert len(tokens) > 0

    @pytest.mark.asyncio
    async def test_async_token_renewal_with_skip_initial(self):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 1000,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 1200,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 1400,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
        ]

        async def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(0.5, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, skip_initial=True)
        assert len(tokens) == 0

        # the renewal is due at ~0.5 s; poll so a stalled runner gets more time
        deadline = monotonic() + 3
        while not tokens and monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert len(tokens) > 0

    def test_success_token_renewal_with_retry(self):
        tokens = []
        errors = []
        call_count = [0]

        # Use a function to generate fresh tokens at request time
        # to avoid timing issues on slow CI runners
        def request_token_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 2:
                raise RequestTokenErr("Simulated failure")
            now = datetime.now(timezone.utc).timestamp() * 1000
            return SimpleToken("value", now + 10000, now, {"oid": "test"})

        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = request_token_side_effect

        def on_next(token):
            nonlocal tokens
            tokens.append(token)

        def on_error(err):
            nonlocal errors
            errors.append(err)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)

        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert mock_provider.request_token.call_count > 0
        assert len(tokens) > 0

    @pytest.mark.asyncio
    async def test_async_success_token_renewal_with_retry(self):
        tokens = []
        errors = []
        call_count = [0]

        # Use a function to generate fresh tokens at request time
        # to avoid timing issues on slow CI runners
        def request_token_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 2:
                raise RequestTokenErr("Simulated failure")
            now = datetime.now(timezone.utc).timestamp() * 1000
            return SimpleToken("value", now + 10000, now, {"oid": "test"})

        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = request_token_side_effect

        async def on_next(token):
            nonlocal tokens
            tokens.append(token)

        async def on_error(err):
            nonlocal errors
            errors.append(err)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)

        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert mock_provider.request_token.call_count > 0
        assert len(tokens) > 0

    def test_no_token_renewal_on_process_complete(self):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) + 1000,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"oid": "test"},
        )

        def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(0.9, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)
        # the next renewal is due at ~0.9 s; give an early one a chance to show
        sleep(0.2)

        assert len(tokens) == 1

    @pytest.mark.asyncio
    async def test_async_no_token_renewal_on_process_complete(self):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) + 1000,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"oid": "test"},
        )

        async def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(0.9, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)
        # the next renewal is due at ~0.9 s; give an early one a chance to show
        await asyncio.sleep(0.2)

        assert len(tokens) == 1

    def test_failed_token_renewal_with_retry(self):
        tokens = []
        exceptions = []

        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
        ]

        def on_next(token):
            nonlocal tokens
            tokens.append(token)

        def on_error(exception):
            nonlocal exceptions
            exceptions.append(exception)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)

        assert mock_provider.request_token.call_count == 4
        assert len(tokens) == 0
        assert len(exceptions) == 1

    @pytest.mark.asyncio
    async def test_async_failed_token_renewal_with_retry(self):
        tokens = []
        exceptions = []

        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
        ]

        async def on_next(token):
            nonlocal tokens
            tokens.append(token)

        async def on_error(exception):
            nonlocal exceptions
            exceptions.append(exception)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)

        assert mock_provider.request_token.call_count == 4
        assert len(tokens) == 0
        assert len(exceptions) == 1

    def test_failed_renewal_on_expired_token(self):
        errors = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) - 100,
            (datetime.now(timezone.utc).timestamp() * 1000) - 1000,
            {"oid": "test"},
        )

        def on_error(error: TokenRenewalErr):
            nonlocal errors
            errors.append(error)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)

        assert len(errors) == 1
        assert isinstance(errors[0], TokenRenewalErr)
        assert str(errors[0]) == "Requested token is expired"

    @pytest.mark.asyncio
    async def test_async_failed_renewal_on_expired_token(self):
        errors = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) - 100,
            (datetime.now(timezone.utc).timestamp() * 1000) - 1000,
            {"oid": "test"},
        )

        async def on_error(error: TokenRenewalErr):
            nonlocal errors
            errors.append(error)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)

        assert len(errors) == 1
        assert isinstance(errors[0], TokenRenewalErr)
        assert str(errors[0]) == "Requested token is expired"

    def test_failed_renewal_on_callback_error(self):
        errors = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) + 1000,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"oid": "test"},
        )

        def on_next(token):
            raise Exception("Some exception")

        def on_error(error):
            nonlocal errors
            errors.append(error)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)

        assert len(errors) == 1
        assert isinstance(errors[0], TokenRenewalErr)
        assert str(errors[0]) == "Some exception"

    def test_renewal_continues_after_callback_error(self):
        errors = []
        renewed = threading.Event()

        def generate_token(ttl):
            now = datetime.now(timezone.utc).timestamp() * 1000
            return SimpleToken("value", now + ttl, now, {"oid": "test"})

        ttls = iter([100, 60000])
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = (
            lambda *args, **kwargs: generate_token(next(ttls))
        )

        def on_next(token):
            raise Exception("Some exception")

        def on_error(error):
            nonlocal errors
            errors.append(error)
            if len(errors) == 2:
                renewed.set()

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(0.9, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)
        try:
            assert renewed.wait(timeout=5)
        finally:
            mgr.stop()

        assert mock_provider.request_token.call_count == 2
        assert all(isinstance(error, TokenRenewalErr) for error in errors)

    @pytest.mark.asyncio
    async def test_async_failed_renewal_on_callback_error(self):
        errors = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) + 1000,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"oid": "test"},
        )

        async def on_next(token):
            raise Exception("Some exception")

        async def on_error(error):
            nonlocal errors
            errors.append(error)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)

        assert len(errors) == 1
        assert isinstance(errors[0], TokenRenewalErr)
        assert str(errors[0]) == "Some exception"

    @pytest.mark.asyncio
    async def test_async_renewal_continues_after_callback_error(self):
        errors = []
        renewed = asyncio.Event()

        def generate_token(ttl):
            now = datetime.now(timezone.utc).timestamp() * 1000
            return SimpleToken("value", now + ttl, now, {"oid": "test"})

        ttls = iter([100, 60000])
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = (
            lambda *args, **kwargs: generate_token(next(ttls))
        )

        async def on_next(token):
            raise Exception("Some exception")

        async def on_error(error):
            nonlocal errors
            errors.append(error)
            if len(errors) == 2:
                renewed.set()

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(0.9, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)
        await asyncio.wait_for(renewed.wait(), timeout=5)
        mgr.stop()

        assert mock_provider.request_token.call_count == 2
        assert all(isinstance(error, TokenRenewalErr) for error in errors)

    @pytest.mark.asyncio
    async def test_async_stop_cancels_renewal_after_callback_error(self):
        now = datetime.now(timezone.utc).timestamp() * 1000
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value", now + 60000, now, {"oid": "test"}
        )

        async def on_next(token):
            raise Exception("Some exception")

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = AsyncMock()

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(0.9, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)
        mgr.stop()

        mock_listener.on_error.assert_awaited_once()
        assert mgr._next_timer.cancelled()

    @pytest.mark.asyncio
    async def test_async_renewal_is_scheduled_after_callback(self):
        timers = []
        now = datetime.now(timezone.utc).timestamp() * 1000
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value", now + 60000, now, {"oid": "test"}
        )

        async def on_next(token):
            timers.append(mgr._next_timer)
            raise Exception("Some exception")

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = AsyncMock()

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(0.9, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)
        mgr.stop()

        assert timers == [None]
        assert mgr._next_timer is not None
