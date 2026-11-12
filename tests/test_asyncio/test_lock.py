import asyncio
import uuid

import pytest
import pytest_asyncio
import redis.asyncio.lock as lock_module
from redis.asyncio.lock import Lock
from redis.exceptions import LockError, LockNotOwnedError


class FakeLockTime:
    def __init__(self):
        self.now = 0
        self.sleeps = []

    def get_running_loop(self):
        return self

    def time(self):
        return self.now

    async def sleep(self, sleep):
        self.sleeps.append(sleep)
        self.now += sleep


class TestLock:
    @pytest_asyncio.fixture()
    async def r_decoded(self, create_redis):
        redis = await create_redis(decode_responses=True)
        yield redis
        await redis.flushall()

    @pytest.fixture()
    def fake_lock_time(self, monkeypatch):
        fake_time = FakeLockTime()
        monkeypatch.setattr(lock_module, "asyncio", fake_time)
        return fake_time

    def get_lock(self, redis, *args, **kwargs):
        kwargs["lock_class"] = Lock
        return redis.lock(*args, **kwargs)

    async def test_lock(self, r):
        lock = self.get_lock(r, "foo")
        assert await lock.acquire(blocking=False)
        assert await r.get("foo") == lock.local.token
        assert await r.ttl("foo") == -1
        await lock.release()
        assert await r.get("foo") is None

    async def test_lock_token(self, r):
        lock = self.get_lock(r, "foo")
        await self._test_lock_token(r, lock)

    async def test_lock_token_thread_local_false(self, r):
        lock = self.get_lock(r, "foo", thread_local=False)
        await self._test_lock_token(r, lock)

    async def test_default_token_is_random_uuid4(self, r):
        # The default token should be random and should not embed host or
        # time data: a v1 UUID writes the host MAC address and a clock
        # reading into the key's value. A v4 UUID is random.
        lock = self.get_lock(r, "foo")
        assert await lock.acquire(blocking=False)
        token = lock.local.token
        assert uuid.UUID(token.decode()).version == 4
        await lock.release()

    async def _test_lock_token(self, r, lock):
        assert await lock.acquire(blocking=False, token="test")
        assert await r.get("foo") == b"test"
        assert lock.local.token == b"test"
        assert await r.ttl("foo") == -1
        await lock.release()
        assert await r.get("foo") is None
        assert lock.local.token is None

    async def test_locked(self, r):
        lock = self.get_lock(r, "foo")
        assert await lock.locked() is False
        await lock.acquire(blocking=False)
        assert await lock.locked() is True
        await lock.release()
        assert await lock.locked() is False

    async def _test_owned(self, client):
        lock = self.get_lock(client, "foo")
        assert await lock.owned() is False
        await lock.acquire(blocking=False)
        assert await lock.owned() is True
        await lock.release()
        assert await lock.owned() is False

        lock2 = self.get_lock(client, "foo")
        assert await lock.owned() is False
        assert await lock2.owned() is False
        await lock2.acquire(blocking=False)
        assert await lock.owned() is False
        assert await lock2.owned() is True
        await lock2.release()
        assert await lock.owned() is False
        assert await lock2.owned() is False

    async def test_owned(self, r):
        await self._test_owned(r)

    async def test_owned_with_decoded_responses(self, r_decoded):
        await self._test_owned(r_decoded)

    async def test_competing_locks(self, r):
        lock1 = self.get_lock(r, "foo")
        lock2 = self.get_lock(r, "foo")
        assert await lock1.acquire(blocking=False)
        assert not await lock2.acquire(blocking=False)
        await lock1.release()
        assert await lock2.acquire(blocking=False)
        assert not await lock1.acquire(blocking=False)
        await lock2.release()

    async def test_timeout(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        assert 8 < (await r.ttl("foo")) <= 10
        await lock.release()

    async def test_float_timeout(self, r):
        lock = self.get_lock(r, "foo", timeout=9.5)
        assert await lock.acquire(blocking=False)
        assert 8 < (await r.pttl("foo")) <= 9500
        await lock.release()

    async def test_blocking(self, r):
        blocking = False
        lock = self.get_lock(r, "foo", blocking=blocking)
        assert not lock.blocking

        lock_2 = self.get_lock(r, "foo")
        assert lock_2.blocking

    async def test_blocking_timeout(self, r, fake_lock_time):
        lock1 = self.get_lock(r, "foo")
        assert await lock1.acquire(blocking=False)
        try:
            bt = 0.2
            sleep = 0.05
            lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
            assert not await lock2.acquire()
            assert fake_lock_time.now == pytest.approx(bt)
            assert fake_lock_time.now > bt - sleep
            assert fake_lock_time.sleeps == [sleep] * 4
        finally:
            await lock1.release()

    async def test_context_manager(self, r):
        # blocking_timeout prevents a deadlock if the lock can't be acquired
        # for some reason
        async with self.get_lock(r, "foo", blocking_timeout=0.2) as lock:
            assert await r.get("foo") == lock.local.token
        assert await r.get("foo") is None

    async def test_context_manager_raises_when_locked_not_acquired(self, r):
        await r.set("foo", "bar")
        with pytest.raises(LockError):
            async with self.get_lock(r, "foo", blocking_timeout=0.1):
                pass

    async def test_context_manager_not_raise_on_release_lock_not_owned_error(self, r):
        try:
            async with self.get_lock(
                r, "foo", timeout=0.1, raise_on_release_error=False
            ):
                await asyncio.sleep(0.15)
        except LockNotOwnedError:
            pytest.fail("LockNotOwnedError should not have been raised")

        with pytest.raises(LockNotOwnedError):
            async with self.get_lock(
                r, "foo", timeout=0.1, raise_on_release_error=True
            ):
                await asyncio.sleep(0.15)

    async def test_context_manager_not_raise_on_release_lock_error(self, r):
        # No lock timeout here: the error under test is the LockError raised by the
        # redundant release in __aexit__, not the LockNotOwnedError of an expired lock
        # (covered by test_context_manager_not_raise_on_release_lock_not_owned_error).
        try:
            async with self.get_lock(r, "foo", raise_on_release_error=False) as lock:
                await lock.release()
        except LockError:
            pytest.fail("LockError should not have been raised")

        with pytest.raises(LockError):
            async with self.get_lock(r, "foo", raise_on_release_error=True) as lock:
                await lock.release()

    async def test_acquire_sleep_overrides_constructor_sleep(self, r, fake_lock_time):
        lock1 = self.get_lock(r, "foo")
        assert await lock1.acquire(blocking=False)
        try:
            bt = 0.2
            sleep = 0.05
            override = 0.025
            lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
            assert not await lock2.acquire(sleep=override)
            assert fake_lock_time.sleeps == [override] * 8
        finally:
            await lock1.release()

    async def test_blocking_timeout_sleep_overrides_constructor_sleep(
        self, r, fake_lock_time
    ):
        lock1 = self.get_lock(r, "foo")
        assert await lock1.acquire(blocking=False)
        try:
            bt = 0.2
            sleep = 0.1
            override = 0.05
            lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
            assert not await lock2.acquire(sleep=override)
            assert fake_lock_time.now == pytest.approx(bt)
            assert fake_lock_time.sleeps == [override] * 4
        finally:
            await lock1.release()

    async def test_blocking_first_positional_still_binds(self, r, fake_lock_time):
        lock1 = self.get_lock(r, "foo")
        assert await lock1.acquire(blocking=False)
        try:
            lock2 = self.get_lock(r, "foo")
            assert not await lock2.acquire(False)
            assert fake_lock_time.sleeps == []
        finally:
            await lock1.release()

    async def test_high_sleep_small_blocking_timeout(self, r, fake_lock_time):
        lock1 = self.get_lock(r, "foo")
        assert await lock1.acquire(blocking=False)
        try:
            sleep = 60
            bt = 1
            lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
            assert not await lock2.acquire()
            assert fake_lock_time.now == 0
            assert fake_lock_time.sleeps == []
        finally:
            await lock1.release()

    async def test_releasing_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        with pytest.raises(LockError):
            await lock.release()

    async def test_releasing_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        await lock.acquire(blocking=False)
        # manually change the token
        await r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            await lock.release()
        # even though we errored, the token is still cleared
        assert lock.local.token is None

    async def test_release_does_not_clear_another_acquirers_token(self, r, monkeypatch):
        """
        release() must only clear the token if it is still ours.

        threading.local() is shared by every task on one event loop, so a Lock
        shared between tasks can have another acquirer's token stored between
        do_release() and the clear. Clearing unconditionally leaves that new
        owner holding a lock it cannot release.
        """
        lock = self.get_lock(r, "foo")
        await lock.acquire(blocking=False)

        async def steal(expected_token):
            lock.local.token = "another-acquirers-token"

        monkeypatch.setattr(lock, "do_release", steal)
        await lock.release()
        assert lock.local.token == "another-acquirers-token"

        async def steal_and_raise(expected_token):
            lock.local.token = "another-acquirers-token"
            raise LockNotOwnedError("Cannot release a lock that's no longer owned")

        lock.local.token = "ours"
        monkeypatch.setattr(lock, "do_release", steal_and_raise)
        with pytest.raises(LockNotOwnedError):
            await lock.release()
        assert lock.local.token == "another-acquirers-token"

        monkeypatch.undo()
        await r.delete("foo")

    async def test_extend_lock(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        assert 8000 < (await r.pttl("foo")) <= 10000
        assert await lock.extend(10)
        assert 16000 < (await r.pttl("foo")) <= 20000
        await lock.release()

    async def test_extend_lock_replace_ttl(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        assert 8000 < (await r.pttl("foo")) <= 10000
        assert await lock.extend(10, replace_ttl=True)
        assert 8000 < (await r.pttl("foo")) <= 10000
        await lock.release()

    async def test_extend_lock_zero_additional_time_replace_ttl_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=0)
        assert await lock.acquire(blocking=False)
        with pytest.raises(LockNotOwnedError):
            await lock.extend(0, replace_ttl=True)
        assert await r.get("foo") == lock.local.token
        assert await r.pttl("foo") == -1
        await lock.release()

    async def test_extend_lock_zero_timeout_replace_ttl_keeps_existing_ttl(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        old_ttl = await r.pttl("foo")
        assert await lock.extend(0, replace_ttl=True)
        assert await r.get("foo") == lock.local.token
        assert 0 < (await r.pttl("foo")) <= old_ttl
        await lock.release()

    async def test_extend_lock_negative_timeout_replace_ttl_keeps_existing_ttl(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        old_ttl = await r.pttl("foo")
        assert await lock.extend(-1, replace_ttl=True)
        assert await r.get("foo") == lock.local.token
        assert 0 < (await r.pttl("foo")) <= old_ttl
        await lock.release()

    async def test_extend_lock_negative_additional_time_without_replace_keeps_existing_ttl(
        self, r
    ):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        old_ttl = await r.pttl("foo")
        assert await lock.extend(-20)
        assert await r.get("foo") == lock.local.token
        assert 0 < (await r.pttl("foo")) <= old_ttl
        await lock.release()

    async def test_extend_lock_zero_timeout_without_replace_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=0)
        assert await lock.acquire(blocking=False)
        with pytest.raises(LockNotOwnedError):
            await lock.extend(0)
        assert await r.get("foo") == lock.local.token
        assert await r.pttl("foo") == -1
        await lock.release()

    async def test_extend_lock_float(self, r):
        lock = self.get_lock(r, "foo", timeout=10.5)
        assert await lock.acquire(blocking=False)
        assert 10000 < (await r.pttl("foo")) <= 10500
        old_ttl = await r.pttl("foo")
        assert await lock.extend(10.5)
        assert old_ttl + 10000 < (await r.pttl("foo")) <= old_ttl + 10500
        await lock.release()

    async def test_extending_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        with pytest.raises(LockError):
            await lock.extend(10)

    async def test_extending_lock_with_no_timeout_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        assert await lock.acquire(blocking=False)
        with pytest.raises(LockError):
            await lock.extend(10)
        await lock.release()

    async def test_extending_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        await r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            await lock.extend(10)

    async def test_reacquire_lock(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        assert await r.pexpire("foo", 5000)
        assert await r.pttl("foo") <= 5000
        assert await lock.reacquire()
        assert 8000 < (await r.pttl("foo")) <= 10000
        await lock.release()

    async def test_reacquire_lock_zero_timeout_keeps_lock(self, r):
        lock = self.get_lock(r, "foo", timeout=0)
        assert await lock.acquire(blocking=False)
        assert await lock.reacquire()
        assert await r.get("foo") == lock.local.token
        assert await r.pttl("foo") == -1
        await lock.release()

    async def test_reacquiring_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        with pytest.raises(LockError):
            await lock.reacquire()

    async def test_reacquiring_lock_with_no_timeout_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        assert await lock.acquire(blocking=False)
        with pytest.raises(LockError):
            await lock.reacquire()
        await lock.release()

    async def test_reacquiring_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        await r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            await lock.reacquire()

    async def test_lock_error_gives_correct_lock_name(self, r):
        await r.set("foo", "bar")
        with pytest.raises(LockError) as excinfo:
            async with self.get_lock(r, "foo", blocking_timeout=0.1):
                pass
        assert excinfo.value.lock_name == "foo"

    async def test_lock_not_owned_error_gives_correct_lock_name(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        await r.set("foo", "a")
        with pytest.raises(LockNotOwnedError) as excinfo:
            await lock.extend(10)
        assert excinfo.value.lock_name == "foo"

    async def test_release_cancellation_preserves_lock_state(self, r):
        """
        Test that cancelling release() doesn't leave lock in inconsistent state.

        Regression test for GitHub issue #3847. Before the fix, if release()
        was cancelled during execution, the token would be cleared but the
        Redis key would remain, causing a permanent deadlock.
        """
        lock = self.get_lock(r, "foo")
        await lock.acquire(blocking=False)

        # Verify lock is owned
        original_token = lock.local.token
        assert original_token is not None
        assert await lock.owned()

        # Create release task and cancel it immediately
        release_task = asyncio.create_task(lock.release())
        release_task.cancel()

        try:
            await release_task
        except asyncio.CancelledError:
            # Expected: the release task was deliberately cancelled to test lock state.
            pass

        # Check the lock state after cancellation
        if lock.local.token is not None:
            # Release was cancelled before completion - token preserved
            # This is the fix: we can now retry the release
            assert lock.local.token == original_token
            await lock.release()
            assert await lock.locked() is False
        else:
            # Release completed before cancel took effect
            assert await lock.locked() is False


@pytest.mark.onlynoncluster
class TestLockClassSelection:
    def test_lock_class_argument(self, r):
        class MyLock:
            def __init__(self, *args, **kwargs):
                pass

        lock = r.lock("foo", lock_class=MyLock)
        assert isinstance(lock, MyLock)
