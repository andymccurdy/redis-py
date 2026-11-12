import time
import uuid

import pytest
from redis import lock as lock_module
from redis.client import Redis
from redis.exceptions import ConnectionError, LockError, LockNotOwnedError
from redis.lock import Lock

from .conftest import _get_client


class FakeLockTime:
    def __init__(self):
        self.now = 0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, sleep):
        self.sleeps.append(sleep)
        self.now += sleep


class TestLock:
    @pytest.fixture()
    def r_decoded(self, request):
        return _get_client(Redis, request=request, decode_responses=True)

    @pytest.fixture()
    def fake_lock_time(self, monkeypatch):
        fake_time = FakeLockTime()
        monkeypatch.setattr(lock_module, "mod_time", fake_time)
        return fake_time

    def get_lock(self, redis, *args, **kwargs):
        kwargs["lock_class"] = Lock
        return redis.lock(*args, **kwargs)

    def test_lock(self, r):
        lock = self.get_lock(r, "foo")
        assert lock.acquire(blocking=False)
        assert r.get("foo") == lock.local.token
        assert r.ttl("foo") == -1
        lock.release()
        assert r.get("foo") is None

    def test_lock_token(self, r):
        lock = self.get_lock(r, "foo")
        self._test_lock_token(r, lock)

    def test_lock_token_thread_local_false(self, r):
        lock = self.get_lock(r, "foo", thread_local=False)
        self._test_lock_token(r, lock)

    def test_default_token_is_random_uuid4(self, r):
        # The default token should be random and should not embed host or
        # time data: a v1 UUID writes the host MAC address and a clock
        # reading into the key's value. A v4 UUID is random.
        lock = self.get_lock(r, "foo")
        assert lock.acquire(blocking=False)
        token = lock.local.token
        assert uuid.UUID(token.decode()).version == 4
        lock.release()

    def _test_lock_token(self, r, lock):
        assert lock.acquire(blocking=False, token="test")
        assert r.get("foo") == b"test"
        assert lock.local.token == b"test"
        assert r.ttl("foo") == -1
        lock.release()
        assert r.get("foo") is None
        assert lock.local.token is None

    def test_locked(self, r):
        lock = self.get_lock(r, "foo")
        assert lock.locked() is False
        lock.acquire(blocking=False)
        assert lock.locked() is True
        lock.release()
        assert lock.locked() is False

    def _test_owned(self, client):
        lock = self.get_lock(client, "foo")
        assert lock.owned() is False
        lock.acquire(blocking=False)
        assert lock.owned() is True
        lock.release()
        assert lock.owned() is False

        lock2 = self.get_lock(client, "foo")
        assert lock.owned() is False
        assert lock2.owned() is False
        lock2.acquire(blocking=False)
        assert lock.owned() is False
        assert lock2.owned() is True
        lock2.release()
        assert lock.owned() is False
        assert lock2.owned() is False

    def test_owned(self, r):
        self._test_owned(r)

    def test_owned_with_decoded_responses(self, r_decoded):
        self._test_owned(r_decoded)

    def test_competing_locks(self, r):
        lock1 = self.get_lock(r, "foo")
        lock2 = self.get_lock(r, "foo")
        assert lock1.acquire(blocking=False)
        assert not lock2.acquire(blocking=False)
        lock1.release()
        assert lock2.acquire(blocking=False)
        assert not lock1.acquire(blocking=False)
        lock2.release()

    def test_timeout(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        assert 8 < r.ttl("foo") <= 10
        lock.release()

    def test_float_timeout(self, r):
        lock = self.get_lock(r, "foo", timeout=9.5)
        assert lock.acquire(blocking=False)
        assert 8 < r.pttl("foo") <= 9500
        lock.release()

    def test_blocking_timeout(self, r, fake_lock_time):
        lock1 = self.get_lock(r, "foo")
        assert lock1.acquire(blocking=False)
        try:
            bt = 0.4
            sleep = 0.05
            lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
            assert not lock2.acquire()
            assert fake_lock_time.now == pytest.approx(bt)
            assert fake_lock_time.now > bt - sleep
            assert fake_lock_time.sleeps == [sleep] * 8
        finally:
            lock1.release()

    def test_context_manager(self, r):
        # blocking_timeout prevents a deadlock if the lock can't be acquired
        # for some reason
        with self.get_lock(r, "foo", blocking_timeout=0.2) as lock:
            assert r.get("foo") == lock.local.token
        assert r.get("foo") is None

    def test_context_manager_blocking_timeout(self, r, fake_lock_time):
        with self.get_lock(r, "foo", blocking=False):
            bt = 0.4
            sleep = 0.05
            lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
            assert not lock2.acquire()
            assert fake_lock_time.now == pytest.approx(bt)
            assert fake_lock_time.now > bt - sleep
            assert fake_lock_time.sleeps == [sleep] * 8

    def test_context_manager_raises_when_locked_not_acquired(self, r):
        r.set("foo", "bar")
        with pytest.raises(LockError):
            with self.get_lock(r, "foo", blocking_timeout=0.1):
                pass

    def test_context_manager_not_raise_on_release_lock_not_owned_error(self, r):
        try:
            with self.get_lock(r, "foo", timeout=0.1, raise_on_release_error=False):
                time.sleep(0.15)
        except LockNotOwnedError:
            pytest.fail("LockNotOwnedError should not have been raised")

        with pytest.raises(LockNotOwnedError):
            with self.get_lock(r, "foo", timeout=0.1, raise_on_release_error=True):
                time.sleep(0.15)

    def test_context_manager_not_raise_on_release_lock_error(self, r):
        # No lock timeout here: the error under test is the LockError raised by the
        # redundant release in __exit__, not the LockNotOwnedError of an expired lock
        # (covered by test_context_manager_not_raise_on_release_lock_not_owned_error).
        try:
            with self.get_lock(r, "foo", raise_on_release_error=False) as lock:
                lock.release()
        except LockError:
            pytest.fail("LockError should not have been raised")

        with pytest.raises(LockError):
            with self.get_lock(r, "foo", raise_on_release_error=True) as lock:
                lock.release()

    def test_high_sleep_small_blocking_timeout(self, r, fake_lock_time):
        lock1 = self.get_lock(r, "foo")
        assert lock1.acquire(blocking=False)
        try:
            sleep = 60
            bt = 1
            lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
            assert not lock2.acquire()
            assert fake_lock_time.now == 0
            assert fake_lock_time.sleeps == []
        finally:
            lock1.release()

    def test_releasing_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        with pytest.raises(LockError):
            lock.release()

    def test_releasing_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        lock.acquire(blocking=False)
        # manually change the token
        r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            lock.release()
        # even though we errored, the token is still cleared
        assert lock.local.token is None

    def test_release_connection_error_preserves_lock_state(self, r, monkeypatch):
        """
        A failed release must leave the lock retryable.

        Same state as GitHub issue #3847, reached through a connection error
        instead of a cancellation. Clearing the token before the round trip
        left owned() False while the Redis key survived, so the caller could
        neither retry the release nor extend the lock.
        """
        lock = self.get_lock(r, "foo")
        lock.acquire(blocking=False)
        original_token = lock.local.token
        assert original_token is not None

        def fail(expected_token):
            raise ConnectionError("Connection closed by server.")

        monkeypatch.setattr(lock, "do_release", fail)
        with pytest.raises(ConnectionError):
            lock.release()

        # the lock is still held in Redis, so the token must survive
        assert lock.local.token == original_token
        assert lock.owned()
        assert lock.locked()

        # and the release can now be retried
        monkeypatch.undo()
        lock.release()
        assert lock.locked() is False

    def test_release_does_not_clear_another_acquirers_token(self, r, monkeypatch):
        """
        release() must only clear the token if it is still ours.

        With thread_local=False the token namespace is shared, so another
        acquirer can store its token between our do_release() and the clear.
        Clearing unconditionally there is the overwrite #489 fixed in 2014, and
        it leaves the new owner holding a lock it cannot release.
        """
        lock = self.get_lock(r, "foo")
        lock.acquire(blocking=False)

        def steal(expected_token):
            lock.local.token = "another-acquirers-token"

        monkeypatch.setattr(lock, "do_release", steal)
        lock.release()
        assert lock.local.token == "another-acquirers-token"

        def steal_and_raise(expected_token):
            lock.local.token = "another-acquirers-token"
            raise LockNotOwnedError("Cannot release a lock that's no longer owned")

        lock.local.token = "ours"
        monkeypatch.setattr(lock, "do_release", steal_and_raise)
        with pytest.raises(LockNotOwnedError):
            lock.release()
        assert lock.local.token == "another-acquirers-token"

        monkeypatch.undo()
        r.delete("foo")

    def test_extend_lock(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        assert 8000 < r.pttl("foo") <= 10000
        assert lock.extend(10)
        assert 16000 < r.pttl("foo") <= 20000
        lock.release()

    def test_extend_lock_replace_ttl(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        assert 8000 < r.pttl("foo") <= 10000
        assert lock.extend(10, replace_ttl=True)
        assert 8000 < r.pttl("foo") <= 10000
        lock.release()

    def test_extend_lock_zero_additional_time_replace_ttl_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=0)
        assert lock.acquire(blocking=False)
        with pytest.raises(LockNotOwnedError):
            lock.extend(0, replace_ttl=True)
        assert r.get("foo") == lock.local.token
        assert r.pttl("foo") == -1
        lock.release()

    def test_extend_lock_zero_timeout_replace_ttl_keeps_existing_ttl(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        old_ttl = r.pttl("foo")
        assert lock.extend(0, replace_ttl=True)
        assert r.get("foo") == lock.local.token
        assert 0 < r.pttl("foo") <= old_ttl
        lock.release()

    def test_extend_lock_negative_timeout_replace_ttl_keeps_existing_ttl(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        old_ttl = r.pttl("foo")
        assert lock.extend(-1, replace_ttl=True)
        assert r.get("foo") == lock.local.token
        assert 0 < r.pttl("foo") <= old_ttl
        lock.release()

    def test_extend_lock_negative_additional_time_without_replace_keeps_existing_ttl(
        self, r
    ):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        old_ttl = r.pttl("foo")
        assert lock.extend(-20)
        assert r.get("foo") == lock.local.token
        assert 0 < r.pttl("foo") <= old_ttl
        lock.release()

    def test_extend_lock_zero_timeout_without_replace_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=0)
        assert lock.acquire(blocking=False)
        with pytest.raises(LockNotOwnedError):
            lock.extend(0)
        assert r.get("foo") == lock.local.token
        assert r.pttl("foo") == -1
        lock.release()

    def test_extend_lock_float(self, r):
        lock = self.get_lock(r, "foo", timeout=10.5)
        assert lock.acquire(blocking=False)
        assert 10000 < r.pttl("foo") <= 10500
        old_ttl = r.pttl("foo")
        assert lock.extend(10.5)
        assert old_ttl + 10000 < r.pttl("foo") <= old_ttl + 10500
        lock.release()

    def test_extending_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        with pytest.raises(LockError):
            lock.extend(10)

    def test_extending_lock_with_no_timeout_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        assert lock.acquire(blocking=False)
        with pytest.raises(LockError):
            lock.extend(10)
        lock.release()

    def test_extending_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            lock.extend(10)

    def test_reacquire_lock(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        assert r.pexpire("foo", 5000)
        assert r.pttl("foo") <= 5000
        assert lock.reacquire()
        assert 8000 < r.pttl("foo") <= 10000
        lock.release()

    def test_reacquire_lock_zero_timeout_keeps_lock(self, r):
        lock = self.get_lock(r, "foo", timeout=0)
        assert lock.acquire(blocking=False)
        assert lock.reacquire()
        assert r.get("foo") == lock.local.token
        assert r.pttl("foo") == -1
        lock.release()

    def test_reacquiring_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        with pytest.raises(LockError):
            lock.reacquire()

    def test_reacquiring_lock_with_no_timeout_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        assert lock.acquire(blocking=False)
        with pytest.raises(LockError):
            lock.reacquire()
        lock.release()

    def test_reacquiring_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            lock.reacquire()

    def test_context_manager_reacquiring_lock_with_no_timeout_raises_error(self, r):
        with self.get_lock(r, "foo", timeout=None, blocking=False) as lock:
            with pytest.raises(LockError):
                lock.reacquire()

    def test_context_manager_reacquiring_lock_no_longer_owned_raises_error(self, r):
        with pytest.raises(LockNotOwnedError):
            with self.get_lock(r, "foo", timeout=10, blocking=False):
                r.set("foo", "a")

    def test_lock_not_owned_error_gives_correct_lock_name(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        r.set("foo", "a")
        with pytest.raises(LockNotOwnedError) as excinfo:
            lock.extend(10)
        assert excinfo.value.lock_name == "foo"

    def test_lock_error_gives_correct_lock_name(self, r):
        r.set("foo", "bar")
        with pytest.raises(LockError) as excinfo:
            with self.get_lock(r, "foo", blocking_timeout=0.1):
                pass
        # Outside the pytest.raises block: the lock raises on __enter__, so an
        # assertion inside it is never reached.
        assert excinfo.value.lock_name == "foo"


class TestLockClassSelection:
    def test_lock_class_argument(self, r):
        class MyLock:
            def __init__(self, *args, **kwargs):
                pass

        lock = r.lock("foo", lock_class=MyLock)
        assert isinstance(lock, MyLock)
