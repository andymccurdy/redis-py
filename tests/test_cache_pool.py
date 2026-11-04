import multiprocessing
import signal
import socket
import threading
import time
import uuid
from unittest.mock import Mock, patch

import pytest

from redis import Redis
from redis._parsers import _HiredisParser, _RESP3Parser
from redis.cache import CacheConfig, CacheEntryStatus, TrackingMode
from redis.connection import BlockingConnectionPool, Connection, ConnectionPool
from redis.event import (
    EventDispatcher,
    EventListenerInterface,
    MaintenanceCompletedEvent,
    MaintenanceStartedEvent,
)
from redis.exceptions import AuthenticationError, InvalidResponse
from redis.maint_notifications import (
    MaintNotificationsConfig,
    MaintNotificationsConnectionHandler,
    MaintNotificationsPoolHandler,
    MaintenanceState,
    NodeMovingNotification,
)
from redis.utils import HIREDIS_AVAILABLE

from .conftest import skip_if_server_version_lt


@pytest.mark.fixed_client
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("7.4.0")
@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
@pytest.mark.parametrize(
    "parser_class",
    [
        _RESP3Parser,
        pytest.param(
            _HiredisParser,
            marks=pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="requires hiredis"),
        ),
    ],
)
@pytest.mark.parametrize("tracking_mode", list(TrackingMode))
class TestCachePoolOwnership:
    @pytest.mark.parametrize(
        "shutdown",
        ["close", "disconnect", "disconnect_idle", "disconnect_free_connections"],
    )
    def test_custom_connection_without_maintenance_can_close(
        self, pool, pool_class, r2, shutdown
    ):
        class CustomConnection:
            # Delegate real socket I/O without inheriting the maintenance mixin.
            def __init__(self, **kwargs):
                self.connection = Connection(
                    maint_notifications_config=MaintNotificationsConfig(enabled=False),
                    **kwargs,
                )

            def __getattr__(self, name):
                return getattr(self.connection, name)

        custom_pool = pool_class(
            connection_class=CustomConnection,
            cache_config=pool.cache.config,
            **pool.connection_kwargs,
        )
        assert not custom_pool.maint_notifications_enabled()
        idle, active = custom_pool.get_connection(), custom_pool.get_connection()
        key = str(uuid.uuid4())
        try:
            r2.set(key, "value")
            idle.send_command("GET", key, keys=[key])
            assert idle.read_response() == b"value"
            assert custom_pool.cache.size == 1
            custom_pool.release(idle)
            active.send_command("PING")

            if shutdown == "disconnect_idle":
                custom_pool.disconnect(inuse_connections=False)
            else:
                getattr(custom_pool, shutdown)()

            assert not idle.is_connected
            assert custom_pool.cache.size == 0
            if shutdown in ["disconnect_idle", "disconnect_free_connections"]:
                assert active.is_connected
                assert active.read_response() == b"PONG"
            else:
                assert not active.is_connected
        finally:
            # Direct disconnect also cleans up when the pool shutdown fails.
            idle.disconnect()
            active.disconnect()
            custom_pool.release(active)

    def test_maintenance_config_disconnect_joins_owner_drain(self, pool, r2):
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner = pool.get_connection()
        peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        borrower = peers.get_connection()
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)
        draining, finish, attempted = (threading.Event() for _ in range(3))
        errors = []
        real_can_read, real_disconnect = owner._conn.can_read, owner.disconnect

        def can_read(*args, **kwargs):
            if threading.current_thread() is reader:
                draining.set()
                assert finish.wait(5)
                with pool._lock:
                    pass
            return real_can_read(*args, **kwargs)

        def disconnect(*args, **kwargs):
            assert finish.is_set(), "configuration disconnected a draining owner"
            return real_disconnect(*args, **kwargs)

        def read():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)

        def configure():
            attempted.set()
            try:
                pool.update_maint_notifications_config(
                    MaintNotificationsConfig(enabled=True)
                )
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read, daemon=True)
        updater = threading.Thread(target=configure, daemon=True)
        try:
            with (
                patch.object(owner._conn, "can_read", side_effect=can_read),
                patch.object(owner, "disconnect", side_effect=disconnect),
            ):
                reader.start()
                assert draining.wait(3)
                updater.start()
                assert attempted.wait(3)
                updater.join(0.1)
                finish.set()
                reader.join(3)
                updater.join(3)
                assert not reader.is_alive() and not updater.is_alive()
                assert not errors
                assert not owner.is_connected
        finally:
            finish.set()
            for thread in [reader, updater]:
                if thread.ident is not None:
                    thread.join(5)
            peers.release(borrower)
            peers.disconnect()

    def test_maintenance_config_defers_disconnect_after_checkout(self, pool):
        owner = pool.get_connection()
        pool.release(owner)
        configured, finish = threading.Event(), threading.Event()
        errors = []
        real_disconnect = owner._disconnect_from_pool

        def disconnect(*args, **kwargs):
            configured.set()
            assert finish.wait(5)
            return real_disconnect(*args, **kwargs)

        def configure():
            try:
                pool.update_maint_notifications_config(
                    MaintNotificationsConfig(enabled=True)
                )
            except BaseException as exc:
                errors.append(exc)

        updater = threading.Thread(target=configure, daemon=True)
        acquired = None
        try:
            with patch.object(owner, "_disconnect_from_pool", side_effect=disconnect):
                updater.start()
                assert configured.wait(3)
                acquired = pool.get_connection()
                assert acquired is owner
                assert owner.should_reconnect()
                owner.send_command("PING")
                finish.set()
                updater.join(3)
                assert not updater.is_alive() and not errors
                assert owner._in_use and owner.is_connected
                assert owner.read_response() == b"PONG"
        finally:
            finish.set()
            if updater.ident is not None:
                updater.join(5)
            if acquired is not None:
                pool.release(acquired)
        assert not owner.is_connected

    @pytest.mark.parametrize("shutdown", ["close", "disconnect_free_connections"])
    def test_completion_listener_can_close_cached_pool(self, pool, shutdown):
        events, errors = [], []

        class ClosingListener(EventListenerInterface):
            def listen(self, event):
                try:
                    # Detect the deadlock without leaving a blocked thread in
                    # the failing test's cleanup. The nested close takes this
                    # lock again in _disconnect_from_pool.
                    acquired = event.connection._in_use_lock.acquire(blocking=False)
                    assert acquired, "completion listener holds the owner lock"
                    event.connection._in_use_lock.release()
                    assert not event.connection._idle_maintenance, (
                        "completion listener holds idle ownership"
                    )
                    assert not event.connection.is_connected
                    events.append(event)
                    pool.close()
                except BaseException as exc:
                    errors.append(exc)

        # Initialize maintenance state even when the test server does not
        # support the optional notification handshake.
        config = MaintNotificationsConfig(
            enabled="auto",
            event_dispatcher=EventDispatcher(
                {MaintenanceCompletedEvent: [ClosingListener()]}
            ),
        )
        pool.update_maint_notifications_config(config)
        owners = [pool.get_connection() for _ in range(2)]
        for owner in owners:
            pool.release(owner)
            owner.maintenance_state = MaintenanceState.MAINTENANCE

        getattr(pool, shutdown)()

        assert not errors
        assert len(events) == 2
        assert {event.connection for event in events} == set(owners)
        assert all(not owner.is_connected for owner in owners)

    @pytest.mark.parametrize("outcome", ["close", "listener_error", "parse_error"])
    def test_idle_owner_push_listeners_run_after_drain(self, pool, r2, outcome):
        pool.update_maint_notifications_config(MaintNotificationsConfig(enabled="auto"))
        owner = pool.get_connection()
        peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        borrower = peers.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)
        events, errors = [], []

        class Listener(EventListenerInterface):
            def listen(self, event):
                try:
                    # Fail deterministically on the old code without leaving
                    # a blocked thread behind when close reacquires this lock.
                    acquired = owner._in_use_lock.acquire(blocking=False)
                    assert acquired, "push listener holds the idle owner's lock"
                    owner._in_use_lock.release()
                    assert not owner._idle_maintenance
                    events.append(event)
                    if isinstance(event, MaintenanceCompletedEvent):
                        assert owner.maintenance_state == MaintenanceState.NONE
                        pool.close()
                except BaseException as exc:
                    errors.append(exc)
                if outcome == "listener_error":
                    raise RuntimeError("application listener failed")

        config = MaintNotificationsConfig(
            enabled=True,
            event_dispatcher=EventDispatcher(
                {
                    MaintenanceStartedEvent: [Listener()],
                    MaintenanceCompletedEvent: [Listener()],
                }
            ),
        )
        # A socket pair delivers real RESP3 frames to both parser implementations
        # without requiring an Enterprise server to emit maintenance pushes.
        owner._conn.disconnect()
        client, server = socket.socketpair()
        owner._conn._sock = client
        owner._conn._parser.on_connect(owner._conn)
        handler = MaintNotificationsConnectionHandler(owner, config)
        owner._conn._parser.set_maintenance_push_handler(handler.handle_notification)
        owner.maint_notifications_config = config
        payload = b">3\r\n+MIGRATING\r\n:1\r\n:10\r\n>2\r\n+MIGRATED\r\n:1\r\n"
        if outcome == "parse_error":
            payload += b"?invalid\r\n"
        server.sendall(payload)

        try:
            if outcome == "parse_error":
                with pytest.raises(InvalidResponse):
                    borrower.send_command("GET", key, keys=[key])
            else:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            assert not errors
            assert [type(event) for event in events] == [
                MaintenanceStartedEvent,
                MaintenanceCompletedEvent,
            ]
            assert all(event.connection is owner for event in events)
            assert not owner.is_connected
            # A callback close invalidates the candidate; subsequent replies
            # still come from the correct socket, including after a parse error.
            r2.set(key, "new")
            if outcome != "parse_error":
                # SET's reply on the writer does not guarantee delivery of the
                # invalidation on the borrower's independent connection yet.
                assert borrower.can_read(timeout=1)
            borrower.send_command("GET", key, keys=[key])
            assert borrower.read_response() == b"new"
            borrower.send_command("PING")
            assert borrower.read_response() == b"PONG"
        finally:
            server.close()
            client.close()
            peers.release(borrower)
            peers.disconnect()

    def test_own_cache_hit_drains_once_and_observes_invalidations(self, pool, r2):
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner = pool.get_connection()
        try:
            owner.send_command("GET", key, keys=[key])
            assert owner.read_response() == b"old"
            with (
                patch.object(
                    owner._conn, "can_read", wraps=owner._conn.can_read
                ) as can_read,
                patch.object(
                    owner._conn, "send_command", wraps=owner._conn.send_command
                ) as send,
            ):
                owner.send_command("GET", key, keys=[key])
                assert owner.read_response() == b"old"
                assert can_read.call_count == 1
                send.assert_not_called()

            r2.set(key, "new")
            assert owner._conn.can_read(timeout=1)
            owner.send_command("GET", key, keys=[key])
            assert owner.read_response() == b"new"
            owner.send_command("PING")
            assert owner.read_response() == b"PONG"
        finally:
            pool.release(owner)

    @pytest.mark.skipif(
        not hasattr(signal, "setitimer"), reason="requires interval-timer signals"
    )
    @pytest.mark.parametrize("maintenance", [False, True])
    def test_interrupted_ownership_wait_returns_pool_slot(self, pool, maintenance):
        owner = pool.get_connection()
        pool.release(owner)
        free_before = set(pool._get_free_connections())
        held, finish = threading.Event(), threading.Event()
        errors = []

        class Cancelled(BaseException):
            pass

        def interrupt(signum, frame):
            raise Cancelled()

        def hold():
            try:
                if maintenance:
                    assert owner._try_acquire_idle()
                    try:
                        held.set()
                        assert finish.wait(5)
                    finally:
                        owner._release_idle()
                else:
                    with owner._in_use_lock:
                        held.set()
                        assert finish.wait(5)
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=hold, daemon=True)
        previous_handler = signal.signal(signal.SIGALRM, interrupt)
        previous_timer = None
        try:
            worker.start()
            assert held.wait(3)
            previous_timer = signal.setitimer(signal.ITIMER_REAL, 0.05)
            with pytest.raises(Cancelled):
                pool.get_connection()
            signal.setitimer(signal.ITIMER_REAL, 0)
            assert set(pool._get_free_connections()) == free_before
            if maintenance:
                assert owner._in_use and owner._idle_maintenance
            else:
                assert not owner._in_use
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)
            if previous_timer is not None:
                signal.setitimer(signal.ITIMER_REAL, *previous_timer)
            finish.set()
            if worker.ident is not None:
                worker.join(5)
        assert not worker.is_alive() and not errors
        acquired = pool.get_connection()
        try:
            assert acquired is owner
            acquired.send_command("PING")
            assert acquired.read_response() == b"PONG"
        finally:
            pool.release(acquired)

    def test_concurrent_owner_invalidations_remove_shared_entries(self, pool, r2):
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owners = [pool.get_connection() for _ in range(2)]
        borrowers = [pool.get_connection() for _ in range(2)]
        commands = ["GET", "STRLEN"]
        for owner, command, value in zip(owners, commands, [b"old", 3]):
            owner.send_command(command, key, keys=[key])
            assert owner.read_response() == value
            pool.release(owner)
        r2.set(key, "new-value")
        for owner in owners:
            assert owner._conn.can_read(timeout=1)

        entries = pool.cache.collection
        real_lock, real_holders = entries._lock, entries.holders_of
        real_invalidate = pool.cache.delete_by_redis_keys
        snapshot, finish, attempted, unblocked, second_done = (
            threading.Event() for _ in range(5)
        )
        errors, callback_errors, replies = [], [], [None, None]

        class ObservedLock:
            def __enter__(self):
                if threading.current_thread() is second and not attempted.is_set():
                    acquired = real_lock.acquire(blocking=False)
                    if acquired:
                        unblocked.set()
                    attempted.set()
                    if acquired:
                        return self
                real_lock.acquire()
                return self

            def __exit__(self, *args):
                real_lock.release()

        def holders(redis_key):
            result = real_holders(redis_key)
            if result and threading.current_thread() is first and not snapshot.is_set():
                snapshot.set()
                assert finish.wait(5)
            return result

        def invalidate(redis_keys):
            try:
                return real_invalidate(redis_keys)
            except Exception as exc:
                # The parser logs callback errors and continues the read, so
                # check that the invalidation itself completed as well.
                callback_errors.append(exc)
                raise

        def read(index):
            try:
                borrowers[index].send_command(commands[index], key, keys=[key])
                replies[index] = borrowers[index].read_response()
            except BaseException as exc:
                errors.append(exc)
            finally:
                if index == 1:
                    second_done.set()

        first = threading.Thread(target=read, args=(0,), daemon=True)
        second = threading.Thread(target=read, args=(1,), daemon=True)
        try:
            with (
                patch.object(entries, "_lock", ObservedLock()),
                patch.object(entries, "holders_of", side_effect=holders),
                patch.object(
                    pool.cache, "delete_by_redis_keys", side_effect=invalidate
                ),
            ):
                first.start()
                assert snapshot.wait(3)
                second.start()
                assert attempted.wait(3)
                # If the second invalidation can mutate the snapshot, finish it
                # first. Otherwise let the first atomic invalidation complete.
                if unblocked.is_set():
                    assert second_done.wait(3)
                finish.set()
                first.join(3)
                second.join(3)
                assert not first.is_alive() and not second.is_alive()
                assert not errors
                assert not callback_errors
                assert replies == [b"new-value", 9]
        finally:
            finish.set()
            for thread in [first, second]:
                if thread.ident is not None:
                    thread.join(5)
            for borrower in borrowers:
                pool.release(borrower)

    @pytest.mark.skipif(
        "fork" not in multiprocessing.get_all_start_methods(),
        reason="requires fork",
    )
    def test_fork_does_not_wait_for_inherited_owner_drain(self, pool, r2):
        key = str(uuid.uuid4())
        r2.set(key, "value")
        peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        owner = peers.get_connection()
        borrower = pool.get_connection()
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        peers.release(owner)
        draining, finish = threading.Event(), threading.Event()
        errors = []
        real_can_read = owner._conn.can_read
        context = multiprocessing.get_context("fork")
        parent, child = context.Pipe()

        def can_read(*args, **kwargs):
            if threading.current_thread() is reader:
                draining.set()
                assert finish.wait(10)
            return real_can_read(*args, **kwargs)

        def read():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)

        def read_in_child():
            try:
                child.send(Redis(connection_pool=pool).get(key))
            except BaseException as exc:
                child.send(repr(exc))
            finally:
                child.close()

        reader = threading.Thread(target=read, daemon=True)
        process = context.Process(target=read_in_child)
        try:
            with patch.object(owner._conn, "can_read", side_effect=can_read):
                reader.start()
                assert draining.wait(3)
                process.start()
                assert parent.poll(3), "child waited on an inherited drain lock"
                assert parent.recv() == b"value"
                process.join(3)
                assert process.exitcode == 0
                finish.set()
                reader.join(3)
                assert not reader.is_alive() and not errors
        finally:
            if process.pid is not None:
                if process.is_alive():
                    process.terminate()
                process.join(5)
            finish.set()
            if reader.ident is not None:
                reader.join(5)
            parent.close()
            child.close()
            pool.release(borrower)
            peers.disconnect()

    def test_checkout_waits_for_idle_maintenance(self, pool, r2):
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner = pool.get_connection()
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)
        maintaining, finish, attempted = (threading.Event() for _ in range(3))
        errors, acquired = [], []
        real_disconnect, real_ensure = owner.disconnect, pool.ensure_connection
        real_get = pool.get_connection

        def disconnect(*args, **kwargs):
            maintaining.set()
            assert finish.wait(5)
            return real_disconnect(*args, **kwargs)

        def ensure(connection):
            assert finish.is_set(), "checkout used a socket during idle maintenance"
            return real_ensure(connection)

        def maintain():
            try:
                pool.disconnect_free_connections()
            except BaseException as exc:
                errors.append(exc)

        def checkout():
            attempted.set()
            try:
                acquired.append(real_get())
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=maintain, daemon=True)
        checkout_thread = threading.Thread(target=checkout, daemon=True)
        try:
            with (
                patch.object(owner, "disconnect", side_effect=disconnect),
                patch.object(pool, "ensure_connection", side_effect=ensure),
            ):
                worker.start()
                assert maintaining.wait(3)
                checkout_thread.start()
                assert attempted.wait(3)
                # Leave the maintenance socket operation suspended until checkout
                # has either reached its wait or exposed an overlap.
                checkout_thread.join(0.1)
                finish.set()
                worker.join(3)
                checkout_thread.join(3)
                assert not worker.is_alive() and not checkout_thread.is_alive()
                assert not errors
                assert acquired == [owner]
                assert owner._in_use
                owner.send_command("PING")
                assert owner.read_response() == b"PONG"
        finally:
            finish.set()
            for thread in [worker, checkout_thread]:
                if thread.ident is not None:
                    thread.join(5)
            for connection in acquired:
                pool.release(connection)

    @pytest.mark.parametrize("inuse_connections", [False, True])
    def test_pool_disconnect_joins_idle_owner_drain(self, pool, r2, inuse_connections):
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner = pool.get_connection()
        peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        borrower = peers.get_connection()
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)
        draining, finish, attempted = (threading.Event() for _ in range(3))
        errors = []
        real_can_read, real_disconnect = owner._conn.can_read, owner.disconnect

        def can_read(*args, **kwargs):
            if threading.current_thread() is reader:
                draining.set()
                assert finish.wait(5)
                # A drain can dispatch maintenance that needs the pool lock.
                with pool._lock:
                    pass
            return real_can_read(*args, **kwargs)

        def disconnect(*args, **kwargs):
            assert finish.is_set(), "pool closed a socket during its idle drain"
            return real_disconnect(*args, **kwargs)

        def read():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)

        def close():
            attempted.set()
            try:
                pool.disconnect(inuse_connections=inuse_connections)
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read, daemon=True)
        closer = threading.Thread(target=close, daemon=True)
        try:
            with (
                patch.object(owner._conn, "can_read", side_effect=can_read),
                patch.object(owner, "disconnect", side_effect=disconnect),
            ):
                reader.start()
                assert draining.wait(3)
                closer.start()
                assert attempted.wait(3)
                closer.join(0.1)
                finish.set()
                reader.join(3)
                closer.join(3)
                assert not reader.is_alive() and not closer.is_alive()
                assert not errors
                assert not owner.is_connected
        finally:
            finish.set()
            for thread in [reader, closer]:
                if thread.ident is not None:
                    thread.join(5)
            peers.release(borrower)
            peers.disconnect()

    def test_cached_candidate_does_not_reconnect_disconnected_owner(self, pool, r2):
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner = pool.get_connection()
        peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        borrower = peers.get_connection()
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)
        found, finish = threading.Event(), threading.Event()
        errors, replies = [], []
        real_get = pool.cache.get

        def get(cache_key):
            entry = real_get(cache_key)
            if threading.current_thread() is reader and not found.is_set():
                assert entry is not None and entry.connection_ref is owner
                found.set()
                assert finish.wait(5)
            return entry

        def read():
            try:
                borrower.send_command("GET", key, keys=[key])
                replies.append(borrower.read_response())
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read, daemon=True)
        try:
            with patch.object(pool.cache, "get", side_effect=get):
                reader.start()
                assert found.wait(3)
                pool.disconnect()
                assert not owner.is_connected
                finish.set()
                reader.join(3)
                assert not reader.is_alive() and not errors
                assert replies == [b"value"]
                assert not owner.is_connected
        finally:
            finish.set()
            if reader.ident is not None:
                reader.join(5)
            peers.release(borrower)
            peers.disconnect()

    @pytest.fixture
    def pool(self, request, pool_class, parser_class, tracking_mode):
        config = CacheConfig(
            tracking_mode=tracking_mode,
            cache_predicate=(
                None if tracking_mode is TrackingMode.PLAIN else lambda *_: True
            ),
        )
        pool = pool_class.from_url(
            request.config.getoption("--redis-url"),
            protocol=3,
            parser_class=parser_class,
            cache_config=config,
            max_connections=16,
            socket_timeout=1,
            maint_notifications_config=MaintNotificationsConfig(enabled=False),
        )
        yield pool
        pool.disconnect()

    @pytest.mark.parametrize("invalidate", [False, True])
    def test_busy_cache_owner_keeps_its_command_reply(self, pool, r2, invalidate):
        """A valid cached value does not make its owner's socket idle."""
        foo, bar = f"{uuid.uuid4()}:foo", f"{uuid.uuid4()}:bar"
        r2.mset({foo: "old", bar: "B"})
        owner = pool.get_connection()
        borrower = pool.get_connection()
        try:
            owner.send_command("GET", foo, keys=[foo])
            assert owner.read_response() == b"old"

            # Leave a different command's reply on the checked-out owner's socket.
            owner.send_command("GET", bar, keys=[bar])
            assert owner.can_read(timeout=1)
            if invalidate:
                r2.set(foo, "new")

            borrower.send_command("GET", foo, keys=[foo])
            assert borrower.read_response() == (b"new" if invalidate else b"old")
            assert owner.read_response() == b"B"
        finally:
            pool.release(owner)
            pool.release(borrower)

    def test_busy_owner_serves_hits_as_soon_as_reply_is_cached(self, pool, r2):
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner = pool.get_connection()
        borrower = pool.get_connection()
        published = threading.Event()
        finish_publish = threading.Event()
        errors = []
        real_set = pool.cache.set

        def pause_after_publish(entry):
            result = real_set(entry)
            if entry.connection_ref is owner and entry.status == CacheEntryStatus.VALID:
                published.set()
                assert finish_publish.wait(3)
            return result

        def read_owner_reply():
            try:
                assert owner.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read_owner_reply, daemon=True)
        try:
            with patch("redis.connection.time", wraps=time) as clock:
                clock.monotonic.return_value = time.monotonic()
                owner.send_command("GET", key, keys=[key])
                with patch.object(pool.cache, "set", side_effect=pause_after_publish):
                    reader.start()
                    try:
                        assert published.wait(3)
                        with patch.object(
                            borrower._conn,
                            "send_packed_command",
                            wraps=borrower._conn.send_packed_command,
                        ) as send:
                            borrower.send_command("GET", key, keys=[key])
                            assert borrower.read_response() == b"value"
                            send.assert_not_called()
                    finally:
                        finish_publish.set()
                        reader.join(5)
                assert not reader.is_alive()
                assert not errors
        finally:
            pool.release(owner)
            pool.release(borrower)

    @pytest.mark.parametrize("readers", [2, 8])
    def test_concurrent_readers_keep_hitting_cache(self, pool, r2, readers):
        key = str(uuid.uuid4())
        r2.set(key, "value")
        client = Redis(connection_pool=pool)
        connections = [pool.get_connection() for _ in range(readers)]
        for connection in connections:
            pool.release(connection)

        barrier = threading.Barrier(readers)
        errors = []
        sends = []

        def read_cached():
            connection = pool.get_connection()
            try:
                with patch.object(
                    connection._conn,
                    "send_packed_command",
                    wraps=connection._conn.send_packed_command,
                ) as send:
                    for _ in range(20):
                        barrier.wait(timeout=3)
                        connection.send_command("GET", key, keys=[key])
                        assert connection.read_response() == b"value"
                    sends.append(send.call_count)
            except BaseException as exc:
                errors.append(exc)
                barrier.abort()
            finally:
                pool.release(connection)

        # Keep the invalidation grace period fixed independently of CI scheduling.
        # The expiration test below covers an owner that stops processing reads.
        with patch("redis.connection.time", wraps=time) as clock:
            clock.monotonic.return_value = time.monotonic()
            assert client.get(key) == b"value"
            threads = [
                threading.Thread(target=read_cached, daemon=True)
                for _ in range(readers)
            ]
            try:
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(5)
                assert not any(thread.is_alive() for thread in threads)
                assert not errors
                assert sends == [0] * readers
            finally:
                barrier.abort()
                for thread in threads:
                    thread.join(5)
                client.close()

    def test_busy_owner_stops_serving_hits_when_invalidation_check_expires(
        self, pool, r2
    ):
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner = pool.get_connection()
        borrower = pool.get_connection()
        try:
            with patch("redis.connection.time", wraps=time) as clock:
                clock.monotonic.return_value = time.monotonic()
                owner.send_command("GET", key, keys=[key])
                assert owner.read_response() == b"old"
                r2.set(key, "new")
                assert owner.can_read(timeout=1)

                with patch.object(
                    borrower._conn,
                    "send_packed_command",
                    wraps=borrower._conn.send_packed_command,
                ) as send:
                    borrower.send_command("GET", key, keys=[key])
                    assert borrower.read_response() == b"old"
                    send.assert_not_called()

                # A checked-out connection may remain idle indefinitely, including
                # a single-connection client. Its old entries must expire for peers;
                # the peer's hit above must not renew the owner's grace period.
                clock.monotonic.return_value += owner._BUSY_CACHE_GRACE_SECONDS
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"new"
        finally:
            pool.release(owner)
            pool.release(borrower)

    @pytest.mark.parametrize("command", ["BLPOP", "SUBSCRIBE", "packed"])
    def test_long_lived_owner_does_not_serve_stale_values(self, pool, r2, command):
        key, other = str(uuid.uuid4()), str(uuid.uuid4())
        r2.set(key, "old")
        owner = pool.get_connection()
        borrower = pool.get_connection()
        try:
            owner.send_command("GET", key, keys=[key])
            assert owner.read_response() == b"old"
            if command == "packed":
                owner.send_packed_command(owner.pack_command("BLPOP", other, 0))
            elif command == "BLPOP":
                owner.send_command("BLPOP", other, 0)
            else:
                messages = []
                owner._conn._parser.set_pubsub_push_handler(messages.append)
                owner.send_command("SUBSCRIBE", other)
            r2.set(key, "new")

            borrower.send_command("GET", key, keys=[key])
            assert borrower.read_response() == b"new"
            if command != "SUBSCRIBE":
                r2.rpush(other, "B")
                assert owner.read_response() == [other.encode(), b"B"]
            else:
                assert messages == []
                # An invalidation push can precede the subscription acknowledgement.
                while not messages:
                    owner.read_response(push_request=True)
                assert messages == [[b"subscribe", other.encode(), 1]]
        finally:
            owner.disconnect()
            pool.release(owner)
            pool.release(borrower)

    def test_idle_cache_owner_still_serves_hits_and_invalidations(self, pool, r2):
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner = pool.get_connection()
        borrower = pool.get_connection()
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"old"
        pool.release(owner)

        try:
            with patch.object(
                borrower._conn,
                "send_packed_command",
                wraps=borrower._conn.send_packed_command,
            ) as send:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"old"
                send.assert_not_called()

                r2.set(key, "new")
                assert owner.can_read(timeout=1)
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"new"
                send.assert_called_once()
        finally:
            pool.release(borrower)

    @pytest.mark.parametrize("shared_cache", [False, True])
    def test_checkout_waits_for_an_idle_owner_drain(self, pool, r2, shared_cache):
        """Checkout must join the owner's drain lock before touching the socket."""
        key = str(uuid.uuid4())
        r2.set(key, "value")
        borrower_pool = pool
        if shared_cache:
            other_pool_class = (
                ConnectionPool
                if isinstance(pool, BlockingConnectionPool)
                else BlockingConnectionPool
            )
            borrower_pool = other_pool_class(cache=pool.cache, **pool.connection_kwargs)
        owner = pool.get_connection()
        borrower = borrower_pool.get_connection()
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)

        draining = threading.Event()
        finish_drain = threading.Event()
        checkout_attempted = threading.Event()
        errors = []
        real_can_read = owner._conn.can_read
        real_ensure = pool.ensure_connection
        real_owner_lock = owner._in_use_lock

        class ObservedOwnerLock:
            def __enter__(self):
                if threading.current_thread() is checkout_thread:
                    checkout_attempted.set()
                return real_owner_lock.__enter__()

            def __exit__(self, *args):
                return real_owner_lock.__exit__(*args)

        def pause_drain(*args, **kwargs):
            if threading.current_thread() is reader:
                draining.set()
                assert finish_drain.wait(3)
            return real_can_read(*args, **kwargs)

        def ensure_connection(connection):
            overlaps_drain = not finish_drain.is_set()
            checkout_attempted.set()
            assert not overlaps_drain, "checkout used a socket while it was drained"
            return real_ensure(connection)

        def read_cached():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)

        def checkout():
            try:
                connection = pool.get_connection()
                pool.release(connection)
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read_cached, daemon=True)
        checkout_thread = threading.Thread(target=checkout, daemon=True)
        try:
            with (
                patch.object(owner._conn, "can_read", side_effect=pause_drain),
                patch.object(pool, "ensure_connection", side_effect=ensure_connection),
                patch.object(owner, "_in_use_lock", ObservedOwnerLock()),
            ):
                reader.start()
                assert draining.wait(3)
                checkout_thread.start()
                assert checkout_attempted.wait(3)
                finish_drain.set()
                reader.join(3)
                checkout_thread.join(3)
                assert not reader.is_alive()
                assert not checkout_thread.is_alive()
                assert not errors
        finally:
            finish_drain.set()
            reader.join(3)
            if checkout_thread.ident is not None:
                checkout_thread.join(3)
            borrower_pool.release(borrower)
            if shared_cache:
                borrower_pool.disconnect()

    @pytest.mark.parametrize("operation", ["checkout", "release"])
    def test_idle_owner_drain_does_not_block_other_connections(
        self, pool, r2, operation
    ):
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner = pool.get_connection()
        borrower = pool.get_connection()
        other = pool.get_connection()
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)
        if operation == "checkout":
            # Both pools return the most recently released connection first.
            pool.release(other)

        draining = threading.Event()
        finish_drain = threading.Event()
        finished = threading.Event()
        errors = []
        real_can_read = owner._conn.can_read

        def pause_drain(*args, **kwargs):
            if threading.current_thread() is reader:
                draining.set()
                assert finish_drain.wait(5)
            return real_can_read(*args, **kwargs)

        def read_cached():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)

        def use_other():
            try:
                connection = pool.get_connection() if operation == "checkout" else other
                try:
                    assert connection is other
                finally:
                    pool.release(connection)
                finished.set()
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read_cached, daemon=True)
        worker = threading.Thread(target=use_other, daemon=True)
        try:
            with patch.object(owner._conn, "can_read", side_effect=pause_drain):
                reader.start()
                assert draining.wait(3)
                worker.start()
                try:
                    assert finished.wait(3), (
                        "an unrelated connection waited on the drain"
                    )
                    assert not errors
                finally:
                    finish_drain.set()
                    reader.join(3)
                    worker.join(3)
                assert not reader.is_alive()
                assert not worker.is_alive()
                assert not errors
        finally:
            finish_drain.set()
            reader.join(3)
            if worker.ident is not None:
                worker.join(3)
            pool.release(borrower)

    @pytest.mark.parametrize("operation", ["auth", "moving"])
    def test_idle_drain_defers_pool_socket_operation(self, pool, r2, operation):
        owner_pool = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        owner = owner_pool.get_connection()
        borrower = pool.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        owner_pool.release(owner)
        sock = owner._conn._sock
        draining, finish, completed = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )
        errors = []
        token = Mock()
        token.try_get.return_value = "default"
        token.get_value.return_value = ""
        real_can_read, real_send, real_disconnect = (
            owner._conn.can_read,
            owner._conn.send_command,
            owner.disconnect,
        )

        def can_read(*args, **kwargs):
            if threading.current_thread() is reader:
                draining.set()
                assert finish.wait(5)
            return real_can_read(*args, **kwargs)

        def send(*args, **kwargs):
            assert not draining.is_set() or finish.is_set(), (
                "AUTH overlapped the idle drain"
            )
            return real_send(*args, **kwargs)

        def disconnect(*args, **kwargs):
            assert not draining.is_set() or finish.is_set(), (
                "disconnect overlapped the idle drain"
            )
            return real_disconnect(*args, **kwargs)

        def read_cached():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)

        def maintain():
            try:
                if operation == "auth":
                    owner_pool.re_auth_callback(token)
                else:
                    owner_pool.disconnect_free_connections()
            except BaseException as exc:
                errors.append(exc)
            finally:
                completed.set()

        reader = threading.Thread(target=read_cached, daemon=True)
        worker = threading.Thread(target=maintain, daemon=True)
        try:
            with (
                patch.object(owner._conn, "can_read", side_effect=can_read),
                patch.object(owner._conn, "send_command", side_effect=send),
                patch.object(owner, "disconnect", side_effect=disconnect),
            ):
                reader.start()
                assert draining.wait(3)
                worker.start()
                assert completed.wait(3), (
                    "maintenance waited for the drain under the pool lock"
                )
                assert not errors
                assert owner._conn._sock is sock
                if operation == "auth":
                    assert owner._conn._re_auth_token is token
                else:
                    assert owner.should_reconnect()
                finish.set()
                reader.join(3)
                worker.join(3)
                assert not reader.is_alive() and not worker.is_alive()
                assert not errors
            # Deferred work runs when the connection is returned after its next use.
            reused = owner_pool.get_connection()
            assert reused is owner
            reused.send_command("PING")
            assert reused.read_response() == b"PONG"
            owner_pool.release(reused)
            assert owner._conn._re_auth_token is None
            assert not owner.should_reconnect()
            if operation == "moving":
                assert owner._conn._sock is None
        finally:
            finish.set()
            if reader.ident is not None:
                reader.join(5)
            if worker.ident is not None:
                worker.join(5)
            pool.release(borrower)
            owner_pool.disconnect()

    def test_idle_drain_can_dispatch_moving_during_free_disconnect(self, pool, r2):
        owner = pool.get_connection()
        # A separate pool keeps the borrower's own cache lock out of the lock order.
        peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        borrower = peers.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)
        draining, holding_pool, dispatching, finished = (
            threading.Event() for _ in range(4)
        )
        errors = []
        handler = MaintNotificationsPoolHandler(
            pool, MaintNotificationsConfig(proactive_reconnect=True)
        )
        notification = NodeMovingNotification(
            id=19, new_node_host=None, new_node_port=None, ttl=20
        )
        real_can_read, real_free, real_disconnect = (
            owner._conn.can_read,
            pool._get_free_connections,
            owner.disconnect,
        )
        dispatched = False

        def can_read(*args, **kwargs):
            nonlocal dispatched
            if threading.current_thread() is reader and not dispatched:
                dispatched = True
                draining.set()
                assert holding_pool.wait(3)
                dispatching.set()
                handler.handle_notification(notification)
            return real_can_read(*args, **kwargs)

        def free_connections():
            if threading.current_thread() is worker:
                holding_pool.set()
                assert dispatching.wait(3)
            return real_free()

        def disconnect(*args, **kwargs):
            assert not draining.is_set() or finished.is_set(), (
                "disconnect touched the draining socket"
            )
            return real_disconnect(*args, **kwargs)

        def read_cached():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)
            finally:
                finished.set()

        def maintain():
            try:
                pool.disconnect_free_connections()
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read_cached, daemon=True)
        worker = threading.Thread(target=maintain, daemon=True)
        try:
            with (
                patch.object(owner._conn, "can_read", side_effect=can_read),
                patch.object(
                    pool, "_get_free_connections", side_effect=free_connections
                ),
                patch.object(owner, "disconnect", side_effect=disconnect),
                patch.object(handler, "_schedule"),
            ):
                reader.start()
                assert draining.wait(3)
                worker.start()
                reader.join(5)
                worker.join(5)
                assert not reader.is_alive() and not worker.is_alive(), (
                    "MOVING and idle drain deadlocked"
                )
                assert not errors
                assert owner.should_reconnect()
                assert notification in handler._processed_notifications
            reused = pool.get_connection()
            assert reused is owner
            reused.send_command("PING")
            assert reused.read_response() == b"PONG"
            pool.release(reused)
            assert not owner.should_reconnect()
            assert owner._conn._sock is None
        finally:
            holding_pool.set()
            dispatching.set()
            if reader.ident is not None:
                reader.join(5)
            if worker.ident is not None:
                worker.join(5)
            peers.release(borrower)
            peers.disconnect()

    @pytest.mark.parametrize("auth_fails", [False, True])
    def test_release_event_retains_ownership_without_locking_socket_io(
        self, pool, r2, auth_fails
    ):
        owner_pool = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        owner = owner_pool.get_connection()
        borrower = pool.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        token = Mock()
        token.try_get.return_value = (
            "missing-" + str(uuid.uuid4()) if auth_fails else "default"
        )
        token.get_value.return_value = ""
        owner.set_re_auth_token(token)
        pending = threading.Event()
        finish = threading.Event()
        reader_done = threading.Event()
        auth_errors, read_errors, replies = [], [], []
        real_read = owner._conn.read_response
        real_can_read = owner._conn.can_read

        def auth_read(*args, **kwargs):
            if threading.current_thread() is worker:
                pending.set()
                assert finish.wait(5)
                result = real_read(*args, **kwargs)
                replies.append(result)
                return result
            return real_read(*args, **kwargs)

        def can_read(*args, **kwargs):
            if threading.current_thread() is reader:
                assert not pending.is_set() or finish.is_set(), (
                    "peer touched the socket with an AUTH reply outstanding"
                )
            return real_can_read(*args, **kwargs)

        def release():
            try:
                owner_pool.release(owner)
            except BaseException as exc:
                auth_errors.append(exc)

        def cached_read():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            except BaseException as exc:
                read_errors.append(exc)
            finally:
                reader_done.set()

        worker = threading.Thread(target=release, daemon=True)
        reader = threading.Thread(target=cached_read, daemon=True)
        try:
            with (
                patch.object(owner._conn, "read_response", side_effect=auth_read),
                patch.object(owner._conn, "can_read", side_effect=can_read),
            ):
                worker.start()
                assert pending.wait(3)
                # A peer must be able to inspect ownership while AUTH is paused.
                unlocked = owner._in_use_lock.acquire(blocking=False)
                if unlocked:
                    owner._in_use_lock.release()
                assert unlocked, "release event held the drain lock across socket I/O"
                assert owner._in_use, (
                    "release event exposed its outstanding AUTH reply as idle"
                )
                reader.start()
                assert reader_done.wait(3), "peer cache access blocked on idle AUTH"
                assert not read_errors
                finish.set()
                worker.join(5)
                reader.join(5)
                assert not worker.is_alive() and not reader.is_alive()
                assert not owner._in_use
                assert owner in owner_pool._available_connections
                assert owner not in owner_pool._in_use_connections
                if auth_fails:
                    assert len(auth_errors) == 1 and isinstance(
                        auth_errors[0], AuthenticationError
                    )
                    assert owner._conn._re_auth_token is token
                    assert replies == []
                else:
                    assert auth_errors == []
                    assert owner._conn._re_auth_token is None
                    assert replies == [b"OK"]
            # Clear only the intentionally invalid test token before reusing the
            # connection. The library must not have swallowed its original error.
            owner.set_re_auth_token(None)
            reused = owner_pool.get_connection()
            try:
                assert reused is owner
                reused.send_command("PING")
                assert reused.read_response() == b"PONG"
            finally:
                owner_pool.release(reused)
        finally:
            finish.set()
            if worker.ident is not None:
                worker.join(5)
            if reader.ident is not None:
                reader.join(5)
            owner.set_re_auth_token(None)
            pool.release(borrower)
            owner_pool.disconnect()
