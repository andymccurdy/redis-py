import gc
import multiprocessing
import signal
import socket
import threading
import time
import uuid
import weakref
from unittest.mock import Mock, patch

import pytest

from redis import Redis
from redis._parsers import _HiredisParser, _RESP3Parser
from redis.cache import CacheConfig, CacheEntryStatus, TrackingMode
from redis.cluster import ClusterNode, NodesManager
from redis.connection import BlockingConnectionPool, Connection, ConnectionPool
from redis.event import (
    AfterConnectionReleasedEvent,
    EventDispatcher,
    EventListenerInterface,
    MaintenanceCompletedEvent,
    MaintenanceStartedEvent,
)
from redis.exceptions import AuthenticationError, ConnectionError, InvalidResponse
from redis.maint_notifications import (
    MaintNotificationsConfig,
    MaintNotificationsConnectionHandler,
    MaintNotificationsPoolHandler,
    MaintenanceState,
    NodeMigratingNotification,
    NodeMovingNotification,
    OSSMaintNotificationsHandler,
    OSSNodeMigratedNotification,
    _should_skip_connection_timeout_update,
)
from redis.multidb.event import (
    ActiveDatabaseChanged,
    CloseConnectionOnActiveDatabaseChanged,
)
from redis.utils import HIREDIS_AVAILABLE

from .conftest import skip_if_server_version_lt


def _gc_finalizer_during_idle_drain(
    url, pool_class, parser_class, tracking_mode, outcome
):
    # Run in a bounded child: the regression blocks inside Redis.__del__, where
    # an ordinary test exception cannot safely release the ownership lock.
    pool = pool_class.from_url(
        url,
        protocol=3,
        parser_class=parser_class,
        cache_config=CacheConfig(
            tracking_mode=tracking_mode,
            cache_predicate=None
            if tracking_mode is TrackingMode.PLAIN
            else lambda *_: True,
        ),
        socket_timeout=1,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
    writer = Redis.from_url(url)
    key = str(uuid.uuid4())
    writer.set(key, "old")
    victim = Redis.from_pool(pool)
    assert victim.get(key) == b"old"
    owner = pool.get_connection()
    pool.release(owner)
    borrower = peers.get_connection()
    owner._conn.disconnect()
    client, server = socket.socketpair()
    client.settimeout(1)
    owner._conn._sock = client
    owner._conn._parser.on_connect(owner._conn)
    parser = owner._conn._parser
    read_response = parser.read_response
    disconnect = owner._conn.disconnect
    drain = borrower._drain_invalidations
    collected, closing, reading = [], [], False

    class Cancelled(BaseException):
        pass

    interruption = (
        RuntimeError("drain interrupted") if outcome == "error" else Cancelled()
    )

    def observe_gc(phase, info):
        if phase == "start":
            collected.append(reading)

    def parse(*args, **kwargs):
        nonlocal reading
        reading = True
        try:
            if not collected:
                gc.collect()
                assert abandoned() is None, "pool-owning client was not finalized"
                assert owner._conn._sock is client, "finalizer closed an active parser"
            return read_response(*args, **kwargs)
        finally:
            reading = False

    def close_socket(*args, **kwargs):
        closing.append(reading)
        return disconnect(*args, **kwargs)

    def drain_and_interrupt(conn):
        drain(conn)
        if conn is owner._conn and outcome != "success":
            raise interruption

    was_enabled, thresholds = gc.isenabled(), gc.get_threshold()
    gc.disable()
    gc.collect()
    victim.reference_cycle = victim
    abandoned = weakref.ref(victim)
    del victim
    gc.callbacks.append(observe_gc)
    try:
        writer.set(key, "new")
        encoded = key.encode()
        server.sendall(
            b">2\r\n+invalidate\r\n*1\r\n$"
            + str(len(encoded)).encode()
            + b"\r\n"
            + encoded
            + b"\r\n"
        )
        with (
            patch.object(parser, "read_response", side_effect=parse),
            patch.object(owner._conn, "disconnect", side_effect=close_socket),
            patch.object(
                borrower, "_drain_invalidations", side_effect=drain_and_interrupt
            ),
        ):
            if outcome == "success":
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"new"
            else:
                with pytest.raises(type(interruption)) as error:
                    borrower.send_command("GET", key, keys=[key])
                assert error.value is interruption
        assert collected and collected[0], "GC did not run inside the idle parser"
        assert closing and not any(closing), "socket closed while parser was active"
        assert owner._conn._sock is None
        assert not owner._in_use
        assert owner._in_use_lock.acquire(blocking=False)
        owner._in_use_lock.release()
        # A flushed owner cannot leave a stale cached reply on its peer.
        borrower.send_command("GET", key, keys=[key])
        assert borrower.read_response() == b"new"
        borrower.send_command("PING")
        assert borrower.read_response() == b"PONG"
        reused = pool.get_connection()
        assert reused is owner
        reused.send_command("PING")
        assert reused.read_response() == b"PONG"
        pool.release(reused)
    finally:
        gc.callbacks.remove(observe_gc)
        gc.set_threshold(*thresholds)
        if was_enabled:
            gc.enable()
        else:
            gc.disable()
        server.close()
        peers.release(borrower)
        peers.close()
        pool.close()
        writer.delete(key)
        writer.close()


@pytest.mark.fixed_client
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("7.4.0")
@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("state", list(MaintenanceState))
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
def test_blocking_checkout_during_moving_allows_socket_error_release(
    request, cached, state, parser_class
):
    pool = BlockingConnectionPool.from_url(
        request.config.getoption("--redis-url"),
        protocol=3,
        parser_class=parser_class,
        cache_config=CacheConfig() if cached else None,
        max_connections=1,
        timeout=None,
        socket_timeout=1,
        maint_notifications_config=MaintNotificationsConfig(
            enabled="auto", proactive_reconnect=False
        ),
    )
    owner = pool.get_connection()
    raw = owner._conn if cached else owner
    raw.disconnect()
    client, server = socket.socketpair()
    client.settimeout(1)
    raw._sock = client
    raw._parser.on_connect(raw)
    owner.maintenance_state = state
    attempted, waiting, read_done = (threading.Event() for _ in range(3))
    errors, acquired = [], []
    pool_lock, queue_get = pool._lock, pool.pool.get

    class ObservedLock:
        def acquire(self, *args, **kwargs):
            if threading.current_thread() is borrower:
                attempted.set()
            return pool_lock.acquire(*args, **kwargs)

        def release(self):
            pool_lock.release()

        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, *args):
            self.release()

    def get(*args, **kwargs):
        if threading.current_thread() is borrower:
            waiting.set()
        return queue_get(*args, **kwargs)

    def checkout():
        try:
            connection = pool.get_connection()
            acquired.append(connection)
            connection.send_command("PING")
            assert connection.read_response() == b"PONG"
            pool.release(connection)
        except BaseException as exc:
            errors.append(exc)

    def read_closed_socket():
        try:
            with pytest.raises(ConnectionError):
                owner.read_response()
            pool.release(owner)
        except BaseException as exc:
            errors.append(exc)
        finally:
            read_done.set()

    borrower = threading.Thread(target=checkout, daemon=True)
    reader = threading.Thread(target=read_closed_socket, daemon=True)
    try:
        with (
            patch.object(pool, "_lock", ObservedLock()),
            patch.object(pool.pool, "get", side_effect=get),
        ):
            # Pause at the same boundary as a MOVING handler: a checkout sees
            # the flag, then acquires the lock only after the handoff finishes.
            with pool_lock:
                pool.set_in_maintenance(True)
                borrower.start()
                assert attempted.wait(3)
                pool.set_in_maintenance(False)
            assert waiting.wait(3)
            server.close()
            reader.start()
            assert read_done.wait(3), "socket-error cleanup waited for a pool slot"
            borrower.join(3)
            assert not borrower.is_alive()
            assert not errors
            assert acquired == [owner]
            assert pool.pool.qsize() == 1
    finally:
        pool.set_in_maintenance(False)
        server.close()
        # Unstick the negative control without leaking either worker. The
        # fixed path returns the original connection and never needs a slot.
        if borrower.is_alive() and not read_done.is_set():
            pool.pool.put_nowait(None)
        for worker in (borrower, reader):
            if worker.ident is not None:
                worker.join(4)
        pool.close()
        client.close()


@pytest.mark.fixed_client
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("7.4.0")
@pytest.mark.parametrize("shutdown", ["disconnect", "close"])
@pytest.mark.parametrize("tracking_mode", list(TrackingMode))
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
def test_blocking_pool_release_during_moving_does_not_invert_owner_lock(
    request, shutdown, tracking_mode, parser_class
):
    pool = BlockingConnectionPool.from_url(
        request.config.getoption("--redis-url"),
        protocol=3,
        parser_class=parser_class,
        cache_config=CacheConfig(
            tracking_mode=tracking_mode,
            cache_predicate=None
            if tracking_mode is TrackingMode.PLAIN
            else lambda *_: True,
        ),
        max_connections=1,
        socket_timeout=1,
        maint_notifications_config=MaintNotificationsConfig(
            enabled="auto", proactive_reconnect=False
        ),
    )
    owner = pool.get_connection()
    disconnecting, moving, moved, release_started, owner_waited, finish_moving = (
        threading.Event() for _ in range(6)
    )
    errors = []
    pool_lock, owner_lock = pool._lock, owner._in_use_lock
    real_disconnect = owner._conn.disconnect
    real_set_maintenance = pool.set_in_maintenance

    class PoolLock:
        def acquire(self, *args, **kwargs):
            if threading.current_thread() is releaser:
                release_started.set()
            return pool_lock.acquire(*args, **kwargs)

        def release(self):
            return pool_lock.release()

        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, *args):
            self.release()

    class OwnerLock:
        def acquire(self, *args, **kwargs):
            if threading.current_thread() is releaser:
                release_started.set()
                owner_waited.set()
            return owner_lock.acquire(*args, **kwargs)

        def release(self):
            return owner_lock.release()

        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, *args):
            self.release()

    def disconnect(*args, **kwargs):
        if threading.current_thread() is closer:
            disconnecting.set()
            assert owner_waited.wait(3)
            assert moved.wait(3)
            # Shutdown owns the connection lock and is about to complete
            # maintenance under the pool lock. A release waiting for ownership
            # must not keep that pool lock, or the real call below deadlocks.
            acquired = pool_lock.acquire(blocking=False)
            if acquired:
                pool_lock.release()
            assert acquired, "release holds the pool lock while waiting for ownership"
        return real_disconnect(*args, **kwargs)

    def set_maintenance(value):
        real_set_maintenance(value)
        if value:
            moving.set()
            assert finish_moving.wait(3)

    def run(callback):
        try:
            callback()
        except BaseException as exc:
            errors.append(exc)

    notification = NodeMovingNotification(
        id=1, new_node_host=None, new_node_port=None, ttl=60
    )
    handler = pool._maint_notifications_pool_handler

    def move():
        try:
            handler.handle_notification(notification)
        finally:
            moved.set()

    closer = threading.Thread(target=run, args=(getattr(pool, shutdown),), daemon=True)
    mover = threading.Thread(
        target=run,
        args=(move,),
        daemon=True,
    )
    releaser = threading.Thread(
        target=run, args=(lambda: pool.release(owner),), daemon=True
    )
    try:
        with (
            patch.object(pool, "_lock", PoolLock()),
            patch.object(owner, "_in_use_lock", OwnerLock()),
            patch.object(owner._conn, "disconnect", disconnect),
            patch.object(pool, "set_in_maintenance", set_maintenance),
        ):
            closer.start()
            assert disconnecting.wait(3)
            mover.start()
            assert moving.wait(3)
            releaser.start()
            assert release_started.wait(3)
            finish_moving.set()
            for worker in (mover, releaser, closer):
                worker.join(3)
                assert not worker.is_alive()
            assert not errors
        assert not owner._in_use
        assert pool.pool.qsize() == 1
        assert not owner.is_connected
        handler.cancel_scheduled_tasks()
        reused = pool.get_connection()
        assert reused is owner
        reused.send_command("PING")
        assert reused.read_response() == b"PONG"
        pool.release(reused)
    finally:
        finish_moving.set()
        for worker in (mover, releaser, closer):
            if worker.ident is not None:
                worker.join(4)
        pool.close()


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
    @pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
    def test_gc_finalizer_during_idle_drain(
        self, request, pool_class, parser_class, tracking_mode, outcome
    ):
        context = multiprocessing.get_context("spawn")
        worker = context.Process(
            target=_gc_finalizer_during_idle_drain,
            args=(
                request.config.getoption("--redis-url"),
                pool_class,
                parser_class,
                tracking_mode,
                outcome,
            ),
        )
        worker.start()
        try:
            worker.join(5)
            assert not worker.is_alive(), "client finalizer deadlocked an idle drain"
            assert worker.exitcode == 0
        finally:
            if worker.is_alive():
                worker.terminate()
            worker.join(5)
            worker.close()

    @pytest.mark.parametrize("relaxed_timeout", [0.5, None])
    @pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
    def test_idle_poll_stays_nonblocking_during_moving(
        self, pool, r2, relaxed_timeout, outcome
    ):
        pool.update_maint_notifications_config(
            MaintNotificationsConfig(
                enabled="auto",
                proactive_reconnect=False,
                relaxed_timeout=relaxed_timeout,
            )
        )
        owner, borrower = pool.get_connection(), pool.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"old"
        pool.release(owner)
        owner._conn.disconnect()
        client, server = socket.socketpair()
        client.settimeout(1)
        draining, ready, checked, polling, moved, receiving = (
            threading.Event() for _ in range(6)
        )
        errors, read_timeouts = [], []
        drain = borrower._drain_invalidations

        class Cancelled(BaseException):
            pass

        interruption = (
            RuntimeError("poll interrupted") if outcome == "error" else Cancelled()
        )

        def drain_and_interrupt(conn):
            drain(conn)
            if conn is owner._conn and outcome != "success":
                raise interruption

        class ObservedSocket:
            def __getattr__(self, name):
                return getattr(client, name)

            def gettimeout(self):
                timeout = client.gettimeout()
                if threading.current_thread() is mover and not checked.is_set():
                    checked.set()
                    ready.set()
                    assert polling.wait(3)
                return timeout

            def settimeout(self, timeout):
                client.settimeout(timeout)
                if (
                    threading.current_thread() is reader
                    and timeout == 0
                    and checked.is_set()
                    and not polling.is_set()
                ):
                    polling.set()
                    assert moved.wait(3)

            def recv(self, *args):
                if len(args) == 1 and draining.is_set():
                    read_timeouts.append(client.gettimeout())
                    receiving.set()
                return client.recv(*args)

            def recv_into(self, *args):
                if draining.is_set():
                    read_timeouts.append(client.gettimeout())
                    receiving.set()
                return client.recv_into(*args)

        owner._conn._sock = ObservedSocket()
        owner._conn._parser.on_connect(owner._conn)
        parser = owner._conn._parser
        socket_reader, method = (
            (parser._buffer, "_read_from_socket")
            if isinstance(parser, _RESP3Parser)
            else (parser, "read_from_socket")
        )
        read_socket = getattr(socket_reader, method)
        reads = 0

        def poll(*args, **kwargs):
            nonlocal reads
            reads += 1
            if reads == 2:
                # The first read consumed the partial push and restored the
                # original timeout. Race the next read, when the socket is empty.
                draining.set()
                assert ready.wait(3)
            return read_socket(*args, **kwargs)

        def read():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"old"
            except BaseException as exc:
                errors.append(exc)

        def move():
            try:
                assert draining.wait(3)
                pool._maint_notifications_pool_handler.handle_notification(
                    NodeMovingNotification(104, None, None, 60)
                )
            except BaseException as exc:
                errors.append(exc)
            finally:
                moved.set()
                ready.set()

        reader = threading.Thread(target=read, daemon=True)
        mover = threading.Thread(target=move, daemon=True)
        try:
            # A partial push forces the nonblocking drain to poll again for its
            # missing key list. No extra server data should be needed to return.
            server.sendall(b">2\r\n+invalidate\r\n")
            with (
                patch.object(socket_reader, method, side_effect=poll),
                patch.object(
                    borrower, "_drain_invalidations", side_effect=drain_and_interrupt
                ),
            ):
                reader.start()
                mover.start()
                assert receiving.wait(3)
                assert read_timeouts[0] == 0, "maintenance made the poll blocking"
                reader.join(3)
                mover.join(3)
                assert not reader.is_alive() and not mover.is_alive()
                assert errors == ([] if outcome == "success" else [interruption])
                assert all(timeout == 0 for timeout in read_timeouts)
            assert client.gettimeout() == relaxed_timeout
            parser = owner._conn._parser
            assert (
                parser._buffer.socket_timeout
                if isinstance(parser, _RESP3Parser)
                else parser._socket_timeout
            ) == relaxed_timeout
            r2.set(key, "new")
            server.sendall(f"*1\r\n${len(key)}\r\n{key}\r\n".encode())
            borrower.send_command("GET", key, keys=[key])
            assert borrower.read_response() == b"new"
            pool._maint_notifications_pool_handler.cancel_scheduled_tasks()
            assert owner.maintenance_state is MaintenanceState.NONE
            assert client.gettimeout() == 1
            borrower.send_command("PING")
            assert borrower.read_response() == b"PONG"
        finally:
            ready.set()
            polling.set()
            server.close()
            for worker in (reader, mover):
                if worker.ident is not None:
                    worker.join(4)
            pool._maint_notifications_pool_handler.cancel_scheduled_tasks()
            pool.release(borrower)
            client.close()

    @pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
    def test_rejected_owner_is_idle_before_completion(self, pool, r2, outcome, caplog):
        owner_pool = type(pool)(
            cache=pool.cache, max_connections=1, **pool.connection_kwargs
        )
        owner = owner_pool.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"old"
        r2.set(key, "new")
        calls = []

        class Cancelled(BaseException):
            pass

        class Listener(EventListenerInterface):
            def listen(self, event):
                assert event.connection is owner
                assert not owner.is_connected
                # A discarded connection must no longer report an active
                # borrower to completion listeners or idle-maintenance callers.
                assert owner._try_acquire_idle()
                owner._release_idle()
                calls.append(event)
                owner_pool.close()
                if outcome == "error":
                    raise RuntimeError("completion listener failed")
                if outcome == "cancel":
                    raise Cancelled

        owner.maint_notifications_config = MaintNotificationsConfig(
            enabled=True,
            event_dispatcher=EventDispatcher({MaintenanceCompletedEvent: [Listener()]}),
        )
        owner.maintenance_state = MaintenanceState.MAINTENANCE
        try:
            # Model a topology-aware pool rejecting a checked-out connection.
            # The socket, cache, disconnect and replacement checkout are real.
            with patch.object(
                owner_pool,
                "owns_connection",
                side_effect=lambda conn: conn is not owner,
            ):
                if outcome == "cancel":
                    with pytest.raises(Cancelled):
                        owner_pool.release(owner)
                else:
                    owner_pool.release(owner)
            dispatch_errors = [
                record
                for record in caplog.records
                if record.name == "redis.maint_notifications" and record.exc_info
            ]
            if outcome == "error":
                # Maintenance dispatch logs ordinary listener errors, while
                # cancellation still propagates to the caller.
                assert len(dispatch_errors) == 1
                assert isinstance(dispatch_errors[0].exc_info[1], RuntimeError)
                assert (
                    str(dispatch_errors[0].exc_info[1]) == "completion listener failed"
                )
            else:
                assert not dispatch_errors
            assert len(calls) == 1
            assert not owner._in_use and not owner.is_connected
            # The sole slot is usable even when completion was interrupted.
            replacement = owner_pool.get_connection()
            try:
                assert replacement is not owner
                replacement.send_command("GET", key, keys=[key])
                assert replacement.read_response() == b"new"
                replacement.send_command("PING")
                assert replacement.read_response() == b"PONG"
            finally:
                owner_pool.release(replacement)
        finally:
            owner_pool.disconnect()

    @pytest.mark.parametrize("transition", ["start", "complete", "disconnect"])
    def test_idle_maintenance_transition_preserves_concurrent_handoff(
        self, pool, r2, transition
    ):
        pool.connection_kwargs.update(socket_timeout=0.1, socket_connect_timeout=0.1)
        config = MaintNotificationsConfig(enabled="auto", proactive_reconnect=False)
        pool.update_maint_notifications_config(config)
        owner, borrower, peer = (
            pool.get_connection(),
            pool.get_connection(),
            pool.get_connection(),
        )
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)
        sockets = []
        for conn in (owner, peer):
            conn._conn.disconnect()
            client, server = socket.socketpair()
            sockets.append((client, server))
            client.settimeout(0.1)
            conn._conn._sock = client
            conn._conn._parser.on_connect(conn._conn)
            # The injected socket bypasses the maintenance handshake.
            conn._conn._parser.set_node_moving_push_handler(
                conn._maint_notifications_pool_handler.handle_notification
            )
        if transition != "start":
            owner._maint_notifications_connection_handler.handle_notification(
                NodeMigratingNotification(101, 60)
            )
        checked, attempted, finished = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )
        blocked, errors = [], []
        lock = pool._lock
        reset = owner._conn.reset_tmp_settings

        class ObservedLock:
            def __enter__(self):
                if threading.current_thread() is mover and not attempted.is_set():
                    acquired = lock.acquire(blocking=False)
                    blocked.append(not acquired)
                    attempted.set()
                    if not acquired:
                        lock.acquire()
                else:
                    lock.acquire()
                return self

            def __exit__(self, *args):
                lock.release()

        def pause_after_guard():
            checked.set()
            assert attempted.wait(5)
            # If the transition did not reserve the pool's state, force the
            # handoff to finish before it resumes. Otherwise let it release
            # that reservation before the waiting handoff can run.
            if not blocked[0]:
                assert finished.wait(5)

        def check(state, current_config):
            result = _should_skip_connection_timeout_update(state, current_config)
            if threading.current_thread() is reader and not checked.is_set():
                pause_after_guard()
            return result

        def reset_settings(*args, **kwargs):
            if threading.current_thread() is reader and not checked.is_set():
                pause_after_guard()
            return reset(*args, **kwargs)

        def move():
            try:
                assert checked.wait(5)
                sockets[1][1].sendall(b">4\r\n+MOVING\r\n:102\r\n:60\r\n_\r\n")
                peer._conn.read_response(push_request=True)
            except BaseException as exc:
                errors.append(exc)
            finally:
                finished.set()

        def read():
            try:
                if transition == "disconnect":
                    with pytest.raises(InvalidResponse):
                        borrower.send_command("GET", key, keys=[key])
                else:
                    borrower.send_command("GET", key, keys=[key])
                    assert borrower.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)

        payload = {
            "start": b">3\r\n+MIGRATING\r\n:101\r\n:60\r\n",
            "complete": b">2\r\n+MIGRATED\r\n:101\r\n",
            "disconnect": b"?invalid\r\n",
        }[transition]
        reader = threading.Thread(target=read, daemon=True)
        mover = threading.Thread(target=move, daemon=True)
        try:
            with (
                patch.object(pool, "_lock", ObservedLock()),
                patch.object(peer, "getpeername", return_value=None),
                patch.object(peer._conn, "getpeername", return_value=None),
                patch(
                    "redis.maint_notifications._should_skip_connection_timeout_update",
                    side_effect=check,
                ),
                patch.object(
                    owner._conn, "reset_tmp_settings", side_effect=reset_settings
                ),
            ):
                sockets[0][1].sendall(payload)
                reader.start()
                mover.start()
                reader.join(10)
                mover.join(10)
                assert not reader.is_alive() and not mover.is_alive()
            assert not errors
            assert checked.is_set() and attempted.is_set()
            assert (
                pool.connection_kwargs["maintenance_state"] is MaintenanceState.MOVING
            )
            assert owner.maintenance_state is MaintenanceState.MOVING
            assert owner.socket_timeout == owner.socket_connect_timeout == 10
            if transition != "disconnect":
                assert owner._conn._sock.gettimeout() == 10
                parser = owner._conn._parser
                assert (
                    parser._buffer.socket_timeout
                    if isinstance(parser, _RESP3Parser)
                    else parser._socket_timeout
                ) == 10
            pool._maint_notifications_pool_handler.cancel_scheduled_tasks()
            assert owner.maintenance_state is MaintenanceState.NONE
            assert owner.socket_timeout == owner.socket_connect_timeout == 0.1
            borrower.send_command("PING")
            assert borrower.read_response() == b"PONG"
        finally:
            checked.set()
            for thread in (reader, mover):
                if thread.ident is not None:
                    thread.join(10)
            pool._maint_notifications_pool_handler.cancel_scheduled_tasks()
            for client, server in sockets:
                server.close()
                client.close()
            pool.release(borrower)
            pool.release(peer)

    @pytest.mark.parametrize("initial_handler", [False, True])
    @pytest.mark.parametrize("drain_mode", ["direct", "pending"])
    @pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
    def test_idle_drain_preserves_concurrent_handler_update(
        self, pool, r2, initial_handler, drain_mode, outcome
    ):
        config = MaintNotificationsConfig(enabled="auto")
        old_handler = Mock(spec=OSSMaintNotificationsHandler, config=config)
        replacement = Mock(spec=OSSMaintNotificationsHandler, config=config)
        pool.update_maint_notifications_config(
            config, old_handler if initial_handler else None
        )
        owner, borrower = pool.get_connection(), pool.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"old"
        pool.release(owner)
        r2.set(key, "new")
        owner._conn.disconnect()
        client_socket, server = socket.socketpair()
        owner._conn._sock = client_socket
        owner._conn._parser.on_connect(owner._conn)
        draining, updated, inspected = (threading.Event() for _ in range(3))
        errors, delivered = [], []
        real_disconnect = owner._disconnect_from_pool
        real_drain = borrower._drain_invalidations
        parser = owner._conn._parser

        class Cancelled(BaseException):
            pass

        def receive(name, notification):
            acquired = owner._in_use_lock.acquire(blocking=False)
            if acquired:
                owner._in_use_lock.release()
            else:
                errors.append("topology callback still owns the idle socket")
            delivered.append((name, notification.id))

        old_handler.handle_notification.side_effect = lambda n: receive("old", n)
        replacement.handle_notification.side_effect = lambda n: receive("new", n)

        def disconnect(*args, **kwargs):
            # The public update has installed the new handler and released the
            # pool lock. Let the callback inspect ownership before this other
            # thread can acquire the lock for its own disconnect.
            updated.set()
            assert inspected.wait(5)
            return real_disconnect(*args, **kwargs)

        def reconfigure():
            try:
                assert draining.wait(3)
                pool.update_maint_notifications_config(config, replacement)
            except BaseException as exc:
                errors.append(exc)

        def drain(conn):
            if conn is not owner._conn:
                return real_drain(conn)
            if initial_handler:
                server.sendall(b">3\r\n+SMIGRATED\r\n:91\r\n*0\r\n")
                real_drain(conn)
            draining.set()
            assert updated.wait(3)
            # Notifications read after replacement must use the new handler,
            # but still wait for the socket claim to end.
            server.sendall(b">3\r\n+SMIGRATED\r\n:92\r\n*0\r\n")
            if drain_mode == "pending":
                conn.handle_pending_push_notifications()
            else:
                real_drain(conn)
            if outcome == "error":
                raise RuntimeError("drain failed")
            if outcome == "cancel":
                raise Cancelled

        worker = threading.Thread(target=reconfigure, daemon=True)
        try:
            with (
                patch.object(owner, "_disconnect_from_pool", side_effect=disconnect),
                patch.object(borrower, "_drain_invalidations", side_effect=drain),
            ):
                worker.start()
                if outcome == "success":
                    borrower.send_command("GET", key, keys=[key])
                    assert borrower.read_response() == b"new"
                else:
                    with pytest.raises(
                        RuntimeError if outcome == "error" else Cancelled
                    ):
                        borrower.send_command("GET", key, keys=[key])
                inspected.set()
                worker.join(5)
                assert not worker.is_alive()
            assert (
                parser.oss_cluster_maint_push_handler_func
                == replacement.handle_notification
            )
            assert owner._conn._oss_cluster_maint_notifications_handler is replacement
            assert not errors
            assert delivered == ([("old", 91)] if initial_handler else []) + [
                ("new", 92)
            ]
            reused = pool.get_connection()
            assert reused is owner
            reused.send_command("PING")
            assert reused.read_response() == b"PONG"
            pool.release(reused)
            parser.handle_push_response([b"SMIGRATED", 93, []])
            assert delivered[-1] == ("new", 93)
            assert not errors
            borrower.send_command("GET", key, keys=[key])
            assert borrower.read_response() == b"new"
        finally:
            inspected.set()
            draining.set()
            updated.set()
            if worker.ident is not None:
                worker.join(5)
            server.close()
            client_socket.close()
            pool.release(borrower)

    @pytest.mark.parametrize("push_phase", ["send", "response"])
    @pytest.mark.parametrize("outcome", ["success", "auth_error", "cancel"])
    def test_idle_auth_defers_smigrated_checkout(self, pool, r2, push_phase, outcome):
        auth_pool = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        owner = auth_pool.get_connection()
        borrower = pool.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"old"
        auth_pool.release(owner)
        owner._conn.disconnect()
        client, server = socket.socketpair()
        server.settimeout(3)
        # AuthenticationError closes the connection. Supply a fresh transport
        # for the real topology exchange after that error; handshake behavior
        # is covered separately by the Sentinel reconnect test.
        topology_client, topology_server = socket.socketpair()
        topology_server.settimeout(3)
        owner._conn._sock = client
        owner._conn._parser.on_connect(owner._conn)
        r2.set(key, "new")
        node = ClusterNode(
            owner.host, owner.port, redis_connection=Redis(connection_pool=auth_pool)
        )
        with patch.object(NodesManager, "initialize"):
            manager = NodesManager(
                [node],
                cache=pool.cache,
                **{
                    k: v
                    for k, v in pool.connection_kwargs.items()
                    if k not in ("host", "port", "maint_notifications_config")
                },
            )
        manager.nodes_cache = {node.name: node}
        handler = OSSMaintNotificationsHandler(
            Mock(nodes_manager=manager), MaintNotificationsConfig()
        )
        owner._conn._parser.set_oss_cluster_maint_push_handler(
            handler.handle_notification
        )
        push = (
            b">3\r\n+SMIGRATED\r\n:84\r\n*1\r\n*3\r\n+"
            + node.name.encode()
            + b"\r\n+"
            + node.name.encode()
            + b"\r\n+0-16383\r\n"
        )
        slots = (
            b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n+"
            + node.host.encode()
            + b"\r\n:"
            + str(node.port).encode()
            + b"\r\n+node-id\r\n"
        )
        token = Mock()
        token.try_get.return_value = "default"
        token.get_value.return_value = "test-token"
        commands = [
            b"".join(owner._conn.pack_command(*args))
            for args in [("AUTH", "default", "test-token"), ("CLUSTER SLOTS",)]
        ]
        received, errors = [], []
        real_acquire, real_read = owner._acquire, owner.read_response
        real_connect = owner.connect

        def connect():
            if outcome == "auth_error" and not owner.is_connected:
                owner._conn._sock = topology_client
                owner._conn._parser.on_connect(owner._conn)
            else:
                real_connect()

        class Cancelled(BaseException):
            pass

        class NestedAuthCheckout(BaseException):
            pass

        def acquire():
            # Bound the old implementation's wait on this call's own AUTH
            # claim; the fixed path still performs the real nested checkout.
            if owner._idle_maintenance:
                raise NestedAuthCheckout("topology checkout owns idle AUTH")
            assert pool.cache.size == 0
            assert (
                owner._conn._parser.oss_cluster_maint_push_handler_func
                == handler.handle_notification
            )
            return real_acquire()

        def read(*args, **kwargs):
            response = real_read(*args, **kwargs)
            if outcome == "cancel" and owner._idle_maintenance:
                raise Cancelled
            return response

        def serve():
            try:
                for index, expected in enumerate(commands):
                    transport = (
                        topology_server
                        if index == 1 and outcome == "auth_error"
                        else server
                    )
                    data = b""
                    while len(data) < len(expected):
                        chunk = transport.recv(len(expected) - len(data))
                        if not chunk:
                            return
                        data += chunk
                    assert data == expected
                    received.append(data)
                    if index == 0:
                        reply = (
                            b"-WRONGPASS invalid token\r\n"
                            if outcome == "auth_error"
                            else b"+OK\r\n"
                        )
                        server.sendall(
                            (push if push_phase == "response" else b"") + reply
                        )
                    else:
                        transport.sendall(push + slots)
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=serve, daemon=True)
        try:
            if push_phase == "send":
                server.sendall(push)
            worker.start()
            with (
                patch.object(owner, "connect", side_effect=connect),
                patch.object(owner, "_acquire", side_effect=acquire),
                patch.object(owner, "read_response", side_effect=read),
            ):
                if outcome == "success":
                    auth_pool.re_auth_callback(token)
                else:
                    with pytest.raises(
                        AuthenticationError if outcome == "auth_error" else Cancelled
                    ):
                        auth_pool.re_auth_callback(token)
            worker.join(3)
            assert not worker.is_alive() and not errors
            assert received == commands
            if outcome == "auth_error":
                assert client.fileno() == -1
                assert owner._conn._sock is topology_client
            assert not owner._idle_maintenance and not owner._in_use
            assert len(manager.slots_cache) == 16384
            assert manager.default_node.redis_connection.connection_pool is auth_pool
            borrower.send_command("GET", key, keys=[key])
            assert borrower.read_response() == b"new"
            borrower.send_command("PING")
            assert borrower.read_response() == b"PONG"
        finally:
            owner.disconnect()
            server.close()
            client.close()
            topology_server.close()
            topology_client.close()
            if worker.ident is not None:
                worker.join(5)
            pool.release(borrower)
            auth_pool.disconnect()

    @pytest.mark.parametrize("maintenance", [False, True])
    def test_release_reconnect_completion_runs_after_socket_close(
        self, pool, maintenance
    ):
        pool.update_maint_notifications_config(MaintNotificationsConfig(enabled="auto"))
        owner = pool.get_connection()
        errors, events = [], []

        class Listener(EventListenerInterface):
            def listen(self, event):
                try:
                    assert not owner.is_connected, "release completion precedes close"
                    unlocked = []

                    def probe():
                        acquired = pool._lock.acquire(blocking=False)
                        unlocked.append(acquired)
                        if acquired:
                            pool._lock.release()

                    worker = threading.Thread(target=probe, daemon=True)
                    worker.start()
                    worker.join(2)
                    assert unlocked == [True], "release completion holds pool lock"
                    events.append(event)
                    pool.close()
                except BaseException as exc:
                    errors.append(exc)

        owner.maint_notifications_config = MaintNotificationsConfig(
            enabled=True,
            event_dispatcher=EventDispatcher({MaintenanceCompletedEvent: [Listener()]}),
        )
        owner.maintenance_state = MaintenanceState.MAINTENANCE
        owner.mark_for_reconnect()
        if isinstance(pool, BlockingConnectionPool):
            pool.set_in_maintenance(maintenance)
        try:
            pool.release(owner)
            assert not errors
            assert len(events) == 1 and events[0].connection is owner
            assert not owner.is_connected and not owner._in_use
            reused = pool.get_connection()
            assert reused is owner
            reused.send_command("PING")
            assert reused.read_response() == b"PONG"
            pool.release(reused)
        finally:
            if isinstance(pool, BlockingConnectionPool):
                pool.set_in_maintenance(False)

    @pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
    def test_smigrated_refresh_reuses_owner_after_peer_drain(self, pool, r2, outcome):
        owner = pool.get_connection()
        peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        borrower = peers.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"old"
        pool.release(owner)
        owner._conn.disconnect()
        client, server = socket.socketpair()
        server.settimeout(3)
        owner._conn._sock = client
        owner._conn._parser.on_connect(owner._conn)
        r2.set(key, "new")
        node = ClusterNode(
            owner.host, owner.port, redis_connection=Redis(connection_pool=pool)
        )
        # Only skip construction's initial discovery. The SMIGRATED handler
        # below runs the real initialize, checkout and CLUSTER SLOTS exchange.
        with patch.object(NodesManager, "initialize"):
            manager = NodesManager(
                [node],
                cache=pool.cache,
                **{
                    k: v
                    for k, v in pool.connection_kwargs.items()
                    if k not in ("host", "port", "maint_notifications_config")
                },
            )
        manager.nodes_cache = {node.name: node}
        handler = OSSMaintNotificationsHandler(
            Mock(nodes_manager=manager), MaintNotificationsConfig()
        )
        owner._conn._parser.set_oss_cluster_maint_push_handler(
            handler.handle_notification
        )
        notification = OSSNodeMigratedNotification(
            83, {node.name: [{node.name: "0-16383"}]}
        )
        push = (
            b">3\r\n+SMIGRATED\r\n:83\r\n*1\r\n*3\r\n+"
            + node.name.encode()
            + b"\r\n+"
            + node.name.encode()
            + b"\r\n+0-16383\r\n"
        )
        slots = (
            b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n+"
            + node.host.encode()
            + b"\r\n:"
            + str(node.port).encode()
            + b"\r\n+node-id\r\n"
        )
        command = b"".join(owner._conn.pack_command("CLUSTER SLOTS"))
        commands, errors = [], []
        real_acquire = owner._acquire
        real_read_response = owner._conn.read_response
        interrupted = False

        class Cancelled(BaseException):
            pass

        def read_response(*args, **kwargs):
            nonlocal interrupted
            reply = real_read_response(*args, **kwargs)
            if kwargs.get("push_request") and not interrupted:
                interrupted = True
                if outcome == "error":
                    raise RuntimeError("drain failed")
                if outcome == "cancel":
                    raise Cancelled
            return reply

        class NestedDrainCheckout(BaseException):
            pass

        def acquire():
            # Fail before the old implementation blocks forever on its own
            # non-reentrant lock; do not leave a deadlocked worker in the suite.
            if not owner._in_use_lock.acquire(blocking=False):
                raise NestedDrainCheckout("topology checkout holds the drain lock")
            owner._in_use_lock.release()
            assert pool.cache.size == 0, "SMIGRATED left entries visible before refresh"
            return real_acquire()

        def serve():
            try:
                received = b""
                while len(received) < len(command):
                    chunk = server.recv(len(command) - len(received))
                    if not chunk:
                        return
                    received += chunk
                assert received == command
                commands.append(received)
                # The same notification can arrive on CLUSTER SLOTS too.
                server.sendall(push + slots)
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=serve, daemon=True)
        try:
            invalidation = b">2\r\n+invalidate\r\n*1\r\n+" + key.encode() + b"\r\n"
            server.sendall(push + invalidation)
            worker.start()
            with (
                patch.object(owner, "_acquire", side_effect=acquire),
                patch.object(owner._conn, "read_response", side_effect=read_response),
            ):
                if outcome == "success":
                    borrower.send_command("GET", key, keys=[key])
                    assert borrower.read_response() == b"new"
                else:
                    with pytest.raises(
                        RuntimeError if outcome == "error" else Cancelled
                    ):
                        borrower.send_command("GET", key, keys=[key])
                    borrower.send_command("GET", key, keys=[key])
                    assert borrower.read_response() == b"new"
            worker.join(3)
            assert not worker.is_alive() and not errors
            assert commands == [command]
            assert notification in handler._processed_notifications
            assert len(manager.slots_cache) == 16384
            assert manager.default_node.redis_connection.connection_pool is pool
            assert (
                owner._conn._parser.oss_cluster_maint_push_handler_func
                == handler.handle_notification
            )
            borrower.send_command("PING")
            assert borrower.read_response() == b"PONG"
        finally:
            owner.disconnect()
            server.close()
            client.close()
            if worker.ident is not None:
                worker.join(5)
            peers.release(borrower)
            peers.disconnect()

    @pytest.mark.parametrize(
        "cleanup", ["direct", "removed", "queried", "multidb", "multidb_cluster"]
    )
    @pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
    def test_topology_cleanup_releases_locks_before_completion_listener(
        self, pool, cleanup, outcome
    ):
        pool.update_maint_notifications_config(MaintNotificationsConfig(enabled="auto"))
        owner = pool.get_connection()
        pool.release(owner)
        seed = Redis(connection_pool=pool)
        node = ClusterNode(owner.host, owner.port, redis_connection=seed)
        with patch.object(NodesManager, "initialize"):
            manager = NodesManager(
                [node],
                cache=pool.cache,
                **{
                    k: v
                    for k, v in pool.connection_kwargs.items()
                    if k not in ("host", "port", "maint_notifications_config")
                },
            )
        manager.nodes_cache = {node.name: node} if cleanup == "removed" else {}
        events, errors = [], []

        class Cancelled(BaseException):
            pass

        class Listener(EventListenerInterface):
            def listen(self, event):
                try:
                    unlocked = []

                    def probe():
                        for lock in [
                            pool._lock,
                            manager._initialization_lock,
                            manager._lock,
                        ]:
                            acquired = lock.acquire(blocking=False)
                            unlocked.append(acquired)
                            if acquired:
                                lock.release()

                    worker = threading.Thread(target=probe, daemon=True)
                    worker.start()
                    worker.join(2)
                    assert unlocked == [True, True, True], (
                        "completion listener holds topology or pool lock"
                    )
                    assert not owner._idle_maintenance and not owner.is_connected
                    assert not getattr(pool, "_in_maintenance", False)
                    events.append(event)
                    pool.close()
                except BaseException as exc:
                    errors.append(exc)

        owner.maint_notifications_config = MaintNotificationsConfig(
            enabled=True,
            event_dispatcher=EventDispatcher({MaintenanceCompletedEvent: [Listener()]}),
        )
        owner.maintenance_state = MaintenanceState.MAINTENANCE
        # Supply the server's slot map while retaining the real refresh,
        # removed-node cleanup and finally-block queried-client cleanup.
        slots = [[0, 16383, [owner.host.encode(), owner.port + 1, b"replacement"]]]

        def disconnect(*args, **kwargs):
            was_connected = owner.is_connected
            real_disconnect(*args, **kwargs)
            if was_connected and outcome == "error":
                raise RuntimeError("disconnect failed")
            if was_connected and outcome == "cancel":
                raise Cancelled

        real_disconnect = owner.disconnect
        try:
            with (
                patch.object(seed, "execute_command", return_value=slots),
                patch.object(owner, "disconnect", side_effect=disconnect),
            ):

                def clean():
                    if cleanup == "direct":
                        NodesManager._free_pool_connections(pool)
                    elif cleanup.startswith("multidb"):
                        client = (
                            seed
                            if cleanup == "multidb"
                            else Mock(nodes_manager=manager)
                        )
                        manager.nodes_cache = {node.name: node}
                        CloseConnectionOnActiveDatabaseChanged().listen(
                            ActiveDatabaseChanged(Mock(client=client), Mock(), Mock())
                        )
                    else:
                        manager.initialize()

                if outcome == "success":
                    clean()
                else:
                    with pytest.raises(
                        RuntimeError if outcome == "error" else Cancelled
                    ):
                        clean()
            assert not errors
            assert len(events) == 1 and events[0].connection is owner
            assert not owner.is_connected
        finally:
            manager.close()

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

    @pytest.mark.parametrize(
        "shutdown", ["disconnect_idle", "disconnect_free_connections"]
    )
    @pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
    @pytest.mark.parametrize("listener_fails", [False, True])
    def test_disconnect_completion_precedes_reconnected_maintenance(
        self, pool, r2, shutdown, outcome, listener_fails
    ):
        events, checkout_errors, worker_errors, acquired = [], [], [], []
        unlocked, finish, checked_out = (threading.Event() for _ in range(3))

        class Cancelled(BaseException):
            pass

        class Listener(EventListenerInterface):
            def listen(self, event):
                events.append(event)
                if listener_fails:
                    raise RuntimeError("application listener failed")

        config = MaintNotificationsConfig(
            enabled="auto",
            event_dispatcher=EventDispatcher(
                {
                    MaintenanceStartedEvent: [Listener()],
                    MaintenanceCompletedEvent: [Listener()],
                }
            ),
        )
        pool.update_maint_notifications_config(config)
        owner = pool.get_connection()
        owner.maint_notifications_config = config
        handler = MaintNotificationsConnectionHandler(owner, config)
        first, second = (
            NodeMigratingNotification(1, 10),
            NodeMigratingNotification(2, 10),
        )
        handler.handle_notification(first)
        pool.release(owner)
        key = str(uuid.uuid4())
        r2.set(key, "value")
        real_disconnect = owner._conn.disconnect
        # Pause after the lock that makes checkout possible is actually released.
        # No sleeps or callback hooks are needed to force the competing checkout.
        target = owner if shutdown == "disconnect_idle" else pool
        lock_name = "_in_use_lock" if shutdown == "disconnect_idle" else "_lock"
        real_lock = getattr(target, lock_name)

        class PausingLock:
            def __getattr__(self, name):
                return getattr(real_lock, name)

            def __enter__(self):
                return real_lock.__enter__()

            def __exit__(self, *args):
                result = real_lock.__exit__(*args)
                if (
                    threading.current_thread() is worker
                    and not owner.is_connected
                    and not owner._idle_maintenance
                    and not unlocked.is_set()
                ):
                    unlocked.set()
                    assert finish.wait(5)
                return result

        def disconnect(*args, **kwargs):
            real_disconnect(*args, **kwargs)
            if threading.current_thread() is worker:
                if outcome == "error":
                    raise OSError("disconnect failed after closing the socket")
                if outcome == "cancel":
                    raise Cancelled

        def close_idle():
            try:
                if shutdown == "disconnect_idle":
                    pool.disconnect(inuse_connections=False)
                else:
                    pool.disconnect_free_connections()
            except BaseException as exc:
                worker_errors.append(exc)

        def checkout():
            try:
                reused = pool.get_connection()
                acquired.append(reused)
                assert reused is owner and reused.is_connected
                handler.handle_notification(second)
                reused.send_command("GET", key, keys=[key])
                assert reused.read_response() == b"value"
                reused.send_command("PING")
                assert reused.read_response() == b"PONG"
            except BaseException as exc:
                checkout_errors.append(exc)
            finally:
                checked_out.set()

        worker = threading.Thread(target=close_idle, daemon=True)
        borrower = threading.Thread(target=checkout, daemon=True)
        try:
            with (
                patch.object(target, lock_name, PausingLock()),
                patch.object(owner._conn, "disconnect", side_effect=disconnect),
            ):
                worker.start()
                assert unlocked.wait(3)
                borrower.start()
                assert checked_out.wait(3), "checkout waited for deferred callbacks"
                finish.set()
                worker.join(3)
                borrower.join(3)
                assert not worker.is_alive() and not borrower.is_alive()
                assert not checkout_errors
                if outcome == "success":
                    assert not worker_errors
                else:
                    assert len(worker_errors) == 1
                    assert isinstance(
                        worker_errors[0], OSError if outcome == "error" else Cancelled
                    )
                assert [type(event) for event in events] == [
                    MaintenanceStartedEvent,
                    MaintenanceCompletedEvent,
                    MaintenanceStartedEvent,
                ]
                assert [event.notification for event in events] == [first, None, second]
                assert all(event.connection is owner for event in events)
                assert all(
                    event.state == MaintenanceState.MAINTENANCE for event in events
                )
                assert owner.maintenance_state == MaintenanceState.MAINTENANCE
        finally:
            finish.set()
            if worker.ident is not None:
                worker.join(5)
            if borrower.ident is not None:
                borrower.join(5)
            for connection in acquired:
                pool.release(connection)

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

    @pytest.mark.parametrize("cleanup", ["timer", "direct", "cancel", "start"])
    @pytest.mark.parametrize("listener_error", [False, True])
    def test_moving_cleanup_listener_can_close_during_peer_drain(
        self, pool, r2, cleanup, listener_error
    ):
        pool.update_maint_notifications_config(MaintNotificationsConfig(enabled="auto"))
        owner = pool.get_connection()
        peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        borrower = peers.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)
        draining, resume = threading.Event(), threading.Event()
        events, errors, pushes = [], [], []
        first = NodeMovingNotification(
            id=71, new_node_host=None, new_node_port=None, ttl=60
        )

        class Listener(EventListenerInterface):
            def listen(self, event):
                if event.notification != first:
                    return
                try:
                    unlocked = []

                    def probe():
                        acquired = handler._lock.acquire(blocking=False)
                        unlocked.append(acquired)
                        if acquired:
                            handler._lock.release()

                    worker = threading.Thread(target=probe, daemon=True)
                    worker.start()
                    worker.join(2)
                    # Fail before close() waits for the owner on the old code,
                    # so a negative control cannot leave deadlocked threads.
                    assert unlocked == [True], "maintenance callback holds handler lock"
                    events.append(event)
                    resume.set()
                    pool.close()
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    resume.set()
                if listener_error:
                    raise RuntimeError("application listener failed")

        config = MaintNotificationsConfig(
            enabled=True,
            proactive_reconnect=False,
            event_dispatcher=EventDispatcher(
                {
                    (
                        MaintenanceStartedEvent
                        if cleanup == "start"
                        else MaintenanceCompletedEvent
                    ): [Listener()]
                }
            ),
        )
        handler = MaintNotificationsPoolHandler(pool, config)
        pool._maint_notifications_pool_handler = handler
        owner.maint_notifications_config = config
        owner._conn.disconnect()
        client, server = socket.socketpair()
        owner._conn._sock = client
        owner._conn._parser.on_connect(owner._conn)
        owner.set_maint_notifications_pool_handler_for_connection(handler)
        real_can_read = owner._conn.can_read

        def handle_push(notification):
            owner._conn._maint_notifications_pool_handler.handle_notification(
                notification
            )
            pushes.append(notification.id)

        owner._conn._parser.set_node_moving_push_handler(handle_push)

        def can_read(*args, **kwargs):
            if threading.current_thread() is reader and not draining.is_set():
                draining.set()
                assert resume.wait(5)
            return real_can_read(*args, **kwargs)

        def read():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
                borrower.send_command("PING")
                assert borrower.read_response() == b"PONG"
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read, daemon=True)
        try:
            # Run the real scheduled wrapper deterministically, without waiting
            # for a wall-clock TTL or changing its lock/cancellation semantics.
            with (
                patch("redis.maint_notifications.threading.Timer") as timer,
                # AF_UNIX socket pairs have no IP address to match against.
                patch.object(owner._conn, "getpeername", return_value="127.0.0.1"),
            ):
                if cleanup != "start":
                    handler.handle_notification(first)
                    expire = timer.call_args.args[1]
                server.sendall(b">4\r\n+MOVING\r\n:72\r\n:60\r\n_\r\n")
                with patch.object(owner._conn, "can_read", side_effect=can_read):
                    reader.start()
                    assert draining.wait(3)
                    if cleanup == "timer":
                        expire()
                    elif cleanup == "direct":
                        assert handler.handle_node_moved_notification(first)
                    elif cleanup == "start":
                        handler.handle_notification(first)
                    else:
                        handler.cancel_scheduled_tasks()
                    reader.join(3)
                    assert not reader.is_alive()
                    assert not errors
                    assert len(events) == 1
                    assert not owner.is_connected
                    assert pushes == [72]
            r2.set(key, "new")
            if peers.cache.size:
                assert borrower.can_read(timeout=1)
            borrower.send_command("GET", key, keys=[key])
            assert borrower.read_response() == b"new"
        finally:
            resume.set()
            if reader.ident is not None:
                reader.join(5)
            handler.cancel_scheduled_tasks()
            server.close()
            client.close()
            peers.release(borrower)
            peers.disconnect()

    @pytest.mark.parametrize("cleanup", ["timer", "direct", "cancel"])
    @pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
    @pytest.mark.parametrize("listener_error", [False, True])
    def test_moving_events_keep_order_across_peer_drain_and_cleanup(
        self, pool, r2, cleanup, outcome, listener_error
    ):
        class DrainCancelled(BaseException):
            pass

        events, errors, pushes = [], [], []
        moving, resume = threading.Event(), threading.Event()

        class Listener(EventListenerInterface):
            def listen(self, event):
                events.append(event)
                if listener_error:
                    raise RuntimeError("application listener failed")

        config = MaintNotificationsConfig(
            enabled="auto",
            proactive_reconnect=False,
            event_dispatcher=EventDispatcher(
                {
                    MaintenanceStartedEvent: [Listener()],
                    MaintenanceCompletedEvent: [Listener()],
                }
            ),
        )
        pool.update_maint_notifications_config(config)
        owner = pool.get_connection()
        peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        borrower = peers.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)
        handler = MaintNotificationsPoolHandler(pool, config)
        pool._maint_notifications_pool_handler = handler
        owner._conn.disconnect()
        client, server = socket.socketpair()
        owner._conn._sock = client
        owner._conn._parser.on_connect(owner._conn)
        owner.set_maint_notifications_pool_handler_for_connection(handler)

        def handle_push(notification):
            owner._conn._maint_notifications_pool_handler.handle_notification(
                notification
            )
            pushes.append(notification)

        owner._conn._parser.set_node_moving_push_handler(handle_push)
        real_can_read = owner._conn.can_read

        def can_read(*args, **kwargs):
            if threading.current_thread() is reader and pushes:
                moving.set()
                assert resume.wait(5)
                if outcome == "error":
                    raise InvalidResponse("interrupted owner drain")
                if outcome == "cancel":
                    raise DrainCancelled()
            return real_can_read(*args, **kwargs)

        def read():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read, daemon=True)
        try:
            with (
                patch("redis.maint_notifications.threading.Timer") as timer,
                patch.object(owner._conn, "getpeername", return_value="127.0.0.1"),
                patch.object(owner._conn, "can_read", side_effect=can_read),
            ):
                server.sendall(b">4\r\n+MOVING\r\n:81\r\n:60\r\n_\r\n")
                reader.start()
                assert moving.wait(3)
                assert len(pushes) == 1
                if cleanup == "timer":
                    timer.call_args.args[1]()
                elif cleanup == "direct":
                    assert handler.handle_node_moved_notification(pushes[0])
                else:
                    handler.cancel_scheduled_tasks()
                assert (
                    pool.connection_kwargs["maintenance_state"] == MaintenanceState.NONE
                )
                resume.set()
                reader.join(3)
                assert not reader.is_alive()
                if outcome == "success":
                    assert not errors
                else:
                    expected = InvalidResponse if outcome == "error" else DrainCancelled
                    assert len(errors) == 1 and isinstance(errors[0], expected)
                assert [type(event) for event in events] == [
                    MaintenanceStartedEvent,
                    MaintenanceCompletedEvent,
                ]
                assert all(event.notification == pushes[0] for event in events)
                assert all(event.connection_pool is pool for event in events)
                assert not handler._pending_handoffs
            borrower.send_command("PING")
            assert borrower.read_response() == b"PONG"
        finally:
            resume.set()
            if reader.ident is not None:
                reader.join(5)
            handler.cancel_scheduled_tasks()
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

    @pytest.mark.parametrize("outcome", ["close", "error", "cancel"])
    def test_release_listener_can_close_during_idle_moving(self, pool, r2, outcome):
        owner_pool = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        owner, released = owner_pool.get_connection(), owner_pool.get_connection()
        borrower = pool.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        owner_pool.release(owner)
        draining, listening, dispatching, finish = (threading.Event() for _ in range(4))
        read_errors, release_errors, calls = [], [], []
        handler = MaintNotificationsPoolHandler(
            owner_pool, MaintNotificationsConfig(proactive_reconnect=True)
        )
        notification = NodeMovingNotification(
            id=41, new_node_host=None, new_node_port=None, ttl=20
        )
        real_can_read = owner._conn.can_read

        class Cancelled(BaseException):
            pass

        class Listener(EventListenerInterface):
            def listen(self, event):
                if event.connection is not released:
                    return
                calls.append(event)
                unlocked = []

                def probe():
                    acquired = owner_pool._lock.acquire(blocking=False)
                    unlocked.append(acquired)
                    if acquired:
                        owner_pool._lock.release()

                probe_thread = threading.Thread(target=probe, daemon=True)
                probe_thread.start()
                probe_thread.join(2)
                assert unlocked == [True], "release listener holds the pool lock"
                assert released._in_use
                assert released not in owner_pool._available_connections
                # Recursive release must remain a no-op, while close must still
                # find this connection among the pool's active connections.
                owner_pool.release(released)
                assert len(calls) == 1
                listening.set()
                assert dispatching.wait(3)
                owner_pool.close()
                assert not released.is_connected and not owner.is_connected
                if outcome == "error":
                    raise RuntimeError("release listener failed")
                if outcome == "cancel":
                    raise Cancelled

        owner_pool._event_dispatcher.register_listeners(
            {AfterConnectionReleasedEvent: [Listener()]}
        )
        notified = False

        def can_read(*args, **kwargs):
            nonlocal notified
            if threading.current_thread() is reader and not notified:
                notified = True
                draining.set()
                assert listening.wait(5)
                if not finish.is_set():
                    dispatching.set()
                    handler.handle_notification(notification)
            return real_can_read(*args, **kwargs)

        def read():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
                borrower.send_command("PING")
                assert borrower.read_response() == b"PONG"
            except BaseException as exc:
                read_errors.append(exc)

        def release():
            try:
                owner_pool.release(released)
            except BaseException as exc:
                release_errors.append(exc)

        reader = threading.Thread(target=read, daemon=True)
        worker = threading.Thread(target=release, daemon=True)
        try:
            with (
                patch.object(owner._conn, "can_read", side_effect=can_read),
                patch.object(handler, "_schedule"),
            ):
                reader.start()
                assert draining.wait(3)
                worker.start()
                worker.join(3)
                # Unblock the old-head negative control after its lock guard fails.
                finish.set()
                listening.set()
                reader.join(3)
                assert not worker.is_alive() and not reader.is_alive()
                assert not read_errors
                if outcome == "close":
                    assert not release_errors
                else:
                    expected = RuntimeError if outcome == "error" else Cancelled
                    assert len(release_errors) == 1 and isinstance(
                        release_errors[0], expected
                    )
                assert len(calls) == 1
                assert notification in handler._processed_notifications
                assert not released._in_use
                assert released not in owner_pool._in_use_connections
                assert owner_pool._available_connections.count(released) == 1
        finally:
            finish.set()
            listening.set()
            dispatching.set()
            if reader.ident is not None:
                reader.join(5)
            if worker.ident is not None:
                worker.join(5)
            pool.release(borrower)
            owner_pool.disconnect()

    @pytest.mark.parametrize("mark_before_release", [True, False])
    def test_release_reconnect_completion_listener_runs_outside_pool_lock(
        self, pool, mark_before_release
    ):
        owner_pool = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        owner = owner_pool.get_connection()
        events, errors = [], []

        class CompletionListener(EventListenerInterface):
            def listen(self, event):
                try:
                    unlocked = []

                    def probe():
                        acquired = owner_pool._lock.acquire(blocking=False)
                        unlocked.append(acquired)
                        if acquired:
                            owner_pool._lock.release()

                    worker = threading.Thread(target=probe, daemon=True)
                    worker.start()
                    worker.join(2)
                    assert unlocked == [True], "completion listener holds pool lock"
                    assert not owner.is_connected
                    if not mark_before_release:
                        assert not owner._in_use
                    events.append(event)
                    owner_pool.close()
                except BaseException as exc:
                    errors.append(exc)

        class ReleaseListener(EventListenerInterface):
            def listen(self, event):
                if not mark_before_release:
                    owner.mark_for_reconnect()

        owner.maint_notifications_config = MaintNotificationsConfig(
            enabled=True,
            event_dispatcher=EventDispatcher(
                {MaintenanceCompletedEvent: [CompletionListener()]}
            ),
        )
        owner.maintenance_state = MaintenanceState.MAINTENANCE
        owner_pool._event_dispatcher.register_listeners(
            {AfterConnectionReleasedEvent: [ReleaseListener()]}
        )
        if mark_before_release:
            owner.mark_for_reconnect()
        try:
            owner_pool.release(owner)
            assert not errors
            assert len(events) == 1
            assert not owner.is_connected and not owner._in_use
            assert owner not in owner_pool._in_use_connections
            assert owner_pool._available_connections.count(owner) == 1
        finally:
            owner_pool.disconnect()

    @pytest.mark.parametrize("outcome", ["success", "auth_error", "cancel"])
    def test_new_idle_refresh_retires_deferred_token(self, pool, r2, outcome):
        owner_pool = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        owner = owner_pool.get_connection()
        owner_pool.release(owner)
        username = "cache-refresh-" + str(uuid.uuid4())
        token_a, token_b = Mock(), Mock()
        for token, value in [(token_a, "old"), (token_b, "new")]:
            token.try_get.return_value = username
            token.get_value.return_value = value

        class Cancelled(BaseException):
            pass

        try:
            r2.execute_command("ACL", "SETUSER", username, "on", ">old", "~*", "+@all")
            # Model a peer holding the idle drain lock when token A arrives.
            with owner._in_use_lock:
                owner_pool.re_auth_callback(token_a)
            assert owner._conn._re_auth_token is token_a
            if outcome == "success":
                r2.execute_command("ACL", "SETUSER", username, "resetpass", ">new")
                owner_pool.re_auth_callback(token_b)
                assert owner._conn._re_auth_token is None
            elif outcome == "auth_error":
                with pytest.raises(AuthenticationError):
                    owner_pool.re_auth_callback(token_b)
                assert owner._conn._re_auth_token is token_a
            else:
                with patch.object(owner, "read_response", side_effect=Cancelled):
                    with pytest.raises(Cancelled):
                        owner_pool.re_auth_callback(token_b)
                assert owner._conn._re_auth_token is token_a
                owner.disconnect()
            reused = owner_pool.get_connection()
            assert reused is owner
            reused.send_command("PING")
            assert reused.read_response() == b"PONG"
            # The expired token A must not undo successful authentication with B.
            owner_pool.release(reused)
            assert owner._conn._re_auth_token is None
        finally:
            owner.set_re_auth_token(None)
            owner_pool.disconnect()
            r2.execute_command("ACL", "DELUSER", username)

    def test_token_refresh_during_release_keeps_newest_token(self, pool, r2):
        owner_pool = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        owner = owner_pool.get_connection()
        username = "cache-release-" + str(uuid.uuid4())
        token_a, token_b = Mock(), Mock()
        for token, value in [(token_a, "old"), (token_b, "new")]:
            token.try_get.return_value = username
            token.get_value.return_value = value
        pending, finish = threading.Event(), threading.Event()
        errors = []
        real_read = owner._conn.read_response

        def read_response(*args, **kwargs):
            result = real_read(*args, **kwargs)
            if threading.current_thread() is worker:
                pending.set()
                assert finish.wait(5)
            return result

        def release():
            try:
                owner_pool.release(owner)
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=release, daemon=True)
        try:
            r2.execute_command("ACL", "SETUSER", username, "on", ">old", "~*", "+@all")
            owner.set_re_auth_token(token_a)
            with patch.object(owner._conn, "read_response", side_effect=read_response):
                worker.start()
                assert pending.wait(3)
                acquired = owner_pool._lock.acquire(blocking=False)
                if acquired:
                    owner_pool._lock.release()
                assert acquired, "release AUTH blocks a newer token refresh"
                assert owner._in_use and owner not in owner_pool._available_connections
                r2.execute_command("ACL", "SETUSER", username, "resetpass", ">new")
                owner_pool.re_auth_callback(token_b)
                finish.set()
                worker.join(3)
                assert not worker.is_alive() and not errors
                assert owner._conn._re_auth_token is token_b
            reused = owner_pool.get_connection()
            assert reused is owner
            reused.send_command("PING")
            assert reused.read_response() == b"PONG"
            owner_pool.release(reused)
            assert owner._conn._re_auth_token is None
        finally:
            finish.set()
            if worker.ident is not None:
                worker.join(5)
            owner.set_re_auth_token(None)
            owner_pool.disconnect()
            r2.execute_command("ACL", "DELUSER", username)

    def test_smigrated_removed_node_preserves_idle_owner_drain(self, pool, r2):
        owner = pool.get_connection()
        peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        borrower = peers.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"old"
        pool.release(owner)
        owner._conn.disconnect()
        client, server = socket.socketpair()
        owner._conn._sock = client
        owner._conn._parser.on_connect(owner._conn)
        r2.set(key, "new")
        server.sendall(
            b">2\r\n+invalidate\r\n*1\r\n$"
            + str(len(key)).encode()
            + b"\r\n"
            + key.encode()
            + b"\r\n"
        )
        node = Mock(redis_connection=Redis(connection_pool=pool))
        cluster = Mock()
        cluster.nodes_manager._initialization_lock = threading.RLock()
        cluster.nodes_manager.get_node.return_value = node
        cluster.nodes_manager.nodes_cache = {"source": node}

        def initialize(**kwargs):
            NodesManager._free_pool_connections(pool)
            cluster.nodes_manager.nodes_cache = {}

        cluster.nodes_manager.initialize.side_effect = initialize
        handler = OSSMaintNotificationsHandler(cluster, MaintNotificationsConfig())
        notification = OSSNodeMigratedNotification(
            id=31,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "0-10"}]},
        )
        draining, finish = threading.Event(), threading.Event()
        errors = []
        real_can_read = owner._conn.can_read

        def can_read(*args, **kwargs):
            readable = real_can_read(*args, **kwargs)
            if threading.current_thread() is reader and not draining.is_set():
                assert readable
                draining.set()
                assert finish.wait(5)
            return readable

        def read():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"new"
                borrower.send_command("PING")
                assert borrower.read_response() == b"PONG"
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read, daemon=True)
        try:
            with patch.object(owner._conn, "can_read", side_effect=can_read):
                reader.start()
                assert draining.wait(3)
                handler.handle_notification(notification)
                finish.set()
                reader.join(3)
                assert not reader.is_alive()
                assert not errors
                assert not owner.is_connected
                assert not owner.should_reconnect()
                assert notification in handler._processed_notifications
        finally:
            finish.set()
            if reader.ident is not None:
                reader.join(5)
            server.close()
            client.close()
            peers.release(borrower)
            peers.disconnect()

    @pytest.mark.parametrize("cleanup", ["topology", "smigrated"])
    @pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
    def test_removed_idle_owner_disconnects_after_drain(
        self, pool, r2, cleanup, outcome
    ):
        owner = pool.get_connection()
        peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        borrower = peers.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"old"
        pool.release(owner)
        # Model a removed replica: its tracking socket remains open but never
        # sends an invalidation for writes on the replacement node.
        owner._conn.disconnect()
        client, server = socket.socketpair()
        owner._conn._sock = client
        owner._conn._parser.on_connect(owner._conn)
        r2.set(key, "new")
        node = Mock(redis_connection=Redis(connection_pool=pool))
        cluster = Mock()
        cluster.nodes_manager._initialization_lock = threading.RLock()
        cluster.nodes_manager.get_node.return_value = node
        cluster.nodes_manager.nodes_cache = {}
        handler = OSSMaintNotificationsHandler(cluster, MaintNotificationsConfig())
        notification = OSSNodeMigratedNotification(
            id=43,
            nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "0-10"}]},
        )
        draining, finish = threading.Event(), threading.Event()
        errors, disconnected = [], []
        real_can_read, real_disconnect = owner._conn.can_read, owner.disconnect

        class Cancelled(BaseException):
            pass

        def can_read(*args, **kwargs):
            if threading.current_thread() is reader:
                draining.set()
                assert finish.wait(5)
                if outcome == "error":
                    raise RuntimeError("drain failed")
                if outcome == "cancel":
                    raise Cancelled
            return real_can_read(*args, **kwargs)

        def disconnect(*args, **kwargs):
            assert finish.is_set(), "cleanup interrupted the active drain"
            acquired = owner._in_use_lock.acquire(blocking=False)
            if acquired:
                owner._in_use_lock.release()
            assert not acquired, "deferred disconnect lost socket ownership"
            disconnected.append(True)
            return real_disconnect(*args, **kwargs)

        def read():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"new"
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read, daemon=True)
        try:
            with (
                patch.object(owner._conn, "can_read", side_effect=can_read),
                patch.object(owner, "disconnect", side_effect=disconnect),
            ):
                reader.start()
                assert draining.wait(3)
                if cleanup == "topology":
                    NodesManager._free_pool_connections(pool)
                else:
                    handler.handle_notification(notification)
                assert owner._conn._sock is client
                assert owner.should_reconnect()
                finish.set()
                reader.join(3)
                assert not reader.is_alive()
                assert disconnected == [True]
                assert not owner.is_connected and not owner.should_reconnect()
                if outcome == "success":
                    assert not errors
                else:
                    expected = RuntimeError if outcome == "error" else Cancelled
                    assert len(errors) == 1 and isinstance(errors[0], expected)
            # No checkout or release of the removed owner's pool is needed.
            for _ in range(3):
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"new"
            borrower.send_command("PING")
            assert borrower.read_response() == b"PONG"
        finally:
            finish.set()
            if reader.ident is not None:
                reader.join(5)
            server.close()
            client.close()
            peers.release(borrower)
            peers.disconnect()

    @pytest.mark.parametrize("cleanup_during_refresh", [False, True])
    def test_smigrated_removed_idle_node_listener_can_close_pool(
        self, pool, cleanup_during_refresh
    ):
        pool.update_maint_notifications_config(MaintNotificationsConfig(enabled="auto"))
        owner = pool.get_connection()
        pool.release(owner)
        events, errors = [], []

        class Listener(EventListenerInterface):
            def listen(self, event):
                try:
                    unlocked = []

                    def probe():
                        for lock in [
                            pool._lock,
                            cluster.nodes_manager._initialization_lock,
                            handler._lock,
                        ]:
                            acquired = lock.acquire(blocking=False)
                            unlocked.append(acquired)
                            if acquired:
                                lock.release()

                    worker = threading.Thread(target=probe, daemon=True)
                    worker.start()
                    worker.join(2)
                    assert unlocked == [True, True, True], (
                        "listener holds a pool or topology lock"
                    )
                    assert not owner._idle_maintenance
                    assert not owner.is_connected
                    events.append(event)
                    pool.close()
                except BaseException as exc:
                    errors.append(exc)

        owner.maint_notifications_config = MaintNotificationsConfig(
            enabled=True,
            event_dispatcher=EventDispatcher({MaintenanceCompletedEvent: [Listener()]}),
        )
        owner.maintenance_state = MaintenanceState.MAINTENANCE
        node = Mock(redis_connection=Redis(connection_pool=pool))
        cluster = Mock()
        cluster.nodes_manager._initialization_lock = threading.RLock()
        cluster.nodes_manager.get_node.return_value = node
        cluster.nodes_manager.nodes_cache = {}
        if cleanup_during_refresh:
            cluster.nodes_manager.initialize.side_effect = (
                lambda **_: NodesManager._free_pool_connections(pool)
            )
        handler = OSSMaintNotificationsHandler(cluster, MaintNotificationsConfig())
        handler.handle_notification(
            OSSNodeMigratedNotification(
                id=32,
                nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "0-10"}]},
            )
        )
        assert not errors
        assert len(events) == 1 and events[0].connection is owner
        assert not owner.is_connected

    def test_free_disconnect_retires_socket_before_concurrent_checkout(self, pool):
        owner = pool.get_connection()
        owner.send_command("CLIENT", "ID")
        old_id = owner.read_response()
        pool.release(owner)
        captured, finish = threading.Event(), threading.Event()
        errors = []
        real_disconnect = owner._disconnect_from_pool

        def disconnect(inuse_connections):
            captured.set()
            assert finish.wait(5)
            real_disconnect(inuse_connections)

        def retire():
            try:
                pool.disconnect(inuse_connections=False)
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=retire, daemon=True)
        checked_out = None
        try:
            with patch.object(owner, "_disconnect_from_pool", side_effect=disconnect):
                worker.start()
                assert captured.wait(3)
                checked_out = pool.get_connection()
                finish.set()
                worker.join(3)
                assert not worker.is_alive() and not errors
                checked_out.send_command("CLIENT", "ID")
                assert checked_out.read_response() != old_id
                checked_out.send_command("PING")
                assert checked_out.read_response() == b"PONG"
        finally:
            finish.set()
            worker.join(5)
            if checked_out is not None:
                pool.release(checked_out)

    @pytest.mark.parametrize("outcome", ["success", "auth_error", "cancel"])
    def test_idle_auth_finishes_moving_reconnect(self, pool, r2, outcome):
        auth_pool = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        auth_pool.update_maint_notifications_config(
            MaintNotificationsConfig(enabled="auto")
        )
        owner = auth_pool.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        auth_pool.release(owner)
        events, errors = [], []

        class Cancelled(BaseException):
            pass

        class Listener(EventListenerInterface):
            def listen(self, event):
                events.append((owner._idle_maintenance, owner.is_connected))

        config = MaintNotificationsConfig(
            enabled=True,
            event_dispatcher=EventDispatcher({MaintenanceStartedEvent: [Listener()]}),
        )
        handler = MaintNotificationsPoolHandler(auth_pool, config)
        auth_pool._maint_notifications_pool_handler = handler
        owner.maint_notifications_config = config
        owner._conn.disconnect()
        client, server = socket.socketpair()
        server.settimeout(3)
        owner._conn._sock = client
        owner._conn._parser.on_connect(owner._conn)
        owner.set_maint_notifications_pool_handler_for_connection(handler)
        owner._conn._parser.set_node_moving_push_handler(handler.handle_notification)
        token = Mock()
        token.try_get.return_value = "default"
        token.get_value.return_value = "test-token"
        command = b"".join(owner._conn.pack_command("AUTH", "default", "test-token"))
        port = auth_pool.connection_kwargs.get("port", 6379)
        moving = f">4\r\n+MOVING\r\n:103\r\n:60\r\n+127.0.0.1:{port}\r\n".encode()

        def authenticate():
            try:
                received = b""
                while len(received) < len(command):
                    part = server.recv(len(command) - len(received))
                    assert part, "AUTH socket closed before sending the command"
                    received += part
                assert received == command
                server.sendall(
                    moving
                    + (
                        b"-WRONGPASS invalid token\r\n"
                        if outcome == "auth_error"
                        else b"+OK\r\n"
                    )
                )
            except BaseException as exc:
                errors.append(exc)

        real_read = owner.read_response

        def read(*args, **kwargs):
            result = real_read(*args, **kwargs)
            if outcome == "cancel":
                raise Cancelled
            return result

        worker = threading.Thread(target=authenticate, daemon=True)
        try:
            with (
                patch.object(handler, "_schedule"),
                patch.object(owner._conn, "getpeername", return_value="127.0.0.1"),
                patch.object(owner, "read_response", side_effect=read),
            ):
                worker.start()
                if outcome == "success":
                    auth_pool.re_auth_callback(token)
                else:
                    with pytest.raises(
                        AuthenticationError if outcome == "auth_error" else Cancelled
                    ):
                        auth_pool.re_auth_callback(token)
                worker.join(3)
                assert not worker.is_alive() and not errors
                assert not owner._idle_maintenance and not owner._in_use
                assert not owner.is_connected, (
                    "idle AUTH returned the pre-MOVING socket"
                )
                assert events == [(False, False)]
            with Redis(connection_pool=auth_pool) as client:
                assert client.get(key) == b"value"
                assert client.ping()
        finally:
            server.close()
            worker.join(5)
            handler.cancel_scheduled_tasks()
            auth_pool.disconnect()

    @pytest.mark.parametrize(
        "outcome", ["close", "auth_error", "listener_error", "cancel"]
    )
    def test_reauthentication_push_listeners_release_idle_owner(
        self, pool, r2, outcome
    ):
        # Token refresh uses ConnectionPool; exercise shared-cache borrowers
        # from both pool implementations while its completion listener closes it.
        auth_pool = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
        auth_pool.update_maint_notifications_config(
            MaintNotificationsConfig(enabled="auto")
        )
        owner = auth_pool.get_connection()
        borrower = pool.get_connection()
        key = str(uuid.uuid4())
        r2.set(key, "value")
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        auth_pool.release(owner)
        events, errors = [], []

        class Cancelled(BaseException):
            pass

        class Listener(EventListenerInterface):
            def listen(self, event):
                try:
                    assert not owner._idle_maintenance, "listener holds idle ownership"
                    unlocked = []

                    def probe():
                        acquired = auth_pool._lock.acquire(blocking=False)
                        unlocked.append(acquired)
                        if acquired:
                            auth_pool._lock.release()

                    worker = threading.Thread(target=probe, daemon=True)
                    worker.start()
                    worker.join(2)
                    assert unlocked == [True], "listener holds the pool lock"
                    events.append(event)
                    if isinstance(event, MaintenanceCompletedEvent):
                        auth_pool.close()
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
        owner._conn.disconnect()
        client, server = socket.socketpair()
        server.settimeout(3)
        owner._conn._sock = client
        owner._conn._parser.on_connect(owner._conn)
        handler = MaintNotificationsConnectionHandler(owner, config)
        owner._conn._parser.set_maintenance_push_handler(handler.handle_notification)
        owner.maint_notifications_config = config
        server.sendall(b">3\r\n+MIGRATING\r\n:1\r\n:10\r\n>2\r\n+MIGRATED\r\n:1\r\n")
        token = Mock()
        token.try_get.return_value = "default"
        token.get_value.return_value = "test-token"
        command = b"".join(owner._conn.pack_command("AUTH", "default", "test-token"))

        def authenticate():
            try:
                received = b""
                while len(received) < len(command):
                    received += server.recv(len(command) - len(received))
                assert received == command
                if outcome != "cancel":
                    server.sendall(
                        b"-WRONGPASS invalid token\r\n"
                        if outcome == "auth_error"
                        else b"+OK\r\n"
                    )
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=authenticate, daemon=True)
        try:
            worker.start()
            if outcome == "cancel":
                with patch.object(owner, "read_response", side_effect=Cancelled):
                    with pytest.raises(Cancelled):
                        auth_pool.re_auth_callback(token)
            elif outcome == "auth_error":
                with pytest.raises(AuthenticationError, match="invalid token"):
                    auth_pool.re_auth_callback(token)
            else:
                auth_pool.re_auth_callback(token)
            worker.join(3)
            assert not worker.is_alive()
            assert not errors
            assert [type(event) for event in events] == [
                MaintenanceStartedEvent,
                MaintenanceCompletedEvent,
            ]
            assert not owner._idle_maintenance and not owner.is_connected
            borrower.send_command("GET", key, keys=[key])
            assert borrower.read_response() == b"value"
            borrower.send_command("PING")
            assert borrower.read_response() == b"PONG"
        finally:
            server.close()
            client.close()
            worker.join(5)
            pool.release(borrower)
            auth_pool.disconnect()

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

    @pytest.mark.parametrize("reuse", ["checkout", "idle_auth"])
    @pytest.mark.parametrize("maintenance", [False, "auto"])
    def test_reused_owner_does_not_keep_previous_busy_cache_grace(
        self, pool, r2, reuse, maintenance
    ):
        owner_pool = (
            pool
            if reuse == "checkout" and maintenance is False
            else (type(pool) if reuse == "checkout" else ConnectionPool)(
                cache=pool.cache,
                **{
                    **pool.connection_kwargs,
                    "maint_notifications_config": MaintNotificationsConfig(
                        enabled=maintenance
                    ),
                },
            )
        )
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner, borrower = owner_pool.get_connection(), pool.get_connection()
        checked_out = owner
        try:
            # Keep both checkouts inside the previous command's grace window.
            # Socket I/O and invalidation delivery still use the real server.
            with patch("redis.connection.time.monotonic", return_value=1000):
                owner.send_command("GET", key, keys=[key])
                assert owner.read_response() == b"old"
                owner_pool.release(owner)
                checked_out = None
                r2.set(key, "new")
                assert owner.can_read(timeout=1)

                def read_fresh():
                    with patch.object(
                        owner._conn, "read_response", wraps=owner._conn.read_response
                    ) as read:
                        borrower.send_command("GET", key, keys=[key])
                        assert borrower.read_response() == b"new"
                        read.assert_not_called()

                if reuse == "checkout":
                    checked_out = owner_pool.get_connection()
                    assert checked_out is owner
                    read_fresh()
                else:
                    real_read = owner._conn.read_response
                    auth_replies = []

                    def auth_read(*args, **kwargs):
                        read_fresh()
                        reply = real_read(*args, **kwargs)
                        if not kwargs.get("push_request", False):
                            auth_replies.append(reply)
                        return reply

                    token = Mock()
                    token.try_get.return_value = "default"
                    token.get_value.return_value = ""
                    with patch.object(
                        owner._conn, "read_response", side_effect=auth_read
                    ):
                        owner_pool.re_auth_callback(token)
                    assert auth_replies == [b"OK"]
                    checked_out = owner_pool.get_connection()
                    assert checked_out is owner
                owner.send_command("PING")
                assert owner.read_response() == b"PONG"
        finally:
            if checked_out is not None:
                owner_pool.release(checked_out)
            pool.release(borrower)
            if owner_pool is not pool:
                owner_pool.disconnect()

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
            def acquire(self, *args, **kwargs):
                if threading.current_thread() is checkout_thread:
                    checkout_attempted.set()
                return real_owner_lock.acquire(*args, **kwargs)

            def release(self):
                return real_owner_lock.release()

            def __enter__(self):
                return self.acquire()

            def __exit__(self, *args):
                return self.release()

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
                if operation == "moving":
                    assert not owner.is_connected and not owner.should_reconnect()
            # Deferred AUTH still runs on release; MOVING already closed the
            # socket at the end of its drain, so checkout opens a new session.
            reused = owner_pool.get_connection()
            assert reused is owner
            reused.send_command("PING")
            assert reused.read_response() == b"PONG"
            owner_pool.release(reused)
            assert owner._conn._re_auth_token is None
            assert not owner.should_reconnect()
            if operation == "moving":
                assert owner.is_connected and owner._conn._sock is not sock
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
        real_drain = borrower._drain_invalidations
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

        def drain(conn):
            try:
                return real_drain(conn)
            finally:
                if conn is owner._conn:
                    finished.set()

        def read_cached():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)

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
                patch.object(borrower, "_drain_invalidations", side_effect=drain),
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
                assert not owner.is_connected and not owner.should_reconnect()
                assert notification in handler._processed_notifications
            reused = pool.get_connection()
            assert reused is owner
            reused.send_command("PING")
            assert reused.read_response() == b"PONG"
            pool.release(reused)
            assert not owner.should_reconnect()
            assert owner.is_connected
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
