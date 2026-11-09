import gc
import multiprocessing
import threading
import uuid
import weakref
from unittest.mock import Mock, patch

import pytest

from redis import Redis
from redis._parsers import _HiredisParser, _RESP3Parser
from redis.cache import CacheConfig
from redis.connection import BlockingConnectionPool, ConnectionPool
from redis.event import AfterConnectionReleasedEvent
from redis.maint_notifications import MaintNotificationsConfig
from redis.sentinel import SentinelConnectionPool
from redis.utils import HIREDIS_AVAILABLE

from .conftest import skip_if_server_version_lt

pytestmark = [
    pytest.mark.fixed_client,
    pytest.mark.onlynoncluster,
    skip_if_server_version_lt("7.4.0"),
]

PARSERS = [
    _RESP3Parser,
    pytest.param(
        _HiredisParser,
        marks=pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="requires hiredis"),
    ),
]


def _run_bounded(target, args):
    worker = multiprocessing.get_context("spawn").Process(target=target, args=args)
    worker.start()
    try:
        worker.join(10)
        assert not worker.is_alive(), "cached pool shutdown deadlocked"
        assert worker.exitcode == 0
    finally:
        if worker.is_alive():
            worker.terminate()
        worker.join(5)
        worker.close()


def _finalizer_during_shutdown(url, pool_class, parser_class, automatic, operation):
    pool = pool_class.from_url(
        url,
        protocol=3,
        parser_class=parser_class,
        cache_config=CacheConfig(),
        socket_timeout=1,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    owner = pool.get_connection()
    if operation != "release":
        pool.release(owner)
    enabled, thresholds = gc.isenabled(), gc.get_threshold()
    gc.disable()
    victim = Redis.from_pool(pool)
    victim.reference_cycle = victim
    abandoned = weakref.ref(victim)
    del victim
    original = owner._conn.disconnect
    collected = False

    def disconnect(*args, **kwargs):
        nonlocal collected
        if not collected:
            collected = True
            if automatic:
                gc.set_threshold(1, 1, 1)
                gc.enable()
                allocations = [[] for _ in range(100)]
                assert allocations
            else:
                gc.collect()
            assert abandoned() is None, "pool-owning client was not finalized"
        return original(*args, **kwargs)

    try:
        with patch.object(owner._conn, "disconnect", disconnect):
            if operation == "release":
                owner.mark_for_reconnect()
                pool.release(owner)
            elif operation == "free_disconnect":
                pool.disconnect_free_connections()
            else:
                pool.close()
        assert collected
        assert not owner.is_connected
        assert not owner._in_use_lock.locked()
        assert not owner._idle_maintenance
        reused = pool.get_connection()
        reused.send_command("PING")
        assert reused.read_response() == b"PONG"
        pool.release(reused)
    finally:
        gc.set_threshold(*thresholds)
        if enabled:
            gc.enable()
        else:
            gc.disable()
        pool.close()


@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("automatic", [False, True])
@pytest.mark.parametrize("operation", ["close", "release", "free_disconnect"])
def test_finalizer_during_pool_shutdown(
    request, pool_class, parser_class, automatic, operation
):
    _run_bounded(
        _finalizer_during_shutdown,
        (
            request.config.getoption("--redis-url"),
            pool_class,
            parser_class,
            automatic,
            operation,
        ),
    )


def _finalizer_during_release_listener_reconnect(url, parser_class, automatic):
    pool = ConnectionPool.from_url(
        url,
        protocol=3,
        parser_class=parser_class,
        cache_config=CacheConfig(),
        max_connections=2,
        socket_timeout=1,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    owner, released = (pool.get_connection() for _ in range(2))
    # The peer shares the cache, but closing the owner's pool must not close
    # the peer's active socket while it waits for the owner's drain to finish.
    peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
    borrower = peers.get_connection()
    key = str(uuid.uuid4())
    writer = Redis.from_url(url)
    writer.set(key, "old")
    owner.send_command("GET", key, keys=[key])
    assert owner.read_response() == b"old"
    pool.release(owner)
    draining, finalizing = threading.Event(), threading.Event()
    errors = []
    original_poll = owner._conn.can_read
    original_disconnect = released._conn.disconnect
    collected = False
    enabled, thresholds = gc.isenabled(), gc.get_threshold()
    gc.disable()
    victim = Redis.from_pool(pool)
    victim.reference_cycle = victim
    abandoned = weakref.ref(victim)
    del victim

    def poll(*args, **kwargs):
        if threading.current_thread() is reader:
            draining.set()
            assert finalizing.wait(5), "release did not reach its final disconnect"
        return original_poll(*args, **kwargs)

    def read():
        try:
            borrower.send_command("GET", key, keys=[key])
            assert borrower.read_response() == b"old"
        except BaseException as error:
            errors.append(error)

    def mark_for_reconnect(event):
        if event.connection is released:
            assert not released.should_reconnect()
            released.mark_for_reconnect()

    def disconnect(*args, **kwargs):
        nonlocal collected
        if not collected:
            collected = True
            finalizing.set()
            if automatic:
                gc.set_threshold(1, 1, 1)
                gc.enable()
                allocations = [[] for _ in range(100)]
                assert allocations
            else:
                gc.collect()
            assert abandoned() is None, "pool-owning client was not finalized"
        return original_disconnect(*args, **kwargs)

    listener = Mock()
    listener.listen.side_effect = mark_for_reconnect
    reader = threading.Thread(target=read, daemon=True)
    pool._event_dispatcher.register_listeners({AfterConnectionReleasedEvent: [listener]})
    try:
        with (
            patch.object(owner._conn, "can_read", poll),
            patch.object(released._conn, "disconnect", disconnect),
        ):
            reader.start()
            assert draining.wait(3), "peer did not start draining the idle owner"
            pool.release(released)
            reader.join(3)
            assert not reader.is_alive(), "release and idle drain deadlocked"
            assert not errors, errors
        assert collected
        assert not released.is_connected
        assert not owner._in_use_lock.locked()
        assert not owner._idle_maintenance
        pool._event_dispatcher.unregister_listeners(
            {AfterConnectionReleasedEvent: [listener]}
        )
        reused = pool.get_connection()
        reused.send_command("PING")
        assert reused.read_response() == b"PONG"
        pool.release(reused)
    finally:
        gc.set_threshold(*thresholds)
        if enabled:
            gc.enable()
        else:
            gc.disable()
        peers.release(borrower)
        peers.close()
        pool.close()
        writer.delete(key)
        writer.close()


@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("automatic", [False, True])
def test_finalizer_during_release_listener_reconnect(
    request, parser_class, automatic
):
    _run_bounded(
        _finalizer_during_release_listener_reconnect,
        (request.config.getoption("--redis-url"), parser_class, automatic),
    )


def _sentinel_auth_during_idle_drain(url, parser_class):
    writer = Redis.from_url(url)
    address = writer.connection_pool.connection_kwargs
    host, port = address.get("host", "localhost"), address.get("port", 6379)
    manager = Mock()
    manager.discover_master.return_value = host, port
    pool = SentinelConnectionPool(
        "cache-reentry",
        manager,
        protocol=3,
        db=address.get("db", 0),
        parser_class=parser_class,
        cache_config=CacheConfig(),
        max_connections=3,
        socket_timeout=1,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    owner, borrower, reconnect = (pool.get_connection() for _ in range(3))
    reconnect.disconnect()
    key = str(uuid.uuid4())
    writer.set(key, "old")
    owner.send_command("GET", key, keys=[key])
    assert owner.read_response() == b"old"
    pool.release(owner)
    pool.release(reconnect)
    # A fresh discovery of the same reachable server retires free sockets.
    pool.proxy.master_address = ("previous-master", port)
    draining, discovering = threading.Event(), threading.Event()
    errors = []
    original_poll = owner._conn.can_read
    original_discover = pool.proxy.get_master_address

    def poll(*args, **kwargs):
        if threading.current_thread() is reader:
            draining.set()
            assert discovering.wait(5), "AUTH did not start Sentinel discovery"
        return original_poll(*args, **kwargs)

    def discover():
        discovering.set()
        return original_discover()

    def read():
        borrower.send_command("GET", key, keys=[key])
        assert borrower.read_response() == b"old"

    token = Mock()
    token.try_get.return_value = "default"
    token.get_value.return_value = ""

    def run(callback):
        try:
            callback()
        except BaseException as error:
            errors.append(error)

    reader = threading.Thread(target=run, args=(read,), daemon=True)
    auth = threading.Thread(
        target=run, args=(lambda: pool.re_auth_callback(token),), daemon=True
    )
    try:
        with (
            patch.object(owner._conn, "can_read", poll),
            patch.object(pool.proxy, "get_master_address", discover),
        ):
            reader.start()
            assert draining.wait(3)
            auth.start()
            for worker in (reader, auth):
                worker.join(3)
            assert not reader.is_alive() and not auth.is_alive(), (
                "Sentinel AUTH and idle-drain cleanup waited on each other's locks"
            )
            assert not errors
        assert not owner.is_connected
        writer.set(key, "new")
        borrower.send_command("GET", key, keys=[key])
        assert borrower.read_response() == b"new"
        borrower.send_command("PING")
        assert borrower.read_response() == b"PONG"
    finally:
        pool.release(borrower)
        pool.close()
        writer.delete(key)
        writer.close()


@pytest.mark.parametrize("parser_class", PARSERS)
def test_sentinel_auth_during_idle_drain(request, parser_class):
    _run_bounded(
        _sentinel_auth_during_idle_drain,
        (request.config.getoption("--redis-url"), parser_class),
    )
