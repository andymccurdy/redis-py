import os
import socket
import threading
import uuid
from unittest import mock

import pytest
from redis.client import StrictRedis

import redis.sentinel
from redis import Redis
from redis._parsers import _HiredisParser, _RESP3Parser
from redis.cache import CacheConfig, TrackingMode
from redis.event import (
    AfterConnectionReleasedEvent,
    EventDispatcher,
    EventListenerInterface,
    MaintenanceCompletedEvent,
)
from redis.maint_notifications import MaintNotificationsConfig, MaintenanceState
from redis import exceptions
from redis.observability.attributes import ConnectionState, get_pool_name
from redis.sentinel import (
    MasterNotFoundError,
    ReplicaNotFoundError,
    Sentinel,
    SentinelConnectionPool,
    SlaveNotFoundError,
)
from redis.utils import HIREDIS_AVAILABLE

from .conftest import skip_if_server_version_lt


@pytest.fixture(scope="module")
def master_ip(master_host):
    yield socket.gethostbyname(master_host[0])


class SentinelTestClient:
    def __init__(self, cluster, id):
        self.cluster = cluster
        self.id = id

    def sentinel_masters(self):
        self.cluster.connection_error_if_down(self)
        self.cluster.timeout_if_down(self)
        self.cluster.response_error_if_down(self)
        return {self.cluster.service_name: self.cluster.master}

    def sentinel_slaves(self, master_name):
        self.cluster.connection_error_if_down(self)
        self.cluster.timeout_if_down(self)
        if master_name != self.cluster.service_name:
            return []
        return self.cluster.slaves

    def execute_command(self, *args, **kwargs):
        # wrapper  purely to validate the calls don't explode
        from redis.client import bool_ok

        return bool_ok


class SentinelTestCluster:
    def __init__(self, servisentinel_ce_name="mymaster", ip="127.0.0.1", port=6379):
        self.clients = {}
        self.master = {
            "ip": ip,
            "port": port,
            "is_master": True,
            "is_sdown": False,
            "is_odown": False,
            "num-other-sentinels": 0,
        }
        self.service_name = servisentinel_ce_name
        self.slaves = []
        self.nodes_down = set()
        self.nodes_timeout = set()
        self.nodes_response_error = set()

    def connection_error_if_down(self, node):
        if node.id in self.nodes_down:
            raise exceptions.ConnectionError

    def timeout_if_down(self, node):
        if node.id in self.nodes_timeout:
            raise exceptions.TimeoutError

    def response_error_if_down(self, node):
        if node.id in self.nodes_response_error:
            raise exceptions.ResponseError(
                "unknown command 'SENTINEL', with args beginning with: 'masters'"
            )

    def client(self, host, port, **kwargs):
        return SentinelTestClient(self, (host, port))


@pytest.fixture()
def cluster(request, master_ip):
    def teardown():
        redis.sentinel.Redis = saved_Redis

    cluster = SentinelTestCluster(ip=master_ip)
    saved_Redis = redis.sentinel.Redis
    redis.sentinel.Redis = cluster.client
    request.addfinalizer(teardown)
    return cluster


@pytest.fixture()
def sentinel(request, cluster):
    return Sentinel([("foo", 26379), ("bar", 26379)])


@pytest.fixture()
def deployed_sentinel(request):
    sentinel_ips = request.config.getoption("--sentinels")
    sentinel_endpoints = [
        (ip.strip(), int(port.strip()))
        for ip, port in (endpoint.split(":") for endpoint in sentinel_ips.split(","))
    ]
    kwargs = {}
    decode_responses = True

    sentinel_kwargs = {"decode_responses": decode_responses}
    force_master_ip = "localhost"

    protocol = request.config.getoption("--protocol")
    if protocol:
        kwargs["protocol"] = protocol

    sentinel = Sentinel(
        sentinel_endpoints,
        force_master_ip=force_master_ip,
        sentinel_kwargs=sentinel_kwargs,
        socket_timeout=0.1,
        decode_responses=decode_responses,
        **kwargs,
    )
    yield sentinel
    for s in sentinel.sentinels:
        s.close()


@pytest.mark.onlynoncluster
def test_discover_master(sentinel, master_ip):
    address = sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)


@pytest.mark.onlynoncluster
def test_discover_master_error(sentinel):
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master("xxx")


@pytest.mark.onlynoncluster
def test_dead_pool(sentinel):
    master = sentinel.master_for("mymaster", db=9)
    conn = master.connection_pool.get_connection()
    conn.disconnect()
    del master
    conn.connect()


@pytest.mark.onlynoncluster
def test_discover_master_sentinel_down(cluster, sentinel, master_ip):
    # Put first sentinel 'foo' down
    cluster.nodes_down.add(("foo", 26379))
    address = sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


@pytest.mark.onlynoncluster
def test_discover_master_sentinel_timeout(cluster, sentinel, master_ip):
    # Put first sentinel 'foo' down
    cluster.nodes_timeout.add(("foo", 26379))
    address = sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


@pytest.mark.onlynoncluster
def test_discover_master_sentinel_response_error(cluster, sentinel, master_ip):
    # 'foo' answers SENTINEL MASTERS with an error reply (a node that is not a
    # sentinel, or an ACL that denies the command): try 'bar' instead
    cluster.nodes_response_error.add(("foo", 26379))
    address = sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


@pytest.mark.onlynoncluster
def test_discover_master_error_names_response_error(cluster, sentinel):
    cluster.nodes_response_error.update({("foo", 26379), ("bar", 26379)})
    with pytest.raises(
        MasterNotFoundError, match="ResponseError: unknown command 'SENTINEL'"
    ):
        sentinel.discover_master("mymaster")


@pytest.mark.onlynoncluster
def test_master_min_other_sentinels(cluster, master_ip):
    sentinel = Sentinel([("foo", 26379)], min_other_sentinels=1)
    # min_other_sentinels
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master("mymaster")
    cluster.master["num-other-sentinels"] = 2
    address = sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)


@pytest.mark.onlynoncluster
def test_master_odown(cluster, sentinel):
    cluster.master["is_odown"] = True
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master("mymaster")


@pytest.mark.onlynoncluster
def test_master_sdown(cluster, sentinel):
    cluster.master["is_sdown"] = True
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master("mymaster")


@pytest.mark.onlynoncluster
def test_discover_slaves(cluster, sentinel):
    assert sentinel.discover_slaves("mymaster") == []

    cluster.slaves = [
        {"ip": "slave0", "port": 1234, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 1234, "is_odown": False, "is_sdown": False},
    ]
    assert sentinel.discover_slaves("mymaster") == [("slave0", 1234), ("slave1", 1234)]

    # slave0 -> ODOWN
    cluster.slaves[0]["is_odown"] = True
    assert sentinel.discover_slaves("mymaster") == [("slave1", 1234)]

    # slave1 -> SDOWN
    cluster.slaves[1]["is_sdown"] = True
    assert sentinel.discover_slaves("mymaster") == []

    cluster.slaves[0]["is_odown"] = False
    cluster.slaves[1]["is_sdown"] = False

    # node0 -> DOWN
    cluster.nodes_down.add(("foo", 26379))
    assert sentinel.discover_slaves("mymaster") == [("slave0", 1234), ("slave1", 1234)]
    cluster.nodes_down.clear()

    # node0 -> TIMEOUT
    cluster.nodes_timeout.add(("foo", 26379))
    assert sentinel.discover_slaves("mymaster") == [("slave0", 1234), ("slave1", 1234)]


@pytest.mark.onlynoncluster
def test_discover_replicas(cluster, sentinel):
    assert sentinel.discover_replicas("mymaster") == []

    cluster.slaves = [
        {"ip": "slave0", "port": 1234, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 1234, "is_odown": True, "is_sdown": False},
    ]

    assert sentinel.filter_replicas(cluster.slaves) == [("slave0", 1234)]
    assert sentinel.discover_replicas("mymaster") == [("slave0", 1234)]


@pytest.mark.onlynoncluster
def test_master_for(sentinel, master_ip):
    master = sentinel.master_for("mymaster", db=9)
    assert master.ping()
    assert master.connection_pool.master_address == (master_ip, 6379)

    # Use internal connection check
    master = sentinel.master_for("mymaster", db=9, check_connection=True)
    assert master.ping()


@pytest.mark.onlynoncluster
def test_slave_for(cluster, sentinel):
    cluster.slaves = [
        {"ip": "127.0.0.1", "port": 6379, "is_odown": False, "is_sdown": False}
    ]
    slave = sentinel.slave_for("mymaster", db=9)
    assert slave.ping()


@pytest.mark.onlynoncluster
def test_replica_for(cluster, sentinel):
    cluster.slaves = [
        {"ip": "127.0.0.1", "port": 6379, "is_odown": False, "is_sdown": False}
    ]
    replica = sentinel.replica_for("mymaster", db=9)
    assert replica.ping()


@pytest.mark.onlynoncluster
def test_slave_for_slave_not_found_error(cluster, sentinel):
    cluster.master["is_odown"] = True
    slave = sentinel.slave_for("mymaster", db=9)
    with pytest.raises(SlaveNotFoundError):
        slave.ping()


@pytest.mark.onlynoncluster
def test_replica_not_found_alias(cluster, sentinel):
    assert ReplicaNotFoundError is SlaveNotFoundError

    cluster.master["is_odown"] = True
    replica = sentinel.replica_for("mymaster", db=9)
    with pytest.raises(ReplicaNotFoundError):
        replica.ping()


@pytest.mark.onlynoncluster
def test_slave_round_robin(cluster, sentinel, master_ip):
    cluster.slaves = [
        {"ip": "slave0", "port": 6379, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 6379, "is_odown": False, "is_sdown": False},
    ]
    pool = SentinelConnectionPool("mymaster", sentinel)
    rotator = pool.rotate_slaves()
    assert next(rotator) in (("slave0", 6379), ("slave1", 6379))
    assert next(rotator) in (("slave0", 6379), ("slave1", 6379))
    # Fallback to master
    assert next(rotator) == (master_ip, 6379)
    with pytest.raises(SlaveNotFoundError):
        next(rotator)


@pytest.mark.fixed_client
def test_master_failover_reclaims_discarded_connection_slot():
    master_a = ("master-a", 6379)
    master_b = ("master-b", 6379)

    class FakeConnection:
        def __init__(self, **kwargs):
            self.host, self.port = master_a
            self.pid = os.getpid()

        def connect(self):
            pass

        def disconnect(self):
            pass

        def can_read(self, timeout=0):
            return False

        def should_reconnect(self):
            return False

    pool = SentinelConnectionPool(
        "mymaster",
        mock.MagicMock(),
        connection_class=FakeConnection,
        max_connections=2,
    )
    pool.proxy.master_address = master_a

    for _ in range(pool.max_connections):
        connection = pool.get_connection()
        pool.proxy.master_address = master_b
        pool.release(connection)
        pool.proxy.master_address = master_a

    assert pool._created_connections == 0
    pool.get_connection()


@pytest.mark.fixed_client
def test_master_failover_release_decrements_used_count():
    master_a = ("master-a", 6379)
    master_b = ("master-b", 6379)

    class FakeConnection:
        def __init__(self, **kwargs):
            self.host, self.port = master_a
            self.pid = os.getpid()

        def connect(self):
            pass

        def disconnect(self):
            pass

        def can_read(self, timeout=0):
            return False

        def should_reconnect(self):
            return False

    pool = SentinelConnectionPool(
        "mymaster",
        mock.MagicMock(),
        connection_class=FakeConnection,
        max_connections=2,
    )
    pool.proxy.master_address = master_a
    pool_name = get_pool_name(pool)

    with mock.patch("redis.connection.record_connection_count") as mock_rec:
        connection = pool.get_connection()
        pool.proxy.master_address = master_b
        pool.release(connection)

    net = {ConnectionState.IDLE: 0, ConnectionState.USED: 0}
    for c in mock_rec.call_args_list:
        if c.kwargs["pool_name"] == pool_name:
            net[c.kwargs["connection_state"]] += c.kwargs["counter"]
    assert net[ConnectionState.USED] == 0
    assert net[ConnectionState.IDLE] == 0


@pytest.mark.fixed_client
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("7.4.0")
@pytest.mark.parametrize("phase", ["release", "auth"])
@pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
@pytest.mark.parametrize("recursive_release", [False, True])
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
def test_cached_master_change_during_release(
    r2, stack_url, phase, outcome, recursive_release, tracking_mode, parser_class
):
    # Two independent Redis endpoints give the same key different values. Only
    # Sentinel's discovery result is controlled; connection and AUTH I/O are real.
    new_master = Redis.from_url(
        stack_url, db=r2.connection_pool.connection_kwargs.get("db", 0)
    )
    addresses = [
        (
            client.connection_pool.connection_kwargs["host"],
            client.connection_pool.connection_kwargs.get("port", 6379),
        )
        for client in [r2, new_master]
    ]
    assert addresses[0] != addresses[1]
    manager = mock.Mock()
    manager.discover_master.return_value = addresses[0]
    kwargs = dict(r2.connection_pool.connection_kwargs)
    kwargs.update(
        protocol=3,
        parser_class=parser_class,
        decode_responses=False,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    pool = SentinelConnectionPool(
        "mymaster",
        manager,
        max_connections=1,
        cache_config=CacheConfig(
            tracking_mode=tracking_mode,
            cache_predicate=None
            if tracking_mode is TrackingMode.PLAIN
            else lambda *_: True,
        ),
        **kwargs,
    )
    key = str(uuid.uuid4())
    r2.set(key, "old-master")
    new_master.set(key, "new-master")
    owner = pool.get_connection()
    owner.send_command("GET", key, keys=[key])
    assert owner.read_response() == b"old-master"
    changed = []

    class Cancelled(BaseException):
        pass

    def change_master():
        if changed:
            return
        changed.append(True)
        manager.discover_master.return_value = addresses[1]
        assert pool.get_master_address() == addresses[1]
        assert not pool.owns_connection(owner)
        if recursive_release:
            pool.release(owner)
        if outcome == "error":
            raise RuntimeError("release listener failed")
        if outcome == "cancel":
            raise Cancelled

    class Listener(EventListenerInterface):
        def listen(self, event):
            change_master()

    real_read = owner._conn.read_response

    def auth_read(*args, **kwargs):
        reply = real_read(*args, **kwargs)
        change_master()
        return reply

    if phase == "release":
        pool._event_dispatcher.register_listeners(
            {AfterConnectionReleasedEvent: [Listener()]}
        )
    else:
        token = mock.Mock()
        token.try_get.return_value = "default"
        token.get_value.return_value = ""
        owner.set_re_auth_token(token)
    try:
        with (
            mock.patch.object(
                owner._conn,
                "read_response",
                side_effect=auth_read if phase == "auth" else real_read,
            ),
            mock.patch("redis.connection.record_connection_count") as metric,
        ):
            if outcome == "success":
                pool.release(owner)
            else:
                with pytest.raises(RuntimeError if outcome == "error" else Cancelled):
                    pool.release(owner)
            assert changed == [True]
            release_metrics = [
                call.kwargs
                for call in metric.call_args_list
                if call.kwargs["pool_name"] == get_pool_name(pool)
            ]
            # Check the user-visible read before the bookkeeping assertions: the
            # old implementation reuses the old master's socket and cached value.
            with Redis(connection_pool=pool) as client:
                assert client.get(key) == b"new-master"
                assert client.ping()
            assert not owner.is_connected
            assert owner not in pool._available_connections
            assert owner not in pool._in_use_connections
            assert not owner._in_use
            assert not pool._releasing_connections
            assert pool._created_connections == 1
            assert len(pool._available_connections) == 1
            replacement = pool._available_connections[0]
            assert replacement.is_connected
            assert (replacement.host, replacement.port) == addresses[1]
            assert release_metrics == [
                dict(
                    pool_name=get_pool_name(pool),
                    connection_state=ConnectionState.USED,
                    counter=-1,
                )
            ]
    finally:
        pool.disconnect()
        new_master.delete(key)
        new_master.close()


@pytest.mark.fixed_client
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("7.4.0")
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
def test_rejected_cached_master_completes_maintenance_outside_pool_lock(
    r2, tracking_mode, parser_class
):
    kwargs = dict(r2.connection_pool.connection_kwargs)
    kwargs.update(protocol=3, parser_class=parser_class, decode_responses=False)
    address = (kwargs["host"], kwargs.get("port", 6379))
    manager = mock.Mock()
    manager.discover_master.return_value = address
    calls = []

    class Listener(EventListenerInterface):
        def listen(self, event):
            unlocked = []

            def probe():
                acquired = pool._lock.acquire(blocking=False)
                unlocked.append(acquired)
                if acquired:
                    pool._lock.release()

            worker = threading.Thread(target=probe, daemon=True)
            worker.start()
            worker.join(2)
            # A concurrent idle MOVING handler needs this lock before close()
            # can join it. Detect the inversion without stranding a worker.
            assert unlocked == [True], "rejected release listener holds pool lock"
            assert not event.connection.is_connected
            assert not event.connection._in_use
            calls.append(event)
            pool.close()

    kwargs["maint_notifications_config"] = MaintNotificationsConfig(
        enabled="auto",
        event_dispatcher=EventDispatcher({MaintenanceCompletedEvent: [Listener()]}),
    )
    pool = SentinelConnectionPool(
        "mymaster",
        manager,
        cache_config=CacheConfig(
            tracking_mode=tracking_mode,
            cache_predicate=None
            if tracking_mode is TrackingMode.PLAIN
            else lambda *_: True,
        ),
        max_connections=1,
        **kwargs,
    )
    owner = pool.get_connection()
    try:
        owner.maintenance_state = MaintenanceState.MAINTENANCE
        manager.discover_master.return_value = ("new-master", address[1])
        assert pool.get_master_address() == ("new-master", address[1])
        assert not pool.owns_connection(owner)
        pool.release(owner)
        assert len(calls) == 1
        assert calls[0].connection is owner
        assert not owner.is_connected
        assert not owner._in_use
        assert not pool._in_use_connections
        assert not pool._available_connections
        assert pool._created_connections == 0
        # Discovery can reconnect after discarding the demoted master's slot.
        manager.discover_master.return_value = address
        with Redis(connection_pool=pool) as client:
            assert client.ping()
    finally:
        pool.disconnect()


@pytest.mark.fixed_client
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("7.4.0")
@pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
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
def test_cached_sentinel_idle_auth_reconnect_after_failover(
    r2, stack_url, outcome, tracking_mode, parser_class
):
    new_master = Redis.from_url(
        stack_url, db=r2.connection_pool.connection_kwargs.get("db", 0)
    )
    addresses = [
        (
            client.connection_pool.connection_kwargs["host"],
            client.connection_pool.connection_kwargs.get("port", 6379),
        )
        for client in [r2, new_master]
    ]
    assert addresses[0] != addresses[1]
    manager = mock.Mock()
    manager.discover_master.return_value = addresses[0]
    kwargs = dict(r2.connection_pool.connection_kwargs)
    kwargs.update(
        protocol=3,
        parser_class=parser_class,
        decode_responses=False,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    pool = SentinelConnectionPool(
        "mymaster",
        manager,
        max_connections=1,
        cache_config=CacheConfig(
            tracking_mode=tracking_mode,
            cache_predicate=None
            if tracking_mode is TrackingMode.PLAIN
            else lambda *_: True,
        ),
        **kwargs,
    )
    key = str(uuid.uuid4())
    r2.set(key, "old-master")
    new_master.set(key, "new-master")
    owner = pool.get_connection()
    owner.send_command("GET", key, keys=[key])
    assert owner.read_response() == b"old-master"
    pool.release(owner)
    owner.disconnect()
    manager.discover_master.return_value = addresses[1]
    token = mock.Mock()
    token.try_get.return_value = (
        "missing-" + str(uuid.uuid4()) if outcome == "error" else "default"
    )
    token.get_value.return_value = ""
    real_send, real_read = owner._conn.send_command, owner._conn.read_response
    auth_sent = False

    class Cancelled(BaseException):
        pass

    class RecursiveIdleWait(BaseException):
        pass

    def send(*args, **kwargs):
        nonlocal auth_sent
        result = real_send(*args, **kwargs)
        if args and args[0] == "AUTH":
            auth_sent = True
        return result

    def read(*args, **kwargs):
        response = real_read(*args, **kwargs)
        if auth_sent and outcome == "cancel":
            raise Cancelled
        return response

    try:
        with (
            mock.patch.object(
                owner._idle_maintenance_done,
                "wait",
                side_effect=RecursiveIdleWait("free disconnect waited on its own AUTH"),
            ),
            mock.patch.object(owner._conn, "send_command", side_effect=send),
            mock.patch.object(owner._conn, "read_response", side_effect=read),
        ):
            if outcome == "success":
                pool.re_auth_callback(token)
            else:
                with pytest.raises(
                    exceptions.AuthenticationError if outcome == "error" else Cancelled
                ):
                    pool.re_auth_callback(token)
        assert auth_sent
        assert not owner._idle_maintenance and not owner._in_use
        assert (owner.host, owner.port) == addresses[1]
        assert pool.owns_connection(owner)
        assert pool._created_connections == 1
        assert pool._available_connections == [owner]
        with Redis(connection_pool=pool) as client:
            assert client.get(key) == b"new-master"
            assert client.ping()
    finally:
        pool.disconnect()
        new_master.delete(key)
        new_master.close()


@pytest.mark.onlynoncluster
def test_replica_round_robin(cluster, sentinel, master_ip):
    cluster.slaves = [
        {"ip": "slave0", "port": 6379, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 6379, "is_odown": False, "is_sdown": False},
    ]
    pool = SentinelConnectionPool("mymaster", sentinel)
    rotator = pool.rotate_replicas()
    assert next(rotator) in (("slave0", 6379), ("slave1", 6379))
    assert next(rotator) in (("slave0", 6379), ("slave1", 6379))
    # Fallback to master
    assert next(rotator) == (master_ip, 6379)
    with pytest.raises(ReplicaNotFoundError):
        next(rotator)


@pytest.mark.onlynoncluster
def test_ckquorum(sentinel):
    resp = sentinel.sentinel_ckquorum("mymaster")
    assert resp is True


@pytest.mark.onlynoncluster
def test_flushconfig(sentinel):
    resp = sentinel.sentinel_flushconfig()
    assert resp is True


@pytest.mark.onlynoncluster
def test_reset(cluster, sentinel):
    cluster.master["is_odown"] = True
    resp = sentinel.sentinel_reset("mymaster")
    assert resp is True


@pytest.mark.onlynoncluster
@pytest.mark.parametrize("method_name", ["master_for", "slave_for", "replica_for"])
def test_auto_close_pool(cluster, sentinel, method_name):
    """
    Check that the connection pool created by the sentinel client is
    automatically closed
    """

    method = getattr(sentinel, method_name)
    client = method("mymaster", db=9)
    pool = client.connection_pool
    assert client.auto_close_connection_pool is True
    calls = 0

    def mock_disconnect():
        nonlocal calls
        calls += 1

    with mock.patch.object(pool, "disconnect", mock_disconnect):
        client.close()

    assert calls == 1
    pool.disconnect()


@pytest.mark.onlynoncluster
def test_close(cluster, sentinel):
    sentinel.sentinels = [mock.Mock() for _ in range(2)]

    with sentinel as entered:
        assert entered is sentinel

    # exiting the context closes every sentinel client and its pool
    for s in sentinel.sentinels:
        s.close.assert_called_once()


@pytest.mark.onlynoncluster
def test_close_error_still_closes_remaining_clients(cluster, sentinel):
    sentinel.sentinels = [mock.Mock() for _ in range(3)]
    sentinel.sentinels[0].close.side_effect = Exception("sentinel down")

    with pytest.raises(Exception, match="sentinel down"):
        sentinel.close()

    # the failing client must not prevent closing the others
    for s in sentinel.sentinels:
        s.close.assert_called_once()


@pytest.mark.onlynoncluster
def test_close_idempotent(cluster, sentinel):
    sentinel.sentinels = [mock.Mock() for _ in range(2)]

    sentinel.close()
    sentinel.close()

    for s in sentinel.sentinels:
        assert s.close.call_count == 2


# Tests against real sentinel instances
@pytest.mark.onlynoncluster
def test_get_sentinels(deployed_sentinel):
    resps = deployed_sentinel.sentinel_sentinels("redis-py-test", return_responses=True)

    # validate that the original command response is returned
    assert isinstance(resps, list)

    # validate that the command has been executed against all sentinels
    # each response from each sentinel is returned
    assert len(resps) > 1

    # validate default behavior
    resps = deployed_sentinel.sentinel_sentinels("redis-py-test")
    assert isinstance(resps, bool)


@pytest.mark.onlynoncluster
def test_get_master_addr_by_name(deployed_sentinel):
    resps = deployed_sentinel.sentinel_get_master_addr_by_name(
        "redis-py-test", return_responses=True
    )

    # validate that the original command response is returned
    assert isinstance(resps, list)

    # validate that the command has been executed just once
    # when executed once, only one response element is returned
    assert len(resps) == 1

    assert isinstance(resps[0], tuple)

    # validate default behavior
    resps = deployed_sentinel.sentinel_get_master_addr_by_name("redis-py-test")
    assert isinstance(resps, bool)


@pytest.mark.onlynoncluster
def test_redis_master_usage(deployed_sentinel):
    r = deployed_sentinel.master_for("redis-py-test", db=0)
    r.set("foo", "bar")
    assert r.get("foo") == "bar"


@pytest.mark.onlynoncluster
def test_sentinel_commands_with_strict_redis_client(request):
    sentinel_ips = request.config.getoption("--sentinels")
    sentinel_host, sentinel_port = sentinel_ips.split(",")[0].split(":")
    protocol = request.config.getoption("--protocol")
    kwargs = {}
    if protocol:
        kwargs["protocol"] = protocol

    client = StrictRedis(
        host=sentinel_host, port=sentinel_port, decode_responses=True, **kwargs
    )
    # skipping commands that change the state of the sentinel setup
    assert isinstance(client.sentinel_get_master_addr_by_name("redis-py-test"), tuple)
    assert isinstance(client.sentinel_master("redis-py-test"), dict)
    assert isinstance(client.sentinel_masters(), dict)

    assert isinstance(client.sentinel_sentinels("redis-py-test"), list)
    assert isinstance(client.sentinel_slaves("redis-py-test"), list)

    assert isinstance(client.sentinel_ckquorum("redis-py-test"), bool)

    client.close()


# ---------------------------------------------------------------------------
# HIMPORT on Sentinel
# ---------------------------------------------------------------------------
# HIMPORT needs no Sentinel-specific code: the client returned by ``master_for``
# is a normal ``Redis`` whose ``SentinelConnectionPool`` inherits the shared
# ``HImportRegistry`` from ``ConnectionPool``. Fieldsets are declared at runtime
# with ``himport_prepare`` and survive failover because the registry lives on the
# long-lived pool while the per-connection PREPARE is reset on reconnect and
# re-applied lazily on the next ``himport_set``.


@pytest.mark.onlynoncluster
def test_himport_master_for_exposes_runtime_prepare(sentinel):
    """``master_for`` returns a client exposing the HIMPORT family; runtime
    ``himport_prepare`` populates the client's registry with no init argument, and
    discard is never forbidden."""
    master = sentinel.master_for("mymaster", db=9)
    assert master.himport_prepare("users", ["name", "email", "age"]) is True
    fs = master.himport_registry.get("users")
    assert fs is not None
    assert fs.fields == ("name", "email", "age")
    # Discard is always allowed under the runtime-only model.
    assert master.himport_discard("users") == 1
    assert master.himport_registry.get("users") is None


@pytest.mark.onlynoncluster
def test_himport_registry_shared_with_pool_connections(sentinel):
    """The runtime-prepared fieldset lives on the pool-level registry object that is
    injected into every connection via ``connection_kwargs``, so a connection created
    after a Sentinel failover (pool re-point) still sees it."""
    master = sentinel.master_for("mymaster", db=9)
    master.himport_prepare("shared", ["a", "b"])
    pool = master.connection_pool
    # The same registry object reaches every connection the pool creates.
    assert pool.connection_kwargs["himport_registry"] is pool.himport_registry
    assert "shared" in pool.himport_registry


@pytest.mark.onlynoncluster
def test_himport_prepare_set_survives_reconnect(deployed_sentinel):
    """Runtime ``himport_prepare`` on a reused ``master_for`` client survives a
    connection drop (the Sentinel-failover analog): the per-connection PREPARE is
    reset on reconnect and re-applied lazily on the next ``himport_set`` against the
    re-pointed pool. Requires a deployed Sentinel and Redis >= 8.9.0."""
    master = deployed_sentinel.master_for("redis-py-test", db=0)
    try:
        master.himport_prepare("users", ["name", "email"])
        master.delete("himport:sentinel:1")
        master.himport_set("himport:sentinel:1", "users", ["alice", "a@x.com"])
    except exceptions.ResponseError as exc:
        if "unknown command" in str(exc).lower() or "no such fieldset" in str(exc):
            pytest.skip("server does not support HIMPORT")
        raise
    assert master.hget("himport:sentinel:1", "name") == "alice"
    # Drop the pooled connection to simulate the failover reconnect, then reuse the
    # same client: the fieldset is re-prepared lazily on the next himport_set.
    for conn in list(master.connection_pool._available_connections):
        conn.disconnect()
    master.delete("himport:sentinel:2")
    master.himport_set("himport:sentinel:2", "users", ["bob", "b@x.com"])
    assert master.hget("himport:sentinel:2", "email") == "b@x.com"


@pytest.mark.fixed_client
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("7.4.0")
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
def test_cached_master_change_racing_with_checkout(
    r2, stack_url, tracking_mode, parser_class
):
    new_master = Redis.from_url(
        stack_url, db=r2.connection_pool.connection_kwargs.get("db", 0)
    )
    addresses = [
        (
            client.connection_pool.connection_kwargs["host"],
            client.connection_pool.connection_kwargs.get("port", 6379),
        )
        for client in [r2, new_master]
    ]
    assert addresses[0] != addresses[1]
    manager = mock.Mock()
    manager.discover_master.return_value = addresses[0]
    kwargs = dict(r2.connection_pool.connection_kwargs)
    kwargs.update(
        protocol=3,
        parser_class=parser_class,
        decode_responses=False,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    pool = SentinelConnectionPool(
        "mymaster",
        manager,
        max_connections=1,
        cache_config=CacheConfig(
            tracking_mode=tracking_mode,
            cache_predicate=None
            if tracking_mode is TrackingMode.PLAIN
            else lambda *_: True,
        ),
        **kwargs,
    )
    key = str(uuid.uuid4())
    r2.set(key, "old-master")
    new_master.set(key, "new-master")
    owner = pool.get_connection()
    owner.send_command("GET", key, keys=[key])
    assert owner.read_response() == b"old-master"
    pool.release(owner)
    captured, finish = threading.Event(), threading.Event()
    errors = []
    real_disconnect = owner._disconnect_from_pool

    def disconnect(inuse_connections):
        captured.set()
        assert finish.wait(5)
        real_disconnect(inuse_connections)

    def discover():
        try:
            assert pool.get_master_address() == addresses[1]
        except BaseException as exc:
            errors.append(exc)

    manager.discover_master.return_value = addresses[1]
    worker = threading.Thread(target=discover, daemon=True)
    checked_out = None
    try:
        with mock.patch.object(owner, "_disconnect_from_pool", side_effect=disconnect):
            worker.start()
            assert captured.wait(3)
            checked_out = pool.get_connection()
            finish.set()
            worker.join(3)
            assert not worker.is_alive() and not errors
            checked_out.send_command("GET", key, keys=[key])
            assert checked_out.read_response() == b"new-master"
            checked_out.send_command("PING")
            assert checked_out.read_response() == b"PONG"
            assert (checked_out.host, checked_out.port) == addresses[1]
    finally:
        finish.set()
        worker.join(5)
        if checked_out is not None:
            pool.release(checked_out)
        pool.disconnect()
        new_master.delete(key)
        new_master.close()
