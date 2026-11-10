import os
import socket
from unittest import mock

import pytest
from redis.client import StrictRedis

import redis.sentinel
from redis import exceptions
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.observability.attributes import ConnectionState, get_pool_name
from redis.sentinel import (
    MasterNotFoundError,
    ReplicaNotFoundError,
    Sentinel,
    SentinelConnectionPool,
    SlaveNotFoundError,
)


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


@pytest.mark.onlynoncluster
def test_sentinel_clients_do_not_retry_by_default():
    # Master discovery moves on to the next sentinel by itself; retrying an
    # unreachable sentinel with the client default policy multiplies its timeout.
    sentinel = Sentinel([("localhost", 26379), ("localhost", 26380)])
    assert [s.get_retry().get_retries() for s in sentinel.sentinels] == [0, 0]


@pytest.mark.onlynoncluster
def test_sentinel_clients_retry_once_on_error_when_asked():
    sentinel = Sentinel(
        [("localhost", 26379)],
        sentinel_kwargs={"retry_on_error": [exceptions.ConnectionError]},
    )
    assert sentinel.sentinels[0].get_retry().get_retries() == 1


@pytest.mark.onlynoncluster
def test_sentinel_clients_use_the_given_retry():
    sentinel = Sentinel(
        [("localhost", 26379)],
        sentinel_kwargs={"retry": Retry(ExponentialBackoff(), 2)},
    )
    assert sentinel.sentinels[0].get_retry().get_retries() == 2


@pytest.mark.onlynoncluster
def test_sentinel_kwargs_are_not_modified():
    sentinel_kwargs = {"socket_timeout": 1}
    Sentinel([("localhost", 26379)], sentinel_kwargs=sentinel_kwargs)
    assert sentinel_kwargs == {"socket_timeout": 1}
