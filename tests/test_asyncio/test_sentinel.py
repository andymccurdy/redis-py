import asyncio
import socket
from unittest import mock

import pytest
import pytest_asyncio
from redis.asyncio.client import StrictRedis

import redis.asyncio.sentinel
from redis import exceptions
from redis.asyncio.sentinel import (
    MasterNotFoundError,
    ReplicaNotFoundError,
    Sentinel,
    SentinelConnectionPool,
    SlaveNotFoundError,
)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
def master_ip(master_host):
    yield socket.gethostbyname(master_host[0])


class SentinelTestClient:
    def __init__(self, cluster, id):
        self.cluster = cluster
        self.id = id

    async def sentinel_masters(self):
        self.cluster.connection_error_if_down(self)
        self.cluster.timeout_if_down(self)
        # real sentinel I/O suspends the call mid-discovery; yield so that
        # concurrent discover_master() sequences can interleave
        await asyncio.sleep(0)
        return {self.cluster.service_name: self.cluster.master}

    async def sentinel_slaves(self, master_name):
        self.cluster.connection_error_if_down(self)
        self.cluster.timeout_if_down(self)
        if master_name != self.cluster.service_name:
            return []
        return self.cluster.slaves

    async def execute_command(self, *args, **kwargs):
        # wrapper purely to validate the calls don't explode
        from redis.asyncio.client import bool_ok

        return bool_ok


class SentinelTestCluster:
    def __init__(self, service_name="mymaster", ip="127.0.0.1", port=6379):
        self.clients = {}
        self.master = {
            "ip": ip,
            "port": port,
            "is_master": True,
            "is_sdown": False,
            "is_odown": False,
            "num-other-sentinels": 0,
        }
        self.service_name = service_name
        self.slaves = []
        self.nodes_down = set()
        self.nodes_timeout = set()

    def connection_error_if_down(self, node):
        if node.id in self.nodes_down:
            raise exceptions.ConnectionError

    def timeout_if_down(self, node):
        if node.id in self.nodes_timeout:
            raise exceptions.TimeoutError

    def client(self, host, port, **kwargs):
        return SentinelTestClient(self, (host, port))


@pytest_asyncio.fixture()
async def cluster(master_ip):
    cluster = SentinelTestCluster(ip=master_ip)
    saved_Redis = redis.asyncio.sentinel.Redis
    redis.asyncio.sentinel.Redis = cluster.client
    yield cluster
    redis.asyncio.sentinel.Redis = saved_Redis


@pytest_asyncio.fixture()
def sentinel(request, cluster):
    return Sentinel([("foo", 26379), ("bar", 26379)])


@pytest.fixture()
async def deployed_sentinel(request):
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
        await s.close()


@pytest.mark.onlynoncluster
async def test_discover_master(sentinel, master_ip):
    address = await sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)


@pytest.mark.onlynoncluster
async def test_discover_master_rotates_over_healthy_sentinels(
    cluster, sentinel, master_ip
):
    served = []
    for _ in range(6):
        assert await sentinel.discover_master("mymaster") == (master_ip, 6379)
        # after each call the sentinel that just answered is at the head
        served.append(sentinel.sentinels[0].id)
    assert set(served) == {("foo", 26379), ("bar", 26379)}


@pytest.mark.onlynoncluster
async def test_discover_master_keeps_failed_sentinel_last(cluster, sentinel, master_ip):
    await sentinel.discover_master("mymaster")
    cluster.nodes_down.add(("bar", 26379))
    for _ in range(3):
        assert await sentinel.discover_master("mymaster") == (master_ip, 6379)
        # 'foo' answers every call: the failed sentinel is not retried at
        # the head of the list
        assert sentinel.sentinels[0].id == ("foo", 26379)
    cluster.nodes_down.clear()
    # the failed sentinel is retried after a bounded number of calls and
    # rejoins the rotation once it answers again
    served = set()
    for _ in range(sentinel.FAILED_SENTINEL_QUARANTINE + 2):
        assert await sentinel.discover_master("mymaster") == (master_ip, 6379)
        served.add(sentinel.sentinels[0].id)
    assert served == {("foo", 26379), ("bar", 26379)}


@pytest.mark.onlynoncluster
async def test_discover_master_rotates_over_healthy_sentinels_during_partial_outage(
    cluster, master_ip
):
    # With one sentinel down and two healthy, discovery traffic must keep
    # alternating between the two healthy nodes instead of pinning to
    # whichever of them answered first.
    cluster.nodes_down.add(("foo", 26379))
    sentinel = Sentinel([("foo", 26379), ("first", 26379), ("second", 26379)])
    served = []
    for _ in range(8):
        assert await sentinel.discover_master("mymaster") == (master_ip, 6379)
        served.append(sentinel.sentinels[0].id)
    # quarantine expires every FAILED_SENTINEL_QUARANTINE calls, so 'foo'
    # is retried at most once inside this loop and cannot stay at the head
    assert served.count(("first", 26379)) >= 3
    assert served.count(("second", 26379)) >= 3
    assert served.count(("foo", 26379)) <= 1


@pytest.mark.onlynoncluster
async def test_discover_master_order_permutes_never_dups(cluster, sentinel, master_ip):
    orders = set()
    for _ in range(10):
        assert await sentinel.discover_master("mymaster") == (master_ip, 6379)
        ids = tuple(s.id for s in sentinel.sentinels)
        # every order is a permutation invariant: no duplicates, nothing lost
        assert sorted(ids) == [("bar", 26379), ("foo", 26379)]
        orders.add(ids)
    # both orders are actually exercised
    assert len(orders) == 2


@pytest.mark.onlynoncluster
async def test_discover_master_rotates_concurrently(cluster, sentinel, master_ip):
    # Two interleaved discovery sequences must not duplicate or drop
    # sentinels while they reorder the rotation.
    errors = []

    async def worker():
        try:
            for _ in range(25):
                assert await sentinel.discover_master("mymaster") == (master_ip, 6379)
                ids = [s.id for s in sentinel.sentinels]
                assert len(ids) == len(set(ids)), f"duplicate sentinels: {ids}"
                assert set(ids) == {("foo", 26379), ("bar", 26379)}
                # yield to the other worker so the sequences interleave
                await asyncio.sleep(0)
        except Exception as e:  # pragma: no cover - failure reporting
            errors.append(e)

    await asyncio.gather(worker(), worker())
    assert not errors


@pytest.mark.onlynoncluster
async def test_discover_master_error(sentinel):
    with pytest.raises(MasterNotFoundError):
        await sentinel.discover_master("xxx")


@pytest.mark.onlynoncluster
async def test_discover_master_sentinel_down(cluster, sentinel, master_ip):
    # Put first sentinel 'foo' down
    cluster.nodes_down.add(("foo", 26379))
    address = await sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


@pytest.mark.onlynoncluster
async def test_discover_master_sentinel_timeout(cluster, sentinel, master_ip):
    # Put first sentinel 'foo' down
    cluster.nodes_timeout.add(("foo", 26379))
    address = await sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


@pytest.mark.onlynoncluster
async def test_master_min_other_sentinels(cluster, master_ip):
    sentinel = Sentinel([("foo", 26379)], min_other_sentinels=1)
    # min_other_sentinels
    with pytest.raises(MasterNotFoundError):
        await sentinel.discover_master("mymaster")
    cluster.master["num-other-sentinels"] = 2
    address = await sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)


@pytest.mark.onlynoncluster
async def test_master_odown(cluster, sentinel):
    cluster.master["is_odown"] = True
    with pytest.raises(MasterNotFoundError):
        await sentinel.discover_master("mymaster")


@pytest.mark.onlynoncluster
async def test_master_sdown(cluster, sentinel):
    cluster.master["is_sdown"] = True
    with pytest.raises(MasterNotFoundError):
        await sentinel.discover_master("mymaster")


@pytest.mark.onlynoncluster
async def test_discover_slaves(cluster, sentinel):
    assert await sentinel.discover_slaves("mymaster") == []

    cluster.slaves = [
        {"ip": "slave0", "port": 1234, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 1234, "is_odown": False, "is_sdown": False},
    ]
    assert await sentinel.discover_slaves("mymaster") == [
        ("slave0", 1234),
        ("slave1", 1234),
    ]

    # slave0 -> ODOWN
    cluster.slaves[0]["is_odown"] = True
    assert await sentinel.discover_slaves("mymaster") == [("slave1", 1234)]

    # slave1 -> SDOWN
    cluster.slaves[1]["is_sdown"] = True
    assert await sentinel.discover_slaves("mymaster") == []

    cluster.slaves[0]["is_odown"] = False
    cluster.slaves[1]["is_sdown"] = False

    # node0 -> DOWN
    cluster.nodes_down.add(("foo", 26379))
    assert await sentinel.discover_slaves("mymaster") == [
        ("slave0", 1234),
        ("slave1", 1234),
    ]
    cluster.nodes_down.clear()

    # node0 -> TIMEOUT
    cluster.nodes_timeout.add(("foo", 26379))
    assert await sentinel.discover_slaves("mymaster") == [
        ("slave0", 1234),
        ("slave1", 1234),
    ]


@pytest.mark.onlynoncluster
async def test_discover_replicas(cluster, sentinel):
    assert await sentinel.discover_replicas("mymaster") == []

    cluster.slaves = [
        {"ip": "slave0", "port": 1234, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 1234, "is_odown": True, "is_sdown": False},
    ]

    assert sentinel.filter_replicas(cluster.slaves) == [("slave0", 1234)]
    assert await sentinel.discover_replicas("mymaster") == [("slave0", 1234)]


@pytest.mark.onlynoncluster
async def test_master_for(cluster, sentinel, master_ip):
    async with sentinel.master_for("mymaster", db=9) as master:
        assert await master.ping()
        assert master.connection_pool.master_address == (master_ip, 6379)

    # Use internal connection check
    async with sentinel.master_for("mymaster", db=9, check_connection=True) as master:
        assert await master.ping()


@pytest.mark.onlynoncluster
async def test_slave_for(cluster, sentinel):
    cluster.slaves = [
        {"ip": "127.0.0.1", "port": 6379, "is_odown": False, "is_sdown": False}
    ]
    async with sentinel.slave_for("mymaster", db=9) as slave:
        assert await slave.ping()


@pytest.mark.onlynoncluster
async def test_replica_for(cluster, sentinel):
    cluster.slaves = [
        {"ip": "127.0.0.1", "port": 6379, "is_odown": False, "is_sdown": False}
    ]
    async with sentinel.replica_for("mymaster", db=9) as replica:
        assert await replica.ping()


@pytest.mark.onlynoncluster
async def test_slave_for_slave_not_found_error(cluster, sentinel):
    cluster.master["is_odown"] = True
    async with sentinel.slave_for("mymaster", db=9) as slave:
        with pytest.raises(SlaveNotFoundError):
            await slave.ping()


@pytest.mark.onlynoncluster
async def test_replica_not_found_alias(cluster, sentinel):
    assert ReplicaNotFoundError is SlaveNotFoundError

    cluster.master["is_odown"] = True
    async with sentinel.replica_for("mymaster", db=9) as replica:
        with pytest.raises(ReplicaNotFoundError):
            await replica.ping()


@pytest.mark.onlynoncluster
async def test_slave_round_robin(cluster, sentinel, master_ip):
    cluster.slaves = [
        {"ip": "slave0", "port": 6379, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 6379, "is_odown": False, "is_sdown": False},
    ]
    pool = SentinelConnectionPool("mymaster", sentinel)
    rotator = pool.rotate_slaves()
    assert await rotator.__anext__() in (("slave0", 6379), ("slave1", 6379))
    assert await rotator.__anext__() in (("slave0", 6379), ("slave1", 6379))
    # Fallback to master
    assert await rotator.__anext__() == (master_ip, 6379)
    with pytest.raises(SlaveNotFoundError):
        await rotator.__anext__()


@pytest.mark.onlynoncluster
async def test_replica_round_robin(cluster, sentinel, master_ip):
    cluster.slaves = [
        {"ip": "slave0", "port": 6379, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 6379, "is_odown": False, "is_sdown": False},
    ]
    pool = SentinelConnectionPool("mymaster", sentinel)
    rotator = pool.rotate_replicas()
    assert await rotator.__anext__() in (("slave0", 6379), ("slave1", 6379))
    assert await rotator.__anext__() in (("slave0", 6379), ("slave1", 6379))
    # Fallback to master
    assert await rotator.__anext__() == (master_ip, 6379)
    with pytest.raises(ReplicaNotFoundError):
        await rotator.__anext__()


@pytest.mark.onlynoncluster
async def test_ckquorum(sentinel):
    resp = await sentinel.sentinel_ckquorum("mymaster")
    assert resp is True


@pytest.mark.onlynoncluster
async def test_flushconfig(sentinel):
    resp = await sentinel.sentinel_flushconfig()
    assert resp is True


@pytest.mark.onlynoncluster
async def test_reset(cluster, sentinel):
    cluster.master["is_odown"] = True
    resp = await sentinel.sentinel_reset("mymaster")
    assert resp is True


@pytest.mark.onlynoncluster
@pytest.mark.parametrize("method_name", ["master_for", "slave_for", "replica_for"])
async def test_auto_close_pool(cluster, sentinel, method_name):
    """
    Check that the connection pool created by the sentinel client is
    automatically closed
    """

    method = getattr(sentinel, method_name)
    client = method("mymaster", db=9)
    pool = client.connection_pool
    assert client.auto_close_connection_pool is True
    calls = 0

    async def mock_disconnect():
        nonlocal calls
        calls += 1

    with mock.patch.object(pool, "disconnect", mock_disconnect):
        await client.aclose()

    assert calls == 1
    await pool.disconnect()


@pytest.mark.onlynoncluster
async def test_repr_correctly_represents_connection_object(sentinel):
    pool = SentinelConnectionPool("mymaster", sentinel)
    connection = await pool.get_connection()

    assert (
        str(connection)
        == "<redis.asyncio.sentinel.SentinelManagedConnection,host=127.0.0.1,port=6379)>"  # noqa: E501
    )
    assert connection.connection_pool == pool
    await pool.release(connection)

    del pool

    assert (
        str(connection)
        == "<redis.asyncio.sentinel.SentinelManagedConnection,host=127.0.0.1,port=6379)>"  # noqa: E501
    )


@pytest.mark.onlynoncluster
async def test_aclose(cluster, sentinel):
    sentinel.sentinels = [mock.AsyncMock() for _ in range(2)]

    async with sentinel as entered:
        assert entered is sentinel

    # exiting the context closes every sentinel client and its pool
    for s in sentinel.sentinels:
        s.aclose.assert_awaited_once()


@pytest.mark.onlynoncluster
async def test_aclose_error_still_closes_remaining_clients(cluster, sentinel):
    sentinel.sentinels = [mock.AsyncMock() for _ in range(3)]
    sentinel.sentinels[0].aclose.side_effect = Exception("sentinel down")

    with pytest.raises(Exception, match="sentinel down"):
        await sentinel.aclose()

    # the failing client must not prevent closing the others
    for s in sentinel.sentinels:
        s.aclose.assert_awaited_once()


@pytest.mark.onlynoncluster
async def test_aclose_idempotent(cluster, sentinel):
    sentinel.sentinels = [mock.AsyncMock() for _ in range(2)]

    await sentinel.aclose()
    await sentinel.aclose()

    for s in sentinel.sentinels:
        assert s.aclose.await_count == 2


# Tests against real sentinel instances
@pytest.mark.onlynoncluster
async def test_get_sentinels(deployed_sentinel):
    resps = await deployed_sentinel.sentinel_sentinels(
        "redis-py-test", return_responses=True
    )

    # validate that the original command response is returned
    assert isinstance(resps, list)

    # validate that the command has been executed against all sentinels
    # each response from each sentinel is returned
    assert len(resps) > 1

    # validate default behavior
    resps = await deployed_sentinel.sentinel_sentinels("redis-py-test")
    assert isinstance(resps, bool)


@pytest.mark.onlynoncluster
async def test_get_master_addr_by_name(deployed_sentinel):
    resps = await deployed_sentinel.sentinel_get_master_addr_by_name(
        "redis-py-test",
        return_responses=True,
    )

    # validate that the original command response is returned
    assert isinstance(resps, list)

    # validate that the command has been executed just once
    # when executed once, only one response element is returned
    assert len(resps) == 1

    assert isinstance(resps[0], tuple)

    # validate default behavior
    resps = await deployed_sentinel.sentinel_get_master_addr_by_name("redis-py-test")
    assert isinstance(resps, bool)


@pytest.mark.onlynoncluster
async def test_redis_master_usage(deployed_sentinel):
    r = await deployed_sentinel.master_for("redis-py-test", db=0)
    await r.set("foo", "bar")
    assert (await r.get("foo")) == "bar"


@pytest.mark.onlynoncluster
async def test_sentinel_commands_with_strict_redis_client(request):
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
    assert isinstance(
        await client.sentinel_get_master_addr_by_name("redis-py-test"), tuple
    )
    assert isinstance(await client.sentinel_master("redis-py-test"), dict)
    assert isinstance(await client.sentinel_masters(), dict)

    assert isinstance(await client.sentinel_sentinels("redis-py-test"), list)
    assert isinstance(await client.sentinel_slaves("redis-py-test"), list)

    assert isinstance(await client.sentinel_ckquorum("redis-py-test"), bool)

    await client.close()


# ---------------------------------------------------------------------------
# HIMPORT on Sentinel (async)
# ---------------------------------------------------------------------------
# Mirrors tests/test_sentinel.py: the client from ``master_for`` inherits the shared
# ``HImportRegistry`` from ``ConnectionPool`` and declares fieldsets at runtime; the
# registry lives on the long-lived pool so it survives failover.


@pytest.mark.onlynoncluster
async def test_himport_master_for_exposes_runtime_prepare(sentinel):
    """``master_for`` returns a client exposing the HIMPORT family; runtime
    ``himport_prepare`` populates the client's registry with no init argument, and
    discard is never forbidden."""
    async with sentinel.master_for("mymaster", db=9) as master:
        assert await master.himport_prepare("users", ["name", "email", "age"]) is True
        fs = master.himport_registry.get("users")
        assert fs is not None
        assert fs.fields == ("name", "email", "age")
        assert await master.himport_discard("users") == 1
        assert master.himport_registry.get("users") is None


@pytest.mark.onlynoncluster
async def test_himport_registry_shared_with_pool_connections(sentinel):
    """The runtime-prepared fieldset lives on the pool-level registry object injected
    into every connection via ``connection_kwargs``, so a connection created after a
    Sentinel failover (pool re-point) still sees it."""
    async with sentinel.master_for("mymaster", db=9) as master:
        await master.himport_prepare("shared", ["a", "b"])
        pool = master.connection_pool
        assert pool.connection_kwargs["himport_registry"] is pool.himport_registry
        assert "shared" in pool.himport_registry


@pytest.mark.onlynoncluster
async def test_himport_prepare_set_survives_reconnect(deployed_sentinel):
    """Async mirror of the sync reconnect test: runtime ``himport_prepare`` on a
    reused ``master_for`` client survives a connection drop (the Sentinel-failover
    analog); the fieldset is re-prepared lazily on the next ``himport_set`` against
    the re-pointed pool. Requires a deployed Sentinel and Redis >= 8.9.0."""
    master = await deployed_sentinel.master_for("redis-py-test", db=0)
    try:
        await master.himport_prepare("users", ["name", "email"])
        await master.delete("himport:sentinel:async:1")
        await master.himport_set(
            "himport:sentinel:async:1", "users", ["alice", "a@x.com"]
        )
    except exceptions.ResponseError as exc:
        if "unknown command" in str(exc).lower() or "no such fieldset" in str(exc):
            pytest.skip("server does not support HIMPORT")
        raise
    assert await master.hget("himport:sentinel:async:1", "name") == "alice"
    # Drop pooled connections to simulate the failover reconnect, then reuse the same
    # client: the fieldset is re-prepared lazily on the next himport_set.
    await master.connection_pool.disconnect()
    await master.delete("himport:sentinel:async:2")
    await master.himport_set("himport:sentinel:async:2", "users", ["bob", "b@x.com"])
    assert await master.hget("himport:sentinel:async:2", "email") == "b@x.com"
