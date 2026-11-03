import random
from contextlib import asynccontextmanager as _asynccontextmanager
from typing import Union
from unittest import mock

import pytest
import pytest_asyncio
import redis.asyncio as redis
from packaging.version import Version
from redis.asyncio import Sentinel
from redis.asyncio.client import Monitor
from redis.asyncio.connection import Connection, parse_url
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.credentials import CredentialProvider
from tests.conftest import REDIS_INFO, get_credential_provider


async def _get_info(redis_url):
    client = redis.Redis.from_url(redis_url)
    info = await client.info()
    await client.connection_pool.disconnect()
    return info


@pytest_asyncio.fixture(
    params=[
        pytest.param(
            (True,),
            marks=pytest.mark.skipif(
                'config.REDIS_INFO["cluster_enabled"]', reason="cluster mode enabled"
            ),
        ),
        (False,),
    ],
    ids=[
        "single",
        "pool",
    ],
)
async def create_redis(request):
    """Wrapper around redis.create_redis."""
    (single_connection,) = request.param

    teardown_clients = []

    async def client_factory(
        url: str = request.config.getoption("--redis-url"),
        cls=redis.Redis,
        flushdb=True,
        **kwargs,
    ):
        if "protocol" not in url and kwargs.get("protocol") is None:
            protocol_opt = request.config.getoption("--protocol")
            if protocol_opt:
                kwargs["protocol"] = protocol_opt

        if "legacy_responses" not in kwargs:
            legacy_opt = request.config.getoption("--legacy-responses")
            if legacy_opt and legacy_opt != "default":
                kwargs["legacy_responses"] = legacy_opt.lower() == "true"

        cluster_mode = REDIS_INFO["cluster_enabled"]
        if not cluster_mode:
            single = kwargs.pop("single_connection_client", False) or single_connection
            url_options = parse_url(url)
            url_options.update(kwargs)
            pool = redis.ConnectionPool(**url_options)
            client = cls(connection_pool=pool)
        else:
            client = redis.RedisCluster.from_url(url, **kwargs)
            await client.initialize()
            single = False
        if single:
            client = client.client()
            await client.initialize()

        async def teardown():
            if not cluster_mode:
                if flushdb and "username" not in kwargs:
                    try:
                        await client.flushdb()
                    except redis.ConnectionError:
                        # handle cases where a test disconnected a client
                        # just manually retry the flushdb
                        await client.flushdb()
                await client.aclose()
                await client.connection_pool.disconnect()
            else:
                if flushdb:
                    try:
                        await client.flushdb(target_nodes="primaries")
                    except redis.ConnectionError:
                        # handle cases where a test disconnected a client
                        # just manually retry the flushdb
                        await client.flushdb(target_nodes="primaries")
                await client.aclose()

        teardown_clients.append(teardown)
        return client

    yield client_factory

    for teardown in teardown_clients:
        await teardown()


@pytest_asyncio.fixture()
async def r(create_redis):
    return await create_redis()


@pytest_asyncio.fixture()
async def r2(create_redis):
    """A second client for tests that need multiple"""
    return await create_redis()


@pytest_asyncio.fixture()
async def decoded_r(create_redis):
    return await create_redis(decode_responses=True)


@pytest_asyncio.fixture()
async def sentinel_setup(local_cache, request):
    sentinel_ips = request.config.getoption("--sentinels")
    sentinel_endpoints = [
        (ip.strip(), int(port.strip()))
        for ip, port in (endpoint.split(":") for endpoint in sentinel_ips.split(","))
    ]
    kwargs = request.param.get("kwargs", {}) if hasattr(request, "param") else {}
    force_master_ip = request.param.get("force_master_ip", None)
    sentinel = Sentinel(
        sentinel_endpoints,
        force_master_ip=force_master_ip,
        socket_timeout=0.1,
        client_cache=local_cache,
        protocol=3,
        **kwargs,
    )
    yield sentinel
    for s in sentinel.sentinels:
        await s.aclose()


@pytest_asyncio.fixture()
async def master(request, sentinel_setup):
    master_service = request.config.getoption("--master-service")
    master = sentinel_setup.master_for(master_service)
    yield master
    await master.aclose()


def _gen_cluster_mock_resp(r, response):
    connection = mock.AsyncMock(spec=Connection)
    connection._resp2_fallback = False
    connection.retry = Retry(NoBackoff(), 0)
    connection.read_response.return_value = response
    connection.host = "localhost"
    connection.port = 6379
    connection.db = 0
    with mock.patch.object(r, "connection", connection):
        yield r


@pytest_asyncio.fixture()
async def mock_cluster_resp_ok(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    for mocked in _gen_cluster_mock_resp(r, "OK"):
        yield mocked


@pytest_asyncio.fixture()
async def mock_cluster_resp_int(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    for mocked in _gen_cluster_mock_resp(r, 2):
        yield mocked


@pytest_asyncio.fixture()
async def mock_cluster_resp_info(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    response = (
        "cluster_state:ok\r\ncluster_slots_assigned:16384\r\n"
        "cluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\n"
        "cluster_slots_fail:0\r\ncluster_known_nodes:7\r\n"
        "cluster_size:3\r\ncluster_current_epoch:7\r\n"
        "cluster_my_epoch:2\r\ncluster_stats_messages_sent:170262\r\n"
        "cluster_stats_messages_received:105653\r\n"
    )
    for mocked in _gen_cluster_mock_resp(r, response):
        yield mocked


@pytest_asyncio.fixture()
async def mock_cluster_resp_nodes(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    response = (
        "c8253bae761cb1ecb2b61857d85dfe455a0fec8b 172.17.0.7:7006 "
        "slave aa90da731f673a99617dfe930306549a09f83a6b 0 "
        "1447836263059 5 connected\n"
        "9bd595fe4821a0e8d6b99d70faa660638a7612b3 172.17.0.7:7008 "
        "master - 0 1447836264065 0 connected\n"
        "aa90da731f673a99617dfe930306549a09f83a6b 172.17.0.7:7003 "
        "myself,master - 0 0 2 connected 5461-10922\n"
        "1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
        "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
        "1447836262556 3 connected\n"
        "4ad9a12e63e8f0207025eeba2354bcf4c85e5b22 172.17.0.7:7005 "
        "master - 0 1447836262555 7 connected 0-5460\n"
        "19efe5a631f3296fdf21a5441680f893e8cc96ec 172.17.0.7:7004 "
        "master - 0 1447836263562 3 connected 10923-16383\n"
        "fbb23ed8cfa23f17eaf27ff7d0c410492a1093d6 172.17.0.7:7002 "
        "master,fail - 1447829446956 1447829444948 1 disconnected\n"
    )
    for mocked in _gen_cluster_mock_resp(r, response):
        yield mocked


@pytest_asyncio.fixture()
async def mock_cluster_resp_slaves(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    response = (
        "['1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
        "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
        "1447836789290 3 connected']"
    )
    for mocked in _gen_cluster_mock_resp(r, response):
        yield mocked


@pytest_asyncio.fixture()
async def credential_provider(request) -> CredentialProvider:
    return get_credential_provider(request)


async def wait_for_command(
    client: redis.Redis, monitor: Monitor, command: str, key: Union[str, None] = None
):
    # issue a command with a key name that's local to this process.
    # if we find a command with our key before the command we're waiting
    # for, something went wrong
    if key is None:
        # generate key
        redis_version = REDIS_INFO["version"]
        if Version(redis_version) >= Version("5.0.0"):
            id_str = str(await client.client_id())
        else:
            id_str = f"{random.randrange(2**32):08x}"
        key = f"__REDIS-PY-{id_str}__"
    await client.get(key)
    while True:
        monitor_response = await monitor.next_command()
        if command in monitor_response["command"]:
            return monitor_response
        if key in monitor_response["command"]:
            return None


# python 3.6 doesn't have the asynccontextmanager decorator.  Provide it here.
class AsyncContextManager:
    def __init__(self, async_generator):
        self.gen = async_generator

    async def __aenter__(self):
        try:
            return await self.gen.__anext__()
        except StopAsyncIteration as err:
            raise RuntimeError("Pickles") from err

    async def __aexit__(self, exc_type, exc_inst, tb):
        if exc_type:
            await self.gen.athrow(exc_type, exc_inst, tb)
            return True
        try:
            await self.gen.__anext__()
        except StopAsyncIteration:
            return
        raise RuntimeError("More pickles")


def asynccontextmanager(func):
    return _asynccontextmanager(func)


# helpers to get the connection arguments for this run
@pytest.fixture()
def redis_url(request):
    return request.config.getoption("--redis-url")


@pytest.fixture()
def connect_args(request):
    url = request.config.getoption("--redis-url")
    return parse_url(url)


async def _async_enable_keyspace_notifications(client, flags="KEA"):
    """
    Enable keyspace notifications on an async Redis server (standalone or cluster).

    Returns a dict mapping a label to (connection, original_value, owns_conn).
    """
    cluster_mode = REDIS_INFO.get("cluster_enabled", False)

    original_configs = {}
    if cluster_mode:
        for node in client.get_primaries():
            node_conn = redis.Redis(host=node.host, port=node.port)
            original = await node_conn.config_get("notify-keyspace-events")
            original_configs[node.name] = (
                node_conn,
                original.get("notify-keyspace-events", ""),
            )
            await node_conn.config_set("notify-keyspace-events", flags)
    else:
        original = await client.config_get("notify-keyspace-events")
        original_configs["standalone"] = (
            client,
            original.get("notify-keyspace-events", ""),
        )
        await client.config_set("notify-keyspace-events", flags)

    return original_configs, cluster_mode


async def _async_restore_keyspace_notifications(original_configs, cluster_mode):
    for _name, (conn, original_value) in original_configs.items():
        try:
            await conn.config_set("notify-keyspace-events", original_value)
        except Exception:
            pass
        if cluster_mode:
            await conn.aclose()


@pytest_asyncio.fixture()
async def async_r_with_keyspace_notifications(create_redis):
    """An async Redis client with keyspace notifications enabled.

    Uses notify-keyspace-events=KEA, supported on all Redis versions.

    Works for both standalone Redis and RedisCluster.
    In cluster mode, keyspace notifications are enabled on all primary nodes.
    """
    client = await create_redis()
    original_configs, cluster_mode = await _async_enable_keyspace_notifications(
        client, flags="KEA"
    )
    try:
        yield client
    finally:
        await _async_restore_keyspace_notifications(original_configs, cluster_mode)


@pytest_asyncio.fixture()
async def async_r_with_subkey_notifications(create_redis):
    """An async Redis client with keyspace and subkey notifications enabled.

    Uses notify-keyspace-events=KEASTIV, which enables subkey notification
    flags (S, T, I, V). These flags are only available in Redis >= 8.7.2;
    tests using this fixture must be guarded accordingly.

    Works for both standalone Redis and RedisCluster.
    """
    client = await create_redis()
    original_configs, cluster_mode = await _async_enable_keyspace_notifications(
        client, flags="KEASTIV"
    )
    try:
        yield client
    finally:
        await _async_restore_keyspace_notifications(original_configs, cluster_mode)
