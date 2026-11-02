import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Callable, TypeVar
from unittest import mock
from unittest.mock import Mock
from urllib.parse import quote, urlparse

import pytest
import redis
from packaging.version import Version
from redis import Sentinel
from redis.auth.idp import IdentityProviderInterface
from redis.auth.token import JWToken
from redis.backoff import NoBackoff
from redis.cache import (
    CacheConfig,
    CacheFactoryInterface,
    CacheInterface,
    CacheKey,
    EvictionPolicy,
)
from redis.connection import Connection, ConnectionInterface, SSLConnection, parse_url
from redis.credentials import CredentialProvider
from redis.event import EventDispatcherInterface
from redis.exceptions import RedisClusterException
from redis.retry import Retry
from redis.utils import DEFAULT_RESP_VERSION
from tests.ssl_utils import get_tls_certificates

REDIS_INFO = {}
default_redis_url = "redis://localhost:6379/0"
# Empty string means "do not override": clients fall back to the library
# default (which is RESP3 on the wire as of the RESP3-default change).
default_protocol = ""
default_redismod_url = "redis://localhost:6479"

# default ssl client ignores verification for the purpose of testing
default_redis_ssl_url = "rediss://localhost:6666"
default_cluster_nodes = 6

_DecoratedTest = TypeVar("_DecoratedTest", bound="Callable")
_TestDecorator = Callable[[_DecoratedTest], _DecoratedTest]


# Taken from python3.9
class BooleanOptionalAction(argparse.Action):
    def __init__(
        self,
        option_strings,
        dest,
        default=None,
        type=None,
        choices=None,
        required=False,
        help=None,
        metavar=None,
    ):
        _option_strings = []
        for option_string in option_strings:
            _option_strings.append(option_string)

            if option_string.startswith("--"):
                option_string = "--no-" + option_string[2:]
                _option_strings.append(option_string)

        if help is not None and default is not None:
            help += f" (default: {default})"

        super().__init__(
            option_strings=_option_strings,
            dest=dest,
            nargs=0,
            default=default,
            type=type,
            choices=choices,
            required=required,
            help=help,
            metavar=metavar,
        )

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string in self.option_strings:
            setattr(namespace, self.dest, not option_string.startswith("--no-"))

    def format_usage(self):
        return " | ".join(self.option_strings)


@pytest.fixture(scope="session", autouse=True)
def enable_tracemalloc(request):
    """
    Enable tracemalloc while tests are being executed, if ``--tracemalloc``
    is given.

    Tracing every allocation makes allocation-heavy code, such as parsing the
    ``COMMAND`` reply during cluster client setup, several times slower, so it
    is off by default. Turn it on to get allocation tracebacks in
    ``ResourceWarning`` messages.
    """
    if not request.config.getoption("--tracemalloc"):
        yield
        return
    try:
        import tracemalloc

        # Leave tracing started by PYTHONTRACEMALLOC or -X tracemalloc running.
        started = not tracemalloc.is_tracing()
        if started:
            tracemalloc.start()
        yield
        if started:
            tracemalloc.stop()
    except ImportError:
        yield


def pytest_addoption(parser):
    parser.addoption(
        "--redis-url",
        default=default_redis_url,
        action="store",
        help="Redis connection string, defaults to `%(default)s`",
    )

    parser.addoption(
        "--redis-mod-url",
        default=default_redismod_url,
        action="store",
        help="Redis with modules connection string, defaults to `%(default)s`",
    )

    parser.addoption(
        "--protocol",
        default=default_protocol,
        action="store",
        help="Protocol version, defaults to `%(default)s`",
    )
    parser.addoption(
        "--legacy-responses",
        default="default",
        action="store",
        help=(
            "Override the ``legacy_responses`` argument on test clients. "
            "Use 'true' or 'false' to force the value; 'default' leaves it "
            "unset so the client constructor's default applies."
        ),
    )
    parser.addoption(
        "--redis-ssl-url",
        default=default_redis_ssl_url,
        action="store",
        help="Redis SSL connection string, defaults to `%(default)s`",
    )

    parser.addoption(
        "--redis-cluster-nodes",
        default=default_cluster_nodes,
        action="store",
        help="The number of cluster nodes that need to be "
        "available before the test can start,"
        " defaults to `%(default)s`",
    )

    parser.addoption(
        "--uvloop", action=BooleanOptionalAction, help="Run tests with uvloop"
    )

    parser.addoption(
        "--tracemalloc",
        action=BooleanOptionalAction,
        default=False,
        help="Trace memory allocations with tracemalloc while tests run",
    )

    parser.addoption(
        "--sentinels",
        action="store",
        default="localhost:26379,localhost:26380,localhost:26381",
        help="Comma-separated list of sentinel IPs and ports",
    )
    parser.addoption(
        "--master-service",
        action="store",
        default="redis-py-test",
        help="Name of the Redis master service that the sentinels are monitoring",
    )

    parser.addoption(
        "--endpoint-name",
        action="store",
        default=None,
        help="Name of the Redis endpoint the tests should be executed on",
    )

    parser.addoption(
        "--cluster-endpoint-name",
        action="store",
        default=None,
        help="Name of the Redis endpoint with OSS API the tests should be executed on",
    )


def _redis_url_from_endpoints_config():
    """Build a connection URL for a Redis Enterprise database.

    Reads the database named by ``RE_DB_NAME`` (default ``standalone``) from the
    endpoints config at ``REDIS_ENDPOINTS_CONFIG_PATH`` and returns a redis URL
    carrying the resolved host, port, credentials and (for ``rediss``) TLS. This
    is the same endpoints-config shape consumed by ``tests/test_scenario``.
    """
    db_name = os.getenv("RE_DB_NAME", "standalone")
    config_path = os.getenv("REDIS_ENDPOINTS_CONFIG_PATH")
    if not (config_path and os.path.exists(config_path)):
        raise FileNotFoundError(f"Endpoints config file not found: {config_path}")

    with open(config_path, "r") as f:
        data = json.load(f)
    db = data[db_name]

    parsed = urlparse(db["endpoints"][0])
    scheme = parsed.scheme or "redis"
    host = parsed.hostname
    port = parsed.port or 6379
    username = db.get("username") or ""
    password = db.get("password")

    userinfo = ""
    if password is not None:
        userinfo = f"{quote(username, safe='')}:{quote(password, safe='')}@"

    url = f"{scheme}://{userinfo}{host}:{port}"
    if scheme == "rediss":
        url += "?ssl_cert_reqs=none&ssl_check_hostname=false"
    return url


def pytest_configure(config):
    # When RE_CLUSTER is set, point the suite at the managed Redis Enterprise
    # database resolved from the endpoints config instead of localhost. When the
    # variable is unset, behaviour is unchanged (localhost:6379, no auth).
    if os.getenv("RE_CLUSTER", "").lower() == "true":
        re_url = _redis_url_from_endpoints_config()
        config.option.redis_url = re_url
        config.option.redis_mod_url = re_url


def _get_info(redis_url):
    client = redis.Redis.from_url(redis_url)
    info = client.info()
    try:
        client.execute_command("DPING")
        info["enterprise"] = True
    except redis.ResponseError:
        info["enterprise"] = False
    client.connection_pool.disconnect()
    return info


def pytest_sessionstart(session):
    # during test discovery, e.g. with VS Code, we may not
    # have a server running.
    protocol = session.config.getoption("--protocol")
    # When ``--protocol`` is not provided (or is empty), resolve to the
    # library default so RESP-version-gated decorators reflect the wire
    # protocol that clients will actually negotiate.
    REDIS_INFO["resp_version"] = int(protocol) if protocol else DEFAULT_RESP_VERSION
    redis_url = session.config.getoption("--redis-url")
    try:
        info = _get_info(redis_url)
        version = info["redis_version"]
        arch_bits = info["arch_bits"]
        cluster_enabled = info["cluster_enabled"]
        enterprise = info["enterprise"]
    except redis.ConnectionError:
        # provide optimistic defaults
        info = {}
        version = "10.0.0"
        arch_bits = 64
        cluster_enabled = False
        enterprise = False
    REDIS_INFO["version"] = version
    REDIS_INFO["arch_bits"] = arch_bits
    REDIS_INFO["cluster_enabled"] = cluster_enabled
    REDIS_INFO["tls_cert_subdir"] = "cluster" if cluster_enabled else "standalone"
    REDIS_INFO["enterprise"] = enterprise
    # store REDIS_INFO in config so that it is available from "condition strings"
    session.config.REDIS_INFO = REDIS_INFO

    # module info
    stack_url = session.config.getoption("--redis-mod-url")

    try:
        stack_info = _get_info(stack_url)
        REDIS_INFO["modules"] = stack_info["modules"]
    except (KeyError, redis.exceptions.ConnectionError):
        pass

    if cluster_enabled:
        cluster_nodes = session.config.getoption("--redis-cluster-nodes")
        wait_for_cluster_creation(redis_url, cluster_nodes)

    use_uvloop = session.config.getoption("--uvloop")

    if use_uvloop:
        try:
            import uvloop

            uvloop.install()
        except ImportError as e:
            raise RuntimeError(
                "Can not import uvloop, make sure it is installed"
            ) from e


def wait_for_cluster_creation(redis_url, cluster_nodes, timeout=60):
    """
    Waits for the cluster creation to complete.
    As soon as all :cluster_nodes: nodes become available, the cluster will be
    considered ready.
    :param redis_url: the cluster's url, e.g. redis://localhost:16379/0
    :param cluster_nodes: The number of nodes in the cluster
    :param timeout: the amount of time to wait (in seconds)
    """
    now = time.monotonic()
    end_time = now + timeout
    client = None
    print(f"Waiting for {cluster_nodes} cluster nodes to become available")
    while now < end_time:
        try:
            client = redis.RedisCluster.from_url(redis_url)
            if len(client.get_nodes()) == int(cluster_nodes):
                print("All nodes are available!")
                break
        except RedisClusterException:
            pass
        time.sleep(1)
        now = time.monotonic()
    if now >= end_time:
        available_nodes = 0 if client is None else len(client.get_nodes())
        raise RedisClusterException(
            f"The cluster did not become available after {timeout} seconds. "
            f"Only {available_nodes} nodes out of {cluster_nodes} are available"
        )


def skip_if_server_version_lt(min_version: str) -> _TestDecorator:
    redis_version = REDIS_INFO.get("version", "0")
    check = Version(redis_version) < Version(min_version)
    return pytest.mark.skipif(check, reason=f"Redis version required >= {min_version}")


def skip_if_server_version_gte(min_version: str) -> _TestDecorator:
    redis_version = REDIS_INFO.get("version", "0")
    check = Version(redis_version) >= Version(min_version)
    return pytest.mark.skipif(check, reason=f"Redis version required < {min_version}")


def skip_unless_arch_bits(arch_bits: int) -> _TestDecorator:
    return pytest.mark.skipif(
        REDIS_INFO.get("arch_bits", "") != arch_bits,
        reason=f"server is not {arch_bits}-bit",
    )


def skip_ifmodversion_lt(min_version: str, module_name: str):
    try:
        modules = REDIS_INFO["modules"]
    except KeyError:
        return pytest.mark.skipif(True, reason="Redis server does not have modules")
    if modules == []:
        return pytest.mark.skipif(True, reason="No redis modules found")

    for j in modules:
        if module_name == j.get("name"):
            version = j.get("ver")
            mv = int(
                "".join(["%02d" % int(segment) for segment in min_version.split(".")])
            )
            check = version < mv
            return pytest.mark.skipif(check, reason="Redis module version")

    raise AttributeError(f"No redis module named {module_name}")


def skip_if_redis_enterprise() -> _TestDecorator:
    check = REDIS_INFO.get("enterprise", False) is True
    return pytest.mark.skipif(check, reason="Redis enterprise")


def skip_ifnot_redis_enterprise() -> _TestDecorator:
    check = REDIS_INFO.get("enterprise", False) is False
    return pytest.mark.skipif(check, reason="Not running in redis enterprise")


def skip_if_nocryptography() -> _TestDecorator:
    # try:
    #     import cryptography  # noqa
    #
    #     return pytest.mark.skipif(False, reason="Cryptography dependency found")
    # except ImportError:
    # TODO: Because JWT library depends on cryptography,
    #  now it's always true and tests should be fixed
    return pytest.mark.skipif(True, reason="No cryptography dependency")


def skip_if_cryptography() -> _TestDecorator:
    try:
        import cryptography  # noqa

        return pytest.mark.skipif(True, reason="Cryptography dependency found")
    except ImportError:
        return pytest.mark.skipif(False, reason="No cryptography dependency")


def skip_if_resp_version(resp_version) -> _TestDecorator:
    check = REDIS_INFO.get("resp_version", None) == resp_version
    return pytest.mark.skipif(check, reason=f"RESP version required != {resp_version}")


def skip_if_hiredis_parser() -> _TestDecorator:
    try:
        import hiredis  # noqa

        return pytest.mark.skipif(True, reason="hiredis dependency found")
    except ImportError:
        return pytest.mark.skipif(False, reason="No hiredis dependency")


def _get_client(
    cls, request, single_connection_client=True, flushdb=True, from_url=None, **kwargs
):
    """
    Helper for fixtures or tests that need a Redis client

    Uses the "--redis-url" command line argument for connection info. Unlike
    ConnectionPool.from_url, keyword arguments to this function override
    values specified in the URL.
    """
    if from_url is None:
        redis_url = request.config.getoption("--redis-url")
    else:
        redis_url = from_url

    redis_tls_url = request.config.getoption("--redis-ssl-url")

    if "protocol" not in redis_url and kwargs.get("protocol") is None:
        protocol_opt = request.config.getoption("--protocol")
        if protocol_opt:
            kwargs["protocol"] = protocol_opt

    if "legacy_responses" not in kwargs:
        legacy_opt = request.config.getoption("--legacy-responses")
        if legacy_opt and legacy_opt != "default":
            kwargs["legacy_responses"] = legacy_opt.lower() == "true"

    cluster_mode = REDIS_INFO["cluster_enabled"]
    ssl = kwargs.pop("ssl", False)
    if not cluster_mode:
        url_options = parse_url(redis_url)
        connection_class = Connection
        if ssl:
            connection_class = SSLConnection
            kwargs["ssl_certfile"], kwargs["ssl_keyfile"], kwargs["ssl_ca_certs"] = (
                get_tls_certificates()
            )
            kwargs["ssl_cert_reqs"] = "required"
            kwargs["port"] = urlparse(redis_tls_url).port
        kwargs["connection_class"] = connection_class
        url_options.update(kwargs)
        pool = redis.ConnectionPool(**url_options)
        client = cls(connection_pool=pool)
    else:
        client = redis.RedisCluster.from_url(redis_url, **kwargs)
        single_connection_client = False
    if single_connection_client:
        client = client.client()
    if request:

        def teardown():
            if not cluster_mode:
                if flushdb:
                    try:
                        client.flushdb()
                    except redis.ConnectionError:
                        # handle cases where a test disconnected a client
                        # just manually retry the flushdb
                        client.flushdb()
                client.close()
                client.connection_pool.disconnect()
            else:
                cluster_teardown(client, flushdb)

        request.addfinalizer(teardown)
    return client


def cluster_teardown(client, flushdb):
    if flushdb:
        try:
            client.flushdb(target_nodes="primaries")
        except redis.ConnectionError:
            # handle cases where a test disconnected a client
            # just manually retry the flushdb
            client.flushdb(target_nodes="primaries")
    client.close()
    client.disconnect_connection_pools()


def patch_monotonic(module: str, value: float):
    """
    Freeze ``time.monotonic`` as :module: sees it, without patching the
    ``time`` module that every other thread and library also uses.
    """
    frozen_time = Mock(wraps=time)
    frozen_time.monotonic.return_value = value
    return mock.patch(f"{module}.time", frozen_time)


@pytest.fixture()
def no_topology_refresh_interval():
    """
    Let every error-triggered cluster topology refresh run at once, for tests
    of the refresh logic itself rather than of its minimum interval.
    """
    with mock.patch("redis.cluster._TOPOLOGY_REFRESH_MIN_INTERVAL_SECONDS", 0):
        yield


@pytest.fixture()
def no_cluster_retry_delay():
    """
    Skip the backoff sleep between cluster retries, for tests that count
    retries rather than time them.
    """
    # The async clients inherit it from the same class
    with mock.patch.object(
        redis.cluster.AbstractRedisCluster, "_retry_backoff_delay", return_value=0
    ):
        yield


@pytest.fixture()
def r(request):
    with _get_client(redis.Redis, request) as client:
        yield client


def _enable_keyspace_notifications(client, flags="KEA"):
    """
    Enable keyspace notifications on a Redis server (standalone or cluster).

    Returns a function to restore the original configuration.
    """
    cluster_mode = REDIS_INFO.get("cluster_enabled", False)

    if cluster_mode:
        # For cluster, configure all primary nodes
        # Store both the original config and the node connection for cleanup
        original_configs = {}
        node_connections = {}
        for node in client.get_primaries():
            node_client = client.get_redis_connection(node)
            original = node_client.config_get("notify-keyspace-events")
            original_configs[node.name] = original.get("notify-keyspace-events", "")
            node_connections[node.name] = node_client
            node_client.config_set("notify-keyspace-events", flags)

        def restore():
            # Restore configuration on all nodes that were originally configured
            # Use the stored connections to ensure we restore the same nodes
            for node_name, original_value in original_configs.items():
                try:
                    # First try the stored connection
                    if node_name in node_connections:
                        node_connections[node_name].config_set(
                            "notify-keyspace-events", original_value
                        )
                    else:
                        # Fallback: try to find the node in current topology
                        for node in client.get_primaries():
                            if node.name == node_name:
                                node_client = client.get_redis_connection(node)
                                node_client.config_set(
                                    "notify-keyspace-events", original_value
                                )
                                break
                except Exception as e:
                    # Log but don't fail cleanup - node may have changed
                    import warnings

                    warnings.warn(
                        f"Failed to restore keyspace config on {node_name}: {e}"
                    )

        return restore
    else:
        # For standalone, configure the single server
        original_config = client.config_get("notify-keyspace-events")
        original_value = original_config.get("notify-keyspace-events", "")
        client.config_set("notify-keyspace-events", flags)

        def restore():
            try:
                client.config_set("notify-keyspace-events", original_value)
            except Exception as e:
                import warnings

                warnings.warn(f"Failed to restore keyspace config: {e}")

        return restore


@pytest.fixture()
def r_with_keyspace_notifications(request):
    """
    A Redis client with keyspace notifications enabled on the server.

    This fixture configures the server to emit all keyspace and keyevent
    notifications (notify-keyspace-events=KEA) for the duration of the test,
    then restores the original configuration.

    Works for both standalone Redis and RedisCluster.
    """
    with _get_client(redis.Redis, request) as client:
        restore = _enable_keyspace_notifications(client)
        try:
            yield client
        finally:
            restore()


@pytest.fixture()
def r_with_subkey_notifications(request):
    """
    A Redis client with keyspace and subkey notifications enabled.

    Uses notify-keyspace-events=KEASTIV, which enables subkey notification
    flags (S, T, I, V). These flags are only available in Redis >= 8.7.2;
    tests using this fixture must be guarded accordingly.

    Works for both standalone Redis and RedisCluster.
    """
    with _get_client(redis.Redis, request) as client:
        restore = _enable_keyspace_notifications(client, flags="KEASTIV")
        try:
            yield client
        finally:
            restore()


@pytest.fixture()
def stack_url(request):
    return request.config.getoption("--redis-mod-url", default=default_redismod_url)


@pytest.fixture()
def stack_r(request, stack_url):
    with _get_client(redis.Redis, request, from_url=stack_url) as client:
        yield client


@pytest.fixture()
def decoded_r(request):
    with _get_client(redis.Redis, request, decode_responses=True) as client:
        yield client


@pytest.fixture()
def r_timeout(request):
    with _get_client(redis.Redis, request, socket_timeout=1) as client:
        yield client


@pytest.fixture()
def r2(request):
    "A second client for tests that need multiple"
    with _get_client(redis.Redis, request) as client:
        yield client


@pytest.fixture()
def sslclient(request):
    with _get_client(redis.Redis, request, ssl=True) as client:
        yield client


@pytest.fixture()
def sentinel_setup(request):
    sentinel_ips = request.config.getoption("--sentinels")
    sentinel_endpoints = [
        (ip.strip(), int(port.strip()))
        for ip, port in (endpoint.split(":") for endpoint in sentinel_ips.split(","))
    ]
    kwargs = request.param.get("kwargs", {}) if hasattr(request, "param") else {}
    cache = request.param.get("cache", None)
    cache_config = request.param.get("cache_config", None)
    force_master_ip = request.param.get("force_master_ip", None)
    decode_responses = request.param.get("decode_responses", False)
    sentinel = Sentinel(
        sentinel_endpoints,
        force_master_ip=force_master_ip,
        socket_timeout=0.1,
        cache=cache,
        cache_config=cache_config,
        protocol=3,
        decode_responses=decode_responses,
        **kwargs,
    )
    yield sentinel
    for s in sentinel.sentinels:
        s.close()


@pytest.fixture()
def master(request, sentinel_setup):
    master_service = request.config.getoption("--master-service")
    master = sentinel_setup.master_for(master_service)
    yield master
    master.close()


def _gen_cluster_mock_resp(r, response):
    connection = Mock(spec=Connection)
    connection.retry = Retry(NoBackoff(), 0)
    connection.read_response.return_value = response
    connection.host = "localhost"
    connection.port = 6379
    connection.db = 0
    with mock.patch.object(r, "connection", connection):
        yield r


@pytest.fixture()
def mock_cluster_resp_ok(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    yield from _gen_cluster_mock_resp(r, "OK")


@pytest.fixture()
def mock_cluster_resp_int(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    yield from _gen_cluster_mock_resp(r, 2)


@pytest.fixture()
def mock_cluster_resp_info(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    response = (
        "cluster_state:ok\r\ncluster_slots_assigned:16384\r\n"
        "cluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\n"
        "cluster_slots_fail:0\r\ncluster_known_nodes:7\r\n"
        "cluster_size:3\r\ncluster_current_epoch:7\r\n"
        "cluster_my_epoch:2\r\ncluster_stats_messages_sent:170262\r\n"
        "cluster_stats_messages_received:105653\r\n"
    )
    yield from _gen_cluster_mock_resp(r, response)


@pytest.fixture()
def mock_cluster_resp_nodes(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
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
    yield from _gen_cluster_mock_resp(r, response)


@pytest.fixture()
def mock_cluster_resp_slaves(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    response = (
        "['1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
        "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
        "1447836789290 3 connected']"
    )
    yield from _gen_cluster_mock_resp(r, response)


@pytest.fixture(scope="session")
def master_host(request):
    url = request.config.getoption("--redis-url")
    parts = urlparse(url)
    return parts.hostname, (parts.port or 6379)


@pytest.fixture()
def cache_conf() -> CacheConfig:
    return CacheConfig(max_size=100, eviction_policy=EvictionPolicy.LRU)


@pytest.fixture()
def mock_cache_factory() -> CacheFactoryInterface:
    mock_factory = Mock(spec=CacheFactoryInterface)
    return mock_factory


@pytest.fixture()
def mock_cache() -> CacheInterface:
    mock_cache = Mock(spec=CacheInterface)
    return mock_cache


@pytest.fixture()
def mock_connection() -> ConnectionInterface:
    mock_connection = Mock(spec=ConnectionInterface)
    # Add host and port attributes needed by find_connection_owner
    mock_connection.host = "127.0.0.1"
    mock_connection.port = 6379
    return mock_connection


@pytest.fixture()
def mock_ed() -> EventDispatcherInterface:
    mock_ed = Mock(spec=EventDispatcherInterface)
    return mock_ed


@pytest.fixture()
def cache_key(request) -> CacheKey:
    command = request.param.get("command")
    keys = request.param.get("redis_keys")

    return CacheKey(command, keys)


def mock_identity_provider() -> IdentityProviderInterface:
    jwt = pytest.importorskip("jwt")
    mock_provider = Mock(spec=IdentityProviderInterface)
    token = {"exp": datetime.now(timezone.utc).timestamp() + 3600, "oid": "username"}
    encoded = jwt.encode(token, "secret", algorithm="HS256")
    jwt_token = JWToken(encoded)
    mock_provider.request_token.return_value = jwt_token
    return mock_provider


def get_credential_provider(request) -> CredentialProvider:
    cred_provider_class = request.param.get("cred_provider_class")
    cred_provider_kwargs = request.param.get("cred_provider_kwargs", {})

    # Since we can't import EntraIdCredentialsProvider in this module,
    # we'll just check the class name.
    if cred_provider_class.__name__ != "EntraIdCredentialsProvider":
        return cred_provider_class(**cred_provider_kwargs)

    from tests.entraid_utils import get_entra_id_credentials_provider

    return get_entra_id_credentials_provider(request, cred_provider_kwargs)


@pytest.fixture()
def credential_provider(request) -> CredentialProvider:
    return get_credential_provider(request)


def get_endpoint(endpoint_name: str):
    endpoints_config = os.getenv("REDIS_ENDPOINTS_CONFIG_PATH", None)

    if not (endpoints_config and os.path.exists(endpoints_config)):
        raise FileNotFoundError(f"Endpoints config file not found: {endpoints_config}")

    try:
        with open(endpoints_config, "r") as f:
            data = json.load(f)
            db = data[endpoint_name]
            return db["endpoints"][0]
    except Exception as e:
        raise ValueError(
            f"Failed to load endpoints config file: {endpoints_config}"
        ) from e


def wait_for_command(client, monitor, command, key=None):
    # issue a command with a key name that's local to this process.
    # if we find a command with our key before the command we're waiting
    # for, something went wrong
    if key is None:
        # generate key
        redis_version = REDIS_INFO["version"]
        if Version(redis_version) >= Version("5.0.0"):
            id_str = str(client.client_id())
        else:
            id_str = f"{random.randrange(2**32):08x}"
        key = f"__REDIS-PY-{id_str}__"
    client.get(key)
    while True:
        monitor_response = monitor.next_command()
        if command in monitor_response["command"]:
            return monitor_response
        if key in monitor_response["command"]:
            return None


def is_resp2_connection(r):
    return get_protocol_version(r) in ["2", 2]


def _get_raw_protocol(r):
    """Return the raw ``protocol`` from ``connection_kwargs`` (no default).

    Returns ``None`` when the caller did not pin a protocol value, which
    is distinct from the wire version the connection eventually
    negotiates (see :func:`get_protocol_version`).
    """
    if isinstance(r, redis.Redis) or isinstance(r, redis.asyncio.Redis):
        return r.connection_pool.connection_kwargs.get("protocol")
    elif isinstance(r, redis.cluster.AbstractRedisCluster):
        return r.nodes_manager.connection_kwargs.get("protocol")
    return None


def get_protocol_version(r):
    protocol = _get_raw_protocol(r)
    # ``protocol is None`` means the caller did not specify one, so the
    # connection negotiates the library default on the wire.
    if protocol is None:
        return DEFAULT_RESP_VERSION
    return protocol


def get_legacy_responses(r):
    """Return the ``legacy_responses`` setting on the client.

    Defaults to ``True`` (matching the client constructor) when absent
    from ``connection_kwargs``.
    """
    if isinstance(r, redis.Redis) or isinstance(r, redis.asyncio.Redis):
        return r.connection_pool.connection_kwargs.get("legacy_responses", True)
    elif isinstance(r, redis.cluster.AbstractRedisCluster):
        return r.nodes_manager.connection_kwargs.get("legacy_responses", True)
    return True


def expected_response_shape(r):
    """Return the expected Python response shape for ``r``.

    One of:

    * ``"legacy_resp2"`` — native RESP2 wire, or RESP3 wire normalised by
      the legacy adapters back to RESP2 Python shapes.
    * ``"legacy_resp3"`` — Python shapes from the previous RESP3 callbacks,
      produced when the caller pinned ``protocol=3`` with
      ``legacy_responses=True``.
    * ``"unified"`` — protocol-agnostic unified Python shapes selected by
      ``legacy_responses=False``.
    """
    if not get_legacy_responses(r):
        return "unified"
    raw_protocol = _get_raw_protocol(r)
    if raw_protocol is None or raw_protocol in ("2", 2):
        return "legacy_resp2"
    return "legacy_resp3"


def expects_resp2_shape(r):
    """Return ``True`` when Python responses on ``r`` use RESP2 shapes."""
    return expected_response_shape(r) == "legacy_resp2"


def expects_resp3_shape(r):
    """Return ``True`` when Python responses on ``r`` use the previous RESP3
    callback shapes (``protocol=3`` with ``legacy_responses=True``)."""
    return expected_response_shape(r) == "legacy_resp3"


def expects_unified_shape(r):
    """Return ``True`` when Python responses on ``r`` use the unified shape
    (``legacy_responses=False``)."""
    return expected_response_shape(r) == "unified"


_UNSET = object()


def _select_expected(r, resp2_expected, resp3_expected, unified_expected):
    match expected_response_shape(r):
        case "legacy_resp2":
            return resp2_expected
        case "legacy_resp3":
            return resp3_expected
        case "unified":
            # Fall back to ``resp3_expected`` when no dedicated
            # ``unified_expected`` is supplied, since unified shapes are
            # typically RESP3-style with only a few divergences.
            if unified_expected is _UNSET:
                return resp3_expected
            return unified_expected
        case other:
            raise ValueError(f"unknown response shape: {other!r}")


def assert_resp_response(
    r, response, resp2_expected, resp3_expected, unified_expected=_UNSET
):
    expected = _select_expected(r, resp2_expected, resp3_expected, unified_expected)
    assert response == expected


def assert_resp_response_in(
    r, response, resp2_expected, resp3_expected, unified_expected=_UNSET
):
    expected = _select_expected(r, resp2_expected, resp3_expected, unified_expected)
    assert response in expected
