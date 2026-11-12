import inspect
import socket

import pytest
from redis._defaults import (
    DEFAULT_SOCKET_CONNECT_TIMEOUT,
    DEFAULT_SOCKET_READ_SIZE,
    DEFAULT_SOCKET_TIMEOUT,
    get_default_socket_keepalive_options,
)
from redis.asyncio.client import Redis as AsyncRedis
from redis.asyncio.cluster import ClusterNode as AsyncClusterNode
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.asyncio.connection import AbstractConnection as AsyncAbstractConnection
from redis.asyncio.connection import Connection as AsyncConnection
from redis.asyncio.connection import ConnectionPool as AsyncConnectionPool
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import ExponentialWithJitterBackoff
from redis.client import Redis
from redis.cluster import REDIS_ALLOWED_KEYS, RedisCluster
from redis.connection import AbstractConnection
from redis.connection import Connection
from redis.connection import ConnectionPool
from redis.connection import UnixDomainSocketConnection
from redis.retry import Retry
from redis.utils import SENTINEL


@pytest.mark.fixed_client
def test_socket_keepalive_signature_defaults_are_true():
    classes = (Redis, Connection, AsyncRedis, AsyncConnection, AsyncRedisCluster)

    for cls in classes:
        default = inspect.signature(cls.__init__).parameters["socket_keepalive"].default
        assert default is True


@pytest.mark.fixed_client
def test_connection_socket_keepalive_defaults_to_true():
    assert Connection().socket_keepalive is True
    assert AsyncConnection().socket_keepalive is True


@pytest.mark.fixed_client
def test_connection_socket_read_size_defaults_to_32kb():
    classes = (
        Redis,
        AbstractConnection,
        AsyncRedis,
        AsyncRedisCluster,
        AsyncAbstractConnection,
    )

    for cls in classes:
        default = inspect.signature(cls.__init__).parameters["socket_read_size"].default
        assert default == DEFAULT_SOCKET_READ_SIZE

    assert Connection()._socket_read_size == DEFAULT_SOCKET_READ_SIZE
    assert AsyncConnection()._socket_read_size == DEFAULT_SOCKET_READ_SIZE
    assert "socket_read_size" in REDIS_ALLOWED_KEYS


@pytest.mark.fixed_client
def test_socket_read_size_can_be_configured_from_clients_and_pools():
    socket_read_size = 65536

    assert (
        Redis(socket_read_size=socket_read_size).connection_pool.connection_kwargs[
            "socket_read_size"
        ]
        == socket_read_size
    )
    assert (
        AsyncRedis(socket_read_size=socket_read_size).connection_pool.connection_kwargs[
            "socket_read_size"
        ]
        == socket_read_size
    )
    assert (
        ConnectionPool(socket_read_size=socket_read_size).connection_kwargs[
            "socket_read_size"
        ]
        == socket_read_size
    )
    assert (
        AsyncConnectionPool(socket_read_size=socket_read_size).connection_kwargs[
            "socket_read_size"
        ]
        == socket_read_size
    )
    assert (
        ConnectionPool.from_url(
            f"redis://localhost?socket_read_size={socket_read_size}"
        ).connection_kwargs["socket_read_size"]
        == socket_read_size
    )
    assert (
        AsyncConnectionPool.from_url(
            f"redis://localhost?socket_read_size={socket_read_size}"
        ).connection_kwargs["socket_read_size"]
        == socket_read_size
    )


@pytest.mark.fixed_client
def test_socket_timeout_default():
    classes = (Redis, AbstractConnection, AsyncRedis, AsyncAbstractConnection)

    for cls in classes:
        parameters = inspect.signature(cls.__init__).parameters
        assert parameters["socket_timeout"].default == DEFAULT_SOCKET_TIMEOUT
        assert (
            parameters["socket_connect_timeout"].default
            == DEFAULT_SOCKET_CONNECT_TIMEOUT
        )

    cluster_parameters = inspect.signature(AsyncRedisCluster.__init__).parameters
    assert cluster_parameters["socket_timeout"].default == DEFAULT_SOCKET_TIMEOUT
    assert (
        cluster_parameters["socket_connect_timeout"].default
        == DEFAULT_SOCKET_CONNECT_TIMEOUT
    )

    assert Connection().socket_timeout == DEFAULT_SOCKET_TIMEOUT
    assert Connection().socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
    assert AsyncConnection().socket_timeout == DEFAULT_SOCKET_TIMEOUT
    assert AsyncConnection().socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
    assert UnixDomainSocketConnection().socket_timeout == DEFAULT_SOCKET_TIMEOUT


@pytest.mark.fixed_client
def test_default_max_connections():
    assert ConnectionPool().max_connections == 100
    assert AsyncConnectionPool().max_connections == 100
    assert Redis().connection_pool.max_connections == 100
    assert AsyncRedis().connection_pool.max_connections == 100

    async_cluster_parameters = inspect.signature(AsyncRedisCluster.__init__).parameters
    assert async_cluster_parameters["max_connections"].default == 100
    async_cluster_node_parameters = inspect.signature(
        AsyncClusterNode.__init__
    ).parameters
    assert async_cluster_node_parameters["max_connections"].default == 100


@pytest.mark.fixed_client
def test_default_retry_config():
    sync_retry = inspect.signature(Redis.__init__).parameters["retry"].default
    assert isinstance(sync_retry, Retry)
    assert sync_retry.get_retries() == 10
    assert isinstance(sync_retry._backoff, ExponentialWithJitterBackoff)
    assert sync_retry._backoff._base == 0.01
    assert sync_retry._backoff._cap == 1

    async_retry = inspect.signature(AsyncRedis.__init__).parameters["retry"].default
    assert isinstance(async_retry, AsyncRetry)
    assert async_retry.get_retries() == 10
    assert isinstance(async_retry._backoff, ExponentialWithJitterBackoff)
    assert async_retry._backoff._base == 0.01
    assert async_retry._backoff._cap == 1

    for cls in (RedisCluster, AsyncRedisCluster):
        default = (
            inspect.signature(cls.__init__)
            .parameters["cluster_error_retry_attempts"]
            .default
        )
        assert default == 10


@pytest.mark.fixed_client
def test_default_socket_keepalive_options():
    options = get_default_socket_keepalive_options()

    tcp_keepidle = getattr(socket, "TCP_KEEPIDLE", None)
    if tcp_keepidle is None:
        tcp_keepidle = getattr(socket, "TCP_KEEPALIVE", None)
    if tcp_keepidle is not None:
        assert options[tcp_keepidle] == 30

    tcp_keepintvl = getattr(socket, "TCP_KEEPINTVL", None)
    if tcp_keepintvl is not None:
        assert options[tcp_keepintvl] == 5

    tcp_keepcnt = getattr(socket, "TCP_KEEPCNT", None)
    if tcp_keepcnt is not None:
        assert options[tcp_keepcnt] == 3


@pytest.mark.fixed_client
def test_socket_keepalive_options_signature_defaults_use_sentinel():
    sync_classes = (Redis, Connection)

    for cls in sync_classes:
        default = (
            inspect.signature(cls.__init__)
            .parameters["socket_keepalive_options"]
            .default
        )
        assert default is SENTINEL

    async_classes = (AsyncRedis, AsyncConnection, AsyncRedisCluster)

    for cls in async_classes:
        default = (
            inspect.signature(cls.__init__)
            .parameters["socket_keepalive_options"]
            .default
        )
        assert default is SENTINEL


@pytest.mark.fixed_client
def test_connection_socket_keepalive_options_resolve_sentinel_default():
    assert (
        Connection().socket_keepalive_options == get_default_socket_keepalive_options()
    )
    assert (
        AsyncConnection().socket_keepalive_options
        == get_default_socket_keepalive_options()
    )


@pytest.mark.fixed_client
def test_connection_socket_keepalive_options_default_is_not_shared():
    assert (
        Connection().socket_keepalive_options
        is not Connection().socket_keepalive_options
    )
    assert (
        AsyncConnection().socket_keepalive_options
        is not AsyncConnection().socket_keepalive_options
    )


@pytest.mark.fixed_client
def test_connection_socket_keepalive_options_none_stays_empty():
    assert Connection(socket_keepalive_options=None).socket_keepalive_options == {}
    assert AsyncConnection(socket_keepalive_options=None).socket_keepalive_options == {}
