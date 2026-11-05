from unittest.mock import patch

import pytest
from redis.cluster import CommandsParser, NodesManager, RedisCluster
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_class", [RedisCluster, AsyncRedisCluster], ids=["sync", "async"]
)
@pytest.mark.parametrize("scheme", ["redis", "rediss"])
@pytest.mark.parametrize(
    "value,expected",
    [
        ("False", False),
        ("false", False),
        ("0", False),
        ("no", False),
        ("True", True),
        ("true", True),
        ("1", True),
        ("yes", True),
    ],
)
async def test_cluster_url_dynamic_startup_nodes(client_class, scheme, value, expected):
    with (
        patch.object(NodesManager, "initialize"),
        patch.object(CommandsParser, "initialize"),
    ):
        client = client_class.from_url(
            f"{scheme}://localhost:6379?dynamic_startup_nodes={value}",
            dynamic_startup_nodes=not expected,
        )
    try:
        assert client.nodes_manager._dynamic_startup_nodes is expected
    finally:
        if isinstance(client, AsyncRedisCluster):
            await client.aclose()
        else:
            client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_class", [RedisCluster, AsyncRedisCluster], ids=["sync", "async"]
)
@pytest.mark.parametrize("expected", [False, True])
async def test_cluster_url_keeps_dynamic_startup_nodes_keyword(client_class, expected):
    with (
        patch.object(NodesManager, "initialize"),
        patch.object(CommandsParser, "initialize"),
    ):
        client = client_class.from_url(
            "redis://localhost:6379", dynamic_startup_nodes=expected
        )
    try:
        assert client.nodes_manager._dynamic_startup_nodes is expected
    finally:
        if isinstance(client, AsyncRedisCluster):
            await client.aclose()
        else:
            client.close()
