from unittest.mock import patch

import pytest
from redis.cluster import CommandsParser, NodesManager, RedisCluster
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster


@pytest.mark.asyncio
@pytest.mark.parametrize("option", ["dynamic_startup_nodes", "require_full_coverage"])
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
async def test_cluster_url_boolean_options(
    client_class, scheme, value, expected, option
):
    with (
        patch.object(NodesManager, "initialize"),
        patch.object(CommandsParser, "initialize"),
    ):
        client = client_class.from_url(
            f"{scheme}://localhost:6379?{option}={value}",
            **{option: not expected},
        )
    try:
        attribute = (
            option
            if option == "require_full_coverage"
            and isinstance(client, AsyncRedisCluster)
            else f"_{option}"
        )
        assert getattr(client.nodes_manager, attribute) is expected
    finally:
        if isinstance(client, AsyncRedisCluster):
            await client.aclose()
        else:
            client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("option", ["dynamic_startup_nodes", "require_full_coverage"])
@pytest.mark.parametrize(
    "client_class", [RedisCluster, AsyncRedisCluster], ids=["sync", "async"]
)
@pytest.mark.parametrize("expected", [False, True])
async def test_cluster_url_keeps_boolean_keyword(client_class, expected, option):
    with (
        patch.object(NodesManager, "initialize"),
        patch.object(CommandsParser, "initialize"),
    ):
        client = client_class.from_url("redis://localhost:6379", **{option: expected})
    try:
        attribute = (
            option
            if option == "require_full_coverage"
            and isinstance(client, AsyncRedisCluster)
            else f"_{option}"
        )
        assert getattr(client.nodes_manager, attribute) is expected
    finally:
        if isinstance(client, AsyncRedisCluster):
            await client.aclose()
        else:
            client.close()
