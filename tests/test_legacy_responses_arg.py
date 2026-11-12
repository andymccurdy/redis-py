"""Tests for the ``legacy_responses`` x ``protocol`` construction matrix.

For every combination of ``legacy_responses`` and ``protocol`` the
public client constructor must:

* propagate the user-supplied values into ``connection_pool.connection_kwargs``
  (with ``protocol`` left absent / ``None`` when the caller does not pass it);
* select the response-callback overlay that matches the resolved
  (wire, Python-shape) pair, where ``protocol=None`` resolves to RESP3.
"""

import pytest

import redis
from redis.cluster import (
    parse_cluster_shards,
    parse_cluster_shards_unified,
    parse_cluster_shards_with_str_keys,
)
from redis._parsers.response_callbacks import (
    _RedisCallbacks,
    _RedisCallbacksRESP2,
    _RedisCallbacksRESP2Unified,
    _RedisCallbacksRESP3,
    _RedisCallbacksRESP3Unified,
    _RedisCallbacksRESP3toRESP2Legacy,
)

# ``None`` represents the "default" case where the caller does not pass
# ``protocol`` at all and the client resolves the wire to RESP3.
_PROTOCOL_VARIANTS = [
    pytest.param(2, id="resp2"),
    pytest.param(3, id="resp3"),
    pytest.param(None, id="default"),
]
_LEGACY_VARIANTS = [
    pytest.param(True, id="legacy"),
    pytest.param(False, id="unified"),
]


def _expected_overlay(protocol, legacy_responses):
    """Return the callback overlay expected for the given combo."""
    if legacy_responses:
        if protocol is None:
            return _RedisCallbacksRESP3toRESP2Legacy
        if protocol in (3, "3"):
            return _RedisCallbacksRESP3
        return _RedisCallbacksRESP2
    if protocol is None or protocol in (3, "3"):
        return _RedisCallbacksRESP3Unified
    return _RedisCallbacksRESP2Unified


def _make_client_kwargs(protocol, legacy_responses):
    """Build the kwargs dict, omitting ``protocol`` for the default case."""
    kwargs = {"legacy_responses": legacy_responses}
    if protocol is not None:
        kwargs["protocol"] = protocol
    return kwargs


def _assert_callbacks_match(actual_callbacks, protocol, legacy_responses):
    expected = {**_RedisCallbacks, **_expected_overlay(protocol, legacy_responses)}
    # ``actual_callbacks`` is a ``CaseInsensitiveDict`` that uppercases keys;
    # iterate through the expected mapping and compare by identity to make
    # mismatches localised and easy to read.
    for command, callback in expected.items():
        assert actual_callbacks[command] is callback, (
            f"callback mismatch for {command!r} "
            f"(protocol={protocol!r}, legacy_responses={legacy_responses!r})"
        )


def _expected_cluster_shards_callback(protocol, legacy_responses):
    if not legacy_responses:
        return parse_cluster_shards_unified
    if protocol is None:
        return parse_cluster_shards_with_str_keys
    return parse_cluster_shards


@pytest.mark.fixed_client
@pytest.mark.parametrize("legacy_responses", _LEGACY_VARIANTS)
@pytest.mark.parametrize("protocol", _PROTOCOL_VARIANTS)
def test_sync_client_protocol_legacy_matrix(protocol, legacy_responses):
    client = redis.Redis(**_make_client_kwargs(protocol, legacy_responses))
    try:
        kwargs = client.connection_pool.connection_kwargs
        assert kwargs.get("protocol") == protocol
        assert kwargs.get("legacy_responses") == legacy_responses
        _assert_callbacks_match(client.response_callbacks, protocol, legacy_responses)
    finally:
        client.close()


@pytest.mark.fixed_client
@pytest.mark.parametrize("legacy_responses", _LEGACY_VARIANTS)
@pytest.mark.parametrize("protocol", _PROTOCOL_VARIANTS)
async def test_async_client_protocol_legacy_matrix(protocol, legacy_responses):
    client = redis.asyncio.Redis(**_make_client_kwargs(protocol, legacy_responses))
    try:
        kwargs = client.connection_pool.connection_kwargs
        assert kwargs.get("protocol") == protocol
        assert kwargs.get("legacy_responses") == legacy_responses
        _assert_callbacks_match(client.response_callbacks, protocol, legacy_responses)
    finally:
        await client.aclose()


@pytest.mark.fixed_client
@pytest.mark.parametrize("legacy_responses", _LEGACY_VARIANTS)
@pytest.mark.parametrize("protocol", _PROTOCOL_VARIANTS)
async def test_async_cluster_client_cluster_shards_callback(protocol, legacy_responses):
    client = redis.asyncio.RedisCluster(
        host="localhost",
        port=6379,
        **_make_client_kwargs(protocol, legacy_responses),
    )
    try:
        assert client.response_callbacks["CLUSTER SHARDS"] is (
            _expected_cluster_shards_callback(protocol, legacy_responses)
        )
    finally:
        await client.aclose()


_DEBUG_OBJECT_REPLY = (
    b"Value at:0x7f0000000000 refcount:1 encoding:embstr serializedlength:4 "
    b"lru:0 lru_seconds_idle:0"
)


@pytest.mark.fixed_client
@pytest.mark.parametrize("protocol", _PROTOCOL_VARIANTS)
def test_unified_status_replies_have_the_resp2_shape(protocol):
    """BGSAVE, BGREWRITEAOF and DEBUG OBJECT give the RESP2 shape when unified."""
    client = redis.Redis(**_make_client_kwargs(protocol, False))
    callbacks = client.response_callbacks
    assert callbacks["BGSAVE"](b"Background saving started") is True
    assert (
        callbacks["BGREWRITEAOF"](b"Background append only file rewriting started")
        is True
    )
    assert callbacks["DEBUG OBJECT"](_DEBUG_OBJECT_REPLY) == {
        "type": "Value",
        "at": "0x7f0000000000",
        "refcount": 1,
        "encoding": "embstr",
        "serializedlength": 4,
        "lru": 0,
        "lru_seconds_idle": 0,
    }
