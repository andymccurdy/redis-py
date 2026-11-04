"""Tests for the cached default response-callback table on the async client.

``redis.asyncio.Redis.__init__`` gets its callback table from
``default_response_callbacks``, the same helper the sync client uses. The
helper itself is covered in ``tests/test_response_callbacks_cache.py``.

These tests are offline: clients are constructed with no Redis server
(``single_connection_client`` is left at its default ``False`` so no connection
is opened) and are closed in ``finally``.
"""

import pytest

import redis
import redis.asyncio
from redis.client import CaseInsensitiveDict, _cached_default_response_callbacks

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


def _make_client_kwargs(protocol, legacy_responses):
    """Build the kwargs dict, omitting ``protocol`` for the default case."""
    kwargs = {"legacy_responses": legacy_responses}
    if protocol is not None:
        kwargs["protocol"] = protocol
    return kwargs


# Sync vs async parity. Both client stacks produce equal contents
# (value-by-identity) for the same combo, across the full matrix.
@pytest.mark.fixed_client
@pytest.mark.parametrize("legacy_responses", _LEGACY_VARIANTS)
@pytest.mark.parametrize("protocol", _PROTOCOL_VARIANTS)
async def test_sync_async_parity(protocol, legacy_responses):
    sync_client = redis.Redis(**_make_client_kwargs(protocol, legacy_responses))
    async_client = redis.asyncio.Redis(
        **_make_client_kwargs(protocol, legacy_responses)
    )
    try:
        sync_rc = sync_client.response_callbacks
        async_rc = async_client.response_callbacks

        assert isinstance(async_rc, CaseInsensitiveDict)
        assert set(sync_rc.keys()) == set(async_rc.keys())
        for command, callback in sync_rc.items():
            assert async_rc[command] is callback, (
                f"sync/async callback mismatch for {command!r} "
                f"(protocol={protocol!r}, legacy_responses={legacy_responses!r})"
            )
    finally:
        sync_client.close()
        await async_client.aclose()


# Mutation isolation. Mutating one client must not affect another same-combo
# client, nor the shared cached canonical table.
@pytest.mark.fixed_client
async def test_mutation_isolation_between_clients_and_canonical():
    c1 = redis.asyncio.Redis()
    c2 = redis.asyncio.Redis()
    try:
        rc1 = c1.response_callbacks
        rc2 = c2.response_callbacks

        canonical = _cached_default_response_callbacks(None, True)
        assert rc1 is not canonical
        assert rc2 is not canonical
        canonical_len_before = len(canonical)

        def _cb(x):
            return x

        c1.set_response_callback("CACHE_TEST_CMD", _cb)

        assert rc1["cache_test_cmd"] is _cb
        assert "CACHE_TEST_CMD" not in rc2
        assert "CACHE_TEST_CMD" not in canonical
        assert len(canonical) == canonical_len_before
    finally:
        await c1.aclose()
        await c2.aclose()


# Two same-combo clients share equal-but-not-identical dicts (the whole point
# of the per-instance ``copy()``).
@pytest.mark.fixed_client
async def test_same_combo_async_clients_equal_not_identical():
    c1 = redis.asyncio.Redis(protocol=3, legacy_responses=False)
    c2 = redis.asyncio.Redis(protocol=3, legacy_responses=False)
    try:
        assert c1.response_callbacks == c2.response_callbacks
        assert c1.response_callbacks is not c2.response_callbacks
    finally:
        await c1.aclose()
        await c2.aclose()
