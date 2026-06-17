"""Tests for the cached canonical response-callback table (issue #3624).

``Redis.__init__`` and ``redis.asyncio.Redis.__init__`` no longer rebuild and
re-upper-case the ~165-entry response-callback table on every construction.
Instead the canonical ``CaseInsensitiveDict`` is memoized once per
``(protocol, legacy_responses)`` combination via ``lru_cache`` and each client
receives a cheap ``CaseInsensitiveDict.copy()``.

These tests assert the correctness invariants that the refactor must preserve.
They are offline: clients are constructed with no Redis server
(``single_connection_client`` is left at its default ``False`` so no connection
is opened) and are closed in ``finally``.
"""

import pytest

import redis
import redis.asyncio
from redis._parsers.helpers import get_response_callbacks
from redis.client import (
    CaseInsensitiveDict,
    _get_default_response_callbacks,
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


def _make_client_kwargs(protocol, legacy_responses):
    """Build the kwargs dict, omitting ``protocol`` for the default case."""
    kwargs = {"legacy_responses": legacy_responses}
    if protocol is not None:
        kwargs["protocol"] = protocol
    return kwargs


# Invariant 1: identical contents (value-by-identity) against the pre-refactor
# construction ``CaseInsensitiveDict(get_response_callbacks(...))``, across the
# full protocol x legacy matrix.
@pytest.mark.fixed_client
@pytest.mark.parametrize("legacy_responses", _LEGACY_VARIANTS)
@pytest.mark.parametrize("protocol", _PROTOCOL_VARIANTS)
def test_contents_match_old_construction_by_identity(protocol, legacy_responses):
    # Arrange: reconstruct the table the old code path produced.
    expected = CaseInsensitiveDict(
        get_response_callbacks(
            user_protocol=protocol,
            legacy_responses=legacy_responses,
        )
    )

    # Act
    client = redis.Redis(**_make_client_kwargs(protocol, legacy_responses))
    try:
        actual = client.response_callbacks

        # Assert: same keys, same number of entries, each value identical object.
        assert isinstance(actual, CaseInsensitiveDict)
        assert len(actual) == len(expected)
        assert set(actual.keys()) == set(expected.keys())
        for command, callback in expected.items():
            assert actual[command] is callback, (
                f"callback mismatch for {command!r} "
                f"(protocol={protocol!r}, legacy_responses={legacy_responses!r})"
            )
    finally:
        client.close()


# Invariant 2: case-insensitive lookups preserved on the per-instance dict,
# including newly-registered callbacks via ``set_response_callback``.
@pytest.mark.fixed_client
def test_case_insensitive_lookups_preserved():
    client = redis.Redis()
    try:
        rc = client.response_callbacks

        # Pre-existing key resolves identically regardless of case.
        assert rc["geohash"] is rc["GEOHASH"]
        assert rc["geohash"] is rc["GeOhAsH"]
        assert "geohash" in rc
        assert "GEOHASH" in rc

        # Newly registered callback is also case-insensitive.
        def _cb(x):
            return x

        client.set_response_callback("FOO", _cb)
        assert rc["FOO"] is _cb
        assert rc["foo"] is _cb
        assert "foo" in rc
    finally:
        client.close()


# Invariant 3: mutation isolation. Mutating one client must not affect another
# same-combo client, nor the shared cached canonical table.
@pytest.mark.fixed_client
def test_mutation_isolation_between_clients_and_canonical():
    c1 = redis.Redis()
    c2 = redis.Redis()
    try:
        rc1 = c1.response_callbacks
        rc2 = c2.response_callbacks

        # Same combo -> equal contents but distinct objects.
        assert rc1 == rc2
        assert rc1 is not rc2

        # The shared canonical for the same (protocol=None, legacy=True) combo.
        canonical = _get_default_response_callbacks(None, True)
        assert rc1 is not canonical
        assert rc2 is not canonical
        canonical_len_before = len(canonical)

        # Mutate c1 only.
        def _cb(x):
            return x

        c1.set_response_callback("CACHE_TEST_CMD", _cb)

        assert "CACHE_TEST_CMD" in rc1
        assert "CACHE_TEST_CMD" not in rc2
        assert "CACHE_TEST_CMD" not in canonical
        assert len(canonical) == canonical_len_before
    finally:
        c1.close()
        c2.close()


# Invariant 4: ``copy()`` returns a ``CaseInsensitiveDict`` (not a plain dict)
# and is independent of the source.
@pytest.mark.fixed_client
def test_copy_returns_caseinsensitivedict_and_is_independent():
    canonical = _get_default_response_callbacks(None, True)

    copied = canonical.copy()

    # Correct type and equal contents.
    assert isinstance(copied, CaseInsensitiveDict)
    assert type(copied) is CaseInsensitiveDict
    assert copied == canonical
    assert copied is not canonical

    # Case-insensitivity survives the copy (bypassing __init__ must still leave
    # keys upper-cased so __getitem__ resolves correctly).
    assert copied["geohash"] is copied["GEOHASH"]

    # Independence: mutating the copy does not touch the source.
    canonical_len_before = len(canonical)
    copied["EXTRA_ONLY_IN_COPY"] = lambda x: x
    assert "EXTRA_ONLY_IN_COPY" in copied
    assert "EXTRA_ONLY_IN_COPY" not in canonical
    assert len(canonical) == canonical_len_before


# Invariant 5: sync vs async parity. Both client stacks produce equal contents
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


# Invariant 6: two same-combo clients share equal-but-not-identical dicts (the
# whole point of the per-instance ``copy()``), for both sync and async stacks.
@pytest.mark.fixed_client
def test_same_combo_sync_clients_equal_not_identical():
    c1 = redis.Redis(protocol=3, legacy_responses=False)
    c2 = redis.Redis(protocol=3, legacy_responses=False)
    try:
        assert c1.response_callbacks == c2.response_callbacks
        assert c1.response_callbacks is not c2.response_callbacks
    finally:
        c1.close()
        c2.close()


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
