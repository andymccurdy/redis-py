import asyncio
import inspect
import threading
from unittest import mock

import pytest

import redis.asyncio.cluster as async_cluster_mod
import redis.cluster as cluster_mod
from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.connection import ConnectionPool as AsyncConnectionPool
from redis.connection import BlockingConnectionPool, ConnectionPool
from redis._parsers.base import BaseParser
from redis.exceptions import DataError, NoSuchFieldsetError, ResponseError
from redis.himport import (
    HIMPORT_SET,
    HImportFieldset,
    HImportRegistry,
    is_himport_set_command,
    parse_himport_set_args,
)
from redis.sentinel import Sentinel


@pytest.mark.fixed_client
class TestHImportRegistry:
    # -- construction -----------------------------------------------------

    def test_empty_by_default(self):
        registry = HImportRegistry()
        assert len(registry) == 0
        assert registry.names() == []
        assert registry.items() == []
        assert list(registry) == []
        assert registry.get("missing") is None
        assert "missing" not in registry

    def test_prepared_entry_exposes_name_fields_version(self):
        registry = HImportRegistry()
        registry.prepare("shared", ["name", "email", "age"])
        fs = registry.get("shared")
        assert isinstance(fs, HImportFieldset)
        assert fs.name == "shared"
        assert fs.fields == ("name", "email", "age")
        assert isinstance(fs.version, int)
        assert "shared" in registry
        assert registry.names() == ["shared"]

    # -- field-order fidelity --------------------------------------------

    def test_field_order_preserved(self):
        registry = HImportRegistry()
        registry.prepare("fs", ["c", "a", "b"])
        assert registry.get("fs").fields == ("c", "a", "b")

    def test_duplicates_not_deduplicated(self):
        # The server rejects duplicate field names; the client must not silently
        # deduplicate and hide that error.
        registry = HImportRegistry()
        registry.prepare("fs", ["a", "a", "b"])
        assert registry.get("fs").fields == ("a", "a", "b")

    def test_accepts_various_iterables(self):
        registry = HImportRegistry()
        registry.prepare("tuple", ("a", "b"))
        registry.prepare("gen", (f"f{i}" for i in range(3)))
        assert registry.get("tuple").fields == ("a", "b")
        assert registry.get("gen").fields == ("f0", "f1", "f2")

    def test_prepare_with_registry_reentrant_generator_does_not_deadlock(self):
        # The field iterable is materialized *before* the mutation lock, so a
        # generator that consults the same registry mid-iteration (yield, then a
        # locked read) must not deadlock on the non-reentrant lock. Run under a
        # join timeout so a regression fails deterministically instead of hanging.
        registry = HImportRegistry()
        registry.prepare("existing", ["a"])
        seen = []

        def fields():
            yield "f1"
            seen.extend(registry.names())  # locked read while being consumed
            yield "f2"

        done = threading.Event()

        def run():
            registry.prepare("gen", fields())
            done.set()

        thread = threading.Thread(target=run)
        thread.start()
        thread.join(timeout=5)
        assert done.is_set(), (
            "prepare() deadlocked materializing a registry-re-entrant generator"
        )
        assert registry.get("gen").fields == ("f1", "f2")
        assert "existing" in seen

    def test_empty_string_names_and_fields_allowed(self):
        # Empty strings are valid fieldset names and field names per the HLD;
        # the client must not reject them locally.
        registry = HImportRegistry()
        registry.prepare("", [""])
        assert registry.get("").fields == ("",)
        registry.prepare("fs", ["", "x", ""])
        assert registry.get("fs").fields == ("", "x", "")

    # -- validation -------------------------------------------------------

    def test_empty_field_list_rejected(self):
        registry = HImportRegistry()
        with pytest.raises(DataError):
            registry.prepare("fs", [])

    @pytest.mark.parametrize(
        "bad", ["name", b"name", bytearray(b"name"), memoryview(b"name")]
    )
    def test_string_fields_rejected(self, bad):
        # A bare single field name (str/bytes/bytearray/memoryview) would iterate
        # element-by-element; reject it rather than silently register
        # single-character/single-byte fields.
        registry = HImportRegistry()
        with pytest.raises(DataError):
            registry.prepare("fs", bad)

    # -- versioning -------------------------------------------------------

    def test_versions_are_monotonic(self):
        registry = HImportRegistry()
        v1 = registry.prepare("a", ["x"]).version
        v2 = registry.prepare("b", ["y"]).version
        assert v2 > v1

    def test_replace_bumps_version_and_updates_fields(self):
        registry = HImportRegistry()
        first = registry.prepare("fs", ["a"])
        second = registry.prepare("fs", ["a", "b"])
        assert second.version > first.version
        assert registry.get("fs").fields == ("a", "b")
        assert len(registry) == 1

    def test_readd_after_discard_gets_fresh_version(self):
        registry = HImportRegistry()
        first = registry.prepare("fs", ["a"])
        assert registry.discard("fs") is True
        reAdded = registry.prepare("fs", ["a"])
        assert reAdded.version > first.version

    # -- revision (mutation clock) ---------------------------------------

    def test_revision_starts_at_zero(self):
        assert HImportRegistry().revision == 0

    def test_revision_advances_on_prepare(self):
        registry = HImportRegistry()
        before = registry.revision
        registry.prepare("fs", ["a"])
        assert registry.revision > before

    def test_revision_advances_on_discard(self):
        registry = HImportRegistry()
        registry.prepare("fs", ["a"])
        before = registry.revision
        registry.discard("fs")
        assert registry.revision > before

    def test_revision_advances_on_discard_all(self):
        registry = HImportRegistry()
        registry.prepare("a", ["x"])
        registry.prepare("b", ["y"])
        before = registry.revision
        registry.discard_all()
        assert registry.revision > before

    def test_revision_unchanged_on_noop_discard(self):
        registry = HImportRegistry()
        before = registry.revision
        assert registry.discard("missing") is False
        assert registry.revision == before

    def test_revision_unchanged_on_empty_discard_all(self):
        registry = HImportRegistry()
        before = registry.revision
        assert registry.discard_all() == 0
        assert registry.revision == before

    # -- names_to_discard -------------------------------------------------

    def test_names_to_discard_returns_removed_names(self):
        registry = HImportRegistry()
        registry.prepare("a", ["x"])
        registry.prepare("b", ["y"])
        registry.discard("a")
        # Connection had prepared "a" and "b"; only "a" is now gone.
        assert registry.names_to_discard(["a", "b"]) == ["a"]

    def test_names_to_discard_empty_when_all_registered(self):
        registry = HImportRegistry()
        registry.prepare("a", ["x"])
        registry.prepare("b", ["y"])
        assert registry.names_to_discard(["a", "b"]) == []

    def test_names_to_discard_preserves_input_order(self):
        registry = HImportRegistry()
        assert registry.names_to_discard(["c", "a", "b"]) == ["c", "a", "b"]

    def test_names_to_discard_ignores_readd(self):
        # A name discarded then re-declared is still registered, so it is not
        # flagged for discard (the version bump handles re-prepare instead).
        registry = HImportRegistry()
        registry.prepare("fs", ["a"])
        registry.discard("fs")
        registry.prepare("fs", ["a", "b"])
        assert registry.names_to_discard(["fs"]) == []

    # -- discard ----------------------------------------------------------

    def test_discard_removes_and_returns_true(self):
        registry = HImportRegistry()
        registry.prepare("fs", ["a"])
        assert registry.discard("fs") is True
        assert "fs" not in registry
        assert len(registry) == 0

    def test_discard_unknown_returns_false(self):
        registry = HImportRegistry()
        assert registry.discard("missing") is False

    def test_discard_never_raises_and_removes_any_fieldset(self):
        # There is no init/protected concept anymore: any prepared fieldset is
        # freely discardable and discard never raises.
        registry = HImportRegistry()
        registry.prepare("fs", ["a"])
        assert registry.discard("fs") is True
        assert "fs" not in registry

    # -- discard_all ------------------------------------------------------

    def test_discard_all_removes_all_and_returns_count(self):
        registry = HImportRegistry()
        registry.prepare("a", ["x"])
        registry.prepare("b", ["y"])
        assert registry.discard_all() == 2
        assert len(registry) == 0

    def test_discard_all_on_empty_returns_zero(self):
        assert HImportRegistry().discard_all() == 0

    def test_discard_all_never_raises_and_clears_everything(self):
        # discard_all removes every fieldset and returns the count with no
        # DataError, even for fieldsets that would formerly have been "init".
        registry = HImportRegistry()
        registry.prepare("a", ["x"])
        registry.prepare("b", ["y"])
        assert registry.discard_all() == 2
        assert len(registry) == 0
        assert "a" not in registry
        assert "b" not in registry

    # -- read-only access / immutability ----------------------------------

    def test_fieldset_is_immutable(self):
        registry = HImportRegistry()
        registry.prepare("fs", ["a"])
        fs = registry.get("fs")
        with pytest.raises(Exception):
            fs.fields = ("b",)

    def test_items_snapshot_does_not_mutate_registry(self):
        registry = HImportRegistry()
        registry.prepare("fs", ["a"])
        items = registry.items()
        items.clear()
        assert "fs" in registry

    def test_repr_lists_entries(self):
        registry = HImportRegistry()
        registry.prepare("fs", ["a", "b"])
        text = repr(registry)
        assert "fs" in text

    def test_concurrent_prepares_do_not_lose_revision_bumps(self):
        # A sync Redis instance (and its shared HImportRegistry) is routinely used
        # across threads. Each prepare of a distinct name must advance the revision
        # exactly once; the mutation lock keeps the read-modify-write from racing and
        # collapsing bumps, which would let a connection skip a reconcile.
        registry = HImportRegistry()
        count = 200
        barrier = threading.Barrier(count)

        def worker(i):
            barrier.wait()  # maximize contention on the mutation path
            registry.prepare(f"fs{i}", ["a"])

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(registry) == count
        assert registry.revision == count  # no bump lost to a race

    def test_concurrent_reads_and_mutations_never_raise(self):
        # A shared sync registry may be iterated/snapshotted by one thread while
        # another prepares/discards. The iterating reads take the mutation lock, so
        # none of them may observe a mid-mutation resize (which would surface as
        # "dictionary changed size during iteration") or a torn view.
        registry = HImportRegistry()
        # Seed some runtime entries so discards have something to remove from iteration 0.
        for i in range(20):
            registry.prepare(f"seed{i}", ["a"])

        iterations = 500
        errors = []
        n_readers, n_mutators = 4, 4
        barrier = threading.Barrier(n_readers + n_mutators)

        def reader():
            barrier.wait()
            try:
                for _ in range(iterations):
                    list(registry)
                    registry.items()
                    registry.names()
                    repr(registry)
                    registry.names_to_discard([f"seed{j}" for j in range(20)])
            except Exception as exc:  # pragma: no cover - only on regression
                errors.append(exc)

        def mutator(worker):
            barrier.wait()
            try:
                for i in range(iterations):
                    name = f"w{worker}-{i}"
                    registry.prepare(name, ["a", "b"])
                    registry.discard(name)
                    if i % 50 == 0:
                        registry.discard_all()
            except Exception as exc:  # pragma: no cover - only on regression
                errors.append(exc)

        threads = [threading.Thread(target=reader) for _ in range(n_readers)]
        threads += [
            threading.Thread(target=mutator, args=(w,)) for w in range(n_mutators)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []


class _RecordingConn:
    """Minimal stand-in for a connection carrying HIMPORT state.

    ``pack_commands`` / ``send_packed_command`` are no-ops (the client's
    ``parse_response`` is mocked to supply the replies), so the batched
    DISCARD/PREPARE read loops can be exercised without a real socket.
    """

    def __init__(self, registry, prepared=None):
        self.himport_registry = registry
        self._himport_prepared = dict(prepared or {})
        self._himport_reconciled_revision = 0

    def pack_commands(self, commands):
        return list(commands)

    def send_packed_command(self, packed, **kwargs):
        pass


class _AsyncRecordingConn(_RecordingConn):
    async def send_packed_command(self, packed, **kwargs):
        pass


class _CapturingConn(_RecordingConn):
    """Recording connection that captures the wire writes in order.

    ``writes`` records ``("packed", [cmd, ...])`` for each ``send_packed_command``
    and ``("cmd", (args...))`` for each ``send_command`` so tests can assert the
    exact command ordering (e.g. that ASKING is packed immediately before the SET).
    """

    def __init__(self, registry, prepared=None):
        super().__init__(registry, prepared)
        self.writes = []

    def send_packed_command(self, packed, **kwargs):
        self.writes.append(("packed", list(packed)))

    def send_command(self, *args, **kwargs):
        self.writes.append(("cmd", args))


class _AsyncCapturingConn(_CapturingConn):
    async def send_packed_command(self, packed, **kwargs):
        self.writes.append(("packed", list(packed)))

    async def send_command(self, *args, **kwargs):
        self.writes.append(("cmd", args))


@pytest.mark.fixed_client
class TestHImportBatchedReadDrain:
    """A per-command ResponseError inside a packed DISCARD/PREPARE batch must not
    leave replies unread — that would desync the pooled connection. Each loop drains
    every reply, marks only the ones that succeeded, then surfaces the first error.
    """

    def test_reconcile_discards_drains_all_replies_on_error(self):
        # prepare+discard advances the revision so the connection reconciles; it has
        # three stale prepared fieldsets, so three DISCARD replies must be read even
        # though the second one errors.
        registry = HImportRegistry()
        registry.prepare("a", ["f"])
        registry.discard("a")
        conn = _RecordingConn(registry, {"a": 1, "b": 1, "c": 1})
        client = Redis()
        replies = [True, ResponseError("boom"), True]
        with mock.patch.object(client, "parse_response", side_effect=replies) as pr:
            with pytest.raises(ResponseError, match="boom"):
                client._himport_reconcile_discards(conn)
        assert pr.call_count == 3  # every packed DISCARD reply drained
        assert conn._himport_prepared == {}  # every stale name dropped regardless

    def test_prepare_pipeline_drains_all_replies_on_error(self):
        registry = HImportRegistry()
        registry.prepare("a", ["f"])
        registry.prepare("b", ["f"])
        conn = _RecordingConn(registry)
        conn._himport_reconciled_revision = registry.revision  # no discard to reconcile
        commands = [
            (("HIMPORT SET", "k1", "a", "v"), {}),
            (("HIMPORT SET", "k2", "b", "v"), {}),
        ]
        assert commands[0][0][0] == HIMPORT_SET  # guards the wire-token assumption
        pipe = Redis().pipeline()
        replies = [True, ResponseError("boom")]
        with mock.patch.object(pipe, "parse_response", side_effect=replies) as pr:
            with pytest.raises(ResponseError, match="boom"):
                pipe._himport_prepare_pipeline(conn, commands)
        assert pr.call_count == 2  # both PREPARE replies drained
        assert "a" in conn._himport_prepared  # first PREPARE succeeded, marked
        assert "b" not in conn._himport_prepared  # second errored, not marked

    def test_async_reconcile_discards_drains_all_replies_on_error(self):
        async def run():
            registry = HImportRegistry()
            registry.prepare("a", ["f"])
            registry.discard("a")
            conn = _AsyncRecordingConn(registry, {"a": 1, "b": 1, "c": 1})
            client = AsyncRedis()
            client.parse_response = mock.AsyncMock(
                side_effect=[True, ResponseError("boom"), True]
            )
            with pytest.raises(ResponseError, match="boom"):
                await client._himport_reconcile_discards(conn)
            assert client.parse_response.call_count == 3
            assert conn._himport_prepared == {}

        asyncio.run(run())

    def test_async_prepare_pipeline_drains_all_replies_on_error(self):
        async def run():
            registry = HImportRegistry()
            registry.prepare("a", ["f"])
            registry.prepare("b", ["f"])
            conn = _AsyncRecordingConn(registry)
            conn._himport_reconciled_revision = registry.revision
            commands = [
                (("HIMPORT SET", "k1", "a", "v"), {}),
                (("HIMPORT SET", "k2", "b", "v"), {}),
            ]
            pipe = AsyncRedis().pipeline()
            pipe.parse_response = mock.AsyncMock(
                side_effect=[True, ResponseError("boom")]
            )
            with pytest.raises(ResponseError, match="boom"):
                await pipe._himport_prepare_pipeline(conn, commands)
            assert pipe.parse_response.call_count == 2
            assert "a" in conn._himport_prepared
            assert "b" not in conn._himport_prepared

        asyncio.run(run())


DISCARD_B = ("HIMPORT", "DISCARD", "b")


@pytest.mark.fixed_client
class TestHImportReconcileRevisionStamp:
    """Reconcile must stamp the connection with the revision it actually caught up
    to, not the live registry revision read after the DISCARD I/O completes.

    A ``himport_discard`` that lands while reconcile is draining replies (a
    thread-shared sync client, or another task while an async reconcile awaits)
    advances the registry revision. If reconcile then re-read that live revision to
    stamp the connection, the connection would be marked reconciled past a discard
    it never sent — leaving the just-removed fieldset prepared on the server session
    and permanently short-circuiting future reconciles. It must stamp the revision
    snapshotted *before* ``stale`` was computed, so the next reconcile re-runs and
    drops the fieldset.
    """

    def test_sync_reconcile_does_not_overstamp_on_concurrent_discard(self):
        registry = HImportRegistry()
        va = registry.prepare("a", ["f"]).version
        vb = registry.prepare("b", ["f"]).version
        registry.discard("a")  # "a" is stale; "b" still registered
        conn = _CapturingConn(registry, {"a": va, "b": vb})
        rev_after_a = registry.revision
        client = Redis()

        def drain(*args, **kwargs):
            # A concurrent himport_discard lands while the DISCARD-"a" reply is read.
            registry.discard("b")
            return True

        with mock.patch.object(client, "parse_response", side_effect=drain):
            client._himport_reconcile_discards(conn)

        # "a" was discarded and dropped from the connection's prepared set.
        assert conn._himport_prepared == {"b": vb}
        # The connection is stamped to the pre-drain snapshot, not the post-discard
        # revision, so it is not falsely considered up to date for "b".
        assert conn._himport_reconciled_revision == rev_after_a
        assert conn._himport_reconciled_revision != registry.revision

        # A second reconcile now sees "b" as stale and DISCARDs it on the connection.
        conn.writes.clear()
        with mock.patch.object(client, "parse_response", return_value=True):
            client._himport_reconcile_discards(conn)
        assert conn._himport_prepared == {}
        assert conn.writes == [("packed", [DISCARD_B])]
        assert conn._himport_reconciled_revision == registry.revision

    def test_async_reconcile_does_not_overstamp_on_concurrent_discard(self):
        async def run():
            registry = HImportRegistry()
            va = registry.prepare("a", ["f"]).version
            vb = registry.prepare("b", ["f"]).version
            registry.discard("a")
            conn = _AsyncCapturingConn(registry, {"a": va, "b": vb})
            rev_after_a = registry.revision
            client = AsyncRedis()

            def drain(*args, **kwargs):
                registry.discard("b")
                return True

            client.parse_response = mock.AsyncMock(side_effect=drain)
            await client._himport_reconcile_discards(conn)
            assert conn._himport_prepared == {"b": vb}
            assert conn._himport_reconciled_revision == rev_after_a
            assert conn._himport_reconciled_revision != registry.revision

            conn.writes.clear()
            client.parse_response = mock.AsyncMock(return_value=True)
            await client._himport_reconcile_discards(conn)
            assert conn._himport_prepared == {}
            assert conn.writes == [("packed", [DISCARD_B])]
            assert conn._himport_reconciled_revision == registry.revision

        asyncio.run(run())

    def test_async_cluster_reconcile_does_not_overstamp_on_concurrent_discard(self):
        # The async ClusterNode owns its own _himport_reconcile_discards; guard it too.
        async def run():
            registry = HImportRegistry()
            va = registry.prepare("a", ["f"]).version
            vb = registry.prepare("b", ["f"]).version
            registry.discard("a")
            conn = _AsyncCapturingConn(registry, {"a": va, "b": vb})
            rev_after_a = registry.revision
            node = mock.Mock()

            def drain(*args, **kwargs):
                registry.discard("b")
                return True

            node.parse_response = mock.AsyncMock(side_effect=drain)
            await async_cluster_mod.ClusterNode._himport_reconcile_discards(node, conn)
            assert conn._himport_prepared == {"b": vb}
            assert conn._himport_reconciled_revision == rev_after_a
            assert conn._himport_reconciled_revision != registry.revision

            conn.writes.clear()
            node.parse_response = mock.AsyncMock(return_value=True)
            await async_cluster_mod.ClusterNode._himport_reconcile_discards(node, conn)
            assert conn._himport_prepared == {}
            assert conn.writes == [("packed", [DISCARD_B])]
            assert conn._himport_reconciled_revision == registry.revision

        asyncio.run(run())


PREPARE_FS_AB = ("HIMPORT", "PREPARE", "fs", "a", "b")
ASKING = ("ASKING",)
SET_K_FS = ("HIMPORT", "SET", "k", "fs", "v")


@pytest.mark.fixed_client
@pytest.mark.fixed_client
class TestIsHImportSetCommand:
    """``is_himport_set_command`` selects the connection-state-aware HIMPORT SET
    path for the raw ``execute_command`` API, where the caller controls the
    command-name spelling and encoding. An exact comparison would miss
    case/byte variants and send a bare SET that fails ``no such fieldset``.
    """

    @pytest.mark.parametrize(
        "name",
        [
            "HIMPORT SET",
            "himport set",
            "Himport Set",
            b"HIMPORT SET",
            b"himport set",
            bytearray(b"himport set"),
            memoryview(b"HIMPORT SET"),
        ],
    )
    def test_matches_case_and_encoding_variants(self, name):
        assert is_himport_set_command(name) is True

    @pytest.mark.parametrize(
        "name",
        [
            "HIMPORT PREPARE",
            "HIMPORT DISCARD",
            "SET",
            "himport setx",  # length differs -> no upper() needed, still rejected
            "himport se",
            "",
            b"GET",
            None,
            123,
        ],
    )
    def test_rejects_other_commands_and_non_strings(self, name):
        assert is_himport_set_command(name) is False


@pytest.mark.fixed_client
class TestParseHImportSetArgs:
    """``parse_himport_set_args`` recognises both wire-equivalent raw forms of
    HIMPORT SET and returns the operands at the right offsets for either.
    """

    @pytest.mark.parametrize(
        "args, expected",
        [
            # Joined form: args[0] == "HIMPORT SET".
            (("HIMPORT SET", "k", "fs", "v1", "v2"), ("k", "fs", ["v1", "v2"])),
            (("himport set", "k", "fs", "v1"), ("k", "fs", ["v1"])),
            ((b"HIMPORT SET", "k", "fs"), ("k", "fs", [])),  # no values
            # Split form: args[0] == "HIMPORT", args[1] == "SET".
            (("HIMPORT", "SET", "k", "fs", "v1", "v2"), ("k", "fs", ["v1", "v2"])),
            (("himport", "set", "k", "fs", "v1"), ("k", "fs", ["v1"])),
            ((b"HImPoRt", b"SeT", "k", "fs", "v1"), ("k", "fs", ["v1"])),
        ],
    )
    def test_parses_both_forms(self, args, expected):
        assert parse_himport_set_args(args) == expected

    @pytest.mark.parametrize(
        "args",
        [
            (),  # empty
            ("HIMPORT",),  # bare, no subcommand
            ("HIMPORT SET",),  # joined, too few operands
            ("HIMPORT", "SET", "k"),  # split, missing fieldset
            ("HIMPORT", "PREPARE", "fs", "a"),  # different subcommand
            ("HIMPORT", "DISCARD", "fs"),
            ("GET", "k"),  # unrelated command
            ("HSET", "k", "f", "v"),
        ],
    )
    def test_returns_none_for_non_set_or_too_few(self, args):
        assert parse_himport_set_args(args) is None


@pytest.mark.fixed_client
class TestHImportAskRedirectSync:
    """An ASK-redirected ``HIMPORT SET`` must carry the ASKING allowance on the
    same connection, immediately before the SET.

    Redis clears the per-command ASKING flag after the next command, so any
    session command (deferred DISCARD, lazy PREPARE) sent between ASKING and the
    SET would consume the allowance and leave the slot-scoped SET to fail or
    redirect again during migration. The sync cluster owns its own HIMPORT
    executor on ``RedisCluster`` (mirroring the async ``ClusterNode``): the ASKING
    allowance is folded into the SET's own packed write so it lands immediately
    before the SET. ``_himport_prepare_and_set`` ignores ``self`` (call it unbound);
    ``_himport_execute_set`` dispatches to ``_himport_prepare_and_set`` and
    ``_himport_reconcile_discards`` via ``self``, so a stub ``self`` carrying the
    real ``RedisCluster`` methods exercises the real code. ``parse_response`` is
    taken from the ``redis_node`` argument (the reconcile helper drains its own
    replies through that same ``redis_node``).
    """

    class _ClusterStub:
        _himport_execute_set = cluster_mod.RedisCluster._himport_execute_set
        _himport_prepare_and_set = cluster_mod.RedisCluster._himport_prepare_and_set
        _himport_reconcile_discards = (
            cluster_mod.RedisCluster._himport_reconcile_discards
        )

    def test_prepare_and_set_packs_prepare_asking_set(self):
        registry = HImportRegistry()
        registry.prepare("fs", ["a", "b"])
        conn = _CapturingConn(registry)
        fieldset = registry.get("fs")
        node = mock.Mock()
        node.parse_response = mock.Mock(side_effect=["OK", "OK", 1])
        resp = cluster_mod.RedisCluster._himport_prepare_and_set(
            None, node, conn, "k", "fs", ["v"], fieldset, asking=True
        )
        assert resp == 1
        assert conn.writes == [("packed", [PREPARE_FS_AB, ASKING, SET_K_FS])]
        assert conn._himport_prepared["fs"] == fieldset.version
        assert node.parse_response.call_count == 3

    def test_prepare_and_set_without_asking_keeps_bundle(self):
        # Regression guard: without an ASK redirect the batch stays [PREPARE, SET].
        registry = HImportRegistry()
        registry.prepare("fs", ["a", "b"])
        conn = _CapturingConn(registry)
        fieldset = registry.get("fs")
        node = mock.Mock()
        node.parse_response = mock.Mock(side_effect=["OK", 1])
        cluster_mod.RedisCluster._himport_prepare_and_set(
            None, node, conn, "k", "fs", ["v"], fieldset
        )
        assert conn.writes == [("packed", [PREPARE_FS_AB, SET_K_FS])]

    def test_bare_set_packs_asking_then_set(self):
        registry = HImportRegistry()
        registry.prepare("fs", ["a"])
        fieldset = registry.get("fs")
        conn = _CapturingConn(registry, prepared={"fs": fieldset.version})
        conn._himport_reconciled_revision = registry.revision
        node = mock.Mock()
        node.parse_response = mock.Mock(side_effect=["OK", 1])
        resp = self._ClusterStub()._himport_execute_set(
            node, conn, "k", "fs", ["v"], asking=True
        )
        assert resp == 1
        assert conn.writes == [("packed", [ASKING, SET_K_FS])]

    def test_bare_set_recovers_with_asking_on_missing_fieldset(self):
        # The bare SET's ASKING allowance is consumed by the failed SET, so the
        # NoSuchFieldsetError recovery must re-arm ASKING: [PREPARE, ASKING, SET].
        registry = HImportRegistry()
        registry.prepare("fs", ["a", "b"])
        fieldset = registry.get("fs")
        conn = _CapturingConn(registry, prepared={"fs": fieldset.version})
        conn._himport_reconciled_revision = registry.revision
        node = mock.Mock()
        node.parse_response = mock.Mock(
            side_effect=[
                "OK",  # ASKING (bare)
                NoSuchFieldsetError("no such fieldset"),  # SET (bare) -> recover
                "OK",  # PREPARE (recovery)
                "OK",  # ASKING (recovery)
                1,  # SET (recovery)
            ]
        )
        resp = self._ClusterStub()._himport_execute_set(
            node, conn, "k", "fs", ["v"], asking=True
        )
        assert resp == 1
        assert conn.writes == [
            ("packed", [ASKING, SET_K_FS]),
            ("packed", [PREPARE_FS_AB, ASKING, SET_K_FS]),
        ]
        assert conn._himport_prepared["fs"] == fieldset.version
        assert node.parse_response.call_count == 5

    def test_bare_set_recovery_without_asking_keeps_bundle(self):
        # Regression: a non-ASK recovery still packs [PREPARE, SET] (no ASKING).
        registry = HImportRegistry()
        registry.prepare("fs", ["a", "b"])
        fieldset = registry.get("fs")
        conn = _CapturingConn(registry, prepared={"fs": fieldset.version})
        conn._himport_reconciled_revision = registry.revision
        node = mock.Mock()
        node.parse_response = mock.Mock(
            side_effect=[NoSuchFieldsetError("no such fieldset"), "OK", 1]
        )
        resp = self._ClusterStub()._himport_execute_set(node, conn, "k", "fs", ["v"])
        assert resp == 1
        assert conn.writes == [
            ("cmd", SET_K_FS),
            ("packed", [PREPARE_FS_AB, SET_K_FS]),
        ]

    def test_bare_set_asking_error_drains_set_reply(self):
        # ASKING and SET are one packed write (two queued replies). If the ASKING
        # reply errors, the SET reply must still be drained before the error is
        # surfaced, or the connection returns to the pool with an unread reply.
        registry = HImportRegistry()
        registry.prepare("fs", ["a"])
        fieldset = registry.get("fs")
        conn = _CapturingConn(registry, prepared={"fs": fieldset.version})
        conn._himport_reconciled_revision = registry.revision
        node = mock.Mock()
        node.parse_response = mock.Mock(
            side_effect=[ResponseError("bad ASKING"), 1]  # ASKING errors, SET drained
        )
        with pytest.raises(ResponseError, match="bad ASKING"):
            self._ClusterStub()._himport_execute_set(
                node, conn, "k", "fs", ["v"], asking=True
            )
        assert node.parse_response.call_count == 2  # SET reply drained
        assert conn.writes == [("packed", [ASKING, SET_K_FS])]  # no retry write


@pytest.mark.fixed_client
class TestHImportAskRedirectAsyncCluster:
    """The async cluster owns its own HIMPORT executor on ``ClusterNode``; there
    the ASKING allowance is folded into the SET's own packed write (the ASKING is
    not sent as a separately pooled command), so ASKING lands immediately before
    the SET. The methods only touch ``self.parse_response`` /
    ``self._himport_reconcile_discards``; a stub ``self`` exercises the real code.
    """

    def test_prepare_and_set_packs_prepare_asking_set(self):
        async def run():
            registry = HImportRegistry()
            registry.prepare("fs", ["a", "b"])
            conn = _AsyncCapturingConn(registry)
            fieldset = registry.get("fs")
            node = mock.Mock()
            node.parse_response = mock.AsyncMock(side_effect=["OK", "OK", 1])
            resp = await async_cluster_mod.ClusterNode._himport_prepare_and_set(
                node, conn, "k", "fs", ["v"], fieldset, asking=True
            )
            assert resp == 1
            assert conn.writes == [("packed", [PREPARE_FS_AB, ASKING, SET_K_FS])]
            assert conn._himport_prepared["fs"] == fieldset.version
            assert node.parse_response.call_count == 3

        asyncio.run(run())

    def test_prepare_and_set_without_asking_keeps_bundle(self):
        # Regression guard: without an ASK redirect the batch stays [PREPARE, SET].
        async def run():
            registry = HImportRegistry()
            registry.prepare("fs", ["a", "b"])
            conn = _AsyncCapturingConn(registry)
            fieldset = registry.get("fs")
            node = mock.Mock()
            node.parse_response = mock.AsyncMock(side_effect=["OK", 1])
            await async_cluster_mod.ClusterNode._himport_prepare_and_set(
                node, conn, "k", "fs", ["v"], fieldset
            )
            assert conn.writes == [("packed", [PREPARE_FS_AB, SET_K_FS])]

        asyncio.run(run())

    def test_bare_set_packs_asking_then_set(self):
        async def run():
            registry = HImportRegistry()
            registry.prepare("fs", ["a"])
            fieldset = registry.get("fs")
            conn = _AsyncCapturingConn(registry, prepared={"fs": fieldset.version})
            conn._himport_reconciled_revision = registry.revision
            node = mock.Mock()
            node._himport_reconcile_discards = mock.AsyncMock()
            node.parse_response = mock.AsyncMock(side_effect=["OK", 1])
            resp = await async_cluster_mod.ClusterNode._himport_execute_set(
                node, conn, "k", "fs", ["v"], asking=True
            )
            assert resp == 1
            assert conn.writes == [("packed", [ASKING, SET_K_FS])]

        asyncio.run(run())

    def test_bare_set_recovers_with_asking_on_missing_fieldset(self):
        # The bare SET's ASKING allowance is consumed by the failed SET, so the
        # NoSuchFieldsetError recovery must re-arm ASKING: [PREPARE, ASKING, SET].
        async def run():
            registry = HImportRegistry()
            registry.prepare("fs", ["a", "b"])
            fieldset = registry.get("fs")
            conn = _AsyncCapturingConn(registry, prepared={"fs": fieldset.version})
            conn._himport_reconciled_revision = registry.revision
            node = mock.Mock()
            node._himport_reconcile_discards = mock.AsyncMock()
            node.parse_response = mock.AsyncMock(
                side_effect=[
                    "OK",  # ASKING (bare)
                    NoSuchFieldsetError("no such fieldset"),  # SET (bare) -> recover
                    "OK",  # PREPARE (recovery)
                    "OK",  # ASKING (recovery)
                    1,  # SET (recovery)
                ]
            )
            node._himport_prepare_and_set = (
                async_cluster_mod.ClusterNode._himport_prepare_and_set.__get__(node)
            )
            resp = await async_cluster_mod.ClusterNode._himport_execute_set(
                node, conn, "k", "fs", ["v"], asking=True
            )
            assert resp == 1
            assert conn.writes == [
                ("packed", [ASKING, SET_K_FS]),
                ("packed", [PREPARE_FS_AB, ASKING, SET_K_FS]),
            ]
            assert conn._himport_prepared["fs"] == fieldset.version
            assert node.parse_response.call_count == 5

        asyncio.run(run())

    def test_bare_set_recovery_without_asking_keeps_bundle(self):
        # Regression: a non-ASK recovery still packs [PREPARE, SET] (no ASKING).
        async def run():
            registry = HImportRegistry()
            registry.prepare("fs", ["a", "b"])
            fieldset = registry.get("fs")
            conn = _AsyncCapturingConn(registry, prepared={"fs": fieldset.version})
            conn._himport_reconciled_revision = registry.revision
            node = mock.Mock()
            node._himport_reconcile_discards = mock.AsyncMock()
            node.parse_response = mock.AsyncMock(
                side_effect=[NoSuchFieldsetError("no such fieldset"), "OK", 1]
            )
            node._himport_prepare_and_set = (
                async_cluster_mod.ClusterNode._himport_prepare_and_set.__get__(node)
            )
            resp = await async_cluster_mod.ClusterNode._himport_execute_set(
                node, conn, "k", "fs", ["v"]
            )
            assert resp == 1
            assert conn.writes == [
                ("cmd", SET_K_FS),
                ("packed", [PREPARE_FS_AB, SET_K_FS]),
            ]

        asyncio.run(run())

    def test_bare_set_asking_error_drains_set_reply(self):
        # ASKING and SET are one packed write (two queued replies). If the ASKING
        # reply errors, the SET reply must still be drained before the error is
        # surfaced, or the connection returns to the pool with an unread reply.
        async def run():
            registry = HImportRegistry()
            registry.prepare("fs", ["a"])
            fieldset = registry.get("fs")
            conn = _AsyncCapturingConn(registry, prepared={"fs": fieldset.version})
            conn._himport_reconciled_revision = registry.revision
            node = mock.Mock()
            node._himport_reconcile_discards = mock.AsyncMock()
            node.parse_response = mock.AsyncMock(
                side_effect=[ResponseError("bad ASKING"), 1]  # ASKING errs, SET drained
            )
            with pytest.raises(ResponseError, match="bad ASKING"):
                await async_cluster_mod.ClusterNode._himport_execute_set(
                    node, conn, "k", "fs", ["v"], asking=True
                )
            assert node.parse_response.call_count == 2  # SET reply drained
            assert conn.writes == [("packed", [ASKING, SET_K_FS])]  # no retry write

        asyncio.run(run())


@pytest.mark.fixed_client
class TestHImportSingleConnectionPrepareMaterializesFields:
    """On a single-connection client the immediate wire PREPARE must send the
    fully materialized fields even when the caller passes a one-shot iterable.

    ``himport_registry.prepare`` consumes the iterable into a tuple, so re-passing
    the original ``fields`` to the wire call would forward an exhausted iterator
    and PREPARE zero fields. The wire call must reuse the stored
    ``HImportFieldset.fields`` returned by ``prepare`` instead.
    """

    def test_generator_fields_reach_wire_prepare(self):
        client = Redis()
        client._single_connection_client = True
        conn = mock.Mock()
        conn.is_connected = True
        conn._himport_prepared = {}
        client.connection = conn
        with mock.patch.object(client, "himport_prepare_internal") as internal:
            client.himport_prepare("fs", (f"f{i}" for i in range(3)))
        internal.assert_called_once_with("fs", ("f0", "f1", "f2"))
        assert conn._himport_prepared["fs"] == client.himport_registry.get("fs").version

    def test_async_generator_fields_reach_wire_prepare(self):
        async def run():
            client = AsyncRedis()
            client.single_connection_client = True
            client.initialize = mock.AsyncMock(return_value=client)
            conn = mock.Mock()
            conn.is_connected = True
            conn._himport_prepared = {}
            client.connection = conn
            client.himport_prepare_internal = mock.AsyncMock()
            await client.himport_prepare("fs", (f"f{i}" for i in range(3)))
            client.himport_prepare_internal.assert_awaited_once_with(
                "fs", ("f0", "f1", "f2")
            )
            assert (
                conn._himport_prepared["fs"]
                == client.himport_registry.get("fs").version
            )

        asyncio.run(run())


@pytest.mark.fixed_client
class TestHImportPropagationSync:
    """Data propagation: client -> pool -> connection (standalone + sentinel)."""

    def test_pool_registry_reaches_connection_via_runtime_prepare(self):
        # A shared registry always exists on the pool; a runtime prepare on it is
        # visible through the same object reached by every connection.
        pool = ConnectionPool()
        pool.himport_registry.prepare("shared", ["a", "b"])
        assert isinstance(pool.himport_registry, HImportRegistry)
        assert pool.himport_registry.get("shared").fields == ("a", "b")
        conn = pool.make_connection()
        # Same shared object reaches the connection, plus empty per-conn state.
        assert conn.himport_registry is pool.himport_registry
        assert conn.himport_registry.get("shared").fields == ("a", "b")
        assert conn._himport_prepared == {}
        assert conn._himport_reconciled_revision == 0

    def test_pool_accepts_prebuilt_registry_internally(self):
        # Internal channel (used by the cluster to share one object): a pre-built
        # himport_registry passed via connection_kwargs is used as-is, not rebuilt.
        registry = HImportRegistry()
        registry.prepare("x", ["1"])
        pool = ConnectionPool(himport_registry=registry)
        assert pool.himport_registry is registry
        assert pool.make_connection().himport_registry is registry

    def test_pool_empty_registry_when_unconfigured(self):
        # A registry always exists (empty) so runtime himport_prepare has a single
        # shared object to mutate; the same object reaches connections.
        pool = ConnectionPool()
        assert isinstance(pool.himport_registry, HImportRegistry)
        assert len(pool.himport_registry) == 0
        assert pool.make_connection().himport_registry is pool.himport_registry

    def test_blocking_pool_resolves(self):
        pool = BlockingConnectionPool()
        pool.himport_registry.prepare("s", ["a"])
        assert pool.himport_registry.get("s").fields == ("a",)
        assert pool.make_connection().himport_registry is pool.himport_registry

    def test_redis_property_and_propagation(self):
        r = Redis()
        r.himport_prepare("shared", ["name", "email"])
        assert r.himport_registry is r.connection_pool.himport_registry
        assert r.himport_registry.get("shared").fields == ("name", "email")

    def test_redis_unix_socket_propagates_runtime_prepare(self):
        # Regression: runtime himport_prepare must reach the shared pool registry for
        # Unix-socket clients, not just TCP. Construction does not connect, so no
        # socket is needed.
        r = Redis(unix_socket_path="/tmp/does-not-connect.sock")
        r.himport_prepare("shared", ["name", "email"])
        assert r.himport_registry is r.connection_pool.himport_registry
        assert r.himport_registry.get("shared").fields == ("name", "email")

    def test_redis_unconfigured_is_empty(self):
        registry = Redis().himport_registry
        assert isinstance(registry, HImportRegistry)
        assert len(registry) == 0

    def test_redis_has_no_public_himport_param(self):
        # Under the runtime-only model there is no HIMPORT constructor argument at
        # all: himport_registry is internal-only and himport_schemas was removed.
        # Fieldsets are declared exclusively via the runtime himport_prepare method.
        params = inspect.signature(Redis.__init__).parameters
        assert not any("himport" in name for name in params)

    def test_sentinel_master_client_runtime_prepare(self):
        # Sentinel needs no HIMPORT-specific wiring: a client from master_for exposes
        # the runtime himport API and mutates its own pool's shared registry. Pure
        # unit test: construction and himport_prepare do not connect.
        s = Sentinel([("127.0.0.1", 26379)])
        master = s.master_for("mymaster")
        master.himport_prepare("s", ["a"])
        assert master.himport_registry is master.connection_pool.himport_registry
        assert master.himport_registry.get("s").fields == ("a",)


@pytest.mark.fixed_client
class TestHImportPropagationAsync:
    """Async mirror of the standalone propagation checks."""

    def test_async_pool_and_client(self):
        async def run():
            pool = AsyncConnectionPool()
            pool.himport_registry.prepare("s", ["a", "b"])
            assert pool.himport_registry.get("s").fields == ("a", "b")
            conn = pool.make_connection()
            assert conn.himport_registry is pool.himport_registry
            assert conn.himport_registry.get("s").fields == ("a", "b")
            assert conn._himport_prepared == {}
            assert conn._himport_reconciled_revision == 0

            r = AsyncRedis()
            await r.himport_prepare("s", ["a"])
            assert r.himport_registry is r.connection_pool.himport_registry
            assert r.himport_registry.get("s").fields == ("a",)

            assert len(AsyncRedis().himport_registry) == 0

        asyncio.run(run())


@pytest.mark.fixed_client
class TestHImportSetValueValidation:
    """himport_set must reject bare string-like value iterables (str/bytes/
    bytearray/memoryview): splatting them sends single chars/bytes as separate
    positional values instead of one value. The guard runs before any connection
    use, so no server is needed. Mirrors HImportRegistry's field-list guard."""

    @pytest.mark.parametrize("bad", ["abc", b"x", bytearray(b"x"), memoryview(b"x")])
    def test_sync_rejects_string_like_values(self, bad):
        with pytest.raises(DataError, match="must be a collection of values"):
            Redis().himport_set("k", "fs", bad)

    @pytest.mark.parametrize("bad", ["abc", b"x", bytearray(b"x"), memoryview(b"x")])
    def test_async_rejects_string_like_values(self, bad):
        async def run():
            with pytest.raises(DataError, match="must be a collection of values"):
                await AsyncRedis().himport_set("k", "fs", bad)

        asyncio.run(run())


@pytest.mark.fixed_client
class TestHImportSetRawArityFallThrough:
    """A raw ``execute_command`` for HIMPORT SET with too few args must fall through
    to the normal send path so the server returns its arity error, instead of the
    client raising IndexError while slicing key/fieldset_name out of ``args``.
    ``himport_set`` itself always supplies key + fieldset_name (len(args) >= 3), so
    the real command path is unaffected."""

    def test_sync_too_few_args_falls_through_to_send(self):
        conn = _CapturingConn(HImportRegistry())
        r = Redis()
        r._himport_execute_set = mock.Mock(
            side_effect=AssertionError("must not take the HIMPORT executor path")
        )
        r.parse_response = mock.Mock(return_value="server-arity-error")
        # (HIMPORT_SET, key) — no fieldset_name; must not raise IndexError.
        r._send_command_parse_response(conn, HIMPORT_SET, HIMPORT_SET, "k")
        assert conn.writes == [("cmd", (HIMPORT_SET, "k"))]

    def test_sync_well_formed_uses_himport_executor(self):
        conn = _CapturingConn(HImportRegistry())
        r = Redis()
        r._himport_execute_set = mock.Mock(return_value="OK")
        r._send_command_parse_response(conn, HIMPORT_SET, HIMPORT_SET, "k", "fs", "v")
        r._himport_execute_set.assert_called_once_with(conn, "k", "fs", ["v"])
        assert conn.writes == []  # did not fall through to a plain send

    def test_async_too_few_args_falls_through_to_send(self):
        async def run():
            conn = _AsyncCapturingConn(HImportRegistry())
            r = AsyncRedis()
            r._himport_execute_set = mock.AsyncMock(
                side_effect=AssertionError("must not take the HIMPORT executor path")
            )
            r.parse_response = mock.AsyncMock(return_value="server-arity-error")
            await r._send_command_parse_response(conn, HIMPORT_SET, HIMPORT_SET, "k")
            assert conn.writes == [("cmd", (HIMPORT_SET, "k"))]

        asyncio.run(run())

    def test_async_well_formed_uses_himport_executor(self):
        async def run():
            conn = _AsyncCapturingConn(HImportRegistry())
            r = AsyncRedis()
            r._himport_execute_set = mock.AsyncMock(return_value="OK")
            await r._send_command_parse_response(
                conn, HIMPORT_SET, HIMPORT_SET, "k", "fs", "v"
            )
            r._himport_execute_set.assert_awaited_once_with(conn, "k", "fs", ["v"])
            assert conn.writes == []

        asyncio.run(run())


@pytest.mark.fixed_client
class TestHImportClusterWiring:
    """Cluster wiring guards (offline). Full topology behavior is covered by the
    cluster suite; here we assert the plumbing that makes propagation possible."""

    def test_nodes_manager_accepts_shared_registry(self):
        # The one shared registry is threaded to nodes via NodesManager (and injected
        # onto each node pool), not through connection_kwargs.
        params = inspect.signature(cluster_mod.NodesManager.__init__).parameters
        assert "himport_registry" in params

    def test_sync_cluster_has_no_public_himport_param(self):
        params = inspect.signature(cluster_mod.RedisCluster.__init__).parameters
        # Runtime-only model: no HIMPORT constructor knob is public (the shared
        # registry is threaded internally, and the old schemas dict was removed).
        assert not any("himport" in name for name in params)

    def test_async_cluster_has_no_public_himport_param(self):
        params = inspect.signature(async_cluster_mod.RedisCluster.__init__).parameters
        assert not any("himport" in name for name in params)

    def test_async_cluster_has_himport_slot(self):
        # Private backing attribute lives in __slots__; the public name is a property.
        assert "_himport_registry" in async_cluster_mod.RedisCluster.__slots__

    def test_himport_registry_is_read_only_property(self):
        # All client classes expose himport_registry as a read-only property (no setter),
        # so it cannot be rebound to desync the shared registry.
        from redis import Redis, RedisCluster
        from redis.asyncio import Redis as AsyncRedis
        from redis.asyncio import RedisCluster as AsyncRedisCluster

        for cls in (Redis, RedisCluster, AsyncRedis, AsyncRedisCluster):
            attr = inspect.getattr_static(cls, "himport_registry")
            assert isinstance(attr, property), cls
            assert attr.fset is None, cls  # read-only: no setter

    def test_sync_cluster_pipeline_shares_parent_registry(self):
        # The pipeline inherits himport_prepare/discard/discard_all from RedisCluster;
        # they mutate the one registry threaded through the NodesManager (and referenced
        # by every node pool), so a fieldset declared on the pipeline is visible to the
        # batched himport_set pre-flight exactly as on the parent client.
        registry = HImportRegistry()
        nodes_manager = mock.Mock()
        nodes_manager.himport_registry = registry
        pipe = cluster_mod.ClusterPipeline(
            nodes_manager=nodes_manager, commands_parser=mock.Mock()
        )

        assert pipe.himport_registry is registry
        assert pipe.himport_prepare("fs", ["a", "b"]) is True
        assert registry.get("fs") is not None  # mutated the shared object
        assert pipe.himport_discard("fs") == 1
        assert registry.get("fs") is None
        assert pipe.himport_discard_all() == 0

    def test_async_cluster_pipeline_delegates_to_parent_registry(self):
        # The async pipeline holds only a client reference, so its HIMPORT lifecycle
        # methods delegate to the parent client, mutating the one shared registry.
        registry = HImportRegistry()
        client = mock.Mock()
        client.himport_registry = registry
        client.himport_prepare = mock.AsyncMock(return_value=True)
        client.himport_discard = mock.AsyncMock(return_value=1)
        client.himport_discard_all = mock.AsyncMock(return_value=0)
        pipe = async_cluster_mod.ClusterPipeline(client)

        assert pipe.himport_registry is registry

        async def run():
            assert await pipe.himport_prepare("fs", ["a", "b"]) is True
            assert await pipe.himport_discard("fs") == 1
            assert await pipe.himport_discard_all() == 0

        asyncio.run(run())
        client.himport_prepare.assert_awaited_once_with("fs", ["a", "b"])
        client.himport_discard.assert_awaited_once_with("fs")
        client.himport_discard_all.assert_awaited_once_with()


@pytest.mark.fixed_client
class TestNoSuchFieldsetErrorMapping:
    """The parser maps the server's "no such fieldset" reply to the dedicated
    NoSuchFieldsetError so the HIMPORT SET recovery path can catch it by type.

    The server reply is a fixed message with no fieldset name appended (verified
    against the server: always exactly ``ERR no such fieldset``), so the mapping is
    an exact match in ``EXCEPTION_CLASSES``, like the other ERR-message entries.
    """

    def test_maps_no_such_fieldset_reply(self):
        # Exact wire reply the server sends for an unprepared fieldset.
        exc = BaseParser.parse_error("ERR no such fieldset")
        assert isinstance(exc, NoSuchFieldsetError)
        assert isinstance(exc, ResponseError)  # remains a ResponseError subtype
        assert exc.status_code == "ERR"

    def test_unrelated_err_stays_generic(self):
        exc = BaseParser.parse_error("ERR something else went wrong")
        assert type(exc) is ResponseError
        assert not isinstance(exc, NoSuchFieldsetError)
