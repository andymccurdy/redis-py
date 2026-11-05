"""Observability recorder microbenchmark: no network, observability not enabled.

Every command calls the metrics recorder whether or not observability was
initialized, so what the recorder costs while disabled is paid by every
client that never opts in. That cost is tenths of a microsecond per command,
far below the round-trip noise run_bench works with, so it is timed here
without a server. Two scenarios, each on the sync and the async stack:

- record_operation_duration: the recorder call every command makes, called
  directly. It isolates the recorder's own disabled path.
- pooled_get: Redis.get() through a ConnectionPool, with only the
  connection's I/O replaced (connect, can_read, send_packed_command,
  read_response and disconnect). The client's command path, the retry
  wrapper, command packing, the pool's checkout and release and every
  recorder call on that path (one for the command, two each for checkout
  and release) are the library code of the tree under test, so the
  recorder's share of the client's per-command CPU time is timed as well.

The recorder is never initialized, so both scenarios time the default
configuration; the enabled path needs an OpenTelemetry SDK and is out of
scope. Each tree runs in its own interpreter.

Run from the repository root:

  python -m benchmarks.measure_read_path.recorder_micro \\
      --trees base=../redis-py-bench-base pr=../redis-py-bench-pr
"""

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import gate  # noqa: E402
import run_bench  # noqa: E402
import worker  # noqa: E402

# operations per round; a round of either scenario takes tens of milliseconds
OPS = {"record_operation_duration": 100_000, "pooled_get": 10_000}
REPLY = b"bar"


def memory_connection_classes():
    """Connection classes whose I/O is in memory, for the sync and async stack.

    Only the methods that touch the socket are replaced, so everything the
    pool and the client do around them runs unchanged. The connection
    reports nothing to read, so the pool hands it out without reconnecting,
    and every reply is REPLY.
    """
    from redis.asyncio.connection import Connection as AsyncConnection
    from redis.connection import Connection

    class MemoryConnection(Connection):
        def connect(self):
            pass

        def can_read(self, timeout=0):
            return False

        def send_packed_command(self, command, check_health=True):
            pass

        def read_response(self, *args, **kwargs):
            return REPLY

        def disconnect(self, *args, **kwargs):
            pass

    class AsyncMemoryConnection(AsyncConnection):
        async def connect(self):
            pass

        async def can_read(self):
            return False

        async def can_read_destructive(self):
            return False

        async def send_packed_command(self, command, check_health=True):
            pass

        async def read_response(self, *args, **kwargs):
            return REPLY

        async def disconnect(self, *args, **kwargs):
            pass

    return MemoryConnection, AsyncMemoryConnection


def ns_per_op(samples, ops):
    return statistics.median(samples) / ops


def time_sync(fn, ops, rounds):
    samples = []
    # the first round is a warmup
    for i in range(rounds + 1):
        start = time.perf_counter_ns()
        for _ in range(ops):
            fn()
        if i:
            samples.append(time.perf_counter_ns() - start)
    return ns_per_op(samples, ops)


async def time_async(fn, ops, rounds):
    samples = []
    # the first round is a warmup
    for i in range(rounds + 1):
        start = time.perf_counter_ns()
        for _ in range(ops):
            await fn()
        if i:
            samples.append(time.perf_counter_ns() - start)
    return ns_per_op(samples, ops)


def timed(fn):
    start = time.perf_counter()
    ns = fn()
    return {
        "ns_per_op": round(ns, 1),
        "wall_s": round(time.perf_counter() - start, 2),
    }


async def timed_async(fn):
    start = time.perf_counter()
    ns = await fn()
    return {
        "ns_per_op": round(ns, 1),
        "wall_s": round(time.perf_counter() - start, 2),
    }


def sync_cells(connection_class, rounds):
    from redis import ConnectionPool, Redis
    from redis.observability import recorder

    def record():
        recorder.record_operation_duration("GET", 0.0001, "localhost", 6379, "0")

    client = Redis(connection_pool=ConnectionPool(connection_class=connection_class))
    try:
        assert client.get("key") == REPLY
        return {
            "record_operation_duration": timed(
                lambda: time_sync(record, OPS["record_operation_duration"], rounds)
            ),
            "pooled_get": timed(
                lambda: time_sync(lambda: client.get("key"), OPS["pooled_get"], rounds)
            ),
        }
    finally:
        client.close()


async def async_cells(connection_class, rounds):
    from redis.asyncio import ConnectionPool, Redis
    from redis.asyncio.observability import recorder

    def record():
        return recorder.record_operation_duration("GET", 0.0001, "localhost", 6379, "0")

    client = Redis(connection_pool=ConnectionPool(connection_class=connection_class))
    try:
        assert await client.get("key") == REPLY
        return {
            "record_operation_duration": await timed_async(
                lambda: time_async(record, OPS["record_operation_duration"], rounds)
            ),
            "pooled_get": await timed_async(
                lambda: time_async(lambda: client.get("key"), OPS["pooled_get"], rounds)
            ),
        }
    finally:
        await client.aclose()


def child(args):
    worker.import_redis(args.tree)
    connection_class, async_connection_class = memory_connection_classes()
    out = {
        "sync": sync_cells(connection_class, args.rounds),
        "async": asyncio.run(async_cells(async_connection_class, args.rounds)),
    }
    print(json.dumps(out))


def main():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    p.add_argument("--trees", nargs="+")
    p.add_argument("--tree", help=argparse.SUPPRESS)
    p.add_argument("--python", default=sys.executable)
    p.add_argument("--rounds", type=int, default=10)
    p.add_argument(
        "--passes",
        type=int,
        default=5,
        help="runs per tree; their spread sets the uncertainty of a delta",
    )
    p.add_argument(
        "--output-dir",
        default=os.path.join(HERE, "results"),
        help="where the JSON and Markdown results are written",
    )
    gate.add_gate_args(p)
    args = p.parse_args()
    if args.tree:
        return child(args)
    if not args.trees:
        p.error("--trees is required")
    trees = {k: os.path.abspath(v) for k, v in (t.split("=", 1) for t in args.trees)}
    pairs = gate.compare_pairs(args, trees)
    # passes[tree] -> list of {config: {scenario: result}}
    passes = {t: [] for t in trees}
    total_start = time.perf_counter()
    for i in range(args.passes):
        for label in run_bench.tree_order(trees, i):
            path = trees[label]
            cmd = [
                args.python,
                "-I",
                os.path.abspath(__file__),
                "--tree",
                path,
                "--rounds",
                str(args.rounds),
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if proc.returncode != 0:
                raise SystemExit(proc.stderr)
            passes[label].append(json.loads(proc.stdout.strip().splitlines()[-1]))
    first = passes[next(iter(trees))][0]
    cells = [(cfg, scenario) for cfg in first for scenario in first[cfg]]
    heads = {k: run_bench.git_head(v) for k, v in trees.items()}

    def values(tree, cfg, scenario, key="ns_per_op"):
        return [r[cfg][scenario][key] for r in passes[tree]]

    lines = [
        "Observability recorder, disabled, nanoseconds per operation "
        f"(median of {args.passes} passes)",
        "",
        "Trees: " + ", ".join(f"`{k}`={heads[k]}" for k in trees),
        "",
        "| config | scenario | " + " | ".join(trees) + " |",
        "|" + "---|" * (2 + len(trees)),
    ]
    for cfg, scenario in cells:
        row = [f"{statistics.median(values(t, cfg, scenario)):.1f} ns" for t in trees]
        lines.append(f"| {cfg} | {scenario} | " + " | ".join(row) + " |")
    lines += [
        "",
        "## Execution times",
        "",
        f"Whole run: "
        f"{time.perf_counter() - total_start:.0f}s. Median seconds per "
        "config and scenario run:",
        "",
    ]
    for cfg, scenario in cells:
        row = [
            f"{t}={statistics.median(values(t, cfg, scenario, 'wall_s')):.2f}s"
            for t in trees
        ]
        lines.append(f"- {cfg} {scenario}: " + ", ".join(row))
    text = "\n".join(lines)
    print(text)

    summaries = [
        gate.render(
            "Observability recorder, disabled",
            gate.evaluate(
                [
                    {
                        "config": cfg,
                        "scenario": scenario,
                        "old_samples": values(old, cfg, scenario),
                        "new_samples": values(new, cfg, scenario),
                    }
                    for cfg, scenario in cells
                ],
                args.allowed_regression,
                args.max_limit,
            ),
            new,
            old,
            args.allowed_regression,
            "nanoseconds per operation",
            args.max_limit,
            inconclusive_fails=not args.warn_inconclusive,
        )
        for new, old in pairs
    ]
    run_bench.save(
        args,
        heads,
        {
            "passes": passes,
            "report": text + "\n\n" + "\n\n".join(s for s, _, _ in summaries),
        },
        suffix="recorder",
    )
    gate.finish(args, summaries)


if __name__ == "__main__":
    main()
