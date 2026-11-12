"""Cluster command routing microbenchmark: no network after setup.

Before a cluster client sends a keyed command, it resolves the command's
policies and picks the node that owns the command's slot. That CPU work is a
few microseconds per command, far below the round-trip noise run_bench works
with, so it is timed here without sending anything. The client connects to
the cluster once to learn the topology, then only the per-node execution is
replaced, so everything that routes a command runs the tree's own code.
Two scenarios, each run on the sync and the async client:

- execute_keyed (sync and async): RedisCluster.set() with
  RedisCluster._execute_command replaced. It times the routing of one keyed
  command the policy resolver does not know, which is where the slot is
  computed: policy resolution, node selection, and the retry wrapper around
  them.
- pipeline_1k (sync and async): queue 1000 SETs to keys in many slots on a
  ClusterPipeline and execute it, with only the per-node send and read
  replaced (NodeCommands.write and read on sync, ClusterNode.execute_pipeline
  on async). It times queuing the commands, routing them, grouping them by
  node and collecting the results. The sync pipeline still checks a
  connection out of each node's pool and in again, as it does for real.

Before timing, every scenario checks once that each command went to the
node that owns its key's slot, so a routing change that picks a wrong node
cannot pass as a fast one. No key is written. Each tree runs in its own
interpreter.

Run from the repository root, with the cluster that `invoke devenv` starts:

  python -m benchmarks.measure_read_path.routing_micro \\
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

# operations per round; a round of any scenario takes tens of milliseconds
OPS = {"execute_keyed": 10_000, "pipeline_1k": 20}
PIPELINE_SIZE = 1000
# enough distinct keys to cover many slots and every node
KEYS = [f"bench_routing:{i}" for i in range(PIPELINE_SIZE)]


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


def sync_cells(args, rounds):
    from redis.cluster import NodeCommands, RedisCluster

    routed = []
    checking = True

    def execute_command(self, target_node, *command_args, **kwargs):
        if checking:
            routed.append((target_node.name, command_args[1]))
        return True

    def write(self):
        if checking:
            node_name = f"{self.connection.host}:{self.connection.port}"
            routed.extend((node_name, c.args[1]) for c in self.commands)

    def read(self):
        for c in self.commands:
            c.result = True

    # the topology is discovered before the execution is replaced
    client = RedisCluster(host=args.host, port=args.cluster_port)
    RedisCluster._execute_command = execute_command
    NodeCommands.write = write
    NodeCommands.read = read

    def pipeline_1k():
        with client.pipeline() as pipe:
            for key in KEYS:
                pipe.set(key, "x")
            return pipe.execute()

    try:
        for key in KEYS:
            assert client.set(key, "x") is True
        assert pipeline_1k() == [True] * PIPELINE_SIZE
        assert len(routed) == 2 * PIPELINE_SIZE
        assert all(name == client.get_node_from_key(key).name for name, key in routed)
        assert len({name for name, _ in routed}) > 1, "one node only"
        checking = False
        key = KEYS[0]
        return {
            "execute_keyed": timed(
                lambda: time_sync(
                    lambda: client.set(key, "x"), OPS["execute_keyed"], rounds
                )
            ),
            "pipeline_1k": timed(
                lambda: time_sync(pipeline_1k, OPS["pipeline_1k"], rounds)
            ),
        }
    finally:
        client.close()


async def async_cells(args, rounds):
    from redis.asyncio.cluster import ClusterNode, RedisCluster

    routed = []
    checking = True

    async def execute_command(self, target_node, *command_args, **kwargs):
        if checking:
            routed.append((target_node, command_args[1]))
        return True

    async def execute_pipeline(self, commands):
        for cmd in commands:
            if checking:
                routed.append((self, cmd.args[1]))
            cmd.result = True
        return False

    # the topology is discovered before the execution is replaced
    client = RedisCluster(host=args.host, port=args.cluster_port)
    await client.initialize()
    RedisCluster._execute_command = execute_command
    ClusterNode.execute_pipeline = execute_pipeline

    async def pipeline_1k():
        pipe = client.pipeline()
        for key in KEYS:
            pipe.set(key, "x")
        return await pipe.execute()

    try:
        for key in KEYS:
            assert await client.set(key, "x") is True
        assert await pipeline_1k() == [True] * PIPELINE_SIZE
        assert len(routed) == 2 * PIPELINE_SIZE
        assert all(node is client.get_node_from_key(key) for node, key in routed)
        assert len({node.name for node, _ in routed}) > 1, "one node only"
        checking = False
        key = KEYS[0]
        return {
            "execute_keyed": await timed_async(
                lambda: time_async(
                    lambda: client.set(key, "x"), OPS["execute_keyed"], rounds
                )
            ),
            "pipeline_1k": await timed_async(
                lambda: time_async(pipeline_1k, OPS["pipeline_1k"], rounds)
            ),
        }
    finally:
        await client.aclose()


def child(args):
    worker.import_redis(args.tree)
    out = {
        "sync": sync_cells(args, args.rounds),
        "async": asyncio.run(async_cells(args, args.rounds)),
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
    p.add_argument("--host", default="localhost")
    p.add_argument("--cluster-port", type=int, default=16379)
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
                "--host",
                args.host,
                "--cluster-port",
                str(args.cluster_port),
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
        "Cluster command routing, nanoseconds per operation "
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
            "Cluster command routing",
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
        suffix="routing",
    )
    gate.finish(args, summaries)


if __name__ == "__main__":
    main()
