"""A/B benchmark of the sync client-side cache across redis-py checkouts.

Runs benchmarks/client_side_caching.py unchanged against every tree: each
pass runs every config once per tree, in a fresh interpreter that imports
redis from that tree and checks where it came from, with the tree order
rotated from pass to pass. A config is a topology (standalone or cluster), a
workload (read-heavy or write-heavy) and the cache on or off; cache off is
the control that tells a slowdown in the cache path from one in the plain
sync path. Every GET and SET reply is checked by client_side_caching.py, and
a run with errors fails.

Two values per config are gated, both lower is better: the time per
operation, from the run's throughput, and the p50 latency. The median of
--passes runs per tree is reported, and their spread sets the uncertainty of
a delta (see gate.py). One client thread is the default, because more
threads add GIL contention to the numbers. Only the sync client has a
client-side cache, so there is no async side.

Run from the repository root, against a Redis server that nothing else is
using (RESP3 and Redis 7.4 or later):

  python -m benchmarks.measure_read_path.csc_bench \\
      --trees base=../redis-py-bench-base pr=../redis-py-bench-pr \\
      --compare pr:base
"""

import argparse
import importlib.util
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

CSC_TOOL = os.path.join(os.path.dirname(HERE), "client_side_caching.py")
METRICS = ("us_per_op", "p50_us")


def child(argv):
    """Run client_side_caching.py with redis imported from argv[0]."""
    tree, tool_args = argv[0], argv[1:]
    worker.import_redis(tree)
    spec = importlib.util.spec_from_file_location("client_side_caching", CSC_TOOL)
    module = importlib.util.module_from_spec(spec)
    # dataclasses resolve the tool's string annotations through sys.modules
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    sys.argv = [CSC_TOOL, *tool_args]
    return module.main()


def tool_args(args, mode, workload, cache):
    port = args.cluster_port if mode == "cluster" else args.port
    return [
        "--mode",
        mode,
        "--host",
        args.host,
        "--port",
        str(port),
        "--protocol",
        "3",
        "--client-side-cache",
        cache,
        "--workload",
        workload,
        "--duration",
        str(args.duration),
        "--warmup",
        str(args.warmup),
        "--clients",
        str(args.clients),
        "--key-range",
        str(args.key_range),
        "--json",
    ]


def run_once(args, tree, mode, workload, cache):
    cmd = [
        args.python,
        "-I",
        os.path.abspath(__file__),
        "--child",
        tree,
        *tool_args(args, mode, workload, cache),
    ]
    start = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        raise SystemExit(f"{' '.join(cmd)} failed:\n{proc.stdout}\n{proc.stderr}")
    summary = json.loads(proc.stdout)
    return {
        # time per operation of one client thread
        "us_per_op": 1e6 * summary["clients"] / summary["throughput_ops_per_sec"],
        "p50_us": summary["p50_latency_ms"] * 1e3,
        "wall_s": round(time.perf_counter() - start, 2),
    }


def main():
    if sys.argv[1:2] == ["--child"]:
        return child(sys.argv[2:])
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    p.add_argument("--trees", nargs="+", required=True)
    p.add_argument("--python", default=sys.executable)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--cluster-port", type=int, default=16379)
    p.add_argument(
        "--mode", nargs="+", choices=["standalone", "cluster"], default=["standalone"]
    )
    p.add_argument(
        "--workload",
        nargs="+",
        choices=["read-heavy", "write-heavy"],
        default=["read-heavy", "write-heavy"],
    )
    p.add_argument("--cache", nargs="+", choices=["on", "off"], default=["on", "off"])
    p.add_argument("--duration", type=float, default=3.0)
    p.add_argument("--warmup", type=float, default=1.0)
    p.add_argument("--clients", type=int, default=1)
    # every run preloads, primes and deletes the keys one command at a time;
    # 1000 keys keep that setup short and still fit in the cache
    p.add_argument("--key-range", type=int, default=1000)
    p.add_argument(
        "--passes",
        type=int,
        default=6,
        help="runs per tree and config; their spread sets the uncertainty of a delta",
    )
    p.add_argument(
        "--output-dir",
        default=os.path.join(HERE, "results"),
        help="where the JSON and Markdown results are written",
    )
    gate.add_gate_args(p)
    args = p.parse_args()
    trees = {k: os.path.abspath(v) for k, v in (t.split("=", 1) for t in args.trees)}
    pairs = gate.compare_pairs(args, trees)
    heads = {k: run_bench.git_head(v) for k, v in trees.items()}
    configs = [
        (mode, workload, cache)
        for mode in args.mode
        for workload in args.workload
        for cache in args.cache
    ]
    # results[config][tree] -> list of {metric: value}, one per pass
    results = {f"{m}/{w}/cache={c}": {t: [] for t in trees} for m, w, c in configs}
    total_start = time.perf_counter()
    for i in range(args.passes):
        for mode, workload, cache in configs:
            cfg = f"{mode}/{workload}/cache={cache}"
            for label in run_bench.tree_order(trees, i):
                results[cfg][label].append(
                    run_once(args, trees[label], mode, workload, cache)
                )
        print(f"pass {i + 1}/{args.passes} done", file=sys.stderr)

    def values(cfg, tree, metric):
        return [r[metric] for r in results[cfg][tree]]

    lines = [
        f"Client-side cache, microseconds (median of {args.passes} passes, "
        f"{args.clients} client thread(s), {args.duration:g}s per run)",
        "",
        "Trees: " + ", ".join(f"`{k}`={heads[k]}" for k in trees),
        "",
        "| config | metric | " + " | ".join(trees) + " |",
        "|" + "---|" * (2 + len(trees)),
    ]
    for cfg in results:
        for metric in METRICS:
            row = [
                f"{statistics.median(values(cfg, t, metric)):.2f} "
                f"(±{run_bench.rel_iqr(values(cfg, t, metric)) * 100:.0f}%)"
                for t in trees
            ]
            lines.append(f"| {cfg} | {metric} | " + " | ".join(row) + " |")
    lines += [
        "",
        "## Execution times",
        "",
        f"Whole run: {time.perf_counter() - total_start:.0f}s. Median seconds per run:",
        "",
    ]
    for cfg in results:
        row = [f"{t}={statistics.median(values(cfg, t, 'wall_s')):.1f}s" for t in trees]
        lines.append(f"- {cfg}: " + ", ".join(row))
    text = "\n".join(lines)
    print(text)
    summaries = [
        gate.render(
            "Client-side cache",
            gate.evaluate(
                [
                    {
                        "config": cfg,
                        "scenario": metric,
                        "old_samples": values(cfg, old, metric),
                        "new_samples": values(cfg, new, metric),
                    }
                    for cfg in results
                    for metric in METRICS
                ],
                args.allowed_regression,
                args.max_limit,
            ),
            new,
            old,
            args.allowed_regression,
            "microseconds",
            args.max_limit,
            inconclusive_fails=not args.warn_inconclusive,
        )
        for new, old in pairs
    ]
    run_bench.save(
        args,
        heads,
        {
            "passes": results,
            "report": text + "\n\n" + "\n\n".join(s for s, _, _ in summaries),
        },
        suffix="csc",
    )
    gate.finish(args, summaries)


if __name__ == "__main__":
    sys.exit(main())
