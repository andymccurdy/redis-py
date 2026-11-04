"""A/B benchmark of the read path across redis-py checkouts.

Each round runs worker.py once per (config, tree) in a fresh interpreter,
with the tree order rotated per round so drift (thermal, other load) is
spread evenly. The reported value per cell is the median over rounds of the
worker's median time per operation, shown with the relative IQR across
rounds. The report also lists how long every scenario, worker run and the
whole benchmark took.

With --compare, the run ends with a regression summary per pair (see
gate.py). Cells that regress or are inconclusive get --recheck-rounds more
rounds before the verdict, so a single noisy round cannot fail the run, and
with --recheck-passes the cells still flagged get them again: more rounds
narrow the uncertainty of a delta.

Run from the repository root, against a Redis server that nothing else is
using:

  python -m benchmarks.measure_read_path.run_bench \\
      --trees master=../redis-py-bench-master pr=../redis-py-bench-pr \\
      --compare pr:master --fail-on-regression
  ... --grid python:3:5 hiredis:3:none --rounds 8
  ... --client async sync --topology standalone cluster
  ... --pool blocking    # BlockingConnectionPool; --pool connection blocking runs both
  ... --count-arms       # timeouts armed per operation, no timing (async only)

Every combination of --client, --topology, --pool, --grid and --uvloop runs,
except the ones that do not exist: the async cluster client has no pool
class, and uvloop applies to the async client only. The cluster configs use
--cluster-port (default 16379, the cluster `invoke devenv` starts).
"""

import argparse
import datetime
import json
import os
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import gate  # noqa: E402

# (parser, protocol, socket_timeout); 5 is the library default socket_timeout
DEFAULT_GRID = [
    (parser, protocol, timeout)
    for parser in ("python", "hiredis")
    for protocol in (3, 2)
    for timeout in ("5", "none")
]


def parse_grid_entry(entry):
    parser, protocol, timeout = entry.split(":")
    if parser not in ("python", "hiredis") or protocol not in ("2", "3"):
        raise argparse.ArgumentTypeError(f"bad grid entry {entry!r}")
    return parser, int(protocol), timeout


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--trees",
        nargs="+",
        required=True,
        help="label=path entries, each path a redis-py checkout",
    )
    p.add_argument("--python", default=sys.executable)
    p.add_argument("--rounds", type=int, default=5)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument(
        "--recheck-rounds",
        type=int,
        default=3,
        help="extra rounds for regressed cells before the verdict (0 = off)",
    )
    p.add_argument(
        "--recheck-passes",
        type=int,
        default=1,
        help="recheck again, up to this many times, while cells stay "
        "regressed or inconclusive; more rounds narrow their uncertainty",
    )
    p.add_argument(
        "--grid",
        nargs="+",
        type=parse_grid_entry,
        default=DEFAULT_GRID,
        metavar="PARSER:PROTOCOL:TIMEOUT",
        help="configs to run, e.g. python:3:5 hiredis:3:none (default: all eight)",
    )
    p.add_argument(
        "--client",
        nargs="+",
        choices=["async", "sync"],
        default=["async"],
        help="clients to run (default: async)",
    )
    p.add_argument(
        "--topology",
        nargs="+",
        choices=["standalone", "cluster"],
        default=["standalone"],
        help="topologies to run (default: standalone)",
    )
    p.add_argument("--uvloop", action="store_true", help="also run under uvloop")
    p.add_argument(
        "--pool",
        nargs="+",
        choices=["connection", "blocking"],
        default=["connection"],
        help="connection pool classes to run (default: connection)",
    )
    p.add_argument("--scenarios", default="all")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--db", type=int, default=13)
    p.add_argument("--cluster-port", type=int, default=16379)
    p.add_argument("--count-arms", action="store_true")
    p.add_argument(
        "--output-dir",
        default=os.path.join(HERE, "results"),
        help="where the JSON and Markdown results are written",
    )
    gate.add_gate_args(p)
    return p.parse_args()


def git_head(path):
    return subprocess.run(
        ["git", "-C", path, "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def run_worker(args, tree, extra, scenarios=None):
    # -I: the worker must import redis only from `tree`, never from the
    # current directory or an editable install of another checkout
    cmd = [
        args.python,
        "-I",
        os.path.join(HERE, "worker.py"),
        "--tree",
        tree,
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--db",
        str(args.db),
        "--repeats",
        str(args.repeats),
        "--scenarios",
        scenarios or args.scenarios,
        *extra,
    ]
    start = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        raise SystemExit(f"worker failed: {' '.join(cmd)}\n{proc.stderr}")
    return json.loads(proc.stdout.strip().splitlines()[-1]), elapsed


def tree_order(trees, i):
    """The tree labels for run i, rotated so that over len(trees) runs every
    tree runs in every position once. Drift between consecutive runs
    (thermal, CPU frequency, other load) then falls evenly on the trees
    instead of mostly on one of them."""
    labels = list(trees)
    k = i % len(labels)
    return labels[k:] + labels[:k]


def topology_args(args, topology):
    if topology == "cluster":
        # a cluster has only db 0
        return ["--topology", topology, "--port", str(args.cluster_port), "--db", "0"]
    return ["--topology", topology, "--port", str(args.port), "--db", str(args.db)]


def config_args(args, client, topology, parser, protocol, timeout, loop, pool):
    return [
        "--client",
        client,
        *topology_args(args, topology),
        "--parser",
        parser,
        "--protocol",
        str(protocol),
        "--socket-timeout",
        timeout,
        "--loop",
        loop,
        "--pool",
        pool,
    ]


def config_label(client, topology, parser, protocol, timeout, loop, pool):
    label = f"{client}/{topology}/{pool}/{parser}/resp{protocol}/timeout={timeout}"
    return label + f"/{loop}" if client == "async" else label


def build_configs(args):
    loops = ["asyncio", "uvloop"] if args.uvloop else ["asyncio"]
    return [
        (client, topology, *c, loop, pool)
        for client in args.client
        for topology in args.topology
        for pool in args.pool
        for loop in loops
        for c in args.grid
        if not (client == "async" and topology == "cluster" and pool != "connection")
        and not (client == "sync" and loop != "asyncio")
    ]


def rel_iqr(values):
    if len(values) < 4:
        return (max(values) - min(values)) / statistics.median(values)
    # inclusive: the quartiles stay within the samples; the default
    # exclusive method pushes them out to the extremes on a handful of
    # rounds, which inflates the spread shown in the report
    q = statistics.quantiles(values, n=4, method="inclusive")
    return (q[2] - q[0]) / statistics.median(values)


class Run:
    """Samples, execution times and worker timings of one benchmark run."""

    def __init__(self, args, trees):
        self.args = args
        self.trees = trees
        self.rounds_run = 0
        # samples[config][tree][scenario] -> list of per-round medians
        self.samples = {}
        # wall[config][tree][scenario] -> list of per-round execution times
        self.wall = {}
        self.worker_times = []
        self.extra_rounds = 0

    def round(self, configs, scenarios_for=None):
        """Run every tree once per config, in a rotated order. scenarios_for
        maps a config label to the scenarios to run (default: all)."""
        for cfg in configs:
            label_cfg = config_label(*cfg)
            scenarios = scenarios_for.get(label_cfg) if scenarios_for else None
            for label in tree_order(self.trees, self.rounds_run):
                res, elapsed = run_worker(
                    self.args,
                    self.trees[label],
                    config_args(self.args, *cfg),
                    scenarios,
                )
                self.worker_times.append(elapsed)
                for sc, v in res["per_op_us"].items():
                    self.samples.setdefault(label_cfg, {}).setdefault(
                        label, {}
                    ).setdefault(sc, []).append(v)
                for sc, v in res["wall_s"].items():
                    self.wall.setdefault(label_cfg, {}).setdefault(
                        label, {}
                    ).setdefault(sc, []).append(v)
        self.rounds_run += 1

    def cells(self, new, old):
        out = []
        for cfg, per_tree in self.samples.items():
            for sc in per_tree[old]:
                out.append(
                    {
                        "config": cfg,
                        "scenario": sc,
                        "old_samples": per_tree[old][sc],
                        "new_samples": per_tree[new][sc],
                    }
                )
        return out


def main():
    args = parse_args()
    trees = {k: os.path.abspath(v) for k, v in (t.split("=", 1) for t in args.trees)}
    pairs = gate.compare_pairs(args, trees)
    heads = {k: git_head(v) for k, v in trees.items()}
    configs = build_configs(args)
    if not configs:
        raise SystemExit("no config left: the async cluster client has no pool class")
    topologies = sorted({c[1] for c in configs})
    any_tree = next(iter(trees.values()))

    print("trees:", ", ".join(f"{k}={heads[k]}" for k in trees), file=sys.stderr)
    total_start = time.perf_counter()
    for topology in topologies:
        run_worker(args, any_tree, ["--setup", *topology_args(args, topology)])
    try:
        if args.count_arms:
            configs = [c for c in configs if c[0] == "async"]
            report_arms(args, trees, heads, configs, total_start)
            return
        run = Run(args, trees)
        for rnd in range(args.rounds):
            round_start = time.perf_counter()
            run.round(configs)
            print(
                f"round {rnd + 1}/{args.rounds} done in "
                f"{time.perf_counter() - round_start:.0f}s",
                file=sys.stderr,
            )
        recheck(args, run, pairs, configs)
    finally:
        for topology in topologies:
            run_worker(args, any_tree, ["--teardown", *topology_args(args, topology)])
    total = time.perf_counter() - total_start
    report(args, run, heads, pairs, total)


def recheck(args, run, pairs, configs):
    """Give the regressed and inconclusive cells more rounds, so the verdict
    on them rests on more samples instead of a few noisy rounds. The
    uncertainty of a delta narrows as rounds are added, so a cell that is
    still flagged is rechecked again, up to --recheck-passes times."""
    if not args.recheck_rounds or not pairs:
        return
    for _ in range(args.recheck_passes):
        flagged = {}
        for new, old in pairs:
            for c in gate.evaluate(
                run.cells(new, old), args.allowed_regression, args.max_limit
            ):
                if c["regressed"] or c["inconclusive"]:
                    flagged.setdefault(c["config"], set()).add(c["scenario"])
        if not flagged:
            return
        scenarios_for = {cfg: ",".join(sorted(sc)) for cfg, sc in flagged.items()}
        flagged_configs = [c for c in configs if config_label(*c) in flagged]
        print(
            f"rechecking {sum(len(s) for s in flagged.values())} regressed or "
            "inconclusive cell(s) "
            f"with {args.recheck_rounds} more rounds",
            file=sys.stderr,
        )
        for _ in range(args.recheck_rounds):
            run.round(flagged_configs, scenarios_for)
        run.extra_rounds += args.recheck_rounds


def report(args, run, heads, pairs, total):
    trees = run.trees
    lines = [
        f"# Read-path benchmark ({datetime.datetime.now():%Y-%m-%d %H:%M})",
        "",
        "Trees: " + ", ".join(f"`{k}`={heads[k]}" for k in trees),
        f"Rounds: {args.rounds} (up to +{run.extra_rounds} recheck rounds for "
        f"regressed and inconclusive cells), repeats per round: {args.repeats}. "
        "Values are median microseconds per operation, with the relative IQR "
        "across rounds; the gate's limit comes from the uncertainty of the "
        "delta.",
        "",
    ]
    header = "| config | scenario | " + " | ".join(trees) + " |"
    if pairs:
        header += " " + " | ".join(f"{a} vs {b}" for a, b in pairs) + " |"
    lines += [header, "|" + "---|" * (2 + len(trees) + len(pairs))]
    evaluated = {
        (new, old): {
            (c["config"], c["scenario"]): c
            for c in gate.evaluate(
                run.cells(new, old), args.allowed_regression, args.max_limit
            )
        }
        for new, old in pairs
    }
    for cfg, per_tree in run.samples.items():
        for sc in next(iter(per_tree.values())):
            row = [cfg, sc]
            row += [
                f"{statistics.median(per_tree[t][sc]):.1f} "
                f"(±{rel_iqr(per_tree[t][sc]) * 100:.0f}%)"
                for t in trees
            ]
            for pair in pairs:
                c = evaluated[pair][(cfg, sc)]
                if c["regressed"]:
                    flag = " **slower**"
                elif c["inconclusive"]:
                    flag = " **too noisy**"
                elif c["delta"] < -c["limit"]:
                    flag = " faster"
                else:
                    flag = ""
                row.append(f"{c['delta'] * 100:+.1f}%{flag}")
            lines.append("| " + " | ".join(row) + " |")
    lines += execution_times(trees, run.wall, run.worker_times, total)
    summaries = [
        gate.render(
            "Read path, time per operation",
            list(evaluated[(new, old)].values()),
            new,
            old,
            args.allowed_regression,
            "microseconds per operation",
            args.max_limit,
            inconclusive_fails=not args.warn_inconclusive,
        )
        for new, old in pairs
    ]
    text = "\n".join(lines)
    print(text)
    save(
        args,
        heads,
        {
            "samples": run.samples,
            "wall_s": run.wall,
            "report": text + "\n\n" + "\n\n".join(s for s, _, _ in summaries),
        },
    )
    gate.finish(args, summaries)


def execution_times(trees, wall, worker_times, total):
    """Execution time of every scenario, worker run and the whole run."""
    lines = [
        "",
        "## Execution times",
        "",
        f"Whole benchmark: {total / 60:.1f} min; {len(worker_times)} worker "
        f"runs, median {statistics.median(worker_times):.1f}s, "
        f"max {max(worker_times):.1f}s (each includes interpreter start).",
        "",
        "Median seconds per scenario execution (warmup plus repeats), and "
        "the total across all rounds:",
        "",
        "| config | scenario | " + " | ".join(trees) + " | total |",
        "|" + "---|" * (3 + len(trees)),
    ]
    for cfg, per_tree in wall.items():
        for sc in next(iter(per_tree.values())):
            cells = [f"{statistics.median(per_tree[t][sc]):.2f}" for t in trees]
            scenario_total = sum(sum(per_tree[t][sc]) for t in trees)
            lines.append(
                f"| {cfg} | {sc} | " + " | ".join(cells) + f" | {scenario_total:.0f}s |"
            )
    return lines


def report_arms(args, trees, heads, configs, total_start):
    lines = [
        "| config | scenario | " + " | ".join(trees) + " |",
        "|" + "---|" * (2 + len(trees)),
    ]
    out = {}
    for cfg in configs:
        res = {
            t: run_worker(args, p, config_args(args, *cfg) + ["--count-arms"])[0]
            for t, p in trees.items()
        }
        out[config_label(*cfg)] = res
        for sc in next(iter(res.values()))["arms_per_op"]:
            lines.append(
                f"| {config_label(*cfg)} | {sc} | "
                + " | ".join(f"{res[t]['arms_per_op'][sc]:.2f}" for t in trees)
                + " |"
            )
    total = time.perf_counter() - total_start
    # informational only: arming more timeouts can be intended behavior
    text = (
        "Timeouts armed per operation (informational, never fails the run)\n\n"
        + "\n".join(lines)
        + f"\n\nExecution time: {total:.0f}s"
    )
    print(text)
    if args.summary:
        with open(args.summary, "a") as f:
            f.write("### " + text + "\n\n")
    save(args, heads, {"arms": out, "report": text}, suffix="arms")


def save(args, heads, payload, suffix="timing"):
    os.makedirs(args.output_dir, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    tag = "-".join(f"{k}{v}" for k, v in heads.items())
    # a label like feature/parser would name a directory that does not exist
    tag = "".join(c if c.isalnum() or c in "._-" else "_" for c in tag)
    path = os.path.join(args.output_dir, f"{stamp}-{suffix}-{tag}.json")
    with open(path, "w") as f:
        json.dump({"heads": heads, **payload}, f, indent=1)
    with open(path[:-5] + ".md", "w") as f:
        f.write(payload["report"] + "\n")
    print(f"saved {path}", file=sys.stderr)


if __name__ == "__main__":
    main()
