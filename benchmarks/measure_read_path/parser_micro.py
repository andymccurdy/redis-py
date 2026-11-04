"""Parser-only microbenchmark: no network, one reply per iteration.

Feeds a whole reply to every parser and times it reading the reply, so
per-call overhead in the parser is not hidden behind round-trip latency. Two
replies: an LRANGE-style array of 10k bulk strings (many line reads, few
socket reads) and one 10 MB bulk string (one line read, about 160 socket
reads of 64 KB). The async parsers read from an in-memory StreamReader and
the sync parsers from an in-memory socket; both are attached through the
parser's own on_connect(). Covered: the pure-Python RESP2 and RESP3 parsers
and the hiredis parser, sync and async. The async socket timeout is armed by
Connection.read_response(), not by the parser, so it is out of scope here;
run_bench times both timeouts through the connection. Each tree runs in its
own interpreter.

Only the OS socket is replaced; every parser, its on_connect() and
read_response(), the pure-Python SocketBuffer and the hiredis reader are
the library code of the tree under test. A real socket (a socketpair with a
writer thread) would add kernel copies, syscalls and thread scheduling,
which cost more than parsing a 10 MB reply and swing a lot on a shared
runner. run_bench and mem_probe measure that real path through Connection,
real sockets and a Redis server.

Each parser still reads the way it does on a real socket: the pure-Python
parsers call recv(), which allocates a new bytes object per read that
SocketBuffer then copies into its BytesIO, and hiredis calls recv_into()
into its preallocated buffer and feeds that to the C reader. So the
user-space difference between them, an allocation and an extra copy per
read, is timed. What is not:

- the syscall per read, about the same cost for recv() and recv_into();
- short reads: a real socket returns what has arrived, often less than
  requested, while the in-memory one always fills the request, so a large
  reply takes fewer reads here than over a network. More reads cost the
  pure-Python SocketBuffer more than hiredis, so this hides part of the
  per-read overhead of the pure-Python parsers;
- the async transport delivering a reply in chunks as it arrives: the
  whole reply is fed to the StreamReader at once, though each async parser
  still reads it with its own calls (readline() and readexactly() for the
  pure-Python ones, read() for hiredis).

Run from the repository root:

  python -m benchmarks.measure_read_path.parser_micro \\
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
from types import SimpleNamespace

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import gate  # noqa: E402
import run_bench  # noqa: E402
import worker  # noqa: E402

N = 10_000
BULK = 10 * 1024 * 1024
# scenario -> (wire bytes, expected reply), compared in full after timing so
# a parser that corrupts the content fails instead of looking fast
REPLIES = {
    "lrange_10k_parse": (
        b"*%d\r\n" % N + b"".join(b"$10\r\nitem%06d\r\n" % i for i in range(N)),
        [b"item%06d" % i for i in range(N)],
    ),
    "get_10mb_parse": (b"$%d\r\n" % BULK + b"x" * BULK + b"\r\n", b"x" * BULK),
}


class BufferSocket:
    """The part of a socket the sync parsers use, reading from bytes.

    It stands in for the OS socket only, so the library's parsers run
    unchanged against it. recv() returns a new bytes object and recv_into()
    copies into the caller's buffer, as a real socket does, so the
    pure-Python and hiredis parsers keep their different read paths. Unlike
    a real socket it always fills the requested size; see the module
    docstring for what that leaves out.
    """

    def __init__(self, data):
        self._view = memoryview(data)
        self._pos = 0

    def recv(self, size):
        chunk = self._view[self._pos : self._pos + size]
        self._pos += len(chunk)
        return chunk.tobytes()

    def recv_into(self, buffer, nbytes=0):
        size = min(nbytes or len(buffer), len(self._view) - self._pos)
        buffer[:size] = self._view[self._pos : self._pos + size]
        self._pos += size
        return size

    def settimeout(self, timeout):
        pass


def encoder():
    from redis._parsers.encoders import Encoder

    return Encoder("utf-8", "strict", False)


async def time_async(parser_class, wire, expected, rounds):
    parser = parser_class(65536)
    samples = []
    # the first read is a warmup: it settles the allocator for the reply size
    for i in range(rounds + 1):
        reader = asyncio.StreamReader(limit=2**30)
        reader.feed_data(wire)
        parser.on_connect(
            SimpleNamespace(_reader=reader, encoder=encoder(), socket_timeout=None)
        )
        start = time.perf_counter()
        res = await parser.read_response(disable_decoding=True)
        if i:
            samples.append(time.perf_counter() - start)
        assert res == expected
    return statistics.median(samples) * 1e3


def time_sync(parser_class, wire, expected, rounds):
    samples = []
    # the first read is a warmup: it settles the allocator for the reply size
    for i in range(rounds + 1):
        parser = parser_class(65536)
        parser.on_connect(
            SimpleNamespace(
                _sock=BufferSocket(wire), encoder=encoder(), socket_timeout=5.0
            )
        )
        start = time.perf_counter()
        res = parser.read_response(disable_decoding=True)
        if i:
            samples.append(time.perf_counter() - start)
        assert res == expected
        parser.on_disconnect()
    return statistics.median(samples) * 1e3


def timed(fn):
    start = time.perf_counter()
    ms = fn()
    return {
        # hiredis reads the array in about 0.2 ms; keep enough digits that
        # rounding does not show up as noise
        "ms_per_reply": round(ms, 4),
        "wall_s": round(time.perf_counter() - start, 2),
    }


def child(args):
    worker.import_redis(args.tree)
    from redis._parsers import (
        _AsyncHiredisParser,
        _AsyncRESP2Parser,
        _AsyncRESP3Parser,
        _HiredisParser,
        _RESP2Parser,
        _RESP3Parser,
    )
    from redis.utils import HIREDIS_AVAILABLE

    if not HIREDIS_AVAILABLE:
        raise SystemExit("hiredis is not installed for " + args.python)
    async_parsers = {
        "python/resp2": _AsyncRESP2Parser,
        "python/resp3": _AsyncRESP3Parser,
        "hiredis": _AsyncHiredisParser,
    }
    sync_parsers = {
        "python/resp2": _RESP2Parser,
        "python/resp3": _RESP3Parser,
        "hiredis": _HiredisParser,
    }
    # out[config][scenario] -> result
    out = {}
    for name, parser_class in async_parsers.items():
        out[f"async/{name}"] = {
            scenario: timed(
                lambda: asyncio.run(
                    time_async(parser_class, wire, expected, args.rounds)
                )
            )
            for scenario, (wire, expected) in REPLIES.items()
        }
    for name, parser_class in sync_parsers.items():
        out[f"sync/{name}"] = {
            scenario: timed(
                lambda: time_sync(parser_class, wire, expected, args.rounds)
            )
            for scenario, (wire, expected) in REPLIES.items()
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
    p.add_argument("--rounds", type=int, default=30)
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

    def values(tree, cfg, scenario, key="ms_per_reply"):
        return [r[cfg][scenario][key] for r in passes[tree]]

    lines = [
        f"Parser only, milliseconds per reply (median of {args.passes} passes)",
        "",
        "Trees: " + ", ".join(f"`{k}`={heads[k]}" for k in trees),
        "",
        "| config | scenario | " + " | ".join(trees) + " |",
        "|" + "---|" * (2 + len(trees)),
    ]
    for cfg, scenario in cells:
        row = [f"{statistics.median(values(t, cfg, scenario)):.3f} ms" for t in trees]
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
            "Parser only",
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
            "milliseconds per reply",
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
        suffix="parser",
    )
    gate.finish(args, summaries)


if __name__ == "__main__":
    main()
