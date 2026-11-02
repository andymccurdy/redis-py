"""Print the Benchmarks workflow's job matrix as JSON.

One job each for peak memory, the parser-only benchmark, the
observability recorder benchmark and the client-side cache on each topology, and one run_bench job per config: every client, topology, pool class, parser,
protocol and socket timeout combination run_bench supports. The workflow
reads it from the `matrix` output of its plan job, so the grid lives here
and in run_bench.py, not in the YAML.

  python benchmarks/measure_read_path/ci_matrix.py
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import run_bench  # noqa: E402

POOL_NAMES = {"connection": "PlainPool", "blocking": "BlockingPool"}


def jobs():
    out = [
        {"name": "Peak memory tests", "tool": "mem_probe", "args": "--passes 6"},
        {"name": "Parser only tests", "tool": "parser_micro", "args": "--passes 16"},
        {
            "name": "Observability recorder tests",
            "tool": "recorder_micro",
            "args": "--passes 16",
        },
        {
            "name": "Client-side cache; standalone",
            "tool": "csc_bench",
            "args": "--mode standalone --passes 8",
        },
        {
            "name": "Client-side cache; cluster",
            "tool": "csc_bench",
            "args": "--mode cluster --passes 8",
        },
    ]
    every = argparse.Namespace(
        client=["async", "sync"],
        topology=["standalone", "cluster"],
        pool=["connection", "blocking"],
        grid=run_bench.DEFAULT_GRID,
        uvloop=False,
    )
    for client, topology, parser, protocol, timeout, _, pool in run_bench.build_configs(
        every
    ):
        if client == "async" and topology == "cluster":
            # connections are managed per node, without a pool class
            pool_name = "NodePool"
        else:
            pool_name = POOL_NAMES[pool]
        timeout_name = "None" if timeout == "none" else timeout
        out.append(
            {
                "name": f"Time; {client.capitalize()}; {topology}; {pool_name}; "
                f"{parser} parser; RESP{protocol}; socket_timeout={timeout_name}",
                "tool": "run_bench",
                "args": f"--client {client} --topology {topology} --pool {pool} "
                f"--grid {parser}:{protocol}:{timeout}",
            }
        )
    return out


if __name__ == "__main__":
    print(json.dumps({"include": jobs()}))
