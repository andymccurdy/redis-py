"""
Benchmark for the sync connection handshake (``Connection.on_connect``).

Opening a connection runs a short sequence of setup commands -- HELLO/AUTH,
CLIENT SETNAME, CLIENT SETINFO (LIB-NAME / LIB-VER) and SELECT. This benchmark
times how long a full connection handshake takes, averaged over many fresh
connections, and reports latency percentiles.

The handshake cost is dominated by network round-trips, so on localhost (where
RTT is effectively zero) the difference between a sequential and a pipelined
handshake is hard to see. Pass ``--delay-ms`` to inject an artificial per
round-trip latency: the injected delay is charged once per *round-trip* (the
first socket read after a write), so a handshake that issues N sequential
send/read round-trips pays roughly N * delay, while a pipelined handshake that
collapses several commands into one write + batched reads pays it far fewer
times. This makes the round-trip reduction observable without a remote server.

To measure the improvement of a change, run this script on the baseline commit
and on the change (same arguments) and compare the reported means, e.g.:

    python -m benchmarks.connection_handshake --n 2000 --client-name bench --db 9 --delay-ms 1

Assumes a Redis server reachable at --host/--port (defaults to localhost:6379).
Not part of the CI test matrix.
"""

import argparse
import statistics
import time

import redis
from redis.maint_notifications import MaintNotificationsConfig


class _LatencySocket:
    """Socket proxy that charges ``delay`` seconds once per round-trip.

    A round-trip is modeled as: one or more writes, then the first read that
    follows them (the wait for the reply to come back). Replies that are already
    in flight -- i.e. subsequent reads with no intervening write, as produced by
    a pipelined batch -- are served without additional delay.
    """

    def __init__(self, sock, delay):
        self._sock = sock
        self._delay = delay
        self._pending = False

    def sendall(self, data, *args, **kwargs):
        self._pending = True
        return self._sock.sendall(data, *args, **kwargs)

    def send(self, data, *args, **kwargs):
        self._pending = True
        return self._sock.send(data, *args, **kwargs)

    def _wait_for_reply(self):
        if self._pending:
            time.sleep(self._delay)
            self._pending = False

    def recv(self, *args, **kwargs):
        self._wait_for_reply()
        return self._sock.recv(*args, **kwargs)

    def recv_into(self, *args, **kwargs):
        self._wait_for_reply()
        return self._sock.recv_into(*args, **kwargs)

    def __getattr__(self, name):
        # Delegate everything else (settimeout, close, fileno, ...) to the socket.
        return getattr(self._sock, name)


class _LatencyConnection(redis.Connection):
    """Connection whose socket injects an artificial per round-trip latency."""

    def __init__(self, *args, latency_seconds=0.0, **kwargs):
        self._latency_seconds = latency_seconds
        super().__init__(*args, **kwargs)

    def _connect(self):
        sock = super()._connect()
        if self._latency_seconds > 0:
            sock = _LatencySocket(sock, self._latency_seconds)
        return sock


def _percentile(sorted_values, pct):
    if not sorted_values:
        return float("nan")
    k = (len(sorted_values) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


def run(args):
    conn_kwargs = dict(
        host=args.host,
        port=args.port,
        db=args.db,
        protocol=args.protocol,
        client_name=args.client_name or None,
    )
    if args.username:
        conn_kwargs["username"] = args.username
    if args.password:
        conn_kwargs["password"] = args.password
    if args.delay_ms > 0:
        conn_kwargs["latency_seconds"] = args.delay_ms / 1000.0

    if args.maint_notifications != "off":
        # Maintenance notifications require RESP3; the connection would otherwise
        # raise when configuring the (RESP2) parser.
        if args.protocol != 3:
            raise SystemExit("--maint-notifications requires --protocol 3 (RESP3).")
        # enabled="auto" still sends CLIENT MAINT_NOTIFICATIONS (so its cost is
        # measured) but tolerates servers that don't support it; enabled=True
        # (mode "on") fails the connection if the server rejects the command.
        enabled = True if args.maint_notifications == "on" else "auto"
        conn_kwargs["maint_notifications_config"] = MaintNotificationsConfig(
            enabled=enabled
        )

    connection_cls = _LatencyConnection if args.delay_ms > 0 else redis.Connection

    latencies_ms = []
    for _ in range(args.n):
        conn = connection_cls(**conn_kwargs)
        start = time.perf_counter()
        conn.connect()
        latencies_ms.append((time.perf_counter() - start) * 1000.0)
        conn.disconnect()

    latencies_ms.sort()
    print(f"Connection handshake benchmark ({args.n} connections)")
    print(
        f"  host={args.host}:{args.port} db={args.db} protocol={args.protocol} "
        f"client_name={args.client_name or ''!r} delay_ms={args.delay_ms} "
        f"maint_notifications={args.maint_notifications}"
    )
    print(f"  mean   : {statistics.fmean(latencies_ms):.3f} ms")
    print(f"  median : {statistics.median(latencies_ms):.3f} ms")
    print(f"  p95    : {_percentile(latencies_ms, 95):.3f} ms")
    print(f"  p99    : {_percentile(latencies_ms, 99):.3f} ms")
    print(f"  min    : {latencies_ms[0]:.3f} ms")
    print(f"  max    : {latencies_ms[-1]:.3f} ms")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--username", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--protocol", type=int, default=3, choices=(2, 3))
    parser.add_argument("--client-name", default="")
    parser.add_argument("--db", type=int, default=0)
    parser.add_argument(
        "--n", type=int, default=1000, help="number of fresh connections to time"
    )
    parser.add_argument(
        "--delay-ms",
        type=float,
        default=0.0,
        help="artificial per round-trip latency (ms) to make the handshake "
        "round-trip count visible on localhost",
    )
    parser.add_argument(
        "--maint-notifications",
        choices=("off", "auto", "on"),
        default="off",
        help="enable maintenance notifications during the handshake (requires "
        "--protocol 3). 'auto' sends CLIENT MAINT_NOTIFICATIONS but tolerates "
        "servers that reject it; 'on' fails the connection if it is rejected. "
        "With this change the command is pipelined into the handshake, so "
        "enabling it does not add a round-trip.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
