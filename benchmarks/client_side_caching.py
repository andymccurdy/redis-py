#!/usr/bin/env python3
"""
Client-side caching benchmark for redis-py.

The benchmark compares the same GET/SET workload with redis-py client-side
caching enabled or disabled. It supports standalone Redis and Redis Cluster,
and prints an extractable JSON summary with throughput and latency percentiles.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import redis
from redis.cache import CacheConfig
from redis.cluster import ClusterNode, RedisCluster

READ_RATIOS = {
    "read-heavy": 0.95,
    "write-heavy": 0.20,
}


@dataclass
class WorkerResult:
    read_operations: int = 0
    write_operations: int = 0
    errors: int = 0
    first_error: str | None = None
    latencies_ms: list[float] | None = None
    sampled_operations: int = 0
    min_latency_ms: float = float("inf")
    max_latency_ms: float = 0.0

    def __post_init__(self) -> None:
        if self.latencies_ms is None:
            self.latencies_ms = []


@dataclass
class BenchmarkSummary:
    mode: str
    client_side_cache: str
    workload: str
    read_ratio: float
    duration_seconds: float
    clients: int
    key_range: int
    value_size: int
    total_operations: int
    read_operations: int
    write_operations: int
    errors: int
    warmup_errors: int
    total_errors: int
    throughput_ops_per_sec: float
    min_latency_ms: float
    max_latency_ms: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    latency_samples: int
    cache_sizes: list[int]
    first_error: str | None
    warmup_first_error: str | None
    metadata: dict[str, Any]


def parse_on_off(value: str) -> str:
    lowered = value.lower()
    if lowered not in {"on", "off"}:
        raise argparse.ArgumentTypeError("value must be 'on' or 'off'")
    return lowered


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    lowered = value.lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"{value!r} is not a boolean value")


def parse_cluster_nodes(raw_nodes: str | None) -> list[ClusterNode]:
    if not raw_nodes:
        return []

    nodes = []
    for raw_node in raw_nodes.split(","):
        host, port = raw_node.strip().rsplit(":", 1)
        nodes.append(ClusterNode(host, int(port)))
    return nodes


def key_name(prefix: str, key_id: int) -> str:
    return f"{prefix}:{key_id}"


def build_connection_kwargs(
    args: argparse.Namespace, *, csc_enabled: bool
) -> dict[str, Any]:
    if csc_enabled and args.protocol != 3:
        raise ValueError("Client-side caching requires RESP3; use --protocol 3")

    kwargs: dict[str, Any] = {
        "username": args.username,
        "password": args.password,
        "decode_responses": False,
        "socket_timeout": args.socket_timeout,
        "socket_connect_timeout": args.socket_connect_timeout,
        "max_connections": args.max_connections,
        "protocol": args.protocol,
    }

    if args.ssl:
        kwargs.update(
            {
                "ssl": True,
                "ssl_cert_reqs": args.ssl_cert_reqs,
                "ssl_ca_certs": args.ssl_ca_certs,
                "ssl_check_hostname": args.ssl_check_hostname,
            }
        )

    if csc_enabled:
        kwargs["cache_config"] = CacheConfig(max_size=args.cache_max_size)

    return {key: value for key, value in kwargs.items() if value is not None}


def create_client(args: argparse.Namespace, *, csc_enabled: bool):
    kwargs = build_connection_kwargs(args, csc_enabled=csc_enabled)

    if args.mode == "cluster":
        startup_nodes = parse_cluster_nodes(args.cluster_nodes)
        if startup_nodes:
            return RedisCluster(startup_nodes=startup_nodes, **kwargs)
        return RedisCluster(host=args.host, port=args.port, **kwargs)

    return redis.Redis(host=args.host, port=args.port, db=args.db, **kwargs)


def preload_keyspace(args: argparse.Namespace, value: bytes) -> None:
    client = create_client(args, csc_enabled=False)
    try:
        for key_id in range(args.key_range):
            client.set(key_name(args.key_prefix, key_id), value)
    finally:
        client.close()


def delete_keyspace(args: argparse.Namespace) -> None:
    client = create_client(args, csc_enabled=False)
    try:
        # one key per DEL: in a cluster the keys live in different slots
        for key_id in range(args.key_range):
            client.delete(key_name(args.key_prefix, key_id))
    finally:
        client.close()


def prime_client_cache(client: Any, args: argparse.Namespace) -> None:
    if not args.prime_cache:
        return

    prime_count = args.prime_keys if args.prime_keys is not None else args.key_range
    for key_id in range(min(prime_count, args.key_range)):
        client.get(key_name(args.key_prefix, key_id))


def run_worker(
    client: Any,
    args: argparse.Namespace,
    read_ratio: float,
    stop_event: threading.Event,
    start_barrier: threading.Barrier,
    worker_index: int,
    phase_seed_offset: int,
    max_latency_samples: int,
    results: list[WorkerResult],
) -> None:
    operation_seed = (
        None if args.seed is None else args.seed + phase_seed_offset + worker_index
    )
    sample_seed = (
        None
        if args.seed is None
        else args.seed + phase_seed_offset + worker_index + 1_000_000
    )
    rng = random.Random(operation_seed)
    sample_rng = random.Random(sample_seed)
    value = b"x" * args.value_size
    result = WorkerResult()
    operation_index = 0
    sampled_operations = 0

    try:
        start_barrier.wait()
    except threading.BrokenBarrierError:
        return

    while not stop_event.is_set():
        key = key_name(args.key_prefix, rng.randrange(args.key_range))
        is_read = rng.random() < read_ratio
        should_sample = (
            max_latency_samples > 0 and operation_index % args.latency_sample_rate == 0
        )
        start_ns = time.perf_counter_ns() if should_sample else 0

        try:
            if is_read:
                # every key holds `value`, so any other reply is a broken
                # read (a cache returning stale or missing data), not a fast one
                reply = client.get(key)
                if reply != value:
                    raise ValueError(f"GET {key} returned {reply!r:.80}")
                result.read_operations += 1
            else:
                reply = client.set(key, value)
                if reply is not True:
                    raise ValueError(f"SET {key} returned {reply!r:.80}")
                result.write_operations += 1

            if should_sample:
                elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
                sampled_operations += 1
                # the reservoir may drop the extremes, so track them here
                if elapsed_ms < result.min_latency_ms:
                    result.min_latency_ms = elapsed_ms
                if elapsed_ms > result.max_latency_ms:
                    result.max_latency_ms = elapsed_ms
                if len(result.latencies_ms) < max_latency_samples:
                    result.latencies_ms.append(elapsed_ms)
                else:
                    replacement_index = sample_rng.randrange(sampled_operations)
                    if replacement_index < max_latency_samples:
                        result.latencies_ms[replacement_index] = elapsed_ms

        except Exception as exc:
            result.errors += 1
            if result.first_error is None:
                result.first_error = f"{type(exc).__name__}: {exc}"

        operation_index += 1

    result.sampled_operations = sampled_operations
    results[worker_index] = result


def run_phase(
    clients: list[Any],
    args: argparse.Namespace,
    read_ratio: float,
    duration_seconds: float,
    *,
    collect_latencies: bool,
    phase_seed_offset: int,
) -> tuple[float, list[WorkerResult]]:
    worker_count = len(clients)
    stop_event = threading.Event()
    start_barrier = threading.Barrier(worker_count + 1)
    results = [WorkerResult() for _ in range(worker_count)]
    threads = []

    if collect_latencies:
        max_latency_samples = max(1, args.max_latency_samples // worker_count)
    else:
        max_latency_samples = 0

    worker_index = 0
    for client in clients:
        thread = threading.Thread(
            target=run_worker,
            args=(
                client,
                args,
                read_ratio,
                stop_event,
                start_barrier,
                worker_index,
                phase_seed_offset,
                max_latency_samples,
                results,
            ),
            daemon=True,
        )
        thread.start()
        threads.append(thread)
        worker_index += 1

    start_barrier.wait()
    phase_start = time.perf_counter()
    time.sleep(duration_seconds)
    stop_event.set()

    # A worker blocked in a request (socket_timeout=None by default) never
    # sees stop_event; bound the wait instead of hanging. The threads are
    # daemons, so they do not keep the process alive.
    join_deadline = time.monotonic() + max(10.0, duration_seconds)
    for thread in threads:
        thread.join(max(0.0, join_deadline - time.monotonic()))
    stuck = sum(thread.is_alive() for thread in threads)
    if stuck:
        raise RuntimeError(
            f"{stuck} of {worker_count} workers still blocked in a request "
            "after the phase ended"
        )

    return time.perf_counter() - phase_start, results


def merge_latency_samples(results: list[WorkerResult], seed: int | None) -> list[float]:
    """Merge the per-worker reservoirs into one sample of all operations.

    Each worker keeps an equal-sized reservoir, so a worker that completed
    fewer operations is sampled at a higher rate. Subsample every reservoir
    down to the lowest rate so each worker contributes in proportion to the
    operations it sampled.
    """
    sampled = [r for r in results if r.sampled_operations and r.latencies_ms]
    if not sampled:
        return []
    rate = min(len(r.latencies_ms) / r.sampled_operations for r in sampled)
    rng = random.Random(seed)
    merged = []
    for r in sampled:
        k = min(len(r.latencies_ms), round(rate * r.sampled_operations))
        merged.extend(rng.sample(r.latencies_ms, k))
    return merged


def percentile(sorted_values: list[float], percent: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]

    rank = (len(sorted_values) - 1) * (percent / 100)
    lower_index = int(rank)
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    fraction = rank - lower_index
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    return lower_value + ((upper_value - lower_value) * fraction)


def collect_cache_sizes(clients: list[Any]) -> list[int]:
    sizes = []
    seen_cache_ids = set()

    def append_cache_size(cache: Any) -> None:
        cache_id = id(cache)
        if cache_id in seen_cache_ids:
            return
        seen_cache_ids.add(cache_id)
        sizes.append(cache.size)

    for client in clients:
        try:
            if hasattr(client, "get_cache"):
                cache = client.get_cache()
                if cache is not None:
                    append_cache_size(cache)
            elif hasattr(client, "get_nodes"):
                for node in client.get_nodes():
                    connection = getattr(node, "redis_connection", None)
                    if connection is None or not hasattr(connection, "get_cache"):
                        continue
                    cache = connection.get_cache()
                    if cache is not None:
                        append_cache_size(cache)
        except Exception:
            continue
    return sizes


def build_summary(
    args: argparse.Namespace,
    read_ratio: float,
    duration_seconds: float,
    results: list[WorkerResult],
    warmup_results: list[WorkerResult],
    clients: list[Any],
) -> BenchmarkSummary:
    read_operations = sum(result.read_operations for result in results)
    write_operations = sum(result.write_operations for result in results)
    errors = sum(result.errors for result in results)
    warmup_errors = sum(result.errors for result in warmup_results)
    total_errors = errors + warmup_errors
    total_operations = read_operations + write_operations
    first_error = next(
        (result.first_error for result in results if result.first_error), None
    )
    warmup_first_error = next(
        (result.first_error for result in warmup_results if result.first_error), None
    )

    sorted_latencies = sorted(merge_latency_samples(results, args.seed))

    if sorted_latencies:
        # from every sampled operation, not just the ones the reservoirs kept
        min_latency_ms = min(result.min_latency_ms for result in results)
        max_latency_ms = max(result.max_latency_ms for result in results)
        avg_latency_ms = statistics.fmean(sorted_latencies)
    else:
        min_latency_ms = 0.0
        max_latency_ms = 0.0
        avg_latency_ms = 0.0

    metadata = {
        "host": args.host,
        "port": args.port,
        "cluster_nodes": args.cluster_nodes if args.mode == "cluster" else None,
        "username_provided": bool(args.username),
        "password_provided": bool(args.password),
        "db": args.db if args.mode == "standalone" else None,
        "ssl": args.ssl,
        "protocol": args.protocol,
        "cache_max_size": args.cache_max_size
        if args.client_side_cache == "on"
        else None,
        "redis_py_version": redis.__version__,
        "prime_cache": args.prime_cache,
        "prime_keys": args.prime_keys,
        "warmup_seconds": args.warmup,
        "latency_sample_rate": args.latency_sample_rate,
        "max_latency_samples": args.max_latency_samples,
        "seed": args.seed,
        "allow_errors": args.allow_errors,
    }

    return BenchmarkSummary(
        mode=args.mode,
        client_side_cache=args.client_side_cache,
        workload=args.workload,
        read_ratio=read_ratio,
        duration_seconds=round(duration_seconds, 3),
        clients=args.clients,
        key_range=args.key_range,
        value_size=args.value_size,
        total_operations=total_operations,
        read_operations=read_operations,
        write_operations=write_operations,
        errors=errors,
        warmup_errors=warmup_errors,
        total_errors=total_errors,
        throughput_ops_per_sec=round(total_operations / duration_seconds, 2)
        if duration_seconds > 0
        else 0.0,
        min_latency_ms=round(min_latency_ms, 4),
        max_latency_ms=round(max_latency_ms, 4),
        avg_latency_ms=round(avg_latency_ms, 4),
        p50_latency_ms=round(percentile(sorted_latencies, 50), 4),
        p95_latency_ms=round(percentile(sorted_latencies, 95), 4),
        p99_latency_ms=round(percentile(sorted_latencies, 99), 4),
        latency_samples=len(sorted_latencies),
        cache_sizes=collect_cache_sizes(clients),
        first_error=first_error,
        warmup_first_error=warmup_first_error,
        metadata=metadata,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark redis-py client-side caching performance."
    )

    parser.add_argument(
        "--mode", choices=["standalone", "cluster"], default="standalone"
    )
    parser.add_argument("--host", default=os.getenv("REDIS_HOST", "localhost"))
    parser.add_argument(
        "--port", type=int, default=int(os.getenv("REDIS_PORT", "6379"))
    )
    parser.add_argument("--cluster-nodes", default=os.getenv("REDIS_CLUSTER_NODES"))
    parser.add_argument("--username", default=os.getenv("REDIS_USERNAME"))
    parser.add_argument("--password", default=os.getenv("REDIS_PASSWORD"))
    parser.add_argument("--db", type=int, default=int(os.getenv("REDIS_DB", "0")))
    parser.add_argument(
        "--ssl", type=parse_bool, default=parse_bool(os.getenv("REDIS_SSL", "false"))
    )
    parser.add_argument(
        "--ssl-cert-reqs",
        choices=["none", "optional", "required"],
        default=os.getenv("REDIS_SSL_CERT_REQS", "required"),
    )
    parser.add_argument("--ssl-ca-certs", default=os.getenv("REDIS_SSL_CA_CERTS"))
    parser.add_argument(
        "--ssl-check-hostname",
        type=parse_bool,
        default=parse_bool(os.getenv("REDIS_SSL_CHECK_HOSTNAME", "true")),
    )
    parser.add_argument("--socket-timeout", type=float, default=None)
    parser.add_argument("--socket-connect-timeout", type=float, default=None)
    parser.add_argument("--max-connections", type=int, default=None)
    parser.add_argument("--protocol", type=int, choices=[2, 3], default=3)

    parser.add_argument(
        "--client-side-cache",
        type=parse_on_off,
        choices=["on", "off"],
        required=True,
        help="Enable or disable redis-py client-side caching.",
    )
    parser.add_argument("--cache-max-size", type=int, default=10000)

    parser.add_argument(
        "--workload",
        choices=sorted(READ_RATIOS),
        required=True,
        help="Predefined read/write mix.",
    )
    parser.add_argument(
        "--read-ratio",
        type=float,
        default=None,
        help="Override the workload read ratio, from 0.0 to 1.0.",
    )
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--warmup", type=float, default=5.0)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--key-prefix", default="redis_py_csc_bench")
    parser.add_argument("--key-range", type=int, default=10000)
    parser.add_argument("--value-size", type=int, default=128)
    parser.add_argument(
        "--prime-cache",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Read the keyspace before timed measurement.",
    )
    parser.add_argument(
        "--prime-keys",
        type=int,
        default=None,
        help="Number of keys to read while priming. Defaults to --key-range.",
    )
    parser.add_argument("--latency-sample-rate", type=int, default=1)
    parser.add_argument("--max-latency-samples", type=int, default=500000)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--output-json", default=None)
    parser.add_argument(
        "--allow-errors",
        action="store_true",
        help="Exit successfully even when Redis operations failed.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print only the JSON summary to stdout.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.mode == "cluster" and args.db != 0:
        raise ValueError("Redis Cluster does not support SELECT; use --db 0")
    if args.client_side_cache == "on" and args.protocol != 3:
        raise ValueError("Client-side caching requires RESP3; use --protocol 3")
    if args.key_range <= 0:
        raise ValueError("--key-range must be greater than zero")
    if args.value_size <= 0:
        raise ValueError("--value-size must be greater than zero")
    if args.clients <= 0:
        raise ValueError("--clients must be greater than zero")
    if args.duration <= 0:
        raise ValueError("--duration must be greater than zero")
    if args.warmup < 0:
        raise ValueError("--warmup must be zero or greater")
    if args.latency_sample_rate <= 0:
        raise ValueError("--latency-sample-rate must be greater than zero")
    if args.max_latency_samples <= 0:
        raise ValueError("--max-latency-samples must be greater than zero")
    if args.prime_keys is not None and args.prime_keys < 0:
        raise ValueError("--prime-keys must be zero or greater")
    if args.read_ratio is not None and not 0 <= args.read_ratio <= 1:
        raise ValueError("--read-ratio must be between 0.0 and 1.0")


def print_progress(message: str, *, json_only: bool) -> None:
    if not json_only:
        print(message)


def main() -> int:
    args = parse_args()
    validate_args(args)

    read_ratio = (
        args.read_ratio if args.read_ratio is not None else READ_RATIOS[args.workload]
    )
    csc_enabled = args.client_side_cache == "on"
    value = b"x" * args.value_size

    print_progress(
        f"Preparing {args.mode} CSC benchmark "
        f"({args.client_side_cache}, {args.workload})...",
        json_only=args.json,
    )
    clients = []
    warmup_results = []
    try:
        print_progress(f"Preloading {args.key_range:,} keys...", json_only=args.json)
        preload_keyspace(args, value)

        for _ in range(args.clients):
            clients.append(create_client(args, csc_enabled=csc_enabled))

        for client in clients:
            client.ping()

        if args.prime_cache:
            prime_count = (
                args.prime_keys if args.prime_keys is not None else args.key_range
            )
            print_progress(
                f"Priming {min(prime_count, args.key_range):,} keys per client...",
                json_only=args.json,
            )
            for client in clients:
                prime_client_cache(client, args)

        if args.warmup:
            print_progress(f"Warming up for {args.warmup:.1f}s...", json_only=args.json)
            _, warmup_results = run_phase(
                clients,
                args,
                read_ratio,
                args.warmup,
                collect_latencies=False,
                phase_seed_offset=0,
            )

        print_progress(
            f"Running measured workload for {args.duration:.1f}s...",
            json_only=args.json,
        )
        duration, results = run_phase(
            clients,
            args,
            read_ratio,
            args.duration,
            collect_latencies=True,
            phase_seed_offset=10_000_000,
        )

        summary = build_summary(
            args, read_ratio, duration, results, warmup_results, clients
        )
        summary_dict = asdict(summary)

        if args.output_json:
            output_path = Path(args.output_json)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(summary_dict, indent=2) + "\n")

        if args.json:
            print(json.dumps(summary_dict, indent=2))
        else:
            print("\nClient-side caching benchmark result")
            print("=" * 60)
            print(f"Mode:              {summary.mode}")
            print(f"Client cache:      {summary.client_side_cache}")
            print(
                f"Workload:          {summary.workload} ({summary.read_ratio:.0%} reads)"
            )
            print(f"Duration:          {summary.duration_seconds:.3f}s")
            print(f"Clients:           {summary.clients:,}")
            print(f"Total operations:  {summary.total_operations:,}")
            print(f"Throughput:        {summary.throughput_ops_per_sec:,.2f} ops/sec")
            print(f"Avg latency:       {summary.avg_latency_ms:.4f} ms")
            print(f"P50 latency:       {summary.p50_latency_ms:.4f} ms")
            print(f"P95 latency:       {summary.p95_latency_ms:.4f} ms")
            print(f"P99 latency:       {summary.p99_latency_ms:.4f} ms")
            print(f"Errors:            {summary.errors:,}")
            print(f"Warmup errors:     {summary.warmup_errors:,}")
            if summary.first_error:
                print(f"First error:       {summary.first_error}")
            if summary.warmup_first_error:
                print(f"Warmup first error:{summary.warmup_first_error}")
            if args.output_json:
                print(f"JSON summary:      {args.output_json}")
            print("=" * 60)

        if summary.total_errors and not args.allow_errors:
            return 1
    finally:
        for client in clients:
            client.close()
        delete_keyspace(args)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
