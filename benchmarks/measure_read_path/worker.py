"""Time the read path of one redis-py tree under one client config.

Run by run_bench.py in a fresh interpreter per (tree, config, round), so
every measurement imports redis from exactly one checkout. Prints one JSON
object: ``per_op_us``, the median time per operation (microseconds) of each
scenario, and ``wall_s``, how long each scenario took to execute in total
(warmup plus all repeats).

A config is a client (``--client async|sync``), a topology (``--topology
standalone|cluster``), a connection pool class (``--pool connection|blocking``,
``ConnectionPool`` or ``BlockingConnectionPool``), a parser, a protocol and a
socket timeout. The async cluster client manages its connections per node
without a pool class, so it takes only ``--pool connection``. Every client
is checked to use the requested parser before anything is timed.

Modes:
  --setup     write the benchmark keys (any tree can do it)
  --teardown  delete them
  --count-arms  instead of timing, count how many read timeouts the parser
                and connection arm per operation (async only, diagnostic,
                not timed)
"""

import argparse
import asyncio
import json
import os
import statistics
import sys
import threading
import time

PREFIX = "bench_read_path:"
SMALL = b"v" * 100
BIG_200K = b"k" * (200 * 1024)
BIG_1M = b"m" * (1024 * 1024)
BIG_10M = b"M" * (10 * 1024 * 1024)
LIST_LEN = 10_000
HASH_LEN = 1_000
# full contents, so the scenarios can reject a reply of the right size but
# the wrong data instead of timing it as a fast one
LIST_ITEMS = [b"item%06d" % i for i in range(LIST_LEN)]
HASH_ITEMS = {b"f%05d" % i: b"x" * 10 for i in range(HASH_LEN)}

K_SMALL = PREFIX + "small"
K_200K = PREFIX + "big200k"
K_1M = PREFIX + "big1m"
K_10M = PREFIX + "big10m"
K_LIST = PREFIX + "list10k"
K_HASH = PREFIX + "hash1k"

# 64 tasks or threads share a 64-connection pool in concurrent_get
CONCURRENCY = 64


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--tree", required=True)
    p.add_argument("--client", choices=["async", "sync"], default="async")
    p.add_argument(
        "--topology", choices=["standalone", "cluster"], default="standalone"
    )
    p.add_argument("--parser", choices=["python", "hiredis"], default="python")
    p.add_argument("--protocol", type=int, choices=[2, 3], default=3)
    p.add_argument("--socket-timeout", default="5")
    p.add_argument("--loop", choices=["asyncio", "uvloop"], default="asyncio")
    p.add_argument("--pool", choices=["connection", "blocking"], default="connection")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--db", type=int, default=13)
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--scenarios", default="all")
    p.add_argument("--setup", action="store_true")
    p.add_argument("--teardown", action="store_true")
    p.add_argument("--count-arms", action="store_true")
    args = p.parse_args()
    if (
        args.client == "async"
        and args.topology == "cluster"
        and args.pool != ("connection")
    ):
        p.error("the async cluster client has no connection pool class")
    if args.client == "sync" and (args.count_arms or args.loop != "asyncio"):
        p.error("--count-arms and --loop apply to the async client only")
    return args


def import_redis(tree):
    tree = os.path.realpath(tree)
    sys.path.insert(0, tree)
    import redis

    loaded = os.path.realpath(redis.__file__)
    if not loaded.startswith(tree + os.sep):
        raise SystemExit(f"redis imported from {loaded}, expected under {tree}")
    return redis


def socket_timeout(args):
    return None if args.socket_timeout.lower() == "none" else float(args.socket_timeout)


def parser_class(args):
    from redis._parsers import (
        _AsyncHiredisParser,
        _AsyncRESP2Parser,
        _AsyncRESP3Parser,
        _HiredisParser,
        _RESP2Parser,
        _RESP3Parser,
    )

    if args.client == "async":
        hiredis, resp2, resp3 = (
            _AsyncHiredisParser,
            _AsyncRESP2Parser,
            _AsyncRESP3Parser,
        )
    else:
        hiredis, resp2, resp3 = _HiredisParser, _RESP2Parser, _RESP3Parser
    if args.parser == "hiredis":
        return hiredis
    return resp3 if args.protocol == 3 else resp2


def make_client(redis, args, max_connections=None, pool="connection"):
    """The async standalone client; mem_probe uses it directly."""
    from redis.asyncio import BlockingConnectionPool, ConnectionPool, Redis

    pool_class = BlockingConnectionPool if pool == "blocking" else ConnectionPool
    connection_pool = pool_class(
        host=args.host,
        port=args.port,
        db=args.db,
        protocol=args.protocol,
        parser_class=parser_class(args),
        socket_timeout=socket_timeout(args),
        max_connections=max_connections,
    )
    return Redis(connection_pool=connection_pool)


def make_async_cluster(redis, args):
    from redis.asyncio.cluster import RedisCluster

    r = RedisCluster(
        host=args.host,
        port=args.port,
        protocol=args.protocol,
        socket_timeout=socket_timeout(args),
        max_connections=CONCURRENCY,
    )
    # RedisCluster takes no parser_class. Every node, the startup nodes and
    # the ones discovered later, builds its connections from these kwargs.
    r.nodes_manager.connection_kwargs["parser_class"] = parser_class(args)
    for node in r.nodes_manager.startup_nodes.values():
        node.connection_kwargs["parser_class"] = parser_class(args)
    return r


def make_sync_client(redis, args):
    pool_class = (
        redis.BlockingConnectionPool
        if args.pool == "blocking"
        else redis.ConnectionPool
    )
    if args.topology == "cluster":
        chosen = parser_class(args)

        # RedisCluster passes connection_class, not parser_class, to the
        # node pools, and only when it is built from a URL: otherwise it
        # hands both kwargs to Redis(), which takes neither.
        class BenchConnection(redis.Connection):
            def __init__(self, *a, **kw):
                kw["parser_class"] = chosen
                super().__init__(*a, **kw)

        return redis.RedisCluster(
            url=f"redis://{args.host}:{args.port}",
            protocol=args.protocol,
            socket_timeout=socket_timeout(args),
            max_connections=CONCURRENCY,
            connection_class=BenchConnection,
            connection_pool_class=pool_class,
        )
    connection_pool = pool_class(
        host=args.host,
        port=args.port,
        db=args.db,
        protocol=args.protocol,
        parser_class=parser_class(args),
        socket_timeout=socket_timeout(args),
        max_connections=CONCURRENCY,
    )
    return redis.Redis(connection_pool=connection_pool)


# Each scenario returns (number of operations, elapsed seconds) and checks
# the reply, so a broken read path cannot pass as a fast one. The sync
# scenarios further down mirror these one for one.


async def sc_get_small_seq(r):
    n = 2000
    start = time.perf_counter()
    for _ in range(n):
        v = await r.get(K_SMALL)
    elapsed = time.perf_counter() - start
    assert v == SMALL
    return n, elapsed


async def sc_set_small_seq(r):
    n = 2000
    key = PREFIX + "set_target"
    start = time.perf_counter()
    for _ in range(n):
        ok = await r.set(key, SMALL)
    elapsed = time.perf_counter() - start
    assert ok is True
    return n, elapsed


async def sc_pipeline_1k_get(r):
    n = 20
    start = time.perf_counter()
    for _ in range(n):
        pipe = r.pipeline(transaction=False)
        for _ in range(1000):
            pipe.get(K_SMALL)
        res = await pipe.execute()
    elapsed = time.perf_counter() - start
    assert res == [SMALL] * 1000
    return n, elapsed


async def sc_lrange_10k(r):
    n = 30
    start = time.perf_counter()
    for _ in range(n):
        res = await r.lrange(K_LIST, 0, -1)
    elapsed = time.perf_counter() - start
    assert res == LIST_ITEMS
    return n, elapsed


async def sc_hgetall_1k(r):
    n = 200
    start = time.perf_counter()
    for _ in range(n):
        res = await r.hgetall(K_HASH)
    elapsed = time.perf_counter() - start
    assert res == HASH_ITEMS
    return n, elapsed


async def sc_get_200kb(r):
    # same total bytes as get_1mb, in more and smaller replies
    n = 1500
    start = time.perf_counter()
    for _ in range(n):
        v = await r.get(K_200K)
    elapsed = time.perf_counter() - start
    assert v == BIG_200K
    return n, elapsed


async def sc_get_1mb(r):
    n = 300
    start = time.perf_counter()
    for _ in range(n):
        v = await r.get(K_1M)
    elapsed = time.perf_counter() - start
    assert v == BIG_1M
    return n, elapsed


async def sc_get_10mb(r):
    n = 30
    start = time.perf_counter()
    for _ in range(n):
        v = await r.get(K_10M)
    elapsed = time.perf_counter() - start
    assert v == BIG_10M
    return n, elapsed


async def sc_concurrent_get(r):
    # CONCURRENCY tasks share the pool; time per GET across all tasks
    per_task = 100

    async def worker():
        for _ in range(per_task):
            v = await r.get(K_SMALL)
        assert v == SMALL

    start = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(CONCURRENCY)))
    elapsed = time.perf_counter() - start
    return CONCURRENCY * per_task, elapsed


async def sc_pool_checkout(r):
    # taking a connected idle connection and handing it back, no round trip:
    # the pool's own per-command overhead
    n = 5000
    if hasattr(r, "nodes_manager"):
        node = r.get_node_from_key(K_SMALL)
        start = time.perf_counter()
        for _ in range(n):
            conn = node.acquire_connection()
            node.release(conn)
        elapsed = time.perf_counter() - start
    else:
        pool = r.connection_pool
        start = time.perf_counter()
        for _ in range(n):
            conn = await pool.get_connection()
            await pool.release(conn)
        elapsed = time.perf_counter() - start
    assert conn.is_connected
    return n, elapsed


SCENARIOS = {
    "get_small_seq": sc_get_small_seq,
    "set_small_seq": sc_set_small_seq,
    "pipeline_1k_get": sc_pipeline_1k_get,
    "lrange_10k": sc_lrange_10k,
    "hgetall_1k": sc_hgetall_1k,
    "get_200kb": sc_get_200kb,
    "get_1mb": sc_get_1mb,
    "get_10mb": sc_get_10mb,
    "concurrent_get": sc_concurrent_get,
    "pool_checkout": sc_pool_checkout,
}


def sync_get_small_seq(r):
    n = 2000
    start = time.perf_counter()
    for _ in range(n):
        v = r.get(K_SMALL)
    elapsed = time.perf_counter() - start
    assert v == SMALL
    return n, elapsed


def sync_set_small_seq(r):
    n = 2000
    key = PREFIX + "set_target"
    start = time.perf_counter()
    for _ in range(n):
        ok = r.set(key, SMALL)
    elapsed = time.perf_counter() - start
    assert ok is True
    return n, elapsed


def sync_pipeline_1k_get(r):
    n = 20
    start = time.perf_counter()
    for _ in range(n):
        pipe = r.pipeline(transaction=False)
        for _ in range(1000):
            pipe.get(K_SMALL)
        res = pipe.execute()
    elapsed = time.perf_counter() - start
    assert res == [SMALL] * 1000
    return n, elapsed


def sync_lrange_10k(r):
    n = 30
    start = time.perf_counter()
    for _ in range(n):
        res = r.lrange(K_LIST, 0, -1)
    elapsed = time.perf_counter() - start
    assert res == LIST_ITEMS
    return n, elapsed


def sync_hgetall_1k(r):
    n = 200
    start = time.perf_counter()
    for _ in range(n):
        res = r.hgetall(K_HASH)
    elapsed = time.perf_counter() - start
    assert res == HASH_ITEMS
    return n, elapsed


def sync_get_200kb(r):
    # same total bytes as get_1mb, in more and smaller replies
    n = 1500
    start = time.perf_counter()
    for _ in range(n):
        v = r.get(K_200K)
    elapsed = time.perf_counter() - start
    assert v == BIG_200K
    return n, elapsed


def sync_get_1mb(r):
    n = 300
    start = time.perf_counter()
    for _ in range(n):
        v = r.get(K_1M)
    elapsed = time.perf_counter() - start
    assert v == BIG_1M
    return n, elapsed


def sync_get_10mb(r):
    n = 30
    start = time.perf_counter()
    for _ in range(n):
        v = r.get(K_10M)
    elapsed = time.perf_counter() - start
    assert v == BIG_10M
    return n, elapsed


def sync_concurrent_get(r):
    # CONCURRENCY threads share the pool; time per GET across all threads
    per_thread = 100
    errors = []

    def worker():
        try:
            for _ in range(per_thread):
                v = r.get(K_SMALL)
            assert v == SMALL
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(CONCURRENCY)]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start
    if errors:
        raise errors[0]
    return CONCURRENCY * per_thread, elapsed


def sync_pool_checkout(r):
    # taking a connected idle connection and handing it back, no round trip:
    # the pool's own per-command overhead
    n = 5000
    if hasattr(r, "nodes_manager"):
        pool = r.get_node_from_key(K_SMALL).redis_connection.connection_pool
    else:
        pool = r.connection_pool
    start = time.perf_counter()
    for _ in range(n):
        conn = pool.get_connection()
        pool.release(conn)
    elapsed = time.perf_counter() - start
    assert conn.is_connected
    return n, elapsed


SYNC_SCENARIOS = {
    "get_small_seq": sync_get_small_seq,
    "set_small_seq": sync_set_small_seq,
    "pipeline_1k_get": sync_pipeline_1k_get,
    "lrange_10k": sync_lrange_10k,
    "hgetall_1k": sync_hgetall_1k,
    "get_200kb": sync_get_200kb,
    "get_1mb": sync_get_1mb,
    "get_10mb": sync_get_10mb,
    "concurrent_get": sync_concurrent_get,
    "pool_checkout": sync_pool_checkout,
}


async def setup(r):
    await r.set(K_SMALL, SMALL)
    await r.set(K_200K, BIG_200K)
    await r.set(K_1M, BIG_1M)
    await r.set(K_10M, BIG_10M)
    await r.delete(K_LIST)
    await r.delete(K_HASH)
    for i in range(0, LIST_LEN, 1000):
        await r.rpush(K_LIST, *LIST_ITEMS[i : i + 1000])
    await r.hset(K_HASH, mapping=HASH_ITEMS)


async def teardown(r):
    # one key per DEL: in a cluster the keys live in different slots
    keys = [k async for k in r.scan_iter(match=PREFIX + "*")]
    for k in keys:
        await r.delete(k)


async def check_parser(r, expected, expected_pool):
    if hasattr(r, "nodes_manager"):
        # the async cluster client keeps connections per node, no pool class.
        # acquire_connection() does not connect, so connect it here:
        # pool_checkout times this node's idle connection and may run alone
        node = r.get_node_from_key(K_SMALL)
        conn = node.acquire_connection()
        await conn.connect()
        node.release(conn)
    else:
        if type(r.connection_pool) is not expected_pool:
            raise SystemExit(
                f"client uses {type(r.connection_pool)}, expected {expected_pool}"
            )
        conn = await r.connection_pool.get_connection()
        await r.connection_pool.release(conn)
    if type(conn._parser) is not expected:
        raise SystemExit(f"connection uses {type(conn._parser)}, expected {expected}")


def check_sync_parser(r, expected, expected_pool):
    if hasattr(r, "nodes_manager"):
        pool = r.get_node_from_key(K_SMALL).redis_connection.connection_pool
    else:
        pool = r.connection_pool
    if type(pool) is not expected_pool:
        raise SystemExit(f"client uses {type(pool)}, expected {expected_pool}")
    conn = pool.get_connection()
    pool.release(conn)
    if type(conn._parser) is not expected:
        raise SystemExit(f"connection uses {type(conn._parser)}, expected {expected}")


def install_arm_counter(redis):
    """Wrap every async_timeout the read path can use with a counter."""
    import redis._parsers.base as base
    import redis.asyncio.connection as aconn

    counts = {"armed": 0}
    modules = [base, aconn]
    try:
        import redis._parsers.hiredis as hi

        modules.append(hi)
    except ImportError:
        pass
    for mod in modules:
        original = getattr(mod, "async_timeout", None)
        if original is None or getattr(original, "_counting", False):
            continue

        def counting(*a, _orig=original, **kw):
            counts["armed"] += 1
            return _orig(*a, **kw)

        counting._counting = True
        mod.async_timeout = counting
    return counts


def selected_scenarios(args):
    return list(SCENARIOS) if args.scenarios == "all" else args.scenarios.split(",")


async def run(args, redis):
    if args.topology == "cluster":
        r = make_async_cluster(redis, args)
    else:
        r = make_client(redis, args, max_connections=CONCURRENCY, pool=args.pool)
    try:
        await r.ping()
        if args.setup:
            await setup(r)
            return {"setup": "ok"}
        if args.teardown:
            await teardown(r)
            return {"teardown": "ok"}
        await check_parser(
            r,
            parser_class(args),
            redis.asyncio.BlockingConnectionPool
            if args.pool == "blocking"
            else redis.asyncio.ConnectionPool,
        )
        results = {}
        wall = {}
        if args.count_arms:
            counts = install_arm_counter(redis)
            for name in selected_scenarios(args):
                start = time.perf_counter()
                await SCENARIOS[name](r)  # warm the pool outside the count
                counts["armed"] = 0
                ops, _ = await SCENARIOS[name](r)
                results[name] = counts["armed"] / ops
                wall[name] = time.perf_counter() - start
            return {"arms_per_op": results, "wall_s": wall}
        for name in selected_scenarios(args):
            fn = SCENARIOS[name]
            start = time.perf_counter()
            await fn(r)  # warmup: fills the pool, warms caches
            per_op = []
            for _ in range(args.repeats):
                ops, elapsed = await fn(r)
                per_op.append(elapsed / ops * 1e6)
            results[name] = statistics.median(per_op)
            wall[name] = time.perf_counter() - start
        return {"per_op_us": results, "wall_s": wall}
    finally:
        await r.aclose()


def run_sync(args, redis):
    r = make_sync_client(redis, args)
    try:
        r.ping()
        check_sync_parser(
            r,
            parser_class(args),
            redis.BlockingConnectionPool
            if args.pool == "blocking"
            else redis.ConnectionPool,
        )
        results = {}
        wall = {}
        for name in selected_scenarios(args):
            fn = SYNC_SCENARIOS[name]
            start = time.perf_counter()
            fn(r)  # warmup: fills the pool, warms caches
            per_op = []
            for _ in range(args.repeats):
                ops, elapsed = fn(r)
                per_op.append(elapsed / ops * 1e6)
            results[name] = statistics.median(per_op)
            wall[name] = time.perf_counter() - start
        return {"per_op_us": results, "wall_s": wall}
    finally:
        r.close()


def main():
    args = parse_args()
    redis = import_redis(args.tree)
    # the keys are written and deleted through the async client, whatever
    # client the config times
    if args.setup or args.teardown:
        args.client = "async"
    if args.client == "sync":
        out = run_sync(args, redis)
    else:
        if args.loop == "uvloop":
            import uvloop

            uvloop.install()
        out = asyncio.run(run(args, redis))
    print(json.dumps(out))


if __name__ == "__main__":
    main()
