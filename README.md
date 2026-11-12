# redis-py

The Python interface to the Redis key-value store.

[![CI](https://github.com/redis/redis-py/actions/workflows/integration.yaml/badge.svg?branch=master)](https://github.com/redis/redis-py/actions/workflows/integration.yaml)
[![docs](https://readthedocs.org/projects/redis/badge/?version=stable&style=flat)](https://redis.readthedocs.io/en/stable/)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/redis/redis-py/blob/master/LICENSE)
[![pypi](https://badge.fury.io/py/redis.svg)](https://pypi.org/project/redis/)
[![pre-release](https://img.shields.io/github/v/release/redis/redis-py?include_prereleases&label=latest-prerelease)](https://github.com/redis/redis-py/releases)
[![codecov](https://codecov.io/gh/redis/redis-py/branch/master/graph/badge.svg?token=yenl5fzxxr)](https://codecov.io/gh/redis/redis-py)

[Installation](#installation) |  [Usage](#usage) | [Advanced Topics](#advanced-topics) | [Contributing](https://github.com/redis/redis-py/blob/master/CONTRIBUTING.md)

---------------------------------------------

**Note:** redis-py 5.0 is the last version of redis-py that supports Python 3.7, as it has reached [end of life](https://devguide.python.org/versions/). redis-py 5.1 supports Python 3.8+.<br>
**Note:** redis-py 6.1.0 is the last version of redis-py that supports Python 3.8, as it has reached [end of life](https://devguide.python.org/versions/). redis-py 6.2.0 supports Python 3.9+.<br>
**Note:** redis-py 8.1.0 is the last version of redis-py that supports Python 3.10, as it has reached [end of life](https://devguide.python.org/versions/). redis-py 8.2.0 supports Python 3.11+.

---------------------------------------------

## How do I Redis?

[Learn for free at Redis University](https://redis.io/learn/university)

[Try the Redis Cloud](https://redis.io/try-free/)

[Dive in developer tutorials](https://redis.io/learn)

[Join the Redis community](https://redis.io/community/)

[Work at Redis](https://redis.io/careers/)

## Installation

Start a redis via docker (for Redis versions >= 8.0):

``` bash
docker run -p 6379:6379 -it redis:latest
```

Start a redis via docker (for Redis versions < 8.0):

``` bash
docker run -p 6379:6379 -it redis/redis-stack:latest
```
To install redis-py, simply:

``` bash
$ pip install redis
```

For faster performance, install redis with hiredis support, this provides a compiled response parser, and *for most cases* requires zero code changes.
By default, if hiredis >= 1.0 is available, redis-py will attempt to use it for response parsing.

``` bash
$ pip install "redis[hiredis]"
```

Looking for a high-level library to handle object mapping? See [redis-om-python](https://github.com/redis/redis-om-python)!

## Supported Redis Versions

The most recent version of this library supports Redis version [7.2](https://github.com/redis/redis/blob/7.2/00-RELEASENOTES), [7.4](https://github.com/redis/redis/blob/7.4/00-RELEASENOTES), [8.0](https://github.com/redis/redis/blob/8.0/00-RELEASENOTES), [8.2](https://github.com/redis/redis/blob/8.2/00-RELEASENOTES), [8.4](https://github.com/redis/redis/blob/8.4/00-RELEASENOTES), [8.6](https://github.com/redis/redis/blob/8.6/00-RELEASENOTES) and [8.8](https://github.com/redis/redis/blob/8.8/00-RELEASENOTES).

The table below highlights version compatibility of the most-recent library versions and redis versions.

| Library version | Supported redis versions |
|-----------------|-------------------|
| 3.5.3 | <= 6.2 Family of releases |
| >= 4.5.0 | Version 5.0 to 7.0 |
| >= 5.0.0 | Version 5.0 to 7.4 |
| >= 6.0.0 | Version 7.2 to current |


## Usage

### Basic Example

``` python
>>> import redis
>>> r = redis.Redis(host='localhost', port=6379, db=0)
>>> r.set('foo', 'bar')
True
>>> r.get('foo')
b'bar'
```

The above code connects to localhost on port 6379, sets a value in Redis, and retrieves it. All responses are returned as bytes in Python, to receive decoded strings, set *decode_responses=True*.  For this, and more connection options, see [these examples](https://redis.readthedocs.io/en/stable/examples.html).


#### RESP3 Support
redis-py supports RESP3 starting with version 5.0. Starting with redis-py 8.0,
clients use RESP3 on the wire by default while preserving legacy
RESP2-compatible Python response shapes for existing applications.

When *protocol* is left unset, a server or proxy that rejects `HELLO` with
an unknown-command or `NOPROTO` error triggers a fallback to RESP2 and a warning.
Authentication and other server errors still propagate. Automatic fallback is
disabled when *protocol=3* is explicit, client-side caching is configured, or
maintenance notifications are explicitly enabled.

Automatic fallback adapts core command response callbacks only. Module commands
(JSON, Search, TimeSeries, Bloom, and vector sets) still select their response
parsers from the configured protocol and can return incorrect results after a
fallback, including empty results from `FT.SEARCH`. When using these commands
with a server or proxy that does not support `HELLO`, set *protocol=2* explicitly
when creating the client.

Set *protocol=3* explicitly when your application should receive
RESP3-specific response shapes or when you want the wire protocol choice to be
visible in code. To force RESP2 on the wire, set *protocol=2*. To opt in to
protocol-independent response shapes, set *legacy_responses=False*.

For new projects, we recommend opting out of legacy response compatibility by
setting *legacy_responses=False*. This makes redis-py return unified Python
response shapes for affected commands whether the connection uses RESP2 or
RESP3. Existing applications can keep the default legacy-compatible behavior
while they migrate response handling at their own pace.

``` python
>>> import redis
>>> r = redis.Redis(host='localhost', port=6379, db=0, legacy_responses=False)
```

See the [unified responses migration guide](https://redis.readthedocs.io/en/stable/unified_responses.html)
for activation instructions and command-by-command response differences. For
RESP3-specific behavior, see the [RESP3 features guide](https://redis.readthedocs.io/en/stable/resp3_features.html).

### Connection Pools

By default, redis-py uses a connection pool to manage connections. Each instance of a Redis class receives its own connection pool. You can however define your own [redis.ConnectionPool](https://redis.readthedocs.io/en/stable/connections.html#connection-pools).

``` python
>>> pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
>>> r = redis.Redis(connection_pool=pool)
```

Alternatively, you might want to look at [Async connections](https://redis.readthedocs.io/en/stable/examples/asyncio_examples.html), or [Cluster connections](https://redis.readthedocs.io/en/stable/connections.html#cluster-client), or even [Async Cluster connections](https://redis.readthedocs.io/en/stable/connections.html#async-cluster-client).

### Redis Commands

There is built-in support for all of the [out-of-the-box Redis commands](https://redis.io/commands). They are exposed using the raw Redis command names (`HSET`, `HGETALL`, etc.) except where a word (i.e. del) is reserved by the language. The complete set of commands can be found [here](https://github.com/redis/redis-py/tree/master/redis/commands), or [the documentation](https://redis.readthedocs.io/en/stable/commands.html).

## Advanced Topics

The [official Redis command documentation](https://redis.io/commands)
does a great job of explaining each command in detail. redis-py attempts
to adhere to the official command syntax. There are a few exceptions:

-   **MULTI/EXEC**: These are implemented as part of the Pipeline class.
    The pipeline is wrapped with the MULTI and EXEC statements by
    default when it is executed, which can be disabled by specifying
    transaction=False. See more about Pipelines below.

-   **SUBSCRIBE/LISTEN**: Similar to pipelines, PubSub is implemented as
    a separate class as it places the underlying connection in a state
    where it can\'t execute non-pubsub commands. Calling the pubsub
    method from the Redis client will return a PubSub instance where you
    can subscribe to channels and listen for messages. You can only call
    PUBLISH from the Redis client (see [this comment on issue
    #151](https://github.com/redis/redis-py/issues/151#issuecomment-1545015)
    for details).

For more details, please see the documentation on [advanced topics page](https://redis.readthedocs.io/en/stable/advanced_features.html).

### Pipelines

The following is a basic example of a [Redis pipeline](https://redis.io/docs/manual/pipelining/), a method to optimize round-trip calls, by batching Redis commands, and receiving their results as a list.


``` python
>>> pipe = r.pipeline()
>>> pipe.set('foo', 5)
>>> pipe.set('bar', 18.5)
>>> pipe.set('blee', "hello world!")
>>> pipe.execute()
[True, True, True]
```

### PubSub

The following example shows how to utilize [Redis Pub/Sub](https://redis.io/docs/manual/pubsub/) to subscribe to specific channels.

``` python
>>> r = redis.Redis(...)
>>> p = r.pubsub()
>>> p.subscribe('my-first-channel', 'my-second-channel', ...)
>>> p.get_message()
{'pattern': None, 'type': 'subscribe', 'channel': b'my-second-channel', 'data': 1}
```

### Redis’ search and query capabilities default dialect

Release 6.0.0 introduces a client-side default dialect for Redis’ search and query capabilities.
By default, the client now overrides the server-side dialect with version 2, automatically appending *DIALECT 2* to commands like *FT.AGGREGATE* and *FT.SEARCH*.

**Important**: Be aware that the query dialect may impact the results returned. If needed, you can revert to a different dialect version by configuring the client accordingly.

``` python
>>> from redis.commands.search.field import TextField
>>> from redis.commands.search.query import Query
>>> from redis.commands.search.index_definition import IndexDefinition
>>> import redis

>>> r = redis.Redis(host='localhost', port=6379, db=0)
>>> r.ft().create_index(
>>>     (TextField("name"), TextField("lastname")),
>>>     definition=IndexDefinition(prefix=["test:"]),
>>> )

>>> r.hset("test:1", "name", "James")
>>> r.hset("test:1", "lastname", "Brown")

>>> # Query with default DIALECT 2
>>> query = "@name: James Brown"
>>> q = Query(query)
>>> res = r.ft().search(q)

>>> # Query with explicit DIALECT 1
>>> query = "@name: James Brown"
>>> q = Query(query).dialect(1)
>>> res = r.ft().search(q)
```

You can find further details in the [query dialect documentation](https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/dialects/).

### Multi-database client (Active-Active)

The multi-database client allows your application to connect to multiple Redis databases, which are typically replicas of each other. It is designed to work with Redis Software and Redis Cloud Active-Active setups. The client continuously monitors database health, detects failures, and automatically fails over to the next healthy database using a configurable strategy. When the original database becomes healthy again, the client can automatically switch back to it.<br>
This is useful when:

1. You have more than one Redis deployment. This might include two independent Redis servers or two or more Redis databases replicated across multiple [active-active Redis Enterprise](https://redis.io/docs/latest/operate/rs/databases/active-active/) clusters.
2. You want your application to connect to one deployment at a time and to fail over to the next available deployment if the first deployment becomes unavailable.

For the complete failover configuration options and examples, see the [Multi-database client docs](https://redis.readthedocs.io/en/latest/geographic_failover.html).

### Bulk hash ingestion (HIMPORT)

Redis 8.10 adds the `HIMPORT` command family for loading many hashes that share
the same set of field names: register the field names once with `himport_prepare`,
then create each hash by sending only its values. Keys written this way are regular
hashes — every hash command works on them.

``` python
r.himport_prepare("users", ["name", "email", "age"])
r.himport_set("user:1", "users", ["alice", "alice@example.com", "25"])
r.himport_set("user:2", "users", ["bob", "bob@example.com", "30"])
r.himport_discard("users")  # => 1
```

Values pair positionally with the prepared fields. Hash enumeration order
(`HGETALL`, `HKEYS`) is not guaranteed to match the prepare order.

**Fieldsets are connection state.** A prepared fieldset lives in the server-side
session of the physical connection that prepared it: it is invisible to other
connections and destroyed by a disconnect or `RESET`. redis-py handles this for you —
`himport_prepare` records the fieldset in a client-level registry, and the `PREPARE`
is applied lazily on whatever pooled connection serves each `himport_set` (and
re-applied automatically after a reconnect, `RESET`, or Sentinel/cluster failover).
You declare each fieldset once per client with `himport_prepare`; there is no
constructor argument for it.

For the highest ingestion throughput, send the `PREPARE` and its `SET`s in one
pipeline — a single batch always executes on one connection:

``` python
with r.pipeline(transaction=False) as pipe:
    pipe.himport_prepare("users", ["name", "email", "age"])
    for uid, row in rows:
        pipe.himport_set(f"user:{uid}", "users", row)
    pipe.execute()
```

The automatic re-prepare applies to direct calls only, not to commands inside
`pipeline`/`transaction` blocks: a batched `himport_set` relies on the single
pre-flight `PREPARE` in that batch.

With `RedisCluster`, `himport_prepare` / `himport_discard` / `himport_discard_all`
update the client's shared, cluster-wide registry and return immediately — like the
standalone API, they perform no server I/O of their own. The server-side `PREPARE`
(and, after a discard, `DISCARD`) is applied lazily on each node's connection the
next time it serves an `himport_set`, and re-applied after reconnects or failover;
`himport_set` itself routes by the key's hash slot. A discard is therefore not
removed from every server session at once: each connection drops the fieldset on its
next `himport_set` (or on disconnect). With Sentinel, call
`himport_prepare` on the long-lived client returned by `master_for(...)`; the fieldset
survives failover automatically. HIMPORT is not supported on the multi-database
(Active-Active) client.

The async client mirrors this exactly:

``` python
await r.himport_prepare("users", ["name", "email", "age"])
await r.himport_set("user:1", "users", ["alice", "alice@example.com", "25"])
```

---------------------------------------------

### Author

redis-py is developed and maintained by [Redis Inc](https://redis.io). It can be found [here](
https://github.com/redis/redis-py), or downloaded from [pypi](https://pypi.org/project/redis/).

Special thanks to:

-   Andy McCurdy (<sedrik@gmail.com>) the original author of redis-py.
-   Ludovico Magnocavallo, author of the original Python Redis client,
    from which some of the socket code is still used.
-   Alexander Solovyov for ideas on the generic response callback
    system.
-   Paul Hubbard for initial packaging support.

[![Redis](https://github.com/redis/redis-py/blob/master/docs/_static/logo-redis.svg)](https://redis.io)
