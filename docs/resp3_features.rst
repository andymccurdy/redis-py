RESP 3 Features
===============

As of version 5.0, redis-py supports the `RESP 3 standard <https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md>`_. Starting with redis-py 8.0, clients use RESP3 on the wire by default.

By default, redis-py keeps legacy RESP2-compatible Python response shapes for
existing applications. Set ``protocol=3`` explicitly when your application
should receive RESP3-specific Python response shapes or when you want the wire
protocol choice to be visible in code. Set ``protocol=2`` to force RESP2 on the
wire. Set
``legacy_responses=False`` to opt in to protocol-independent unified response
shapes; see :doc:`unified_responses`.

Connecting
-----------

The default connection already uses RESP3 on the wire in redis-py 8.0 and
later while preserving legacy RESP2-compatible Python response shapes. The
following examples show how to set ``protocol=3`` explicitly when you want
RESP3-specific response shapes or visible protocol configuration for standard,
async, and cluster clients.

Connect with a standard connection, explicitly specifying RESP3:

.. code:: python

    >>> import redis
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3)
    >>> r.ping()

Or using the URL scheme:

.. code:: python

    >>> import redis
    >>> r = redis.from_url("redis://localhost:6379?protocol=3")
    >>> r.ping()

Connect with async, explicitly specifying RESP3:

.. code:: python

    >>> import redis.asyncio as redis
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3)
    >>> await r.ping()

The URL scheme with the async client

.. code:: python

    >>> import redis.asyncio as Redis
    >>> r = redis.from_url("redis://localhost:6379?protocol=3")
    >>> await r.ping()

Connecting to an OSS Redis Cluster with RESP 3

.. code:: python

    >>> from redis.cluster import RedisCluster, ClusterNode
    >>> r = RedisCluster(startup_nodes=[ClusterNode('localhost', 6379), ClusterNode('localhost', 6380)], protocol=3)
    >>> r.ping()

Push notifications
------------------

Push notifications are a way that redis sends out of band data. The RESP 3 protocol includes a `push type <https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md#push-type>`_ that allows our client to intercept these out of band messages. By default, clients will log simple messages, but redis-py includes the ability to bring your own function processor.

This means that should you want to perform something, on a given push notification, you specify a function during the connection, as per this examples:

.. code:: python

    >> from redis import Redis
    >>
    >> def our_func(message):
    >>    if message.find("This special thing happened"):
    >>        raise IOError("This was the message: \n" + message)
    >>
    >> r = Redis(protocol=3)
    >> p = r.pubsub(push_handler_func=our_func)

In the example above, upon receipt of a push notification, rather than log the message, in the case where specific text occurs, an IOError is raised. This example, highlights how one could start implementing a customized message handler.

Client-side caching
-------------------

Client-side caching is a technique used to create high performance services.
It utilizes the memory on application servers, typically separate from the database nodes, to cache a subset of the data directly on the application side.
For more information please check the `Redis client-side caching documentation <https://redis.io/docs/latest/develop/use/client-side-caching/>`_.
Please notice that this feature is available only with RESP3 protocol enabled
in sync clients. redis-py 8.0 and later use RESP3 on the wire by default, and
the examples below pass ``protocol=3`` explicitly to make the requirement clear.
Supported in standalone, Cluster, and Sentinel clients.

Basic usage:

Enable caching with default configuration:

.. code:: python

    >>> import redis
    >>> from redis.cache import CacheConfig
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3, cache_config=CacheConfig())

The same interface applies to Redis Cluster and Sentinel.

Enable caching with custom cache implementation:

.. code:: python

    >>> import redis
    >>> from foo.bar import CacheImpl
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3, cache=CacheImpl())

CacheImpl should implement a `CacheInterface` specified in `redis.cache` package.

Which commands are cached
~~~~~~~~~~~~~~~~~~~~~~~~~

A reply is cached only when both of the following hold:

1. The command's metadata says its reply may be cached at all: it is ``readonly``, is not
   ``blocking``, takes at least one key name argument, and carries none of the
   ``nondeterministic_output``, ``script_runner`` or ``dont_cache`` markers.
2. The client can identify the key arguments of that particular invocation. A command whose
   keys the client does not yet extract executes normally with caching skipped - it never
   fails and never changes the reply.

The first question is answered by a ``redis.commands.metadata.MetadataResolver``, which is
also what the Cluster client resolves its routing by. By default the client resolves the
command metadata this library ships, so enabling caching adds no ``COMMAND`` round trips.
An unknown command is never cached.

A different resolver can be supplied as ``metadata_resolver``, which is the seam to implement
if you need eligibility decided some other way - it is a small public ABC, and
``StaticMetadataResolver`` shows what a resolver has to answer. Resolvers chain through
``with_fallback``, first match wins, so a resolver placed in front of the static one overrides
the commands it carries and the static records answer for everything else.

Eligibility can also be decided from the connected server, by building a
``DynamicMetadataResolver`` from a live ``COMMAND`` reply. Use it with care: reading that reply
relies on ``CommandsParser``, which lives in the private ``redis._parsers`` package and is not
part of the public API.

.. code:: python

    >>> import redis
    >>> from redis._parsers.commands import CommandsParser
    >>> from redis.cache import CacheConfig
    >>> from redis.commands.metadata import DynamicMetadataResolver, StaticMetadataResolver
    >>> records = CommandsParser(redis.Redis(host='localhost', port=6379)).get_commands_metadata_cache()
    >>> resolver = StaticMetadataResolver(fallback=DynamicMetadataResolver(records))
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3,
    ...                 cache_config=CacheConfig(), metadata_resolver=resolver)

Chained this way the server answers only for the commands the shipped table does not carry;
swap the order to let the server override it.

The static table is the more trustworthy of the two sources. For some commands, the server
metadata is incomplete or wrong. These are ``TOUCH``, ``VRANDMEMBER``, and the read-only
script runners on servers older than 8.10. The static table has correct entries for them.

Some read commands do not keep their keys at fixed argument positions. Redis marks these
commands with the ``movablekeys`` flag. Examples are ``SINTERCARD``, ``ZDIFF``, ``ZINTER``,
``ZINTERCARD``, ``ZUNION`` and ``XREAD``. Both sources mark these commands as keyed:

- The static table lists them as keyed commands.
- With the server metadata, the parser reads their key specs and marks them as keyed.

In both cases, the Cluster client asks the server for their keys with ``COMMAND GETKEYS``
and routes the command by those keys.

The same argument is accepted by ``ConnectionPool`` (configure it there when you supply your
own ``connection_pool=``) and by ``RedisCluster``, which shares the one resolver with every
node's client. On the Cluster client it also supersedes ``policy_resolver``: given only
``metadata_resolver``, routing is derived from it, so one object serves both. An explicit
``policy_resolver`` still decides routing, for backwards compatibility.

``CacheConfig.DEFAULT_ALLOW_LIST`` is deprecated and no longer consulted.

Tracking modes
~~~~~~~~~~~~~~

Eligibility decides what *may* be cached. ``tracking_mode`` and ``cache_predicate`` decide what
the application *wants* cached, and with it how much the server has to remember.

By default every cache-managed connection sends ``CLIENT TRACKING ON``, so the server remembers
the keys of every read-only, keyed command that connection performs - whether or not the reply
was stored. ``redis.cache.TrackingMode`` selects a narrower contract:

- ``TrackingMode.PLAIN`` - default behaviour. Every trackable read is tracked, and every
  eligible reply is stored.
- ``TrackingMode.OPTIN`` - ``CLIENT TRACKING ON OPTIN``. The server remembers nothing unless
  ``CLIENT CACHING YES`` comes right before the read, which the client sends for exactly the
  misses it is about to store. Pick it when you cache a small, chosen subset.
- ``TrackingMode.OPTOUT`` - ``CLIENT TRACKING ON OPTOUT``. The server remembers every trackable
  read unless ``CLIENT CACHING NO`` comes right before it, which the client sends for the reads
  it will not store. Pick it when you cache almost everything.

``cache_predicate`` is the intent decision. It is called with the command name and the keys of
the invocation, and is consulted under ``optin`` and ``optout`` only - passing it with ``plain``
warns and is ignored. The keys arrive as the
invocation supplied them, so a predicate that inspects them should not assume ``str`` or
``bytes`` - ``redis.utils.str_if_bytes`` normalises both, as in the examples below.

.. code:: python

    >>> import redis
    >>> from redis.cache import CacheConfig, TrackingMode
    >>> from redis.utils import str_if_bytes
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3,
    ...                 cache_config=CacheConfig(
    ...                     max_size=10_000,
    ...                     tracking_mode=TrackingMode.OPTOUT,
    ...                     cache_predicate=lambda command, keys: not str_if_bytes(keys[0]).startswith('counter:'),
    ...                 ))
    >>> r.get('user:42')          # sent alone; stored, and tracked by default
    >>> r.get('counter:hits')     # CLIENT CACHING NO + GET, one write; not stored, not tracked

Under ``optin`` the predicate selects what to cache instead:

.. code:: python

    >>> r = redis.Redis(host='localhost', port=6379, protocol=3,
    ...                 cache_config=CacheConfig(
    ...                     tracking_mode=TrackingMode.OPTIN,
    ...                     cache_predicate=lambda command, keys: str_if_bytes(keys[0]).startswith('user:'),
    ...                 ))
    >>> r.get('user:42')          # miss: CLIENT CACHING YES + GET, one write; stored, tracked
    >>> r.get('user:42')          # hit: nothing on the wire
    >>> r.get('session:9')        # sent alone; not stored, not tracked

``optin`` with no ``cache_predicate`` caches **nothing** - every read is sent alone and left
untracked. That configuration is inert, and the client warns about it when the config is built.

Some things work the same in every mode:

- A cache hit sends nothing to the server.
- Pipelines, transactions and ``send_packed_command`` bypass the cache. Under ``optout`` the
  server still tracks the keys they read.
- A command redirected with ``ASK`` is sent without ``CLIENT CACHING``, and its reply is not
  stored. ``ASKING`` and ``CLIENT CACHING`` each apply only to the command that immediately
  follows them, so one command cannot carry both. Pairing the redirected read with
  ``CLIENT CACHING`` would strip the ``ASK`` allowance, and the read would be redirected
  again. The reply is not stored because it belongs to a slot that is migrating. Under
  ``optout`` the server tracks the read by default anyway.
- Under ``optout``, the client sends ``CLIENT CACHING NO`` only for commands that the metadata
  table marks ``readonly``. Any other read stays tracked. The worst case is an unused entry in
  the server's invalidation table, never a stale reply.

The mode is sent in the tracking handshake, because the server refuses to switch a live
connection between ``OPTIN`` and ``OPTOUT``. A configuration change therefore applies to new
connections only.

Commands the cache owns
~~~~~~~~~~~~~~~~~~~~~~~

The cache depends on the tracking state of each connection it manages. Three commands change
that state, so a client that has a cache refuses them with a ``RedisError``, on every
connection and in every mode. This includes pipelines and transactions:

- ``CLIENT CACHING``. The server applies the flag to the next command on that socket, and a
  pooled client cannot promise which command that is. A stray ``CLIENT CACHING NO`` under
  ``optout`` would leave the next cached read untracked, so its stored reply would never be
  invalidated. The client sends ``CLIENT CACHING`` itself, together with the read it applies
  to, whenever the tracking mode needs it.
- ``CLIENT TRACKING``, in every form, including ``client_tracking_on()`` and
  ``client_tracking_off()``. Turning tracking off or redirecting it would leave the cache
  storing replies that no invalidation reaches. The client enables tracking itself when it
  opens the connection.
- ``RESET``, including ``reset()``. It turns off tracking together with the rest of the
  connection state.

``CLIENT TRACKINGINFO`` only reads the state, so it stays available. A client without a cache
sends all of these commands to the server as before.

Client-side caching is not yet implemented in the async clients, so
``redis.asyncio.Redis`` takes no ``metadata_resolver`` argument. However,
``redis.asyncio.RedisCluster`` accepts ``metadata_resolver`` for dynamic replica routing.

More comprehensive documentation soon will be available at the `Redis documentation site <https://redis.io/docs/latest/>`_.
