Connecting to Redis
###################


Generic Client
**************

This is the client used to connect directly to a standard Redis node.

.. autoclass:: redis.Redis
   :members:


Sentinel Client
***************

Redis `Sentinel <https://redis.io/topics/sentinel>`_ provides high availability for Redis. There are commands that can only be executed against a Redis node running in sentinel mode. Connecting to those nodes, and executing commands against them requires a Sentinel connection.

Connection example (assumes Redis exists on the ports listed below):

   >>> from redis import Sentinel
   >>> sentinel = Sentinel([('localhost', 26379)], socket_timeout=0.1)
   >>> sentinel.discover_master('mymaster')
   ('127.0.0.1', 6379)
   >>> sentinel.discover_slaves('mymaster')
   [('127.0.0.1', 6380)]

Sentinel
========
.. autoclass:: redis.sentinel.Sentinel
    :members:

SentinelConnectionPool
======================
.. autoclass:: redis.sentinel.SentinelConnectionPool
    :members:


Cluster Client
**************

This client is used for connecting to a Redis Cluster.

RedisCluster
============
.. autoclass:: redis.cluster.RedisCluster
    :members:

ClusterNode
===========
.. autoclass:: redis.cluster.ClusterNode
    :members:


Async Client
************

See complete example: `here <examples/asyncio_examples.html>`__

This client is used for communicating with Redis, asynchronously.

.. autoclass:: redis.asyncio.client.Redis
    :members:


Async Cluster Client
********************

RedisCluster (Async)
====================
.. autoclass:: redis.asyncio.cluster.RedisCluster
    :members:
    :member-order: bysource

ClusterNode (Async)
===================
.. autoclass:: redis.asyncio.cluster.ClusterNode
    :members:
    :member-order: bysource

ClusterPipeline (Async)
=======================
.. autoclass:: redis.asyncio.cluster.ClusterPipeline
    :members: execute_command, execute
    :member-order: bysource


Connection
**********

See complete example: `here <examples/connection_examples.html>`__

Connection
==========
.. autoclass:: redis.connection.Connection
    :members:

Connection (Async)
==================
.. autoclass:: redis.asyncio.connection.Connection
    :members:


Connection Pools
****************

See complete example: `here <examples/connection_examples.html>`__

Each client takes its connections from a pool. Since redis-py 8.0, a ``ConnectionPool`` holds at
most 100 connections by default (``max_connections=100``). When all of them are in use, it does
not wait for one to be released: it raises ``MaxConnectionsError`` ("Too many connections")
straight away. This is easiest to hit with the asyncio client, where many commands can be in
flight at the same time.

There are two ways to handle bursts of concurrent requests:

* Raise the limit, if Redis and your system can take more connections:

  .. code-block:: python

     import redis

     r = redis.Redis(host="localhost", port=6379, max_connections=500)

* Use a ``BlockingConnectionPool``. When all of its connections are in use, a caller waits for one
  to be released, for up to ``timeout`` seconds, and only then raises ``ConnectionError``:

  .. code-block:: python

     import redis

     pool = redis.BlockingConnectionPool(
         host="localhost", port=6379, max_connections=100, timeout=10
     )
     r = redis.Redis(connection_pool=pool)

  The asyncio client has the same pool:

  .. code-block:: python

     import redis.asyncio

     pool = redis.asyncio.BlockingConnectionPool.from_url(
         "redis://localhost:6379", max_connections=100, timeout=10
     )
     r = redis.asyncio.Redis.from_pool(pool)

  ``BlockingConnectionPool`` has its own defaults: 50 connections and a 20 second timeout.

ConnectionPool
==============
.. autoclass:: redis.connection.ConnectionPool
    :members:

ConnectionPool (Async)
======================
.. autoclass:: redis.asyncio.connection.ConnectionPool
    :members:

BlockingConnectionPool
======================
.. autoclass:: redis.connection.BlockingConnectionPool
    :members:

BlockingConnectionPool (Async)
==============================
.. autoclass:: redis.asyncio.connection.BlockingConnectionPool
    :members:
