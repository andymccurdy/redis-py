.. redis-py documentation master file, created by
   sphinx-quickstart on Thu Jul 28 13:55:57 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

redis-py - Python Client for Redis
====================================

Getting Started
****************

`redis-py <https://pypi.org/project/redis>`_ requires a running Redis server, and Python 3.7+. See the `Redis
quickstart <https://redis.io/topics/quickstart>`_ for Redis installation instructions.

redis-py can be installed using pip via ``pip install redis``.


Quickly connecting to redis
***************************

There are two quick ways to connect to Redis.

**Assuming you run Redis on localhost:6379 (the default)**

.. code-block:: python

   import redis
   r = redis.Redis()
   r.ping()

**Running redis on foo.bar.com, port 12345**

.. code-block:: python

   import redis
   r = redis.Redis(host='foo.bar.com', port=12345)
   r.ping()

**Another example with foo.bar.com, port 12345**

.. code-block:: python

   import redis
   r = redis.from_url('redis://foo.bar.com:12345')
   r.ping()

After that, you probably want to `run redis commands <commands.html>`_.

.. toctree::
   :hidden:

   genindex

Redis Command Functions
***********************
.. toctree::
   :maxdepth: 2

   commands
   redismodules

Module Documentation
********************
.. toctree::
   :maxdepth: 1

   connections
   clustering
   geographic_failover
   exceptions
   backoff
   lock
   retry
   lua_scripting
   opentelemetry
   resp3_features
   unified_responses
   advanced_features
   examples

Contributing
*************

- `How to contribute <https://github.com/redis/redis-py/blob/master/CONTRIBUTING.md>`_
- `Issue Tracker <https://github.com/redis/redis-py/issues>`_
- `Source Code <https://github.com/redis/redis-py/>`_
- `Release History <https://github.com/redis/redis-py/releases/>`_

License
*******

This project is licensed under the `MIT license <https://github.com/redis/redis-py/blob/master/LICENSE>`_.
