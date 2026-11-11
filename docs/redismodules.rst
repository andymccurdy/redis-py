Redis Modules Commands
######################

Redis Open Source 8.0 and later include Search, JSON, time series, and
probabilistic commands. You can run the examples below without installing
separate modules. To start a local Redis 8 server with Docker:

.. code-block:: console

    docker run --rm -p 127.0.0.1:6379:6379 redis:8

For older Redis versions, use a server with the corresponding modules enabled,
such as Redis Stack. See the `Redis module lifecycle documentation
<https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/modules-lifecycle/>`_
for version details.


RedisBloom Commands
*******************

These are the commands for interacting with the `RedisBloom module <https://redisbloom.io>`_. Below is a brief example, as well as documentation on the commands themselves.

**Create and add to a bloom filter**

.. code-block:: python

    import redis
    r = redis.Redis()
    r.bf().create("bloom", 0.01, 1000)
    r.bf().add("bloom", "foo")

**Create and add to a cuckoo filter**

.. code-block:: python

    import redis
    r = redis.Redis()
    r.cf().create("cuckoo", 1000)
    r.cf().add("cuckoo", "filter")

**Create Count-Min Sketch and get information**

.. code-block:: python

    import redis
    r = redis.Redis()
    r.cms().initbydim("dim", 1000, 5)
    r.cms().incrby("dim", ["foo"], [5])
    r.cms().info("dim")

**Create a topk list, and access the results**

.. code-block:: python

    import redis
    r = redis.Redis()
    r.topk().reserve("mytopk", 3, 50, 4, 0.9)
    r.topk().info("mytopk")

.. automodule:: redis.commands.bf.commands
    :members: BFCommands, CFCommands, CMSCommands, TOPKCommands

------

RedisJSON Commands
******************

These are the commands for interacting with the `RedisJSON module <https://redisjson.io>`_. Below is a brief example, as well as documentation on the commands themselves.

**Create a json object**

.. code-block:: python

    import redis
    r = redis.Redis()
    r.json().set("mykey", ".", {"hello": "world", "i am": ["a", "json", "object!"]})

Examples of how to combine search and json can be found `here <examples/search_json_examples.html>`_.

.. automodule:: redis.commands.json.commands
    :members: JSONCommands

-----

RediSearch Commands
*******************

These are the commands for interacting with the `RediSearch module <https://redisearch.io>`_. Below is a brief example, as well as documentation on the commands themselves. In the example
below, an index named *my_index* is being created. When an index name is not specified, an index named *idx* is created.

**Create a search index, and display its information**

.. code-block:: python

    import redis
    from redis.commands.search.field import TextField

    r = redis.Redis()
    index_name = "my_index"
    schema = (
        TextField("play", weight=5.0),
        TextField("ball"),
    )
    r.ft(index_name).create_index(schema)
    print(r.ft(index_name).info())


.. automodule:: redis.commands.search.commands
    :members: SearchCommands

-----

RedisTimeSeries Commands
************************

These are the commands for interacting with the `RedisTimeSeries module <https://redistimeseries.io>`_. Below is a brief example, as well as documentation on the commands themselves.


**Create a timeseries object with 5 second retention**

.. code-block:: python

    import redis
    r = redis.Redis()
    r.ts().create(2, retention_msecs=5000)

.. automodule:: redis.commands.timeseries.commands
    :members: TimeSeriesCommands


