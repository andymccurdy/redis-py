Lua Scripting
===

`Lua Scripting <#lua-scripting-in-default-connections>`__ \|
`Pipelines <#pipelines>`__ \| `Cluster mode <#cluster-mode>`__

--------------

Lua Scripting in default connections
------------------------------------

redis-py supports the EVAL, EVALSHA, and SCRIPT commands. However, there
are a number of edge cases that make these commands tedious to use in
real world scenarios. Therefore, redis-py exposes a Script object that
makes scripting much easier to use. (RedisClusters have limited support
for scripting.)

To create a Script instance, use the register_script function on a
client instance passing the Lua code as the first argument.
register_script returns a Script instance that you can use throughout
your code.

The following trivial Lua script accepts two parameters: the name of a
key and a multiplier value. The script fetches the value stored in the
key, multiplies it with the multiplier value and returns the result.

.. code:: python

   >>> r = redis.Redis()
   >>> lua = """
   ... local value = redis.call('GET', KEYS[1])
   ... value = tonumber(value)
   ... return value * ARGV[1]"""
   >>> multiply = r.register_script(lua)

multiply is now a Script instance that is invoked by calling it like a
function. Script instances accept the following optional arguments:

-  **keys**: A list of key names that the script will access. This
   becomes the KEYS list in Lua.
-  **args**: A list of argument values. This becomes the ARGV list in
   Lua.
-  **client**: A redis-py Client or Pipeline instance that will invoke
   the script. If client isn't specified, the client that initially
   created the Script instance (the one that register_script was invoked
   from) will be used.

Continuing the example from above:

.. code:: python

   >>> r.set('foo', 2)
   >>> multiply(keys=['foo'], args=[5])
   10

The value of key 'foo' is set to 2. When multiply is invoked, the 'foo'
key is passed to the script along with the multiplier value of 5. Lua
executes the script and returns the result, 10.

Script instances can be executed using a different client instance, even
one that points to a completely different Redis server.

.. code:: python

   >>> r2 = redis.Redis('redis2.example.com')
   >>> r2.set('foo', 3)
   >>> multiply(keys=['foo'], args=[5], client=r2)
   15

The Script object ensures that the Lua script is loaded into Redis's
script cache. In the event of a NOSCRIPT error, it will load the script
and retry executing it.

Pipelines
---------

Script objects can also be used in pipelines. The pipeline instance
should be passed as the client argument when calling the script. Care is
taken to ensure that the script is registered in Redis's script cache
just prior to pipeline execution.

.. code:: python

   >>> pipe = r.pipeline()
   >>> pipe.set('foo', 5)
   >>> multiply(keys=['foo'], args=[5], client=pipe)
   >>> pipe.execute()
   [True, 25]

Cluster Mode
------------

Cluster mode has limited support for lua scripting.

The following commands are supported, with caveats:

- ``EVAL`` and ``EVALSHA``: The command is sent to the relevant node,
  depending on the keys (i.e., in ``EVAL "<script>" num_keys key_1 ...
  key_n ...``). The keys *must* all be on the same node. If the script
  requires 0 keys, *the command is sent to a random (primary) node*.
- ``SCRIPT EXISTS``: The command is sent to all primaries. The result
  is a list of booleans corresponding to the input SHA hashes. Each
  boolean is an AND of “does the script exist on each node?”. In other
  words, each boolean is True iff the script exists on all nodes.
- ``SCRIPT FLUSH``: The command is sent to all primaries. The result
  is a bool AND over all nodes’ responses.
- ``SCRIPT LOAD``: The command is sent to all primaries. The result
  is the SHA1 digest.

The following commands are not supported:

- ``EVAL_RO``
- ``EVALSHA_RO``

``EVALSHA`` can be used inside a ``ClusterPipeline``. Keys must map to the
same hash slot (or ``numkeys`` may be ``0``, in which case the command is
routed to a random primary, exactly as in the non-pipelined case above). In a
transactional pipeline (``pipeline(transaction=True)``), zero-key ``EVALSHA``
reuses the transaction's existing slot when one is already chosen, so multiple
zero-key scripts (or a mix with keyed commands) stay single-slot.

``load_scripts`` and ``script_load_for_pipeline`` remain **not supported**
on cluster pipelines. On the sync client, ``ClusterPipeline.eval()`` is also
blocked; the async cluster pipeline has no ``eval`` override.

Important caveats when using ``EVALSHA`` in a cluster pipeline:

- The Lua script cache in Redis is **per-node and is not replicated**.
  ``SCRIPT LOAD`` loads the script onto the *current* primaries only, at the
  moment it is called.
- Any topology change -- a failover that promotes a replica, a rolling
  upgrade that replaces nodes, resharding, or adding a new shard -- can route
  an ``EVALSHA`` to a node whose cache does not contain the script, which
  fails with ``NOSCRIPT`` (``redis.exceptions.NoScriptError``).
- Unlike the non-pipelined ``Script`` object, **a cluster pipeline performs no
  automatic reload or retry** on ``NOSCRIPT``. Recovery is the caller's
  responsibility: catch ``NoScriptError``, re-run ``SCRIPT LOAD``, then retry
  only when replay is safe (for example an idempotent or single-command
  pipeline). Blindly re-executing a multi-command pipeline can duplicate
  side effects: Redis still runs the rest of a non-transactional batch when
  one ``EVALSHA`` returns ``NOSCRIPT``, and the client raises only after
  reading every response. Because zero-key ``EVALSHA`` is routed to a random
  primary, the script must be present on **all** primaries.
- In a **transactional** pipeline, a ``NOSCRIPT`` from ``EVALSHA`` is raised at
  ``EXEC`` time and does **not** roll back the other commands (this follows
  Redis ``MULTI``/``EXEC`` semantics), so partial application is possible.

.. code:: python

   >>> from redis.exceptions import NoScriptError
   >>> sha = rc.script_load(lua)  # loads on all current primaries
   >>> def run():
   ...     with rc.pipeline() as pipe:
   ...         pipe.evalsha(sha, 1, "{user}:1")
   ...         return pipe.execute()
   >>> try:
   ...     result = run()
   ... except NoScriptError:
   ...     # single-command pipeline: safe to reload and retry
   ...     sha = rc.script_load(lua)  # reload on current primaries
   ...     result = run()
