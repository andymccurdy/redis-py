Migrate to Unified Responses
============================

redis-py can return protocol-independent Python response shapes. When unified
responses are enabled, redis-py applies the same public response structure for
the affected commands whether the connection uses RESP2 or RESP3.

Unified responses are selected with ``legacy_responses=False``. The protocol
still controls the wire format; ``legacy_responses`` controls the Python shape
returned by redis-py.

Unified responses are the recommended mode for new projects and for
applications that can update their response handling. They provide the most
portable application-facing API because command results keep the same Python
shape when you change the Redis serialization protocol.

Enable Unified Responses
------------------------

Use ``legacy_responses=False`` when constructing a client.

.. code:: python

   import redis

   # Default wire protocol, unified Python responses.
   r = redis.Redis(legacy_responses=False)

   # RESP2 wire, unified Python responses.
   r_resp2 = redis.Redis(protocol=2, legacy_responses=False)

   # RESP3 wire, unified Python responses.
   r_resp3 = redis.Redis(protocol=3, legacy_responses=False)

The same option is available for asyncio and cluster clients.

.. code:: python

   import redis.asyncio as redis_async
   from redis.cluster import RedisCluster

   async_r = redis_async.Redis(legacy_responses=False)
   cluster = RedisCluster(host="localhost", port=6379, legacy_responses=False)

Connection URLs can also select unified responses.

.. code:: python

   r = redis.from_url("redis://localhost:6379?legacy_responses=false")
   r = redis.from_url(
       "redis://localhost:6379?protocol=2&legacy_responses=false"
   )


Response Modes
--------------

.. list-table::
   :header-rows: 1
   :widths: 30 25 45

   * - Client options
     - Wire protocol
     - Python response shape
   * - ``Redis()``
     - Default RESP3 wire
     - Legacy RESP2-compatible shape
   * - ``Redis(protocol=2)``
     - RESP2
     - Legacy RESP2 shape
   * - ``Redis(protocol=3)``
     - RESP3
     - Native RESP3 shape
   * - ``Redis(legacy_responses=False)``
     - Default RESP3 wire
     - Unified shape
   * - ``Redis(protocol=2, legacy_responses=False)``
     - RESP2
     - Unified shape
   * - ``Redis(protocol=3, legacy_responses=False)``
     - RESP3
     - Unified shape

``decode_responses`` is independent from unified responses. It still controls
bulk-string decoding where the command parser does not otherwise normalize a
structural key or value.


Migration Checklist
-------------------

1. Find the redis-py client construction points in your application, including
   background workers, admin scripts, asyncio clients, and cluster clients.
2. Enable ``legacy_responses=False`` for one environment or service path first.
   The server protocol can stay unchanged unless you also want to pin
   ``protocol=2`` or ``protocol=3``.
3. Check the Redis commands and ``execute_command()`` calls your application
   depends on against the tables below. Update application API type hints,
   response models, serializers, and assertions to reflect the unified
   response shapes.
4. Review module command responses such as JSON, TimeSeries, RediSearch, and
   probabilistic commands separately; several of them return richer objects or
   nested containers in unified mode.
5. Keep ``decode_responses`` decisions separate. Unified responses normalize
   response structures, while ``decode_responses`` still controls how bulk
   string data is decoded.
6. Roll the change out gradually and monitor application paths that parse Redis
   responses manually.


RESP2 Legacy to Unified
-----------------------

The following tables summarize the main differences when moving from
``protocol=2`` with legacy responses to unified responses.

Core Commands
^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 24 32 22 22

   * - Command
     - Change
     - RESP2 legacy example
     - Unified example
   * - ``ZDIFF`` with scores
     - Flat or tuple score pairs normalize to list pairs with float scores.
     - ``[b"a", b"1"]`` or ``[(b"a", b"1")]``
     - ``[[b"a", 1.0]]``
   * - ``ZINTER``, ``ZRANGE``, ``ZRANGEBYSCORE``, ``ZREVRANGE``, ``ZREVRANGEBYSCORE``, ``ZUNION`` with scores
     - Tuple pairs become list pairs; scores are floats.
     - ``[(b"a", b"1")]``
     - ``[[b"a", 1.0]]``
   * - ``ZPOPMAX``, ``ZPOPMIN``
     - Tuple pairs become list pairs; scores are floats.
     - ``[(b"a", 3)]``
     - ``[[b"a", 3.0]]``
   * - ``BZPOPMAX``, ``BZPOPMIN``
     - Tuple result becomes list result; score is a float.
     - ``(b"key", b"a", 3)``
     - ``[b"key", b"a", 3.0]``
   * - ``ZRANK``, ``ZREVRANK`` with score
     - Rank response score is a float.
     - ``[2, b"3"]``
     - ``[2, 3.0]``
   * - ``ZSCAN``
     - Score pairs are lists; ``score_cast_func`` receives a float.
     - ``(0, [(b"a", b"1")])``
     - ``(0, [[b"a", 1.0]])``
   * - ``ZRANDMEMBER`` with scores
     - Flat interleaved values become nested score pairs.
     - ``[b"a", b"1", b"b", b"2"]``
     - ``[[b"a", 1.0], [b"b", 2.0]]``
   * - ``HRANDFIELD`` with values
     - Flat interleaved values become nested field/value pairs.
     - ``[b"f1", b"v1", b"f2", b"v2"]``
     - ``[[b"f1", b"v1"], [b"f2", b"v2"]]``
   * - ``BLPOP``, ``BRPOP``
     - Tuple result becomes list result.
     - ``(b"key", b"value")``
     - ``[b"key", b"value"]``
   * - ``ZMPOP``, ``BZMPOP``
     - Score values are floats inside nested list pairs.
     - ``[b"key", [(b"a", b"1")]]``
     - ``[b"key", [[b"a", 1.0]]]``
   * - ``XREAD``, ``XREADGROUP``
     - List of stream entries becomes a dict keyed by stream.
     - ``[[b"s", [(b"1-0", {})]]]``
     - ``{b"s": [(b"1-0", {})]}``
   * - ``LCS`` with ``IDX``
     - Flat key/value list becomes a dict with string keys.
     - ``[b"matches", [...], b"len", 3]``
     - ``{"matches": [...], "len": 3}``
   * - ``STRALGO ... IDX``
     - Match ranges use lists and string keys.
     - ``{"matches": [((0, 2), (0, 2))], "len": 3}``
     - ``{"matches": [[[0, 2], [0, 2]]], "len": 3}``
   * - ``CLIENT TRACKINGINFO``
     - Flat list becomes dict; structural strings are decoded.
     - ``[b"flags", [b"on"], b"redirect", -1]``
     - ``{"flags": ["on"], "redirect": -1}``
   * - ``COMMAND``
     - Flags and ACL categories are sets of strings.
     - ``{"get": {"flags": [b"readonly"]}}``
     - ``{"get": {"flags": {"readonly"}, "acl_categories": {...}}}``
   * - ``ACL GETUSER``
     - Selector lists become selector dicts.
     - ``{"selectors": [[b"~*", b"+get"]]}``
     - ``{"selectors": [{"keys": "~*", "commands": "+get"}]}``
   * - ``ACL LOG``
     - ``age-seconds`` is float and ``client-info`` is parsed.
     - ``{"age-seconds": b"0.5"}``
     - ``{"age-seconds": 0.5, "client-info": {...}}``
   * - ``SENTINEL`` state commands
     - ``flags`` becomes a set and derived booleans are present.
     - ``{"flags": "master,odown"}``
     - ``{"flags": {"master", "odown"}, "is_master": True}``
   * - ``CLUSTER LINKS``
     - Link dictionaries use string keys.
     - ``[{b"direction": b"to"}]``
     - ``[{"direction": b"to"}]``
   * - ``CLUSTER SHARDS``
     - Shard and node dictionaries use string keys.
     - ``{b"nodes": [{b"id": b"abc"}]}``
     - ``{"nodes": [{"id": b"abc"}]}``
   * - ``GEOPOS``
     - Coordinates are lists.
     - ``[(1.0, 2.0), None]``
     - ``[[1.0, 2.0], None]``
   * - ``GEOSEARCH``, ``GEORADIUS``, ``GEORADIUSBYMEMBER`` with coordinates
     - RESP2 and RESP3 unified responses use tuple coordinates.
     - ``[b"place", (1.0, 2.0)]``
     - ``[b"place", (1.0, 2.0)]``
   * - ``FUNCTION LIST``
     - Flat function data becomes nested dictionaries.
     - ``[[b"library_name", b"lib", ...]]``
     - ``[{b"library_name": b"lib", b"functions": [...]}]``
   * - ``FUNCTION STATS``
     - Flat stats become nested dictionaries.
     - ``[b"running_script", None, b"engines", [b"LUA", [...]]]``
     - ``{b"running_script": None, b"engines": {b"LUA": {...}}}``
   * - ``INCREX`` with ``BYFLOAT``
     - The new value and the increment are floats.
     - ``[b"1.75", b"0.25"]``
     - ``[1.75, 0.25]``
   * - ``MEMORY STATS``
     - Structural keys are decoded and numeric values use native types.
     - ``{b"peak.allocated": b"1024"}``
     - ``{"peak.allocated": 1024}``

JSON
^^^^

.. list-table::
   :header-rows: 1
   :widths: 24 32 22 22

   * - Command
     - Change
     - RESP2 legacy example
     - Unified example
   * - ``JSON.NUMINCRBY``, ``JSON.NUMMULTBY``
     - Legacy scalar paths are normalized with JSONPath array behavior.
     - ``5``
     - ``[5]``
   * - ``JSON.RESP``
     - Numeric string leaves that represent floats become Python floats.
     - ``[b"{", b"price", b"-19.5"]``
     - ``[b"{", b"price", -19.5]``
   * - ``JSON.OBJKEYS``
     - Key values respect ``decode_responses`` instead of being forced to str.
     - ``["a", "b"]``
     - ``[b"a", b"b"]`` when ``decode_responses=False``

TimeSeries
^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 24 32 22 22

   * - Command
     - Change
     - RESP2 legacy example
     - Unified example
   * - ``TS.GET``
     - Tuple becomes list.
     - ``(1, 2.0)``
     - ``[1, 2.0]``
   * - ``TS.RANGE``, ``TS.REVRANGE``
     - Sample tuples become sample lists.
     - ``[(1, 2.0)]``
     - ``[[1, 2.0]]``
   * - ``TS.MGET``
     - Sorted list of dicts becomes a key/value dict.
     - ``[{b"k": [{}, None, None]}]``
     - ``{b"k": [{}, [1, 2.0]]}``
   * - ``TS.MRANGE``, ``TS.MREVRANGE``
     - Sorted list of dicts becomes a dict with metadata slot.
     - ``[{b"k": [labels, samples]}]``
     - ``{b"k": [labels, metadata, samples]}``
   * - ``TS.QUERYINDEX``
     - Key strings are preserved instead of coerced to numbers.
     - ``2``
     - ``"2"``

RediSearch Commands
^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 24 32 22 22

   * - Command
     - Change
     - RESP2 legacy example
     - Unified example
   * - ``FT.INFO``
     - Attribute sublists become structured dictionaries.
     - ``[["identifier", "title", "SORTABLE"]]``
     - ``[{"identifier": "title", "flags": ["SORTABLE"]}]``
   * - ``FT.CONFIG GET``
     - Keys and values are strings.
     - ``{b"TIMEOUT": b"500"}``
     - ``{"TIMEOUT": "500"}``
   * - ``FT.SEARCH``
     - ``Result`` includes response warnings.
     - ``result.total``, ``result.docs``
     - ``result.total``, ``result.docs``, ``result.warnings``
   * - ``FT.AGGREGATE``
     - ``AggregateResult`` includes ``total`` and ``warnings``.
     - ``result.rows``
     - ``result.total``, ``result.rows``, ``result.warnings``
   * - ``FT.PROFILE``
     - Returns parsed result plus ``ProfileInformation``.
     - ``(result, ProfileInformation(list_data))``
     - ``(result, ProfileInformation(profile_data))``
   * - ``FT.HYBRID``
     - Returns ``HybridResult``; result field values and warnings stay bytes by default.
     - ``HybridResult(..., results=[{"field": b"value"}])``
     - ``HybridResult(..., results=[{"field": b"value"}])``

Probabilistic
^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 24 32 22 22

   * - Command
     - Change
     - RESP2 legacy example
     - Unified example
   * - ``TOPK.ADD``, ``TOPK.INCRBY``, ``TOPK.LIST``
     - Item names are preserved instead of coerced through numeric parsing.
     - ``"42"`` could become ``42``
     - ``"42"`` stays ``"42"``

Vector Sets
^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 24 32 22 22

   * - Command
     - Change
     - RESP2 legacy example
     - Unified example
   * - ``VEMB``
     - Vector components are always floats, as on RESP3.
     - ``[1, 0.5, 0]``
     - ``[1.0, 0.5, 0.0]``


RESP3 Legacy to Unified
-----------------------

The following tables summarize the main differences when moving from
``protocol=3`` with legacy responses to unified responses.

Core Commands
^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 24 32 22 22

   * - Command
     - Change
     - RESP3 legacy example
     - Unified example
   * - Sorted-set score commands
     - Scores pass through the same callback normalization as RESP2 unified.
     - ``[[b"a", 1.0]]``
     - ``[[b"a", 1.0]]``
   * - ``ZSCAN``
     - Score pairs are lists and ``score_cast_func`` receives floats.
     - ``(0, [[b"a", 1.0]])``
     - ``(0, [[b"a", 1.0]])``
   * - ``ACL CAT``, ``ACL HELP``, ``ACL LIST``, ``ACL USERS``
     - Byte strings are decoded to strings.
     - ``[b"default"]``
     - ``["default"]``
   * - ``ACL GENPASS``, ``ACL WHOAMI``, ``CLIENT GETNAME``, ``RESET``
     - Byte scalar becomes string scalar.
     - ``b"default"``
     - ``"default"``
   * - ``ACL LOG``
     - ``age-seconds`` is float, ``client-info`` is parsed, and structural strings are decoded.
     - ``{"age-seconds": "0.5"}``
     - ``{"age-seconds": 0.5, "client-info": {...}}``
   * - ``BGREWRITEAOF``, ``BGSAVE``
     - The status reply becomes ``True``.
     - ``b"Background saving started"``
     - ``True``
   * - ``CLIENT TRACKINGINFO``
     - Structural keys and string lists are decoded.
     - ``{b"flags": [b"on"]}``
     - ``{"flags": ["on"]}``
   * - ``COMMAND``
     - Flags and ACL categories are sets of strings.
     - ``{"get": {"flags": [b"readonly"]}}``
     - ``{"get": {"flags": {"readonly"}, "acl_categories": {...}}}``
   * - ``CLUSTER LINKS``, ``CLUSTER SHARDS``
     - Dict keys are strings at the exposed structural levels.
     - ``[{b"direction": b"to"}]``
     - ``[{"direction": b"to"}]``
   * - ``DEBUG OBJECT``
     - The status string is parsed into a dict.
     - ``b"Value at:0x... refcount:1 encoding:embstr ..."``
     - ``{"type": "Value", "refcount": 1, "encoding": "embstr", ...}``
   * - ``GEOHASH``
     - Hash strings are decoded.
     - ``[b"sqc8b49rny0"]``
     - ``["sqc8b49rny0"]``
   * - ``GEOPOS``
     - Keeps RESP3-style list coordinates.
     - ``[[1.0, 2.0]]``
     - ``[[1.0, 2.0]]``
   * - ``GEOSEARCH``, ``GEORADIUS``, ``GEORADIUSBYMEMBER`` with coordinates
     - Coordinates normalize to tuples.
     - ``[b"place", [1.0, 2.0]]``
     - ``[b"place", (1.0, 2.0)]``
   * - ``LCS`` and ``STRALGO ... IDX``
     - Dict keys are strings and match ranges use the unified list shape.
     - ``{b"matches": [...]}``
     - ``{"matches": [...], "len": 3}``
   * - ``SENTINEL`` state commands
     - Native RESP3 state maps normalize to the unified state dictionaries.
     - ``{b"flags": b"master"}``
     - ``{"flags": {"master"}, "is_master": True}``

Module Commands
^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 24 32 22 22

   * - Command
     - Change
     - RESP3 legacy example
     - Unified example
   * - ``BF.INFO``, ``CF.INFO``, ``CMS.INFO``, ``TOPK.INFO``, ``TDIGEST.INFO``
     - Raw module info maps become rich info objects.
     - ``{b"Capacity": 100}``
     - ``info.capacity == 100``
   * - ``TDIGEST.BYRANK``, ``TDIGEST.BYREVRANK``, ``TDIGEST.CDF``, ``TDIGEST.QUANTILE``
     - Raw lists are parsed with numeric and special-value handling.
     - ``[0.5, b"inf"]``
     - ``[0.5, "inf"]``
   * - ``TS.INFO``
     - Raw TimeSeries info map becomes ``TSInfo``.
     - ``{b"totalSamples": 10}``
     - ``info.total_samples == 10``
   * - ``TS.MRANGE``, ``TS.MREVRANGE``
     - Keeps the metadata slot in the unified three-element value.
     - ``{b"k": [labels, samples]}``
     - ``{b"k": [labels, metadata, samples]}``
   * - ``JSON.TYPE`` for missing keys
     - Wrapped missing value becomes bare ``None``.
     - ``[None]``
     - ``None``
   * - ``JSON.RESP``
     - Numeric float leaves are normalized consistently.
     - ``[b"{", b"price", b"-19.5"]``
     - ``[b"{", b"price", -19.5]``
   * - ``FT.SEARCH``
     - Raw RESP3 result map becomes ``Result``.
     - ``{"total_results": 2, "results": [...]}``
     - ``result.total == 2``
   * - ``FT.AGGREGATE``
     - Raw RESP3 aggregate map becomes ``AggregateResult``.
     - ``{"total_results": 2, "results": [...]}``
     - ``result.total == 2``
   * - ``FT.PROFILE``
     - Single ``ProfileInformation`` wrapper becomes parsed result plus profile info.
     - ``ProfileInformation(raw_profile_response)``
     - ``(result, ProfileInformation(profile_data))``
   * - ``FT.SPELLCHECK``
     - Native nested RESP3 spellcheck map becomes normalized term suggestions.
     - ``{"results": {"term": [{"fix": 0.0}]}}``
     - ``{"term": [{"score": "0", "suggestion": "fix"}]}``
   * - ``FT.INFO``, ``FT.CONFIG GET``, ``FT.SYNDUMP``
     - Structural keys are strings.
     - ``{b"index_name": b"idx"}``
     - ``{"index_name": "idx"}``
   * - ``FT.HYBRID``
     - Raw native response becomes ``HybridResult``; field values remain bytes by default.
     - ``{"total_results": 1, "results": [...]}``
     - ``HybridResult(total_results=1, results=[...])``


HYBRID Command Loaded Fields
----------------------------

``FT.HYBRID`` is experimental. Unified responses for the HYBRID command
preserve loaded field values as bytes by default to keep binary data, such as
vector fields, intact.

Use ``decode_field=True`` only for fields that are known text values.

.. code:: python

   post = HybridPostProcessingConfig()
   post.load("@title", decode_field=True)
   post.load("@embedding", decode_field=False)
