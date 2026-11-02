# Migration Guide: Unified Responses

Unified responses are an opt-in Python response shape in redis-py. They are
enabled with `legacy_responses=False` and are designed so affected commands
return the same Python structure whether the connection uses RESP2 or RESP3.

This guide is a review/spec companion for `docs/unified_responses.rst`.

## Activation

```python
import redis

# Default wire protocol, unified responses.
r = redis.Redis(legacy_responses=False)

# RESP2 wire, unified responses.
r2 = redis.Redis(protocol=2, legacy_responses=False)

# RESP3 wire, unified responses.
r3 = redis.Redis(protocol=3, legacy_responses=False)
```

```python
import redis.asyncio as redis_async
from redis.cluster import RedisCluster

ar = redis_async.Redis(legacy_responses=False)
cluster = RedisCluster(host="localhost", port=6379, legacy_responses=False)
url_client = redis.from_url("redis://localhost:6379?legacy_responses=false")
```

## Response Mode Matrix

| Client options | Wire protocol | Python response shape |
| --- | --- | --- |
| `Redis()` | Default RESP3 wire | Legacy RESP2-compatible shape |
| `Redis(protocol=2)` | RESP2 | Legacy RESP2 shape |
| `Redis(protocol=3)` | RESP3 | Native RESP3 shape |
| `Redis(legacy_responses=False)` | Default RESP3 wire | Unified shape |
| `Redis(protocol=2, legacy_responses=False)` | RESP2 | Unified shape |
| `Redis(protocol=3, legacy_responses=False)` | RESP3 | Unified shape |

`decode_responses` is independent. It still controls bulk-string decoding where
the parser does not otherwise normalize structural keys or values.

## Migration Checklist

1. Find the redis-py client construction points in your application, including
   background workers, admin scripts, asyncio clients, and cluster clients.
2. Enable `legacy_responses=False` for one environment or service path first.
   The server protocol can stay unchanged unless you also want to pin
   `protocol=2` or `protocol=3`.
3. Check the commands your application reads from the tables below. Update code
   that indexes into tuples, expects flat lists, compares exact response
   containers, or serializes command responses directly.
4. Review module command responses such as JSON, TimeSeries, RediSearch, and
   probabilistic commands separately; several of them return richer objects or
   nested containers in unified mode.
5. Keep `decode_responses` decisions separate. Unified responses normalize
   response structures, while `decode_responses` still controls how bulk string
   data is decoded.
6. Roll the change out gradually and monitor application paths that parse Redis
   responses manually.

## RESP2 Legacy to Unified

| Area | Commands | RESP2 legacy shape | Unified shape |
| --- | --- | --- | --- |
| Sorted sets | `ZDIFF`, `ZINTER`, `ZRANGE`, `ZRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYSCORE`, `ZUNION` with scores | Flat or tuple score pairs, scores often bytes | List score pairs, scores as floats |
| Sorted sets | `ZPOPMAX`, `ZPOPMIN` | `[(member, score)]` | `[[member, float_score]]` |
| Sorted sets | `BZPOPMAX`, `BZPOPMIN` | `(key, member, score)` | `[key, member, float_score]` |
| Sorted sets | `ZRANK`, `ZREVRANK` with score | `[rank, score]` where score can be bytes/int | `[rank, float_score]` |
| Sorted sets | `ZSCAN` | `(cursor, [(member, score)])`; score cast receives raw bytes | `(cursor, [[member, score]])`; score cast receives float |
| Random fields | `ZRANDMEMBER` with scores, `HRANDFIELD` with values | Flat interleaved list | Nested `[[item, value], ...]` pairs |
| Blocking list pops | `BLPOP`, `BRPOP` | `(key, value)` | `[key, value]` |
| Sorted set pops | `ZMPOP`, `BZMPOP` | Nested tuple pairs with raw scores | Nested list pairs with float scores |
| Streams | `XREAD`, `XREADGROUP` | `[[stream, entries], ...]` | `{stream: entries}` |
| String algorithms | `LCS` with `IDX` | Flat key/value list | Dict with string keys |
| String algorithms | `STRALGO ... IDX` | Tuple ranges | List ranges |
| Client metadata | `CLIENT TRACKINGINFO` | Flat list | Dict with string keys and decoded string lists |
| Command metadata | `COMMAND` | `flags` as list | `flags` and `acl_categories` as sets of strings |
| ACL | `ACL GETUSER` | Selector flat lists | Selector dicts |
| ACL | `ACL LOG` | String/bytes scalar values | `age-seconds` as float, `client-info` as dict |
| Sentinel | `SENTINEL MASTER`, `MASTERS`, `SLAVES`, `SENTINELS` | `flags` comma-string | `flags` set plus derived booleans |
| Cluster | `CLUSTER LINKS`, `CLUSTER SHARDS` | Raw byte structural keys | String structural keys |
| Geo | `GEOPOS` | Coordinate tuples | Coordinate lists |
| Geo | `GEOSEARCH`, `GEORADIUS`, `GEORADIUSBYMEMBER` with coordinates | Tuple coordinates | Tuple coordinates, matching the approved unified shape |
| Functions | `FUNCTION LIST` | Flat sublists | Nested dictionaries |
| Functions | `FUNCTION STATS` | Flat lists | Nested dictionaries |
| Strings | `INCREX` with `BYFLOAT` | Value and increment as bytes | Value and increment as floats |
| Memory | `MEMORY STATS` | Raw string-like values | Structural keys decoded; numeric values native |
| JSON | `JSON.NUMINCRBY`, `JSON.NUMMULTBY` | Legacy scalar path | JSONPath-compatible array behavior |
| JSON | `JSON.RESP` | Float values can be string leaves | Float leaves become Python floats |
| JSON | `JSON.OBJKEYS` | Keys forced to strings | Keys respect `decode_responses` |
| TimeSeries | `TS.GET`, `TS.RANGE`, `TS.REVRANGE` | Tuples | Lists |
| TimeSeries | `TS.MGET` | Sorted list of dicts | Dict keyed by series |
| TimeSeries | `TS.MRANGE`, `TS.MREVRANGE` | Dict/list without metadata slot | Dict values include `[labels, metadata, samples]` |
| TimeSeries | `TS.QUERYINDEX` | Numeric-looking keys can be coerced | Keys are preserved |
| RediSearch | `FT.INFO` | Attribute flat sublists | Attribute dicts with `flags` |
| RediSearch | `FT.CONFIG GET` | Bytes keys/values | String keys/values |
| RediSearch | `FT.SEARCH` | `Result` without unified warnings surface | `Result` with `warnings` |
| RediSearch | `FT.AGGREGATE` | `AggregateResult` without unified total/warnings surface | `AggregateResult` with `total` and `warnings` |
| RediSearch | `FT.PROFILE` | Parsed tuple with legacy profile data | Parsed tuple with unified profile data |
| RediSearch | `FT.HYBRID` | `HybridResult`; bytes by default | `HybridResult`; bytes preserved by default |
| Probabilistic | `TOPK.ADD`, `TOPK.INCRBY`, `TOPK.LIST` | Numeric-looking item names can be coerced | Item names are preserved |

## RESP3 Legacy to Unified

| Area | Commands | RESP3 legacy shape | Unified shape |
| --- | --- | --- | --- |
| Sorted sets | Sorted-set score commands | Native RESP3 arrays and scores | Same public score-pair normalization as RESP2 unified |
| Sorted sets | `ZSCAN` | Native score pairs | List score pairs with unified score casting |
| ACL | `ACL CAT`, `ACL HELP`, `ACL LIST`, `ACL USERS` | Lists of bytes | Lists of strings |
| ACL | `ACL GENPASS`, `ACL WHOAMI` | Bytes scalar | String scalar |
| ACL | `ACL LOG` | RESP3 map with string-like scalars | Normalized map with float `age-seconds` and parsed `client-info` |
| Client metadata | `CLIENT GETNAME` | Bytes scalar | String scalar |
| Client metadata | `CLIENT TRACKINGINFO` | Native RESP3 map | Dict with string keys and decoded string lists |
| Command metadata | `COMMAND` | Native RESP3 command metadata | Unified command metadata with set fields |
| Cluster | `CLUSTER LINKS`, `CLUSTER SHARDS` | Native maps with raw structural keys | String structural keys |
| Geo | `GEOHASH` | List of bytes | List of strings |
| Geo | `GEOPOS` | List coordinates | List coordinates |
| Geo | `GEOSEARCH`, `GEORADIUS`, `GEORADIUSBYMEMBER` with coordinates | List coordinates | Tuple coordinates |
| String algorithms | `LCS`, `STRALGO ... IDX` | RESP3 maps | Dicts with string keys and unified match range lists |
| Sentinel | `SENTINEL MASTER`, `MASTERS`, `SLAVES`, `SENTINELS` | Native RESP3 state maps | Unified state dicts |
| Probabilistic | `BF.INFO`, `CF.INFO`, `CMS.INFO`, `TOPK.INFO`, `TDIGEST.INFO` | Raw maps | Rich info objects |
| Probabilistic | `TDIGEST.BYRANK`, `TDIGEST.BYREVRANK`, `TDIGEST.CDF`, `TDIGEST.QUANTILE` | Raw lists | Parsed lists with numeric and special-value handling |
| TimeSeries | `TS.INFO` | Raw map | `TSInfo` object |
| TimeSeries | `TS.MRANGE`, `TS.MREVRANGE` | Native response without unified metadata slot | Dict values include `[labels, metadata, samples]` |
| JSON | `JSON.TYPE` for missing keys | `[None]` | `None` |
| JSON | `JSON.RESP` | Numeric float leaves can be string/bytes | Float leaves become Python floats |
| RediSearch | `FT.SEARCH` | Raw RESP3 result map | `Result` object |
| RediSearch | `FT.AGGREGATE` | Raw RESP3 aggregate map | `AggregateResult` object |
| RediSearch | `FT.PROFILE` | `ProfileInformation(raw_response)` | `(Result | AggregateResult, ProfileInformation)` |
| RediSearch | `FT.SPELLCHECK` | Native nested spellcheck map | Normalized term suggestion dict |
| RediSearch | `FT.INFO`, `FT.CONFIG GET`, `FT.SYNDUMP` | Raw structural keys | String structural keys |
| RediSearch | `FT.HYBRID` | Raw native response | `HybridResult`; field values remain bytes by default |

## HYBRID Command Field Decoding

`FT.HYBRID` is experimental. Unified responses for the HYBRID command preserve
result field values and warnings as bytes by default. This keeps binary loaded
fields, such as vector embeddings, intact.

Use `decode_field=True` only for fields that are known text values:

```python
post = HybridPostProcessingConfig()
post.load("@title", decode_field=True)
post.load("@embedding", decode_field=False)
```
