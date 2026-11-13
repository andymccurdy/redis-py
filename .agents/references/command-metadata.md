# Command metadata, routing, replica safety and cache eligibility

Read this before you change cluster routing, replica reads, client-side-cache eligibility, or add a command that needs routing metadata.

## Where the metadata lives

`redis/commands/metadata.py` holds the command metadata record types: `RequestPolicy` / `ResponsePolicy`, `CommandPolicies` / `PolicyRecords`, and `CommandMetadata` / `CommandMetadataRecordsCache`, plus the metadata resolvers. It is the only place to change routing, replica eligibility or cache eligibility.

- `CommandMetadata` is the superset record. `_to_command_policies` projects one down to the routing view a `PolicyResolver` serves. It projects to `None` when the record withholds its routing policies. That is how a resolver tells the cluster client to resolve the target itself. `SCAN`, `DBSIZE`, `KEYS` and most other `COMMAND_FLAGS` commands need this. `RANDOMKEY` is the exception: its entry keeps the server's `ALL_SHARDS` / `SPECIAL` routing tips.
- The `movablekeys` reads are recorded `DEFAULT_KEYED`, which `_build_command_records` also derives from their key specs. The cluster client finds their keys from the key specs that `CommandsParser.initialize()` compiles, and asks `COMMAND GETKEYS` only where those specs are not enough.
- The two record types are kept separate on purpose. `CommandPolicies` shipped in 7.1.0 as a mutable class. `CommandMetadata` is frozen so one record can be shared across many commands.
- `redis/_parsers/commands.py` re-exports the policy types for backwards compatibility. Import them from `redis.commands.metadata`.

## The resolver

A resolver is a client-level constructor argument. `metadata_resolver=` exists on `Redis`, `ConnectionPool`, `RedisCluster`, `NodesManager` and `ClusterPipeline`. On the async side it exists only on `redis.asyncio.RedisCluster`, because the async stack has no client-side cache to be the second consumer. It is built once and shared with every node client.

It serves three questions:

- cluster routing, through `resolve_policies`;
- replica safety, through `is_replica_safe`, which reads the `is_readonly` flag on the record;
- client-side-cache eligibility, through `is_cacheable`.

When only `metadata_resolver` is given, the routing resolver is derived from it. An explicit `policy_resolver` still wins for the routing view, which decides the nodes a command targets. It does not win for anything that view cannot express. Replica safety and cache eligibility keep resolving through `metadata_resolver` either way, because a `CommandPolicies` record carries no `is_readonly` flag to answer them with.

## Deprecated tables that are not read

Editing these changes nothing. They are kept only so external importers keep working.

- `READ_COMMANDS` in `redis/commands/cluster.py`. Replica eligibility comes from `is_replica_safe`.
- `STATIC_POLICIES` in `redis/commands/policies.py`. A frozen verbatim copy of the 7.1.0 table, deliberately not derived from or synced with `_STATIC_COMMAND_METADATA`.
- `DEFAULT_ALLOW_LIST` in `redis/cache.py`. Cache eligibility comes from `is_cacheable`.

## Client-side cache

`CacheConfig.is_allowed_to_cache` in `redis/cache.py` is the single eligibility decision point. It delegates to the `MetadataResolver` the config holds. That resolver defaults to the static table, and `ConnectionPool.__init__` injects it from the client-level `metadata_resolver=`.

`CacheProxyConnection.send_command` asks that decision point and then requires the invocation to carry `keys`. A cacheable command without `keys` bypasses the cache instead of raising. So plumbing `keys=` into a command method is a purely additive way to start caching it.

## Cluster routing

`redis/cluster.py` and `redis/asyncio/cluster.py` route with the `RequestPolicy` / `ResponsePolicy` enums from `redis/commands/metadata.py`, resolved through `redis/commands/policies.py` (`PolicyResolver` / `StaticPolicyResolver`).
