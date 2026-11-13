# Redis commands guide

## Commands API specification

The Redis API is specified in the [Redis documentation](https://redis.io/commands). This is the source of truth
for all command-related information. However, Redis is a living project and new commands are added all the time.

A new command may not yet be available in the documentation. In this case, the developer needs to create a new
command specification from the `.claude/command-specification-template.md` template.

## Files structure

```
redis/
├── commands/            # Base directory
│   ├── module/          # Module commands directory
│   │   ├── commands.py  # Module commands public API
│   │   └── file.py      # Custom helpers
│   │...                 # Other modules   
│   ├── __init__.py      # Module exports
│   ├── cluster.py       # Cluster commands public API 
│   ├── core.py          # Core commands public API
│   ├── helpers.py       # Helpers for commands modules
│   ├── redismodules.py  # Trait for all Redis modules
│   └── sentinel.py      # Sentinel commands public API
```

## Commands Public API

Commands public API exposed through the different types of clients (Standalone, Cluster, Sentinel), inheriting trait
objects defined in the directories according to the Files structure section above. Each command implementation at the
end calls generic `execute_command(*args, **kwargs)` defined by `CommandProtocol`.

SDK expose sync and async commands API through methods overloading, see `.agents/sync_async_type_hints_overload_guide.md`
for more information.

### Binary representation

Our SDK can be configured with `decode_responses=False` option which means that command response will be returned
as bytes, as well as we allow to pass bytes arguments instead of strings. For this purpose, we define a custom types
like `KeyT`, `EncodableT`, etc. in `redis/typing.py`.

## Protocols compatibility

SDK supports two types of Redis wire protocol: RESP2 and RESP3. Starting with redis-py 8.0, clients use RESP3 on
the wire by default, while `legacy_responses=True` preserves legacy RESP2-compatible Python response shapes for the
default configuration.

Wire protocol and Python response shape are selected independently:

- `Redis()` uses RESP3 on the wire and legacy RESP2-compatible Python response shapes.
- `Redis(protocol=2)` uses RESP2 on the wire and legacy RESP2 Python response shapes.
- `Redis(protocol=3)` uses RESP3 on the wire and legacy RESP3 Python response shapes.
- `Redis(legacy_responses=False)` enables unified response shapes. Unified shapes must be protocol-independent and
  should match for RESP2 and RESP3 unless the command has a documented server-side difference.

To understand how does Redis types defined by RESP2 and RESP3 protocol maps to Python types,
see `redis/_parsers/resp2.py` and `redis/_parsers/resp3.py`.

Parsers are responsible for RESP protocol parsing. However, protocol and response-shape compatibility is achieved by
2nd layer parsing through response callbacks defined in `redis/_parsers/response_callbacks.py` for core commands and
module-specific callback maps under `redis/commands/$module/`.

When adding or changing a command, identify the expected response shape for every supported mode:

- RESP2 legacy (`protocol=2`, `legacy_responses=True`)
- RESP3 legacy (`protocol=3`, `legacy_responses=True`)
- default legacy (`protocol` unset, `legacy_responses=True`; RESP3 wire with RESP2-compatible Python shape)
- RESP2 unified (`protocol=2`, `legacy_responses=False`)
- RESP3 unified (`protocol=3`, `legacy_responses=False`)

Legacy response callbacks should preserve existing behavior unless the change intentionally fixes a bug. Unified
response callbacks should provide the recommended protocol-independent public shape. Public command return type hints
must include any new structures introduced by unified responses.

## Arguments definition

Some of the Redis commands may have a complex API structure, so we need to make it user-friendly and apply
some transformation within specific command method. For example, some commands may need to have
a COUNT argument for aggregated types followed by number of arguments and arguments itself. In this case
we hide this complexity from user and instead expose argument as list in public API and transform it
to Redis-friendly format.

```python
def scan(
    self,
    cursor: int = 0,
    count: int | None = None,
    _type: str | None = None,
    **kwargs,
):
    pieces: list = [cursor]
    
    if count is not None:
        pieces.extend([b"COUNT", count])
```

In terms of required and optional arguments we follow the specification and trying to reflect it as close
as possible.

For argument type hints, prefer the most general abstract interface that expresses what the command actually needs
over a concrete implementation type. When an argument is only iterated over, type it as `Iterable[...]` rather than
`list[...]` so the API accepts any compatible input (lists, tuples, sets, generators, etc.) instead of just one
concrete type. Only pin a concrete type (e.g. `list[...]`) when the implementation explicitly requires that type's
capabilities, such as indexing, slicing, ordering guarantees, or in-place mutation. This keeps the public API
permissive on input while the command internally normalizes the value to whatever Redis-friendly format it needs.

```python
# Preferred: accepts any iterable of strings.
def querylabels(self, label: str | None = None, filters: Iterable[str] | None = None): ...

# Avoid unless a list is explicitly required (indexing, ordering, mutation):
def querylabels(self, label: str | None = None, filters: list[str] | None = None): ...
```

Note that this applies to input arguments. Return type hints should reflect the concrete type the command actually
produces (see "Protocols compatibility" above).

### Testing

Command tests are located in `tests/test_*command_type*.py` and `tests/test_asyncio/test_*command_type*.py` for async
commands. So it's important to identify command type upfront to resolve correct test file.

We usually provide only integration testing for commands with defined version constraint (if required). It's controlled
by custom annotations `@skip_if_server_version_lt()` and `@skip_if_server_version_gte()` defined in `tests/conftest.py`.

