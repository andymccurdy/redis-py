# Instructions for AI agents contributing to redis-py

redis-py is the Python client for Redis, with a sync stack and an asyncio stack that mirror each other. This file holds only what an agent needs on every task. For what the library does and how to use it, see [README.md](README.md). For the human contribution process, see [CONTRIBUTING.md](CONTRIBUTING.md).

- `redis/` - the sync client; `redis/asyncio/` - the async mirror.
- `redis/commands/` - command mixins and command metadata; `redis/_parsers/` - RESP parsing and response callbacks.
- `tests/` - sync tests; `tests/test_asyncio/` - async tests; `docs/` - Sphinx docs; `dockers/` - test server config.
- `.agents/` - skills, references and guides for agents; `specs/` - design notes.

## Commands
Development tasks are driven by `invoke` (see `tasks.py`). Activate `.venv` first.
- `pip install -r dev_requirements.txt && pip install -e '.[jwt]'` - install the dev environment.
- `invoke devenv` - start the docker-compose test servers (standalone, replica, sentinel, cluster, redis-stack, stunnel). `--profile all` is the default; profiles are in `docker-compose.yml`.
- `invoke clean` - stop all dockers and remove `build/` and `dist/`.
- `invoke linters` / `invoke linters-fix` - `ruff check`, `ruff format` and `vulture redis whitelist.py --min-confidence 80`.
- `invoke tests` - run the `fixed_client`, `standalone` and `cluster` suites in sequence. Accepts `--uvloop`, `--protocol=2|3|""`, `--legacy-responses=true|false`, `--profile`.
- `invoke standalone-tests` / `invoke cluster-tests` - one suite. The cluster suite targets `redis://localhost:16379/0` (`rediss://localhost:27379/0` for TLS).
- `invoke fixed-client-tests` - only tests marked `fixed_client`.
- `invoke run-test-matrix` - the full `protocol` x `legacy_responses` matrix that CI runs.
- `invoke all-tests` - linters and tests. `invoke build-docs` - Sphinx HTML under `docs/`.

Run a single test:
```
pytest tests/test_commands.py::TestRedisCommands::test_set -x
pytest tests/test_asyncio/test_commands.py -k "test_set and not cluster"
pytest --redis-url=redis://localhost:16379/0 -m onlycluster tests/test_cluster.py
```

Verify a change: `invoke linters`, then the focused tests for the touched behavior in both `tests/` and `tests/test_asyncio/`. Then follow the Self-Check Loop, Diff Validation, Code Review and Final Checklist below.

`tests/conftest.py` adds `--redis-url`, `--redis-ssl-url`, `--redis-mod-url`, `--protocol` (`""` = library default, currently RESP3), `--legacy-responses` (`true|false|default`), `--redis-cluster-nodes` and `--uvloop`. It also adds `--tracemalloc` (off by default, not exposed via invoke), which shows where a `ResourceWarning` object was allocated and makes cluster client setup several times slower.

Pytest markers (defined in `pyproject.toml`):

- `onlycluster` / `onlynoncluster` - limit a test to one topology.
- `redismod` - requires Redis modules (Stack image); skipped against cluster.
- `fixed_client` - the test fixes its own client config; excluded from the protocol/legacy matrix runs.
- `experimental`, `replica`, `ssl`, `pipeline`, `cp_integration`, `no_mock_connections`.

`tests/test_scenario/` and `tests/test_asyncio/test_scenario/` need external infrastructure (Redis Enterprise, EntraID). The default suites exclude them with `--ignore`.

## Architecture

### Sync and async mirrors
- Sync: `redis/client.py`, `redis/cluster.py`, `redis/connection.py`, `redis/sentinel.py`, `redis/lock.py`, `redis/retry.py`.
- Async mirrors under `redis/asyncio/`: `client.py`, `cluster.py`, `connection.py`, `sentinel.py`, and so on.

A change in one stack almost always belongs in the other. `specs/sync_async_deduplication_analysis.md` documents the deliberate duplication. The process rules under "Sync / Async Consistency" below are mandatory whenever you touch either stack.

### Commands layer
`Redis` and `redis.asyncio.Redis` are assembled from mixins in `redis/commands/` plus the connection and pool plumbing.
- `core.py` - `CoreCommands` / `AsyncCoreCommands`, every standard Redis command.
- `cluster.py` - `RedisClusterCommands` / `AsyncRedisClusterCommands`.
- `sentinel.py` - sentinel commands.
- `redismodules.py` - aggregates the module mixins (`bf/`, `json/`, `search/`, `timeseries/`, `vectorset/`). Each module package owns its command surface and response parsing.
- `metadata.py` - command metadata: routing policies (`RequestPolicy` / `ResponsePolicy`), replica safety and cache eligibility, served by a `metadata_resolver=` shared by every node client. `policies.py` holds the `PolicyResolver` routing view.
- `helpers.py` - shared utilities (`list_or_args`, pubsub subscription partitioning).

Change routing, replica eligibility and cache eligibility in `metadata.py` only. `READ_COMMANDS` (`commands/cluster.py`), `STATIC_POLICIES` (`commands/policies.py`) and `DEFAULT_ALLOW_LIST` (`cache.py`) are deprecated and unread, kept only for external importers. Before touching any of this, read [`.agents/references/command-metadata.md`](.agents/references/command-metadata.md).

### Wire protocol and response parsing
`redis/_parsers/` holds `resp2.py` and `resp3.py` (pure Python), `hiredis.py` (used automatically if `hiredis>=3.2.0` is installed), `response_callbacks.py` (reshapes raw replies into Python types), `commands.py` (`CommandsParser`, which learns command metadata via `COMMAND INFO`) and `encoders.py`.

RESP3 is the default on the wire. Two knobs interact:
- `protocol=2|3` chooses the wire protocol.
- `legacy_responses=True|False` chooses the Python response shape. `True` (the default) keeps RESP2-era shapes when `protocol` is not set, while an explicit `protocol=3` returns native RESP3 shapes; `False` returns the unified shapes, which **must be identical on RESP2 and RESP3**. See `specs/unified_responses_migration_guide.md`.

### Other subsystems
- `redis/cluster.py` (and async) - topology discovery, slot mapping (`redis/crc.py`, `key_slot`), per-node pools, MOVED/ASK redirection.
- `redis/multidb/` (and `redis/asyncio/multidb/`) - Active-Active client with health checks, failure detection and failover.
- `redis/cache.py` - client-side caching. `CacheConfig.is_allowed_to_cache` is the single eligibility decision point.
- `redis/maint_notifications.py` - server-pushed maintenance notifications. See [`.agents/references/maint-notifications.md`](.agents/references/maint-notifications.md).
- `redis/auth/` - credential providers and token auth (EntraID via `redis-entraid`, JWT).
- `redis/observability/` - OpenTelemetry instrumentation, gated by the `otel` extra. The async stack has only `recorder.py` and delegates the rest to the sync package.
- `redis/keyspace_notifications.py`, `redis/event.py` (`EventDispatcher`), `redis/http/`, `redis/retry.py` and `redis/backoff.py` (`ExponentialWithJitterBackoff` is the default).
- `dockers/` - test server config. The compose file pulls `redislabs/client-libs-test:<tag>` (`CLIENT_LIBS_TEST_IMAGE_TAG` / `CLIENT_LIBS_TEST_STACK_IMAGE_TAG`). CI pins it via `CURRENT_REDIS_VERSION` in `.github/workflows/integration.yaml`.

## Repo conventions
- Python `>=3.11`. Optional extras: `hiredis`, `xxhash`, `ocsp`, `jwt`, `circuit_breaker`, `otel`.
- Ruff is the only formatter and linter (`target-version = "py311"`, `line-length = 88`). `tests/*` has relaxed naming rules, and the module command packages (`bf`, `timeseries`, `json`, `search`) opt out of pep8 naming. `whitelist.py` is the vulture allowlist; extend it instead of silencing vulture inline.
- Docstrings follow the Google style and change with the behavior. Do not repeat type hints in them.
- A new command is not done until every step of the `add-new-command` checklist is done.

---

## MUST
1. Preserve public API compatibility (signatures, argument names, defaults, return types); 2. Keep sync and async implementations fully aligned (behavior, signatures, type hints) when possible; 3. Run the full test suite and fix all failures; 4. Run linting and fix all issues; 5. Follow existing code patterns and conventions exactly; 6. Maintain Redis command semantics and behavior precisely; 7. Optimize for performance and memory efficiency

## MUST NOT
1. Introduce breaking API changes without explicit need; 2. Modify unrelated code; 3. Change Redis behavior, edge cases, or error semantics; 4. Introduce new dependencies without strong justification; 5. Add unnecessary abstractions or new patterns; 6. Weaken or remove existing tests to make them pass

## General Principles
1. Make minimal, surgical changes; 2. Prefer consistency over innovation; 3. If unsure, follow existing implementations in the codebase; 4. Prioritize correctness first, then performance

## Type Hints & Overloads
1. Use PEP 604 (`X | Y`) syntax; 2. Follow `.agents/sync_async_type_hints_overload_guide.md`; 3. Keep type hints identical between sync and async APIs; 4. Do not change public type signatures unnecessarily

## Imports
1. Import all types at the top of the file; 2. Avoid function-level imports unless absolutely necessary; 3. Keep imports minimal and consistent

## Sync / Async Consistency
1. Any change in sync must be mirrored in async (and vice versa); 2. Ensure identical signatures, return types, and behavior; 3. Ensure overloads remain aligned

## Performance & Memory
1. Prioritize correctness first, then optimize for performance and memory efficiency; 2. Never trade correctness for performance; 3. Avoid unnecessary allocations in hot paths; 4. Avoid creating temporary objects when not needed; 5. Reuse existing helpers/utilities instead of duplicating logic; 6. Be careful with large data structures

## Redis Semantics
1. Match Redis server behavior exactly; 2. Preserve error types and messages; 3. Do not introduce silent behavior changes; 4. Handle edge cases explicitly and correctly

## Testing
1. Add or update tests for every behavior change; 2. Cover edge cases and failure scenarios; 3. Keep tests deterministic and stable; 4. Do not relax assertions to pass tests

## Linting & Code Quality
1. Run all linters and fix issues; 2. Ensure code is clean, readable, and consistent; 3. Perform a full code review after changes; 4. Refactor if needed to meet project standards

## Documentation
1. Update docstrings when behavior changes; 2. Keep docstrings consistent with type hints; 3. Avoid redundant or outdated documentation

## Dependencies
1. Do not introduce new dependencies unless absolutely necessary; 2. Prefer existing utilities within the project

---

# 🔁 Self-Check Loop (MANDATORY)
Before finalizing changes, perform this loop:
1. **Correctness Check**: Does the change preserve Redis semantics exactly? Are edge cases handled correctly?
2. **API Check**: Did any public function signature change? If yes, revert or justify explicitly
3. **Sync/Async Check**: Are both implementations updated and identical in behavior? Are type hints and overloads aligned?
4. **Performance Check**: Did this introduce extra allocations or unnecessary work? Can this be simplified or reused?
5. **Consistency Check**: Does this match existing patterns in the codebase? Are naming and structure consistent?
6. **Test Check**: Are tests added/updated for the change? Do all tests pass without weakening assertions?

Repeat this loop until all answers are satisfactory.

---

# 🔍 Diff Validation (MANDATORY)
Before submitting changes, review the full diff:
1. Ensure only relevant files are modified; 2. Ensure no unrelated lines were changed; 3. Ensure no accidental deletions of logic; 4. Ensure no debug code or comments remain; 5. Ensure imports were not unnecessarily altered; 6. Ensure public APIs were not unintentionally modified

If any issue is found:
1. Fix it; 2. Re-run validation

---

## 🔎 Code Review (MANDATORY)
After implementing changes, perform a thorough code review of the final code.

### Verify:
1. **Correctness**: Logic is correct and complete; Edge cases are handled properly; Redis semantics are preserved exactly
2. **API Stability**: No unintended changes to public APIs; Function signatures, defaults, and return types are unchanged
3. **Sync/Async Parity**: Sync and async implementations are fully aligned; Type hints and overloads match
4. **Performance**: No unnecessary allocations or inefficiencies introduced; No redundant work or duplicate logic
5. **Consistency**: Code follows existing patterns and conventions; Naming and structure match the rest of the codebase
6. **Simplicity**: No unnecessary abstractions added; Implementation is as simple as possible
7. **Tests**: Tests cover the change adequately; No weakened assertions

### If any issue is found:
1. Fix it; 2. Re-run tests and linting; 3. Repeat the review

---

# ✅ Final Checklist (must all be true)
1. Tests pass; 2. Lint passes; 3. Sync/async parity verified; 4. No API breakage; 5. No unnecessary allocations introduced; 6. Behavior matches Redis exactly; 7. Code follows existing patterns; 8. Diff is minimal and clean

## Boundaries
Always:
1. Mirror a change into the other stack, and test both; 2. Run `invoke linters` and the focused tests before calling a change done, and fix every failure. The full suite (`invoke tests`) must pass before a PR is ready.

Ask first:
1. Adding or upgrading a dependency; 2. Editing CI or release workflows under `.github/`; 3. Any change to public API, defaults or error semantics. Breaking changes ship only in major releases.

Never:
1. Weaken, skip or delete a test to make a change pass. If a test is wrong or obsolete, say so in the PR description and let a human decide; 2. Add `Signed-off-by`, `Co-Authored-By` or "Generated with" lines to commits or PR text. The human is the author of record; 3. Push, create or merge PRs, or post comments, reviews or issues, unless a maintainer started the agent for that purpose. Draft the text and let a human post it; 4. Modify unrelated code.

## PR conventions
The full process is in [CONTRIBUTING.md](CONTRIBUTING.md#pull-request-process). The rules to follow every time:
- PRs are squash-merged, so the PR title becomes the commit message. Write it as one imperative sentence about the user-visible change. A conventional prefix such as `fix:`, `feat:` or `perf(cluster):` is common but optional.
- Fill in the PR template checklist, and include tests and docs with the change.
- Link the issue the PR fixes, for example `Fixes #1234`.

## Skills
Task procedures live in `.agents/skills/`; `.claude/skills` is a symlink to it. Use the skill when the task matches.

| Skill | Use when |
|---|---|
| `add-new-command` | Adding a new Redis command from a specification (template: `.agents/skills/add-new-command/assets/command-specification-template.md`) |
| `sync-claude-md` | Auditing AGENTS.md against the current repo after structural changes |
| `pr-draft-summary` | Writing a commit message and PR description for local changes |
| `generate-release-notes` | Drafting release notes for a version or branch |
| `maintainer-review` | Assessing an issue or PR as a maintainer |
| `prepare-for-takeover` | Marking stalled PRs `open-for-takeover` |
