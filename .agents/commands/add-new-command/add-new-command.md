---
description: Adds support for a new Redis command from a given specification. Check command-specification-template.md.
argument-hint: [path-to-specification]
---

# Execute: Add new Redis command support

## Plan to Execute

Read specification file: `$ARGUMENTS`

## Execution Instructions

### 1. Preparations

- Follow instructions from `.agent/instructions.md`
- Go through the guide `specs/redis_commands_guide.md`

### 2. Read and Understand

- Read the ENTIRE specification carefully
- Go through Command Description and identify command type (string, list, set, etc.)
- Go through the Command API:
  - Identify required and optional arguments
  - Identify how to match Redis command arguments type to Python types
  - Identify return value and possible response types
- Check relevant Redis-Cli examples, if provided
- Review the Test Plan

### 3. Execute Tasks in Order

#### a. Navigate to the task
- Identify the files and action required
- Read existing related files if modifying

#### b. Implement the command
- Add new command method within matching trait object (for example: `redis/commands/core.py` for core Redis commands)
- Ensure overloading is implemented for sync and async API
- Follow Arguments definition section from `specs/redis_commands_guide.md` before defining command arguments
- Ensure arguments and response types consider bytes representation
- Ensure response RESP2 and RESP3 compatibility via `response_callbacks`

#### c. Record the command's metadata
- Obtain the server's metadata for the command: use the `COMMAND INFO` output from the
  specification's "Command metadata" section, or run `COMMAND INFO <name>` against a server
  that ships the command. For a container command (`BLESS`, `MEMORY`, ...) the subcommands
  are nested in the reply as `<container>|<sub>`; each one gets its own record.
- Add a record for the command to `_STATIC_COMMAND_METADATA` in `redis/commands/metadata.py`,
  keyed lowercase under its module (`"core"` for non-module commands, the lowercased prefix
  such as `"json"` or `"ft"` for module commands). Key a container subcommand by its
  space-joined name (`"bless scan"`), the form `execute_command` receives.
- Map the reply onto the record fields and reuse an existing shape (`_CACHEABLE_KEYED`,
  `_WRITE_KEYED`, `_NONDETERMINISTIC_KEYED`, ...) when one matches; otherwise spell out every
  field of a `CommandMetadata` inline, as the table does for `touch` and `vrandmember`:
  - `is_readonly` is the `readonly` command flag - not the `RO` key spec flag. A command the
    server does not flag `readonly` is neither client-side cacheable nor replica-eligible.
  - `is_blocking` is the `blocking` flag; `is_script_runner` the `script_runner` flag.
  - `has_key_argument` is true when `first_key_pos > 0 and step_count > 0`, or when a
    `movablekeys` command reports a key spec not flagged `not_key`.
  - `has_nondeterministic_output` and `is_dont_cache` are the `nondeterministic_output` and
    `dont_cache` tips, matched exactly (`nondeterministic_output_order` is a different tip).
  - `has_complete_metadata` is true: the record was taken from a reply that carries flags and
    tips.
  - `request_policy` / `response_policy` are the keyed or keyless defaults unless a
    `request_policy:` / `response_policy:` tip overrides them. Withhold both (set them to
    `None`) when the cluster client must keep resolving the target itself: the command is in
    `RedisCluster.COMMAND_FLAGS` (as `SCAN` and `BLESS SCAN` are), is `movablekeys`, or tips a
    policy the client does not implement (`multi_shard`). A keyed command must never be
    recorded `DEFAULT_KEYLESS`.
- Document in a comment next to the record anything that diverges from the live reply and
  why, and extend the provenance note above the table if the command needs a newer server
  than the table was validated against.
- Update the guards in `tests/test_command_metadata.py` (shared with its async mirror):
  add a withheld record to `ALL_WITHHELD_ROUTING_COMMANDS`, a deliberate cacheability
  divergence to `LIVE_CACHEABILITY_DIVERGENCE`, and raise `STATIC_TABLE_SERVER_VERSION` if
  the command first ships in a newer release. Then run `tests/test_command_metadata.py`,
  `tests/test_asyncio/test_command_metadata.py` and the static-table routing tests in
  `tests/test_cluster.py` / `tests/test_asyncio/test_cluster.py`.

#### d. Verify as you go
- After each file change, check syntax
- Ensure imports are correct
- Verify types are properly defined
- Verify that response schema is similar for RESP2 and RESP3

### 4. Implement Testing Plan

After completing implementation tasks:

- Identify matching test file or create new one if needed
- Implement all test cases as separate test methods
- Ensure adding version constraint if specified in the specification
- Ensure tests cover edge cases

### 5. Run tests

- Run newly added test cases using `pytest` with RESP2 and RESP3 protocol specified via `--protocol` option
- Ensure that the same test cases passed with both protocols
- Get back to the Implementation stage if any test failed

### 6. Final Verification

Before completing:

- ✅ All tasks from plan completed
- ✅ All tests created and passing
- ✅ Code follows project conventions
- ✅ Documentation added/updated as needed

## Output Report

Provide summary:

### Completed Tasks
- List of all tasks completed
- Files created (with paths)
- Files modified (with paths)

### Tests Added
- Test files created
- Test cases implemented
- Test results


