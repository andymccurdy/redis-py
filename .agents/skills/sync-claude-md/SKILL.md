---
name: sync-claude-md
description: Use when wrapping up a feature or after significant structural changes, to audit AGENTS.md (the canonical agent guide; CLAUDE.md is only an `@AGENTS.md` include) against the current state of the repo and update it directly.
disable-model-invocation: true
---

# Execute: Sync AGENTS.md with current repo state

Run this skill only when the user explicitly asks for it by name (for example `/sync-claude-md`). Never start it on your own, for example at the end of a feature. If you were not asked, stop here.

Audit `AGENTS.md` at the repo root and update it to reflect the current state of the codebase. This is the canonical guidance file; `CLAUDE.md` is only an `@AGENTS.md` include and must NOT be edited (editing it would drop the include and duplicate content). Edit `AGENTS.md` directly — do not ask for confirmation first.

## Inputs

- Current `AGENTS.md` (read it before doing anything else — it's the baseline you're updating against).
- Recent git history. Read `git log --oneline -40` and `git log --diff-filter=A --name-only --since="3 months ago"` to find recently added subsystems.
- Current file/directory layout of the repo, especially `redis/`, `redis/asyncio/`, `redis/commands/`, `redis/_parsers/`, `tests/`, `.github/workflows/`, `.agents/`, `.claude/`.

## What to verify

Walk through each section of `AGENTS.md` and check:

### 1. Commands section
- Cross-reference every `invoke` task mentioned against `tasks.py`. Catch wrong defaults, renamed flags, removed/added tasks, changed conftest options.
- Cross-reference pytest options against the `pytest_addoption` calls in `tests/conftest.py`.
- Cross-reference the pytest marker list against `[tool.pytest.ini_options].markers` in `pyproject.toml`.

### 2. Architecture section
- Verify every module list that AGENTS.md already contains (for example under `redis/commands/` and `redis/_parsers/`) matches what's actually in the directory. Do not add module lists for packages that AGENTS.md describes in one line (for example `redis/multidb/` or `redis/observability/`); those are discoverable with `ls`.
- Check whether any new top-level subsystem under `redis/` was added since AGENTS.md was last touched and is not mentioned. Use `git log --diff-filter=A --name-only` scoped to `redis/` to find newly added files.
- Verify any specific symbols I reference (e.g., `READ_COMMANDS`, `RequestPolicy`, `ResponsePolicy`, `CommandsParser`, `ExponentialWithJitterBackoff`) still exist — grep for them.
- Confirm sync/async mirror claim: spot-check that the async equivalents listed exist under `redis/asyncio/`.

### 3. Repo conventions
- Cross-reference `requires-python`, `[project.optional-dependencies]`, and ruff config in `pyproject.toml`.

### 4. Skills index
- Every directory under `.agents/skills/` has exactly one row in the `## Skills` table, and every row names a skill that exists.
- `.claude/skills` is a symlink to `../.agents/skills`, so Claude Code sees the same skills. Do not replace it with copies.

### 5. Pointers to other docs
- Verify referenced files still exist, including the reference files under `.agents/references/`.
- Deep detail about one subsystem belongs in a file under `.agents/references/`, linked from AGENTS.md, not inline.

### 6. Size
- Keep `AGENTS.md` under 24 KiB (`wc -c AGENTS.md`). If it grows, move reference detail (architecture, subsystem notes) to `.agents/references/`.
- Never shorten, reword, move or remove the rule sections from `## MUST` through `# ✅ Final Checklist`. They are deliberate and their wording stays verbatim, even when that keeps the file over 200 lines. Keep their current format: the flat rule lists (for example `## MUST`, `## Testing`, Diff Validation and the Final Checklist) are single lines of numbered rules separated by `; `, while the Self-Check Loop and the Code Review "Verify" list keep one numbered check per line. Do not collapse or split them.

### 7. Boundaries and PR conventions
- Check that the `## Boundaries` tiers still match the repo: `.github/workflows/` still holds the CI and release workflows that "Ask first" refers to, and the commands the tiers mention are still valid. Never remove or weaken a Boundaries rule; if one looks wrong, report it in the summary instead of editing it.
- Check that `## PR conventions` still agrees with `CONTRIBUTING.md` (its `#pull-request-process` anchor still exists) and with `.github/PULL_REQUEST_TEMPLATE.md`, and that recent `git log` titles still follow the stated squash-merge convention.

## What to add (only if genuinely missing)

If a new subsystem, workflow, or developer-facing convention was introduced and is non-obvious from reading the directory tree, add a short note. Examples of things worth adding:

- A new top-level package under `redis/` with its own architectural role.
- A new `invoke` task or a meaningful change to an existing one.
- A new pytest marker or a new convention about how tests are organized.
- A newly added `.agents/skills/` skill, as one row in the `## Skills` table.
- A new spec file under `specs/` that documents a load-bearing design decision.

## What NOT to add

- Per-file descriptions or anything discoverable by `ls`. AGENTS.md is an orienting document, not a directory listing.
- Generic Python or git advice.
- Recent bugfixes or commit-level changes. AGENTS.md describes the repo, not its history.
- Anything obvious from reading `README.md` or `CONTRIBUTING.md`.

## Output

1. Apply edits directly to `AGENTS.md` using the `Edit` tool. Prefer small, targeted edits over rewrites.
2. After editing, print a short summary of what changed and why (under 200 words). Group by section: Commands / Architecture / Other.
3. If nothing needs to change, say so explicitly — do not edit the file just to look productive.
