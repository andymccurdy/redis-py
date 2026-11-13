---
name: pr-draft-summary
description: Use when the user's message mentions "PR", "pr", "PR description", "PR draft", "pull request", "the PR block", "commit msg" or "commit message", or otherwise asks for a commit message or PR write-up for their local redis-py changes. Produces a one-line commit message and a copy-pasteable Markdown PR description (Change summary + Test coverage, 5-6 paragraphs) covering all staged changes plus the commits already on the current branch.
---

# PR Draft Summary

## Purpose

After eligible work is complete, produce exactly two things for redis-py:

1. A **one-line commit message** (imperative, conventional prefix preferred).
2. A **PR description** as a single Markdown block the user can copy-paste into
   the GitHub PR UI with all formatting preserved. Keep it to **5–6 short
   paragraphs** that explain what the changes do.

After the two blocks, add one line with the **suggested PR label** (see
workflow step 3).

By default, emit both as text in the chat. Write them to a `.md` file **only**
if the user explicitly asks for a file.

## Scope of Changes to Evaluate

By default, cover **staged changes** and **commits already on the current
branch** (relative to the PR base) — that is what the commit/PR will actually
contain. Include **unstaged changes** and **untracked files** only when the user
explicitly asks for them; note that untracked files do not appear in
`git diff --stat`, so pick them up from `git status -sb` (the `??` entries). Use
commit messages as supporting context, not as a substitute for reading the
diffs. If the in-scope set is empty, reply briefly that no changes were detected
and skip emitting the blocks.

## Inputs to Collect Automatically (do not ask the user)

- Working tree overview — `git status -sb` (one shot: branch name, ahead/behind,
  and staged/unstaged/untracked entries, with `??` marking untracked files).
- What changed, with line counts — `git diff --stat --cached` for staged
  changes (stats include file names, so a separate `--name-only` is not needed);
  read the staged diff to write the summary. Run `git diff --stat` (unstaged)
  only when the user asks to include unstaged/untracked work.
- PR base — the branch this change will merge into. Accept it as an input: use
  the base branch the user names (which may be a feature/integration branch, not
  `master`). When none is given, default to `origin/master` (fall back to
  `master`). Then set `BASE_REF` to that branch and
  `BASE_COMMIT=$(git merge-base "$BASE_REF" HEAD)`.
- Branch commits vs the base — `git diff --stat "${BASE_COMMIT}..HEAD"` and
  `git log --oneline --no-merges "${BASE_COMMIT}..HEAD"`.

Classify the change from the touched paths: runtime (`redis/`,
`redis/asyncio/`), tests (`tests/`, `tests/test_asyncio/`), docs/examples
(`docs/`, `doctests/`, `benchmarks/`), build/test config (`pyproject.toml`,
`tasks.py`, `dockers/`, …), or repo-meta (`AGENTS.md`, `CLAUDE.md`, `.agents/`,
`.github/`).

## Workflow

1. Determine the PR base first: use the base branch the user provided; otherwise
   default to `origin/master` (fall back to `master`). Compute
   `BASE_REF`/`BASE_COMMIT`, then run the commands above. The base may be a
   feature or integration branch when the change targets one rather than
   `master`.
2. Build the in-scope diff: committed branch changes + staged changes by
   default; add unstaged and untracked changes only when the user explicitly
   asks. Use commit messages as supporting context, not as a substitute for
   inspecting the diff itself.
3. Infer change type from touched paths (feature, fix, refactor, perf, or
   docs-with-impact). Pick the commit-message prefix accordingly: `feat:`,
   `fix:`, `refactor:`, `perf:`, `docs:`, `test:`, `build:`, `chore:`.
   Also pick the category label the PR should carry, using
   `.agents/skills/generate-release-notes/references/pr-labels-guide.md` as the
   authoritative mapping. Judge it from the diff, not from the branch name. For
   example, a new opt-in public option is `feature`, even when it was written
   to fix a reported problem. Keep the commit prefix consistent with the label.
   If more than one category applies (for example `deprecation` and
   `breakingchange`), list every matching label. Only suggest labels; never
   apply them.
4. Flag **backward-compatibility risk** in the description only when the diff
   changes the public API (signatures, argument names, defaults, return types),
   command semantics, error/exception types, wire-protocol (RESP2/RESP3)
   handling, or persisted/connection config. Judge that risk against the latest
   released version, not unreleased branch-only churn.
5. Flag **sync/async parity** explicitly: if the change touches sync code under
   `redis/` without a mirrored change under `redis/asyncio/` (or vice versa, and
   likewise `tests/` vs `tests/test_asyncio/`), call it out, since this repo
   requires both stacks to stay aligned.
6. If the branch name matches `issue-<number>` (digits only) or an issue number
   is otherwise known, reference
   `https://github.com/redis/redis-py/issues/<number>` and add an auto-closing
   line such as `Resolves #<number>.` Do not block if the issue cannot be fetched.
7. Draft the two blocks using the format below. The PR description always has a
   "Change summary" and a "Test coverage" section. Keep it to 5–6 short
   paragraphs total; do not pad. In "Test coverage", describe the tests added or
   updated; if none were added, say so briefly and explain why.
8. Output both blocks as chat text by default. If — and only if — the user
   explicitly requested a file, also write them to a `.md` file and report its
   path.

## Output Format

Emit the commit message first, then the PR description. The description must be
a single fenced Markdown block so it can be copied into the GitHub UI with
formatting intact.

### Commit message

```
<single-line imperative commit message; conventional prefix preferred, e.g. fix: release async pool lock only after pool mutation>
```

### PR description

````
```markdown
## Change summary

<3–4 paragraphs of prose, starting with "This pull request resolves/updates/adds ..."
using a verb that matches the change. Explain what the change does and the
background — for bugs, the symptom/repro; for features, what is needed and why —
then the concrete changes: the main files/areas touched, the approach, and notable
edge cases. Call out public-API or backward-compatibility impact and any sync/async
parity considerations here. Bullets are allowed.>

## Test coverage

<1–2 paragraphs (bullets allowed) describing the tests added or updated and what
behavior they exercise (sync and async, edge/failure cases). If the change is not
testable or no tests were added, say so briefly and explain why.>

Resolves #<number>.   <!-- include only when an issue number is known -->
```
````

### Suggested label

```
Suggested label: <label(s) per pr-labels-guide.md>
```

Keep it tight — 5–6 paragraphs total across both sections, no redundant prose
around the blocks, and no repeating the same detail in both sections.
