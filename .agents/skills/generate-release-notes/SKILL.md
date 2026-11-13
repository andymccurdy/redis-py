---
name: generate-release-notes
description: Use when the user asks for redis-py release notes or a changelog ("release notes", "create/generate/draft/prepare release notes", "changelog"), for a specific version or branch ("release notes for 8.0", "generate release notes for the 8.0 branch"), or for what changed since the last release or tag. "Release notes for 8.0" means: use branch `8.0` as the release-branch input. Generates a GitHub release-notes draft and saves it as a Markdown file. Collects the merged PRs/commits since the previous release, maps each commit to its PR labels, categorizes them into the project's release sections (Breaking Changes, Deprecations, Experimental Features, New Features, Bug Fixes, Maintenance) using the skill's pr-labels-guide, infers a category for unlabeled PRs, and assembles the notes with an optional Highlights section and a contributors footer.
---

# Generate Release Notes

## Objective

Produce a redis-py release-notes draft that matches the structure of the project's published GitHub
releases, derived from the merged PRs on a given **release branch** since the previous release and
categorized by PR label. The release branch is the skill's primary input (step 1). Save the result as a Markdown file. Never publish it — drafting only; the user publishes the release themselves.

**Do not rely on any automated release-drafter output.** We do not use it to decide which PRs are
included or how they are categorized — it misses PRs and miscategorizes them. Collect the PRs
yourself from the git history (step 2) and categorize every one of them manually using this skill's
references, which are the complete and authoritative rules:

- `references/release-notes-template.md` — the section order, titles, line format, and full skeleton.
- `references/pr-labels-guide.md` — the authoritative label→category mapping, what each label means,
  and how to categorize a PR/commit that has no category label.

## Constraints

- **Read-only GitHub access only.** Per the repository owner's standing rule, never run `gh` (or any
  GitHub API call) that writes — no creating/editing releases, comments, labels, or tags — unless the
  user explicitly authorizes that specific action in the same turn. Use `gh ... view/list`,
  `git log`, or GET-only `gh api`/`curl` to gather data. `gh` may fail with a TLS error inside the
  command sandbox; run it with the sandbox disabled when needed.
- This skill **only writes the draft Markdown file** in the working tree. It does not commit, tag,
  push, or create a GitHub release.

## Workflow

### 1. Determine the release range and version

1. **Read the release branch as input.** The branch the release is cut from is the primary input to
   this skill — it defines which changes are included. Take it from the skill invocation/args if
   provided; otherwise ask the user for it (e.g. `8.0`, `release/8.0.2`, `master`). Do not guess.
   Verify it exists (`git rev-parse --verify <branch>`; fetch first if it is a remote branch,
   `origin/<branch>`). Every change to include is a commit reachable from this branch and not from
   the previous release tag.
2. Identify the previous release tag: the most recent `v*` tag reachable from the release branch,
   `git describe --tags --abbrev=0 <branch>` (or `gh release list -L 5`, read-only). Confirm with the
   user if a non-default base is intended.
3. Decide the new version. redis-py ships major, minor, and patch releases (e.g. `8.0.1`, `7.4.1`).
   Infer minor vs patch from the change set (any Breaking/New Feature → at least a minor bump) and
   confirm the exact version with the user if unclear. The tag form is `v<version>`.

### 2. Collect the merged PRs / commits in range

1. List the commits on the release branch since the previous tag:
   `git log --oneline --no-merges <prev-tag>..<branch>`. Each squashed/merged commit title typically
   ends with `(#<pr-number>)`; extract that number. For merge-commit workflows, use
   `git log --merges` or the GitHub compare API instead.
2. For each PR number, fetch its **title, labels, and author** (read-only):
   `gh pr view <n> --json number,title,labels,author` or
   `gh api repos/redis/redis-py/pulls/<n>`. Use the PR title for the change line (it is what the
   published notes use), not the raw commit subject, when they differ.
3. Drop any PR labeled `skip-changelog`.
4. If a commit has no associated PR (direct push), use the commit subject and treat it as unlabeled.

### 3. Categorize each commit

For each remaining commit, categorize it using `references/release-notes-template.md` and
`references/pr-labels-guide.md`:

- If the PR has one or more **category labels**, list it under **every** matching section — a PR with
  two category labels appears in both (e.g. `deprecation` + `breakingchange` → both ⚠️ Deprecations
  and 🔥 Breaking Changes).
- If the PR has **no category label**, or only topic/process labels, or a non-category repo label
  (e.g. `bug-fix`, `security`, `techdebt`), infer the section(s) using
  `references/pr-labels-guide.md` — apply **every** inference rule that matches, so a change may land
  in more than one section. Note the inferred labels you would suggest applying to the PR.
- When a category is genuinely ambiguous, place your best guess and leave a
  `<!-- TODO: confirm category for #<n> -->` marker, then surface it to the user.

### 4. Assemble the draft

Follow `references/release-notes-template.md`:

1. Start with `# Changes`.
2. Optionally add `## ✨ Highlights` (or a lead prose paragraph) — hand-authored `###` subsections
   describing each highlight with API names and doc/spec links. Include it **only when the release has
   something significant to focus the reader on**: a notable new feature, or a breaking change that
   may affect a large share of users. Omit for routine patch releases. When the release contains
   Breaking Changes or notable New Features, ask the user whether highlights are wanted.
3. Emit the category sections **in the order from `release-notes-template.md`** (🚀 New Features,
   🧪 Experimental, 🔥 Breaking Changes, ⚠️ Deprecations, 🐛 Bug Fixes, 🧰 Maintenance), omitting any
   empty section. Each line is `- <PR title> (#<n>)`. **Combine PRs that deliver the same
   feature/change onto one line** with all their numbers in a single bracket group in ascending order,
   e.g. `- <title> (#3434 #3456 #3467)` (common for paired sync/async PRs or a feature split across
   follow-ups) — see the grouping rule in `references/release-notes-template.md`.
4. End with the contributors footer: the thank-you line and the de-duplicated, space-separated list
   of `@<author>` handles for every included PR.

### 5. Save the draft as a local Markdown file

Write the assembled notes to a **local Markdown file** so the maintainer can review it and copy the
contents into the GitHub release UI by hand. This file is a local review artifact only:

- Default path: `release_notes/release_notes_<version>.md` at the repo root (e.g.
  `release_notes/release_notes_8.0.2.md`). Create the `release_notes/` folder if it does not exist.
  Confirm or accept an override path from the user.
- **Do not commit, tag, push, or create/edit a GitHub release.** The skill only writes the file in
  the working tree; the maintainer reviews it and publishes manually. (If the `release_notes/` folder
  is not already git-ignored, mention that so it is not accidentally committed.)
- The file content is exactly what should be pasted into the release body — no extra wrapper or
  commentary inside the file.

After writing, report the file path and a short summary: the version, the previous tag, the
per-section commit counts, the count of PRs whose category was inferred (and which ones), and any
`TODO` markers left in the draft.

## Verification

- Cross-check the commit count: the number of **distinct PR numbers** referenced across all change
  lines (a grouped line contributes all of its numbers), plus any `skip-changelog`/excluded PRs,
  should equal the commits in `git log <prev-tag>..<branch>` (minus merge commits). Grouping related
  PRs reduces the number of lines but not the number of PR references, so reconcile on PR numbers, not
  lines.
- Confirm section order and titles match `references/release-notes-template.md` and no empty section
  was emitted.
- Confirm every included PR number resolves to a real PR and every contributor handle is included
  once.
- Spot-check against the previous published release's formatting (run `gh release view <prev-tag>`).
