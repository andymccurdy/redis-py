---
name: prepare-for-takeover
description: Prepare copy-paste-ready maintainer comments that mark stalled but valuable redis-py PRs as `open-for-takeover`, inviting the original author or any other contributor to finish the work. Each comment summarizes what is left based on the maintainer's earlier review. Use when the user says "prepare for takeover", "open for takeover", "mark as abandoned", "invite contributors to finish", or gives one or more PR numbers/URLs that stalled and should be handed over.
---

# Prepare for Takeover

## Purpose

Some PRs are worth finishing, but the author has gone quiet and the maintainer has no time to complete them. This skill drafts one comment per PR that:

1. Says the PR seems to have stalled but is still valuable for redis-py.
2. Announces the `open-for-takeover` label.
3. Lists what is left, taken from the maintainer's earlier review.
4. Invites the original author to continue, or anyone else to take over while keeping the author's credit.

For now the skill only generates the comment texts. It does not apply labels, post comments, or edit workflows.

## Hard rule: no GitHub writes

Use read-only GitHub access only (`gh pr view`, `gh api` GET). Never comment, label, review, or edit anything on GitHub. The user posts the comments and applies the labels manually. `gh` may fail with a TLS error inside the command sandbox; run it with the sandbox disabled when needed.

## Inputs

- One or more PR numbers or URLs. Default repo: `redis/redis-py`. Invoking the skill on a PR makes it eligible; do not require any label (such as `waiting-for-response`), a minimum inactivity period, or other staleness signals.
- The maintainer's GitHub login: resolve it with `gh api user --jq .login`. Do not guess.

## Data to collect per PR (read-only)

```
gh pr view <n> -R <owner/repo> --json title,author,state,isDraft,labels,updatedAt,body,reviews,comments
gh api graphql --paginate -F owner=<owner> -F repo=<repo> -F n=<n> -f query='
  query($owner: String!, $repo: String!, $n: Int!, $endCursor: String) {
    repository(owner: $owner, name: $repo) {
      pullRequest(number: $n) {
        reviewThreads(first: 100, after: $endCursor) {
          pageInfo { hasNextPage endCursor }
          nodes {
            isResolved isOutdated path line subjectType
            comments(first: 100) {
              nodes { author { login } body createdAt pullRequestReview { state } }
            }
          }
        }
      }
    }
  }'
```

The second command returns the inline review threads, which `gh pr view` does not include. A `CHANGES_REQUESTED` review often has an empty or short body with the details inline. Per thread:

- `isResolved: true` means the thread was settled (often the author resolved it after the fix, with no reply); treat it as done unless a later maintainer comment reopens it.
- `isOutdated: true` means the code moved under the comment; check whether it still applies.
- `subjectType: FILE` is a file-level comment. Its `line` is null, but it is still current.

Ignore pending (unsubmitted) feedback: reviews with `state: PENDING` and thread comments whose `pullRequestReview.state` is `PENDING`. They are drafts visible only to the maintainer, so they must not appear in a public comment or count as an earlier review.

From the output, get:

- `author.login`: the PR author.
- Whether the maintainer has already commented or reviewed (a comment, submitted review or inline comment by the maintainer's login).
- All maintainer feedback in chronological order: review bodies, inline comments and conversation comments. Read it in full; it is the source for "What's left". Drop items in resolved threads, items that a later maintainer comment withdrew or marked as done, and items that a later commit or reply clearly addressed. An empty review body does not mean there is no feedback.
- Current labels (to point out `waiting-for-response` for removal) and `isDraft`.

If the PR is closed or merged, skip it and say so. If the maintainer left no feedback in any of these sources, write "What's left" from the open TODOs in the PR `body` or ask the user for a line, and do not invent requirements.

## Writing "What's left"

- 1 to 5 short bullets, each one concrete action, in the order the maintainer raised them.
- Paraphrase the review. Do not add new requests that the maintainer did not make.
- Keep scope notes from the review (for example "change Fixes #N to Refs #N", "keep #N open, retitle the PR").
- Add "Take the PR out of draft." when `isDraft` is true and the review asked for it.
- Put file and symbol names in single backticks.

## Comment template

Include the greeting line **only** if the maintainer has not commented on or reviewed this PR before. Otherwise start with `@<author>, this PR seems to have stalled...`.

```markdown
Hey @<author>, thank you for your contribution!

This PR seems to have stalled, but the change is still valuable for redis-py and I would like to see it land. I don't have the bandwidth to finish it myself right now, so I'm marking it as `open-for-takeover`.

What's left (details in my review above):
- <action>
- <action>

@<author>, if you want to continue, you're very welcome to, just let me know here. Otherwise, anyone interested can take it over: leave a comment first so two people don't work on it at the same time, then open a new PR that builds on this branch (keep the original commits so @<author> keeps the credit) and links back here.

Any help is very welcome!
```

When there is no earlier maintainer review or inline review comment, use `What's left:` without "(details in my review above)".

## Writing rules

- Use "I", never "we/us/our". The maintainer is one person.
- Never use em dashes, en dashes, or `--` as punctuation; use a single hyphen.
- No code fences inside the comment (it breaks the outer fence). Use single backticks only.
- Keep it short; do not restate the full review.

## Output format

Start with one line naming the audience, for example `Written for: the PR authors and other contributors on GitHub.`

Then for each PR, a bold heading with the number and author, followed by the comment in its own fenced ```markdown block:

**#<n>** (@<author>)

```markdown
<comment>
```

After the blocks, add at most two short notes:

- PRs that still carry `waiting-for-response` (remove it when adding `open-for-takeover`).
- PRs that were skipped (closed or merged).

## Label reference

If the user asks for the label details:

- Name: `open-for-takeover`
- Description: `Stalled but valuable work - anyone is welcome to pick it up and finish it`
- Color: `0e8a16`
- Stale bot: add `open-for-takeover` to `exempt-pr-labels` in `.github/workflows/stale-issues.yml`.
