#!/bin/sh
# Create or refresh three sibling worktrees for an A/B run of a pull request:
#   <prefix>-master  current origin/master
#   <prefix>-base    the PR's merge-base with master (isolates the PR's effect)
#   <prefix>-pr      the PR head
#
#   benchmarks/measure_read_path/prepare_worktrees.sh <PR number> [remote] [prefix]
#
# remote defaults to origin, prefix to ../redis-py-bench. A worktree with
# local changes is never touched.
set -eu
PR="$1"
REMOTE="${2:-origin}"
REPO="$(git rev-parse --show-toplevel)"
PREFIX="${3:-$REPO/../redis-py-bench}"

git -C "$REPO" fetch "$REMOTE" master "+pull/$PR/head:refs/bench/pr-$PR"
BASE="$(git -C "$REPO" merge-base "refs/bench/pr-$PR" "$REMOTE/master")"

place() {
    wt="$1"
    rev="$2"
    if [ -d "$wt" ]; then
        if [ -n "$(git -C "$wt" status --porcelain)" ]; then
            echo "refusing: $wt has local changes" >&2
            exit 1
        fi
        git -C "$wt" checkout --quiet --detach "$rev"
    else
        git -C "$REPO" worktree add --quiet --detach "$wt" "$rev"
    fi
    echo "$wt -> $(git -C "$wt" rev-parse --short HEAD)"
}

place "$PREFIX-master" "$REMOTE/master"
place "$PREFIX-base" "$BASE"
place "$PREFIX-pr" "refs/bench/pr-$PR"
