#!/usr/bin/env bash
# One-time setup: the tools repo (box/gb200), the substrait commit, then UCX/NIXL, the engine,
# the StarRocks FE and the CN (the tools repo's setup/all.sh). Safe to rerun.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

if [[ ! -d "$TOOLS_REPO/.git" ]]; then
    echo "== cloning $TOOLS_REMOTE ($TOOLS_BRANCH) into $TOOLS_REPO"
    git clone -b "$TOOLS_BRANCH" "$TOOLS_REMOTE" "$TOOLS_REPO"
fi
# box/gb200 predates main's 36a4f7e, which compares an empty result (q11 at SF1000) with DuckDB
# instead of failing it. The README says box branches pick up main with `git rebase main`.
if ! git -C "$TOOLS_REPO" merge-base --is-ancestor 36a4f7e HEAD 2>/dev/null; then
    echo "== rebasing $TOOLS_REPO onto origin/main for the q11 fix (36a4f7e)"
    git -C "$TOOLS_REPO" fetch -q origin main
    git -C "$TOOLS_REPO" rebase -q origin/main || {
        echo "rebase failed: run 'git -C $TOOLS_REPO rebase origin/main' by hand" >&2
        exit 1
    }
fi

"$INTEGRATION_DIR/ensure-substrait.sh"

echo "== building with $TOOLS_REPO/setup/all.sh (SIRIUS_DIR=$SIRIUS_DIR)"
"$TOOLS_REPO/setup/all.sh"
