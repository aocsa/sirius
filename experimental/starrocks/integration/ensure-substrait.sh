#!/usr/bin/env bash
# Makes the `substrait` submodule's commit available without depending on an unmerged branch.
#
# The submodule is pinned at 9b5b2d4, the FetchRel count_expr/offset_expr fix (sirius-db/sirius#2107,
# sirius-db/duckdb-substrait-extension#4). Today that commit lives only on the PR's branch
# fetch-count-expr-1.5.5. If it can't be fetched (the branch was deleted after a squash merge, or
# there's no network), this script rebuilds it from patches/ on top of its parent, which is on the
# stable duckdb-1.5.5 branch. The replay keeps the original author, committer and dates, so it
# reproduces the exact same commit hash and `git submodule update` then finds it locally.
#
# SUBSTRAIT_FROM_PATCH=1 skips the fetch and always rebuilds from the patch.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

WANT=9b5b2d43ff929eeff328515782e0aa54406e2674
PARENT=a7e045befd5479569f0a84b241868253cdfde0b3
PATCH=$INTEGRATION_DIR/patches/substrait-fetch-count-expr.patch

cd "$SIRIUS_DIR"
pinned=$(git ls-tree HEAD substrait | awk '{print $3}')
if [[ "$pinned" != "$WANT" ]]; then
    echo "substrait is pinned at $pinned, not $WANT; nothing to do"
    exit 0
fi

have() { git -C substrait cat-file -e "$WANT^{commit}" 2>/dev/null; }

if [[ "${SUBSTRAIT_FROM_PATCH:-0}" != 1 ]] && git submodule update --init substrait 2>/dev/null; then
    echo "substrait: fetched $WANT"
    exit 0
fi

# The clone is there even when checking out the commit failed; make sure it is.
[[ -e substrait/.git ]] || git submodule update --init substrait 2>/dev/null || true
[[ -e substrait/.git ]] || { echo "substrait: could not clone the submodule" >&2; exit 1; }

if ! have; then
    echo "substrait: rebuilding $WANT from $PATCH"
    git -C substrait fetch -q origin duckdb-1.5.5
    git -C substrait checkout -q --detach "$PARENT"
    (
        cd substrait
        export GIT_COMMITTER_NAME="Alexander Victor Ocsa Mamani"
        export GIT_COMMITTER_EMAIL="aocsa.cs@gmail.com"
        export GIT_COMMITTER_DATE="1791553447 +0000"
        git am -q --committer-date-is-author-date "$PATCH"
    )
    have || { echo "substrait: the replay did not produce $WANT" >&2; exit 1; }
    [[ "$(git -C substrait rev-parse HEAD)" == "$WANT" ]] || { echo "substrait: replay gave $(git -C substrait rev-parse HEAD)" >&2; exit 1; }
fi
git submodule update --init substrait
echo "substrait: at $(git -C substrait rev-parse --short HEAD)"
