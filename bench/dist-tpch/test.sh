#!/usr/bin/env bash
# Unit tests for the pieces this experiment touches: pinned-cache matching ([can_serve]) and the
# per-rank byte-range split of a table's files ([byte_range]). Needs a build (build.sh).
# usage: bash bench/dist-q5-sf10000/test.sh [extra Catch2 args]
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO/build/release/test/cpp/sirius_unittest"
[ -x "$BIN" ] || { echo "ERROR: sirius_unittest not built (run build.sh)"; exit 1; }
cd "$REPO"
# The test binary reserves a large GPU pool at start: point it at an idle GPU.
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-3}"
pixi run "$BIN" "[can_serve],[byte_range]" "$@"
