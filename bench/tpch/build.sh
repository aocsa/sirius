#!/usr/bin/env bash
# Build Sirius with the NIXL exchange fabric, which multi-rank runs need.
# usage: NIXL_PREFIX=/path/to/nixl bash bench/tpch/build.sh [> build.log 2>&1]
# A first build takes a long time: run it in the background and log it.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NIXL_PREFIX="${NIXL_PREFIX:-$HOME/tools/nvda_nixl}"
[ -d "$NIXL_PREFIX/include" ] || { echo "ERROR: no NIXL install at $NIXL_PREFIX (set NIXL_PREFIX=)"; exit 1; }

cd "$REPO"
git submodule update --init --recursive
# The Makefile configures the release preset without NIXL; configure it first with NIXL on, and
# make reuses that cache.
pixi run cmake --preset release -DSIRIUS_DUCKDB_SOURCE_DIR="$REPO/duckdb" \
  -DSIRIUS_WITH_NIXL=ON -DSIRIUS_NIXL_PREFIX="$NIXL_PREFIX"
pixi run make
ls -l "$REPO/sirius-duckdb/build/release/extension/sirius/sirius.duckdb_extension"
