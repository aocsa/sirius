#!/usr/bin/env bash
# Copyright 2026, Sirius Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the LICENSE file at the repo root for the full text.
#
# Pin synthetic parquet, then time scan/filter, high-card GROUP BY, partitioned
# equijoin, and skewed join at topology.num_gpus 1 vs 2.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_query_mgpu.sh [options]

Warm GPUs (and nvbandwidth if present), generate multi-file parquet if needed,
pin_table on GPU, then run the query suite in two processes (1-GPU YAML vs
2-GPU YAML). Writes wall time, remote clone bytes, and clone_ms.

Options:
  --output-dir DIR   Default: build/benchmark-results/query-mgpu
  --rows N           Rows in table t (default 100000000; 1000000 with --smoke)
  --join-rows N      Rows in build/probe (default 200000000)
  --files N          Parquet files per table (default 32)
  --reps N           Timed iterations per query (default 3)
  --warmup N         Untimed iterations per query (default 1)
  --timeout SEC      Per DuckDB session timeout (default 7200)
  --smoke            Small data + 1-GPU vs 2-GPU result equality
  --skip-generate    Reuse parquet already under the output dir
  --skip-pcie-warmup Skip run_nvbandwidth.sh (topology / PCIe train / nvbandwidth)
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/build/benchmark-results/query-mgpu"
PY_ARGS=()
SKIP_PCIE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --rows|--join-rows|--files|--reps|--warmup|--timeout)
      PY_ARGS+=("$1" "$2")
      shift 2
      ;;
    --smoke|--skip-generate)
      PY_ARGS+=("$1")
      shift
      ;;
    --skip-pcie-warmup)
      SKIP_PCIE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "${OUTPUT_DIR}"

if [[ "${SKIP_PCIE}" -eq 0 ]]; then
  echo "======== Layer 1: interconnect warmup ========"
  "${SCRIPT_DIR}/run_nvbandwidth.sh" --output-dir "${OUTPUT_DIR}" || true
fi

echo
echo "======== Layer 2: pinned queries, num_gpus 1 vs 2 ========"
python3 "${SCRIPT_DIR}/query_mgpu.py" --output-dir "${OUTPUT_DIR}" "${PY_ARGS[@]+"${PY_ARGS[@]}"}"

CSV="${OUTPUT_DIR}/query_mgpu.csv"
PARENT_CSV="${REPO_ROOT}/build/benchmark-results/query_mgpu.csv"
if [[ -f "${CSV}" ]]; then
  mkdir -p "$(dirname "${PARENT_CSV}")"
  cp -f "${CSV}" "${PARENT_CSV}"
  echo "Copied CSV to ${PARENT_CSV}"
fi
