#!/usr/bin/env bash
# TPC-H SF3000 on one GPU with spilling, spill compression OFF vs ON (PRs #2182/#2183/#2204).
# Adapted from bench/sf1000-repro/run.sh: nothing is pinned (SF3000 does not fit) and the GPU is
# capped at a g7e.8xlarge's 96 GB, so the suite spills to host and disk.
#
# Uses the CLI runner (run_tpch_parquet.sh, built v2 duckdb host) rather than
# performance_test.py: since the DuckDB v2 migration the pixi Python client (1.5.6) cannot
# load the extension. Each arm gets its own YAML with compression.enable_spill_compression.
#
#   pixi run bash bench/sf3000-spill/run.sh                       # both arms, OFF then ON
#   ARMS=on QUERIES="1 6" ITERATIONS=1 pixi run bash bench/sf3000-spill/run.sh   # smoke test
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"

DATA="${DATA:-/scratch/sirius/datasets/tpch_sf3000}"
CFG="${CFG:-$HERE/sirius-sf3000-spill.yaml}"
SPILL_DIR="${SPILL_DIR:-/scratch/prestouser/aocsa/spill}"
OUT="${OUT:-$REPO/bench/sf3000-spill/results}"
ARMS="${ARMS:-off on}"
QUERIES="${QUERIES:-$(seq -s ' ' 1 22)}"
ITERATIONS="${ITERATIONS:-2}"                     # iter 1 cold, iter 2 warm
TIMEOUT="${TIMEOUT:-0}"                           # whole-session timeout; 0 = none
TASKSET="${TASKSET:-}"                            # e.g. 0-31 to mimic a 32-vCPU g7e.8xlarge
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0}"

[ -d "$DATA" ] || { echo "ERROR: no SF3000 parquet at $DATA (set DATA=)"; exit 1; }
mkdir -p "$SPILL_DIR" "$OUT"
STAMP="$(date +%Y%m%d_%H%M%S)"

cd "$REPO"
for arm in $ARMS; do
  case "$arm" in
    on)  spill=true ;;
    off) spill=false ;;
    *) echo "ERROR: arm must be on|off, got $arm"; exit 1 ;;
  esac
  ARM_DIR="$OUT/${STAMP}_${LABEL:+${LABEL}_}spill_$arm"
  mkdir -p "$ARM_DIR"
  # Point the disk tier at SPILL_DIR and set the arm's spill compression, without editing CFG.
  sed "s#downgrade_root_dirs: .*#downgrade_root_dirs: \"$SPILL_DIR\"#" "$CFG" > "$ARM_DIR/sirius.yaml"
  printf '    compression:\n        enable_spill_compression: %s\n' "$spill" >> "$ARM_DIR/sirius.yaml"

  echo "=== arm=$arm  data=$DATA  gpu=$CUDA_VISIBLE_DEVICES  spill=$SPILL_DIR  out=$ARM_DIR"
  echo "    queries: $QUERIES  iterations: $ITERATIONS"
  [ -n "$TASKSET" ] && echo "    cpus: $TASKSET"
  pin=()
  [ -n "$TASKSET" ] && pin=(taskset -c "$TASKSET")
  start=$(date +%s)
  # shellcheck disable=SC2086
  SIRIUS_CONFIG_FILE="$ARM_DIR/sirius.yaml" OUTPUT_DIR="$ARM_DIR" TIMING_CSV="$ARM_DIR/timings.csv" \
    "${pin[@]}" bash test/tpch_performance/run_tpch_parquet.sh \
      --parquet-dir "$DATA" --iterations "$ITERATIONS" --timeout "$TIMEOUT" \
      sirius 3000 $QUERIES 2>&1 | tee "$ARM_DIR/run.log"
  echo "=== arm=$arm done in $(( $(date +%s) - start ))s; spill dir now holds $(du -sh "$SPILL_DIR" | cut -f1)"
done
