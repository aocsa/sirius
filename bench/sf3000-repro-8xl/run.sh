#!/usr/bin/env bash
# Run the sirius-spill-reproducer 8xl-sf3k profile on one GB200 with local parquet (no S3).
#
#   PKG_TAR=/path/sirius-spill-reproducer.tar.gz ENGINE=/path/engine OUT=/path/results \
#     bash run.sh cold lukewarm hot
#
# The package itself is not vendored here (it carries ~770 MB of saved reference outputs and
# names a private bucket). This script extracts it, applies reproducer-gb200.patch, and runs
# the package's own run.py, then validate.py and summarize.py for each mode.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PKG_TAR=${PKG_TAR:?path to sirius-spill-reproducer.tar.gz}
ENGINE=${ENGINE:?built engine checkout (prepare-engine.sh)}
OUT=${OUT:?new results directory}
WORK=${WORK:-$OUT/package}
DATA=${DATA:-/scratch/sirius/datasets/tpch_sf3000}     # <table>/*.parquet, decimal128
SOURCE_POLICY=${SOURCE_POLICY:-integration}           # historical asserts tree 68fee848
TASKSET=${TASKSET:-0-31}                              # 32 vCPU, like g7e.8xlarge
export CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-0}
export REPRO_SPILL_ROOT=${REPRO_SPILL_ROOT:-/scratch/prestouser/aocsa/spill}
# 0.86 x the g7e's 97887 MiB RTX PRO 6000. The package's usage_limit_fraction: 0.86 would
# hand Sirius 163 GB of a 189 GB GB200.
export REPRO_GPU_LIMIT_BYTES=${REPRO_GPU_LIMIT_BYTES:-88272084664}
export PATH="$HOME/.pixi/bin:$PATH"
modes=("${@:-cold}")

if [ ! -d "$WORK/sirius-spill-reproducer" ]; then
  mkdir -p "$WORK"
  tar -xzf "$PKG_TAR" -C "$WORK"
  (cd "$WORK/sirius-spill-reproducer" && patch -p1 < "$HERE/reproducer-gb200.patch")
fi
PKG="$WORK/sirius-spill-reproducer"
mkdir -p "$REPRO_SPILL_ROOT"

cd "$ENGINE"
for mode in "${modes[@]}"; do
  out="$OUT/8xl-sf3k-$mode"
  echo "=== $mode -> $out ($(date -u +%T))"
  taskset -c "$TASKSET" pixi run python "$PKG/run.py" 8xl-sf3k --repo "$ENGINE" \
    --source-policy "$SOURCE_POLICY" --source "$DATA" --mode "$mode" --output "$out"
  pixi run python "$PKG/tools/validate.py" --profile 8xl-sf3k --output "$out" | tee "$out/validation.json"
  pixi run python "$PKG/tools/summarize.py" "$out" | tee "$out/summary.md"
done
