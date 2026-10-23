#!/usr/bin/env bash
# TPC-H Q5 at SF10000 on 4 GPUs, distributed Sirius: generate the Q5 column copy, then run Q5
# from GPFS, from the Linux page cache and from Sirius's pinned host cache, check the answers,
# and build the profile page from the last run's telemetry.
#
# usage (from the repo root, after build.sh):
#   DATA=/local/nvme/q5_sf10000 OUT=/tmp/$USER/q5-runs \
#     pixi run bash bench/dist-q5-sf10000/run_q5.sh [STEP...]
# STEPs, in order, default all: gen gpfs page pinned profile
#   gen      generate the Q5 copy into DATA (skipped when DATA/lineitem exists)
#   gpfs     run queries.py Q5 from GPFS_DATA (skipped when GPFS_DATA is unset)
#   page     read DATA once into the page cache, then run both Q5 texts with page-cache.yaml
#   pinned   run both Q5 texts with pinned-cache.yaml (first run reads DATA with O_DIRECT)
#   profile  build OUT/q5-profile.html from the telemetry of the last run
# Knobs: GPFS_DATA (full SF10000 parquet on GPFS), JOBS/THREADS (generator), RANKS (default 4),
#        ITERS (default 3), STAGE (generator scratch, default /dev/shm/$USER-tpchgen).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA="${DATA:?set DATA to a local directory for the Q5 copy (about 502 GB)}"
OUT="${OUT:-/tmp/$USER/q5-runs}"
GPFS_DATA="${GPFS_DATA:-}"
JOBS="${JOBS:-24}"
THREADS="${THREADS:-6}"
RANKS="${RANKS:-4}"
ITERS="${ITERS:-3}"
STAGE="${STAGE:-/dev/shm/$USER-tpchgen}"
STEPS=("${@:-gen gpfs page pinned profile}")
STEPS=(${STEPS[*]})
mkdir -p "$OUT"

has() { [[ " ${STEPS[*]} " == *" $1 "* ]]; }
run() { python "$HERE/run_query.py" 5 --out "$OUT" --ranks "$RANKS" --iters "$ITERS" "$@"; }
ref_args() {  # compare against the GPFS run of queries.py Q5 when there is one
  local log="$OUT/q05_std_gpfs/exchange_rank_0.log"
  if [ "$1" = std ] && [ -f "$log" ]; then echo "--ref $log"; fi
}

if pgrep -x duckdb >/dev/null; then
  echo "ERROR: a duckdb process is running:"; pgrep -ax duckdb; exit 1
fi
nvidia-smi --query-gpu=index,memory.used,utilization.gpu --format=csv

if has gen; then
  if [ -d "$DATA/lineitem" ]; then
    echo "gen: $DATA/lineitem exists, keeping it"
  else
    python "$HERE/columns.py" --keep 5 > "$OUT/keep-q5.json"
    start=$(date +%s)
    python "$HERE/gen_tables.py" 10000 "$DATA" "$OUT/keep-q5.json" "$JOBS" "$THREADS" "$STAGE"
    echo "gen: $(( $(date +%s) - start )) s, $(du -sh "$DATA" | cut -f1)"
  fi
fi

if has gpfs && [ -n "$GPFS_DATA" ]; then
  run --std --config "$HERE/gpfs.yaml" --data "$GPFS_DATA" --tag _gpfs
fi

if has page; then
  start=$(date +%s)
  find "$DATA" -name '*.parquet' -print0 | xargs -0 -P 64 -n 8 cat > /dev/null
  echo "page: read DATA into the page cache in $(( $(date +%s) - start )) s"
  run --config "$HERE/page-cache.yaml" --data "$DATA" --tag _page
  run --std --config "$HERE/page-cache.yaml" --data "$DATA" --tag _page $(ref_args std)
fi

if has pinned; then
  run --config "$HERE/pinned-cache.yaml" --data "$DATA" --tag _pinned
  run --std --config "$HERE/pinned-cache.yaml" --data "$DATA" --tag _pinned $(ref_args std)
fi

if has profile; then
  # The last run's telemetry: the RANKS newest directories.
  mapfile -t dirs < <(ls -1dt "$OUT"/telemetry_data/*/ | head -n "$RANKS")
  python "$HERE/extract.py" --identify "${dirs[@]}"
  labels=$(python "$HERE/extract.py" --auto "$OUT/prof.json" "${dirs[@]}")
  python "$HERE/build_page.py" "$OUT/prof.json" "$OUT/q5-profile.html" "$labels"
  echo "profile: $OUT/q5-profile.html (edit the prose in template.html for a new run)"
fi

echo "results: $OUT/results.jsonl"
