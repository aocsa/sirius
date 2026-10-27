#!/usr/bin/env bash
# Distributed TPC-H on every GPU of one host, one Sirius rank per GPU. Each query runs in up to
# three cases and every answer is checked:
#   direct  the full dataset, read with O_DIRECT (also the reference answer for the others)
#   page    a copy of only the query's columns, read through the Linux page cache
#   pinned  the same copy, held in Sirius's pinned host cache after the first iteration
#   hostpin the full dataset, its columns pinned in host memory with pin_table before the runs
#
# usage (from the repo root, after build.sh):
#   FULL_DATA=/path/to/tpch_sf10000 COPY_ROOT=/local/disk/tpch-copies OUT=/tmp/$USER/tpch-runs \
#     pixi run bash bench/tpch/run_tpch.sh
# Knobs:
#   QUERIES    queries to run (default "1 2 ... 22")
#   CASES      cases to run (default "direct page pinned"; hostpin is opt-in)
#   SF         scale factor (default 10000)
#   FULL_DATA  the full Parquet dataset, one directory per table (needed by direct and hostpin)
#   COPY_ROOT  where each query's column copy is generated (needed by page and pinned). A copy is
#              deleted after its query unless KEEP_COPIES=1; one that doesn't fit is skipped.
#   RANKS (default: every GPU), ITERS (default 3), JOBS/THREADS/STAGE (generator)
#   HOST_POOL_DIRECT/PAGE/PINNED/HOSTPIN  override each config's host pool per rank, e.g. 300Gi
#   PIN_PLANS  Simpatico plan directory: hostpin pins compressed (tables without a plan pin raw),
#              e.g. src/compression/simpatico_codegen/plans/tpch_sf1000
#   SPILL_RESERVE  bytes left free in COPY_ROOT for spill and staging (default 150 GB)
#   REF_DIR    an earlier run's output, used for the reference answers when this run has no
#              direct case: REF_DIR/q<NN>_direct/ or REF_DIR/q<NN>/ with rank 0's log
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-/tmp/$USER/tpch-runs}"
QUERIES="${QUERIES:-$(seq -s ' ' 1 22)}"
CASES="${CASES:-direct page pinned}"
SF="${SF:-10000}"
FULL_DATA="${FULL_DATA:-}"
COPY_ROOT="${COPY_ROOT:-}"
KEEP_COPIES="${KEEP_COPIES:-0}"
RANKS="${RANKS:-$(nvidia-smi -L | wc -l)}"
ITERS="${ITERS:-3}"
JOBS="${JOBS:-24}"
THREADS="${THREADS:-6}"
STAGE="${STAGE:-/dev/shm/$USER-tpchgen}"
SPILL_RESERVE="${SPILL_RESERVE:-150000000000}"
mkdir -p "$OUT"

has() { [[ " $CASES " == *" $1 "* ]]; }
note() { echo "$(date +%T) $*" | tee -a "$OUT/run_tpch.log"; }
skip() {  # record a case that could not run, so the summary shows it
  python3 -c 'import json,sys; print(json.dumps({"query": int(sys.argv[1]), "tag": sys.argv[2], "skipped": sys.argv[3]}))' \
    "$1" "$2" "$3" >> "$OUT/results.jsonl"
  note "Q$1 $2: skipped: $3"
}
run() {  # run_query.py, and keep going when a query fails
  python "$HERE/run_query.py" "$1" --out "$OUT" --sf "$SF" --ranks "$RANKS" --iters "$ITERS" "${@:2}" \
    || note "Q$1: run_query.py failed (see $OUT/results.jsonl and the rank logs)"
}
pool() { local v="$1"; [ -n "$v" ] && echo "--host-pool $v"; true; }

if pgrep -x duckdb >/dev/null; then
  echo "ERROR: a duckdb process is running:"; pgrep -ax duckdb; exit 1
fi
if has direct && [ -z "$FULL_DATA" ]; then note "FULL_DATA unset: no direct case, answers unverified"; fi
if { has page || has pinned; } && [ -z "$COPY_ROOT" ]; then echo "ERROR: set COPY_ROOT for the page and pinned cases"; exit 1; fi
if has pinned && [ "$(stat -f -c %T "$COPY_ROOT" 2>/dev/null)" = tmpfs ]; then
  echo "ERROR: COPY_ROOT is tmpfs, which has no O_DIRECT: the pinned case needs a disk"; exit 1
fi
note "queries: $QUERIES; cases: $CASES; SF $SF; $RANKS ranks; $ITERS iterations"
nvidia-smi --query-gpu=index,memory.used,utilization.gpu --format=csv

for q in $QUERIES; do
  qq=$(printf %02d "$q")

  if has direct && [ -n "$FULL_DATA" ]; then
    note "Q$q direct"
    run "$q" --config "$HERE/direct-io.yaml" --data "$FULL_DATA" --tag _direct $(pool "${HOST_POOL_DIRECT:-}")
  fi
  # Reference answer: this run's direct case, else REF_DIR's.
  refarg=()
  for ref in "$OUT/q${qq}_direct" ${REF_DIR:+"$REF_DIR/q${qq}_direct" "$REF_DIR/q${qq}"}; do
    if [ -f "$ref/exchange_rank_0.log" ]; then refarg=(--ref "$ref/exchange_rank_0.log"); break; fi
  done

  if has hostpin && [ -n "$FULL_DATA" ]; then
    note "Q$q hostpin"
    run "$q" --config "$HERE/host-pin.yaml" --data "$FULL_DATA" --tag _hostpin --pin-tier host \
      "${refarg[@]}" $(pool "${HOST_POOL_HOSTPIN:-}") ${PIN_PLANS:+--pin-plans "$PIN_PLANS"}
  fi

  if has page || has pinned; then
    copy="$COPY_ROOT/q$qq"
    need=$(python "$HERE/columns.py" --bytes "$q" --sf "$SF")
    if [ ! -f "$copy/.done" ]; then  # gen_tables.py resumes an interrupted copy
      free=$(df -B1 --output=avail "$COPY_ROOT" | tail -1)
      if (( need * 105 / 100 + SPILL_RESERVE > free )); then
        for c in page pinned; do has $c && skip "$q" "_$c" "copy of $((need / 1000000000)) GB does not fit in $COPY_ROOT ($((free / 1000000000)) GB free)"; done
        continue
      fi
      note "Q$q: generating a copy of about $((need / 1000000000)) GB in $copy"
      mkdir -p "$copy"
      python "$HERE/columns.py" --keep "$q" --sf "$SF" > "$copy/keep.json"
      start=$(date +%s)
      python "$HERE/gen_tables.py" "$SF" "$copy" "$copy/keep.json" "$JOBS" "$THREADS" "$STAGE" > "$copy/gen.log" 2>&1
      touch "$copy/.done"
      echo "{\"query\": $q, \"copy_bytes\": $(du -sb "$copy" | cut -f1), \"gen_s\": $(( $(date +%s) - start ))}" >> "$OUT/copies.jsonl"
    fi

    if has page; then
      start=$(date +%s)
      find "$copy" -name '*.parquet' -print0 | xargs -0 -P 64 -n 8 cat > /dev/null
      note "Q$q page: copy read into the page cache in $(( $(date +%s) - start )) s"
      run "$q" --config "$HERE/page-cache.yaml" --data "$copy" --tag _page "${refarg[@]}" $(pool "${HOST_POOL_PAGE:-}")
    fi
    if has pinned; then
      note "Q$q pinned"
      run "$q" --config "$HERE/pinned-cache.yaml" --data "$copy" --tag _pinned "${refarg[@]}" $(pool "${HOST_POOL_PINNED:-}")
    fi
    [ "$KEEP_COPIES" = 1 ] || rm -rf "$copy"
  fi
done

python "$HERE/summarize.py" "$OUT" | tee "$OUT/summary.md"
