#!/usr/bin/env bash
# Runs a SQL file on N ranks on this host, rank i on GPU i, as one distributed query each, and
# prints rank 0's output (the result). Every rank runs the whole file: define tables over every
# file of each table, the same on every rank; each rank scans its own share of the files.
# With one rank no exchange is configured, so the file runs on the plain one-process engine path.
#   pixi run scripts/exchange_sql_ranks.sh <ranks> <file.sql> [base port=27000] [log dir=/tmp]
# Environment:
#   GPUS       rank i runs on the i-th GPU of this comma-separated list (default 0,1,...,N-1)
#   CPUS       semicolon-separated taskset CPU lists, one per rank, e.g. "0-15,48-63;16-47"
#   THREADS    DuckDB threads per rank (SET threads); default DuckDB's own
#   PREAMBLE   a SQL file every rank runs after the settings above, before <file.sql>
#   SIRIUS_BUILD_DIR  the build to run (default this checkout's sirius-duckdb/build/release)
# Needs a build configured with -DSIRIUS_WITH_NIXL=ON when ranks > 1.
set -euo pipefail
ranks=$1
sql=$2
port=${3:-27000}
logs=${4:-/tmp}
here=$(cd "$(dirname "$0")" && pwd)
source "$here/nixl_env.sh"
build=${SIRIUS_BUILD_DIR:-"$here/../sirius-duckdb/build/release"}

if [ -n "${GPUS:-}" ]; then IFS=, read -r -a gpus <<< "$GPUS"; else gpus=($(seq 0 $((ranks - 1)))); fi
cpus=()
if [ -n "${CPUS:-}" ]; then IFS=';' read -r -a cpus <<< "$CPUS"; fi
if ((${#gpus[@]} < ranks)) || { ((${#cpus[@]} > 0)) && ((${#cpus[@]} < ranks)); }; then
  echo "need one GPUS entry (and one CPUS entry, if set) per rank: $ranks ranks" >&2
  exit 2
fi

peers=""
for ((i = 0; i < ranks; i++)); do peers+="${peers:+,}127.0.0.1:$((port + i))"; done

pids=()
for ((i = 0; i < ranks; i++)); do
  pin=()
  if ((${#cpus[@]} > 0)); then pin=(taskset -c "${cpus[$i]}"); fi
  {
    echo "LOAD '$build/extension/sirius/sirius.duckdb_extension';"
    # A rank falling back to the CPU alone would return a wrong answer: fail instead.
    echo "SET enable_duckdb_fallback = false;"
    if [ -n "${THREADS:-}" ]; then echo "SET threads = $THREADS;"; fi
    if ((ranks > 1)); then
      echo "SET sirius_exchange_peers = '$peers';"
      echo "SET sirius_exchange_rank = $i;"
    fi
    if [ -n "${PREAMBLE:-}" ]; then cat "$PREAMBLE"; fi
    cat "$sql"
  } | CUDA_VISIBLE_DEVICES=${gpus[$i]} "${pin[@]}" timeout -s KILL "${EXCHANGE_TIMEOUT:-1800}" \
    "$build/duckdb" -unsigned > "$logs/exchange_rank_$i.log" 2>&1 &
  pids+=($!)
done
status=0
for ((i = 0; i < ranks; i++)); do
  if ! wait "${pids[$i]}"; then status=1; echo "rank $i failed: $logs/exchange_rank_$i.log"; fi
done
grep -v -E 'no version information' "$logs/exchange_rank_0.log"
exit $status
