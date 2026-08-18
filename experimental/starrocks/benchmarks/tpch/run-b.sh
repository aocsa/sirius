#!/usr/bin/env bash
#
# run-b.sh -- TPC-H sweep for engine B only (stock StarRocks CPU backends).
#
#   ./run-b.sh --sf 100
#   ./run-b.sh --sf 500 --bes 2 --runs 3
#   ./run-b.sh --sf 100 --bes 4 --setup          # 4-BE sensitivity; re-lays out confs first
#   ./run-b.sh --sf 1 --queries "q01 q06" --dry-run
#
# This is a thin wrapper around run-abc.sh --engines B. All timing, classification,
# provenance, and teardown live there. Engine B has no Sirius CNs: the parallelism
# knob is BE count (--bes), not NUM_CNS.
#
# Engine A and engine B share port 9030 and the host CPUs. Never run both.
#
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SR_DIR=$(cd "$HERE/../.." && pwd)
SETUP=$SR_DIR/configs/gb200-4gpu/engine-b/setup-engine-b-gb200.sh
ABC=$HERE/run-abc.sh

SF=""
BES=${B_NUM_BES:-2}
RUNS=3
QUERIES=""
OUT=""
DATA=""
SETUP_FIRST=0
DRY_RUN=0
EXTRA=()

usage() {
cat <<'EOF'
usage: run-b.sh --sf <N> [--bes 2|4] [--runs 3] [--queries "q01 q06"] [--out DIR] [--setup]

Engine B only: stock StarRocks 3.5.20, CPU backends, same TPC-H parquet as engine A.
Delegates to run-abc.sh --engines B.

Required:
  --sf N              scale factor. Finds tpch_parquet_sf<N> under TPCH_DATA_ROOTS
                      (default search: /raid first, then $HOME/aocsa). Timeouts scale
                      with SF: warm = max(90, 1.8*SF) s, cold = max(300, 6*SF) s.

Topology (BEs, not CNs):
  --bes 2|4           number of StarRocks backends (default 2). Engine B does not use
                      Sirius CNs. 2 = one BE per Grace socket (headline). 4 = half-socket
                      sensitivity; requires --setup the first time so be3/be4 confs exist.
  --setup             run setup-engine-b-gb200.sh with NUM_BES=--bes before the sweep.
                      Idempotent layout: trees, /raid data dirs, committed confs.
                      Starts nothing itself.

Sweep:
  --runs N            timed warm runs per query (default 3). Run 0 is an extra cold run.
  --queries LIST      subset: "q01 q06", "q01,q06", or "1,6" (default: all 22).
  --out DIR           result directory (default: $ABC_OUT_ROOT/abc-sf<N>-<UTC>).
  --data PATH         dataset override (default: search for tpch_parquet_sf<N>).
  --dry-run           resolve everything, print commands, touch no cluster.

Environment (optional):
  B_DIR               layout dir (default $HOME/starrocks-bench)
  B_DATA_ROOT         local-disk data root (default /raid/prestouser/sr-bench)
  JAVA_HOME           default /usr/lib/jvm/java-21-openjdk-arm64
  TPCH_DATA_ROOTS     colon-separated search roots for tpch_parquet_sf<N>
  ABC_OUT_ROOT        default parent of --out

Examples:
  ./run-b.sh --sf 100
  ./run-b.sh --sf 500 --bes 2 --out $HOME/aocsa/benchmark-results/b-sf500
  ./run-b.sh --sf 100 --bes 4 --setup
  ./run-b.sh --sf 1000 --bes 2 --queries q01,q06,q14

SF1000 memory: the committed 2-BE confs are sized for SF100 (mem_limit=240G). Before
an SF1000 sweep, set mem_limit=224G (and datacache_disk_size=200G) on both BEs; for
4-BE use 112G. See configs/gb200-4gpu/engine-b/README.md.
EOF
}

while [ $# -gt 0 ]; do
  case $1 in
    --sf)       SF=${2:?--sf needs a value}; shift 2 ;;
    --bes)      BES=${2:?--bes needs 2 or 4}; shift 2 ;;
    --runs)     RUNS=${2:?--runs needs a value}; shift 2 ;;
    --queries)  QUERIES=${2:?--queries needs a value}; shift 2 ;;
    --out)      OUT=${2:?--out needs a value}; shift 2 ;;
    --data)     DATA=${2:?--data needs a value}; shift 2 ;;
    --setup)    SETUP_FIRST=1; shift ;;
    --dry-run)  DRY_RUN=1; EXTRA+=(--dry-run); shift ;;
    -h|--help)  usage; exit 0 ;;
    *)          usage >&2; echo "run-b: unknown argument: $1" >&2; exit 2 ;;
  esac
done

[ -n "$SF" ] || { usage >&2; echo "run-b: --sf is required" >&2; exit 2; }
case "$BES" in
  2|4) ;;
  *) echo "run-b: --bes must be 2 (headline) or 4 (sensitivity), got '$BES'" >&2; exit 2 ;;
esac
[ -x "$ABC" ] || { echo "run-b: missing $ABC" >&2; exit 2; }

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-arm64}
export B_NUM_BES=$BES
export PATH=${JAVA_HOME}/bin:${SR_DIR}/.pixi/envs/default/bin:${PATH}

if [ "$SETUP_FIRST" = 1 ]; then
  [ -x "$SETUP" ] || { echo "run-b: missing $SETUP" >&2; exit 2; }
  echo "run-b: laying out engine B with NUM_BES=$BES (starts nothing)"
  if [ "$DRY_RUN" = 1 ]; then
    DRY_RUN=1 NUM_BES=$BES "$SETUP"
  else
    NUM_BES=$BES "$SETUP"
  fi
fi

cmd=("$ABC" --sf "$SF" --engines B --runs "$RUNS")
[ -n "$QUERIES" ] && cmd+=(--queries "$QUERIES")
[ -n "$OUT" ]     && cmd+=(--out "$OUT")
[ -n "$DATA" ]    && cmd+=(--data "$DATA")
cmd+=("${EXTRA[@]+"${EXTRA[@]}"}")

echo "run-b: SF$SF  BEs=$BES  runs=1 cold + $RUNS warm"
echo "  JAVA_HOME=$JAVA_HOME"
echo "  B_DIR=${B_DIR:-$HOME/starrocks-bench}"
echo "  exec: $(printf '%q ' "${cmd[@]}")"
exec "${cmd[@]}"
