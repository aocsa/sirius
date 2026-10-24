#!/usr/bin/env bash
# Run the reference 1-GPU suites serially, each followed by validation and a timing summary.
#   bash run-suite.sh sf3k     # SF3000 cold, lukewarm, hot (all 22 queries)
#   bash run-suite.sh sf10k    # SF10000 cold Q4, Q9, Q17, Q21
#   bash run-suite.sh all
# Environment: DATA_ROOT (default /opt/dlami/nvme), OUT (default ~/sirius-results),
#   GPU (default 0), CPUS (default 0-15,48-63, i.e. 32 vCPUs like a g7e.8xlarge; set empty to disable).
# Run from the repository root through pixi, e.g. `pixi run bash bench/sf3k-sf10k-1gpu-repro/run-suite.sh all`.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
what=${1:?usage: run-suite.sh sf3k|sf10k|all}
data_root=${DATA_ROOT:-/opt/dlami/nvme}
out_root=${OUT:-$HOME/sirius-results}
gpu=${GPU:-0}
cpus=${CPUS-0-15,48-63}
stamp=$(date -u +%Y%m%dT%H%M%SZ)
status=0

run() {  # profile mode
  local profile=$1 mode=$2 scale out
  scale=$([[ $profile == 1gpu-sf3k ]] && echo 3000 || echo 10000)
  out=$out_root/$stamp-$profile-$mode
  # Cold means cold storage too: drop the OS page cache when passwordless sudo allows it.
  sync; sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null || echo 'warning: page cache not dropped'
  echo "=== $profile $mode start $(date -u +%FT%TZ)"
  python "$here/run.py" "$profile" --mode "$mode" --source "$data_root/tpch_sf${scale}_regenerated" \
    --gpus "$gpu" ${cpus:+--cpus "$cpus"} --output "$out" > "$out.log" 2>&1 || { echo "run failed: $out.log"; status=1; return; }
  python "$here/tools/validate.py" --profile "$profile" --output "$out" || status=1
  python "$here/tools/summarize.py" "$out" | tee "$out-summary.md"
}

case $what in
  sf3k) for m in cold lukewarm hot; do run 1gpu-sf3k $m; done ;;
  sf10k) run 1gpu-sf10k cold ;;
  all) for m in cold lukewarm hot; do run 1gpu-sf3k $m; done; run 1gpu-sf10k cold ;;
  *) echo 'usage: run-suite.sh sf3k|sf10k|all' >&2; exit 2 ;;
esac
exit $status
