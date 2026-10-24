#!/usr/bin/env bash
# Run one heavy step under the shared GPU lock, only once the GPUs are idle and enough CPU
# memory is free. If they are not, release the lock, wait, and try again.
# usage: gpu_step.sh MIN_AVAIL_GIB command [args...]   (run from the repo root)
# GPU_LOCK names the lock file shared by everyone benchmarking on the host (default below).
minmem=$1; shift
memtries=0
while true; do
  flock "${GPU_LOCK:-/home/ubuntu/.gpu-bench.lock}" bash -c '
    minmem=$1; shift
    busy_pids=$(nvidia-smi --query-compute-apps=pid --format=csv,noheader)
    if pgrep -x duckdb >/dev/null || [ -n "$busy_pids" ]; then
      echo "$(date +%T) lock held but GPUs busy (duckdb: $(pgrep -x duckdb | tr "\n" " ") apps: $busy_pids); releasing"; exit 75
    fi
    avail=$(awk "/MemAvailable/{print int(\$2/1048576)}" /proc/meminfo)
    if [ "$avail" -lt "$minmem" ]; then echo "$(date +%T) only $avail GiB available, need $minmem; releasing"; exit 76; fi
    echo "$(date +%T) lock held, GPUs idle, $avail GiB available: $*"
    nvidia-smi --query-gpu=index,memory.used,utilization.gpu --format=csv,noheader
    "$@"
    rc=$?; echo "$(date +%T) step exit $rc: $*"; exit $rc
  ' _ "$minmem" "$@"
  rc=$?
  if [ $rc -eq 76 ]; then memtries=$((memtries + 1)); [ $memtries -ge 15 ] && { echo "giving up: memory never freed"; exit 76; }; fi
  if [ $rc -ne 75 ] && [ $rc -ne 76 ]; then exit $rc; fi
  sleep 120
done
