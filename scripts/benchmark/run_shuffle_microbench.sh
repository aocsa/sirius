#!/usr/bin/env bash
# Copyright 2026, Sirius Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the LICENSE file at the repo root for the full text.
#
# Run layer-1 nvbandwidth (if present) then sirius_shuffle_benchmark, and
# print interconnect vs Sirius transfer vs shuffle GB/s.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_shuffle_microbench.sh [--output-dir DIR] [--sizes SPEC] [--reps N]

Runs scripts/benchmark/run_nvbandwidth.sh then sirius_shuffle_benchmark, and
prints a short comparison of nvbandwidth D2D vs Sirius clone_to vs shuffle.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/build/benchmark-results"
SIZES="1MiB,16MiB,256MiB,1GiB"
REPS=8
WARMUP=2

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --sizes)
      SIZES="$2"
      shift 2
      ;;
    --reps)
      REPS="$2"
      shift 2
      ;;
    --warmup)
      WARMUP="$2"
      shift 2
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

echo "======== Layer 1: interconnect (nvbandwidth) ========"
# Convert 1MiB,16MiB,... to nvbandwidth MiB integers (best-effort).
NVBW_SIZES="$(
  python3 - "$SIZES" <<'PY'
import re, sys
out = []
for part in sys.argv[1].split(","):
    part = part.strip()
    m = re.match(r"^([0-9.]+)\s*(.*)$", part)
    if not m:
        continue
    val = float(m.group(1))
    unit = m.group(2).strip().upper()
    if unit in ("G", "GB", "GIB"):
        mib = int(round(val * 1024))
    elif unit in ("K", "KB", "KIB"):
        mib = max(1, int(round(val / 1024.0)))
    elif unit in ("", "B"):
        mib = max(1, int(round(val / (1024.0 * 1024.0))))
    else:
        mib = max(1, int(round(val)))
    out.append(str(mib))
print(",".join(out) if out else "1,16,256,1024")
PY
)"

"${SCRIPT_DIR}/run_nvbandwidth.sh" --output-dir "${OUTPUT_DIR}" --sizes "${NVBW_SIZES}"

find_bench() {
  if [[ -n "${SIRIUS_SHUFFLE_BENCH:-}" && -x "${SIRIUS_SHUFFLE_BENCH}" ]]; then
    printf '%s\n' "${SIRIUS_SHUFFLE_BENCH}"
    return 0
  fi
  local c
  for c in \
    "${REPO_ROOT}/build/release/extension/sirius/test/cpp/sirius_shuffle_benchmark" \
    "${REPO_ROOT}/build/release/test/cpp/sirius_shuffle_benchmark" \
    "${REPO_ROOT}/build/relwithdebinfo/extension/sirius/test/cpp/sirius_shuffle_benchmark" \
    "${REPO_ROOT}/build/clang-relwithdebinfo/extension/sirius/test/cpp/sirius_shuffle_benchmark" \
    "${REPO_ROOT}/build/clang-release/extension/sirius/test/cpp/sirius_shuffle_benchmark"; do
    if [[ -x "$c" ]]; then
      printf '%s\n' "$c"
      return 0
    fi
  done
  return 1
}

echo
echo "======== Layer 2: Sirius shuffle primitives ========"
if ! BENCH="$(find_bench)"; then
  cat <<EOF
sirius_shuffle_benchmark not found. Build it with:

  pixi run make sirius_shuffle_benchmark

Expected path:
  ${REPO_ROOT}/build/release/extension/sirius/test/cpp/sirius_shuffle_benchmark

Override with SIRIUS_SHUFFLE_BENCH=/path/to/sirius_shuffle_benchmark
EOF
  exit 1
fi

SHUFFLE_CSV="${OUTPUT_DIR}/sirius_shuffle.csv"
echo "using ${BENCH}"
"${BENCH}" --mode all --sizes "${SIZES}" --reps "${REPS}" --warmup "${WARMUP}" --csv "${SHUFFLE_CSV}"

echo
echo "Wrote Sirius CSV to ${SHUFFLE_CSV}"

python3 - "${OUTPUT_DIR}" "${SHUFFLE_CSV}" <<'PY'
import csv
import glob
import os
import sys

out_dir, shuffle_csv = sys.argv[1], sys.argv[2]

def latest(pattern):
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None

nvbw_path = latest(os.path.join(out_dir, "nvbandwidth-*.csv"))

def load_nvbw(path):
    # size_mib,testcase,src,dst,bandwidth_GBps,file
    rows = []
    if not path:
        return rows
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                row["size_mib"] = int(float(row["size_mib"]))
                row["bandwidth_GBps"] = float(row["bandwidth_GBps"])
            except (KeyError, ValueError):
                continue
            rows.append(row)
    return rows

def load_sirius(path):
    rows = []
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                row["size_bytes"] = int(float(row["size_bytes"]))
                for k in ("effective_GBps", "transfer_GBps", "partition_ms", "transfer_ms", "shuffle_ms"):
                    row[k] = float(row.get(k) or 0)
            except (KeyError, ValueError):
                continue
            rows.append(row)
    return rows

nvbw = load_nvbw(nvbw_path)
sirius = load_sirius(shuffle_csv)

print()
print("======== Comparison (median-like CSV rows; nvbandwidth may list both matrix entries) ========")
if nvbw_path:
    print(f"nvbandwidth CSV: {nvbw_path}")
else:
    print("nvbandwidth CSV: (missing — tool not installed or JSON parse found no matrix)")
print(f"Sirius CSV:      {shuffle_csv}")
print()
print(f"{'size':>10}  {'nvbw D2D 0->1':>14}  {'nvbw D2D 1->0':>14}  {'nvbw bidir':>12}  {'sirius xfer bidir':>18}  {'sirius shuffle':>14}  {'copy_path':>12}")

def nvbw_pick(size_mib, pred):
    vals = [r["bandwidth_GBps"] for r in nvbw if r["size_mib"] == size_mib and pred(r)]
    if not vals:
        return None
    vals.sort()
    return vals[len(vals) // 2]

sizes = sorted({r["size_bytes"] for r in sirius})
for size_bytes in sizes:
    size_mib = max(1, int(round(size_bytes / (1024 * 1024))))
    def is_d2d(r):
        return "device_to_device" in r.get("testcase", "") and "bidirectional" not in r.get("testcase", "")
    def is_bidir(r):
        return "bidirectional" in r.get("testcase", "")
    d01 = nvbw_pick(size_mib, lambda r: is_d2d(r) and r.get("src") == "GPU0" and r.get("dst") == "GPU1")
    d10 = nvbw_pick(size_mib, lambda r: is_d2d(r) and r.get("src") == "GPU1" and r.get("dst") == "GPU0")
    bid = nvbw_pick(size_mib, is_bidir)
    xfer = next((r for r in sirius if r["size_bytes"] == size_bytes and r.get("mode") == "transfer_bidir"), None)
    shuf = next((r for r in sirius if r["size_bytes"] == size_bytes and r.get("mode") == "shuffle"), None)
    def fmt(v):
        return f"{v:14.3f}" if v is not None else f"{'n/a':>14}"
    copy_path = (xfer or shuf or {}).get("copy_path", "")
    xfer_v = xfer["transfer_GBps"] if xfer else None
    shuf_v = shuf["effective_GBps"] if shuf else None
    print(
        f"{size_mib:>8}MiB  {fmt(d01)}  {fmt(d10)}  {fmt(bid):>12}  {fmt(xfer_v):>18}  {fmt(shuf_v):>14}  {copy_path:>12}"
    )

print()
print("If shuffle effective GB/s << Sirius transfer GB/s, partitioning (hash + gather) dominates.")
print("If Sirius transfer << nvbandwidth D2D, convert_gpu_to_gpu / host-staging / column-wise copies dominate.")
print("If nvbandwidth D2D is far below PCIe Gen5 x16 (~64 GB/s unidirectional theoretical), check pcie_gen and P8 idle.")
PY
