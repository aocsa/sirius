#!/usr/bin/env bash
# Copyright 2026, Sirius Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the LICENSE file at the repo root for the full text.
#
# Layer-1 interconnect baseline: topology, PCIe warmup, nvbandwidth size sweep.
# Does not treat cudaDeviceCanAccessPeer as proof of peer DMA.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_nvbandwidth.sh [--output-dir DIR] [--sizes MIB[,MIB...]] [--samples N]

Print GPU topology and P2P capability, warm the devices out of idle P8 so the
PCIe link can train, then run nvbandwidth if it is on PATH.

Sizes are nvbandwidth --bufferSize values in MiB (default: 1,16,256,1024).
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/build/benchmark-results"
SIZES_MIB="1,16,256,1024"
SAMPLES=3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --sizes)
      SIZES_MIB="$2"
      shift 2
      ;;
    --samples)
      SAMPLES="$2"
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
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
TOPO_LOG="${OUTPUT_DIR}/nvidia-smi-topo-${STAMP}.txt"
NVBW_CSV="${OUTPUT_DIR}/nvbandwidth-${STAMP}.csv"
NVBW_RAW_DIR="${OUTPUT_DIR}/nvbandwidth-raw-${STAMP}"
mkdir -p "${NVBW_RAW_DIR}"

echo "=== GPU inventory ==="
nvidia-smi --query-gpu=index,name,memory.total,pstate,pcie.link.gen.current,pcie.link.gen.max,pcie.link.width.current,pcie.link.width.max --format=csv
echo

{
  echo "=== nvidia-smi topo -m ==="
  nvidia-smi topo -m || true
  echo
  echo "=== P2P reads (-p2p r) ==="
  nvidia-smi topo -p2p r || true
  echo
  echo "=== P2P writes (-p2p w) ==="
  nvidia-smi topo -p2p w || true
  echo
  echo "=== P2P native (-p2p n) ==="
  nvidia-smi topo -p2p n || true
  echo
  echo "NOTE: cudaDeviceCanAccessPeer==1 is not proof of peer DMA."
  echo "Sirius uses cucascade::memory::probe_peer_dma_works after enable-peer-access."
} | tee "${TOPO_LOG}"

echo
echo "=== PCIe link before warmup ==="
nvidia-smi --query-gpu=index,pstate,pcie.link.gen.current,pcie.link.gen.max,power.draw --format=csv

warmup_gpus() {
  python3 - <<'PY'
import ctypes
import ctypes.util
import glob
import os
import sys

candidates = []
lib = ctypes.util.find_library("cudart")
if lib:
    candidates.append(lib)
prefix = os.environ.get("CONDA_PREFIX", "")
if prefix:
    candidates.extend(glob.glob(prefix + "/targets/*/lib/libcudart.so*"))
    candidates.extend(glob.glob(prefix + "/lib/libcudart.so*"))
candidates.extend(glob.glob("/usr/local/cuda*/targets/*/lib/libcudart.so"))
candidates.extend(glob.glob("/usr/local/cuda/lib64/libcudart.so"))

seen = set()
ordered = []
for p in candidates:
    if p and p not in seen and os.path.exists(p):
        seen.add(p)
        ordered.append(p)

err = None
for path in ordered:
    try:
        rt = ctypes.CDLL(path)
    except OSError as e:
        err = e
        continue

    def chk(rc, what):
        if rc != 0:
            raise RuntimeError(f"{what} failed rc={rc} via {path}")

    n = ctypes.c_int()
    chk(rt.cudaGetDeviceCount(ctypes.byref(n)), "cudaGetDeviceCount")
    if n.value < 1:
        print("no CUDA devices; skip warmup", file=sys.stderr)
        sys.exit(0)
    nbytes = 64 * 1024 * 1024
    for i in range(n.value):
        chk(rt.cudaSetDevice(ctypes.c_int(i)), f"cudaSetDevice({i})")
        ptr = ctypes.c_void_p()
        chk(rt.cudaMalloc(ctypes.byref(ptr), ctypes.c_size_t(nbytes)), f"cudaMalloc gpu{i}")
        chk(rt.cudaMemset(ptr, ctypes.c_int(0xA5), ctypes.c_size_t(nbytes)), f"cudaMemset gpu{i}")
        chk(rt.cudaDeviceSynchronize(), f"cudaDeviceSynchronize gpu{i}")
        chk(rt.cudaFree(ptr), f"cudaFree gpu{i}")
        if n.value >= 2:
            j = 0 if i == 1 else 1
            # Best-effort peer copy to train the PIX link; ignore failure.
            rt.cudaSetDevice(ctypes.c_int(i))
            src = ctypes.c_void_p()
            dst = ctypes.c_void_p()
            if rt.cudaMalloc(ctypes.byref(src), ctypes.c_size_t(nbytes)) == 0:
                rt.cudaSetDevice(ctypes.c_int(j))
                if rt.cudaMalloc(ctypes.byref(dst), ctypes.c_size_t(nbytes)) == 0:
                    rt.cudaMemcpyPeer(dst, ctypes.c_int(j), src, ctypes.c_int(i), ctypes.c_size_t(nbytes))
                    rt.cudaSetDevice(ctypes.c_int(j))
                    rt.cudaDeviceSynchronize()
                    rt.cudaFree(dst)
                rt.cudaSetDevice(ctypes.c_int(i))
                rt.cudaFree(src)
    print(f"warmed {n.value} GPU(s) via {path}", file=sys.stderr)
    sys.exit(0)

print("could not load libcudart; PCIe may stay at Gen1 until the first real copy", file=sys.stderr)
sys.exit(0)
PY
}

echo
echo "=== Warming GPUs ==="
warmup_gpus || true

echo
echo "=== PCIe link after warmup ==="
nvidia-smi --query-gpu=index,pstate,pcie.link.gen.current,pcie.link.gen.max,power.draw --format=csv | tee -a "${TOPO_LOG}"

if ! command -v nvbandwidth >/dev/null 2>&1; then
  cat <<'EOF'

nvbandwidth is not on PATH. Skipping the interconnect size sweep (exit 0).

Install from https://github.com/NVIDIA/nvbandwidth (do not vendor it into Sirius):

  git clone https://github.com/NVIDIA/nvbandwidth.git
  cmake -S nvbandwidth -B nvbandwidth/build
  cmake --build nvbandwidth/build -j
  export PATH="$PWD/nvbandwidth/build:$PATH"

Then re-run this script. cudaDeviceCanAccessPeer==1 is still not proof of peer DMA;
compare these numbers to sirius_shuffle_benchmark copy_path=peer_dma|host_staging.
EOF
  exit 0
fi

HELP_TEXT="$(nvbandwidth -h 2>&1 || true)"
FMT_ARGS=()
if grep -q -- '--format' <<<"${HELP_TEXT}"; then
  FMT_ARGS=(-F json)
elif grep -q -- '-j,' <<<"${HELP_TEXT}" || grep -q -- '-j ' <<<"${HELP_TEXT}"; then
  FMT_ARGS=(-j)
fi

PREFERRED_TESTS=(
  device_to_device_memcpy_read_ce
  device_to_device_bidirectional_memcpy_ce
  host_to_device_memcpy_ce
  device_to_host_memcpy_ce
)

LIST_TEXT="$(nvbandwidth -l 2>&1 || true)"
TESTS=()
for t in "${PREFERRED_TESTS[@]}"; do
  if grep -Fq "$t" <<<"${LIST_TEXT}"; then
    TESTS+=("$t")
  fi
done
if [[ ${#TESTS[@]} -eq 0 ]]; then
  echo "nvbandwidth -l did not list expected CE memcpy tests; running prefix device_to_device" >&2
  TESTS=()
  PREFIX_ARGS=(-p device_to_device)
else
  PREFIX_ARGS=()
fi

echo "size_mib,testcase,src,dst,bandwidth_GBps,file" >"${NVBW_CSV}"

parse_json() {
  local json_file="$1"
  local size_mib="$2"
  python3 - "$json_file" "$size_mib" "${NVBW_CSV}" <<'PY'
import json, sys, os
path, size_mib, csv_path = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    with open(path) as f:
        data = json.load(f)
except Exception:
    sys.exit(0)

def emit(testcase, src, dst, bw):
    with open(csv_path, "a") as out:
        out.write(f"{size_mib},{testcase},{src},{dst},{bw},{os.path.basename(path)}\n")

def walk(obj, name=""):
    if isinstance(obj, dict):
        tname = obj.get("name") or obj.get("testcase") or name
        # Common layouts: nested "bandwidths" / "results" matrices
        matrix = obj.get("bandwidths") or obj.get("bandwidth") or obj.get("results")
        if isinstance(matrix, list) and matrix and isinstance(matrix[0], list):
            for i, row in enumerate(matrix):
                for j, val in enumerate(row):
                    if i == j:
                        continue
                    try:
                        bw = float(val)
                    except (TypeError, ValueError):
                        continue
                    if bw != bw:  # NaN
                        continue
                    emit(tname or "unknown", f"GPU{i}", f"GPU{j}", bw)
        for k, v in obj.items():
            walk(v, tname or k)
    elif isinstance(obj, list):
        for item in obj:
            walk(item, name)

walk(data)
PY
}

IFS=',' read -r -a SIZE_ARR <<<"${SIZES_MIB}"
echo
echo "=== nvbandwidth size sweep (MiB: ${SIZES_MIB}) ==="
for size in "${SIZE_ARR[@]}"; do
  size="${size// /}"
  [[ -n "$size" ]] || continue
  if [[ ${#TESTS[@]} -gt 0 ]]; then
    for t in "${TESTS[@]}"; do
      raw="${NVBW_RAW_DIR}/${t}-b${size}.out"
      echo "running nvbandwidth -t ${t} -b ${size} -i ${SAMPLES} ${FMT_ARGS[*]:-}"
      # nvbandwidth returns non-zero on some platforms when a testcase is unsupported.
      set +e
      nvbandwidth -t "$t" -b "$size" -i "$SAMPLES" "${FMT_ARGS[@]}" | tee "$raw"
      rc=$?
      set -e
      if [[ $rc -ne 0 ]]; then
        echo "nvbandwidth ${t} size=${size} exited ${rc} (recorded raw output)" >&2
      fi
      parse_json "$raw" "$size" || true
    done
  else
    raw="${NVBW_RAW_DIR}/prefix-device_to_device-b${size}.out"
    echo "running nvbandwidth ${PREFIX_ARGS[*]} -b ${size} -i ${SAMPLES} ${FMT_ARGS[*]:-}"
    set +e
    nvbandwidth "${PREFIX_ARGS[@]}" -b "$size" -i "$SAMPLES" "${FMT_ARGS[@]}" | tee "$raw"
    rc=$?
    set -e
    if [[ $rc -ne 0 ]]; then
      echo "nvbandwidth prefix run size=${size} exited ${rc}" >&2
    fi
    parse_json "$raw" "$size" || true
  fi
done

echo
echo "Wrote topology to ${TOPO_LOG}"
echo "Wrote nvbandwidth CSV to ${NVBW_CSV}"
echo "Raw outputs in ${NVBW_RAW_DIR}"
echo "NVBW_CSV=${NVBW_CSV}"
