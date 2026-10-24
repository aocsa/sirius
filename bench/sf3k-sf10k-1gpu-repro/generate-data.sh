#!/usr/bin/env bash
# Generate TPC-H SF3000/SF10000 parquet byte-identical to the campaign datasets, then verify every file
# (size, rows, row groups, S3-style ETag) against data/generation-sf*.jsonl.
#   bash generate-data.sh /opt/dlami/nvme            # both scales
#   bash generate-data.sh /opt/dlami/nvme 3000       # one scale
# Output: <root>/tpch_sf<scale>_regenerated/<table>/part.N.parquet
# Needs: git, a C toolchain, python3-venv, Rust via rustup (the generator pins 1.89.0).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=${1:?usage: generate-data.sh <output-root> [3000] [10000]}
shift
(( $# )) || set -- 3000 10000
gen=${TPCHGEN_DIR:-$HOME/tpchgen-rs}
venv=${TPCHGEN_VENV:-$HOME/tpchgen-venv}
commit=cdcf74def0072f94bf1886667e8d2ac51feb8721
jobs=${JOBS:-48}
export PATH="$HOME/.cargo/bin:$PATH"

[[ -d $gen ]] || git clone https://github.com/sirius-db/tpchgen-rs.git "$gen"
git -C "$gen" fetch -q origin "$commit" 2>/dev/null || true
git -C "$gen" checkout -q --detach "$commit"
command -v cargo >/dev/null || { echo 'Install Rust first: curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain 1.89.0' >&2; exit 1; }
(cd "$gen" && RUSTFLAGS='-C target-cpu=native' cargo build --release -p tpchgen-cli)
[[ -x $venv/bin/python ]] || { python3 -m venv "$venv" && "$venv/bin/pip" install -q pyarrow; }

for sf in "$@"; do
  out=$root/tpch_sf${sf}_regenerated
  [[ -e $out ]] && { echo "$out exists; remove it or pick another root" >&2; exit 1; }
  echo "=== SF$sf generate $(date -u +%FT%TZ)"
  "$venv/bin/python" -I "$gen/scripts/generate_tpch.py" -s "$sf" -o "$out" -j "$jobs"
  echo "=== SF$sf verify $(date -u +%FT%TZ)"
  "$venv/bin/python" -I "$here/tools/verify_data_local.py" "$here/data/generation-sf$sf.jsonl" "$out"
done
