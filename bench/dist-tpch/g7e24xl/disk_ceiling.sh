#!/usr/bin/env bash
# Raw O_DIRECT read throughput of the data array: N parallel dd readers over distinct lineitem files.
# usage: disk_ceiling.sh [DATA_DIR]   (needs passwordless sudo to drop the page cache)
D=${1:-/opt/dlami/nvme/tpch_sf3000_regenerated}/lineitem
for n in 4 16 48; do
  sync; sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  files=$(ls $D/*.parquet | head -$n)
  bytes=$(du -cb $files | tail -1 | cut -f1)
  s=$(date +%s.%N)
  for f in $files; do dd if=$f of=/dev/null bs=16M iflag=direct status=none & done; wait
  e=$(date +%s.%N)
  python3 -c "print(f'readers=$n bytes={$bytes/1e9:.1f}GB time={$e-$s:.1f}s rate={$bytes/1e9/($e-$s):.2f}GB/s')"
done
