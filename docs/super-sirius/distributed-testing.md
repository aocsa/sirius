# Testing distributed Sirius on new machines

How to bring up and test [distributed Sirius](distributed-sirius.md) and the
[gateway](distributed-gateway.md) in two setups:

- **[One box, several GPUs](#one-box-several-gpus):** one rank per GPU, driven by the gateway or by
  `scripts/distributed_run_sql.sh`.
- **[Several boxes, one GPU each](#several-boxes-one-gpu-each):** one rank per box, on one network.

| | One box, several GPUs | Several boxes, one GPU each |
|---|---|---|
| Distributed Sirius (NIXL exchange) | Tested on 4 × GB200 | Untested. Nothing in the code is tied to one host, but every run so far used one |
| Gateway | Built; tested with CPU ranks only | Not supported yet: the gateway starts its ranks itself, on its own host (see [What the gateway needs](#what-the-gateway-needs-for-several-boxes)) |

## Contents

- [Prepare each box](#prepare-each-box)
- [Test data](#test-data)
- [One box, several GPUs](#one-box-several-gpus)
- [Several boxes, one GPU each](#several-boxes-one-gpu-each)
- [Troubleshooting](#troubleshooting)

## Prepare each box

Every box needs the same commit, built the same way. Each rank plans the query itself, and the
ranks compare a hash of their plans before running, so different builds fail every query.

### Requirements

- The [Sirius requirements](../README.md#requirements): an NVIDIA GPU (Turing or newer), CUDA 12 or
  13 and its driver, `io_uring`, and [Pixi](https://pixi.sh/latest/installation/).
- Autotools (for UCX), and `meson` and `ninja` (for NIXL).
- For several boxes: a network between them. RDMA (InfiniBand or RoCE) is optional; without it,
  UCX uses TCP.

### Source

```bash
git clone --no-recurse-submodules https://github.com/aocsa/sirius.git
cd sirius
git checkout gateway/simple-gateway
git submodule update --init --depth=1 --jobs 5 duckdb substrait cucascade sirius-duckdb/duckdb sirius-duckdb/extension-ci-tools
```

### UCX and NIXL

Sirius links an out-of-tree NIXL install, which brings its own UCX. `scripts/nixl_env.sh` expects
them in `~/tools/nvda_nixl` and `~/tools/ucx-install` (override with `NIXL_PREFIX` and
`UCX_PREFIX`). Build them following [UCX's](https://github.com/openucx/ucx) and
[NIXL's](https://github.com/ai-dynamo/nixl) own instructions; roughly:

```bash
export CUDA_HOME=/usr/local/cuda
git clone https://github.com/openucx/ucx.git && cd ucx    # a release tag NIXL supports
./autogen.sh
./contrib/configure-release --prefix=$HOME/tools/ucx-install --with-cuda=$CUDA_HOME --enable-mt
# with RDMA NICs, add: --with-verbs --with-rdmacm
make -j && make install && cd ..

git clone https://github.com/ai-dynamo/nixl.git && cd nixl
meson setup build --prefix=$HOME/tools/nvda_nixl -Ducx_path=$HOME/tools/ucx-install
ninja -C build install && cd ..
```

Check that `~/tools/nvda_nixl/lib/<arch>-linux-gnu/plugins` contains the UCX plugin.

### Build

```bash
# Sirius with the NIXL fabric, and the rank DuckDB (sirius-duckdb/build/release/duckdb)
pixi run cmake --preset release -DSIRIUS_DUCKDB_SOURCE_DIR=$PWD/duckdb \
  -DSIRIUS_WITH_NIXL=ON -DSIRIUS_NIXL_PREFIX=$HOME/tools/nvda_nixl
pixi run make

# The gateway: a CPU-only DuckDB with httpfs, quack and sirius_gateway (gateway/build/release/duckdb)
pixi run make -C gateway release
```

The gateway build fetches the `httpfs` and `quack` sources, so it needs internet access.

Then check that the rank DuckDB links Quack. The gateway needs it on the ranks, and this part of
the build has not been run yet:

```bash
sirius-duckdb/build/release/duckdb -c "SELECT extension_name, loaded FROM duckdb_extensions() WHERE extension_name IN ('quack', 'httpfs')"
```

If `quack` is not loaded, or `quack_serve` fails with a crypto error, add `httpfs` to
`sirius-duckdb/extension_config.cmake` the way `gateway/extension_config.cmake` does.

## Test data

Generate TPC-H as Parquet with the Python DuckDB in the Pixi environment. SF10 is about 10 GB;
use SF100 or more to keep several GPUs busy.

```bash
mkdir -p /data/tpch_sf10
pixi run python - <<'PY'
import duckdb
con = duckdb.connect()
con.execute("INSTALL tpch; LOAD tpch; CALL dbgen(sf=10)")
for t in ["lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"]:
    con.execute(f"COPY {t} TO '/data/tpch_sf10/{t}' (FORMAT parquet, PER_THREAD_OUTPUT)")
PY
```

Put the data on a filesystem that supports `O_DIRECT`. On several boxes, every box needs **all**
the files at the same path, through a shared filesystem or a copy. Each rank reads only its share
of the bytes, but every rank binds every file so that they all plan alike.

Write TPC-H Q12 to `/data/q12.sql`, the query [distributed-sirius.md](distributed-sirius.md#running-tpc-h-q12-on-4-gpus)
uses:

```bash
cat > /data/q12.sql <<'SQL'
create view lineitem as select * from read_parquet('/data/tpch_sf10/lineitem/*.parquet');
create view orders as select * from read_parquet('/data/tpch_sf10/orders/*.parquet');
.timer on
select l_shipmode,
  sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
  sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
from orders, lineitem
where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate and l_shipdate < l_commitdate
  and l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'
group by l_shipmode order by l_shipmode;
SQL
```

Get the expected answer from DuckDB on the CPU:

```bash
pixi run python -c "import duckdb; print(duckdb.sql(open('/data/q12.sql').read().replace('.timer on', '')).fetchall())"
```

## One box, several GPUs

### 1. Look at the GPUs

```bash
nvidia-smi -L
nvidia-smi topo -m        # NVLink (NV#) or PCIe paths between GPUs
nvidia-smi topo -p2p r    # OK: peer reads work; NS: none, so the exchange stages through host memory
```

GPUs without peer access still work: the exchange copies through pinned shared memory instead
(see "Host staging" in [distributed-sirius.md](distributed-sirius.md#nixl-fabric-srcexchangenixl_fabric)).
Each rank logs its path to every peer when it starts.

### 2. Unit tests

Run these from the repository root:

```bash
# The gateway with CPU ranks (no GPU), from gateway/
(cd gateway && ./build/release/test/unittest "test/sql/*")

# Exchange layers over the loopback fabric, the query numbering, and two ranks in one process over NIXL
source scripts/nixl_env.sh
pixi run build/release/test/cpp/sirius_unittest "[exchange]"
pixi run build/release/test/cpp/sirius_unittest "[query_windows]"

# One rank per process and GPU over NIXL; GPUS=0,2 picks the GPUs
pixi run scripts/distributed_test_nixl.sh 2
pixi run scripts/distributed_test_nixl.sh 4
TEST="[nixl_bandwidth]" pixi run scripts/distributed_test_nixl.sh 4   # exchange throughput per rank
```

Each process logs to `/tmp/distributed_nixl_rank_<i>.log`.

### 3. A query without the gateway

```bash
pixi run scripts/distributed_run_sql.sh 2 /data/q12.sql
pixi run scripts/distributed_run_sql.sh 4 /data/q12.sql
```

The script starts N ranks, rank i on GPU i, and prints rank 0's result. It should match DuckDB's
answer from [Test data](#test-data). Rank logs are in `/tmp/distributed_rank_<i>.log`.

### 4. The same query through the gateway

Start the gateway and 4 ranks. Its terminal becomes the gateway's console:

```bash
SIRIUS_GATEWAY_TOKEN=my_token pixi run scripts/sirius_gateway.sh 4    # client port 9494, logs in /tmp/sirius_gateway
```

On the console, check that every rank is up:

```sql
FROM sirius_gateway_status();    -- one row per rank: pid, GPU, state, last query number
```

From a second terminal, connect a client (any DuckDB with Quack, e.g. `gateway/build/release/duckdb`):

```sql
ATTACH 'quack:localhost' AS sirius (TOKEN 'my_token');
CONNECT sirius;
.read /data/q12.sql
```

To accept clients from other machines, start the gateway with `SIRIUS_GATEWAY_HOST=0.0.0.0`.
Quack then serves TLS with a self-signed certificate. Read its fingerprint on the console with
`FROM quack_server_list()` (the `info` map), and add `ssl_fingerprint '<fingerprint>'` to the
client's `ATTACH 'quack:<gateway host>'` options.

The answer should match the one from step 3. Then:

| Check | How | Expected |
|---|---|---|
| Two clients | Run Q12 from two client terminals at once | Both get the right answer; the gateway runs them one after the other |
| Errors | `SELECT * FROM no_such_table;` | An error; the next query works |
| Refused statements | `SET sirius_exchange_rank = 1;` or `FROM sirius_gateway_stop();` from a client | Refused: only the gateway's console runs these |
| A rank dies | `kill <pid>` of rank 1 (from the status), then run Q12 | The gateway restarts every rank and replays the views, then answers |
| Timeout | On the console, `SET GLOBAL sirius_gateway_statement_timeout = 2;`, then a long query from a client | "timed out after 2 s; cancelled on every rank"; the next query works. Set it back to 0 |
| Log | `FROM sirius_gateway_log();` on the console | The `CREATE VIEW`s, in order |
| Shutdown | Ctrl-D on the console | Every rank exits |

The gateway writes each rank's output to `/tmp/sirius_gateway/rank_<i>.log`.

## Several boxes, one GPU each

The gateway cannot drive these yet. It starts every rank as its own child process and gives the
ranks `127.0.0.1` addresses (`gateway/src/gateway_state.cpp`, `start_locked`). Distributed Sirius
itself takes any `host:port` in `sirius_exchange_peers`, so start one rank per box and send every
rank the same SQL.

### 1. Network

Pick one address per box that every other box reaches, here `10.0.0.1` and `10.0.0.2`. The peer
list is the same on every box; rank i is the i-th address.

- **Ports.** Each rank's NIXL agent serves its metadata on its exchange port (27000 below), on
  every interface. UCX then opens its own connections on other ports. The simplest firewall
  rule is to allow all TCP between the boxes.
- **Transport.** `scripts/nixl_env.sh` sets `UCX_TLS=cuda_copy,cuda_ipc,tcp,self`, which works
  between boxes over TCP. To use RDMA, add `rc` (GPUDirect RDMA also needs the `nvidia-peermem`
  module), and pick the NIC:

  ```bash
  export UCX_TLS=rc,cuda_copy,cuda_ipc,tcp,self
  export UCX_NET_DEVICES=mlx5_0:1      # or a TCP interface, e.g. eth0, when the boxes have several
  ```

  `ucx_info -d` (in `~/tools/ucx-install/bin`) lists the devices, and `ibv_devinfo` the RDMA ports.
- **Host staging off.** Staging through shared memory only works between processes on one box. Set
  `SIRIUS_EXCHANGE_HOST_STAGING=never` on every box. The automatic choice looks a peer's GPU up
  by PCI bus id on the local box, so on a box with several GPUs a remote GPU could be mistaken
  for a local one without peer access.
- **Start together.** A rank waits up to 2 minutes for every peer to connect, then fails.

### 2. Transport test

Run the exchange's multiprocess test with one rank per box. On box i (0 or 1):

```bash
cd ~/sirius && source scripts/nixl_env.sh
export SIRIUS_EXCHANGE_HOST_STAGING=never
SIRIUS_EXCHANGE_RANK=<i> SIRIUS_EXCHANGE_PEERS=10.0.0.1:27000,10.0.0.2:27000 \
  pixi run build/release/test/cpp/sirius_unittest "[nixl_multiprocess]"
```

Both should pass. Then run `"[nixl_bandwidth]"` the same way to measure the link.

### 3. A query across boxes

Save this as `~/run_rank.sh` on every box:

```bash
#!/usr/bin/env bash
# run_rank.sh <rank> <peers> <file.sql>: runs the file as one rank, printing its output.
set -euo pipefail
cd ~/sirius && source scripts/nixl_env.sh
export SIRIUS_EXCHANGE_HOST_STAGING=never
build=sirius-duckdb/build/release
{
  echo "LOAD '$build/extension/sirius/sirius.duckdb_extension';"
  echo "SET enable_duckdb_fallback = false;"
  echo "SET sirius_exchange_peers = '$2';"
  echo "SET sirius_exchange_rank = $1;"
  cat "$3"
} | pixi run "$build/duckdb" -unsigned
```

Then, from any machine with SSH access to the boxes:

```bash
hosts=(10.0.0.1 10.0.0.2)                       # rank i runs on hosts[i]
peers=$(printf '%s:27000,' "${hosts[@]}"); peers=${peers%,}
for i in "${!hosts[@]}"; do
  ssh "${hosts[$i]}" bash ~/run_rank.sh "$i" "$peers" /data/q12.sql > "rank_$i.log" 2>&1 &
done
wait
cat rank_0.log                                  # the result; the other logs show only timings
```

`pixi` must be on the `PATH` of a non-interactive SSH session on each box.
Rank 0's result should match DuckDB's answer. Every rank must get the same statements in the same
order, which is what the script does. So run interactive sessions this way, not by typing into each
box separately.

### What the gateway needs for several boxes

Not built yet. Two changes would let `sirius_gateway_start` drive ranks on other boxes:

- **Per-rank hosts.** A `hosts` argument giving each rank's exchange address, used in
  `sirius_exchange_peers` instead of `127.0.0.1`.
- **Remote ranks.** Either start each rank over SSH (the SSH session's stdin plays the role of the
  held-open pipe, so the rank still exits with the gateway), or attach to ranks already serving
  Quack on their boxes. The second cannot restart a rank, so recovery would stop the cluster
  instead.

Ranks on other hosts serve Quack over TLS with a self-signed certificate. The gateway would pin
each one with `ssl_fingerprint`, which a rank's `quack_server_list()` shows in its `info` map.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| "needs a Sirius built with -DSIRIUS_WITH_NIXL=ON" | Built without the NIXL flag; configure again as in [Build](#build) |
| `libnixl.so` or the UCX plugin not found | `scripts/nixl_env.sh` not sourced, or `NIXL_PREFIX` points elsewhere |
| "exchange NIXL: rank r reached k of n peers" | A peer did not start within 2 minutes, a port is blocked, or an address in the peer list is wrong |
| A query fails on every rank with a plan-shape mismatch | The boxes run different builds, or see different files or file sizes |
| Wrong answer, no error | A rank fell back to the CPU; check that every rank set `enable_duckdb_fallback = false` |
| Exchange slow between GPUs on one box | `nvidia-smi topo -p2p r` shows `NS`, so it stages through host memory; the rank logs say which path each peer uses |
| Exchange slow between boxes | UCX chose TCP; check `UCX_TLS` and `UCX_NET_DEVICES` |
| `shm_open` fails between boxes | Host staging chosen for a remote peer; set `SIRIUS_EXCHANGE_HOST_STAGING=never` |
| Gateway: "ranks disagree on query K" | One rank failed where the others succeeded; the gateway restarts and replays before the next statement. The rank logs show why |
| Gateway: a rank exits at start | Its log, named in the error, under the gateway's log directory |
