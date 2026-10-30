# TPC-H Q12 SF100 on two GB200 boxes (no gateway)

A checklist for running [distributed Sirius](distributed-sirius.md) TPC-H Q12 at scale factor 100
across **presto-gb200-gcn-17** and **presto-gb200-gcn-18**, one GPU on each box. This is the
[“Several boxes, one GPU each”](distributed-testing.md#several-boxes-one-gpu-each) path from the
testing guide, with the hostnames, IPs, paths, and the one extra UCX setting this pair needs.

This runbook does not use the gateway: it starts one rank process per box and sends both the same
SQL. To run the same pair through the gateway instead, see
[distributed-testing.md](distributed-testing.md#4-the-same-query-through-the-gateway-1)
(`SIRIUS_GATEWAY_HOSTS=10.87.140.52,10.87.140.53 SIRIUS_GATEWAY_GPUS=0,0`).

This was run successfully on 2026-10-30 (commit `0e79da1d`, TCP over `bond0`). Rank 0 matched the
CPU reference in 2.75 s.

## What you need

| | Rank 0 | Rank 1 |
|---|---|---|
| Host | `presto-gb200-gcn-17` | `presto-gb200-gcn-18` |
| `bond0` | `10.87.140.52` | `10.87.140.53` |
| GPU | `CUDA_VISIBLE_DEVICES=0` | `CUDA_VISIBLE_DEVICES=0` |
| Exchange | `10.87.140.52:27000` | `10.87.140.53:27000` |
| Peer list (same on both) | `10.87.140.52:27000,10.87.140.53:27000` | same |

Shared paths (visible on both boxes):

| | |
|---|---|
| Worktree | `/scratch/prestouser/aocsa/wt/gateway-6eda5ee4` |
| Rank DuckDB | `sirius-duckdb/build/release/duckdb` |
| Sirius extension | `sirius-duckdb/build/release/extension/sirius/sirius.duckdb_extension` |
| Dataset | `/scratch/sirius/datasets/tpch_sf100` (`lineitem/` and `orders/` parquet) |
| NIXL / UCX | `$HOME/tools/nvda_nixl` and `$HOME/tools/ucx-install` (`scripts/nixl_env.sh`) |
| Pixi | `$HOME/.pixi/bin/pixi` |

Do not rebuild. Home (`/home/aocsa`) is quota-limited; this binary is already on `/scratch`. Every
rank must be the **same** build: ranks hash their plans, and a mixed pair fails every query.

Log in as yourself on either box (SSH to the other must work without a password prompt). Put pixi
on `PATH` in that shell and in non-interactive SSH:

```bash
export PATH="$HOME/.pixi/bin:$PATH"
```

Always start DuckDB with `pixi run`. If `AI_AGENT`, `CLAUDECODE`, `CLAUDE_CODE`, or `CURSOR_AGENT`
are set in the environment, unset them; otherwise DuckDB prints agent-mode output instead of a
normal result table.

## 1. Confirm the build and the data

On either box:

```bash
cd /scratch/prestouser/aocsa/wt/gateway-6eda5ee4
git log -1 --oneline
ls -l sirius-duckdb/build/release/duckdb \
      sirius-duckdb/build/release/extension/sirius/sirius.duckdb_extension
ls /scratch/sirius/datasets/tpch_sf100/lineitem/*.parquet | wc -l   # 6
ls /scratch/sirius/datasets/tpch_sf100/orders/*.parquet | wc -l     # 2
```

## 2. Check both boxes are idle

Do this on **both** `gcn-17` and `gcn-18` before touching a GPU. Do not start if another user has a
DuckDB or GPU job.

```bash
hostname
nvidia-smi
pgrep -x duckdb || echo "no duckdb"
ss -ltn | grep -E ':27000|:9494|:9500' || echo "exchange/gateway ports free"
```

Every GPU should show ~0 MiB used and no compute processes. Port **27000** must be free on both
boxes (NIXL metadata). 9494/9500 are the gateway ports; leave them alone for this run.

On gcn-18 only, if it is still there, you can also run
`/tmp/aocsa/tpch22/sf30k/scripts/quiet.sh` (exit 0 means quiet). `/tmp` is per machine; that script
is not on gcn-17.

Ping the peer’s `bond0` once:

```bash
ping -c 1 -W 2 10.87.140.53   # from 17
ping -c 1 -W 2 10.87.140.52   # from 18
```

## 3. Write the query (once, on shared scratch)

```bash
cat > /scratch/prestouser/aocsa/wt/gateway-6eda5ee4/q12_sf100.sql <<'SQL'
create view lineitem as select * from read_parquet('/scratch/sirius/datasets/tpch_sf100/lineitem/*.parquet');
create view orders as select * from read_parquet('/scratch/sirius/datasets/tpch_sf100/orders/*.parquet');
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

CPU reference (DuckDB, already measured):

```
MAIL  623115  934713
SHIP  622979  934534
```

## 4. Write the rank launcher (once, on shared scratch)

These boxes have many NICs. UCX’s default TCP path advertises `100.127.*` / `172.16.*` addresses
the other box cannot reach, and NIXL then sits for two minutes and fails with `reached 0 of 1
peers`. Pin TCP to `bond0`. Do not enable RDMA (`rc` / `mlx5`) for this experiment.

```bash
cat > /scratch/prestouser/aocsa/wt/gateway-6eda5ee4/run_rank.sh <<'EOF'
#!/usr/bin/env bash
# usage: run_rank.sh <rank> <peers> <file.sql>
set -euo pipefail
export PATH="$HOME/.pixi/bin:$PATH"
unset AI_AGENT CLAUDECODE CLAUDE_CODE CURSOR_AGENT
cd /scratch/prestouser/aocsa/wt/gateway-6eda5ee4
source scripts/nixl_env.sh
export SIRIUS_EXCHANGE_HOST_STAGING=never
export CUDA_VISIBLE_DEVICES=0
export UCX_NET_DEVICES="${UCX_NET_DEVICES:-bond0}"
build=sirius-duckdb/build/release
{
  echo "LOAD '$PWD/$build/extension/sirius/sirius.duckdb_extension';"
  echo "SET enable_duckdb_fallback = false;"
  echo "SET sirius_exchange_peers = '$2';"
  echo "SET sirius_exchange_rank = $1;"
  cat "$3"
} | pixi run "$build/duckdb" -unsigned -no-agent
EOF
chmod +x /scratch/prestouser/aocsa/wt/gateway-6eda5ee4/run_rank.sh
```

What each setting is for:

| Setting | Why |
|---|---|
| `source scripts/nixl_env.sh` | NIXL/UCX on `LD_LIBRARY_PATH`; default `UCX_TLS=cuda_copy,cuda_ipc,tcp,self` |
| `SIRIUS_EXCHANGE_HOST_STAGING=never` | Shared-memory staging is single-host. Auto GPU lookup can treat a remote GPU as a local one |
| `CUDA_VISIBLE_DEVICES=0` | One GPU per box |
| `UCX_NET_DEVICES=bond0` | Stop UCX picking unreachable NICs on these multi-homed nodes |
| `SET enable_duckdb_fallback = false` | A CPU fallback on one rank silently returns the wrong answer |
| Same peer list, start together | A rank waits up to **2 minutes** for every peer, then fails |

`nixl_env.sh` already chooses TCP. Leave `UCX_TLS` alone.

## 5. Start both ranks together

From **one** box (either is fine). Rank 0 must be gcn-17 and rank 1 gcn-18, because rank *i* is the
*i*-th address in the peer list.

```bash
export PATH="$HOME/.pixi/bin:$PATH"
peers='10.87.140.52:27000,10.87.140.53:27000'
sql=/scratch/prestouser/aocsa/wt/gateway-6eda5ee4/q12_sf100.sql
script=/scratch/prestouser/aocsa/wt/gateway-6eda5ee4/run_rank.sh

ssh presto-gb200-gcn-17 bash "$script" 0 "$peers" "$sql" > /tmp/rank_0.log 2>&1 &
ssh presto-gb200-gcn-18 bash "$script" 1 "$peers" "$sql" > /tmp/rank_1.log 2>&1 &
wait
echo "rank0=$?  (see each log if this is non-zero)"
```

`pixi` must be on `PATH` for a non-interactive SSH session; `run_rank.sh` sets that itself.

Do not type the SQL into two interactive DuckDB sessions. Every rank must see the **same
statements in the same order**.

While it runs you should see, on each box, one `duckdb` process (`pgrep -x duckdb`) and GPU 0
taking on the order of ~180 GiB. Port 27000 listens on `0.0.0.0` on both boxes. The query itself
is a few seconds after NIXL connects; pixi startup is a few seconds on top.

## 6. Read the result

The answer is in **rank 0**. Rank 1 is allowed to print an empty table plus a timer.

```bash
cat /tmp/rank_0.log
cat /tmp/rank_1.log
```

Pass:

```
┌────────────┬─────────────────┬────────────────┐
│ l_shipmode │ high_line_count │ low_line_count │
│  varchar   │     int128      │     int128     │
├────────────┼─────────────────┼────────────────┤
│ MAIL       │          623115 │         934713 │
│ SHIP       │          622979 │         934534 │
└────────────┴─────────────────┴────────────────┘
```

Both SSH jobs should exit 0. Then:

```bash
pgrep -x duckdb || echo "no duckdb"
ss -ltn | grep 27000 || echo "27000 free"
nvidia-smi
```

If a rank is stuck after a failure, kill only the DuckDB processes, by exact name, on **both**
boxes:

```bash
kill $(pgrep -u "$(id -u)" -x duckdb)
```

Do not `pkill -f duckdb`: that string matches the kill command itself.

## If it hangs on connect

Each rank waits 120 s, then:

```
Invalid Error: exchange NIXL: rank N reached 0 of 1 peers in 120000 ms
```

1. Confirm 27000 is listening on both boxes (`ss -ltn | grep 27000`) and that you can TCP-connect
   across `bond0`:

   ```bash
   python3 -c "import socket; s=socket.socket(); s.settimeout(3); print(s.connect_ex(('10.87.140.53',27000)))"
   ```

   `0` means the NIXL metadata port is reachable. That is **not** enough: UCX then opens other
   ports on whatever NIC it advertised.

2. Look in `/tmp/rank_0.log` for `UCX  ERROR   connect(... dest_addr=...)`. If the address is not
   `10.87.140.52` / `10.87.140.53`, UCX picked the wrong NIC. Keep `UCX_NET_DEVICES=bond0`.

3. Optional transport-only test, same peers and `SIRIUS_EXCHANGE_HOST_STAGING=never`. On box *i*
   (0 = gcn-17, 1 = gcn-18), at the same time:

   ```bash
   cd /scratch/prestouser/aocsa/wt/gateway-6eda5ee4
   source scripts/nixl_env.sh
   export SIRIUS_EXCHANGE_HOST_STAGING=never
   export UCX_NET_DEVICES=bond0
   export PATH="$HOME/.pixi/bin:$PATH"
   SIRIUS_EXCHANGE_RANK=<i> \
     SIRIUS_EXCHANGE_PEERS=10.87.140.52:27000,10.87.140.53:27000 \
     pixi run build/release/test/cpp/sirius_unittest "[nixl_multiprocess]"
   ```

Skip that test if Q12 already connected.

## Other failures

| Symptom | Likely cause |
|---|---|
| `libnixl.so` / UCX plugin not found | `scripts/nixl_env.sh` not sourced |
| Plan-shape mismatch on every rank | Different builds, or the boxes see different parquet files/sizes |
| Wrong answer, no error | A rank fell back to CPU; `enable_duckdb_fallback` must be `false` |
| `shm_open` fails | Host staging chosen for a remote peer; set `SIRIUS_EXCHANGE_HOST_STAGING=never` |
| GPU OOM | Another job on GPU 0; this run needs most of the 189 GiB device |
| Rank 1 empty table | Normal. Only rank 0 returns the result |

## What this is not

- Not the gateway. The gateway can drive the same pair with `SIRIUS_GATEWAY_HOSTS`; see
  [distributed-testing.md](distributed-testing.md#4-the-same-query-through-the-gateway-1).
- Not RDMA. Both boxes have mlx5 HCAs; leave `UCX_TLS` on TCP until you deliberately add `rc` and
  pick a device with `UCX_NET_DEVICES=mlx5_0:1`.
- Not a wider TPC-H run. This file is Q12 only.
