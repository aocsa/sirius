# TPC-H Q12 SF100 through the gateway on two boxes (gcn-17 + gcn-18)

A checklist for running [distributed Sirius](distributed-sirius.md) TPC-H Q12 at scale factor 100
**through the gateway** with one rank on **presto-gb200-gcn-17** and one on
**presto-gb200-gcn-18**, GPU 0 on each, over UCX TCP. This is
[“The same query through the gateway”](distributed-testing.md#4-the-same-query-through-the-gateway-1)
for several boxes, with these machines’ addresses and paths.

Everything is typed on gcn-17, in two terminals. You do not log in to gcn-18 by hand except for
the idle check: the gateway starts rank 1 there over SSH with `scripts/sirius_gateway_ssh_rank.sh`.
No AI agent is needed. For the same query without the gateway, see
[two-box-q12-sf100.md](two-box-q12-sf100.md); for the gateway on gcn-17 alone, see
[gateway-q12-sf100-2gpu.md](gateway-q12-sf100-2gpu.md).

Checked 2026-10-30, commit `0e79da1d` plus the uncommitted gateway `hosts` change: rank 0 `up`
on 10.87.140.52, rank 1 `up` on 10.87.140.53, client `ATTACH 'quack:127.0.0.1:9494'` worked, Q12
matched the CPU reference in 3.5 s, shutdown left no `duckdb`, ports or init file on either box.

## What you need

| | |
|---|---|
| Gateway box | `presto-gb200-gcn-17`, bond0 `10.87.140.52` |
| Other box | `presto-gb200-gcn-18`, bond0 `10.87.140.53` |
| GPUs | GPU 0 on each box (`SIRIUS_GATEWAY_GPUS=0,0`) |
| Network | UCX TCP over `bond0` (`UCX_NET_DEVICES=bond0`) |
| Client port | `127.0.0.1:9494` on gcn-17 |
| Rank Quack | `127.0.0.1:9500` (rank 0), `127.0.0.1:9501` (rank 1, SSH-forwarded to gcn-18) |
| Exchange | `10.87.140.52:27000` (rank 0), `10.87.140.53:27001` (rank 1) |
| Worktree | `/scratch/prestouser/aocsa/wt/gateway-6eda5ee4` (same path on both boxes; `/scratch` is shared) |
| Gateway DuckDB | `gateway/build/release/duckdb` (httpfs + quack + `sirius_gateway`) |
| Rank DuckDB | `sirius-duckdb/build/release/duckdb` |
| Dataset | `/scratch/sirius/datasets/tpch_sf100` |
| Pixi | `$HOME/.pixi/bin/pixi` |

Do not rebuild. Use `pixi run`. Put pixi on `PATH`:

```bash
export PATH="$HOME/.pixi/bin:$PATH"
```

If `AI_AGENT`, `CLAUDECODE`, `CLAUDE_CODE`, or `CURSOR_AGENT` are set, unset them in **both**
terminals. Otherwise DuckDB prints agent-mode output. Both ranks inherit the gateway’s
environment (the SSH launcher forwards only `UCX_*`, `NIXL_*` and `SIRIUS_*`, so gcn-18 starts
clean, but rank 0 on gcn-17 does not).

## 1. Confirm SSH, the build and the data

The gateway runs `ssh` to gcn-18 non-interactively, so this must print the host name without
asking for a password:

```bash
ssh -o BatchMode=yes presto-gb200-gcn-18 hostname
```

Both boxes must see the same checkout and build:

```bash
cd /scratch/prestouser/aocsa/wt/gateway-6eda5ee4
git log -1 --oneline
ls -l gateway/build/release/duckdb \
      sirius-duckdb/build/release/duckdb \
      sirius-duckdb/build/release/extension/sirius/sirius.duckdb_extension \
      scripts/sirius_gateway_ssh_rank.sh
ssh -o BatchMode=yes presto-gb200-gcn-18 \
  'cd /scratch/prestouser/aocsa/wt/gateway-6eda5ee4 && git log -1 --oneline && ls -l sirius-duckdb/build/release/duckdb && $HOME/.pixi/bin/pixi --version'
ls /scratch/sirius/datasets/tpch_sf100/lineitem/*.parquet | wc -l   # 6
ls /scratch/sirius/datasets/tpch_sf100/orders/*.parquet | wc -l     # 2
```

The two `git log` lines must be the same commit.

Check the bond0 addresses; they are the exchange addresses you pass in step 4:

```bash
ip -4 -br addr show bond0                                          # 10.87.140.52/27
ssh -o BatchMode=yes presto-gb200-gcn-18 ip -4 -br addr show bond0 # 10.87.140.53/27
```

## 2. Confirm both boxes are idle

On gcn-17:

```bash
nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader
pgrep -x duckdb || echo "no duckdb"
ss -ltn | grep -E ':9494|:9500|:9501|:27000' || echo "gateway ports free"
```

On gcn-18, through SSH:

```bash
ssh -o BatchMode=yes presto-gb200-gcn-18 '
  /tmp/aocsa/tpch22/sf30k/scripts/quiet.sh; echo "quiet rc=$?"
  nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader
  pgrep -x duckdb || echo "no duckdb"
  ss -ltn | grep -E ":9501|:27001" || echo "rank ports free"'
```

`quiet.sh` must print `QUIET` and `quiet rc=0`. `nvidia-smi` must list no compute processes on
either box, and there must be no other user’s `duckdb`. A rank takes on the order of 180 GiB of
GPU memory, so a busy GPU 0 makes it fail with OOM.

## 3. Query file (shared scratch)

Same SQL as the other runbooks. Write it once if it is not already there:

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

CPU reference:

```
MAIL  623115  934713
SHIP  622979  934534
```

## 4. Terminal 1 (gcn-17) — start the gateway and both ranks

```bash
export PATH="$HOME/.pixi/bin:$PATH"
unset AI_AGENT CLAUDECODE CLAUDE_CODE CURSOR_AGENT
cd /scratch/prestouser/aocsa/wt/gateway-6eda5ee4
rm -rf /tmp/sirius_gateway_q12_2box
export UCX_NET_DEVICES=bond0
SIRIUS_GATEWAY_TOKEN=gwtest \
SIRIUS_GATEWAY_HOSTS=10.87.140.52,10.87.140.53 \
SIRIUS_GATEWAY_GPUS=0,0 \
  pixi run scripts/sirius_gateway.sh 2 9494 /tmp/sirius_gateway_q12_2box
```

What each setting does:

- `UCX_NET_DEVICES=bond0` is required. These boxes have several interfaces, and without it UCX
  advertises addresses (100.127.x, 172.16.x) the other box cannot reach; the ranks then fail
  with `reached 0 of 1 peers in 120000 ms`. The launcher passes it on to the rank on gcn-18.
- `SIRIUS_GATEWAY_HOSTS` gives each rank’s exchange address. `10.87.140.52` is this box, so rank 0
  starts as a local child; `10.87.140.53` is not, so rank 1 starts over SSH.
- `SIRIUS_GATEWAY_GPUS=0,0` puts each rank on its box’s GPU 0. Without it, rank 1 would take GPU 1.
- With hosts set, the script also sets `SIRIUS_EXCHANGE_HOST_STAGING=never` (shared-memory
  staging only works on one host).

The script prints `gateway: quack:127.0.0.1:9494, token gwtest, logs in /tmp/sirius_gateway_q12_2box`
immediately. Then the gateway starts both ranks, waits until they reach each other over NIXL, and
serves clients on 9494. This takes a few tens of seconds. Init prints two tables, then the `D`
prompt:

1. `sirius_gateway_start` — two rows, `state = up`:

   ```
   │ rank │   pid   │ gpu │     host     │         uri          │ state │ error │ ...
   │    0 │ ...     │ 0   │ 10.87.140.52 │ quack:127.0.0.1:9500 │ up    │ NULL  │ ...
   │    1 │ ...     │ 0   │ 10.87.140.53 │ quack:127.0.0.1:9501 │ up    │ NULL  │ ...
   ```

   Rank 1’s `pid` is the local `ssh` process, not the DuckDB on gcn-18.
2. `quack_serve` — `listen_uri quack:127.0.0.1:9494`, `auth_token gwtest`.

Rank logs are `/tmp/sirius_gateway_q12_2box/rank_0.log` and `rank_1.log`, both on gcn-17; rank 1’s
output comes back over SSH. While rank 1 starts, `rank_1.log` shows a few
`channel 2: open failed: connect failed: Connection refused` lines, one per ATTACH retry before
the rank listens. That is expected. If start fails, the error names the rank log.

Leave this terminal open. Closing stdin (Ctrl-D) stops both ranks, including the one on gcn-18.

## 5. Terminal 2 (gcn-17) — client ATTACH, then Q12

Use `127.0.0.1`, not `localhost`. The gateway binds IPv4 `127.0.0.1:9494`, and `localhost` can be
`::1` here.

```bash
export PATH="$HOME/.pixi/bin:$PATH"
unset AI_AGENT CLAUDECODE CLAUDE_CODE CURSOR_AGENT
cd /scratch/prestouser/aocsa/wt/gateway-6eda5ee4
pixi run gateway/build/release/duckdb -unsigned -no-agent
```

```sql
ATTACH 'quack:127.0.0.1:9494' AS sirius (TOKEN 'gwtest');
CONNECT sirius;
.read /scratch/prestouser/aocsa/wt/gateway-6eda5ee4/q12_sf100.sql
```

The two `CREATE VIEW`s each come back as an empty `Count` table (rank 0’s DDL result). They run on
both ranks, so the rank on gcn-18 reads the same files from the shared `/scratch`. Then Q12:

```
┌────────────┬─────────────────┬────────────────┐
│ l_shipmode │ high_line_count │ low_line_count │
│  varchar   │     int128      │     int128     │
├────────────┼─────────────────┼────────────────┤
│ MAIL       │          623115 │         934713 │
│ SHIP       │          622979 │         934534 │
└────────────┴─────────────────┴────────────────┘
```

Checked runs took about 2 s for the SELECT. To see that rank 1 is really on gcn-18 while the
query runs, from any shell on gcn-17:

```bash
ssh -o BatchMode=yes presto-gb200-gcn-18 'pgrep -a -x duckdb; nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader'
```

Optional checks on the **gateway console** (terminal 1), not the client:

```sql
FROM sirius_gateway_status();
FROM sirius_gateway_log();
```

The status shows the `host` of each rank; the log lists the `CREATE VIEW`s.

## 6. Shut down

Quit the client (`.quit`), then press Ctrl-D in terminal 1. Both ranks exit with the gateway. Check
both boxes:

```bash
pgrep -x duckdb || echo "17: no duckdb"
ss -ltn | grep -E ':9494|:9500|:9501|:27000' || echo "17: gateway ports free"
ssh -o BatchMode=yes presto-gb200-gcn-18 'pgrep -x duckdb || echo "18: no duckdb"; ss -ltn | grep -E ":9501|:27001" || echo "18: rank ports free"'
```

Rank 1’s init file (it holds the token) is a `/tmp/sirius_gateway_rank.*` file on gcn-18. A
watchdog removes it 60 s after the rank started, so after a short run it can outlive the rank by up
to a minute:

```bash
sleep 60; ssh -o BatchMode=yes presto-gb200-gcn-18 'ls /tmp/sirius_gateway_rank.* 2>/dev/null || echo "18: no init files"'
```

If a rank is left behind, on the box where it runs:

```bash
kill $(pgrep -u "$(id -u)" -x duckdb)
```

Do not `pkill -f duckdb`. If the gateway itself is killed (not Ctrl-D), the watchdog on gcn-18
kills rank 1 within about 5 s after its SSH session goes away (or 60 s after the rank started, if
that is later).

## Ports and processes while it runs

| Box | Process | GPU | Listens |
|---|---|---|---|
| gcn-17 | Gateway DuckDB | none (CPU) | `127.0.0.1:9494` |
| gcn-17 | Rank 0 DuckDB | 0 | Quack `127.0.0.1:9500`, NIXL `*:27000` |
| gcn-17 | `ssh` for rank 1 | none | forward `127.0.0.1:9501` → gcn-18 `127.0.0.1:9501` |
| gcn-18 | Rank 1 DuckDB (under `pixi run`) | 0 | Quack `127.0.0.1:9501`, NIXL `*:27001` |

`pgrep -x duckdb` shows two processes on gcn-17 and one on gcn-18.

## If it fails

| Symptom | Likely cause |
|---|---|
| `reached 0 of 1 peers in 120000 ms` in a rank log | `UCX_NET_DEVICES=bond0` not exported in terminal 1, or a firewall between the boxes on 27000/27001 |
| `rank 1's host 10.87.140.53 is not this machine: SET sirius_gateway_rank_launcher` | Started the gateway without `scripts/sirius_gateway.sh`, which sets the SSH launcher |
| `hosts lists 1 hosts for 2 ranks` | `SIRIUS_GATEWAY_HOSTS` must have one address per rank |
| Rank 1 exits at once; `rank_1.log` says `Permission denied` or `Host key verification failed` | Passwordless SSH to gcn-18 not set up (step 1) |
| `rank_1.log` says `bind [127.0.0.1]:9501: Address already in use` | Something on gcn-17 holds 9501; stop it or pick another `quack_base_port` |
| `ATTACH` hangs or times out | Used `localhost` instead of `127.0.0.1`, or the `D` prompt is not up yet |
| Plan-shape mismatch | The boxes run different builds or see different files (step 1) |
| Rank exits; GPU OOM | GPU 0 busy on one of the boxes (step 2) |
| `shm_open` fails | `SIRIUS_EXCHANGE_HOST_STAGING` overridden to something other than `never` |
| Wrong answer, no error | A rank fell back to CPU; the gateway sets `enable_duckdb_fallback = false` |
| Agent-mode / odd CLI output | `AI_AGENT` or `CURSOR_AGENT` still set in terminal 1 |
