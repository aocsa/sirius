# TPC-H Q12 SF100 through the gateway on one box (2 GPUs)

A checklist for running [distributed Sirius](distributed-sirius.md) TPC-H Q12 at scale factor 100
**through the gateway** on **presto-gb200-gcn-17** only, using two GPUs (0 and 1). This is
[“The same query through the gateway”](distributed-testing.md#4-the-same-query-through-the-gateway)
from the testing guide, with this machine’s paths and the ATTACH host that actually works here.

You need two terminals on gcn-17. No other box, and no AI agent. The gateway starts both ranks
as local children and gives them `127.0.0.1` exchange addresses. To put ranks on gcn-18 as well,
see [distributed-testing.md](distributed-testing.md#4-the-same-query-through-the-gateway-1).

Checked on gcn-17, 2026-10-30, commit `0e79da1d`: two ranks `up` on GPUs 0 and 1, client
`ATTACH 'quack:127.0.0.1:9494'` worked, Q12 matched the CPU reference in 1.994 s, Ctrl-D left no
`duckdb` and freed the ports.

## What you need

| | |
|---|---|
| Host | `presto-gb200-gcn-17` |
| GPUs | 0 and 1 (`sirius_gateway.sh 2` puts rank *i* on GPU *i*) |
| Client port | `127.0.0.1:9494` |
| Rank Quack | `127.0.0.1:9500`, `127.0.0.1:9501` |
| Exchange | `127.0.0.1:27000`, `127.0.0.1:27001` |
| Worktree | `/scratch/prestouser/aocsa/wt/gateway-6eda5ee4` |
| Gateway DuckDB | `gateway/build/release/duckdb` (httpfs + quack + `sirius_gateway`) |
| Rank DuckDB | `sirius-duckdb/build/release/duckdb` |
| Sirius extension | `sirius-duckdb/build/release/extension/sirius/sirius.duckdb_extension` |
| Dataset | `/scratch/sirius/datasets/tpch_sf100` |
| Pixi | `$HOME/.pixi/bin/pixi` |

Do not rebuild. Use `pixi run`. Put pixi on `PATH`:

```bash
export PATH="$HOME/.pixi/bin:$PATH"
```

If `AI_AGENT`, `CLAUDECODE`, `CLAUDE_CODE`, or `CURSOR_AGENT` are set, unset them in **both**
terminals. Otherwise DuckDB prints agent-mode output. Rank processes inherit the gateway’s
environment, so unsetting only in the client terminal is not enough.

## 1. Confirm the build and the data

```bash
cd /scratch/prestouser/aocsa/wt/gateway-6eda5ee4
git log -1 --oneline
ls -l gateway/build/release/duckdb \
      sirius-duckdb/build/release/duckdb \
      sirius-duckdb/build/release/extension/sirius/sirius.duckdb_extension
ls /scratch/sirius/datasets/tpch_sf100/lineitem/*.parquet | wc -l   # 6
ls /scratch/sirius/datasets/tpch_sf100/orders/*.parquet | wc -l     # 2
```

## 2. Confirm this box is idle

```bash
nvidia-smi
pgrep -x duckdb || echo "no duckdb"
ss -ltn | grep -E ':9494|:9500|:9501|:27000|:27001' || echo "gateway ports free"
```

GPUs 0 and 1 must be empty (~0 MiB, no compute processes). Another user’s job on those GPUs will
OOM the ranks (each rank takes on the order of 180 GiB). Leave GPU 2 and 3 alone.

gcn-18 is not used, but check it is not in the middle of a GPU run before you take 2 GPUs on 17.

## 3. Query file (shared scratch)

Same SQL as the two-box run. Write it once if it is not already there:

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

## 4. Terminal 1 — start the gateway and two GPU ranks

```bash
export PATH="$HOME/.pixi/bin:$PATH"
unset AI_AGENT CLAUDECODE CLAUDE_CODE CURSOR_AGENT
cd /scratch/prestouser/aocsa/wt/gateway-6eda5ee4
rm -rf /tmp/sirius_gateway_q12_2gpu
SIRIUS_GATEWAY_TOKEN=gwtest pixi run scripts/sirius_gateway.sh 2 9494 /tmp/sirius_gateway_q12_2gpu
```

The script prints `gateway: quack:127.0.0.1:9494, token gwtest, logs in /tmp/sirius_gateway_q12_2gpu`
immediately, then DuckDB runs the init file: it spawns two rank processes (GPU 0 and GPU 1), waits
until NIXL between them is connected, then serves clients on 9494.

Init itself prints two tables, then you get the `D` prompt:

1. `sirius_gateway_start(2)` — two rows, `state = up`, `gpu` `0` and `1`, URIs
   `quack:127.0.0.1:9500` and `9501`.
2. `quack_serve` — `listen_uri quack:127.0.0.1:9494`, `auth_token gwtest`.

GPU bring-up can take a few tens of seconds. You can run `FROM sirius_gateway_status();` again at
the prompt; it should match the start table. Rank logs are
`/tmp/sirius_gateway_q12_2gpu/rank_0.log` and `rank_1.log`. If start fails, the error names the
rank log.

Leave this terminal open. Closing stdin (Ctrl-D) stops every rank.

## 5. Terminal 2 — client ATTACH, then Q12

Use `127.0.0.1`, not `localhost`. The gateway binds IPv4 `127.0.0.1:9494`. On this box
`localhost` can be `::1`, and `ATTACH 'quack:localhost:9494'` never connects.

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

The two `CREATE VIEW`s each come back as an empty `Count` table (rank 0’s DDL result). Then Q12:

```
┌────────────┬─────────────────┬────────────────┐
│ l_shipmode │ high_line_count │ low_line_count │
│  varchar   │     int128      │     int128     │
├────────────┼─────────────────┼────────────────┤
│ MAIL       │          623115 │         934713 │
│ SHIP       │          622979 │         934534 │
└────────────┴─────────────────┴────────────────┘
```

`.timer on` is a client CLI command; it does not go to the ranks. On the checked run the SELECT
was 1.994 s.

Optional checks on the **gateway console** (terminal 1), not the client:

```sql
FROM sirius_gateway_status();
FROM sirius_gateway_log();
```

The log should list the `CREATE VIEW`s. A client `SET sirius_exchange_rank = 1;` or
`FROM sirius_gateway_stop();` must be refused.

## 6. Shut down

In terminal 1, Ctrl-D (EOF on the gateway CLI). Both ranks should exit with it.

```bash
pgrep -x duckdb || echo "no duckdb"
ss -ltn | grep -E ':9494|:9500|:27000' || echo "gateway ports free"
nvidia-smi
```

If a rank is left behind:

```bash
kill $(pgrep -u "$(id -u)" -x duckdb)
```

Do not `pkill -f duckdb`.

## Ports and processes while it runs

| Process | GPU | Listens |
|---|---|---|
| Gateway DuckDB | none (CPU) | `127.0.0.1:9494` |
| Rank 0 | 0 | Quack `127.0.0.1:9500`, NIXL `*:27000` |
| Rank 1 | 1 | Quack `127.0.0.1:9501`, NIXL `*:27001` |

`pgrep -x duckdb` should show three processes.

## If it fails

| Symptom | Likely cause |
|---|---|
| `ATTACH` hangs or times out | Used `localhost` instead of `127.0.0.1`, or the `D` prompt is not up yet (9494 not listening) |
| Rank exits at start; log says crypto / httpfs | Rank DuckDB was built without statically linked `httpfs` (needed by `quack_serve`) |
| Rank exits; GPU OOM | GPU 0/1 not empty; wait or pick free GPUs with `FROM sirius_gateway_start(2, gpus := '2,3')` on the console instead of the script’s default 0,1 |
| `needs a Sirius built with -DSIRIUS_WITH_NIXL=ON` | Rank binary built without NIXL |
| `libnixl.so` not found in a rank log | Started the gateway without `scripts/sirius_gateway.sh` (it sources `nixl_env.sh` and the ranks inherit it) |
| Wrong answer, no error | A rank fell back to CPU; the gateway sets `enable_duckdb_fallback = false` |
| Plan-shape mismatch | Ranks saw different files or are different builds |
| Agent-mode / odd CLI output | `AI_AGENT` or `CURSOR_AGENT` still set in the gateway’s environment |

For two boxes, through the gateway or without it, see
[distributed-testing.md](distributed-testing.md#several-boxes-one-gpu-each) and
[two-box-q12-sf100.md](two-box-q12-sf100.md).
