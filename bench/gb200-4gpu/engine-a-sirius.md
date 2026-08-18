# Engine A — Sirius as StarRocks CN · runbook

How to actually run it on `presto-gb200-gcn-17`. Everything here was executed on 2026-08-12;
results in [`RESULTS-sf500.md`](RESULTS-sf500.md).

**Engine A** = StarRocks FE + N × `sirius-starrocks-cn`, one CN per GPU. StarRocks owns planning
and distribution; Sirius executes one plan fragment at a time.

---

## 1. Requirements

| | Check | If missing |
|---|---|---|
| CN binary | `ls experimental/starrocks/target/release/sirius-starrocks-cn` | `pixi run make` (aarch64 here) |
| Harness | `experimental/starrocks/benchmarks/tpch/run-abc.sh` | in-repo |
| Launcher + env | `experimental/starrocks/configs/gb200-4gpu/{cluster4-numa.sh,engine-a.env}` | in-repo |
| Data | `/raid/prestouser/aocsa/tpch_parquet_sf500` (132 GB) · `/raid/tpch-sf1000` (283 GB) | both already staged |
| MySQL client | pixi env at `experimental/starrocks/.pixi/envs/default/bin/mysql` | `pixi install` |
| **Box idle** | all 4 GPUs at the **~28–320 MiB floor** | see §4 — this is the one people skip |

`run-abc.sh` starts the FE and CNs itself. Do **not** hand-start `cluster4-numa.sh` as well.

---

## 2. Quick start

```bash
cd experimental/starrocks/benchmarks/tpch

export NUM_CNS=4
export GPU_MEM=140GiB
export STAGING=32GiB          # NOT the 16GiB default — see §3
export HOST_MEM=160GiB

./run-abc.sh \
  --sf 500 \
  --engines A \
  --runs 2 \
  --queries "q01 q02 q03 q04 q06 q07 q11 q12 q14 q15 q16 q19 q20 q22 q13 q17 q21" \
  --data /raid/prestouser/aocsa/tpch_parquet_sf500 \
  --warm-timeout 180 --cold-timeout 300 \
  --out <outdir>
```

**Always `--dry-run` first.** It resolves the dataset, prints the timeout budget and the worst-case
wall clock, and starts nothing:

```bash
./run-abc.sh --sf 500 --engines A --dry-run --out /tmp/dryrun ...
```

---

## 3. The knobs that matter

All of `engine-a.env` is `${VAR:-default}`, so every value is overridable by exporting it first.

| Env | Default | Use | Why |
|---|---|---|---|
| `NUM_CNS` | 4 | 1 / 2 / 4 | Scale-out arms. See [`HARDWARE.md`](HARDWARE.md) for the CN→GPU→CPU map |
| `GPU_MEM` | `140GiB` | `140GiB` | RMM pool per CN. 185.03 GiB card |
| **`STAGING`** | `16GiB` | **`32GiB`** | **Exchange staging arena. 16 GiB refuses q17; 32 GiB passes it 4/4.** Bare `cudaMalloc` **outside** the RMM pool |
| `HOST_MEM` | `160GiB` | `160GiB` | Lazily-grown ceiling per CN |
| `SIRIUS_QUERY_WATCHDOG_SECS` | `0` | consider arming | At 0 a wedge burns the full timeout and can poison later queries |
| `UCX_TLS` | `cuda_copy,cuda_ipc,tcp,self` | leave | `cuda_ipc` is load-bearing — without it nixl falls off a **1349×** cliff |
| `CPU_SPLIT` | `disjoint` | leave | `0-35 36-71 72-107 108-143`, matching GPU→socket affinity |

### The memory arithmetic — get this right or it OOMs on startup

```
GPU_MEM + STAGING + ~1 GiB (CUDA context)  <  185.03 GiB
140      + 32      + 1                      = 173 GiB   ✅
159      + 32      + 1                      = 192 GiB   ❌ OOM
```

`GPU_MEM` does **not** account for `STAGING` — they are separate allocations. Raising one means
lowering the other.

### Harness flags

| Flag | Note |
|---|---|
| `--queries "q01 q02 …"` | **Order is preserved.** Put risky queries last |
| `--runs N` | run 0 = `cold`, runs 1..N = `warm` |
| `--warm-timeout` / `--cold-timeout` | **Override these.** SF500 auto-derives **900 s / 3000 s** — far too slow for a failure hunt. Healthy SF500 queries finish in 0.5–5 s |
| `--data PATH` | Pass it. The default search order hits `/raid/prestouser/kkristensen` **first** |
| `--q11-fraction` | `spec` (default) rewrites the literal to `0.0001/SF`. Correct — leave it |
| `--dry-run` | Validates everything, starts nothing |

---

## 4. Steps

**1 — Verify the box is idle.** Not optional.

```bash
nvidia-smi --query-gpu=index,memory.used --format=csv,noheader
nvidia-smi --query-compute-apps=pid,used_memory --format=csv
```

> ⚠️ **Sirius transparently intercepts ordinary SQL onto the GPU.** A plain `SELECT count(*)` run
> by anyone — a teammate, an editor, an agent — silently takes the whole box. During this
> benchmark a background `duckdb` row-count was holding **~180 GB on two GPUs**. If any compute
> app is listed, find and stop it before measuring.

**2 — Dry run.** Confirms dataset, filesystem, timeouts, worst-case wall clock.

**3 — Run.** It launches the FE + CNs, gates, runs, and tears down. It prints:

```
launching: …/cluster4-numa.sh  (NUM_CNS=4, GPU_MEM=140GiB, STAGING=32GiB, HOST_MEM=160GiB)
4 compute nodes alive and settled
blacklist settled EMPTY
enable_pipeline_engine = true (set explicitly and read back)
```

All four lines must appear. **"blacklist settled EMPTY" is the important one** — the FE
blacklists a CN that misses one heartbeat, and a 2-of-4 cluster answers every query while
silently halving the machine.

**4 — Watch results land.**

```bash
tail -f <outdir>/results.csv
```

**5 — Confirm teardown.** The run ends with `Final GPU state:` — verify it is back at the floor.

---

## 5. Constraints

**`pass` does not mean correct.** The harness defines `pass` as *exit code 0 and ≥ 1 row*. It
**never compares values**. You must diff against a DuckDB oracle (`SET gpu_execution = false`)
separately, at **relative tolerance 1e-12 — never string equality**: q06 legitimately returns
`61662234676.307495` / `.3075` / `.30751` across runs (±1 ULP) and an exact comparator false-fails
it about a third of the time.

**Seven queries return wrong values.** `q01 q03 q05 q07 q14 q15 q19` — the decimal→FP64 defect at
`expr_translator.rs:459-481`. They complete and report `pass`. Not fixed at HEAD.

**Never run a 1-iteration sweep.** q15 is non-deterministic — 2 pass / 4 empty over six runs. One
iteration reports `pass` about a third of the time and publishes a fabricated timing.

**"Cold" is first-contact, not cache-dropped.** True cold needs `drop_caches`, which needs root.
Without sudo, run 0 is first-contact-after-cluster-start only. Say so when publishing.

**Never run the five known failures** (`q05 q08 q09 q10 q18`) in a measurement sweep. At SF500 they
cost ~5.5 h of wedges and can poison the queries after them.

**Don't measure during the nightly CI window** (~02:00–03:50 UTC on the GB200 boxes).

---

## 6. Reading the output

```
<outdir>/results.csv                 engine,scale,query,run,phase,status,ms,rows
<outdir>/manifest.txt                what was requested
<outdir>/engineA/provenance.txt      config as RESOLVED, dataset + filesystem, versions
<outdir>/engineA/cluster.log         FE + CN logs — where the real errors are
<outdir>/engineA/q<NN>.r<N>.out      per-run output; on failure, the client error text
```

| `status` | Meaning |
|---|---|
| `pass` | exit 0, ≥1 row. **Not a correctness claim** |
| `empty` | completed normally, 0 rows. Often a **silent wrong answer** |
| `refused` | engine returned an error — read `q<NN>.r<N>.out` |
| `wedge` | hit the timeout |

`empty` and `wedge` are deliberately distinct. Take **medians over `phase=warm`**; read
`phase=cold` for first contact. Never drop the failure rows.

Quick tally:

```bash
awk -F, 'NR>1{c[$6]++} END{for(s in c) print s,c[s]}' <outdir>/results.csv
```

---

## 7. Known state at SF500 (measured, 4 CNs)

**14 of 17 complete**, 0.5–4.9 s.

| Query | Status | Fix |
|---|---|---|
| q17 | refused @16 GiB → **passes @32 GiB** | `STAGING=32GiB` |
| q21 | refused @16 **and** 32 GiB | Open — leases grow to fill the arena (13–25 → 32–36). Not a sizing problem |
| q15 | non-deterministic `empty` | Blocked on the FP64 defect |

Excluded up front: `q05 q08 q09 q10 q18`.

---

## 8. Scale-out arms (Study 1)

Re-run with `NUM_CNS` changed; see [`HARDWARE.md`](HARDWARE.md) for the mapping.

```bash
NUM_CNS=1 ./run-abc.sh --sf 500 --engines A ... --out <outdir>-1cn
NUM_CNS=2 ./run-abc.sh --sf 500 --engines A ... --out <outdir>-2cn
NUM_CNS=4 ./run-abc.sh --sf 500 --engines A ... --out <outdir>-4cn
```

> At 2 CNs prefer **GPU0 + GPU2** (one per socket) over GPU0 + GPU1 (both on socket 0). All GPU
> pairs are `NV18`, so the fabric is identical either way — but GPU0+GPU1 confines both CNs to
> 72 cores and leaves socket 1 idle. Running both pairings isolates host-NUMA cost exactly.
