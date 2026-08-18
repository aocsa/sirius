# Benchmark Handoff — TPC-H on Sirius (GB200 box)

**For a fresh session with no prior context.** Read §0 and §6 first; everything else is reference.

**Written:** 2026-08-12 · **Box:** `presto-gb200-gcn-17` · **Branch:** `demo-multi-cn` @ `790612fb`

---

## 0. Information gaps — read before trusting anything here

These could not be determined from the repo or the box and need a human answer:

| # | Gap | Why it matters | Current workaround |
|---|---|---|---|
| 1 | **No `$/hr` for this box.** It is on-prem. | Study 3 is a cost study. | Dollars are **modeled** from declared reference prices (8× A100 $13.25, `m8gd` $8.83, `m8i` $12.19). **Break-even ratio is reported as the real result.** A 4× GB200 costs *more* than 8× A100, so every Sirius cost figure is a **floor**. |
| 2 | **No DuckDB oracle diff has ever been run.** | **Nothing measured is a correctness claim.** | The harness scores `pass` as *exit 0 and ≥1 row* — it never compares values. See §5.1. |
| 3 | Whether uncommitted harness/Rust edits should be **kept, gated, or reverted**. | One measured **3.2% slower**. | Listed in §1.4 with a recommendation each. |
| 4 | Whether `kvikio` should be declared in `pixi.toml`. | I installed it ad-hoc into the env. | §2.4. |
| 5 | Is the nightly-CI window (02:00–03:50 UTC) real for **gcn-17**? | Documented for "the GB200 boxes"; unverified here. | Avoid that window. |

Everything else below is measured or read from source, with paths given.

---

## 1. Objective and current state

### 1.1 What this is

Benchmark **Sirius** (GPU SQL engine) on TPC-H against two rivals, on one box, honestly.

| Code name | Engine | What it is |
|---|---|---|
| **A** | **Sirius (GPU)** | StarRocks FE + N × `sirius-starrocks-cn`, one CN per GPU. **The subject.** |
| **B** | **StarRocks (CPU)** | Stock StarRocks 3.5.20, 2 BEs. CPU baseline. |
| **C** | **cudf-polars (GPU)** | RAPIDS 26.08 over Ray. GPU rival. |
| *D* | *standalone Sirius* | `build/release/duckdb` + extension. **No FE, no CN.** Diagnostic only. |

Three studies, defined in [`bench/gb200-4gpu/PLAN.md`](bench/gb200-4gpu/PLAN.md).

### 1.2 Status

| Study | State | Headline |
|---|---|---|
| **1 — Scale-Out** (1→2→4 GPU) | ❌ **NOT RUN** | Only 4-CN measured. Script exists but has a bug — §5.4. |
| **2 — GPU Shootout** (A vs C) | ✅ Done, 7 regimes | **No single winner — the regime decides.** §4.3 |
| **3 — Cost Efficiency** (A vs B) | ✅ Done, SF500 + SF1000 | **Sirius 4.43× faster, 2.95× cheaper** at SF500 |

### 1.3 The five results that matter

1. **Warm benchmarking flatters cudf-polars, hugely.** Same SF500 data/queries, only the page cache
   differs: cudf-polars is **2.27× faster warm** but only **1.22× cold**, and **Sirius wins cold
   run 0**. Any A-vs-C number without a named regime is meaningless.
2. **Sirius beats the CPU engine decisively.** SF500, both cold, both tuned, all 15 queries:
   **23.02 s vs 101.91 s = 4.43×**, clearing the 1.50× cost break-even by 3×.
3. **Sirius cannot be tuned.** Four sweeps (incl. cold at SF500) moved it **±3% with unstable
   sign**; a full config port made it **3.2% slower**. One StarRocks session variable gave **−12%**.
4. **The CN layer costs ~9.7×.** Standalone Sirius (Engine D), pinned + `ast_jit`, ran the same 15
   queries in **2.40 s vs Engine A's 23.4 s** — and beat cudf-polars 4.16×. See §5.2.
5. **Two features are unreachable from the CN**, both worth a lot, both one wiring task:
   `ast_jit` (−4.17% suite; **−49.7% on q01**) and `pin_table`. §5.2.

### 1.4 Uncommitted changes — decide on these

`git status` on `demo-multi-cn`. All are opt-in except the Rust one.

| File | Change | Recommendation |
|---|---|---|
| `experimental/starrocks/benchmarks/tpch/run-abc.sh` | `PRE_QUERY_HOOK` (cold-cache drop, any engine); `C_IO_MODE`/`C_ITERATIONS`; `B_EXTRA_SQL` (with read-back) | **Keep.** All default to previous behaviour. Cold-mode benchmarking is impossible without the first. |
| `experimental/starrocks/configs/gb200-4gpu/cluster4-numa.sh` | `SIRIUS_TUNED_CONFIG` opt-in | **Keep**, or drop in favour of the Rust path. |
| `experimental/starrocks/src/engine_settings.rs` | Emits operator tuning in the derived config | ⚠️ **Gate behind an env flag, default off.** Measured **3.2% slower**. The env-override plumbing is worth keeping. |
| `bench/a100x8/engine-a-sirius.yaml` | Annotated with the measured staging-arena blocker | Keep. |
| `experimental/starrocks/.cn{0,3}/*` | Runtime artifacts | Gitignore. |

---

## 2. Environment setup

### 2.1 Prerequisites (all already satisfied on this box)

| Need | Check | Fix |
|---|---|---|
| CN binary | `ls experimental/starrocks/target/release/sirius-starrocks-cn` | §2.2 |
| Harness | `experimental/starrocks/benchmarks/tpch/run-abc.sh` | in-repo |
| Launcher | `experimental/starrocks/configs/gb200-4gpu/{cluster4-numa.sh,engine-a.env}` | in-repo |
| StarRocks | `~/starrocks-bench/{fe,be1,be2}` | `configs/gb200-4gpu/engine-b/setup-engine-b-gb200.sh` |
| cudf-polars | `cd ~/aocsa && pixi run python -c "import cudf_polars"` | §2.4 |
| Java | `/usr/lib/jvm/java-1.21.0-openjdk-arm64` | present |
| Data | §4.1 | already staged |

### 2.2 Building the CN (only if Rust changed)

```bash
cd ~/aocsa/sirius/experimental/starrocks
pixi run -e cn bash -lc 'source scripts/cn-env.sh; export NIXL_NO_STUBS_FALLBACK=1; \
  cargo build --release -p sirius-starrocks-cn'
```
~46 s for a Rust-only change. `cargo` is **not on PATH** — it lives in the pixi env. The full
`pixi run cn-build` also rebuilds libsirius (slow); skip it unless C++ changed.

### 2.3 Engine B config — already correct, do not "fix"

`~/starrocks-bench/be{1,2}/conf/be.conf`:

- **`mem_limit = 240G` absolute.** Never a percentage: `/proc/meminfo` counts GPU HBM
  (`MemTotal` ~1692 GB vs 956.8 GiB real), and `Swap: 0` makes that an OOM-kill. The harness aborts
  Engine B if it sees a `%`.
- **`num_cores = 72`** — mandatory. StarRocks' `CpuInfo` ignores the cpuset, so a `numactl`-pinned
  BE otherwise reports all 144.
- Storage on `/raid` (local NVMe). `be3`/`be4` carry a **trap conf** (64G, NFS) — `B_NUM_BES=2`
  never starts them and the harness warns.

### 2.4 cudf-polars — `kvikio` was installed ad-hoc

`--io-mode cold` raises `RuntimeError: kvikio is required for cold-run page cache dropping`.
The env had **no pip at all**:

```bash
cd ~/aocsa
pixi run python -m ensurepip --upgrade
pixi run python -m pip install --extra-index-url https://pypi.nvidia.com "kvikio-cu13==26.8.*"
```

**Not declared in `pixi.toml`** — add it under `[pypi-dependencies]` to make it reproducible.

---

## 3. Running benchmarks

### 3.1 Always: verify the box is idle

```bash
nvidia-smi --query-gpu=index,memory.used --format=csv,noheader     # want ~28-320 MiB
nvidia-smi --query-compute-apps=pid,used_memory --format=csv       # want empty
```

> ⚠️ **Sirius transparently intercepts ordinary SQL onto the GPU.** A plain `SELECT count(*)` by
> anyone — teammate, editor, agent — takes the whole box. This bit twice: once a stray `duckdb`
> row-count held **180 GB on two GPUs**, once CNs survived a killed wrapper in a new process group
> (`pgrep -f` missed them; `--query-compute-apps` found them). **Kill only PIDs you can attribute
> to your own run** — this is a shared box.

### 3.2 Always: `--dry-run` first

```bash
cd ~/aocsa/sirius/experimental/starrocks/benchmarks/tpch
./run-abc.sh --sf 500 --engines A --dry-run --out /tmp/dry \
  --data /raid/prestouser/aocsa/tpch_parquet_sf500
```
Resolves the dataset, prints timeout budget and worst-case wall clock, starts nothing.

### 3.3 The runs

All scripts are in `~/aocsa/benchmark-results/` and are self-documenting.

| Goal | Script | ~Time |
|---|---|---|
| A vs C, cold, SF100 | `run-sf100-cold-AvsC.sh` | 15 min |
| A vs C, cold, SF500 | `run-sf500-cold-AvsC.sh` | 30 min |
| A+B+C, warm, SF500 | `run-sf500-studies-2-3.sh` | 45 min |
| A, SF1000 | `run-sf1000-engineA.sh` | 25 min |
| B tuned, SF1000+SF500 | `run-B-tuned-costeff.sh` | 60 min |
| B tuning probe | `run-sf500-B-tuneprobe.sh` | 20 min |
| A tuning probe (cold) | `run-sf500-coldprobe.sh` | 20 min |
| **Study 1 (scale-out)** | `run-sf500-study1.sh` | ⚠️ **has a bug — §5.4** |

### 3.4 Harness flags that matter

| Flag / env | Note |
|---|---|
| `--queries "q01 q02 …"` | **Order preserved.** Put risky queries last. |
| `--warm-timeout` / `--cold-timeout` | **Override.** SF500 derives 900/3000 s, SF1000 1800/6000 s — far too slow for a failure hunt. |
| `--data PATH` | **Pass explicitly.** Default search hits `/raid/prestouser/kkristensen` *first*. |
| `--engines A,B,C` | Serial, full teardown between. |
| `PRE_QUERY_HOOK` | Cold-cache drop before every timed run. **Added this session.** |
| `C_IO_MODE`, `C_ITERATIONS` | cudf-polars regime. Default `hot`/(runs+1). **Added.** |
| `B_EXTRA_SQL` | StarRocks session vars, read back. **Added.** |
| `STAGING=32GiB` | **Not the 16 GiB default** — recovers q17 at SF500. |

### 3.5 Output layout

```
<outdir>/results.csv            engine,scale,query,run,phase,status,ms,rows
<outdir>/manifest.txt           what was requested
<outdir>/engineA/provenance.txt config as RESOLVED, dataset + fs, versions
<outdir>/engineA/cluster.log    FE + CN logs — the real errors are here
<outdir>/engineA/q<NN>.r<N>.out per-run output; on failure, the client error text
```

| `status` | Meaning |
|---|---|
| `pass` | exit 0, ≥1 row. **Not a correctness claim.** |
| `empty` | completed normally, 0 rows. Often a **silent wrong answer**. |
| `refused` | engine error — read `q<NN>.r<N>.out`. |
| `wedge` | hit the timeout. |

```bash
awk -F, 'NR>1{c[$6]++} END{for(s in c) print s,c[s]}' <outdir>/results.csv
```

### 3.6 Interpreting failures

| Symptom | Cause | Action |
|---|---|---|
| `exchange staging arena exhausted` | **The dominant Engine A failure.** Leases grow to fill any arena. | Not fixable by sizing — §5.3 |
| `Query reached its timeout of 300 seconds` | **FE `query_timeout` default.** Applies to A *and* B. | `SET GLOBAL query_timeout = 3600` |
| `empty`, 0 bytes, exit 0 | q15's non-deterministic wrong answer | §5.1 |
| Engine C refuses everything | `kvikio` missing | §2.4 |
| BE won't start | `storage_root_path` missing | `mkdir -p /raid/prestouser/sr-bench/be{1,2}/{storage,spill,log}` |
| "expected exactly N alive backends" | stale be3/be4 rejoined from FE metadata | Working as designed — abort and investigate |

---

## 4. Methodology

### 4.1 Datasets — all staged, no generation needed

| Scale | Path | Size |
|---|---|---|
| SF100 | `/raid/prestouser/aocsa/tpch_parquet_sf100` | 26 GB |
| **SF500** | `/raid/prestouser/aocsa/tpch_parquet_sf500` | **132 GB** |
| SF1000 | `/raid/tpch-sf1000` | 283 GB |

All ext4 on `/dev/md0`, local NVMe RAID0. **Not NFS** — that matters: on NFS, `use_odirect: true`
is a 12.5× regression.

### 4.2 Query set — [`bench/common/QUERYSET.md`](bench/common/QUERYSET.md)

**17 of 22.** `q05 q08 q09 q10 q18` are measured SF100 failures ([`TPCH-SF100-FAILURES.md`](TPCH-SF100-FAILURES.md)) — none is a query-shape problem; the pattern is *state accumulating across runs*, and every mechanism worsens with scale. Running them costs ~5.5 h of wedges and can poison later queries.

| Scale | Engine A completes |
|---|---|
| SF100 | **17/17** |
| SF500 | **14 reliably** + q15 unreliable (7/18) + q21 never (0/12) |
| SF1000 | **13/15** (q07, q17 refused) |

**Numerically wrong (FP64 defect): `q01 q03 q05 q07 q14 q15 q19`.** Reliable *and* sound at SF500 =
**9 queries**: `q02 q04 q06 q11 q12 q13 q16 q20 q22`.

### 4.3 Regimes — the single most important methodological point

| Regime | Definition |
|---|---|
| **cold** | Page cache dropped before **every** timed run — `posix_fadvise(DONTNEED)`, no root needed. Symmetric: `kvikio.drop_file_page_cache` for C, `PRE_QUERY_HOOK` for A/B. |
| **warm** | Page cache hot. |
| **run 0 vs steady** | Run 0 = first touch. Runs 1..N = steady state. |

> ### ⚠️ Never blend run 0 with steady state
> They give **opposite answers**. At SF100: run 0 → Sirius 1.26× (wins 14/17); steady → cudf-polars
> 1.39× (Sirius wins 4/17). A blended average lands between and describes neither. **I published
> such a figure earlier and it was wrong.**

Cause: cudf-polars' suite is **2.05×** slower on run 0 than steady (Ray spin-up, CUDA context,
kernel load); Sirius's only **1.17×**.

Also: **"cold" ≠ every run costs a first touch.** At SF100 (26 GB on ~25 GB/s NVMe) a re-read is
~1 s, small next to process warm-up.

### 4.4 Metrics

- **Wall time** — the measured quantity. Report run 0 and steady **separately**.
- **Aggregate over the shared set.** Coverage differs by engine; crediting Sirius for skipping its
  hardest queries is the easiest way to lie.
- **Cost** = `(wall_s / 3600) × $/hr` — **modeled**. Prefer the **break-even ratio**.
- **Price-free:** GPU-seconds (4 × wall), core-seconds (144 × wall).

### 4.5 Repeatability

- Verify idle before **every** run (§3.1).
- Pre-warm the NVRTC cache (`$HOME/.cudf/$VERSION/$ARCH`) — ~19 s cold. **Never** drop it as part
  of "cold"; that measures the compiler.
- `--runs 2` minimum. **Never 1** — q15 would report `pass` ~39% of the time.
- Verify NUMA with `/proc/<pid>/numa_maps`, **never** `Mems_allowed_list` (it reports the cpuset,
  which `--membind` does not change — false alarm every time).
- Avoid ~02:00–03:50 UTC.

---

## 5. Open issues, risks, next actions

### 5.1 🔴 Nothing is a correctness claim

`run-abc.sh` scores `pass` as *exit 0 and ≥1 row*; it **never compares values**. The oracle diff has
never run.

**Two distinct defects:**

**(a) `(1 − l_discount)` FP64 lowering — 7 queries wrong by ~0.1%.**
`experimental/starrocks/crates/starrocks-plan-translator/src/expr_translator.rs:459-481`
(`translate_arithmetic`) casts **both operands of every decimal `+ − * / %` to FP64**.
`git diff --stat 1d2bbae2..HEAD -- experimental/starrocks/crates/` is **empty** — untouched.
[`OPEN-ISSUES.md`](OPEN-ISSUES.md) **#24** warns that fixing the SUM/AVG lowering at `:826-833`
**changes nothing**. Start at `translate_arithmetic`. **Fixing it takes the headline set 8 → 14.**

**(b) q15 — silent, non-deterministic wrong row count.** Its predicate is
`total_revenue = (SELECT max(total_revenue) …)` — an **exact float equality** on a value (a) makes
unstable. Two CTE evaluations reduce in different orders → equality matches nothing → **0 rows, no
error, exit 0**. 7/18 pass. Under exact decimal this is impossible.

**Action:** build the oracle (`SET gpu_execution = false`) with **relative tolerance 1e-12** — not
string equality: q06 legitimately varies ±1 ULP and an exact comparator false-fails it ~⅓ of runs.

### 5.2 🔴 Two features unreachable from the CN — the biggest wins available

| Feature | Lives in | Worth |
|---|---|---|
| `expression_evaluator_strategy = 'ast_jit'` | `src/config.cpp:27`, set only via the DuckDB extension's `SET` handler | **−4.17% suite, −49.7% on q01** |
| `pin_table` (GPU-resident tables) | `src/sirius_extension.cpp` | Engine D pinned was **9.7× faster than Engine A** |

`experimental/starrocks/{src,crates}` contain **no** `ast_jit`, `pin_table`, `PIN_TIER` or
`PRE_SQL` path. Both are one wiring task, and both target exactly where Sirius loses.

> **Engine D (standalone, pinned, `ast_jit`): 2.40 s vs Engine A's 23.4 s on the same 15 queries.**
> That is where the performance is. Caveat: it conflates pinning + `ast_jit` + absence of the CN.
> **Next experiment: Engine D *unpinned, without* `ast_jit`** — the residual vs Engine A is then
> pure CN overhead.

### 5.3 🔴 Staging-arena lease lifecycle — the dominant failure mode

Costs **q21 at SF500**, **q07 + q17 at SF1000**.

```
exchange staging arena exhausted: requested 1225682880 bytes … (raise SIRIUS_EXCHANGE_STAGING_BYTES)
```

**Not a sizing problem.** Doubling 16 → 32 GiB doubled the outstanding lease count (13–25 → 32–36)
and still refused. Leases grow to fill whatever you give them. Matches the known q09 signature
("75 leases leaked across runs"). **An 80 GiB A100 cannot out-size this** — it blocks
[`bench/a100x8/`](bench/a100x8/) too.

### 5.4 🟠 Study 1 script has a placement bug

`engine-a.env` does `export CN_GPU="0 1 2 3"` with **no `${...:-}` default**, so it **overwrites**
any caller's setting. The 2-CN arm specced as GPU0+GPU2 actually ran on GPU0+GPU1 — the
suboptimal placement (both CNs on socket 0, socket 1 idle).

**Fix before running Study 1:** `export CN_GPU=${CN_GPU:-"0 1 2 3"}` (same for `CN_NODE`, `CN_CPUS`).

### 5.5 🟠 Sirius q07 @ SF1000 is contaminated

Its cold run died on the FE's default `query_timeout = 300 s` at 300,194 ms — recorded `refused`
for a reason unrelated to the engine. **Re-run with `query_timeout = 3600` before quoting that row.**

### 5.6 Recommended order

1. **Oracle diff** — everything else is unvalidated without it. Cheap.
2. **Fix `translate_arithmetic`** — one function; 8 → 14 usable queries.
3. **Wire `ast_jit` to the CN** — measured −49.7% on q01, the consistent weak spot.
4. **Root-cause the staging-lease lifecycle** — unblocks q21/q07/q17 and the A100 plan.
5. **Fix `engine-a.env`, then run Study 1** — the last unrun study.
6. **Re-run SF1000 q07** with the timeout raised.
7. **Measure FE + client overhead** (`SELECT 1` through the FE). Contributes to the small-table
   losses — but **cannot** explain q01, so it is no longer the leading hypothesis.
8. Decide on §1.4.

---

## 6. Quick start

```bash
# 0. Orient
cd ~/aocsa/sirius
cat bench/gb200-4gpu/PLAN.md          # what the studies are
cat bench/common/QUERYSET.md          # which queries, and why not the others
cat bench/gb200-4gpu/RESULTS-STUDIES.md

# 1. Box idle?  (want ~28-320 MiB, no compute apps)
nvidia-smi --query-gpu=index,memory.used --format=csv,noheader
nvidia-smi --query-compute-apps=pid,used_memory --format=csv

# 2. Prereqs
ls experimental/starrocks/target/release/sirius-starrocks-cn
ls ~/starrocks-bench/be1/conf/be.conf
(cd ~/aocsa && pixi run python -c "import cudf_polars,kvikio;print('C ok')")

# 3. Dry run
cd experimental/starrocks/benchmarks/tpch
./run-abc.sh --sf 500 --engines A --dry-run --out /tmp/dry \
  --data /raid/prestouser/aocsa/tpch_parquet_sf500

# 4. Smallest real run (~3 min): 3 queries, Engine A, SF500
export NUM_CNS=4 GPU_MEM=140GiB STAGING=32GiB HOST_MEM=160GiB
./run-abc.sh --sf 500 --engines A --runs 2 --queries "q02 q06 q22" \
  --data /raid/prestouser/aocsa/tpch_parquet_sf500 \
  --warm-timeout 300 --cold-timeout 600 --out /tmp/smoke
awk -F, 'NR>1{print $3,$5,$6,$7"ms"}' /tmp/smoke/results.csv

# 5. Reproduce the headline (~30 min)
bash ~/aocsa/benchmark-results/run-sf500-cold-AvsC.sh
```

**Checklist for any new measurement**

- [ ] Box idle, GPUs at floor
- [ ] `--dry-run` first
- [ ] `--data` passed explicitly
- [ ] Timeouts overridden
- [ ] `STAGING=32GiB`
- [ ] `--runs 2` minimum
- [ ] Regime named (cold/warm; run 0 vs steady) — **never blended**
- [ ] Aggregates over the **shared** query set
- [ ] Teardown clean

---

## 7. File map

### Plans & methodology (in-repo)
| Path | What |
|---|---|
| [`bench/gb200-4gpu/PLAN.md`](bench/gb200-4gpu/PLAN.md) | **The three studies.** Start here. |
| [`bench/gb200-4gpu/HARDWARE.md`](bench/gb200-4gpu/HARDWARE.md) | Box topology, CN→GPU→CPU→NUMA maps, memory arithmetic |
| [`bench/common/QUERYSET.md`](bench/common/QUERYSET.md) | 17-of-22, tiers, the defects |
| [`bench/common/RETARGETING.md`](bench/common/RETARGETING.md) | Moving to different hardware |
| [`bench/gb200-4gpu/engine-a-sirius.md`](bench/gb200-4gpu/engine-a-sirius.md) | **Engine A runbook** |
| [`bench/a100x8/`](bench/a100x8/) | 8× A100 target (not yet run) |
| [`bench/sf1000-repro/`](bench/sf1000-repro/) | Merged SF1000 campaign (PR #1371) — the tuning lineage |
| [`TPCH-SF100-FAILURES.md`](TPCH-SF100-FAILURES.md) | Why 5 queries are excluded |
| [`OPEN-ISSUES.md`](OPEN-ISSUES.md) | **#24** = the decimal defect |

### Results (in-repo)
| Path | What |
|---|---|
| [`bench/gb200-4gpu/RESULTS-STUDIES.md`](bench/gb200-4gpu/RESULTS-STUDIES.md) | By study |
| [`bench/gb200-4gpu/REPORT-cold-AvsC.md`](bench/gb200-4gpu/REPORT-cold-AvsC.md) | Study 2, SF100 + SF500 |
| [`bench/gb200-4gpu/REPORT-study3-cost-efficiency.md`](bench/gb200-4gpu/REPORT-study3-cost-efficiency.md) | Study 3 |
| [`bench/gb200-4gpu/RESULTS-sf500.md`](bench/gb200-4gpu/RESULTS-sf500.md) | Engine A failure analysis |

### Harness & scripts
| Path | What |
|---|---|
| `experimental/starrocks/benchmarks/tpch/run-abc.sh` | **The harness.** Modified — §1.4 |
| `experimental/starrocks/configs/gb200-4gpu/{cluster4-numa.sh,engine-a.env}` | Engine A launcher. **`engine-a.env` has the §5.4 bug** |
| `experimental/starrocks/configs/gb200-4gpu/engine-b/setup-engine-b-gb200.sh` | Engine B setup |
| `~/aocsa/benchmark-results/run-*.sh` | 15 run scripts, self-documenting |
| `~/aocsa/benchmark-results/tools/drop-page-cache-hook.sh` | Cold-cache hook |

### Raw results — `~/aocsa/benchmark-results/`
| Dir | Contents |
|---|---|
| `sf100-cold-AvsC/` | A vs C cold SF100 · A=51 C=51 |
| `sf500-cold-AvsC/` | A vs C cold SF500 · A=50 C=51 |
| `sf500-studies-2-3/` | A+B+C warm SF500 |
| `sf1000-engineA/` · `sf1000-engineB-tuned/` · `sf500-engineB-tuned/` | SF1000 + tuned B |
| `sf500-engineA-failhunt/` · `-retest/` | Failure analysis |
| `sf500-Btune-*/` · `sf500-coldprobe-*/` · `sf500-tuning-*/` | Tuning probes |
| `*.html` | 5 charts — `gpu-shootout.html` is the best of them |

### Source files the next agent will need
| Path | Why |
|---|---|
| `crates/starrocks-plan-translator/src/expr_translator.rs:459-481` | **The decimal defect** |
| `src/config.cpp:27` | `ast_jit` default — unreachable from the CN |
| `src/sirius_extension.cpp` | `pin_table` — unreachable from the CN |
| `experimental/starrocks/src/engine_settings.rs` | Derived config. Modified — §1.4 |
| `experimental/starrocks/src/main.rs:67-71, 260-284` | Why `--sirius-config` was unreachable |
