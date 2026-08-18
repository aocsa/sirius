# Study 3 — Cost Efficiency · Sirius (GPU) vs StarRocks (CPU)

**Box:** `presto-gb200-gcn-17` · 4 × GB200, 144 Grace cores · **Date:** 2026-08-12
**Data:** `/raid/tpch-sf1000` (283 GB), `/raid/prestouser/aocsa/tpch_parquet_sf500` (132 GB), local NVMe
**Queries:** the 15 Engine A completes at SF500 · **Regime:** cold, page cache dropped before every run

---

## What is measured and what is modeled

**Both engines ran on the same physical box** — same data, same filesystem, same protocol, full
teardown between. So the **timing comparison is exact**, which no cloud pairing could claim.

**The dollar figures are modeled, not measured.** `presto-gb200-gcn-17` is on-prem and has no
`$/hr`. The prices below are the declared reference instances, used as stand-ins:

| Role | Instance | $/hr |
|---|---|---|
| GPU (stand-in for this box) | 8× A100 80 GB | $13.25 |
| CPU | `m8gd.48xlarge` — 192 vCPU Graviton4 | $8.83 |
| CPU (x86 ref) | `m8i.48xlarge` — 192 vCPU Intel | $12.19 |

> A 4× GB200 box would realistically cost **more** than an 8× A100 box, so every Sirius cost
> figure here is a **floor**, not an estimate. Treat the break-even ratio as the result and the
> dollars as illustration.

---

## Headline — SF1000, 13 shared queries

| | Wall time | $/run | Price-free |
|---|---:|---:|---|
| **Sirius (GPU)** | **34.56 s** | **$0.1272** | 138.2 GPU-seconds |
| StarRocks (CPU) — `m8gd` | 147.74 s | $0.3624 | 21,274 core-seconds |
| StarRocks (CPU) — `m8i` | 147.74 s | $0.5003 | 21,274 core-seconds |

> ### Sirius is **4.27× faster** and **2.85× cheaper** per run than Graviton4 StarRocks
> (3.93× cheaper vs Intel)

### Break-even — the result that survives price changes

Sirius must beat StarRocks by `price_gpu / price_cpu` to break even:

| vs | Break-even needed | Delivered | Margin |
|---|---:|---:|---:|
| `m8gd.48xlarge` | 1.50× | **4.27×** | cleared by **2.8×** |
| `m8i.48xlarge` | 1.09× | **4.27×** | cleared by **3.9×** |

**The cost case is not marginal.** Even if the GPU box cost 2.8× an 8× A100 instance, Sirius would
still break even against Graviton4 StarRocks.

---

## The finding that must accompany the headline

**StarRocks completed 15 of 15 at SF1000. Sirius completed 13.**

| query | Sirius | StarRocks |
|---|---|---|
| q07 | **REFUSED** — exchange staging arena exhausted (requested 1,635,063,680 B) | 15,184 ms |
| q17 | **REFUSED** — same, requested 1,225,682,880 B | 15,108 ms |

The 4.27× is over the **shared 13 only**. Sirius has no full-suite time at SF1000, and **you cannot
price a run that does not finish.** For a workload that needs all 15 queries, Sirius's cost per run
at SF1000 is currently **undefined**, not low.

This is the same staging-lease bug that costs q21 at SF500. It is now the **dominant failure mode**
of Engine A, and it is not fixable by sizing — doubling the arena 16 → 32 GiB previously just
doubled the outstanding lease count (13–25 → 32–36) and still refused.

### Per-query, SF1000 (cold, steady state, ms)

| query | Sirius | StarRocks | B/A |
|---|---:|---:|---:|
| q01 | 9450 | 9314 | **0.99×** |
| q02 | 1296 | 10572 | 8.16× |
| q03 | 3204 | 24596 | 7.68× |
| q04 | 1486 | 12046 | 8.11× |
| q06 | 2404 | 4961 | 2.06× |
| **q07** | **REFUSED** | 15184 | — |
| q11 | 1457 | 12276 | 8.43× |
| q12 | 1976 | 11140 | 5.64× |
| q13 | 2126 | 12926 | 6.08× |
| q14 | 3106 | 8010 | 2.58× |
| q16 | 534 | 4726 | 8.84× |
| q19 | 3094 | 9569 | 3.09× |
| q20 | 3327 | 11238 | 3.38× |
| q22 | 1100 | 16361 | **14.87×** |
| **q17** | **REFUSED** | 15108 | — |
| **TOTAL (13)** | **34,560** | **147,736** | **4.27×** |

**The spread is 0.99× to 14.87×.** A geomean would hide that; do not collapse this table to one
number without showing the range. **q01 is a dead heat** — Sirius's largest query is no faster than
144 Grace cores, and it is the same query where the FP64 defect lives and where cudf-polars beat
Sirius 3× at SF500. It is the consistent weak spot in every comparison run.

---

## SF500 — the cleanest measurement in this study

**Both cold. Both tuned. All 15 queries complete on both engines. Same box.** No coverage
restriction, no regime mismatch, no missing queries. This is the number to quote.

| | Wall time | $/run | |
|---|---:|---:|---|
| **Sirius (GPU)** | **23.02 s** | **$0.0847** | |
| StarRocks — `m8gd` | 101.91 s | $0.2500 | **2.95× more** |
| StarRocks — `m8i` | 101.91 s | $0.3451 | **4.07× more** |

> ### Sirius is **4.43× faster** and **2.95× cheaper** per run — over the complete 15-query set

| vs | Break-even needed | Delivered | Margin |
|---|---:|---:|---:|
| `m8gd.48xlarge` | 1.50× | **4.43×** | cleared by **3.0×** |
| `m8i.48xlarge` | 1.09× | **4.43×** | cleared by **4.1×** |

### Per-query, SF500 (cold, steady state, tuned both sides, ms)

| query | Sirius | StarRocks | B/A |
|---|---:|---:|---:|
| q01 | 4407 | 5034 | **1.14×** |
| q02 | 920 | 6786 | 7.38× |
| q03 | 1428 | 11596 | 8.12× |
| q04 | 844 | 7237 | 8.57× |
| q06 | 966 | 2590 | 2.68× |
| q07 | 2545 | 6994 | 2.75× |
| q11 | 808 | 7911 | 9.78× |
| q12 | 1091 | 7060 | 6.47× |
| q13 | 1151 | 5174 | 4.50× |
| q14 | 1332 | 3670 | 2.76× |
| q16 | 532 | 6759 | **12.69×** |
| q19 | 1420 | 4444 | 3.13× |
| q20 | 1882 | 6911 | 3.67× |
| q22 | 594 | 4088 | 6.88× |
| q17 | 3099 | 15656 | 5.05× |
| **TOTAL (15)** | **23,021** | **101,908** | **4.43×** |

**q01 is again the outlier at 1.14×** — the same weak spot as at SF1000 (0.99×) and against
cudf-polars. Every other query is 2.68×–12.69×.

### Scale trend

| Scale | Coverage | Sirius | StarRocks | Ratio |
|---|---|---:|---:|---:|
| **SF500** | **15/15 both** | 23.02 s | 101.91 s | **4.43×** |
| SF1000 | 13 shared (Sirius refused 2) | 34.56 s | 147.74 s | 4.27× |

Sirius's advantage is **flat-to-slightly-declining** with scale — 4.43× → 4.27× — and the SF1000
figure is additionally propped up by excluding the two queries Sirius could not run. Earlier
readings that suggested the advantage *grows* with scale were comparing mismatched regimes.

---

## Tuning: the engines are not symmetric

| Engine | Tuning attempts | Best result |
|---|---|---|
| StarRocks | 4-arm session-variable probe | **`pipeline_dop = 144` → −12.0%** (q01 −32.3%) |
| Sirius | 4 sweeps, incl. cold at SF500 | **±3%, sign unstable.** One full config port made it **3.2% slower** |

**StarRocks' defaults were leaving the box underused; Sirius's knobs had nothing to give.** All
StarRocks numbers here use the tuned config, so this comparison is Sirius-untunable vs
StarRocks-at-its-best — the conservative direction for the Sirius claim.

**Deliberately not enabled:** StarRocks `datacache`, which caches external parquet blocks. Sirius
has no equivalent, so enabling it would be the same category of unfairness as pinning. `be.conf`
was already correct for this box: `num_cores = 72` × 2 BEs (mandatory — `CpuInfo` ignores the
cpuset), `mem_limit = 240G` **absolute** (a percentage resolves against ~1692 GB because
`/proc/meminfo` counts GPU HBM, and with `Swap: 0` that is an OOM-kill).

---

## Traps encountered, worth recording

**The FE caps every query at `query_timeout = 300 s` by default.** Engine A runs through the
StarRocks FE, so this applies to *both* engines regardless of harness timeouts. It killed Sirius's
q07 cold run at SF1000 at exactly 300,194 ms — recorded as `refused` for a reason unrelated to the
engine. All Engine B runs here set `query_timeout = 3600`. **Sirius's SF1000 q07 result is
contaminated by this** and should be re-run with it raised.

**Stale GPU memory nearly contaminated two measurements.** Once from a stray `duckdb` row-count
holding 180 GB, once from CNs surviving a killed wrapper in a new process group (`pgrep -f` missed
them; `nvidia-smi --query-compute-apps` found them). Verify the ~28 MiB floor before every run.

---

## What this does not establish

- **No correctness claim.** The harness scores `pass` as *exit 0 and ≥ 1 row* and never compares
  values. Five queries in the SF1000 table (`q01 q03 q14 q19`, and `q07` had it been run) are
  numerically wrong from the decimal→FP64 defect at `expr_translator.rs:459-481`, open at HEAD.
  The DuckDB oracle diff has not been run.
- **The GPU price is invented.** Only wall time, GPU-seconds and core-seconds are measured.
- **Energy is not measured.** A GPU box at load draws far more than a CPU box; a full TCO
  comparison would need power, which was not sampled.

## Reproduce

```
benchmark-results/run-sf1000-engineA.sh       -> sf1000-engineA/
benchmark-results/run-B-tuned-costeff.sh      -> sf1000-engineB-tuned/, sf500-engineB-tuned/
benchmark-results/run-sf500-B-tuneprobe.sh    -> sf500-Btune-{base,dop144,io64,combo}/
```
