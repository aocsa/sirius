# Sirius vs cudf-polars, cold — SF100 and SF500

**Box:** `presto-gb200-gcn-17`, 4 × GB200 · **Date:** 2026-08-12 · **Sirius HEAD:** `4e6439c8`
**Data:** `/raid/prestouser/aocsa/tpch_parquet_sf{100,500}` (26 GB / 132 GB), ext4 on local NVMe
**Queries:** the 17 Engine A completes at SF100 — `q05 q08 q09 q10 q18` excluded (measured failures)

---

## The headline is that there isn't one number

**The winner depends on which runs you quote, and the two answers point opposite ways.**

### SF100

| | Sirius | cudf-polars | |
|---|---:|---:|---|
| **Run 0 — first touch** | **9,694 ms** | 12,244 ms | **Sirius 1.26× faster · wins 14/17** |
| **Runs 1–2 — steady state** | 8,290 ms | **5,970 ms** | cudf-polars 1.39× faster · Sirius wins 4/17 |

Quoting either alone is defensible; quoting one without the other is not.

> ### ⚠️ A blended average is an artifact — do not compute one
> Averaging run 0 together with runs 1–2 gives *"cudf-polars 1.09× faster, Sirius wins 8/17"* — a
> number that sits between two opposite results and describes neither. **I published that figure
> earlier in this session and it was wrong.** It is exactly the error the SF100 audit attributes to
> the original "Sirius beats cudf-polars" chart, in the other direction.

### Why the flip — warm-up cost, not throughput

| q01 | run 0 | steady | warm-up ratio |
|---|---:|---:|---|
| Sirius | 1908 | 1032 | **1.85×** |
| cudf-polars | 1835 | 392 | **4.68×** |

cudf-polars pays a large fixed start-up cost — Ray actor spin-up, CUDA context, cuDF kernel load —
then runs much faster per query. Sirius's cluster is already up, so its first query is cheap
relative to its steady state. That is the documented 1.4–4.6× Engine C first-touch penalty,
reproduced.

**So the two results answer two different questions**, and both are real:

| Question | Answer |
|---|---|
| One query against a fresh process? | **Sirius**, 1.26× |
| Sustained throughput over many queries? | **cudf-polars**, 1.39× |

### SF100 per-query (ms)

| query | A run0 | C run0 | C/A | A steady | C steady | C/A |
|---|---:|---:|---:|---:|---:|---:|
| q01 | 1908 | 1835 | 0.96× | 1032 | 392 | 0.38× |
| q02 | 617 | 407 | 0.66× | 480 | 174 | 0.36× |
| q03 | 489 | 794 | 1.62× | 448 | 359 | 0.80× |
| q04 | 328 | 683 | 2.08× | 266 | 239 | 0.90× |
| q06 | 338 | 619 | 1.83× | 310 | 436 | 1.41× |
| q07 | 816 | 970 | 1.19× | 670 | 528 | 0.79× |
| q11 | 360 | 356 | 0.99× | 354 | 124 | 0.35× |
| q12 | 349 | 747 | 2.14× | 340 | 448 | 1.32× |
| q13 | 305 | 413 | 1.35× | 288 | 303 | 1.05× |
| q14 | 371 | 757 | 2.04× | 374 | 384 | 1.03× |
| q15 | 676 | 728 | 1.08× | 618 | 368 | 0.60× |
| q16 | 292 | 298 | 1.02× | 238 | 223 | 0.94× |
| q17 | 677 | 699 | 1.03× | 728 | 348 | 0.48× |
| q19 | 398 | 751 | 1.89× | 408 | 384 | 0.94× |
| q20 | 557 | 896 | 1.61× | 566 | 498 | 0.88× |
| q21 | 999 | 1007 | 1.01× | 954 | 583 | 0.61× |
| q22 | 214 | 284 | 1.33× | 216 | 178 | 0.82× |
| **TOTAL** | **9694** | **12244** | **1.26×** | **8290** | **5970** | **0.72×** |

**Engine A completes all 17 at SF100**, including q15 and q21 — both of which fail at SF500. Those
failures are scale-dependent, not query-shape problems.

---

## SF500

**Engine A completes 15 of 17** — q15 `empty`, q21 `refused`, exactly as the three prior sweeps
predicted. cudf-polars completes 17/17. All aggregates below are over the shared 15.

| | Sirius | cudf-polars | |
|---|---:|---:|---|
| **Run 0 — first touch** | **25,152 ms** | 25,800 ms | **Sirius 1.03× faster · wins 10/15** |
| **Runs 1–2 — steady state** | 23,021 ms | **18,919 ms** | cudf-polars 1.22× faster · Sirius wins 7/15 |

### The finding: cold halves cudf-polars' advantage at SF500

| SF500 measurement | Result |
|---|---|
| **Warm** (earlier run, page cache hot) | cudf-polars **2.22×** faster, Sirius wins **0/15** |
| **Cold, steady state** | cudf-polars **1.22×** faster, Sirius wins **7/15** |
| **Cold, run 0** | **Sirius 1.03× faster**, wins **10/15** |

Same engines, same box, same data, same queries — **only the I/O regime changed.** The 2.22× warm
gap was substantially an artifact of measuring with the data already in page cache, which removes
the I/O work Sirius does comparatively well and leaves only the per-query overhead it does badly.

This also resolves the SF100-vs-SF500 question left open earlier: the shift was **not** mainly
scale factor. Cold at both scale factors gives the same shape — Sirius at or ahead on first touch,
cudf-polars ahead in steady state — with SF500 slightly *more* favourable to Sirius than SF100.

### SF500 per-query (ms)

| query | A run0 | C run0 | C/A | A steady | C steady | C/A |
|---|---:|---:|---:|---:|---:|---:|
| q01 | 5297 | 3180 | 0.60× | 4407 | 1461 | 0.33× |
| q02 | 1040 | 728 | 0.70× | 920 | 306 | 0.33× |
| q03 | 1412 | 2220 | 1.57× | 1428 | 1606 | 1.12× |
| q04 | 875 | 1434 | 1.64× | 844 | 876 | 1.04× |
| q06 | 989 | 1519 | 1.54× | 966 | 1330 | 1.38× |
| q07 | 2558 | 2767 | 1.08× | 2545 | 2400 | 0.94× |
| q11 | 862 | 690 | 0.80× | 808 | 280 | 0.35× |
| q12 | 1059 | 1660 | 1.57× | 1091 | 1258 | 1.15× |
| q13 | 1174 | 1212 | 1.03× | 1151 | 972 | 0.84× |
| q14 | 1353 | 2203 | 1.63× | 1332 | 1794 | 1.35× |
| **q15** | **EMPTY** | 1952 | — | — | 1586 | — |
| q16 | 548 | 663 | 1.21× | 532 | 442 | 0.83× |
| q17 | 3104 | 2023 | 0.65× | 3099 | 1677 | 0.54× |
| q19 | 1374 | 2152 | 1.57× | 1420 | 1924 | 1.35× |
| q20 | 1902 | 2700 | 1.42× | 1882 | 2160 | 1.15× |
| **q21** | **REFUSED** | 2677 | — | — | 2084 | — |
| q22 | 1605 | 649 | 0.40× | 594 | 431 | 0.72× |
| **TOTAL (15)** | **25152** | **25800** | **1.03×** | **23021** | **18919** | **0.82×** |

At SF500 the warm-up term matters far less than at SF100 — Sirius's run 0 ÷ steady is only
**1.09×**, cudf-polars' **1.36×** — because the I/O work now dwarfs process start-up.

### What separates the wins from the losses

> **CORRECTION.** An earlier version of this report said Sirius "loses the ones dominated by fixed
> per-query cost." **The data does not support that.** A fixed per-query overhead predicts that the
> ratio improves as queries get longer; the measured correlation between Sirius duration and the
> C/A ratio is **−0.307 — the wrong sign**. The decisive counterexample is **q01: Sirius's longest
> query at 4407 ms and its worst ratio (0.33×)**. 4.4 seconds amortises any per-query setup cost.

Sorting by what the query actually touches gives a much cleaner split:

| Sirius **wins** | ratio | shape | Sirius **loses** | ratio | shape |
|---|---|---|---|---|---|
| q06 | 1.38× | `lineitem` scan/filter/agg | q01 | 0.33× | `lineitem`, heaviest expression eval |
| q14 | 1.35× | `lineitem`+`part` | q02 | 0.33× | 5 small tables + correlated subquery |
| q19 | 1.35× | `lineitem`+`part` | q11 | 0.35× | `partsupp`/`supplier`/`nation`, HAVING subquery |
| q12 | 1.15× | `lineitem`+`orders` | q17 | 0.54× | correlated subquery over `part` |
| q20, q03, q04 | 1.04–1.15× | `lineitem`-driven | q22, q16, q13 | 0.73–0.84× | small tables / correlated |

**Two distinct mechanisms, not one:**

1. **Small-table and correlated-subquery work.** Where there is little `lineitem` to scan, there is
   little for 4 GPUs to divide, so fragment distribution and exchange cost more than they buy.
   This covers q02, q11, q17, q22, q16, q13.
2. **Expression-heavy aggregation — q01 alone.** Pure `lineitem`, but the heaviest expression
   evaluation in the suite. It is the query `ast_jit` was worth **−49.7%** on in the SF1000
   campaign, and Engine A **structurally cannot reach `ast_jit`** (`src/config.cpp:27`, settable
   only through the DuckDB extension). It is also where the FP64 decimal defect lives.

A floor effect does exist — Sirius's fastest query is 532 ms against cudf-polars' 280 ms — but at
~250 ms it is far too small to explain a 4407 ms vs 1461 ms gap on q01.

---

## Cross-scale summary

| Scale | Regime | Run 0 | Steady state |
|---|---|---|---|
| SF100 | cold | **Sirius 1.26×** (14/17) | cudf-polars 1.39× (4/17) |
| SF500 | cold | **Sirius 1.03×** (10/15) | cudf-polars 1.22× (7/15) |
| SF500 | warm | — | cudf-polars **2.22×** (0/15) |

**Sirius is competitive with cudf-polars when both pay real I/O cost, at both scale factors.**
The large warm-mode deficit is a property of the measurement regime, not of the engine.

---

## On "cold" — what was actually achieved

| Engine | Mechanism |
|---|---|
| cudf-polars | `--io-mode cold` → `kvikio.drop_file_page_cache` before each iteration |
| Sirius | `PRE_QUERY_HOOK` → the same `posix_fadvise(POSIX_FADV_DONTNEED)` sweep, before each timed run, outside the timing window |

Neither needs root — `fadvise` evicts clean pages for any reader. Verified directly: warming a
2.7 GB parquet file moved `buff/cache` 2 → 4 GB; the drop returned it to 2 GB.

> **But the page cache is not the dominant term.** Runs 1–2 still came in at roughly half of run 0
> despite the cache being dropped before every one. At SF100 the dataset is 26 GB on NVMe RAID0
> (~25 GB/s), so a re-read costs ~1 s — small next to process warm-up. **"Cold" here means the page
> cache was dropped, not that every run cost the same as a first touch.**
>
> `kvikio` was **not installed** in the cudf-polars env; `--io-mode cold` raises
> `RuntimeError: kvikio is required for cold-run page cache dropping`. A first attempt recorded
> C as refusing all 51 runs — a configuration error, not a cudf-polars failure. Installed
> `kvikio-cu13==26.8.*` (the env had no `pip`; bootstrapped via `ensurepip`).

---

## Tuning: four attempts, no effect

Every Sirius config knob available was swept. None moved the result outside noise.

| # | What was tried | Regime | Result |
|---|---|---|---|
| 1 | `--sirius-config`, 4 variants of `scan_task_batch_size` × `uring_n_reactors` | warm SF500 | best **+2.2%**, 1.3% spread |
| 2 | Full `bench/sf500-gb200/sirius-sf500.yaml` ported into the Rust derive path | warm SF500 | **−3.2%** (slower) |
| 3 | Cold-mode probe: base / nvme / mid | **cold SF500** | **2.4% spread**, best 1.4% |
| 4 | `enable_prefetch_cache: true` | both | **2.1× regression** (two independent sources) |

**The scan knobs do not control Engine A's performance.** Attempts 1–2 ran warm, so they had no
I/O to govern — that was expected. Attempt 3 was the real test: cold, at 5× the data, where reads
actually hit NVMe. Still inert.

### A structural finding worth more than the tuning

`--sirius-config` is declared `conflicts_with_all [gpu_memory_limit, gpu_memory_fraction,
host_memory_limit]` (`main.rs`), and `resolve()` prefers the *derived* YAML whenever any memory
flag is set. **Every benchmark launcher sets `--gpu-memory-limit`, so a tuned config was
unreachable by construction** — every Engine A run ever made on this box used memory limits plus
CPU affinity and nothing else. That is now fixed (tuning emitted from the derive path, every value
env-overridable). It simply turned out not to be worth much.

### Two features Engine A structurally cannot use

| Feature | Where it lives | Reachable from the CN? |
|---|---|---|
| `expression_evaluator_strategy = 'ast_jit'` | `src/config.cpp:27`, set only via the DuckDB extension's `SET` handler | **No** — no FE→config path. Worth −4.17% suite (q06 −49.7%, q12 −21.9%) |
| `pin_table` (GPU-resident tables) | `src/sirius_extension.cpp` | **No** — `experimental/starrocks/{src,crates}` contain no `pin_table`/`PIN_TIER` path |

Both are one wiring task each, and both target exactly the queries where cudf-polars leads most.

---

## What none of this establishes

- **No correctness claim.** `run-abc.sh` scores `pass` as *exit 0 and ≥ 1 row* and **never compares
  values**. The DuckDB oracle diff has not been run.
- **Seven Sirius results are numerically wrong** — `q01 q03 q05 q07 q14 q15 q19`, the decimal→FP64
  defect at `expr_translator.rs:459-481`, still open at HEAD.
- **SF100 measures neither engine's memory management.** 26 GB against 740 GiB of aggregate HBM
  fits trivially; SF500/SF1000 is where they diverge.
- **Overhead is not matched.** Sirius's times include a MySQL round trip through the StarRocks FE;
  cudf-polars' are in-process `collect()`. Never measured.

## Reproduce

```
benchmark-results/run-sf100-cold-AvsC.sh      -> sf100-cold-AvsC/
benchmark-results/run-sf500-cold-AvsC.sh      -> sf500-cold-AvsC/
benchmark-results/run-sf500-coldprobe.sh      -> sf500-coldprobe-{base,nvme,mid}/
benchmark-results/tools/drop-page-cache-hook.sh
```
