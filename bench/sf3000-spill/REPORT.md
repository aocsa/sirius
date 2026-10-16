# SF3000 spill compression on one GPU — PRs #2182 / #2183 / #2204

**Bottom line:** spill compression is correct (22/22 identical results) and **12.2% faster cold,
6.4% faster warm** at SF3000 on one GB200 capped to g7e.8xlarge memory. That is well short of the
PR's −38% (2099 s → 1295 s). The same three queries gain most as on g7e (q3, q21, q17), and disk
spill falls 76%, so the mechanism reproduces; the size of the win does not, because this box's
uncompressed baseline spills much less to disk.

## Setup

| | |
|---|---|
| Branch | `integ/spill-compression-sf3k` @ `a1cfdb14` = upstream `main` (`f32589fb`) + merge of `stacked/spill-compression` (all 12 commits of #2182 → #2183 → #2204, clean merge) |
| Checkout | `/scratch/prestouser/aocsa/spillcomp/sirius` (release build, `pixi run make`) |
| Hardware | 1 × GB200 (`CUDA_VISIBLE_DEVICES=0`), GPU capped at **91 GB** (0.95 × 96 GB, g7e.8xlarge's RTX PRO 6000) |
| Data | `/scratch/sirius/datasets/tpch_sf3000` (1.1 TB parquet, decimal128, GPFS) |
| Spill dir | `/scratch/prestouser/aocsa/spill` (GPFS) |
| Runner | `bench/sf3000-spill/run.sh` → `test/tpch_performance/run_tpch_parquet.sh`, one DuckDB session per arm, 22 queries × 2 iterations (cold, then warm), nothing pinned |
| Toggle | YAML `sirius.compression.enable_spill_compression: false / true`; everything else identical |

## Result: compression OFF vs ON (100 GB host pool)

| | OFF | ON | Change |
|---|---|---|---|
| Cold, sum of 22 | 1,561.9 s | 1,370.6 s | **−12.2% (1.14×)** |
| Warm, sum of 22 | 1,459.4 s | 1,365.6 s | −6.4% (1.07×) |
| Session wall time | 3,028 s | 2,743 s | −9.4% |
| Spilled to host | 4,192 GB | 4,167 GB | ≈ |
| Spilled to disk | 1,501 GB | **364 GB** | **−76%** |
| Results | — | 22/22 identical | ✓ |

Per query (seconds; saved = OFF − ON):

| q | OFF cold | ON cold | saved | OFF warm | ON warm | saved |
|---|---|---|---|---|---|---|
| 1 | 42.5 | 42.6 | −0.1 | 34.0 | 34.0 | 0.1 |
| 2 | 29.9 | 30.1 | −0.3 | 20.0 | 20.0 | 0.1 |
| **3** | 137.4 | 79.4 | **58.1** | 118.4 | 78.9 | **39.4** |
| 4 | 56.8 | 47.5 | 9.3 | 55.0 | 56.0 | −1.0 |
| 5 | 84.9 | 74.1 | 10.8 | 70.4 | 73.6 | −3.3 |
| 6 | 48.2 | 50.6 | −2.4 | 48.3 | 50.6 | −2.3 |
| 7 | 86.3 | 82.1 | 4.2 | 78.1 | 82.3 | −4.1 |
| 8 | 84.1 | 75.2 | 8.9 | 70.2 | 75.6 | −5.3 |
| 9 | 93.3 | 95.3 | −2.0 | 92.1 | 97.9 | −5.8 |
| 10 | 76.6 | 75.4 | 1.2 | 74.2 | 74.1 | 0.1 |
| 11 | 14.6 | 13.8 | 0.8 | 13.9 | 13.7 | 0.2 |
| 12 | 68.2 | 56.5 | 11.7 | 54.3 | 65.4 | −11.1 |
| 13 | 26.9 | 19.5 | 7.4 | 18.4 | 18.3 | 0.0 |
| 14 | 56.6 | 57.8 | −1.2 | 56.8 | 59.7 | −2.9 |
| 15 | 57.0 | 56.3 | 0.6 | 59.5 | 55.7 | 3.7 |
| 16 | 7.2 | 6.6 | 0.5 | 7.0 | 6.6 | 0.4 |
| **17** | 110.4 | 85.2 | **25.2** | 109.8 | 84.5 | **25.2** |
| 18 | 86.4 | 82.5 | 3.9 | 80.3 | 78.1 | 2.1 |
| 19 | 71.4 | 71.4 | 0.0 | 71.5 | 70.9 | 0.6 |
| 20 | 65.3 | 65.0 | 0.3 | 64.9 | 64.9 | 0.0 |
| **21** | 247.4 | 193.1 | **54.3** | 250.4 | 194.0 | **56.4** |
| 22 | 10.6 | 10.4 | 0.2 | 11.9 | 10.5 | 1.4 |
| **sum** | **1561.9** | **1370.6** | **191.3** | **1459.4** | **1365.6** | **93.8** |

## Against the atlas chart ("compression benefit is concentrated in a few expensive queries")

- **Ranking reproduces exactly.** Top three cold savings are q3, q21, q17, in the chart's order.
  Everything else is within ±12 s; q9, q6 and q14 are slightly negative, as in the chart.
- **Magnitude is about half.** Chart (g7e, 100 GB host / 40 GB cache): q3 +123 s, q21 +97 s,
  q17 +93 s, 1.61× cold / 1.63× hot. Here: +58 / +54 / +25 s, 1.14× cold / 1.07× warm.
- **Why smaller:** compression's win is spill that no longer reaches disk. Our OFF arm spilled
  1.5 TB to disk versus the PR's 3.4 TB, so there is less disk time to remove; this box also has
  far more CPU than a g7e.8xlarge (144 vs 32 cores).

## Best atlas config ("200 GiB host / 20 GB cache / ON", ~420–460 s on g7e)

The cache part could not be reproduced (see Issues 1), so the closest valid stand-in ran: 200 GiB
host pool, cache off, compression ON, operator batch/partition sizes derived from the 91 GB GPU.

| | OFF (100 GB host) | ON (100 GB host) | ON (200 GiB host) |
|---|---|---|---|
| Cold, sum of 22 | 1,561.9 s | **1,370.6 s** | 1,415.6 s |
| Warm, sum of 22 | 1,459.4 s | **1,365.6 s** | 1,409.3 s |
| Session wall time | 3,028 s | 2,743 s | 2,836 s |
| Spilled to host / disk | 4,192 / 1,501 GB | 4,167 / 364 GB | 3,990 / 404 GB |
| Encode-OOM fallbacks | — | 164 | 34 |
| Results vs OFF | — | 22/22 identical | 22/22 identical |

- **The bigger host pool did not help: +3.3% cold vs the 100 GB-host ON arm.** q1–q20 are within
  noise; q21 is 32 s slower cold, and disk spill did not drop (404 vs 364 GB), so q21's spill still
  overflows 200 GiB. Fewer encode OOMs (34 vs 164) did not translate into time.
- **~500 s is out of reach on this setup.** The best measured suite is 1,371 s cold — 3× the
  atlas's 456.7 s. The gap is not spill (q1/q6 never spill and still take 42/50 s cold versus a
  few seconds' worth of the atlas total). Likely contributors: reading 1.1 TB of parquet from GPFS
  per pass instead of S3 + prefetching cache, the atlas harness's warmup/pinning protocol, and
  `ast_jit` being off. Closing it needs the atlas's exact config and harness, not more spill tuning.

## Issues found

1. **The atlas "20 GB cache" cannot be reproduced on this build.** With
   `scan_manager.cache: {mode: sirius, eviction: lru, eviction_threshold_fraction: 0.093}` the
   prefetching cache kept every chunk it read and took the entire 200 GiB host pool during q1
   (`free_blocks=0`). Scans then hit 600+ host-block OOMs, one task exceeded its 100 retries,
   two queries fell back to DuckDB CPU, and spill went straight to disk (732 GB disk vs 3 GB host
   after three queries). q1 ran 86/88 s vs 42/34 s. Run stopped after q3. The knob that produced
   "20 GB cache" in the atlas is unknown — ask whoever ran it.
2. **Encode OOM fallbacks (#2204).** 164 spills hit
   `simpatico::codegen: cpp encode: exception: std::bad_alloc: out_of_memory` and fell back to
   uncompressed, as designed — in q3, q9, q10, q17, q20, q21, the queries that benefit most.
   Likely leaves some of the win on the table; worth raising with the PR author.
3. **Those messages go to stdout.** simpatico prints them to the stream the runner captures as
   `result.txt`, so 6 queries looked wrong until filtered. `compare.py` now drops `simpatico::` lines.
4. **Stale repro YAML on `main`.** `bench/sf1000-repro/sirius-sf1000.yaml` still sets
   `scan_manager.use_sirius_datasource` and `enable_prefetch_cache`, which `main` rejects at load.
5. **Python harness cannot load the extension.** Since the DuckDB v2 migration (#2143),
   `performance_test.py`'s pixi DuckDB 1.5.6 client fails to `LOAD` the extension; the CLI runner
   was used instead, which means `ast_jit` (set only via `SET`) was off in every arm.

## Files

All under `/scratch/prestouser/aocsa/spillcomp/sirius/bench/sf3000-spill/` (uncommitted):

- `run.sh`, `sirius-sf3000-spill.yaml` — OFF/ON A/B (100 GB host)
- `sirius-sf3000-atlas.yaml` — cache config that failed; `sirius-sf3000-host200-nocache.yaml` — 200 GiB host stand-in
- `compare.py` — per-query table, result equality, spill totals
- `results/20261016_190414_spill_{off,on}/`, `results/compare_off_vs_on.txt`
- `results/20261016_204028_atlas_spill_on/` (stopped), `results/20261016_204850_host200_spill_on/`
- `results/compare_on100_vs_host200.txt`, `results/compare_off_vs_host200.txt`
