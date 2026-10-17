# TPC-H SF3000 `8xl-sf3k` reproduction on one GB200

Reproduces the `sirius-spill-reproducer` package's **g7e.8xlarge SF3000** profile (200 GiB host,
20 GB cache, Simpatico spill compression ON) on one GPU of a 4× GB200 box, reading parquet from
local GPFS instead of S3. The package's own `run.py`, `validate.py` and `summarize.py` drive
every run; only the GPU limit and spill path are patched (see `reproducer-gb200.patch`).

| Mode | g7e.8xlarge | GB200 (this run) | Ratio | Validation |
|---|---:|---:|---:|---|
| Cold, 22 queries | 456.652 s | **558.980 s** | 1.22× | 22/22 vs saved g7e outputs |
| Lukewarm, median of 2 sweeps | 426.627 s | **542.932 s** (540.764, 545.099) | 1.27× | 66/66 (warmup + 2 sweeps) |
| Hot, Q1–Q20 only | 354.456 s | 450.905 s | 1.27× | stopped during Q21; 40/40 results identical to the validated cold run |

Engine: `main` `f32589fb` + #2182/#2183/#2204 @ `0997c1f3` + #2155 @ `ba9cffa4`. A second engine
with the package's exact tested tree (`68fee848`) was built but not benchmarked. Exact commits,
submodule pins and toolchain: [`COMMITS.md`](COMMITS.md).

## Findings

- **The package config is what matters.** An earlier hand-built 8xl config on the same engine took
  1,143.9 s cold (`../sf3000-spill/REPORT.md`). The package's settings — 16 pipeline workers,
  readahead 48 scans / 4 GB, 1 GB scan batches, 4 GB hash partitions, 1 MiB host blocks,
  DuckDB `threads=32` — halve that to 559 s.
- **The remaining 22% gap sits in scan-heavy queries.** Q2 3.0×, Q1 2.0×, Q3 1.7×, Q4 1.5×,
  Q21 1.5×. Data comes from GPFS here and from S3 through #2155's fused reads on g7e.
  Compute-heavy queries match or win: Q9 1.01×, Q11 1.00×, Q13 0.64×, Q17 0.76×, Q22 0.56×.
- **The 20 GB cache barely helps on GPFS.** Lukewarm is 3% faster than cold here, 7% on g7e. The
  cache holds <1% of SF3000 and GPFS has no S3 round trips to hide.
- **Spill compression is active.** A debug-log q3 run compressed 884 spilled batches,
  364.5 GB → 91.6 GB (3.98×), all decompressed back. Successful compressions log only at DEBUG.
- **The local dataset is equivalent.** `/scratch/sirius/datasets/tpch_sf3000` (tpchgen 2.0.2)
  produces outputs identical to the package's saved g7e outputs from S3 (tpchgen-rs `cdcf74de`).

Per query (seconds):

| Query | g7e cold | here cold | ratio | g7e lukewarm | here lukewarm | ratio |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 11.3 | 22.1 | 1.96x | 11.4 | 16.0 | 1.41x |
| Q2 | 4.3 | 13.0 | 3.04x | 4.8 | 6.7 | 1.40x |
| Q3 | 28.2 | 46.7 | 1.66x | 24.6 | 38.9 | 1.58x |
| Q4 | 12.1 | 18.7 | 1.54x | 12.0 | 18.2 | 1.52x |
| Q5 | 19.2 | 23.5 | 1.22x | 19.2 | 24.0 | 1.25x |
| Q6 | 12.1 | 17.1 | 1.41x | 12.0 | 16.4 | 1.37x |
| Q7 | 21.2 | 25.9 | 1.22x | 21.8 | 25.2 | 1.16x |
| Q8 | 23.6 | 24.6 | 1.04x | 23.8 | 25.9 | 1.09x |
| Q9 | 49.1 | 49.5 | 1.01x | 53.6 | 51.3 | 0.96x |
| Q10 | 27.4 | 36.5 | 1.33x | 23.0 | 36.4 | 1.58x |
| Q11 | 5.1 | 5.1 | 1.00x | 5.7 | 5.1 | 0.91x |
| Q12 | 17.9 | 21.1 | 1.18x | 12.9 | 20.7 | 1.61x |
| Q13 | 13.0 | 8.3 | 0.64x | 13.3 | 8.3 | 0.63x |
| Q14 | 16.3 | 18.7 | 1.15x | 16.9 | 19.2 | 1.14x |
| Q15 | 15.5 | 19.2 | 1.24x | 16.3 | 18.5 | 1.14x |
| Q16 | 2.6 | 2.7 | 1.06x | 2.8 | 2.9 | 1.03x |
| Q17 | 46.5 | 35.4 | 0.76x | 31.3 | 37.5 | 1.20x |
| Q18 | 28.5 | 37.4 | 1.31x | 17.9 | 37.2 | 2.08x |
| Q19 | 19.0 | 24.5 | 1.29x | 18.6 | 24.5 | 1.32x |
| Q20 | 19.3 | 16.8 | 0.87x | 19.1 | 21.1 | 1.11x |
| Q21 | 59.9 | 89.5 | 1.49x | 61.4 | 85.6 | 1.39x |
| Q22 | 4.6 | 2.6 | 0.56x | 4.4 | 3.1 | 0.71x |

## Reproduce

Requirements: one free GB200 GPU, ~250 GB free RAM, the SF3000 parquet at
`/scratch/sirius/datasets/tpch_sf3000` (`<table>/*.parquet`, decimal128), a spill directory,
Pixi ≥ 0.78, and `sirius-spill-reproducer.tar.gz` (sha256 in `COMMITS.md`; not vendored).

```bash
# 1. Build an engine (≈25 min). integration = what was benchmarked; historical = package's tree.
bash bench/sf3000-repro-8xl/prepare-engine.sh integration /scratch/$USER/sirius-integration

# 2. Run cold, lukewarm and hot on GPU 0 pinned to 32 cores; validates and summarizes each mode.
PKG_TAR=/path/to/sirius-spill-reproducer.tar.gz \
ENGINE=/scratch/$USER/sirius-integration \
OUT=/scratch/$USER/repro-8xl-sf3k \
REPRO_SPILL_ROOT=/scratch/$USER/spill \
  bash bench/sf3000-repro-8xl/run.sh cold lukewarm hot
```

Cold takes ~10 min, lukewarm ~30 min (one untimed + two timed sweeps), hot ~30 min. For the
historical engine add `SOURCE_POLICY=historical`. Run one benchmark per GPU at a time; GPFS
bandwidth is shared, so concurrent scan-heavy jobs skew timings.

### Adaptations from the g7e package

| Package (g7e.8xlarge) | Here | Why |
|---|---|---|
| `usage_limit_fraction: 0.86` | `usage_limit_bytes: 88272084664` | 0.86 of the g7e's 97,887 MiB. The fraction would give 163 GB of the 189 GB GB200 |
| spill on `/opt/dlami/nvme` | `REPRO_SPILL_ROOT` (GPFS here) | no instance NVMe |
| `--source s3://…/tpch_sf3000_regenerated` | `--source /scratch/sirius/datasets/tpch_sf3000` | no bucket access; `run.py`'s own option, skips the S3 secret |
| 32 vCPU | `taskset -c 0-31` (GPU 0's NUMA node) | match CPU count; Sirius thread pools left at package values |
| `CUDAARCHS=120-real` | `100-real` | GB200 is sm_100 |

Everything else — YAML values, thread counts, schedules, cache resets, `.timer` timing, typed
validation — is the package's own.

## Files

- `COMMITS.md` — exact engine commits (both builds), submodules, toolchain, package hash
- `prepare-engine.sh` — builds either engine from public source and asserts its source tree
- `run.sh` — extracts and patches the package, runs modes, validates, summarizes
- `reproducer-gb200.patch` — the two-line patch to the package's `tools/benchmark.py`
- `results/8xl-sf3k-{cold,lukewarm,hot,q6-smoke}/` — `invocation.json` (source commit/tree,
  binary hashes), `config.yaml`, per-query `record.json`, `validation.json`, `summary.md`.
  Query outputs, logs and telemetry are not committed (hundreds of MB); they remain under
  `/scratch/prestouser/aocsa/spillcomp/repro-results/`.
- `results/8xl-sf3k-hot/hot-partial-summary.md` — Q1–Q20 hot medians (run stopped during Q21;
  the package's `validate.py` rejects incomplete suites, so its `validation.json` reads 0 passed)
