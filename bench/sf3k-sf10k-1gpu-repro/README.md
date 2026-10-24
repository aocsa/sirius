# TPC-H SF3000 / SF10000 on one GPU: best configuration and reproduction

Reproduces 1-GPU TPC-H results on an RTX PRO 6000 Blackwell with spill compression on:

- **SF3000, all 22 queries: 277.2 s cold, 266.4 s lukewarm, 246.0 s hot.** That is −43%, −40% and −44% against the
  pre-integration baseline build measured on the same machine (487.0 s / 444.1 s / 442.2 s).
- **SF10000, Q4/Q9/Q17/Q21 cold: 20.45 / 137.18 / 59.18 / 91.34 s** (308.1 s together).

Every query output, warmups included, was checked against the saved campaign GPU outputs (334 records, all accepted).

Measured 2026-10-23/24 on this branch at `cb172fca` (`aocsa/sirius` `integ/sf3k-joost-2201-2204`). This folder
adds only scripts, configs and results; the engine code is `cb172fca`.

## Quick start on a new machine

```bash
# 1. Build Sirius at this branch
git clone -b integ/sf3k-joost-2201-2204 https://github.com/aocsa/sirius.git && cd sirius
git submodule update --init --recursive -- duckdb cucascade substrait sirius-duckdb/duckdb sirius-duckdb/extension-ci-tools
pixi install --locked
pixi run env CUDAARCHS=120-real make -j"$(nproc)"

# 2. Generate the data (about 7 min for SF3000 and 25 min for SF10000 on 96 vCPUs; 0.86 TB + 2.9 TB)
bash bench/sf3k-sf10k-1gpu-repro/generate-data.sh /opt/dlami/nvme

# 3. Run, validate and summarize (results under ~/sirius-results)
pixi run bash bench/sf3k-sf10k-1gpu-repro/run-suite.sh all
```

`run-suite.sh sf3k` runs SF3000 cold, lukewarm and hot; `run-suite.sh sf10k` runs the four SF10000 queries cold.
Set `DATA_ROOT`, `OUT`, `GPU` and `CPUS` to change the data location, output root, GPU index and CPU pinning.
A single run is `pixi run python bench/sf3k-sf10k-1gpu-repro/run.py 1gpu-sf3k --mode cold --source <data> --output <new dir>`;
add `--dry-run` to print the command. `run.py` refuses an existing output directory and a checkout with uncommitted
tracked changes, and records the source commit, binary hashes and config hash in `invocation.json`.

## Results

All numbers are seconds, one GPU, measured on the machine described under *Test machine*.

| Run | Build | Config | Cold | Lukewarm (2 sweeps) | Hot (2 sweeps) |
|---|---|---|---:|---:|---:|
| Baseline | historical tree `68fee848` | campaign `8xl-sf3k` | 487.0 | 445.4 / 442.8 | 444.0 / 440.3 |
| Integ, default config | `cb172fca` | campaign `8xl-sf3k` | 407.1 | – | – |
| **Best** | `cb172fca` | `configs/sf3k-1gpu.yaml` | **277.2** | **266.3 / 266.5** | **246.1 / 245.8** |

For context, the original campaign on a g7e.8xlarge reading from S3 measured 456.7 s cold, 423.1 / 430.2 s lukewarm and
419.4 / 412.7 s hot with the baseline build.

### SF3000 per query

| Query | Baseline cold | Integ default cold | **Best cold** | Best lukewarm (median) | Best hot (median) | Best cold vs baseline |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 13.20 | 11.79 | **12.12** | 11.44 | 10.45 | −8.2% |
| Q2 | 2.47 | 2.12 | **2.22** | 2.39 | 1.53 | −9.9% |
| Q3 | 36.32 | 30.71 | **11.18** | 11.42 | 10.71 | −69.2% |
| Q4 | 12.47 | 9.14 | **6.46** | 6.54 | 5.53 | −48.2% |
| Q5 | 20.77 | 20.58 | **12.78** | 12.82 | 11.53 | −38.5% |
| Q6 | 12.33 | 12.36 | **8.59** | 7.92 | 7.01 | −30.3% |
| Q7 | 24.19 | 24.77 | **15.21** | 14.18 | 14.22 | −37.1% |
| Q8 | 26.90 | 27.66 | **18.22** | 17.66 | 17.36 | −32.3% |
| Q9 | 48.66 | 31.50 | **28.97** | 27.99 | 27.45 | −40.5% |
| Q10 | 29.51 | 25.48 | **22.82** | 23.21 | 22.65 | −22.7% |
| Q11 | 4.27 | 3.90 | **3.47** | 3.55 | 2.53 | −18.8% |
| Q12 | 12.55 | 12.61 | **8.68** | 8.81 | 7.72 | −30.8% |
| Q13 | 13.57 | 12.58 | **8.62** | 8.65 | 6.78 | −36.5% |
| Q14 | 21.63 | 21.90 | **13.29** | 13.71 | 12.06 | −38.6% |
| Q15 | 20.98 | 20.92 | **12.82** | 12.21 | 11.16 | −38.9% |
| Q16 | 2.10 | 1.69 | **1.80** | 1.79 | 1.51 | −14.2% |
| Q17 | 33.82 | 28.29 | **15.84** | 15.88 | 14.03 | −53.2% |
| Q18 | 20.52 | 18.88 | **22.61** | 16.02 | 15.26 | +10.2% |
| Q19 | 18.55 | 18.08 | **12.60** | 12.51 | 11.76 | −32.1% |
| Q20 | 24.24 | 23.95 | **14.81** | 14.33 | 13.24 | −38.9% |
| Q21 | 85.27 | 45.53 | **21.16** | 20.27 | 19.41 | −75.2% |
| Q22 | 2.70 | 2.66 | **2.92** | 3.12 | 2.06 | +8.4% |
| **Total** | **487.00** | **407.10** | **277.20** | **266.43** | **245.95** | **−43.1%** |

Q18 cold is the one clear regression against the integ default (22.61 s vs 18.88 s); its lukewarm and hot times are lower.

### SF10000, Q4 / Q9 / Q17 / Q21 cold

| Query | **This config** | 8xl dashboard (4 GiB Bloom cap) | Change | Original 8xl campaign |
|---|---:|---:|---:|---:|
| Q4 | **20.45** | 26.0 | −21.4% | 298.64 |
| Q9 | **137.18** | 240.2 | −42.9% | 405.30 |
| Q17 | **59.18** | 68.2 | −13.2% | 274.27 |
| Q21 | **91.34** | 208.4 | −56.2% | 2171.88 |
| Sum | **308.15** | 542.8 | −43.2% | 3150.09 |

The four queries run in one process, in this order, with the Sirius cache reset before each. Peak process RSS was
226 GiB, which fits on a g7e.8xlarge (250 GiB RAM).

## Configurations

`configs/sf3k-1gpu.yaml` is the best SF3000 configuration. `configs/sf10k-1gpu.yaml` is the same file plus
`max_dynamic_filter_bloom_bytes_per_gpu: 4096MiB` (SF10000 Q3 needs about 2.74 GiB of Bloom filter per GPU; 4 GiB is
the cap the SF10000 campaign used). Compared with the campaign `8xl-sf3k` settings, the best config:

| Setting | Campaign `8xl-sf3k` | Best |
|---|---|---|
| `memory.gpu.usage_limit_fraction` | 0.86 | 0.908 |
| `memory.host.capacity_bytes` | 200 GiB | 240 GB, all 1788 pools pinned at startup (about 48 s) |
| scan cache (`eviction_threshold_fraction`) | 20 GB | 0.2 × 240 GB = 48 GB |
| `operator_params.scan_task_batch_size` | 1 GB | 4 GB |
| `operator_params.hash_partition_bytes` | 4 GB | 12 GB |
| `operator_params.max_build_hash_table_bytes` | 4 GB | 12 GB |
| `operator_params.enable_probe_accumulate` | not available | true (new on this branch, one GPU only) |
| `executor.scan_manager.rest` | default | `upkeep_interval_ms: 15000`, `n_max_concurrent_scans: 48` (S3 only) |
| disk tier | 800 GB | 1 TB |
| Quent telemetry | on | off |

The harness uses the YAML as written except `downgrade_root_dirs`, which it replaces with a fresh per-run directory
under `--spill-root` (default `/opt/dlami/nvme/sirius-spill`) and deletes afterwards.

The six `SIRIUS_EXP_*` environment-variable features on this branch were off for every run.

## Method

- **Data.** `generate-data.sh` builds `sirius-db/tpchgen-rs` at `cdcf74de` (decimal128, date32, int32 keys, Parquet v2,
  about 100M rows per file) and runs its `scripts/generate_tpch.py`. It then checks every file against
  `data/generation-sf*.jsonl`, the campaign's generation manifests: size, row count, row groups and S3 ETag (MD5, or
  multipart MD5 with 8 MiB parts). On the test machine all 263 SF3000 files and all 868 SF10000 files were
  byte-identical to the campaign's S3 datasets.
- **Modes.** Cold calls `reset_sirius_cache()` before each timed query, and `run-suite.sh` drops the OS page cache
  before each run. Lukewarm runs one untimed sweep, then two timed sweeps without resets. Hot runs each query once
  untimed, then twice timed. Times are the DuckDB CLI `.timer` real seconds, including CSV output. Engine startup,
  views, `DESCRIBE` and cache resets are excluded.
- **Placement.** One GPU (`CUDA_VISIBLE_DEVICES=0`), the process pinned to 32 vCPUs (`taskset -c 0-15,48-63`, 16 cores
  with their hyperthreads) and 32 DuckDB threads, to match a g7e.8xlarge.
- **Validation.** `tools/validate.py` compares every output with the saved campaign GPU outputs in `reference/`
  (typed values, exact decimals, float tolerance, legal ORDER BY tie reordering). Q3 and Q18 LIMIT-boundary ties are
  proved with the expanded results in `reference/expanded/`. Q11 and Q20 outputs are too large to commit (up to
  202 MB), so `reference/sf*/q11` and `q20` hold an exact digest of the sorted rows (`result-digest.json`); both have only
  integer, decimal and string columns. These are GPU-to-GPU agreement checks, not an independent CPU oracle. A run is
  accepted only if every scheduled query finished on the native path with exit code 0 and agreed.

## Test machine and build

- g7e.24xlarge (eu-west-2): 4× NVIDIA RTX PRO 6000 Blackwell Server Edition (97,887 MiB), only GPU 0 used; Intel Xeon
  Platinum 8559C, 96 vCPUs, one NUMA node; 999 GiB RAM; 2× 3.5 TB instance-store NVMe striped at `/opt/dlami/nvme`
  (data and spill).
- Driver 595.91.07. Toolkit from `pixi.lock` (nvcc 13.4), `CUDAARCHS=120-real`. DuckDB `88ecd0a4`, cuCascade `bd4aeedb`.
- Baseline build: tree `68fee848`, which is historical main `38fe9ba2` + PR #2204 head `10727ae9` + PR #2155 head
  `4d83cd03`, the source tree of the original spill-compression campaign.
- New build: `cb172fca`. It branches from the same historical main `38fe9ba2` and adds later upstream main commits,
  PRs #2201, #2205, #2207, the latest #2155, the spill-compression stack at `0997c1f3`, ANS-coded high-cardinality
  string spill (`eb41431b`) and the DECIMAL128 Simpatico fix (`cb172fca`).

## Caveats

- **Local NVMe, not S3.** The original campaign and the dashboard read from S3. These runs read the same files from
  local NVMe through the local-file scan, so the `rest` settings and reactor count had no effect. Expect different
  I/O times from S3.
- **One run per mode.** Run-to-run noise was not measured; treat per-query differences under about 10% as unconfirmed.
- **Bigger host than a g7e.8xlarge.** The test machine has 999 GiB RAM, so the OS page cache can keep files read
  earlier in a run. Cold resets the Sirius cache before each query, not the page cache. The CPU count matches an 8xl;
  memory bandwidth and the PCIe topology may not.

## Files

| Path | Contents |
|---|---|
| `run-suite.sh` | Runs the SF3000 and/or SF10000 suites, validates and summarizes |
| `run.py` | One profile and mode; writes `invocation.json` and calls `tools/benchmark.py` |
| `generate-data.sh` | Builds the pinned generator, generates SF3000/SF10000 and verifies them |
| `profiles.json` | `1gpu-sf3k` (all 22 queries) and `1gpu-sf10k` (Q4, Q9, Q17, Q21) |
| `configs/` | The SF3000 and SF10000 Sirius configs |
| `queries.py` | Fixed benchmark SQL (identical to `test/tpch_performance/queries.py` at `cb172fca`) |
| `tools/` | Benchmark driver, validators, summary, local data verifier |
| `data/` | Campaign generation manifests used to verify regenerated data |
| `reference/` | Saved campaign outputs used for validation |
| `results/query-times-seconds.csv` | Every timed query of the runs above, including warmups, with validation status |
| `results/suite-totals-seconds.json` | Per-sweep totals |
| `results/runs/<run>/` | `invocation.json`, `validation.json`, rendered `config.yaml`, `suite-status.json`, `memory-summary.json`, `summary.md` |

The harness is derived from the `sirius-spill-reproducer` package for the original 8xl/12xl spill-compression campaign.
