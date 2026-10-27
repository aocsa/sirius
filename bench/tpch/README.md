# TPC-H on 1 GPU and N GPUs, measured the same way

One harness for the one-process engine (1 GPU) and distributed Sirius (one process per GPU, ranks
exchanging batches over NIXL/UCX). The GPU count is the only thing that changes between runs: the
schedule, timer, config, data and validation stay the same. With one rank the launcher configures
no exchange, so that run takes the plain one-process engine path.

## Modes

`run_suite.py` runs every query in one set of long-lived processes, one per GPU, on one of the
three schedules of the one-process benchmark:

| `--schedule` | Order | Timed | Sirius cache |
|---|---|---|---|
| `cold` | Q1 … Qn | each query once | `CALL reset_sirius_cache()` before every query |
| `lukewarm` | `--warmups` sweeps of Q1 … Qn, then `--repeats` sweeps | the `--repeats` sweeps | kept |
| `hot` | per query, `--warmups` runs then `--repeats` runs | the `--repeats` runs | kept |

Defaults: `--warmups 1 --repeats 2`. Before the schedule, every rank runs `--prime-sql` (a small
`nation`/`region` join) and resets the cache, so the first query carries no first-use costs that
the others do not. Resets and the priming statement run with the timer off and their output
dropped. The timer is DuckDB's `.timer` real time on rank 0, including printing the result.

Setting `sirius_exchange_rank` connects the ranks during setup, so no timed query pays for it.

Lukewarm and hot depend on the Sirius cache. Compare only runs with the same
`executor.scan_manager.cache` settings.

`run_query.py` (fresh ranks for each query, `--iters` runs: run 1 cold, the best later run warm)
and `run_tpch.sh` (its `direct`, `page`, `pinned` and `hostpin` cases) remain for single-query
experiments. Their cold runs also pay process start-up costs, so do not compare them with
`run_suite.py`'s.

## Data

Generate the dataset with the pinned generator and verify it, so every run reads the same bytes:

```bash
TPCHGEN_DIR=/big/disk/tpchgen-rs TPCHGEN_VENV=/big/disk/tpchgen-venv \
  bash bench/tpch/generate-data.sh /big/disk 10000        # -> /big/disk/tpch_sf10000_regenerated
python bench/tpch/tools/verify_data_local.py --jobs 32 \
  bench/tpch/data/generation-sf10000.jsonl.gz /big/disk/tpch_sf10000_regenerated
```

The generator is `sirius-db/tpchgen-rs` at `cdcf74de` (decimal128, date32, int32 keys except the
order key, Parquet v2, about 1M rows per row group). The manifests hold every file's size, row
count, row-group count and MD5-based ETag, for SF3000 and SF10000. A copy in another layout (for
example int64 keys or small row groups) gives the same answers but very different times. Every
host and every run must read verified copies.

## Configs

`configs/sf3000.yaml` and `configs/sf10000.yaml` are used unchanged for 1 rank and N ranks, so
every value is per GPU: host pool, cache, batch sizes, Bloom cap, threads and disk tier.
`--host-pool` changes `capacity_bytes` and `initial_number_pools` together. `run_suite.py` refuses
a run whose pools plus 40 GiB per rank exceed 95% of a GPU's CPU NUMA node
(`--rank-overhead-gib`, `--skip-memory-check`).

## Running

Build with NIXL (`build.sh`, or `cmake --preset release -DSIRIUS_WITH_NIXL=ON …`), then run
from the repository root through pixi. The GPUs must be idle: `run_suite.py` refuses to start while
any `duckdb` process runs, and other users' jobs between queries also distort the numbers.

```bash
SF=10000; DATA=/big/disk/tpch_sf10000_regenerated; CFG=bench/tpch/configs/sf$SF.yaml
export THREADS=32                                     # DuckDB threads per rank

# 1 GPU (no exchange): GPU 0 and 32 cores of its NUMA node
GPUS=0 CPUS="0-15,48-63" pixi run python bench/tpch/run_suite.py --ranks 1 \
  --schedule cold --out runs/sf$SF-1gpu-cold --config $CFG --data $DATA --sf $SF \
  --spill-dir /big/disk/spill-1gpu

# 4 GPUs, one rank each, 32 NUMA-local cores per rank
GPUS=0,1,2,3 CPUS="0-15,48-63;16-47;72-87,120-135;88-119" pixi run python bench/tpch/run_suite.py \
  --ranks 4 --schedule cold --out runs/sf$SF-4gpu-cold --config $CFG --data $DATA --sf $SF \
  --spill-dir /big/disk/spill-4gpu --timeout 14400
```

Repeat with `--schedule lukewarm` and `--schedule hot`, each into a new `--out`. Other flags:

- `--queries 9,21,18` runs a subset.
- `--dry-run` writes the SQL file without running it.
- `--ref-dir` points at an earlier `run_query.py` output whose rank 0 logs serve as reference
  answers.
- `SIRIUS_BUILD_DIR` runs another build's binaries.
- `--pin-tier` pins tables once before the schedule (see `run_suite.py --help`).

`--spill-dir` must be empty or one `run_suite.py` created, because it is wiped before and after the
run.

Outputs per run: `executions.jsonl` (one line per execution), `suite_<schedule>/answers/` (rank
0's rows for every measured run), `invocation.json` (commit, build hashes, config hashes, data,
schedule, placement), `suite_<schedule>-summary.json`, and every rank's log and Sirius log.

## Report and validation

```bash
pixi run python bench/tpch/report.py runs/sf10000-1gpu-cold=1gpu runs/sf10000-4gpu-cold=4gpu \
  --validate --compare --jsonl runs/sf10000-cold.jsonl
```

For each run and mode, the report gives:

- queries finished and answers validated;
- the sum of each query's first timed run and of its best timed run;
- with `--compare`, a per-query table of best times and their ratio.

`--validate` checks every measured answer against `reference/sf<SF>/`, the campaign's saved GPU
answers, with typed values, exact decimals and legal ORDER BY tie reordering. Q3 and Q18 differ
between runs only in rows tied at the LIMIT boundary; `reference/expanded/` proves those ties.
Q11 and Q20 are checked by exact row digest. These are GPU-to-GPU checks, not an independent CPU
oracle.

The report also reads `run_query.py`'s `results.jsonl` (pass `--mode cold` or `--mode hot`) and
the one-process benchmark's `record.json` trees (`bench/sf3k-sf10k-1gpu-repro` on
`integ/sf3k-joost-2201-2204`).

## Before a comparison

- [ ] Both runs read the same verified dataset path.
- [ ] Same build (`invocation.json` `build_sha256`), same config file, same `--schedule`,
      `--warmups`, `--repeats` and query list.
- [ ] Same `THREADS`, and the same number of NUMA-local cores per GPU in `CPUS`.
- [ ] Spill directories on the same kind of disk, empty, with room for the disk tier per rank.
- [ ] GPUs idle for several minutes, with nothing else reading the same files.
- [ ] Both reports compare the same statistic (first or best timed run), and every answer
      validated.

## Files

| File | Purpose |
|---|---|
| `run_suite.py` | All queries on a cold, lukewarm or hot schedule in one set of ranks |
| `run_query.py`, `run_tpch.sh` | One query in fresh ranks; the `direct`/`page`/`pinned`/`hostpin` cases |
| `report.py` | Totals, side-by-side and typed validation for every run format |
| `results.py` | Parses rank 0's output and compares answers loosely (used while running) |
| `configs/` | Per-GPU configs for SF3000 and SF10000 |
| `direct-io.yaml`, `page-cache.yaml`, `pinned-cache.yaml`, `host-pin.yaml` | `run_tpch.sh` case configs |
| `generate-data.sh`, `data/`, `tools/verify_data_local.py` | Pinned generator and manifests |
| `reference/`, `tools/result_validation.py`, `tools/sf10k_followup_validation.py` | Reference answers and validators |
| `columns.py`, `colsizes.json`, `gen_tables.py` | Columns per query; per-query column copies for `run_tpch.sh` |
| `build.sh`, `test.sh` | NIXL build; the `[can_serve]` and `[byte_range]` unit tests |
| `summarize.py` | `run_tpch.sh`'s summary of `results.jsonl` |
