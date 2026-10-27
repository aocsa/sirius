# Distributed TPC-H benchmark

These scripts build Sirius and run the 22 TPC-H queries on every GPU of one host, with one Sirius process (rank) per GPU. Each rank scans its share of the files, and the ranks exchange batches over NIXL/UCX.

Each query runs in up to three cases:

| Case | Data | Read path |
|---|---|---|
| `direct` | the full dataset | O_DIRECT, no cache. This run's answer is the reference for the other two. |
| `page` | a copy of only the query's columns | the Linux page cache |
| `pinned` | the same copy | Sirius's pinned host cache, filled by the first iteration |

## Requirements

- **Host:** Linux with N NVIDIA GPUs.
- **Software:** [pixi](https://pixi.sh), Rust (`cargo`), and NIXL with UCX.
- **`direct` case:** the full TPC-H Parquet dataset, one directory per table.
- **`page` and `pinned` cases:** a local disk (`COPY_ROOT`) with room for the largest copy you run. At SF10000, copies range from 21 GB (Q16) to 817 GB (Q9), and `columns.py --sf SF` prints them all. The `pinned` case needs a filesystem that supports O_DIRECT, which tmpfs does not.
- **RAM for `page`:** the copy plus N × the host pool.
- **RAM for `pinned`:** each rank's share of the copy (copy size / N) must fit under 80% of its host pool, or the cache evicts. Set the pool size with `HOST_POOL_PINNED`.
- **RAM for `hostpin`:** N × the host pool must fit in CPU memory, with room to spare. Each pool holds that query's pinned columns plus the query's own host memory.
- **Count CPU memory only:** on hosts whose GPU memory shows up as NUMA nodes (GB200, for example), `free` and `MemTotal` include the GPUs' memory. Sum the nodes that have CPUs (`/sys/devices/system/node/node*/meminfo`).

## Steps

All commands run from the repo root.

```bash
# 1. Build with NIXL (long the first time: log it)
NIXL_PREFIX=/path/to/nixl UCX_PREFIX=/path/to/ucx bash bench/tpch/build.sh > build.log 2>&1

# 2. The TPC-H generator CI uses, for the per-query copies
git clone https://github.com/sirius-db/tpchgen-rs.git test_datasets/tpchgen-rs
(cd test_datasets/tpchgen-rs && cargo build --release -p tpchgen-cli)

# 3. Unit tests, on an idle GPU
CUDA_VISIBLE_DEVICES=0 bash bench/tpch/test.sh

# 4. Benchmark (the GPUs must be idle; it refuses to start while a duckdb process runs)
SF=10000 FULL_DATA=/path/to/tpch_sf10000 COPY_ROOT=/local/disk/tpch-copies OUT=/tmp/$USER/tpch-runs \
  pixi run bash bench/tpch/run_tpch.sh
```

`run_tpch.sh` takes its settings from environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `QUERIES` | `1 … 22` | Queries to run, e.g. `"5 9 18"`. |
| `CASES` | `direct page pinned` | Cases to run. |
| `SF` | `10000` | Scale factor of the data and the query texts. |
| `ITERS` | `3` | Runs per query, in one process per rank. The first is cold; the best of the rest is warm. |
| `RANKS` | every GPU | Number of ranks. |
| `KEEP_COPIES` | `0` | `1` keeps each query's copy instead of deleting it after the query. |
| `HOST_POOL_DIRECT`, `HOST_POOL_PAGE`, `HOST_POOL_PINNED` | from the configs | Host pool per rank for each case, e.g. `300Gi`. |
| `JOBS`, `THREADS`, `STAGE` | `24`, `6`, `/dev/shm/$USER-tpchgen` | Generator parallelism and scratch directory. |
| `SPILL_RESERVE` | 150 GB | Space left free in `COPY_ROOT`. |

For each query, `run_tpch.sh` does the following:
1. Runs the `direct` case.
2. Generates a copy of the query's columns in `COPY_ROOT`, using the CI generator's exact settings, so files, row groups and column chunks match the CI dataset. A copy that doesn't fit is skipped and recorded.
3. For `page`, reads the copy once into the page cache.
4. Runs `page` and `pinned`, and checks each answer against `direct`.
5. Deletes the copy.

Rows with equal values that come out in another order count as the same answer.

## Output

| Path | What it holds |
|---|---|
| `OUT/summary.md` | Cold, warm and the answer check per query and case, with totals. `summarize.py OUT` rebuilds it. |
| `OUT/results.jsonl` | One line per run: every iteration's time, the answer checks, and spill bytes. |
| `OUT/copies.jsonl` | Each copy's size and generation time. |
| `OUT/q<NN>_<case>/` | Each rank's output (`exchange_rank_<i>.log`; rank 0 holds the result), the Sirius log, and the config used. |
| `OUT/telemetry_data/` | Quent telemetry, one directory per rank and run. |

## Profiling a query

`profile_q5.sh OUT` turns the last run's telemetry into a page showing:
- Q5's plan;
- every pipeline's timing, grouped by stage;
- per-rank timelines of the cold and warm iterations.

Run Q5 last to profile it, e.g. `QUERIES=5 CASES=pinned run_tpch.sh`. `extract.py` works for any query; the page layout in `build_page.py` and `template.html` is Q5's.

## Reference results

Measured 2026-10-23 on 4× GB200 (189 GB each, 144 Grace cores, 956 GiB CPU memory). Build: commit 432a7c88 without 4dcfd5f5.

The `direct` case read the full SF10000 dataset from GPFS: all 22 queries, 2 iterations each, every answer the same on both. Total: **1,990 s cold, 680 s warm.**

| Q | Cold | Warm | Q | Cold | Warm | Q | Cold | Warm |
|---|---|---|---|---|---|---|---|---|
| 1 | 31.9 | 18.9 | 9 | 121.0 | 45.6 | 17 | 92.4 | 36.8 |
| 2 | 33.3 | 13.6 | 10 | 108.7 | 38.2 | 18 | 117.4 | 48.1 |
| 3 | 127.8 | 34.5 | 11 | 30.6 | 11.5 | 19 | 118.1 | 39.2 |
| 4 | 91.5 | 27.2 | 12 | 138.1 | 33.0 | 20 | 80.9 | 22.0 |
| 5 | 136.3 | 35.9 | 13 | 24.9 | 15.0 | 21 | 180.3 | 81.5 |
| 6 | 76.2 | 27.2 | 14 | 102.3 | 29.8 | 22 | 13.2 | 7.2 |
| 7 | 147.1 | 44.9 | 15 | 120.9 | 29.7 | | | |
| 8 | 77.1 | 33.7 | 16 | 19.9 | 6.6 | | | |

So far only Q5 has run in the `page` and `pinned` cases. Its copy is 502 GB.

| Q5 | Cold | Warm |
|---|---|---|
| `direct` | 136.3 s | 35.9 s |
| `page` | 8.7 s | 7.1 s |
| `pinned` | 108.9 s | 3.08 s |

- **Warm, pinned:** 68% of Q5's time is the lineitem scan with its Bloom filter and its shuffle on the order key.
- **Cold, pinned:** limited by the one NVMe drive that held the copy, about 5 GB/s.

### Pinned once, SF3000

Measured 2026-10-24 on the same host. `run_suite.py` pinned every table once on the host (210Gi pools, 47-57 s per rank), with lineitem and orders compressed, o_comment left out, and nation, region and supplier replicated, then ran all 22 queries against the pins:

```bash
pixi run python bench/tpch/run_suite.py --out OUT --config bench/tpch/host-pin.yaml \
  --host-pool 210Gi --data /path/to/tpch_sf3000 --sf 3000 --pin-tier host \
  --pin-plans src/compression/simpatico_codegen/plans/tpch_sf1000 --pin-exclude o_comment \
  --pin-replicate nation,region,supplier --ref-dir DIRECT_RUN_OUT
```

[pinned-results.md](pinned-results.md) has the full SF3000 and SF10000 results with pinned tables, and the configs to reproduce them.

Every answer matched the `direct` run's. The 22 queries took 23.2 s warm in total, against 207.6 s for `direct`: from 2.5x (Q11) to 18x (Q6) faster, except Q13 (1.1x), which reads o_comment from disk.

## Files

| File | Purpose |
|---|---|
| `build.sh`, `test.sh`, `run_tpch.sh` | Steps 1, 3 and 4. |
| `run_query.py` | Runs one query on N ranks and checks its answer. Any query, scale factor or SQL file (`--help`). |
| `run_suite.py` | Pins tables once, then runs every query against the pins in one process per rank (`--help`). |
| `summarize.py` | Builds `summary.md` from `results.jsonl`. |
| `gen_tables.py`, `columns.py`, `colsizes.json` | Generate a copy of only the columns a query reads. `colsizes.json` holds the column sizes at SF10000. |
| `results.py` | Parses rank 0's output and compares answers. |
| `profile_q5.sh`, `extract.py`, `build_page.py`, `template.html` | Telemetry to the profile page. |
| `direct-io.yaml`, `page-cache.yaml`, `pinned-cache.yaml`, `host-pin.yaml` | Sirius configs for the `direct`, `page`, `pinned` and `hostpin` cases. |
| `q5_subquery.sql` | Q5 with partial aggregates after the lineitem and supplier joins. Run it with `run_query.py 5 --sql`. |

## Notes

- **Pins across ranks:** each rank pins only its own share of a table's files, and the ranks agree on the result: a pin that fails on one rank fails on all of them. `replicate = true` pins a small table whole on every rank, so joins build from it without exchanging it. A pin fails rather than fill more than `pin_table_memory_limit` (default 0.9) of the memory it pins into, or of its CPU NUMA node.
- **Telemetry location:** telemetry is written to the working directory, so the scripts run queries from `OUT`. Don't run them inside the repo.
- **Parallel filesystems:** `cache: os` may fail on them with "Input/output error". Use a local copy instead.
- **Rank labels in the telemetry:** it identifies rank 0, and the NUMA node of each rank. Two other ranks on the same node can't be told apart, so the profile page labels them `a` and `b`.
