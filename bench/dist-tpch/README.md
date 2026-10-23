# Distributed Sirius: TPC-H Q5 at SF10000 on 4 GB200s

Q5 runs on 4 GPUs as 4 Sirius processes (ranks), one per GPU. Each rank scans its byte-range
share of the files, and the ranks exchange batches over NIXL/UCX (`cuda_ipc`). This folder holds
the notes and scripts to rebuild, test, rerun and profile the 2026-10-23 measurements: Q5
served from GPFS, from the Linux page cache, and from Sirius's pinned host cache.

## Results (2026-10-23)

Host: 4x GB200 (189 GB each), 144 Grace cores, 2 NUMA nodes, 1.7 TB RAM.

Build: `dist-plan-opt` at 59d40bdc plus the thread-affinity fix, which is now commit 432a7c88.
The binary did not include 4dcfd5f5, the Bloom cap change.

| Data served from | Config | Cold | Warm |
|---|---|---|---|
| GPFS, O_DIRECT, full SF10000 dataset | `gpfs.yaml` | 136.3 s | 35.9 s |
| Linux page cache, Q5 copy read once with `cat` | `page-cache.yaml` | 8.7 s | 7.1 s |
| Sirius pinned host cache, Q5 copy, O_DIRECT from one NVMe | `pinned-cache.yaml` | 108.9 s | 3.08 s |

- **Query:** queries.py Q5, 3 iterations in one process per rank. Cold is the first iteration; warm is the best of the other two.
- **Answers:** all runs return the same 5 rows (RUSSIA first, 529,165,993,241.55) on every iteration.
- **The subquery variant** (`q5_subquery.sql`, region ASIA, 1994) takes 7.5 s warm from the page cache and 3.13 s from the pinned cache. Its answer matches the same text run from GPFS.
- **Where the time goes (pinned cache, warm, 3.05 s executing):**
  - The lineitem scan, its Bloom filter and its shuffle on the order key take 68%.
  - The orders scan and its join take 19%.
  - The ranks finish within 0.01 s of each other.
  - Each rank serves about 128,550 chunks from the cache, with 0–1 misses and no evictions.
- **Cold, pinned cache:** all four ranks read the 502 GB copy from one NVMe drive at about 5 GB/s in total.
- **The GPFS data is the full dataset,** with wider join keys. Q5 reads 1.22 TB there, against 502 GB from the copy.

## Files

| File | What it does |
|---|---|
| `build.sh` | Configures the release build with NIXL (`-DSIRIUS_WITH_NIXL=ON`) and builds it. |
| `test.sh` | Unit tests for pinned-cache matching (`[can_serve]`) and the per-rank byte-range split (`[byte_range]`). |
| `run_q5.sh` | End to end: generate the Q5 copy, run Q5 from GPFS, the page cache and the pinned cache, then build the profile page. |
| `run_query.py` | Runs any TPC-H query on N ranks. Creates views only for the tables it reads, checks answers, keeps the telemetry. |
| `gen_tables.py` | Generates TPC-H tables with the CI generator's settings, keeping only the listed columns. |
| `columns.py` | The columns each query reads, and the size of a copy of them at SF10000 (uses `colsizes.json`). |
| `results.py` | Parses rank 0's output and compares answers. Rows with equal values may come out in another order. |
| `extract.py` | Turns each rank's Quent telemetry into per-pipeline timings (JSON). |
| `build_page.py`, `template.html` | The profile page: plan, pipelines by stage, per-rank timelines. |
| `page-cache.yaml`, `pinned-cache.yaml`, `gpfs.yaml` | Sirius configs. `@SPILL_DIR@` is filled in by `run_query.py`. |
| `q5_subquery.sql` | Q5 with partial aggregates after the lineitem and supplier joins, and nation joined last. |

## 1. Build

```bash
git checkout dist-plan-opt-rank-pin
NIXL_PREFIX=$HOME/tools/nvda_nixl bash bench/dist-q5-sf10000/build.sh > build.log 2>&1
```

- **Submodules:** `build.sh` runs `git submodule update --init --recursive`. Worktrees don't initialize submodules on their own.
- **NIXL and UCX:** `scripts/nixl_env.sh` sets their runtime paths. Its defaults are `~/tools/nvda_nixl` and `~/tools/ucx-install`.
- **Build time:** a first build takes a long time, so run it in the background. After that, `pixi run make` rebuilds only what changed.

## 2. Test

```bash
CUDA_VISIBLE_DEVICES=3 bash bench/dist-q5-sf10000/test.sh
```

- **Pick an idle GPU.** The test binary reserves a large GPU memory pool at start, and fails with `out_of_memory` on a GPU another job is using.
- **Answer checks** are done by the benchmark itself (step 3), not by these tests.

## 3. Benchmark

```bash
DATA=/tmp/$USER/tpch/q5/sf10000 OUT=/tmp/$USER/q5-runs \
  GPFS_DATA=/scratch/sirius/datasets/tpch_sf10000 \
  pixi run bash bench/dist-q5-sf10000/run_q5.sh
```

Steps: `gen gpfs page pinned profile`. Pass a subset to run only those, for example `run_q5.sh page pinned`.

- **gen** writes the Q5 copy to `DATA`.
  - Size: 502 GB, of which lineitem is 417 GiB and orders 84 GiB.
  - Time: about 12 minutes with `JOBS=24 THREADS=6` on 144 cores.
  - Columns: `l_orderkey, l_suppkey, l_extendedprice, l_discount`; `o_orderkey, o_custkey, o_orderdate`; `c_custkey, c_nationkey`; `s_suppkey, s_nationkey`; nation and region whole.
  - Format: every part is generated with the CI generator's exact `tpchgen-cli` flags, then rewritten row group by row group with only those columns. Files, row groups and column chunks match the full CI dataset. `CI_TPCHGEN` points at the generator checkout.
- **gpfs** runs queries.py Q5 from `GPFS_DATA` with `gpfs.yaml`: 250 GiB host per rank, O_DIRECT. Later runs of the same text check their answer against it.
- **page** reads `DATA` once with parallel `cat` (about 1.5 minutes), then runs both Q5 texts with `page-cache.yaml`: buffered reads, 120 GiB host per rank.
- **pinned** runs both Q5 texts with `pinned-cache.yaml`. Reads use O_DIRECT into Sirius's pinned prefetching cache: LRU eviction, 200 GiB host per rank.
  - The first iteration fills the cache from disk; the later ones read pinned memory.
  - A rank's share of the copy (about 125 GB) must fit under the eviction threshold, 80% of the host pool.
- **profile** builds `OUT/q5-profile.html` from the newest run's telemetry. `extract.py --auto` orders the ranks:
  - Rank 0 collects the result.
  - Rank 1 shares its NUMA node.
  - The telemetry does not say which of the other two is rank 2, so the page labels them `2/3 a` and `2/3 b`.
  - The page's prose (`template.html`) describes the 2026-10-23 run: edit it for a new one.

Each query run writes to `OUT/<name>/`:
- `exchange_rank_<i>.log`: each rank's output; rank 0's holds the result.
- `logs/`: the Sirius log.
- `sirius.yaml`: the config used.
- `result.json`: times and answer checks.

It also appends a line to `OUT/results.jsonl`.

### Running one query

```bash
pixi run python bench/dist-q5-sf10000/run_query.py 5 --std --out /tmp/$USER/q5-runs \
  --config bench/dist-q5-sf10000/pinned-cache.yaml --data /tmp/$USER/tpch/q5/sf10000 \
  --ref /path/to/earlier/run/exchange_rank_0.log
```

- `--table T=DIR1,DIR2` reads a table from several directories, for copies split between tmpfs and local disk.
- `--ref` compares the answer with rank 0's log of an earlier run of the same text.

## Other queries

`python bench/dist-q5-sf10000/columns.py` prints each query's columns and copy size at SF10000. For example:

| Query | Copy size |
|---|---|
| Q18 | 211 GB |
| Q21 | 419 GB |
| Q10 | 444 GB |
| Q7 | 606 GB |
| Q8 | 784 GB |
| Q9 | 817 GB |

To build a copy for query Q:

```bash
python bench/dist-q5-sf10000/columns.py --keep Q > keep.json
python bench/dist-q5-sf10000/gen_tables.py 10000 DIR keep.json [jobs] [threads] [stage_dir]
```

`keep.json` also takes `"_split": {"lineitem": [[dir, n_parts], ...]}` to spread a table's files across directories.

Where a copy can live, on this host:
- **tmpfs:** `/dev/shm` is 479 GiB. It works with `cache: os`, but not with `cache: sirius`: tmpfs rejects O_DIRECT.
- **Local disk:** about 150 GB free.
- **GPFS:** Q8 and Q9 don't fit anywhere locally. `cache: os` fails on GPFS with "Input/output error", and GPFS files aren't page-cached anyway.

## Rank-aware pin_table

The branch's only code change is commit 8adaa024. The results above were measured without it.

- **Before:** in distributed runs, `pin_table` pinned every file on every rank. A pin matched a scan on the file set alone, so no rank's scan ever used its pin.
- **After:** each rank pins its own share of the files and byte ranges (`planner::file_share_for_rank`), the same split its scans read. A pin serves a scan only when both read the same files and byte ranges.

To try it, pin every table before the runs:

```bash
pixi run python bench/dist-q5-sf10000/run_query.py 5 --std --pin-tier host --out ... --config ... --data ...
```

- **Not yet tested on 4 ranks.**
- **Size:** decoded columns are much larger than the Parquet bytes. Q5's would take about 310 GB per rank at SF10000, even narrowed, so try it at SF1000 first.
- **`--pin-tier parquet`:** pins Parquet bytes instead, and needs `cache: sirius`.

## Notes

- **Shared machine:** run nothing while another job uses the GPUs. `run_query.py` refuses to start while any `duckdb` process runs.
- **Telemetry location:** telemetry goes to `telemetry_data/` in the working directory, so runs start in `OUT`, never inside the repo.
- **DuckDB agent mode:** `run_query.py` drops `AI_AGENT`, `CLAUDECODE` and `CLAUDE_CODE` from the environment, so DuckDB prints its normal output.
- **Disk space:** keep large data and build trees off an NFS `$HOME` with a quota.
- **Batch byte counts:** the telemetry's DataBatch byte counts look like allocation sizes, so the profile page doesn't use them.
