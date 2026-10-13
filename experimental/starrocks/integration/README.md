# StarRocks CN integration branch (temporary)

This branch combines every open PR of the Sirius StarRocks compute-node (CN) work, so it can be built and tested in one place before the stacks merge. **It is temporary and is not meant to be merged.** The PRs themselves are on sirius-db/sirius.

## What's in it

On top of `main` (30991b4f, #1956):

| group | PRs |
|---|---|
| base stack | #2037 translate exchanges, two-phase SUM, joins → #2038 multiple fragments per CN through a local exchange → #2039 direct GPU-to-GPU exchange → #2040 NIXL between CNs → #2041 failed-query cleanup → #2042 pinned tables → #2043 byte-range splits → #2061 spill and streaming → #2062 broadcast runtime filters |
| coverage | #2103 COUNT/MIN/MAX → #2104 RIGHT SEMI joins → #2109 → #2110 AVG → #2111 → #2112 DATE keys → #2127 native DECIMAL → #2129 exact decimal division → #2130 exact AVG and `/` |
| runtime filters | #2102 → #2114 partitioned joins → #2122 non-leaf fragments → #2134 → #2136 → #2137 |
| failures | #2115 failure frames → #2116 reportExecStatus → #2118 cancel reasons → #2120 interrupt and purge → #2121 query timeouts; #2105 engine interrupt |
| from `main` PRs | #2100, #2101, #2107 (`LIMIT` fix, `substrait` submodule at 9b5b2d4), #2124 (decimal cast rounding) |
| local | the fixes needed to combine the three stacks, and a function-table fix for `round` + `sirius_decimal_divide` |
| #2037 review fixes | duplicate exchange bindings rejected, concurrent FILES() footer reads, `output_row_count` throws before `build()` (e65e6aa7, 2546fc3a) |

Not included: #2125, #2128, #2131 (engine fixes TPC-H doesn't need) and #1965.

## Results on 4× GB200 (one CN per GPU, every result checked against DuckDB)

- **SF1000:** 22/22. About 41 s for the 22 queries (median per query, summed).
- **SF1000 with `lineitem` and `orders` pinned in host memory:** 21/22 in about 20 s. q18 runs out of GPU memory when pinned.
- **SF3000:**
  - 20/22 with one query per fresh cluster; q17 and q18 run out of GPU memory;
  - 16–17/22 per iteration with all 22 in sequence on one cluster; the failures are GPU out-of-memory (q04, q08, q09, q17, q18).

## Setup, test, bench

You need the 4× GB200 box (or similar), `pixi` in `~/.pixi/bin`, and the TPC-H parquet data. On the GB200 box the data is in `/scratch/sirius/datasets` (the `box/gb200` default).

```bash
export WORK=/raid/$USER            # a local directory with ~40 GB free, not the NFS $HOME (BOX.md: /raid needs a one-time sudo mkdir)
git clone -b tmp/sr-cn-integration-2026-10-13b https://github.com/aocsa/sirius.git $WORK/sirius
cd $WORK/sirius/experimental/starrocks/integration

./setup.sh            # tools repo (box/gb200) next to this checkout, then UCX/NIXL, engine, FE, CN
./test.sh smoke       # 2 CNs, SF1, all 22 queries: a quick check of the build
./test.sh sf1000      # 4 CNs, SF1000, all 22 queries, each checked against DuckDB
./bench.sh            # SF1000, 3 iterations: runtimes.csv + summary.md
```

- **`setup.sh`:**
  - clones https://github.com/aocsa/starrocks-setup-test-tools (`box/gb200`) to `$WORK/starrocks-setup-test-tools`;
  - rebases it on `main` for the q11 fix;
  - makes sure the `substrait` submodule commit is available (below);
  - runs the tools repo's `setup/all.sh` against this checkout.

  Cold, the build takes about 15–20 minutes on the GB200 box (the engine, the FE and UCX/NIXL build in parallel, then the CN); warm caches bring it to about 6. Logs go to `/tmp/$USER/sirius-starrocks-runs/setup-logs/`.
- **`test.sh`** wraps `tests/4cn_tpch_joins_sf1000.sh`; **`bench.sh`** wraps `scripts/bench.sh`. Both run all 22 queries unless told otherwise. Settings pass through the environment; the tools repo's `README.md` lists them all. The ones most used here:
  - `TPCH_QUERIES="q07 q18"`: run only some queries.
  - `GPUS="0 1"`: use only these GPUs, one CN each.
  - `SIRIUS_CN_RUNTIME_FILTERS=0`: runtime filters off.
  - `ENGINE_LOGS=1`: keep each CN's engine log.
  - `PIN_COMPRESSION=1 ./bench.sh --pin --label pinned`: pin `lineitem` + `orders` in host memory on every CN first.
  - `./test.sh sf3000`: SF3000, with 256 GiB of host spill per CN.
- **Where results go:**
  - CN, FE and query logs: under `/tmp/$USER/sirius-starrocks-runs/` (`$RUN_ROOT`, printed at the start of a run).
  - Bench results: under `.../bench/<timestamp>_<label>/`.
  - The cached DuckDB answers: under `.../oracle`. The first run at a scale factor computes them, which takes several minutes per query.
- **Before starting a run,** check `nvidia-smi`. The scripts refuse to start if the GPUs are busy. On a shared box, other jobs can hold the GPUs.

## The `substrait` submodule

#2107 pins `substrait` at 9b5b2d4, the fix that makes the Substrait consumer read `FetchRel.count_expr`/`offset_expr`. Without it, every `LIMIT` is read as `LIMIT 0`.

That commit is on sirius-db/duckdb-substrait-extension only in the unmerged PR branch `fetch-count-expr-1.5.5`. `ensure-substrait.sh` (run by `setup.sh`) first tries a normal `git submodule update`. If the commit can't be fetched, it rebuilds it from `patches/substrait-fetch-count-expr.patch` on top of its parent, which is on the stable `duckdb-1.5.5` branch. The replay reproduces the exact commit hash, so nothing else changes. Run it by hand after a fresh clone if `git submodule update` complains about `substrait`.

## Multiple fragments per CN

This is #2038 (branch `stacked/sr-local-exchange` on sirius-db/sirius): fragments of one query on the same CN hand batches to each other through a local exchange. Every branch above it in the stack has it, including this one. The code is in `experimental/starrocks/src/local_exchange.rs` and `compute_node_service.rs`.

## Known issues

- **q18 with pinned data:** GPU out-of-memory in a hash join, every time.
- **SF3000:** the GPU out-of-memory failures listed above, and q02 hangs with runtime filters off.
- **First query on a fresh cluster:** about 2 s slower. Each CN sets up its NIXL peers one at a time, and the FE is cold.
- **`--pin`:** every CN pins all of `lineitem` and `orders` (about 115 GiB of host memory per CN at SF1000), so it doesn't fit at SF3000.
