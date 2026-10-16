# StarRocks CN integration branch (temporary)

This branch combines every open PR of the Sirius StarRocks compute-node (CN) work on top of the DuckDB v2 `main`, so it can be built and tested in one place before the stacks merge. **It is temporary and is not meant to be merged.** The PRs themselves are on sirius-db/sirius.

It replaces `tmp/sr-cn-integration-2026-10-13`, which predates DuckDB v2.

## What's in it

The base is `main` at 5f850ef5 (#2188). `main` has been on DuckDB v2 since #2143.

### The three stacks

The heads below are the PR branches on sirius-db/sirius as of Oct 15, and all of them sit on 5f850ef5.

The branch merges the tested versions of these heads, from just before #2188 merged. Each pushed head differs from its tested version only by `main`'s change up to 5f850ef5, and the branch has that change too, so the code is the same.

| stack | PRs (head) |
|---|---|
| base | #2038 multiple fragments per CN through a local exchange (330ad57d) → #2039 direct GPU-to-GPU exchange, with its review fixes (b63cd587) → #2040 NIXL between CNs (1677db06) → #2041 reclaim a failed NIXL write's buffers (a1229adc) → #2042 pinned parquet tables (09818bef) → #2043 split parquet files across CNs by byte range (7aa781da) → #2061 spill exchange data to host, ship sender output while it runs (86abfac9) → #2062 broadcast runtime filters (f7296b73) |
| coverage, on #2062 | #2103 COUNT/MIN/MAX (e8aca57d) → #2104 RIGHT SEMI joins (183b19ea) → #2109 aggregation column-order test (cbaa7e90) → #2110 AVG (78c7c9e0) → #2111 slot refs by id (7d1e3b60) → #2112 DATE and TIMESTAMP partition keys (15b55933) → #2127 native DECIMAL (f509ccbd) → #2129 exact decimal division (804895f5) → #2130 exact AVG and `/` (143e993f) |
| runtime filters, on #2062 | #2102 log each filter's outcome, density on distinct keys (849f2ba3) → #2114 partitioned joins' filters across CNs (6088c00c) → #2122 fragments that read exchanges wait for filters (da948112) → #2134 release probers when no CN can build a filter (cb0fa0bc) → #2136 filters built on a join's output (b89e4d76) → #2137 release probers once a build outgrows exact keys (4a5b9190) |
| failures, on #2062 | #2115 failure frames on a failed sender's hops (e80f808b) → #2116 final reportExecStatus (da200bf9) → #2118 the FE's cancel reasons (fd7a9a4a) → #2119 engine and FFI interrupt (b05a8593) → #2120 stop and free a purged query (4692e8fa) → #2121 query timeouts bound a CN's waits (278380b0) |

#2037, the bottom of the base stack, is already merged into `main`.

### Extras

These are not part of the stacks. Each is either an open PR against `main` or a local fix.

| commit | what | why it's here |
|---|---|---|
| 8a37b82f | #2100: use the shared descriptor table in batch dispatch (PR head c0364803) | With `enable_single_node_schedule`, every batch fragment otherwise fails with "descriptor table cache miss". |
| f1ef77c7 | #2101: the CN heartbeat reports its StarRocks release (PR head d63046bf, same patch) | So `SHOW COMPUTE NODES` tells which StarRocks release a CN was built for. |
| 6230805d | #2124: round GPU casts to DECIMAL like DuckDB, ported to v2 (same patch as cfc69510; the PR head 25820238 adds #2188's commit and a CI change on top) | StarRocks plans TPC-H revenue expressions with this cast. Without it, the GPU truncates (0.92 instead of 0.93) and results differ from DuckDB. The v2 port also covers v2's FLOAT→DECIMAL change. |
| be489700, 29642db2 | #2107: the Substrait consumer reads `FetchRel.count_expr`/`offset_expr` (PR head 05bdf818, same code), with the `substrait` submodule at bdfb4eac | Without it, every `LIMIT` reads as `LIMIT 0` (q02, q03, q10, q18 and q21 have one). See below for the submodule. |
| 883f3dd1, d71fbae9 | fix(partition): a streaming sink converts its hash keys again ([#2217](https://github.com/sirius-db/sirius/pull/2217), draft; d71fbae9 cherry-picks the PR head 325d323d over the earlier local 883f3dd1, adding its `<cstdint>` include, formatting and docs) | #2092 on `main` broke DECIMAL partition keys in streaming fragments. q10 and q18 fail from #2127 (native DECIMAL) on without it. |
| b939f038 | glue for #2100 | Integration only: #2100 on top of #2038 (which requires a result sink) and #2118 (which returns `StatusPb`). |
| (top) | the `substrait` pin and this directory | |

**Not included:**
- #2125 (integer casts), #2128 (unary minus) and #2131 (`regexp_replace`): engine fixes that TPC-H doesn't need.
- #2105 (engine interrupt): #2119 in the failures stack covers it.

## Results on 4× GB200 (one CN per GPU, every result checked against DuckDB)

These were run on `tmp/sr-integration-v8` (b939f038), which is this branch without this directory and with `substrait` at 1326c5f, a local commit with the same tree as bdfb4eac.

- **Unit tests:**
  - The full C++ suite (`pixi run make test`: 8 shards, `[multi_gpu]`, late materialization): 0 failures. This includes `[decimal_cast]` 6/6, which v2 had broken for two FLOAT cases.
  - CN engine-linked tests: 213 passed, 0 failed, 1 ignored.
  - Rust CI: 424 passed.
- **SF1000, all 22 queries:** an A/B against the previous integration (v7, DuckDB v2 before #2039's review fixes) on `/opt/sirius-ci/datasets/tpch_sf1000`, because `/scratch` was unreachable. Two runs of two iterations each:
  - Unpinned: 88/88 passed for both, and the sum of median times was 205.29 s against 205.24 s.
  - Pinned (`--pin`): 88/88 for both, 205.13 s against 205.53 s.
  - No query differed by more than 2%. `/opt` is I/O-bound; on `/scratch`, v7 ran the 22 queries in 39.20 s (44/44).
- **Failure injection** (q05 with one fragment failed on purpose): passes, and the CNs hold nothing afterwards.

**From a fresh clone of this branch** (Oct 15, following the steps below):
- `setup.sh` fetched `substrait` bdfb4eac from the public repo and built everything in 14 minutes, with warm compiler and pixi caches.
- `./test.sh smoke` passed 22/22.
- `TPCH_QUERIES="q03 q10 q18" ./test.sh sf1000` on `/opt` passed 3/3 (9.3, 9.3 and 6.1 s, the same as above).

**SF1000 on `/scratch`** (Oct 16, a fresh clone at 21cc7734, before d71fbae9, which changes no behavior):
- `./test.sh smoke` passed 22/22.
- `GPUS="0 1 2 3" ./bench.sh`: 66/66 (22 queries, 3 iterations), and the sum of median times was 39.08 s. q01 (about 5 s) is the first query on each fresh cluster.

Not re-measured on this branch: SF3000 and pinning with `PIN_COMPRESSION=1` (see the known issues).

## Setup, test, bench

You need the 4× GB200 box (or similar), `pixi` in `~/.pixi/bin`, and the TPC-H parquet data.

```bash
export WORK=/tmp/$USER/sr-cn   # local disk with about 40 GB free, not the NFS $HOME
git clone -b tmp/sr-cn-integration-2026-10-15 https://github.com/aocsa/sirius.git $WORK/sirius
cd $WORK/sirius/experimental/starrocks/integration

./setup.sh            # tools repo (box/gb200) next to this checkout, submodules, then UCX/NIXL, engine, FE, CN
./test.sh smoke       # 2 CNs, SF1, all 22 queries: a quick check of the build
./test.sh sf1000      # 4 CNs, SF1000, all 22 queries, each checked against DuckDB
./bench.sh            # SF1000, 3 iterations: runtimes.csv + summary.md
```

- **`setup.sh`:**
  - clones https://github.com/aocsa/starrocks-setup-test-tools (`box/gb200`) to `$WORK/starrocks-setup-test-tools`, and rebases it on `main` for the q11 fix;
  - checks out the submodules, `substrait` first (below);
  - builds UCX/NIXL (into `$WORK/tools`), the engine and the FE in parallel, then the CN.

  It runs the same steps as the tools repo's `setup/all.sh`, apart from the engine step: the tools' `build-engine.sh` still looks for `libsirius.so` in `build/release/extension/sirius`, and since DuckDB v2 the library is in `build/release`. Logs go to `/tmp/$USER/sirius-starrocks-runs/setup-logs/`.
- **`test.sh`** wraps `tests/4cn_tpch_joins_sf1000.sh`, and **`bench.sh`** wraps `scripts/bench.sh`. Both run all 22 queries unless told otherwise. Settings pass through the environment; the tools repo's `README.md` lists them all. The ones most used here:
  - `TPCH_QUERIES="q07 q18"`: run only some queries.
  - `GPUS="0 1"`: use only these GPUs, one CN each.
  - `SIRIUS_CN_RUNTIME_FILTERS=0`: runtime filters off.
  - `ENGINE_LOGS=1`: keep each CN's engine log.
  - `INJECT_FAILURE_QUERY=q05 ./test.sh`: first run q05 with one fragment failed on purpose, and check that the CNs hold nothing afterwards.
  - `PIN_COMPRESSION=1 ./bench.sh --pin --label pinned`: pin `lineitem` + `orders` in host memory on every CN first.
  - `./test.sh sf3000`: SF3000, with 256 GiB of host spill per CN.
- **`env.sh`** (sourced by the others) sets what the tools repo doesn't know about the v2 `main`:
  - `SIRIUS_LIB=$SIRIUS_DIR/build/release`: the tests look for `libsirius.so` there.
  - `DATA_ROOT`: `box/gb200` reads `/scratch/sirius/datasets`. When `/scratch` is unreachable (a stale file handle), `env.sh` switches to `/opt/sirius-ci/datasets` and says so. That copy is about 5× slower at SF1000, so don't compare its timings with `/scratch` ones. Set `DATA_ROOT` yourself to choose.
- **Where results go:**
  - CN, FE and query logs: under `/tmp/$USER/sirius-starrocks-runs/` (`$RUN_ROOT_BASE`, printed at the start of a run).
  - Bench results: under `.../bench/<timestamp>_<label>/`.
  - The cached DuckDB answers: under `.../oracle`, shared by `test.sh` and `bench.sh`. They are keyed by the data path, so `/scratch` and `/opt` each need their own. The first run at a scale factor computes them, which takes several minutes per query at SF1000.
- **Before starting a run,** check `nvidia-smi`. The scripts refuse to start if the GPUs are busy. On a shared box, other jobs can hold the GPUs.

## The `substrait` submodule

#2107 needs the `substrait` submodule at bdfb4eac, the fix that makes the Substrait consumer read `FetchRel.count_expr`/`offset_expr`. It sits on 922489d, the v2 commit `main` pins.

bdfb4eac is on sirius-db/duckdb-substrait-extension's branch `fetch-count-expr-v2`, the head of PR #5 against `v2`. So a normal `git submodule update --init --recursive` fetches it. If that branch is deleted after the PR is squash-merged, the commit stays on GitHub as `refs/pull/5/head`. `ensure-substrait.sh` (run by `setup.sh`) then fetches it from there. Run it by hand after a fresh clone if `git submodule update` complains about `substrait`.

The previous branch needed a patch to rebuild a local-only commit. This one doesn't, so `patches/` is gone.

## Known issues

- **SF1000:** none failing. All 22 queries pass, unpinned and pinned (`--pin` without compression).
- **The two FLOAT→DECIMAL cast failures** that DuckDB v2 caused in `[decimal_cast]` are fixed (in #2124's v2 port).
- **`/scratch` (GPFS) goes stale** now and then, for hours. Use `/opt/sirius-ci/datasets` (`env.sh` switches to it on its own), and remember that it is I/O-bound.
- **From the Oct 13 branch (pre-v2), not re-measured here:**
  - q18 ran out of GPU memory in a hash join when pinned with `PIN_COMPRESSION=1`.
  - SF3000 with one query per fresh cluster passed 20/22; q17 and q18 ran out of GPU memory. With all 22 in sequence on one cluster, 16–17/22 passed per iteration; the failures were GPU out-of-memory (q04, q08, q09, q17, q18). q02 hung with runtime filters off.
  - `--pin` pins all of `lineitem` and `orders` on every CN (about 115 GiB of host memory per CN at SF1000), so it doesn't fit at SF3000.
- **First query on a fresh cluster:** about 2 s slower. Each CN sets up its NIXL peers one at a time, and the FE is cold.
- **DECIMAL→DOUBLE:** DuckDB v2 rounds this cast correctly, and the GPU's cuDF cast may differ in the last bit. TPC-H results are compared with a tolerance, so it doesn't show there.
