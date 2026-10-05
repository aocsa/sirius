# 4-GPU SF1000 TPC-H joins: build, set up and run from scratch

This runs seven TPC-H join queries (q14, q05, q07, q08, q09, q12, q19) at SF1000 on one
StarRocks FE and four Sirius compute nodes (CNs), one CN per GPU. The CNs exchange data over
NIXL. Every result is checked against DuckDB on the same parquet.

The driver is [`4cn_tpch_joins_sf1000.sh`](4cn_tpch_joins_sf1000.sh). This document covers
everything it needs on a fresh box: NIXL and UCX, the Sirius engine, the StarRocks FE and the
Rust CN.

Commands assume `bash`, and that the repo is cloned to `$WORK/sirius`, next to a
`$WORK/tools` directory for NIXL and UCX. `scripts/cn-env.sh` looks for
`<repo>/../tools` by default.

```bash
export WORK=$HOME/aocsa          # any directory; repo goes in $WORK/sirius, NIXL/UCX in $WORK/tools
```

## What to expect

From the reference run on 2026-10-05 (4× GB200, 189 GB each, 1.7 TB host RAM):

| query | result | wall time |
|---|---|---|
| q14 | PASS | 4 s |
| q05 | PASS | 3 s |
| q07 | PASS | 3 s |
| q08 | PASS | 3 s |
| q09 | FAIL: GPU out of memory (pool full at 156.4 GiB) | 2 s |
| q12 | PASS | 1 s |
| q19 | PASS | 1 s |
| NIXL hops | PASS (~624 GB shipped across the 4 CNs) | |

q09 running out of GPU memory is a known limit at SF1000, not a setup problem. Because of it,
the script exits 1 even when everything else passed.

Wall times count only the StarRocks query. On the first run DuckDB computes each oracle
answer after its query, which adds about a minute per query. The answers are cached, so later
runs skip that.

Build times on that box: engine ~15 min, FE ~10 min, CN ~2 min, NIXL and UCX ~10 min.

## 1. Box prerequisites

- **GPUs:** four NVIDIA GPUs with no MIG, and the driver installed. This was tested on
  GB200 (aarch64).
- **CUDA toolkit:** at `/usr/local/cuda`, for building UCX and NIXL. 13.0 works.
- **System toolchain:** `gcc`/`g++`, `make`, `pkg-config`, `git`, `numactl` and `curl`.
- **RDMA headers:** `libibverbs-dev` / `rdma-core`. MLNX OFED provides them.
- **[pixi](https://pixi.sh) ≥ 0.71:** installs everything else (CUDA 13 libraries, libcudf,
  Rust, JDK 17, Maven, Thrift, the mysql client, Python with DuckDB).
- **meson, ninja and pybind11:** for NIXL. `pip install --user meson ninja pybind11` is
  enough.
- **Network access:** the first builds download conda packages, Maven artifacts, Cargo
  crates and a Go module.
- **Data:** TPC-H SF1000 parquet at `/scratch/sirius/datasets/tpch_sf1000`, laid out as
  `<table>/*.parquet` for all eight tables. It is about 266 GB and should be on fast local or
  parallel storage, not NFS.
- **Ports:** the FE uses 9031, 8031, 9021 and 9011, and the CNs use 9100–9134. The script
  checks that these are free before starting.

## 2. Get the code

```bash
cd $WORK
git clone git@github.com:aocsa/sirius.git
cd sirius
git checkout feat/tpch-groupby-join-nixl
git submodule update --init --recursive
```

Check that the branch has the benchmark files. If any are missing, they haven't been pushed
yet:

```bash
ls experimental/starrocks/tests/4cn_tpch_joins_sf1000.sh experimental/starrocks/tests/patches/starrocks-fe-files-query-whole-file-ranges.patch
```

## 3. Build UCX and NIXL

These live outside the repo in `$WORK/tools`. `scripts/cn-env.sh` finds them there.

**UCX 1.21.0**, with CUDA support:

```bash
mkdir -p $WORK/tools && cd $WORK/tools
curl -LO https://github.com/openucx/ucx/releases/download/v1.21.0/ucx-1.21.0.tar.gz
tar xzf ucx-1.21.0.tar.gz && cd ucx-1.21.0
./configure --prefix=$WORK/tools/ucx-install --with-cuda=/usr/local/cuda --enable-mt
make -j"$(nproc)" install
```

**NIXL**, built against that UCX. The reference build used commit `4d030b94`
(`v1.3.1-81`):

```bash
cd $WORK/tools
git clone https://github.com/ai-dynamo/nixl nixl-src && cd nixl-src
git checkout 4d030b94
meson setup build --prefix=$WORK/tools/nvda_nixl -Ducx_path=$WORK/tools/ucx-install \
    -Dcmake_prefix_path="$(python3 -m pybind11 --cmakedir)"
ninja -C build install
```

Check that the UCX plugin was built:

```bash
ls $WORK/tools/nvda_nixl/lib/*-linux-gnu/plugins/libplugin_UCX.so
```

## 4. Build the Sirius engine

From the repo root. This builds `build/release/extension/sirius/libsirius.so`, which the CN
links against.

```bash
cd $WORK/sirius
pixi run make
```

If a build fails and you rebuild, run `pixi run make clean` first. A half-configured
`build/release` breaks the next configure.

## 5. Build the StarRocks FE

The FE needs one patch for this benchmark. Without it, the FE cuts FILES() scans of large
parquet files into byte ranges spread across different CNs, and the CN refuses them with
`byte-range splits do not tile the parquet file`. The patch adds an FE setting,
`files_query_whole_file_ranges`, which hands out whole files instead. The script turns it on
and stops if the FE doesn't recognize it.

```bash
cd $WORK/sirius/experimental/starrocks
git -C starrocks apply ../tests/patches/starrocks-fe-files-query-whole-file-ranges.patch
pixi install --all
THRIFT=$PWD/.pixi/envs/cn/bin/thrift pixi run fe-build
```

The `THRIFT=` override is required. StarRocks 4.1.3 builds against libthrift 0.23, but the
pixi `fe` env pins the 0.20 Thrift compiler, whose generated Java doesn't compile against it
(`wrong number of type arguments; required 3`). The `cn` env's 0.22 compiler generates
compatible code.

The FE ends up in `starrocks/output/fe`. Before rebuilding the FE over an older build, run
`pixi run fe-clean`. `fe-build` doesn't remove old jars, and two versions of the same jars
in `lib/` end up together on the classpath.

## 6. Build the CN

Build with `scripts/cn-env.sh` sourced, not with `pixi run cn-build`. The env script sets the
NIXL and UCX paths, the clang headers bindgen needs for `nixl-sys`, and the system linker.

The link also needs a workaround, which is what the symlink below is for. The repo-root pixi
env ships a `libcusolver.so.12` with no symbol versioning, while `libcuvs` asks for versioned
symbols, so the final link fails with `undefined reference to cusolverDn...@libcusolver.so.12`.
The fix points the linker at the StarRocks env's versioned copy, for the link only. At
runtime the unversioned library works; the CN just prints a `no version information
available` warning.

```bash
cd $WORK/sirius/experimental/starrocks
mkdir -p $WORK/tools/cusolver-link-shim
ln -sf "$(ls $PWD/.pixi/envs/default/targets/*/lib/libcusolver.so.12.* | head -1)" \
    $WORK/tools/cusolver-link-shim/libcusolver.so.12
SHIM=$WORK/tools/cusolver-link-shim pixi run -e default bash -c \
    'source scripts/cn-env.sh && LD_LIBRARY_PATH="$SHIM:$LD_LIBRARY_PATH" cargo build --release -p sirius-starrocks-cn'
```

Check that the binary starts and finds its libraries:

```bash
pixi run -e default bash -c 'source scripts/cn-env.sh && target/release/sirius-starrocks-cn --help'
```

## 7. Run the benchmark

Make sure nothing else is using the GPUs. The script refuses to start otherwise, because
each CN reserves 85% of its GPU up front.

```bash
nvidia-smi --query-compute-apps=pid,used_memory --format=csv
```

Then, from the repo root:

```bash
cd $WORK/sirius
experimental/starrocks/tests/4cn_tpch_joins_sf1000.sh 2>&1 | tee 4cn-sf1000.log
```

The script:

1. starts an isolated FE (its own config, metadata and logs) on port 9031;
2. starts four CNs on GPUs 0–3, each bound to the CPU socket of its GPU;
3. waits until the FE reports all four as alive;
4. runs each query and compares the result with DuckDB;
5. checks that every CN sent and received data over NIXL;
6. prints a summary and shuts everything down.

A query that fails doesn't stop the run. A CN that dies does: the remaining queries are
reported as SKIP.

**Output:**

- `/scratch/$USER/sirius-4cn-sf1000-joins/run/`: each query's result (`qNN.tsv`), errors
  (`qNN.err`), the FE logs (`fe/log/`) and each CN's log (`cnN.log`). The directory is
  wiped at the start of each run.
- `/scratch/$USER/sirius-4cn-sf1000-joins/oracle/`: the cached DuckDB answers, which are
  kept between runs.

### Settings

All of these are environment variables with defaults:

| variable | default | meaning |
|---|---|---|
| `TPCH_DATA` | `/scratch/sirius/datasets/tpch_sf1000` | dataset root |
| `TPCH_QUERIES` | `q14 q05 q07 q08 q09 q12 q19` | queries from `tests/tpch/` |
| `GPUS` | `0 1 2 3` | one CN per listed GPU |
| `GPU_FRACTION` | `0.85` | share of each GPU reserved for the CN's memory pool |
| `HOST_BYTES` | 128 GiB | host spill memory per CN |
| `PIPELINE_THREADS` | `4` | engine pipeline threads per CN |
| `NUMA_BIND` | `1` | bind each CN to its GPU's CPU socket |
| `QUERY_TIMEOUT_S` | `3600` | FE query timeout |
| `DUCKDB_MEMORY_LIMIT` | `512GB` | cap for the DuckDB oracle |
| `FE_QUERY_PORT`, `FE_HTTP_PORT`, `FE_RPC_PORT`, `FE_EDIT_LOG_PORT` | 9031, 8031, 9021, 9011 | FE ports |
| `PORT_BASE`, `PORT_STRIDE` | 9100, 10 | CN *i* uses ports `PORT_BASE + i*PORT_STRIDE` to `+4` |
| `RUN_ROOT` | `/scratch/$USER/sirius-4cn-sf1000-joins` | run and oracle directories |
| `ALLOW_BUSY_GPUS` | `0` | set to `1` to skip the busy-GPU check |

Examples:

```bash
TPCH_QUERIES="q05 q09" experimental/starrocks/tests/4cn_tpch_joins_sf1000.sh
```

```bash
GPUS="0 1" TPCH_DATA=/scratch/sirius/datasets/tpch_sf100 experimental/starrocks/tests/4cn_tpch_joins_sf1000.sh
```

## Troubleshooting

| symptom | cause and fix |
|---|---|
| `port N is already in use` | Another FE or CN holds the port. Set the `FE_*_PORT` or `PORT_BASE` variables. |
| `FE ... lacks files_query_whole_file_ranges` | The FE was built without the patch. Redo step 5. |
| `failed to create FE plugin dir` | Comes from an older copy of the script that symlinked `plugins/`. Use the current script. |
| FE build: `wrong number of type arguments; required 3` | The FE was built without the `THRIFT=` override from step 5. |
| CN build: `'stdbool.h' file not found` (nixl-sys) | The CN was built without `scripts/cn-env.sh`. Use the command in step 6. |
| CN link: `undefined reference to cusolverDn...@libcusolver.so.12` | The link ran without the cuSOLVER symlink from step 6. |
| `no nixl install at .../tools/nvda_nixl` | NIXL isn't in `<repo>/../tools`. Set `TOOLS_DIR` or `NIXL_PREFIX`. |
| engine configure: `patch does not apply` (testcontainers) | `build/release` is stale. Run `pixi run make clean`, then `pixi run make`. |
| `out_of_memory ... Maximum pool size exceeded` | The query needs more GPU memory than one CN's pool. This is expected for q09 at SF1000. |
| a CN log shows `backend 'UCX' not found` | The UCX plugin failed to load. Check that `libplugin_UCX.so` exists (step 3), and that `cn-env.sh` puts the NIXL and UCX libraries ahead of the pixi ones on `LD_LIBRARY_PATH`. |
