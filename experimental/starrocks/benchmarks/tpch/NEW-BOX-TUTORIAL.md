# Engine B on a CPU cluster

Stand up stock StarRocks 3.5.20 (engine B) on a CPU-only box, point it at TPC-H parquet,
and run the 22-query sweep. There are no Sirius CNs and no GPUs in this path. The
parallelism knob is **BE count** (`--bes`), not `NUM_CNS`.

Queries are `FILES()` scans of parquet. There is no load into native tables.

Companion files:

| File | Role |
|---|---|
| `run-b.sh` | Engine B sweep. This is the command you run. |
| `run-abc.sh` | Underlying harness (`--engines B`). You do not need A or C. |
| `configs/gb200-4gpu/engine-b/setup-engine-b-gb200.sh` | Lays out FE/BE trees and confs. Starts nothing. |
| `configs/gb200-4gpu/engine-b/README.md` | Memory arithmetic behind the committed confs. |

The committed confs were sized for a 2-socket 144-core Grace box with ~957 GiB DRAM.
On a new CPU cluster you keep the layout script, then **edit `mem_limit` and `num_cores`**
for this machine before the first start.

---

## 1. Inventory the box

```bash
lscpu | grep -E 'Socket|Core|NUMA|Model name'
numactl --hardware
free -g
df -PT / /data /raid $HOME 2>/dev/null
swapon --show
echo "JAVA_HOME=${JAVA_HOME:-unset}"; java -version 2>&1 | head -1
nproc
```

Write down:

1. **Sockets and NUMA.** `--bes 2` means one BE per CPU NUMA node. Typical: 2 sockets →
   nodes 0 and 1. `start_be.sh --numa N` is only valid for nodes that have CPUs.
2. **Physical cores** (not hyperthreads if you can tell them apart). `num_cores` in each
   `be.conf` must be cores this BE is allowed, usually `nproc / --bes`. StarRocks' CpuInfo
   **ignores** the cpuset, so without `num_cores` a pinned BE still reports the whole box.
3. **DRAM.** Size `mem_limit` against real host RAM. If this box also has GPUs whose HBM
   shows up as CPU-less NUMA nodes, `free -g` is inflated; sum only NUMA nodes that have
   CPUs. On a CPU-only cluster, `free` is usually the right number.
4. **Local disk.** Parquet, BE storage, spill, FE meta, and logs must be local NVMe/ext4,
   not NFS. `$HOME` is often NFS on shared clusters.
5. **Swap.** If swap is off, an oversized `mem_limit` is an OOM-kill, not a slowdown.
6. **JDK 17+.** The FE needs it. `FILES()` on the BE goes through libhdfs/JNI, so the BE
   needs `JAVA_HOME` too.

You need the sirius repo for the query files and `run-b.sh`. You do **not** need to build
Sirius, nixl, or CUDA.

---

## 2. Paths

```bash
export REPO=/path/to/sirius
export SR=$REPO/experimental/starrocks
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64   # or java-21-openjdk-arm64
export PATH=$JAVA_HOME/bin:$SR/.pixi/envs/default/bin:$PATH

# TPC-H parquet parent. run-b.sh looks for $root/tpch_parquet_sf<N>
export TPCH_DATA_ROOTS=/data/$USER/tpch
export DATA=$TPCH_DATA_ROOTS/tpch_parquet_sf100

# Binaries may live on NFS. Storage must be local.
export B_DIR=$HOME/starrocks-bench
export B_DATA_ROOT=/data/$USER/sr-bench

export ABC_OUT_ROOT=/data/$USER/benchmark-results
```

`mysql` is not a system package on most of these boxes. The starrocks pixi env provides
it (`$SR/.pixi/envs/default/bin/mysql`). `run-b.sh` prepends that PATH. If pixi is not
installed yet:

```bash
curl -fsSL https://pixi.sh/install.sh | bash
cd $SR && pixi install
```

Default data search in `run-abc.sh` is this GB200's `/raid/prestouser/...`. On a new
cluster always set `TPCH_DATA_ROOTS` or pass `--data`.

```bash
./run-b.sh --sf 100 --data /data/$USER/tpch/tpch_parquet_sf100 \
           --out /data/$USER/benchmark-results/b-sf100
```

---

## 3. TPC-H dataset

Required layout:

```
$DATA/
  customer/*.parquet
  lineitem/*.parquet
  nation/*.parquet
  orders/*.parquet
  part/*.parquet
  partsupp/*.parquet
  region/*.parquet
  supplier/*.parquet
```

Name the directory `tpch_parquet_sf<N>` so `--sf N` finds it under `TPCH_DATA_ROOTS`.
Otherwise pass `--data` every time.

Generate (from the repo; `tpchgen-rs`, same bytes as classic dbgen):

```bash
cd $REPO/test/tpch_performance
pixi run bash generate_tpch_data.sh 100 --format parquet \
  --output $TPCH_DATA_ROOTS/tpch_parquet_sf100
```

Or:

```bash
tpchgen-cli --output-dir=$TPCH_DATA_ROOTS/tpch_parquet_sf100 \
            --format=parquet -s 100
```

| SF | Parquet size | Use |
|---|---|---|
| 1 | ~0.3 GB | smoke |
| 100 | ~26 GB | default |
| 500 | ~132 GB | |
| 1000 | ~265 GB | retune `mem_limit` first, see §6 |

Regenerating produces different bytes than an existing copy and invalidates previous
numbers for that SF. To match a recorded run, copy that dataset onto local disk.

```bash
df -PT $DATA | tail -1    # must not be nfs/nfs4/gpfs
ls $DATA/lineitem/*.parquet
```

---

## 4. Get the StarRocks binaries

Engine B is **StarRocks 3.5.20**, copied out of the official artifacts image. Do not build
the vendored submodule (that checkout is 4.1.1).

```bash
export B_DIR=$HOME/starrocks-bench
mkdir -p $B_DIR
docker pull starrocks/artifacts-ubuntu:3.5.20
docker create --name sr-extract starrocks/artifacts-ubuntu:3.5.20 true
docker cp sr-extract:/release/fe_artifacts/fe $B_DIR/fe \
  || docker cp sr-extract:/fe $B_DIR/fe
docker cp sr-extract:/release/be_artifacts/be $B_DIR/be \
  || docker cp sr-extract:/be $B_DIR/be
docker rm sr-extract
```

Pull the image for this CPU arch (`linux/amd64` or `linux/arm64`). Confirm:

```bash
file $B_DIR/be/lib/starrocks_be
# unzip -p $B_DIR/fe/lib/starrocks-fe.jar com/starrocks/common/Version.class \
#   | strings | grep -E '^[0-9]+\.[0-9]+\.[0-9]+'
```

Expect `3.5.20` (git like `4d17879`). If Docker is not on the bench node, extract on a
machine that has it and copy `$B_DIR` over.

Do **not** run `benchmarks/tpch/setup-engine-b.sh`. It stamps `mem_limit=16G`.

---

## 5. Lay out trees and edit confs

```bash
cd $SR/configs/gb200-4gpu/engine-b
export JAVA_HOME
export B_DIR=${B_DIR:-$HOME/starrocks-bench}
export DATA_ROOT=$B_DATA_ROOT          # local disk

NUM_BES=2 ./setup-engine-b-gb200.sh    # copies be/ → be1, be2; installs confs; starts nothing
```

The setup script currently asserts NUMA nodes 0 and 1 have CPUs (2-socket). That is the
supported topology. `--bes 4` is a sensitivity variant with GB200 half-socket CPU maps
(`0-35`, `36-71`, …). On a CPU cluster that is not 144 cores in that layout, stay on
**`--bes 2`**.

Then edit the installed files for **this** box:

| Key | File | What to set |
|---|---|---|
| `mem_limit` | `$B_DIR/beN/conf/be.conf` | Absolute bytes, not `%`. See §6. |
| `num_cores` | same | Physical cores this BE owns, usually `nproc / 2`. |
| `storage_root_path`, `spill_local_storage_dir`, `sys_log_dir` | same | Under `$B_DATA_ROOT/beN/`, local disk. |
| `meta_dir` | `$B_DIR/fe/conf/fe.conf` | `$B_DATA_ROOT/fe/meta` |

`run_mode` stays **unset** (shared-nothing). Setting `shared_data` makes every `FILES()`
query fail with "No available backends".

`disable_storage_page_cache = true` stays. This bench has no native OLAP tables; that
cache would only reserve RAM.

---

## 6. Scale factor: memory and timeouts

`--sf N` picks `tpch_parquet_sfN` and scales **timeouts**. Memory does not auto-scale.
Edit `be.conf` before the sweep.

### Timeouts (automatic)

warm = `max(90, 1.8*SF)` s, cold = `max(300, 6*SF)` s.

| SF | warm | cold |
|---|---|---|
| 1–50 | 90 s | 300 s |
| 100 | 180 s | 600 s |
| 500 | 900 s | 3000 s |
| 1000 | 1800 s | 6000 s |

Override with `run-abc.sh --warm-timeout` / `--cold-timeout` if a query is healthy but
slow on this box.

### `mem_limit`

Never a percentage. StarRocks resolves `%` against `/proc/meminfo MemTotal`. On a GPU
box that includes HBM; on a CPU box it is usually fine, but an absolute value is still
the thing you can audit.

```
fixed ≈ FE JVM (10–20 GiB) + kernel/daemons
page_cache_target ≈ 1.5 × dataset size   # so a warm scan hits page cache
mem_limit_per_be ≈ (DRAM - fixed - page_cache_target) / NUM_BES
```

If you `membind` a BE to one socket, also check the **per-node** ceiling. A BE that
exceeds its node's DRAM is OOM-killed; it does not fall back to the other socket.

Worked example, 2 BEs, ~957 GiB DRAM (the GB200 CPU-side budget):

| SF | Dataset | `mem_limit` / BE | `datacache_disk_size` |
|---|---|---|---|
| 1–100 | ≤26 GB | 240G | 0 |
| 500 | ~132 GB | 240G | 0 |
| 1000 | ~265 GB | 224G | 200G |

On a 768 GiB Graviton box (2 BEs), a starting point at SF100 is on the order of
`mem_limit = 200G` each, leaving ~300 GiB for page cache and the FE. Recalculate; do not
copy 240G blindly.

`datacache_mem_size` (32G in the committed confs) is the cache `FILES()` actually uses.
It comes out of `mem_limit`. At SF1000, `datacache_disk_size = 200G` on local disk helps
when DRAM cannot hold the whole dataset.

---

## 7. BE count

| `--bes` | Meaning |
|---|---|
| **2** | Headline. One BE per CPU socket. `be1 --numa 0`, `be2 --numa 1`. |
| **4** | Sensitivity only. `run-b.sh` pins with GB200 half-socket CPU sets. Do not use this on a different core map. |

Two BEs keep shuffle in-process inside each socket. Four BEs on the same cores add
loopback-TCP shuffle for no extra hardware.

```bash
./run-b.sh --sf 100 --bes 2 --setup --data $DATA
```

`--setup` re-runs the layout script with `NUM_BES` matching `--bes`. After changing
`--bes`, run `--setup` once so `be3`/`be4` exist (or so you are back on the 2-BE confs).
The harness aborts if `SHOW BACKENDS` `CpuCores` does not match `num_cores` in `be.conf`.

---

## 8. Run the benchmark

```bash
cd $SR/benchmarks/tpch

# first time
./run-b.sh --sf 100 --bes 2 --setup --data $DATA

# later
./run-b.sh --sf 100 --bes 2 --data $DATA --out $ABC_OUT_ROOT/b-sf100

# smoke
./run-b.sh --sf 1 --queries "q01 q06" --data $DATA --dry-run
```

What `run-b.sh` does:

1. Optional `--setup` (trees + confs, no processes).
2. Waits until no `starrocks_be` / `StarRocksFE` is running. It does not kill anything.
3. Starts the FE (membind to CPU NUMA nodes) and `--bes` backends.
4. `ALTER SYSTEM ADD BACKEND` on heartbeat ports 9050, 9052, …
5. Gates: exactly N backends Alive, `CpuCores` = `num_cores`, `mem_limit` not a `%`.
6. Runs all 22 queries (or `--queries`): run 0 = cold, runs 1..`--runs` = warm (default 3).
7. Stops FE and BEs.

CSV: `$OUT/results.csv` with columns
`engine,scale,query,run,phase,status,ms,rows`. Provenance:
`$OUT/engineB/provenance.txt`.

Take medians over `phase=warm`. Keep `refused` / `wedge` / `empty` rows. `status=pass`
means exit 0 and ≥1 row; the harness does not check values against DuckDB.

q11: the staged copy rewrites the HAVING fraction to `0.0001/SF`. The repo `q11.sql` is
not modified.

---

## 9. Quick start

```bash
export REPO=/path/to/sirius
export SR=$REPO/experimental/starrocks
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH=$JAVA_HOME/bin:$SR/.pixi/envs/default/bin:$PATH
export TPCH_DATA_ROOTS=/data/$USER/tpch
export B_DIR=$HOME/starrocks-bench
export B_DATA_ROOT=/data/$USER/sr-bench
export ABC_OUT_ROOT=/data/$USER/benchmark-results

# data
cd $REPO/test/tpch_performance
pixi run bash generate_tpch_data.sh 100 --format parquet \
  --output $TPCH_DATA_ROOTS/tpch_parquet_sf100

# binaries: docker pull / docker cp as in §4, then:
cd $SR/configs/gb200-4gpu/engine-b
NUM_BES=2 DATA_ROOT=$B_DATA_ROOT ./setup-engine-b-gb200.sh
# edit $B_DIR/be{1,2}/conf/be.conf: mem_limit, num_cores, storage paths

cd $SR/benchmarks/tpch
pgrep -af 'starrocks_be|StarRocksFE'    # must be empty
./run-b.sh --sf 100 --bes 2 --data $TPCH_DATA_ROOTS/tpch_parquet_sf100
```

---

## 10. Traps

- **`mem_limit = 90%`.** Prefer an absolute value you computed in §6.
- **`num_cores` left at 0.** Each BE reports `nproc` and sizes thread pools for the whole
  box while `numactl` gives it half.
- **Dataset or `$B_DATA_ROOT` on NFS.** You are measuring the network.
- **`JAVA_HOME` unset on the BE.** Every `FILES()` query fails in JNI/libhdfs.
- **`--bes 4` on a non-144-core map.** The 4-BE pin is GB200-specific. Use `--bes 2`.
- **`--bes 4` without `--setup`.** 2-BE confs on a 4-BE launch. The `CpuCores` gate aborts.
- **Quoting `status=pass` as correct.** Diff row counts (and values) against DuckDB if
  you need correctness.

---

## 11. Customizing a run

| Want | Do |
|---|---|
| Different SF | `--sf N` and a `tpch_parquet_sfN` dir; retune `mem_limit` (§6) |
| Dataset path | `--data /abs/path` or `export TPCH_DATA_ROOTS=...` |
| 2 backends (default) | `./run-b.sh --sf N --bes 2` |
| 4 backends | only if the core map matches; `./run-b.sh --sf N --bes 4 --setup` |
| Subset of queries | `--queries "q01 q06 q14"` |
| More warm runs | `--runs 5` (run 0 is still an extra cold) |
| Results directory | `--out /path` |
