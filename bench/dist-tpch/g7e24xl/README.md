# Distributed TPC-H on 4x RTX PRO 6000 (AWS g7e.24xlarge)

How to build, test and benchmark the distributed (one process per GPU) Sirius on this host, and
what it measured: TPC-H SF3000 and SF10000 cold on 2 and 4 GPUs, a per-rank scalability profile,
and the exchange's host-staged path for GPUs without peer access. The 1-GPU comparison point is
integ's best SF3000 cold run (`integ/sf3k-joost-2201-2204`, `bench/sf3k-sf10k-1gpu-repro`): 277.2 s.

## Host

| | |
|---|---|
| Instance | AWS g7e.24xlarge |
| GPUs | 4x RTX PRO 6000 Blackwell Server Edition, 97,887 MiB each, sm_120, PCIe Gen5 |
| GPU topology | GPU0-GPU1 and GPU2-GPU3 each share a PCIe switch (`PIX`); the pairs meet only at the host bridge (`NODE`). Peer access (`nvidia-smi topo -p2p r`) works inside a pair and is `NS` across them. |
| CPU / memory | 96 vCPUs (48 cores x 2), one NUMA node, 999.5 GiB |
| Storage | 2x 3.5 TB NVMe striped at `/opt/dlami/nvme`: data and spill. Raw O_DIRECT reads: 13.5 GB/s (`disk_ceiling.sh`) |
| Data | `/opt/dlami/nvme/tpch_sf3000_regenerated`, `/opt/dlami/nvme/tpch_sf10000_regenerated` (one directory per table, byte-identical to the campaign's) |
| Driver | 595.91.07 |

## What this branch adds

**Host-staged exchange** (`src/exchange/nixl_fabric.*`). UCX moves GPU memory between processes with
`cuda_ipc`, which needs peer access. Between GPUs 0/1 and 2/3 it has none, and UCX falls back to
emulating the GPU-to-GPU put in software over TCP (`results/ucx-tcp-emulation.txt`): 0.3 GB/s.
Every rank now keeps a pinned mirror of its receive ring in POSIX shared memory; to a peer whose
GPU NVML reports without peer access, a write is one device-to-host copy into that peer's mirror,
then a notification; the peer copies the bytes host-to-device into its receive ring and only then
hands the write's notification to the transport. Peer pairs keep NIXL. `SIRIUS_EXCHANGE_HOST_STAGING`
= `auto` (default), `always` or `never`. See `docs/super-sirius/exchange.md`.

**This folder.** `configs/` (the Sirius YAML per rank), `build_nixl.sh`, and the harness:

| File | Purpose |
|---|---|
| `dist_cold.py` | Distributed counterpart of integ's `tools/benchmark.py --mode cold`: one process per rank, `reset_sirius_cache()` before each query, `.timer`, rank 0's `result.csv` per query |
| `finalize.py`, `cpu_schema.py` | Adds each query's `schema.csv` (CPU-only DESCRIBE), then validates with integ's `tools/validate.py` against the saved campaign outputs |
| `profile.py`, `analyze.py`, `compare.py` | Per-rank scalability profile: query windows, per-rank disk bytes, CPU, GPU utilization, PCIe (NVML) and Quent pipelines |
| `gpu_step.sh` | Runs a step under a shared GPU lock (`GPU_LOCK`), only with idle GPUs and enough free memory |
| `disk_ceiling.sh` | Raw read throughput of the data array |

## Build

```bash
git clone -b <this branch> https://github.com/aocsa/sirius.git && cd sirius
git submodule update --init --recursive
pixi install
# NIXL v1.5.0, UCX plugin only, against the pixi env's CUDA 13.4 and CUDA-enabled UCX 1.20.1
pixi run bash bench/dist-tpch/g7e24xl/build_nixl.sh          # -> ~/tools/nvda_nixl
CUDAARCHS=120-real NIXL_PREFIX=$HOME/tools/nvda_nixl bash bench/dist-tpch/build.sh > build.log 2>&1
```

At run time `scripts/nixl_env.sh` needs `UCX_PREFIX=$PWD/.pixi/envs/default` (the pixi env's UCX).

## Tests

```bash
export UCX_PREFIX=$PWD/.pixi/envs/default
CUDA_VISIBLE_DEVICES=0 pixi run build/release/test/cpp/sirius_unittest \
  "[can_serve],[byte_range],[exchange],[pin_table],[pin_table_host],[pin_parquet],[pin_lifecycle],[compressed_materialization],[compression]"
CUDA_VISIBLE_DEVICES=2,3 pixi run build/release/test/cpp/sirius_unittest "[pin_mgpu]"
pixi run bash test/scripts/distributed_pin_narrowing.sh /tmp/$USER/distributed_pin_narrowing
# Exchange across processes: any GPU list, either path
GPUS=0,2 pixi run bash scripts/exchange_nixl_ranks.sh 2
GPUS=0,1 SIRIUS_EXCHANGE_HOST_STAGING=always pixi run bash scripts/exchange_nixl_ranks.sh 2
pixi run bash scripts/exchange_nixl_ranks.sh 4
# Bandwidth per GPU pair and path
GPUS=0,2 SIRIUS_EXCHANGE_HOST_STAGING=never TEST="[nixl_bandwidth]" pixi run bash scripts/exchange_nixl_ranks.sh 2
```

On the benchmarked build: unit 147 cases (226,020 assertions) passed, `[pin_mgpu]` 7 cases passed,
the two-rank pin test printed its PASS line, and the multi-process exchange test passed on GPUs
0,2 (host-staged), 0,1 (NIXL), 0,1 with staging forced, and 0-3.

## Configs

`configs/sf3k-unpinned.yaml` is integ's `configs/sf3k-1gpu.yaml` per rank, with three differences:
`downgrade_root_dirs` is `@SPILL_DIR@` (the harness substitutes a per-run directory), and
`scan_manager.max_readahead_bytes` and `compression.enable_spill_compression` are dropped: their
commits (PR 2155, the spill-compression stack) are not on this branch, which rejects unknown keys.
Everything else is integ's: GPU fractions 0.908/0.9/0.9/0.85, a 240 GB host pool pinned at
startup, 1 TB disk tier, memory prefetcher, 16 scan threads, `cache: sirius` LRU 0.2, 4 GB scan
batches, 12 GB hash partitions and build tables, `enable_probe_accumulate: true`, Quent off.

`configs/sf10k-unpinned.yaml` is integ's `configs/sf10k-1gpu.yaml` (SF3K plus a 4 GiB Bloom cap)
the same way, with a 600 GB disk tier per rank so 4 ranks cannot fill the 3.1 TB free.

`configs/sf3k-pinned.yaml` is the unpinned one without `initial_number_pools`: four ranks
pre-pinning 240 GB each on the one 999.5 GiB NUMA node put it past `pin_table_memory_limit`, and
every `pin_table` then fails. Not benchmarked here.

## Running

Every heavy step on this shared host ran under `gpu_step.sh` (`flock` on `GPU_LOCK`, idle GPUs,
free memory) after dropping the page cache. Each rank gets an equal slice of cores and their
hyperthreads (`dist_cold.py`: 48 / ranks cores; `profile.py`: `--vcpus 24` per rank) and DuckDB
threads equal to its vCPUs. The reference outputs live in integ's `d263b69e`:

```bash
mkdir -p /tmp/repro && git archive d263b69e bench/sf3k-sf10k-1gpu-repro | tar -x -C /tmp/repro
G=bench/dist-tpch/g7e24xl
sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
pixi run python $G/dist_cold.py --ranks 4 --config $G/configs/sf3k-unpinned.yaml \
  --data /opt/dlami/nvme/tpch_sf3000_regenerated --out /tmp/$USER/4gpu-sf3k-cold
pixi run python $G/finalize.py /tmp/$USER/4gpu-sf3k-cold /tmp/repro/bench/sf3k-sf10k-1gpu-repro
# SF10000: --scale 10000 --config $G/configs/sf10k-unpinned.yaml --data .../tpch_sf10000_regenerated
# Profile: Q1, Q6, Q9, Q18, one cold and two warm runs each, Quent on
pixi run python $G/profile.py --gpus 0,2 --out /tmp/$USER/profile-gpus-0-2 --quent
pixi run python $G/analyze.py /tmp/$USER/profile-gpus-0-2 > profile.md
```

## Results

### SF3000, cold, all 22 queries (seconds)

| Query | 1 GPU (integ) | 2 GPUs (0,1) | 4 GPUs before host staging | **4 GPUs host-staged** |
|---|---:|---:|---:|---:|
| Q1 | 12.12 | 10.15 | 9.96 | **9.97** |
| Q2 | 2.22 | 1.95 | 5.20 | **1.67** |
| Q3 | 11.18 | 10.65 | 32.02 | **10.71** |
| Q4 | 6.46 | 6.27 | 22.09 | **6.23** |
| Q5 | 12.78 | 12.47 | 20.08 | **12.51** |
| Q6 | 8.59 | 8.90 | 8.88 | **8.88** |
| Q7 | 15.21 | 15.07 | 42.61 | **14.83** |
| Q8 | 18.22 | 17.83 | 26.10 | **17.77** |
| Q9 | 28.97 | 22.17 | 116.10 | **20.74** |
| Q10 | 22.82 | 12.89 | 94.94 | **12.81** |
| Q11 | 3.47 | 3.20 | 3.83 | **2.95** |
| Q12 | 8.68 | 8.93 | 33.36 | **8.88** |
| Q13 | 8.62 | 8.28 | 26.40 | **8.00** |
| Q14 | 13.29 | 13.07 | 24.36 | **12.98** |
| Q15 | 12.82 | 12.69 | 18.13 | **12.29** |
| Q16 | 1.80 | 1.45 | 16.88 | **1.16** |
| Q17 | 15.84 | 15.91 | 11.78 | **11.62** |
| Q18 | 22.61 | 12.38 | 121.91 | **8.84** |
| Q19 | 12.60 | 12.64 | 13.06 | **12.67** |
| Q20 | 14.81 | 15.19 | 30.00 | **15.33** |
| Q21 | 21.16 | 19.58 | 75.14 | **12.56** |
| Q22 | 2.92 | 2.57 | 4.62 | **2.32** |
| **Total** | **277.2** | **244.2** | **757.4** | **225.7** |

Every 2- and 4-GPU answer validated (22/22 each) against the saved campaign outputs. The other
session's single-process 2-GPU integ run took 248.1 s. Shuffle-heavy queries scale with GPUs
(Q18 22.6 -> 12.4 -> 8.8 s, Q21 21.2 -> 19.6 -> 12.6 s); scan-heavy ones (Q1, Q5-Q8, Q14, Q19,
Q20) do not, because cold they read at the array's 13.5 GB/s whatever the GPU count.

### Scalability profile (`results/profile/`), seconds, cold / best warm

| Query | 1 GPU | 2 ranks, GPUs 0,1 | 2 ranks, GPUs 0,2 before -> host-staged | 4 ranks before -> host-staged |
|---|---:|---:|---:|---:|
| Q1 | 12.32 / 11.63 | 10.18 / 9.57 | 10.27 / 9.54 -> 10.27 / 9.40 | 10.00 / 2.64 -> 10.00 / 2.58 |
| Q6 | 9.05 / 8.89 | 8.85 / 8.77 | 8.89 / 8.70 -> 8.85 / 8.75 | 8.89 / 1.29 -> 8.87 / 1.29 |
| Q9 | 43.31 / 37.19 | 22.34 / 21.95 | 139.82 / 139.36 -> **22.31 / 22.17** | 111.10 / 113.46 -> **21.02 / 18.91** |
| Q18 | 18.35 / 18.47 | 12.56 / 11.95 | 132.07 / 130.68 -> **11.68 / 11.04** | 123.38 / 128.34 -> **8.82 / 4.77** |

What it showed, per rank: the work is split evenly (equal disk bytes, equal bytes and task counts
in every heavy pipeline, ranks finish within 0.01-0.3 s of each other). Before host staging, the
pipelines ending in an exchange to a GPU without peer access ran at about 0.7 GB/s with the GPUs
at 11-13% and four times the CPU time: the TCP emulation. Warm Q1/Q6 on 4 ranks read nothing from
disk because four 48 GB scan caches hold their columns; one or two ranks' caches do not.
`before-vs-host-staged.md` has the before/after table for all four layouts; Q9 on 1 GPU varied
37-43 s between runs.

### Exchange bandwidth

See `results/bandwidth.md`: across the pairs 0.29 GB/s (NIXL) -> 25.7 GB/s (host-staged), as fast
as the peer pair (26.8 GB/s); 4 ranks 0.44 -> 22.0 GB/s.

### SF10000, cold (seconds)

| Query | 1 GPU (integ) | 2 GPUs | 4 GPUs |
|---|---:|---:|---:|
| Q1 | - | 31.89 | 31.55 |
| Q2 | - | 5.21 | 4.91 |
| Q3 | - | 34.94 | 34.44 |
| Q4 | 20.45 | 19.75 | 19.59 |
| Q5 | - | 42.35 | 42.46 |
| Q6 | - | 29.68 | 29.82 |
| Q7 | - | 48.49 | 48.60 |
| Q8 | - | 61.33 | 61.37 |
| Q9 | 137.18 | 180.80 | **80.63** |
| Q10 | - | 44.80 | 40.68 |
| Q11 | - | 9.82 | 9.28 |
| Q12 | - | 28.11 | 28.08 |
| Q13 | - | 39.56 | 28.95 |
| Q14 | - | 44.34 | stopped |
| Q15 | - | **failed** | stopped |
| Q16 | - | 3.88 | stopped |
| Q17 | 59.18 | 59.05 | stopped |
| Q18 | - | 77.00 | stopped |
| Q19 | - | 43.13 | stopped |
| Q20 | - | **failed** | stopped |
| Q21 | 91.35 | 85.71 | stopped |
| Q22 | - | 8.54 | stopped |

- 2 GPUs: 20 of 22 answers match the reference (898.4 s for those 20). Q15 and Q20 failed on rank 0:
  `std::bad_alloc: Not enough free blocks available in fixed_size_host_memory_resource ... only 111
  free blocks available from 1 requested blocks` (240 GB host pool per rank). Not diagnosed.
- 4 GPUs: stopped after Q13 at the user's request; Q1-Q13 all match the reference.
- Q9 on 2 GPUs (180.8 s) is slower than integ's 1 GPU (137.2 s), and 80.6 s on 4. Not diagnosed;
  the integ build also has spill compression and PR 2155, which this branch lacks.

## Notes and limits

- **Cold scans are disk-bound.** Most queries take the same time on 1, 2 and 4 GPUs cold; more GPUs
  pay off for shuffle-heavy queries and for warm runs whose columns fit the ranks' caches.
- **Host-staging mirrors** are `/dev/shm/sirius-exchange-<pid>-<rank>` (`recv frames x 8 MiB`,
  192 MiB with 3 peers). A rank that is killed leaves its mirror behind; remove it by hand.
- **Not on this branch:** integ's PR 2155 (readahead/cache budget; conflicts with upstream #2006),
  the spill-compression stack, and the experimental `SIRIUS_EXP_DECODE_TO_TARGET` and
  `SIRIUS_EXP_PIN_UNIQUE_JOIN` features. `enable_probe_accumulate` is documented single-GPU; each
  rank sees one GPU and every answer validated.
- **One run per configuration**: differences under about 10% are not established.
