# Two-GPU shuffle microbenchmark

Standalone CLI that times Sirius shuffle primitives on two GPUs. SQL query
latency is covered by the [pinned 1-vs-2 GPU suite](#query-1-vs-2-gpu-pinned-sql)
later in this file; this CLI stays at hash_partition plus clone_to.

There is no Shuffle operator. The bench composes the same two steps queries use:

1. Local `gpu_partition_impl::hash_partition` (slices stay on the source GPU).
2. Cross-GPU `lock_or_prepare_batch` → `clone_to` → `convert_gpu_to_gpu`.

## Build

The usual pixi build already reconfigures when `CMakeLists.txt` changes:

```bash
pixi run make                        # also builds duckdb + sirius_unittest
```

To compile only this binary after an existing release configure:

```bash
cd duckdb && cmake --build --preset release --target sirius_shuffle_benchmark
```

Binary (DuckDB extension build):

```
build/release/extension/sirius/test/cpp/sirius_shuffle_benchmark
```

## Run everything (recommended)

```bash
pixi run shuffle-bench
# or
scripts/benchmark/run_shuffle_microbench.sh
```

That script:

1. Prints topology / P2P / PCIe gen, warms the GPUs, and runs **nvbandwidth** if
   it is on `PATH` (`scripts/benchmark/run_nvbandwidth.sh`).
2. Runs `sirius_shuffle_benchmark --mode all`.
3. Prints nvbandwidth D2D vs Sirius `transfer` vs `shuffle` GB/s.

If `nvbandwidth` is missing the hardware sweep is skipped (exit 0) with an
install hint from https://github.com/NVIDIA/nvbandwidth. Do not vendor it.

`cudaDeviceCanAccessPeer==1` is **not** proof of peer DMA. The CLI logs
`probe_peer_dma_works` as `copy_path=peer_dma|host_staging|mixed`.

## CLI

```bash
sirius_shuffle_benchmark --help
sirius_shuffle_benchmark --mode all --sizes 1MiB,16MiB,256MiB,1GiB --reps 8
sirius_shuffle_benchmark --mode shuffle --csv /tmp/shuffle.csv
```

| `--mode` | What it times |
|---|---|
| `transfer` | `clone_to` GPU0→1, GPU1→0, then both directions overlapping |
| `partition` | overlapping `hash_partition(..., 2)` on each GPU |
| `shuffle` | partition + overlapping clone of remote slices until dest sync |
| `all` | all of the above (default) |

CSV columns: `size_bytes`, `mode`, `copy_path`, `partition_ms`, `transfer_ms`,
`shuffle_ms`, `remote_bytes_0to1`, `remote_bytes_1to0`, `imbalance`,
`partition_rows_per_s`, `effective_GBps`, `transfer_GBps`, `pcie_gen`,
`physical_copy_bytes`.

Remote payload is counted once. For `host_staging`, `physical_copy_bytes` is
`2 * remote` because Sirius issues same-stream D2H then H2D.

Requires ≥2 visible CUDA devices; exits 0 if only one GPU is present.

## Nsight

The binary emits NVTX ranges `partition`, `clone_0_to_1`, `clone_1_to_0`,
`clone_transfer`, and `shuffle_exchange`. Time dest-stream completion, not copy
enqueue.

## Query 1-vs-2 GPU (pinned SQL)

SQL-level counterpart to the primitive shuffle CLI. Pin parquet onto the GPU
tier, then time four aggregating queries with `topology.num_gpus: 1` vs `2`
in **two processes** (do not flip GPU count with `SET admission_bytes_per_gpu`
— pins still round-robin the full topology).

```bash
pixi run query-mgpu
# or
scripts/benchmark/run_query_mgpu.sh
scripts/benchmark/run_query_mgpu.sh --smoke          # 1e6 rows, result equality
scripts/benchmark/run_query_mgpu.sh --rows 10000000 --join-rows 20000000
```

Requires the release DuckDB CLI (`build/release/duckdb`) with Sirius linked.
Configs: `scripts/benchmark/sirius-1gpu.yaml` and `sirius-2gpu.yaml`.

Queries (tiny result `COUNT`/`SUM` so D2H is not the limit):

- `scan_filter` — local filter on pinned `t`. Expect ~2× and ~0 remote bytes.
- `groupby_highcard` — `GROUP BY k` on unique keys (partition + merge clone).
- `join_uniform` — large equijoin with `SET max_broadcast_join_size = 1`.
- `join_skew` — same join, ~90% of probe rows on key `0`.

Each Sirius execution window logs
`[clone_stats] bytes_0to1=… bytes_1to0=… clone_ms=… copies=…` from
`convert_gpu_to_gpu` (same-device clone and GPU→HOST result collection are
not counted). The driver joins those lines with client wall time.

CSV: `build/benchmark-results/query_mgpu.csv` (copy) and
`build/benchmark-results/query-mgpu/query_mgpu.csv`. Columns: `query`,
`num_gpus`, `phase`, `iter`, `wall_ms`, `remote_bytes_0to1`,
`remote_bytes_1to0`, `clone_ms`, `copies`, `admitted_gpus`, `pin_hit`.

The summary prints speedup = median(1-GPU wall) / median(2-GPU wall) next to
2-GPU remote GiB, clone_ms, and implied exchange GB/s. Compare that rate to
the shuffle microbench ceiling on this PIX host (~37 GB/s unidirectional
clone, ~33 GB/s shuffle effective at 1 GiB). If wall speedup is poor while
clone_ms is a small fraction of wall, the limit is compute, not the link.

