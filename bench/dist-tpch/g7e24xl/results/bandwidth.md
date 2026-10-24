# Exchange bandwidth, one rank per process

`[nixl_bandwidth]` (test/cpp/exchange/test_exchange_nixl.cpp): every rank sends 96 batches of about
40 MB (3.88 GB) to every peer at once through the transport, with the runtime's ring sizes (8 MiB
frames, 16 send frames, 8 receive frames per peer), and reports what it received per second.
Launched with `GPUS=<list> SIRIUS_EXCHANGE_HOST_STAGING=<auto|always|never>
TEST="[nixl_bandwidth]" scripts/exchange_nixl_ranks.sh <ranks>`.

| GPUs | Path | Received per rank |
|---|---|---:|
| 0,1 (same PCIe switch) | NIXL, UCX `cuda_ipc` | 26.8 GB/s |
| 0,1 | host-staged (forced with `always`) | 27.4 GB/s |
| 0,2 (no peer access) | NIXL (`never`): UCX emulates the put over TCP | 0.29 GB/s |
| 0,2 | host-staged (`auto`) | 25.7 GB/s |
| 0,1,2,3 | NIXL (`never`) | 0.44 GB/s |
| 0,1,2,3 | host-staged where no peer access (`auto`) | 22.0 GB/s |

These numbers come from the first run of the test, whose closing check counted the end notices of
all streams together: a fast rank's barrier end could pass for a slow rank's data end, so several
ranks reported FAILED (one stopped at 287 of 288 batches) and some did not print a figure. The
transport delivered every batch; the test now counts ends per stream. It has not been re-run since
that fix, so treat the table as one measurement per configuration.
