# CN tunables

Environment variables in the Sirius StarRocks CN's validated transport
registry. Most operators should not need to set these by hand.

## How they work

Transport and dispatch knobs live in one registry, [`src/tunable.rs`](../src/tunable.rs).
They are resolved once at bring-up: a typo or out-of-range value **fails CN
startup** (it is never clamped or silently ignored). The CN then logs the
resolved set. That line is what the process actually got, not what the
launcher echoed.

Unset means the compiled default. Empty string is treated as unset.

Everything else below is read outside that registry and follows its own rules,
except where a row says otherwise (`SIRIUS_CN_FRAGMENT_FUSION` is a registry
knob listed under "Dispatch").

## Transport (validated registry)

| Knob | Role |
|---|---|
| `SIRIUS_CN_RPC_TIMEOUT_SECS` | How long a CN waits for a peer RPC (lease, metadata). Raise this before treating a large-SF timeout as a query bug, since a busy peer can sit behind this bound. |
| `SIRIUS_CN_NIXL_XFER_TIMEOUT_SECS` | How long one nixl WRITE may take. Distinguishes a stuck fabric from a busy peer. |
| `SIRIUS_CN_NIXL_CANARY_BYTES` / `_FLOOR_GBPS` | First-contact bandwidth probe. A slow link is refused so a silent staged-copy fallback cannot look like a healthy transfer. `0` on the floor disables the check. |
| `SIRIUS_CN_NIXL_WARMUP_TIMEOUT_SECS` / `_EXPECT_PEERS` | Bring-up session warmup. The timeout is a budget, not a hard fail; expect-peers ends the loop early once that many peers are up. |
| `SIRIUS_CN_NIXL_WARMUP` | Warmup kill switch, `on` by default. `off` returns to lazy sessions: the first cross-node query after bring-up pays first contact, and on a cold cluster that is the first-contact deadlock the warmup exists to prevent. |
| `SIRIUS_CN_NIXL_WARMUP_PEERS` | Explicit `host:port,host:port` warmup peer list, skipping FE discovery. A malformed entry fails startup instead of leaving that peer cold. |
| `SIRIUS_CN_EXCHANGE_TRANSPORT` | How a sender's parked output reaches a REMOTE receiver. `nixl` (default): `export_packed` into a staging lease, nixl WRITE into the peer's lease, `transmit_packed` with the pack metadata. `arrow`: `export_arrow` to host Arrow, each batch sliced into chunks of at most 64 MiB and sent as one Arrow IPC stream in the `transmit_packed` attachment (`arrow_ipc=true`, no lease, no nixl), decoded on the receiver and fed through `push_arrow`. Same-CN exchanges are untouched either way. Any other value fails CN startup; logged as `exchange_transport=` in the `resolved CN transport tunables` line. An Arrow drain runs on an `arrow-drain` thread after the sender's `exec_plan_fragment` has replied (the peer is dialed inside the RPC, so an unreachable destination still fails it; a drain that fails later fails the query on this CN and cancels the receiver at the peer with the cause). The sender logs `transmitted batches via arrow` per stream (`query_id`, `fragment_instance_id`, `stream_id`, `sender_id`, `dest`, `batches`, `bytes`, `elapsed_ms`, `export_ms` = time inside `export_arrow_next`, `encode_ms` and `send_ms` = summed over the workers, `workers`); the receiver logs `received remote batches via arrow` (`stream_id`, `sender_id`, `batches`, `bytes` = the IPC payload total the sender's line counts, `host_bytes` = the decoded Arrow footprint, `push_ms` = the engine thread's `push_arrow` time). Host RAM: a receiver keeps its ENTIRE remote input as decoded Arrow record batches in host memory until it is dispatched, with no bound but the host (the nixl tier is bounded by the arena); size the receiving CN's host memory for the largest exchange input of a query (tens of GB per query at SF1000, read the `bytes` totals). Limit: a parked batch holding more than 2 GiB of characters in one string column exports as `large_utf8`, which the receiver's `push_arrow` refuses by name; the nixl tier carries such a batch. |
| `SIRIUS_CN_ARROW_SEND_WORKERS` | Encode-and-send workers per remote Arrow drain, `4` by default, `1..=64`. The drain thread exports parked batches into a bounded queue; each worker IPC-encodes chunks and sends them over its own connection, so that many frames are in flight per destination instead of one round trip at a time (`1` is the sequential shape of the first SF1000 run, 0.29 GB/s per stream). Frames may reach the receiver out of `seq` order; it holds the early ones and appends them in order. Host memory per drain stays within about three 64 MiB chunks per worker. Loopback on this box, 8 frames of 64 MiB: 0.35 GB/s with 1 worker, 1.3 GB/s with 4, 1.4 with 8 (the receiver's single I/O thread is then the ceiling; see the next row). Logged as `arrow_send_workers=` in the tunables line and `workers=` on each `transmitted batches via arrow` line. |
| `SIRIUS_CN_BRPC_IO_THREADS` | Threads of the brpc server's tokio runtime, `1` by default (the current-thread runtime the CN has always used: every connection's frames are read and decoded on one thread; handlers run on the blocking pool). Above `1` the connections spread over a multi-thread runtime, which is what lets the Arrow receiver take several senders' 64 MiB frames at once: loopback 1.3 -> 2.0 GB/s at 4 workers and 1.4 -> 2.7 GB/s at 8 workers with `4` threads. Left at `1` by default because it changes the threading of every RPC the CN serves, FE traffic included. |

## Results

| Knob | Role |
|---|---|
| `SIRIUS_CN_RESULT_PATH` | How a RESULT_SINK fragment's rows come back from the engine. `duckdb` (default): `result_to_arrow`, the engine's result collector materializes a DuckDB result and DuckDB's Arrow stream wrapper converts it (four copies per byte, 1.1-1.2 GB/s measured). `arrow`: the result fragment declares one output stream, runs as an intermediate fragment and is drained on the engine thread with `export_arrow_next` (`cudf::to_arrow_host`, one D2H copy per batch, 4.1-4.3 GB/s measured for that leg) into the same record batches the MySQL encoder reads; a DECIMAL arrives at the width cudf held it in (`DECIMAL(15,2)` as `decimal64(18,2)`), which the encoder renders to the same text. Any other value fails CN startup; logged as `result_path=` in the tunables line, and the engine logs `drained result via arrow` (`batches`, `rows`, `host_bytes`, `elapsed_ms`) per result fragment. Validated on GPU 1 by `engine::tests::arrow_result_path_renders_the_same_mysql_rows_as_the_duckdb_path`; not yet run in a cluster. |

## Exchange staging

`SIRIUS_EXCHANGE_STAGING_BYTES` sizes each CN's GPU staging arena. Unset means
**no arena**: the CN boots and serves local work, then every remote exchange
fails. There is no engine default — launchers pick a size per box.

## Dispatch

| Knob | Role |
|---|---|
| `SIRIUS_CN_FRAGMENT_FUSION` | Which same-node senders are spliced into their receiver's plan instead of running and parking their rows. `leaf` (default): a leaf fragment (file scans only) whose `HASH_PARTITIONED` stream sink has exactly one destination, on this CN, into a plain exchange that expects one sender and does not feed an aggregation — the shuffle shape that parks a fact table whole at 1 CN. `leaf-any`: every single-destination local leaf whatever its partition type (broadcast dimension tables too; the engine then plans them from footer estimates instead of exact parked counts). `off`: every sender runs and parks, the pre-fusion path, without a rebuild. Validated at bring-up in the registry above (any other value fails CN startup) and logged as `fusion_mode=` in the `resolved CN transport tunables` line. |

A fused sender has no `fragment run started` line of its own. The CN logs
`fused sender fragment into its local receiver` per absorbed sender,
`fused deferred sender plans into receiver` (with `fused=`) per receiver that
absorbed some, and `fragment fusion skipped` (with `reason=`) per sender that
was offered and declined. Fusion is decided when the sender arrives, on the
inline and batch paths alike.
