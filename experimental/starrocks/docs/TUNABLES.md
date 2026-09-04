# CN tunables

Environment variables the Sirius StarRocks CN reads. Most operators only need a
worked config (`configs/gb200-4gpu/engine-a.env`, or the launcher in
`bench/rtxpro6000-2gpu/`) and should not set these by hand.

## How they work

Transport and dispatch knobs live in one registry, [`src/tunable.rs`](../src/tunable.rs).
They are resolved once at bring-up: a typo or out-of-range value **fails CN
startup** (it is never clamped or silently ignored). The CN then logs the
resolved set — that line is what the process actually got, not what the
launcher echoed.

Unset means the compiled default. Empty string is treated as unset.

Everything else below is read outside that registry and follows its own rules,
except where a row says otherwise (`SIRIUS_CN_FRAGMENT_FUSION` is a registry
knob listed under "Dispatch", next to the dispatch switch it pairs with).

## Transport (validated registry)

| Knob | Role |
|---|---|
| `SIRIUS_CN_RPC_TIMEOUT_SECS` | How long a CN waits for a peer RPC (lease, metadata). Raise this before treating a large-SF timeout as a query bug — a busy peer can sit behind this bound. |
| `SIRIUS_CN_NIXL_XFER_TIMEOUT_SECS` | How long one nixl WRITE may take. Distinguishes a stuck fabric from a busy peer. |
| `SIRIUS_CN_NIXL_CANARY_BYTES` / `_FLOOR_GBPS` | First-contact bandwidth probe. A slow link is refused so a silent staged-copy fallback cannot look like a healthy transfer. `0` on the floor disables the check. |
| `SIRIUS_CN_NIXL_WARMUP_TIMEOUT_SECS` / `_EXPECT_PEERS` | Bring-up session warmup. The timeout is a budget, not a hard fail; expect-peers ends the loop early once that many peers are up. |

Related, but not in the registry: `SIRIUS_CN_NIXL_WARMUP` (off switch) and
`SIRIUS_CN_NIXL_WARMUP_PEERS` (explicit peer list).

## Exchange staging

`SIRIUS_EXCHANGE_STAGING_BYTES` sizes each CN's GPU staging arena. Unset means
**no arena**: the CN boots and serves local work, then every remote exchange
fails. There is no engine default — launchers pick a size per box.

## Engine-side

| Knob | Role |
|---|---|
| `SIRIUS_CN_USE_SIRIUS_DATASOURCE` | Scan backend. Default is the uring path; `false` selects kvikio/cudf. |
| `SIRIUS_CN_CPU_AFFINITY` | Pin engine thread pools to a cpulist, or `off` to leave them free. Unset discovers the GPU's socket from sysfs. |
| `SIRIUS_CN_ENABLE_QUENT` | Quent telemetry under `<engine-dir>/telemetry`, same as `--enable-quent`. Off unless set: the derived config always writes `enable_quent`, so a wall-clock run emits nothing. Only decorates a derived config; a `--sirius-config` file decides for itself. |
| `SIRIUS_QUERY_WATCHDOG_SECS` | Kill a wedged statement so it does not poison the CN. `0` / unset is off. |

GPU and host memory carve-outs are CLI flags (`--gpu-memory-limit`,
`--host-memory-limit`), not env vars — they become the derived Sirius YAML.

## Dispatch

| Knob | Role |
|---|---|
| `SIRIUS_CN_FRAGMENT_FUSION` | Which same-node senders are spliced into their receiver's plan instead of running and parking their rows. `leaf` (default): a leaf fragment (file scans only) whose `HASH_PARTITIONED` stream sink has exactly one destination, on this CN, into a plain exchange that expects one sender and does not feed an aggregation — the shuffle shape that parks a fact table whole at 1 CN. `leaf-any`: every single-destination local leaf whatever its partition type (broadcast dimension tables too; the engine then plans them from footer estimates instead of exact parked counts). `off`: every sender runs and parks, the pre-fusion path, without a rebuild. Validated at bring-up in the registry above (any other value fails CN startup) and logged as `fusion_mode=` in the `resolved CN transport tunables` line. |
| `SIRIUS_CN_ASYNC_SENDER_DISPATCH` | `1`, `true` or `on` queues sender-only fragments on the dispatch worker so their RPC returns before the scan runs. Off by default: a queued sender's failure only reaches result instances reserved on this node. Read outside the registry. |

A fused sender has no `fragment run started` line of its own. The CN logs
`fused sender fragment into its local receiver` per absorbed sender,
`fused deferred sender plans into receiver` (with `fused=`) per receiver that
absorbed some, and `fragment fusion skipped` (with `reason=`) per sender that
was offered and declined. Fusion is decided when the sender arrives, on the
inline, batch and queued (`SIRIUS_CN_ASYNC_SENDER_DISPATCH`) paths alike; a
fused leaf never reaches the dispatch queue.

## Debug

`SIRIUS_CN_DUMP_FRAGMENTS` writes received fragments and translated plans.
`SIRIUS_CN_TRANSLATE_ONLY` stops after translation.
