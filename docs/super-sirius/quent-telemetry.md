# Quent Telemetry & Tracing

> **Experimental.** Quent based instrumented telemetry is under active development and the emitted schema may change.

Sirius instruments query execution with [Quent](https://github.com/rapidsai/quent),
a modular instrumentation based telemetry toolkit to better understand runtime
behaviours of complex applications. When a query runs, Sirius emits structured
traces describing the engine, the plan (operators, ports, edges), executor
/task-manager threads, task queues, and per-query activity. These traces are written
as newline-delimited JSON (ndjson) files by default that Quent's analyzer server then ingests
and renders as an interactive timeline in your browser.

## 1. Enable the exporter

Telemetry is controlled entirely by the Sirius YAML config (see the
[Configuration reference](configuration.md#telemetry) for where config files are resolved). Enable
the Quent exporter and choose an output directory:

```yaml
sirius:
  telemetry:
    enable_quent: true
    output_directory: telemetry_data
    engine_name: siriusDB
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enable_quent` | bool | `true` | Emit Quent telemetry using the configured exporter. When `false`, telemetry uses the no-op exporter and nothing is written. |
| `exporter` | string | `ndjson` | Quent filesystem exporter: `ndjson`, `msgpack`, or `postcard`. |
| `output_directory` | non-empty string | `telemetry_data` | Directory for Quent telemetry files. |
| `engine_name` | non-empty string | `siriusDB` | Engine name reported in engine-level telemetry. |

Load the config through the normal resolution path — usually by setting
`SIRIUS_CONFIG_FILE=/path/to/sirius.yaml` before loading the extension. Any Sirius query run with
`enable_quent: true` then writes ndjson files into `output_directory` by default. Set
`exporter: postcard` for compact benchmark or CI telemetry.

## 2. Label your queries (optional)

Per-query labels are configured separately from the YAML config and make individual queries easy to find while analyzing multiple queries. A label can be set with the `sirius_set_query_label` SQL function or inline with
the `query_label` named parameter on `gpu_execution(...)`:

```sql
-- Applies to the next Sirius query, including transparent plain-SQL execution.
CALL sirius_set_query_label('tpch_q1_iter1');
SELECT * FROM lineitem WHERE l_orderkey < 100;

-- Inline label for an explicit gpu_execution call.
CALL gpu_execution(
  'SELECT * FROM lineitem WHERE l_orderkey < 100',
  query_label = 'tpch_q1_iter1'
);
```

`sirius_set_query_label` is consumed once by the very next Sirius query. For an explicit
`gpu_execution(...)` call, an inline `query_label` parameter takes precedence over a pending label set
with `sirius_set_query_label`. Unlabeled queries are reported as `unnamed_query`.

## 3. Generate telemetry

Run any query with the exporter enabled and telemetry files appear under `output_directory`.

### TPC-H helper

For TPC-H Parquet runs, `run_tpch_parquet_and_generate_telemetry.sh` runs the queries, labels each
`(query, iteration)` pair with `sirius_set_query_label` before executing it, and writes the Quent
files to `sirius.telemetry.output_directory`:

```bash
pixi run -- ./test/tpch_performance/run_tpch_parquet_and_generate_telemetry.sh \
  --iterations 1 \
  --parquet-dir /data/tpch/sf100/p16/zstd-8/ \
  100
```

The trailing `100` is the TPC-H scale factor. If no query numbers are provided, all 22 queries are
run; append query numbers to limit the run, e.g. `100 1 6 9`.

The script uses `test/tpch_performance/tpch_telemetry_sirius.yaml` by default, which only enables
telemetry. Pass `--config <path>` when the workload also needs custom memory, executor,
scan-cache, or operator settings:

```bash
pixi run -- ./test/tpch_performance/run_tpch_parquet_and_generate_telemetry.sh \
  --config ~/.sirius/sirius.yaml \
  --iterations 1 \
  --parquet-dir /data/tpch/sf100/p16/zstd-8/ \
  100 1 6 9
```

See [TPC-H Performance Testing](../../test/tpch_performance/run.md) for the full benchmarking
workflow.

## 4. Visualize

Start the Quent analyzer server over the telemetry directory. The `quent` Pixi task runs the
telemetry server with the UI enabled and defaults to `./telemetry_data` as the telemetry data directory:

```bash
pixi run quent                          # serves Quent UI, reading data from /telemetry_data
pixi run quent /path/to/telemetry_data  # if you used a different output_directory
```

Then open Quent UI at `http://localhost:8080` and select the captured Sirius engine/query to explore its
timeline.

## Engine probes: scan splits, stream hops, staging arena

Beyond the plan/task/batch model above, three engine paths report through record types that already
exist; no entity type was added. Like the rest of the schema these are experimental: the attribute
names and carriers below may change. Every probe is a no-op when the telemetry context is null
(`enable_quent: false`, or pipelines built without an engine), so an unprobed run emits nothing.

Files are `<output_directory>/<session-uuid>/<record type>/*.ndjson`; each line is
`{"id":<entity uuid>,"timestamp":<unix ns>,"data":{...}}`.

### `scan_split_read`: one storage read per scan split

| | |
|---|---|
| Record | `operator/` `Statistics` on the scan's pipeline. The line's `id` is the Operator uuid (one Operator per Sirius pipeline), the same `id` as that pipeline's `Declaration`. |
| When | `sirius_gpu_scan_operator::execute`, once a storage-backed split (one carrying scan metadata) has been materialized and its batch built. Resident and cached splits read nothing and are not reported. |
| Fields | `custom_attributes`, serialized as `[{"key":..,"value":{"String":..}}, {"key":..,"value":{"I64":..}}]`: `event` = `scan_split_read`; `sources` = `path[rg,rg,..];path[..]`; `executor_thread` = uuid of the ExecutorThread the split ran on (empty when none), which joins the read to the Task `Computing(GPU_SCAN)` span active on that thread; `device_id`; `operator_id`; `source_files`; `compressed_bytes` (Parquet column-chunk bytes the read fetches; `0` for DuckDB-native files, which do not account); `estimated_output_bytes`; `rows`; `output_bytes`; `materialize_ns` (I/O + decode, i.e. `materialize_table`); `finish_ns` (filter/project/normalize until the batch exists). |
| Timing | `timestamp` is the emission; the read started at `timestamp - materialize_ns - finish_ns`. |

Verbatim from a standalone SF10 run (row-group list shortened):

```json
{"id":"01a06616-0e76-7a40-b09a-1726f61a23e3","timestamp":1788419116754362247,"data":{"Statistics":{"custom_attributes":[{"key":"event","value":{"String":"scan_split_read"}},{"key":"sources","value":{"String":"/scratch/sirius/datasets/tpch_sf10/lineitem/part.0.parquet[34,35,...,55]"}},{"key":"executor_thread","value":{"String":"01a06616-0e5d-7453-a8dd-12c0720aa4a1"}},{"key":"device_id","value":{"I64":0}},{"key":"operator_id","value":{"I64":0}},{"key":"source_files","value":{"I64":1}},{"key":"compressed_bytes","value":{"I64":158567046}},{"key":"estimated_output_bytes","value":{"I64":1007108460}},{"key":"rows","value":{"I64":12399670}},{"key":"output_bytes","value":{"I64":530086244}},{"key":"materialize_ns","value":{"I64":79485183}},{"key":"finish_ns","value":{"I64":75680}}]}}}
```

### Stream hops: `stream_relayed`, `stream_pushed`

| | |
|---|---|
| Record | `batch_placement/` FSM. `BatchRegistered{instance_name:"batch-<id>", batch_id, pipeline_uuid: nil, port_uuid: nil, origin, tier:{resource_id: MemoryTier GPU-<n>, capacity:{capacity_bytes: batch bytes}}}` then `BatchQueued`. No consumer pipeline exists yet (the receiver's pipelines are built after the hop), so the first task that claims the batch adopts this placement (`BatchPackaged{task_uuid}`, `BatchProcessing`, `BatchConsumed{reason:"processed"}`) instead of lazily registering a `reschedule_intermediate` one; the hop's timestamp is therefore where the consumer's queued span starts. |
| When | `origin:"stream_relayed"`: `Fragment::relay_from`, as each parked batch leaves the sender's output stream for a same-process receiver. `origin:"stream_pushed"`: `Fragment::push_packed`, once a wire batch has been unpacked into pool memory. |
| Also | The unpacked wire batch is a `data_batch/` entity of its own: `Constructed{instance_name:"batch", data_batch_id, producer_pipeline_uuid: nil}` then `Stationary` on the GPU Memory. The producer is nil because no local pipeline made it. |
| Not carried | Rows: BatchPlacement has no rows attribute. |

Reading a parked batch: the sender's DataBatch stays `Stationary` (from the sink) until the hop. For a
local receiver the hop is the `stream_relayed` placement; for a remote one it is the DataBatch's
`Destructed`, right after the staging lease's `InTransit` below.

### Staging arena: leases and packed transfers

| | |
|---|---|
| Resources | `memory/`: one Memory `instance_name:"exchange-staging-arena"` under the engine, `operating{capacity_bytes: arena capacity}`. `channel/`: `"<tier>-<dev>->exchange-staging-arena"` and `"exchange-staging-arena-><tier>-<dev>"` for every memory space the manager knows (`source_id` / `target_id` are the two Memory uuids, capacity `u64::MAX`, the `memory_context` convention). Declared once per FFI Context at bring-up when `SIRIUS_EXCHANGE_STAGING_BYTES` configures an arena and a telemetry context exists; `Finalizing` + `Exit` when that Context is torn down. |
| Record | `data_batch/`: one DataBatch per lease, `Constructed{instance_name:"staging_lease", data_batch_id (from the process-wide batch counter), producer_pipeline_uuid: nil}`. |
| When | `exchange_staging_arena::lease`: `Stationary{memory:{resource_id: arena, capacity:{capacity_bytes: aligned lease length}}}`. `export_packed` (GPU pool to lease): `InTransit{source_memory: gpu-N, dest_memory: arena, channel: gpu-N->exchange-staging-arena}`, all three `capacity_bytes` = packed bytes, back to `Stationary` after the stream sync. `push_packed` (lease to GPU pool): `InTransit` the other way, `capacity_bytes` = payload length. `release`: `Destructed` + `Exit`; a lease released while in transit is settled to `Stationary` first. Metadata-only frames (length 0) hold no lease and emit nothing. |
| Reading | The sum of live `Stationary` usages on the arena Memory is the arena's occupancy; the transfer size and direction ride the `InTransit` channel usage. |

## Quent on the StarRocks CN

### Enabling it per CN

Telemetry is **off** on the CN unless asked. The derived config the CN writes
(`<engine-dir>/derived-sirius-config.yaml`, produced whenever a memory carve-out flag such as
`--gpu-memory-limit` is set) always carries the three telemetry keys, so the engine's built-in
`enable_quent: true` default never applies to a wall-clock run:

```yaml
  telemetry:
    enable_quent: false
    output_directory: ".cn1/telemetry"
    engine_name: "127.0.0.1:8060"
```

| Switch | Effect |
|--------|--------|
| `--enable-quent` | `enable_quent: true` in the derived config. Conflicts with `--sirius-config`: a supplied config file decides telemetry itself. |
| `SIRIUS_CN_ENABLE_QUENT=1` | Environment equivalent; unset, empty, `false`, `0`, `no`, `off` (any case) leave it off. |
| (either, without a memory carve-out flag) | No derived config to decorate: the CN warns and the engine config, or the engine defaults, decide. |

`output_directory` is `<engine-dir>/telemetry`, so each CN writes its own tree,
`<engine-dir>/telemetry/<session-uuid>/<record type>/*.ndjson`, one session per CN process, flushed
when the CN exits (the default engine dir is `sirius-cn-<brpc_port>`; the bench launchers use
`.cn0`, `.cn1`, ...). `scripts/clean-telemetry.sh` wipes those trees before a measured run;
`pixi run quent <engine-dir>/telemetry` reads one CN's sessions.

### Query labels

The CN drives the engine through `sirius::ffi::Fragment`; before `build()` it calls
`set_query_label(query_label, session_label)` with the StarRocks ids of the dispatched fragment,
which fill the fields `quent::query::Init` reads (`sirius_interface::query_label` / `session_label`)
and the engine log's execution-window label.

| Quent field | CN value |
|-------------|----------|
| Engine `instance_name` (`sirius.telemetry.engine_name`) | `<advertise_host>:<brpc_port>`, the CN's exchange identity, also its nixl agent name and the `cn` / `dest` of the log lines below |
| Query `instance_name` | `<StarRocks query id>:<fragment instance id>` |
| QueryGroup `instance_name` | `<engine_name>-<StarRocks query id>` (one group per StarRocks query per CN, declared on first use) |

A dispatch without ids is not labeled: the fragment reports as `sirius_ffi` in the engine's default
group `<engine_name>-session-<pid>`. That default group is also where every CN session recorded
before this labeling put its queries, as `sirius_ffi` (result fragments) or
`sirius_streaming_fragment` (intermediate ones; still the name a `streaming_fragment` built without
labels outside the FFI reports).

### Log lines that stitch to the labels

All `info` under the CN's default filter (`sirius_starrocks_cn=info`, or `RUST_LOG`):

| Line | Fields | Where |
|------|--------|-------|
| `fragment run started` / `fragment run finished` (`fragment run failed`) | `query_id`, `fragment_instance_id` (`-` when the dispatch carried none), `cn` (= engine_name), `role` (`result` / `sender`), `inputs`, `outputs`; finished/failed add `elapsed_ms` (and `error`) | `compute_node_service.rs` `run_labeled`, around the executor call |
| engine-thread lines (`relayed native batches ...`, `received remote batches` with `bytes`, `declared input stream cardinality`, the lease-sweep warnings) | wrapped in the span `fragment{query_id, fragment_instance_id}`; the CN subscriber emits span close events, so the span's close line carries the engine-side `time.busy` / `time.idle` of the run | `engine.rs` `run_fragment` |
| `transmitted batches via nixl` | sender's `query_id`, `fragment_instance_id`, `receiver_fragment_instance_id`, `stream_id`, `sender_id`, `dest` (peer `host:port`), `batches`, `bytes`, `elapsed_ms` (whole drain incl. `export_packed`), `lease_ms` (peer lease waits), `write_ms` (nixl WRITEs), `write_gbps` (`bytes / write_ms`, `0.0` when nothing was written) | `nixl_transport.rs` `send_fragment`, one per (sender fragment, destination) |
| `received remote exchange frame` (`debug`) / `remote exchange stream ended` | frame: `receiver_fragment_instance_id`, `stream_id`, `sender_id`, `seq`, `eos`, `batch_bytes`, `rows`; stream end: `receiver_fragment_instance_id`, `stream_id`, `sender_id`, `frames` (= the eos frame's `seq`, the number of batch frames before it) | `compute_node_service.rs` `handle_transmit_packed` |

The nixl WRITE itself is not a Quent event: the CN reaches Quent only through the engine's FFI, so the
transfer's bytes, peer, and duration live in the `transmitted batches via nixl` line. The wire carries
no query id, so the receiver joins on `receiver_fragment_instance_id`. The receiver's side of the
transfer, lease then `push_packed` then release, is the `staging_lease` DataBatch above.

## Example Screenshots

**Default view** — the query plan on the left and the per-resource execution timeline
(executor threads, task-manager loops, task queues) on the right.

Resources are grouped into a collapsible tree by GPU device: each `gpu-N` group (declared once per
GPU at engine startup) contains per-thread-type buckets (`executor_thread`,
`task_manager_loop_thread`) plus that executor's task queue, and a `shared` group under the engine
holds threads with no single GPU (e.g. the task-scheduler thread). The tree shape is entirely
data-driven via each resource's `parent_group_id`; to inspect it offline run:

```bash
pixi run bash -c "cd rust && cargo run -p sirius-telemetry-analyzer --example print_resource_tree -- <output_dir>/<session_uuid>"
```

![Quent standard view](quent-screenshots/standard.png)

**Operator timeline** — selecting an operator or pipeline highlights it in both the plan and the
timeline, so you can see exactly when and where it ran across the resources.

![Quent operator timeline](quent-screenshots/operator-timeline.png)

**Operator stats** — the Operators tab tabulates per-operator/per-pipeline statistics (e.g.
duration), grouped and sortable, to, for example, quickly find the most expensive operators in the query.

![Quent operator stats view](quent-screenshots/operator-stats-view.png)
