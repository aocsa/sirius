# Arrow-based in-process I/O for StarRocks: design, plan and NIXL comparison

Branch `demo/arrow-inprocess-io`, base `281b13bc`. For Morningman (Apache Doris, author of the `push_arrow` proposal
on sirius-db/sirius#1590) and the Sirius/StarRocks team. File:line anchors refer to `d39f72a0`, the last code commit.
Nothing under `experimental/starrocks` changed since `281b13bc`, so its anchors are the base's.

## 0. Summary

This branch demonstrates Sirius embedded in-process as a GPU compute runtime behind a Substrait-plus-Arrow contract.
The host hands Sirius a Substrait plan, feeds host-memory Arrow record batches into `sirius_stream_<id>` through the
new `Fragment::push_arrow` FFI, and reads the result back as Arrow. `push_arrow` is the host-memory twin of
`push_packed`, the device-memory hop the NIXL tier uses between two compute nodes (CNs) today.

Deliverables: (M1, delivered) the `push_arrow` FFI plus a helper that imports Arrow through cudf and checks it against
the declared stream schema, with Catch2 tests (13 cases, 267 assertions on GPU 1); (M2, delivered) Rust bindings, an
Arrow hop test that returns the same rows as the `relay_from` and `push_packed` hops, and a micro-benchmark of both
PCIe legs at 128 MiB, 512 MiB and 2 GiB; (M3, delivered) the Arrow-over-brpc exchange transport in the StarRocks CN,
drained off the RPC thread and pipelined over several connections; (M4, delivered behind `SIRIUS_CN_RESULT_PATH=arrow`)
a one-copy Arrow output through `cudf::to_arrow_host`; (M5) the end-to-end comparison against NIXL at the byte totals
of 5.1: run once (below); the rerun with the M3 closing fixes is pending.

Expected outcomes: a tested path for a CPU host (a Doris BE, a StarRocks BE or CN) to feed a GPU fragment without
device pointers or pack metadata (in hand); the widened threading contract, `start()`/`join()` and the thread-safe
input handle (section 3, in hand);
numbers for what the Arrow path pays (D2H, host copies, H2D, all PCIe-bound) against what NIXL pays (one
device-to-device write at 48-56 GB/s here). The per-leg numbers are measured: 10 GB/s in, 1.1-1.2 GB/s out today. The
end-to-end arms are not run yet.

## 1. The current design

### 1.1 How a StarRocks fragment reaches Sirius today

1. Translate. The FE dispatches a plan fragment over thrift; `starrocks-plan-translator` lowers it to Substrait. Every
   `EXCHANGE_NODE` becomes a `ReadRel` of the named table `sirius_stream_<node_id>` with a `StreamInputSchema {
   node_id, stream_view, columns }` whose column `ty` is a DuckDB type name
   (`experimental/starrocks/crates/starrocks-plan-translator/src/lib.rs:38,167-183`).
2. Declare. On the `sirius-engine` thread, `run_fragment_inner` (`experimental/starrocks/src/engine.rs:391-620`)
   creates a `Fragment`, calls `declare_input_column` per column, `declare_input_sender` per local slot and per remote
   `(node, sender)` pair (`:444-454`), and `declare_input_cardinality` with the exact row count when every contributor
   is known (`:456-491`); then `declare_output(i)` per destination and `declare_output_broadcast` or
   `declare_output_hash_key` (`:496-512`).
3. Build. `Fragment::build(substrait)` (`src/sirius_ffi.cpp:551-634`) opens a setup transaction, parses the type names
   (`:418-435`; no declared sender means sender `0`, `:430`), declares the streams in `stream_bind_catalog`, creates
   the view `main.sirius_stream_<id>` over `sirius_stream_source(<id>)` per input (`:458-470`), commits, opens the
   query window and plans; every view read becomes a `STREAMING_SOURCE`.
4. Feed. A local sender's parked output moves by `relay_from` (native handles, no copy, `:636-700`). A remote sender's
   batches already sit in this CN's staging arena; each goes through `push_packed` (`:788-863`), then
   `Context::staging_release`, then `close_input` (`engine.rs:550-579`).
5. Run. `Fragment::run()` blocks until the pipelines finish and closes the lifecycle (`:932-970`).
6. Deliver. An intermediate fragment parks its output on the GPU for `relay_from` or `export_packed` plus nixl. A
   result fragment goes `result_to_arrow` (`:972-984`) to `FragmentResult { batches: Vec<RecordBatch> }`;
   `MysqlResultEncoder::encode` (`compute_node_service.rs:1204`) renders MySQL text rows.

### 1.2 Constraints

| Constraint | Where it comes from |
|---|---|
| Store-and-forward | `docs/super-sirius/streaming-fragments.md`, "Not yet ported": "Fragments therefore run store-and-forward, one at a time" and "a remote sender feeds a fragment only store-and-forward, through `push_packed()` or `push_arrow()` between `build()` and `run()`" |
| One fragment between `build()` and `run()` | `src/include/sirius_ffi.hpp:170-171`: "Exactly one fragment may sit between its own build() and run() at a time (the engine serializes queries)." |
| Full materialization | Every input batch is pushed before `run()`. `engine.rs:12-13`: "Each fragment result is fully materialized, and the single process-global context serializes fragment execution" |
| No backpressure | `docs/super-sirius/streaming-sessions.md`, "No backpressure": "the streaming layer deliberately carries no channel-level backpressure". Relief is the downgrade executor spilling queued batches GPU to host to disk |
| Four-copy result path | (1) D2H `clone_to<host_data_representation>` (`src/op/sirius_physical_result_collector.cpp:147-192`); (2) host table to `duckdb::DataChunk` (`:208-234`); (3) `DataChunk` to `ColumnDataCollection` to `MaterializedQueryResult` (`:236-243`, `:102-118`); (4) `ColumnDataCollection` to Arrow inside DuckDB's `ResultArrowArrayStreamWrapper`. `result_to_arrow` itself is zero-copy |
| Staging arena | One plain `cudaMalloc` region outside the RMM pool, opt-in via `SIRIUS_EXCHANGE_STAGING_BYTES`; the capacity bounds concurrently live lease bytes (`docs/super-sirius/configuration.md`, "Exchange Staging Arena"). `export_packed` leases `total + 8 MiB` per batch (`sirius_ffi.cpp:705,761`); `push_packed` copies out on arrival so the lease is released at once |
| One CN per GPU | `experimental/starrocks/src/nixl_transport.rs:14-21`: the arena is registered with nixl as CUDA device 0 of the process; bring-up refuses a `CUDA_VISIBLE_DEVICES` that names several devices |

### 1.3 The threading contract today, in the engine's own words

`src/include/sirius_ffi.hpp:118-119` (why `StagingArena` exists), `src/sirius_ffi.cpp:328-330` (the one exception) and
`src/include/sirius_ffi.hpp:260` (the `push_packed` contract):

```text
the `Context` is single-threaded by contract, so its `staging_*` methods can
only be served by the thread that owns it

every method below only touches the arena, whose lease/release serialize on its internal
std::mutex and make no CUDA calls ... these are callable from any thread.

Legal between `build()` and `run()`, exactly where `relay_from` sits.
```

`docs/super-sirius/streaming-sessions.md` states S1, admission ordering (`push()` puts the batch in the repository
before firing `on_data` and returns false once the stream is terminal), and that `wait()` "is for the wrapper's
external threads"; `engine.rs:3-4` that the `SiriusContext` is `!Send`/`!Sync` and lives on one dedicated thread;
`stream_session.hpp:44-45` that "Registration is not thread-safe. Forwarded verbs are as thread-safe as batch_stream +
the repository"; `sirius_physical_streaming_source.hpp:50` labels the producer side "session / wrapper, any thread".
No runtime owning-thread assertion exists in C++.

## 2. The new Arrow-based flow

### 2.1 `push_arrow`: signature and contract

```cpp
void push_arrow(std::uint64_t stream_id, std::uint32_t sender_id,
                std::uintptr_t array_addr, std::uintptr_t schema_addr);
```

Contract, as proposed and as landed (`src/include/sirius_ffi.hpp:270-305`): import one host-memory Arrow record batch
(Arrow C Data Interface) into input stream `stream_id` as sender `sender_id`. Buffers are copied to the GPU before
returning, so the caller may release the Arrow structs at once. It does not close the sender; the producer calls
`close_input(stream_id, sender_id)` when it is done. Same `uintptr_t` style as `result_to_arrow` and `push_packed`, so
`sirius_ffi.hpp` still needs no Arrow headers. `sender_id` stays explicit: several producers can feed one stream, and
`close_input` stays the per-sender end-of-stream, idempotent as today. The signature is the proposal's, unchanged.

The header's throw list is longer than the proposal's: before `build()`, an unknown input stream, a sender not
declared for the stream, null addresses or already-released structs, a schema mismatch, a stream that already ended.
The sender rule is new against the proposal (2.3); it is a membership check, since the batch carries no sender
identity past the call, so a push from a sender that already closed is refused only once every sender has closed. A
slice taken on the struct itself (its `offset`/`length`) is honoured (2.4).

The same call exists twice: `Fragment::push_arrow` for the thread that owns the fragment, and
`FragmentInput::push_arrow` (same four arguments, `const`) on the handle `Fragment::input_handle()` returns after
`build()`, for any other thread. `close_input` has the same pair. Section 3 has the threading contract; 2.2 the body,
shared by both.

### 2.2 What the body does, step by step

| Step | `push_packed` (`src/sirius_ffi.cpp:788-863`) | `push_arrow` (`:865-922`) |
|---|---|---|
| 1 | `built` guard, throws "build() must run before push_packed()" (`:794-796`); an absent `resolved_inputs` entry is tolerated and the session throws later (`:817`) | same guard (`:870-872`); a null `array_addr` or `schema_addr` throws (`:873-876`); `resolved_inputs[stream_id]` must exist, else "target input stream N was never declared on this fragment" (`:879-884`); `sender_id` must be in its `expected_senders`, else "from sender M, which was not declared for it" (`:886-892`) |
| 2 | `exchange_staging_arena::require`, metadata and bounds guards (`:797-807`); `cudf::unpack(metadata, base()+offset)`, a view aliasing the lease (`:809-812`) | no arena; `sirius::estimate_arrow_import_footprint` (`src/helper/arrow_host_import.cpp`) validates the structs and sizes the import from the host buffers (the table at the declared widths plus one column at its arriving width); `sirius::arrow_transfer_reservation` reserves that many bytes in the GPU space (`make_reservation_or_null`, attached to the copy stream's allocation tracker; warn-and-proceed when refused); then `sirius::import_arrow_host_table` imports one child at a time with `cudf::from_arrow_column(schema->children[i], &window, stream, mr)`, the struct window pushed into each child first: device memory is allocated and every host buffer of the window is copied (the H2D copy) |
| 3 | inline schema guard: column count, then `get_cudf_type(declared.types[i]) != column.type()` throws naming the column (`:814-841`) | the same rule in the helper, before the copy from the format string and after it on each imported column, plus the reconciliation rules of 2.4; a decimal128 column is narrowed to the declared width before the next column is imported. `push_packed` keeps its inline copy |
| 4 | GPU memory space `get_memory_space(Tier::GPU, 0)`, null throws (`:843-847`); `cudf::get_default_stream()`, a deep copy of the unpacked view into `gpu_space->get_default_allocator()`, `stream.synchronize()` (`:852-854`) | the same space, resolved once at `build()` and held by the handle; the copy, the casts and the synchronize run on `gpu_space->acquire_stream()` (a stream from the space's pool, never cudf's default stream), into `gpu_space->get_default_allocator()`; on an error after the copy started the helper synchronizes before it throws |
| 5 | `sirius::make_data_batch(std::move(table), *gpu_space, stream, batch_telemetry_info{})` (`:857-858`); `session().push(stream_id, batch)`, `false` throws "refused a packed batch; it had already ended" (`:859-862`); return, the caller releases its lease | same, with the copy stream as the batch's writer stream; same, "refused an Arrow batch; it had already ended"; return, the caller may release the Arrow structs at once (the copy is complete); the reservation is released here, the batch owning its memory as ordinary pool bytes |

Zero changes in `stream_session`, `streaming_source` and cuCascade, as the proposal predicted. The branch diff against
`281b13bc` touches `src/sirius_ffi.{hpp,cpp}`, the new helper, `CMakeLists.txt` (one source line, `:438`), the Catch2
file, the two Rust crates and two docs. The H2D copy is mandatory, for the reasons the proposal gave and the code
confirms: the HOST tier is addressed by offsets inside cuCascade-owned blocks, the host-to-GPU converter reads only
those blocks, spill's `clone` assumes it owns the memory, and with no backpressure a queued batch may be moved to
disk. A HOST-tier push (the source is tier-agnostic; `lock_or_prepare_batch` upgrades on the consuming task,
`batch_lock_utils.hpp:67-186`) would still copy into a cuCascade-owned block first, so it saves nothing. A GPU-tier
push is `push_packed`'s choice: synchronize, return, the caller frees, and Sirius never calls back into host memory.

### 2.3 Where the proposal's assumptions differ from this tree

Per item: the assumption (proposal or draft reply); what the tree has; the resolution on this branch.

1. Stream and reservation. Assumed: `push_packed` picks a GPU space, then `acquire_stream()`, then
   `make_reservation_or_null()`. Tree at the base: it uses `cudf::get_default_stream()` and the space's default
   allocator and calls neither (both exist, `cucascade/include/cucascade/memory/memory_space.hpp:102,105`); the FFI
   hops did not reserve, the result collector (`sirius_physical_result_collector.cpp:169-178`) and
   `lock_or_prepare_batch` (`batch_lock_utils.hpp:158-172`) do, warn-and-proceed. Resolution: `push_arrow` does
   both. It reserves the footprint `estimate_arrow_import_footprint` computes from the host structs, attaches the
   reservation to the copy stream's allocation tracker so the copies are charged against it rather than counted a
   second time, and releases it once the batch is in the session (the batch then owns its memory as ordinary pool
   bytes); a refused reservation is logged and the copy proceeds unreserved, the collector's degrade. The copy runs
   on `acquire_stream()`. `export_arrow` reserves `cudf::to_arrow_host`'s device scratch (decimal widening, bool
   packing) the same way. `push_packed` and `export_packed` are unchanged: they use the default stream and reserve
   nothing.
2. The Arrow C structs. Assumed: nanoarrow 0.7.0 arrives through cudf's vcpkg port. Tree: no nanoarrow package and no
   `find_package(Arrow)` (`CMakeLists.txt:90-98`); `cudf/interop.hpp:24-38` only forward-declares the structs;
   `arrow/c/abi.h` reaches the default pixi env only through `pyarrow` (`pixi.toml:59`, feature `dev-libs`) and,
   inferred from `vcpkg.json` (no arrow port) and `pixi.toml:144` (no `dev-libs`), not the `vcpkg` CI environment
   (that build was not run here). Resolution: the helper's `.cpp` includes DuckDB's layout-identical
   `duckdb/common/arrow/arrow.hpp` (`arrow_host_import.cpp:28`), a hard dependency of every flavour and the definition
   `sirius_ffi.cpp` already sees; its header forward-declares the structs (`arrow_host_import.hpp:31-36`); the test
   vendors `ArrowDeviceArray` for `cudf::to_arrow_host` (`test_sirius_ffi_fragment.cpp:60-74`). No new dependency.
3. The import call. Assumed: `cudf::from_arrow_host` with a hand-built `ArrowDeviceArray{ARROW_DEVICE_CPU}`. Tree:
   `libcudf.so` exports both that and `from_arrow(ArrowSchema const*, ArrowArray const*, stream, mr)` (`nm -D`); both
   copy host buffers to the device, neither releases the input. Resolution: `from_arrow`, the draft reply's pick;
   production code never builds an `ArrowDeviceArray`.
4. String offsets. Assumed: "string offsets to INT64" (proposal), "widened to INT64 where the reader needs it" (draft
   reply). Tree: GPU-resident offsets are INT32; `src/op/partition/crc32_partition_hash.cu:224` throws "INT64 (large)
   string offsets are not supported", `batch_lock_utils.hpp:118-121` says a GPU-resident source is already normalized,
   and `get_cudf_type(VARCHAR)` is `STRING` id-only (`cudf_utils.hpp:188`), so the type guard alone would pass either
   width. Resolution: `utf8` imports as `STRING` with 32-bit offsets; `large_utf8`, `large_binary`, `large_list` are
   refused by name (`arrow_host_import.cpp:68-80`); the hash-partition test runs `crc32_partition_hash` over the
   imported strings (`test_sirius_ffi_fragment.cpp:833-865`).
5. The multi-shot source. Assumed: "the multi-shot source (#836) was meant to be fed while running". Tree: no "#836"
   or "multi-shot" reference exists; `STREAMING_SOURCE` has a persistent `on_data` hook that re-nominates itself on
   every push (`streaming-sessions.md`, "Task-hint lifecycle"). Resolution: that hook is what makes a push during the
   run work. Both tests exist: the producer thread between `build()` and `run()`, and the producer thread pushing
   through `FragmentInput` while the owner blocks in `join()` (section 3).
6. `sender_id`. Assumed: the body "mirrors `push_packed`". Tree: `push_packed` has no sender parameter; the resolved
   spec carries `expected_senders` (`sirius_ffi.cpp:429-430`). Resolution: `push_arrow` validates against it
   (`:886-892`), since an undeclared sender could never close the stream; no `declare_input_sender` means sender `0`
   (`:430`); membership only (2.1), pinned by the multi-sender test (`test_sirius_ffi_fragment.cpp:718-753`).
7. Struct slices. Assumed: `cudf::from_arrow` imports the rows the Arrow structs describe. Tree: it imports each child
   by the child's own `offset`/`length` and ignores the struct's, so a batch sliced on the struct (Arrow C++
   `StructArray::Slice`) or a struct shorter than its children imported every child row (10 for a 6-row slice,
   measured through the raw bindings before the fix). Resolution: the helper pushes the window into shallow copies of
   the children before the import, recounting each child's null count (`arrow_host_import.cpp:200-256`); a window past
   a child is refused naming the column (tests `test_sirius_ffi_fragment.cpp:895-934`, `:940-974`).

### 2.4 Type reconciliation rules

Declared types are DuckDB type names parsed at `build()` and mapped to cudf by `sirius::get_cudf_type`
(`cudf_utils.hpp:161-216`). The helper delivers exactly that cudf type per column, or throws naming the column, the
declared type and both cudf type names (`arrow_host_import.cpp:150-173`). The draft reply proposes the TPC-H set first
(BIGINT, DOUBLE, DECIMAL(15,2), DATE, VARCHAR); M1 covers it plus BOOLEAN.

| Arrow (host) | Declared DuckDB type | cudf type required | Rule in the helper |
|---|---|---|---|
| int8 .. int64, uint8 .. uint64 | TINYINT .. BIGINT, UTINYINT .. UBIGINT | INT8 .. INT64, UINT8 .. UINT64 | direct |
| float32, float64 | FLOAT, DOUBLE | FLOAT32, FLOAT64 | direct |
| bool (bitmap) | BOOLEAN | BOOL8 (`:182`) | cudf expands the bitmap to one byte per value |
| date32 | DATE | TIMESTAMP_DAYS (`:183`) | direct |
| timestamp[s, ms, us, ns] without timezone | TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP, TIMESTAMP_NS | TIMESTAMP_SECONDS .. NANOSECONDS (`:184-187`) | direct; a timezone-aware timestamp is refused by name |
| utf8 | VARCHAR | STRING (`:188`) | id-only compare; 32-bit offsets per 2.3 |
| decimal128(p, s) | DECIMAL(p, s) | DECIMAL32 if p <= 9, DECIMAL64 if p <= 18, else DECIMAL128, scale negated (`:198-210`) | cast the imported decimal128 to the width `get_cudf_type` picks from the declared precision when the scales agree (`arrow_host_import.cpp:322-325`); a scale that disagrees is refused naming both scales, before the copy (`:306-309`); p <= 4 throws in `get_cudf_type` |
| dictionary, large_list, large_utf8, large_binary, timestamp with tz, decimal256 | any | none | refused by name (`:54-96`) before any buffer is touched |
| any | HUGEINT, UHUGEINT | none | refused by the declared type (`:91-95`): Arrow C has no int128 format, and `get_cudf_type` would narrow to 64 bits with a FIXME (`cudf_utils.hpp:169-179`) |
| struct, list | STRUCT, LIST | STRUCT, LIST (`:189-193`) | id-only compare (no child metadata); outside the TPC-H set, not covered by M1 |

Order of the checks (`arrow_host_import.cpp:260-338`): null pointers (`:268-270`); `release == NULL` on either struct,
so an already-released batch is refused instead of read (`:274-277`); top-level format `+s` (`:282-287`); column
count, both child counts named (`:288-295`); per column, the by-name refusals, then the type the format string implies
against `get_cudf_type(declared)` for the scalar formats (`:300-310`); the struct window (`:312-313`);
`cudf::from_arrow` (`:318`); the check on the imported table, the backstop for formats the pre-check does not know
(`:319-328`).

### 2.5 The output side

Today `result_to_arrow` hands out DuckDB's `ResultArrowArrayStreamWrapper` (`sirius_ffi.cpp:981-983`), zero-copy over
a result that already cost the four copies of 1.2, and the CN renders MySQL text from the `RecordBatch`es
(`result_encoder.rs:55`: Utf8, Boolean, Int8-64, Float32/64, Decimal128, Date32, TimestampMicrosecond, LargeUtf8,
Utf8View). The one-copy follow-up is `cudf::to_arrow_host(table_view const&, stream, mr)` (`interop.hpp:617`): one D2H
copy from each GPU result batch straight into Arrow host buffers, returned as an `ArrowDeviceArray` with `device_type`
CPU. Two facts for it: cudf writes decimal32/64 out as decimal128 at the widest precision of the source width
(`interop.hpp:606-610`), and the result fragment runs through `sirius_interface` into a `MaterializedQueryResult`
(`sirius_ffi.cpp:942-947`), so M4 adds an Arrow-producing collector or a separate verb rather than changing
`result_to_arrow`. 5.2 has the measured gap: 1.1-1.2 GB/s today against 4.1-4.3 GB/s.

### 2.6 How it maps onto StarRocks

As delivered (M3, M4; the planning text that follows each item is kept where it still explains a choice):

- The input kind. A remote sender's Arrow frames arrive over the existing `transmit_packed` RPC (`arrow_ipc = true`)
  and are staged as `StagedBatch::arrow` inside `remote_inputs` (`fragment_executor.rs`, `StagedBatch`), so
  `ExecuteRequest` and `FragmentRun` carry no third field: the engine thread's remote push loop calls `push_arrow` for
  an Arrow batch and `push_packed` for a packed one, then `close_input`, all before `run()` (`engine.rs`,
  `run_fragment_inner`). The receiver's rendezvous (`local_exchange.rs`, `push_remote_frame`) holds an Arrow frame that
  overtook its predecessor and appends it in `seq` order; a packed frame out of order, or an eos with a hole, is still
  a lost frame.
- The sender side. The engine gained `Fragment::export_arrow` (`export_arrow_next` in Rust), which pops a parked
  output batch as host Arrow through `cudf::to_arrow_host`, so an Arrow-producing sender runs as an ordinary
  intermediate fragment with `declare_output`, broadcast and hash keys intact; the planning-time worry that a sender
  would have to be a result fragment did not materialize. The drain (`arrow_exchange.rs`, `send_fragment`) runs on an
  `arrow-drain` thread once the sender's `exec_plan_fragment` has replied: it exports into a bounded queue and
  `SIRIUS_CN_ARROW_SEND_WORKERS` workers (default 4) encode and send over one connection each, the eos after every data
  frame is acknowledged. The peer is dialed inside the RPC so an unreachable destination still fails it; a drain that
  fails afterwards fails the query on the sender's CN and cancels the receiver at the peer (`cancel_plan_fragment`
  with the cause).
- Exact cardinality. A staged Arrow batch carries `num_rows`, so `remote_rows` sums it like a packed batch's `rows`
  and the stream keeps the exact branch of `declare_input_cardinality` (`engine.rs`, `run_fragment_inner`). That call
  must precede `build()`, which is one of the two reasons the receiver is still dispatched only once every sender has
  closed (section 3, "Not delivered").
- The result edge (M4). `SIRIUS_CN_RESULT_PATH=arrow` makes a RESULT_SINK fragment declare one output stream, run as an
  intermediate fragment and drain through `export_arrow_next` into the record batches the MySQL encoder reads
  (`engine.rs`, `run_fragment_inner`; `result_encoder.rs` renders the decimal32/64 widths cudf exports). Default
  `duckdb` keeps `result_to_arrow`.
- Feature gating. `experimental/starrocks/Cargo.toml:17` pins `arrow-array = "59"` without the `ffi` feature;
  `arrow_array::ffi::to_ffi` reaches the CN only through the `sirius` crate's `features = ["ffi"]`
  (`rust/crates/sirius/Cargo.toml:21`) and stays inside `sirius::Fragment::push_arrow(&mut self, stream_id, sender_id,
  &RecordBatch)` (`rust/crates/sirius/src/lib.rs:434-460`). The CN only passes `RecordBatch` values, and `engine.rs`
  already sits behind `#[cfg(feature = "sirius-engine")]` (`experimental/starrocks/src/lib.rs:50-51`), so CI's
  `--no-default-features` needs no Cargo change for `ffi`. As delivered, M3 changed Cargo for another reason:
  `arrow-ipc = "59"` (already in `Cargo.lock` through `parquet`) for the IPC framing of the Arrow transport; the CN's
  manifest names no `ffi` feature.
- A CPU-only host feeding `sirius_stream_<id>`. In-process (the Doris shape): include `sirius_ffi.hpp` (no Arrow
  headers), `make_context`, `make_fragment`, `declare_input_column` with DuckDB type names, `declare_input_sender`,
  `declare_input_cardinality` if known, `build` a plan whose read names `stream_view_name(id)`, `push_arrow` per
  batch, `close_input`, `run`, `result_to_arrow`. Across a process boundary (a StarRocks BE, or the process running
  the FE-planned scan): Arrow IPC stream bytes in a brpc attachment, the D3 transport of the multi-CN plan
  (`MULTI-CN-PLAN.md:170`, section 6), which is what the M3 transport now is: `arrow_ipc` frames of `transmit_packed`,
  decoded on the CN into `RecordBatch`es and staged for `push_arrow`. A BE-side producer would emit the same frames.

## 3. The threading contract

What the draft reply to #1590 commits to:

```text
push_arrow and close_input may be called from any thread once build() has returned, including
while run() is blocking on another thread. They touch only the stream session and immutable
post-build() state (the declared schemas). They never touch the DuckDB connection or the query
lifecycle.
Every other Fragment and Context method keeps today's single-threaded rule.
The Fragment must outlive its producers. Destroying it while a producer is inside push_arrow is
undefined, exactly as for any other object.
A push after the stream ended throws. There is no backpressure: the queue is unbounded and a
producer that outruns the query grows the GPU and host tiers.
```

What this branch delivers, and under which conditions (`src/include/sirius_ffi.hpp`, classes `FragmentInput` and
`Fragment`; `docs/super-sirius/streaming-fragments.md`, "`FragmentInput`, `start()` / `join()`"):

- `Fragment::start()` begins execution exactly as `run()` does and returns once the query is started;
  `Fragment::join()` blocks until the pipelines finish, closes the lifecycle and, on failure, poisons every output
  and throws the execution error. `run()` is `start()` followed by `join()`. The split runs through every layer:
  `sirius_engine::start()`/`join()` (the `create_query` + `start_query()` future, then the wait with the watchdog,
  `wait_for_completion()` and the drain on error), `streaming_fragment::start()`/`join()` for an intermediate
  fragment, `sirius_interface::sirius_start_query()`/`sirius_join_query()` for a result fragment. A fragment destroyed
  between `start()` and `join()` joins first.
- `Fragment::input_handle()` returns `std::shared_ptr<FragmentInput>` after `build()`, the same object every call.
  The handle holds only the fragment's stream session, the input schemas and senders resolved at `build()`, and the
  GPU memory space: nothing of the DuckDB connection, the lifecycle or the result. `FragmentInput::push_arrow` and
  `FragmentInput::close_input` are legal from any thread, concurrently, from the moment `build()` returned until
  `join()` (or `run()`) returned, including while `join()` blocks on another thread. `Fragment::push_arrow` and
  `Fragment::close_input` stay the owning thread's entry points and share the implementation.
- The copy is not a device-wide barrier: `push_arrow` copies, casts and synchronizes on a stream from the GPU memory
  space's pool (`memory_space::acquire_stream()`), never on `cudf::get_default_stream()`, and that stream is recorded
  as the batch's writer stream. `push_packed` keeps the default stream.
- Beyond the draft reply: a push after the fragment finished does not fall into the undefined case. The fragment
  detaches the handle when it joins and when it is destroyed, under the exclusive side of a `std::shared_mutex` whose
  shared side every push holds, so a push that races the fragment's teardown completes or throws ("after the
  fragment has finished"), never faults; `FragmentInput::is_open()` reports the state. The Rust `FragmentInput` is
  `Send + Sync` on that argument (`rust/crates/sirius/src/lib.rs`, next to `StagingArena`'s), the `!Send`
  `Fragment` never crosses a thread, and `Fragment::start()`/`join()`/`input_handle()` are bound.
- Unchanged: every other `Fragment` and `Context` method is single-threaded by contract; `push_packed` and
  `relay_from` are legal only between `build()` and `run()` on the owning thread; exactly one fragment sits between
  its own `build()` and `run()`/`join()`; there is no backpressure, the queue is unbounded and a producer that
  outruns the query grows the GPU tier until the downgrade executor spills; no runtime owning-thread assertion
  exists in C++.

Why it holds per the code. The push body reads the handle's copy of the resolved schemas (immutable after `build()`),
reserves and allocates in the GPU memory space, and calls `session().push`, which forwards to `batch_stream::push`
under the stream's one mutex (S1) and fires the source's `on_data` hook, a `task_creator::schedule(head)` that only
enqueues ("The live re-arm", `streaming-sessions.md`). It never touches `ctx.conn`, `lifecycle` or `result`. The
engine thread inside `join()` waits on the scheduler's future; the executors run the pipelines; the producer thread
runs the H2D copy on its own pool stream.

Tests. C++ (`test/cpp/exec/test_sirius_ffi_fragment.cpp`, `[isolated_context][sirius_ffi]`): a result fragment and
an intermediate fragment are `start()`ed, a producer `std::thread` waits until the owner is inside `join()`, pushes
two batches with pauses, closes the sender, and the rows equal a fragment fed before `run()` (the intermediate one
drained through `relay_from` after `join()`); the handle refuses after `join()` and after the fragment is destroyed;
a started fragment destroyed before `join()` is joined by its destructor and the next fragment builds; `run()` after
`start()`, `join()` before `start()` and `input_handle()` before `build()` throw. Rust
(`start_join_takes_arrow_pushes_from_another_thread_while_join_blocks`): the handle is moved into a `std::thread`
that pushes while the main thread blocks in `join()`; the rows equal the pre-materialized run; a push after `join()`
is an `Err` naming the finished fragment. The `[sirius_ffi]` suite is 18 cases, 619 assertions; the Rust suite 20
tests.

Measured. The pool-stream copy costs nothing against the default-stream copy: the hidden `[sirius_ffi_bench]` case
gives `push_arrow` on the 512 MiB batch 0.055 s / 9.81 GB/s before and 0.053 s / 10.20 GB/s after (128 MiB 7.80 to
10.08, 2 GiB 9.92 to 10.23 GB/s, one run each, same box, GPU 1). The transient device peak of the declared-width
import, measured through an rmm statistics adaptor on six decimal128 columns of 2^18 rows declared DECIMAL(15,2), is
one wide column (4 MiB) over the 12 MiB table: 16 MiB, where importing the batch wide and casting afterwards held
24 MiB of wide columns plus the first cast.

Sequencing against T5b (the proposal's second question). The proposal asks that `push_arrow` go on top of T5b (the
`push_packed` FFI layer from sirius-db/sirius#1644) or the `stream/*` stack rather than race the reshaping of
`sirius_ffi.{hpp,cpp}`. This tree already carries that layer (`export_packed`, `push_packed`, the staging arena and
`declare_input_cardinality`, section 1.1), so `push_arrow` here is additive: a new helper file, one new method declared
next to `push_packed`, one new cxx bridge entry, and no edit to `stream_session`, `streaming_source` or cuCascade. When
this lands upstream it should be rebased onto whatever T5b settles as the final `Fragment` surface; the only shared
lines are the method declaration order in the header and the schema guard, which the helper now owns for both hops.

## 4. Plan for the demo branch: M1 to M5

M1 and M2 are delivered (`e354d5d1`, `0d873ac3`, `e51943af`, `d39f72a0`). M3 to M5 are planned.

### M1: `push_arrow` FFI, import helper, Catch2 tests (delivered)

- Files:
  - `src/include/sirius_ffi.hpp:270-305`: declaration and contract, right after `push_packed`.
  - `src/sirius_ffi.cpp:865-922`: the body of 2.2.
  - `src/include/helper/arrow_host_import.hpp`, `src/helper/arrow_host_import.cpp`: `import_arrow_host_table(schema,
    array, what, names, types, stream, mr)`, the import plus its own copy of `push_packed`'s schema guard
    (`push_packed :814-841` keeps its inline loop).
  - `CMakeLists.txt:438`: the new source.
  - `test/cpp/exec/test_sirius_ffi_fragment.cpp`: tags `[isolated_context][sirius_ffi]`,
    `[sirius_ffi][arrow_host_import]`, hidden `[.][sirius_ffi_bench][isolated_context]`; in `TEST_SOURCES :703`; 2 GB
    GPU / 4 GB host `test/cpp/scan/memory.yaml`.
  - `docs/super-sirius/streaming-fragments.md:345-378`: `### push_arrow()`, tests table, "Not yet ported".
- Tests, as landed (input built with `cudf::to_arrow_host` over BIGINT, DOUBLE, BOOLEAN, VARCHAR, DECIMAL(15,2), DATE;
  compared through `result_to_arrow`): round trip (`:695-715`); several batches and two senders, including a push
  naming the already-closed sender (`:718-753`); refusals naming the column: type, count, unknown stream, undeclared
  sender, null addresses, decimal scale (`:755-827`); hash partition keyed on the pushed VARCHAR column (`:833-865`);
  slices with Arrow offsets on the children (`:870-888`) and on the struct itself, plus a window past the children
  (`:895-934`); nulls in every column, whole and struct-sliced (`:940-974`); push before `build()` and after EOS
  (`:976-994`); push from a producer `std::thread` (`:1000-1024`); the helper's by-name, released-struct and pre-copy
  type refusals with hand-built structs, no engine context (`:1029-1135`). No test applies a string `FilterRel` or
  scalar function to the pushed column; the partition kernel is the string operator covered.
- Commands: the build and Catch2 lines of section 6. Acceptance, met: `[sirius_ffi]` passes on GPU 1 (13 cases, 267
  assertions at `d39f72a0`); `pre-commit run --files` clean; no change under `src/exec/`,
  `src/op/sirius_physical_streaming_source.cpp` or `cucascade/`.
- Risks that materialized: the `arrow/c/abi.h` include (2.3, fixed in `e51943af`); struct slices imported every child
  row (2.3, fixed in `d39f72a0`); the decimal width cast and the offset width (2.3, 2.4, covered). Dependencies: none,
  this tree already has the `push_packed` layer.

### M2: Rust bindings, Arrow hop test, micro-benchmark (delivered)

- Files:
  - `rust/crates/sirius-sys/src/lib.rs:249-255`: `unsafe fn push_arrow(self: Pin<&mut Fragment>, stream_id: u64,
    sender_id: u32, array_addr: usize, schema_addr: usize) -> Result<()>`.
  - `rust/crates/sirius/src/lib.rs:434-460`: `Fragment::push_arrow(&mut self, stream_id, sender_id, &RecordBatch)`,
    exporting the batch as a struct array through `arrow_array::ffi::to_ffi`; the stack `FFI_ArrowArray` /
    `FFI_ArrowSchema` run their release callbacks after the engine copied.
- Tests, as landed: `arrow_hop_matches_relay_hop` (`:1351-1450`, modelled on `packed_hop_matches_relay_hop`
  `:1230-1343`: rows equal to the relay hop, post-EOS push errs with "already ended", two senders, undeclared sender 7
  refused); `push_arrow_carries_nulls_and_sliced_batches` (`:1482-1545`); `push_arrow_rejects_a_mismatched_schema`
  (`:1729-1819`, also the `LargeUtf8` and dictionary refusals through arrow-rs's own export). The micro-benchmark is
  the hidden Catch2 case (`test_sirius_ffi_fragment.cpp:1142-1261`): one batch each of 128 MiB, 512 MiB and 2 GiB plus
  a zero-row `run()`, printing GB/s for the H2D leg, a pageable `cudaMemcpy` reference, `cudf::to_arrow_host`,
  `run()`, the drain and their sum (5.2).
- Commands: the cargo test, fmt and clippy lines of section 6. Acceptance, met: all three hops agree (17 Rust tests
  pass on GPU 1); `cargo fmt --check` clean; `cargo clippy -p sirius -p sirius-sys --all-targets -- -D warnings` clean
  (the workspace-wide run fails in the pre-existing `instrumentation-model` crate on this toolchain, untouched by the
  branch); no manifest change in `rust/crates/sirius` (the `ffi` feature is already on).
- Risks: none open. Dependencies: M1.

### M3: CN seam, Arrow input kind, loopback A/B

**Delivered as an Arrow-over-brpc exchange transport** behind the CN knob `SIRIUS_CN_EXCHANGE_TRANSPORT=arrow`
(default `nixl`, any other value fails bring-up; `experimental/starrocks/src/tunable.rs`, `ExchangeTransport`). With it
set, a sender whose destination is REMOTE drains its parked output through the new `Fragment::export_arrow` /
`export_arrow_next` (host Arrow `RecordBatch` per parked batch, no staging lease), slices each batch into chunks of at
most 64 MiB by rows, serializes every chunk as one Arrow IPC stream into the attachment of the existing
`transmit_packed` RPC (`arrow_ipc = true`, field 11 of `PTransmitPackedParams`, offset/length 0), then sends the eos
frame and drops the parked output (`experimental/starrocks/src/arrow_exchange.rs`). The receiver decodes each
attachment into `RecordBatch`es staged lease-free (`StagedBatch::arrow`, rows from `num_rows` feeding the exact
cardinality branch) and the engine thread feeds them through `push_arrow` instead of `push_packed`
(`engine.rs`, `run_fragment_inner`). Same-CN exchanges keep today's `relay_from` / fusion behaviour; nothing changes
with the knob off. The sender logs `transmitted batches via arrow` with `stream_id`, `sender_id`, `dest`, `batches`,
`bytes`, `elapsed_ms`: the nixl line's fields plus `elapsed_ms`, which this tree's nixl line does not carry, so one
extractor reads the shared fields from both lines and `elapsed_ms` from the Arrow one. The receiver logs
`received remote batches via arrow` with `batches`, `bytes` (the IPC payload, the same total as the sender's line) and
`host_bytes` (the decoded Arrow footprint the receiving CN held in host RAM until dispatch: Arrow mode buffers a
receiver's entire remote input on the host with no bound but the host, where the nixl tier is bounded by the arena; the
M5 SF1000 arm lands tens of GB per query on the receiving CN's host). A parked batch holding more than 2 GiB of
characters in one string column exports as `large_utf8`, which `push_arrow` refuses by name; the nixl tier carries such
a batch. The same 2-CN TPC-H arm can therefore run once over NIXL and once over Arrow, which is the M5 comparison.

**Closing changes after the first M5 run** (commits `5503e422`, `2f09fdea`; `experimental/starrocks`):

- The drain left the RPC. The sender fragment and its Arrow drain ran inside `exec_plan_fragment`; the 15.4 GB drain
  of q04 took 59 s, the FE's ~60 s per-RPC deadline (`RpcTimerTask` in the FE log) cancelled the query mid-drain and
  retried it, the re-planned scan parked 62 GB on one CN, and the OOM that followed took the engine's task-creation
  error path, which is the crash the diagnosis traced (an engine defect, out of this branch's scope). Now
  `arrow_exchange::prepare` dials the peer inside the RPC (an unreachable destination still fails it and drops the
  parked output), the RPC replies, and an `arrow-drain` thread runs the drain; the receivers this sender readied are
  dispatched after its drains complete, the order the inline drain kept, so a local receiver never competes with the
  drain's exports for the engine thread. A middle fragment on the dispatch worker joins its drains there. A drain that
  fails after the reply fails the query on this CN (`fail_fragment`) and cancels the receiver at the peer with the
  cause. Tests: `a_slow_arrow_drain_does_not_hold_the_exec_plan_fragment_reply` (gated export; the RPC returns while
  nothing was exported), `an_arrow_drain_failure_after_the_reply_fails_the_query_here_and_at_the_peer`.
- The drain is pipelined. The drain thread exports (`export_arrow_next`, one engine round trip and D2H copy each) into
  a bounded queue; `SIRIUS_CN_ARROW_SEND_WORKERS` workers (default 4, validated in `tunable.rs`) encode and send
  concurrently over one connection each, `seq` assigned at export; the eos goes after every worker has returned with
  its frames acknowledged. The receiver's rendezvous holds Arrow frames that overtake each other and appends them in
  `seq` order (`local_exchange.rs`; packed frames and the eos keep the lost-frame check). The PRPC frame path copies a
  large attachment once instead of four times (the client writes the head then the attachment from the caller's
  buffer; the server reads metadata, body and attachment into their own buffers, in 1 MiB steps). Loopback bench on
  this box (`arrow_drain_loopback_bench`, 8 frames of 64 MiB, host-resident export): 0.35 GB/s with 1 worker before
  the copy fix, 0.64 after; 1.3 GB/s with 4 workers and 1.4 with 8 on the receiver's single I/O thread; 2.0 and 2.7
  GB/s with `SIRIUS_CN_BRPC_IO_THREADS=4` (default 1: a multi-thread brpc runtime changes every RPC's threading, so it
  is opt-in). A raw 32 MiB `transmit_packed` round trip went from 73 ms to 31 ms; a plain loopback TCP round trip of
  the same bytes is 7 ms, so the frame path still has room.
- Not delivered: early receiver dispatch through `start()` + `FragmentInput`. The receiver is still dispatched once
  every sender has closed, and every Arrow batch is pushed before `run()`. Two reasons, both about fix 2's semantics
  and the engine's one-thread contract: (1) the engine thread would block in `join()` for the receiver's whole run,
  and that thread also serves this CN's own `export_arrow_next` calls, so in a 2-CN shuffle where both CNs send and
  receive the same exchange each CN's drain would wait behind the other's receiver — a cross-CN stall; (2)
  `declare_input_cardinality` must precede `build()`, and a remote sender's row count is only known at its eos, so an
  early start plans blind (cardinality 1 on the stream), which is the q07 join-order regression the exact branch
  exists to prevent. Streaming into a running receiver needs an engine that serves exports while a query runs, or a
  sender that announces its row count before its first frame; neither is in this step.
- Log lines. `transmitted batches via arrow` keeps `stream_id sender_id dest batches bytes elapsed_ms` and adds
  `query_id fragment_instance_id export_ms encode_ms send_ms workers`; `received remote batches via arrow` adds
  `push_ms`; the tunables line adds `arrow_send_workers`, `brpc_io_threads`, `result_path`.

Original plan, kept for the record:

- Files:
  - `experimental/starrocks/src/fragment_executor.rs`: `FragmentRun::arrow_inputs`.
  - `experimental/starrocks/src/engine.rs`: `ExecuteRequest::arrow_inputs`, the push loop between `:579` and `:590`,
    the cardinality term.
  - A test under `#[cfg(all(test, feature = "sirius-engine"))]` holding `GPU_ENGINE_TEST_LOCK`
    (`experimental/starrocks/src/lib.rs:81`): sender result fragment over the parquet fixture, its batches fed as
    `arrow_inputs` to a receiver, rows equal to the relay path.
- Commands: the CN CI trio of section 6, then the engine tests with GPU 1. Acceptance: loopback rows equal;
  `--no-default-features` builds with no Cargo change; the `received remote batches` log line gains an Arrow twin with
  `batches` and `bytes`.
- Risks: no partition mode on the Arrow sender (2.6); the engine channel serializes fragments, so the loopback stays
  in one process. Dependencies: M2.

### M4: one-copy output through `cudf::to_arrow_host`

**Delivered behind the CN knob `SIRIUS_CN_RESULT_PATH=arrow`** (default `duckdb`, any other value fails bring-up;
commit `2d0c85bf`). No new engine verb was needed: `Fragment::export_arrow` (M3) already pops a parked batch through
`cudf::to_arrow_host`, so with the knob on a RESULT_SINK fragment declares one output stream, runs as an intermediate
fragment and is drained on the engine thread with `export_arrow_next` into the `RecordBatch`es `MysqlResultEncoder`
reads (`engine.rs`, `run_fragment_inner`); the `duckdb` path (`result_to_arrow`, four copies) is unchanged. A DECIMAL
arrives at the width cudf held it in (`DECIMAL(15,2)` as `decimal64(18,2)`, 2.5) and the encoder gained the
decimal32/64 arms, rendering the same text. The engine logs `drained result via arrow` with `batches`, `rows`,
`host_bytes`, `elapsed_ms`. Test on GPU 1: `engine::tests::arrow_result_path_renders_the_same_mysql_rows_as_the_duckdb_path`
runs a parquet fixture (BIGINT, VARCHAR, DECIMAL(15,2), DATE, BOOLEAN, DOUBLE, nulls) through both paths and compares
the MySQL text rows cell for cell, alternating the knob. Not measured end to end yet (the D2H rate of 5.2 is the
expectation: 1.1-1.2 GB/s today against 4.1-4.3 GB/s for `to_arrow_host`); whether a sorted result stays ordered
through the streaming sink the way it does through the collector is untested beyond the fixture and is the one risk
to check in the arm. The CN-level result-sink tests use the stub executor, which has no result path; the knob is read
by the engine handle (`SiriusEngine::result_path`, a per-instance override in tests).

Original plan, kept for the record:

- Files: an Arrow-producing result collector beside `sirius_physical_result_collector.cpp`, or a separate `Fragment`
  verb in `sirius_ffi.{hpp,cpp}` that walks the GPU result batches and calls `to_arrow_host` per batch into a
  caller-owned `ArrowArrayStream`; tests comparing its rows to `result_to_arrow` row for row on the M1 type set.
- Acceptance: identical rows; copies per byte counted as 1 instead of 4; the decimal128 widening of 2.5 documented and
  covered by a DECIMAL(15,2) case; the D2H rate moves from the measured 1.1-1.2 GB/s toward the measured 4.1-4.3 GB/s
  of `to_arrow_host` (5.2).
- Risks: the `sirius_interface` result path (2.5); decimal precision fidelity at the MySQL edge. Dependencies: M1;
  independent of M2 and M3.

### M5: measured comparison against NIXL (first end-to-end run, 2026-09-04 20:00 UTC)

Same CN binary (this branch at 664653c5), 2 CNs, `GPU_MEM=60GiB STAGING=32GiB`, SF1000, cold + 2 warm, six queries,
knob `SIRIUS_CN_EXCHANGE_TRANSPORT=nixl` then `=arrow` (`harness/arms-arrow-vs-nixl.sh`; arms `X-nixl`, `X-arrow`).

| query | nixl cold / warm | arrow cold / warm | exchange traffic |
|---|---|---|---|
| q01 | 6.9 s / 6.5 s | 6.7 s / 6.6 s | a few KB (partial aggregates); no difference |
| q06 | 5.5 / 5.5 | 5.5 / 5.5 | same |
| q03 | 8.4 / 7.5 | **92.5 / 84.1** | 4 streams of 19.7 GB each; each took 68 s over Arrow: **0.29 GB/s per stream**, 0.28 GB/s aggregate over the arm's 22 transmits (149 GB in 525 s) |
| q04 | 10.3 / 5.9 | fail after 64 s: `std::bad_alloc: out_of_memory` in a fragment | |
| q07 | 9.7 / 9.7 | fail: one CN **segfaulted** (`cluster8.sh` line 96, core to apport) right after translating the lineitem fragment; the launcher tore the cluster down | |
| q22 | 2.6 / 2.5 | not run (cluster gone) | |

Oracle: nixl q01 q03 q07 VALUES-DIFFER (the known decimal class), q04 q06 q22 MATCH; arrow q01/q03 identical verdicts.

Reading. The device-to-device WRITE moves the same bytes at 48-56 GB/s (5.1); the host path measured 0.28-0.29 GB/s
end to end, about 180x slower, far below the sum of its per-leg costs in 5.2 (D2H 4 GB/s, H2D 10 GB/s): the sender
exports, IPC-encodes and ships 64 MiB frames sequentially on the drain thread, the receiver decodes and pushes
sequentially on the engine thread, and every frame is one brpc round trip. Two defects surfaced: q04 ran out of pool
memory (at this run, arriving Arrow batches were copied into the 60 GiB pool with no reservation accounting; 2.3
item 1 has the accounting that now exists), and q07 crashed a CN (not yet reproduced; the last CN log lines are the
lineitem fragment's translation, then the segfault). Conclusion for the design question: for CN-to-CN exchange on one host NIXL stays;
the Arrow path is the host-input contract for a CPU-side producer (its intended role), and needs pipelining
(overlap export, encode, send, decode, push) and the crash fix before it can be measured against NIXL on equal
terms; the reservation accounting is in (2.3). Evidence: `starrocks-tools/evidence/rtxpro6000-fix4/X-nixl`, `X-arrow`, `arrow-vs-nixl.md`.

What the diagnosis of that run and the closing changes established afterwards (M3, "Closing changes"):

- The q04 failure and the q07 crash were one chain. The 59 s drain inside `exec_plan_fragment` tripped the FE's ~60 s
  per-RPC deadline; the FE cancelled q04 mid-drain and retried it; the retry re-planned lineitem so one CN parked 62.1 GB
  in its 60 GiB pool; the join fragment's task creation then threw `out_of_memory` on the one engine error path that
  stops the scheduler from its own pool thread (`task_creator.cpp:589`), and the next fragment (q07's lineitem scan)
  ran against the torn-down scheduler. The same q03 drains (68 s) were cancelled and retried the same way (the r2 in
  the table is a retry). The crash did not reproduce in five gdb-wrapped cluster runs; three induced OOMs took the
  executor path and recovered. The engine fix (report, do not stop, from the creation catch) is out of this branch's
  scope; the CN-side trigger is closed: the RPC replies before the drain, and the drain is 4-7x faster on loopback.
- The arm has not been rerun with these changes yet. The numbers to expect
  from the loopback bench: a 19.7 GB q03 stream at 1.3 GB/s takes ~15 s instead of 68 s (4 workers, one I/O thread),
  ~10 s with `SIRIUS_CN_BRPC_IO_THREADS=4`; the receiver's `push_ms` (H2D at ~10 GB/s) and the sender's `export_ms`
  (D2H at ~4 GB/s, serialized on the engine thread) are now logged per stream so the next run can attribute what is
  left. Knobs for the rerun: `SIRIUS_CN_EXCHANGE_TRANSPORT=arrow SIRIUS_CN_ARROW_SEND_WORKERS=4
  SIRIUS_CN_BRPC_IO_THREADS=4`, and `SIRIUS_CN_RESULT_PATH=arrow` for the M4 leg.

## 5. Performance comparison against NIXL

### 5.1 What NIXL moves, measured here

Arm `V3d-32g` (2026-09-04 02:58; harness layout in section 6): two CNs on one host, SF1000. NIXL moves packed bytes
device to device over UCX `cuda_ipc`, lease to lease; only pack metadata and control frames (agent metadata, lease
grants, per-batch `transmit_packed`, EOS) cross on brpc, logged as `transmitted batches via nixl ... batches=54
bytes=19407986304 elapsed_ms=680 lease_ms=2 write_ms=360 write_gbps="53.9"`.

| Condition | Value (the arm's `##### ARM` header, `logs/v3d-32g.log:1`, and its engine logs) |
|---|---|
| CN build | worktree `fusion` at `9a9c016d`, the fragment-fusion tree this branch's base `281b13bc` adapts; this branch has not run as a CN |
| CNs / data | 2 (one per GPU) / `/home/ubuntu/tpch_parquet_sf1000` |
| Staging arena | `STAGING=32GiB` (`exchange staging arena: 34359738368 bytes (cudaMalloc)` in `engine-.cn{0,1}.log`); the harness default is 8 GiB |
| GPU / host memory per CN | `GPU_MEM=60GiB` (harness default 84 GiB), `HOST_MEM=160GiB` |
| Other knobs | `ASYNC=1` (`SIRIUS_CN_ASYNC_SENDER_DISPATCH=1`), `FUSION` unset (the CN default, `leaf`), watchdog 600 s |
| Runs / oracle | one cold + one warm per query; `compare.txt`: q04, q22 MATCH; q03, q07 VALUES-DIFFER, 4 cells each, max rel diff 1.8e-3 and 9.6e-4 |

| Query | Bytes over nixl | Sum elapsed_ms | Sum write_ms | WRITE GB/s (frames > 1 MB) | Relayed batches/streams | Wall cold / warm |
|---|---|---|---|---|---|---|
| q03 | 40.10 GB | 1408 | 743 | 51.3-55.6 | 121 / 7 | 9804 ms / 7254 ms |
| q04 | 30.82 GB | 1067 | 581 | 52.9-53.5 | 65 / 5 | 8544 ms / 6813 ms |
| q07 | 48.83 GB | 1717 | 912 | 48.3-53.8 | 144 / 15 | 9754 ms / 9687 ms |
| q22 | 6.19 GB | 215 | 117 | 52.2-52.7 | 9 / 7 | 2605 ms / 2488 ms |

`write_ms` is 52-53% of `elapsed_ms`; the rest is the per-batch `request_staging_lease` plus `transmit_packed` brpc
round trips (`lease_ms` about 2 ms per 54 batches).

| Reference point | GB/s | Source |
|---|---|---|
| This box: NIXL WRITE. 2x RTX PRO 6000 (97887 MiB each) behind one PCIe switch (`nvidia-smi topo -m`: `PIX`, no NVLink), PCIe Gen5 x16, 48 cores, 499 GB RAM | 48-56 (canary 50.7-52.3, 16 MiB, floor 2.0) | table above; `cluster.log` |
| Same-host `cuda_ipc`, A100 / GB200 NV18 | 85-90 / 322-399 | `nixl_transport.rs:277-287` |
| Degraded staged-copy path (pool memory under `cuda_ipc`) | about 0.4 | `nixl_transport.rs:277-287` |
| Cross-host `cudaMalloc` IPC host bounce | 0.32-0.43 | `docs/super-sirius/configuration.md`, "Exchange Staging Arena" |

### 5.2 What the Arrow path moves, measured here

Micro-benchmark, not an end-to-end arm: the hidden Catch2 case `[.][sirius_ffi_bench][isolated_context]`
(`test_sirius_ffi_fragment.cpp:1142-1261`). Conditions: the same box, GPU 1 (RTX PRO 6000 Blackwell Server Edition,
PCIe), pageable host memory, default `make_context()`, one process, no CN; batches of 4 columns (int64, double, int64,
double), 8 bytes per cell; wall clock `std::chrono::steady_clock`; GB/s = bytes / s / 1e9. The 512 MiB table
(16,777,216 rows, 536,870,912 bytes) is the min-max over eleven runs across `0d873ac3`, `e51943af` and `d39f72a0`; the
size table is three runs at `d39f72a0`.

| Leg (512 MiB) | What is timed | s | GB/s |
|---|---|---|---|
| H2D `push_arrow` | `from_arrow` copy + `synchronize` + `make_data_batch` + session push | 0.053 | 10.04-10.21 |
| H2D `cudaMemcpy` pageable (reference) | one 512 MiB memcpy of the same byte count | 0.053-0.055 | 9.79-10.18 |
| D2H `cudf::to_arrow_host` (reference, the M4 target) | GPU table to host `ArrowDeviceArray` | 0.127-0.133 | 4.03-4.23 |
| D2H `run()` of the result fragment | the collector: D2H clone, `DataChunk`, `ColumnDataCollection` | 0.324-0.335 | 1.60-1.66 |
| D2H `result_to_arrow` drain | `ColumnDataCollection` to Arrow, 1 Mi-row batches | 0.111-0.135 | 3.97-4.82 |
| D2H `run()` + drain | the whole result path today | 0.436-0.466 | 1.15-1.23 |

| Leg, GB/s | 128 MiB | 512 MiB | 2 GiB |
|---|---|---|---|
| H2D `push_arrow` | 9.97-10.07 | 10.06-10.19 | 10.07-10.23 |
| H2D `cudaMemcpy` pageable | 10.01-10.17 | 9.99-10.18 | 10.01-10.14 |
| D2H `cudf::to_arrow_host` | 4.11-4.33 | 4.07-4.23 | 4.16-4.32 |
| D2H `run()` | 1.76-1.78 | 1.62-1.66 | 1.40-1.42 |
| D2H `result_to_arrow` drain | 2.64-2.74 | 4.75-4.82 | 5.33-5.40 |
| D2H `run()` + drain | 1.06-1.08 | 1.21-1.23 | 1.11-1.13 |

Reading. `push_arrow` runs at pageable-memcpy speed at every size: the cudf import is one H2D copy per buffer and the
schema guard costs nothing measurable. The result path is 3.5-4x slower than `cudf::to_arrow_host` on the same bytes
at every size (1.1-1.2 against 4.1-4.3 GB/s). `run()` over an input that ended with no batches (plan lowering,
pipeline setup, scheduling) takes 0.001 s in all three runs, and the `run()` leg gets slower per byte as the batch
grows (1.78 to 1.41 GB/s), so the gap is the per-byte work of the four-copy collector of 1.2, which M4 closes. Every
exchanged byte crosses PCIe twice (D2H, H2D) and lives in host memory in between; NIXL crosses the switch once, device
to device, and never touches host memory for the payload.

### 5.3 Metrics to record

GB/s per hop (nixl: `write_ms` and `elapsed_ms` of the `transmitted batches via nixl` lines; Arrow: timers around
`result_to_arrow` and `push_arrow` in the same key=value style, as the bench prints them today); end-to-end query
time, cold and warm, from `runs/runs.csv`; copies per byte per hop (5.5); host memory (peak process RSS and the
engine's host tier reservation) and GPU memory (`nvidia-smi`); correctness, the `compare.py` verdict against the
DuckDB oracle (`compare.txt`).

### 5.4 Arms to run

| Arm | What runs | Status |
|---|---|---|
| A0 | NIXL baseline, 2 CNs, q03 q04 q07 q22 (`V3d-32g`) | measured, table 5.1 |
| A1 | M2 micro-benchmark in one process: `push_arrow` then `run()` + `result_to_arrow` | measured at 128 MiB to 2 GiB, table 5.2; the byte totals of 5.1 (6.19, 30.82, 40.10, 48.83 GB) not run |
| A2 | M3 loopback in one CN for the single-destination exchanges of q22 and q04, behind a CN switch that routes a local destination through the Arrow path instead of `relay_from` | after M3, optional |
| A3 | Arrow IPC over brpc between the 2 CNs (the D3 shape; frames under the 256 MiB decoder cap, `prpc.rs:13`) | not scheduled; listed so the wire cost is not forgotten |

### 5.5 Per-byte comparison, measured legs

NIXL moves each byte once, device to device, at 48-56 GB/s on this box (5.1). The Arrow path moves each byte across
PCIe twice and through host memory in between. Per leg (5.2, 512 MiB ranges), the ratios are the bounds divided
pairwise:

| Leg | Copies per byte | Rate | Against NIXL's 48-56 GB/s |
|---|---|---|---|
| Arrow in, `push_arrow` (H2D) | 1 (`from_arrow`) | 10.0-10.2 GB/s | 4.7-5.6x slower |
| Arrow out today, `run()` + `result_to_arrow` (D2H) | 4 (1.2) | 1.15-1.23 GB/s | 39-49x slower |
| Arrow out with M4, `cudf::to_arrow_host` (D2H) | 1 | 4.0-4.2 GB/s | 11-14x slower |

q03 at the measured rates (40.10 GB, table 5.1), arithmetic rather than a measurement:

| Leg | Rate used | Time |
|---|---|---|
| Arrow in through `push_arrow` | 10.1 GB/s | 4.0 s |
| Arrow out through today's result path | 1.2 GB/s | 33 s |
| Arrow out through `to_arrow_host` (M4) | 4.1 GB/s | 9.8 s |
| NIXL, sum of `write_ms` / sum of `elapsed_ms` | 54 GB/s / 28 GB/s | 0.74 s / 1.41 s |

The gap shrinks on q22 (6.19 GB) and disappears only where the data already lives on the host: a CPU scan (an internal
table on a Doris BE) pays the H2D leg instead of a GPU scan, and there the Arrow path buys the host's tables and
scheduler, not bandwidth. The arms of 5.4 still have to decide the end-to-end effect at the 5.1 byte totals (A1) and
inside a CN (A2).

## 6. Repository structure and commands

| Item | Location |
|---|---|
| Branch | `demo/arrow-inprocess-io`, base `281b13bc`; code commits `e354d5d1`, `0d873ac3`, `e51943af`, `d39f72a0` |
| FFI surface | `src/include/sirius_ffi.hpp`, `src/sirius_ffi.cpp`; helper `src/include/helper/arrow_host_import.hpp`, `src/helper/arrow_host_import.cpp` |
| Rust bindings | `rust/crates/sirius-sys/src/lib.rs` (cxx bridge), `rust/crates/sirius/src/lib.rs` (safe wrapper, GPU tests) |
| StarRocks CN | `experimental/starrocks/src/{engine.rs,fragment_executor.rs,nixl_transport.rs,compute_node_service.rs}` |
| Design docs | `docs/super-sirius/streaming-fragments.md` (`### push_arrow()`), `docs/super-sirius/streaming-sessions.md`, `docs/super-sirius/configuration.md`; multi-CN plan (D3): `/home/ubuntu/sirius-wt/base/notes/2026-08-05-multi-cn-nixl/MULTI-CN-PLAN.md` |
| Catch2 | `test/cpp/exec/test_sirius_ffi_fragment.cpp`, tags `[isolated_context][sirius_ffi]`, `[sirius_ffi][arrow_host_import]`, hidden `[.][sirius_ffi_bench][isolated_context]` |
| Demo box | worktree `/home/ubuntu/sirius-wt/arrow`; harness `/home/ubuntu/sirius-wt/harness/` (see its `README.md`); arms under `/home/ubuntu/sirius-wt/arms/<TAG>/{cluster.log,cnlog.txt,runs/runs.csv,compare.txt}`, arm headers under `/home/ubuntu/sirius-wt/logs/`; `source /home/ubuntu/sirius-wt/env.sh` sets up every shell; GPU 1 is the free GPU |

```bash
cd /home/ubuntu/sirius-wt/arrow && pixi run make                      # engine build, incremental
cd /home/ubuntu/sirius-wt/arrow && CUDA_VISIBLE_DEVICES=1 \
  pixi run build/release/extension/sirius/test/cpp/sirius_unittest "[sirius_ffi]"   # Catch2, GPU 1
cd /home/ubuntu/sirius-wt/arrow && CUDA_VISIBLE_DEVICES=1 \
  pixi run build/release/extension/sirius/test/cpp/sirius_unittest "[sirius_ffi_bench]"  # 3-size bench
cd /home/ubuntu/sirius-wt/arrow && CUDA_VISIBLE_DEVICES=1 pixi run bash -c \
  'export LD_LIBRARY_PATH=$PWD/build/release/extension/sirius:$LD_LIBRARY_PATH; \
   RUSTFLAGS="-C link-arg=-Wl,--allow-shlib-undefined" \
   cargo test --manifest-path rust/Cargo.toml -p sirius --lib -- --test-threads=1'  # Rust, GPU 1
pixi run cargo fmt --manifest-path rust/Cargo.toml --all -- --check
pixi run cargo clippy --manifest-path rust/Cargo.toml -p sirius -p sirius-sys --all-targets -- -D warnings
cd /home/ubuntu/sirius-wt/arrow/experimental/starrocks && pixi run bash -c \
  'cargo fmt --all -- --check && cargo clippy --all-targets --no-default-features -- -D warnings \
   && cargo test --workspace --no-default-features'                   # CN CI trio, when touched
cd /home/ubuntu/sirius-wt/arrow && pixi run bash -c 'pre-commit run --files <files>'
H=/home/ubuntu/sirius-wt/harness                        # A0 under the 5.1 conditions (V3d-32g ran from
GPU_MEM=60GiB STAGING=32GiB bash $H/capture-arm.sh \    # /home/ubuntu/sirius-wt/fusion; this branch's
  /home/ubuntu/sirius-wt/arrow 2 A0-nixl 600 1 q03 q04 q07 q22        # CN code is the base's)
python3 $H/cnlog_extract.py /home/ubuntu/sirius-wt/arms/A0-nixl   # bytes, elapsed_ms, write_ms per stream
python3 $H/compare.py /home/ubuntu/sirius-wt/arms/A0-nixl         # verdicts against the oracle
```
