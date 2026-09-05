**StarRocks source review and performance improvement plan**

Reviewed on 2026-09-05 against local branch `demo/q1q6-integration-plus-fixes`, commit `281b13bcb12321bac2927a8f4f996b710a463ec1`. Comparison base: `a5cfa8fdf2ce4c34e9c8ded76ca7b5950e6227d9`, the merge base with the locally available `upstream/dev`. No remote refs were refreshed.

**Recommendation**

Detailed implementation plans are now split into [18 paths with a dependency index](experimental/starrocks/docs/performance/README.md). Use those documents for code touchpoints, implementation slices, tests, benchmarks, and rollout criteria; this document remains the source-review summary.

Keep exchange lifetime and scheduling as the main performance work, but first fix the measurement gate and transport progress/recovery defects. Then introduce independently progressing ingress with bounded memory, overlap destination drains, and increase the in-flight transfer window. Evaluate the local fusion that already exists before expanding it. Remove GPU copies after ownership and reclamation are explicit.

The source supports these mechanisms and failure scenarios. It does not establish their share of query time or a predicted speedup. The only executed behavioral checks in this review were CPU-only synthetic fixtures for the benchmark comparator; no GPU or cluster measurements were made.

**Scope and corrections to the draft**

The previous draft reviewed `feat/pin-table-cn` at `d24f02c4...`, which is not the current checkout and whose commit object is unavailable locally. Its line references and benchmark results cannot be treated as evidence for this tree. There are 46 changed paths under `experimental/starrocks` against the comparison base. This review follows CN orchestration, transport, rendezvous, fusion and exchange translation, scan integration, the benchmark harness, and directly used C++ streaming/FFI/partition/spill paths. It is not an exhaustive review of every translator expression or unrelated engine change.

| Draft or supporting-document claim | Current source | Plan adjustment |
|---|---|---|
| Ready local receivers are dispatched before remote drain tickets are joined. | The sender performs blocking remote drains before returning its ready local receivers. | Treat dispatch/drain overlap as work still to implement. |
| Local fragment fusion is future work. | Default `SIRIUS_CN_FRAGMENT_FUSION=leaf` already defers eligible single-destination local hash leaves into their receiver; `leaf-any` is opt-in. | Measure eligibility, actual fusions, skipped reasons, and join-plan effects before widening coverage. |
| FE scan assignments are whole files. | The translator emits partial byte ranges; the scanner selects row groups by start-offset ownership. | Measure row-group skew and range-aware cache misses, not just file-count skew. |
| The NIXL bandwidth benchmark and SF500/SF1000 study scripts exist here. | This checkout has Q1/Q6 SQL, the cluster launcher, a bandwidth canary, and an ignored agent smoke test; the cited `nixl_bench.rs` and `benchmarks/nixl-nvlink` files are absent. | Build the missing benchmark layers explicitly; remove unverified study numbers. |
| The benchmark README and PRPC comments say lease RPCs queue behind the engine's current fragment. | Lease/release use a direct arena handle on the caller's thread, as the original draft correctly noted. Export still queues behind `Run`. | Attribute engine queue delay to export/ingestion; correct those stale supporting comments. |

Current anchors: [sender drain ordering](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/compute_node_service.rs#L1347), [fusion modes](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/tunable.rs#L287), [byte-range normalization](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/crates/starrocks-plan-translator/src/scan_paths.rs#L143), [row-group selection](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/op/scan/parquet_gpu_ingestible.cpp#L823), and [direct staging operations](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/engine.rs#L691).

**1. P1 — Make receive allocation retry-safe and reclaim unpublished leases**

*Introduced by the remote transport path; high confidence from source. This is a capacity/recovery defect, not merely an optimization.*

`request_staging_lease` receives only a length and immediately allocates arena space. There is no logical allocation ID, query/receiver owner, or generation in that request. Meanwhile, the shared PRPC client retries any transport failure on a cached connection once, including a failure reading the response after the peer already allocated. The second attempt can allocate another lease; the first offset never reaches the sender. Repeated failures reduce usable arena capacity until process teardown.

Even without a retry, a successful remote lease followed by a failed WRITE or a publication that never reaches the receiver strands the lease: the sender releases its local allocation, while the receiver learns the lease's fragment association only from `transmit_packed`. Query retirement can release recorded batches, but cannot identify an unpublished allocation. The canary uses the same allocation/publication pattern.

Evidence: [length-only wire request](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/patches/nixl-exchange-proto.patch#L22), [allocation handler](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/compute_node_service.rs#L1442), [automatic reconnect/retry](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/prpc_client.rs#L66), and [remote allocation, WRITE, publication, local-only release](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/nixl_transport.rs#L686).

Introduce an allocation token scoped to a peer/session epoch and logical frame, associate it with the query and destination at grant time, and make repeated grants return the same allocation. Track granted, transferring, published, and reclaimable states. Cancellation must cover both published and unpublished allocations. Apply an explicit retry policy to allocation, data publication, EOS, and canary release rather than assuming all methods are idempotent.

Reclamation must prove a transfer can no longer access the allocation. A timeout or expired TTL alone is insufficient evidence that a remote GPU WRITE stopped. Establish the pinned NIXL binding's abort/completion contract before reusing memory. Retain terminal sequence state through the retry horizon: the current rendezvous removes sequence tracking when a receiver becomes ready, so an EOS retry after that point is not covered by the normal duplicate-frame check. See [sequence retirement](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/local_exchange.rs#L465).

Acceptance: inject loss after allocation but before its reply, failure before WRITE, loss before publication, duplicate EOS after dispatch, and cancellation at each state. Arena live bytes must return to the pre-query level; a subsequent query must succeed without a CN restart. For unresolved transfers, quarantine capacity and fail explicitly until quiescence is proven.

**2. P1 — Remove the cold-peer handshake cycle from the query path**

*Introduced by the transport; background warmup mitigates it but does not eliminate it.*

`NixlTransport::start` launches warmup in the background and returns. CN readiness is announced after BRPC starts, without waiting for peer sessions. A query arriving before warmup succeeds, or contacting a later peer, can still enter `ensure_session` on the transport thread. That function performs a blocking metadata RPC whose receiver also needs its transport thread. Two CNs entering this path together can wait for each other until timeouts. This produces cold-query tail latency even when warm queries and the bandwidth canary look healthy.

Evidence: [background warmup startup](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/nixl_transport.rs#L321), [CN readiness](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/main.rs#L205), [blocking lazy handshake](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/nixl_transport.rs#L554), and [metadata handler's transport request](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/nixl_transport.rs#L156).

Make peer establishment an asynchronous per-peer state machine. A cold destination's drain should park while a separate control worker obtains metadata; the transport owner must remain free to answer metadata requests and progress other peers. Deduplicate simultaneous setup requests and keep agent-local installation on its owning thread. Expose peer readiness separately from process liveness. A cluster-wide readiness barrier needs a discovery phase that can operate before peers become schedulable, or it creates a new boot dependency cycle.

Acceptance: submit a bidirectional exchange immediately when CNs become alive, with delayed warmup and a late peer. No reciprocal handshake stall; cold setup and steady-state query latency are reported separately. Raising RPC timeouts is a diagnostic experiment, not the fix.

**3. P1 — Reclaim receive staging before EOS, with a real spill/progress budget**

*The run-to-completion fragment model is inherited; retaining packed remote input in staging is part of the branch integration.*

The receive handler creates `StagedBatch` descriptors and the rendezvous accumulates them. `take_ready` waits for every expected sender on every exchange to become complete. Only after dispatch and plan construction does `push_packed` copy each payload into ordinary GPU memory and release its arena lease; then the receiver runs. The phrase “copy-out-on-arrival” in C++ describes what that FFI call does, not when network arrival invokes it.

Evidence: [arrival handling](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/compute_node_service.rs#L658), [batch retention and EOS gate](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/local_exchange.rs#L393), [copy/release/run sequence](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/engine.rs#L547), and [arena exhaustion](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/exec/exchange_staging_arena.cpp#L244).

An unfused receiver's complete remote input set can therefore have to fit in staging before any of it is released. The same arena also serves local exports and canaries. Increasing in-flight transfers before changing retention can exhaust it sooner. Converting allocation failure into a blocking credit wait alone would create a deadlock: the receiver waits for EOS, while the sender cannot allocate the bytes needed to reach EOS.

Create ingress ownership that does not require building/running the receiver plan. Copy arriving payloads into a bounded, memory-managed repository and return receive credits after copy completion. Ingress must progress independently while the engine thread is inside `Run`. The current `Fragment::push_packed` requires `build()` and uses fragment/session state, so merely calling that API earlier from an RPC thread is not a valid implementation.

Spill integration is a deliverable of this step. Exchange repositories are created outside the normal repository manager; the downgrade sweep enumerates registered managers. In addition, `export_packed` explicitly refuses non-GPU-resident batches. Audit all exchange repository registration and lifetime paths, support reload before export, and retain row counts in residency-independent metadata. Do not equate allocation from the normal pool with complete spill support. Evidence: [exchange repositories](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/exec/streaming_fragment.cpp#L100), [downgrade enumeration](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/downgrade/downgrade_executor.cpp#L223), [spilled-export refusal](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/sirius_ffi.cpp#L724), and [GPU-only row counting](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/sirius_ffi.cpp#L936).

Budget transport and compute together:

`GPU allocation budget = compute/pins + exchange arena + ingress/reload working reserve + runtime overhead`

These are disjoint sub-budgets: when ingress uses the ordinary pool, carve its reserve out of that pool rather than counting the same allocation twice. Reserve host/spill capacity separately.

Within the arena, constrain total/per-peer live bytes and reserve progress capacity for receive, export, and control probes. Copy-out must have a guaranteed GPU or host/spill destination, including when ordinary compute allocations are exhausted. Account for temporary double residency during copying and for the sender packer's 8 MiB slack. Cap or split oversized logical batches: the 8 MiB pack working span is not a network-message cap.

Acceptance: transfer a dataset larger than the arena with a slow consumer and constrained compute memory. Demonstrate bounded GPU and host occupancy, actual spill/reload when required, prompt credit return, bidirectional progress, cancellation, and a later successful query. Show the largest free arena block as well as total free bytes.

**4. P1 — Overlap dispatch, packing, and transfers without blocking progress threads**

*Branch orchestration and transport policy; high confidence in serialization, unmeasured runtime impact.*

The unfused sender completes `run_labeled` before draining output. It records ready local receivers, then blocks draining each remote destination in FE order, and only afterward returns those receivers for dispatch. Inside one drain, every batch requires an engine export request, a receiver lease RPC, a WRITE polled to completion, and a publication RPC. Both the transport worker and each peer's PRPC client permit only one such operation at a time.

Evidence: [sender execution and destination loop](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/compute_node_service.rs#L1334), [dispatch after processing returns](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/compute_node_service.rs#L609), [batch drain](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/nixl_transport.rs#L673), and [single-request client](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/prpc_client.rs#L1).

There are two distinct changes:

- Return remote drain tickets promptly and dispatch ready local receivers before waiting for drains. Preserve query-scoped failure propagation and parked-output claims until all destinations finish. This is absent from the current tree and can help before full fragment concurrency.
- Replace whole-destination draining with fair per-peer queues and a bounded progress loop. Pipeline export preparation, lease acquisition, transfer, publication, and ingress. Blocking RPC waits must move off that progress loop or become asynchronous; posting several NIXL handles while retaining blocking control calls is insufficient for peer fairness.

Export is a separate serialization point: `Run` and `ExportNext` share one synchronous engine queue. A ready local receiver or another query can monopolize it for a whole fragment, preventing the transport from obtaining its next payload. Dispatch-before-join alone can expose this stall. Queue priority cannot preempt a `Run` already executing. Introduce export tickets with independently progressing, correctly ordered packing, or integrate packing into the producer pipeline. See [engine queue](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/engine.rs#L234) and [export routing](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/engine.rs#L711).

After steps 1–3 establish ownership and memory progress, test in-flight windows of 1, 2, 4, and 8 under byte limits. Preserve per-sender publication order; EOS follows all earlier visible frames. Batch control messages or pregrant reusable slots only with generation-safe reuse. Check prepared-descriptor support against this checkout's pinned bindings before choosing that implementation.

Acceptance: a slow destination does not block a healthy destination's transfer progress; a ready local receiver can start while remote output drains; queue wait is measured separately from pack time; increased concurrency does not increase peak memory beyond the declared budget. No speedup threshold is justified until the baseline exists.

**5. P2 — Remove GPU copy amplification after establishing immutable ownership**

*Partition/broadcast mechanics are inherited; the packed transport adds serialization and receive copying.*

| Path | Current cost | Candidate improvement |
|---|---|---|
| Hash exchange | Hash-key normalization may add temporary cast columns; cuDF partitions the table, then each destination slice is deep-copied. | Keep the partitioned parent allocation and pack destination views; evaluate avoiding materialization of temporary hash-only columns. |
| Export | `chunked_pack` writes directly into the leased arena allocation, then synchronizes. | There is no separate packed-buffer-to-staging copy to remove. Evaluate producer-ready events and asynchronous pack completion. |
| Import | `unpack` creates a view; the cuDF table constructor deep-copies it and synchronizes. | An immutable owning packed view can remove that copy only with reader-completion tracking and pressure-triggered copy-out/spill. |
| Broadcast | Destination zero gets the native batch; each additional destination gets a clone; remote destinations pack independently. | Pack one immutable representation and fan it out with per-destination completion references. |
| Small hash slices | Every nonempty per-input destination slice becomes a batch. | Measure size distributions; add bounded size/time batching or multi-buffer transfers when control overhead dominates. |

Evidence: [partition casts and slice copies](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/op/partition/gpu_partition_impl.cpp#L45), [pack and slack](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/sirius_ffi.cpp#L747), [receive deep copy](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/sirius_ffi.cpp#L847), [broadcast clones](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/op/sirius_physical_streaming_sink.cpp#L137), and [hash emission](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/op/sirius_physical_streaming_sink.cpp#L168).

Sharing today's mutable batch handle across destinations is unsafe because residency can change; the existing clones intentionally avoid that race. A view also retains its entire parent allocation: a slow destination can keep a large partitioned or broadcast buffer alive. Measure retained parent bytes, not only the child view's logical size.

Preserve writer-event ordering and transfer visibility. Do not delete stream synchronizations until the next owner can wait on a completion primitive. Direct receive views are a later fast path; with an EOS gate, they can prolong staging retention. Coalescing must be paired with concurrent draining: `batch_stream::push` is currently unbounded, and adding a blocking bound while the producer runs to completion can prevent its drain from ever starting. See [stream push](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/exec/batch_stream.cpp#L41).

Acceptance: identical results, fewer copied/packed bytes, lower measured copy/pack time, and bounded parent retention under skew and cancellation. Test strings, null masks, sliced offsets, empty partitions, and mixed-width hash keys.

**6. P2 — Measure existing fusion, then design nonblocking fragment execution**

Default `leaf` fusion already removes eligible local hash-exchange materialization. It requires a single local destination, a registered receiver expecting exactly one sender, and a leaf sender. `leaf-any` extends the partition-type policy, but still does not fuse intermediates or remote edges. Structural checks preserve partial-aggregation, ordering, limits, projections, tuple layouts, and scan-range contracts. Two leaves can be folded into one join receiver; this is already covered by source tests.

Evidence: [policy checks](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/compute_node_service.rs#L1035), [receiver eligibility](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/local_exchange.rs#L289), [structural refusal rules](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/crates/starrocks-plan-translator/src/fusion.rs#L214), and [two-leaf integration fixture](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/compute_node_service.rs#L4184).

Measure `off`, `leaf`, and `leaf-any` on identical FE plans. Report eligible/offered/fused edges, skipped reasons, removed engine runs, parked bytes, scan work, and final physical join choices. Fusion can expose useful cross-boundary planning and pipelining, but loses the exact materialized input statistics for the removed edge; it is not automatically better for every join. Sender-first arrival can also lose a fusion opportunity because no receiver is registered yet. Expand that path only if skipped-edge counts justify bounded plan waiting.

For remaining boundaries, the durable design is a query-scoped execution graph with schemas, estimated cardinalities, and sender sets declared before data arrival. Schedule pipelineable consumers on readable input; use EOS to finish a sender, rather than admit all receiver work. Retain actual semantic barriers for hash-join build, global sort, and blocking aggregates. Do not wait for exact input counts merely to construct a plan, and do not revert to cardinality one when an FE estimate is available.

This cannot be implemented by spawning today's `run()` in additional Rust tasks. Besides the engine queue, the C++ context serializes query lifecycle windows, including the fragment build/run window. Isolate execution state or use one query graph while preserving shared resource management and cancellation. Evidence: [input statistics](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/engine.rs#L456), [FFI lifecycle opening](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/sirius_ffi.cpp#L570), and [lifecycle mutex](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/sirius_context.cpp#L1555).

Acceptance: a pipelineable receiver consumes before upstream EOS, with correct blocking-operator behavior, bounded memory, and no join-plan regressions. Expanding fusion and replacing the fragment runtime should be separate changes with separate baselines.

**7. P2 — Address scan cache misses and metadata latency separately from transport**

Partial file splits already work. However, a byte-range scan deliberately cannot use a whole-file pin: `can_serve_with_columns` rejects either side when it has ranges. That is the correct current safeguard against reading extra rows. A scaling experiment that changes FE splits can therefore change cache serviceability as well as exchange volume; a warm filesystem cache is not a pin hit. See [pin serviceability gate](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/src/scan_manager/sirius_scan_manager.cpp#L1918).

If split scans dominate, add row-group or finer provenance to pinned chunks and serve exactly the groups owned by each range. Keep start-offset ownership, projection, file identity, and empty-split behavior. A batch containing both owned and unowned row groups needs slicing/filtering or a smaller pinning unit. Removing the range check or using file provenance alone would produce wrong results. Measure assigned versus selected row groups, decoded bytes, pin hits/misses, per-CN scan time, and the largest indivisible row group before changing CN count or file layout.

The branch's multi-file schema validation also reads every file footer sequentially, even though the API is async. With many small files, repeated `FILES()` schema RPCs can contribute substantial planning latency independently of GPU execution. Use bounded concurrent footer reads and, if warranted, a bounded cache keyed by reliable file version/identity. Preserve full-set schema validation and deterministic mismatch reporting. Do not replace it with sampling. See [sequential schema loop](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/file_schema.rs#L62).

Ordered exchange remains secondary: the translator creates a `SortRel`. A k-way merge/top-K needs a proven sender-order contract because exchange sequence alone does not prove globally sorted producer output. Preserve null ordering, collation, offsets, and limits. See [exchange sort](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/crates/starrocks-plan-translator/src/node_translator.rs#L783).

**8. P1 for measurement validity — Strengthen the benchmark gate before using timings**

The existing Q1/Q6 harness is useful for scan/aggregation smoke tests. Those query shapes provide no join coverage and are insufficient evidence for large shuffles, broadcast fan-out, or fusion under joins. The ignored NIXL agent test explicitly does not compare transferred payload bytes. The raw transport benchmark described by the original draft must be added or brought into this branch explicitly. See [available harness](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/benchmarks/tpch/README.md#L1) and [agent test limitation](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/src/nixl_transport.rs#L902).

Two concrete defects in `compare.py` weaken its exit status as an acceptance gate:

- It stops after one non-error result, preferring filenames in reverse lexical order, and skips later `ERROR`/empty files. A later failed timed run can be hidden by an older matching result; an earlier wrong answer can be ignored. It does not use the timing CSV to require every expected successful run. Numeric run indices must also be sorted numerically once run counts reach two digits. See [run selection](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/tools/compare.py#L36).
- Every numeric cell is converted to `float`. NaN-versus-finite comparisons can pass because `d > TOL` is false for NaN, and different large integers can collapse to the same float even with tolerance zero. Integers/counts require exact comparison; decimal and floating outputs need explicit per-type tolerances and nonfinite handling. See [numeric comparison](https://github.com/aocsa/sirius/blob/281b13bcb12321bac2927a8f4f996b710a463ec1/experimental/starrocks/tools/compare.py#L68).

Executed synthetic fixtures confirmed all four cases below incorrectly return exit code 0 and `MATCH` against the current comparator:

| Fixture | Expected rejection | Observed |
|---|---|---|
| `r1=42`, `r2=ERROR transfer failed`, oracle `42` | A timed run failed. | MATCH |
| `r1=999`, `r2=42`, oracle `42` | A run returned a wrong answer. | MATCH |
| Actual `nan`, oracle `42` | Finite/nonfinite mismatch. | MATCH |
| Actual `9007199254740993`, oracle `9007199254740992`, tolerance `0` | Unequal integers. | MATCH |

Use a run manifest/CSV to validate every recorded result, fail on missing/error/wedged runs, and distinguish intentional cold-start failure experiments from correctness-passing performance samples. Compare column shape and NULLs explicitly; use multiset comparison only for queries without an ordering contract. A sweep's process exit code must incorporate execution failures as well as value comparison. Add durable regression fixtures for the cases above before changing the transport.

**Implementation sequence and gates**

| Step | Deliverable | Acceptance gate |
|---|---|---|
| 0 | Correct comparator/run accounting; add correlated baseline instrumentation and representative exchange/join workloads. | Every expected run is validated, synthetic false positives fail, and cold/setup, queue, GPU, and control time are distinguishable. |
| 1 | Retry-safe owned leases and nonblocking peer setup. | Fault injection returns memory to baseline; immediate cold bidirectional queries and late peers progress without reciprocal waits. |
| 2 | Independent ingress, registered spill/reload ownership, explicit memory/credit reserves. | Input larger than the arena completes under a slow receiver and tight compute memory without unbounded host growth. |
| 3a | Remote drain tickets; dispatch ready local receivers before joining. | Local work overlaps remote drains; error propagation and per-destination ownership remain correct. |
| 3b | Fair per-peer progress loop, asynchronous control/pack tickets, bounded in-flight bytes. | Healthy peers advance despite a slow peer; no engine-queue or control wait is mislabeled as fabric time. |
| 4 | Measured broadcast pack reuse, partition-view export, and small-batch batching. | Fewer copies/control operations, identical results, bounded retained parent bytes. |
| 5 | Nonblocking query-scoped fragment graph; separately evaluated fusion expansion. | Early consumer execution, correct cardinalities/blocking operators, cancellation and memory progress. |
| 6 | Owning receive views and further copy elimination. | Reader-completion-safe credit return and lower copy cost without staging starvation. |

Step 3a and fusion-mode experiments can be evaluated after step 0 without committing to the full runtime redesign. Do not increase outstanding transfers before steps 1–2 make recovery and reclamation reliable. Schema-read improvements are independent and should be justified by measured planning time.

**Profiling protocol**

Use four layers: (a) registered-buffer WRITE with payload validation, (b) real pack + production lease/publication RPCs + receive copy, (c) multiple edges with slow-peer and constrained-memory scenarios, and (d) complete SQL queries covering large hash shuffle, broadcast join, skew, ordering, and Q1/Q6 controls. Test fixed-width, string/null, empty, and oversized batches. Inspect actual FE/CN plans to verify the intended exchange shape occurred.

Correlate each frame by query, producer fragment, receiver instance, exchange, sender, destination, sequence, and lease generation. `RemoteSendSpec.slot.fragment_instance_id` identifies the receiver; obtain producer/query identity explicitly rather than labeling it as the sender.

| Area | Required measurements |
|---|---|
| Planning/scan | FILES schema latency, translation/build time, selected row groups, pin hits/misses, decoded bytes and per-CN skew. |
| Producer/fusion | First/last batch availability, partition/broadcast copy time and bytes, parked lifetime, offered/fused/skipped edges, final join build sides. |
| Export | Enqueue/start/finish, GPU pack time, writer-event wait, payload bytes, actual lease bytes including slack. |
| Transport/control | Peer setup, lease RPC, transfer preparation/post/completion, publication RPC, per-peer active bytes and blocked-credit duration. |
| Receiver | Arrival, EOS, dispatch, copy-out completion, first consumption, lease/credit return. |
| Memory/recovery | Live/peak arena bytes, largest free block, compute/pin/ingress occupancy, spill/reload, quarantined/unpublished leases, post-query baseline. |

Record monotonic durations within each process and CUDA-event intervals for GPU work. Do not subtract unsynchronized clocks across hosts to infer one-way latency. Separate engine queue wait, GPU copies, and RPC waits from NIXL post-to-completion time. Connect incoming exchange frames to telemetry lineage; the current receive path creates batches without an originating operator lineage.

For a serialized batch, the diagnostic model remains:

`service time = export queue wait + pack/readiness wait + lease RPC + WRITE + publication RPC`

`effective sender throughput ≈ payload bytes / service time`

This omits producer execution and receiver processing. A pipeline needs enough outstanding bytes to cover the measured service latency at the target bandwidth, subject to GPU bandwidth and memory limits. Sweep windows 1/2/4/8 and controlled payloads such as 1/4/16/64 MiB as experiments, not default recommendations.

For every comparison record commit/binary identity, FE plan, CN/GPU placement, data/file versions, split assignments, actual pin hits, compute/staging/host budgets, fusion mode, UCX/NIXL settings, and peer-ready state. Keep startup and warm query samples separate. Report individual outcomes, latency distributions with sufficient repetitions, throughput, copied bytes, memory peaks, and correctness. A fast warm median must not hide cold timeouts or failed runs. The existing timer includes client/FE/planning time; label it end-to-end and add internal timings rather than interpreting it as GPU execution time.

**Validation performed and remaining**

Source paths and call ordering were checked against the current checkout. The four comparator fixtures above were executed using temporary TSVs, without changing implementation code. Only this plan was edited.

Attempted:

```bash
pixi run --manifest-path experimental/starrocks/pixi.toml -e cn --locked \
  cargo test --workspace --no-default-features --locked
```

Pixi refused before running tests because this machine is `osx-arm64` and the CN environment supports only Linux CUDA platform variants. Rust/C++ tests, cold-session races, lease fault injection, and all GPU/cluster performance claims remain unexecuted. Run the pure-Rust suite on a supported host, then the engine-linked and targeted GPU tests sequentially with representative FE plans. Verify remote source coverage first: the earlier `rdev info` in this session excluded `experimental`, so a default remote build would not by itself validate the current StarRocks CN changes.
