# Sirius Internals course — content update plan

> **Status:** Implemented locally; domain-owner review and learner acceptance remain pending
>
> **Content baseline:** `03be7dd2` on `docs-tutorial` (2026-07-07)
>
> **Implementation source snapshot:** Sirius `834e27c2`; DuckDB gitlink `08e34c44`
>
> **Target:** the live Super Sirius engine; `src/legacy/` remains out of scope
>
> **Primary audience:** engineers preparing to modify Super Sirius or its StarRocks integration

## 1. Objective

Make the course an accurate, practical path from “I can run Sirius” to “I can trace, modify,
test, and debug a Super Sirius operator without breaking its execution, memory, or lifecycle
contracts.”

The update must do three things in this order:

1. Correct the architectural model taught across the existing modules, including the distinction
   between DuckDB's push-based executor and Sirius's push-oriented hybrid execution model.
2. Add the missing planner, expression, initialization, observability, and teardown material.
3. Add reproducible content validation so stale links, duplicate IDs, and unsupported claims do
   not silently ship again.

This is a content plan. Visual redesign is out of scope except where a visual is necessary to
explain ownership, sequencing, or status. `styles.css` and `main.js` remain stock assets unless a
separate UI task explicitly changes that constraint.

## 2. Course-wide content contract

Before editing individual lessons, add a short “How to use this course” section to the opening
module and encode these conventions throughout the course.

### 2.1 Status vocabulary

Every capability or API that is not unambiguously live must carry one of these labels:

- **Current** — present and active in the pinned source snapshot.
- **Historical** — previously landed or useful for design history, but not a current capability.
- **Proposed** — design material that is not implemented in the pinned source snapshot.
- **Disabled** — source may exist, but the path is excluded, unwired, or otherwise non-functional.

The current Module 8 (target Module 10) must use **Proposed** at section level, not only on the
first API card. Iceberg must use **Historical + Disabled** where its history and current state are
discussed together.

### 2.2 Source hierarchy

Course claims are checked in this order:

1. Current code and tests at the pinned commit.
2. `docs/super-sirius/`, starting with its `README.md` reading order.
3. Current implementation plans under `experimental/starrocks/docs/`.
4. PR history, used as design provenance rather than proof of current behavior.

External conceptual material may define terminology but must not override current implementation
evidence. Use Justin Jaffray's
[“Query Engines: Push vs. Pull”](https://justinjaffray.com/query-engines-push-vs.-pull/) to define
producer-driven versus consumer-driven control. Use the
[DuckDB internals overview](https://duckdb.org/docs/current/internals/overview), vector execution
documentation, and `PipelineExecutor` source to classify DuckDB itself. Classify Sirius from the
pinned Super Sirius source. DuckDB source links in the finished lesson must target the DuckDB
submodule commit recorded by that Sirius snapshot rather than floating `main`, and the lesson
metadata must record both revisions.

The opening module and the operator-authoring lesson must tell readers to follow the
`docs/super-sirius/README.md` reading order **before modifying Super Sirius code**. Operator,
memory, expression, and IO work must also call out the repository's `/module-context` requirement.

### 2.3 Required lesson metadata

Each module starts with:

- 3–5 measurable learning objectives;
- prerequisites and required prior modules;
- estimated completion time;
- current/historical/proposed scope;
- the source files and authoritative docs used by the lesson.

Each module ends with:

- a concise recap;
- one code-reading task;
- one executable or observable exercise;
- one failure scenario;
- links to the next authoritative reading and lesson.

### 2.4 Terminology to standardize

Add a shared glossary and stop using “handle” as an unqualified synonym for four different
things. Distinguish:

- `shared_ptr<data_batch>` — an owning pointer to the batch object;
- `read_only_data_batch` / `mutable_data_batch` — RAII accessors and locks;
- repository `batch_id` — a stable lookup identifier;
- proposed `exchange_batch_handle` — channel metadata containing an ID and byte count.

Also define source, sink, pipeline breaker, boundary operator, port, repository, split, task,
reservation, and memory space once and reuse those definitions. Add these execution-model terms:

- **pull / iterator execution** — a consumer recursively asks its child for the next tuple or batch;
- **push execution** — a producer or pipeline driver passes an available batch into downstream
  operators;
- **pipeline driver** — the component that fetches source work and invokes operators in forward
  order;
- **control plane** — readiness discovery, dependency traversal, task creation, and scheduling;
- **data plane** — movement and transformation of `operator_data` and `data_batch` objects;
- **buffered handoff** — a producer publishes to a repository and a consumer later pops from it.

Do not classify an entire engine from a single method name such as `GetData()`, `execute()`, or
`get_next_task_input_data()`. State the layer being classified: client/result API, pipeline source,
intra-pipeline operator path, inter-pipeline boundary, or task scheduler.

## 3. Blocking factual corrections

These corrections precede new material because later lessons depend on them.

### 3.1 Execution model: push, pull, and the Sirius hybrid

Add one canonical comparison that every later lesson reuses:

| Scope | Classification | Why |
|---|---|---|
| DuckDB physical execution | **Vectorized push-based pipeline execution** | A pipeline driver fetches a `DataChunk` from its source, forwards it through `ExecutePushInternal()` and intermediate `Execute()` calls, then passes it to the sink. |
| DuckDB source/result boundaries | **Locally pull-like** | The pipeline driver calls `GetData()`, and a client may fetch results, but intermediate operators do not recursively call child `next()` methods. |
| Sirius intra-pipeline data path | **Push/forward-oriented** | `gpu_pipeline_task::compute_task()` owns input and invokes operators in plan order, passing each returned `operator_data` to the next operator. |
| Sirius inter-pipeline boundary | **Buffered asynchronous handoff** | A sink pushes `data_batch` references into repositories; a later consumer task pops them from repository-backed ports. |
| Sirius task discovery | **Pull-like readiness traversal** | `task_creator` asks for a task hint and follows `WAITING_FOR_INPUT_DATA` toward an upstream producer. |
| Sirius overall | **Push-oriented, event-driven hybrid** | Forward batch execution and producer publication are combined with readiness-driven scheduling and buffered consumer reads. It is not a Volcano iterator engine. |

Use this exact short-form conclusion in the introductory lesson:

> DuckDB uses vectorized push-based pipeline execution. Sirius uses a push-oriented, task-based GPU
> dataflow architecture with buffered repository boundaries and a pull-like readiness scheduler.

Required clarifications:

- Calling `source.GetData()` does not make DuckDB a pull engine; the source is polled by the
  pipeline driver, after which the driver pushes the chunk forward.
- Returning `operator_data` from a Sirius operator does not make Sirius a pull engine; the GPU task
  invokes operators in forward plan order rather than a root recursively requesting child output.
- Popping a repository entry is a pull-like operation at a buffered boundary, not evidence of an
  end-to-end iterator model.
- Client-side result fetching is a separate API boundary and must not be used to classify the
  internal executor.
- Sirius's classification applies only to supported queries executed by Super Sirius. Unsupported
  queries run through DuckDB's CPU executor and therefore use DuckDB's execution model.

Show these two control flows side by side:

```text
DuckDB pipeline task:
  FetchFromSource -> ExecutePushInternal -> operator Execute* -> sink -> combine/finalize

Sirius:
  control: schedule -> get_next_task_hint -> maybe follow producer -> create GPU task
  data:    repository/scan input -> execute operator* -> sink -> downstream repository
  events:  task completion -> update pipeline status -> notify/schedule downstream work
```

Primary sources:

- DuckDB `src/parallel/pipeline_executor.cpp` and the official internals/vector documentation;
- `src/creator/task_creator.cpp`;
- `src/pipeline/gpu_pipeline_task.cpp`;
- `src/op/sirius_physical_operator.cpp`;
- `src/pipeline/sirius_pipeline.cpp`;
- `src/pipeline/task_scheduler.cpp`.

Affected content:

- `modules/01-big-picture.html` — concise classification and diagram;
- target Module 3 — detailed DuckDB/Sirius execution-loop comparison;
- target Module 4 — control-plane hint traversal and completion events;
- target Module 6 — intra-pipeline and repository-boundary data flow;
- target Module 9 — tradeoff card for the hybrid architecture.

Exercise: label each edge in a DuckDB and Sirius trace as producer-driven, consumer-driven, or a
buffered handoff, then explain why neither `GetData()` nor a returned `operator_data` determines the
classification by itself.

### 3.2 Data flow: inside a pipeline versus across a pipeline boundary

Replace every absolute claim that “operators never return data” with the actual two-path model:

- **Inside one pipeline:** `gpu_pipeline_task::compute_task()` calls each operator's `execute()`
  and passes the returned `operator_data` to the next operator.
- **Across pipeline boundaries:** `publish_output()` calls the terminal operator's `sink()`, which
  publishes `data_batch` references into repository-backed ports for another pipeline.

Affected content:

- `modules/01-big-picture.html`
- `modules/03-life-of-a-query.html`
- `modules/04-data-ingest.html`
- `modules/05-data-plane.html`
- `modules/07-design-decisions.html`
- the control/data-plane language in `briefs/ui-upgrade-spec.md`

The revised data-plane module must show both paths side by side. It must not claim that a PIPELINE
barrier is the mechanism between adjacent operators executing in the same task.

### 3.3 `SiriusContext` and state lifetimes

Correct Module 2's “private per-connection engine” model. The extension callback constructs one
shared `SiriusContext` and registers that same object on each connection. The query lifecycle slot
serializes active query state.

Add a lifetime table:

| Lifetime | Representative state |
|---|---|
| Extension/database | `SiriusContext`, config, memory manager, executors, telemetry context |
| Active query | `planner::query`, pipelines, repositories, completion handler, scan providers |
| Pipeline | task global state, task counters, in-query `pipeline_memory_history` |
| Task | local state, reservation, stream, input accessors, retry state |

Replace the “where does per-query state live?” quiz with a lifetime/owner matching exercise.

### 3.4 Query completion

Correct all statements that say `RESULT_COLLECTOR` fulfills the future. The collector materializes
the result. `gpu_pipeline_executor` observes that the collector's pipeline is finished and calls
`completion_handler::mark_completed()`.

Update the Module 2 radio traffic, Module 3 sequence, glossary tooltip, and Module 8 empty-stream
story to name the completion handler explicitly.

### 3.5 Spillability and ownership

Teach four independent properties:

1. **Discoverability:** the candidate is in a registered repository or the inspectable scheduler
   queue.
2. **Lock state:** the batch must be idle/unlocked before conversion.
3. **Ownership:** the objects keeping the batch alive are distinct from its current lock state.
4. **Residency:** the current representation may be GPU, host, or disk.

Repositories are the first downgrade candidate tier; queued pipeline tasks are the second. A
`shared_ptr` does not by itself lock a batch or make a repository-registered batch invisible.
Proposed exchange channels use IDs to preserve a single authoritative repository lookup and a
clear ownership graph, not because reference counting alone prevents downgrade.

### 3.6 Cache scopes

Replace the existing “two caches stack” quiz with a cache matrix:

| Mechanism | Stores | Scope | Decode on hit? | How enabled |
|---|---|---|---|---|
| IO prefetch cache | input byte ranges | Sirius context/config dependent | Yes | scan-manager IO config |
| Pinned-table GPU cache | decoded columns on GPU | explicit pin until unpin/context teardown | Usually no | `pin_table(..., tier='gpu')` |
| Pinned-table host cache | decoded columns in pinned host | explicit pin until unpin/context teardown | GPU materialization still required | `pin_table(..., tier='host')` |
| Per-query provider/repository state | transient splits and batches | one query | N/A | automatic |

The lesson must show `pin_table`/`unpin_table`, distinguish an IO cache hit from a pinned-table hit,
and state how each hit is observed.

### 3.7 Scoped language instead of false absolutes

Replace or qualify these claims:

- “the only push in the engine” → “the initial task-creation request”;
- “exactly two kinds of threads” → a teaching grouping, followed by the actual thread inventory;
- “every bug reduces to seven objects” → “seven useful first triage components”;
- “nothing runs without a reservation” → “a GPU pipeline task cannot execute without a GPU
  reservation”;
- “all distributed thinking lives in the FE” → define the FE, wrapper/session, and engine split
  without erasing stream routing, resource accounting, or transport responsibilities.

## 4. Target curriculum

Expand from eight modules to ten. Preserve the current narrative arc while giving planning and
debugging first-class space.

| # | Target module | Source |
|---|---|---|
| 1 | The Big Picture, Execution Model, and First Run | revise current Module 1 |
| 2 | Actors, Threads, Ownership, and Lifetimes | revise current Module 2 |
| 3 | From DuckDB Plan to Sirius Pipelines | **new** |
| 4 | Life of a Query: Tasks, Hints, and Completion | revise current Module 3 |
| 5 | Scan, IO, and Cache | revise current Module 4 |
| 6 | The Data Plane: Operator Data, Ports, and Repositories | revise current Module 5 |
| 7 | Reservations, Downgrade, and Multi-GPU | revise current Module 6 |
| 8 | Operator Authoring, Observability, Testing, and Error Teardown | **new** |
| 9 | Design Decisions and Their Tradeoffs | revise current Module 7 |
| 10 | Streaming Frontier | revise current Module 8; optional **Proposed** capstone |

Renumber module files, navigation, TOC entries, lesson links, module chips, quiz IDs, and
`data-target` attributes in one mechanical pass after the content files are stable. Do not mix
renumbering with substantive rewrites.

## 5. New Module 3 — From DuckDB Plan to Sirius Pipelines

This module fills the largest architectural gap between interception and execution.

### 5.1 Push, pull, and the two execution loops

Start with Jaffray's producer-versus-consumer control distinction, then use source-backed traces
rather than generic engine diagrams.

For DuckDB, walk the current `PipelineExecutor` loop:

```text
PipelineExecutor::Execute()
  -> FetchFromSource(DataChunk)
  -> ExecutePushInternal(DataChunk)
  -> PhysicalOperator::Execute() in forward order
  -> PhysicalOperator::Sink()
  -> PushFinalize()/Combine()
```

Explain the source, intermediate-operator, and sink state machines, including the meaning of
`NEED_MORE_INPUT`, `HAVE_MORE_OUTPUT`, `FINISHED`, and `BLOCKED`. Show how local/global operator
state permits parallel pipeline tasks, and how joins, aggregates, and sorts introduce pipeline
boundaries. The lesson must explicitly reconcile DuckDB's `GetData()` call with its official
push-based classification.

For Sirius, separate control from data:

```text
Control plane:
  task_scheduler::start_query()
    -> task_creator::schedule()
    -> get_next_task_hint()
    -> READY or recursive WAITING_FOR_INPUT_DATA producer traversal
    -> pop repository inputs and create a GPU task

Data plane:
  gpu_pipeline_task::compute_task()
    -> run_one_operator() in pipeline order
    -> publish_output()
    -> terminal sink()
    -> downstream repository-backed ports

Completion events:
  mark_task_completed()
    -> update_pipeline_status()
    -> notify_downstream_pipelines()
    -> schedule newly eligible consumers / update parents
```

Explain that repositories deliberately break the direct producer-to-consumer call stack. The
producer publishes a reference, repository state buffers it, and the consumer later claims it.
This supports independent GPU scheduling, memory reservations, barriers, and downgrade, at the
cost of a more complex readiness and completion protocol.

Include one supported-query routing note: DuckDB performs parsing, binding, and optimization;
Sirius builds and executes its own physical pipeline graph when the plan is supported; otherwise
the query stays on DuckDB's CPU executor. Do not imply that Super Sirius merely swaps GPU kernels
into DuckDB's `PipelineExecutor`.

Exercise: given the source-level traces for a projection and a hash join, identify the pipeline
driver, source, operators, sink, buffered boundaries, and the component that decides what runs
next. The answer must classify DuckDB as push-based and Sirius as a push-oriented hybrid without
calling either engine a Volcano pull executor.

### 5.2 Expression lowering and execution

Explain the boundary and direction introduced by #796/#847:

```text
DuckDB expression
  -> planner translation
  -> sirius::ast
  -> GPU expression translator/executor
  -> cuDF AST / JIT / fused GPU work
```

Required coverage:

- why DuckDB expression objects stop at the planner boundary;
- the native type/AST design current;
- references, literals, functions, casts, comparisons, and unsupported-expression fallback;
- where cuDF AST lowering occurs;
- what “JIT” and “fusion” mean in Sirius, with one concrete expression trace;
- how to add a new expression and which unit/integration tests prove it.

Primary sources:

- `src/expression/`
- `src/expression_executor/`
- `src/planner/`
- `docs/super-sirius/expression-executor.md`

Exercise: trace a filter expression from DuckDB logical plan to the cuDF call and identify the
fallback point for an unsupported function.

### 5.3 Engine initialization and compound-operator splitting

Show the stage currently skipped between `create_plan()` and task execution:

```text
logical plan conversion
  -> Sirius physical operator tree
  -> sirius_engine::initialize_internal()
  -> sirius_pipeline_converter
  -> compound-operator expansion and pipeline graph
```

Use two vertical slices:

- `HASH_JOIN` → partition/build plumbing and `PARTITION + CONCAT` pipelines;
- `ORDER_BY` → `SORT_SAMPLE + SORT_PARTITION + MERGE_SORT` chain.

State which transformations are plan conversion, which are pipeline construction, and which occur
only after an engine/context exists.

### 5.4 Repository wiring descriptors and materialization

Explain #607/#770 as an actual two-phase contract rather than only naming it:

1. `sirius_pipeline_converter::compute_repository_wiring()` emits pure `repository_wiring`
   descriptors.
2. `materialize_repository_wiring()` creates repositories, attaches ports, and records downstream
   consumers once runtime state exists.

Primary sources:

- `src/include/pipeline/repository_wiring.hpp`
- `src/pipeline/repository_wiring_materializer.cpp`
- `src/pipeline/sirius_pipeline_converter.cpp`
- `src/sirius_engine.cpp`

Exercise: given one FULL and one PARTIAL edge, identify the descriptor fields and the runtime
objects materialization creates.

## 6. Revised Module 4 — Tasks, hints, completion, and locking

Retain the current end-to-end query trace, but make these contracts explicit. Keep the
control-plane and data-plane lanes visually separate: hint traversal discovers eligible work;
`gpu_pipeline_task` moves data forward after a task exists.

### 6.1 Use a real hint-chain example

Do not ask for the hint of a filter executing inside the same pipeline. Use a downstream pipeline
source with a repository-backed port and an unfinished producer pipeline. Show READY,
WAITING_FOR_INPUT_DATA, and done on objects the task creator actually schedules. Label the
recursive producer traversal as pull-like readiness discovery, not row/batch pulling and not the
engine's overall execution model.

### 6.2 Quote the task-creation lock rule exactly

Include the comment on `get_task_creation_lock()` from
`src/include/pipeline/sirius_pipeline.hpp:177-182` verbatim as a code excerpt. Explain the exact
critical section:

```text
acquire pipeline status lock
  -> consume pipeline state (pop port data / claim partition)
  -> construct task (constructor calls mark_task_created)
  -> release lock
```

Do not rename this rule or paraphrase it into a different invariant. Pair it with the RAII task
accounting paths in `gpu_pipeline_task` and `cpu_source_task`.

Exercise: identify the race if the lock is released after the pop but before task construction.

### 6.3 Explain `update_pipeline_status(bool original_pipeline = true)`

Show the real signature and explain both paths:

- `mark_task_completed()` evaluates the original pipeline with the default `true`;
- `notify_downstream_pipelines()` cascades to parent pipelines with `false`;
- the flag prevents duplicate consumer scheduling for the original task path while allowing parent
  status changes to schedule newly unblocked consumers.

The completion diagram must include the parent cascade and the separate executor-side terminal
query-completion check. It must also distinguish three events that can occur at different times:
output becomes visible in a repository, a consumer becomes eligible under its barrier, and the
producer pipeline becomes fully complete.

## 7. Revised Module 5 — Scan, IO, and cache

Required updates:

- Link `gpu_ingestible` to its contract header,
  `src/include/op/scan/gpu_ingestible.hpp`, not only its `.cpp` implementation.
- Separate table abstraction, storage format, and datasource location:
  Iceberg / Parquet / DuckDB-native / local file / S3 are not four values of one dimension.
- Keep the current Iceberg disabled-state warning.
- Replace the stale cache story with the cache matrix from §3.6.
- Add one trace showing provider → connector → `scan_operator_input` → `gpu_ingestible` →
  `operator_data` → remaining operators in the same pipeline → boundary repository.

## 8. Revised Modules 6–7 — data and memory contracts

### 8.1 Data plane

Show these objects separately:

- `operator_data` passed between operators inside a task;
- `data_batch` and its representations;
- RAII accessors;
- repository ownership and port/barrier metadata;
- downgrade discovery through repositories and the scheduler queue.

Add an execution-model callout: `operator_data` being returned from `execute()` is a local API
shape inside a forward-driven GPU task, while `get_next_task_input_data()` popping a repository is
a consumer read at a buffered pipeline boundary. Neither fact alone classifies the whole engine.

Revise the decision card in the later history module to say “repositories at pipeline boundaries,”
not “operators never return data.”

### 8.2 Reservations and memory history

Add a real code walk:

1. `gpu_pipeline_task::get_estimated_reservation_size_info()` obtains an estimate from in-query
   pipeline history or operator no-history estimates.
2. `gpu_pipeline_executor` calls `_memory_space->make_reservation(bytes_needs)`.
3. A partial reservation triggers `request_downgrade()` and retries with
   `make_reservation_or_null()`.
4. The task attaches the reservation-aware allocator to its stream.
5. An OOM records `record_on_failure(input_basis, peak_bytes)`.
6. Later tasks/retries in that query call `estimate_peak_memory()` and reserve more accurately.

Explicitly state that the current `pipeline_memory_history` is recreated by
`task_creator::prepare_for_query()`; it does not teach a separately planned next query.

Exercise: use an OOM log record to calculate the next reservation components: input basis, operator
peak estimate, and bytes required to materialize input.

## 9. New Module 8 — Operator authoring, observability, testing, and error teardown

### 9.1 Operator-authoring contract

Turn the scattered “where files go” advice into one end-to-end checklist:

1. Run `/module-context` for the operator and dependency APIs being changed.
2. Add the operator enum and `ToString` name.
3. Choose its pipeline role and implement the correct `execute()`, `sink()`, source, hint, input,
   and finalization methods.
4. Define task local/global state and obey the exact task-creation lock rule if it consumes
   pipeline state outside the normal path.
5. Implement a defensible `no_history_peak_memory_estimate()` and ensure OOM retry is either
   resumable or task-atomic.
6. Register production and test files in CMake.
7. Add focused, integration, multi-GPU, and race coverage in proportion to the contract touched.
8. Update `docs/super-sirius/` and link the authoritative section from the course.

Use one small existing operator as a worked example and contrast it with a source, a boundary
operator, and a blocking/compound operator so learners do not copy the wrong shape.

### 9.2 Logs, NVTX, and Quent

Explain the three complementary views:

- structured/log messages for control flow, errors, task IDs, pipeline IDs, and reservation data;
- NVTX ranges around pipelines, operators, sinks, and task execution for GPU profiler timelines;
- experimental Quent telemetry for plans, ports, tasks, queues, routing, reservation, downgrade,
  compute, and finalization states.

Use `docs/super-sirius/quent-telemetry.md` as the primary guide and preserve its
**Experimental** warning. Include:

- telemetry configuration;
- `sirius_set_query_label` and explicit query labels;
- how to generate telemetry;
- `pixi run quent`;
- how to correlate one query across a log, an NVTX profile, and the Quent task timeline.

Exercise: provide a short trace and ask the learner to identify whether time was spent waiting for
input, reserving, downgrading, queued, or computing.

### 9.3 Test selection

Teach the actual validation ladder:

1. focused Catch2 test/tag;
2. relevant SQLLogic or integration test;
3. DuckDB CPU comparison where correctness is involved;
4. multi-GPU or race coverage where ownership/scheduling changes;
5. `pixi run make test`;
6. `pixi run pre-commit run -a`.

Replace the generic `[cpu_cache]` recommendation in `briefs/ui-upgrade-spec.md` with a rule to use
the tag belonging to the code being taught. Module 10 uses `[exchange_channel]`,
`[streaming_source]`, and `[streaming_sink]` from the implementation plans.

### 9.4 Error propagation and teardown

Promote `drain_after_error()` from a Module 8 tooltip to a full lifecycle:

```text
report error
  -> stop task creator first and keep its queue interrupted
  -> drain top-level task queue
  -> quiesce GPU/scan execution
  -> drain stale creation requests and per-query state
  -> allow teardown/restart only after raw operator references cannot be consumed
```

Explain why the ordering exists, which objects still hold raw operator pointers, how the future
receives the exception, and what an external consumer/session must guarantee before query state is
destroyed.

Exercise: reorder two teardown steps and identify the resulting use-after-free window.

## 10. Revised Module 9 — decisions with tradeoffs

Turn each “standing decision” card into a compact ADR:

- context/problem;
- decision;
- benefits;
- cost/tradeoff;
- current status;
- evidence/source;
- condition that would justify revisiting it.

Add first-class cards for:

- vectorized push execution in DuckDB versus Sirius's push-oriented hybrid control/data paths;
- native `sirius::ast` and expression lowering;
- compound-operator splitting;
- plan-time wiring descriptors versus runtime materialization;
- exact task-creation locking and RAII task accounting;
- observability as part of execution design, not an optional debugging add-on.

Correct the stale “module 4” data-plane reference after renumbering.

## 11. Revised Module 10 — proposed streaming capstone

Add an unavoidable module-level banner:

> **Proposed capstone:** `exchange_channel`, streaming source, and streaming sink do not exist in
> the pinned source tree. Code blocks in this lesson are design targets, not callable APIs.

Repeat a compact **Proposed** badge on every screen that presents a planned type or method. Planned
file links must point to the implementation plan or be rendered as unlinked future paths; they must
not look like existing GitHub source files.

Include an “open before coding” table covering:

- channel element type;
- hint convention;
- sink finalization;
- boundary shape;
- re-arm ownership;
- capacity/config plumbing;
- external batch accounting;
- ordering guarantees;
- cancellation/abort semantics;
- multi-sender EOS and multi-GPU affinity.

Rename “war story” to “predicted liveness failures” unless the cases are reproduced by code and
tests. Correct the module-number chips after renumbering. Replace the `[cpu_cache]` command with the
planned tags and the full validation ladder.

## 12. Build and content validation

Extend `build.sh` beyond concatenation. Keep generation deterministic, then run a validation helper
before replacing `index.html`.

Required checks:

1. Every module has exactly one root `<section>` and no `<script>` or `<style>` tag.
2. All HTML `id` values are globally unique after assembly.
3. Every internal `href="#..."`, `data-target`, lesson link, and TOC link resolves exactly once.
4. Every quiz container ID matches its module number and is unique.
5. Every `data-steps` JSON payload parses.
6. Local paths referenced by Sirius GitHub `blob/dev` or `tree/dev` links exist in the checkout.
7. Required GitHub and documentation links are syntactically valid; an optional network mode checks
   external status without making offline builds fail.
8. Generated `index.html` exactly equals `_base.html + modules in numeric order + _footer.html`.
9. No stale module-number chip or old filename remains after renumbering.
10. Content-source metadata and status labels are present in every module.

Implementation shape:

- make `build.sh` resolve its own directory and use strict shell settings;
- generate into a temporary file;
- run `validate-course` against the temporary assembly;
- replace `index.html` only after validation succeeds;
- support a non-mutating `build.sh --check` mode for CI and pre-commit use.

## 13. Work sequence and review boundaries

Use small, reviewable changes in this order.

### PR 1 — content contract and factual corrections

- Add lesson metadata/status/glossary conventions.
- Add the canonical DuckDB push-based / Sirius push-oriented hybrid classification to Module 1 and
  remove unqualified whole-engine push/pull claims.
- Correct data flow, `SiriusContext`, completion, spillability, cache scope, and false absolutes.
- Fix `gpu_ingestible` contract link.
- Do not renumber modules yet.

### PR 2 — new plan-to-pipeline module

- Add the source-level DuckDB/Sirius execution-loop comparison, expression lowering, JIT/fusion,
  initialization, compound splitting, and wiring materialization.
- Review by planner/expression and pipeline-converter owners.

### PR 3 — task/completion and data/memory revisions

- Rewrite the hint example.
- Add the exact task-creation lock rule.
- Explain `original_pipeline` and parent cascade.
- Add reservation API and memory-history code walk.
- Review by scheduler/task-creator and memory owners.

### PR 4 — scan/cache revision and observability module

- Replace cache content and add pinning exercises.
- Add logs, NVTX, Quent, test selection, and teardown.
- Review by scan/IO, telemetry, and lifecycle owners.

### PR 5 — decisions and streaming capstone

- Convert decision cards to tradeoff-aware ADRs.
- Strengthen proposed-state treatment and surface open streaming decisions.
- Correct test commands and planned-file references.

### PR 6 — renumbering and validation

- Renumber to ten modules in one mechanical change.
- Fix all quiz IDs, anchors, nav dots, TOC entries, lesson links, and module chips.
- Add validation helper and `build.sh --check`.
- Rebuild `index.html` only after all checks pass.

## 14. Definition of done

The content update is complete when all of the following are true:

- Every load-bearing claim is current-source-backed, explicitly historical, or explicitly proposed.
- The course consistently identifies DuckDB as a vectorized push-based pipeline executor and
  Sirius as a push-oriented, event-driven hybrid; no lesson calls Sirius a classical Volcano pull
  engine or presents it as a pure unbuffered push chain.
- A learner can explain why DuckDB's source `GetData()`, Sirius's returned `operator_data`, and a
  repository pop are layer-local API choices rather than sufficient whole-engine classifications.
- The course correctly distinguishes intra-pipeline `operator_data` flow from inter-pipeline
  repository flow.
- A learner can explain the shared `SiriusContext` lifetime and terminal completion path.
- Expression lowering, compound splitting, and wiring materialization are traceable end to end.
- The exact task-creation lock rule, `original_pipeline` cascade, reservation call, memory-history
  update, and error-drain order are taught with source excerpts.
- A learner can use logs, NVTX, and Quent to classify a query stall.
- Module 10 cannot be mistaken for implemented code.
- Every exercise command is relevant to the lesson and has been run or explicitly marked as
  planned/not-yet-runnable.
- `build.sh --check` detects duplicate IDs, broken internal anchors, stale local source links, and
  generated-output drift.
- Domain owners approve their sections and one engineer unfamiliar with Sirius completes the first
  run, query trace, memory diagnosis, and operator-testing exercises without factual correction from
  an instructor.

## 15. Learner acceptance scenario

Use one final practical assessment instead of only multiple-choice recall. The learner must:

1. Run a query and determine whether Sirius or DuckDB executed it.
2. Classify DuckDB and Sirius by execution layer, then explain why a source fetch or repository pop
   does not make either engine an end-to-end pull iterator.
3. Trace one expression from DuckDB into `sirius::ast` and the GPU executor.
4. Sketch the pipelines produced for either a hash join or an order by.
5. Trace one batch inside a pipeline and then across a repository boundary, labeling forward push,
   buffered handoff, and readiness traversal.
6. Point to the lock that closes the port-pop/task-construction race.
7. Explain how a reservation shortfall triggers downgrade and how an OOM peak influences the next
   in-query estimate.
8. Correlate one task across logs, NVTX, and Quent telemetry.
9. Explain the `drain_after_error()` ordering.
10. Identify which streaming APIs are proposed and list the unresolved decisions before coding.

Passing this assessment is the evidence that the course prepares someone to modify the engine,
not merely recognize its vocabulary.
