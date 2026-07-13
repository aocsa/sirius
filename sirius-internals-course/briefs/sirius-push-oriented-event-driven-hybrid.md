# Why Sirius is a push-oriented, event-driven hybrid query engine

> **Status:** Engineering discovery brief
>
> **Source snapshot:** Sirius `834e27c2` on `docs-tutorial`
>
> **Scope:** Current Super Sirius implementation under `src/`; proposed StarRocks streaming
> behavior is labeled explicitly
>
> **Audience:** Engineers reading or modifying the Sirius planner, pipeline, scheduler, operator,
> memory, or StarRocks integration paths

## Executive summary

Sirius is best classified as a **vectorized, push-oriented engine with event-driven scheduling and
pull-like control points**. It is not a classic Volcano iterator engine, and it is not a pure
callback-style push engine.

The short version is:

> Sirius uses producer-driven, forward batch execution inside a GPU task; buffered repository
> handoffs between pipelines; event-triggered task discovery and completion; and pull-signal
> admission for input batches, worker capacity, memory, and final results.

Four independent dimensions are easy to conflate:

| Dimension | Sirius classification | Evidence |
|---|---|---|
| Data granularity | Vectorized and columnar | Tasks carry `operator_data` containing cuDF-backed `data_batch` objects, not individual rows. |
| Intra-pipeline direction | Push/forward-oriented | `gpu_pipeline_task::compute_task()` invokes operators in plan order and passes each returned `operator_data` to the next operator. |
| Inter-pipeline handoff | Buffered producer publication | A terminal sink publishes `data_batch` references into downstream repository-backed ports. |
| Scheduling and admission | Event-driven with pull-like claims | Readiness events trigger task discovery; the task creator pops input; GPU executors advertise capacity; the scheduler matches ready work to ready devices. |

The term **hybrid** is important. Calling Sirius simply “push-based” hides its repository pops,
readiness traversal, worker pull-signals, barriers, memory admission, and materialized result
boundary. Calling it “pull-based” is more misleading: no root operator recursively calls `next()`
through a Sirius physical plan.

## 1. Reference model: producer-driven versus consumer-driven

[Justin Jaffray's “Query Engines: Push vs. Pull”](https://justinjaffray.com/query-engines-push-vs.-pull/)
uses a useful control-flow distinction:

- In a **pull/iterator** engine, the consumer asks its child for the next tuple or batch. The request
  recursively travels toward the scan.
- In a **push** engine, an available tuple or batch moves from its producer toward downstream
  consumers.

A classic Volcano-shaped request looks like this:

```text
Result.next()
  -> Aggregate.next()
       -> Filter.next()
            -> Scan.next()
```

The corresponding forward-oriented shape is:

```text
scan batch -> filter -> projection -> aggregate -> result
```

Method names do not classify an engine. `execute()`, `GetData()`, a returned value, or a repository
`pop` describes one local interface. Classification requires following control and data across the
whole execution path.

## 2. Current Sirius control and data paths

Sirius separates the control plane from the data plane:

```text
Control:
  initial source schedule
    -> task hint traversal
    -> input claim and task construction
    -> scheduler queue
    -> ready-device match
    -> GPU execution
    -> completion/status event
    -> downstream scheduling

Data:
  scan or repository input
    -> operator_data
    -> execute operator 0
    -> execute operator 1
    -> ...
    -> terminal sink
    -> downstream repository
```

The main implementation points are:

- [`task_scheduler::start_query()`](../../src/pipeline/task_scheduler.cpp) seeds execution by
  scheduling the first indexed scan.
- [`task_creator::get_operator_for_next_task()`](../../src/creator/task_creator.cpp) follows
  `READY` and `WAITING_FOR_INPUT_DATA` hints.
- [`task_creator::manager_loop()`](../../src/creator/task_creator.cpp) claims input, constructs a
  `gpu_pipeline_task`, and submits it to the scheduler.
- [`task_scheduler::management_eventloop()`](../../src/pipeline/task_scheduler.cpp) matches queued
  work with `device_ready` events.
- [`gpu_pipeline_task::compute_task()`](../../src/pipeline/gpu_pipeline_task.cpp) carries a batch
  forward through the operator chain.
- [`gpu_pipeline_task::publish_output()`](../../src/pipeline/gpu_pipeline_task.cpp) invokes the
  terminal sink.
- [`sirius_physical_operator::sink()`](../../src/op/sirius_physical_operator.cpp) publishes output
  batches into downstream ports.
- [`sirius_pipeline::update_pipeline_status()`](../../src/pipeline/sirius_pipeline.cpp) evaluates
  pipeline completion and propagates status.

## 3. Why the intra-pipeline path is push-oriented

A Sirius pipeline is an ordered list of physical operators. A `gpu_pipeline_task` owns the current
`operator_data` and invokes those operators in forward plan order:

```cpp
for (size_t i = start_index; i < operators.size(); i++) {
  operator_input_output_data = run_one_operator(
    operators[i].get(), *operator_input_output_data, stream, ...);
}
```

Each operator returns the input for the next operator. This return value does **not** make Sirius a
pull engine. The downstream operator did not request data from its child; the pipeline task already
had an available batch and forwarded it through the plan.

This is push-oriented rather than pure callback push:

- The producer does not directly call an arbitrary downstream closure.
- `gpu_pipeline_task` is a centralized pipeline driver.
- The driver passes output forward eagerly for the current batch.
- There is no recursive consumer-to-producer `next()` chain.

Sirius is also not currently a whole-pipeline, compiled push engine. `run_one_operator()` retains
explicit physical-operator calls and currently synchronizes the CUDA stream after each operator.
Expression lowering or fusion within an operator is a different concern from compiling the entire
pipeline into one function.

Consequences:

- Operator and kernel boundaries remain observable.
- One batch follows an ordered sequence within its task.
- Parallelism primarily comes from multiple tasks, CUDA streams, worker threads, partitions, and
  GPUs—not from one batch executing all pipeline stages asynchronously at once.

## 4. Why the inter-pipeline path is a buffered push handoff

Pipeline breakers split execution at joins, aggregates, sorts, partitioning, concatenation, and
other stateful boundaries. When a task reaches the end of a pipeline, `publish_output()` calls the
pipeline's sink. The default sink publishes each output batch to every configured downstream port.

```text
Pipeline A task
  -> execute operators
  -> sink(output)
  -> add batch to downstream repository

Pipeline B
  <- later task-creation event
  <- pop repository input
  <- execute operators
```

The repository provides three important properties:

1. **Lifetime decoupling:** the producer task can finish before the consumer task begins.
2. **Scheduling decoupling:** producing data and choosing a worker for the consumer are separate
   decisions.
3. **Memory visibility:** idle batches remain registered with cuCascade repositories and can be
   considered by downgrade/spill machinery.

Because a sink can publish to multiple next ports, producer publication also maps naturally to
DAG-shaped dataflow. The downstream branches do not need to issue competing recursive requests to
the same producer.

Repository consumption is locally pull-like: `get_next_task_input_data()` pops batches from ports.
That is a consumer claim at a buffered boundary, not an end-to-end iterator model.

## 5. Why scheduling is event-driven

The main scheduling path waits for state changes instead of repeatedly scanning the entire plan.
Important events include:

- initial source scheduling;
- a newly created task entering the central queue;
- a GPU executor reserving a worker slot and emitting `device_ready`;
- a task completing and exposing downstream consumers;
- a pipeline becoming drained and finalizable;
- an error or cancellation interrupting queues and executors;
- in the proposed streaming design, data arrival, channel capacity, EOS, and abort.

The central scheduler consumes two explicit event types:

| Event | Meaning |
|---|---|
| `task_available` | A task was added to the central queue; rerun matching in case a device is already waiting. |
| `device_ready` | A GPU executor has reserved a worker slot and is ready to accept a task. |

Dispatch requires both sides:

```text
ready task + compatible ready GPU = dispatch
```

This is why the source comment calls it a **pull-signal scheduler**. Executors advertise demand
instead of accepting an unbounded push into private queues. Tasks remain in the central,
downgrade-visible queue until a compatible executor is ready.

## 6. Pull-like control points that make the design hybrid

### 6.1 Readiness traversal

The task creator asks an operator for `get_next_task_hint()`:

- `READY`: construct a task for this operator.
- `WAITING_FOR_INPUT_DATA`: follow the named producer and look for upstream work.
- no hint: there is no task to create now.

Following `WAITING_FOR_INPUT_DATA` toward a producer is pull-like control flow. It discovers where
work can originate; it does not pull result tuples recursively through the operator chain.

### 6.2 Repository input claims

After readiness is established, `get_next_task_input_data()` pops one batch from the applicable
ports. The claim is protected by the pipeline task-creation lock through task construction so
completion cannot observe empty ports and balanced task counters in the middle of a claim.

### 6.3 Worker-capacity admission

A GPU executor reserves a bounded-pool slot before emitting `device_ready`. No slot means no ready
signal, so the scheduler does not dispatch more work to that executor.

### 6.4 Memory admission

Before a GPU task runs, its executor requests a memory reservation. The reservation determines the
target memory space, and `prepare_for_processing()` locks or converts all input batches into that
space before the first operator executes.

An OOM can produce an `oom_reschedule_exception` carrying the current operator index and input
state. Observed peaks update pipeline memory history so later reservations can improve. This is
admission and retry around a push-oriented data path.

### 6.5 Client result retrieval

The current terminal path uses a materialized result collector. `sirius_engine::execute()` waits on
a completion future, validates executor queues, and later returns the collector's result to DuckDB.
Client result fetching is a separate boundary and does not determine the internal executor model.

## 7. Barriers: pipelined and materialized execution coexist

Sirius ports carry barrier semantics:

| Barrier | Behavior |
|---|---|
| `PIPELINE` | A consumer may run as batches arrive while its producer is still active. |
| `PARTIAL` | Incremental consumption is possible subject to operator/phase constraints. |
| `FULL` | The consumer waits until its producer pipeline finishes before consuming buffered data. |

Example:

```text
scan -> filter -> partial aggregate
                         |
                         | FULL barrier
                         v
                   merge aggregate -> result collector
```

Sirius therefore mixes pipelined overlap with deliberate materialization. “Push-oriented” does not
mean that every batch immediately crosses every query operator.

## 8. Completion is event-triggered and edge-sensitive

A normal pipeline finishes when its effective source is finished, its relevant ports are empty,
and `tasks_created == tasks_completed`. Task construction increments the created counter; task
destruction increments the completed counter and calls `update_pipeline_status()`.

The task-creation lock is load-bearing:

```text
lock pipeline status
  -> claim port/partition input
  -> construct task
  -> mark task created
unlock
```

Without that atomic sequence, a completion thread could observe an empty port and equal counters
while a new task exists conceptually but has not yet been counted.

This completion model is also edge-sensitive. If a condition becomes true without an event that
re-evaluates it, completion can stall.

Proposed streaming example:

```text
last data task completes
  -> status check sees stream still open

EOS arrives later with zero tasks in flight
  -> stream is now drained
  -> no task completion remains to trigger another status check
```

Therefore a production streaming source/session must turn push, capacity, close/EOS, and
cancellation into explicit scheduler/status wakes. A correct predicate is insufficient if no event
invokes it.

## 9. Backpressure and memory behavior

Sirius expresses backpressure mainly as **task-admission state**, not by blocking a GPU worker in a
sink:

- a GPU executor without a worker slot emits no ready event;
- a task without an adequate reservation waits, downgrades data, or retries;
- a consumer without ready input creates no task;
- a `FULL` barrier delays consumer task creation;
- in the proposed exchange sink, a full output channel should stop sink-task admission while
  upstream batches remain repository-visible and spillable.

This separation matters for GPU systems. Parking a scarce worker inside blocking channel or network
I/O can create head-of-line blocking or memory deadlocks. Keeping blocked work represented as
queued tasks and idle repository batches preserves scheduler and downgrade visibility.

The “event-driven” label applies to the main task path, not every subsystem. The downgrade executor
also has a monitor loop that periodically checks memory pressure using `monitor_period_ms`. Sirius
is therefore event-driven at its execution core, not a system with zero polling anywhere.

## 10. End-to-end example

Consider:

```sql
SELECT region, SUM(revenue)
FROM sales
WHERE year = 2026
GROUP BY region;
```

A simplified Sirius execution is:

1. DuckDB plans the query; Sirius converts it into physical operators and splits compound operators
   into pipelines.
2. `start_query()` schedules the initial indexed scan.
3. The task creator obtains scan input and constructs a GPU task.
4. The task enters the central queue and emits `task_available`.
5. A GPU executor reserves a worker and emits `device_ready`.
6. The scheduler matches the task to the device, respecting device preferences and locality.
7. The executor acquires a GPU memory reservation and prepares input batches on that device.
8. The task carries the batch forward through scan/filter/partial-aggregation operators.
9. The sink publishes partial aggregates into a repository.
10. Completion schedules or unblocks the merge-aggregate pipeline according to its barrier.
11. Merge tasks claim buffered partials, compute the final groups, and publish to the result
    collector.
12. Terminal pipeline completion resolves the future; DuckDB retrieves the materialized result.

No terminal aggregate recursively asks the scan for the next batch. But several control layers
claim buffered inputs and advertise resource demand. That combination is the hybrid.

## 11. What the classification does and does not imply

### It does imply

- Batch availability, rather than client `next()` calls, drives internal progress.
- Pipelines can overlap across repository boundaries where barriers permit.
- Scheduling and data ownership are decoupled.
- GPU capacity, locality, reservations, and spill can participate in admission.
- External streaming inputs fit the architecture only when their edges are translated into Sirius
  scheduling and completion events.

### It does not imply

- Every operator directly invokes a downstream callback.
- Every query edge streams without buffering.
- Every CUDA kernel is fused into one compiled pipeline.
- All subsystems are free of periodic polling.
- Backpressure happens automatically; bounded channels need explicit capacity and re-arm contracts.
- A method called `get`, `pop`, or `pull` makes the entire engine pull-based.

## 12. Current implementation caveats

These details should remain visible in course material and design reviews:

1. **Initial source scheduling:** the current `start_query()` seeds `scans.front()`. New external
   source types must decide whether they join the query source index or are scheduled exclusively by
   session/channel events.
2. **Per-operator synchronization:** `run_one_operator()` synchronizes the CUDA stream after each
   operator, limiting asynchronous overlap within one task even though tasks can run concurrently.
3. **Completion edges:** task completion normally triggers status evaluation. Zero-task EOS paths
   require their own wake and evaluation.
4. **Task-accounting race:** input claims and `mark_task_created()` must remain under the same
   pipeline status lock.
5. **Error teardown:** task creation must stop before GPU executors drain because queued creation
   requests contain raw operator pointers owned by the query plan.
6. **Streaming status:** exchange-channel/source/sink behavior described in Modules 10–11 includes
   proposed or feature-branch behavior. It must not be presented as universally live in the pinned
   base snapshot.

## 13. Review checklist

When classifying or modifying an execution path, answer each question at the correct layer:

- Who initiates work: a producer event, a consumer request, or a pipeline driver?
- Who owns the current `operator_data` or `data_batch`?
- Does the edge pass data directly or publish it into a repository/channel?
- Which event causes task creation to run again?
- What prevents over-admission of worker threads, tasks, channel entries, and GPU bytes?
- Which barrier controls consumer readiness?
- Which event re-evaluates completion when zero tasks are in flight?
- Can queued data remain visible to spill/downgrade?
- At what boundary is the result materialized or pulled by an external client?

If those questions have precise answers, the label “push-oriented, event-driven hybrid” is useful.
Without them, it is only shorthand.

## Further reading

- [`docs/super-sirius/architecture-overview.md`](../../docs/super-sirius/architecture-overview.md)
- [`docs/super-sirius/pipeline-execution.md`](../../docs/super-sirius/pipeline-execution.md)
- [`modules/03-plan-to-pipelines.html`](../modules/03-plan-to-pipelines.html)
- [`modules/04-life-of-a-query.html`](../modules/04-life-of-a-query.html)
- [`modules/06-data-plane.html`](../modules/06-data-plane.html)
- [`modules/10-streaming.html`](../modules/10-streaming.html)
- [`modules/11-starrocks-integration.html`](../modules/11-starrocks-integration.html)
- [“Query Engines: Push vs. Pull”](https://justinjaffray.com/query-engines-push-vs.-pull/)
