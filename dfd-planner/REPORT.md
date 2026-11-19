# Using DataFusion Distributed's planner inside distributed Sirius

2026-11-19. Exploration only. Nothing pushed, nothing committed, no engine build touched.

## Summary

1. The `datafusion` branch of `mbrobbel/sirius` does not run a DataFusion physical plan. It sends DataFusion's optimized **logical** plan to Sirius as Substrait; DuckDB's optimizer and Sirius's own plan generator then choose every physical operator. No `RepartitionExec` reaches Sirius, so DFD has nothing to attach a boundary to on that path.
2. DFD's public entry point does work as a library on a DataFusion physical plan made from the same session. A 136-line probe built against DFD **without its gRPC feature** reproduces, byte for byte, all 88 plans DFD's own coordinator produced at SF100 (22 queries × 2, 4, 8, 16 workers), in under 0.1 s per query, with no worker running.
3. The probe reads every boundary back through public API and prints it in `exchange_spec` terms. The three boundary kinds map one to one: shuffle → `hash`, broadcast → `broadcast`, coalesce → `gather`; consumer task count → `width`.
4. What DFD decides is small: a boundary at each hash repartition, CollectLeft build and coalesce that DataFusion already placed, plus a task count from file bytes. `distribute_plan()` applies the same rule to Sirius's plan today. DFD's adaptive part is crate-private and changes task counts only.
5. With DataFusion's defaults DFD shuffles more than Sirius does (Q9: 9 hash + 1 broadcast against Sirius's 5 + 3), because DataFusion broadcasts only below 1 MiB. With that threshold at 1 GiB the counts converge (Q9: 5 + 3) or pass Sirius (Q5: 1 hash + 5 broadcast against 5 + 3).
6. DFD has no counterpart for the Bloom merges (56 of the 161 GB Sirius sent at SF1000, as the sibling session measured), and none for range exchange, delim joins or the deferred probe scan.
7. Making DFD the planner needs a new translator from DataFusion physical nodes to Sirius operators (for TPC-H: 10 DataFusion and 6 DFD node types, 8 join shapes, 4 aggregate modes). That is the cost, and it replaces Sirius's plan passes.
8. Recommendation: do not make DFD the planner. Build the one hook that is useful whoever decides — `distribute_plan()` following a list of directives — feed it DFD's choices from the probe for Q3 and Q12, and measure bytes before any larger work.
9. Versions do not line up: the branch pins DataFusion 53.1.0 / Arrow 58.2.0, DFD 4.0.0 needs DataFusion 55 / Arrow 59.3.0, `sirius-ffi` here uses Arrow 60.1. A planner-only use passes no Arrow data, so this blocks nothing until batches cross.
10. Not verified: anything that needs a GPU or a built engine (the branch was read, not run); the effort estimates; whether the FFI/Substrait path can run distributed at all.

Marks: **[V]** verified in this session, with the file and line or the command. **[I]** inferred. Paths under `sirius-df/` are in the scratch clone `/scratch/prestouser/aocsa/dfd-planner/sirius-df` (checked out at `exp/stage-tasks` `f66eeee62`; branch `mbrobbel-datafusion` = `d5d406c34`). `DFD/` is `/scratch/prestouser/aocsa/dfd-refs/datafusion-distributed` at `9b62f20`.

## 1. What the `datafusion` branch of mbrobbel/sirius does

Fetched read-only into the scratch clone; head `d5d406c34` (2026-07-17), 7 non-merge commits on base `7a7bc4e6b`. [V: `git log`, `git diff --stat 7a7bc4e6b mbrobbel-datafusion`] Everything below was read, nothing was run: the engine half needs a built Sirius and a GPU.

- **Where it lives.** A standalone Rust crate `experimental/datafusion` (`sirius-datafusion`), plus small changes in `rust/crates/sirius` (Arrow 58/59 feature switch), `src/sirius_ffi.cpp`, `src/expression/function_id.cpp` and the `substrait` submodule pointer. [V: diff stat]
- **How it plans.** `prepare_query()` runs `ctx.sql(sql)`, `state.optimize(&plan)` (logical optimizer only), then `datafusion_substrait::logical_plan::producer::to_substrait_plan`, and rewrites each `NamedTable` read into a Parquet `LocalFiles` read. [V: `experimental/datafusion/src/plan.rs:24-33`, `:171-205`] It never calls `create_physical_plan`.
- **How it runs.** The bytes go to `SiriusContext::execute_substrait` on a dedicated thread [V: `src/engine.rs:88-92`, `rust/crates/sirius/src/lib.rs:79`]. In C++ that is `duckdb::SubstraitToDuckDB` → DuckDB `Relation` → DuckDB `Planner` → DuckDB `Optimizer` → Sirius's plan generator. [V: branch `src/sirius_ffi.cpp:131-161`; same shape in this tree, `sirius-df/src/sirius_ffi.cpp:365-402`] The FFI path disables five DuckDB optimizers (IN_CLAUSE, COMPRESSED_MATERIALIZATION, STATISTICS_PROPAGATION, COLUMN_LIFETIME, LATE_MATERIALIZATION); **join ordering stays on**. [V: branch `src/sirius_ffi.cpp:101-109`]
- **Mapping of DataFusion physical nodes to `sirius_physical_operator`: there is none.** No file in the branch names `ExecutionPlan`, `RepartitionExec`, `HashJoinExec` or `AggregateExec`. [V: read of all five source files of the crate] So for `RepartitionExec`, `CoalescePartitionsExec`, `SortPreservingMergeExec`, the `HashJoinExec` partition modes and partial/final `AggregateExec` the answer is the same: they are never created. Join sides, join order, the partial/merge aggregate split and partitioning are chosen by DuckDB's optimizer and `sirius_physical_plan_generator`. The `--partitions` flag of `sirius-dfbench` sets DataFusion's `target_partitions` [V: `src/bin/sirius-dfbench.rs:79-83`], which a logical plan does not use. [I]
- **Substrait relations Sirius reads.** Read, Filter, Fetch, Aggregate, Sort, Join, Project, Set, Cross, Window, Write, Reference. `ExchangeRel`, `HashJoinRel`, `MergeJoinRel` and `NestedLoopJoinRel` throw "Unsupported relation type". [V: `/scratch/prestouser/aocsa/wt/pr-late-dim/substrait/src/from_substrait.cpp:1276-1307`] `datafusion-substrait` 55's physical producer handles `DataSourceExec` only. [V: `datafusion-substrait-55.0.0/src/physical_plan/producer.rs:54,130`] So Substrait cannot carry a DataFusion physical plan to Sirius today, from either end.
- **Which TPC-H queries.** `plan.md` names Q1 and Q6 at SF10 then SF100 as the target, and the README's harness example runs `q1,q6`. [V: `plan.md:122`, `README.md:73`] The later commits add Substrait arithmetic names (`add`, `subtract`, `multiply`, `divide`, `modulus`), datetime and unicode features and a LIKE reader update [V: `src/expression/function_id.cpp` diff, `Cargo.toml:29-34`], which points at more queries [I]. The branch records no list of passing queries and no results; the query files are in an uninitialised submodule (`experimental/datafusion/libcudf-rs`). Not verified which queries pass.

## 2. Can `create_distributed_plan` run on what the branch produces?

**Not on the branch's output; yes on a physical plan from the same `SessionContext`.**

- The entry point takes a physical plan: `create_distributed_plan(Arc<dyn ExecutionPlan>, &dyn Session)`. [V: `DFD/src/distributed_planner/distributed_query_planner.rs:84-87`] The branch stops at a logical plan, so one more call (`DefaultPhysicalPlanner::create_physical_plan`) is needed before DFD, and what comes back is a DataFusion physical plan that Sirius cannot read (section 1).
- **Prototype.** `/scratch/prestouser/aocsa/dfd-planner/dfd-probe` (copy of the source: `probe-main.rs`, `probe-Cargo.toml` beside this file). It registers the TPC-H Parquet tables, makes DataFusion's physical plan, calls `create_distributed_plan`, and walks the result with `NetworkBoundaryExt::as_network_boundary`, `NetworkBoundary::input_stage`, `Stage::{num, task_count, local_plan}`. [V: `probe-main.rs:36-45,97-120`] What it proves:
  - DFD compiles and plans with `default-features = false, features = ["parquet","sql"]`: `arrow-flight` is not in the dependency tree. [V: `cargo tree -i arrow-flight` → "did not match any packages"] `tonic` is still linked as a dependency of `tonic-prost`. [V: `cargo tree -i tonic`]
  - The planner needs a `WorkerResolver` only to count workers; the probe gives it `sirius://rank/N` URLs. [V: `probe-main.rs:23-30`]
  - It needs `with_distributed_planner()` on the session builder, because that is the only public way to register DFD's built-in file-scan task-count handlers. [V: `DFD/src/distributed_planner/session_state_builder_ext.rs:23-52`]
  - Its plans equal the stored ones: for 22 queries × {2, 4, 8, 16} workers at SF100, the probe's `display_plan_ascii` output is byte-identical to `dfd-refs/runs/sf100-Np/plans/qNN.txt`, 88 of 88. [V: `cmp` loop this session; outputs in `/scratch/prestouser/aocsa/dfd-planner/out/sf100-*r/`] Planning one query takes 0.09 s wall. [V: `time` on Q3]
  - Output for Q3 on 4 ranks [V]:
    ```
    q03.sql: ranks=4 target_partitions=36 boundaries=5
      stage  5 -> stage  0  exchange_spec{routing=gather target=0}  senders=4 below=SortExec(TopK)
      stage  3 -> stage  5  exchange_spec{routing=hash key_columns=[0:o_orderkey] buckets=4 salted width=4}  senders=4 below=HashJoinExec
      stage  1 -> stage  3  exchange_spec{routing=hash key_columns=[0:c_custkey] buckets=4 salted width=4}  senders=2 below=FilterExec
      stage  2 -> stage  3  exchange_spec{routing=hash key_columns=[1:o_custkey] buckets=4 salted width=4}  senders=4 below=FilterExec
      stage  4 -> stage  5  exchange_spec{routing=hash key_columns=[0:l_orderkey] buckets=4 salted width=4}  senders=4 below=FilterExec
    ```
    All 22 queries on 4 ranks: `probe-sf100-4r.txt` beside this file.
- **Versions.** [V: the three manifests and `Cargo.lock`]

  | piece | DataFusion | Arrow |
  |---|---|---|
  | `mbrobbel/sirius` `experimental/datafusion` | =53.1.0 | =58.2.0 (`sirius` crate feature `arrow-58`) |
  | DFD 4.0.0 at `9b62f20`, and the newer clone `7fed65b` | 55 (lock: 55.0.0) | 59 (lock: 59.3.0) |
  | `sirius-df/rust/crates/sirius-ffi` | none | arrow-array 60.1 |

  `datafusion-substrait` 55.0.0 exists on crates.io [V: `cargo info`], so the branch's planning half can move to 55; whether `plan.rs` compiles unchanged on 55 was not tried. Different Arrow majors cannot share a `RecordBatch` type, but the planner-only use passes plan descriptions, not batches, and the Arrow C Data Interface the `sirius` crate already uses is stable across majors. [I]

## 3. DFD's boundaries against Sirius's exchange

| DFD | Sirius | notes |
|---|---|---|
| `NetworkShuffleExec` over a hash `RepartitionExec` | `exchange_spec{routing=hash, key_columns, key_cast_types}` below a `PARTITION` | Same shape: Sirius's `PARTITION` above the exchange is DFD's consumer-side local `RepartitionExec`. [V: `sirius-df/src/op/sirius_physical_exchange_sink.hpp:38-56`, `distributed_plan.cpp:589-602`; `DFD/src/execution_plans/network_shuffle.rs:227-240`] |
| the salt and bucket count of a two-step shuffle | `hash_seed` | Each engine hashes its own way; only agreement among ranks matters. [I] |
| `NetworkBroadcastExec` | `routing=broadcast` | Same. The size rule differs: DataFusion's CollectLeft threshold, 1 MiB or 131072 rows [V: `datafusion-common-55.0.0/src/config.rs:1692,1696`], against Sirius's build ≤ 1 GiB and ≤ probe / (ranks − 1) [V: `distributed_plan.cpp:105,693-695`]. |
| `NetworkCoalesceExec` | `routing=gather, target=0` | Sirius uses it under `MERGE_AGGREGATE`, `MERGE_TOP_N` and at the root. [V: `distributed_plan.cpp:838-853,967-971`] |
| consumer stage task count | `width` (hash only): ranks 0..width−1 receive | [V: `sirius_physical_exchange_sink.hpp:68-70`] With DFD's defaults a consumer stage is as wide as its widest input, so `width` is the world size everywhere; with `cardinality_task_count_factor=2.0` the probe shows `width=2` on Q3. [V: probe run] |
| producer stage task count (`senders=1`, `2`) | none | Every rank reads its byte share of every table and every rank sends. [V: `distributed_plan.cpp:545-550,993-1068`] In the 22 SF100 plans a count below the world size appears only on producer stages of small tables. [V: `grep` over `runs/sf100-4p/plans`] |

No counterpart in DFD:
- **Range exchange for a distributed sort.** DFD refuses range partitioning [V: `DFD/src/execution_plans/common.rs:47-49`]; its ORDER BY is a per-task sort, a coalesce and a merge on the coordinator. [V: every stored plan head]
- **Bloom all-gather merge** (`src/op/distributed/bloom_all_gather_merge.*`, installed at `distributed_plan.cpp:623-655`). DFD has remote dynamic filters (`distributed.remote_dynamic_filters`, default true [V: `DFD/src/distributed_planner/distributed_config.rs:38`]), but they travel in its coordinator/worker protocol, which a library caller does not run. [I]
- **Delim joins** (`distributed_plan.cpp:727-763`). DataFusion decorrelates into ordinary joins, so a DataFusion plan has none; the rule would be unused. [I]
- **Deferred probe scan** (`:268-303`), **complete-group HAVING** (`:307-383`), **NULL keys to every rank for MARK joins** (`sirius_physical_exchange_sink.hpp:57-62`), **`DENSE_COUNT_JOIN`**, **rank-0 placement of `GPU_VALUES`**, **the plan-shape all-gather** (`:901-948`). [V: lines read]

No counterpart in Sirius at `exp/stage-tasks`:
- **Group-by reuse of the join's partitioning.** DataFusion plans `AggregateExec: mode=SinglePartitioned` above a join on the same key, so DFD adds no exchange (Q3 stage 5, Q18 stage 6). [V: stored plans] This tree always exchanges below `MERGE_GROUP_BY` unless the input is narrow (`distributed_plan.cpp:817-836`); Q3's stream `s5` and Q18's `s8` are those exchanges. [V: `engine.log` labels] The co-partition rules of PR #27/#28 are not in this tree. [V: no such rule in the file]
- Per-stage sender counts, `partial_reduce`, the union isolator, the sampler. [V: `DFD/src/lib.rs` exports and `mod.rs` visibility]

## 4. Hooks distributed Sirius would need

| hook | where it belongs | what it does |
|---|---|---|
| Read the DFD plan back | new crate `rust/crates/sirius-dfd` (`src/boundaries.rs`), built without `grpc` | The probe's `walk()`: `as_network_boundary()`, `input_stage()`, `Stage::task_count()`, `Stage::local_plan()`; emits a stage plan. Proven by the probe. |
| Follow the stage plan instead of deciding | `src/planner/distributed_plan.cpp`, policy struct in `distributed_plan.hpp` beside `stage_policy`, filled at `sirius_physical_plan_generator.cpp:2391` | At each site where `visit()` decides today (join `:693-724`, group-by `:835`, sort `:864`), take the directive: hash, broadcast, none, gather, width. Uses the existing `insert()` (`:527-584`), so standalone operators do not change. |
| Same plan on every rank | `distributed_plan.cpp:901-948` stays | Two ways: every rank runs the planner and the shape check catches a difference (DataFusion planning is a pure function of SQL, statistics and settings [I]); or rank 0 plans and sends the stage plan on an all-gather stream before `distribute_plan()` (`src/exchange/all_gather.*`). DFD's own way, one coordinator that ships plans, needs its worker layer. |
| Bypass Arrow Flight and gRPC | no code: never call `DistributedExec::execute` | The `Network*Exec` nodes are read, not run; Sirius's `EXCHANGE_SINK`/`EXCHANGE_SOURCE` carry the rows. Building DFD without `grpc` removes `arrow-flight`. [V: probe] |
| Read a DataFusion physical plan (only for the full path) | new `src/planner/datafusion_plan_reader.*` or a wider Substrait reader | The large piece; section 6. |

DFD handlers:
- `WorkerResolver` — **applies, required.** One URL per rank; the planner counts them. [V: probe; `DFD/src/worker_resolver.rs:9-21`]
- `DesiredTaskCountHandler` — **applies.** The way to tell DFD how wide a leaf is from Sirius's knowledge (footer rows, the rank count), or to pin a stage to one task. [V: `DFD/src/events/desired_task_count.rs:135-219`]
- `ScaleUpLeafNodeHandler` — **applies as a guard.** DFD's built-in one rewrites a scan into `DistributedLeafExec` with per-task file groups; Sirius assigns scan shares itself (`byte_ranges_for_rank`), so its handler returns the leaf unchanged or uses the variants to drive `assign_files_to_rank`. [V: `DFD/src/events/scale_up_leaf_node.rs:7-49`; `distributed_plan.cpp:1017-1068`]
- `RouteTaskHandler` — **does not apply.** It runs when the coordinator dials a worker at execution. [V: `DFD/src/events/route_tasks.rs:12-34`] In Sirius task k is rank k.
- `WorkerPlanRewriteHandler` — **does not apply.** It runs inside a DFD `Worker` when a stage plan arrives. [V: `DFD/src/events/worker_plan_rewrite.rs:8-39`]

## 5. DFD's boundaries against today's exchanges

DFD: SF100, 4 workers, `target_partitions` 36, defaults (stored plans = probe output). Sirius: SF1000, 4 ranks, `exp/stage-tasks` with both stage rules off (`sf1k-off/engine.log`, `exchange_stage_bytes: 0`, `exchange_gather_sort_rows: 0`). The scale factors differ, so compare placement, not bytes.

| query | DFD shuffle / broadcast / coalesce | DFD with 1 GiB broadcast threshold | Sirius hash / broadcast / gather / range / Bloom merge | Sirius GB sent: hash, broadcast, Bloom |
|---|---|---|---|---|
| Q3 | 4 / 0 / 1 | 1 / 2 | 3 / 1 / 1 / 0 / 1 | 2.95, 0.36, 6.44 |
| Q5 | 7 / 2 / 1 | 1 / 5 | 5 / 3 / 1 / 1 / 2 | 5.53, 0.24, 2.42 |
| Q9 | 9 / 1 / 1 | 5 / 3 | 5 / 3 / 1 / 1 / 2 | 32.75, 0.58, 3.22 |
| Q12 | 3 / 0 / 1 | not run | 3 / 0 / 1 / 1 / 1 | 0.88, 0, 0.80 |
| Q18 | 5 / 0 / 1 | not run | 5 / 0 / 1 / 0 / 1 | 2.97, 0, 0.00 |
| Q21 | 7 / 1 / 1 | 5 / 3 | 4 / 3 / 1 / 0 / 1 | 6.33, 1.71, 25.77 |

[V: `grep -c` over the stored plans; probe runs with `--set datafusion.optimizer.hash_join_single_partition_threshold=1073741824 --set ..._rows=100000000`; stream labels and GB from `engine.log`, summed per kind from `joinsizes.py` output]

What the six show:
- **Q12** is the same plan in both: hash both join inputs on the order key, hash the partial aggregate on `l_shipmode`, gather. Sirius adds a range exchange for the sort (0 GB) and a Bloom merge.
- **Q3.** Same join tree. DFD shuffles `customer` and `orders` on the customer key; Sirius broadcasts the filtered `customer` (0.36 GB) and leaves `orders` in place. DFD needs no exchange for the group-by; Sirius sends 0.20 GB for it.
- **Q9.** DataFusion joins `part`, then `supplier`, then `partsupp`, then `orders`, and with its defaults reshuffles the `lineitem` side four times (stages 6, 7, 8, 9). DuckDB's order in Sirius hashes `lineitem` once (9.8 GB) and broadcasts `part` and `supplier`. The join orders differ, so DFD's boundaries cannot be laid over Sirius's Q9 plan as it is.
- **Q5, Q21.** Same pattern: more shuffles with DataFusion's 1 MiB rule, more broadcasts than Sirius at 1 GiB.
- **Q18.** Same count; DFD keeps the final group-by in the join's stage.
- **Q21's 25.8 GB and Q3's 6.4 GB of Bloom merges have no DFD boundary.** Any DFD-planned run still needs Sirius's merge, or loses the filter.

Every stage in every stored plan that consumes a boundary has 4 tasks; narrower stages are producers over small tables only. [V] So at defaults DFD's task counts change who sends, not how many ranks receive, and they move no fewer bytes. [I]

## 6. Recommendation

**Do not make DFD the planner of distributed Sirius.** Reasons, in order of weight:

1. The premise does not hold yet: there is no translator from a DataFusion physical plan to Sirius operators, in the branch or anywhere. [V, section 1] Building one means DataFusion replaces DuckDB's optimizer and `sirius_physical_plan_generator` as the physical planner, and the Sirius passes that depend on the DuckDB plan (late materialization, delim joins, mark-join conversion, dynamic-filter planning, the scan-elimination rewrites) are lost or redone. For TPC-H the translator must read 16 node types (10 of DataFusion, 6 of DFD), 8 hash-join mode/type pairs and 4 aggregate modes, plus `PhysicalExpr`. [V: counts over the 22 stored plans] Estimate: 2 to 3 months for 22 correct queries. [I]
2. What DFD adds on top of DataFusion's plan is a mechanical rule that `distribute_plan()` already applies to Sirius's plan, plus a file-bytes task count. [V: sections 2, 3] Its adaptive execution is crate-private (`prepare_dynamic_plan` is `pub(super)`, `inject_network_boundaries`, `NetworkBoundaryBuilder`, `calculate_cost` are `pub(crate)` [V: grep]) and changes task counts only [V: `DFD/docs/source/advanced/08-adaptive-query-execution.md:9-11`].
3. Its choices are not better for the network as measured: more shuffles at defaults, and no answer for the Bloom merges. [V: section 5]

**What is worth building: the directive hook, then a measurement.**

- *Slice (one query, one boundary kind):* Q12 on 4 ranks, hash boundaries. `distribute_plan()` takes a list of directives, one per site in visit order, and inserts the exchange the directive names instead of deciding. For Q12 the directives are DFD's three hash boundaries; the result must be the same plan shape and the same answer as today. That proves the hook with no behaviour change.
- *Then Q3:* the same hook with DFD's "no exchange under the group-by" directive. It removes stream `s5` (0.20 GB of 2.95 GB hashed at SF1000). This is the one DFD choice in the six queries that moves fewer bytes on an unchanged join tree. [I]
- *Cost:* the hook is a policy struct and three lookups in one file, about a week with tests. [I] Feeding it from the probe by hand needs no Rust in the engine. Wiring DFD in (crate `sirius-dfd`, a DataFusion front end on DataFusion 55, join-order pinning so the trees match) is 3 to 4 more weeks and is only worth starting if the hand-fed runs save bytes. [I]
- *Gains against the current planner:* decisions become data that any source can supply (DFD, a file, a cost model); DFD's stage and task vocabulary for reports; co-partitioned group-by.
- *Gives up:* nothing in the slice. On the full path: DuckDB's join order (better than DataFusion's on Q9 here), Sirius's broadcast rule, and every rule listed under "no counterpart in DFD".

Open, not verified: whether the FFI/Substrait path can run with `sirius_exchange_peers` set (the distributed runs so far use the DuckDB CLI); whether `plan.rs` of the branch compiles on DataFusion 55; any timing.

## Files

- `REPORT.md` — this file. `probe-main.rs`, `probe-Cargo.toml` — the probe's source. `probe-sf100-4r.txt` — its output for 22 queries on 4 ranks.
- `/scratch/prestouser/aocsa/dfd-planner/dfd-probe` — the crate; build with `PATH=$HOME/.cargo/bin:$PATH CARGO_HOME=/scratch/prestouser/aocsa/dfd-planner/cargo-home CARGO_TARGET_DIR=/scratch/prestouser/aocsa/dfd-planner/target cargo build --release`; run `target/release/dfd-probe /scratch/sirius/datasets/tpch_sf100 /scratch/prestouser/aocsa/dfd-refs/queries/q03.sql 4 36 [--plan] [--set key=value]`.
- `/scratch/prestouser/aocsa/dfd-planner/out/` — probe outputs; `sirius-sf1k-off-streams.txt` — the Sirius stream summary.
