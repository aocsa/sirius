# Distributed TPC-H: what runs today, and the plan for the other queries

First measured on branch `exchange-operator-v2` (head `0006f65a`), 2 ranks on 2 RTX PRO 4500 GPUs of one
host. Every query ran in its own process, 1 GPU and 2 ranks, and its answer was compared to
DuckDB's CPU answer at SF1 (numbers within 1e-9 relative). At SF1000 the 2-rank answer was compared
to the 1-GPU answer.

## Status: all 22 correct on 2 ranks

F1-F8 are on branch `exchange-operator-v3`, with two fixes cherry-picked from
`exchange-operator-v2` (`d325fb6e`, `1dc8fc35`) and one more found at SF1000 (below).

- **SF1:** all 22 match DuckDB's CPU answers on 2 ranks, and every 1-GPU run is still correct.
- **SF1000:** all 22 on 2 ranks match the 1-GPU answers.
- **No Bloom merge timed out** at either scale.
- **Found at SF1000:** a rank whose stream brought nothing handed nothing on, while local
  producers always hand on at least an empty batch.
  - Q15 hung 600 s: its one result row hashed to rank 0, so rank 1's last join got no probe
    batch and never released its build.
  - Q11 failed: no rank had rows, so the range exchange had no boundaries.
  - Fixed: an exchange source with senders now hands on one empty batch, and the sink skips
    batches with no rows.

SF1000, one run per process (the first, so it includes reading the data). Not a benchmark.
Totals: **1 GPU 138.8 s, 2 GPUs 104.5 s**.

| Query | 1 GPU | 2 GPUs | | Query | 1 GPU | 2 GPUs |
|---|---|---|---|---|---|---|
| Q1 | 7.6 s | 6.3 s | | Q12 | 4.8 s | 4.0 s |
| Q2 | 1.0 s | 2.1 s | | Q13 | 8.6 s | 5.0 s |
| Q3 | 26.1 s | 3.6 s | | Q14 | 4.5 s | 3.2 s |
| Q4 | 4.2 s | 3.8 s | | Q15 | 3.1 s | 3.2 s |
| Q5 | 4.5 s | 3.6 s | | Q16 | 1.4 s | 2.4 s |
| Q6 | 3.0 s | 2.8 s | | Q17 | 3.9 s | 3.9 s |
| Q7 | 5.5 s | 4.5 s | | Q18 | 13.0 s | 5.7 s |
| Q8 | 5.5 s | 4.1 s | | Q19 | 6.6 s | 4.8 s |
| Q9 | 10.6 s | 8.4 s | | Q20 | 3.6 s | 3.5 s |
| Q10 | 8.8 s | 7.5 s | | Q21 | 10.1 s | 18.0 s |
| Q11 | 1.1 s | 1.9 s | | Q22 | 1.4 s | 2.0 s |

- **Q21 is slower on 2 GPUs.** It runs the CTE plan from F3; the 1-GPU run uses the delim-join
  plan. F9 is the fix.
- **Q2, Q6, Q11, Q16 and Q22 are slower on 2 GPUs.** They take 1-3 s, and the exchange setup
  and extra pipelines cost more than the work they split.
- **Q3 takes 26 s on 1 GPU in both runs, against 3.6 s on 2.** This is not yet explained.

## Before the fixes

| Status | Queries | Count |
|---|---|---|
| Correct on 2 GPUs | Q1, Q5, Q7, Q8, Q9, Q12 | 6 |
| Correct with `SET delim_join_as_cte = true` (no code change) | Q4 | 1 |
| Small fixes (F1-F3) | Q3, Q6, Q10, Q14, Q18, Q19, Q21 | 7 |
| Medium fixes (F5-F8) | Q2, Q11, Q13, Q15, Q16, Q17, Q20, Q22 | 8 |

- Q7 is correct, but at SF1 it once took 241 s instead of ~2 s: two Bloom merges waited out their
  120 s timeouts (F4). At SF1000 it took 4.6 s.
- Every 1-GPU run was correct, so all the failures below are in distributed planning or execution.
- Each failing query was only checked up to its **first** refused operator. The full operator list
  (from the 1-GPU plans) says which fixes it needs, but a query can still fail later in ways the
  first error hides. The sweep below re-checks each one as fixes land.

## Results

SF1, 2 ranks, before any fix:

| Query | 2 GPUs | First blocker | Needs |
|---|---|---|---|
| Q1 | ok | | |
| Q2 | refused | MERGE_TOP_N | F2, F3, F8 |
| Q3 | refused | MERGE_TOP_N | F1, F2 |
| Q4 | refused | LEFT_DELIM_JOIN | F3 (verified: ok) |
| Q5 | ok | | |
| Q6 | **wrong**: 2 rows, not 1 | MERGE_AGGREGATE runs per batch | F1 |
| Q7 | ok, 241 s | Bloom merge waits | F4 (performance only) |
| Q8 | ok | | |
| Q9 | ok | | |
| Q10 | refused | MERGE_TOP_N | F1, F2 |
| Q11 | refused | NESTED_LOOP_JOIN | F1, F7 |
| Q12 | ok | | |
| Q13 | refused | DENSE_COUNT_JOIN | F5 |
| Q14 | **wrong**: 2 rows, not 1 | MERGE_AGGREGATE runs per batch | F1 |
| Q15 | refused | CTE | F1, F7, F8 |
| Q16 | refused | COUNT(DISTINCT) partials | F6 |
| Q17 | refused | RIGHT_DELIM_JOIN | F1, F3, F8 |
| Q18 | refused | MERGE_TOP_N | F1, F2 |
| Q19 | **wrong**: 2 rows, not 1 | MERGE_AGGREGATE runs per batch | F1 |
| Q20 | refused | RIGHT_DELIM_JOIN | F3, F8 |
| Q21 | refused | MERGE_TOP_N, then LEFT_DELIM_JOIN | F1, F2, F3 |
| Q22 | refused | LEFT_DELIM_JOIN | F1, F3, F7, F8 |

The wrong answers are the dangerous ones: Q6, Q14 and Q19 return one partial result per rank with
no error.

SF1000, the queries that already run (first run in each process; not a benchmark):

| Query | 1 GPU | 2 GPUs | Answer |
|---|---|---|---|
| Q1 | 7.8 s | 5.6 s | same as 1 GPU |
| Q7 | 5.6 s | 4.6 s | same as 1 GPU |
| Q8 | 6.6 s | 4.1 s | same as 1 GPU |
| Q9 | 10.8 s | 7.6 s | same as 1 GPU |

Q5 and Q12 were verified at SF1000 earlier (see the dist-sirius-scripts notes).

## Fixes, cheapest first

### F1. A merge must wait for its whole input after an exchange (small) -> Q6, Q14, Q19

- **Bug:** `MERGE_AGGREGATE` and `MERGE_TOP_N` rely on their child being a sink, which ends the
  upstream pipeline. On one GPU the child is `UNGROUPED_AGGREGATE` / `TOP_N`. The rewriter puts an
  `EXCHANGE_SOURCE` there, which is not a sink, so the merge is pipelined behind it
  (`Pipeline #2: EXCHANGE_SOURCE -> MERGE_AGGREGATE`) and merges whatever has landed each time
  a task runs.
- **Evidence:** Q6 on 2 ranks printed 40,897,074.43 and 82,244,003.80. Their sum,
  123,141,078.23, is the right answer.
- **Fix:** give the merge a full-input barrier when its child is an `EXCHANGE_SOURCE`: either the
  merge's `build_pipelines` starts a new pipeline, or the exchange source ends one as `PARTITION`
  does. Check `MERGE_TOP_N`'s `fuse_into_parent` path the same way.
- **Test:** an operator test with 2 senders and one batch each, plus Q6 on 2 ranks.

### F2. Distributed TOP_N (small) -> Q3, Q10, Q18; Q2 and Q21 with F3/F8

- `MERGE_TOP_N -> TOP_N` has the same shape as `MERGE_AGGREGATE -> UNGROUPED_AGGREGATE`: each rank
  keeps its top N, those are gathered to rank 0, and rank 0's merge picks the final N.
- **Fix:** a `MERGE_TOP_N` case in `distributed_plan.cpp` that gathers below the merge, like the
  `MERGE_AGGREGATE` case. Needs F1.
- At most N rows per rank cross the network, so the gather costs nothing.

### F3. Plan correlated subqueries as CTEs in distributed mode (no code / small) -> Q4; Q21 with F2

- Sirius forces `delim_join_as_cte = false` (`default_to_explicit_delim_joins` in
  `src/sirius_extension.cpp`), so correlated subqueries reach the GPU as `LEFT/RIGHT_DELIM_JOIN`,
  which distributed mode refuses.
- DuckDB v2's default (`true`) decorrelates them into ordinary joins, or into a materialized CTE.
  With `SET delim_join_as_cte = true`:
  - Q4 ran correctly on 2 ranks (a RIGHT_SEMI join, already supported).
  - Q21 then needs only F2.
  - Q2, Q17, Q20 and Q22 become CTE plans (F8).
- **Fix:** set it in distributed sessions, in `run_ranks.sh` first, then in Sirius when exchange
  peers are configured.
- **Cost:** the comment there measured the CTE shapes 1.2-3.5x slower on one GPU at SF1000 for
  Q20, Q21 and Q22. Coverage first; F9 recovers the speed.

### F4. A rank with no build filter owed its share of the merge until too late (done)

- **What happened at SF1:** rank 0 waited 120 s in the all-gather for one join's filter, a 2-row
  build (`filters not merged across ranks ... 1 of 2 ranks in 120000 ms`). Q7 did this twice, 241 s
  in total; Q21 once. The answers stayed correct, because a timed-out merge drops the filter.
- **Stack dumps of both ranks during the stall:**
  - Rank 0: one worker in `all_gather` <- `merge_across_ranks` <- `observe_whole_build` <-
    `CONCAT::sink`, delivering its build.
  - Rank 1: every worker and the task creator idle.
  - It was not two merges waiting on each other.
- **Cause, a wait cycle:**
  - Both nation rows hashed to rank 0, so rank 1's build was empty and no batch reached its join.
  - Such a rank contributes only from `finish_input()`, which a hash join called from
    `on_finalize_operator`, after its probe side.
  - Rank 1's probe needs rank 0's probe rows. Rank 0 does not start its probe side until its build
    pipeline ends, which waits on that merge.
- **Fix:**
  - When a distributed hash join's build CONCAT finishes, it closes the join's publication
    (`finish_input`), before the probe side starts.
  - A rank without a filter now takes part in each merge with `leave_all_gather`: it sends its end
    and does not wait, since it ignores the merged filters anyway.

### F5. DENSE_COUNT_JOIN (small-medium) -> Q13

- Q13's `LEFT JOIN ... count(o_orderkey)` is planned as a fused count join over partitioned inputs
  (`sirius_physical_dense_count_join`, a partition consumer like `HASH_JOIN`).
- **Fix:** a rewriter case that hash-exchanges both inputs on the join key, as for `HASH_JOIN`.
  Each rank then counts only its own customers.
- **Check:** customers with zero orders must come out exactly once, on the rank that owns them,
  not from a key-range ("dense") pass on every rank.

### F6. COUNT(DISTINCT) (small-medium) -> Q16

- Partial results of `COUNT(DISTINCT)` cannot be merged by summing. **Fix:** for an aggregate with
  `has_count_distinct`, hash-exchange the raw rows on the group keys *below* the aggregate, then
  run the whole aggregate locally. No partial merge is needed.
- Q16's `NOT IN` is a MARK join. It is correct here because `s_suppkey` is never NULL; the general
  NULL case is still open (see "Not covered").

### F7. Scalar subquery results feeding a join (medium) -> Q11, Q15, Q22

- Q11 and Q22 compare against a scalar (`> (select sum(...) * 0.0001)`, `> (select avg(...))`)
  through `NESTED_LOOP_JOIN`. Q15 joins on `= (select max(...))`.
- The scalar's `MERGE_AGGREGATE` lands on rank 0, and the rewriter refuses "a join over a
  gathered input".
- **Fix:**
  - Hash join: let `hash_below` exchange a rank-0 input too. Rank 0 sends its rows by key and the
    other ranks send nothing.
  - Nested-loop join: broadcast the small side to every rank. `exchange_spec::mode::broadcast`
    already exists in the sink; the rewriter needs a "replicated" placement and a
    `NESTED_LOOP_JOIN` case for spread × replicated.

### F8. Materialized CTEs (medium) -> Q15, Q17, Q20; Q2 with F2; Q22 with F7

- Q15 is a CTE in its text; with F3, Q2, Q17, Q20 and Q22 become CTE plans too.
- **Fix:** a `CTE` case. Each rank materializes its share (the materialization's placement, spread
  for TPC-H), and the CTE scans under the consumer side return that placement.
- The joins over them already exchange by key, so no new exchange kind is needed. Exchanges
  inside the materialization get stream ids like any other subtree.

### F9. Later, for speed: distributed delim joins (large)

Only needed to get back the faster single-GPU delim-join plans for Q20-Q22. One option is to
hash-exchange the outer side on the correlated columns, so each rank's DISTINCT and inner side are
local to its keys. Another is to broadcast the DISTINCT set to every rank. Not needed for coverage.

## Order of work

1. **F1 + F2 + F3: 7 -> 14 queries.**
   - Small and independent of each other, except that F2 needs F1.
   - F1 also removes the only silent wrong answers.
2. **F5, F6: Q13 and Q16, 16 queries.** Each is a self-contained rewriter case.
3. **F7 + F8: the last 6, 22 queries.** F8 first, since Q17 and Q20 need only F8 (plus F1).
4. **F4 (done).** It didn't block correctness, but it had to be fixed before any timing is
   published.
5. **Scale-up.** Rerun all 22 at SF1000 on 2 GPUs, then SF3000 with spilling, using the
   dist-sirius-scripts benchmark.

Each step ends with the sweep at SF1 against DuckDB's CPU answers, and at SF1000 against the 1-GPU
answers.

## Not covered by TPC-H, still open

- `NOT IN` with NULL build keys (MARK joins are hash-exchanged like inner joins).
- S3 / DuckDB-native table scans are not split across ranks, but the plan treats them as split.
- An OOM before a Bloom merge makes the peers wait out the 120 s timeout.

## Reproducing

The sweep lives in `/home/ubuntu/sirius-notes/exchange-q12/tpch22/` (not in this repo):

```bash
./sweep.sh 1                                              # all 22 at SF1, 1 GPU and 2 ranks
PRE_SQL="SET delim_join_as_cte = true;" ./sweep.sh 1 q04  # with session settings
GPU_COUNTS=2 ./sweep.sh 1000 q01 q07                      # one SF, chosen queries
```

- It writes `runs/sf<SF>-<time>/results.csv` with the first error per run. Results are `ok`,
  `ORDER` (same rows in a different order), `WRONG(<n> rows vs <m>)`, `ERROR` or `MISSING`.
- Expected answers are computed once by the same DuckDB shell without Sirius, in `expected/`.
