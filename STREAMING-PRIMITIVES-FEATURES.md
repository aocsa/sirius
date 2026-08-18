# Streaming primitives: what we built, and how the PR stack is organised

A guide for the people who will review this work. It covers the nine features in Tracks 1 and 2, how
each one works, what it unlocks downstream, and the exact commits and files behind it.

**Every statement here comes from [`STREAMING-PRIMITIVES-HANDOFF.md`](STREAMING-PRIMITIVES-HANDOFF.md).**
An interactive version with diagrams is in
[`STREAMING-PRIMITIVES-FEATURES.html`](STREAMING-PRIMITIVES-FEATURES.html) — open it in a browser.

**Scope:** PRs 1–34 (Tracks 1 and 2). Tracks 3 and 4 (benchmark, docs) exist but are not covered here.

---

## Read this first

Two repositories are involved and **they share no git objects**.

| | |
|---|---|
| Source | `demo-multi-cn` @ `790612fb` — 100 commits, 0 merges |
| Target | `aocsa/sirius`, branch `stream/15-fragment` @ `d1c0dc6d` |

Because there are no shared objects, **cherry-pick by hash is impossible**. Everything moves as patches.

The decision is already made: **we work on top of the target, not the other way round.** The target is
further along on the streaming series and carries review hardening the source lacks — a producer-error
plane on `batch_stream`, `stream_session::fail_output`, and 17 test cases in `test_batch_stream.cpp`.

Three facts that shape everything below:

1. **21 of the 100 source commits are already in the target.** 16 are dropped completely; 5 are re-landed
   as *Rust hunks only*. They were absorbed into `3bea68de`, `641b77eb` and `d1c0dc6d`.
2. **The target has the C++ `Fragment` FFI but no Rust binding for it.** Verified:
   `git grep 'Fragment|declare_output|declare_input'` over `rust/crates/sirius*` at `stream/15-fragment`
   returns nothing. Every CN commit calls that binding, so PR 1 comes first.
3. **The base is `stream/15-fragment`**, which is `origin/dev` (`f107ba88`) plus exactly three commits.
   The `stream/02` … `stream/12` branches and `demo-streaming-integration` are a superseded 2026-08-03
   generation — checked with `git merge-base --is-ancestor` against `origin/dev`; all 16 `stream/*` refs
   report unmerged.

---

## Where the features live

```
  ┌──────────────────────────────────────────────────────────────┐
  │  Plan translator                starrocks-plan-translator    │  T2 · PRs 26-34
  ├──────────────────────────────────────────────────────────────┤
  │  StarRocks compute node         experimental/starrocks/src   │  T2 · PRs 17-25
  ├══════════════════════════════════════════════════════════════┤
  ║  Rust FFI binding      [GAP]    rust/crates/sirius           ║  T1 · PRs 1-2
  ├══════════════════════════════════════════════════════════════┤
  │  Engine primitives (C++)        src/exec · src/op · planner  │  T1 · PRs 3-16
  ├──────────────────────────────────────────────────────────────┤
  │  Already in target              3bea68de 641b77eb d1c0dc6d   │  dropped
  └──────────────────────────────────────────────────────────────┘
```

The bottom band is done. The C++ half of the band above it is largely done too. **The Rust FFI binding is
the missing rung** — without it, nothing in the two top bands can reach the engine.

---

## Track 1 — engine and Rust primitives

### 1. Rust binding for the Fragment FFI

**PRs 1–2** · risk: medium · **the only partial cherry-pick in the stack**

**Summary.** Gives Rust callers a way to build a `Fragment`, declare its inputs and outputs, and hand
device-resident batches across a fragment boundary. The C++ side already exists in the target; only the
Rust surface is missing.

**How it works.** A thin `-sys` layer declares the C ABI, and a safe wrapper exposes `Fragment`,
`declare_output_broadcast`, `declare_output_hash_key`, and the schema-agreement guard. Hash keys are
normalised before partitioning — `DECIMAL` casts to `FLOAT64`, string keys use the kernel's hash-as-is
sentinel — so two senders that see the same key always agree on the destination.

**What it enables.** Everything in Track 2. The compute node is a Rust binary; without this binding it
cannot construct a plan fragment, so no CN feature can be reviewed or tested.

**Where to find it**
- Commits (Rust hunks only): `f8249e7c`, `cfad5dce`, `02aaa6c0`, `d5c59a0a`, `14333ead`
- New code: `rust/crates/sirius/src/lib.rs`, `rust/crates/sirius-sys`
- C++ side, already in target: `src/sirius_ffi.cpp:403` (broadcast), `:413` (hash key),
  `src/exec/streaming_fragment.cpp:44-59` (decimal normalisation)

> **For reviewers.** Take *only* the `rust/crates/` hunks — the `src/` and `test/cpp/` halves conflict
> head-on with the target's own version. Check the `-sys` declarations against the **target's** current
> C++ ABI, not the source's older signature. Confirm no panic can unwind across the FFI boundary.

---

### 2. Engine correctness fixes

**PRs 3–9** · risk: 4 of 6 are high

**Summary.** Six independent fixes plus one characterisation test. Each closes a hang or a wrong-answer
bug that the streaming path exposed. They are independent, so they can be reviewed in parallel.

**How it works**

| PR | Fix |
|---|---|
| 3 | Loads `core_functions` into the embedded DuckDB, so FFI-submitted plans can resolve scalar functions |
| 5 | Plans Substrait inside a transaction, so planning cannot observe a torn catalog |
| 6 | Completes a query whose input stream ends with zero batches — previously the fragment waited forever |
| 7 | Completes a hash join whose build side finishes empty; the probe was never released |
| 8 | Refuses a 64-bit integer sum that could overflow, instead of silently wrapping |
| 9 | Makes float grouped sums bit-stable via the sort-based groupby path |

**What it enables.** Reproducibility and trust. PR 9 is what makes downstream benchmark numbers
comparable — without it the same query can return different sums on different runs.

**Where to find it**
- Commits: `7e09aabf`, `f8360593`, `0c49416b`, `e11d4ac5`, `e8ca9e60`, `5d149277`; test `4bf9598e` (PR 4)
- Files: `src/sirius_ffi.cpp`, `src/creator/task_creator.cpp`,
  `src/include/pipeline/task_scheduler.hpp`, `src/op/sirius_physical_partition.cpp`

> **For reviewers.** PR 9 ships an *ungated* performance and memory regression — a full sort and gather on
> **every** float SUM. Ask for timing data before approving. PR 8's bound is whole-column, not per-group,
> so confirm it does not push ordinary TPC-H sums onto the CPU. PRs 6 and 7 both touch
> `task_scheduler.hpp`; PR 7 introduces a new lock order (`partition → join → sibling`) needing an ABBA check.

---

### 3. Byte-range parquet scan

**PRs 10–11** · risk: medium / high

**Summary.** Lets several compute nodes read *one* parquet file at the same time without overlapping.
Each node gets a byte range and reads only the row groups it owns.

**How it works.** A pure, deterministic rule maps a byte range to the row groups that range owns. The
parquet ingestible honours a per-file range, and the ranges travel from the Rust caller through the
Substrait plan into the scan.

**What it enables.** Scale-out reads. Without it the CN refused split scans outright, so a table in a
single large file could only be read by one node. Feature 9 is the CN-side consumer.

**Where to find it**
- Commits: `7f5fbf07`, `a77e52f9` (PR 10); `5c19836f`, `4db3aea2` (PR 11)
- New code: `src/op/scan/parquet_byte_range.{cpp,hpp}`, `src/planner/substrait_scan_ranges.{cpp,hpp}`

> **For reviewers.** The ownership rule must be a true partition — no gaps, no double-reads at range
> boundaries — and must match the StarRocks BE rule byte-for-byte, or rows go missing. `start + length`
> can overflow. `extract_scan_byte_ranges` runs on *every* plan and throws on unknown relation types,
> which is a regression surface far wider than this feature. `4db3aea2` has **no test** and strips any
> authority from a URI, so a remote path can alias to a local one.

---

### 4. Exchange staging arena

**PRs 12–16** · risk: all high

**Summary.** A dedicated GPU memory region, allocated with a bare `cudaMalloc` outside the RMM pool, used
to stage batches about to cross a fragment or host boundary. Callers take a lease, pack, push, release.

**How it works.** A bump allocator hands out leases. `export_packed` / `push_packed` give Rust a device
pointer, so a batch crosses the boundary without a round trip through host memory. Two follow-up fixes
harden the protocol: zero-row exports no longer orphan their lease, and leases are served off the engine
thread so acquisition cannot deadlock under load. The last PR adds fabric handles for cross-host reach.

**Lease lifecycle**

```
                  ENV SET          LEASE()           PACK
   ● ─► Unconfigured ─────► Ready ────────► Leased ────────► Packed
              │               │               │                 │
              │ UNSET         │ EXHAUSTED     │ ZERO-ROW        │ PUSH_PACKED
              ▼               ▼               ▼                 ▼
           ┌──────────────────────┐      ┌──────────┐        Pushed
           │  Refused             │      │ Orphaned │           │
           │  throws, never       │      │ fixed by │           │
           │  degrades            │      │  PR 14   │           │
           └──────────────────────┘      └──────────┘           │
                                                                │
              Ready ◄───────────────── RELEASE ─────────────────┘
```

The arena **never degrades**. It throws when the env var is unset
(`exchange_staging_arena.cpp:175`) and throws again when exhausted (`:192`). That is why *Refused* is
terminal, not a fallback. *Orphaned* is the leak PR 14 closes — before it, a zero-row export took a lease
and never gave it back, capping a cluster at roughly 20 queries.

**What it enables.** The whole exchange path, including nixl (feature 6) and every fan-out mode
(feature 7). It is also the single biggest lever on how large a query can run.

**Where to find it**
- Commits: `fe1199f6` (12), `bd8e0a97` (13), `295ea7be` (14), `ae949ade` (15), `3b19962f` (16)
- New code: `src/exec/exchange_staging_arena.{cpp,hpp}`; lease call site `src/sirius_ffi.cpp:755`,
  chunked packer `:761`
- Configured by: `SIRIUS_EXCHANGE_STAGING_BYTES` (environment only)

> **For reviewers.** Lease lifetime is the whole review — every early return must release, or the bump
> allocator wedges permanently on one stuck lease. In PR 13, decide who releases the lease if Rust drops
> the handle without pushing. In PR 15, a lease served after arena teardown begins is a use-after-free.
> PR 16 has no `cuInit` (misleading error if it is the first CUDA touch) and ships with zero tests.
>
> Split out the Track 2 hunks: `295ea7be`'s `nixl_transport.rs`, `ae949ade`'s SIGTERM handling,
> `3b19962f`'s benchmarks hunk.

---

## Track 2 — StarRocks compute node and plan translator

### 5. Multi-fragment compute node

**PRs 17–19** · risk: high · **exceeds the size guideline irreducibly**

**Summary.** Turns the compute node from a single-fragment demo into one that runs several fragments and
passes data between them. Adds per-instance GPU carve-outs so several CNs can share a box, and multi-file
schema inference so they can read a partitioned table.

**How it works.** A local exchange and a result store handle the handoff between fragments inside one
node. Batches cross the fragment boundary natively, replacing an earlier file-based hop — `ExchangeFile`
is deleted by this feature. `EngineConfig` carves the GPU into per-instance slices.

**What it enables.** Everything else in Track 2. It is the component the exchange, the fan-out and the
translator all plug into.

**Where to find it**
- Commits: `2f03b3fd` (17), `2121c3ea` + `e0451ca3` (18), `874fb330` + `d3b7c3f0` (19)
- New code: `experimental/starrocks/src/local_exchange.rs`, `result_store.rs`, `engine_settings.rs`,
  `gpu_affinity.rs`

> **For reviewers.** PR 17 is 15 files, about +2,972 lines, and *cannot be usefully split* — no subset
> compiles alone. Review it as a new component and ask for a reading order. The `parked` map pins GPU
> memory until dispatch, so check what happens when dispatch never comes. In PR 19 the carve-out
> arithmetic matters: over-subscription does not fail here, it surfaces as an OOM in an unrelated query
> later. `d3b7c3f0` does serial file opens on the RPC path.
>
> **Check the target's prior art first.** `origin/stream/09-cn-multifragment` and
> `origin/stream/10-native-crossing` already contain this work under different hashes, with subjects
> already rewritten `demo(...)` → `feat(...)`. They are superseded 2026-08-03 branches, so they are *not*
> in the base — but if they carry review fixes the source lacks, rebase those instead of re-applying the
> source patches.

---

### 6. Addressed exchange and the nixl transport

**PRs 20–23** · risk: high · **exceeds the size guideline irreducibly**

**Summary.** Makes the exchange address its destinations, then carries the hop over nixl (RDMA) so
fragments on different hosts can exchange data. Adds loud failure propagation, then a formatting pass.

**How it works.** A routing table maps a destination address to a sender. The nixl transport registers
the staging arena's memory and moves batches host-to-host without going through the CPU. `fetch_data`
long-polls, because a not-ready reply desynchronises the frontend.

**What it enables.** Multi-node execution, and both fan-out modes in feature 7 — you cannot broadcast or
hash-partition until senders are addressable.

**Where to find it**
- Commits: `6a126c13` (20); `681c3089`, `82a6fa25`, `f83fc549` (21); `9669d9fb` (22); `4ab267de` (23)
- New code: `experimental/starrocks/src/nixl_transport.rs`, `prpc_client.rs`, `compute_node_service.rs`

> **For reviewers.** PR 21 is ~3,260 lines across 16 files and has **zero CI coverage** —
> `cargo test --workspace --no-default-features` excludes all of `nixl_transport.rs`, so it needs a manual
> two-CN run. **Blocker: parameterise the hardcoded `/home/ubuntu/...` paths before this lands.**
> Destination matching is string hostname equality, and a single dispatch thread serialises all receivers.
> PR 22's failure propagation must not itself deadlock — peers blocked on the exchange have to unblock.
> PR 23 is formatting only: verify by re-running rustfmt on the parent and confirming an empty diff. Its
> placement is deliberate, because later diffs are written against the reflowed lines.

---

### 7. Fan-out — broadcast and hash partitioning

**PRs 24–25** · risk: high

**Summary.** Lets one fragment send its output to N destinations in two modes: *broadcast*, where every
destination gets a full copy, and *hash*, where each gets a disjoint slice keyed by a hash of chosen columns.

```
                          ┌─ declare_output_broadcast ─► N destinations, full copy
   Fragment output ───────┤                                 arena: N x payload
       one batch          │        ▲ (silent fall-through, no test)
                          │        ┆
                          └─ declare_output_hash_key ──► N destinations, disjoint
                                                            arena: 1 x payload
```

**How it works.** A sender is parked once and then owns one output stream per destination. Broadcast fans
the same batch to all of them. Hash mode resolves the frontend's partition expressions to output column
indices and routes each row by hash. The Rust declarations for both modes come from feature 1.

**What it enables.** Distributed joins and distributed aggregation. Hash fan-out is what lets the grouped
two-phase aggregation in feature 8 run across nodes.

**Where to find it**
- Commits: `bed62cd1` + `666769dc` (24); `b5609d01` + `21dbe458` (25)
- Files: `experimental/starrocks/src/engine.rs:441-445` (one output stream per destination),
  `nixl_transport.rs:496-552` (lease per destination)

> **For reviewers.** Two silent-failure risks, both producing wrong answers rather than errors. First, the
> mode selector can fall through from hash to broadcast — and `21dbe458` ships with **no test** for it.
> Second, an off-by-one in index resolution mis-partitions silently. Also check the cleanup path: a partial
> remote-send failure strands local receivers and leaks GPU memory. `b5609d01` narrows the set of accepted
> plans for single-destination hash sinks and adds **zero new tests**.
>
> Broadcast takes **one lease per destination** even though the payload is byte-identical, so a fan-out of
> N occupies N× the arena. Listed as an open optimisation: one lease, N transmits, refcounted release.

---

### 8. Two-phase aggregation translation

**PRs 26–30** · risk: high · **every failure mode here is a wrong number**

**Summary.** Teaches the plan translator to run the frontend's default two-phase aggregation: a *partial*
aggregate on each node, an exchange, then a *merge* producing the final answer.

```
  FE plan ─► Phase classifier ─► Partial phase ─► Exchange ═╗
             agg_phase.rs         PR 28           partial_  ║ WIRE
             PR 26                                state.rs  ║ TYPE
                                                  PR 27     ║
   Wire-type parity  ◄┄ CI ┄┄  Finalizing    ◄──  Merge  ◄══╝
   harness · PR 30             projection         phase
   fails CI on drift           avg = sum/count    PR 28
                               PR 29
```

**How it works.** First a refactor replaces a boolean rejection with a phase classification. Then a data
model maps each aggregate function to its partial-state wire type. The partial and merge phases are
translated together, with the exchange schema overridden to carry partial state instead of final values.
Grouping keys follow. `avg` cannot be merged directly, so it is expanded into a `sum`/`count` pair and
divided in a finalizing projection. A CI harness then checks the wire-type model against what the engine
actually produces.

**What it enables.** Most of TPC-H. Without it the CN rejected the frontend's default plan shape, so
queries had to be rewritten by hand.

**Where to find it**
- Commits: `a2582212` (26), `178c1770` (27), `35bb03c7` + `4c8193fe` + `759e0835` (28),
  `7d349606` + `398e9c5f` + `c1d21506` (29), `a9d7135b` (30)
- New code: `crates/starrocks-plan-translator/src/agg_phase.rs`, `partial_state.rs`;
  harness at `tests/wire_type_parity.rs`
- Cross-check: PR 4's characterisation test (`4bf9598e`) landed *before* this work — it is the
  independent oracle.

> **For reviewers.** `4c8193fe` is the riskiest commit in the whole range. Check the exchange schema
> override against PR 27's model line by line. For `avg`, check where the division happens and what a null
> or zero count does. PR 26 claims behaviour preservation — that claim *is* the review: it deletes the only
> double-aggregation guard and leaves safety positional, so confirm every previously-rejected plan is still
> rejected. PR 30 is labelled `test` but adds a **new public FFI method** (`Fragment::output_types`) across
> three layers, and must fail loudly on an unmodelled aggregate rather than skipping.

---

### 9. Translator correctness — splits, slot order, slot resolution, types

**PRs 31–34** · risk: high · **silent corruption**

**Summary.** The CN-side consumer of the byte-range work, plus four groups of translator fixes that share
one property: when they are wrong, the query still succeeds and returns bad data.

| PR | Fix |
|---|---|
| 31 | Emits byte-range splits instead of refusing them; adds a narrower refusal for compressed containers, where byte offsets do not map to row groups |
| 32 | Ships sort tuples and aggregation grouping keys in *materialized-slot* order — the CN used declaration order, the frontend expects materialized order |
| 33 | Three slot-resolution defects: `CLONE_EXPR` unwrapped as identity, fallback to slot id when the tuple id is stale, materialising a project's common slots consumed above it |
| 34 | Casts frontend-narrowed builtin returns back to their declared types |

**What it enables.** Correct results at scale. PR 32's grouping-key fix is what closed q03; PR 31 is what
lets several CNs split one large parquet file.

**Where to find it**
- Commits: `864e917a`, `397dd878`, `25e3a80f` (31); `7b52c882`, `db5d5088` (32);
  `9e3f97d9`, `56d541af`, `e934c7c7` (33); `cff3618a` (34)
- Files: `crates/starrocks-plan-translator/src/` — mostly translator plus large test fixtures

> **For reviewers.** In PR 31 the compressed-container guard must be conservative: any offset it cannot
> resolve should fall back to a whole-file scan. Note `864e917a` deletes the previous whole-file safety
> net, and an all-empty-range node falls back to a full scan. In PR 32, ask whether any *other*
> tuple-emitting site has the same declaration-versus-materialized confusion. In PR 33, `56d541af`
> **relaxes a correctness guard** — unique-by-id is not the same as correct-by-id, so ask for an
> optimizer-version assertion. In PR 34, lossy narrowing should error rather than truncate; the fix uses a
> hardcoded five-name allowlist, so it closes five instances, not the class.

---

## How the PRs are organised

The stack is **layered on top of the target's existing streaming PRs**, not merged into them. The
target's `3bea68de`, `641b77eb` and `d1c0dc6d` already sit on `dev`; our stack branches from that tip.

### Layering

- **Base:** `origin/stream/15-fragment` — `origin/dev` (`f107ba88`) plus exactly three commits. Branching
  here inherits every upstream commit the source has never compiled against (#1320, #1444, #1447, #1450, #1456).
- **Preparation commit:** a single `xfer/base` branch that resolves the two renames **once**, before
  anything else is applied. This is the main reason to rebase rather than port — you resolve them once
  instead of 75 times.
- **Then one branch per PR**, each stacked on the previous one within its track.

### The two renames

| Source symbol | Target symbol | Why it matters |
|---|---|---|
| `stream_lifecycle` (25 files) | `batch_stream` (15 files) | Target adds a producer-error plane. Source code assuming no error plane may need a `fail_input` call, not just a rename. |
| `partition_spec::partition_mode` — `mode` is **last** | `sirius::op::partition_mode` — `mode` is **first** | Source relies on `{keys, casts}` aggregate init still meaning hash. Every initialiser must be rewritten as designated init or reordered. **`sed` cannot do this.** |
| *absent* | `stream_session::fail_output(id, error)` | Target-only verb. Source error paths may need to call it. |
| inline cast logic | `derive_key_cast_type()` | Textual conflict despite identical semantics. Prefer the target's version, drop the source hunk. |

### Naming

- `xfer/base` — the preparation branch that resolves the renames.
- `pr/NN-short-slug` — one per PR, numbered by review order. Example: `pr/01-rust-fragment-ffi`.
- The number is the review order, so a reviewer can always tell what a branch sits on top of.
- Three commit subjects need rewriting before they land: `4b72e708` ("step 1 gb200"), `031c2494` and
  `e3756be6` (both "minor"). Fifteen more have a prefix that does not match their content — the handoff
  lists them.

### Ordering and dependencies

| PR | Title | Depends on | Risk |
|---:|---|---|---|
| 1 | Rust binding for the Fragment FFI | — | med |
| 2 | Rust binding for fan-out, schema and decimal keys | 1 | med |
| 3 | Load `core_functions` into embedded DuckDB | — | low |
| 4 | Test: partial aggregates merge across the hop | 1 | low |
| 5 | Plan Substrait inside a transaction | — | med |
| 6 | Complete a query whose stream ends with zero batches | 1 | high |
| 7 | Hash join with an empty build side completes | 6 | high |
| 8 | Refuse a 64-bit integer sum that could overflow | 1 | high |
| 9 | Bit-stable float grouped sums | — | high |
| 10 | Byte-range to row-group ownership | — | med |
| 11 | Byte ranges ride the Substrait plan into the scan | 10 | high |
| 12 | `cudaMalloc` exchange staging arena | — | high |
| 13 | `export_packed` / `push_packed` | 1, 12 | high |
| 14 | Zero-row exports no longer orphan leases | 13 | high |
| 15 | Serve staging leases off the engine thread | 14 | high |
| 16 | Fabric-handle arena for cross-host exchange | 15 | high |
| 17 | Multi-fragment compute node | 1, 3 | high |
| 18 | Cross the fragment boundary as native batches | 17 | high |
| 19 | Per-instance GPU carve-outs + `FILES()` inference | 17 | high |
| 20 | Route exchange senders by destination address | 18 | high |
| 21 | Carry the exchange hop over nixl | 16, 20 | high |
| 22 | A failing fragment fails its whole query, loudly | 21 | high |
| 23 | rustfmt pass over the CN crate | 22 | low |
| 24 | Park a sender once, fan out to N destinations | 2, 20, 23 | high |
| 25 | Hash-partitioned fan-out through the CN | 24 | high |
| 26 | Classify aggregation phases | 23 | med |
| 27 | Model the partial-state wire type per aggregate | 26 | low |
| 28 | Translate the partial and merge phases | 4, 27 | high |
| 29 | Grouped two-phase aggregation and `avg` expansion | 28 | high |
| 30 | Wire-type model checked against the engine in CI | 29 | high |
| 31 | CN emits byte-range splits; refuses compressed containers | 11, 23 | high |
| 32 | Sort and aggregation tuples in materialized-slot order | 29 | high |
| 33 | Slot resolution: CLONE_EXPR, stale tuple ids, common slots | 32 | high |
| 34 | Cast FE-narrowed builtin returns to declared types | 33 | med |

> PR 23, a pure formatting pass, is a dependency of PRs 24, 26 and 31. That is deliberate: later diffs are
> written against the reflowed lines, so applying it out of order creates conflicts everywhere.

---

## Bringing the commits across

```
  demo-multi-cn ──► format-patch ──► drop 16, keep 84 ──► rename map
    790612fb          /tmp/xfer        5 are rust-only     2 renames, once
                                                                │
                                                                ▼ SED + HAND
  stream/15-fragment ──► xfer/base ──► pr/01 ... pr/34 ──► review and merge
   d1c0dc6d = dev+3      green build     one per PR           track by track
```

There is no cherry-pick step, because there are no shared objects. Adding the source as a remote is still
worth doing — it makes the source SHAs reachable so you can run `git diff` and `git format-patch` from
inside the target — but it does **not** make the histories related.

```bash
# 0 — establish a green baseline BEFORE applying anything.
#     The source branch could not build its own test target for ~16 days.
cd /home/prestouser/aocsa/aocsa_upstream/sirius
git fetch --all
git submodule update --init --recursive     # worktrees do NOT auto-init
pixi run make && pixi run make test

# 1 — make the source objects reachable (does NOT relate the histories)
git remote add sirius-db /home/prestouser/aocsa/sirius
git fetch sirius-db demo-multi-cn:refs/remotes/sirius-db/demo-multi-cn

# 2 — generate the series, then delete the 16 dropped patches by number
cd /home/prestouser/aocsa/sirius
git format-patch --no-merges -o /tmp/xfer efdf3dc..790612fb

# 3 — the 5 partial commits: Rust hunks ONLY
for h in f8249e7c cfad5dce 02aaa6c0 d5c59a0a 14333ead; do
  git format-patch -1 "$h" --stdout -- rust/ > /tmp/xfer/rustonly-$h.patch
done
grep -c '^+++ b/rust/' /tmp/xfer/rustonly-*.patch   # each must be non-empty

# 4 — resolve the renames ONCE on the preparation branch
cd /home/prestouser/aocsa/aocsa_upstream/sirius
git checkout -b xfer/base origin/stream/15-fragment
sed -i 's/\bstream_lifecycle\b/batch_stream/g' /tmp/xfer/00*.patch
# then hand-fix the partition_spec aggregate initialisers — sed cannot reorder members

# 5 — one branch per PR, in the table order
git checkout -b pr/01-rust-fragment-ffi xfer/base
git am --3way --keep-non-patch /tmp/xfer/rustonly-f8249e7c.patch
```

### The 16 commits to drop

Every one is verified present in the target tree, file by file. Re-landing any of them conflicts head-on
with the target's better-reviewed version.

| Absorbed into | Source commits |
|---|---|
| `3bea68de` — streaming sink | `2d26d9d8` `97f42556` `7e2d3c2c` `61803a72` `f779b30e` `876576e3` |
| `641b77eb` — stream_session | `e5831f8f` `67c35837` `00a4ce64` |
| `d1c0dc6d` — streaming fragment | `c27832ac` `cb5e906d` `d87a955b` `655dfa2b` `5c456e04` `adacda38` `77f7b825` |

---

## How to review the stack

### Review track by track, not PR by PR

1. **Track 1 must build and pass with zero `experimental/starrocks` changes applied.** If it does not, a
   track boundary has leaked. This is the single most useful check in the whole plan.
2. After PR 16, run the GPU tests by hand — `[streaming_fragment]`, `[batch_stream]`, `[staging_arena]`.
   They do not run in a CPU-only CI lane.
3. After PR 34, `cargo test --workspace --no-default-features` must be green. Remember this **excludes**
   all of `nixl_transport.rs`.

### Conflict strategy, in priority order

1. A conflict in `src/exec/`, `src/op/sirius_physical_streaming_sink.cpp`, `src/include/op/` or
   `src/pipeline/` almost certainly means you are re-landing something already excluded.
   **Stop and check the drop list.**
2. When source and target disagree on streaming semantics, **take the target**. It went through upstream
   review and carries the error plane the source lacks.
3. A `.rej` in `experimental/starrocks/` is usually base drift from the rustfmt pass. Apply PR 23 before
   anything downstream of it.
4. `CMakeLists.txt` conflicts are almost always additive — both sides add sources. Union them.
5. If a source commit modifies a file the target deleted, drop the whole hunk. That signals the CN track
   has no base there yet.

### What "it compiles" does not prove

- **Bit-exactness.** Run PR 9's permuted-order tests and the two-sender determinism test on the target
  build. If the `batch_stream` rename or the `partition_mode` reordering perturbed the hash path, these
  fail loudly instead of silently mis-partitioning.
- **Numeric agreement.** Diff a 22-query sweep against a single-process DuckDB run. Expect the *known*
  ~0.1% deficit on every `sum(l_extendedprice*(1-l_discount))` — it is **not fixed anywhere in this
  range**. Any other divergence is a transfer defect.
- **Endurance.** Three consecutive sweeps with zero restarts and the same CN process ids. This is the only
  way to prove PR 14's lease fix survived the transfer.

---

## Known gaps in this work

Recorded so reviewers do not have to rediscover them. All from the handoff's §G.

| Item | Where | Impact |
|---|---|---|
| One monolithic staging lease | `sirius_ffi.cpp:755` | Leases the whole table at once even though the packer already chunks at `:761`. Direct cause of q17 failing at SF500 with a 16 GiB arena. |
| One lease per broadcast destination | `nixl_transport.rs:496-552` | N× arena for byte-identical data. |
| Receiver holds leases until drain | `local_exchange.rs:44` | High-water mark is cumulative epoch bytes, not the in-flight set. A retention bug, not a sizing shortfall. |
| `SIRIUS_EXCHANGE_STAGING_BYTES` undocumented | `docs/super-sirius/configuration.md` | Load-bearing, no default, throws when unset. Documented with four contradictory values across runbooks. |
| No test for `stream_plan_bindings.cpp` | `src/exec/` | Six of seven translation units have a matching test; this high-risk one does not. |
| `streaming-sessions.md` is 96 commits stale | `docs/super-sirius/` | Its "Not here yet" list names four things that shipped inside this range. `CLAUDE.md` tells you to read it first, so it actively misleads. |

---

## Missing info / Questions

**Open, and should be answered before anyone applies a patch.** Items 1–4 are from the handoff's §J;
item 5 is a gap in this document specifically.

1. **Is `790612fb` the intended tip?** The originally requested end commit
   `fa836455846c71572bb5156ace9798aae32f1c88` *does not exist in either repository*. Everything here
   assumes `790612fb`.
2. **Is `aocsa/sirius` the destination, or a staging fork for `sirius-db/sirius`?** This changes whether
   the subject rewrites and path parameterisation are needed at all.
3. **Are the CN and benchmark tracks wanted upstream?** The target carries no `nixl_transport.rs`,
   `engine_settings.rs`, `partial_state.rs`, `agg_phase.rs` or `.claude/skills/` today. Whether it *wants*
   them is a product decision, not a merge decision.
4. **Should the three `derived-sirius-config.yaml` files be tracked?** They are regenerated on every CN
   start, so tracking them guarantees a permanently dirty tree — and the committed values have already
   drifted from the code defaults.
5. **There are no PR URLs to link.** None of these 34 PRs exist yet. The only real PR references are the
   target's merged `#1320` (source `#836`) and the source's `#837`/`#838`. Everything else is referenced by
   commit hash and file path. If you want live links, the PRs have to be opened first.

---

> **Nothing in this document is a correctness claim about query results.** The benchmark harness scores a
> query as passing when it exits cleanly with at least one row — it never compares values. The DuckDB
> oracle diff has not been run.
