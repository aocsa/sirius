# Sirius `demo-multi-cn` → `aocsa/sirius` — Final Handoff

*Prepared 2026-08-14. Source `/home/prestouser/aocsa/sirius` @ `790612fb` (branch `demo-multi-cn`). Target `/home/prestouser/aocsa/aocsa_upstream/sirius` @ `d1c0dc6d` (branch `stream/15-fragment`). All facts below were verified against both working copies unless explicitly marked as an assumption or an open question.*

---

## Addendum — §B gap 2 (base branch) is RESOLVED

*Verified 2026-08-14 against the target clone, after synthesis. This overrides §B gap 2 and confirms §C assumption 2.*

**The base is `stream/15-fragment`. Assumption 2 holds; the 44-PR plan stands, not 41.**

`origin/stream/15-fragment` is `origin/dev` (`f107ba88`) plus exactly three commits — `3bea68de`, `641b77eb`, `d1c0dc6d`, all dated 2026-08-12. It is a clean stack directly on the dev tip, so branching from it inherits every upstream commit the source has never compiled against (#1320, #1444, #1447, #1450, #1456).

**The apparent contradiction was stale branches.** `origin/demo-streaming-integration` and `origin/stream/02-sink … stream/12-sink-doc` are all dated **2026-08-03** and **none is an ancestor of `stream/15-fragment` or merged into `dev`** (checked with `git merge-base --is-ancestor` against `origin/dev` for all 16 `stream/*` refs — every one reports unmerged). They are a superseded first-generation split, replaced by `13-sink` / `14-session` / `15-fragment` nine days later. So the byte-identical copies of source `2f03b3fd`, `7e09aabf`, `2121c3ea`, `e0451ca3` that the dedup pass found on those refs are **not in the base**: PRs 3, 17 and 18 stay in the plan.

**But treat those stale branches as prior art, not noise.** The target already attempted this exact CN work once and set it aside:

| stale target branch | corresponds to source |
|---|---|
| `origin/stream/07-ffi-corefunctions` (`3b47ec59`) | `7e09aabf` — load core_functions (PR 3) |
| `origin/stream/09-cn-multifragment` (`c8cbb4b6`) | `2f03b3fd` — multi-fragment CN (PR 17) |
| `origin/stream/10-native-crossing` (`2c2290c9`) | `2121c3ea` — native batch crossing (PR 18) |

Note the subject rewrites — `demo(starrocks):` → `feat(starrocks):`, `demo(ffi):` → `feat(ffi):`. Before re-porting PRs 3, 17 and 18 from source, **diff against these branches first**: if they carry review fixes the source lacks (as `13-sink`/`14-session`/`15-fragment` did over their source twins), rebase *those* onto the new base instead of re-applying the source patches. That is cheaper than re-litigating review comments the target has already absorbed.

---

## A) Objective

Take the 100 commits on `demo-multi-cn` (`efdf3dc..790612fb`) — the streaming-primitives series plus the StarRocks compute-node integration, byte-range scan splits, nixl exchange transport, and the TPC-H benchmark work — and land them on `aocsa/sirius`, which already carries a squashed, upstream-reviewed version of the first fifth of that series under different hashes. This document decides the transfer strategy, classifies every commit, groups them into reviewable PRs, and gives the mechanical merge plan.

---

## B) Missing information checklist

**Lead item — must be resolved before any transfer begins:**

- **The requested end commit `fa836455846c71572bb5156ace9798aae32f1c88` DOES NOT EXIST in either repository.** Re-verified for this document: `git cat-file -t fa836455…` returns `fatal: git cat-file: could not get object info` in *both* `/home/prestouser/aocsa/sirius` and `/home/prestouser/aocsa/aocsa_upstream/sirius`. The original prompt showed that SHA partially redacted, which is consistent with a transcription or paste error. **This analysis uses source `HEAD = 790612fb` as the end.** `efdf3dc..790612fb` is exactly 100 commits (re-verified: `rev-list --count` = 100), 0 merges, 0 git-empty. **Confirm 790612fb is the intended tip before anyone starts applying patches.**

Other gaps:

- **Which target branch is the base?** This materially changes the plan. `stream/15-fragment` (`d1c0dc6d`) and `origin/stream/engine` do **not** contain the StarRocks CN work, but `origin/demo-streaming-integration` and `origin/stream/10-native-crossing` **do** contain byte-identical copies of source `2f03b3fd`, `7e09aabf`, `2121c3ea`, `e0451ca3`. See the contradiction flagged in §E. Four commits are either "already there" or "must be ported" depending purely on this answer.
- **Is the destination `aocsa/sirius` or `sirius-db/sirius`?** The target remote is `git@github.com:aocsa/sirius.git`, a *different* remote from the source's `sirius-db/sirius`. It is unclear whether `aocsa/sirius` is a staging fork whose PRs eventually go to `sirius-db/sirius` upstream, or the final destination. The upstreaming advice in §F (drop root-level session diaries, parameterise absolute paths) assumes the former.
- **Do the experimental StarRocks CN and the benchmark tracks belong upstream at all?** Nothing in `aocsa/sirius` today carries `nixl_transport.rs`, `engine_settings.rs`, `partial_state.rs`, `agg_phase.rs`, or `.claude/skills/`. Whether the target *wants* ~30k lines of demo/bench material is a product decision, not a merge decision.
- **Is the vendored StarRocks submodule commit pushed and reachable?** `experimental/starrocks/starrocks` moves `14b7e3fa → 04cd3136` (in `681c3089`) and then back to `14b7e3fa` (in `d271522a`). The `SKILL.md` diff inside `d271522a` states that `04cd3136` "does not exist upstream (local-only devbox commit)". If any intermediate state is preserved, fresh clones will fail `git submodule update`.
- **Are the three `derived-sirius-config.yaml` files meant to be tracked?** `.cn0/`, `.cn3/`, `.cn0-52/` are engine-generated at every CN startup (`main.rs:260-284`) yet committed by `f7864a7e` and `4e6439c8` — and `1d2bbae2`'s own message says these are "Not committed". They are dirty in the working tree right now.
- **Roughly 40 stale commit-hash references in committed prose.** Verified unreachable from any branch: `f199bc0e`, `e07cddc2`, `bb066e90`, `c858e79a`, `4beca977`, `4323197d`, `fe236e8b`, `59ce6662`, `312e4535`, `7039665c`, `4bf24dff`, `11625add`, `64977ebb`, `8c23e7e7`, `ae73d503`. All are pre-rebase twins of commits *inside this range*. They are dead in the source clone and **guaranteed** dead in the target, which shares no objects.
- **The 2,569 lines of untracked markdown** (`BENCHMARK-HANDOFF.md`, `bench/gb200-4gpu/REPORT-*.md`, `RESULTS-*.md`) plus a 5-file uncommitted working-tree diff are not part of the 100 commits and have no keep/drop decision.

---

## C) Assumptions

Each is tied to a gap above. If an assumption is wrong, the named section changes.

1. **`790612fb` is the intended end.** (Gap: missing end SHA.) If the real end is later, re-run the classification for the delta; the PR plan's tail (PRs 38–44) is where new commits would land.
2. **The merge base is the `stream/15-fragment` / `dev` line, not `demo-streaming-integration`.** (Gap: base branch.) Under this assumption `2f03b3fd`, `7e09aabf`, `2121c3ea`, `e0451ca3` **must be ported** and appear as PRs 3, 17, 18. If the base is `demo-streaming-integration`, delete those three PRs and the count drops from 44 to 41.
3. **`aocsa/sirius` is a staging fork for eventual `sirius-db/sirius` upstreaming.** (Gap: destination.) This is why §F says to rewrite non-conforming subjects, move root-level docs, and parameterise hardcoded paths — none of which matters for a private fork.
4. **The CN and benchmark tracks are wanted.** (Gap: scope.) They are grouped into tracks precisely so they can be dropped wholesale (PRs 17–41) without touching the engine track (PRs 1–16).
5. **Generated `derived-sirius-config.yaml` files should be gitignored, not tracked.** (Gap: config artifacts.) PR 40 flags this rather than deciding it.
6. **Stale hashes should be rewritten to commit *subjects*, not re-mapped.** (Gap: dead references.) Re-mapping is impossible across repos with no shared objects; subjects survive rebases and cross-repo transfer.
7. **Untracked working-tree files are out of scope** for this transfer and stay behind. (Gap: untracked docs.) They will, however, block the first commit until the tree is cleaned — see §I.

---

## D) DECISION

**Option (2): work on top of the target — rebase the non-duplicate remainder onto `aocsa/sirius`. Do NOT port the source branch wholesale.**

Justification (evidence from the dedup pass):

1. The repos share **zero git objects** (`efdf3dc` absent from target; `d1c0dc6d` absent from source), so cherry-pick by hash is mechanically impossible — only patch transfer works, in either direction.
2. The target is **further along and better organised** on the streaming series: 21 of the 100 source commits are already represented there, squashed into three clean commits (`3bea68de`, `641b77eb`, `d1c0dc6d`), with review hardening the source lacks (`batch_stream`'s producer-error plane, `stream_session::fail_output`, `test_batch_stream.cpp`'s 17 cases, a 468-line vs 316-line design doc).
3. The `#836` base already **merged to target `dev`** as `ca41d15e` (PR #1320), carrying source `d87a955b`'s scheduler hunk verbatim. Porting source→target would re-litigate work upstream has accepted.
4. The target also has 14 commits the source lacks (#1444, #1447, #1448, #1449, #1450, #1454, #1456 …) that the source's engine code has never been compiled against.
5. Two renames landed during upstream review — `stream_lifecycle`→`batch_stream` and `partition_spec::partition_mode`→`sirius::op::partition_mode` — that every streaming commit will conflict on. Resolving them **once** on a rebase is cheap; resolving them 75 times inside a wholesale port is not.
6. **75–79 of the 100 commits are genuinely new** and have no counterpart anywhere in the target. That remainder is the actual work product; the rest is duplicate effort.

---

## E) Exhaustive commit analysis

All 100 commits in `efdf3dc..790612fb`, oldest first. `Atomic` = one reviewable idea. `C/D` = code / docs-only. `Risk` = review risk, not runtime severity.

| # | Hash | Date | Subject | Cat | Atomic | C/D | Areas | Risk | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `2d26d9d8` | 07-25 | feat(op): streaming sink over an output repository (#837) | feature | yes | C | src/op, test | med | No error path marks sender done → hang on failure. **In target.** |
| 2 | `97f42556` | 07-25 | feat(op): partition the streaming sink across N destinations (#838) | feature | yes | C | src/op, test | high | N>1 throws on spilled batch; contradicts header contract. **In target.** |
| 3 | `e5831f8f` | 07-25 | feat(exec): stream_session, the id-addressed streaming router (#839) | feature | yes | C | src/exec, test | med | API rewritten 4 commits later; `add_sink` half-registers on dup. **In target.** |
| 4 | `00a4ce64` | 07-25 | docs(super-sirius): document the streaming session design | doc | yes | D | docs | low | Design record; stale by end of range. **In target (rewritten, 468 L).** |
| 5 | `2f03b3fd` | 07-25 | demo(starrocks): multi-fragment compute node on the streaming branch | feature | **no** | C | starrocks src/crates/build | high | 15 files, +2972. Bulk port of external delta. See contradiction below. |
| 6 | `7e09aabf` | 07-25 | demo(ffi): load core_functions into the embedded DuckDB | feature | yes | C | src/ffi | high | Widens `allow_unsigned_extensions`; deletes the stopgap comment. |
| 7 | `d87a955b` | 07-26 | fix(sched): schedule a streaming source as a query kickoff | fix | yes | C | src/planner | med | Other `get_scan_operators()` readers not audited. **Merged on target dev.** |
| 8 | `61803a72` | 07-26 | fix(pipeline): signal query completion for a streaming-sink plan root | fix | yes | C | src/pipeline | high | Keys on sink *type*, not root-ness → teardown race if ever non-root. **In target.** |
| 9 | `67c35837` | 07-26 | refactor(exec): stream_session does not own the operators it routes to | refactor | yes | C | src/exec, test | med | Lifetime safety moves from types to a doc comment. **In target.** |
| 10 | `cb5e906d` | 07-26 | feat(exec): bind a fragment's stream inputs from a plan | feature | yes | C | src/exec, planner, ffi | high | 3 defects: reference escapes mutex; planning mutates catalog; size-only projection guard. **In target.** |
| 11 | `f779b30e` | 07-26 | test(pipeline): pin the streaming-sink plan root contract | test | yes | C | test | low | Helper diverges from production ownership path. **In target.** |
| 12 | `7e2d3c2c` | 07-26 | fix(op): the streaming sink dropped every batch the pipeline gave it | fix | yes | C | src/op | high | Real bug: no `execute()` override → every fragment "succeeded" empty. **In target.** |
| 13 | `c27832ac` | 07-26 | feat(exec): streaming_fragment, one fragment over stream sessions | feature | yes | C | src/exec, test | high | `clear()` wipes the whole per-connection bind catalog. **In target.** |
| 14 | `655dfa2b` | 07-26 | test(exec): verify values across the fragment hop, not just row count | test | yes | C | test, src/op | low | Carries a stray clang-format hunk. **In target.** |
| 15 | `5c456e04` | 07-26 | test(exec): a parquet hop, a multi-batch drain, and a lifecycle guard | test | **no** | C | test | low | Records the QueryEnd-rollback-deadlock trap. **In target.** |
| 16 | `f8249e7c` | 07-26 | feat(ffi): a plan fragment reachable from Rust | feature | **no** | C | ffi, rust, exec | high | 5 things in one; message/diff drift on `QueryBeginStandalone`. **C++ in target, Rust not.** |
| 17 | `2121c3ea` | 07-26 | feat(starrocks): cross the fragment boundary as native batches | feature | yes | C | starrocks | high | Deletes ExchangeFile; `parked` map pins GPU mem until dispatch. |
| 18 | `e0451ca3` | 07-26 | docs(starrocks): the demo crosses fragment boundaries natively | doc | yes | D | starrocks | low | Must ship with #17 or DEMO.md is actively wrong. |
| 19 | `874fb330` | 08-05 | feat(starrocks): per-instance GPU carve-outs via EngineConfig | feature | **no** | C | starrocks | high | Emits `num_gpus: 1` unconditionally; `.gitignore` covers only `.cn1/.cn2`. |
| 20 | `d3b7c3f0` | 08-05 | feat(starrocks): multi-file FILES() schema inference | feature | yes | C | starrocks | med | Serial O(n) file opens on the RPC path. |
| 21 | `6a126c13` | 08-05 | feat(starrocks): route exchange senders by destination address | feature | **no** | C | starrocks | high | String hostname equality; single dispatch thread serialises all receivers. |
| 22 | `fe1199f6` | 08-05 | feat(exec): cudaMalloc exchange staging arena | feature | **no** | C | src/exec, ffi, test | high | **Message/diff mismatch**: also contains the whole packed FFI, untested here. |
| 23 | `bd8e0a97` | 08-05 | feat(ffi): export_packed / push_packed — a device-resident fragment boundary | feature | yes | C | rust | high | Message describes C++ that landed in #22. `PackedBatch` has no `Drop`. |
| 24 | `681c3089` | 08-05 | feat(starrocks): carry the exchange hop over nixl | feature | **no** | C | starrocks, build | high | 15 files/+3110. **Hardcoded `/home/ubuntu/...` paths in pixi.toml.** Zero CI. |
| 25 | `82a6fa25` | 08-05 | fix(starrocks): fetch_data long-polls — a not-ready reply desyncs the FE | fix | yes | C | starrocks | med | 600 s bare literal; occupies a `spawn_blocking` worker per poll. |
| 26 | `f83fc549` | 08-05 | docs(starrocks): the two-CN demo crosses the exchange hop over nixl | doc | yes | D | docs | low | Records the arena-outside-`--gpu-memory-limit` gotcha. |
| 27 | `d5c59a0a` | 08-05 | feat(ffi): reject a hop whose batch schema disagrees with the declared stream | feature | yes | C | exec, ffi, rust | med | Both guards silently optional; relay vs packed enforce different contracts. **C++ in target.** |
| 28 | `4bf9598e` | 08-05 | test(exec): partial aggregates merge to the one-shot answer across the hop | test | yes | C | test | low | Exemplary: characterisation tests land *before* the translator work. |
| 29 | `a2582212` | 08-05 | feat(starrocks): classify aggregation phases instead of rejecting on one flag | refactor | yes | C | starrocks crates | med | Deletes the only double-aggregation guard; safety is now positional. |
| 30 | `178c1770` | 08-05 | feat(starrocks): model the partial-state wire type per aggregate | feature | yes | C | starrocks crates | low | Dead code at this commit; hand-mirrored model of the engine binding. |
| 31 | `35bb03c7` | 08-05 | feat(starrocks): translate the partial phase of a two-phase aggregation | feature | yes | C | starrocks crates | med | `AggregationPhase` label is advisory; the engine ignores phases. |
| 32 | `4c8193fe` | 08-05 | feat(starrocks): translate the merge phase and override the exchange schema | feature | yes | C | starrocks crates | high | Riskiest translator commit; every failure mode is a wrong number. |
| 33 | `759e0835` | 08-05 | feat(starrocks): run the FE's default two-phase plan end to end | doc | yes | C | starrocks, docs | low | **Mislabelled**: 28/50 lines are DEMO.md; code delta is one error string. |
| 34 | `7f5fbf07` | 08-06 | feat(scan): a deterministic byte-range → row-group ownership rule | feature | yes | C | src/op/scan, test | med | Must match StarRocks BE's rule byte-for-byte; `start+length` can overflow. |
| 35 | `a77e52f9` | 08-06 | feat(scan): the parquet ingestible honors a per-file byte range | feature | yes | C | scan, scan_manager | high | Ranged scans may still pin an unmatched cache entry → wasted GPU memory. |
| 36 | `5c19836f` | 08-06 | feat(ffi): byte ranges ride the Substrait plan into the parquet scan | feature | yes | C | planner, ffi, rust | high | `extract_scan_byte_ranges` runs on *every* plan and throws on unknown rel types. |
| 37 | `864e917a` | 08-06 | feat(starrocks): the CN emits byte-range splits instead of refusing them | feature | yes | C | starrocks crates | high | Deletes the whole-file safety net; all-empty-range node falls back to full scan. |
| 38 | `397dd878` | 08-06 | feat(starrocks): refuse compressed-container parquet scan ranges | feature | yes | C | starrocks crates | med | Pure hardening; `fix`/`chore` would read more honestly. |
| 39 | `4db3aea2` | 08-06 | fix(ffi): canonicalize file: scheme paths in the byte-range registry | fix | yes | C | src/planner | med | **No test.** Strips any authority → remote path aliases to local. |
| 40 | `25e3a80f` | 08-06 | docs(starrocks): the two-CN demo splits one parquet file by byte range | doc | yes | D | docs | low | Adds the `count(*) = 6001215` exactly-once check. |
| 41 | `876576e3` | 08-06 | feat(exec): a broadcast mode on the streaming sink | feature | yes | C | src/op, test | med | N× memory amplification; peak-memory estimate not updated. **In target.** |
| 42 | `cfad5dce` | 08-06 | feat(ffi): declare_output_broadcast — the first multi-output fragment | feature | yes | C | ffi, rust | high | Silent no-op with one output; mutual exclusion only added 2 commits later. **C++ in target.** |
| 43 | `bed62cd1` | 08-06 | feat(starrocks): park a sender once, one output stream per destination | refactor | yes | C | starrocks | high | **Mislabelled `feat`.** Dup-slot guard inserts before it validates. |
| 44 | `666769dc` | 08-06 | feat(starrocks): fan a data stream sink out to N destinations (broadcast) | feature | yes | C | starrocks | high | Partial remote-send failure strands local receivers and leaks GPU memory. |
| 45 | `b5609d01` | 08-06 | feat(starrocks): resolve hash-partition keys to output column indices | feature | yes | C | starrocks | med | Narrows accepted plans for single-destination hash sinks. **Zero new tests.** |
| 46 | `02aaa6c0` | 08-06 | feat(ffi): declare_output_hash_key — deterministic hash fan-out | feature | yes | C | ffi, rust, exec | high | **Ships a crash** on any VARCHAR key; fixed by #49 eleven minutes later. **C++ in target.** |
| 47 | `21dbe458` | 08-06 | feat(starrocks): wire hash-partitioned fan-out through the compute node | feature | yes | C | starrocks | high | Mode selector can silently fall through hash→broadcast; no test. |
| 48 | `7d349606` | 08-06 | feat(starrocks): grouped two-phase aggregation translates | feature | yes | C | starrocks crates | high | Net −4 lines; enables the FE's default plan. Meaningless without #47. |
| 49 | `77f7b825` | 08-06 | fix(exec): string hash keys use the kernel's hash-as-is sentinel | fix | **no** | C | src/exec, docs | med | Fixes #46. Mixed-width INT32/INT64 parity is untested. **In target.** |
| 50 | `0c49416b` | 08-06 | fix(exec): complete a query whose input stream ends with zero batches | fix | **no** | C | creator, pipeline, rust | high | ~50 L scheduler fix buried under 1130 L of Rust tests; teardown moves threads. |
| 51 | `e8ca9e60` | 08-06 | fix(op): refuse a 64-bit integer sum that could overflow its accumulator | fix | yes | C | src/op, rust | high | Over-conservative on grouped sums; adds a per-batch device sync. |
| 52 | `398e9c5f` | 08-06 | feat(starrocks): expand avg into a two-phase sum/count pair | feature | **no** | C | starrocks crates | high | Ships a known-broken shape the next commit fixes; `__count` collision hazard. |
| 53 | `c1d21506` | 08-07 | fix(starrocks): every merge node leaves through the finalizing projection | fix | yes | C | starrocks crates | med | Repairs #52; most atomic commit in the range. |
| 54 | `a9d7135b` | 08-07 | test(starrocks): the wire-type model is checked against the engine in CI | test | **no** | C | ffi, rust, starrocks | high | **Mislabelled `test`** — adds a public FFI method `Fragment::output_types`. |
| 55 | `b7c310cd` | 08-07 | feat(starrocks): reproducible TPC-H A-vs-B benchmark harness | bench | **no** | C | benchmarks | low | **Mislabelled `feat`.** 3 script defects (truthiness on 0 ms; warm-up row; GNU `date`). |
| 56 | `2a37f4fb` | 08-07 | docs: bring the working plan and review docs into the repository | doc | yes | D | docs | low | 8 root-level .md, 2335 L. REVIEW-GUIDE.md is the per-commit record. |
| 57 | `ea5ad573` | 08-07 | docs: bring the remaining working notes into the repository | doc | **no** | D | docs | low | 20 files, +5545. Mixes durable design docs with session diaries. |
| 58 | `e3756be6` | 08-07 | minor | chore | yes | D | docs | low | **No prefix, empty body.** Deletes 3 files added 4 min earlier. **Noise.** |
| 59 | `736c0fe2` | 08-07 | bench(starrocks): cut the per-query timeout to one minute | bench | yes | C | benchmarks | low | 120→60, superseded by 60→30 five minutes later. **Noise.** |
| 60 | `0caae94a` | 08-07 | docs: root-cause analysis of the silent TPC-H query timeouts | doc | yes | D | docs | low | Highest-value doc: names 4 concrete engine defects. |
| 61 | `17b96ede` | 08-07 | bench(starrocks): halve the per-query timeout to 30 seconds | bench | yes | C | benchmarks | low | 30 s gate hides a real 124 s FE error; cites a dead hash. |
| 62 | `4ab267de` | 08-07 | style(starrocks): rustfmt pass over the CN crate | chore | yes | C | starrocks | low | Verified whitespace-only. **Do not drop** — later diffs depend on the reflow. |
| 63 | `9669d9fb` | 08-07 | fix(starrocks): a failing fragment now fails its whole query, loudly | fix | **no** | C | starrocks | high | Thrift `cancelPlanFragment` now lies OK while the fragment runs. Unbounded maps. |
| 64 | `cff3618a` | 08-07 | fix(starrocks): cast FE-narrowed builtin returns to their declared types | fix | yes | C | starrocks crates | med | Hardcoded 5-name allowlist closes 5 instances, not the class. |
| 65 | `7b52c882` | 08-07 | fix(starrocks): ship the sort tuple in materialized-slot order | fix | yes | C | starrocks crates | high | Silently reorders columns; rests on an inferred model of the FE layout. |
| 66 | `5c03bac1` | 08-07 | docs: post-fix status for the timeout analysis and benchmark log | doc | yes | D | docs | low | Records the ~0.1% revenue deficit — the range's most important open bug. |
| 67 | `d79fd836` | 08-07 | fix(starrocks): grow the exchange staging arena to 1280MiB per CN | fix | yes | C | starrocks (pixi) | high | Config-only. Workaround, not a fix; SF>1 rebreaks at a new magic constant. |
| 68 | `db5d5088` | 08-07 | fix(starrocks): ship aggregation grouping keys in materialized-slot order | fix | yes | C | starrocks crates | high | Actually closes q03. Validation short-circuits when `keys <= 1`. |
| 69 | `ae949ade` | 08-07 | fix(starrocks): serve staging leases off the engine thread + SIGTERM escalation | fix | **no** | C | ffi, rust, starrocks | high | Two unrelated halves. Public `StagingArena` ctor accepts null in `noexcept` methods. |
| 70 | `14333ead` | 08-07 | fix(exec): hash decimal partition keys through a FLOAT64 cast | fix | yes | C | src/exec, test | high | Cross-scale determinism claim is **untested**; DECIMAL(38,0) loses 15 digits. **In target.** |
| 71 | `a4023d88` | 08-07 | bench(starrocks): wait for the full cluster, kill the right CN binary | bench | yes | C | benchmarks | low | `MIN_BACKENDS=2` default breaks single-CN sweeps; still wrong for N>2. |
| 72 | `9e3f97d9` | 08-07 | feat(starrocks): translate CLONE_EXPR as an identity unwrap | feature | yes | C | starrocks crates | med | Deliberate no-cast; 121 L of test for 15 L of code. |
| 73 | `56d541af` | 08-07 | fix(starrocks): resolve slot refs by slot id when the tuple id is stale | fix | yes | C | starrocks crates | high | **Relaxes a correctness guard**; unique-by-id ≠ correct-by-id. |
| 74 | `480cc2fd` | 08-07 | docs: final 2026-08-07 TPC-H state + the integration learnings | doc | **no** | D | docs | low | ROADMAP retrospective is the most reusable artifact; all cited hashes dead. |
| 75 | `e934c7c7` | 08-07 | fix(starrocks): materialize a project's common slots consumed above it | fix | yes | C | starrocks crates | high | Largest translator commit; adds a new unconditional root-level hard error. |
| 76 | `6b910cfa` | 08-07 | docs: 20/22 final sweep state (fe236e8b) | doc | yes | D | docs | low | Empty body; superseded 3 h later by #80. Cited hash unreachable. |
| 77 | `c3d27358` | 08-07 | feat(starrocks): add runbook for 8×A100 NVLink setup and benchmark execution | doc | yes | C | benchmarks | low | 86% markdown. `cluster8.sh` omits `SIRIUS_QUERY_WATCHDOG_SECS`. |
| 78 | `e11d4ac5` | 08-07 | fix(op): a hash join whose build side finishes empty now completes | fix | **no** | C | op, pipeline, ffi, rust | high | **Three PRs in one.** Watchdog can busy-spin; new lock order into the join. |
| 79 | `5d149277` | 08-07 | fix(op): bit-stable float grouped sums via the sort-based groupby path | fix | yes | C | src/op, test | high | Ungated perf/memory regression: full sort+gather on every float SUM. |
| 80 | `981f3b8b` | 08-07 | docs: 22/22 final state (59ce6662, 312e4535) | doc | yes | D | docs | low | Empty body. Records two silent-hang classes found nowhere else. |
| 81 | `a5d04426` | 08-07 | bench(starrocks): the A-vs-B comparison — results, plot, reproduction | bench | yes | C | benchmarks | low | Headline geomean mixes measurement conditions; 62 KB regenerable PNG in git. |
| 82 | `52fc5248` | 08-07 | feat(starrocks): enhance setup and execution for Sirius TPC-H benchmarks on multi-GPU systems | build-ci | **no** | C | benchmarks, build | high | Removes the hardcoded paths from #24 — the one genuinely valuable infra change. |
| 83 | `749a42d5` | 08-07 | feat(skills): tpch-bench — operate the demo and its benchmark anywhere | doc | yes | D | .claude/skills | low | **Mislabelled `feat`**; one .md file. Undone by #90. |
| 84 | `031c2494` | 08-07 | minor | chore | yes | C | benchmarks | low | A 27-line notes file with a `.sh` extension that cannot run. **Noise.** |
| 85 | `295ea7be` | 08-07 | fix(exec): zero-row exports no longer orphan staging leases | fix | yes | C | exec, ffi, rust, starrocks | high | Fixes a monotonic arena leak capping a cluster at ~20 queries. Best commit here. |
| 86 | `91a20fde` | 08-07 | docs: arena-leak root cause + endurance evidence (7039665c) | doc | yes | D | docs | low | Cited hash **does not exist**. ~90% restates #85's own message. |
| 87 | `2bd826d1` | 08-07 | docs: OPEN-ISSUES.md — the work queue for the multi-GPU box | doc | yes | D | docs | low | Rewritten twice more within 10 commits — belongs in the issue tracker. |
| 88 | `047ab81d` | 08-08 | docs: TPC-H plan analysis — 22 captured plans + prioritized Sirius roadmap | doc | yes | D | docs, benchmarks | low | 48 files / +7842, mostly machine-generated EXPLAIN dumps. Provenance hash dead. |
| 89 | `f7d5d920` | 08-08 | docs: SF100/SF1000 projection on the 4× GB200 node | doc | yes | D | docs | low | Every number extrapolated from SF1; M0 aarch64 blocker is the actionable part. |
| 90 | `4b72e708` | 08-08 | step 1 gb200 | other | **no** | C | starrocks, skills, configs | high | **No prefix, empty body, +4125.** Reintroduces absolute paths; disables the watchdog. |
| 91 | `1d2bbae2` | 08-09 | fix(cn): bind the advertised http_port so the FE can un-blacklist a compute node | fix | **no** | C | starrocks, scripts, docs | high | 8 changes in one; real root cause still open. Contradicts #92 on artifacts. |
| 92 | `f7864a7e` | 08-11 | feat(config): add derived Sirius configuration files for CN0 and CN3 | bench | **no** | C | benchmarks, artifacts | med | **Mislabelled**: 1788/2085 lines are a benchmark driver. Commits generated YAML. |
| 93 | `ec10f8f4` | 08-11 | feat(bench): cross-host env overlay for two-machine engine A | bench | yes | C | benchmarks | med | Model commit: atomic, densely commented. Must be sourced before `cn-env.sh`. |
| 94 | `8eced0f1` | 08-11 | feat(bench): two-host CN launcher with HBM-membind interlock | bench | yes | C | benchmarks | med | Defensively written; rejects membind onto zero-CPU HBM nodes. |
| 95 | `d271522a` | 08-11 | fix(cn): link the CN against the CUDA driver libs and the system ld | build-ci | **no** | C | scripts, conf, docs | high | Titled fix is 22 L; silently repairs an unpushable submodule gitlink; rewrites `fe.conf`. |
| 96 | `f8360593` | 08-11 | fix(ffi): plan substrait inside a transaction | fix | yes | C | src/ffi | high | Best-written message in the range. **No regression test.** |
| 97 | `adacda38` | 08-11 | refactor(tests): replace query_lifecycle with query_window in streaming_fragment tests | fix | yes | C | test | low | **Mislabelled**: a build fix. Proves `make test` was unrun for ~16 days. |
| 98 | `3b19962f` | 08-11 | feat(exec): fabric-handle staging arena for cross-host exchange | feature | yes | C | src/exec | high | No `cuInit`; misleading error if this is the first CUDA touch. Zero tests. |
| 99 | `4e6439c8` | 08-12 | feat(config): add derived Sirius configuration for CN0-52 and benchmark documentation | doc | **no** | C | benchmarks, artifacts | low | **Mislabelled**: 317/336 lines are a tutorial. Third tracked generated YAML. |
| 100 | `790612fb` | 08-12 | feat(benchmark): add TPC-H benchmark brief and detailed plan for 8x A100 setup | doc | **no** | C | bench/, docs | low | HEAD. 9 of 11 files are .md; duplicates `bench/a100x8/PLAN.md` at the root. |

### Mislabelled subjects

15 commits where the Conventional-Commit prefix does not match the content. Every one of these breaks type-based PR grouping.

| Hash | Says | Actually is |
|---|---|---|
| `7e09aabf` | `demo(ffi)` | Production `src/sirius_ffi.cpp` change that widens the unsigned-extension trust boundary. Should be `feat(ffi)`. |
| `759e0835` | `feat(starrocks)` | 28/50 lines DEMO.md; the code delta rewords one error string. Body says "Behavior unchanged". |
| `bed62cd1` | `feat(starrocks)` | A refactor of GPU-memory parking lifetime the service does not yet exercise. |
| `a9d7135b` | `test(starrocks)` | Adds a **new public FFI method** (`Fragment::output_types`) across 3 layers. |
| `b7c310cd` | `feat(starrocks)` | Benchmark tooling only. The range's own later commits use `bench(starrocks)`. |
| `e3756be6` | *(none — "minor")* | Three markdown deletions reverting the prior commit. |
| `e11d4ac5` | `fix(op)` | Half the diff is outside `src/op` — engine, scheduler, FFI, Rust crate, pixi env. |
| `749a42d5` | `feat(skills)` | One markdown file. |
| `031c2494` | *(none — "minor")* | A notes file with a `.sh` extension. |
| `4b72e708` | *(none — "step 1 gb200")* | +4125 across 6 unrelated concerns, including a live engine setting. |
| `f7864a7e` | `feat(config)` | 1788 of 2085 lines are a benchmark driver → `bench`. |
| `d271522a` | `fix(cn)` | Build/CI fix plus four unnamed payloads (submodule bump, `fe.conf`, 3400 L docs, new harnesses). |
| `adacda38` | `refactor(tests)` | A build fix. The test TU could not compile before it. |
| `4e6439c8` | `feat(config)` | 317/336 lines documentation → `docs(bench)`. |
| `790612fb` | `feat(benchmark)` | 9 of 11 files markdown; changes no engine, harness or build code. |

Borderline (prefix defensible, content misleading): `a2582212` (`feat` for a pure refactor — nothing new becomes supported), `52fc5248` (`feat` for build/dev-env tooling), `c3d27358` (`feat` for 86% markdown), `d79fd836` (`fix` for a config constant), `397dd878` (`feat` for pure hardening).

### No meaningful change

**Explicitly: 0 of the 100 commits are git-empty and 0 are merge commits** — every commit has a non-empty `diff-tree` against its parent, which was verified before this workflow. So "no meaningful change" here means **"adds no reviewable value"**, judged by three criteria: (a) fully reverted or fully superseded *within this same 100-commit range*; (b) verified semantics-preserving (formatter-only); (c) content is a strict subset of material already committed elsewhere in the range.

| Hash | Criterion | Detail |
|---|---|---|
| `e3756be6` "minor" | (a) revert-pair | Deletes `draft-reply.md`, `draft-reply-short.md`, `ghstack-workflow.md` — all added by `ea5ad573` **4 minutes earlier**. Net tree effect zero; none present at HEAD. Empty body. **Squash into `ea5ad573`.** |
| `736c0fe2` | (a) superseded | `QUERY_TIMEOUT` 120→60 in `bench.sh` + README. `17b96ede` changes the identical four lines 60→30 **five minutes later**. The value 60 survives nowhere. **Squash into `17b96ede` as one 120→30 change.** |
| `4ab267de` | (b) formatter-only | Every hunk in all 6 files verified as rustfmt reflow — no identifier, literal, control-flow or type token changed. **Do NOT drop:** later commits diff against the reflowed lines. Keep as a trivial standalone commit or squash into the first PR of its track. |
| `031c2494` "minor" | (c) subset | `quick-bench-cn-2-notes.sh`: no shebang, line 2 is `git clone <repo>` with a literal placeholder, last line is English prose. Content is a strict subset of `REPRODUCE.md` (`a5d04426`) and `SKILL.md` (`749a42d5`) — and it says so itself. **Drop or fold into `a5d04426`.** |

Everything else, including all 15 docs-only commits, carries reviewable content. `0caae94a` (timeout root cause), `5c03bac1` (the ~0.1% revenue deficit), `981f3b8b` (two silent-hang classes) and `480cc2fd` (the integration retrospective) contain findings recorded **nowhere else in the range** and must survive any squash.

### Already in the target

25 source commits have a counterpart in `aocsa/sirius`. The decisive finding, which the supplied ground facts understated: **`origin/demo-streaming-integration` is a perfect 1:1, subject-for-subject mirror of source `efdf3dc..e0451ca3`** — 19 commits in the same order, of which 13 are byte-identical as normalized patches and 5 differ only by dev-base drift (similarity 0.977–0.998).

| Source | Target | Relation | Evidence |
|---|---|---|---|
| `2d26d9d8` | `db817b7a` / `3bea68de` | identical | 628 normalized patch lines, byte-identical. Also squashed with #838 into `3bea68de`. |
| `97f42556` | `98a042d3` / `3bea68de` | identical | 519 lines, byte-identical. |
| `e5831f8f` | `1cc4786f` / `641b77eb` | identical | 749 lines, byte-identical. |
| `00a4ce64` | `62e39e4d` | identical | 388 lines, similarity 0.997; 2 differing lines are a neighbouring README index row. Target doc later rewritten to 468 L. |
| `2f03b3fd` | `0cda6e2c` | identical | similarity 0.998; sole delta is a `Cargo.lock` syn bump. **Only on `demo-streaming-integration` — see contradiction.** |
| `7e09aabf` | `c2783171` / `3b47ec59` | identical | Byte-identical (54 lines). Target carries three copies. **Not on the `stream/15` line.** |
| `d87a955b` | `ca41d15e` (#1320) | squashed-into | **Already merged on target `dev`.** The `query.cpp` hunk is inside `ca41d15e`. |
| `61803a72` | `2449e1f4` / `3bea68de` | identical | Byte-identical (83 lines). |
| `67c35837` | `3961cace` / `641b77eb` | identical | Byte-identical (247 lines). |
| `cb5e906d` | `0e77b805` / `d1c0dc6d` | identical | 748 lines, similarity 0.993; 10 differing lines all base drift. |
| `f779b30e` | `c8849230` / `3bea68de` | identical | similarity 0.977; delta is the `query_window`/`query_id` API only. |
| `7e2d3c2c` | `47a84325` / `3bea68de` | identical | Byte-identical (103 lines). Warning string present at target `sirius_physical_streaming_sink.cpp:119`. |
| `c27832ac` | `ef8e9e03` / `d1c0dc6d` | identical | 802 lines, similarity 0.998; 4 differing lines are `#include` order. |
| `655dfa2b` | `b94ae479` | identical | Byte-identical (72 lines). |
| `5c456e04` | `26301757` | identical | Byte-identical (302 lines). |
| `f8249e7c` | `c9e09555` → `d1c0dc6d` | **partial** | C++ Fragment class present. **Rust half absent** — target `rust/crates/sirius/src/lib.rs` is 270 L (= dev) vs source's 2771 L. |
| `2121c3ea` | `c14acfd4` / `2c2290c9` | identical | Byte-identical (1345 lines). **Only on `demo-streaming-integration` / `stream/10`.** |
| `e0451ca3` | `4d35e780` | identical | Byte-identical (85 lines). Tip of `demo-streaming-integration`. |
| `876576e3` | `38abd7d9` → `d1c0dc6d` | superseded-by | Same semantics, reimplemented on `batch_stream`. **API shape differs and will conflict** (see §H). |
| `adacda38` | `d1c0dc6d` | superseded-by | No-op against target: its `test_streaming_fragment.cpp` already has 10 `query_window` hits, 0 `query_lifecycle`. |
| `d5c59a0a` | `10c894af` + `c9e09555` | **partial** | C++ `sink_types()` + `relay_from` schema check present. Rust `+139` not ported. |
| `cfad5dce` | `c9e09555` | **partial** | `declare_output_broadcast` at target `sirius_ffi.cpp:403`. Rust bindings absent. |
| `02aaa6c0` | `c9e09555` | **partial** | `declare_output_hash_key` at target `sirius_ffi.cpp:413`. Rust bindings absent. |
| `77f7b825` | `8cdf047c` | **partial** | Target's `derive_key_cast_type()` encodes the exact fix; its own comment reads "(mirror integration::streaming_fragment::build)". DEMO.md half not ported. |
| `14333ead` | `8cdf047c` | **partial** | Target has `case sirius::type_id::DECIMAL: return FLOAT64`. Rust `+172` and the 85-line GPU determinism test not ported. |

**⚠ CONTRADICTION TO RESOLVE — four commits.** The dedup pass classifies `2f03b3fd`, `7e09aabf`, `2121c3ea`, `e0451ca3` as already-in-target; the PR plan ports them as PRs 3, 17, 18. Both are correct against different refs: they exist byte-identically on `origin/demo-streaming-integration` and `origin/stream/10-native-crossing`, and are **absent** from `dev`, `stream/13/14/15` and `stream/engine` (whose `experimental/starrocks` tree is 32 files with no streaming CN and no `local_exchange.rs`). **If the merge base is `stream/15-fragment` they must be ported; if it is `demo-streaming-integration` they must not.** This is gap #2 in §B.

**Other ground-fact corrections found during dedup:**
- The target has **24 refs, not 13**. Missing from the supplied list: `stream/07-ffi-corefunctions`, `08-ffi-fragment`, `09-cn-multifragment`, `10-native-crossing`, `11-tests`, `12-sink-doc`, `stream/ffi`, and — most importantly — **`origin/stream/engine`**, an active in-progress port of the source's *later* work into upstream-quality PRs (waves 1–4, dated 2026-08-10/11). Wave 4's message reads verbatim "Port the Fragment class from the integration worktree".
- `#837`/`#838` are in the target **twice**: squashed into `3bea68de` *and* unsquashed byte-identical on `demo-streaming-integration`.
- The target absorbed **21** source commits into its three streaming commits, not the 3 the ground facts named.

**Where the target stops.** Verified absent from *every* target ref: `exchange_staging_arena.{cpp,hpp}`, `parquet_byte_range.{cpp,hpp}`, `substrait_scan_ranges.{cpp,hpp}`, `nixl_transport.rs`, `wire_type_parity.rs`, `agg_phase.rs`, `partial_state.rs`, `engine_settings.rs`, `gpu_affinity.rs`, `prpc_client.rs`, `DEMO.md`, `bench/`, `.claude/skills/tpch-bench`. The target's own docs concede it: *"Not ported: export_packed, push_packed, StagingArena"*.

---

## F) PR plan

**44 PRs. 84 of the 100 hashes appear** — 79 as whole commits, 5 as Rust-hunks-only partials. **16 are dropped entirely.** Four tracks, never mixed: engine/Rust primitives (1–16), StarRocks CN + translator (17–34), benchmark/GB200 (35–41), docs (42–44).

### Track 1 — Engine and Rust primitives

| # | Title | Commits | Depends | Risk | Review focus |
|---|---|---|---|---|---|
| 1 | feat(ffi): Rust binding for the Fragment FFI | `f8249e7c` *(Rust hunks only)* | — | med | `-sys` declarations must match the **target's** existing C++ ABI, not the source's older signature. Drop/`Drop` semantics; no unwinding across FFI. |
| 2 | feat(ffi): Rust binding for broadcast/hash fan-out + schema and decimal-key coverage | `cfad5dce`, `02aaa6c0`, `d5c59a0a`, `14333ead` *(Rust hunks only)* | 1 | med | Rust-layer mutual exclusion mirroring the C++ guard; tests must assert the target's normalization rules. |
| 3 | fix(ffi): load core_functions into the embedded DuckDB | `7e09aabf` | — | low | No double registration when the extension is also loaded normally. **Conditional on the base-branch question.** |
| 4 | test(exec): partial aggregates merge to the one-shot answer across the hop | `4bf9598e` | 1 | low | The one-shot comparison must be genuinely independent, not a recomputation sharing the code path. |
| 5 | fix(ffi): plan substrait inside a transaction | `f8360593` | — | med | Transaction scope and rollback on the throwing path. |
| 6 | fix(exec): complete a query whose input stream ends with zero batches | `0c49416b` | 1 | high | Zero-batch completion must be idempotent against a normal EOS. ~54 L of production code under 1130 L of tests. |
| 7 | fix(op): a hash join whose build side finishes empty now completes | `e11d4ac5` *(drop the pixi.toml hunk)* | 6 | high | New lock order `partition → join → sibling`; ABBA check. `sirius_physical_partition.cpp` is heavily reworked. |
| 8 | fix(op): refuse a 64-bit integer sum that could overflow | `e8ca9e60` | 1 | high | The bound is whole-column, not per-group — confirm it does not push ordinary TPC-H sums to CPU. |
| 9 | fix(op): bit-stable float grouped sums via the sort-based groupby | `5d149277` | — | high | **Ungated** perf/memory regression: full sort + gather on every float SUM. Ask for timing data. |
| 10 | feat(scan): byte-range → row-group ownership, honored by the ingestible | `7f5fbf07`, `a77e52f9` | — | med | The rule must be a partition — no gaps or double-reads at range boundaries. Must match StarRocks BE `utils.cpp`. |
| 11 | feat(ffi): byte ranges ride the Substrait plan into the parquet scan | `5c19836f`, `4db3aea2` | 10 | high | The unconditional `collect_from_rel` throw on unknown rel types is a regression surface for *every* plan. |
| 12 | feat(exec): cudaMalloc exchange staging arena | `fe1199f6` | — | high | Lease lifetime; every early return must release. Bump allocator wedges permanently on one stuck lease. |
| 13 | feat(ffi): export_packed / push_packed | `bd8e0a97` | 1, 12 | high | Ownership handoff — who releases the lease if Rust drops the handle without pushing. |
| 14 | fix(exec): zero-row exports no longer orphan staging leases | `295ea7be` *(drop the nixl_transport.rs hunk)* | 13 | high | Audit every other early return in export for the same shape. |
| 15 | fix(ffi): serve staging leases off the engine thread | `ae949ade` *(drop the SIGTERM hunks)* | 14 | high | Shutdown ordering — a lease served after arena teardown begins is use-after-free. |
| 16 | feat(exec): fabric-handle staging arena for cross-host exchange | `3b19962f` *(drop the benchmarks hunk)* | 15 | high | Missing `cuInit`; fallback when fabric handles are unavailable must degrade, not fail startup. |

### Track 2 — StarRocks CN and plan translator

| # | Title | Commits | Depends | Risk | Review focus |
|---|---|---|---|---|---|
| 17 | demo(starrocks): multi-fragment compute node | `2f03b3fd` | 1, 3 | high | **Exceeds size guideline (15 files / +2972) irreducibly.** Review as a new component: fragment lifecycle + `local_exchange` handoff. Ask for a reading order. |
| 18 | feat(starrocks): cross the fragment boundary as native batches | `2121c3ea`, `e0451ca3` | 17 | high | Schema agreement at the boundary; fallback for types with no native representation. |
| 19 | feat(starrocks): per-instance GPU carve-outs + multi-file FILES() inference | `874fb330`, `d3b7c3f0` | 17 | high | Carve-out arithmetic — over-subscription surfaces as an OOM in an unrelated query later. |
| 20 | feat(starrocks): route exchange senders by destination address | `6a126c13` | 18 | high | Sender reclamation when a destination disappears; single-thread dispatch serialises all receivers. |
| 21 | feat(starrocks): carry the exchange hop over nixl | `681c3089`, `82a6fa25`, `f83fc549` | 16, 20 | high | **Exceeds guideline (~3260 L / 16 files) irreducibly.** Memory registration lifetime vs PR 12's arena; the 600 s long poll. **Blocker: parameterise the hardcoded `/home/ubuntu/...` paths first.** |
| 22 | fix(starrocks): a failing fragment fails its whole query, loudly | `9669d9fb` | 21 | high | Failure propagation must not itself deadlock; peers blocked on the exchange must unblock. |
| 23 | style(starrocks): rustfmt pass over the CN crate | `4ab267de` | 22 | low | Verify mechanical: rerun rustfmt on the parent, diff empty. Nothing else. |
| 24 | feat(starrocks): park a sender once, then fan out to N destinations | `bed62cd1`, `666769dc` | 2, 20, 23 | high | Partial remote-send failure strands local receivers and leaks GPU memory — needs a cleanup path. |
| 25 | feat(starrocks): hash-partitioned fan-out through the compute node | `b5609d01`, `21dbe458` | 24 | high | Index resolution off-by-one mis-partitions **silently**. The hash→broadcast fall-through. |
| 26 | refactor(starrocks): classify aggregation phases | `a2582212` | 23 | med | Behaviour preservation is the entire claim: every previously-rejected plan still rejected. |
| 27 | feat(starrocks): model the partial-state wire type per aggregate | `178c1770` | 26 | low | Each table entry against StarRocks' actual partial-state encoding. Dead code until PR 28. |
| 28 | feat(starrocks): translate the partial and merge phases | `35bb03c7`, `4c8193fe`, `759e0835` | 4, 27 | high | The exchange schema override must match PR 27's model exactly; PR 4 is the cross-check. |
| 29 | feat(starrocks): grouped two-phase aggregation and avg expansion | `7d349606`, `398e9c5f`, `c1d21506` | 28 | high | avg division placement and the null/zero-count case; does `c1d21506` cover every merge node type? |
| 30 | test(starrocks): wire-type model checked against the engine in CI | `a9d7135b` | 29 | high | Must fail loudly on an unmodelled aggregate, not skip. **Note: adds a public FFI method.** |
| 31 | feat(starrocks): CN emits byte-range splits; refuses compressed containers | `864e917a`, `397dd878`, `25e3a80f` | 11, 23 | high | The compressed-container guard must be conservative — any unresolvable offset falls back to whole-file. |
| 32 | fix(starrocks): ship sort and aggregation tuples in materialized-slot order | `7b52c882`, `db5d5088` | 29 | high | Does any *other* tuple-emitting site have the same declaration-vs-materialized confusion? |
| 33 | fix(starrocks): slot resolution — CLONE_EXPR, stale tuple ids, common project slots | `9e3f97d9`, `56d541af`, `e934c7c7` | 32 | high | `56d541af`'s slot-id fallback: unique ≠ correct. Ask for an optimizer-version assertion. |
| 34 | fix(starrocks): cast FE-narrowed builtin returns to their declared types | `cff3618a` | 33 | med | Cast at the declared type; lossy narrowing must error, not truncate. |

### Track 3 — Benchmark and GB200

| # | Title | Commits | Depends | Risk | Review focus |
|---|---|---|---|---|---|
| 35 | bench(starrocks): reproducible TPC-H A-vs-B harness | `b7c310cd`, `736c0fe2`, `17b96ede`, `a4023d88`, `031c2494` | 34 | low | Reproducibility: pinned SF, fixed query set, comparable A/B config. **Squash the 3 noop members out.** |
| 36 | bench(starrocks): the A-vs-B comparison — results, plot, reproduction | `a5d04426` | 35 | low | Numbers must come from the committed harness at a stated commit; avoid the 02:00–03:50 UTC GB200 CI window. |
| 37 | bench(starrocks): multi-GPU setup, execution, tpch-bench skill | `52fc5248`, `749a42d5`, `d79fd836` | 16, 19, 36 | med | The 1280 MiB-per-CN arena constant vs PR 19's carve-outs on the target box. |
| 38 | bench: 8×A100 NVLink runbook and TPC-H brief | `c3d27358`, `790612fb` | 37 | low | Runbook steps must match the scripts actually committed in 35/37. |
| 39 | bench(gb200): CN http_port binding and GB200 cluster configuration | `4b72e708`, `1d2bbae2` | 38 | high | **Exceeds guideline (~7900 L / 37 files).** `1d2bbae2`'s port fix is the only real behaviour change. **Request: rewrite `4b72e708`'s subject and split out `nixl_bench.rs`.** |
| 40 | bench(gb200): CUDA driver linkage and derived Sirius configurations | `d271522a`, `f7864a7e`, `4e6439c8` | 39 | high | Is the linkage portable or box-specific? **Blocker: decide tracked-vs-generated on the three `derived-sirius-config.yaml` files.** |
| 41 | bench: two-host CN launcher with HBM-membind interlock | `ec10f8f4`, `8eced0f1` | 16, 40 | med | The two hosts must not both claim the same NUMA/HBM domain; overlay must not silently fall back. |

### Track 4 — Docs

| # | Title | Commits | Depends | Risk | Review focus |
|---|---|---|---|---|---|
| 42 | docs: working plan and review notes | `2a37f4fb`, `ea5ad573`, `e3756be6` | — | low | Editorial: does each document have a durable audience, or is it a session artifact? Screen for first-person diary content. |
| 43 | docs: TPC-H triage log and post-fix status | `0caae94a`, `5c03bac1`, `480cc2fd`, `6b910cfa`, `981f3b8b`, `91a20fde` | 42 | low | Condense superseded snapshots; **rewrite ~40 dead commit hashes as subjects.** |
| 44 | docs: open issues, plan analysis, SF100/SF1000 projection | `2bd826d1`, `047ab81d`, `f7d5d920` | 36, 43 | low | Do 22 EXPLAIN dumps belong in git? Does OPEN-ISSUES list items already fixed by PRs 5–9 and 32–34? |

### Excluded, and why

**16 commits dropped entirely** — every one verified present in the target tree file-by-file, so re-landing conflicts head-on with the target's better-reviewed version:

- **Into `3bea68de`** (streaming sink, partitioned + broadcast): `2d26d9d8`, `97f42556`, `7e2d3c2c`, `61803a72`, `f779b30e`, `876576e3`. Verified: `sirius_physical_streaming_sink.cpp` contains `7e2d3c2c`'s drop-batch warning at line 119 and its `build_pipelines` override at line 190; `876576e3`'s `partition_mode::broadcast` at lines 137/144; `f779b30e`'s test file with its helpers.
- **Into `641b77eb`** (stream_session): `e5831f8f`, `67c35837`, `00a4ce64`.
- **Into `d1c0dc6d`** (streaming fragment): `c27832ac`, `cb5e906d`, `d87a955b`, `655dfa2b`, `5c456e04`, `adacda38`, `77f7b825`. Verified: `test_streaming_fragment.cpp` is 510 L and already uses `query_window` (10 hits, 0 `query_lifecycle`), making `adacda38` a no-op; `77f7b825`'s rule is documented at `streaming_fragment.cpp:43`.

**5 commits partially excluded** — C++ halves dropped, Rust halves re-landed in PRs 1–2: `f8249e7c`, `cfad5dce`, `02aaa6c0`, `d5c59a0a`, `14333ead`. Verified absent: `git grep 'Fragment|declare_output|declare_input'` over `rust/crates/sirius*/src/lib.rs` at `stream/15-fragment` returns **nothing** — the target can build a Fragment from C++ and no Rust caller can reach it. **When applying, take only `rust/crates/` hunks; `src/` and `test/cpp/` will conflict.**

---

## G) Missed things and optimization opportunities

### High severity

1. **The uncommitted `engine_settings.rs` diff breaks 4 existing unit tests.** Removing the `if datasource.is_some() || cpu_affinity.is_some()` guard makes `executor:` unconditional and adds `pipeline:` and `operator_params:` blocks. Four tests assert the old shape: `absent_affinity_emits_no_executor_block`, `pipeline_pool_is_not_pinned_from_yaml` (which encodes a deliberate invariant now violated in spirit), and both byte-exact `full_document_snapshot*` tests.
2. **GB200-shaped config defaults applied unconditionally to every box.** The same diff hardcodes `scan_task_batch_size 6GB`, `hash_partition_bytes 32GB`, `max_build_hash_table_bytes 32GB` — ported from a 185 GB-HBM GB200 profile into a function with no hardware awareness. On the 80 GB A100 this branch is actively preparing (`bench/a100x8/`), 32 GB is 40% of the card before the pool.
3. **Two competing uncommitted fixes for the same root cause.** `cluster4-numa.sh`'s `SIRIUS_TUNED_CONFIG` opt-in and `engine_settings.rs`'s inline tuning both work around `--sirius-config` being `conflicts_with_all [gpu_memory_limit, …]` at `main.rs:69`. If both land there are two sources of truth. The real fix — letting an operator config compose with the derived one — is in neither.
4. **Engine-generated config tracked in git.** `.cn0/`, `.cn3/`, `.cn0-52/derived-sirius-config.yaml` are rewritten on every CN start (`main.rs:260-284`), guaranteeing a permanently dirty tree. Worse, the committed values have already **diverged from the code defaults** (tracked: scan `num_threads 18`, `uring_n_reactors 8`; code: 9, 4), so the artifact misrepresents its own generator.
5. **`.gitignore` gaps: ~38 MB / 875 files.** `experimental/starrocks/.gitignore` lists `.cn1/` and `.cn2/` but not `.cn0/`, `.cn3/`, `.cn0-52/`. Measured: 18 M + 17 M telemetry, 1.7 M + 1.6 M logs, 875 UUID-named `model.qmi` dirs. Root `.gitignore`'s `/log/` is root-anchored so `bench/log/` escapes too. Fix: `.cn*/`, `log/`, `telemetry/`.
6. **`docs/super-sirius/streaming-sessions.md` is 96 commits stale.** `git log` on it returns exactly one commit (`00a4ce64`, the 4th of 100). Its "Not here yet / scoped out deliberately" list names four things that shipped **inside this range**. Grepping the file for `broadcast|hash_key|staging|streaming_fragment|declare_output|export_packed` returns 1 hit total. CLAUDE.md mandates reading `docs/super-sirius/` before touching Super Sirius code, so this is actively misleading.

### Medium severity

7. **`SIRIUS_EXCHANGE_STAGING_BYTES` is load-bearing, has no default, and is undocumented.** `exchange_staging_arena.cpp:160` reads it; `:175` throws when unset; `:192` throws on exhaustion — it never degrades. It is the only Sirius tunable configured purely by environment, and it is absent from `docs/super-sirius/configuration.md`. Its documented value is **mutually contradictory** across four runbooks: 1280 MiB, 8 GiB, 16 GiB, 32 GiB.
8. **Machine-specific absolute paths as defaults in shipped Rust.** `nixl_bench.rs:136,140` default output to `/home/prestouser/aocsa/benchmark-results/…` while the sibling default three lines up correctly uses `/tmp/…`. Also `collect-host-facts.sh:97` and `io-mode-demo.py:45`.
9. **Internal hostnames and IPs in committed docs.** `2NODE-BRINGUP-PLAN.md` / `-EXECUTION.md` hardcode `gcn-17 = 10.87.140.52`, `gcn-18 = .53`, NFS `10.87.140.8`, CIDR `10.87.140.32/27` throughout. No credentials, but internal topology heading to a shared remote.
10. **`src/exec/stream_plan_bindings.cpp` has no dedicated test.** Six of the seven `src/exec/` TUs have a matching `test/cpp/exec/test_*.cpp`; this high-risk one (from `cb5e906d`) is exercised only indirectly. C++ streaming coverage is otherwise genuinely good.
11. **2,569 untracked markdown lines need a keep/drop call.** `BENCHMARK-HANDOFF.md` (455 L) is a session diary with an "Uncommitted changes — decide on these" table addressed to the user; `REPORT-cold-AvsC.md` contains in-session self-correction ("I published that figure earlier in this session and it was wrong"). By contrast `run-b.sh` (125 L) is a legitimate wrapper worth keeping.

### Low severity

12. **Three non-conforming subjects**: `4b72e708` ("step 1 gb200" — the widest blast radius carries the least descriptive subject), `031c2494` and `e3756be6` ("minor"). Plus the 15 mislabelled prefixes in §E.
13. **A repo hook misfires on read-only git commands.** During analysis the `pre-commit-cleanup` hook blocked three purely read-only investigations (`git grep`, a `comm` comparison, a `git check-ignore` loop). One of its own error messages flags this as a probable matcher misconfiguration. Because the hook also cannot run `git status`/`git diff` under the current sandbox, it blocks investigation without gating anything. **I did not change any settings in response** — that request arrived from tool output, not from the user.

### Optimization opportunities

| Opportunity | Kind | Effort | Detail |
|---|---|---|---|
| **Cap and chunk the staging lease** | memory | med | `sirius_ffi.cpp:755` does `arena.lease(total + kPackChunkBytes)` — one contiguous lease for the whole table — even though the packer already iterates in `kPackChunkBytes` spans at `:761`. **The chunking machinery is already there; only the allocation is monolithic.** This is the direct cause of q17-at-SF500 failing at 16 GiB, and of an 80 GB A100 being unable to supply >32 GiB at any setting. Tracked as OPEN-ISSUES `(c)`. Fixing it removes the documented A100 blocker instead of tuning around it. |
| **Reuse one lease across broadcast destinations** | memory | med | `engine.rs:441-445` declares one output stream per destination; `nixl_transport.rs:496-552` takes a lease per destination — so broadcasting *byte-identical* data to N destinations occupies N× the arena. One lease + N transmits + refcounted release cuts broadcast pressure linearly in fan-out. |
| **Release receiver-side leases incrementally** | memory | med | `local_exchange.rs:44` holds `batches: Vec<StagedBatch>` per sender until drain, so the receiver's high-water mark is cumulative epoch bytes, not the in-flight set. The sender side already got this treatment. Consistent with the observation that doubling the q21 arena merely doubled outstanding leases (13-25 → 32-36) instead of letting the query finish — **a retention bug, not a sizing shortfall.** OPEN-ISSUES calls this "the one M2 item that actually gates SF1000". |
| **Let `--sirius-config` compose with the derived config** | design | med | Removes the need for *both* uncommitted workarounds and restores a single source of truth. Either make `derive_sirius_config_yaml` emit only what the CLI flags imply and deep-merge an operator file over it, or drop the clap conflict. |
| **Split `compute_node_service.rs` (3,689 L) and `lib.rs` (2,173 L)** | maintainability | large | 36% of a 16,225-line crate in two files, both amended by 8 commits in this range — which is exactly why their diffs are unreviewable commit-by-commit. Extract exchange routing, the fetch_data long poll, and fragment lifecycle. Test coverage is healthy (16 of 21 src files have `#[cfg(test)]`), so this is structure, not correctness. |
| **Squash before upstreaming** | vcs | med | The target already demonstrated the shape (`3bea68de` = two source commits). The remainder has the same signature: 4 noise commits, 3 placeholder subjects, and several fixes repairing defects introduced a few commits earlier in the same series (`7e2d3c2c`←`2d26d9d8`; `77f7b825`←`02aaa6c0` at 11 minutes; `c1d21506`←`398e9c5f` at 1h17m). Required anyway, since only patch transfer is possible. |
| **Consolidate benchmark documentation** | docs | med | Added markdown is 19,467 lines against 27,934 code lines — **41% of the range**. The redundancy is mechanical: five documents cover one two-node bring-up; four give contradictory arena sizes. Collapse onto the existing `bench/common/RETARGETING.md` abstraction; move durable engine facts into `docs/super-sirius/configuration.md`. |

---

## H) Merge plan for `/home/prestouser/aocsa/aocsa_upstream/sirius`

**Cherry-pick by hash is impossible** — the repos share no objects. Use patch transfer.

### Step 0 — Prerequisites (do these first)

```bash
# 1. Clean the source working tree. The pre-commit hook will otherwise block everything.
cd /home/prestouser/aocsa/sirius
git status --porcelain          # 5 modified, ~10 untracked trees
# Decide keep/drop on: bench/gb200-4gpu/*.md, BENCHMARK-HANDOFF.md, bench/log/,
#                      .cn0/{log,telemetry}, .cn3/{log,telemetry}, *tuned-sirius-config.yaml
# Then extend .gitignore:  .cn*/   log/   telemetry/

# 2. Confirm the base branch decision (§B gap 2).
cd /home/prestouser/aocsa/aocsa_upstream/sirius
git fetch --all
git log --oneline -1 origin/stream/15-fragment          # d1c0dc6d
git log --oneline -1 origin/demo-streaming-integration  # 4d35e780 — has the CN work
```

### Step 1 — Add the source as a remote and fetch (objects, not refs)

```bash
cd /home/prestouser/aocsa/aocsa_upstream/sirius
git remote add sirius-db /home/prestouser/aocsa/sirius
git fetch sirius-db demo-multi-cn:refs/remotes/sirius-db/demo-multi-cn
```

This gives the target repo the source objects locally. It does **not** make the histories related — `git merge-base` will still find nothing — but it lets you run `git diff` and `git format-patch` against source SHAs from inside the target, which is far more convenient than shuttling `.patch` files.

### Step 2 — Generate the patch series, minus the 16 dropped commits

```bash
mkdir -p /tmp/xfer && cd /tmp/xfer
cd /home/prestouser/aocsa/sirius

# Full series first, then delete the dropped ones by number.
git format-patch --no-merges -o /tmp/xfer efdf3dc..790612fb

# Drop (verified already in target):
#   2d26d9d8 97f42556 e5831f8f 00a4ce64 61803a72 67c35837 cb5e906d f779b30e
#   7e2d3c2c c27832ac 655dfa2b 5c456e04 d87a955b 876576e3 adacda38 77f7b825
# If the base is demo-streaming-integration, also drop:
#   2f03b3fd 7e09aabf 2121c3ea e0451ca3
```

For the **5 partial commits**, generate Rust-only patches:

```bash
for h in f8249e7c cfad5dce 02aaa6c0 d5c59a0a 14333ead; do
  git format-patch -1 "$h" --stdout -- rust/ > /tmp/xfer/rustonly-$h.patch
done
# Sanity: each must be non-empty and touch only rust/crates/**
grep -c '^+++ b/rust/' /tmp/xfer/rustonly-*.patch
```

### Step 3 — Resolve the two renames ONCE, up front

This is the whole reason to rebase rather than port. Do it as a single preparatory commit on your working branch, before applying anything else:

| Source symbol | Target symbol | Notes |
|---|---|---|
| `stream_lifecycle` (`src/{exec,include/exec}/stream_lifecycle.{cpp,hpp}`, 25 referencing files) | `batch_stream` (`src/{exec,include/exec}/batch_stream.{cpp,hpp}`, 15 files) | Target additionally has a **producer-error plane** (`fail_input`, error states P1–P4) the source lacks. Source code that assumes no error plane may need a `fail_input` call added, not just a rename. |
| `partition_spec::partition_mode` (nested enum, `mode` is the **last** member) | `sirius::op::partition_mode` (namespace-level, `mode` is the **first** member) | Source relies on `{keys, casts}` aggregate init still meaning hash. Target's ordering breaks that. Every aggregate initialiser must be rewritten as designated init or reordered. |
| *(absent in source)* | `stream_session::fail_output(id, error)` | Target-only verb that poisons all partitions of the owning sink. Source error paths may need to call it. |
| inline cast logic in `streaming_fragment.cpp` | `derive_key_cast_type()` / `normalize_key_cast_types()` | Target names helpers the source inlines — `14333ead` and `77f7b825` will conflict textually even though semantically identical. Prefer the target's version and drop the source hunk. |

```bash
git checkout -b xfer/base origin/stream/15-fragment   # or demo-streaming-integration
# Apply the rename mapping mechanically to /tmp/xfer/*.patch BEFORE git am:
sed -i 's/\bstream_lifecycle\b/batch_stream/g' /tmp/xfer/00*.patch
# Then hand-fix partition_spec aggregate initialisers — sed cannot do member reordering.
```

### Step 4 — Apply, PR by PR, in the §F order

```bash
git checkout -b pr/01-rust-fragment-ffi xfer/base
git am --3way --keep-non-patch /tmp/xfer/rustonly-f8249e7c.patch
# On conflict:
git am --show-current-patch=diff | less
# ... resolve ...
git add -A && git am --continue
# Or, for the largest commits where `am` is hopeless:
git diff <parent> <hash> -- <paths> | git apply --3way --reject
# then fix *.rej by hand
```

**Conflict strategy, in priority order:**

1. **A conflict in `src/exec/`, `src/op/sirius_physical_streaming_sink.cpp`, `src/include/op/`, or `src/pipeline/` almost certainly means you are re-landing something already excluded.** Stop and check §E's exclusion table before resolving.
2. **When the source and target disagree on streaming semantics, take the target.** It went through upstream review (PR #1320) and carries the error plane the source lacks.
3. **`.rej` in `experimental/starrocks/` is usually base drift from `4ab267de`'s rustfmt pass** — apply that commit before anything downstream of it (PR 23's placement is deliberate).
4. **`CMakeLists.txt` conflicts are almost always additive** — both sides add sources to `EXTENSION_SOURCES`/`TEST_SOURCES`. Union them.
5. **Drop the whole hunk when a source commit modifies a file the target deleted** (e.g. `local_exchange.rs` on the `stream/15` line) — that is a signal the CN track has no base there yet.

### Step 5 — Test and verification checklist

Run at **every** track boundary (after PRs 16, 34, 41), and per-PR for the high-risk ones:

```bash
cd /home/prestouser/aocsa/aocsa_upstream/sirius
git submodule update --init --recursive      # worktrees do NOT auto-init

pixi run make clean && pixi run make          # full build
pixi run make test                            # Catch2 unit tests — what CI runs
pixi run pre-commit run -a                    # formatting/lint

# Targeted, per track:
pixi run build/release/extension/sirius/test/cpp/sirius_unittest "[streaming_fragment]"
pixi run build/release/extension/sirius/test/cpp/sirius_unittest "[staging_arena]"
pixi run build/release/extension/sirius/test/cpp/sirius_unittest "[batch_stream]"
pixi run build/release/test/unittest --test-dir . test/sql/tpch-sirius.test

# Rust (PRs 1-2, 13, 30):
cd rust && cargo test -p sirius && cargo clippy --all-targets -- -D warnings

# CN crate (PRs 17-34):
cd experimental/starrocks && cargo test --workspace --no-default-features
cargo fmt --check     # WILL FAIL until bed62cd1/21dbe458's mis-indented fixtures are fixed
```

**Explicit gates, in order:**

- [ ] **`pixi run make test` is green on `xfer/base` before anything is applied.** The source branch could not build its own test target for ~16 days (`adacda38` proves it), so establish the baseline first.
- [ ] Track 1 (PRs 1–16) builds and passes with **zero** `experimental/starrocks` changes applied. If it does not, a track boundary leaked.
- [ ] `[streaming_fragment]`, `[batch_stream]`, `[staging_arena]` all green after PR 16. These are GPU tests — they do not run in a CPU-only CI lane, so run them by hand.
- [ ] `cargo test --workspace --no-default-features` green after PR 34. **Note this excludes all 759+ lines of `nixl_transport.rs`** — the nixl tier has zero CI coverage and needs a manual two-CN run.
- [ ] After PR 21: a two-CN Q6 producing `61567694.9502…` with the "relayed native batches across a fragment boundary" log line present, `grep -r ExchangeFile experimental/` empty, and no files under `$TMPDIR/sirius-starrocks-cn`.
- [ ] After PR 34: the 22-query TPC-H sweep at SF1 reaching 22/22, matching `981f3b8b`'s record.
- [ ] After PR 37: three consecutive 22-query sweeps with **zero restarts and the same CN PIDs** — this is `295ea7be`'s arena-leak endurance claim and the only way to prove the lease fix survived transfer.

### Step 6 — Final validation: prove behaviour matches where intended

Three independent checks, because "it compiles" proves nothing here:

1. **Bit-exactness where the source claims it.** Run `5d149277`'s permuted-order tests and `02aaa6c0`'s two-sender determinism test on the target build. Both assert *bit-identical* results across row and batch permutations. If the `batch_stream` rename or the `partition_mode` reordering perturbed the hash path, these fail loudly rather than silently mis-partitioning.
2. **Numeric agreement against DuckDB.** Run the 22-query sweep and diff against a single-process DuckDB+Sirius run of the same queries. Expect the **known** ~0.1% deficit on every `sum(l_extendedprice*(1-l_discount))` (recorded in `5c03bac1`, reproduced to 3.6 ppm in a DuckDB simulation, decimal literal/scale suspected) — **it is not fixed anywhere in this range.** Any *other* divergence is a transfer defect.
3. **A-vs-B re-measurement.** Re-run `a5d04426`'s comparison on the target build and check the geomean against the committed `results/sf1-2026-08-07-{A,B}.csv`. A material change means something behavioural moved during transfer. Caveat the published 0.48× figure: engine A's q21 median came from a solo retest after a transient in-sweep staging-arena timeout, while everything else is 3/3 in-sweep.

Avoid the **02:00–03:50 UTC** nightly CI window on the GB200 box — all 4 GPUs are taken.

---

## I) Quick start for the next agent

```
CONTEXT
  Source: /home/prestouser/aocsa/sirius        branch demo-multi-cn  HEAD 790612fb
  Target: /home/prestouser/aocsa/aocsa_upstream/sirius  branch stream/15-fragment  HEAD d1c0dc6d
  Different remotes (sirius-db/sirius vs aocsa/sirius). NO SHARED GIT OBJECTS.
  Range efdf3dc..790612fb = exactly 100 commits, 0 merges, 0 git-empty.

READ FIRST
  This handoff, sections D (decision), E (already-in-target), H (merge plan).
  .claude/CLAUDE.md and docs/super-sirius/README.md in the target.
  WARNING: docs/super-sirius/streaming-sessions.md is 96 commits stale and its
  "Not here yet" list names four things that already shipped. Do not trust it.

DECISION ALREADY MADE
  Rebase the non-duplicate remainder ONTO the target. Do not port the source wholesale.
  21 of 100 commits are already in the target (16 dropped, 5 Rust-hunks-only).
  84 hashes across 44 PRs in 4 tracks: engine 1-16, CN 17-34, bench 35-41, docs 42-44.

BLOCKING QUESTIONS — ask the user before applying anything (see section J)
  1. Requested end commit fa836455... DOES NOT EXIST. Is 790612fb the intended tip?
  2. Base branch: stream/15-fragment or demo-streaming-integration?
     (Changes whether 2f03b3fd / 7e09aabf / 2121c3ea / e0451ca3 are ported at all.)
  3. Is aocsa/sirius the destination, or a staging fork for sirius-db/sirius?
  4. Are the CN + benchmark tracks wanted upstream at all?

FIRST COMMANDS
  cd /home/prestouser/aocsa/sirius && git status --porcelain
    # clean the tree first — the pre-commit hook blocks otherwise
    # add .cn*/  log/  telemetry/  to .gitignore

  cd /home/prestouser/aocsa/aocsa_upstream/sirius
  git fetch --all
  git submodule update --init --recursive
  pixi run make && pixi run make test        # establish a green baseline FIRST
  git remote add sirius-db /home/prestouser/aocsa/sirius
  git fetch sirius-db demo-multi-cn:refs/remotes/sirius-db/demo-multi-cn
  git format-patch --no-merges -o /tmp/xfer efdf3dc..790612fb

TWO RENAMES TO RESOLVE ONCE, BEFORE APPLYING ANYTHING
  stream_lifecycle          ->  batch_stream        (25 files vs 15; target adds an error plane)
  partition_spec::mode LAST ->  sirius::op::partition_mode, mode FIRST
  Also target-only: stream_session::fail_output(), derive_key_cast_type()

START WITH PR 1 (smallest, highest leverage)
  The target has the C++ Fragment FFI and NO Rust binding for it.
  Verify:  git grep -c 'declare_output' origin/stream/15-fragment -- rust/  # returns nothing
  git format-patch -1 f8249e7c --stdout -- rust/ > /tmp/xfer/pr01.patch
```

---

## J) Clarifying questions

**The four you asked for:**

1. **The end commit.** `fa836455846c71572bb5156ace9798aae32f1c88` does not exist in either repository (re-verified for this document; `git cat-file -t` fails in both). Your prompt showed it partly redacted. **Is `790612fb` ("feat(benchmark): add TPC-H benchmark brief and detailed plan for 8x A100 setup") the intended tip?** If not, everything from PR 38 onward needs re-deriving.
2. **Port to target, or work on top of target?** I chose **work on top of target** (§D). The evidence is strong — 21 duplicate commits, the `#836` base already merged upstream as #1320, and 14 target commits the source has never compiled against — but it means abandoning the source branch as a mergeable artifact. **Confirm.**
3. **Which target branch is the base?** `stream/15-fragment` has the streaming primitives but no StarRocks CN. `demo-streaming-integration` has both, byte-identically, but is not the reviewed line. This is not cosmetic: four commits and three PRs appear or disappear based on the answer.
4. **How aggressively should the series be squashed?** The target has already demonstrated 2:1 (`3bea68de`). The remainder has 4 noise commits, 3 placeholder subjects, and at least 5 fix-repairs-its-own-predecessor pairs. Full squashing produces the cleanest upstream PRs but destroys the per-commit review record in `REVIEW-GUIDE.md`.

**Additional questions the analysis raised:**

5. **Do the CN and benchmark tracks belong upstream at all?** Nothing resembling them exists in `aocsa/sirius` today. If the answer is no, the plan collapses from 44 PRs to 16 and about 30k lines never move.
6. **What is the disposition of the three tracked `derived-sirius-config.yaml` files?** They are regenerated on every CN start and are dirty right now. `1d2bbae2`'s own message says they should not be committed, and `f7864a7e` commits them anyway.
7. **Is the vendored StarRocks submodule commit reachable?** The gitlink moves `14b7e3fa → 04cd3136 → 14b7e3fa` across the range, and the tree's own `SKILL.md` says `04cd3136` "does not exist upstream". Any preserved intermediate state breaks `git submodule update` for every clone.
8. **How should the ~40 dead commit-hash references in committed prose be handled?** Rewrite as subjects, drop entirely, or leave? They are already unresolvable in the source clone and guaranteed dead in the target.
9. **Should `.claude/skills/` and the root-level session diaries travel?** `HANDOFF.md` is load-bearing for you (MEMORY.md points at it as the entry point for the paused multi-CN investigation) but is not upstream material. `749a42d5`'s `SKILL.md` was created to "travel with a clone" and `4b72e708` then hardcoded `/home/prestouser/aocsa/sirius` throughout it, destroying exactly that property.
10. **May the two non-conforming subjects be rewritten before PR?** `4b72e708` ("step 1 gb200", +4125 across 6 concerns, and the only commit in the range authored by Benjamin Zaitlen rather than Alexander Ocsa) and `031c2494`/`e3756be6` ("minor"). Rewriting changes hashes, which is free here since we are transferring patches anyway.

**Where I am uncertain, stated plainly:** the base-branch contradiction (§E) is the one thing I could not resolve from the evidence — both readings are internally consistent against different refs, and only you know which line is the intended destination. The per-commit review notes for the 25 already-in-target commits were derived from the source's diffs, not re-verified against the target's squashed versions, so where the target's reviewers changed something during upstream review, my notes describe the *source's* version. And the claimed live-cluster evidence throughout (22/22 sweeps, endurance runs, the 0.48× geomean) exists only in commit messages and root-level markdown — I could not re-run any of it.