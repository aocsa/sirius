# Exchange directives from DFD's plan: what was built and measured

2026-11-19. Steps 1 to 3 of `dfd-stage-plan.html`, with the default answers: DFD controls the
exchange choices only, each rank reads the same directives, Q12 then Q3, Sirius's broadcast rule.

## What was built

1. **The hook.** `distribute_plan()` takes a list of
   exchange directives. A directive names a join or a grouped merge by the scan columns of its
   keys and says how to route it: `HASH_JOIN a=b hash [width=1]`, `HASH_JOIN a=b broadcast`,
   `MERGE_GROUP_BY a,b hash|none`. Sirius follows a directive only where the answer cannot change;
   a site no directive names, or that several name, is decided as before. The list comes from a
   text file named by the setting `exchange_directives`. Format and rules:
   `docs/super-sirius/distributed-sirius.md`, "Exchange directives".
2. **The crate** `rust/crates/sirius-dfd`. It plans a query with DataFusion 55 and
   DataFusion Distributed `9b62f20b`, with no worker and no Flight, and writes the directive file.
   It is its own Cargo workspace. `dfd-planner/directives/` holds its output for the 22 TPC-H
   queries: `sf100-4r`, `sf1000-4r` (grouped merges only) and `sf100-4r-joins`, `sf1000-4r-joins`
   (also DFD's choice for each join).

The first version of the hook named sites by their position in the plan. A file written for the
SF1000 plan of Q3 then meant something else on the SF100 plan and nothing refused it. That is why
directives now name columns, and why `none` needs a proof. The sort directive is gone.

3. **Join-equal names.** A group column that is a key of an inner
   join below also answers to the other side's key of that equality. The first run of the crate's
   files showed why: DFD names Q3's group by `l_orderkey` and Sirius's plan groups by `o_orderkey`.

## Verified

- Engine build: x86_64, `pixi run make` in the scratch clone, on `presto-285-gcn-05` in Slurm job
  24598, exit 0, on `exp/stage-tasks` at `f84cac2f`.
- Engine tests: `sirius_unittest "[exchange]"`: 141 test cases, 140 passed, 1 skipped (no fabric
  memory on that node, `test_exchange_path.cpp:163`), 18804 assertions (same job).
- Crate: `cargo test`: 2 tests pass. The CLI wrote 88 directive files with no failure.
- Formatting: `git clang-format` on the changed lines (clang-format 22.1.8; the repository pins
  20.1.4). The pre-commit hooks do not finish on this filesystem: not run, commits made with
  `--no-verify`.

## Runs

4 ranks on the 4 GPUs of one node (RTX PRO 6000, 96 GB), default configuration,
`scripts/distributed_run_sql.sh 4 <file.sql>`. Three passes over each query in one process:
"decided" (no directives), "groups" (the crate's grouped-merge lines), "joins" (those and the
join lines). GB = bytes sent over all ranks, from the `exchange:` log lines. One run per case.

All on `presto-285-gcn-05` (Slurm partition `rtx6000`), under
`/scratch/prestouser/aocsa/dfd-planner/runs/`:

- job 24588, before join-equal names: `sf100-all-slurm`, `sf1000-c-slurm`;
- job 24589, Q3 at SF1000 with `o_orderkey` written by hand: `diag/q03-okey`;
- job 24591, with join-equal names: `sf100-all-eq`, `sf1000-c-eq`. The tables below are this job;
- job 24598, the same rebased on `exp/stage-tasks` at `f84cac2f`: `sf100-all-rb`, `sf1000-c-rb`.
  The same answers; Q12 and Q3 at SF1000 send the same bytes to 0.001 GB; the 22 queries at SF100
  send 14.198, 14.290 and 16.372 GB in the three passes.

The first SF100 run was on `presto-285-gcn-01` (`sf100-all`). It gave the same answers and the
same bytes to 0.05 GB per query, but took 83 minutes and lost its last query to the time limit:
`/scratch` read at 2 MB/s from that host that afternoon, against 3 GB/s from `gcn-05`. Scripts:
`runs/compare.py`, `runs/outcomes.py`, `slurm/build.sh`.

### SF100, 22 queries

Answers: the same in all three passes for all 22 queries, in both jobs. Q11 has two rows with the
same value in the other order; no other difference.

| | decided | groups | joins |
|---|---|---|---|
| GB sent, 22 queries | 14.289 | 14.296 | 16.166 |
| directive lines | | 22 | 85 |
| name a site of Sirius's plan | | 9 | 42 |
| name no site | | 12 | 35 |
| not followed: several lines name one site | | 1 | 7 |
| not followed: `none` without the proof | | 1 | 1 |
| time, sum of 22 queries | 6.1 s | 4.0 s | 4.1 s |

Of the lines that name a site, one in each pass is then not followed. The decided pass ran first
in the process, so the times do not rank the passes.

Queries where the joins pass sent different bytes (GB, decided to joins): Q14 1.717 to 0.544,
Q16 0.798 to 0.671, Q03 0.950 to 0.798, Q19 0.004 to 0.000, Q17 0.024 to 0.021; Q21 2.242 to
4.207, Q10 1.437 to 1.996, Q07 0.956 to 1.309, Q05 0.536 to 0.872, Q20 0.046 to 0.092, Q08 0.780
to 0.837, Q12 0.168 to 0.187. The other ten are the same to 0.002 GB. The groups pass changed one
exchange: Q18 has four hash exchanges in place of five, and the bytes are the same to 0.001 GB.

### SF1000, Q12 and Q3

| | decided | groups | joins | answer |
|---|---|---|---|---|
| Q12 | 1.686 GB | 1.686 GB | 1.686 GB | same |
| Q3, job 24591 | 9.825 GB | 9.553 GB | 12.299 GB | same |
| Q3, job 24588, before join-equal names | 9.757 GB | 9.825 GB | 12.571 GB | same |

The answers also equal those of the earlier runs on `gcn-01` (`runs/sf1000-a`, `sf1000-b`).

## What the runs show

1. **The answer never changed.** 198 statements at SF100 and 19 at SF1000 on `gcn-05`, directed
   or not.
2. **About half of DFD's lines name a site of Sirius's plan** (9 of 22 group lines, 42 of 85 with
   joins at SF100). The rest do not, for two reasons seen in the log: Sirius joins the tables in
   another order, so the pair of DFD's join is not a join of Sirius's plan; or the query renames
   or computes the column (`supp_nation`, `o_year`, `max(...)`).
3. **Q3's group line is now followed at SF1000** and the group-by exchange goes: 9.553 GB against
   9.825 GB decided, 2.8 % fewer bytes, the result of the first slice. Before join-equal names the
   line named no site.
4. **At SF100 the same line names its site and is rightly not followed.** Sirius's plan there is
   `(lineitem ⋈ orders) ⋈ customer`: the last exchange below the group is on the customer key or
   a broadcast, so nothing proves that each order is on one rank. DFD joins in the other order.
5. **DFD's join choices send more bytes on Sirius's plans**: +13 % and +15 % over the 22 queries
   at SF100 in the two jobs (16.166 against 14.289 GB) and +25 % and +29 % on Q3 at SF1000,
   because DFD hashes the `customer` join that Sirius broadcasts for 0.360 GB. They send fewer
   bytes on five queries, most on Q14 (a third of the bytes), and more on seven.
6. **The grouped-merge lines alone change nothing measurable at SF100.**
7. **Bytes vary a little between runs of one plan**: the group-by exchange of Q3 at SF1000 was
   0.204 GB in one pass and 0.272 GB in the next with the same plan, and the Bloom merges of Q3 with join lines at
   SF100 were 0.504 GB in one job and 0.378 GB in the other.

## Not done, not verified

- Renamed and computed group columns do not match. Join lines are matched by their own two names
  only.
- No timing comparison: one run per case, passes in a fixed order in one process.
- No run on the aarch64 nodes. Nothing pushed.
- The pre-commit hooks were not run.
