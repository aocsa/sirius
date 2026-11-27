---
name: distributed-sirius-bench
description: >
  Run a query set on Distributed Sirius through its gateway, one rank per GPU, on one node or
  several, with or without Slurm: TPC-H by default, or any dataset with a folder of .sql files.
  Plan, preflight, build, a smoke test, then the full benchmark, with the Sirius configuration
  shown and approved before anything starts. Use it for a distributed smoke test or benchmark of
  a branch. For a single-process run, use the benchmark skill.
disable-model-invocation: true
---

# distributed-sirius-bench

Runs a query set on Distributed Sirius: one gateway, one engine process (a rank) per GPU, SQL sent
through the client. The query set is TPC-H by default, or any dataset you point it at (section
1a). `scripts/bench.py`, next to this file, does the mechanical work and keeps
everything about a run in one run folder. Your part is to collect the inputs, get the user's
approval at each gate, and read the results critically.

This skill is for runs through the gateway ([Distributed experiments](../../../docs/super-sirius/distributed-experiments.md)).
For a single-process run with `performance_test.py`, use the `benchmark` skill.

The gates, in order: **collect, confirm, preflight, build, show the config, smoke test, full
benchmark, report.** Do not skip one. GPU nodes are shared and a full benchmark can hold them for
hours, so nothing that uses a node starts before the user has agreed to it in this conversation.
The script enforces this: without a terminal, every such step refuses to run unless it gets
`--yes`, and you pass `--yes` only after the user approved that step.

## 1. Collect the inputs

Ask the user for each of these. Do not guess, and do not reuse a value from an earlier run or from
memory without confirming it: a wrong dataset or branch produces a number that looks valid.
Where a question tool exists (AskUserQuestion in Claude Code), use it; otherwise ask in plain text.

| Input | Flag | Notes |
|---|---|---|
| Branch to test | `--branch` | |
| Benchmark | `--benchmark tpch` or `custom` | TPC-H (default), or any dataset and queries: section 1a |
| Dataset location | `--data` | A folder with one subfolder of Parquet files per table |
| Scale factor | `--sf` | TPC-H: required; the smoke test checks it against the `supplier` row count, and Q11 uses it. Custom: optional, shown in the reports |
| Execution mode | `--mode single` or `--mode multi` | Single: one node, all its GPUs. Multi: several nodes, all GPUs of each. For multi also ask which nodes (`--nodes a,b`) or, with Slurm, how many (`--num-nodes`) |
| Sirius configuration | `--config` | Run `bench.py configs` and show the list: a preset, the user's own YAML file, or `engine-defaults` |

You also need to know where things go. Ask if the conversation does not already say:

| Input | Flag | Notes |
|---|---|---|
| Checkout folder | `--checkout` | Used if it exists, cloned there if not. On a file system every node mounts. The checkout you are working in is the natural choice |
| Run folder | `--out` | New, visible from this host and from the nodes, usually under `$HOME`. With the `local` launcher a machine that does not see it gets its own at the same path |
| Launcher | `--launcher slurm` or `local` | Slurm whenever Slurm schedules the nodes. `local` is only for machines it does not: section 1c |
| Slurm partition, site options | `--partition`, `--sbatch-args`, `--time-limit` | |
| Spill folder | `--spill-dir` | Needed by the presets; on a disk with free space |
| Large output | `--big-dir` | The client output reaches gigabytes at large scale factors. Keep it off a `$HOME` near its quota |
| Extra environment | `--env KEY=VALUE` | For the gateway and the ranks. Where `$HOME` has a quota, `--env PIXI_CACHE_DIR=<a folder on scratch>` keeps Pixi's cache out of it |

Other flags have defaults that rarely change: `--runs 2` (cold, then warm), `--queries` (TPC-H:
`1-22`), `--smoke-queries` (TPC-H: `1,6,12`), `--statement-timeout`, `--setup-sql`,
`--env KEY=VALUE`. `bench.py plan --help` lists them all.

Five more shape the run itself. A reference run (section 1b) sets them from its record:

| Flag | Notes |
|---|---|
| `--commit` | The commit to test. A new clone is checked out at it; an existing checkout must already be at it |
| `--order pass` | All queries once, then all again, instead of the runs of a query in a row |
| `--sessions '1-8;9;10-22'` | Instead of `--queries`: each group is one job with new ranks. The report adds them up |
| `--results parquet` | A second client writes each result as a Parquet file under `--big-dir`, so a query's time has no printing of rows in it. The time then includes the fetch and the write of the file |
| `--exclusive` | With Slurm: the job takes its nodes exclusively. Without it another job can share the node's CPUs |

## 1a. Any other dataset

With `--benchmark custom` the script makes no assumption about the tables or the queries:

| Input | Flag | Notes |
|---|---|---|
| Queries | `--queries-dir` | Required. A folder of `.sql` files, one query per file. A query's name is its file name without `.sql` (letters, digits, `_`, `.`, `-`) |
| Which queries | `--queries` | `all` (default), names (`q7,q12`), or numbers and ranges (`1-5`), which match the number at the end of a file name |
| Smoke test | `--smoke-queries` | Same forms. Default: the first three queries, in natural order |
| Tables | `--tables` | Default `auto`: one view per subfolder of `--data` that holds Parquet files, named after the subfolder. Or a comma-separated list |
| Views | `--views-sql` | Instead of the generated views, a file whose statements create the tables or views: CSV, another layout, a DuckDB file. `--data` is then optional |
| Report name | `--name` | Default: the dataset folder's name |

The TPC-H specifics (the Q11 fraction and the `supplier` row-count check) apply only to
`--benchmark tpch`. A custom smoke test passes on the other checks: no error, a time for every
query, every rank up and ready. So ask the user how they will know the answers are right (a
reference engine, expected files), and say in the report if nothing checked them.

Run the script on a host where it can start jobs: with Slurm, one where `sbatch` works. If that is
not the host you are on, run every command below there (for example over `ssh` to the head node,
never to a compute node). The skill folder and the run folder must be visible from that host.

## 1b. A run of the Reference numbers table

`bench.py reference` lists the 18 runs of the
[Reference numbers](../../../docs/super-sirius/distributed-experiments.md#reference-numbers)
table: `sf10k`, `sf30k` or `sf100k`, on `n1`, `n2` or `n4` nodes, with `gds` or `uring` (io_uring)
scans. It also lists the two one-node GDS runs of that file's "One node since the join-side
commits" section, `sf30k-n1-gds-35f5573` and `sf100k-n1-gds-35f5573`. `plan --reference <name>`
takes what made that number from the run's record in `reference/runs.json`, so ask the user only
for the site inputs:

```bash
python3 <this folder>/scripts/bench.py plan --reference sf30k-n2-gds --data ... --checkout ... \
  --out ... --spill-dir ... [--big-dir ...] [--nodes a,b] [--partition ...] --yes
```

| From the record | What the plan gets |
|---|---|
| Commit | `--commit`: `147b0f3` for SF10000 and for SF30000 on one node, `93893eb` for the other runs of the table, `35f5573d` for the two `-35f5573` runs. Give a checkout that is at it, or a new folder to clone into |
| Config | A `ref-gb200-*` preset, plus the run's GDS threads and rate cap per rank for a GDS run |
| Environment | `SIRIUS_GDS=1` and the UCX settings of the `ref-gb200-*.env` files, for io_uring runs too. `UCX_NET_DEVICES=bond0` is that cluster's; elsewhere give `--env UCX_NET_DEVICES=...` |
| SQL | `SET parquet_footer_statistics = true` first. SF30000 and SF100000 use the query texts in `queries/tpch-standard` (the standard parameters), SF10000 the checkout's `orig` folder |
| Order | `--order pass` for five of the SF30000 runs; Q9 last for the others |
| SF100000 | Five sessions, one run per query, `--results parquet`. Give `--big-dir` |
| Launch | `--exclusive`: the recorded runs had their nodes to themselves |
| Statement timeout | 300 s, 600 s or 900 s by scale |

- `plan` prints the recorded total and, under "its dataset", the dataset the run read. `--data`
  must be that dataset: the same scale factor with DECIMAL money columns or other row groups
  gives other times.
- A flag the user gives replaces the record's value. The run is then not the reference run any
  more: say so in the report.
- The SF100000 preset sets `enable_build_side_swap`, which exists only up to the recorded commit:
  a later commit refuses the config. To test a later commit with these settings, give
  `--commit` and a copy of the preset without that key as `--config`.
- A GDS run follows the GDS rule at the end of this file: the runbook, the probe, the storage
  admins.
- An SF100000 run is five jobs, one after another; each has its files in `full/s1` to `full/s5`.
  A session that fails stops the ones after it. Find the cause, then plan the sessions that are
  left in a new run folder with `--sessions`; do not rerun in a loop.
- `summary.md` of the full run repeats the recorded total. Compare it with "best" for SF10000 and
  SF30000 and with run 1 for SF100000, and read the record's `note` first: `sf100k-n1-gds` is
  a composite of eight sessions, and `sf100k-n4-uring` counts Q5 from a rerun.

## 1c. Machines without Slurm

For machines reached over `ssh` (workstations, lab nodes), the `sirius-cluster` skill prepares
them and this skill runs the benchmark. `cluster.sh init` records the hosts and their addresses
and writes a rank config, `setup` builds the branch on each machine, `data` generates TPC-H, and
`nodes` and `status` show what runs there. Its cluster files give the inputs of `plan`:

| In `clusters/<name>.env` | For `plan` |
|---|---|
| `ZENO_ADDRS` | `--nodes`, comma-separated; the first is the machine `bench.py` runs on |
| `ZENO_NET` | `--env SIRIUS_GATEWAY_PRIORITY_NETWORKS=<CIDR>` |
| `ZENO_WORK` | `--checkout <work>/sirius`, `--out <work>/runs/<name>`, `--spill-dir <work>/spill` |
| `ZENO_BRANCH`, `ZENO_REPO` | `--branch`, `--repo` |
| `ZENO_SF` | `--sf`, and `--data <work>/tpch_sf<SF>` (or `ZENO_DATA`, where the file sets it) |
| `clusters/<name>.yaml` | `--config`, once it is on the first machine (`cluster.sh push`) |

- Run `bench.py` on the first machine, for example with `cluster.sh sh <first> '...'`: this skill
  is in that machine's checkout.
- The first machine needs `ssh` without a password to the others. Every machine needs the
  checkout, the build, the data, Pixi and `$HOME` at the same paths.
- The machines may share `$HOME` and the run folder, or each have its own. A machine with its
  own gets the run folder, `config.yaml` and the gateway's `gateway.conf` copied to it, and its
  rank logs are copied back at the end.
- `build` builds on the first machine only. Machines with their own file system each need a
  build at the same commit: `cluster.sh setup`, then compare the `checkout:` lines of
  `cluster.sh nodes`.
- `bench.py` takes Pixi from `$PIXI_HOME/bin`, or from `~/.pixi/bin`. Where `$HOME` is shared with
  machines of another architecture, export `PIXI_HOME` as the folder that holds this machine's
  Pixi before every `bench.py` command.
- A checkout that is not on the branch (a detached commit) needs `--commit` with its commit.
- `cluster.sh nodes` before the first gate, `cluster.sh status` after the last.
- **AWS instances** (launched with the `aws-ec2` skill) are such machines too:
  `cluster.sh` drives them through an alias file (`SSH_CONFIG`, see the
  `sirius-cluster` skill). `--nodes` takes the private addresses, which is what
  `init` records. `--checkout`, `--data`, `--out` and `--spill-dir` go on the
  instance store. The security group must allow traffic from itself (the gateway
  listens on 9494, rank i on 9500 + i and 27000 + i). No
  `SIRIUS_GATEWAY_PRIORITY_NETWORKS` and no `tcp_lanes` was needed. Copy the run
  folder off before the instances are terminated: the instance store goes with
  them.

## 2. Plan and confirm

```bash
python3 <this folder>/scripts/bench.py plan --branch ... --data ... --sf ... --mode ... \
  --config ... --checkout ... --out ... [--nodes ...] [--partition ...] [--spill-dir ...] --yes
```

It validates the inputs, writes `plan.json` and the resolved `config.yaml` into the run folder, and
prints the plan. Show that output to the user and ask whether every line is right. If one is not,
run `plan` again with `--replace`. A person at a terminal can run `plan` with no flags and answer
its questions instead.

## 3. Preflight and build

```bash
python3 .../bench.py preflight --out <run folder> --yes   # short checks on the nodes
python3 .../bench.py build     --out <run folder> --yes   # clone if absent, then build
```

- Preflight checks the tools on this host and, on each node, that every table has Parquet files
  and that the run folder is visible or can be made there. It counts the GPUs, which gives the number of ranks. For a
  custom benchmark it also finds the tables (with `--tables auto`) and resolves the query
  selection against the files in `--queries-dir`; show the user the lists it prints.
- Build reuses a build that is already there and says which commit it is. It builds only when
  there is none, or with `--rebuild`. A fresh build takes 35 to 40 minutes on 144 cores, so under
  Slurm it runs as its own job on one node.
- The script never switches the branch of an existing checkout. If the checkout is on another
  branch it stops and says so; let the user decide.
- If the clone or `pixi` fails for lack of network on the compute nodes, clone and run
  `pixi install` where there is network, then run `build` again.

## 4. Show the Sirius configuration, then the smoke test

```bash
python3 .../bench.py show  --out <run folder>
python3 .../bench.py smoke --out <run folder> --yes
```

`show` prints the plan again, now with the rank count and the build's commit, followed by the full
Sirius configuration: the YAML file and the environment variables the gateway and ranks will get.
Put that in front of the user and wait for a yes before the smoke test.

The smoke test starts the gateway and the ranks, runs a few queries once each, and passes only if
all of these hold: the client exited cleanly, no statement returned an error, every query has a
time, every rank is `up` and `ready` in the final status, and, for TPC-H, `supplier` has
`10000 x SF` rows.
`--dry-run` writes the launch script and the SQL without starting anything, which is a good way to
show the user exactly what will run. It leaves the `smoke` folder behind, so the real smoke test
after it needs `--rerun`.

If it fails, stop and diagnose (section 7). Do not go on to the full benchmark, and do not rerun
in a loop: after a job whose ranks were killed, wait until the node is idle again.

## 5. Full benchmark

Tell the user the smoke result and ask before starting:

```bash
python3 .../bench.py full --out <run folder> --yes
```

It refuses to run unless the smoke test passed for this plan. A full run can take from minutes to
hours, longer than a tool call may wait. Start it in the background, with its output in a file,
and look at progress with `report`, which works while the run is going:

```bash
nohup python3 .../bench.py full --out <run folder> --yes > <run folder>/full.log 2>&1 &
python3 .../bench.py report --out <run folder>
```

Interrupting the script cancels the Slurm job (or stops the gateway and ranks). If you stop it any
other way, check that nothing is left: `squeue -u $USER`, or on a machine without Slurm
`nvidia-smi --query-compute-apps=pid --format=csv,noheader` must print nothing.

The smoke test stops at its first failed statement. A full run does not: the queries after a
failed one still run, so one failure does not hide the next. The run still ends as failed, and
with `--sessions` the sessions after it do not start.

## 6. Report

`summary.md` and `timings.csv` are in `<run folder>/smoke/` and `<run folder>/full/`. Give the user:

1. Pass or fail, and every line under "Problems", quoted.
2. The totals: run 1 (cold), run 2 (warm) and best of both, with the per-query table.
3. What was run: branch and commit, nodes and ranks, dataset and scale factor, the Sirius config.
4. What was not checked. By default that includes the answers.

Read the numbers before passing them on. A query far slower than its neighbours, or than an
earlier run, is more often storage or memory than the engine: look in the engine logs under
`<run folder>/<kind>/rank-logs/` for `[gds] held:` lines (a storage stall) and for the exchange
path of each pair of ranks (on GB200 NVL it should read `NIXL over the NVLink fabric`). Single runs
on shared storage differ by around ten percent between days and nodes, so do not call a smaller
difference a change.

**Check the answers.** Times mean little if the answers are wrong, and `smoke` and `full` do not
compare them. With a reference for the same data and the same query texts, `check` does:

```bash
python3 .../bench.py check --out <run folder> --expected <folder> --yes
python3 .../bench.py check --out <run folder> --ref-client <reference client output> --yes
```

- `--expected` is a folder with one answer per query, `q01.parquet` or `q01.csv` (CSV with a
  header); `--pattern` gives another file name. `--ref-client` is a client output with the same
  markers.
- It runs the checkout's `scripts/compare_answers.py` in a short step on a node, without a GPU:
  rows in any order, numbers within 1e-6, and a LIMIT tie in Q3 or Q18 counts as a match. With
  `--results parquet` it reads the answer files; an integer sum can be a DOUBLE there, so whole
  numbers are printed as integers where the reference column holds integers, and the "seconds"
  of its table are the time to read the file, not the query's. It checks the full run, or the
  smoke run if there is no full run (`--kind`).
- The result is in `<run folder>/<kind>/check/result.txt`, and `report` then says in
  `summary.md` whether the answers match. `check` exits 1 if one differs.
- Answers depend on the query parameters: expected answers for the standard TPC-H parameters fit
  the texts in `queries/tpch-standard` (the SF30000 and SF100000 reference runs), not the `orig`
  texts of the SF10000 reference runs.

A reference can also be the same `run.sql` (without its last line) run through the build's plain
DuckDB on the CPU; at SF10000 that needs about 15 minutes and 1 TB of memory, so run it as its own
job and ask first. If no reference exists, say in the report that the answers were not checked.

## 7. When something fails

| Symptom | Where to look |
|---|---|
| `preflight failed` | The lines under it name the node and what is missing |
| `job ... ended before the gateway was up` | `<run folder>/<kind>/job.out`. `rank exited` repeating there means the ranks die at start, usually from the config |
| The client output says `the gateway starts once N ranks are registered` | Fewer ranks joined than expected: `job.out`, then the engine logs |
| An `Error` line for a query | The line itself; CPU fallback is off, so a query Sirius cannot run on the GPU fails |
| Fewer ranks `up,ready` than expected | A rank died during the run: its log under `rank-logs/` |
| `supplier has ... rows` | The dataset is not the scale factor the user gave. Ask which is right |
| `sbatch failed`, or a job that never starts | The site needs other options: ask the user and pass them with `--sbatch-args` |
| `check` prints `NO REFERENCE` for a query | The expected folder has no `q<nn>.parquet` or `q<nn>.csv` for it: give `--pattern` |
| Times far slower than expected | CPUs per rank (`scontrol show job <id>`), storage stalls, the exchange path |

## Rules that come from the cluster, not from this skill

- On nodes that Slurm schedules, reach them only through Slurm. A process started over `ssh` can
  get the node drained. The `local` launcher uses `ssh` to other machines and is only for machines
  Slurm does not schedule.
- One job per node at a time. Ask before holding four or more nodes.
- Start narrow: the smoke test, then a few queries (`--queries 1,6,12`), before the whole set.
- The `gb200-gds-one-node` preset and the `-gds` reference runs read through GPUDirect Storage. Read
  `docs/super-sirius/gds-runbook.md` in the checkout first, probe the node, and tell the storage
  admins before the first GDS run on a node. The preset was written for one node; the reference
  runs on two and four nodes cap each rank's read rate (`gds.max_gb_per_s`).
- TPC-H query texts come from `<checkout>/test/tpch_performance/tpch_queries/orig/` unless
  `--queries-dir` says otherwise. Say in the report which set ran: numbers from different query
  parameters do not compare.

## What the script has and has not been tested on

- **Run for real** (2026-11-18, GB200 under Slurm, existing builds, SF10000, io_uring scans):
  - The smoke test of `plan --reference sf10k-n1-uring` on one node and of `sf10k-n2-uring` on two
    nodes. Q1, Q6 and Q12 took 14.9 s, 10.4 s and 8.8 s on one node and 9.4 s, 5.5 s and 4.6 s on
    two.
  - On one node, a smoke test and a full run of two sessions (Q1, then Q6) with
    `--results parquet`.
  - `check` against expected answers in Parquet, with the texts of `queries/tpch-standard`: a
    smoke test with CSV results, and a smoke test and a two-session full run with Parquet
    results. Q1, Q6 and Q12 matched in each.
  - Earlier that day, the smoke test of the `gb200-sf10000` preset on one node.
- **Run for real without Slurm** (2026-11-18, one x86_64 machine with four RTX PRO 6000 GPUs, an
  existing build at `18fdfca21`, SF100, the config of `cluster.sh init`): the `local` launcher's
  smoke test in single mode, 4 ranks. Q1, Q6 and Q12 took 0.61 s, 0.93 s and 0.60 s; the answers
  were not checked. For two machines that share `$HOME` and the run folder: `plan`, `preflight`
  (over `ssh`), `smoke --dry-run`, and a smoke test stopped by hand after 24 s. In it the second
  machine passed `sirius_node.sh --check` and started its four ranks; no rank had joined yet, so
  no query ran. Stopping it left the second machine's ranks running, because the checkout was a
  link; the fix (the stop also matches the real path) was tested on stand-in processes only.
- **Run for real on two machines without Slurm** (2026-11-18, two aarch64 machines with one GB10
  GPU each, each with its own `$HOME` and disk, started from a macOS control machine through
  `cluster.sh`; a new build of this branch at `5d6c3f08` and SF100 from `cluster.sh setup` and
  `data`): `plan`, `preflight`, `build` (reuse), `show`, `smoke --dry-run` and the `local`
  launcher's smoke test in multi mode, 2 ranks. The first run failed at session setup
  (`rank 0 reached 0 of 1 peers in 120000 ms`): the machines cap locked memory at 15 GiB and the
  default 16 TCP lanes pin more. With `distributed: { exchange: { tcp_lanes: 2 } }` in the
  config it passed: Q1, Q6 and Q12 took 0.98 s, 0.29 s and 0.42 s, exchange host-staged over TCP
  lanes; the answers were not checked. The second machine got the run folder, `config.yaml` and
  `gateway.conf`, and its rank log came back. `cluster.sh status` was clean on both machines
  after the failed run and after the passed one.
- **Run for real on AWS** (2026-11-18, instances of the `aws-ec2` skill driven from a macOS
  control machine through `cluster.sh` with an alias file; Deep Learning Base AMI, driver 595, a
  new build of this branch at `28d30249`, SF100 generated on the instance store, the config of
  `cluster.sh init`, every gate from `plan` to `smoke`):
  - One `g7e.8xlarge` (one RTX PRO 6000 Blackwell 96 GB, 32 vCPUs), single mode, 1 rank. Q1, Q6
    and Q12 took 0.86 s, 0.57 s and 0.72 s.
  - Two `g6.4xlarge` (one L4 24 GB and 16 vCPUs each, each with its own `$HOME`), multi mode,
    2 ranks, `--nodes` as private addresses, the default TCP lanes. Q1, Q6 and Q12 took 1.31 s,
    1.27 s and 1.65 s, exchange host-staged over TCP lanes.
  - In both, `check --ref-client` against the same `run.sql` (without its last line) run through
    the build's plain DuckDB: Q1, Q6 and Q12 matched. This is the first answer check without
    Slurm.
- **Checked with `--dry-run` only:** the 20 reference runs as a whole. For each, the script wrote
  the config, the SQL statements and their order, the statement timeout, the commit, the rank
  count and the environment, and they equal the files of the recorded run.
- **Tested only against stand-ins** for Slurm and for the client: a full benchmark of 22 queries,
  failure detection, a build from a fresh clone, and custom datasets (tables found
  automatically, a views file, queries chosen by name and by number). Also the `local` launcher's
  script on three machines, one of them with its own `$HOME` and file system: the copies of
  `config.yaml` and `gateway.conf`, and the rank logs copied back.
- **Not run at all:** four nodes, more than one instance of a `g7e` type or an instance with
  several GPUs on AWS, a dataset read from S3, `full` without Slurm, a query through the `local` launcher on machines that share
  `$HOME`, any GDS run through this script, a run at SF30000 or SF100000, `--order pass`, a new clone at
  `--commit`,
  `check` on all 22 queries or with `--ref-client` on nodes, and `--results parquet` where
  Slurm confines a step to its CPUs (the client step asks for one CPU, and fetching a large
  result needs more).

For any of the untested paths, run the step with `--dry-run` first, read the generated script with
the user, and treat the first smoke test as the test of that path.
