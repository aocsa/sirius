---
name: sirius-cluster
description: >-
  Drive a set of GPU machines the user already reaches by ssh alias (on-prem
  nodes, lab boxes, workstations) as one small cluster, and run distributed
  Sirius on them. Use whenever the user wants to set up or pick a cluster from
  their ssh hosts, see what the nodes are doing, run a command on all or some of
  them, copy files to or from them, build the `experimental/distributed-sirius`
  branch there, or run a distributed Sirius benchmark (gateway + ranks, TPC-H)
  on machines that are not AWS instances — even if they only say "the zenos",
  "the Spark boxes", "my GPU boxes" or "run it on two nodes".
---

# sirius-cluster: any ssh hosts as a cluster, one script

`scripts/cluster.sh` drives the nodes of one cluster over plain `ssh <alias>`.
Below, `cluster.sh` means `bash <this skill's folder>/scripts/cluster.sh`.
A cluster is two files in `clusters/`: `<name>.env` (nodes, addresses, work
folder, repository) and `<name>.yaml` (the Sirius config of every rank). The
skill ships with none. `clusters/<name>.md`, where it exists, holds what was
observed on that cluster: read it before a real run there.

## First step: set up a cluster

Do this when `cluster.sh hosts` prints `clusters set up: none`, or when the user
names machines that are in no cluster.

1. **The first question is always which hosts.** Run `cluster.sh hosts`: it
   lists the clusters set up and the host aliases of the user's `~/.ssh/config`
   (alias names only). Show the list and ask: **"Which of these registered ssh
   hosts do you want to control?"** Do not guess from the names, and do not go
   on before the answer. If the command cannot read the file, ask the user to
   type the aliases. Never open or edit the ssh config yourself.
2. Ask for a short cluster name if the user gave none, and which branch to
   build if it is not `experimental/distributed-sirius`. Then
   `[ZENO_BRANCH=<branch>] cluster.sh init <name> <host,host,...>`, first host =
   the one that will run the gateway. `ZENO_REPO=<url>` picks a fork. Both end
   up in `clusters/<name>.env`, where they can be edited later; a second cluster
   over the same nodes needs the same branch given again. It only looks: one ssh per host for GPUs, memory, cores,
   addresses and missing tools, then writes the two files on this machine.
   Nothing is created or changed on the hosts.
3. Read what `init` printed and settle these with the user:
   - **A host it could not reach.** Show the ssh error. Adding a key or
     accepting a host key is the user's step, never yours.
   - **Addresses, first.** `init` records each node's default-route address
     and prints every address with its interface and link speed in Mb/s. If the
     nodes share a faster network than the default route, ask the user which
     one to use, then put one address per node in `ZENO_ADDRS` (same order as
     `ZENO_NODES`; any one of a node's addresses on that network) and the
     network's CIDR in `ZENO_NET` in `clusters/<name>.env`.
   - **`ssh to <addr>: NO (<reason>)`.** The first node must reach every other
     node, by the address in `ZENO_ADDRS`, without a password, to run more than
     one node. `init` tested the default-route address only: after changing
     addresses, test again with
     `cluster.sh sh <first> 'ssh -o BatchMode=yes <addr> true'`. If it fails,
     give the user these two commands and let them run them (never run them
     yourself):
     `ssh <first> 'cat ~/.ssh/id_ed25519.pub' | ssh <other> 'cat >> ~/.ssh/authorized_keys'`
     and `ssh <first> 'ssh -o StrictHostKeyChecking=accept-new <addr> hostname'`.
   - **Locked memory limit.** `init` and `nodes` print it. If it is not
     `unlimited`, tell the user: a two-node run can fail on it (see
     `references/gotchas.md`). Raising it is a system setting, not yours to
     change.
   - **Missing tools.** `pixi`, `tmux` and `git` are needed on every node; the
     user installs them.
   - **The rank config is computed from rules, not tested.** Say so.
4. `CLUSTER=<name> cluster.sh nodes` to confirm every node answers.

`CLUSTER=<name>` picks the cluster for every other command; it may be left out
while only one is set up.

### Cloud instances (launched by `aws-ec2`)

Instances have no alias in `~/.ssh/config`, and their addresses change with every
launch. Write the aliases to a file of their own, outside `~/.ssh`, and export
`SSH_CONFIG=<that file>` before every `cluster.sh` command: `hosts` lists it and
every `ssh` and `scp` reads it. One `Host <name>` block per instance with its
public address as `HostName`, and `User`, `IdentityFile` (the launch key),
`IdentitiesOnly yes`, `StrictHostKeyChecking accept-new` and
`UserKnownHostsFile /dev/null` for all of them. Then:

- `init` records the private address of each instance, which is the one the
  instances reach each other by. After `init`, set `ZENO_WORK` in
  `clusters/<name>.env` to a folder on the instance store
  (`/opt/dlami/nvme/work` on the Deep Learning AMI): the default is under
  `$HOME`, on the root volume.
- An instance the user launched for this work is theirs alone and is thrown
  away, so with their yes you may install Pixi on it and give the first instance
  `ssh` to the others (a key made on the first instance, added to the others'
  `authorized_keys`). Never copy a key from this machine to an instance.
- Launching, the security group and terminating stay with `aws-ec2`: copy the
  run folders off first, and end with its `status`, not only `cluster.sh status`.
  `references/gotchas.md` has what was seen on AWS. Removing a cluster is deleting its files in
`clusters/` (ask first).

## The rule that matters most: leave the nodes as you found them

Treat every node as permanent and shared, unless the user says it is theirs
alone. There is no launch and no terminate; the equivalent of a cloud teardown
guarantee is:

- **Never stop, kill or signal a process you did not start.** `cluster.sh stop`
  ends only this tool's own tmux server (`tmux -L zeno`) and the user's
  processes that run from the work folder or name it. Do not `pkill` by program
  name, and do not use `nvidia-smi` or `kill` on someone else's pid.
- **Never** reboot, install drivers or system packages, use `sudo`, format or
  repartition anything, or change system settings (firewall, mounts, services).
- **Never delete data you did not create.** What this tool creates on a node:
  the work folder (`ZENO_WORK`, default `~/cluster-work`: checkout, build,
  datasets, runs, logs); `~/.sirius/nixl/<commit>` from the build;
  `~/.sirius/gateways/bench` during a run; and `/tmp/sirius_node_<user>_bench/`
  and `/tmp/sirius_gateway_<user>_bench/`, which a run leaves behind. Anything
  else on the nodes is not yours. Tell the user what a session left, with
  sizes: a dataset can be hundreds of GB.
- **Ask before the first heavy action of a session** (a build, a data
  generation, a benchmark): run `cluster.sh nodes`, show the user who is on the
  nodes and what is running, and wait for a yes. `bench` refuses by itself when
  a GPU holds any compute process or a Sirius port is taken; a build has no such
  guard, so the look and the question are the guard.
- **End every session with `cluster.sh status`** and report its output. It exits
  0 only when no node has a GPU process of the user, a listening gateway or rank
  port, or anything of this tool still running. A node it could not reach is
  `CANNOT VERIFY`, never "clean".

If a node is scheduled by Slurm (`command -v sinfo`), stop and tell the user:
this tool starts processes outside Slurm, which can get a node drained. The
branch's `docs/super-sirius/distributed-experiments.md` covers the sbatch path.

## Commands (`bash scripts/cluster.sh <command>`)

`<nodes>` is `all` or a comma list in rank order, e.g. `box-a,box-b`.

| Need | Command |
|---|---|
| Clusters set up, and the ssh aliases to choose from | `cluster.sh hosts` |
| Make a cluster from ssh aliases | `cluster.sh init <name> <host,host,...>` |
| State of every node: GPU, GPU processes and their owner, busy users, load, free disk, ports, checkout | `cluster.sh nodes` |
| Run a command on nodes in parallel, output prefixed `[node]` | `cluster.sh sh <nodes> '<command>'` |
| Copy to / from nodes | `cluster.sh push <nodes> <local> <remote>` / `pull <nodes> <remote> <local-dir>` (lands in `<local-dir>/<node>/`) |
| Work folder, checkout, `pixi run build-distributed`, optionally TPC-H data | `cluster.sh setup <nodes> [data]` |
| Only TPC-H data at another scale factor, no rebuild | `ZENO_SF=3000 cluster.sh data <nodes>` |
| One benchmark arm, answers checked against DuckDB | `cluster.sh bench <name> <nodes> [query...]` |
| Block until a detached job ends, with its real exit code | `cluster.sh wait <node> <setup\|data\|bench>` |
| Prove nothing is left | `cluster.sh status [nodes]` |
| End what this tool started, then `status` | `cluster.sh stop [nodes]` |

`sh` sources `<work>/env.sh` first (Pixi on `PATH`, `WORK`), because a
non-interactive ssh does not read `~/.bashrc`. Remote paths of `push`/`pull` are
absolute or relative to the node's `$HOME`. `setup`, `data` and `bench` run in a
detached tmux session on the node; their files are
`<work>/logs/<session>.{sh,log,rc}`. `wait` holds an ssh connection for the
whole job: run it in the background.

## Workflow, once a cluster is set up

1. **Look.** `cluster.sh nodes`. Tell the user who else is there. Ask before going on.
2. **Build.** `cluster.sh setup <nodes>` then `cluster.sh wait <node> setup` per
   node. It clones the branch into `<work>/sirius`, initialises the submodules
   and runs `pixi run build-distributed`. Safe to run again; it does not pull.
   Every node of a run needs its own build at the same commit: compare the
   `checkout:` lines of `cluster.sh nodes`.
3. **Data.** Every rank reads every file, so the dataset must be at the same
   path on every node (`<work>/tpch_sf<SF>`, or `ZENO_DATA`). `setup <nodes> data`
   generates it the way the Sirius repo's `dataset-manager` skill does
   (`cd test/tpch_performance && pixi run bash generate_tpch_data.sh <SF> --format
   parquet --output <path>`), once per node unless the work folder is on shared
   storage. `ZENO_SF=<SF> cluster.sh data <nodes>` does only that step; pass the
   same `ZENO_SF` to `bench`. Generation is niced but uses every core: on a
   16-core node the load reached 85 to 150, so ask before running it next to
   someone's work. Check free disk first: SF100 is 26 GB, SF3000 is
   802 GB.
4. **Run one arm.** `cluster.sh bench <name> <nodes> [query...]` (default
   queries 1 6 12). The gateway and the client run on the first node; every node
   runs one rank per GPU. `<name>` must be new. It runs
   `scripts/sirius_aws_run.sh` (fresh gateway and ranks, each query cold and
   warm, a 5 s sampler, no `-bail`, everything stopped at the end), then runs
   the same SQL through the build's plain DuckDB and compares with
   `scripts/compare_answers.py`. Start with one node, then two.
   `ZENO_CHECK=0` skips the DuckDB comparison (at SF3000 the reference for all
   22 queries is slow and may not fit in memory); say in the report that the
   answers were not checked. Before a heavy arm, look at `RAM available` in
   `cluster.sh nodes` too.
5. **Read the result.** `cluster.sh wait <first-node> bench` prints the tail of
   the log: errors, ranks up, seconds per query, one `match` line per run. Exit
   0 means every answer matched. **A run whose ranks did not come up is slow to
   say so**: every statement waits 120 s, about 35 minutes for three queries.
   Two minutes after the start, look at `<work>/runs/<name>/client.txt` on the
   first node (`grep -c Error`); if the first statements already failed,
   `cluster.sh stop` and read the cause instead of waiting. The run's files are in `<work>/runs/<name>` on
   the first node.
6. **Leave clean.** `cluster.sh status`. If it lists anything of yours,
   `cluster.sh stop`, and report what `status` printed.
   Settings a branch needs for these nodes go in `clusters/<name>.yaml`, one
   change at a time (example in `references/gotchas.md`: `tcp_lanes`).
7. **Write down what you saw** on this cluster in `clusters/<name>.md`: only
   observed facts, with what is still unverified.

**Secrets in logs.** Rank logs (`rank-logs*/` in a run folder, and
`/tmp/sirius_node_<user>_bench/*/rank.log`) and `~/.sirius/gateways/bench/`
contain the gateway's token, and rank logs repeat an authorization string on
every statement line (`quack_nop_authorization('<hex>', ...)`). Do not `cat`,
`tail` or paste them. To look for a cause, `grep` for the error text only, for
example `grep -h 'ERROR' <log> | grep -v authorization | cut -c1-300`.

Read `references/gotchas.md` before a first build or run on a new cluster.
