# Gotchas that apply to any cluster

All of these were seen on the first cluster this tool drove (three DGX Spark
nodes, 2026-11-10). Whether they hold on another cluster is unverified until
you see them there.

## Setting up

- `cluster.sh hosts` reads only the top-level `~/.ssh/config`: hosts defined in
  a file it includes are not listed. Ask the user for those.
- `init` records each node's default-route address. On the first cluster that
  was a 1 Gbit/s port while the nodes also shared a 200 Gbit/s network: read the
  `addresses:` lines and put the fast ones in `ZENO_ADDRS`, its CIDR in `ZENO_NET`.
- Nodes may not resolve each other's names. The tool uses addresses between
  nodes for that reason.
- The first node of a `bench` starts the other nodes' ranks over ssh. It needs
  ssh without a password to each of them, and their host keys already accepted.
  The user sets that up; the assistant is not allowed to.
- A GPU that shares the host's memory (GB10) reports no memory of its own.
  `init` then caps the GPU at 0.4 x RAM and the host pool at 0.2 x RAM.
- A node with several GPUs runs one rank per GPU. The generated config divides
  host memory by the GPU count. This path has not been run by this tool.

- A node may cap locked memory (`ulimit -l`; 15 GiB on the first cluster). On the
  branch `experimental/distributed-sirius-tcp` a two-node run then failed at
  session setup: `exchange NIXL: registerMem(receive mirror) failed:
  NIXL_ERR_BACKEND`, 0 ranks up, because the default 16 TCP lanes pin 256 MiB
  each. Adding `distributed: { exchange: { tcp_lanes: 2 } }` under `sirius:` in
  the cluster's yaml made the same run pass (2 of 2 ranks, Q1/Q6/Q12 at SF3000
  about twice as fast as one node, answers equal to the one-node run). That
  branch's doc reports wrong answers with 2 lanes on four machines for Q5 and
  Q13: check answers. The branch `experimental/distributed-sirius` has the TCP
  lanes too at `18f91952`: on 2026-11-18 a two-node smoke test under the same
  limit failed with `rank 0 reached 0 of 1 peers in 120000 ms` and passed with
  `tcp_lanes: 2`. `init` does not write the key: add it when `init` prints a
  locked memory limit that is not `unlimited`.

## Builds

- sccache's server listens on 127.0.0.1:4226 for every user of a node. A build
  then compiled through another user's server and failed with `Permission
  denied` on `.o.d` files. `setup` gives each user a port of their own
  (`SCCACHE_SERVER_PORT` in `<work>/env.sh`).
- Builds are niced, and `setup` refuses while a build of the same user runs.
- A build took 11 to 16 minutes on 16 arm64 cores.

## Runs

- A run with no rank up takes about 35 minutes to end (120 s per statement).
  Look at the run's `client.txt` after two minutes and stop early.
- The run script passes only `SIRIUS_*` variables to the other nodes; there is
  no way to set `UCX_*` variables for a run. Use the yaml.
- `nodes` lists every `tpch_sf*` folder in the work folder, whatever `ZENO_SF` is.

- A rank next to someone else's build filled a node's memory and swap. Look at
  `RAM available` in `cluster.sh nodes` before a heavy arm, and run when quiet.
- `stop` finds processes by their working folder or command line under the work
  folder. It was never proven on a real leftover started by a relative path.
- Do not edit `cluster.sh` while a `cluster.sh wait` runs: the waiting bash ends
  in a syntax error.
- The reference answers of `bench` are kept in `<work>/ref/`; reusing a kept
  file was never exercised.


## AWS instances

Seen on 2026-11-18 with `aws-ec2` instances (Deep Learning Base AMI, Ubuntu
22.04, driver 595): one `g7e.8xlarge`, then two `g6.4xlarge`.

- The AMI has `tmux` and `git` but no Pixi. The instance store is mounted at
  `/opt/dlami/nvme` and any user can write there; the root volume had 144 GB
  free of 200.
- The locked memory limit is `unlimited`, so `tcp_lanes` stays at its default.
  A two-instance run passed with it.
- The instances reach each other by their private addresses once the security
  group allows traffic from itself. `ZENO_NET` stayed empty: the ranks chose the
  private address, not the AMI's `docker0`.
- `init` prints `ssh to <addr>: NO (Host key verification failed.)` for a new
  pair. On instances that is yours to fix, with the user's yes: see SKILL.md.
- The build took 12 minutes on 32 vCPUs and 34 minutes on 16. SF100 took about
  2 minutes to generate (26 GB).
- `status` right after a build lists the build's `sccache` server as `LEFT`. It
  ended by itself: `status` was clean 12 minutes after the build.
- Capacity decides the instance type. A second `g7e.8xlarge` was not to be had
  in any of five regions that evening. Launch every instance of a run at the same
  moment, and give back a lone one, before building anything.
- The biggest type was harder still: on 2026-11-19 a `g7e.48xlarge` (8 GPUs) was
  refused in four regions for almost three hours before one zone gave one.
- An instance ends at the time its shutdown timer was given, whatever was agreed
  later. One was lost in the middle of a run because the old timer was still
  armed after the run was extended. When a run is extended, move every timer,
  the one on the instance and any on this machine, and read the new time back.
- The AWS login on this machine can expire during a long run. Then nothing can
  be extended, terminated early or proven terminated from here until the user
  logs in again. The on-box timer still ends the instance: copy the results off
  well before it, and take the proof of termination after the next login.
- Several ranks on one instance that read from S3: see section 1d of the
  `distributed-sirius-bench` skill before choosing the rank count.
