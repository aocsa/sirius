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
  Q13: check answers. The branch `experimental/distributed-sirius` ran two
  nodes under the same limit without this setting; the key was not tried there.

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
