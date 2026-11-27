#!/usr/bin/env bash
# cluster.sh — drive a cluster of GPU nodes you reach by ssh alias, and run distributed
# Sirius on them. A cluster is ../clusters/<name>.env (+ .yaml, the rank config), made
# by `init`. The nodes are treated as shared and permanent: nothing is launched or
# terminated, and `stop` ends only what this tool started.
#
# Usage: [CLUSTER=<name>] bash cluster.sh <command> [args...]   <nodes> is `all` or a list: a,b
#   hosts                                  # the clusters set up, and the ssh aliases of ~/.ssh/config
#   init   <name> <host,host,...>          # look at the hosts, write ../clusters/<name>.env and .yaml;
#                                          #   ZENO_BRANCH=<b> / ZENO_REPO=<url> pick what setup clones
#   nodes                             # per node: GPU, who uses it, load, free disk, ports, checkout
#   sh     <nodes> <command...>            # run on the nodes in parallel, output prefixed
#   push   <nodes> <local-path> <remote-path>
#   pull   <nodes> <remote-path> <local-dir>     # lands in <local-dir>/<node>/
#   setup  <nodes> [data]                  # work folder, checkout, `pixi run build-distributed`,
#                                          #   with `data` also TPC-H SF$ZENO_SF; detached, session `setup`
#   data   <nodes>                         # only TPC-H SF$ZENO_SF (ZENO_SF=3000 cluster.sh data ...); session `data`
#   bench  <name> <nodes> [query...]       # one benchmark arm (default queries 1 6 12), gateway on the
#                                          #   first node, answers checked against DuckDB; session `bench`.
#                                          #   ZENO_SF=<SF> picks the data, ZENO_CHECK=0 skips the check
#   wait   <node> <session>                # block until that session ends; exit code is the job's
#   status [nodes]                         # exit 0 only if nothing of this tool runs and no Sirius port listens
#   stop   [nodes]                         # end what this tool started there, then `status`
#
# CLUSTER=<name> picks the cluster; it may be left out while only one is set up.
# Remote paths of push/pull are absolute or relative to the node's $HOME.
# A session's files are <work>/logs/<session>.{sh,log,rc} on the node.
# Runs under macOS's bash 3.2: no associative arrays, no GNU-only flags locally.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTERS="$HERE/../clusters"
_clusters() { ls "$CLUSTERS"/*.env 2>/dev/null | sed 's|.*/||; s|\.env$||'; }
_load() { # the cluster's config; every command but hosts and init needs it
  local c="${CLUSTER:-}" all
  if [ -z "$c" ]; then
    all=$(_clusters)
    case $(echo "$all" | grep -c .) in
      0) echo "no cluster is set up yet: cluster.sh hosts, then cluster.sh init <name> <host,host,...>" >&2; exit 2;;
      1) c="$all";;
      *) echo "several clusters are set up: pick one with CLUSTER=<name> ($(echo $all))" >&2; exit 2;;
    esac
  fi
  CLUSTER="$c"
  # shellcheck source=/dev/null
  . "$CLUSTERS/$c.env" || { echo "no cluster '$c' in $CLUSTERS" >&2; exit 2; }
  : "${ZENO_NODES:?} ${ZENO_ADDRS:?} ${ZENO_WORK:?} ${ZENO_REPO:?} ${ZENO_BRANCH:?} ${ZENO_SF:?}"
  ZENO_NET="${ZENO_NET:-}"
  DATA="${ZENO_DATA:-$ZENO_WORK/tpch_sf$ZENO_SF}"
}
# The gateway's port, and rank i's two ports (9500 + i, 27000 + i).
PORTS=':(9494|950[0-9]|2700[0-9]) '
SSH_OPTS="-o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=30 -o ServerAliveCountMax=20"
TMUX="tmux -L zeno"   # a tmux server of its own: stopping it never touches the user's sessions

log() { echo "[cluster $(date -u +%H:%M:%S)] $*" >&2; }

_resolve() { # all | a,b -> "a b", each checked against ZENO_NODES
  local want n out=""
  [ "${1:-all}" = all ] && { echo "$ZENO_NODES"; return 0; }
  want=$(echo "$1" | tr ',' ' ')
  for n in $want; do
    case " $ZENO_NODES " in *" $n "*) out="$out $n";; *) log "unknown node '$n' (config has: $ZENO_NODES)"; return 2;; esac
  done
  echo "${out# }"
}

_addr() { # node -> its address in ZENO_ADDRS
  local i=1 n
  for n in $ZENO_NODES; do
    [ "$n" = "$1" ] && { echo "$ZENO_ADDRS" | tr ' ' '\n' | sed -n "${i}p"; return 0; }
    i=$((i+1))
  done
  return 1
}

# The variables every remote script starts with. Double quotes: $HOME expands on the node.
_prelude() {
  printf 'W="%s"\nD="%s"\nPORTS="%s"\nT="%s"\n' "$ZENO_WORK" "$DATA" "$PORTS" "$TMUX"
  # ours: pids of this user that run from the work folder or name it. The command line
  # alone misses a program started by a relative path (`pixi run sirius-duckdb/...`).
  cat <<'EOF'
ours() {
  local p c a
  for p in $(pgrep -u "$(id -u)"); do
    [ "$p" = "$$" ] && continue
    c=$(readlink "/proc/$p/cwd" 2>/dev/null); a=$(cat "/proc/$p/cmdline" 2>/dev/null | tr '\0' ' ')
    case "$c/ $a" in "$W"/*|*"$W/"*) echo "$p";; esac
  done
}
EOF
}

# node — run the script on stdin there. On stdin, not as an argument: the remote
# shell's command line is then `bash -s`, so `pkill -f <work folder>` cannot match it.
_remote() { { _prelude; cat; } | ssh $SSH_OPTS "$1" bash -s; }

_each() { # "nodes" fn — run `fn node` per node in parallel, output prefixed; rc = how many failed
  local fn="$2" n pids="" failed=0 p
  for n in $1; do
    ( "$fn" "$n" 2>&1 | sed "s/^/[$n] /"; exit "${PIPESTATUS[0]}" ) &
    pids="$pids $!"
  done
  for p in $pids; do wait "$p" || failed=$((failed+1)); done
  return "$failed"
}

_put() { # node local-file path-under-work — small files only (it travels inside the command)
  local b64; b64=$(base64 < "$2" | tr -d '\n')
  _remote "$1" <<EOF
f="\$W/$3"; mkdir -p "\$(dirname "\$f")" && printf '%s' '$b64' | base64 -d > "\$f"
EOF
}

_job_script() { # session base64-of-script — printed, so no heredoc sits inside $(...) (bash 3.2 misparses that)
  cat <<EOF
mkdir -p "\$W/logs" || exit 90
\$T has-session -t '$1' 2>/dev/null && { echo ALREADY; exit 0; }
rm -f "\$W/logs/$1.rc"
printf '%s' '$2' | base64 -d > "\$W/logs/$1.sh" || exit 90
\$T new-session -d -s '$1' "bash '\$W/logs/$1.sh' > '\$W/logs/$1.log' 2>&1; echo \\\$? > '\$W/logs/$1.rc'" || exit 91
sleep 3
if \$T has-session -t '$1' 2>/dev/null; then echo RUNNING
elif [ -f "\$W/logs/$1.rc" ]; then echo "EXITED:\$(cat "\$W/logs/$1.rc")"
else echo NOSTART; fi
EOF
}

_job() { # node session script — start it in a detached tmux session on the node
  local b64 out
  b64=$( { _prelude; printf '%s\n' "$3"; } | base64 | tr -d '\n')
  out=$(_job_script "$2" "$b64" | _remote "$1") || { log "$1: could not start '$2' (ssh or tmux failed): $out"; return 1; }
  case "$out" in
    *RUNNING*)  log "$1: '$2' runs detached. Wait: cluster.sh wait $1 $2   (log: <work>/logs/$2.log)";;
    *ALREADY*)  log "$1: a '$2' session already runs — wait for it, or 'stop'"; return 1;;
    *EXITED:0*) log "$1: '$2' already finished cleanly";;
    *)          log "$1: '$2' FAILED at start ($out) — read <work>/logs/$2.log"; return 1;;
  esac
}

# --- hosts / init ------------------------------------------------------------
cmd_hosts() {
  local f="${SSH_CONFIG:-$HOME/.ssh/config}" c
  c=$(_clusters | paste -sd' ' -)
  echo "clusters set up: ${c:-none}"
  [ -r "$f" ] || { echo "cannot read $f: ask the user for the ssh aliases"; return 1; }
  echo "ssh hosts in $f (files it includes are not read):"
  # Only the alias names; patterns (*, ?, !) are not hosts.
  awk 'tolower($1) == "host" { for (i = 2; i <= NF; i++) if ($i !~ /[*?!]/ && !seen[$i]++) print "  " $i }' "$f"
}

_probe() {
  ssh $SSH_OPTS "$1" bash -s <<'EOF'
export PATH="$HOME/.pixi/bin:$PATH"
echo "hostname=$(hostname)"
echo "gpus=$(nvidia-smi -L 2>/dev/null | grep -c .)"
echo "gpu=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1)"
echo "gpumem=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null | head -1)"
echo "ram=$(awk '/MemTotal/ { print int($2 / 1048576) }' /proc/meminfo)"
echo "cores=$(nproc)"
a=$(ip -4 route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \([0-9.]*\).*/\1/p')
all=$(ip -4 -o addr show scope global | while read -r _ ifc _ ad _; do
  echo "$ad($ifc,$(cat "/sys/class/net/$ifc/speed" 2>/dev/null || echo '?')Mb/s)"; done | paste -sd' ' -)
echo "lockmem=$(ulimit -l)"
echo "addr=${a:-$(echo "$all" | sed 's|/.*||')}"
echo "addrs=$all"
m=""; for t in tmux git; do command -v $t > /dev/null || m="$m $t"; done
# Run it: a $HOME shared with machines of another architecture holds a pixi that cannot start here.
pixi --version > /dev/null 2>&1 || m="pixi$m"
echo "missing=${m# }"
EOF
}
_val() { sed -n "s/^$2=//p" "$1" | head -1; }

cmd_init() { # name host,host,... — only looks at the hosts; writes two files on this machine
  local u="usage: init <name> <host,host,...>"
  local name="${1:?$u}" hosts tmp h f g ram mem addrs="" gmax=1 rmin="" mmin="" first="" a err
  hosts=$(echo "${2:?$u}" | tr ',' ' ')
  case "$name" in *[!A-Za-z0-9._-]*) log "name: letters, digits, . _ - only"; return 2;; esac
  [ -e "$CLUSTERS/$name.env" ] && { log "cluster '$name' exists: edit or delete $CLUSTERS/$name.env"; return 1; }
  tmp=$(mktemp -d)
  for h in $hosts; do
    f="$tmp/probe"
    _probe "$h" > "$f" 2> "$f.err" || {
      log "$h: ssh failed: $(tail -1 "$f.err"). The user fixes the key or host key; then run init again"; return 1; }
    g=$(_val "$f" gpus); ram=$(_val "$f" ram); mem=$(_val "$f" gpumem); a=$(_val "$f" addr)
    case "$mem" in ''|*[!0-9]*) mem=0;; esac          # not a number: the GPU shares the host's memory
    echo "[$h] $(_val "$f" hostname): $g x $(_val "$f" gpu) (${mem} MiB of its own; 0 = shares the host's), RAM $ram GiB, $(_val "$f" cores) cores, locked memory limit $(_val "$f" lockmem) kB, missing tools: $(_val "$f" missing)"
    echo "[$h] addresses (interface, link speed): $(_val "$f" addrs)   chosen: $a"
    [ "$g" -ge 1 ] 2>/dev/null || { log "$h: no NVIDIA GPU seen (nvidia-smi -L): not a node for Sirius"; return 1; }
    [ -n "$a" ] || { log "$h: no IPv4 address found"; return 1; }
    addrs="$addrs $a"; [ -n "$first" ] || first="$h"
    [ "$g" -gt "$gmax" ] && gmax="$g"
    { [ -z "$rmin" ] || [ "$ram" -lt "$rmin" ]; } && rmin="$ram"
    { [ -z "$mmin" ] || [ "$mem" -lt "$mmin" ]; } && mmin="$mem"
  done
  rm -rf "$tmp"
  addrs="${addrs# }"
  # The run script starts the other nodes' ranks over ssh from the first node.
  for a in ${addrs#* }; do
    [ "$a" = "${addrs%% *}" ] && continue
    if err=$(ssh $SSH_OPTS "$first" "ssh -o BatchMode=yes -o ConnectTimeout=5 $a true" 2>&1); then
      echo "[$first] ssh to $a: ok"
    else
      echo "[$first] ssh to $a: NO ($(echo "$err" | tail -1)). A run on both needs it; the user sets it up (SKILL.md)"
    fi
  done

  mkdir -p "$CLUSTERS"
  cat > "$CLUSTERS/$name.env" <<EOF
# Cluster '$name', written by \`cluster.sh init\` on $(date -u +%F). Plain KEY=value lines.

# ssh aliases of the nodes, and each node's address in the same order: the nodes talk
# to each other by these. \`init\` took the address of each node's default route; if the
# nodes share a faster network, put those addresses here and its CIDR in ZENO_NET.
ZENO_NODES="$hosts"
ZENO_ADDRS="$addrs"
# The network the gateway and the ranks listen on, e.g. 10.0.0.0/24. Empty: Sirius chooses.
ZENO_NET=

# Everything this tool creates on a node lives here. Single quotes: \$HOME is the
# node's. It must be the same path on every node.
ZENO_WORK='\$HOME/cluster-work'

# What \`setup\` clones on a node that has no checkout yet.
ZENO_REPO=${ZENO_REPO:-https://github.com/sirius-db/sirius.git}
ZENO_BRANCH=${ZENO_BRANCH:-experimental/distributed-sirius}
# TPC-H scale factor of \`data\`, \`setup <nodes> data\` and \`bench\`; the data is
# \$ZENO_WORK/tpch_sf<SF>. The environment wins: ZENO_SF=3000 cluster.sh data all
ZENO_SF=\${ZENO_SF:-100}
EOF

  # Rank config: the rules and the two tested shapes of distributed-experiments-aws.md
  # (24 GB GPUs: g6.4xlarge; 96 GB GPUs: g7e.24xlarge). One rank per GPU.
  local gpuline="" host scan=8 react=1 bloom=512Mi bp=6Gi ht=4Gi disk=150000000000
  if [ "$mmin" -eq 0 ]; then
    gpuline="    gpu: { usage_limit_bytes: $((rmin * 4 / 10 / gmax))Gi }"; host=$((rmin * 2 / 10 / gmax))
  else host=$((rmin * 6 / 10 / gmax)); fi
  [ "$mmin" -ge 80000 ] && { scan=32; react=4; bloom=2Gi; bp=32Gi; ht=32Gi; disk=1000000000000; }
  {
    echo "# Sirius config of every rank of cluster '$name' ($gmax GPU(s) per node, ${mmin} MiB each,"
    echo "# $rmin GiB RAM). Written by \`cluster.sh init\` from rules, NOT tested on these nodes:"
    echo "# host memory is 0.6 x RAM / ranks per node (0.4 GPU + 0.2 host where the GPU shares"
    echo "# the host's memory). Change one value at a time, only when a run fails."
    echo "sirius:"
    echo "  memory:"
    [ -n "$gpuline" ] && echo "$gpuline"
    echo "    host: { capacity_bytes: ${host}Gi }"
    echo "    disk: { disk_id: 0, capacity_bytes: $disk, downgrade_root_dirs: \"@SPILL@\" }"
    echo "  executor:"
    echo "    pipeline: { num_threads: 4 }"
    echo "    scan_manager: { num_threads: $scan, uring_n_reactors: $react, cache: { mode: none } }"
    echo "  operator_params:"
    echo "    max_dynamic_filter_bloom_bytes_per_gpu: $bloom"
    echo "    max_build_probe_resident_bytes: $bp"
    echo "    max_build_hash_table_bytes: $ht"
  } > "$CLUSTERS/$name.yaml"
  log "wrote $CLUSTERS/$name.env and $name.yaml. Next: CLUSTER=$name cluster.sh nodes"
}

# --- nodes -------------------------------------------------------------------
_nodes_one() {
  _remote "$1" <<'EOF'
uid=$(id -u)
echo "gpu: $(nvidia-smi --query-gpu=name,utilization.gpu,driver_version --format=csv,noheader | paste -sd';' -)"
apps=$(nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader)
if [ -z "$apps" ]; then echo "gpu processes: none"
else echo "$apps" | while IFS=, read -r pid mem; do
  echo "gpu process: pid $pid user $(ps -o user= -p "$pid") mem$mem $(ps -o comm= -p "$pid")"; done
fi
busy=$(ps -eo user:16,pcpu,etime,comm --sort=-pcpu | awk 'NR > 1 && $1 != "root" && $2 > 50 && $4 != "ps"' | head -5)
echo "non-root processes over 50% CPU: ${busy:-none}"
echo "load $(cut -d' ' -f1-3 /proc/loadavg) on $(nproc) cores, RAM available $(awk '/MemAvailable/ { print int($2 / 1048576) }' /proc/meminfo) GiB, locked memory limit $(ulimit -l) kB, logged in: $(who | awk '{ print $1 }' | sort -u | paste -sd, -)"
echo "disk free under \$HOME: $(df -h --output=avail "$HOME" | tail -1 | tr -d ' ')"
p=$(ss -ltn | grep -E "$PORTS")
echo "sirius ports: ${p:-free}"
if [ -d "$W/sirius" ]; then
  echo "checkout: $(git -C "$W/sirius" log -1 --format='%h %cs') setup rc=$(cat "$W/logs/setup.rc" 2>/dev/null || echo none) branch $(git -C "$W/sirius" branch --show-current) data: $(ls -d "$W"/tpch_sf* 2>/dev/null | sed 's|.*/||' | paste -sd, - | grep . || echo none)"
else echo "checkout: none in $W"; fi
s=$($T ls -F '#S' 2>/dev/null | paste -sd, -)
echo "sessions of this tool: ${s:-none}"
EOF
}
cmd_nodes() { _each "$ZENO_NODES" _nodes_one || { log "some node(s) did not answer"; return 1; }; }

# --- sh / push / pull --------------------------------------------------------
# Non-interactive ssh does not read ~/.bashrc, so the node's env.sh (Pixi on PATH, WORK) is sourced.
_sh_one()   { ssh $SSH_OPTS "$1" "WORK=\"$ZENO_WORK\"; [ -f \"\$WORK/env.sh\" ] && . \"\$WORK/env.sh\"; $REMOTE_CMD"; }
_push_one() { scp $SSH_OPTS -r "$LOCAL_PATH" "$1:$REMOTE_PATH"; }
_pull_one() { mkdir -p "$LOCAL_PATH/$1" && scp $SSH_OPTS -r "$1:$REMOTE_PATH" "$LOCAL_PATH/$1/"; }
cmd_sh()   { local n; n=$(_resolve "$1") || return 2; shift; REMOTE_CMD="$*"; _each "$n" _sh_one; }
cmd_push() { local n; n=$(_resolve "$1") || return 2; LOCAL_PATH="$2"; REMOTE_PATH="$3"; _each "$n" _push_one; }
cmd_pull() { local n; n=$(_resolve "$1") || return 2; REMOTE_PATH="$2"; LOCAL_PATH="$3"; _each "$n" _pull_one; }

# --- setup -------------------------------------------------------------------
_setup_one() { _job "$1" setup "$SETUP_SCRIPT"; }
_setup_script() { # [scale factor]
  local sf="$1"
  cat <<EOF
set -uo pipefail
export PATH="\$HOME/.pixi/bin:\$HOME/.cargo/bin:\$PATH"
command -v pixi > /dev/null || { echo "no pixi on this node: install it (https://pixi.sh), then run setup again"; exit 1; }
# All builds of one user share one sccache server: two at once corrupt each other.
if pgrep -u "\$(id -u)" -x 'ninja|cc1plus|nvcc' > /dev/null; then echo "a build of yours already runs on this node"; exit 1; fi
mkdir -p "\$W/logs" "\$W/runs" "\$W/spill" && cd "\$W" || exit 1
# sccache's server listens on 127.0.0.1:4226 for every user of the node: a build then
# compiles through whoever started it first, as that user (seen: "Permission denied"
# writing .o.d files). A port of our own keeps the two apart.
printf 'export WORK=%s\nexport PATH=\$HOME/.pixi/bin:\$HOME/.cargo/bin:\$PATH\nexport SCCACHE_SERVER_PORT=%s\n' "\$W" "\$((24000 + \$(id -u) % 10000))" > "\$W/env.sh"
. "\$W/env.sh"
[ -d sirius ] || git clone --branch '$ZENO_BRANCH' '$ZENO_REPO' sirius || exit 1
cd sirius && git submodule update --init --recursive || exit 1
echo "== build \$(git log -1 --format=%h) begins \$(date -u +%T)"
nice -n 19 pixi run build-distributed || exit 1      # niced: other people build here too
echo "== build done \$(date -u +%T)"
[ -n '$sf' ] || exit 0
[ -d "\$D/lineitem" ] && { echo "== \$D exists: not generated again"; exit 0; }
# The command of the repo's dataset-manager skill (.claude/skills/dataset-manager).
cd test/tpch_performance && nice -n 19 pixi run bash generate_tpch_data.sh '$sf' --format parquet --output "\$D"
EOF
}
cmd_setup() { # nodes [data]
  local n sf=""
  n=$(_resolve "${1:?usage: setup <nodes> [data]}") || return 2
  [ "${2:-}" = data ] && sf="$ZENO_SF"
  SETUP_SCRIPT=$(_setup_script "$sf")
  _each "$n" _setup_one
}

# --- data --------------------------------------------------------------------
_data_script() {
  cat <<EOF
. "\$W/env.sh" || exit 1
[ -x "\$W/sirius/sirius-duckdb/build/release/duckdb" ] || { echo "no build in \$W/sirius: cluster.sh setup"; exit 1; }
# The command of the repo's dataset-manager skill; it skips an output folder that exists.
cd "\$W/sirius/test/tpch_performance" && nice -n 19 pixi run bash generate_tpch_data.sh '$ZENO_SF' --format parquet --output "\$D"
EOF
}
_data_one() { _job "$1" data "$DATA_SCRIPT"; }
cmd_data() { # nodes — TPC-H SF$ZENO_SF as Parquet on each, without rebuilding
  local n; n=$(_resolve "${1:?usage: data <nodes>}") || return 2
  DATA_SCRIPT=$(_data_script)
  _each "$n" _data_one
}

# --- bench -------------------------------------------------------------------
_bench_script() { # name frac others queries
  cat <<EOF
. "\$W/env.sh" && cd "\$W/sirius" || exit 1
${ZENO_NET:+export SIRIUS_GATEWAY_PRIORITY_NETWORKS=$ZENO_NET}
sed "s|@SPILL@|\$W/spill|" "\$W/logs/bench-config.in.yaml" > "\$W/logs/bench-config.yaml"
OUT=\$W/runs/$1
pixi run bash scripts/sirius_aws_run.sh --name '$1' --data "\$D" --frac $2 \\
  --config "\$W/logs/bench-config.yaml" --nodes '$3' $4
[ -s "\$OUT/client.txt" ] || { echo "== no client output: the run did not start"; exit 1; }
if [ '${ZENO_CHECK:-1}' = 0 ]; then
  echo "== answers NOT checked (ZENO_CHECK=0)"
  ! grep -q Error "\$OUT/client.txt"; exit
fi
# The answers, against the build's plain DuckDB on the CPU (no-slurm doc, step 6).
# The run script has stopped every rank by now, so the memory is free.
# Kept per scale factor and query list: at SF3000 it is the slowest part of an arm.
REF="\$W/ref/sf$ZENO_SF-q\$(echo '$4' | tr ' ' '-').txt"
if [ -s "\$REF" ]; then echo "== reference answers: reusing \$REF"
else
  echo "== reference answers with plain DuckDB"
  mkdir -p "\$W/ref" && sed '\$d' "\$OUT/run.sql" > "\$OUT/ref.sql"
  pixi run sirius-duckdb/build/release/duckdb -no-agent < "\$OUT/ref.sql" > "\$OUT/ref.txt" 2>&1
  cp "\$OUT/ref.txt" "\$REF"
fi
pixi run python3 scripts/compare_answers.py "\$OUT/client.txt" --ref-client "\$REF" \\
  --ties 3:revenue,o_orderdate --ties 18:o_totalprice,o_orderdate | tee "\$OUT/compare.txt"
exit "\${PIPESTATUS[0]}"
EOF
}

_bench_check() {
  _remote "$1" <<'EOF'
bad=0; uid=$(id -u)
[ -f "$W/sirius/scripts/sirius_node.sh" ] && [ "$(cat "$W/logs/setup.rc" 2>/dev/null)" = 0 ] || { echo "FAIL no finished build in $W/sirius: cluster.sh setup"; bad=1; }
[ -d "$D/lineitem" ] || { echo "FAIL no dataset at $D (every node needs it at this path)"; bad=1; }
for pid in $(nvidia-smi --query-compute-apps=pid --format=csv,noheader); do
  echo "FAIL the GPU is in use: pid $pid of $(ps -o user= -p "$pid")"; bad=1
done
ss -ltn | grep -E "$PORTS" && { echo "FAIL a Sirius port is taken (above)"; bad=1; }
[ $bad = 0 ] && echo "ok"; exit $bad
EOF
}

cmd_bench() { # name nodes [query...]
  local name="${1:?usage: bench <name> <nodes> [query...]}" nodes
  nodes=$(_resolve "${2:?usage: bench <name> <nodes> [query...]}") || return 2
  shift 2
  local queries="${*:-1 6 12}" gw others="" n
  case "$name$queries" in *[!A-Za-z0-9._\ -]*) log "name and queries: letters, digits, . _ - only"; return 2;; esac
  # shellcheck disable=SC2086
  set -- $nodes
  gw="$1"; shift
  for n in "$@"; do others="$others $(_addr "$n")"; done
  others="${others# }"

  _each "$nodes" _bench_check || { log "not started: fix the FAIL lines above"; return 1; }
  if [ -n "$others" ]; then
    # The run script starts the other nodes' ranks over ssh from the gateway's node.
    _remote "$gw" <<EOF || { log "not started: $gw cannot ssh to every other node (see references/gotchas.md)"; return 1; }
for a in $others; do ssh -o BatchMode=yes -o ConnectTimeout=5 "\$a" true || { echo "no ssh without a password from $gw to \$a"; exit 1; }; done
EOF
  fi
  # scripts/sirius_aws_run.sh is not on the branch until sirius-db/sirius#2513 merges.
  for n in $nodes; do
    _remote "$n" <<< '[ -e "$W/sirius/scripts/sirius_aws_run.sh" ]' ||
      _put "$n" "$HERE/sirius_aws_run.sh" sirius/scripts/sirius_aws_run.sh || return 1
  done
  _put "$gw" "$CLUSTERS/$CLUSTER.yaml" logs/bench-config.in.yaml || return 1

  local frac; frac=$(awk "BEGIN { printf \"%.10f\", 0.0001 / $ZENO_SF }")
  _job "$gw" bench "$(_bench_script "$name" "$frac" "$others" "$queries")"
}

# --- wait / status / stop ----------------------------------------------------
cmd_wait() { # node session — the session's real exit code
  local node="${1:?usage: wait <node> <session>}" s="${2:?usage: wait <node> <session>}"
  _remote "$node" <<EOF
while \$T has-session -t '$s' 2>/dev/null; do sleep 15; done
[ -f "\$W/logs/$s.log" ] || { echo "no '$s' job was started on $node"; exit 1; }
tail -n 30 "\$W/logs/$s.log"
rc=\$(cat "\$W/logs/$s.rc" 2>/dev/null || echo UNKNOWN)
echo "JOB_DONE $s rc=\$rc"
[ "\$rc" = 0 ]
EOF
}

_status_one() {
  _remote "$1" <<'EOF'
bad=0; uid=$(id -u)
for pid in $(nvidia-smi --query-compute-apps=pid --format=csv,noheader); do
  [ "$(ps -o uid= -p "$pid" 2>/dev/null | tr -d ' ')" = "$uid" ] &&
    { echo "LEFT gpu process of yours: $pid $(ps -o args= -p "$pid" | cut -c1-100)"; bad=1; }
done
p=$(ss -ltnp | grep -E "$PORTS") && { echo "LEFT listening Sirius port (no pid shown = another user's):"; echo "$p"; bad=1; }
s=$($T ls -F '#S' 2>/dev/null | paste -sd, -)
[ -n "$s" ] && { echo "LEFT session of this tool: $s"; bad=1; }
q=$(for p in $(ours); do echo "$p $(cat "/proc/$p/cmdline" 2>/dev/null | tr '\0' ' ' | cut -c1-110)"; done)
[ -n "$q" ] && { echo "LEFT process of yours under $W:"; echo "$q"; bad=1; }
[ $bad = 0 ] && echo "clean: no GPU process of yours, no Sirius port, nothing of this tool running"
exit $bad
EOF
  local rc=$?
  # ssh's own failure is not "clean": never report a node as left alone off a failed connection.
  [ $rc -eq 255 ] && echo "CANNOT VERIFY: ssh to $1 failed"
  return $rc
}
cmd_status() {
  local n; n=$(_resolve "${1:-all}") || return 2
  _each "$n" _status_one && { log "all clean: $n"; return 0; }
  log "NOT CLEAN or not verified — see the LEFT / CANNOT VERIFY lines"; return 1
}

_stop_one() {
  # Ends: this tool's tmux server, and processes of this user that run from the work
  # folder or name it (the gateway, ranks, client, sampler, reference and build).
  _remote "$1" <<'EOF'
uid=$(id -u)
p=$(ours); [ -n "$p" ] && kill $p 2>/dev/null; sleep 5   # TERM first: the run script then stops its own ranks
$T kill-server 2>/dev/null
p=$(ours); [ -n "$p" ] && kill -9 $p 2>/dev/null; sleep 1
exit 0
EOF
}
cmd_stop() {
  local n; n=$(_resolve "${1:-all}") || return 2
  _each "$n" _stop_one || log "stop did not reach every node"
  cmd_status "${1:-all}"
}

main() { local c="${1:-}"; shift || true; case "$c" in
  hosts) cmd_hosts; return;; init) cmd_init "$@"; return;;
  nodes|sh|push|pull|setup|data|bench|wait|status|stop) _load;; esac; case "$c" in
  nodes) cmd_nodes;; sh) cmd_sh "$@";; push) cmd_push "$@";; pull) cmd_pull "$@";;
  setup) cmd_setup "$@";; data) cmd_data "$@";; bench) cmd_bench "$@";; wait) cmd_wait "$@";;
  status) cmd_status "$@";; stop) cmd_stop "$@";;
  *) sed -n '2,29p' "$0" >&2; exit 2;; esac; }
main "$@"
