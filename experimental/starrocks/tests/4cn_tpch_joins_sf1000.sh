#!/usr/bin/env bash
# FE + four Sirius CNs, one per whole GPU, run:
#   TPC-H joins via FILES() over SF1000 parquet (TPCH_DATA)
# The 4-GPU, SF1000 sibling of 2cn_tpch_joins.sh: same queries, same NIXL slab shuffle,
# same DuckDB oracle and tolerance (identifiers and dates exact, measures within rel 5e-3).
#
# SF1000 differs from SF1 in three ways this script has to handle:
#   - Scans are spread over all CNs. The FE cuts FILES() byte ranges at instance
#     boundaries, and the CN refuses a parquet file whose ranges are split across
#     instances ("byte-range splits do not tile the parquet file"). The FE must carry
#     the `files_query_whole_file_ranges` patch (Config.java + FileScanNode.java); the
#     script turns it on and stops if the FE does not know it.
#   - Cold scans of ~266 GB outlast the FE's 60 s fragment-deploy RPC timeout, so it is
#     raised to 30 min, and the query timeout to 1 h.
#   - DuckDB's answer takes minutes per query at this scale, so it is cached under
#     ORACLE_DIR, keyed by query name, SQL hash and data path.
#
# A failing query does not stop the run; a CN that dies does (later queries would only
# report the dead CN). The summary lists every query with its wall time.
#
# Requires: the release CN (see scripts/cn-env.sh for the NIXL build), the packaged FE
# with the whole-file patch, the `client` pixi env (mysql), and the repo-root pixi env
# (python + duckdb).
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SR_DIR=$(cd "$HERE/.." && pwd)
REPO_ROOT=$(cd "$SR_DIR/../.." && pwd)

# nixl / UCX paths, UCX_TLS, and LD_LIBRARY_PATH (engine .so, nixl, UCX, pixi).
# shellcheck source=/dev/null
source "$SR_DIR/scripts/cn-env.sh"

RUN_ROOT=${RUN_ROOT:-/scratch/$USER/sirius-4cn-sf1000-joins}
E2E=${SIRIUS_4CN_E2E_DIR:-$RUN_ROOT/run}
ORACLE_DIR=${ORACLE_DIR:-$RUN_ROOT/oracle}
CN_BIN=${CN_BIN:-$SR_DIR/target/release/sirius-starrocks-cn}
STARROCKS_FE=${STARROCKS_FE:-$SR_DIR/starrocks/output/fe}
MYSQL=${MYSQL:-$SR_DIR/.pixi/envs/client/bin/mysql}
PYTHON=${PYTHON:-$REPO_ROOT/.pixi/envs/default/bin/python}
SIRIUS_LIB=${SIRIUS_LIB:-$REPO_ROOT/build/release/extension/sirius}
# The FE was built with the fe env's JDK 17.
export JAVA_HOME=${JAVA_HOME:-$SR_DIR/.pixi/envs/fe/lib/jvm}

TPCH_DATA=${TPCH_DATA:-/scratch/sirius/datasets/tpch_sf1000}
QUERIES=${TPCH_QUERIES:-q14 q05 q07 q08 q09 q12 q19}
GPUS=(${GPUS:-0 1 2 3})
NUM_CNS=${#GPUS[@]}

# Off the StarRocks defaults (9030/8030/9020/9010): another FE may already hold them.
FE_QUERY_PORT=${FE_QUERY_PORT:-9031}
FE_HTTP_PORT=${FE_HTTP_PORT:-8031}
FE_RPC_PORT=${FE_RPC_PORT:-9021}
FE_EDIT_LOG_PORT=${FE_EDIT_LOG_PORT:-9011}
PORT_BASE=${PORT_BASE:-9100}
PORT_STRIDE=${PORT_STRIDE:-10}

# Per CN. The NIXL slab reserves GPU_FRACTION of the GPU up front; HOST_BYTES is the
# host spill tier (four CNs share the box's host memory with the DuckDB oracle).
GPU_FRACTION=${GPU_FRACTION:-0.85}
HOST_BYTES=${HOST_BYTES:-137438953472} # 128 GiB
PIPELINE_THREADS=${PIPELINE_THREADS:-4}
# Bind each CN's threads to the CPU socket of its GPU.
NUMA_BIND=${NUMA_BIND:-1}
QUERY_TIMEOUT_S=${QUERY_TIMEOUT_S:-3600}
# DuckDB defaults to 80% of RAM, which would collide with the CNs' host tiers.
export DUCKDB_MEMORY_LIMIT=${DUCKDB_MEMORY_LIMIT:-512GB}

export UCX_TLS=${UCX_TLS:-cuda_copy,cuda_ipc,tcp,self}

[ -x "$CN_BIN" ] || {
    echo "no CN binary at $CN_BIN — build with scripts/cn-env.sh sourced:" >&2
    echo "  cargo build --release -p sirius-starrocks-cn" >&2
    exit 1
}
[ -x "$STARROCKS_FE/bin/start_fe.sh" ] || {
    echo "no packaged StarRocks FE at $STARROCKS_FE (pixi run fe-build)" >&2
    exit 1
}
[ -x "$MYSQL" ] || {
    echo "mysql client not found at $MYSQL (pixi install -e client)" >&2
    exit 1
}
"$PYTHON" -c "import duckdb" 2>/dev/null || {
    echo "$PYTHON cannot import duckdb (the repo-root default pixi env has it)" >&2
    exit 1
}
[ -x "$JAVA_HOME/bin/java" ] || {
    echo "no java under JAVA_HOME=$JAVA_HOME" >&2
    exit 1
}
[ -e "$SIRIUS_LIB/libsirius.so" ] || {
    echo "libsirius.so missing under $SIRIUS_LIB — build the engine first" >&2
    exit 1
}
for table in customer lineitem nation orders part partsupp region supplier; do
    compgen -G "$TPCH_DATA/$table/*.parquet" >/dev/null || {
        echo "no parquet under $TPCH_DATA/$table" >&2
        exit 1
    }
done

port_busy() {
    "$PYTHON" -c 'import socket, sys; s = socket.socket(); s.settimeout(1); sys.exit(0 if s.connect_ex(("127.0.0.1", int(sys.argv[1]))) == 0 else 1)' "$1"
}
ports=("$FE_QUERY_PORT" "$FE_HTTP_PORT" "$FE_RPC_PORT" "$FE_EDIT_LOG_PORT")
for i in $(seq 0 $((NUM_CNS - 1))); do
    for off in 0 1 2 3 4; do
        ports+=($((PORT_BASE + i * PORT_STRIDE + off)))
    done
done
for port in "${ports[@]}"; do
    if port_busy "$port"; then
        echo "port $port is already in use; set FE_*_PORT / PORT_BASE" >&2
        exit 1
    fi
done

# The GPUs may be shared with other sessions; each CN reserves most of its GPU.
busy=$(nvidia-smi --query-compute-apps=gpu_bus_id,pid,used_memory --format=csv,noheader || true)
if [[ -n "$busy" && "${ALLOW_BUSY_GPUS:-0}" != 1 ]]; then
    echo "GPUs already have compute processes (ALLOW_BUSY_GPUS=1 to ignore):" >&2
    echo "$busy" >&2
    exit 2
fi

# A leftover CUDA_VISIBLE_DEVICES in the operator shell would pin every CN to one GPU.
unset CUDA_VISIBLE_DEVICES

rm -rf "$E2E"
mkdir -p "$E2E/frags" "$E2E/fe/"{conf,meta,log} "$ORACLE_DIR"

cn_pids=()
dump_logs_on_fail=1

cleanup() {
    status=$?
    trap - EXIT INT TERM
    if [[ "$dump_logs_on_fail" -eq 1 && "$status" -ne 0 ]]; then
        echo "---- fe.warn.log (tail) ----" >&2
        tail -n 40 "$E2E/fe/log/fe.warn.log" 2>/dev/null || tail -n 40 "$E2E/fe-start.log" 2>/dev/null || true
        for i in $(seq 0 $((NUM_CNS - 1))); do
            echo "---- cn$i.log (tail) ----" >&2
            tail -n 60 "$E2E/cn$i.log" 2>/dev/null || true
        done
    fi
    if [[ -x "$E2E/fe/bin/stop_fe.sh" ]]; then
        "$E2E/fe/bin/stop_fe.sh" >/dev/null 2>&1 || true
    fi
    for pid in "${cn_pids[@]}"; do
        kill -TERM -"$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    done
    # Releasing a 160 GB slab takes a few seconds.
    for _ in $(seq 1 20); do
        alive=0
        for pid in "${cn_pids[@]}"; do
            kill -0 "$pid" 2>/dev/null && alive=1
        done
        [[ "$alive" -eq 0 ]] && break
        sleep 1
    done
    for pid in "${cn_pids[@]}"; do
        kill -KILL -"$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
    done
    exit "$status"
}
trap cleanup EXIT INT TERM

set_fe_conf() {
    local file=$1 key=$2 value=$3
    if grep -qE "^[[:space:]]*#?[[:space:]]*${key}[[:space:]]*=" "$file"; then
        sed -i -E "s|^[[:space:]]*#?[[:space:]]*${key}[[:space:]]*=.*|${key} = ${value}|" "$file"
    else
        printf '%s = %s\n' "$key" "$value" >>"$file"
    fi
}

mysql_exec() {
    "$MYSQL" --host 127.0.0.1 --port "$FE_QUERY_PORT" --user root --batch --raw --skip-column-names "$@"
}

mysql_table() {
    "$MYSQL" --host 127.0.0.1 --port "$FE_QUERY_PORT" --user root --batch --raw "$@"
}

cat >"$E2E/sirius.yaml" <<YAML
sirius:
  topology:
    num_gpus: 1
  memory:
    gpu:
      usage_limit_fraction: ${GPU_FRACTION}
      allocator: slab
    host:
      capacity_bytes: ${HOST_BYTES}
  executor:
    pipeline:
      num_threads: ${PIPELINE_THREADS}
YAML

echo "== packaging isolated FE =="
cp -a "$STARROCKS_FE/bin" "$E2E/fe/bin"
cp -a "$STARROCKS_FE/conf/." "$E2E/fe/conf/"
for dir in lib webroot hive-udf spark-dpp; do
    ln -sfn "$STARROCKS_FE/$dir" "$E2E/fe/$dir"
done
# plugins/ is FE runtime state, absent from a freshly built package; a symlink to it
# dangles and the FE dies with "failed to create FE plugin dir".
mkdir -p "$E2E/fe/plugins"
fe_conf="$E2E/fe/conf/fe.conf"
set_fe_conf "$fe_conf" meta_dir "$E2E/fe/meta"
set_fe_conf "$fe_conf" sys_log_dir "$E2E/fe/log"
set_fe_conf "$fe_conf" audit_log_dir "$E2E/fe/log"
set_fe_conf "$fe_conf" priority_networks "127.0.0.1/32"
set_fe_conf "$fe_conf" query_port "$FE_QUERY_PORT"
set_fe_conf "$fe_conf" http_port "$FE_HTTP_PORT"
set_fe_conf "$fe_conf" rpc_port "$FE_RPC_PORT"
set_fe_conf "$fe_conf" edit_log_port "$FE_EDIT_LOG_PORT"
set_fe_conf "$fe_conf" qe_query_timeout_second "$QUERY_TIMEOUT_S"
set_fe_conf "$fe_conf" brpc_send_plan_fragment_timeout_ms 1800000
set_fe_conf "$fe_conf" files_query_whole_file_ranges true

echo "== starting FE on :$FE_QUERY_PORT =="
"$E2E/fe/bin/start_fe.sh" --daemon >"$E2E/fe-start.log" 2>&1
# start_fe.sh can return before catalog bootstrap is finished.
for _ in $(seq 1 90); do
    if mysql_exec -e "SHOW FRONTENDS" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done
mysql_exec -e "SHOW FRONTENDS" >/dev/null

# An unpatched FE ignores the unknown fe.conf key; ask it directly.
whole_file=$(mysql_exec -e "ADMIN SHOW FRONTEND CONFIG LIKE 'files_query_whole_file_ranges'" | cut -f3)
if [[ "$whole_file" != true ]]; then
    echo "FE at $STARROCKS_FE lacks files_query_whole_file_ranges (got '${whole_file}')." >&2
    echo "Apply the whole-file patch to starrocks/ (Config.java, FileScanNode.java) and re-run fe-build." >&2
    exit 1
fi

gpu_numa_node() {
    local bus
    bus=$(nvidia-smi -i "$1" --query-gpu=pci.bus_id --format=csv,noheader | tr 'A-F' 'a-f')
    cat "/sys/bus/pci/devices/${bus#0000}/numa_node" 2>/dev/null ||
        cat "/sys/bus/pci/devices/${bus}/numa_node" 2>/dev/null || echo -1
}

start_cn() {
    local i=$1 gpu=$2
    local base=$((PORT_BASE + i * PORT_STRIDE))
    local bind=() node=-1
    if [[ "$NUMA_BIND" == 1 ]] && command -v numactl >/dev/null; then
        node=$(gpu_numa_node "$gpu")
        if [[ "$node" -ge 0 ]]; then
            bind=(numactl --cpunodebind="$node")
        fi
    fi
    # CUDA remaps the pinned GPU to ordinal 0 inside the process.
    # NO_COLOR keeps tracing fields as plain key=value for the log checks below.
    setsid env \
        CUDA_VISIBLE_DEVICES="$gpu" \
        NO_COLOR=1 \
        UCX_TLS="$UCX_TLS" \
        NIXL_PREFIX="${NIXL_PREFIX:-}" \
        NIXL_PLUGIN_DIR="${NIXL_PLUGIN_DIR:-}" \
        NIXL_NO_STUBS_FALLBACK="${NIXL_NO_STUBS_FALLBACK:-1}" \
        LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
        RUST_LOG="${RUST_LOG:-sirius_starrocks_cn=info}" \
        RUST_BACKTRACE=1 \
        SIRIUS_CN_DUMP_FRAGMENTS="$E2E/frags" \
        "${bind[@]}" \
        stdbuf -oL -eL \
        "$CN_BIN" \
        --fe-host 127.0.0.1 \
        --fe-query-port "$FE_QUERY_PORT" \
        --advertise-host 127.0.0.1 \
        --heartbeat-port "$base" \
        --thrift-port $((base + 1)) \
        --brpc-port $((base + 2)) \
        --http-port $((base + 3)) \
        --starlet-port $((base + 4)) \
        --sirius-config "$E2E/sirius.yaml" \
        </dev/null >"$E2E/cn$i.log" 2>&1 &
    cn_pids+=($!)
    echo "CN$i gpu=$gpu numa=$node heartbeat=$base brpc=$((base + 2)) http=$((base + 3)) pid=$!"
}

cns_alive() {
    local i
    for i in "${!cn_pids[@]}"; do
        if ! kill -0 "${cn_pids[$i]}" 2>/dev/null; then
            echo "CN$i (pid ${cn_pids[$i]}) exited" >&2
            return 1
        fi
    done
}

echo "== starting $NUM_CNS CNs =="
for i in $(seq 0 $((NUM_CNS - 1))); do
    start_cn "$i" "${GPUS[$i]}"
done

echo "== waiting for $NUM_CNS Alive compute nodes =="
alive=0
for _ in $(seq 1 120); do
    cns_alive || exit 1
    alive=$({ mysql_table -e "SHOW COMPUTE NODES" 2>/dev/null || true; } |
        awk -F'\t' 'NR == 1 { for (c = 1; c <= NF; c++) if ($c == "Alive") col = c; next }
                    col && tolower($col) == "true" { n++ } END { print n + 0 }')
    [[ "$alive" -ge "$NUM_CNS" ]] && break
    sleep 2
done
if [[ "$alive" -lt "$NUM_CNS" ]]; then
    echo "only $alive of $NUM_CNS compute nodes came up Alive" >&2
    mysql_table -e "SHOW COMPUTE NODES" >&2 || true
    exit 1
fi
mysql_table -e "SHOW COMPUTE NODES" | cut -f1-9

SQL_DIR="$HERE/tpch"
SESSION="SET query_timeout = ${QUERY_TIMEOUT_S}; SET pipeline_dop = 1; SET parallel_fragment_exec_instance_num = 1;"
data_tag=$(printf '%s' "$TPCH_DATA" | tr -c 'A-Za-z0-9._-' '_')

results=()
failed=0
cn_died=0
for q in $QUERIES; do
    if [[ "$cn_died" -eq 1 ]]; then
        results+=("SKIP ${q}")
        failed=$((failed + 1))
        continue
    fi
    echo "== running TPC-H ${q} via FILES() on ${NUM_CNS} CNs =="
    sql=$(sed "s|__TPCH_DATA__|${TPCH_DATA}|g" "$SQL_DIR/${q}.sql")
    sql_hash=$(sha256sum "$SQL_DIR/${q}.sql" | cut -c1-12)
    oracle="$ORACLE_DIR/$data_tag/${q}-${sql_hash}.json"
    [[ -e "$oracle" ]] || echo "   (no cached oracle; DuckDB computes it after the query, which can take minutes)"
    start=$SECONDS
    if mysql_table -e "${SESSION} ${sql}" >"$E2E/${q}.tsv" 2>"$E2E/${q}.err"; then
        elapsed=$((SECONDS - start))
        cat "$E2E/${q}.tsv"
        if "$PYTHON" "$HERE/tpch_files_compare.py" "$SQL_DIR/${q}.sql" "$TPCH_DATA" "$E2E/${q}.tsv" "$oracle"; then
            results+=("PASS ${q} ${elapsed}s")
        else
            results+=("FAIL ${q} ${elapsed}s (result mismatch)")
            failed=$((failed + 1))
        fi
    else
        elapsed=$((SECONDS - start))
        cat "$E2E/${q}.err" >&2
        results+=("FAIL ${q} ${elapsed}s ($(head -c 160 "$E2E/${q}.err" | tr '\n' ' '))")
        failed=$((failed + 1))
    fi
    cns_alive || cn_died=1
done

echo "== checking packed NIXL hops over transmit_chunk =="
hops=PASS
"$PYTHON" - "$E2E" "$NUM_CNS" <<'PY' || hops=FAIL
import re, sys
from pathlib import Path
e2e, n = Path(sys.argv[1]), int(sys.argv[2])
hop_re = re.compile(r"shipping packed exchange hop.*\bbytes=(\d+)\b")
recv_re = re.compile(r"received remote packed batches.*\bbatches=(\d+)")
names = [f"cn{i}" for i in range(n)]
logs = {name: (e2e / f"{name}.log").read_text(errors="replace") for name in names}
shipped = {name: sum(int(m.group(1)) for m in hop_re.finditer(text)) for name, text in logs.items()}
received = {name: sum(int(m.group(1)) for m in recv_re.finditer(text)) for name, text in logs.items()}
if not any(shipped.values()):
    raise SystemExit(f"no non-empty remote packed hop logged (shipped={shipped})")
# Every CN that shipped needs some other CN to have received.
unmatched = [name for name in names if shipped[name] and not any(received[p] for p in names if p != name)]
if unmatched:
    raise SystemExit(f"{unmatched} shipped but no peer logged a receive (received={received})")
if not any("transmit_chunk" in text for text in logs.values()):
    raise SystemExit("no transmit_chunk in CN logs")
print("packed hops", {"shipped_bytes": shipped, "received_batches": received})
PY
[[ "$hops" == PASS ]] || failed=$((failed + 1))

echo "== summary (data=${TPCH_DATA}, cns=${NUM_CNS}, logs=${E2E}) =="
printf '%s\n' "${results[@]}" "${hops} nixl-hops"
[[ "$failed" -eq 0 ]] || exit 1

dump_logs_on_fail=0
echo "OK: SF1000 TPC-H join queries matched DuckDB across ${NUM_CNS} CNs with a packed NIXL shuffle"
