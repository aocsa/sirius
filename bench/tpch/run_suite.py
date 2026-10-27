#!/usr/bin/env python3
"""Run TPC-H queries in one set of long-lived processes, one per GPU, on a cold, lukewarm or hot
schedule. The same schedules as the one-process benchmark, so 1 GPU and N GPUs compare directly.

usage: run_suite.py --out DIR --config YAML --data DIR [--sf 10000] [--queries 1,2,...]
                    [--schedule cold|lukewarm|hot] [--warmups 1] [--repeats 2] [--ranks 4]
                    [--prime-sql SQL] [--host-pool 200Gi] [--ref-dir DIR] [--tag NAME]
                    [--timeout 7200] [--dry-run]
                    [--pin-tier host|gpu|parquet] [--pin-plans DIR] [--pin-only T,T]
                    [--pin-skip T,T] [--pin-replicate T,T] [--pin-exclude o_comment]

Schedules (as tools/benchmark.py on integ/sf3k-joost-2201-2204):
  cold      Q1 … Qn once each; CALL reset_sirius_cache() before every query.
  lukewarm  --warmups sweeps of Q1 … Qn, then --repeats timed sweeps; no resets.
  hot       per query, --warmups runs then --repeats timed runs; no resets.
Cache resets and --prime-sql (one statement after setup, before the schedule, so the first query
carries no first-use costs the others do not) run with the timer off and their output dropped.
With --ranks 1 the launcher configures no exchange: the one-process engine path.

Writes, under DIR:
  executions.jsonl  one line per execution: query, phase (warmup|measured), repeat, seconds,
                    rows, same_as_first, vs_ref, rc
  <name>/answers/   rank 0's rows of every measured run, q<N>_r<repeat>.csv (report.py validates)
  invocation.json   commit, build hashes, config hash, data, schedule, placement (GPUS, CPUS,
                    THREADS from the environment)
  <name>/           every rank's log, the Sirius logs and the config used
Pins (--pin-tier) are optional and made once before the schedule; see README.md.
Run it through pixi from the repo root: pixi run python bench/tpch/run_suite.py ...
"""
import argparse
import csv
import datetime
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)
from columns import SIZES, cols, text  # noqa: E402
from results import check, parse  # noqa: E402

SPILL_MARK = ".tpch-bench-spill"


def quiet(stmt):
    """A statement rank 0's parser never sees: no timer line, no output."""
    return f".timer off\n.once /dev/null\n{stmt};\n.timer on"


def schedule(kind, queries, warmups, repeats):
    """[(query, phase, repeat)] in execution order, as tools/benchmark.py:schedule()."""
    if kind == "cold":
        return [(q, "measured", 1) for q in queries]
    if kind == "lukewarm":
        return [
            (q, phase, i)
            for phase, n in (("warmup", warmups), ("measured", repeats))
            for i in range(1, n + 1)
            for q in queries
        ]
    return [
        (q, phase, i)
        for q in queries
        for phase, n in (("warmup", warmups), ("measured", repeats))
        for i in range(1, n + 1)
    ]


def clear(d):
    """Empty a spill directory this script created (it carries SPILL_MARK); refuse any other."""
    os.makedirs(d, exist_ok=True)
    entries = [e for e in os.listdir(d) if e != SPILL_MARK]
    if entries and not os.path.exists(os.path.join(d, SPILL_MARK)):
        sys.exit(f"--spill-dir {d} holds files this script did not create; pick an empty directory")
    for e in entries:
        p = os.path.join(d, e)
        subprocess.run(["rm", "-rf", p], check=True)
    open(os.path.join(d, SPILL_MARK), "w").close()


def to_bytes(v):
    """Sirius-style byte sizes: 210Gi, 4GB, 1MiB, or a plain integer."""
    m = re.fullmatch(r"\s*([0-9.]+)\s*([KMGT]?)(i?)B?\s*", str(v))
    if not m:
        sys.exit(f"cannot read the byte size {v!r}")
    n, unit, binary = m.groups()
    return int(float(n) * (1024 if binary else 1000) ** " KMGT".index(unit or " "))


def check_memory(ranks, pool, overhead_gib):
    """Refuse a run whose host pools (plus per-rank overhead) overfill a GPU's CPU NUMA node."""
    gpus = os.environ.get("GPUS") or ",".join(str(i) for i in range(ranks))
    gpus = gpus.split(",")[:ranks]
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,pci.bus_id", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=30, check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        print("warning: nvidia-smi unavailable, host memory not checked", file=sys.stderr)
        return
    node_of = {}
    for line in out.splitlines():
        idx, bus = (x.strip() for x in line.split(","))
        with open(f"/sys/bus/pci/devices/{bus[4:].lower()}/numa_node") as f:
            node_of[idx] = max(0, int(f.read()))
    need = {}
    for g in gpus:
        n = node_of.get(g, 0)
        need[n] = need.get(n, 0) + pool + overhead_gib * 2**30
    for n, b in need.items():
        with open(f"/sys/devices/system/node/node{n}/meminfo") as f:
            total = int(next(l.split()[3] for l in f if "MemTotal" in l)) * 1024
        if b > 0.95 * total:
            sys.exit(
                f"NUMA node {n}: {b / 2**30:.0f} GiB of host pools and per-rank overhead "
                f"exceed 95% of its {total / 2**30:.0f} GiB; lower --host-pool or pass "
                "--skip-memory-check"
            )


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(1 << 24):
            h.update(b)
    return h.hexdigest()


def git(*args):
    r = subprocess.run(["git", "-C", REPO, *args], capture_output=True, text=True)
    return r.stdout.strip()


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--out", required=True)
    ap.add_argument("--config", required=True, help="Sirius YAML; @SPILL_DIR@ is replaced")
    ap.add_argument("--data", required=True)
    ap.add_argument("--sf", type=int, default=10000)
    ap.add_argument("--queries", default=",".join(str(q) for q in range(1, 23)))
    ap.add_argument("--schedule", choices=["cold", "lukewarm", "hot"], default="hot")
    ap.add_argument("--warmups", type=int, default=1)
    ap.add_argument("--repeats", type=int, default=2)
    ap.add_argument("--ranks", type=int, default=4)
    ap.add_argument(
        "--prime-sql",
        default="select count(*) from nation join region on n_regionkey = r_regionkey",
        help="untimed statement after setup; empty to skip",
    )
    ap.add_argument("--port", type=int, default=33000)
    ap.add_argument("--timeout", type=int, default=7200, help="seconds for the whole suite")
    ap.add_argument("--pin-tier", default="none", choices=["none", "host", "gpu", "parquet"])
    ap.add_argument("--pin-plans", help="Simpatico plan directory: pin with compression")
    ap.add_argument("--pin-only", default="", help="pin only these tables")
    ap.add_argument("--pin-skip", default="", help="tables not to pin")
    ap.add_argument("--pin-exclude", default="", help="columns not to pin, e.g. o_comment")
    ap.add_argument(
        "--pin-replicate", default="", help="tables to pin whole on every rank"
    )
    ap.add_argument(
        "--host-pool",
        help="override the config's host capacity_bytes (and initial_number_pools to match)",
    )
    ap.add_argument(
        "--rank-overhead-gib",
        type=int,
        default=40,
        help="host memory each rank uses outside its pool, for the NUMA-node check",
    )
    ap.add_argument("--skip-memory-check", action="store_true")
    ap.add_argument("--ref-dir", help="an earlier run_query.py output, for reference answers")
    ap.add_argument(
        "--spill-dir", default=f"/tmp/{os.environ.get('USER', 'sirius')}/tpch-bench-spill"
    )
    ap.add_argument("--tag", default="")
    ap.add_argument("--dry-run", action="store_true", help="write the SQL file and stop")
    a = ap.parse_args()
    if a.schedule == "cold" and (a.warmups, a.repeats) != (1, 2):
        sys.exit("cold is one measured run per query: drop --warmups/--repeats")
    if a.schedule != "cold" and (a.warmups < 1 or a.repeats < 1):
        sys.exit("lukewarm and hot need --warmups >= 1 and --repeats >= 1")
    if not a.dry_run and subprocess.run(["pgrep", "-x", "duckdb"], capture_output=True).returncode == 0:
        sys.exit("a duckdb process is running: wait for it, or stop it, before benchmarking")

    queries = [int(q) for q in a.queries.split(",")]
    texts = {q: text(q, a.sf) for q in queries}
    union = {}
    for q in queries:
        for t, cs in cols(q, a.sf).items():
            union.setdefault(t, set()).update(cs)
    views = set(union) | ({"nation", "region"} if a.prime_sql else set())
    pins = {}
    if a.pin_tier != "none":
        skip = set(filter(None, a.pin_skip.split(",")))
        exclude = set(filter(None, a.pin_exclude.split(",")))
        pins = {
            t: [c for c in SIZES[t] if c in cs and c not in exclude]
            for t, cs in union.items()
            if t not in skip and (not a.pin_only or t in a.pin_only.split(","))
        }
    replicate = set(filter(None, a.pin_replicate.split(",")))
    plan = schedule(a.schedule, queries, a.warmups, a.repeats)

    os.makedirs(a.out, exist_ok=True)
    a.out = os.path.abspath(a.out)
    name = f"suite_{a.schedule}{a.tag}"
    qout = f"{a.out}/{name}"
    if os.path.exists(f"{qout}/exchange_rank_0.log"):
        sys.exit(f"{qout} holds an earlier run: use a new --out or --tag")
    os.makedirs(f"{qout}/answers", exist_ok=True)
    import yaml

    conf = yaml.safe_load(open(a.config).read().replace("@SPILL_DIR@", a.spill_dir))
    host = conf["sirius"]["memory"]["host"]
    if a.host_pool:
        host["capacity_bytes"] = a.host_pool
        if "initial_number_pools" in host:  # keep pinning the whole pool at start
            per_pool = to_bytes(host.get("block_size", "1MiB")) * int(host.get("pool_size", 128))
            host["initial_number_pools"] = to_bytes(a.host_pool) // per_pool
    cfg = f"{qout}/sirius.yaml"
    open(cfg, "w").write(yaml.safe_dump(conf, sort_keys=False))
    if not a.dry_run and not a.skip_memory_check:
        check_memory(a.ranks, to_bytes(host["capacity_bytes"]), a.rank_overhead_gib)

    lines = [
        f"create view {t} as select * from read_parquet('{a.data}/{t}/*.parquet');"
        for t in sorted(views)
    ]
    if a.pin_plans:
        lines.append(
            f"SET pin_table_input_compression_plan_dir = '{os.path.abspath(a.pin_plans)}';"
        )
    lines += [".mode csv", ".timer on"]
    for t, cs in pins.items():
        col_list = ", ".join(f"'{c}'" for c in cs)
        lines.append(
            f"CALL pin_table('{a.data}/{t}/*.parquet', name = '{t}', tier = '{a.pin_tier}', "
            f"cols = [{col_list}]"
            + (", replicate = true" if t in replicate else "")
            + (", compression = true);" if a.pin_plans else ");")
        )
    if a.prime_sql:
        lines.append(quiet(a.prime_sql))
    lines.append(quiet("CALL reset_sirius_cache()"))
    for q, phase, i in plan:
        if a.schedule == "cold":
            lines.append(quiet("CALL reset_sirius_cache()"))
        lines.append(texts[q] + ";")
    sqlf = f"{a.out}/{name}.sql"
    open(sqlf, "w").write("\n".join(lines) + "\n")
    json.dump(plan, open(f"{qout}/schedule.json", "w"))
    if a.dry_run:
        print(f"wrote {sqlf}: {len(pins)} pins, {len(plan)} executions ({a.schedule})")
        return

    build = os.environ.get("SIRIUS_BUILD_DIR", f"{REPO}/sirius-duckdb/build/release")
    binaries = {
        "duckdb": f"{build}/duckdb",
        "extension": f"{build}/extension/sirius/sirius.duckdb_extension",
        "libsirius": os.path.normpath(f"{build}/../../../build/release/install/lib/libsirius.so.0.0.0"),
    }
    json.dump(
        dict(
            started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            host=socket.gethostname(),
            commit=git("rev-parse", "HEAD"),
            dirty=bool(git("status", "--porcelain", "--untracked-files=no")),
            build=build,
            build_sha256={k: sha256(p) for k, p in binaries.items() if os.path.exists(p)},
            config=os.path.abspath(a.config),
            config_sha256=sha256(a.config),
            rendered_config_sha256=sha256(cfg),
            data=a.data,
            sf=a.sf,
            queries=queries,
            schedule=a.schedule,
            warmups=a.warmups,
            repeats=a.repeats,
            ranks=a.ranks,
            prime_sql=a.prime_sql,
            host_pool=a.host_pool,
            pins=list(pins),
            placement={k: os.environ.get(k) for k in ("GPUS", "CPUS", "THREADS", "PREAMBLE")},
            queries_sha256=sha256(f"{REPO}/test/tpch_performance/queries.py"),
        ),
        open(f"{a.out}/invocation.json", "w"),
        indent=1,
    )

    env = dict(
        os.environ,
        SIRIUS_CONFIG_FILE=cfg,
        SIRIUS_LOG_DIR=f"{qout}/logs",
        EXCHANGE_TIMEOUT=str(a.timeout),
    )
    for k in ("AI_AGENT", "CLAUDECODE", "CLAUDE_CODE"):
        env.pop(k, None)
    clear(a.spill_dir)
    t0 = time.time()
    p = subprocess.run(
        [f"{REPO}/scripts/exchange_sql_ranks.sh", str(a.ranks), sqlf, str(a.port), qout],
        cwd=a.out,
        env=env,
        capture_output=True,
        text=True,
        timeout=a.timeout + 300,
    )
    wall = time.time() - t0
    spill = sum(
        os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(a.spill_dir) for f in fs
    )
    clear(a.spill_dir)

    runs = parse(open(f"{qout}/exchange_rank_0.log", errors="replace").read())
    pin_runs, runs = runs[: len(pins)], runs[len(pins) :]
    first, refs = {}, {}
    with open(f"{a.out}/executions.jsonl", "a") as out:
        for k, (q, phase, i) in enumerate(plan):
            got = runs[k] if k < len(runs) else None
            if q not in refs and a.ref_dir:
                for d in (f"q{q:02d}_direct", f"q{q:02d}"):
                    f = f"{a.ref_dir}/{d}/exchange_rank_0.log"
                    if os.path.exists(f):
                        refs[q] = parse(open(f, errors="replace").read())[-1][0]
                        break
            rec = dict(query=q, phase=phase, repeat=i, schedule=a.schedule, sf=a.sf, ranks=a.ranks)
            if got is None or not got[1] or any("Error" in c for c in got[1]):
                rec.update(rc=1, seconds=None)
            else:
                rows, header, secs = got
                first.setdefault(q, rows)
                rec.update(
                    rc=0,
                    seconds=secs,
                    rows=len(rows),
                    same_as_first=check(rows, first[q]),
                    vs_ref=check(rows, refs[q]) if q in refs else "unverified",
                )
                if phase == "measured":
                    with open(f"{qout}/answers/q{q}_r{i}.csv", "w", newline="") as f:
                        csv.writer(f).writerows([header] + rows)
            out.write(json.dumps(rec) + "\n")
    json.dump(
        dict(
            pin_s=dict(zip(pins, [r[2] for r in pin_runs])),
            pin_ok=all(r[0] == [["true"]] for r in pin_runs) and len(pin_runs) == len(pins),
            wall_s=round(wall, 1),
            spill_bytes=spill,
            executions=len(plan),
            parsed=len(runs),
            rc=p.returncode,
        ),
        open(f"{a.out}/{name}-summary.json", "w"),
        indent=1,
    )
    print(f"{len(runs)}/{len(plan)} executions parsed, rc={p.returncode}, wall {wall:.0f} s")
    if p.returncode or len(runs) != len(plan):
        print(p.stdout[-3000:], p.stderr[-3000:], sep="\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
