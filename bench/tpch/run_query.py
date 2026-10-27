#!/usr/bin/env python3
"""Run one TPC-H query distributed on N ranks (one per GPU) and check its answer.

usage: run_query.py Q --out DIR --config YAML (--data DIR | --table T=DIR[,DIR...] ...)
                    [--sf 10000] [--sql FILE] [--iters 3] [--ranks 4] [--host-pool 200Gi]
                    [--ref RANK0_LOG] [--pin-tier host|gpu|parquet] [--tag NAME]

- Query Q's text is test/tpch_performance/queries.py's at --sf, or --sql FILE's.
- Creates views only for the tables the query reads, over DIR/<table>/*.parquet.
- Runs the query --iters times in one process per rank: the first run is cold, the best of the
  others is warm.
- Answer checks: every iteration against the first, and against --ref (rank 0's log of an earlier
  run of the same query text, e.g. from the full dataset) when given.
- Quent telemetry lands in DIR/telemetry_data (one directory per rank); see extract.py.
- --pin-tier pins every table with pin_table before the runs (needs a build with rank-aware
  pin_table). The result records each pin's time and how many scans were served from a pin.
Run it through pixi from the repo root: pixi run python bench/tpch/run_query.py ...
"""
import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)
from columns import cols, text  # noqa: E402
from results import check, parse  # noqa: E402


def clear(d):
    for p in glob.glob(f"{d}/*"):
        shutil.rmtree(p, ignore_errors=True) if os.path.isdir(p) else os.remove(p)


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("q", type=int)
    ap.add_argument("--out", required=True)
    ap.add_argument(
        "--config",
        required=True,
        help="Sirius YAML; @SPILL_DIR@ is replaced by --spill-dir",
    )
    ap.add_argument("--data", help="one directory holding every table")
    ap.add_argument(
        "--table",
        action="append",
        default=[],
        help="TABLE=DIR[,DIR...] (overrides --data)",
    )
    ap.add_argument(
        "--sf", type=int, default=10000, help="scale factor of the query text"
    )
    ap.add_argument("--sql", help="file with the query text to run instead of TPC-H Q")
    ap.add_argument(
        "--host-pool", help="override the config's host capacity_bytes, e.g. 200Gi"
    )
    ap.add_argument("--iters", type=int, default=3)
    ap.add_argument("--ranks", type=int, default=4)
    ap.add_argument("--port", type=int, default=31000)
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--ref", help="rank 0 log of an earlier run of the same query text")
    ap.add_argument(
        "--spill-dir", default=f"/tmp/{os.environ.get('USER', 'sirius')}/sirius-disk"
    )
    ap.add_argument("--pin-tier", choices=["host", "gpu", "parquet"])
    ap.add_argument(
        "--pin-plans",
        help="Simpatico plan directory (<table>.txt): pin with compression; tables without a "
        "plan pin uncompressed",
    )
    ap.add_argument(
        "--pin-skip",
        default="",
        help="tables not to pin, e.g. nation,region",
    )
    ap.add_argument("--tag", default="")
    a = ap.parse_args()

    if subprocess.run(["pgrep", "-x", "duckdb"], capture_output=True).returncode == 0:
        sys.exit(
            "a duckdb process is running: wait for it, or stop it, before benchmarking"
        )
    tables = cols(a.q, a.sf, a.sql)
    dirs = {t: a.data for t in tables} if a.data else {}
    dirs.update(dict(x.split("=", 1) for x in a.table))
    missing = [t for t in tables if not dirs.get(t)]
    if missing:
        sys.exit(f"no directory for {missing}: pass --data or --table")

    os.makedirs(a.out, exist_ok=True)
    a.out = os.path.abspath(a.out)
    stem = f"_{os.path.splitext(os.path.basename(a.sql))[0]}" if a.sql else ""
    name = f"q{a.q:02d}{stem}{a.tag}"
    qout = f"{a.out}/{name}"
    os.makedirs(qout, exist_ok=True)
    os.makedirs(a.spill_dir, exist_ok=True)
    cfg = f"{qout}/sirius.yaml"
    yaml = open(a.config).read().replace("@SPILL_DIR@", a.spill_dir)
    if a.host_pool:
        yaml = re.sub(
            r"(host: \{ capacity_bytes: )[^ }]+", rf"\g<1>{a.host_pool}", yaml
        )
    open(cfg, "w").write(yaml)

    def globs(t):
        g = [f"'{d}/{t}/*.parquet'" for d in dirs[t].split(",")]
        return g[0] if len(g) == 1 else "[" + ", ".join(g) + "]"

    lines = [
        f"create view {t} as select * from read_parquet({globs(t)});" for t in tables
    ]
    if a.pin_tier and a.pin_plans:
        # Before the timer, so parse() sees only the pins and the queries.
        lines.append(
            f"SET pin_table_input_compression_plan_dir = '{os.path.abspath(a.pin_plans)}';"
        )
    lines += [".mode csv", ".timer on"]
    pin_tables = {}
    if a.pin_tier:
        skip = set(a.pin_skip.split(",")) if a.pin_skip else set()
        pin_tables = {t: cs for t, cs in tables.items() if t not in skip}
        # Timed like the queries: parse() returns the pins first, one statement per table.
        for t, cs in pin_tables.items():
            if "," in dirs[t]:
                sys.exit(f"--pin-tier needs one directory per table; {t} has {dirs[t]}")
            col_list = ", ".join(f"'{c}'" for c in cs)
            lines.append(
                f"CALL pin_table('{dirs[t]}/{t}/*.parquet', name = '{t}', "
                f"tier = '{a.pin_tier}', cols = [{col_list}]"
                + (", compression = true);" if a.pin_plans else ");")
            )
    lines += [text(a.q, a.sf, a.sql) + ";"] * a.iters
    sqlf = f"{a.out}/{name}.sql"
    open(sqlf, "w").write("\n".join(lines) + "\n")

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
    # The working directory decides where telemetry_data goes: keep it out of the repo.
    p = subprocess.run(
        [
            f"{REPO}/scripts/exchange_sql_ranks.sh",
            str(a.ranks),
            sqlf,
            str(a.port + a.q * 10),
            qout,
        ],
        cwd=a.out,
        env=env,
        capture_output=True,
        text=True,
        timeout=a.timeout + 120,
    )
    wall = time.time() - t0
    spill = sum(
        os.path.getsize(os.path.join(r, f))
        for r, _, fs in os.walk(a.spill_dir)
        for f in fs
    )
    clear(a.spill_dir)

    log = f"{qout}/exchange_rank_0.log"
    runs = parse(open(log, errors="replace").read()) if os.path.exists(log) else []
    npins = len(pin_tables)
    pin_s = [r[2] for r in runs[:npins]]
    runs = runs[npins:]
    times = [r[2] for r in runs]
    # One line per scan served from a pinned table, from every rank.
    pin_hits = sum(
        open(f, errors="replace").read().count("assigned pinned entry")
        for f in glob.glob(f"{qout}/logs/*.log")
    )
    same = [check(r[0], runs[0][0]) for r in runs]
    ref = parse(open(a.ref, errors="replace").read()) if a.ref else []
    vs_ref = (
        [check(r[0], ref[-1][0]) for r in runs] if ref else ["unverified"] * len(runs)
    )
    res = dict(
        query=a.q,
        name=name,
        sf=a.sf,
        sql=a.sql,
        tag=a.tag,
        rc=p.returncode,
        wall_s=round(wall, 1),
        times_s=times,
        cold_s=times[0] if times else None,
        warm_s=min(times[1:]) if len(times) > 1 else None,
        rows=len(runs[0][0]) if runs else 0,
        same_as_first=same,
        vs_ref=vs_ref,
        spill_bytes=spill,
        config=os.path.basename(a.config),
        host_pool=a.host_pool,
        pin_tier=a.pin_tier,
        pinned_tables=list(pin_tables),
        pin_plans=a.pin_plans,
        pin_s=pin_s,
        pin_hits=pin_hits,
    )
    json.dump(res, open(f"{qout}/result.json", "w"), indent=1)
    with open(f"{a.out}/results.jsonl", "a") as f:
        f.write(json.dumps(res) + "\n")
    print(json.dumps(res))
    if p.returncode or len(runs) != a.iters:
        print(p.stdout[-3000:], p.stderr[-3000:], sep="\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
