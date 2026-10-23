#!/usr/bin/env python3
"""Run one TPC-H query distributed on N ranks (one per GPU) and check its answer.

usage: run_query.py Q --out DIR --config YAML (--data DIR | --table T=DIR[,DIR...] ...)
                    [--std] [--iters 3] [--ranks 4] [--ref RANK0_LOG] [--pin-tier host|gpu|parquet]

- Creates views only for the tables query Q reads, over DIR/<table>/*.parquet.
- Runs the query --iters times in one process per rank: the first run is cold, the best of the
  others is warm.
- Answer checks: every iteration against the first, and against --ref (rank 0's log of an earlier
  run of the same query text, e.g. from GPFS) when given.
- Quent telemetry lands in DIR/telemetry_data (one directory per rank); see extract.py.
- --pin-tier pins every table with pin_table before the runs (needs a build with rank-aware
  pin_table; untested at the time of writing).
Run it through pixi from the repo root: pixi run python bench/dist-q5-sf10000/run_query.py ...
"""
import argparse
import glob
import json
import os
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
        "--std",
        action="store_true",
        help="Q5: the queries.py text, not q5_subquery.sql",
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
    ap.add_argument("--tag", default="")
    a = ap.parse_args()

    if subprocess.run(["pgrep", "-x", "duckdb"], capture_output=True).returncode == 0:
        sys.exit(
            "a duckdb process is running: wait for it, or stop it, before benchmarking"
        )
    tables = cols(a.q, a.std)
    dirs = {t: a.data for t in tables} if a.data else {}
    dirs.update(dict(x.split("=", 1) for x in a.table))
    missing = [t for t in tables if not dirs.get(t)]
    if missing:
        sys.exit(f"no directory for {missing}: pass --data or --table")

    os.makedirs(a.out, exist_ok=True)
    a.out = os.path.abspath(a.out)
    name = f"q{a.q:02d}{'_std' if a.std else ''}{a.tag}"
    qout = f"{a.out}/{name}"
    os.makedirs(qout, exist_ok=True)
    os.makedirs(a.spill_dir, exist_ok=True)
    cfg = f"{qout}/sirius.yaml"
    open(cfg, "w").write(open(a.config).read().replace("@SPILL_DIR@", a.spill_dir))

    def globs(t):
        g = [f"'{d}/{t}/*.parquet'" for d in dirs[t].split(",")]
        return g[0] if len(g) == 1 else "[" + ", ".join(g) + "]"

    lines = [
        f"create view {t} as select * from read_parquet({globs(t)});" for t in tables
    ]
    if a.pin_tier:
        lines.append(".output /dev/null")
        for t, cs in tables.items():
            if "," in dirs[t]:
                sys.exit(f"--pin-tier needs one directory per table; {t} has {dirs[t]}")
            col_list = ", ".join(f"'{c}'" for c in cs)
            lines.append(
                f"CALL pin_table('{dirs[t]}/{t}/*.parquet', name = '{t}', "
                f"tier = '{a.pin_tier}', cols = [{col_list}]);"
            )
        lines.append(".output")
    lines += [".mode csv", ".timer on"] + [text(a.q, a.std) + ";"] * a.iters
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
    times = [r[2] for r in runs]
    same = [check(r[0], runs[0][0]) for r in runs]
    ref = parse(open(a.ref, errors="replace").read()) if a.ref else []
    vs_ref = (
        [check(r[0], ref[-1][0]) for r in runs] if ref else ["unverified"] * len(runs)
    )
    res = dict(
        query=a.q,
        std=a.std,
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
        config=a.config,
        pin_tier=a.pin_tier,
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
