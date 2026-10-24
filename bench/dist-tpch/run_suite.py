#!/usr/bin/env python3
"""Pin the TPC-H tables once, then run every query against the pins, in one process per rank.

usage: run_suite.py --out DIR --config YAML --data DIR [--sf 10000] [--queries 1,2,...]
                    [--iters 3] [--ranks 4] [--pin-tier host] [--pin-plans DIR]
                    [--pin-only T,T] [--pin-skip T,T] [--pin-replicate T,T]
                    [--pin-exclude o_comment]
                    [--host-pool 200Gi]
                    [--ref-dir DIR] [--tag _pinonce] [--timeout 7200]

- Pins, per table, the union of the columns the chosen queries read, minus --pin-exclude.
  A query that reads an excluded column reads that table from disk instead.
- Pins the smallest tables first, so one that does not fit under pin_table_memory_limit is the
  largest; queries then read it from disk.
- --pin-replicate pins those tables whole on every rank (small ones, such as nation, region and
  supplier), so joins build from them without exchanging them.
- Runs each query --iters times: the first is cold (the pins are already resident), the best
  of the rest is warm.
- Checks every iteration against the first, and against --ref-dir's rank 0 log of the same
  query (DIR/q<NN>_direct/ or DIR/q<NN>/), when there is one.
- Appends one line per query to DIR/results.jsonl (summarize.py reads it), with the number of
  scans served from pins during that query; the pins' times go to DIR/<tag>-pins.json.
Run it through pixi from the repo root: pixi run python bench/dist-tpch/run_suite.py ...
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
from columns import SIZES, cols, text  # noqa: E402
from results import check, parse  # noqa: E402


def clear(d):
    for p in glob.glob(f"{d}/*"):
        shutil.rmtree(p, ignore_errors=True) if os.path.isdir(p) else os.remove(p)


def norm(sql):
    return " ".join(sql.split()).lower()


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--out", required=True)
    ap.add_argument("--config", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--sf", type=int, default=10000)
    ap.add_argument("--queries", default=",".join(str(q) for q in range(1, 23)))
    ap.add_argument("--iters", type=int, default=3)
    ap.add_argument("--ranks", type=int, default=4)
    ap.add_argument("--port", type=int, default=33000)
    ap.add_argument("--timeout", type=int, default=7200)
    ap.add_argument("--pin-tier", default="host", choices=["host", "gpu", "parquet"])
    ap.add_argument(
        "--pin-plans", help="Simpatico plan directory: pin with compression"
    )
    ap.add_argument(
        "--pin-only", default="", help="pin only these tables, e.g. partsupp,supplier"
    )
    ap.add_argument(
        "--pin-skip", default="", help="tables not to pin, e.g. nation,region"
    )
    ap.add_argument(
        "--pin-exclude", default="", help="columns not to pin, e.g. o_comment"
    )
    ap.add_argument(
        "--pin-replicate",
        default="",
        help="tables to pin whole on every rank, e.g. nation,region,supplier",
    )
    ap.add_argument("--host-pool", help="override the config's host capacity_bytes")
    ap.add_argument("--ref-dir", help="an earlier run's output, for reference answers")
    ap.add_argument(
        "--spill-dir", default=f"/tmp/{os.environ.get('USER', 'sirius')}/sirius-disk"
    )
    ap.add_argument("--tag", default="_pinonce")
    a = ap.parse_args()

    if subprocess.run(["pgrep", "-x", "duckdb"], capture_output=True).returncode == 0:
        sys.exit(
            "a duckdb process is running: wait for it, or stop it, before benchmarking"
        )
    queries = [int(q) for q in a.queries.split(",")]
    texts = {q: text(q, a.sf) for q in queries}
    union = {}
    for q in queries:
        for t, cs in cols(q, a.sf).items():
            union.setdefault(t, set()).update(cs)
    skip = set(filter(None, a.pin_skip.split(",")))
    exclude = set(filter(None, a.pin_exclude.split(",")))
    replicate = set(filter(None, a.pin_replicate.split(",")))
    pins = {
        t: [c for c in SIZES[t] if c in cs and c not in exclude]
        for t, cs in union.items()
        if t not in skip and (not a.pin_only or t in a.pin_only.split(","))
    }
    # Smallest first: a pin past pin_table_memory_limit fails, and the queries then read that
    # table from disk, so let it be the largest.
    pins = dict(
        sorted(pins.items(), key=lambda kv: sum(SIZES[kv[0]][c] for c in kv[1]))
    )

    os.makedirs(a.out, exist_ok=True)
    a.out = os.path.abspath(a.out)
    name = f"suite{a.tag}"
    qout = f"{a.out}/{name}"
    os.makedirs(qout, exist_ok=True)
    os.makedirs(a.spill_dir, exist_ok=True)
    yaml = open(a.config).read().replace("@SPILL_DIR@", a.spill_dir)
    if a.host_pool:
        yaml = re.sub(
            r"(host: \{ capacity_bytes: )[^ }]+", rf"\g<1>{a.host_pool}", yaml
        )
    cfg = f"{qout}/sirius.yaml"
    open(cfg, "w").write(yaml)

    lines = [
        f"create view {t} as select * from read_parquet('{a.data}/{t}/*.parquet');"
        for t in union
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
    for q in queries:
        lines += [texts[q] + ";"] * a.iters
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
    p = subprocess.run(
        [
            f"{REPO}/scripts/exchange_sql_ranks.sh",
            str(a.ranks),
            sqlf,
            str(a.port),
            qout,
        ],
        cwd=a.out,
        env=env,
        capture_output=True,
        text=True,
        timeout=a.timeout + 300,
    )
    wall = time.time() - t0
    spill = sum(
        os.path.getsize(os.path.join(r, f))
        for r, _, fs in os.walk(a.spill_dir)
        for f in fs
    )
    clear(a.spill_dir)

    # Scans served from pins, per query: each "assigned pinned entry" line belongs to the query
    # whose QueryBegin (from any rank) came last before it.
    starts = {norm(texts[q])[:200]: q for q in queries}
    hits, current = {q: 0 for q in queries}, None
    for f in glob.glob(f"{qout}/logs/*.log"):
        for line in open(f, errors="replace"):
            m = re.search(
                r"QueryBegin: instance=\S+ connection=\d+ query=\d+ SQL: (.*)", line
            )
            if m:
                current = starts.get(norm(m.group(1))[:200], current)
            elif "assigned pinned entry" in line and current in hits:
                hits[current] += 1

    runs = parse(open(f"{qout}/exchange_rank_0.log", errors="replace").read())
    pin_runs, runs = runs[: len(pins)], runs[len(pins) :]
    json.dump(
        {
            "tables": {t: cs for t, cs in pins.items()},
            "replicated": sorted(replicate & set(pins)),
            "pin_s": dict(zip(pins, [r[2] for r in pin_runs])),
            "pin_ok": all(r[0] == [["true"]] for r in pin_runs)
            and len(pin_runs) == len(pins),
            "wall_s": round(wall, 1),
            "rc": p.returncode,
        },
        open(f"{a.out}/{name}-pins.json", "w"),
        indent=1,
    )
    with open(f"{a.out}/results.jsonl", "a") as out:
        for i, q in enumerate(queries):
            block = runs[i * a.iters : (i + 1) * a.iters]
            times = [r[2] for r in block]
            ref = None
            for d in (f"q{q:02d}_direct", f"q{q:02d}"):
                f = f"{a.ref_dir}/{d}/exchange_rank_0.log" if a.ref_dir else ""
                if f and os.path.exists(f):
                    ref = parse(open(f, errors="replace").read())
                    break
            failed = len(block) < a.iters or any(
                any("Error" in c for c in r[1]) or not r[1] for r in block
            )
            res = dict(
                query=q,
                name=f"q{q:02d}{a.tag}",
                sf=a.sf,
                tag=a.tag,
                rc=1 if failed else 0,
                times_s=times,
                cold_s=times[0] if times else None,
                warm_s=min(times[1:]) if len(times) > 1 else None,
                rows=len(block[0][0]) if block else 0,
                same_as_first=[check(r[0], block[0][0]) for r in block],
                vs_ref=(
                    [check(r[0], ref[-1][0]) for r in block]
                    if ref
                    else ["unverified"] * len(block)
                ),
                pin_tier=a.pin_tier,
                pin_plans=a.pin_plans,
                pin_hits=hits[q],
                reads_excluded=sorted(
                    c for cs in cols(q, a.sf).values() for c in cs if c in exclude
                ),
                spill_bytes=spill,
                config=os.path.basename(a.config),
                host_pool=a.host_pool,
            )
            out.write(json.dumps(res) + "\n")
            print(json.dumps(res))
    if p.returncode:
        print(p.stdout[-3000:], p.stderr[-3000:], sep="\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
