#!/usr/bin/env python3
"""One per-query table for standalone and distributed TPC-H runs, validated the same way.

usage: report.py RUN[=LABEL] [RUN[=LABEL] ...] [--mode cold|hot] [--validate] [--jsonl OUT]
                 [--compare]

RUN is any of:
  - a run_suite.py output directory (executions.jsonl): any schedule, any number of ranks;
  - a run_query.py results.jsonl (fresh ranks per query): --mode cold reports run 1, --mode hot
    reports runs 2+ with run 1 as the warm-up;
  - a tools/benchmark.py output directory (record.json files, integ/sf3k-joost-2201-2204).
Per run and mode it prints how many queries finished and validated, and two totals: the sum of
each query's first timed run and of its best timed run. --compare adds a per-query table of the
first two runs' best times. --validate checks every measured answer of a run_suite.py run against
reference/sf<SF> with the typed validator (exact decimals, legal ORDER BY ties, expanded
LIMIT-boundary references for Q3 and Q18, digests for Q11 and Q20).
"""
import argparse
import glob
import json
import os
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "tools"))
from result_validation import validate, validate_with_expanded_q3  # noqa: E402
from sf10k_followup_validation import validate_q18_boundary  # noqa: E402


def check_answer(answer_csv, sf, q):
    """Typed check of one rank-0 answer against reference/sf<SF>/q<N>: True, False or None."""
    ref = HERE / f"reference/sf{sf}/q{q}"
    if not ref.exists():
        return None
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        shutil.copyfile(answer_csv, d / "result.csv")
        shutil.copyfile(ref / "schema.csv", d / "schema.csv")
        r = validate(d, ref, q)
        expanded = HERE / f"reference/expanded/sf{sf}/q{q}"
        if not r["agree"] and q == 3 and expanded.exists():
            r = validate_with_expanded_q3(d, ref, q, expanded)
        if not r["agree"] and q == 18 and expanded.exists():
            r = validate_q18_boundary(d, ref, expanded)
        return bool(r["agree"])


def suite_rows(run, label, do_validate):
    inv = (
        json.load(open(f"{run}/invocation.json"))
        if os.path.exists(f"{run}/invocation.json")
        else {}
    )
    rows = {}
    for line in open(f"{run}/executions.jsonl"):
        e = json.loads(line)
        key = (e["schedule"], e["query"])
        r = rows.setdefault(
            key,
            dict(
                harness="suite",
                label=label,
                sf=e["sf"],
                gpus=e["ranks"],
                mode=e["schedule"],
                query=e["query"],
                warmup_s=[],
                timed_s=[],
                status="ok",
                answer="unvalidated",
                checks=[],
            ),
        )
        if e["rc"]:
            r["status"] = "failed"
            continue
        (r["timed_s"] if e["phase"] == "measured" else r["warmup_s"]).append(
            e["seconds"]
        )
        if e["phase"] == "measured":
            r["checks"].append(e.get("vs_ref"))
            if do_validate:
                ans = glob.glob(
                    f"{run}/suite_{e['schedule']}*/answers/q{e['query']}_r{e['repeat']}.csv"
                )
                r["checks"].append(
                    {True: "ok", False: "differs", None: "unverified"}[
                        check_answer(ans[0], e["sf"], e["query"]) if ans else None
                    ]
                )
    for r in rows.values():
        r["answer"] = verdict(r.pop("checks"))
        r["build"] = inv.get("commit")
    return rows.values()


def verdict(checks):
    checks = [c for c in checks if c not in (None, "unverified")]
    if not checks:
        return "unvalidated"
    return "ok" if all(c == "ok" for c in checks) else "differs"


def run_query_rows(path, label, mode):
    latest = {}
    for line in open(path):
        r = json.loads(line)
        if "skipped" not in r:
            latest[r["query"]] = r
    for q, r in sorted(latest.items()):
        t, v, cold = r.get("times_s") or [], r.get("vs_ref", []), mode == "cold"
        yield dict(
            harness="run_query",
            label=label,
            sf=r["sf"],
            gpus=None,
            mode=mode,
            query=q,
            warmup_s=[] if cold else t[:1],
            timed_s=t[:1] if cold else t[1:],
            status="ok" if r.get("rc") == 0 and t else "failed",
            answer=verdict(v[:1] if cold else v[1:]),
        )


def benchmark_rows(run, label):
    rows = {}
    for f in glob.glob(f"{run}/**/record.json", recursive=True):
        e = json.load(open(f))
        r = rows.setdefault(
            (e["mode"], e["query"]),
            dict(
                harness="benchmark",
                label=label,
                sf=e["scale"],
                gpus=e["num_gpus"],
                mode=e["mode"],
                query=e["query"],
                warmup_s=[],
                timed_s=[],
                status="ok",
            ),
        )
        (r["warmup_s"] if e["phase"] == "warmup" else r["timed_s"]).append(
            (e["repeat"], e["query_s"])
        )
        if e["status"] != "ok" or e.get("exit_code") != 0:
            r["status"] = "failed"
    accepted = defaultdict(list)
    for v in glob.glob(f"{run}/**/validation.json", recursive=True):
        for c in json.load(open(v))["checks"]:
            accepted[c["query"]].append(c["accepted"])
    for r in rows.values():
        for k in ("warmup_s", "timed_s"):
            r[k] = [s for _, s in sorted(r[k])]
        got = accepted.get(r["query"])
        r["answer"] = "unvalidated" if not got else ("ok" if all(got) else "differs")
    return rows.values()


def load(spec, mode, do_validate):
    run, _, label = spec.partition("=")
    label = label or os.path.basename(run.rstrip("/"))
    if os.path.isdir(run) and os.path.exists(f"{run}/executions.jsonl"):
        return list(suite_rows(run, label, do_validate))
    if os.path.isfile(run):
        if not mode:
            sys.exit(
                f"{run}: a run_query.py results.jsonl needs --mode cold or --mode hot"
            )
        return list(run_query_rows(run, label, mode))
    if glob.glob(f"{run}/**/record.json", recursive=True):
        return list(benchmark_rows(run, label))
    sys.exit(f"{run}: not a run_suite.py, run_query.py or benchmark.py output")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("runs", nargs="+")
    ap.add_argument(
        "--mode", choices=["cold", "hot"], help="for run_query.py results.jsonl"
    )
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--jsonl", help="also write the per-query rows here")
    ap.add_argument("--compare", action="store_true")
    a = ap.parse_args()
    rows = [r for spec in a.runs for r in load(spec, a.mode, a.validate)]
    if a.jsonl:
        with open(a.jsonl, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
    groups = defaultdict(dict)
    for r in rows:
        groups[(r["label"], r["mode"])][r["query"]] = r
    print(
        "| Run | Mode | Finished | Answers ok | Total, first timed run (s) | Total, best timed run (s) |"
    )
    print("|---|---|---:|---:|---:|---:|")
    for (label, mode), qs in groups.items():
        done = [r for r in qs.values() if r["status"] == "ok" and r["timed_s"]]
        ok = sum(r["answer"] == "ok" for r in done)
        print(
            f"| {label} | {mode} | {len(done)}/{len(qs)} | {ok}/{len(qs)} | "
            f"{sum(r['timed_s'][0] for r in done):.1f} | {sum(min(r['timed_s']) for r in done):.1f} |"
        )
    if a.compare and len(groups) >= 2:
        (ka, ra), (kb, rb) = list(groups.items())[:2]
        print(
            f"\n| Query | {ka[0]} {ka[1]} best (s) | {kb[0]} {kb[1]} best (s) | ratio |"
        )
        print("|---|---:|---:|---:|")
        for q in sorted(set(ra) | set(rb)):
            x, y = (
                min(g[q]["timed_s"]) if q in g and g[q]["timed_s"] else None
                for g in (ra, rb)
            )
            ratio = f"{x / y:.2f}x" if x and y else ""
            print(
                f"| Q{q} | {x if x is not None else ''} | {y if y is not None else ''} | {ratio} |"
            )


if __name__ == "__main__":
    main()
