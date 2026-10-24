#!/usr/bin/env python3
"""Markdown table of a run_tpch.sh output directory: cold and warm time and answer check per
query and case, totals per case, and what was skipped.

usage: summarize.py OUT_DIR
The last result of each query and case wins, so a rerun replaces an earlier one.
"""
import json
import os
import sys

out = sys.argv[1]
latest = {}
for line in open(os.path.join(out, "results.jsonl")):
    r = json.loads(line)
    latest[(r["query"], r.get("tag", ""))] = r
ORDER = ["_direct", "_page", "_pinned", "_hostpin"]
cases = sorted(
    {tag for _, tag in latest}, key=lambda t: ORDER.index(t) if t in ORDER else 9
)


def verdict(r):
    if "skipped" in r:
        return "skipped"
    if r.get("rc") or not r.get("times_s"):
        return "failed"
    if any(c != "ok" for c in r["same_as_first"]):
        return "differs between iterations"
    v = set(r["vs_ref"])
    if v == {"unverified"}:
        v = "reference" if r.get("tag") == "_direct" else "unverified"
    else:
        v = "ok" if v == {"ok"} else "differs from direct"
    if r.get("pin_tier") and not r.get("pin_hits"):
        v += ", pins unused"
    return v


head = (
    "| Query | "
    + " | ".join(
        f"{c.strip('_')} cold | {c.strip('_')} warm | {c.strip('_')} answer"
        for c in cases
    )
    + " |"
)
print(head)
print("|---" * (1 + 3 * len(cases)) + "|")
totals = {c: [0.0, 0.0, 0] for c in cases}
for q in sorted({q for q, _ in latest}):
    cells = []
    for c in cases:
        r = latest.get((q, c))
        if not r:
            cells += ["", "", ""]
            continue
        v = verdict(r)
        cold, warm = r.get("cold_s"), r.get("warm_s")
        cells += [f"{cold:.2f}" if cold else "", f"{warm:.2f}" if warm else "", v]
        if cold and warm and v.split(",")[0] in ("ok", "unverified", "reference"):
            totals[c][0] += cold
            totals[c][1] += warm
            totals[c][2] += 1
    print(f"| Q{q} | " + " | ".join(cells) + " |")
print(
    "| **Total** | "
    + " | ".join(
        f"**{t[0]:.1f}** | **{t[1]:.1f}** | {t[2]} queries" for t in totals.values()
    )
    + " |"
)
skipped = [r for r in latest.values() if "skipped" in r]
if skipped:
    print()
    for r in sorted(skipped, key=lambda r: (r["query"], r["tag"])):
        print(f"- Q{r['query']} {r['tag'].strip('_')}: {r['skipped']}")
pinned = [r for r in latest.values() if r.get("pin_tier") and "skipped" not in r]
if pinned:
    print()
    print("| Query | Case | Pin time (s, all tables) | Scans served from pins |")
    print("|---|---|---|---|")
    for r in sorted(pinned, key=lambda r: (r["query"], r["tag"])):
        print(
            f"| Q{r['query']} | {r['tag'].strip('_')} | {sum(r.get('pin_s', [])):.1f} | {r.get('pin_hits', 0)} |"
        )
