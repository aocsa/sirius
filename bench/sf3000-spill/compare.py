#!/usr/bin/env python3
"""Compare two run_tpch_parquet.sh result dirs (spill compression OFF vs ON).

Per query: cold (iter_1) and warm (iter_2) seconds for each arm, seconds saved by ON, and
whether result.txt is identical between arms. Also totals spill volume from the Sirius log.

  python3 bench/sf3000-spill/compare.py <off_dir> <on_dir>
"""
import csv
import glob
import os
import re
import sys

SPILL_RE = re.compile(r"to_host: \d+/(\d+) batches/bytes, to_disk: \d+/(\d+)")


def timings(arm_dir, q):
    path = os.path.join(arm_dir, f"q{q}", "timings.csv")
    out = {}
    if os.path.exists(path):
        with open(path) as f:
            for row in csv.reader(f):
                if len(row) == 2 and row[0].startswith("iter_"):
                    out[int(row[0][5:])] = float(row[1])
    return out.get(1), out.get(2)


def result(arm_dir, q):
    path = os.path.join(arm_dir, f"q{q}", "result.txt")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        # simpatico prints encode-OOM fallbacks to the same stream the runner captures.
        return [line for line in f if not line.startswith(("estimate:", "simpatico::"))]


def spill(arm_dir):
    host = disk = 0
    for log in glob.glob(os.path.join(arm_dir, "sirius_*.log")):
        with open(log, errors="replace") as f:
            for line in f:
                m = SPILL_RE.search(line)
                if m:
                    host += int(m.group(1))
                    disk += int(m.group(2))
    return host / 1e9, disk / 1e9


def fmt(x):
    return f"{x:8.1f}" if x is not None else "     n/a"


def main(off_dir, on_dir):
    print(f"{'q':>4} {'off cold':>8} {'on cold':>8} {'saved':>8} | "
          f"{'off warm':>8} {'on warm':>8} {'saved':>8} | result")
    tot = [0.0, 0.0, 0.0, 0.0]
    rows = []
    for q in range(1, 23):
        oc, ow = timings(off_dir, q)
        nc, nw = timings(on_dir, q)
        ro, rn = result(off_dir, q), result(on_dir, q)
        same = "missing" if ro is None or rn is None else ("same" if ro == rn else "DIFFERS")
        sc = oc - nc if oc is not None and nc is not None else None
        sw = ow - nw if ow is not None and nw is not None else None
        for i, v in enumerate((oc, nc, ow, nw)):
            tot[i] += v or 0.0
        rows.append((q, sc))
        print(f"{q:>4} {fmt(oc)} {fmt(nc)} {fmt(sc)} | {fmt(ow)} {fmt(nw)} {fmt(sw)} | {same}")
    print(f"{'sum':>4} {fmt(tot[0])} {fmt(tot[1])} {fmt(tot[0] - tot[1])} | "
          f"{fmt(tot[2])} {fmt(tot[3])} {fmt(tot[2] - tot[3])} |")
    if tot[1] and tot[3]:
        print(f"speedup: cold {tot[0] / tot[1]:.2f}x ({(tot[1] / tot[0] - 1) * 100:+.1f}%), "
              f"warm {tot[2] / tot[3]:.2f}x ({(tot[3] / tot[2] - 1) * 100:+.1f}%)")
    ranked = sorted((r for r in rows if r[1] is not None), key=lambda r: -r[1])
    print("cold seconds saved, ranked: " + ", ".join(f"q{q} {s:+.0f}" for q, s in ranked))
    for name, d in (("off", off_dir), ("on", on_dir)):
        h, k = spill(d)
        print(f"spill {name:>3}: to_host {h:7.1f} GB, to_disk {k:7.1f} GB")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
