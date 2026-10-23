"""Parse rank 0's DuckDB output into per-statement results, and compare two answers."""

import csv
import io
import math
import re


def parse(out):
    """Split rank 0's output (`.mode csv`, `.timer on`) into (rows, header, seconds) per statement."""
    runs, block = [], []
    for line in out.splitlines():
        m = re.match(r"Run Time \(s\): real ([0-9.]+)", line)
        if m:
            runs.append((block, float(m.group(1))))
            block = []
        elif (
            line
            and not line.startswith(
                ("WARNING", "[", "estimate:", "duckdb agent mode", '{"exception')
            )
            and "no version information" not in line
        ):
            block.append(line)
    res = []
    for block, secs in runs:
        rows = list(csv.reader(io.StringIO("\n".join(block))))
        res.append((rows[1:] if rows else [], rows[0] if rows else [], secs))
    return res


def close(a, b):
    if a == b:
        return True
    try:
        x, y = float(a), float(b)
    except (TypeError, ValueError):
        return str(a).strip() == str(b).strip()
    return math.isclose(x, y, rel_tol=1e-6, abs_tol=1e-2)


def check(rows, want):
    """ "ok" when rows match want, also when only rows with equal values come out in another order."""
    if len(rows) != len(want):
        return f"WRONG({len(rows)} rows vs {len(want)})"
    if all(
        len(r) == len(w) and all(close(a, b) for a, b in zip(r, w))
        for r, w in zip(rows, want)
    ):
        return "ok"
    norm = lambda r: [
        (
            f"{float(v):.2f}"
            if re.fullmatch(r"-?[0-9.]+(e[-+]?[0-9]+)?", str(v))
            else str(v)
        )
        for v in r
    ]
    if sorted(map(norm, rows)) == sorted(map(norm, want)):
        return "ok"
    return "WRONG(values)"
