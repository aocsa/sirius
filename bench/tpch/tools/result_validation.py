"""Typed TPC-H result validation, allowing only legal SQL tie reordering."""

import csv
import gzip
import hashlib
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

ORDER_KEYS = {
    1: [0, 1],
    2: [0, 2, 1, 3],
    3: [1, 2],
    4: [0],
    5: [1],
    6: [],
    7: [0, 1, 2],
    8: [0],
    9: [0, 1],
    10: [2],
    11: [1],
    12: [0],
    13: [1, 0],
    14: [],
    15: [0],
    16: [3, 0, 1, 2],
    17: [],
    18: [4, 3],
    19: [],
    20: [0],
    21: [1, 0],
    22: [0],
}


def load(directory):
    with (directory / "schema.csv").open(newline="") as f:
        types = [x["column_type"].upper() for x in csv.DictReader(f)]
    # Large references are stored gzipped (result.csv.gz) to stay under the repo's file-size limit.
    path = directory / "result.csv"
    with (
        open(path, newline="")
        if path.exists()
        else gzip.open(directory / "result.csv.gz", "rt", newline="")
    ) as f:
        rows = list(csv.reader(f))
        headers = rows.pop(0)

    def cell(value, dtype):
        if value == "":
            return None
        if dtype in ["DOUBLE", "FLOAT", "REAL"]:
            return float(value)
        if dtype.startswith(
            (
                "DECIMAL",
                "HUGEINT",
                "UBIGINT",
                "BIGINT",
                "INTEGER",
                "SMALLINT",
                "TINYINT",
                "UINTEGER",
                "USMALLINT",
                "UTINYINT",
            )
        ):
            return Decimal(value)
        return value

    return headers, types, [[cell(v, t) for v, t in zip(row, types)] for row in rows]


def equal_cell(x, y):
    if isinstance(x, float) and isinstance(y, float):
        return abs(x - y) <= max(1e-6, max(abs(x), abs(y)) * 1e-9)
    return x == y


def equal_rows(l, r):
    return len(l) == len(r) and all(equal_cell(x, y) for x, y in zip(l, r))


def row_digest(directory):
    """Order-independent exact digest, for large references with no floating-point columns."""
    headers, types, rows = load(directory)
    if any(t in ["DOUBLE", "FLOAT", "REAL"] for t in types):
        raise ValueError("digest references need exact column types")
    key = lambda row: tuple((type(x).__name__, str(x)) for x in row)
    text = "\n".join(
        "|".join("" if x is None else str(x) for x in row)
        for row in sorted(rows, key=key)
    )
    return dict(
        headers=headers,
        types=types,
        rows=len(rows),
        sha256=hashlib.sha256(text.encode()).hexdigest(),
    )


def validate(left, right, q):
    if not (right / "result.csv").exists() and not (right / "result.csv.gz").exists():
        want = json.loads((right / "result-digest.json").read_text())
        got = row_digest(left)
        return dict(
            agree=got == want,
            digest_match=got == want,
            rows=got["rows"],
            detail=(
                None
                if got == want
                else f"digest differs: rows {got['rows']} vs {want['rows']}, sha256 {got['sha256'][:12]} vs {want['sha256'][:12]}"
            ),
        )
    lh, lt, l = load(left)
    rh, rt, r = load(right)
    if lh != rh or lt != rt:
        return dict(agree=False, detail="schema differs")
    if len(l) != len(r):
        return dict(agree=False, detail="row count differs")
    ordered = all(equal_rows(x, y) for x, y in zip(l, r))
    if ordered:
        return dict(agree=True, ordered_match=True, rows=len(l))
    key = lambda row: tuple((type(x).__name__, str(x)) for x in row)
    same_set = all(
        equal_rows(x, y) for x, y in zip(sorted(l, key=key), sorted(r, key=key))
    )
    if not same_set:
        return dict(agree=False, detail="result values differ", rows=len(l))
    keys = ORDER_KEYS[q]
    legal = all(
        equal_rows([x[i] for i in keys], [y[i] for i in keys]) for x, y in zip(l, r)
    )
    return dict(
        agree=legal,
        ordered_match=False,
        legal_sql_tie_reordering=legal,
        rows=len(l),
        detail=None if legal else "ORDER BY keys changed order",
    )


def validate_with_expanded_q3(left, right, q, expanded):
    """Validate Q3 LIMIT ties only when an expanded result proves boundary-row values."""
    result = validate(left, right, q)
    if result["agree"] or q != 3:
        return result
    lh, lt, l = load(left)
    rh, rt, r = load(right)
    eh, et, e = load(expanded)
    if not (
        lh == rh == eh and lt == rt == et and len(l) == len(r) == 10 and len(e) > 10
    ):
        return result
    order = lambda row: tuple(row[i] for i in ORDER_KEYS[3])
    boundary = order(e[9])
    if order(e[-1]) == boundary:
        return result  # reference must extend beyond the full cutoff tie
    if any(order(a) != order(b) for a, b in zip(l, e[:10])):
        return result
    if any(order(a) != order(b) for a, b in zip(r, e[:10])):
        return result
    expected = Counter(tuple(row) for row in e)
    for rows in [l, r]:
        if any(
            count > expected[row]
            for row, count in Counter(tuple(x) for x in rows).items()
        ):
            return result
    prefix = lambda rows: Counter(tuple(row) for row in rows if order(row) != boundary)
    if not (prefix(l) == prefix(r) == prefix(e[:10])):
        return result
    return dict(
        agree=True,
        ordered_match=False,
        legal_sql_limit_tie=True,
        rows=10,
        expanded_reference_rows=len(e),
        detail="Boundary row values verified against expanded GPU result; ORDER BY keys and all non-boundary rows agree. No independent CPU oracle implied.",
    )


if __name__ == "__main__":
    import sys

    for directory in map(Path, sys.argv[1:]):
        print(directory, json.dumps(row_digest(directory)))
