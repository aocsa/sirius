#!/usr/bin/env python3
"""The columns each TPC-H query reads, and the size of a copy holding only those columns.

Columns are found by matching the schema's column names in the query text (TPC-H column names
are unique). Sizes come from colsizes.json: compressed bytes per column at SF10000 in the CI
generator's format, measured from one generated part of each table times the part count.

usage: columns.py                 sizes and columns of all 22 queries
       columns.py --keep Q        gen_tables.py keep spec for query Q (5 = the subquery variant)
       columns.py --keep-std 5    the same for the queries.py text of Q5
"""
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
# {table: [[column, bytes], ...]}, columns in schema order (a list, so the order survives).
SIZES = {
    t: dict(cs)
    for t, cs in json.load(open(os.path.join(HERE, "colsizes.json"))).items()
}
Q5_SUBQUERY = os.path.join(HERE, "q5_subquery.sql")


def text(q, std=False):
    """Query q's text: queries.py's for SF10000, or q5_subquery.sql for Q5 unless std."""
    if q == 5 and not std:
        body = "".join(l for l in open(Q5_SUBQUERY) if not l.startswith("--"))
        return body.strip().rstrip(";")
    sys.path.insert(0, os.path.join(REPO, "test", "tpch_performance"))
    from queries import queries_for_scale_factor

    return queries_for_scale_factor(10000)[f"q{q}"].strip().rstrip(";")


def cols(q, std=False):
    """{table: [columns]} for the tables query q reads."""
    words = set(re.findall(r"\b[a-z_]+\b", text(q, std).lower()))
    keep = {}
    for table, columns in SIZES.items():
        used = [c for c in columns if c in words]
        if used:
            keep[table] = used
    return keep


def size(keep):
    return sum(SIZES[t][c] for t, cs in keep.items() for c in cs)


def keep_spec(q, std=False):
    """gen_tables.py's spec: nation and region whole (they are tiny), the rest pruned."""
    return {
        t: (None if t in ("nation", "region") else cs) for t, cs in cols(q, std).items()
    }


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] in ("--keep", "--keep-std"):
        json.dump(
            keep_spec(int(sys.argv[2]), sys.argv[1] == "--keep-std"),
            sys.stdout,
            indent=1,
        )
        print()
    else:
        for q in range(1, 23):
            k = cols(q)
            print(
                f"Q{q:<2} {size(k) / 1e9:7.1f} GB  "
                + "; ".join(f"{t}: {','.join(c)}" for t, c in k.items())
            )
