#!/usr/bin/env python3
"""The columns each TPC-H query reads, and the size of a copy holding only those columns.

Columns are found by matching the schema's column names in the query text (TPC-H column names
are unique). Sizes come from colsizes.json: compressed bytes per column at SF10000 in the CI
generator's format (one generated part of each table times the part count), scaled linearly to
other scale factors.

usage: columns.py [--sf SF]                          every query's columns and copy size
       columns.py --keep Q [--sf SF] [--sql FILE]    gen_tables.py keep spec for query Q
       columns.py --bytes Q [--sf SF] [--sql FILE]   estimated copy size in bytes
--sql FILE uses that file's query text instead of the repo's TPC-H query Q.
"""
import argparse
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


def text(q, sf=10000, sql=None):
    """Query q's text at scale factor sf (test/tpch_performance/queries.py), or sql's."""
    if sql:
        body = "".join(l for l in open(sql) if not l.lstrip().startswith("--"))
        return body.strip().rstrip(";")
    sys.path.insert(0, os.path.join(REPO, "test", "tpch_performance"))
    from queries import queries_for_scale_factor

    return queries_for_scale_factor(sf)[f"q{q}"].strip().rstrip(";")


def cols(q, sf=10000, sql=None):
    """{table: [columns]} for the tables query q reads."""
    words = set(re.findall(r"\b[a-z_]+\b", text(q, sf, sql).lower()))
    keep = {}
    for table, columns in SIZES.items():
        used = [c for c in columns if c in words]
        if used:
            keep[table] = used
    return keep


def size(keep, sf=10000):
    """Estimated bytes of a copy of keep's columns. nation and region are negligible."""
    return sum(SIZES[t][c] for t, cs in keep.items() for c in cs) * sf / 10000


def keep_spec(q, sf=10000, sql=None):
    """gen_tables.py's spec: nation and region whole (they are tiny), the rest pruned."""
    return {
        t: (None if t in ("nation", "region") else cs)
        for t, cs in cols(q, sf, sql).items()
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, default=10000)
    ap.add_argument("--sql")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--keep", type=int, metavar="Q")
    mode.add_argument("--bytes", type=int, metavar="Q")
    a = ap.parse_args()
    if a.keep:
        json.dump(keep_spec(a.keep, a.sf, a.sql), sys.stdout, indent=1)
        print()
    elif a.bytes:
        print(int(size(cols(a.bytes, a.sf, a.sql), a.sf)))
    else:
        for q in range(1, 23):
            k = cols(q, a.sf)
            print(
                f"Q{q:<2} {size(k, a.sf) / 1e9:8.1f} GB  "
                + "; ".join(f"{t}: {','.join(c)}" for t, c in k.items())
            )
