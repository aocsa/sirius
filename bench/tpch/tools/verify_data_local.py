#!/usr/bin/env python3
"""Compare a locally generated TPC-H directory with a campaign manifest: size, rows, row groups, S3 ETag."""
import argparse, gzip, hashlib, json, math
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pyarrow.parquet as pq

p = argparse.ArgumentParser()
p.add_argument("manifest", type=Path)
p.add_argument("data", type=Path)
p.add_argument("--tables", default="")
p.add_argument("--jobs", type=int, default=32)
a = p.parse_args()
text = (
    gzip.decompress(a.manifest.read_bytes()).decode()
    if a.manifest.suffix == ".gz"
    else a.manifest.read_text()
)
records = [json.loads(l) for l in text.splitlines() if l.strip()]
if a.tables:
    records = [r for r in records if r["table"] in a.tables.split(",")]


def etag(path, size, want):
    want = want.strip('"')
    if "-" not in want:
        return hashlib.md5(path.read_bytes()).hexdigest()
    n = int(want.split("-")[1])
    for mib in (8, 16, 32, 64, 5, 10, 100):
        chunk = mib * 2**20
        if math.ceil(size / chunk) == n:
            break
    else:
        return f"no-chunk-size-for-{n}-parts"
    digests = []
    with path.open("rb") as f:
        while block := f.read(chunk):
            digests.append(hashlib.md5(block).digest())
    return f'{hashlib.md5(b"".join(digests)).hexdigest()}-{len(digests)}'


def check(r):
    path = a.data / r["table"] / f"part.{r['part'] - 1}.parquet"
    if not path.is_file():
        return r["key"], ["missing"]
    size = path.stat().st_size
    meta = pq.ParquetFile(path).metadata
    problems = []
    if size != r["bytes"]:
        problems.append(f"bytes {size}!={r['bytes']}")
    if meta.num_rows != r["rows"]:
        problems.append(f"rows {meta.num_rows}!={r['rows']}")
    if meta.num_row_groups != r["row_groups"]:
        problems.append(f"row_groups {meta.num_row_groups}!={r['row_groups']}")
    if not problems:
        got = etag(path, size, r["etag"])
        if got != r["etag"].strip('"'):
            problems.append(f"etag {got}!={r['etag']}")
    return r["key"], problems


with ThreadPoolExecutor(a.jobs) as pool:
    results = list(pool.map(check, records))
bad = [(k, v) for k, v in results if v]
for k, v in bad[:20]:
    print("FAIL", k, "; ".join(v))
extra = (
    sorted(
        str(f.relative_to(a.data))
        for f in a.data.rglob("*.parquet")
        if not any(
            f == a.data / r["table"] / f"part.{r['part'] - 1}.parquet" for r in records
        )
    )
    if not a.tables
    else []
)
print(
    json.dumps(
        dict(
            checked=len(results),
            failed=len(bad),
            extra_files=extra[:10],
            bytes=sum(r["bytes"] for r in records),
            byte_identical=not bad,
        )
    )
)
raise SystemExit(1 if bad or extra else 0)
