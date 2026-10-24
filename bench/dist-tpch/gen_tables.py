#!/usr/bin/env python3
"""TPC-H tables keeping only the listed columns (generalized from gen_q5_tables.py).

Each part is generated with the CI generator's exact tpchgen-cli flags (row-group sizes,
encodings, uncompressed columns, decimal128/date32/i32 keys), then rewritten row group by row
group with only the kept columns, so files, row groups and column chunks match the full dataset.

usage: gen_tables.py <scale> <out_dir> <keep.json> [jobs] [threads_per_job] [stage_dir]
keep.json: {"table": [columns] or null (whole table), ...}, plus an optional
"_dirs": {"table": dir} to put a table under dir instead of out_dir, and an optional
"_split": {"table": [[dir, n_parts], ...]} to put the table's first n_parts files under the
first dir, the next ones under the second, and so on.
"""

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pyarrow.parquet as pq

# A tpchgen-rs checkout (github.com/sirius-db/tpchgen-rs) with tpchgen-cli built. Its
# scripts/generate_tpch.py holds the CI dataset's settings.
REPO = Path(__file__).resolve().parents[2]
CI = os.environ.get("CI_TPCHGEN", str(REPO / "test_datasets" / "tpchgen-rs"))
spec = importlib.util.spec_from_file_location("gen", f"{CI}/scripts/generate_tpch.py")
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)
CLI = f"{CI}/target/release/tpchgen-cli"

KEEP = {}
DIRS = {}
SPLIT = {}


def table_dir(table, part, out):
    first = 1
    for d, n in SPLIT.get(table, []):
        if part < first + n:
            return Path(d)
        first += n
    return Path(DIRS.get(table, out))


UNCOMPRESSED = set(gen.UNCOMPRESSED_COLUMN_OVERRIDES.split(","))
NO_DICT = set(gen.DEFAULT_DISABLE_DICTIONARY_ENCODING_COLUMNS)


def cli_cmd(table, parts, part, scale, out, threads):
    cmd = [
        CLI,
        "-s",
        str(scale),
        "--tables",
        table,
        "--format=parquet",
        "--output-dir",
        str(out),
        "--parts",
        str(parts),
        "--part",
        str(part),
        "-n",
        str(threads),
        "--parquet-row-group-bytes",
        str(gen.PARQUET_ROW_GROUP_BYTES_DEFAULTS[table]),
        f"--uncompressed-column-overrides={gen.UNCOMPRESSED_COLUMN_OVERRIDES}",
    ]
    cmd += [
        f"--column-encoding={c}={e}" for c, e in gen.DEFAULT_COLUMN_ENCODINGS.items()
    ]
    cmd += [
        f"--disable-dictionary-encoding={','.join(gen.DEFAULT_DISABLE_DICTIONARY_ENCODING_COLUMNS)}",
        "--decimal-column-type",
        "decimal128",
        "--date-column-type",
        "date32",
        "--nationkey-type",
        "i32",
        "--regionkey-type",
        "i32",
        "--parquet-version",
        "v2",
    ]
    return cmd


def prune(src, dst, cols):
    f = pq.ParquetFile(src)
    tmp = dst.with_suffix(".tmp")
    writer = pq.ParquetWriter(
        tmp,
        f.schema_arrow.empty_table().select(cols).schema,
        version="2.6",
        compression={c: ("NONE" if c in UNCOMPRESSED else "SNAPPY") for c in cols},
        use_dictionary=[c for c in cols if c not in NO_DICT],
        column_encoding={
            c: gen.DEFAULT_COLUMN_ENCODINGS[c]
            for c in cols
            if c in NO_DICT and c in gen.DEFAULT_COLUMN_ENCODINGS
        },
        data_page_version="2.0",
        write_statistics=True,
        store_decimal_as_integer=True,
    )
    for i in range(f.num_row_groups):
        t = f.read_row_group(i, columns=cols)
        writer.write_table(t, row_group_size=t.num_rows)
    writer.close()
    tmp.rename(dst)


def one(table, parts, part, scale, out, stage, threads):
    dst = table_dir(table, part, out) / table / f"part.{part - 1}.parquet"
    if dst.exists():
        return 0.0
    t0 = time.time()
    work = stage / f"{table}-{part}"
    shutil.rmtree(work, ignore_errors=True)
    subprocess.run(
        cli_cmd(table, parts, part, scale, work, threads),
        check=True,
        stdout=subprocess.DEVNULL,
    )
    src = work / table / f"{table}.{part}.parquet"
    if KEEP[table] is None:
        shutil.copyfile(src, dst.with_suffix(".tmp"))
        dst.with_suffix(".tmp").rename(dst)
    else:
        prune(src, dst, KEEP[table])
    shutil.rmtree(work)
    return time.time() - t0


def main():
    scale, out = int(sys.argv[1]), Path(sys.argv[2])
    spec = json.load(open(sys.argv[3]))
    DIRS.update(spec.pop("_dirs", {}))
    SPLIT.update(spec.pop("_split", {}))
    KEEP.update(spec)
    jobs = int(sys.argv[4]) if len(sys.argv) > 4 else 24
    threads = int(sys.argv[5]) if len(sys.argv) > 5 else 6
    stage = Path(
        sys.argv[6]
        if len(sys.argv) > 6
        else f"/dev/shm/{os.environ.get('USER', 'sirius')}-tpchgen-sf{scale}"
    )
    stage.mkdir(parents=True, exist_ok=True)
    work = []
    for table in KEEP:
        parts = gen.calculate_partitions(scale, gen.TABLE_MULTIPLIERS[table])
        for p in range(1, parts + 1):
            (table_dir(table, p, out) / table).mkdir(parents=True, exist_ok=True)
        work += [(table, parts, p) for p in range(1, parts + 1)]
    # Largest tables first, so the tail is short.
    order = {"lineitem": 0, "orders": 1, "customer": 2}
    work.sort(key=lambda w: order.get(w[0], 3))
    print(
        f"SF{scale}: {len(work)} parts, {jobs} jobs x {threads} threads -> {out}",
        flush=True,
    )
    t0, done = time.time(), 0
    with ThreadPoolExecutor(max_workers=jobs) as ex:
        futs = {
            ex.submit(one, t, n, p, scale, out, stage, threads): (t, p)
            for t, n, p in work
        }
        for f in as_completed(futs):
            f.result()
            done += 1
            if done % 20 == 0 or done == len(work):
                print(f"{done}/{len(work)} parts, {time.time() - t0:.0f}s", flush=True)
    shutil.rmtree(stage, ignore_errors=True)
    print(f"done in {time.time() - t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
