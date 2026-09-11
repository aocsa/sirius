#!/usr/bin/env python3
# Copyright 2026, Sirius Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the LICENSE file at the repo root for the full text.
"""Generate synthetic parquet and run pinned 1-GPU vs 2-GPU query timings."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

QUERIES = (
    "scan_filter",
    "groupby_highcard",
    "join_uniform",
    "join_skew",
)

SQL_MARKERS = {
    "scan_filter": "FROM t WHERE k % 10 = 0",
    "groupby_highcard": "FROM t GROUP BY k",
    "join_skew": "FROM probe_skew JOIN build",
    "join_uniform": "FROM probe JOIN build",
}

CLONE_RE = re.compile(
    r"\[clone_stats\] bytes_0to1=(\d+) bytes_1to0=(\d+) bytes_other=(\d+) "
    r"clone_ms=([0-9.]+) copies=(\d+)"
)
GPU_ALLOC_RE = re.compile(r"\[gpu_alloc\] query allocated (\d+) GPU")
PIN_HIT_RE = re.compile(r"assigned pinned entry")
SQL_RE = re.compile(r"QueryBegin:.*SQL:\s*(.*)$")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def find_duckdb(root: Path) -> Path:
    override = os.environ.get("SIRIUS_DUCKDB")
    if override:
        p = Path(override)
        if p.is_file() and os.access(p, os.X_OK):
            return p
        raise SystemExit(f"SIRIUS_DUCKDB is not an executable: {override}")
    candidates = [
        root / "build/release/duckdb",
        root / "build/relwithdebinfo/duckdb",
        root / "build/clang-release/duckdb",
        root / "build/clang-relwithdebinfo/duckdb",
    ]
    for c in candidates:
        if c.is_file() and os.access(c, os.X_OK):
            return c
    raise SystemExit(
        "DuckDB binary not found. Build with `pixi run make` "
        f"(expected {root / 'build/release/duckdb'})."
    )


def run_duckdb(
    duckdb: Path,
    sql: str,
    *,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.run(
        [str(duckdb), "-unsigned", "-c", sql],
        check=True,
        cwd=cwd,
        timeout=timeout,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=merged,
    )


def table_ready(directory: Path, num_files: int) -> bool:
    parts = sorted(directory.glob("part_*.parquet"))
    return directory.is_dir() and len(parts) == num_files and all(p.stat().st_size > 0 for p in parts)


def manifest_path(data_dir: Path) -> Path:
    return data_dir / "manifest.json"


def write_manifest(data_dir: Path, rows: int, join_rows: int, num_files: int) -> None:
    manifest_path(data_dir).write_text(
        json.dumps({"rows": rows, "join_rows": join_rows, "files": num_files}, indent=2) + "\n"
    )


def manifest_matches(data_dir: Path, rows: int, join_rows: int, num_files: int) -> bool:
    path = manifest_path(data_dir)
    if not path.is_file():
        return False
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return False
    return (
        data.get("rows") == rows
        and data.get("join_rows") == join_rows
        and data.get("files") == num_files
    )


def generate_table(
    duckdb: Path,
    directory: Path,
    num_files: int,
    rows: int,
    select_sql: str,
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for old in directory.glob("part_*.parquet"):
        old.unlink()
    rows_per_file = rows // num_files
    remainder = rows % num_files
    env = os.environ.copy()
    env["SIRIUS_DISABLE"] = "1"
    env.pop("SIRIUS_CONFIG_FILE", None)
    offset = 0
    for i in range(num_files):
        n = rows_per_file + (1 if i < remainder else 0)
        out = directory / f"part_{i:04d}.parquet"
        select = select_sql.replace("__OFFSET__", str(offset)).replace("__N__", str(n))
        sql = (
            f"COPY (SELECT {select} FROM range({n}) AS t(r)) "
            f"TO '{out}' (FORMAT PARQUET);"
        )
        print(f"  write {out.name} rows={n} offset={offset}")
        run_duckdb(duckdb, sql, env=env)
        offset += n
    print(f"  wrote {directory}")


def generate_parquet(duckdb: Path, data_dir: Path, rows: int, join_rows: int, num_files: int) -> None:
    print(f"Generating parquet under {data_dir} (rows={rows}, join_rows={join_rows}, files={num_files})")
    tables_ok = all(
        table_ready(data_dir / name, num_files)
        for name in ("t", "build", "probe", "probe_skew")
    )
    if tables_ok and manifest_matches(data_dir, rows, join_rows, num_files):
        print("  reuse existing parquet (manifest matches)")
        return
    t_sel = "r + __OFFSET__ AS k, (r + __OFFSET__) * 3 AS v"
    uniform_sel = "r + __OFFSET__ AS k, (r + __OFFSET__) * 7 AS v"
    # 90% of rows hash to key 0; the rest are unique positive keys.
    skew_sel = (
        "CASE WHEN (r + __OFFSET__) % 10 = 0 THEN (r + __OFFSET__) ELSE 0 END AS k, "
        "(r + __OFFSET__) AS v"
    )
    generate_table(duckdb, data_dir / "t", num_files, rows, t_sel)
    generate_table(duckdb, data_dir / "build", num_files, join_rows, uniform_sel)
    generate_table(duckdb, data_dir / "probe", num_files, join_rows, uniform_sel)
    generate_table(duckdb, data_dir / "probe_skew", num_files, join_rows, skew_sel)
    write_manifest(data_dir, rows, join_rows, num_files)


def glob_for(data_dir: Path, name: str) -> str:
    return str(data_dir / name / "*.parquet")


def view_sql(data_dir: Path, name: str) -> str:
    g = glob_for(data_dir, name)
    return f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{g}');"


def pin_sql(data_dir: Path, name: str) -> str:
    g = glob_for(data_dir, name)
    return f"CALL pin_table('{g}', tier='gpu', name='{name}', cols=['k', 'v']);"


QUERY_SQL = {
    "scan_filter": "SELECT COUNT(*), SUM(v) FROM t WHERE k % 10 = 0",
    "groupby_highcard": "SELECT COUNT(*) FROM (SELECT k, SUM(v) AS s FROM t GROUP BY k)",
    "join_uniform": "SELECT COUNT(*), SUM(probe.v) FROM probe JOIN build ON probe.k = build.k",
    "join_skew": (
        "SELECT COUNT(*), SUM(probe_skew.v) FROM probe_skew JOIN build ON probe_skew.k = build.k"
    ),
}


def write_session_sql(
    path: Path,
    data_dir: Path,
    timings_csv: Path,
    results_dir: Path,
    warmup: int,
    reps: int,
    smoke: bool,
) -> None:
    lines: list[str] = [
        "SET gpu_execution = true;",
        view_sql(data_dir, "t"),
        view_sql(data_dir, "build"),
        view_sql(data_dir, "probe"),
        view_sql(data_dir, "probe_skew"),
        pin_sql(data_dir, "t"),
        pin_sql(data_dir, "build"),
        pin_sql(data_dir, "probe"),
        pin_sql(data_dir, "probe_skew"),
        "CREATE TEMP TABLE _timings (query VARCHAR, phase VARCHAR, iter INTEGER, ts_ms BIGINT);",
    ]

    def mark(query: str, phase: str, it: int) -> None:
        lines.append("SET gpu_execution = false;")
        lines.append(
            f"INSERT INTO _timings VALUES ('{query}', '{phase}', {it}, epoch_ms(now()));"
        )
        lines.append("SET gpu_execution = true;")

    def run_query(query: str, phase: str, it: int) -> None:
        if query in ("join_uniform", "join_skew"):
            lines.append("SET max_broadcast_join_size = 1;")
        mark(query, f"{phase}_start", it)
        lines.append(f"CALL sirius_set_query_label('{query}_{phase}_{it}');")
        sql = QUERY_SQL[query]
        # Bare SELECT (not COPY) so the statement goes through Sirius GPU
        # execution. COPY (... ) TO file is a COPY statement and stays on CPU.
        if smoke:
            out = results_dir / f"{query}_{phase}_{it}.csv"
            lines.append(".mode csv")
            lines.append(".headers on")
            lines.append(f".once {out}")
            lines.append(sql + ";")
            lines.append(".mode duckbox")
        else:
            lines.append(sql + ";")
        mark(query, f"{phase}_end", it)

    for q in QUERIES:
        for w in range(warmup):
            run_query(q, "warmup", w)
        for it in range(reps):
            run_query(q, "timed", it)

    lines.append("SET gpu_execution = false;")
    lines.append("CALL unpin_table('t');")
    lines.append("CALL unpin_table('build');")
    lines.append("CALL unpin_table('probe');")
    lines.append("CALL unpin_table('probe_skew');")
    lines.append(f"COPY _timings TO '{timings_csv}' (HEADER, DELIMITER ',');")
    path.write_text("\n".join(lines) + "\n")


def classify_sql(sql: str) -> str | None:
    compact = " ".join(sql.split())
    for name, marker in SQL_MARKERS.items():
        if marker in compact:
            return name
    return None


def parse_log(log_text: str) -> list[dict]:
    events: list[dict] = []
    current: dict | None = None
    pin_hit_session = False
    for line in log_text.splitlines():
        if PIN_HIT_RE.search(line):
            pin_hit_session = True
            if current is not None:
                current["pin_hit"] = True
        m_sql = SQL_RE.search(line)
        if m_sql:
            qname = classify_sql(m_sql.group(1))
            if qname is None:
                continue
            if current is not None:
                events.append(current)
            current = {
                "query": qname,
                "sql": m_sql.group(1).strip(),
                "admitted_gpus": "",
                "pin_hit": pin_hit_session,
                "bytes_0to1": 0,
                "bytes_1to0": 0,
                "bytes_other": 0,
                "clone_ms": 0.0,
                "copies": 0,
                "has_clone": False,
            }
            continue
        m_gpu = GPU_ALLOC_RE.search(line)
        if m_gpu and current is not None:
            current["admitted_gpus"] = m_gpu.group(1)
        m_clone = CLONE_RE.search(line)
        if m_clone and current is not None:
            current["bytes_0to1"] = int(m_clone.group(1))
            current["bytes_1to0"] = int(m_clone.group(2))
            current["bytes_other"] = int(m_clone.group(3))
            current["clone_ms"] = float(m_clone.group(4))
            current["copies"] = int(m_clone.group(5))
            current["has_clone"] = True
            events.append(current)
            current = None
    if current is not None:
        events.append(current)
    return [e for e in events if e.get("query") in QUERIES]


def load_timings(path: Path) -> dict[tuple[str, str, int], float]:
    starts: dict[tuple[str, str, int], int] = {}
    walls: dict[tuple[str, str, int], float] = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            phase = row["phase"]
            base = phase.removesuffix("_start").removesuffix("_end")
            key = (row["query"], base, int(row["iter"]))
            ts_ms = int(row["ts_ms"])
            if phase.endswith("_start"):
                starts[key] = ts_ms
            elif phase.endswith("_end") and key in starts:
                walls[key] = float(ts_ms - starts[key])
    return walls


def collect_log_text(log_dir: Path) -> str:
    chunks: list[str] = []
    if not log_dir.is_dir():
        return ""
    for p in sorted(log_dir.rglob("*")):
        if p.is_file() and (p.suffix in {".log", ".txt"} or "sirius" in p.name):
            try:
                chunks.append(p.read_text(errors="replace"))
            except OSError:
                continue
    return "\n".join(chunks)


def median(vals: list[float]) -> float | None:
    if not vals:
        return None
    return float(statistics.median(vals))


def fmt(v: float | None, digits: int = 3) -> str:
    if v is None:
        return "n/a"
    return f"{v:.{digits}f}"


def print_summary(csv_path: Path) -> None:
    rows: list[dict[str, str]] = []
    with csv_path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    print()
    print("======== 1-GPU vs 2-GPU (median timed iters) ========")
    print(f"CSV: {csv_path}")
    print()
    header = (
        f"{'query':>18}  {'t1_ms':>10}  {'t2_ms':>10}  {'speedup':>8}  "
        f"{'remote_GiB':>10}  {'clone_ms':>9}  {'xchg_GB/s':>10}  {'imbalance':>9}"
    )
    print(header)
    by_q: dict[str, dict[int, list[dict[str, str]]]] = {q: {1: [], 2: []} for q in QUERIES}
    for r in rows:
        if r.get("phase") != "timed":
            continue
        q = r["query"]
        g = int(r["num_gpus"])
        if q in by_q and g in by_q[q]:
            by_q[q][g].append(r)
    for q in QUERIES:
        g1 = by_q[q][1]
        g2 = by_q[q][2]
        t1 = median([float(r["wall_ms"]) for r in g1])
        t2 = median([float(r["wall_ms"]) for r in g2])
        speedup = (t1 / t2) if t1 and t2 and t2 > 0 else None
        remote = median(
            [
                (int(r["remote_bytes_0to1"]) + int(r["remote_bytes_1to0"]) + int(r["bytes_other"]))
                / (1024**3)
                for r in g2
            ]
        )
        clone = median([float(r["clone_ms"]) for r in g2])
        xchg = None
        if g2:
            rates = []
            for r in g2:
                b = int(r["remote_bytes_0to1"]) + int(r["remote_bytes_1to0"]) + int(r["bytes_other"])
                ms = float(r["clone_ms"])
                if ms > 0 and b > 0:
                    rates.append((b / 1e9) / (ms / 1e3))
            xchg = median(rates) if rates else None
        imb = None
        imb_label = None
        if g2:
            imbs = []
            one_way = False
            for r in g2:
                a, b = int(r["remote_bytes_0to1"]), int(r["remote_bytes_1to0"])
                lo, hi = min(a, b), max(a, b)
                if lo == 0 and hi > 0:
                    one_way = True
                elif lo > 0:
                    imbs.append(hi / lo)
            if imbs:
                imb = median(imbs)
            elif one_way:
                imb_label = "one-way"
        print(
            f"{q:>18}  {fmt(t1, 1):>10}  {fmt(t2, 1):>10}  {fmt(speedup):>8}  "
            f"{fmt(remote):>10}  {fmt(clone, 2):>9}  {fmt(xchg):>10}  "
            f"{(imb_label or fmt(imb)):>9}"
        )
    print()
    print("Shuffle microbench ceiling on this PIX host (1 GiB): ~37 GB/s uni clone, ~33 GB/s shuffle.")
    print("scan_filter: expect ~2x and ~0 remote bytes. join_skew: expect speedup near 1 and imbalance.")
    print("If wall speedup is poor while clone_ms is a small fraction of wall, compute not PIX is the limit.")


def compare_smoke(results_1: Path, results_2: Path) -> None:
    mismatches = 0
    for q in QUERIES:
        f1 = results_1 / f"{q}_timed_0.csv"
        f2 = results_2 / f"{q}_timed_0.csv"
        if not f1.is_file() or not f2.is_file():
            print(f"smoke: missing result for {q}: {f1} vs {f2}")
            mismatches += 1
            continue
        t1 = f1.read_text().strip()
        t2 = f2.read_text().strip()
        if t1 != t2:
            print(f"smoke MISMATCH {q}:\n  1gpu: {t1}\n  2gpu: {t2}")
            mismatches += 1
        else:
            print(f"smoke OK {q}: {t1.splitlines()[-1] if t1 else ''}")
    if mismatches:
        raise SystemExit(f"smoke failed: {mismatches} mismatch(es)")


def session_env(config: Path, log_dir: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.pop("SIRIUS_DISABLE", None)
    env["SIRIUS_CONFIG_FILE"] = str(config)
    env["SIRIUS_LOG_DIR"] = str(log_dir)
    return env


def run_one_gpu_count(
    duckdb: Path,
    root: Path,
    out_dir: Path,
    data_dir: Path,
    num_gpus: int,
    warmup: int,
    reps: int,
    smoke: bool,
    timeout: int,
) -> tuple[Path, Path, Path]:
    gpu_dir = out_dir / f"{num_gpus}gpu"
    log_dir = gpu_dir / "log"
    results_dir = gpu_dir / "results"
    if log_dir.exists():
        for p in log_dir.rglob("*"):
            if p.is_file():
                p.unlink()
    log_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)
    config = root / "scripts" / "benchmark" / f"sirius-{num_gpus}gpu.yaml"
    sql_path = gpu_dir / "session.sql"
    timings_csv = gpu_dir / "timings_raw.csv"
    write_session_sql(sql_path, data_dir, timings_csv, results_dir, warmup, reps, smoke)
    stdout_path = gpu_dir / "duckdb_stdout.txt"
    print(f"\n======== num_gpus={num_gpus} config={config} ========")
    env = session_env(config, log_dir)
    t0 = time.perf_counter()
    proc = subprocess.run(
        [str(duckdb), "-unsigned", "-f", str(sql_path)],
        cwd=root,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    elapsed = time.perf_counter() - t0
    stdout_path.write_text(proc.stdout or "")
    print(proc.stdout)
    print(f"session wall {elapsed:.1f}s exit={proc.returncode}")
    if proc.returncode != 0:
        raise SystemExit(f"DuckDB session failed for num_gpus={num_gpus} (see {stdout_path})")
    return gpu_dir, timings_csv, log_dir


def write_combined_csv(
    out_csv: Path,
    per_gpu: dict[int, tuple[Path, Path, Path]],
) -> None:
    fieldnames = [
        "query",
        "num_gpus",
        "phase",
        "iter",
        "wall_ms",
        "remote_bytes_0to1",
        "remote_bytes_1to0",
        "bytes_other",
        "clone_ms",
        "copies",
        "admitted_gpus",
        "pin_hit",
    ]
    rows_out: list[dict[str, object]] = []
    for num_gpus, (_gpu_dir, timings_csv, log_dir) in per_gpu.items():
        walls = load_timings(timings_csv)
        events = parse_log(collect_log_text(log_dir))
        by_query: dict[str, list[dict]] = {q: [] for q in QUERIES}
        for e in events:
            by_query[e["query"]].append(e)
        # Replay the same warmup-then-timed order used in write_session_sql.
        for q in QUERIES:
            q_events = by_query[q]
            idx = 0
            phases: list[tuple[str, int]] = []
            # Infer warmup/timed counts from timings keys.
            warm_iters = sorted({it for (qq, ph, it) in walls if qq == q and ph == "warmup"})
            timed_iters = sorted({it for (qq, ph, it) in walls if qq == q and ph == "timed"})
            for it in warm_iters:
                phases.append(("warmup", it))
            for it in timed_iters:
                phases.append(("timed", it))
            for phase, it in phases:
                ev = q_events[idx] if idx < len(q_events) else {}
                idx += 1
                wall = walls.get((q, phase, it), "")
                rows_out.append(
                    {
                        "query": q,
                        "num_gpus": num_gpus,
                        "phase": phase,
                        "iter": it,
                        "wall_ms": f"{wall:.4f}" if wall != "" else "",
                        "remote_bytes_0to1": ev.get("bytes_0to1", ""),
                        "remote_bytes_1to0": ev.get("bytes_1to0", ""),
                        "bytes_other": ev.get("bytes_other", ""),
                        "clone_ms": ev.get("clone_ms", ""),
                        "copies": ev.get("copies", ""),
                        "admitted_gpus": ev.get("admitted_gpus", ""),
                        "pin_hit": "1" if ev.get("pin_hit") else "0",
                    }
                )
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--rows", type=int, default=100_000_000)
    parser.add_argument("--join-rows", type=int, default=200_000_000)
    parser.add_argument("--files", type=int, default=32)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=7200)
    parser.add_argument("--smoke", action="store_true", help="Small row counts and 1-vs-2 result equality")
    parser.add_argument("--skip-generate", action="store_true")
    args = parser.parse_args()

    root = repo_root()
    out_dir = args.output_dir or (root / "build" / "benchmark-results" / "query-mgpu")
    out_dir = out_dir.resolve()
    data_dir = out_dir / "parquet"
    rows = 1_000_000 if args.smoke else args.rows
    join_rows = 2_000_000 if args.smoke else args.join_rows
    files = min(args.files, 8) if args.smoke else args.files

    duckdb = find_duckdb(root)
    print(f"duckdb={duckdb}")
    out_dir.mkdir(parents=True, exist_ok=True)

    if not args.skip_generate:
        generate_parquet(duckdb, data_dir, rows, join_rows, files)

    per_gpu: dict[int, tuple[Path, Path, Path]] = {}
    for n in (1, 2):
        per_gpu[n] = run_one_gpu_count(
            duckdb, root, out_dir, data_dir, n, args.warmup, args.reps, args.smoke, args.timeout
        )

    csv_path = out_dir / "query_mgpu.csv"
    write_combined_csv(csv_path, per_gpu)
    print(f"\nWrote {csv_path}")
    print_summary(csv_path)
    if args.smoke:
        compare_smoke(per_gpu[1][0] / "results", per_gpu[2][0] / "results")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as e:
        print(e.stdout or e, file=sys.stderr)
        sys.exit(e.returncode or 1)
