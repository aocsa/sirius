#!/usr/bin/env python3
"""Distributed counterpart of bench/sf3k-sf10k-1gpu-repro/tools/benchmark.py --mode cold.

One DuckDB process per rank (rank i on GPU i, pinned to its own CPU slice), every rank running
the same script: the integ harness's setup (SET threads, one view per table over the full
dataset, reset_sirius_cache), then for each query, as the harness does: reset_sirius_cache()
and the query under .timer into result.csv (finalize.py adds each query's schema.csv). Rank 0's
outputs land in OUT/q<N>/ with record.json and suite-status.json, so the repro's
tools/validate.py and tools/summarize.py read them unchanged.

--pin first pins every table's queried columns (minus --pin-exclude) with pin_table, timed, then
runs the same cold sweep: reset_sirius_cache() empties the scan cache, not the pins.

usage: dist_cold.py --ranks N --config YAML --data DIR --out NEW_DIR [--pin ...]
Run from the repo root (a build with -DSIRIUS_WITH_NIXL=ON) through `pixi run python`.
"""
import argparse
import glob
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
REPRO = REPO / 'bench/sf3k-sf10k-1gpu-repro'
sys.path.insert(0, str(REPO / 'bench/dist-tpch'))
from columns import SIZES, cols  # noqa: E402

TABLES = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
TIMER = re.compile(r'Run Time\s*(?:\(s\))?:\s*real\s+([\d.]+)')


def lit(s):
    return "'" + str(s).replace("'", "''") + "'"


def cpu_slices(ranks):
    """Equal disjoint core slices with their hyperthreads (cores 0-47, siblings 48-95)."""
    per = 48 // ranks
    return [f'{i * per}-{(i + 1) * per - 1},{48 + i * per}-{48 + (i + 1) * per - 1}' for i in range(ranks)]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--ranks', type=int, required=True)
    ap.add_argument('--config', type=Path, required=True)
    ap.add_argument('--data', required=True)
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--scale', type=int, default=3000)
    ap.add_argument('--queries', default=','.join(str(q) for q in range(1, 23)))
    ap.add_argument('--threads', type=int, help='DuckDB threads per rank (default: its vCPU count)')
    ap.add_argument('--spill-root', type=Path, default=Path('/opt/dlami/nvme/sirius-spill-dist'))
    ap.add_argument('--port', type=int, default=35000)
    ap.add_argument('--timeout', type=int, default=10800)
    ap.add_argument('--pin', action='store_true')
    ap.add_argument('--pin-plans', default=str(REPO / 'src/compression/simpatico_codegen/plans/tpch_sf1000'))
    ap.add_argument('--pin-exclude', default='o_comment')
    ap.add_argument('--pin-replicate', default='nation,region,supplier')
    a = ap.parse_args()

    if subprocess.run(['pgrep', '-x', 'duckdb'], capture_output=True).returncode == 0:
        sys.exit('a duckdb process is running')
    if a.out.exists():
        sys.exit(f'output must be a new directory: {a.out}')
    a.out.mkdir(parents=True)
    spec = importlib.util.spec_from_file_location('tpch_queries', REPRO / 'queries.py')
    qmod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(qmod)
    texts = qmod.queries_for_scale_factor(a.scale)
    queries = [int(q) for q in a.queries.split(',')]
    slices = cpu_slices(a.ranks)
    vcpus = 96 // a.ranks
    threads = a.threads or vcpus

    spill = a.spill_root / a.out.name
    shutil.rmtree(spill, ignore_errors=True)
    spill.mkdir(parents=True)
    cfg = a.out / 'sirius.yaml'
    cfg.write_text(a.config.read_text().replace('@SPILL_DIR@', str(spill)))
    shutil.copyfile(a.config, a.out / 'config-template.yaml')

    pins = {}
    if a.pin:
        exclude = set(filter(None, a.pin_exclude.split(',')))
        union = {}
        for q in queries:
            for t, cs in cols(q, a.scale).items():
                union.setdefault(t, set()).update(cs)
        pins = {t: [c for c in SIZES[t] if c in cs and c not in exclude] for t, cs in union.items()}
    replicate = set(filter(None, a.pin_replicate.split(',')))

    peers = ','.join(f'127.0.0.1:{a.port + i}' for i in range(a.ranks))
    ext = REPO / 'sirius-duckdb/build/release/extension/sirius/sirius.duckdb_extension'
    procs = []
    for r in range(a.ranks):
        base = a.out if r == 0 else a.out / f'rank{r}'
        lines = ['.timer off', f'LOAD {lit(ext)};', 'SET enable_duckdb_fallback = false;',
                 f"SET sirius_exchange_peers = '{peers}';", f'SET sirius_exchange_rank = {r};',
                 f'SET threads={threads};']
        lines += [f'CREATE VIEW {t} AS SELECT * FROM read_parquet({lit(a.data.rstrip("/") + "/" + t + "/*.parquet")});'
                  for t in TABLES]
        if pins:
            lines.append(f'SET pin_table_input_compression_plan_dir = {lit(a.pin_plans)};')
            lines.append('.timer on')
            for t, cs in pins.items():
                lines.append(f"CALL pin_table({lit(a.data.rstrip('/') + '/' + t + '/*.parquet')}, name = {lit(t)}, "
                             f"tier = 'host', cols = [{', '.join(lit(c) for c in cs)}]"
                             + (', replicate = true' if t in replicate else '') + ', compression = true);')
            lines.append('.timer off')
        lines.append('CALL reset_sirius_cache();')
        for q in queries:
            out = base / f'q{q}'
            out.mkdir(parents=True, exist_ok=True)
            (out / 'query.sql').write_text(texts[f'q{q}'] + ';\n')
            sql = texts[f'q{q}'].rstrip().rstrip(';')
            # No DESCRIBE here: with enable_duckdb_fallback off Sirius refuses it, and with it on it
            # prints nothing. finalize.py writes schema.csv from a CPU-only DuckDB DESCRIBE.
            lines += ['.timer off', 'CALL reset_sirius_cache();',
                      f'.print __Q{q}_READY__', '.timer on', f'.once {out / "result.csv"}', f'{sql};', '.timer off',
                      f'.print __Q{q}_DONE__']
        lines.append('.print __SWEEP_DONE__')
        sqlf = a.out / f'rank{r}.sql'
        sqlf.write_text('\n'.join(lines) + '\n')
        logdir = a.out / 'logs' / f'rank{r}'
        logdir.mkdir(parents=True)
        env = dict(os.environ, SIRIUS_CONFIG_FILE=str(cfg), SIRIUS_LOG_DIR=str(logdir), CUDA_VISIBLE_DEVICES=str(r))
        for k in ('AI_AGENT', 'CLAUDECODE', 'CLAUDE_CODE'):
            env.pop(k, None)
        cmd = ['bash', '-c', 'source "$1"; exec taskset -c "$2" timeout -s KILL "$3" "$4" -unsigned -batch -csv -header :memory: < "$5"',
               '_', str(REPO / 'scripts/nixl_env.sh'), slices[r], str(a.timeout), str(REPO / 'sirius-duckdb/build/release/duckdb'),
               str(sqlf)]
        procs.append(subprocess.Popen(cmd, cwd=a.out, env=env, stdout=open(a.out / f'rank{r}.log', 'w'),
                                      stderr=subprocess.STDOUT))
    t0 = time.time()
    codes = [p.wait() for p in procs]
    wall = time.time() - t0

    log0 = (a.out / 'rank0.log').read_text(errors='replace')
    times = [float(x) for x in TIMER.findall(log0)]
    pin_s = times[:len(pins)]
    qtimes = times[len(pins):]
    gpu_done = {r: sum(open(f, errors='replace').read().count('Transparent GPU execution: query completed')
                       for f in glob.glob(str(a.out / 'logs' / f'rank{r}' / '*.log'))) for r in range(a.ranks)}
    pin_hits = sum(open(f, errors='replace').read().count('assigned pinned entry')
                   for f in glob.glob(str(a.out / 'logs' / '*' / '*.log')))
    ok_all = all(c == 0 for c in codes)
    # A query fails when any rank reports an error while running it: an aborted distributed query
    # still prints a timer line, and rank 0 may still write an (empty) result.
    logs = [(a.out / f'rank{r}.log').read_text(errors='replace') for r in range(a.ranks)]

    def rank_error(q):
        for r, log in enumerate(logs):
            start = log.find(f'__Q{q}_READY__')
            end = log.find(f'__Q{q}_DONE__', start)
            if start >= 0 and end > start:
                for line in log[start:end].splitlines():
                    if 'Error' in line:
                        return f'rank {r}: {line.strip()[:300]}'
        return None

    results = []
    for i, q in enumerate(queries):
        out = a.out / f'q{q}'
        done = f'__Q{q}_DONE__' in log0
        seconds = qtimes[i] if i < len(qtimes) and done else None
        error = rank_error(q)
        status = ('failed' if error else 'ok') if seconds is not None and (out / 'result.csv').exists() \
            else 'missing-timing-or-result'
        record = dict(scale=a.scale, mode='cold', repeat=1, query=q, status=status, query_s=seconds,
                      exit_code=0 if ok_all else max(codes), ranks=a.ranks, cpus_per_rank=slices, threads_per_rank=threads,
                      pinned=bool(pins), source=a.data, error=error,
                      cache='reset_sirius_cache before each timed query; ' + ('tables pinned once before the sweep' if pins else 'no table pins'))
        (out / 'record.json').write_text(json.dumps(record, indent=2) + '\n')
        results.append(record)
    complete = ok_all and all(r['status'] == 'ok' for r in results)
    status = dict(mode='cold', ranks=a.ranks, queries_finished=sum(r['status'] == 'ok' for r in results),
                  expected=len(queries), rank_exit_codes=codes, gpu_completion_log_entries=gpu_done,
                  pin_tables=list(pins), pin_s=dict(zip(pins, pin_s)), pin_hits=pin_hits, wall_s=round(wall, 1),
                  total_query_s=round(sum(r['query_s'] or 0 for r in results), 3), complete=complete)
    (a.out / 'suite-status.json').write_text(json.dumps(status, indent=2) + '\n')
    shutil.rmtree(spill, ignore_errors=True)
    print(json.dumps(status))
    for r in results:
        print(f"Q{r['query']:>2} {r['query_s']} {r['status']}")
    sys.exit(0 if complete else 1)


if __name__ == '__main__':
    main()
