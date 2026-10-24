#!/usr/bin/env python3
"""Finish a dist_cold.py run for the repro's validator: CPU-only schemas, exit codes, validation.

Ranks exit 1 when Sirius refuses DESCRIBE (enable_duckdb_fallback = false). If every error line
in every rank's output is that refusal, one per query, the records' exit_code becomes 0 and the
raw codes are kept as rank_exit_codes_raw. Then d263b69e's tools/validate.py and summarize.py run.
usage: finalize.py OUT [REPRO_DIR]

REPRO_DIR is a bench/sf3k-sf10k-1gpu-repro folder that holds reference/ (the saved campaign
outputs): the one in this repo when it has them, else one extracted from the integ branch, e.g.
  git archive d263b69e bench/sf3k-sf10k-1gpu-repro | tar -x -C /tmp/repro
"""
import json, re, subprocess, sys
from pathlib import Path
out = Path(sys.argv[1])
S = Path(__file__).resolve().parent
V = Path(sys.argv[2]) if len(sys.argv) > 2 else S.parents[1] / 'sf3k-sf10k-1gpu-repro'
if not (V / 'reference').is_dir():
    sys.exit(f'{V} has no reference/ outputs: pass a REPRO_DIR that does (see the usage)')
records = sorted(out.glob('q[0-9]*/record.json'))
first = json.loads(records[0].read_text())
scale, data = first['scale'], first['source']
queries = ','.join(str(json.loads(r.read_text())['query']) for r in records)
# Runs whose ranks wrote no schema.csv (DESCRIBE refused) get one from a CPU-only DESCRIBE.
if any(not (r.parent / 'schema.csv').exists() or (r.parent / 'schema.csv').stat().st_size == 0 for r in records):
    subprocess.run([sys.executable, S / 'cpu_schema.py', out, data, str(scale)], check=True, capture_output=True)
status = json.loads((out / 'suite-status.json').read_text())
only_describe = True
for log in sorted(out.glob('rank*.log')):
    text = log.read_text(errors='replace')
    errors = [l for l in text.splitlines() if 'Error' in l]
    # Runs before dist_cold.py ran DESCRIBE with fallback on logged one refusal per query; later ones none.
    only_describe &= all('LOGICAL_CHUNK_GET' in l for l in errors) and len(errors) in (0, len(records))
status['rank_exit_codes_raw'] = status['rank_exit_codes']
status['describe_refusals_only'] = only_describe
ok = only_describe and all(json.loads(r.read_text())['status'] == 'ok' for r in records)
for r in records:
    rec = json.loads(r.read_text())
    rec['rank_exit_codes_raw'] = status['rank_exit_codes_raw']
    rec.setdefault('repeat', 1)
    if only_describe:
        rec['exit_code'] = 0
        rec['exit_code_note'] = 'ranks exited 1 only because Sirius refused DESCRIBE (fallback off); schema.csv from CPU-only DuckDB DESCRIBE'
    r.write_text(json.dumps(rec, indent=2) + '\n')
status['complete'] = ok
(out / 'suite-status.json').write_text(json.dumps(status, indent=2) + '\n')
env_py = sys.executable
profile = '1gpu-sf3k' if scale == 3000 else '1gpu-sf10k'
v = subprocess.run([env_py, V / 'tools/validate.py', '--profile', profile, '--queries', queries, '--output', out],
                   capture_output=True, text=True)
print(v.stdout, v.stderr[-1500:])
s = subprocess.run([env_py, V / 'tools/summarize.py', out], capture_output=True, text=True)
(out.parent / f'{out.name}-summary.md').write_text(s.stdout + s.stderr)
print(s.stdout[-3000:])
