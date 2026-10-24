#!/usr/bin/env python3
"""Write OUT/q<N>/schema.csv with a CPU-only DuckDB DESCRIBE of each query (Sirius not loaded).

The distributed sweep runs with enable_duckdb_fallback = false, which makes Sirius refuse
DESCRIBE's catalog scan; the integ harness's schema.csv is a DuckDB binder result, so a plain
DuckDB DESCRIBE over the same views gives the same column types.
usage: cpu_schema.py OUT [DATA] [SCALE]
"""
import importlib.util, os, subprocess, sys
from pathlib import Path
REPO = Path(__file__).resolve().parents[3]
out = Path(sys.argv[1]); data = sys.argv[2] if len(sys.argv) > 2 else '/opt/dlami/nvme/tpch_sf3000_regenerated'
scale = int(sys.argv[3]) if len(sys.argv) > 3 else 3000
spec = importlib.util.spec_from_file_location('q', REPO / 'bench/sf3k-sf10k-1gpu-repro/queries.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); texts = m.queries_for_scale_factor(scale)
lines = [f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{data}/{t}/*.parquet');"
         for t in ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']]
for d in sorted(out.glob('q[0-9]*'), key=lambda p: int(p.name[1:])):
    q = int(d.name[1:])
    lines += [f'.once {d / "schema.csv"}', 'DESCRIBE ' + texts[f'q{q}'].rstrip().rstrip(';') + ';']
env = {k: v for k, v in os.environ.items() if k not in ('AI_AGENT', 'CLAUDECODE', 'CLAUDE_CODE')}
r = subprocess.run([str(REPO / 'sirius-duckdb/build/release/duckdb'), '-batch', '-csv', '-header', ':memory:'],
                   input='\n'.join(lines) + '\n', text=True, capture_output=True, env=env)
print(r.stdout[-2000:], r.stderr[-2000:]); sys.exit(r.returncode)
