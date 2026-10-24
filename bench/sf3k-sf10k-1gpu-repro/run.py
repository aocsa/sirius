#!/usr/bin/env python3
"""Run one 1-GPU TPC-H profile (cold, lukewarm or hot) against a built Sirius checkout."""
import argparse
import hashlib
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
PROFILES = json.loads((ROOT / 'profiles.json').read_text())
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('profile', choices=PROFILES)
p.add_argument('--source', required=True, help='Dataset root holding <table>/*.parquet (local path or s3:// prefix)')
p.add_argument('--output', type=Path, required=True, help='New directory for this run')
p.add_argument('--mode', choices=['cold', 'lukewarm', 'hot'], default='cold')
p.add_argument('--queries', help='Comma-separated query numbers; default is the profile list')
p.add_argument('--repo', type=Path, default=ROOT.parents[1], help='Built Sirius checkout (default: this repository)')
p.add_argument('--sirius-config', type=Path, help='Sirius YAML; default is the profile config')
p.add_argument('--gpus', default='0', help='CUDA_VISIBLE_DEVICES for the run')
p.add_argument('--cpus', help='taskset CPU list, e.g. 0-15,48-63 to match a 32-vCPU g7e.8xlarge')
p.add_argument('--spill-root', default='/opt/dlami/nvme/sirius-spill', help='Directory for per-run spill subdirectories')
p.add_argument('--dry-run', action='store_true', help='Print the command without touching GPUs or data')
a = p.parse_args()
profile = PROFILES[a.profile]
config = (a.sirius_config or ROOT / profile['config']).resolve()
queries = a.queries or profile['queries']
cmd = [sys.executable, str(ROOT / 'tools/benchmark.py'), '--repo', str(a.repo.resolve()),
       '--query-definitions', str(ROOT / 'queries.py'), '--config-template', str(config),
       '--output', str(a.output.resolve() / a.mode), '--source', a.source, '--spill-root', a.spill_root,
       '--scale', str(profile['scale']), '--mode', a.mode, '--queries', queries, '--timeout', '0',
       '--warmups', '0' if a.mode == 'cold' else '1', '--repeats', '1' if a.mode == 'cold' else '2',
       '--num-gpus', str(profile['num_gpus']), '--cpu-threads', str(profile['cpu_threads']),
       '--cache-gb', str(profile['cache_gb'])]
if a.cpus:
    cmd = ['taskset', '-c', a.cpus] + cmd
if a.dry_run:
    print(f'CUDA_VISIBLE_DEVICES={a.gpus} ' + shlex.join(cmd))
    raise SystemExit(0)
if a.output.exists():
    p.error('output must be a new directory')
git = lambda *args: subprocess.check_output(['git', '-C', str(a.repo), *args], text=True).strip()
if git('status', '--porcelain', '--untracked-files=no'):
    p.error('commit tracked source changes before recording a benchmark build')
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
artifacts = {}
for name in ['sirius-duckdb/build/release/duckdb',
             'sirius-duckdb/build/release/extension/sirius/sirius.duckdb_extension',
             'build/release/install/lib/libsirius.so.0.0.0']:
    path = a.repo / name
    if not path.is_file():
        p.error(f'build artifact missing; build the checkout first: {path}')
    artifacts[name] = digest(path)
a.output.mkdir(parents=True)
(a.output / 'invocation.json').write_text(json.dumps(dict(
    profile=a.profile, mode=a.mode, queries=queries, source=a.source,
    source_commit=git('rev-parse', 'HEAD'), source_tree=git('rev-parse', 'HEAD^{tree}'),
    build_sha256=artifacts, sirius_config=str(config), sirius_config_sha256=digest(config),
    query_definitions_sha256=digest(ROOT / 'queries.py'), cuda_visible_devices=a.gpus, cpus=a.cpus,
    command=cmd, started_utc=datetime.now(timezone.utc).isoformat()), indent=2) + '\n')
subprocess.run(cmd, cwd=a.repo, env=dict(os.environ, CUDA_VISIBLE_DEVICES=a.gpus), check=True)
print('RUN COMPLETE. Validate with tools/validate.py before accepting timings.')
