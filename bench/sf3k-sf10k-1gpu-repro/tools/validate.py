#!/usr/bin/env python3
"""Validate a run's query outputs against the saved campaign GPU outputs in reference/."""
import argparse
import json
from pathlib import Path
from result_validation import validate, validate_with_expanded_q3
from sf10k_followup_validation import validate_q18_boundary

ROOT = Path(__file__).resolve().parents[1]
PROFILES = json.loads((ROOT / 'profiles.json').read_text())
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--profile', choices=PROFILES, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--queries', help='Comma-separated query numbers the run was asked for; default is the profile list')
a = p.parse_args()
scale = PROFILES[a.profile]['scale']
reference = ROOT / f'reference/sf{scale}'
checks = []
for record_path in sorted(a.output.rglob('record.json')):
    record = json.loads(record_path.read_text())
    if record.get('scale') != scale:
        continue
    q = record['query']
    current = record_path.parent
    other = reference / f'q{q}'
    result = validate(current, other, q)
    expanded = ROOT / f'reference/expanded/sf{scale}/q{q}'
    if not result['agree'] and q == 3 and expanded.exists():
        result = validate_with_expanded_q3(current, other, q, expanded)
    if not result['agree'] and q == 18 and expanded.exists():
        result = validate_q18_boundary(current, other, expanded)
    result['native_status'] = record.get('status')
    result['native_exit_code'] = record.get('exit_code')
    result['accepted'] = bool(result['agree'] and record.get('status') == 'ok' and record.get('exit_code') == 0)
    checks.append(dict(query=q, path=str(current), **result))
suites = [json.loads(s.read_text()) for s in a.output.rglob('suite-status.json')]
expected_queries = {int(q) for q in (a.queries or PROFILES[a.profile]['queries']).split(',')}
seen_queries = {c['query'] for c in checks}
summary = dict(checks=checks, compared=len(checks),
               reference='Saved campaign GPU outputs (Q11/Q20 as exact row digests), with proved Q3/Q18 LIMIT ties; not an independent CPU oracle',
               suite_checks=dict(suites_found=len(suites), all_suites_complete=bool(suites) and all(s.get('complete') for s in suites),
                                 expected_records=sum(s.get('expected', 0) for s in suites),
                                 expected_queries=sorted(expected_queries), seen_queries=sorted(seen_queries)))
summary['complete'] = (bool(checks) and all(c['accepted'] for c in checks)
                       and summary['suite_checks']['all_suites_complete']
                       and summary['compared'] == summary['suite_checks']['expected_records']
                       and expected_queries == seen_queries)
(a.output / 'validation.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(dict(compared=summary['compared'], passed=sum(c['accepted'] for c in checks),
                      complete=summary['complete']), indent=2))
raise SystemExit(0 if summary['complete'] else 1)
