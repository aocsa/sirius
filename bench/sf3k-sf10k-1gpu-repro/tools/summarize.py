#!/usr/bin/env python3
"""Print measured query seconds and subtotals; exclude explicit warmups."""
import argparse
from collections import defaultdict
from decimal import Decimal
import json
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('output', type=Path)
a = p.parse_args()
groups = defaultdict(list)
for path in a.output.rglob('record.json'):
    record = json.loads(path.read_text())
    if record.get('phase') == 'warmup':continue
    if record.get('status') != 'ok' or record.get('exit_code') != 0:
        raise SystemExit(f'Unaccepted native record: {path}; validate before publishing totals')
    groups[(record['mode'], record['repeat'])].append((record['query'], Decimal(str(record['query_s']))))
print('| Mode | Repeat | Query | Seconds |\n|---|---:|---:|---:|')
for (mode, repeat), rows in sorted(groups.items()):
    for q, seconds in sorted(rows):print(f'| {mode} | {repeat} | Q{q} | {seconds:.3f} |')
    print(f'| **{mode} subtotal** | **{repeat}** | **{len(rows)}/22** | **{sum(s for q,s in rows):.3f}** |')
if not groups:raise SystemExit('No measured records found')
print('\nPartial subtotals are not full-suite results. Native success does not replace typed validation.')
