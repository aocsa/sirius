#!/usr/bin/env python3
"""Before/after table for the scalability profile, cold and best warm.

usage: compare.py OLD_DIR NEW_DIR   each holding profile.py runs named gpus-0, gpus-0-1, gpus-0-2,
                                    gpus-0-1-2-3 (OLD) and staged-<same> (NEW)
"""
import json
import sys
from pathlib import Path
OLD, NEW = Path(sys.argv[1]), Path(sys.argv[2])
LAYOUTS = [('gpus-0', '1 GPU'), ('gpus-0-1', '2 ranks, GPUs 0,1'), ('gpus-0-2', '2 ranks, GPUs 0,2'),
           ('gpus-0-1-2-3', '4 ranks')]


def times(d):
    if not (d / 'windows.json').exists():
        return None
    out = {}
    for w in json.loads((d / 'windows.json').read_text()):
        t = out.setdefault(w['query'], {'cold': None, 'warm': None})
        if w['kind'] == 'cold':
            t['cold'] = w['wall_s']
        else:
            t['warm'] = w['wall_s'] if t['warm'] is None else min(t['warm'], w['wall_s'])
    return out


print('Seconds, cold / best warm. Change is new vs old, cold and warm; negative is faster.\n')
print('| Query | Layout | Before | After | Change cold | Change warm |')
print('|---|---|---:|---:|---:|---:|')
for q in (1, 6, 9, 18):
    for name, label in LAYOUTS:
        a, b = times(OLD / name), times(NEW / f'staged-{name}')
        if not a or not b or q not in a or q not in b:
            print(f'| Q{q} | {label} | - | - | - | - |')
            continue
        o, n = a[q], b[q]
        pct = lambda x, y: f'{(y - x) / x * 100:+.0f}%'
        print(f"| Q{q} | {label} | {o['cold']:.2f} / {o['warm']:.2f} | {n['cold']:.2f} / {n['warm']:.2f} | "
              f"{pct(o['cold'], n['cold'])} | {pct(o['warm'], n['warm'])} |")
