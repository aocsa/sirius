#!/usr/bin/env python3
"""Analyze profile.py runs: per query run and per rank, time, disk, CPU, GPU, PCIe, Quent pipelines.

usage: analyze.py PROFILE_DIR...   (each a profile.py --out directory) -> markdown on stdout,
       plus PROFILE_DIR/analysis.json
"""
import glob
import json
import sys
from pathlib import Path

# extract.py runs its command line at import; take only its function definitions.
_src = (Path(__file__).resolve().parents[1] / 'extract.py').read_text()
_ns = {}
exec(_src[:_src.index('if sys.argv[1] == "--auto":')], _ns)
rank_profile = _ns['rank_profile']

GB = 1e9


def window_stats(samples, t0, t1, rank, gpu):
    inside = [s for s in samples if t0 <= s['t'] <= t1]
    before = [s for s in samples if s['t'] <= t0]
    after = [s for s in samples if s['t'] >= t1]
    a = before[-1] if before else (inside[0] if inside else None)
    b = after[0] if after else (inside[-1] if inside else None)
    if a is None or b is None:
        return {}
    dt = max(b['t'] - a['t'], 1e-6)
    ra, rb = a['rank'].get(str(rank)) or a['rank'].get(rank), b['rank'].get(str(rank)) or b['rank'].get(rank)
    g = [s['gpu'].get(str(gpu)) or s['gpu'].get(gpu) for s in inside]
    g = [x for x in g if x]
    # PCIe KB/s samples, integrated over the window.
    pts = [s for s in samples if a['t'] <= s['t'] <= b['t']]
    tx = rx = 0.0
    for p, q in zip(pts, pts[1:]):
        gp, gq = p['gpu'].get(str(gpu)) or p['gpu'].get(gpu), q['gpu'].get(str(gpu)) or q['gpu'].get(gpu)
        tx += (gp['pcie_tx_kbs'] + gq['pcie_tx_kbs']) / 2 * 1e3 * (q['t'] - p['t'])
        rx += (gp['pcie_rx_kbs'] + gq['pcie_rx_kbs']) / 2 * 1e3 * (q['t'] - p['t'])
    return {
        'disk_read_gb': (rb['read_bytes'] - ra['read_bytes']) / GB if ra and rb else None,
        'cpu_core_s': (rb['cpu_ticks'] - ra['cpu_ticks']) / 100 if ra and rb else None,
        'gpu_util_avg': sum(x['util'] for x in g) / len(g) if g else None,
        'gpu_util_busy_frac': sum(x['util'] > 50 for x in g) / len(g) if g else None,
        'gpu_mem_peak_gb': max(x['mem_used'] for x in g) / GB if g else None,
        'pcie_tx_gb': tx / GB, 'pcie_rx_gb': rx / GB,
        'array_read_gb': (b['disk_sectors'] - a['disk_sectors']) * 512 / GB,
        'array_gbs': (b['disk_sectors'] - a['disk_sectors']) * 512 / GB / dt,
        'mem_available_min_gb': min(s['mem_available'] for s in inside) / 2**30 if inside else None,
    }


def quent(rank_dir):
    dirs = sorted(glob.glob(f'{rank_dir}/telemetry_data/*'))
    if not dirs:
        return None
    prof = rank_profile(dirs[-1])
    return prof['queries']


def kind(label):
    l = label.upper()
    for k in ('GPU_SCAN', 'EXCHANGE_SOURCE', 'EXCHANGE_SINK', 'PARTITION', 'HASH_JOIN', 'GROUP_BY', 'CONCAT'):
        if k in l:
            return k
    return 'OTHER'


def main():
    out = []
    for d in map(Path, sys.argv[1:]):
        meta = json.loads((d / 'meta.json').read_text())
        windows = json.loads((d / 'windows.json').read_text())
        samples = json.loads((d / 'samples.json').read_text())
        q_by_rank = {r: quent(d / f'rank{r}') for r in range(meta['ranks'])}
        runs = []
        for wi, w in enumerate(windows):
            t1 = w['t0'] + w['wall_s']
            ranks = []
            for rr in w['ranks']:
                r = rr['rank']
                st = window_stats(samples, w['t0'], rr['t_done'], r, meta['gpus'][r])
                qp = None
                qs = q_by_rank.get(r)
                if qs and wi < len(qs):
                    pipes = qs[wi]['pipelines']
                    agg = {}
                    for p in pipes:
                        k = kind(p['label'])
                        a = agg.setdefault(k, {'compute_s': 0, 'queue_s': 0, 'reserve_s': 0, 'tasks': 0, 'bytes': 0, 'end': 0})
                        a['compute_s'] += p['compute_s']
                        a['queue_s'] += p['queue_s']
                        a['reserve_s'] += p['reserve_s']
                        a['tasks'] += p['tasks']
                        a['bytes'] += p['bytes']
                        a['end'] = max(a['end'], p['end'] or 0)
                    qp = {'exec_s': qs[wi]['exec_s'], 'plan_s': qs[wi]['plan_s'], 'by_kind': agg,
                          'compute_total_s': sum(p['compute_s'] for p in pipes),
                          'pipelines': pipes}
                ranks.append(dict(rank=r, gpu=meta['gpus'][r], timer_s=rr['timer_s'], done_after_s=rr['t_done'] - w['t0'],
                                  error=rr['error'], **st, quent=qp))
            arr = window_stats(samples, w['t0'], t1, 0, meta['gpus'][0])
            runs.append(dict(query=w['query'], iter=w['iter'], kind=w['kind'], wall_s=w['wall_s'],
                             array_gbs=arr.get('array_gbs'), array_read_gb=arr.get('array_read_gb'), ranks=ranks))
        res = dict(name=d.name, meta=meta, runs=runs)
        (d / 'analysis.json').write_text(json.dumps(res, indent=1) + '\n')
        out.append(res)

    # Markdown summary.
    print('## Query time by layout (s): cold, best warm\n')
    names = [r['name'] for r in out]
    qs = sorted({run['query'] for r in out for run in r['runs']})
    print('| Query | ' + ' | '.join(names) + ' |')
    print('|---|' + '---:|' * len(names))
    for q in qs:
        cells = []
        for r in out:
            rr = [x for x in r['runs'] if x['query'] == q]
            cold = next((x['wall_s'] for x in rr if x['kind'] == 'cold'), None)
            warm = min((x['wall_s'] for x in rr if x['kind'] == 'warm'), default=None)
            cells.append(f'{cold:.2f} / {warm:.2f}' if cold and warm else '-')
        print(f'| Q{q} | ' + ' | '.join(cells) + ' |')
    for r in out:
        print(f"\n## {r['name']} ({r['meta']['ranks']} ranks on GPUs {r['meta']['gpus']})\n")
        print('| Q | run | wall s | array GB/s | rank | done s | disk GB | CPU core-s | GPU util % | PCIe H2D GB | PCIe D2H GB | Quent compute s | scan out GB | exch-src out GB |')
        print('|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|')
        for run in r['runs']:
            for k in run['ranks']:
                qk = (k.get('quent') or {}).get('by_kind', {})
                f = lambda v, p=1: '-' if v is None else f'{v:.{p}f}'
                print(f"| Q{run['query']} | {run['kind']}{run['iter']} | {run['wall_s']:.2f} | {f(run['array_gbs'], 2)} | {k['rank']} | "
                      f"{k['done_after_s']:.2f} | {f(k.get('disk_read_gb'))} | {f(k.get('cpu_core_s'))} | {f(k.get('gpu_util_avg'), 0)} | "
                      f"{f(k.get('pcie_rx_gb'))} | {f(k.get('pcie_tx_gb'))} | {f((k.get('quent') or {}).get('compute_total_s'))} | "
                      f"{f(qk.get('GPU_SCAN', {}).get('bytes', 0) / GB if qk else None)} | {f(qk.get('EXCHANGE_SOURCE', {}).get('bytes', 0) / GB if qk else None)} |")


if __name__ == '__main__':
    main()
