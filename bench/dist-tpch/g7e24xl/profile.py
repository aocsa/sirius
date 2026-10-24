#!/usr/bin/env python3
"""Scalability profile: the same TPC-H queries on 1 GPU (single process, no exchange) or on N
ranks (one process per GPU), with per-rank OS/GPU sampling and Quent telemetry.

Every rank gets the same CPU slice size (--vcpus, cores plus their hyperthreads) and DuckDB
threads, so resources grow with the rank count. The driver feeds all ranks the same statements
through stdin and records, per query run, when it sent the query and when each rank printed its
end marker. For each query: one cold run (reset_sirius_cache first), then --iters - 1 warm runs.

A sampler records every 0.25 s: each rank's /proc/<pid>/io read_bytes and CPU time, each GPU's
utilization, memory and PCIe TX/RX (NVML), and sectors read from the data NVMe devices.
Each rank runs in OUT/rank<i>/ so its Quent telemetry lands in OUT/rank<i>/telemetry_data/.

usage: profile.py --gpus 0,1 --out NEW_DIR [--queries 1,6,9,18] [--iters 3] [--vcpus 24]
Run from the repo root through `pixi run python`.
"""
import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import pynvml

REPO = Path(__file__).resolve().parents[3]
TABLES = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
DISKS = ['nvme1n1', 'nvme2n1']
TIMER = re.compile(r'Run Time\s*(?:\(s\))?:\s*real\s+([\d.]+)')


def lit(s):
    return "'" + str(s).replace("'", "''") + "'"


class Rank:
    def __init__(self, r, gpu, cpus, cwd, env, cmd):
        self.r, self.gpu, self.cpus = r, gpu, cpus
        self.lines = []  # (time, line)
        self.cv = threading.Condition()
        self.log = open(cwd / 'stdout.log', 'w')
        self.p = subprocess.Popen(cmd, cwd=cwd, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True, bufsize=1)
        threading.Thread(target=self.read, daemon=True).start()

    def read(self):
        for line in self.p.stdout:
            now = time.time()
            self.log.write(f'{now:.3f} {line}')
            self.log.flush()
            with self.cv:
                self.lines.append((now, line.rstrip('\n')))
                self.cv.notify_all()
        with self.cv:
            self.lines.append((time.time(), '__EOF__'))
            self.cv.notify_all()

    def send(self, text):
        self.p.stdin.write(text)
        self.p.stdin.flush()

    def wait(self, marker, start, timeout):
        """Index and time of the first line == marker at or after index start."""
        deadline = time.time() + timeout
        with self.cv:
            while True:
                for i in range(start, len(self.lines)):
                    if self.lines[i][1] == marker:
                        return i, self.lines[i][0]
                    if self.lines[i][1] == '__EOF__':
                        raise RuntimeError(f'rank {self.r} exited before {marker}')
                if time.time() > deadline:
                    raise TimeoutError(f'rank {self.r}: no {marker}')
                self.cv.wait(1)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--gpus', required=True, help='GPU per rank, e.g. 0 or 0,1 or 0,2 or 0,1,2,3')
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--config', type=Path, default=Path(__file__).resolve().parent / 'configs/sf3k-unpinned.yaml')
    ap.add_argument('--data', default='/opt/dlami/nvme/tpch_sf3000_regenerated')
    ap.add_argument('--queries', default='1,6,9,18')
    ap.add_argument('--iters', type=int, default=3)
    ap.add_argument('--vcpus', type=int, default=24, help='vCPUs per rank (half cores, half their hyperthreads)')
    ap.add_argument('--quent', action='store_true', help='turn Quent telemetry on')
    ap.add_argument('--port', type=int, default=36000)
    ap.add_argument('--timeout', type=int, default=3600)
    a = ap.parse_args()

    if subprocess.run(['pgrep', '-x', 'duckdb'], capture_output=True).returncode == 0:
        sys.exit('a duckdb process is running')
    if a.out.exists():
        sys.exit(f'output must be a new directory: {a.out}')
    a.out.mkdir(parents=True)
    gpus = [int(g) for g in a.gpus.split(',')]
    ranks = len(gpus)
    per = a.vcpus // 2
    cpus = [f'{i * per}-{(i + 1) * per - 1},{48 + i * per}-{48 + (i + 1) * per - 1}' for i in range(ranks)]
    spec = importlib.util.spec_from_file_location('q', REPO / 'bench/sf3k-sf10k-1gpu-repro/queries.py')
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    texts = m.queries_for_scale_factor(3000)
    queries = [int(q) for q in a.queries.split(',')]

    spill = Path('/opt/dlami/nvme/sirius-spill-dist') / a.out.name
    spill.mkdir(parents=True, exist_ok=True)
    cfg_text = a.config.read_text().replace('@SPILL_DIR@', str(spill))
    if a.quent:
        cfg_text = cfg_text.replace('enable_quent: false', 'enable_quent: true')
        assert 'enable_quent: true' in cfg_text
    cfg = a.out / 'sirius.yaml'
    cfg.write_text(cfg_text)

    peers = ','.join(f'127.0.0.1:{a.port + i}' for i in range(ranks))
    ext = REPO / 'sirius-duckdb/build/release/extension/sirius/sirius.duckdb_extension'
    rs = []
    for r, g in enumerate(gpus):
        cwd = a.out / f'rank{r}'
        (cwd / 'logs').mkdir(parents=True)
        env = dict(os.environ, SIRIUS_CONFIG_FILE=str(cfg), SIRIUS_LOG_DIR=str(cwd / 'logs'), CUDA_VISIBLE_DEVICES=str(g))
        for k in ('AI_AGENT', 'CLAUDECODE', 'CLAUDE_CODE'):
            env.pop(k, None)
        cmd = ['bash', '-c', 'source "$1"; exec taskset -c "$2" "$3" -unsigned -batch -csv -header :memory:', '_',
               str(REPO / 'scripts/nixl_env.sh'), cpus[r], str(REPO / 'sirius-duckdb/build/release/duckdb')]
        rs.append(Rank(r, g, cpus[r], cwd, env, cmd))

    # Sampler.
    pynvml.nvmlInit()
    handles = {g: pynvml.nvmlDeviceGetHandleByIndex(g) for g in range(pynvml.nvmlDeviceGetCount())}
    samples, stop = [], threading.Event()

    def sample():
        while not stop.is_set():
            s = {'t': time.time(), 'rank': {}, 'gpu': {}, 'disk_sectors': 0}
            for rk in rs:
                try:
                    io = dict(l.split(': ') for l in open(f'/proc/{rk.p.pid}/io').read().split('\n') if ': ' in l)
                    st = open(f'/proc/{rk.p.pid}/stat').read().rsplit(')', 1)[1].split()
                    rss = int(open(f'/proc/{rk.p.pid}/statm').read().split()[1]) * 4096
                    s['rank'][rk.r] = {'read_bytes': int(io['read_bytes']), 'cpu_ticks': int(st[11]) + int(st[12]), 'rss': rss}
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    pass
            for g, h in handles.items():
                u = pynvml.nvmlDeviceGetUtilizationRates(h)
                s['gpu'][g] = {'util': u.gpu, 'mem_used': pynvml.nvmlDeviceGetMemoryInfo(h).used,
                               'pcie_tx_kbs': pynvml.nvmlDeviceGetPcieThroughput(h, pynvml.NVML_PCIE_UTIL_TX_BYTES),
                               'pcie_rx_kbs': pynvml.nvmlDeviceGetPcieThroughput(h, pynvml.NVML_PCIE_UTIL_RX_BYTES)}
            for line in open('/proc/diskstats'):
                f = line.split()
                if f[2] in DISKS:
                    s['disk_sectors'] += int(f[5])
            s['mem_available'] = int(next(l for l in open('/proc/meminfo') if l.startswith('MemAvailable')).split()[1]) * 1024
            samples.append(s)
            stop.wait(0.25)

    threading.Thread(target=sample, daemon=True).start()

    def all_send(text):
        for rk in rs:
            rk.send(text)

    def all_wait(marker, timeout):
        return [rk.wait(marker, 0, timeout) for rk in rs]

    setup = ['.timer off', f'LOAD {lit(ext)};', 'SET enable_duckdb_fallback = false;']
    if ranks > 1:
        setup += [f"SET sirius_exchange_peers = '{peers}';"]
    setup += [f'SET threads={a.vcpus};']
    setup += [f'CREATE VIEW {t} AS SELECT * FROM read_parquet({lit(a.data + "/" + t + "/*.parquet")});' for t in TABLES]
    t_launch = time.time()
    for rk in rs:
        extra = [f'SET sirius_exchange_rank = {rk.r};'] if ranks > 1 else []
        rk.send('\n'.join(setup[:3] + setup[3:4] + extra + setup[4:]) + '\n.print __READY__\n')
    all_wait('__READY__', 600)
    t_ready = time.time()

    windows = []
    seen = [0] * ranks
    for q in queries:
        sql = texts[f'q{q}'].rstrip().rstrip(';')
        for it in range(a.iters):
            if it == 0:
                all_send(f'CALL reset_sirius_cache();\n.print __RESET_{q}__\n')
                for rk in rs:
                    seen[rk.r] = rk.wait(f'__RESET_{q}__', seen[rk.r], 600)[0] + 1
            marker = f'__DONE_{q}_{it}__'
            t0 = time.time()
            all_send(f'.timer on\n{sql};\n.timer off\n.print {marker}\n')
            done = []
            for rk in rs:
                i, t = rk.wait(marker, seen[rk.r], a.timeout)
                body = [l for _, l in rk.lines[seen[rk.r]:i]]
                timer = [float(x) for l in body for x in TIMER.findall(l)]
                rows = [l for l in body if l and not TIMER.search(l) and not l.startswith('__')]
                done.append({'rank': rk.r, 't_done': t, 'timer_s': timer[-1] if timer else None,
                             'out_lines': len(rows), 'error': next((l for l in body if 'Error' in l), None)})
                seen[rk.r] = i + 1
            w = {'query': q, 'iter': it, 'kind': 'cold' if it == 0 else 'warm', 't0': t0, 'ranks': done,
                 'wall_s': max(d['t_done'] for d in done) - t0}
            windows.append(w)
            print(f"Q{q} {w['kind']}{it}: wall {w['wall_s']:.2f}s, rank timers "
                  f"{[d['timer_s'] for d in done]}, done spread {max(d['t_done'] for d in done) - min(d['t_done'] for d in done):.2f}s",
                  flush=True)

    all_send('.quit\n')
    for rk in rs:
        rk.p.wait(timeout=300)
    stop.set()
    time.sleep(0.5)
    meta = dict(gpus=gpus, ranks=ranks, cpus=cpus, vcpus=a.vcpus, queries=queries, iters=a.iters, quent=a.quent,
                config=str(a.config), t_launch=t_launch, t_ready=t_ready, pids=[rk.p.pid for rk in rs],
                exit_codes=[rk.p.returncode for rk in rs], commit=subprocess.check_output(
                    ['git', '-C', str(REPO), 'rev-parse', 'HEAD'], text=True).strip())
    (a.out / 'meta.json').write_text(json.dumps(meta, indent=1) + '\n')
    (a.out / 'windows.json').write_text(json.dumps(windows, indent=1) + '\n')
    (a.out / 'samples.json').write_text(json.dumps(samples) + '\n')
    subprocess.run(['rm', '-rf', str(spill)])


if __name__ == '__main__':
    main()
