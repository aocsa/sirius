#!/usr/bin/env python3
"""Reduced compression-on TPC-H experiments with explicit host/cache limits and reactor selection."""
import argparse, importlib.util, hashlib, json, os, re, select, shutil, subprocess, time, urllib.request, threading
from pathlib import Path
from q21_spill_metrics import summarize as spill_summary


def schedule(mode, queries, warmups, repeats):
    if mode == 'paired':
        return [(phase,1,q) for phase in ['cold','lukewarm'] for q in queries]
    if mode == 'cold':
        return [('measured', iteration, q) for iteration in range(1,repeats+1) for q in queries]
    if mode == 'lukewarm':
        return [(phase, iteration, q) for phase, n in [('warmup',warmups),('measured',repeats)]
                for iteration in range(1,n+1) for q in queries]
    return [(phase, iteration, q) for q in queries
            for phase,n in [('warmup',warmups),('measured',repeats)] for iteration in range(1,n+1)]

p=argparse.ArgumentParser()
p.add_argument('--mode',choices=['cold','lukewarm','hot','paired'],required=True)
p.add_argument('--cache-gb',type=int,required=True)
p.add_argument('--bloom-cap-mib',type=int,default=None)
p.add_argument('--host-gib',type=int)
p.add_argument('--host-fraction',type=float,default=0.9)
p.add_argument('--num-gpus',type=int,choices=[1,2],default=1)
p.add_argument('--reactors',type=int,choices=[2,4,8,16,32,64],default=16)
p.add_argument('--cpu-threads',type=int,default=32)
p.add_argument('--disk-gb',type=int,default=800)
p.add_argument('--source',required=True)
p.add_argument('--scale',type=int,default=3000)
p.add_argument('--q3-limit',type=int,choices=[10,20],default=10)
p.add_argument('--queries',default=','.join(map(str,range(1,23))))
p.add_argument('--repeats',type=int,default=2)
p.add_argument('--warmups',type=int,default=1)
p.add_argument('--initial-pools',type=int,default=384)
p.add_argument('--repo',type=Path,default=Path('/home/ubuntu/sirius'))
p.add_argument('--query-definitions',type=Path,help='Use fixed benchmark SQL across main/PR source revisions')
p.add_argument('--output',type=Path,required=True)
p.add_argument('--checkpoint',default='')
p.add_argument('--timeout',type=int,default=0,help='0 means no query deadline')
p.add_argument('--engines',default='compression-on')
p.add_argument('--spill-root',type=Path,default=Path('/opt/dlami/nvme/sirius-spill'))
p.add_argument('--config-template',type=Path,help='Sirius YAML used as-is except downgrade_root_dirs (and Quent output_directory); host/cache labels come from it')
a=p.parse_args()
template=None
if a.config_template:
    import yaml
    template=yaml.safe_load(a.config_template.read_text())
    if template['sirius']['topology']['num_gpus']!=a.num_gpus:p.error('--num-gpus must match the config template topology')
    if not isinstance(template['sirius']['memory']['host'].get('capacity_bytes'),int):p.error('Config template host capacity_bytes must be an integer byte count')
if a.timeout<0:p.error('timeout must be nonnegative')
if a.engines!='compression-on':p.error('Only compression-on is authorized for this rerun')
physical=int(next(x.split()[1] for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemTotal:')))*1024
if not 0<a.host_fraction<=0.9:p.error('Managed host capacity must be at most 90% of physical RAM')
a.host_bytes=a.host_gib*2**30 if a.host_gib else int(physical*a.host_fraction)
a.host_label=f'{a.host_gib}GiB' if a.host_gib else f'{100*a.host_fraction:g}pctRAM'
if template:
    a.host_bytes=template['sirius']['memory']['host']['capacity_bytes'];a.host_label=f'{a.host_bytes}B'
    a.cache_gb=round(template['sirius']['executor']['scan_manager']['cache']['eviction_threshold_fraction']*a.host_bytes/10**9)
if physical-a.host_bytes<16*2**30:p.error('Leave at least 16GiB outside the managed host pool')
if a.cache_gb<1:p.error('Cache must be positive')
if a.cache_gb*10**9>=a.host_bytes:p.error('Cache must fit inside the managed host pool')
if a.repeats<1 or a.warmups<0 or (a.mode not in ['cold','paired'] and a.warmups<1):p.error('Warm modes require a warmup; all modes require a measured repetition')
if a.mode in ['cold','paired'] and (a.warmups!=0 or a.repeats!=1):p.error('Cold is one measured sweep with zero warmups')
a.output.mkdir(parents=True,exist_ok=True)
spec=importlib.util.spec_from_file_location('tpch_queries',a.query_definitions or a.repo/'test/tpch_performance/queries.py')
queries=importlib.util.module_from_spec(spec);spec.loader.exec_module(queries)
query_texts=queries.queries_for_scale_factor(a.scale)
if a.q3_limit!=10:
    query_texts['q3'],changed=re.subn(r'LIMIT\s+10\s*;?\s*$',f'LIMIT {a.q3_limit}',query_texts['q3'],flags=re.IGNORECASE)
    assert changed==1,'Expanded Q3 validation must change exactly the LIMIT'
query_ids=[int(q) for q in a.queries.split(',')]
cli=a.repo/'sirius-duckdb/build/release/duckdb'
ext=a.repo/'sirius-duckdb/build/release/extension/sirius/sirius.duckdb_extension'

CONFIG='''sirius:
  topology:
    num_gpus: {num_gpus}
  memory:
    gpu:
      usage_limit_fraction: 0.86
      reservation_limit_fraction: 0.9
      downgrade_trigger_fraction: 0.9
      downgrade_stop_fraction: 0.85
    host:
      {host_capacity}
      block_size: 1MiB
      pool_size: 128
      initial_number_pools: {initial_pools}
    disk:
      disk_id: 0
      capacity_bytes: {disk_gb}GB
      downgrade_root_dirs: {spill}
  executor:
    scan_manager:
      backend: sirius
      memory_prefetcher:
        enable: true
        num_threads: 4
      num_threads: 16
      rest_n_reactors: {reactors}
      max_readahead_scans: 48
      max_readahead_bytes: 4GB
      cache:
        mode: {cache_mode}
        eviction: lru
        eviction_threshold_fraction: {cache_fraction}
    pipeline:
      num_threads: 16
    downgrade:
      num_threads: 2
    task_creator:
      num_threads: 2
  operator_params:
    scan_task_batch_size: 1GB
    hash_partition_bytes: 4GB
    concat_batch_bytes: 2GB
    max_build_hash_table_bytes: 4GB
{bloom_config_line}
  compression:
    enable_spill_compression: {enabled}
  telemetry:
    enable_quent: true
    enable_nvtx: false
    exporter: ndjson
    output_directory: {telemetry}
    engine_name: {name}
'''
def lit(s):return "'"+str(s).replace("'","''")+"'"
def imds_credentials():
    base='http://169.254.169.254/latest/'
    req=urllib.request.Request(base+'api/token',method='PUT',headers={'X-aws-ec2-metadata-token-ttl-seconds':'60'})
    token=urllib.request.urlopen(req,timeout=5).read().decode()
    def get(path):
        req=urllib.request.Request(base+'meta-data/'+path,headers={'X-aws-ec2-metadata-token':token})
        return urllib.request.urlopen(req,timeout=5).read().decode()
    role=get('iam/security-credentials/').strip()
    return json.loads(get('iam/security-credentials/'+role))
def secret_sql():
    c=imds_credentials()
    a.credentials_expiration=c.get('Expiration')
    return ("SET sirius_log_level='warn';\n"+'CREATE OR REPLACE SECRET experiment_s3 (TYPE SIRIUS_S3, PROVIDER CONFIG, '
            f"KEY_ID {lit(c['AccessKeyId'])}, SECRET {lit(c['SecretAccessKey'])}, "
            f"SESSION_TOKEN {lit(c['Token'])}, REGION 'eu-west-2', URL_STYLE 'path', SCOPE {lit(a.source.rstrip('/')+'/')});\nSET sirius_log_level='info';\n"), [c['AccessKeyId'],c['SecretAccessKey'],c['Token']]


def checkpoint():
    if not a.checkpoint:return
    for attempt in range(4):
        done=subprocess.run(['aws','--region','eu-west-2','s3','sync','--only-show-errors',str(a.output),a.checkpoint])
        if done.returncode==0:return
        time.sleep(2**attempt)
    raise RuntimeError('Checkpoint failed after four attempts; local results preserved')

def suite(engine):
    root=a.output/f'sf{a.scale}'/engine
    if (root/'suite-status.json').exists():
        status=json.loads((root/'suite-status.json').read_text())
        if status['complete']:return
        raise RuntimeError(f'Incomplete prior mode suite: {root}')
    root.mkdir(parents=True,exist_ok=True)
    enabled=engine=='compression-on'
    spill=a.spill_root/f'{a.mode}-host{a.host_label}-cache{a.cache_gb}-gpu{a.num_gpus}-reactors{a.reactors}-sf{a.scale}-{engine}-{hashlib.sha256(str(a.output).encode()).hexdigest()[:10]}'
    if spill.exists():raise RuntimeError(f'Existing spill dir {spill}')
    spill.mkdir(parents=True)
    cfg=root/'config.yaml'
    if template:
        import copy,yaml
        rendered=copy.deepcopy(template);rendered['sirius']['memory']['disk']['downgrade_root_dirs']=str(spill)
        if rendered['sirius'].get('telemetry',{}).get('enable_quent'):rendered['sirius']['telemetry']['output_directory']=str(root/'telemetry')
        shutil.copyfile(a.config_template,root/'config-template.yaml');cfg.write_text(yaml.safe_dump(rendered,sort_keys=False))
    else:cfg.write_text(CONFIG.format(host_capacity=(f"capacity_bytes: {a.host_gib}GiB" if a.host_gib else f"capacity_fraction: {a.host_fraction}"),bloom_config_line=(f"    max_dynamic_filter_bloom_bytes_per_gpu: {a.bloom_cap_mib}MiB" if a.bloom_cap_mib else ""),bloom_cap_mib=a.bloom_cap_mib,spill=spill,telemetry=root/'telemetry',name=f'{a.mode}-host{a.host_label}-cache{a.cache_gb}-gpu{a.num_gpus}-reactors{a.reactors}-{engine}',enabled=str(enabled).lower(),initial_pools=a.initial_pools,host_fraction=a.host_fraction,num_gpus=a.num_gpus,reactors=a.reactors,disk_gb=a.disk_gb,cache_fraction=((20 if a.cache_gb==0 else a.cache_gb)*10**9/a.host_bytes),cache_mode=('none' if a.cache_gb==0 else 'sirius')))
    env=os.environ.copy();env['SIRIUS_CONFIG_FILE']=str(cfg)
    values=[]
    setup='.bail on\n.timer off\n'
    setup+=f'LOAD {lit(ext)};\nSET sirius_log_dir={lit(root/"log")};\nSET sirius_log_level=\'info\';\nSET spill_compression={str(enabled).lower()};\nSET threads={a.cpu_threads};\n'
    if a.source.startswith('s3://'):
        secret,values=secret_sql();setup+=secret
    for table in ['customer','lineitem','nation','orders','part','partsupp','region','supplier']:
        setup+=f'CREATE VIEW {table} AS SELECT * FROM read_parquet({lit(a.source.rstrip("/")+"/"+table+"/*.parquet")});\n'
    interfaces=[p for p in Path('/sys/class/net').iterdir() if (p/'device').exists()]
    def nic_rx():
        return {p.name:int((p/'statistics/rx_bytes').read_text()) for p in interfaces}
    def redact(text):
        for value in values:text=text.replace(value,'[redacted]')
        return text
    proc=subprocess.Popen(['/usr/bin/stdbuf','-oL','-eL',str(cli),'-unsigned','-batch','-csv','-header',':memory:'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,cwd=root,env=env)
    memory=dict(physical_mem_total_bytes=physical,host_budget_bytes=a.host_bytes,unmanaged_headroom_bytes=physical-a.host_bytes,sampling_interval_s=1,peak_process_rss_bytes=0,peak_process_hwm_bytes=0,min_system_mem_available_bytes=physical)
    stop_monitor=threading.Event()
    memory_log=(root/'memory-samples.jsonl').open('w')
    memory_lock=threading.Lock()
    def sample_memory_unlocked():
        try:
            status={x.split(':',1)[0]:int(x.split()[1])*1024 for x in Path(f'/proc/{proc.pid}/status').read_text().splitlines() if x.startswith(('VmRSS:','VmHWM:','VmPin:','VmLck:'))}
            available=int(next(x.split()[1] for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:')))*1024
            memory['peak_process_rss_bytes']=max(memory['peak_process_rss_bytes'],status.get('VmRSS',0))
            memory['peak_process_hwm_bytes']=max(memory['peak_process_hwm_bytes'],status.get('VmHWM',0))
            memory['min_system_mem_available_bytes']=min(memory['min_system_mem_available_bytes'],available)
            row=dict(timestamp_ns=time.time_ns(),pid=proc.pid,system_mem_available_bytes=available,**status)
            memory_log.write(json.dumps(row)+'\n');memory_log.flush()
            return row
        except (FileNotFoundError,ProcessLookupError):return None
    def sample_memory():
        with memory_lock:return sample_memory_unlocked()
    def monitor_memory():
        while not stop_monitor.is_set():sample_memory();stop_monitor.wait(1)
    monitor=threading.Thread(target=monitor_memory,daemon=True);monitor.start()
    pending=b'';log=(root/'process.log').open('a')
    def send(text):proc.stdin.write(text.encode());proc.stdin.flush()
    def until(marker,timeout):
        nonlocal pending
        deadline=time.monotonic()+timeout if timeout else None;lines=[]
        while True:
            if b'\n' in pending:
                raw,pending=pending.split(b'\n',1);line=redact(raw.decode(errors='replace'))
                log.write(line+'\n');log.flush();lines.append(line)
                if line.strip()==marker:return '\n'.join(lines)+'\n'
                continue
            remaining=deadline-time.monotonic() if deadline is not None else 5
            if remaining<=0:raise TimeoutError(f'Timed out waiting for {marker}')
            ready,_,_=select.select([proc.stdout],[],[],min(5,remaining))
            if ready:
                chunk=os.read(proc.stdout.fileno(),65536)
                if chunk:pending+=chunk
                else:raise RuntimeError(f'DuckDB exited before {marker}: '+redact('\n'.join(lines[-10:])))
    results=[]
    plan=schedule(a.mode,query_ids,a.warmups,a.repeats)
    (root/'schedule.json').write_text(json.dumps(plan,indent=2)+'\n')
    try:
        # Warm modes retain cache; cold additionally resets it before each timed query.
        send(setup+"CALL reset_sirius_cache();\n.print __ENGINE_READY__\n");until('__ENGINE_READY__',180)
        for phase,iteration,q in plan:
            out=root/phase/f'repeat{iteration}'/f'q{q}';out.mkdir(parents=True,exist_ok=True)
            (out/'query.sql').write_text(query_texts[f'q{q}']+';\n')
            before=f'.timer off\n.once {out/"schema.csv"}\nDESCRIBE '+query_texts[f'q{q}'].rstrip(';')+';\n'
            # Keep the initial credentials for this process: rotating the S3
            # config also rotates the ioctx and would invalidate its warm cache.
            label=f'{a.mode}-host{a.host_label}-cache{a.cache_gb}-gpu{a.num_gpus}-reactors{a.reactors}-sf{a.scale}-q{q}-{engine}-{phase}-r{iteration}'
            if a.mode=='cold' or (a.mode=='paired' and phase=='cold'):before+='CALL reset_sirius_cache();\n'
            before+=f'CALL sirius_set_query_label({lit(label)});\n'
            send(before+'.print __QUERY_READY__\n');until('__QUERY_READY__',120)
            start=time.time_ns();wall=time.monotonic();rx_before=nic_rx()
            send(f'.timer on\n.once {out/"result.csv"}\n'+query_texts[f'q{q}'].rstrip(';')+';\n.timer off\n.print __QUERY_DONE__\n')
            try:stdout=until('__QUERY_DONE__',a.timeout);status='ok'
            except (RuntimeError,TimeoutError) as e:stdout=str(e);status='timeout' if isinstance(e,TimeoutError) else 'query-process-error'
            end=time.time_ns();(out/'stdout.log').write_text(stdout)
            times=re.findall(r'Run Time\s*(?:\(s\))?:\s*real\s+([\d.]+)',stdout)
            seconds=float(times[-1]) if len(times)==1 else None
            if status=='ok' and (seconds is None or not (out/'result.csv').exists()):status='missing-timing-or-result'
            record=dict(bloom_cap_mib=a.bloom_cap_mib,scale=a.scale,mode=(phase if a.mode=='paired' else a.mode),host_bytes=a.host_bytes,host_label=a.host_label,host_fraction=a.host_fraction,num_gpus=a.num_gpus,reactors=a.reactors,cache_gb=a.cache_gb,cache_mode=('none' if a.cache_gb==0 else 'sirius'),phase=('measured' if a.mode=='paired' else phase),query=q,repeat=iteration,engine=engine,status=status,query_s=seconds,process_wall_s=time.monotonic()-wall,exit_code=None,window_start_ns=start,window_end_ns=end,source=a.source,cache=('reset_sirius_cache before each timed query; no table pins' if a.mode=='cold' or (a.mode=='paired' and phase=='cold') else ('cache disabled; no table pins' if a.cache_gb==0 else ('retained LRU cache from preceding cold sweep in same process; no additional warmup' if a.mode=='paired' else 'retained LRU cache; no table pins; initial reset only'))))
            rx_after=nic_rx();record['nic_rx_bytes']={name:rx_after[name]-value for name,value in rx_before.items()};record['nic_rx_gbps_observed']=sum(record['nic_rx_bytes'].values())*8/record['process_wall_s']/10**9
            record['nic_rx_note']='Whole-interface receive counters over the query wall window; excludes setup, includes any concurrent host traffic; not an S3-only byte counter.'
            record['memory_after_query']=sample_memory()
            (out/'record.json').write_text(json.dumps(record,indent=2)+'\n')
            results.append((out,record));print('QUERY_RESULT',json.dumps(record),flush=True)
            if status!='ok':break
        if proc.poll() is None:
            if len(results)==len(plan) and all(r['status']=='ok' for _,r in results):send('.quit\n');proc.stdin.close()
            else:proc.kill()
        proc.wait()
    finally:
        if proc.poll() is None:proc.kill();proc.wait()
        stop_monitor.set();monitor.join(timeout=5);memory_log.close()
        (root/'memory-summary.json').write_text(json.dumps(memory,indent=2)+'\n')
        log.close();shutil.rmtree(spill)
    logs='\n'.join(f.read_text(errors='replace') for f in (root/'log').glob('*') if f.is_file())
    completions=logs.count('Transparent GPU execution: query completed')
    for out,record in results:
        record['spill']=spill_summary(root/'telemetry',record['window_start_ns'],record['window_end_ns'])
        record['exit_code']=proc.returncode
        if completions<len(results) and record['status']=='ok':record['status']='gpu-route-unverified'
        (out/'record.json').write_text(json.dumps(record,indent=2)+'\n')
    complete=len(results)==len(plan) and all(r['status']=='ok' for _,r in results) and proc.returncode==0
    status=dict(mode=a.mode,host_bytes=a.host_bytes,host_label=a.host_label,host_fraction=a.host_fraction,num_gpus=a.num_gpus,reactors=a.reactors,cache_gb=a.cache_gb,engine=engine,queries_finished=len(results),expected=len(plan),gpu_completion_log_entries=completions,exit_code=proc.returncode,complete=complete,credential_expiration=getattr(a,'credentials_expiration',None))
    status['memory']=memory
    (root/'suite-status.json').write_text(json.dumps(status,indent=2)+'\n');checkpoint()
    if not complete:raise RuntimeError(f'Mode suite incomplete: {status}')

for engine in a.engines.split(','):
    if engine not in ['compression-off','compression-on']:p.error('Unknown engine')
    suite(engine)
checkpoint()
print('CACHE_MODE_COMPLETE',a.mode,flush=True)
