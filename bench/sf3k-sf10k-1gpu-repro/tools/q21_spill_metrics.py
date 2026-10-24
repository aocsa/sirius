#!/usr/bin/env python3
"""Count completed downgrade events from Quent, using post-conversion representation sizes."""
from collections import defaultdict
import json
from pathlib import Path
import re

def summarize(telemetry,start_ns=None,end_ns=None):
    telemetry=Path(telemetry)
    spaces={}
    memory_files=[p for p in telemetry.rglob('*.ndjson') if p.parent.name=='MemorySpace']
    batch_files=[p for p in telemetry.rglob('*.ndjson') if p.parent.name=='DataBatch']
    if not memory_files or not batch_files:
        return {'available':False,'reason':'no MemorySpace/DataBatch event files'}
    for p in memory_files:
        for line in p.open():
            event=json.loads(line)
            declaration=event['data'].get('Declaration')
            if declaration:
                match=re.search(r'tier=(GPU|HOST|DISK)',declaration['label'])
                if match:spaces[event['id']]=match.group(1)
    if not spaces:
        return {'available':False,'reason':'MemorySpace files exist but contain no tier declarations; buffered/incomplete telemetry cannot establish zero spilling'}
    batches=defaultdict(list)
    for p in batch_files:
        for line in p.open():
            event=json.loads(line)
            if start_ns is not None and not (start_ns <= event['timestamp'] <= end_ns):continue
            payload=event['data']
            if 'InTransit' in payload:
                x=payload['InTransit']
                batches[event['id']].append((x['seq'],'transit',x['source_memory']['target'],x['dest_memory']['target'],x['source_memory']['data']['bytes']))
            elif 'Stationary' in payload:
                x=payload['Stationary']
                batches[event['id']].append((x['seq'],'stationary',x['memory']['target'],x['memory']['data']['bytes']))
    counts={t:{'events':0,'preconversion_bytes':0,'destination_representation_bytes':0,'unique_batches':set()} for t in ['HOST','DISK']}
    for batch,events in batches.items():
        pending=None
        for event in sorted(events,key=lambda x:x[0]):
            if event[1]=='transit':pending=event
            elif pending:
                _,_,src,dest,before=pending
                tier=spaces.get(dest)
                source_tier=spaces.get(src)
                if event[2]==dest and tier in counts and (source_tier=='GPU' or (source_tier=='HOST' and tier=='DISK')):
                    row=counts[tier]
                    row['events']+=1
                    row['preconversion_bytes']+=before
                    row['destination_representation_bytes']+=event[3]
                    row['unique_batches'].add(batch)
                pending=None
    for row in counts.values():row['unique_batches']=len(row['unique_batches'])
    return {'available':True,'tiers':counts,
            'semantics':'Successful downgrade events; repeat movements count again. Destination bytes are representation sizes after conversion, not filesystem I/O counters. Preconversion bytes can already be compressed for HOST to DISK.'}

if __name__=='__main__':
    import sys
    print(json.dumps(summarize(sys.argv[1]),indent=2))
