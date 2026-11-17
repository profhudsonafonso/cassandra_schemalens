"""Read-only post-benchmark SSTable metrics for the precise new Rev3 keyspaces.

Intentionally does not flush, compact, alter, truncate, or drop Cassandra data.
Call manual nodetool flush AFTER latency measurement and BEFORE this snapshot.
"""
from __future__ import annotations
import argparse
import csv
from datetime import datetime,timezone
import json
from pathlib import Path
import re
import shlex
import subprocess
from . import gate
from .benchmark import guard_keyspace

FIELDS={'SSTable count':'sstable_count','Space used (live)':'sstable_live_bytes',
 'Space used (total)':'sstable_total_bytes','Memtable data size':'memtable_bytes',
 'Number of partitions (estimate)':'estimated_partitions',
 'Compacted partition maximum bytes':'max_compacted_partition_bytes',
 'Compacted partition mean bytes':'mean_compacted_partition_bytes',
 'SSTable Compression Ratio':'compression_ratio'}


def number(val):
    pieces=val.strip().split()
    if not pieces:return None
    x=pieces[0].replace(',','')
    if x.lower() in ('nan','n/a','null','none','?'):return None
    try:return int(x) if re.fullmatch(r'-?\d+',x) else float(x)
    except ValueError:return None


def parse_stats(content,keyspace):
    current_ks=None;current_table=None;current={};records=[]
    def emit():
        if current_table and current_ks==keyspace:records.append(dict(current))
    for line in content.splitlines():
        s=line.strip()
        if s.startswith('Keyspace :'):
            emit();current_ks=s.partition(':')[2].strip();current_table=None;current={}
        elif s.startswith('Table:'):
            emit();current_table=s.partition(':')[2].strip();current={'keyspace':keyspace,'table':current_table}
        elif current_table and ':' in s:
            key,value=s.split(':',1)
            if key in FIELDS:current[FIELDS[key]]=number(value)
    emit()
    return records


def get_benchmark(workspace):
    exp,protocol,selected=gate.verify(workspace)
    mans=sorted((exp/'outputs/rev3_independent_confirmatory').glob('batch_*/manifest.json'))
    if len(mans)!=1:raise ValueError('Exactly one complete Rev3 benchmark is required')
    m=gate.read(mans[0])
    if m['status']!='REV3_INDEPENDENT_JAVA_CONFIRMATION_COMPLETE':
        raise ValueError('No complete benchmark for storage collection')
    if gate.digest(Path(workspace)/gate.PROTO)!=m['premeasurement_protocol_sha256']:
        raise ValueError('Benchmark and premeasurement protocol differ')
    for ks in m['keyspaces'].values():guard_keyspace(ks)
    return exp,protocol,m,mans[0]


def print_flush(workspace,container):
    _,_,manifest,_=get_benchmark(workspace)
    if not re.fullmatch(r'[a-zA-Z0-9_-]+',container):raise ValueError('Unsafe container name')
    print('Run ONLY AFTER the benchmark; this tool only prints these commands:')
    for ks in sorted(set(manifest['keyspaces'].values())):
        print('docker exec '+container+' nodetool flush '+ks)


def collect(workspace,nodetool_cmd,out):
    exp,proto,manifest,benchpath=get_benchmark(workspace)
    out=Path(out).resolve()
    if out.exists():raise FileExistsError('Refusing to overwrite storage snapshot '+str(out))
    if not out.is_relative_to(exp/'outputs'):
        raise ValueError('Store postbenchmark physical evidence inside NEW experiment outputs only')
    command=shlex.split(nodetool_cmd)
    if not command or command[-1]!='nodetool':
        raise ValueError('Expected command ending in nodetool (example: docker exec CONTAINER nodetool)')
    entries=[];raw={}
    for ks in sorted(set(manifest['keyspaces'].values())):
        cp=subprocess.run([*command,'tablestats',ks],text=True,capture_output=True,timeout=180)
        if cp.returncode:
            raise RuntimeError('nodetool tablestats failed for '+ks+': '+cp.stderr[-500:])
        rows=parse_stats(cp.stdout,ks)
        if not rows:raise ValueError('No tables parsed in nodetool output for '+ks)
        entries.extend(rows);raw[ks]=cp.stdout
    out.mkdir(parents=True,exist_ok=False)
    for ks,content in raw.items():
        (out/('nodetool_'+ks+'.txt')).write_text(content,encoding='utf8')
    cols=['keyspace','table',*FIELDS.values()]
    with (out/'table_storage.csv').open('x',newline='',encoding='utf8') as fh:
        w=csv.DictWriter(fh,fieldnames=cols,extrasaction='ignore');w.writeheader();w.writerows(entries)
    summary=[]
    for variant,ks in manifest['keyspaces'].items():
        tables=[x for x in entries if x['keyspace']==ks]
        def total(field):
            vals=[x.get(field) for x in tables]
            return sum(vals) if vals and all(isinstance(x,(int,float)) for x in vals) else None
        summary.append({'variant':variant,'keyspace':ks,'tables':len(tables),
            'sstable_count':total('sstable_count'),
            'sstable_live_bytes':total('sstable_live_bytes'),
            'sstable_total_bytes':total('sstable_total_bytes'),
            'memtable_bytes':total('memtable_bytes'),
            'estimated_partitions':total('estimated_partitions')})
    with (out/'deployment_storage.csv').open('x',newline='',encoding='utf8') as fh:
        w=csv.DictWriter(fh,fieldnames=list(summary[0]));w.writeheader();w.writerows(summary)
    gate.dump(out/'manifest.json',{'status':'READ_ONLY_REV3_STORAGE_SNAPSHOT',
        'timestamp_utc':datetime.now(timezone.utc).isoformat(),
        'benchmark_manifest_sha256':gate.digest(benchpath),
        'rev3_selection_sha256':proto['rev3_selection_sha256'],
        'keys_are_strictly_from_benchmark_manifest':True,
        'no_automatic_flush':True,
        'disk_measurements_interpretation':'SSTables current at collection; compression and compaction dependent',
        'snapshots_sha256':{name:gate.digest(out/('nodetool_'+name+'.txt')) for name in raw}})
    print('SNAPSHOT:',out)
    print('SSTables:',[(r['variant'],r['sstable_count'],r['sstable_live_bytes']) for r in summary])


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('action',choices=['show-flush','collect'])
    ap.add_argument('--workspace',required=True,type=Path)
    ap.add_argument('--container',default='schemalens_iot_cassandra_pipeline-cassandra-1')
    ap.add_argument('--nodetool-cmd',default='')
    ap.add_argument('--out',type=Path)
    a=ap.parse_args()
    if a.action=='show-flush':print_flush(a.workspace,a.container)
    else:
        if not a.nodetool_cmd or not a.out:ap.error('--nodetool-cmd and --out required')
        collect(a.workspace,a.nodetool_cmd,a.out)

if __name__=='__main__':main()
