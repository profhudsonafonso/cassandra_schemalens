"""Stable paths, serialization and guards for all research stages."""
from __future__ import annotations
import csv
import hashlib
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
A1=ROOT/'phase_a1'
OUT=ROOT/'outputs'
POLICY=ROOT/'config/pipeline_policy.json'

def read(path): return json.loads(Path(path).read_text(encoding='utf-8'))
def write(path,obj):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def csv_write(path,rows,fields):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',newline='',encoding='utf-8') as f:
        wr=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');wr.writeheader();wr.writerows(rows)
def path4(stage,stem):return OUT/f'stage{stage}'/stem

def require_policy():
    p=read(POLICY)
    assert p['scope']=='SCHEMALENS_TS_ONLY_IOT_CASSANDRA'
    assert p['benchmark_metrics_allowed_for_selection'] is False
    assert p['raw_reference_required'] is True
    assert set(p['supported_physical_families']) <= set(p['families'])
    assert all(x.startswith('TS') for x in p['families'])
    return p

def inputs():
    act=A1/'outputs/activation_ts_only.json'; queries=A1/'outputs/query_instances_pilot.json'
    native=A1/'outputs/normalized_double_readings.csv'; specs=A1/'outputs/cassandra_ddl_blueprints.json'
    for x in [act,queries,native,specs]:
        if not x.exists():raise FileNotFoundError(f'Missing A1 input {x}; execute phase_a1/run_a1.sh first')
    return {'activation':act,'queries':queries,'native':native,'blueprints':specs}

def read_rows(path):
    with Path(path).open(encoding='utf-8',newline='') as f:
        rows=list(csv.DictReader(f))
    return [{'device_id':r['device_id'],'sensor_name':r['sensor_name'], 'timestamp':int(r['timestamp']), 'value_double':float(r['value_double'])} for r in rows]

def manifest(stage,upstream,status,extra=None):
    return {'stage':stage,'status':status,'scope':'TS_ONLY_IOT_CASSANDRA','C0_C9_used':False,
            'benchmark_measured':False,'inputs_sha256':{str(Path(v).relative_to(ROOT)):sha(v) for v in upstream.values()},
            'limitations':['QUERY_SEED not replayed in A1 fixture; official native parameter-object capture utility is supplied separately, not wired into Stage8',
                           'Live Cassandra results unavailable until user performs Stage9'],**(extra or {})}
