"""V4-a experimental candidates + E4: independent of V3 Stage4-8 selection.

Only diagnostic keyspaces may be created/dropped; old HBench is verified and copied
as a read-only *specification*. Cassandra measurements require explicit live mode.
"""
from __future__ import annotations
import copy
import csv
import hashlib
import json
import math
import random
import re
import statistics
import time
from collections import defaultdict
from pathlib import Path

FROZEN_SHA='2c4f46c70fc272046b42d1c7a8e6fc7c20c4ca9d8b63cef707ff7f89fa3fa6eb'
DAY=86_400_000
RESET='RESET_V4_DIAGNOSTIC_KEYSPACES_ONLY'
BASE='BASELINE_ORIGINAL'
BASE_OPT='BASELINE_Q8_OPTIMIZED'
DESIGNS={
    'V4_TS2U': ('ts2_iot_wide_unbucketed', 'TS2',False),
    'V4_TS2U_TS4': ('ts2_iot_wide_unbucketed','TS2',True),
    'V4_TS2D': ('ts2_iot_wide','TS2',False),
    'V4_TS2D_TS4': ('ts2_iot_wide','TS2',True),
}


def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def save_json(p,obj):
    p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(obj,ensure_ascii=False,indent=2,sort_keys=True,allow_nan=False)+'\n',encoding='utf8')

def save_csv(p, rows, fields=None):
    p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
    if not rows:raise ValueError('Empty CSV: '+str(p))
    with p.open('w',newline='',encoding='utf8') as f:
        w=csv.DictWriter(f,fieldnames=fields or list(rows[0]),extrasaction='ignore')
        w.writeheader();w.writerows(rows)

def pctl(xs,p):
    if not xs:raise ValueError('Empty measured values')
    vals=sorted(xs)
    return vals[max(0,min(len(vals)-1,math.ceil(len(vals)*p/100)-1))]

def snapshot(exp):
    """Hash old V3 freeze, all selection inputs, original measurement manifest."""
    from iot_pipeline.shared import ROOT, read, sha
    from iot_pipeline.stage9 import confirm_frozen
    if Path(ROOT).resolve()!=Path(exp).resolve():raise ValueError('Wrong imported iot_pipeline; cannot trust experiment')
    confirm_frozen()
    mani=read(exp/'outputs/stage8/32_frozen_manifest.json')
    freeze=exp/'outputs/stage8/28_hbench_frozen.json'
    if digest(freeze)!=FROZEN_SHA:raise ValueError('This tool only accepts the verified Native V3 frozen HBench')
    paths=set(mani['inputs_sha256'])|{'outputs/stage8/28_hbench_frozen.json','outputs/stage8/30_hbench_summary.csv',
              'outputs/stage8/32_frozen_manifest.json'}
    for rel in ('outputs/stage9_native_reference_v3/37_manifest.json','outputs/stage9_hbench_native_v3/37_manifest.json'):
        if (exp/rel).exists():paths.add(rel)
    return {s:digest(exp/s) for s in sorted(paths)}


def source(exp):
    from iot_pipeline.native_reference_v3 import prepare
    from iot_pipeline.shared import inputs,sha
    selected,rows,queries,sensors,wide_count,passed,meta,hashes=prepare()
    if len(selected)!=4 or len(rows)!=1800 or passed!=12 or wide_count!=900:
        raise ValueError('Not the 1800-cell Native Java V3 experiment')
    return selected,rows,queries,sensors,meta,hashes


def create_days(original,days):
    if days not in (1,7,30):raise ValueError('V4 preregistered only for scales 1, 7 and 30 days')
    if len({r['timestamp']//DAY for r in original})!=1:raise ValueError('Original spans > 1 day; use a new design protocol')
    output=[];seen=set()
    for i in range(days):
        for r in original:
            q=dict(r);q['timestamp']+=i*DAY
            key=(q['device_id'],q['sensor_name'],q['timestamp'])
            if key in seen:raise ValueError('Replicated metric key collision')
            output.append(q);seen.add(key)
    return output


def generated_queries(queries,rows,extended):
    out=copy.deepcopy(queries)
    for q in out:q['_label']='NATIVE_FIXED_'+q['query_id']
    if extended:
        lower=min(r['timestamp'] for r in rows)
        upper=max(r['timestamp'] for r in rows)+1
        for qid in ('Q2','Q7','Q9','Q11','Q12'):
            q=copy.deepcopy(next(q for q in queries if q['query_id']==qid))
            q['start_ms']=lower;q['end_ms_exclusive']=upper
            if qid=='Q12':
                for ch in q['set_children']:
                    ch['start_ms']=lower;ch['end_ms_exclusive']=upper
            q['_label']='EXTENDED_'+qid+'_FULL_SPAN'
            q['_synthetic_parameter_extension']=True
            out.append(q)
    return out


def plans(queries):
    """Pre-registered four V4 candidates, no historical latencies in construction."""
    out=[]
    for label,(table,family,latest) in DESIGNS.items():
        out.append({'deployment_id':label,'canonical_source':table,
                    'families':['TS2']+(['TS4'] if latest else []),
                    'query_routes':{q['query_id']:{'family':'TS4' if latest and q['query_id']=='Q8' else 'TS2',
                                                    'candidate_id':'ts4_iot_latest' if latest and q['query_id']=='Q8' else table,
                                                    'reason':'cross-query semantic reuse; verified by oracle'} for q in queries},
                    'stage':'V4_EXPLORATORY_NOT_AUTOMATIC_HBENCH',
                    'candidate_activation_changed':False,
                    'original_V3_frozen_routes_unchanged':True})
    return out


def offline_cross_coverage(rows,queries,sensors):
    """Logical validation of the 4 new candidate routes; no DB or performance oracle."""
    from iot_pipeline.native_reference_v3 import source_oracle,physical_to_raw
    from iot_pipeline.reference_baseline import build_wide_records
    from iot_pipeline.offline import compare,physical_state
    oracle=source_oracle();w=build_wide_records(rows,sensors)
    latest=physical_state(rows,20_000)['TS4']
    checks=[]
    for d in plans(queries):
        for q in queries:
            if q['query_id']=='Q8' and 'TS4' in d['families']:
                records=[]
                for device in q['devices']:
                    for sensor in q['sensors']:
                        if (device,sensor) in latest:
                            t,v=latest[(device,sensor)]
                            records.append({'device_id':device,'sensor_name':sensor,'timestamp':t,'value_double':v})
            else:
                records=physical_to_raw(w,sensors,q)
            match=compare(oracle(records,q),oracle(rows,q))
            checks.append({'deployment':d['deployment_id'],'query':q['_label'],'correct':match})
            if not match:raise AssertionError('V4 offline cross coverage failed: '+str(checks[-1]))
    return checks


def safe_keyspace(scale,label):
    slugs={BASE:'ref',BASE_OPT:'ref',**{x:x.lower().removeprefix('v4_') for x in DESIGNS}}
    if label.startswith('H_') and re.fullmatch(r'H_[0-9a-f]{12}',label):slug='h_'+label[2:]
    elif label in slugs:slug=slugs[label]
    else:raise ValueError('Unsafe deployment ID')
    return f'sl_diag_v4_d{scale:02d}_{slug}'


def assert_safe_reset(name,confirm):
    if confirm!=RESET:raise ValueError('Refusing destructive V4 operation without exact confirmation')
    if not re.fullmatch(r'sl_diag_v4_d(?:01|07|30)_(?:ref|ts2u|ts2u_ts4|ts2d|ts2d_ts4|h_[0-9a-f]{12})',name):
        raise ValueError('Refusing to reset keyspace outside diagnostic V4 namespace: '+name)


def init_keyspace(session,keyspace,token):
    assert_safe_reset(keyspace,token)
    session.execute('DROP KEYSPACE IF EXISTS '+keyspace)
    session.execute("CREATE KEYSPACE "+keyspace+" WITH replication = {'class':'SimpleStrategy','replication_factor':1} AND durable_writes=true")
    session.set_keyspace(keyspace)


def metric_columns(sensors):
    from iot_pipeline.reference_baseline import columns_for
    return columns_for(sensors)


def read_wide(session,q,sensors,table,days,*,bucket=False,optimized_q8=False,page_size=16):
    """Shared decoder, exact Q12 child requests, correct global Q8 last non-null.

    For optimized Q8, Cassandra pages at page_size. Python stops iteration as
    soon as every projected sensor has a valid latest value. This is NOT a
    server-only timing; prefetched page may contain more than iterated rows.
    """
    from iot_pipeline.native_reference_v3 import requests_for
    from iot_pipeline.offline import date_of,day_indices
    allowed={'iot_reference_table','ts2_iot_wide_unbucketed','ts2_iot_wide'}
    if table not in allowed:raise ValueError('Unsafe physical table name')
    columns=set(metric_columns(sensors));out={};exec_calls=0;iterated=0
    if q['query_id']=='Q8' and optimized_q8:
        from cassandra.query import SimpleStatement
        for d in q['devices']:
            projection=tuple(q['sensors'])
            missing=set(projection)
            fields=['m_'+s for s in projection]
            if not set(fields)<=columns:raise ValueError('Unexpected metric in Q8')
            daylist=sorted(days,reverse=True) if bucket else [None]
            for day in daylist:
                cql=f'SELECT timestamp_ms,{",".join(fields)} FROM {table} WHERE device_id=%s'
                params=(d,)
                if bucket:cql+=' AND bucket_day=%s';params+=(date_of(day*DAY),)
                # Key schema is DESC; never force arbitrary ORDER BY with unsupported CQL.
                res=session.execute(SimpleStatement(cql,fetch_size=page_size),params)
                exec_calls+=1
                for r in res:
                    iterated+=1
                    for col,s in zip(fields,projection):
                        if s in missing:
                            v=getattr(r,col,None)
                            if v is not None:
                                out[(d,s,int(r.timestamp_ms))]={'device_id':d,'sensor_name':s,
                                    'timestamp':int(r.timestamp_ms),'value_double':float(v)}
                                missing.remove(s)
                    if not missing:break
                if not missing:break
        return list(out.values()),exec_calls,iterated

    for d,projection,start,end in requests_for(q):
        fields=['m_'+s for s in projection]
        if not set(fields)<=columns:raise ValueError('Unexpected projected metric')
        partitions=(list(day_indices(start,end)) if start is not None else sorted(days)) if bucket else [None]
        for day in partitions:
            cql=f'SELECT timestamp_ms,{",".join(fields)} FROM {table} WHERE device_id=%s'
            params=(d,)
            if bucket:cql+=' AND bucket_day=%s';params+=(date_of(day*DAY),)
            if start is not None:cql+=' AND timestamp_ms >= %s AND timestamp_ms < %s';params+=(start,end)
            exec_calls+=1
            for r in session.execute(cql,params):
                iterated+=1
                t=int(r.timestamp_ms)
                for col,s in zip(fields,projection):
                    value=getattr(r,col,None)
                    if value is not None:
                        out[(d,s,t)]={'device_id':d,'sensor_name':s,'timestamp':t,'value_double':float(value)}
    return list(out.values()),exec_calls,iterated


def query_result(session,q,sensors,label,days,frozen_by_id,page_size):
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline import stage9
    oracle=source_oracle()
    if label.startswith('H_'):
        h=frozen_by_id[label]
        got=stage9.execute_query(session,q,h['query_routes'][q['query_id']],sensors)
        return got,{'requests':None,'iterated_rows':None,'route':h['query_routes'][q['query_id']]['family']}
    if label in (BASE,BASE_OPT):
        records,calls,seen=read_wide(session,q,sensors,'iot_reference_table',days,
                       optimized_q8=(label==BASE_OPT),page_size=page_size)
        return oracle(records,q),{'requests':calls,'iterated_rows':seen,'route':'REFERENCE_OPT_Q8' if label==BASE_OPT and q['query_id']=='Q8' else 'REFERENCE_FULL'}
    if label not in DESIGNS:raise ValueError('Unregistered V4 candidate')
    table,_,latest=DESIGNS[label]
    if latest and q['query_id']=='Q8':
        records=[];calls=0;seen=0
        for device in q['devices']:
            for sensor in q['sensors']:
                calls+=1
                for r in session.execute('SELECT latest_timestamp_ms,value_double FROM ts4_iot_latest WHERE device_id=%s AND sensor_name=%s',(device,sensor)):
                    seen+=1
                    if r.latest_timestamp_ms is not None:
                        records.append({'device_id':device,'sensor_name':sensor,'timestamp':int(r.latest_timestamp_ms),'value_double':float(r.value_double)})
        return oracle(records,q),{'requests':calls,'iterated_rows':seen,'route':'TS4'}
    records,calls,seen=read_wide(session,q,sensors,table,days,bucket=(table=='ts2_iot_wide'),
                              optimized_q8=q['query_id']=='Q8',page_size=page_size)
    return oracle(records,q),{'requests':calls,'iterated_rows':seen,'route':'TS2_CANONICAL'}


def load_wide(session,rows,sensors,table,bucket):
    from iot_pipeline.reference_baseline import build_wide_records
    from iot_pipeline.offline import date_of
    wide=build_wide_records(rows,sensors)
    cols=['device_id']+(['bucket_day'] if bucket else [])+['timestamp_ms']+metric_columns(sensors)
    sql=f'INSERT INTO {table} ({",".join(cols)}) VALUES ({",".join("?" for _ in cols)})'
    prepared=session.prepare(sql)
    for d,t,*values in wide:
        data=(d,date_of(t),t,*values) if bucket else (d,t,*values)
        session.execute(prepared,data)
    return len(wide)


def load_design(session,label,rows,sensors,specs,policy):
    """Self-contained canonical TS2 materialization; TS0 is NOT materialized."""
    from iot_pipeline.offline import physical_state
    if label in (BASE,BASE_OPT):
        from iot_pipeline.reference_baseline import table_ddl
        session.execute(table_ddl(sensors))
        return load_wide(session,rows,sensors,'iot_reference_table',False)
    table,_,latest=DESIGNS[label]
    matching=[s for s in specs if s['candidate_id']==table]
    if len(matching)!=1:raise ValueError('V4 candidate absent from frozen TS2 specifications')
    ddl=matching[0]['ddl'];session.execute(ddl)
    count=load_wide(session,rows,sensors,table,table=='ts2_iot_wide')
    if latest:
        spec4=[x for x in specs if x['candidate_id']=='ts4_iot_latest']
        if len(spec4)!=1:raise ValueError('TS4 missing')
        session.execute(spec4[0]['ddl'])
        state=physical_state(rows,policy['rollup_window_ms'])
        prepared=session.prepare('INSERT INTO ts4_iot_latest (device_id,sensor_name,latest_timestamp_ms,value_double) VALUES (?,?,?,?)')
        for (d,s),(t,v) in state['TS4'].items():
            session.execute(prepared,(d,s,t,v));count+=1
    return count


def aggregate(rows,queries):
    """Group per variant, native-only, with and without Q8. Equal query weights, NOT workload p95."""
    result=[]
    for variant in sorted({r['variant'] for r in rows}):
        pool=[r for r in rows if r['variant']==variant]
        for typ in ('NATIVE_ALL_12','NATIVE_EX_Q8','EXTENDED_ONLY'):
            subset=[r['p95_ms'] for r in pool if (r['query_label'].startswith('NATIVE_FIXED_') if typ!='EXTENDED_ONLY' else r['query_label'].startswith('EXTENDED_')) and (typ!='NATIVE_EX_Q8' or r['query_label']!='NATIVE_FIXED_Q8')]
            if subset:result.append({'variant':variant,'metric':typ,'mean_of_query_p95_ms':round(statistics.mean(subset),6),'query_count':len(subset)})
    return result
