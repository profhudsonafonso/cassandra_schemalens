"""Post-selection *diagnostic* Cassandra translation of IoT Benchmark TABLE schema.

This is NOT an official IoT Benchmark Cassandra schema or benchmark client.
The official IoTDB table model uses a TIME column, device_id TAG, and
measurement FIELD columns. Cassandra cannot implement those IoTDB types directly;
we translate to one wide row per (device_id, timestamp_ms).

The baseline is independent of the SchemaLens TS families and is never allowed
in Stage 4-8 selection or the HBench catalogue.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import re
import statistics
import sys
import time

from .shared import ROOT,A1,inputs,read,read_rows,sha,write,csv_write,path4
from .offline import compare
from .stage9 import confirm_frozen,_pctl

KEYSPACE='sl_iot_ref_table'
TABLE='iot_reference_table'
LABEL='REF_IOT_TABLE_CASSANDRA'
NAME=re.compile(r'^[A-Za-z_][A-Za-z_0-9]*$')


def columns_for(sensors: list[str]) -> list[str]:
    if not sensors or len(set(sensors))!=len(sensors):
        raise ValueError('At least one unique sensor required')
    result=['m_'+s for s in sensors]
    if any(not NAME.fullmatch(n) for n in result):
        raise ValueError('Unsafe sensor name')
    return result


def table_ddl(sensors: list[str]) -> str:
    """Device/time wide shape, without TS2's extra per-day partition key."""
    cols=columns_for(sensors)
    return (f'CREATE TABLE IF NOT EXISTS {TABLE} '
            '(device_id text, timestamp_ms bigint, '
            +', '.join(f'{col} double' for col in cols)
            + ', PRIMARY KEY ((device_id), timestamp_ms)) '
              'WITH CLUSTERING ORDER BY (timestamp_ms DESC);')


def build_wide_records(rows: list[dict], sensors: list[str]) -> list[tuple]:
    """Build one row per device/timestamp. Ignore absent metric cells (NULL)."""
    columns_for(sensors)
    idx={s:i for i,s in enumerate(sensors)}
    by_key={}
    for item in rows:
        s=item['sensor_name']
        if s not in idx:raise ValueError(f'Sensor missing from schema: {s}')
        key=(item['device_id'],int(item['timestamp']))
        vals=by_key.setdefault(key,[None]*len(sensors))
        if vals[idx[s]] is not None:raise ValueError(f'Duplicate metric reading: {key} {s}')
        val=float(item['value_double'])
        if not math.isfinite(val):raise ValueError('Non-finite sensor reading')
        vals[idx[s]]=val
    return [(*k,*by_key[k]) for k in sorted(by_key)]


def _physical_to_raw(physical_records: list[tuple], sensors: list[str], q: dict) -> list[dict]:
    """In-memory reference query: filter at the device/timestamp keys."""
    selected=set(q['devices']);wanted=set(q['sensors'])
    start=q['point_ms'] if q['query_id']=='Q1' else q['start_ms']
    end=start+1 if q['query_id']=='Q1' else q['end_ms_exclusive']
    unrestricted=q['query_id']=='Q5'
    result=[]
    for d,t,*vals in physical_records:
        if d not in selected or (not unrestricted and not start<=t<end):continue
        for s,v in zip(sensors,vals):
            if s in wanted and v is not None:
                result.append({'device_id':d,'sensor_name':s,'timestamp':t,'value_double':v})
    return result


def fetch_query_records(session,q:dict,sensors:list[str]) -> list[dict]:
    """Cassandra partition-key read for a device; no ALLOW FILTERING."""
    allcols=columns_for(sensors)
    selected=[(s,'m_'+s) for s in q['sensors']]
    if any(col not in allcols for _,col in selected):
        raise ValueError('Unknown selected sensor')
    start=q['point_ms'] if q['query_id']=='Q1' else q['start_ms']
    end=start+1 if q['query_id']=='Q1' else q['end_ms_exclusive']
    unrestricted=q['query_id']=='Q5'
    sql=(f'SELECT timestamp_ms,{",".join(col for _,col in selected)} FROM {TABLE} '
         'WHERE device_id=%s')
    if not unrestricted:sql+=' AND timestamp_ms >= %s AND timestamp_ms < %s'
    result=[]
    for device in q['devices']:
        args=(device,) if unrestricted else (device,start,end)
        for row in session.execute(sql,args):
            for sensor,col in selected:
                v=getattr(row,col,None)
                if v is not None:
                    result.append({'device_id':device,'sensor_name':sensor,
                                   'timestamp':int(row.timestamp_ms),'value_double':float(v)})
    return result


def ensure_pilot_invariants(rows,queries,sensors):
    if {q['query_id'] for q in queries}!={f'Q{i}' for i in range(1,13)}:
        raise ValueError('Pilot needs all 12 query classes')
    if any(x['sensor_name'] not in sensors for x in rows):raise ValueError('Bad sensor')
    if any(set(q['sensors'])-set(sensors) for q in queries):
        raise ValueError('Query references unknown sensor')
    if any(q.get('parameter_origin','').find('illustrative')==-1 for q in queries):
        raise ValueError('This baseline is only verified for the frozen illustrative A1 contract')


def offline_checks(rows,queries,sensors):
    sys.path.insert(0,str(A1))
    from core.bridge import _result
    wide=build_wide_records(rows,sensors)
    for q in queries:
        result=_result(_physical_to_raw(wide,sensors,q),q)
        expected=_result(rows,q)
        if not compare(result,expected):raise AssertionError(f'Baseline mismatch: {q["query_id"]}')
    return len(wide),len(queries)


def run_live(args):
    # Baseline is diagnostic and added after, without rerunning or refitting Stage8.
    confirm_frozen()
    if args.confirm_reset!='RESET_SL_IOT_REF_TABLE':
        raise ValueError('Explicit baseline keyspace reset confirmation required')
    if args.rounds<5 or args.warmup<0:raise ValueError('Require >=5 rounds and >=0 warmup')
    if args.output.exists() and any(args.output.iterdir()):
        raise ValueError('Refusing to overwrite a nonempty output folder')
    rows=read_rows(inputs()['native']);qs=read(inputs()['queries'])
    sensors=sorted({r['sensor_name'] for r in rows})
    ensure_pilot_invariants(rows,qs,sensors)
    physical_count,checked=offline_checks(rows,qs,sensors)
    wide=build_wide_records(rows,sensors)
    sys.path.insert(0,str(A1))
    from core.bridge import _result
    from cassandra.cluster import Cluster
    cluster=Cluster([args.host],port=args.port)
    latencies=[];summaries=[];load=None;ok=False
    try:
        session=cluster.connect()
        # This exact, fixed experimental keyspace is the *only* possible DROP target.
        session.execute(f'DROP KEYSPACE IF EXISTS {KEYSPACE}')
        session.execute(f"CREATE KEYSPACE {KEYSPACE} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}} AND durable_writes=true")
        session.set_keyspace(KEYSPACE)
        session.execute(table_ddl(sensors))
        insert_cols=['device_id','timestamp_ms',*columns_for(sensors)]
        statement=session.prepare(f'INSERT INTO {TABLE} ({",".join(insert_cols)}) VALUES ({",".join(["?"]*len(insert_cols))})')
        started=time.perf_counter_ns()
        for physical_row in wide:session.execute(statement,physical_row)
        elapsed=(time.perf_counter_ns()-started)/1e9
        load={'deployment_id':LABEL,'logical_metric_cells':len(rows),
              'physical_rows_written':len(wide),'load_elapsed_seconds':elapsed,
              'logical_points_per_second':len(rows)/max(elapsed,1e-9),
              'scope':'Cassandra-translated IoTDB table-like reference; sequential Python loader, not official throughput'}
        # Query result correctness first, before any latency collection.
        for q in qs:
            got=_result(fetch_query_records(session,q,sensors),q)
            if not compare(got,_result(rows,q)):
                raise AssertionError(f'Live reference schema correctness failed for {q["query_id"]}')
        for q in qs:
            for _ in range(args.warmup):
                _result(fetch_query_records(session,q,sensors),q)
            observed=[]
            for _ in range(args.rounds):
                start=time.perf_counter_ns()
                _result(fetch_query_records(session,q,sensors),q)
                observed.append((time.perf_counter_ns()-start)/1e6)
            summaries.append({'deployment_id':LABEL,'query_id':q['query_id'],
                              'family':'REFERENCE_TABLE','mean_ms':statistics.mean(observed),
                              'median_ms':statistics.median(observed),'p95_ms':_pctl(observed,95),
                              'p99_ms':_pctl(observed,99),
                              'successful_operations':len(observed),'correctness':'PASS'})
            latencies.extend({'deployment_id':LABEL,'query_id':q['query_id'],
                              'rep':i+1,'latency_ms':v} for i,v in enumerate(observed))
        ok=True
    finally:
        cluster.shutdown()
    if ok:
        out=args.output;out.mkdir(parents=True,exist_ok=True)
        csv_write(out/'34_load_metrics.csv',[load],['deployment_id','logical_metric_cells','physical_rows_written',
                  'load_elapsed_seconds','logical_points_per_second','scope'])
        csv_write(out/'35_query_p95.csv',summaries,['deployment_id','query_id','family','mean_ms',
                  'median_ms','p95_ms','p99_ms','successful_operations','correctness'])
        csv_write(out/'36_raw_latencies.csv',latencies,['deployment_id','query_id','rep','latency_ms'])
        write(out/'37_manifest.json',{'status':'REFERENCE_BASELINE_POST_HBENCH',
            'deployment_id':LABEL,'official_iot_benchmark_score':False,
            'official_cassandra_schema':False,
            'reference_origin':'Cassandra structural translation of IoTDB 2.x table mode schema',
            'semantic_scope':'A1 simplified illustrative operations; not Java QUERY_SEED/SQL replay',
            'materialization':'device_id TAG => Cassandra partition key; time => clustering key; sensor FIELDS => DOUBLE columns',
            'keyspace':KEYSPACE, 'ddl':table_ddl(sensors),
            'hbench_sha256':sha(path4(8,'28_hbench_frozen.json')),
            'source_rows_sha256':sha(inputs()['native']),
            'query_instances_sha256':sha(inputs()['queries']),
            'correctness':f'PASS_ALL_{checked}_QUERIES',
            'row_count':physical_count,'logical_metric_cells':len(rows),
            'measurement_rounds':args.rounds,'warmup':args.warmup,
            'limitations':['No official Cassandra driver in upstream IoT Benchmark',
                           'No exact Java query trace replay; Q8 and Q12 not official SQL-equivalent',
                           'Fixed sequential order; shared cache effects possible',
                           'DOUBLE/ms/homogeneous sensor fixture only']})
        print(f'PASS baseline reference: {checked} correctness checks, {len(rows)} cells, {physical_count} wide rows; metrics saved to {out}')


def main():
    parser=argparse.ArgumentParser(description='Post-selection diagnostic IoTDB-table-shaped Cassandra baseline')
    mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--dry-run',action='store_true');mode.add_argument('--live',action='store_true')
    parser.add_argument('--host',default='127.0.0.1');parser.add_argument('--port',type=int,default=9043)
    parser.add_argument('--warmup',type=int,default=5);parser.add_argument('--rounds',type=int,default=20)
    parser.add_argument('--confirm-reset',default='')
    parser.add_argument('--output',type=Path,default=ROOT/'outputs/stage9_reference_table')
    args=parser.parse_args()
    if args.dry_run:
        confirm_frozen();rows=read_rows(inputs()['native']);qs=read(inputs()['queries'])
        sensors=sorted({r['sensor_name'] for r in rows});ensure_pilot_invariants(rows,qs,sensors)
        n,m=offline_checks(rows,qs,sensors)
        print(table_ddl(sensors));print(f'OFFLINE BASELINE PASS: {n} device-time rows, {m} queries, {len(rows)} metric cells; NO CASSANDRA')
    else:run_live(args)
if __name__=='__main__':main()
