"""Stage9: optional post-HBench Cassandra evaluation. Importing this module has no database side effects.

Never infer or alter family activation, analytical scores, routing or HBench from results.
A successful pilot run is NOT an official Apache IoT Benchmark score because the
QUERY_SEED sequence and generated dialect-specific SQL are not replayed.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
from datetime import datetime,timezone
import math
import re
import statistics
import sys
import time
from .shared import *
from .offline import date_of,day_indices,physical_state,compare

TABLES={'TS0':'ts0_iot_raw','TS1':'ts1_iot_bucket','TS2':'ts2_iot_wide',
        'TS4':'ts4_iot_latest','TS5':'ts5_iot_rollup'}

def confirm_frozen():
    mani=read(path4(8,'32_frozen_manifest.json'))
    if mani['status']!='FROZEN_PREBENCH_SELECTION' or mani['benchmark_measured'] or mani['C0_C9_used']:
        raise ValueError('Stage8 not frozen or invalid scope')
    if sha(path4(8,'28_hbench_frozen.json'))!=mani['frozen_hbench_sha256']:
        raise ValueError('HBench changed after freeze')
    for rel,hashval in mani['inputs_sha256'].items():
        file=ROOT/rel
        if not file.exists() or sha(file)!=hashval:
            raise ValueError(f'Frozen selection input changed: {rel}; rerun offline Stage4-8 to make new selection')
    return read(path4(8,'28_hbench_frozen.json'))

def _query_rows(session,family,q,sensors,candidate_id=None):
    """Live physical reads. Do not use ALLOW FILTERING; every request binds partition keys."""
    records=[];start=q['start_ms'];end=q['end_ms_exclusive']
    if q['query_id']=='Q1':start=q['point_ms'];end=start+1
    # Native Q5 has no narrow time filter; native table Q8 uses global last_by.
    unrestricted=q['query_id'] in {'Q5','Q8'}
    if q['query_id']=='Q8':start=-2**63;end=2**63-1
    if family=='TS4':
        for device in q['devices']:
            for sensor in q['sensors']:
                rows=session.execute('SELECT latest_timestamp_ms,value_double FROM ts4_iot_latest WHERE device_id=%s AND sensor_name=%s',(device,sensor))
                for row in rows:
                    t=int(row.latest_timestamp_ms)
                    if start<=t<end:records.append({'device_id':device,'sensor_name':sensor,'timestamp':t,'value_double':float(row.value_double)})
        return records
    if family=='TS0':
        for d in q['devices']:
            for s in q['sensors']:
                if unrestricted:
                    cql='SELECT timestamp_ms,value_double FROM ts0_iot_raw WHERE device_id=%s AND sensor_name=%s';param=(d,s)
                else:
                    cql='SELECT timestamp_ms,value_double FROM ts0_iot_raw WHERE device_id=%s AND sensor_name=%s AND timestamp_ms >= %s AND timestamp_ms < %s';param=(d,s,start,end)
                for row in session.execute(cql,param):
                    records.append({'device_id':d,'sensor_name':s,'timestamp':int(row.timestamp_ms),'value_double':float(row.value_double)})
    if family=='TS1':
        # For unrestricted reads the loader must enumerate every populated day.
        if unrestricted:raise ValueError('TS1 unrestricted operation is unsupported; route Q5 to TS0')
        for d in q['devices']:
            for s in q['sensors']:
                for day in day_indices(start,end):
                    cql='SELECT timestamp_ms,value_double FROM ts1_iot_bucket WHERE device_id=%s AND sensor_name=%s AND bucket_day=%s AND timestamp_ms >= %s AND timestamp_ms < %s'
                    for row in session.execute(cql,(d,s,date_of(day*86400000),start,end)):
                        records.append({'device_id':d,'sensor_name':s,'timestamp':int(row.timestamp_ms),'value_double':float(row.value_double)})
    if family=='TS2':
        if unrestricted:raise ValueError('TS2 unrestricted operation is unsupported; route Q5 to TS0')
        allowed={f'm_{s}':s for s in q['sensors']}
        if any(not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',x) for x in allowed):raise ValueError('Bad metric column')
        for d in q['devices']:
            if candidate_id=='ts2_iot_wide_unbucketed':
                sources=[('SELECT * FROM ts2_iot_wide_unbucketed WHERE device_id=%s AND timestamp_ms >= %s AND timestamp_ms < %s',
                          (d,start,end))]
            else:
                sources=[('SELECT * FROM ts2_iot_wide WHERE device_id=%s AND bucket_day=%s AND timestamp_ms >= %s AND timestamp_ms < %s',
                          (d,date_of(day*86400000),start,end)) for day in day_indices(start,end)]
            for cql,args in sources:
                for row in session.execute(cql,args):
                    for column,sensor in allowed.items():
                        v=getattr(row,column,None)
                        if v is not None:records.append({'device_id':d,'sensor_name':sensor,'timestamp':int(row.timestamp_ms),'value_double':float(v)})
    if family not in ('TS0','TS1','TS2','TS4'):
        raise ValueError(f'Unsupported physical raw-reading route {family}')
    return records

def _rollup_rows(session,q):
    g=q['group_by_ms'];start=q['start_ms'];end=q['end_ms_exclusive']
    if start%g or end%g:raise ValueError('TS5 selected for unaligned query!')
    out=[]
    for d in q['devices']:
        for s in q['sensors']:
            for day in day_indices(start,end):
                sql='SELECT window_start_ms,value_sum,sample_count,minimum,maximum FROM ts5_iot_rollup WHERE device_id=%s AND sensor_name=%s AND bucket_day=%s AND window_start_ms >= %s AND window_start_ms < %s'
                for row in session.execute(sql,(d,s,date_of(day*86400000),start,end)):
                    agg={'count':row.sample_count,'sum':row.value_sum,'min':row.minimum,'max':row.maximum,
                         'avg':row.value_sum/row.sample_count}[q['aggregation']]
                    out.append({'device_id':d,'sensor_name':s,'window_start_ms':int(row.window_start_ms),'aggregate':agg})
    return sorted(out,key=lambda r:(r['device_id'],r['sensor_name'],-r['window_start_ms'] if q['query_id']=='Q11' else r['window_start_ms']))

def execute_query(session,q,route,sensors):
    # The pilot's logical contract is intentionally checked against A1's oracle.
    sys.path.insert(0,str(A1))
    from core.bridge import _result
    f=route['family']
    if f=='TS5':return _rollup_rows(session,q)
    return _result(_query_rows(session,f,q,sensors,candidate_id=route['candidate_id']),q)

def _create_keyspace(session,keyspace,confirm):
    if not re.fullmatch('sl_iot_h_[0-9a-f]{12}',keyspace):raise ValueError('Unsafe keyspace name')
    if confirm!=keyspace:raise ValueError(f'Explicit opt-in required: --confirm-reset-keyspace {keyspace}')
    session.execute(f'DROP KEYSPACE IF EXISTS {keyspace}')
    session.execute(f"CREATE KEYSPACE {keyspace} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}} AND durable_writes=true")
    session.set_keyspace(keyspace)

def _materialize(session,h,rows,sensors,p):
    specs={s['candidate_id']:s for s in read(path4(4,'10_candidate_specs.json'))}
    for cid in h['candidates']:
        session.execute(specs[cid]['ddl'])
    state=physical_state(rows,p['rollup_window_ms'])
    counts={}
    for cid in h['candidates']:
        fam=specs[cid]['family'];inserted=0
        if fam=='TS0':
            prepared=session.prepare('INSERT INTO ts0_iot_raw (device_id,sensor_name,timestamp_ms,value_double) VALUES (?,?,?,?)')
            for r in rows:
                session.execute(prepared,(r['device_id'],r['sensor_name'],r['timestamp'],r['value_double']));inserted+=1
        elif fam=='TS1':
            prepared=session.prepare('INSERT INTO ts1_iot_bucket (device_id,sensor_name,bucket_day,timestamp_ms,value_double) VALUES (?,?,?,?,?)')
            for r in rows:
                session.execute(prepared,(r['device_id'],r['sensor_name'],date_of(r['timestamp']),r['timestamp'],r['value_double']));inserted+=1
        elif fam=='TS2':
            columns=[f'm_{s}' for s in sensors]
            if any(not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',c) for c in columns):raise ValueError('Invalid sensor name')
            variant=specs[cid].get('partition_variant')
            if variant=='unbucketed':
                cols=['device_id','timestamp_ms']+columns
                sql=f"INSERT INTO {specs[cid]['table']} ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
                source=((d,t,m) for (d,t),m in state['TS2_UNBUCKETED'].items())
                parameters=lambda d,t,m:tuple([d,t]+[m.get(s) for s in sensors])
            else:
                cols=['device_id','bucket_day','timestamp_ms']+columns
                sql=f"INSERT INTO {specs[cid]['table']} ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
                source=((d,t,m) for (d,day,t),m in state['TS2'].items())
                parameters=lambda d,t,m:tuple([d,date_of(t),t]+[m.get(s) for s in sensors])
            prepared=session.prepare(sql)
            for d,t,measures in source:
                session.execute(prepared,parameters(d,t,measures));inserted+=1
        elif fam=='TS4':
            prepared=session.prepare('INSERT INTO ts4_iot_latest (device_id,sensor_name,latest_timestamp_ms,value_double) VALUES (?,?,?,?)')
            for (d,s),(t,v) in state['TS4'].items():session.execute(prepared,(d,s,t,v));inserted+=1
        elif fam=='TS5':
            prepared=session.prepare('INSERT INTO ts5_iot_rollup (device_id,sensor_name,bucket_day,window_start_ms,value_sum,sample_count,minimum,maximum) VALUES (?,?,?,?,?,?,?,?)')
            for (d,s,w),a in state['TS5'].items():
                session.execute(prepared,(d,s,date_of(w),w,float(a['sum']),int(a['count']),float(a['min']),float(a['max'])));inserted+=1
        counts[cid]=inserted
    return counts

def _pctl(numbers,p):
    nums=sorted(numbers);return nums[min(len(nums)-1,max(0,math.ceil((p/100)*len(nums))-1))]

def run_live(args):
    selected=confirm_frozen();p=require_policy();qs=read(inputs()['queries']);rows=read_rows(inputs()['native'])
    sensors=sorted({r['sensor_name'] for r in rows});requested=selected if args.all else [h for h in selected if h['deployment_id']==args.deployment]
    if not requested:raise ValueError('No frozen HBench deployment matching the chosen ID')
    if args.all and args.confirm_reset_all!='RESET_ALL_SL_IOT':raise ValueError('All-keyspace reset requires --confirm-reset-all RESET_ALL_SL_IOT')
    if not args.all and args.confirm_reset_keyspace!=f'sl_iot_h_{requested[0]["deployment_id"][2:]}':
        raise ValueError('Single keyspace confirmation mismatch')
    if args.rounds<5 or args.warmup<0:raise ValueError('Require at least 5 benchmark rounds and nonnegative warmup')
    if args.output.exists() and any(args.output.iterdir()):raise ValueError('Refusing to overwrite nonempty results directory')
    # Optional dependency only used inside explicit live command.
    from cassandra.cluster import Cluster
    sys.path.insert(0,str(A1))
    from core.bridge import _result
    cluster=Cluster([args.host],port=args.port)
    measures=[];summaries=[];load_summaries=[];success=False
    try:
        session=cluster.connect()
        for h in requested:
            ks=f'sl_iot_h_{h["deployment_id"][2:]}'
            _create_keyspace(session,ks,ks if args.all else args.confirm_reset_keyspace)
            load_start_ns=time.perf_counter_ns()
            writecounts=_materialize(session,h,rows,sensors,p)
            load_seconds=(time.perf_counter_ns()-load_start_ns)/1e9
            load_summaries.append({"deployment_id":h["deployment_id"],
                "logical_metric_cells":len(rows),"physical_rows_written":sum(writecounts.values()),
                "load_elapsed_seconds":load_seconds,"logical_points_per_second":len(rows)/max(load_seconds,1e-9),
                "scope":"sequential Python loader; not official IoT Benchmark ingestion throughput"})
            # Fail before any measurements if any logical query differs.
            for q in qs:
                got=execute_query(session,q,h['query_routes'][q['query_id']],sensors)
                expect=_result(rows,q)
                if not compare(got,expect):
                    raise AssertionError(f'Correctness failed on live Cassandra: {h["deployment_id"]} {q["query_id"]}')
            for q in qs:
                for i in range(args.warmup):execute_query(session,q,h['query_routes'][q['query_id']],sensors)
                observed=[]
                for i in range(args.rounds):
                    t=time.perf_counter_ns();execute_query(session,q,h['query_routes'][q['query_id']],sensors)
                    observed.append((time.perf_counter_ns()-t)/1e6)
                summaries.append({'deployment_id':h['deployment_id'],'query_id':q['query_id'],
                                  'family':h['query_routes'][q['query_id']]['family'],
                                  'mean_ms':statistics.mean(observed),'median_ms':statistics.median(observed),
                                  'p95_ms':_pctl(observed,95),'p99_ms':_pctl(observed,99),
                                  'successful_operations':len(observed),'correctness':'PASS'})
                measures.extend({'deployment_id':h['deployment_id'],'query_id':q['query_id'],'rep':i+1,
                                 'latency_ms':v} for i,v in enumerate(observed))
            print(f'Validated and benchmarked {h["deployment_id"]}: {writecounts}')
        success=True
    finally:
        cluster.shutdown()
    if success:
        out=args.output;out.mkdir(parents=True,exist_ok=True)
        csv_write(out/'34_load_metrics.csv',load_summaries,['deployment_id','logical_metric_cells','physical_rows_written','load_elapsed_seconds','logical_points_per_second','scope'])
        csv_write(out/'35_query_p95.csv',summaries,['deployment_id','query_id','family','mean_ms','median_ms','p95_ms','p99_ms','successful_operations','correctness'])
        csv_write(out/'36_raw_latencies.csv',measures,['deployment_id','query_id','rep','latency_ms'])
        write(out/'37_manifest.json',{'scope':'PILOT_IOT_CASSANDRA_LIVE_POST_SELECTION',
           'official_iot_benchmark_score':False,'native_query_sql_replay':False,
           'hbench_sha256':sha(path4(8,'28_hbench_frozen.json')),
           'source_rows_sha256':sha(inputs()['native']),
           'deployments':[x['deployment_id'] for x in requested],
           'correctness':'PASS_ALL','measurement_rounds':args.rounds,'warmup':args.warmup,
           'limitations':['Client-side postprocessing and uniform illustrative query parameters',
                          'No official QUERY_SEED replay, no official IoT Benchmark client throughput',
                          'Sequential bounded pilot, Cassandra writes included only in load not throughput',
                          'No query parameter randomization; Cassandra cache/order effects are possible']})

def main():
    ap=argparse.ArgumentParser(description='SchemaLens Stage9 optional LOCAL Cassandra executor')
    ap.add_argument('--dry-run',action='store_true',help='In-memory semantic validation of frozen deployments, no Cassandra')
    ap.add_argument('--live',action='store_true',help='Opt into live Cassandra destructive disposable keyspace runs')
    ap.add_argument('--deployment',help='Frozen HBench deployment ID (one at a time)')
    ap.add_argument('--all',action='store_true',help='Execute all frozen deployments')
    ap.add_argument('--host',default='127.0.0.1');ap.add_argument('--port',type=int,default=9042)
    ap.add_argument('--confirm-reset-keyspace',default='');ap.add_argument('--confirm-reset-all',default='')
    ap.add_argument('--warmup',type=int,default=5);ap.add_argument('--rounds',type=int,default=20)
    ap.add_argument('--output',type=Path,default=ROOT/'outputs/stage9_local')
    args=ap.parse_args()
    if args.live==args.dry_run:ap.error('Exactly one of --dry-run or --live is required')
    if args.dry_run:
        from .offline import offline_validate
        chosen=confirm_frozen();results=offline_validate()
        print('OFFLINE ONLY:',len(results),'selected route semantic checks passed across',len(chosen),'deployments')
        return
    if not (args.all or args.deployment) or (args.all and args.deployment):ap.error('Live: choose --deployment ID OR --all')
    run_live(args)
if __name__=='__main__':main()
