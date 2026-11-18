"""Confirmatory Cassandra benchmark: ONLY selected V4 Rev2 HBench plus two baseline queries.

Hard gates: frozen Rev2, new Java data/queries, separate keyspaces, correctness,
no benchmark output overwritten, V3 + Rev2 hash verification before/after.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import math
from pathlib import Path
import random
import re
import statistics
import time
from collections import defaultdict
from . import prebenchmark as P
from . import runtime_helpers as R
from v4_auto import selector as A

TOKEN='RESET_ONLY_SCHEMA_CONF_R2_KEYSPACES'

def safe_ks(fingerprint,label):
    if not re.fullmatch(r'[0-9a-f]{8}',fingerprint):raise ValueError('Invalid frozen selection fingerprint')
    if label in ('BASELINE_ORIGINAL','BASELINE_Q8_OPTIMIZED'):tail='reference'
    elif re.fullmatch(r'R2H_[0-9a-f]{12}',label):tail=label.lower()
    else:raise ValueError('Refusing unknown or non-frozen deployment ID '+label)
    return 'sl_conf_r2_'+fingerprint+'_'+tail

def create_only_safe(session,name,token):
    if token!=TOKEN or not re.fullmatch(r'sl_conf_r2_[0-9a-f]{8}_(?:reference|r2h_[0-9a-f]{12})',name):
        raise ValueError('Unsafe destructive Cassandra keyspace operation refused')
    session.execute('DROP KEYSPACE IF EXISTS '+name)
    session.execute("CREATE KEYSPACE "+name+" WITH replication = {'class':'SimpleStrategy','replication_factor':1} AND durable_writes=true")
    session.set_keyspace(name)

def result_for(session,q,h,sensors,days,page_size,oracle):
    """Runtime respects frozen per-query routing. No post-selection rerouting."""
    if h in ('BASELINE_ORIGINAL','BASELINE_Q8_OPTIMIZED'):
        got,calls,visited=R.read_wide(session,q,sensors,'iot_reference_table',days,
                            optimized_q8=(h=='BASELINE_Q8_OPTIMIZED' and q['query_id']=='Q8'),page_size=page_size)
        return oracle(got,q),calls,visited,'REFERENCE_OPT' if h=='BASELINE_Q8_OPTIMIZED' else 'REFERENCE_ORIGINAL'
    route=h['query_routes'][q['query_id']]
    cid=route['candidate_id'];family=route['family']
    if family=='TS0':
        from iot_pipeline.stage9 import execute_query
        return execute_query(session,q,route,sensors),None,None,'TS0_FROZEN_ROUTE'
    if family=='TS4':
        records=[];calls=0;visited=0
        for dev in q['devices']:
            for sen in q['sensors']:
                calls+=1
                for r in session.execute('SELECT latest_timestamp_ms,value_double FROM ts4_iot_latest WHERE device_id=%s AND sensor_name=%s',(dev,sen)):
                    visited+=1
                    if r.latest_timestamp_ms is not None:
                        records.append({'device_id':dev,'sensor_name':sen,'timestamp':int(r.latest_timestamp_ms),'value_double':float(r.value_double)})
        return oracle(records,q),calls,visited,'TS4_FROZEN_ROUTE'
    if family=='TS2':
        if cid not in ('ts2_iot_wide','ts2_iot_wide_unbucketed'):
            raise ValueError('Frozen TS2 route points to unexpected table')
        got,calls,visited=R.read_wide(session,q,sensors,cid,days,
             bucket=cid=='ts2_iot_wide',optimized_q8=q['query_id']=='Q8',page_size=page_size)
        return oracle(got,q),calls,visited,'TS2_FROZEN_ROUTE'
    raise ValueError('Unsupported frozen query route family '+family)

def write_csv(path,rows):
    if not rows:raise ValueError('Empty measurement dataset')
    with path.open('w',newline='',encoding='utf-8') as out:
        writer=csv.DictWriter(out,fieldnames=list(rows[0]),extrasaction='ignore')
        writer.writeheader();writer.writerows(rows)

def run(args):
    if args.confirm_reset!=TOKEN:raise ValueError('Explicit reset token required; destructive operation only on new confirmatory namespace')
    if args.warmup<0 or args.rounds<20 or args.page_size<1 or args.page_size>500:
        raise ValueError('Require >=20 rounds, >=0 warmups and page_size in 1..500')
    exp=args.experiment.resolve();meta=P.verify(exp)
    if not meta['independent_from_original_v3']:
        raise ValueError('Cannot call an old-data exploratory Rev2 freeze a confirmatory benchmark')
    chosen=A.jread(exp/P.OUTPUT/'stage8/28_hbench_v4_rev2_frozen.json')
    if not chosen or len({h['deployment_id'] for h in chosen})!=len(chosen):
        raise ValueError('Empty/duplicate frozen IDs')
    from iot_pipeline.shared import read_rows,read,require_policy
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.offline import compare
    from iot_pipeline.stage9 import _materialize
    from iot_pipeline.reference_baseline import table_ddl
    from cassandra.cluster import Cluster
    rows=read_rows(exp/'phase_a1/outputs/normalized_double_readings.csv')
    if len(rows)>args.max_cells:raise ValueError('Live run size safety cap exceeded')
    queries=read(exp/'phase_a1/outputs/query_instances_pilot.json')
    sensors=sorted({r['sensor_name'] for r in rows});days={r['timestamp']//R.DAY for r in rows}
    oracle=source_oracle();policy=require_policy()
    label_orig='BASELINE_ORIGINAL';label_opt='BASELINE_Q8_OPTIMIZED'
    labels=[label_orig,label_opt,*[h['deployment_id'] for h in chosen]]
    designs={h['deployment_id']:h for h in chosen}
    sel_hash=A.digest(exp/P.OUTPUT/'stage8/28_hbench_v4_rev2_frozen.json');prefix=sel_hash[:8]
    names={label:safe_ks(prefix,label) for label in labels}
    out=exp/'outputs/v4_rev2_confirmatory' / ('batch_'+prefix)
    if out.exists():raise FileExistsError('Refusing to overwrite previous confirmatory results: '+str(out))
    cluster=Cluster([args.host],port=args.port)
    load=[];raw=[];orders=[];cardinality={};complete=False
    try:
        session=cluster.connect()
        for label in labels:
            if label==label_opt:continue
            ks=names[label]
            create_only_safe(session,ks,args.confirm_reset)
            t=time.perf_counter_ns()
            if label==label_orig:
                session.execute(table_ddl(sensors));count=R.load_wide(session,rows,sensors,'iot_reference_table',False)
            else:
                count=sum(_materialize(session,designs[label],rows,sensors,policy).values())
            load.append({'variant':label,'keyspace':ks,'physical_write_operations':count,
                         'load_seconds':round((time.perf_counter_ns()-t)/1e9,8),'metric_cells':len(rows)})
            print('Materialized:',label,'writes:',count,flush=True)
        for q in queries:
            expected=oracle(rows,q)
            cardinality[q['query_id']]=len(expected) if hasattr(expected,'__len__') else None
            for label in labels:
                session.set_keyspace(names[label]);d=designs.get(label,label)
                actual,_,_,_=result_for(session,q,d,sensors,days,args.page_size,oracle)
                if not compare(actual,expected):
                    raise AssertionError('Semantic mismatch '+label+' '+q['query_id']+'; aborting before timing')
        print('PASS: exact result equivalence for',len(labels)*len(queries),'routes',flush=True)
        rng=random.Random(args.measurement_seed)
        for q in queries:
            for label in labels:
                session.set_keyspace(names[label]);d=designs.get(label,label)
                for _ in range(args.warmup):result_for(session,q,d,sensors,days,args.page_size,oracle)
            for rep in range(1,args.rounds+1):
                # Each round contains all designs. Rotated and optionally reversed to reduce ordering bias.
                order=list(labels);rng.shuffle(order)
                if rep%2==0:order.reverse()
                orders.append({'query_id':q['query_id'],'rep':rep,'variants_order':'|'.join(order)})
                for position,label in enumerate(order,1):
                    session.set_keyspace(names[label]);d=designs.get(label,label)
                    start=time.perf_counter_ns()
                    actual,req,visited,route=result_for(session,q,d,sensors,days,args.page_size,oracle)
                    latency=(time.perf_counter_ns()-start)/1e6
                    raw.append({'query_id':q['query_id'],'variant':label,'rep':rep,'position':position,
                                'latency_ms':round(latency,9),'requests':req,'iterated_rows':visited,'route':route})
            print('Measured:',q['query_id'],flush=True)
        complete=True
    finally:cluster.shutdown()
    if not complete:return
    post=P.verify(exp)
    if post!=meta:raise ValueError('Frozen Rev2 integrity changed during Cassandra measurement')
    agg=[];means=[]
    for label in labels:
        items=[]
        for q in queries:
            subset=[x for x in raw if x['variant']==label and x['query_id']==q['query_id']]
            latencies=[x['latency_ms'] for x in subset];p95=R.pctl(latencies,95);p99=R.pctl(latencies,99)
            row={'query_id':q['query_id'],'variant':label,'p95_ms':p95,'p99_ms':p99,
                 'mean_ms':statistics.mean(latencies),'runs':len(latencies),
                 'result_correctness':'PASS_BEFORE_TIMING'}
            agg.append(row);items.append(row)
        means.append({'variant':label,'mean_12_query_p95_ms':statistics.mean(x['p95_ms'] for x in items),
                     'mean_11_query_p95_excluding_q8_ms':statistics.mean(x['p95_ms'] for x in items if x['query_id']!='Q8'),
                     'note':'unweighted mean of per-query p95; NOT mixed-workload p95'})
    out.mkdir(parents=True,exist_ok=False)
    write_csv(out/'raw_latencies.csv',raw);write_csv(out/'query_p95.csv',agg)
    write_csv(out/'aggregated.csv',means);write_csv(out/'load.csv',load);write_csv(out/'orders.csv',orders)
    A.dump(out/'manifest.json',{'status':'CONFIRMATORY_CASSANDRA_COMPLETE',
       'revision':'V4_REV2_FROZEN_BEFORE_MEASUREMENT',
       'frozen_HBench_sha256':sel_hash,
       'parent_v3_sha256':meta['parent_v3_hbench_sha256'],
       'frozen_manifest_sha256':A.digest(exp/P.OUTPUT/'stage8/manifest.json'),
       'selected_ids':[h['deployment_id'] for h in chosen],
       'dataset_sha256':meta['dataset_query_hashes']['data'],
       'queries_sha256':meta['dataset_query_hashes']['queries'],
       'new_generator_capture_sha256':meta['java_capture']['sha256'],
       'runtime_code_sha256':{'benchmark':A.digest(Path(__file__)),
           'runtime_helpers':A.digest(Path(__file__).with_name('runtime_helpers.py'))},
       'oracle_result_cardinality_per_query':cardinality,
       'zero_cardinality_classes':[k for k,v in cardinality.items() if v==0],
       'records':len(raw),'rounds':args.rounds,'warmup':args.warmup,
       'measurement_seed':args.measurement_seed,'query_classes':len(queries),
       'baseline_original_and_optimized_share_same_table':True,
       'keyspaces':names,'correctness_checked_before_timing':True,
       'time_units':'client end-to-end milliseconds; includes driver+Python processing',
       'is_official_iot_benchmark_result':False,
       'limitations':['One Java object sample per query class, not native IoTDB SQL replay',
          'Dataset generated independently using original Java generateDataMode',
          'Only 2 aligned DOUBLE sensors unless upstream profile is further extended',
          'No workload operation-frequency replay','TS4 only snapshot maintenance',
          'Per-query p95 mean is not the global workload p95']})
    print('CONFIRMATORY RESULTS:',out)

def main():
    ap=argparse.ArgumentParser(description='SchemaLens V4 Rev2 independent Cassandra benchmark')
    ap.add_argument('--experiment',required=True,type=Path)
    ap.add_argument('--host',default='127.0.0.1');ap.add_argument('--port',default=9043,type=int)
    ap.add_argument('--rounds',type=int,default=40);ap.add_argument('--warmup',type=int,default=5)
    ap.add_argument('--measurement-seed',type=int,default=80421)
    ap.add_argument('--page-size',type=int,default=16);ap.add_argument('--max-cells',type=int,default=100000)
    ap.add_argument('--confirm-reset',default='')
    run(ap.parse_args())
if __name__=='__main__':main()
