"""Cassandra Rev3 confirmation: separate keyspaces, frozen plan IDs, clean holdout.

NEVER DROPS anything and never overwrites previous measurements. It records
end-to-end client latency (including Python result transformation), not internal
Cassandra execution time. The query oracle is checked before timing and after
all measured calls. This is NOT the official IoT Benchmark workload replay.
"""
from __future__ import annotations
import argparse
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
from . import gate

CONFIRM='CREATE_NEW_DISPOSABLE_R3_KEYSPACES'
LABEL_ORIG='BASELINE_ORIGINAL'
LABEL_OPT='BASELINE_Q8_OPTIMIZED'


def keyspace_names(selsha,datahash,run_tag,selection):
    signature='|'.join((selsha,datahash,run_tag))
    fingerprint=hashlib.sha256(signature.encode()).hexdigest()[:10]
    def name(label):
        if label in (LABEL_ORIG,LABEL_OPT):tail='reference'
        elif re.fullmatch(r'R3H_[0-9a-f]{12}',label):tail=label.lower()
        else:raise ValueError('Unknown Rev3 frozen deployment ID '+label)
        return 'sl_conf_r3_'+fingerprint+'_'+tail
    return {label:name(label) for label in [LABEL_ORIG,LABEL_OPT]+[h['deployment_id'] for h in selection]}


def guard_keyspace(name):
    if not re.fullmatch(r'sl_conf_r3_[0-9a-f]{10}_(reference|r3h_[0-9a-f]{12})',name):
        raise ValueError('Unsafe Cassandra keyspace name: '+name)
    return name


def ensure_absent(session,names):
    """Check every requested keyspace BEFORE the first CREATE operation."""
    for name in sorted(set(names.values())):
        guard_keyspace(name)
        found=list(session.execute('SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=%s',(name,)))
        if found:raise FileExistsError('Refusing existing Cassandra keyspace '+name)


def create_only(session,name,confirm):
    if confirm!=CONFIRM:raise PermissionError('Explicit create-only confirmation token missing')
    guard_keyspace(name)
    # No DROP, TRUNCATE, ALTER, destructive reset, or IF NOT EXISTS.
    session.execute('CREATE KEYSPACE '+name+
        " WITH replication = {'class':'SimpleStrategy','replication_factor':1} AND durable_writes=true")
    session.set_keyspace(name)


def pctl(values,p):
    if not values:raise ValueError('No observations for percentile')
    s=sorted(values)
    return s[min(len(s)-1,max(0,math.ceil(len(s)*p/100)-1))]


def save_csv(path,records,cols=None):
    if not records:raise ValueError('Cannot write empty CSV '+str(path))
    if cols is None:cols=list(records[0])
    with Path(path).open('x',newline='',encoding='utf8') as fh:
        w=csv.DictWriter(fh,fieldnames=cols,extrasaction='ignore')
        w.writeheader();w.writerows(records)


def baseline_dispatch(session,q,optim,sensors,days,page_size,oracle):
    from v4_rev2.runtime_helpers import read_wide
    records,calls,iterated=read_wide(session,q,sensors,'iot_reference_table',days,
        optimized_q8=optim and q['query_id']=='Q8',page_size=page_size)
    return oracle(records,q),{'execute_calls':calls,'iterated_rows':iterated,
        'fallbacks':0,'physical_plan':'BASELINE_Q8_DESC_SPARSE' if optim and q['query_id']=='Q8' else 'BASELINE_FULL_OR_RANGE'}


def execute_plan(session,q,label,selection_by_id,catalog,sensors,days,page_size,oracle):
    if label in (LABEL_ORIG,LABEL_OPT):
        return baseline_dispatch(session,q,label==LABEL_OPT,sensors,days,page_size,oracle)
    from rev3.runtime import dispatch
    return dispatch(session,q,selection_by_id[label],catalog,sensors,days,page_size,oracle)


def analyze_proxies(raw,selection,catalog):
    stage7={p['plan_id']:p for p in catalog}
    out=[]
    for h in selection:
        for qid,route in sorted(h['query_routes'].items()):
            s=[x for x in raw if x['variant']==h['deployment_id'] and x['query_id']==qid]
            if not s:raise ValueError('Missing raw telemetry for '+h['deployment_id']+' '+qid)
            p=stage7[route['plan_id']]
            req=set(x['execute_calls'] for x in s);rows=set(x['iterated_rows'] for x in s)
            predreq=p['request_count_proxy'];predrows=p['physical_rows_proxy']
            if len(req)!=1 or len(rows)!=1:status='VARIABLE_OBSERVED'
            elif next(iter(req))==predreq and next(iter(rows))==predrows:status='MATCH'
            else:status='MISMATCH'
            out.append({'deployment_id':h['deployment_id'],'query_id':qid,
                        'plan_id':route['plan_id'],'physical_plan':route['physical_plan'],
                        'predicted_execute_calls':predreq,'observed_execute_calls':';'.join(map(str,sorted(req))),
                        'predicted_iterated_rows':predrows,'observed_iterated_rows':';'.join(map(str,sorted(rows))),
                        'status':status})
    return out


def run(workspace,host,port,confirm,max_cells):
    if confirm!=CONFIRM:raise PermissionError('Refusing live Cassandra without explicit CREATE token')
    if host not in ('127.0.0.1','localhost','::1'):
        raise ValueError('Confirmatory runner restricted to local Cassandra, use SSH port forwarding')
    if not 1<=port<=65535:raise ValueError('Invalid Cassandra port')
    exp,protocol,selected=gate.verify(workspace)
    if len({h['deployment_id'] for h in selected})!=len(selected):raise ValueError('Duplicate frozen deployment ID')
    from iot_pipeline.shared import read,read_rows,require_policy
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.offline import compare
    from iot_pipeline.stage9 import _materialize
    from iot_pipeline.reference_baseline import table_ddl
    from v4_rev2.runtime_helpers import load_wide,DAY
    from cassandra.cluster import Cluster
    rows=read_rows(exp/'phase_a1/outputs/normalized_double_readings.csv')
    if not rows or len(rows)>max_cells:raise ValueError('Unsafe metric cell count')
    queries=read(exp/'phase_a1/outputs/query_instances_pilot.json')
    if len(queries)!=12 or len({q['query_id'] for q in queries})!=12:
        raise ValueError('Current protocol supports one query object per Q1..Q12 only')
    sensors=sorted({r['sensor_name'] for r in rows});days={r['timestamp']//DAY for r in rows}
    oracle=source_oracle();policy=require_policy()
    catalog_list=read(exp/gate.REV3/'stage5/15_executable_plan_catalog.json')
    catalog={p['plan_id']:p for p in catalog_list}
    selection_by_id={h['deployment_id']:h for h in selected}
    labels=[LABEL_ORIG,LABEL_OPT]+list(selection_by_id)
    names=keyspace_names(protocol['rev3_selection_sha256'],protocol['source_sha256']['new']['data'],protocol['run_tag'],selected)
    batch_sha=next(iter(names.values())).split('_')[3]
    out=exp/'outputs/rev3_independent_confirmatory'/('batch_'+batch_sha)
    if out.exists():raise FileExistsError('Refusing to overwrite prior benchmark result: '+str(out))
    print('Confirmed new Java dataset:',len(rows),'metric cells;',len(queries),'Java query objects')
    print('Frozen Rev3:',protocol['rev3_selection_sha256'])
    print('CREATE-ONLY keyspaces:',*sorted(set(names.values())),sep='\n  ')
    cluster=Cluster([host],port=port)
    load=[];raw=[];orders=[];cardinality={};status='FAILED'
    out.mkdir(parents=True,exist_ok=False)
    started=time.time()
    try:
        session=cluster.connect()
        ensure_absent(session,names)
        for label in labels:
            if label==LABEL_OPT:continue
            ks=names[label]
            create_only(session,ks,confirm)
            begin=time.perf_counter_ns()
            if label==LABEL_ORIG:
                session.execute(table_ddl(sensors))
                writes=load_wide(session,rows,sensors,'iot_reference_table',False)
            else:
                writes=sum(_materialize(session,selection_by_id[label],rows,sensors,policy).values())
            load.append({'variant':label,'keyspace':ks,'physical_write_operations':writes,
                         'metric_cells':len(rows),
                         'load_seconds':(time.perf_counter_ns()-begin)/1e9,
                         'caution':'Sequential Python loader; NOT official ingestion throughput'})
            print('Materialized',label,'physical writes',writes,flush=True)
        for q in queries:
            expected=oracle(rows,q)
            cardinality[q['query_id']]=len(expected) if hasattr(expected,'__len__') else None
            for label in labels:
                session.set_keyspace(names[label])
                actual,trace=execute_plan(session,q,label,selection_by_id,catalog,sensors,days,
                                             protocol['page_size'],oracle)
                if not compare(actual,expected):
                    raise AssertionError('Pre-timing semantic mismatch: '+label+' '+q['query_id'])
        print('All',len(labels)*len(queries),'routes passed oracle before timing',flush=True)
        rng=random.Random(protocol['measurement_seed'])
        for q in queries:
            expected=oracle(rows,q)
            for label in labels:
                session.set_keyspace(names[label])
                for _ in range(protocol['warmup']):
                    actual,trace=execute_plan(session,q,label,selection_by_id,catalog,sensors,days,
                                              protocol['page_size'],oracle)
                    if not compare(actual,expected):raise AssertionError('Warmup semantic mismatch '+label)
            for rep in range(1,protocol['rounds']+1):
                order=list(labels);rng.shuffle(order)
                if rep%2==0:order.reverse()
                orders.append({'query_id':q['query_id'],'rep':rep,'variants_order':'|'.join(order)})
                for position,label in enumerate(order,1):
                    session.set_keyspace(names[label])
                    begin=time.perf_counter_ns()
                    got,trace=execute_plan(session,q,label,selection_by_id,catalog,sensors,days,
                                             protocol['page_size'],oracle)
                    elapsed=(time.perf_counter_ns()-begin)/1e6
                    if not compare(got,expected):
                        raise AssertionError('Post-timing oracle mismatch '+label+' '+q['query_id']+' rep='+str(rep))
                    raw.append({'query_id':q['query_id'],'variant':label,
                        'rep':rep,'position':position,'latency_ms':round(elapsed,9),
                        'execute_calls':trace['execute_calls'],'iterated_rows':trace['iterated_rows'],
                        'fallbacks':trace['fallbacks'],'physical_plan':trace['physical_plan'],
                        'plan_id':(selection_by_id[label]['query_routes'][q['query_id']]['plan_id']
                                   if label in selection_by_id else 'REFERENCE')})
            print('Measured '+q['query_id'],flush=True)
        status='COMPLETE'
    except BaseException as exc:
        failure={'status':'FAILED_PRESERVE_CASSANDRA_KEYSPACES_AND_OUTPUT',
            'error_type':type(exc).__name__,'error_message':str(exc),
            'keyspaces':names,'do_not_retry_with_same_run_tag':True,
            'no_overwrite':True,'recorded_at_epoch_seconds':int(time.time())}
        (out/'failure_manifest.json').write_text(json.dumps(failure,indent=2)+'\n',encoding='utf8')
        if raw:save_csv(out/'partial_raw_latencies.csv',raw)
        if load:save_csv(out/'partial_load.csv',load)
        raise
    finally:
        cluster.shutdown()
    # Selection and source must be identical to the premeasurement seal.
    _,after,_=gate.verify(workspace)
    if after!=protocol:raise ValueError('Frozen metadata changed during Cassandra measurement')
    summary=[];aggregate=[]
    for label in labels:
        qresults=[]
        for q in queries:
            qid=q['query_id']
            samples=[r['latency_ms'] for r in raw if r['variant']==label and r['query_id']==qid]
            if len(samples)!=protocol['rounds']:raise AssertionError('Missing sample '+label+' '+qid)
            line={'variant':label,'query_id':qid,'runs':len(samples),
                  'mean_ms':round(statistics.mean(samples),9),
                  'median_ms':round(statistics.median(samples),9),
                  'p95_ms':pctl(samples,95),'p99_ms':pctl(samples,99),
                  'result_cardinality':cardinality[qid],'correctness':'PASS'}
            summary.append(line);qresults.append(line)
        aggregate.append({'variant':label,
            'mean_12_query_p95_ms':statistics.mean(r['p95_ms'] for r in qresults),
            'mean_11_query_p95_excluding_q8_ms':statistics.mean(r['p95_ms'] for r in qresults if r['query_id']!='Q8'),
            'mean_11_query_p95_excluding_q5_ms':statistics.mean(r['p95_ms'] for r in qresults if r['query_id']!='Q5'),
            'scope':'Unweighted per-class nearest-rank p95 means; not a workload-mix p95'})
    diagnostics=analyze_proxies(raw,selected,catalog_list)
    save_csv(out/'raw_latencies.csv',raw)
    save_csv(out/'query_p95.csv',summary)
    save_csv(out/'aggregated.csv',aggregate)
    save_csv(out/'orders.csv',orders)
    save_csv(out/'load.csv',load)
    save_csv(out/'stage7_vs_stage9.csv',diagnostics)
    manifest={'status':'REV3_INDEPENDENT_JAVA_CONFIRMATION_COMPLETE',
       'revision':'SCHEMALENS_REV3_PLAN_AWARE_V1',
       'not_official_IoT_Benchmark_score':True,
       'not_frequency_replay':True,
       'post_E5_method_development':True,
       'premeasurement_protocol_sha256':gate.digest(Path(workspace)/gate.PROTO),
       'frozen_rev3_sha256':protocol['rev3_selection_sha256'],
       'data_sha256':protocol['source_sha256']['new']['data'],
       'queries_sha256':protocol['source_sha256']['new']['queries'],
       'keyspaces':names,'selected_ids':protocol['selected_ids'],
       'oracle_cardinality':cardinality,
       'query_count':len(queries),'metric_cells':len(rows),'sensor_count':len(sensors),
       'rounds':protocol['rounds'],'warmup':protocol['warmup'],'page_size':protocol['page_size'],
       'measurement_seed':protocol['measurement_seed'],'run_tag':protocol['run_tag'],
       'raw_measurements':len(raw),'oracle':'PASS_ALL_ROUTES_BEFORE_AND_DURING_TIMING',
       'observed_vs_stage7':{k:sum(x['status']==k for x in diagnostics)
            for k in ('MATCH','MISMATCH','VARIABLE_OBSERVED')},
       'latency_scope':'End-to-end Python client/driver, not Cassandra server latency',
       'telemetry_scope':'Session.execute calls and Python-iterated rows, not server SSTable reads',
       'load_scope':'Sequential Python inserts, not realistic ingestion throughput',
       'storage_scope':'NOT MEASURED HERE; separate postflush SSTable run required',
       'started_epoch_seconds':int(started),'finished_epoch_seconds':int(time.time()),
       'output_sha256':{p.name:gate.digest(p) for p in out.iterdir() if p.is_file()}}
    with (out/'manifest.json').open('x',encoding='utf8') as f:
        json.dump(manifest,f,sort_keys=True,indent=2);f.write('\n')
    print('COMPLETE:',out)
    print('Raw measurements:',len(raw),'proxy outcomes:',manifest['observed_vs_stage7'])
    return out


def main():
    ap=argparse.ArgumentParser(description='Frozen Rev3 independent Java Cassandra benchmark')
    ap.add_argument('--workspace',required=True,type=Path)
    ap.add_argument('--host',default='127.0.0.1')
    ap.add_argument('--port',type=int,default=9043)
    ap.add_argument('--confirm-create',default='')
    ap.add_argument('--max-cells',type=int,default=100000)
    args=ap.parse_args()
    run(args.workspace,args.host,args.port,args.confirm_create,args.max_cells)

if __name__=='__main__':main()
