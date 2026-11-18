#!/usr/bin/env python3
"""Rev3 S1: read-only, pre-registered controlled window sweep on B keyspaces.

This is a post-A/B exploratory experiment. Query parameter objects are derived
from Java-captured B query classes; they are NOT additional Java-generated
queries nor an official IoT Benchmark operation mix.

Cassandra work is restricted to SELECT only in the audited Rev3 dispatcher.
All source/frozen hashes and plan IDs are verified before measuring.
"""
from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import math
from pathlib import Path
import random
import statistics
import sys
import time

DAY = 86_400_000
EXPECTED_B_SHA = '8f88693851245838a7485300f8ce70aa850bd65c0c2d881225e9365cbcb39231'
B_TAG = 'rev3B_20261105'
WINDOWS = (1, 7, 14, 21)
TEMPORAL_QUERIES = ('Q2', 'Q7', 'Q9', 'Q12')
CONTROLS = ('Q5', 'Q8')
LABEL_BASE = 'BASELINE_Q8_OPTIMIZED'
CANONICAL = ('ts2_iot_wide_unbucketed', 'ts2_iot_wide')
TABLES = {'ts2_iot_wide_unbucketed':'ts2_iot_wide_unbucketed',
          'ts2_iot_wide':'ts2_iot_wide', LABEL_BASE:'iot_reference_table'}
S1_STATUS = 'S1_PREREGISTERED_BEFORE_NEW_TIMING'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load(path):
    return json.loads(Path(path).read_text(encoding='utf8'))


def write_new(path, value):
    path = Path(path)
    with path.open('x',encoding='utf8') as f:
        json.dump(value,f,indent=2,sort_keys=True,ensure_ascii=False)
        f.write('\n')


def write_csv_new(path, records, fields=None):
    if not records:raise ValueError('Refusing empty CSV')
    with Path(path).open('x',encoding='utf8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields or list(records[0]))
        w.writeheader();w.writerows(records)


def pctl(xs,p):
    if not xs:raise ValueError('No observations')
    a=sorted(xs)
    return a[math.ceil(len(a)*p/100)-1]


def make_cases(java_queries, anchor_ms, windows=WINDOWS):
    """Change only temporal bounds; never change captured devices or sensors."""
    if anchor_ms%DAY:raise ValueError('Anchor must be midnight UTC')
    by_id={q['query_id']:q for q in java_queries}
    if len(by_id)!=12 or set(by_id)!={'Q'+str(i) for i in range(1,13)}:
        raise ValueError('Exactly Q1..Q12 Java samples required')
    if tuple(windows)!=tuple(sorted(set(windows))) or not windows or min(windows)<1:
        raise ValueError('Windows must be sorted positive unique values')
    cases=[]
    for win in windows:
        end=anchor_ms+win*DAY
        for qid in TEMPORAL_QUERIES:
            q=copy.deepcopy(by_id[qid])
            q['start_ms']=anchor_ms
            q['end_ms_exclusive']=end
            if qid=='Q12':
                for child in q['set_children']:
                    child['start_ms']=anchor_ms
                    child['end_ms_exclusive']=end
            q['parameter_origin_s1']='DETERMINISTIC_WINDOW_DERIVATION_OF_B_JAVA_QUERY_OBJECT'
            q['s1_case_id']=f'{qid}_W{win:02d}'
            q['s1_window_days']=win
            cases.append(q)
    for qid in CONTROLS:
        q=copy.deepcopy(by_id[qid])
        q['parameter_origin_s1']='UNCHANGED_B_JAVA_QUERY_OBJECT_CONTROL'
        q['s1_case_id']=qid+'_CONTROL'
        q['s1_window_days']=None
        cases.append(q)
    if len(cases)!=len(windows)*len(TEMPORAL_QUERIES)+len(CONTROLS):
        raise AssertionError('Unexpected S1 case count')
    return cases


def select_designs(selected):
    found={h['canonical_source']:h for h in selected if h['canonical_source'] in CANONICAL}
    if len(found)!=2:raise ValueError('Frozen B HBench must contain exactly TS2-U and TS2-D')
    if len({x['deployment_id'] for x in found.values()})!=2:
        raise ValueError('Selected duplicate deployment IDs')
    return found


def runtime_inputs(workspace):
    """Run only with the audited B workspace, its unchanged frozen source and B manifest."""
    from rev3_confirmatory import gate
    exp,proto,selected=gate.verify(workspace)
    if proto['rev3_selection_sha256']!=EXPECTED_B_SHA or proto['run_tag']!=B_TAG:
        raise ValueError('S1 is bound to the frozen Rev3 B experiment, not any other campaign')
    source=exp/'phase_a1/outputs/normalized_double_readings.csv'
    qpath=exp/'phase_a1/outputs/query_instances_pilot.json'
    if sha(source)!=proto['source_sha256']['new']['data'] or sha(qpath)!=proto['source_sha256']['new']['queries']:
        raise ValueError('Generated Java source changed')
    ms=sorted((exp/'outputs/rev3_independent_confirmatory').glob('batch_*/manifest.json'))
    if len(ms)!=1:raise ValueError('Expected exactly one original completed B benchmark')
    bm=load(ms[0]);
    if bm['status']!='REV3_INDEPENDENT_JAVA_CONFIRMATION_COMPLETE' or bm['metric_cells']!=54000:
        raise ValueError('Original B benchmark not complete or unexpected scale')
    if bm['frozen_rev3_sha256']!=EXPECTED_B_SHA or bm['run_tag']!=B_TAG:
        raise ValueError('Original B benchmark provenance mismatch')
    designs=select_designs(selected)
    required={LABEL_BASE,*[h['deployment_id'] for h in designs.values()]}
    if not required.issubset(set(bm['keyspaces'])):raise ValueError('Missing original B keyspace names')
    return exp,proto,designs,bm,ms[0],source,qpath


def ensure_context(workspace):
    """Import original frozen pipeline modules without copying or modifying them."""
    from rev3_confirmatory import gate
    exp=gate.experiment_for(workspace)
    if str(exp) not in sys.path:sys.path.insert(0,str(exp))
    return exp


def compute_anchor(rows):
    lo=min(int(r['timestamp']) for r in rows)
    hi=max(int(r['timestamp']) for r in rows)
    return (lo//DAY)*DAY,lo,hi


def analytic_accesses(q, wide_ts_by_dev, dataset_days, bucket):
    """Descriptive counts derived from B data, NOT original frozen Stage7 predictions.

    Current two aligned DOUBLE sensors imply each wide timestamp yields one
    iterated Cassandra row; no claim about server-side disk reads or pages.
    """
    from iot_pipeline.native_reference_v3 import requests_for
    from iot_pipeline.offline import day_indices
    if q['query_id']=='Q8':
        # This control has fully aligned, non-null sensors in this dataset.
        calls=len(q['devices'])
        return calls,calls
    calls=0;rows=0
    for device,projection,start,end in requests_for(q):
        if bucket:
            days=list(day_indices(start,end)) if start is not None else sorted(dataset_days)
        else:days=[None]
        calls+=len(days)
        for t in wide_ts_by_dev.get(device,[]):
            if start is None or start<=t<end:
                rows+=1
    return calls,rows


def analytic_sanity(rows, sensors):
    """Reject sparse/null B data; a model for dense wide rows would be invalid."""
    from collections import defaultdict
    values=defaultdict(set)
    for r in rows:
        if r['value_double'] is None:raise ValueError('S1 B source has null metric values')
        values[(r['device_id'],r['timestamp'])].add(r['sensor_name'])
    if any(s!=set(sensors) for s in values.values()):
        raise ValueError('B source not aligned: descriptive row count model invalid')
    by_device={}
    for d,t in values:by_device.setdefault(d,[]).append(t)
    for k in by_device:by_device[k].sort()
    return by_device


def preregister(workspace,out,rounds=60,warmup=5,seed=20261127,page_size=16):
    from rev3_confirmatory import gate
    from iot_pipeline.shared import read_rows
    from iot_pipeline.native_reference_v3 import source_oracle
    workspace=Path(workspace).resolve();out=Path(out).resolve()
    if out.exists():raise FileExistsError('S1 output directory already exists; preserve all previous evidence')
    if out.is_relative_to(workspace):raise ValueError('Keep S1 evidence outside the frozen B workspace')
    if rounds<20 or warmup<0 or page_size<1:raise ValueError('Invalid preregistered protocol settings')
    exp,proto,designs,bm,bmpath,src,qpath=runtime_inputs(workspace)
    rows=read_rows(src);qs=load(qpath);anchor,first,last=compute_anchor(rows)
    if last>=anchor+WINDOWS[-1]*DAY:raise ValueError('Window max does not span all observed B points')
    if last<anchor+(WINDOWS[-2]*DAY):raise ValueError('B history does not support 14-day comparison')
    sensors=sorted({r['sensor_name'] for r in rows});analytic_sanity(rows,sensors)
    cases=make_cases(qs,anchor)
    oracle=source_oracle()
    card={q['s1_case_id']:len(oracle(rows,q)) for q in cases}
    if any(card[q['s1_case_id']]==0 for q in cases if q['query_id'] in TEMPORAL_QUERIES):
        raise ValueError('An S1 temporal case has empty oracle result; do not time vacuous queries')
    out.mkdir(parents=True,exist_ok=False)
    case_path=out/'s1_cases.json'
    write_new(case_path,cases)
    registry={
        'status':S1_STATUS,'protocol':'CONTROLLED_WINDOW_SWEEP_POST_A_B_EXPLORATORY',
        'original_B_experiment':str(exp),'original_B_workspace':str(workspace),
        's1_query_samples':'Deterministically modified time windows of Java B query objects',
        'not_official_Java_query_generation_for_S1_windows':True,
        'not_an_independent_confirmatory_holdout':True,
        'reuse_frozen_schema_plans_no_reselection':True,
        'frozen_rev3_B_sha256':EXPECTED_B_SHA,
        'original_B_benchmark_manifest_sha256':sha(bmpath),
        'source_data_sha256':sha(src),'source_queries_sha256':sha(qpath),
        'original_B_protocol_sha256':sha(workspace/gate.PROTO),
        'source_stage5_catalog_sha256':sha(exp/gate.REV3/'stage5/15_executable_plan_catalog.json'),
        's1_cases_sha256':sha(case_path),'s1_code_sha256':sha(Path(__file__).resolve()),
        'original_B_keyspaces':{k:bm['keyspaces'][k] for k in
            [LABEL_BASE,*[h['deployment_id'] for h in designs.values()]]},
        'selected_ids':{k:h['deployment_id'] for k,h in designs.items()},
        'source_cells':len(rows),'sensor_count':len(sensors),'device_count':len({r['device_id'] for r in rows}),
        'first_observation_ms':first,'last_observation_ms':last,
        'actual_history_days':round((last-first)/DAY,5),
        'window_anchor_utc_ms':anchor,'windows_days':list(WINDOWS),
        'time_varying_query_classes':list(TEMPORAL_QUERIES),
        'unchanged_control_classes':list(CONTROLS),
        'oracle_cardinality_by_case':card,
        'rounds_per_case_and_variant':rounds,'warmup_per_case_and_variant':warmup,
        'measurement_seed':seed,'page_size':page_size,'baseline_label':LABEL_BASE,
        'expected_observations':len(cases)*3*rounds,
        'no_cassandra_used_in_preregistration':True,
        'read_only_no_ddl_dml_or_flush':True,
        'B_already_postflush_when_S1_runs':'Verify before timing; do not compare absolute p95 to B preflush',
        'interpretation':'Client p95 and execute/iterated rows only; not internal server reads',
        'caution':'21d nearly saturates the 20.8d source; no 30d test, no 32MiB partition stress',
    }
    write_new(out/'s1_preregister.json',registry)
    print('PREREGISTERED S1, before Cassandra timing:',out)
    print('Source history:',registry['actual_history_days'],'days; cells:',len(rows))
    print('Cases:',len(cases),'expected raw measurements:',registry['expected_observations'])
    print('SHA256 S1 cases:',sha(case_path))
    return registry


def verify(workspace,out):
    workspace=Path(workspace).resolve();out=Path(out).resolve()
    reg=load(out/'s1_preregister.json')
    if reg['status']!=S1_STATUS:raise ValueError('Unrecognized S1 pre-registration')
    if str(workspace)!=reg['original_B_workspace']:raise ValueError('Wrong B workspace')
    if sha(Path(__file__).resolve())!=reg['s1_code_sha256']:
        raise ValueError('S1 implementation changed after preregistration')
    if sha(out/'s1_cases.json')!=reg['s1_cases_sha256']:
        raise ValueError('S1 workload changed after preregistration')
    exp,proto,designs,bm,bmpath,src,qpath=runtime_inputs(workspace)
    from rev3_confirmatory import gate
    checks=[(bmpath,'original_B_benchmark_manifest_sha256'),
            (src,'source_data_sha256'),(qpath,'source_queries_sha256'),
            (workspace/gate.PROTO,'original_B_protocol_sha256'),
            (exp/gate.REV3/'stage5/15_executable_plan_catalog.json','source_stage5_catalog_sha256')]
    for path,k in checks:
        if sha(path)!=reg[k]:raise ValueError('Frozen B input mutated: '+k)
    if reg['original_B_keyspaces']!={k:bm['keyspaces'][k] for k in reg['original_B_keyspaces']}:
        raise ValueError('S1 keyspace map no longer matches B')
    if reg['selected_ids']!={k:h['deployment_id'] for k,h in designs.items()}:
        raise ValueError('Selected B designs changed')
    cases=load(out/'s1_cases.json')
    anchor=reg['window_anchor_utc_ms'];original=load(qpath)
    if cases!=make_cases(original,anchor,tuple(reg['windows_days'])):
        raise ValueError('Derived queries not reproducible from original Java capture')
    if len(cases)*3*reg['rounds_per_case_and_variant']!=reg['expected_observations']:
        raise ValueError('Incorrect preregistered sample count')
    return reg,exp,designs,bm,cases


def summary_from_raw(raw,reg):
    """Recompute simple nearest-rank stats; never report workload-mix p95."""
    by={}
    for x in raw:
        k=(x['case_id'],x['variant'])
        by.setdefault(k,[]).append(float(x['latency_ms']))
    summ=[]
    for (case,label),a in sorted(by.items()):
        if len(a)!=reg['rounds_per_case_and_variant']:
            raise ValueError('Incorrect measured sample count: '+str((case,label)))
        example=next(x for x in raw if x['case_id']==case and x['variant']==label)
        summ.append({'case_id':case,'query_id':example['query_id'],
            'window_days':example['window_days'],'variant':label,'runs':len(a),
            'mean_ms':round(statistics.mean(a),9),
            'median_ms':round(statistics.median(a),9),
            'p95_ms':round(pctl(a,95),9),'p99_ms':round(pctl(a,99),9),
            'execute_calls':example['execute_calls'],'iterated_rows':example['iterated_rows'],
            'oracle_cardinality':example['oracle_cardinality'],'correctness':'PASS'})
    if len(summ)!=len(TEMPORAL_QUERIES)*len(WINDOWS)*3+len(CONTROLS)*3:
        raise ValueError('Incomplete case/variant summary')
    by2={(s['case_id'],s['variant']):s for s in summ}
    comparisons=[]
    for qid in TEMPORAL_QUERIES:
        for days in WINDOWS:
            case=f'{qid}_W{days:02d}'
            uu=reg['selected_ids']['ts2_iot_wide_unbucketed']
            dd=reg['selected_ids']['ts2_iot_wide']
            u=by2[case,uu];d=by2[case,dd];b=by2[case,LABEL_BASE]
            comparisons.append({'query_id':qid,'window_days':days,
                'p95_baseline_ms':b['p95_ms'],'p95_TS2U_ms':u['p95_ms'],
                'p95_TS2D_ms':d['p95_ms'],
                'TS2D_over_TS2U_pct':round((d['p95_ms']/u['p95_ms']-1)*100,4) if u['p95_ms'] else None,
                'TS2U_execute_calls':u['execute_calls'],
                'TS2D_execute_calls':d['execute_calls'],
                'TS2U_iterated_rows':u['iterated_rows'],
                'TS2D_iterated_rows':d['iterated_rows'],
                'result_cardinality':u['oracle_cardinality']})
    return summ,comparisons


def run_benchmark(workspace,out,host,port):
    from rev3_confirmatory import gate
    from rev3_confirmatory.benchmark import baseline_dispatch
    from rev3.runtime import dispatch
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.offline import compare
    from iot_pipeline.shared import read_rows
    from v4_rev2.runtime_helpers import DAY as RDAY
    if host not in ('127.0.0.1','localhost','::1') or not 1<=port<=65535:
        raise ValueError('Live S1 limited to local experimental Cassandra')
    if DAY!=RDAY:raise ValueError('Day-unit mismatch')
    reg,exp,designs,bm,cases=verify(workspace,out)
    results=Path(out)/'results'
    if results.exists():raise FileExistsError('S1 results already exist; no overwrites or reruns')
    rows=read_rows(exp/'phase_a1/outputs/normalized_double_readings.csv')
    sensors=sorted({r['sensor_name'] for r in rows})
    wide=analytic_sanity(rows,sensors)
    days={r['timestamp']//DAY for r in rows}
    oracle=source_oracle()
    catalog_list=load(exp/gate.REV3/'stage5/15_executable_plan_catalog.json')
    catalog={p['plan_id']:p for p in catalog_list}
    by_id={h['deployment_id']:h for h in designs.values()}
    labels=[LABEL_BASE,reg['selected_ids']['ts2_iot_wide_unbucketed'],reg['selected_ids']['ts2_iot_wide']]
    for q in cases:
        if len(oracle(rows,q))!=reg['oracle_cardinality_by_case'][q['s1_case_id']]:
            raise ValueError('Offline oracle cardinality changed after pre-registration')
    from cassandra.cluster import Cluster
    cluster=Cluster([host],port=port)
    raw=[];orders=[];status='FAILED'
    results.mkdir(parents=True,exist_ok=False)
    begin=time.time()
    try:
        session=cluster.connect()
        # Read-only checks of keyspace names already recorded in the completed B benchmark.
        for name in reg['original_B_keyspaces'].values():
            exists=list(session.execute('SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=%s',(name,)))
            if not exists:raise ValueError('Original B keyspace is absent: '+name)
        def execute(q,label):
            # Caller selects keyspace BEFORE timing; keyspace switching is not charged.
            if label==LABEL_BASE:
                return baseline_dispatch(session,q,True,sensors,days,reg['page_size'],oracle)
            return dispatch(session,q,by_id[label],catalog,sensors,days,reg['page_size'],oracle)
        expected={q['s1_case_id']:oracle(rows,q) for q in cases}
        predicted_by_case_label={
            (q['s1_case_id'],label):analytic_accesses(
                q,wide,days,label==reg['selected_ids']['ts2_iot_wide'])
            for q in cases for label in labels}
        for q in cases:
            case=q['s1_case_id'];truth=expected[case]
            for label in labels:
                session.set_keyspace(reg['original_B_keyspaces'][label])
                got,trace=execute(q,label)
                if not compare(got,truth):
                    raise AssertionError('Pre-timing correctness failure '+case+'/'+label)
                # Verify descriptive operation counts from raw source: no timing involved.
                predicted=predicted_by_case_label[case,label]
                if (trace['execute_calls'],trace['iterated_rows'])!=predicted:
                    raise AssertionError('Descriptive source/plan mismatch '+case+'/'+label+' predicted='+str(predicted)+' observed='+str(trace))
        print('PASS: all',len(cases)*len(labels),'premeasurement query/variant oracles and row/request counts',flush=True)
        # Randomize both case order and variant order within each repetition.
        rng=random.Random(reg['measurement_seed'])
        for q in cases:
            for label in labels:
                session.set_keyspace(reg['original_B_keyspaces'][label])
                for _ in range(reg['warmup_per_case_and_variant']):
                    got,trace=execute(q,label)
                    if not compare(got,expected[q['s1_case_id']]):raise AssertionError('Warmup oracle mismatch')
        for rep in range(1,reg['rounds_per_case_and_variant']+1):
            sweep=list(cases);rng.shuffle(sweep)
            for q in sweep:
                order=list(labels);rng.shuffle(order)
                if rep%2==0:order.reverse()
                case=q['s1_case_id'];orders.append({'rep':rep,'case_id':case,
                    'variants_order':'|'.join(order)})
                for pos,label in enumerate(order,1):
                    session.set_keyspace(reg['original_B_keyspaces'][label])
                    start=time.perf_counter_ns()
                    got,trace=execute(q,label)
                    elapsed_ms=(time.perf_counter_ns()-start)/1e6
                    if not compare(got,expected[case]):
                        raise AssertionError('Measured correctness failure '+case+'/'+label)
                    pred=predicted_by_case_label[case,label]
                    if (trace['execute_calls'],trace['iterated_rows'])!=pred:
                        raise AssertionError('Measured descriptive count mismatch '+case+'/'+label)
                    raw.append({'case_id':case,'query_id':q['query_id'],
                        'window_days':q['s1_window_days'] if q['s1_window_days'] is not None else 'CONTROL',
                        'variant':label,'rep':rep,'position':pos,
                        'latency_ms':round(elapsed_ms,9),
                        'execute_calls':trace['execute_calls'],'iterated_rows':trace['iterated_rows'],
                        'oracle_cardinality':reg['oracle_cardinality_by_case'][case],
                        'plan_id':by_id[label]['query_routes'][q['query_id']]['plan_id'] if label in by_id else 'REFERENCE',
                        'physical_plan':trace['physical_plan'],
                        'correctness':'PASS'})
            print('S1 rounds',rep,'/',reg['rounds_per_case_and_variant'],flush=True) if rep%10==0 else None
        if len(raw)!=reg['expected_observations']:
            raise AssertionError('Missing raw measurements')
        status='COMPLETE'
    except BaseException as exc:
        write_new(results/'failure_manifest.json',{'status':'FAILED_PRESERVE_EVIDENCE',
            'error_type':type(exc).__name__,'error':str(exc),'partial_count':len(raw),
            'no_rerun_over_same_evidence':True})
        if raw:write_csv_new(results/'partial_latencies.csv',raw)
        raise
    finally:
        cluster.shutdown()
    # Full provenance validation after timing; not a reselection.
    verify(workspace,out)
    summary,comparisons=summary_from_raw(raw,reg)
    write_csv_new(results/'s1_raw_latencies.csv',raw)
    write_csv_new(results/'s1_orders.csv',orders)
    write_csv_new(results/'s1_query_p95.csv',summary)
    write_csv_new(results/'s1_window_comparison.csv',comparisons)
    result_manifest={'status':'S1_WINDOW_SWEEP_COMPLETE',
        'premeasurement_prereg_sha256':sha(Path(out)/'s1_preregister.json'),
        'original_B_rev3_sha256':EXPECTED_B_SHA,
        'query_cases_sha256':sha(Path(out)/'s1_cases.json'),
        'raw_measurements':len(raw),'query_variant_oracles':'PASS_ALL_PRE_AND_POST_TIMING',
        'host':host,'port':port,
        'latency_context':'Post-flush B snapshot; DO NOT compare directly with preflush B p95',
        'independence':'POST_A_B_EXPLORATORY_DIAGNOSTIC',
        'telemetry_scope':'Python Session.execute calls/iterated rows, not Cassandra server IO',
        'started_epoch_seconds':int(begin),'finished_epoch_seconds':int(time.time()),
        'csv_sha256':{p.name:sha(p) for p in results.glob('*.csv')}}
    write_new(results/'manifest.json',result_manifest)
    print('COMPLETE: S1 window sweep results',results,'samples:',len(raw))
    return results


def audit_results(workspace,out):
    """Recompute all S1 metrics and sample/order invariants without Cassandra."""
    import collections
    reg,exp,designs,bm,cases=verify(workspace,out)
    out=Path(out)
    folder=out/'results'
    rm=load(folder/'manifest.json')
    if rm['status']!='S1_WINDOW_SWEEP_COMPLETE':raise ValueError('No complete S1 benchmark')
    if rm['premeasurement_prereg_sha256']!=sha(out/'s1_preregister.json'):
        raise ValueError('S1 preregistration modified after timing')
    for name,digest in rm['csv_sha256'].items():
        if sha(folder/name)!=digest:raise ValueError('S1 output CSV changed: '+name)
    def get_csv(name):
        with (folder/name).open(newline='',encoding='utf8') as f:
            return list(csv.DictReader(f))
    raw=get_csv('s1_raw_latencies.csv')
    orders=get_csv('s1_orders.csv')
    old=get_csv('s1_query_p95.csv')
    comparisons=get_csv('s1_window_comparison.csv')
    if len(raw)!=reg['expected_observations'] or len(orders)!=len(cases)*reg['rounds_per_case_and_variant']:
        raise ValueError('Incomplete raw samples or randomized orders')
    labels=[LABEL_BASE,*reg['selected_ids'].values()]
    if len(set(labels))!=3:raise ValueError('Expected exactly three distinct variants')
    caseset={q['s1_case_id'] for q in cases}
    order_map={}
    for o in orders:
        key=(o['case_id'],int(o['rep']))
        if key in order_map or key[0] not in caseset or not (1<=key[1]<=reg['rounds_per_case_and_variant']):
            raise ValueError('Duplicate or invalid execution order')
        ls=o['variants_order'].split('|')
        if len(ls)!=3 or set(ls)!=set(labels):raise ValueError('Bad variant permutation')
        order_map[key]=ls
    seen=set();groups=collections.defaultdict(list);ops=collections.defaultdict(set)
    for r in raw:
        key=(r['case_id'],r['variant'],int(r['rep']))
        if key in seen:raise ValueError('Duplicate sample identity')
        seen.add(key)
        if r['case_id'] not in caseset or r['variant'] not in labels or r['correctness']!='PASS':
            raise ValueError('Invalid case/variant or failed oracle')
        if r['latency_ms']=='' or float(r['latency_ms'])<=0:
            raise ValueError('Invalid latency')
        order=order_map[r['case_id'],int(r['rep'])]
        pos=int(r['position'])
        if not 1<=pos<=3 or order[pos-1]!=r['variant']:
            raise ValueError('Raw measurement does not agree with order of execution')
        groups[r['case_id'],r['variant']].append(r)
        ops[r['case_id'],r['variant']].add((int(r['execute_calls']),int(r['iterated_rows'])))
        if int(r['oracle_cardinality'])!=reg['oracle_cardinality_by_case'][r['case_id']]:
            raise ValueError('Oracle cardinality disagrees with preregistration')
    if len(seen)!=len(raw) or len(groups)!=len(cases)*3:
        raise ValueError('Some cases/variants are missing')
    if any(len(v)!=1 for v in ops.values()):
        raise ValueError('Different operation counts across repetitions')
    new_sum,new_cmp=summary_from_raw(raw,reg)
    if len(new_sum)!=len(old) or len(new_cmp)!=len(comparisons):
        raise ValueError('Incomplete summary CSV')
    old_by={(x['case_id'],x['variant']):x for x in old}
    for v in new_sum:
        x=old_by.get((v['case_id'],v['variant']))
        if not x:raise ValueError('Summary missing case/variant')
        for key in ['mean_ms','median_ms','p95_ms','p99_ms']:
            if not math.isclose(float(x[key]),float(v[key]),rel_tol=1e-9,abs_tol=1e-8):
                raise ValueError('Recomputed statistic differs: '+v['case_id']+' '+key)
    old_cmp={(x['query_id'],int(x['window_days'])):x for x in comparisons}
    for v in new_cmp:
        x=old_cmp.get((v['query_id'],int(v['window_days'])))
        if not x:raise ValueError('Window comparison missing '+str(v))
        for key in ('p95_baseline_ms','p95_TS2U_ms','p95_TS2D_ms','TS2D_over_TS2U_pct'):
            if not math.isclose(float(x[key]),float(v[key]),rel_tol=1e-9,abs_tol=1e-7):
                raise ValueError('Window comparison statistic differs: '+key)
    report=[
      '# Auditoria S1 — janelas temporais (pós-flush B)',
      '',
      '- **Estado:** PASS — auditados registros, ordens e estatísticas.',
      f'- Medições: **{len(raw)}** de **{reg["expected_observations"]}** previstas.',
      f'- Ordens de execução: **{len(orders)}**.',
      f'- Casos × variantes: **{len(groups)}**; todos com oráculo PASS.',
      '- Mesmo `plan_id` físico da seleção B; parâmetros temporais derivados, não nativos Java adicionais.',
      '- Comparação **exploratória** pós-A/B; não utilizar como confirmação independente.',
      '- Latências do S1 **pós-flush**; não comparar valores absolutos diretamente com B pré-flush.',
      '',
      '## Q2 e Q12: p95 e chamadas CQL',
      '',
      '| Consulta | Janela | TS2-U p95 (ms) | TS2-D p95 (ms) | Chamadas U / D |',
      '|---|---:|---:|---:|---|',
    ]
    for c in new_cmp:
        if c['query_id'] not in ('Q2','Q12'):continue
        report.append(f'| {c["query_id"]} | {c["window_days"]} d | {c["p95_TS2U_ms"]:.4f} | {c["p95_TS2D_ms"]:.4f} | {c["TS2U_execute_calls"]} / {c["TS2D_execute_calls"]} |')
    report.extend(['','Contadores de requisições e linhas: executor Python, não métricas internas do servidor Cassandra.',''])
    target=folder/'AUDITORIA_S1_PT.md'
    if target.exists():raise FileExistsError('Audit report exists; preserve prior audited evidence')
    target.write_text('\n'.join(report),encoding='utf8')
    print('AUDIT PASS:',len(raw),'raw measurements,',len(groups),'case/variant groups')
    print('Report:',target)
    return target


def pack(out,archive):
    import zipfile
    out=Path(out).resolve();archive=Path(archive).resolve()
    reg=load(out/'s1_preregister.json')
    rm=load(out/'results/manifest.json')
    if rm['status']!='S1_WINDOW_SWEEP_COMPLETE':raise ValueError('Only completed S1 can be exported')
    if sha(out/'s1_preregister.json')!=rm['premeasurement_prereg_sha256']:
        raise ValueError('Preregistration changed')
    for name,digest in rm['csv_sha256'].items():
        if sha(out/'results'/name)!=digest:raise ValueError('Result CSV changed '+name)
    if not (out/'results/AUDITORIA_S1_PT.md').is_file():
        raise ValueError('Run S1 audit before packing results')
    if archive.exists():raise FileExistsError('ZIP already exists; do not overwrite')
    if archive.is_relative_to(out):raise ValueError('Archive must be outside S1 evidence directory')
    with zipfile.ZipFile(archive,'x',compression=zipfile.ZIP_DEFLATED,compresslevel=8) as z:
        for f in sorted(out.rglob('*')):
            if f.is_file():z.write(f,arcname=str(f.relative_to(out)))
    print('ARCHIVE:',archive)


def main():
    parser=argparse.ArgumentParser(description='Read-only controlled Rev3 S1 B window sweep')
    parser.add_argument('action',choices=('prepare','verify','run','audit','pack'))
    parser.add_argument('--workspace',type=Path,required=True)
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--archive',type=Path)
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=9043)
    args=parser.parse_args()
    ensure_context(args.workspace)
    if args.action=='prepare':preregister(args.workspace,args.out)
    elif args.action=='verify':
        verify(args.workspace,args.out)
        print('PASS: S1 preregistration and B integrity verified; no Cassandra')
    elif args.action=='run':run_benchmark(args.workspace,args.out,args.host,args.port)
    elif args.action=='audit':audit_results(args.workspace,args.out)
    else:
        if args.archive is None:parser.error('pack requires --archive')
        verify(args.workspace,args.out)
        pack(args.out,args.archive)

if __name__=='__main__':main()
