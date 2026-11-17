"""Run an independently preregistered V4 diagnostic without altering V3 HBench."""
from __future__ import annotations
import argparse
import json
import random
import statistics
import time
from pathlib import Path
from . import core as C


def context(exp,days,extended):
    from iot_pipeline.shared import ROOT,read,path4,require_policy
    if exp.resolve()!=ROOT.resolve():raise ValueError('Imported V3 project path mismatch')
    prior=C.snapshot(exp)
    selected,original,queries,sensors,meta,hashes=C.source(exp)
    captures=list(exp.parent.glob('native_queries_q12fixed_*.jsonl'))
    if len(captures)!=1 or C.digest(captures[0])!=meta['capture_sha256']:
        raise ValueError('Captured native Java Q1-Q12 JSONL absent or hash mismatch')
    meta={**meta,'capture_bytes_verified':True,'capture_filename':captures[0].name}
    specs=read(path4(4,'10_candidate_specs.json'))
    policy=require_policy()
    required={'ts2_iot_wide_unbucketed','ts2_iot_wide','ts4_iot_latest'}
    if not required.issubset({x['candidate_id'] for x in specs}):
        raise ValueError('Frozen V3 did not instantiate required physical families')
    rows=C.create_days(original,days)
    bounds={d:sum(r['device_id']==d for r in rows)//len(sensors) for d in sorted({r['device_id'] for r in rows})}
    # Exploratory V4 guard is explicit and separate from historic V3 guard.
    if max(bounds.values())>50_000:raise ValueError('Unbucketed canonical partition bound exceeded')
    qs=C.generated_queries(queries,rows,extended)
    checks=C.offline_cross_coverage(rows,qs,sensors)
    return prior,selected,rows,qs,sensors,specs,policy,hashes,meta,checks,bounds


def manifest(exp,days,extended,selected,rows,qs,sensors,hashes,meta,checks,bounds,prior):
    addon_root=Path(__file__).resolve().parents[1]
    code_files=['diagnostic_v4/core.py','diagnostic_v4/cli.py','diagnostic_v4/report.py']
    return {'v4_source_sha256':{f:C.digest(addon_root/f) for f in code_files},
       'revision':'V4_EXPLORATORY_CROSS_REUSE_AND_CANONICAL_SOURCE',
       'evidence':'E1-E3 diagnostic motivated design; NEW protocol; not retrospective V3 HBench',
       'scale_days':days,'derived_synthetic_days':days>1,'official_Java_query_objects_original':12,
       'derived_full_span_probe_count':5 if extended else 0,
       'frequency_replay':False,'official_sql_replay':False,
       'candidate_specs':C.plans(qs),'candidate_specification_mode':'FIXED_PREDECLARED_NO_PERFORMANCE_INPUT',
       'coverage_verification':'logical in-memory oracle, live checks required separately',
       'coverage_pass_count':len(checks),'sensor_names':sensors,'native_capture':meta,
       'canonical_partition_rows_per_device':bounds,
       'original_v3_hbench_sha256':C.FROZEN_SHA,
       'original_v3_files_sha256':prior,
       'source_hashes':hashes,'physical_metric_cells':len(rows),
       'limitations':['Reference is an independent Cassandra translation, not official IoTDB or official IoT Benchmark score',
           'Original 12 queries are one captured Java DTO per class, not observed workload sequence',
           'New 7/30-day datasets repeat native values with deterministic time shifts',
           'Do not use V3 observations to claim independent V4 family selection',
           'Sequential loader timings are not official ingestion throughput',
           'TS4 latest materialization assumes offline batch load, not arbitrary concurrent writes',
           'Optimized last-value scan assumes Cassandra clustering timestamp DESC and retains sparse-sensor null semantics'],
       'C0_C9_used':False,'old_stage8_or_stage9_modified':False,
       'all_variants':[C.BASE,C.BASE_OPT,*C.DESIGNS,*[h['deployment_id'] for h in selected]]}


def offline(args):
    exp=args.experiment.resolve()
    base=C.snapshot(exp)
    ctx=context(exp,args.days,args.extended_probes)
    prior,selected,rows,qs,sensors,specs,policy,hashes,meta,checks,bounds=ctx
    m=manifest(exp,args.days,args.extended_probes,selected,rows,qs,sensors,hashes,meta,checks,bounds,prior)
    out=exp/'outputs/diagnostics_v4'
    out.mkdir(parents=True,exist_ok=True)
    p=out/f'V4_PROPOSAL_d{args.days:02d}.json'
    if p.exists():
        old=json.loads(p.read_text())
        if old!=m:raise ValueError('Existing preregistration differs; do not overwrite')
    else:C.save_json(p,m)
    if C.snapshot(exp)!=base:raise AssertionError('V3 inputs changed during preregistration')
    print(f'PASS_OFFLINE: {len(checks)} V4 plan-query semantic checks; {len(rows)} metric cells; {len(qs)} queries')
    print('V3 SHA-256 unchanged:',C.FROZEN_SHA)
    print('V4 candidate plan (no performance-based scoring):',p)


def live(args):
    exp=args.experiment.resolve()
    out=exp/f'outputs/diagnostics_v4/live_d{args.days:02d}'
    if out.exists():raise FileExistsError('Refusing to overwrite any previous V4 live result: '+str(out))
    if args.confirm_reset!=C.RESET:raise ValueError('Explicit confirmation required for diagnostic keyspace resets')
    if args.rounds<10 or args.warmup<0 or args.page_size<1 or args.page_size>500:
        raise ValueError('Rounds >=10, warmup >=0, and page_size 1..500 required')
    ctx=context(exp,args.days,args.extended_probes)
    prior,selected,rows,qs,sensors,specs,policy,hashes,meta,checks,bounds=ctx
    proposal=exp/f'outputs/diagnostics_v4/V4_PROPOSAL_d{args.days:02d}.json'
    expected_proposal=manifest(exp,args.days,args.extended_probes,selected,rows,qs,sensors,hashes,meta,checks,bounds,prior)
    if not proposal.is_file() or json.loads(proposal.read_text())!=expected_proposal:
        raise ValueError('First preregister this exact new V4 scale using --mode offline')
    frozen_by_id={h['deployment_id']:h for h in selected}
    variants=[C.BASE,C.BASE_OPT,*C.DESIGNS]
    if not args.skip_frozen_controls:variants.extend(frozen_by_id)
    # Each new keyspace is determined by an immutable symbolic variant + day scale.
    labels={v:C.safe_keyspace(args.days,v) for v in variants}
    from iot_pipeline.offline import compare
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.stage9 import _materialize
    oracle=source_oracle()
    from cassandra.cluster import Cluster
    cluster=Cluster([args.host],port=args.port)
    data=[];loads=[];timings=[];orders=[]
    rng=random.Random(args.seed+args.days)
    completed=False
    try:
        session=cluster.connect()
        # E4 uses the *same* baseline data table for both algorithms.
        session_keyspaces={}
        for v in variants:
            ks=labels[v];session_keyspaces[v]=ks
            if v==C.BASE_OPT:continue
            C.init_keyspace(session,ks,args.confirm_reset)
            t0=time.perf_counter_ns()
            if v==C.BASE:count=C.load_design(session,v,rows,sensors,specs,policy)
            elif v.startswith('H_'):count=sum(_materialize(session,frozen_by_id[v],rows,sensors,policy).values())
            else:count=C.load_design(session,v,rows,sensors,specs,policy)
            elapsed=(time.perf_counter_ns()-t0)/1e9
            loads.append({'variant':v,'keyspace':ks,'scale_days':args.days,
                          'physical_writes':count,'load_seconds':elapsed,'logical_cells':len(rows)})
            print('Loaded:',v,'keyspace:',ks,'writes:',count,flush=True)
        for q in qs:
            expected=oracle(rows,q)
            for v in variants:
                session.set_keyspace(session_keyspaces[v])
                actual,trace=C.query_result(session,q,sensors,v,
                             {r['timestamp']//C.DAY for r in rows},frozen_by_id,args.page_size)
                if not compare(actual,expected):
                    raise AssertionError(f'V4 live correctness mismatch: {v} {q["_label"]}; abort before timing')
        print(f'PASS: live semantic correctness for {len(variants)} variants × {len(qs)} queries',flush=True)
        # Equal warmup for all routes.
        days_set={r['timestamp']//C.DAY for r in rows}
        for q in qs:
            for v in variants:
                session.set_keyspace(session_keyspaces[v])
                for _ in range(args.warmup):C.query_result(session,q,sensors,v,days_set,frozen_by_id,args.page_size)
            per_variant={v:[] for v in variants}
            for rep in range(1,args.rounds+1):
                if rep%2:
                    order=list(variants);rng.shuffle(order)
                else:order=list(reversed(order))
                orders.append({'query_label':q['_label'],'rep':rep,'order':'|'.join(order)})
                for pos,v in enumerate(order,1):
                    session.set_keyspace(session_keyspaces[v])
                    t0=time.perf_counter_ns()
                    got,trace=C.query_result(session,q,sensors,v,days_set,frozen_by_id,args.page_size)
                    ms=(time.perf_counter_ns()-t0)/1e6
                    record={'query_label':q['_label'],'query_id':q['query_id'],
                      'variant':v,'rep':rep,'position':pos,'latency_ms':ms,
                      'requests':trace['requests'],'iterated_rows':trace['iterated_rows'],'route':trace['route']}
                    per_variant[v].append(ms);data.append(record)
            for v in variants:
                seq=per_variant[v]
                subset=[x for x in data if x['variant']==v and x['query_label']==q['_label']]
                requests=[x['requests'] for x in subset if x['requests'] is not None]
                r_count=[x['iterated_rows'] for x in subset if x['iterated_rows'] is not None]
                timings.append({'variant':v,'query_label':q['_label'],'query_id':q['query_id'],
                  'route':subset[0]['route'],'p95_ms':C.pctl(seq,95),'p99_ms':C.pctl(seq,99),
                  'mean_ms':statistics.mean(seq),'median_ms':statistics.median(seq),
                  'mean_requests':statistics.mean(requests) if requests else '',
                  'mean_iterated_rows':statistics.mean(r_count) if r_count else '',
                  'runs':len(seq),'correctness':'PASS_BEFORE_TIMING'})
            print('Measured:',q['_label'],flush=True)
        completed=True
    finally:cluster.shutdown()
    if completed:
        after=C.snapshot(exp)
        if after!=prior:raise AssertionError('Frozen V3 file/hash changed during V4!')
        out.mkdir(parents=True,exist_ok=False)
        C.save_csv(out/'v4_raw.csv',data)
        C.save_csv(out/'v4_queries.csv',timings)
        C.save_csv(out/'v4_load.csv',loads)
        C.save_csv(out/'v4_orders.csv',orders)
        C.save_csv(out/'v4_aggregate.csv',C.aggregate(timings,qs))
        m=manifest(exp,args.days,args.extended_probes,selected,rows,qs,sensors,hashes,meta,checks,bounds,prior)
        m.update({'status':'LIVE_DIAGNOSTIC_COMPLETE','Cassandra_used':True,
                  'old_v3_hashes_after':after,'old_v3_hashes_unchanged':True,
                  'keyspaces':labels,'rounds':args.rounds,'warmup':args.warmup,
                  'random_seed':args.seed,'q8_fetch_size':args.page_size,
                  'sample_count':len(data),'live_validated_routes':len(variants)*len(qs),
                  'baseline_full_vs_optimized_q8_same_table':True,
                  'frozen_hbench_routes_retained':not args.skip_frozen_controls,
                  'cassandra_reset_scope':'Only sl_diag_v4_d* keyspaces; original Stage9 keyspaces untouched'})
        C.save_json(out/'manifest.json',m)
        print('SUCCESS: V4/E4 measured; original V3 HBench SHA unchanged:',C.FROZEN_SHA)
        print('Results:',out)


def main():
    p=argparse.ArgumentParser(description='SchemaLens V4 diagnostic; never modifies V3 freeze')
    p.add_argument('--experiment',type=Path,required=True)
    p.add_argument('--mode',choices=['offline','live'],required=True)
    p.add_argument('--days',type=int,choices=[1,7,30],default=1)
    p.add_argument('--extended-probes',action='store_true')
    p.add_argument('--host',default='127.0.0.1');p.add_argument('--port',type=int,default=9043)
    p.add_argument('--warmup',type=int,default=5);p.add_argument('--rounds',type=int,default=30)
    p.add_argument('--seed',type=int,default=666);p.add_argument('--page-size',type=int,default=16)
    p.add_argument('--confirm-reset',default='');p.add_argument('--skip-frozen-controls',action='store_true')
    a=p.parse_args();return offline(a) if a.mode=='offline' else live(a)

if __name__=='__main__':main()
