"""E1/E2 read-only; E3 exclusively resets named diagnostic keyspaces."""
from __future__ import annotations
import json
import random
import statistics
import time
from pathlib import Path
from .core import (
    BASELINE_ID, DAY, ReadTrace, TraceSession, all_deployments,
    check_readonly_sql, collapse, connect, create_synthetic_days, double_check,
    ensure_tables, find_ts2u, hash_file, keyspace_h, probes,
    protect_output, quantile, read_context, run_path, write_csv, write_json
)


def _oracle():
    from iot_pipeline.native_reference_v3 import source_oracle
    return source_oracle()


def _record_sample(variant,query_label,index,metrics):
    return {'variant':variant,'query_id':query_label,'rep':index,**{k:v for k,v in metrics.items() if k!='cql'}}


def _require_older_tables(cluster,selected):
    from iot_pipeline.native_reference_v3 import TABLE
    ensure_tables(cluster,'sl_iot_ref_native_v3',[TABLE])
    for h in selected:ensure_tables(cluster,keyspace_h(h),h['candidates'])


def e1(args,out,base):
    """Timing breakdown for original Stage9 routes, no Cassandra mutations."""
    protect_output(out)
    selected,rows,queries,sensors,_,hashes=read_context();oracle=_oracle()
    cluster=connect(args.host,args.port)
    raw=[];summ=[];requests=[]
    try:
        _require_older_tables(cluster,selected)
        session=cluster.connect()
        allv=[(BASELINE_ID,None,'sl_iot_ref_native_v3','reference')]
        allv += [(h['deployment_id'],h,keyspace_h(h),'hbench') for h in selected]
        for variant,h,ks,mode in allv:
            session.set_keyspace(ks)
            for q in queries:
                label=q['query_id']
                correct=oracle(rows,q)
                got,stat=run_path(session,q,mode,sensors,h)
                double_check(got,correct,f'E1 {variant} {label}')
                for _ in range(args.warmup):run_path(session,q,mode,sensors,h)
                reps=[]
                for rep in range(1,args.rounds+1):
                    got,stat=run_path(session,q,mode,sensors,h)
                    # Correctness performed before timing; identical code path in repeats.
                    reps.append(stat);raw.append(_record_sample(variant,label,rep,stat))
                    if rep==1:
                        for cql in stat['cql']:
                            requests.append({'variant':variant,'query_id':label,'cql':cql})
                summ.append({'variant':variant,'query_id':label,'family':(h or {}).get('query_routes',{}).get(label,{}).get('family','REFERENCE'),**collapse(reps)})
            print('E1 complete:',variant)
    finally:cluster.shutdown()
    write_csv(out/'e1_raw.csv',raw);write_csv(out/'e1_summary.csv',summ)
    write_csv(out/'e1_cql.csv',requests)
    write_json(out/'manifest.json',{**base,**hashes,'experiment':'E1',
        'semantics':'Read-only original frozen query routes (no rerouting)',
        'timing':'db_fetch_ms is client session.execute + full row materialization; NOT server latency',
        'decode_ms':'Client mapping/reconstruction plus non-CQL overhead',
        'logic_ms':'Python result evaluator; totals measured end-to-end',
        'response_repr_bytes':'Approximate Python repr size, NOT true network bytes',
        'rounds':args.rounds,'warmup':args.warmup,'Cassandra_mutations':False,'output_count':len(raw)})


def equal_projection_query(s,q,sensors,table):
    """The SAME CQL template for baseline & TS2-U, except FROM table name.

    Full per-device row requests are used for Q5/Q8. Q12 uses independent child
    predicates, unlike the original Stage9 reader's union-envelope approach.
    """
    from iot_pipeline.native_reference_v3 import requests_for
    from iot_pipeline.reference_baseline import columns_for
    allowed=set(columns_for(sensors))
    trace=ReadTrace();session=TraceSession(s,trace)
    from iot_pipeline.native_reference_v3 import source_oracle
    oracle=source_oracle()
    started=time.perf_counter_ns()
    result={}
    for device,projection,start,end in requests_for(q):
        cols=['m_'+x for x in projection]
        if not set(cols)<=allowed:raise ValueError('Unknown sensor projection')
        # Table is internal constant, verified by caller, NEVER arbitrary user text.
        if table not in {'iot_reference_table','ts2_iot_wide_unbucketed'}:
            raise ValueError('Illegal E2 table')
        cql=f"SELECT timestamp_ms,{','.join(cols)} FROM {table} WHERE device_id=%s"
        params=(device,)
        if start is not None:
            cql+=' AND timestamp_ms >= %s AND timestamp_ms < %s'
            params+=(start,end)
        for row in session.execute(cql,params):
            t=int(row.timestamp_ms)
            for sensor,col in zip(projection,cols):
                value=getattr(row,col,None)
                if value is not None:
                    result[(device,sensor,t)]={'device_id':device,'sensor_name':sensor,'timestamp':t,'value_double':float(value)}
    mapped=time.perf_counter_ns()
    got=oracle(list(result.values()),q)
    ended=time.perf_counter_ns()
    return got,{'total_ms':(ended-started)/1e6,'db_fetch_ms':trace.ns/1e6,
                'decode_ms':max(0,mapped-started-trace.ns)/1e6,'logic_ms':(ended-mapped)/1e6,
                'requests':trace.calls,'cql_rows':trace.returned_rows,
                'response_repr_bytes':trace.value_bytes_approx,'cql':trace.cql}


def _content_equal(s,rows,sensors):
    """Compare complete device partitions from both physical tables to native data."""
    from iot_pipeline.reference_baseline import build_wide_records
    expected=sorted(build_wide_records(rows,sensors),key=lambda r:(r[0],r[1]))
    keys={}
    for keyspace,table in [('sl_iot_ref_native_v3','iot_reference_table'),
                           ('sl_iot_h_4f73edf0446d','ts2_iot_wide_unbucketed')]:
        s.set_keyspace(keyspace)
        received=[]
        for device in sorted({r['device_id'] for r in rows}):
            sql='SELECT timestamp_ms,'+','.join('m_'+x for x in sensors)+f' FROM {table} WHERE device_id=%s'
            for dbrow in s.execute(sql,(device,)):
                received.append(tuple([device,int(dbrow.timestamp_ms)]+[getattr(dbrow,'m_'+x) for x in sensors]))
        received=sorted(received,key=lambda r:(r[0],r[1]))
        if len(expected)!=len(received):raise AssertionError(f'E2 incomplete physical table {table}: {len(received)} vs {len(expected)}')
        for exp,got in zip(expected,received):
            if exp[:2]!=got[:2] or any((a is None) != (b is None) or (a is not None and a!=b) for a,b in zip(exp[2:],got[2:])):
                raise AssertionError('E2 physical table values differ from native source')
        keys[table]=len(received)
    return keys


def e2(args,out,base):
    """Paired AB/BA read-only controlled experiment with identical CQL projection."""
    protect_output(out)
    selected,rows,queries,sensors,_,hashes=read_context();h=find_ts2u(selected)
    if h['deployment_id']!='H_4f73edf0446d':raise ValueError('Unexpected TS2-U frozen ID')
    oracle=_oracle();cluster=connect(args.host,args.port)
    raw=[];summ=[];orders=[]
    labels=[('BASELINE','sl_iot_ref_native_v3','iot_reference_table'),
            ('TS2U',keyspace_h(h),'ts2_iot_wide_unbucketed')]
    rng=random.Random(args.seed)
    try:
        ensure_tables(cluster,labels[0][1],[labels[0][2]])
        ensure_tables(cluster,labels[1][1],[labels[1][2]])
        s=cluster.connect();size=_content_equal(s,rows,sensors)
        for q in queries:
            expected=oracle(rows,q)
            for label,ks,table in labels:
                s.set_keyspace(ks)
                got,_=equal_projection_query(s,q,sensors,table)
                double_check(got,expected,f'E2 content {label} {q["query_id"]}')
                for _ in range(args.warmup):equal_projection_query(s,q,sensors,table)
            observations={x[0]:[] for x in labels}
            for rep in range(1,args.rounds+1):
                if rep%2==1:
                    first_order=list(labels);rng.shuffle(first_order)
                order=first_order if rep%2==1 else list(reversed(first_order))
                orders.append({'query_id':q['query_id'],'pair':rep,'order':'-'.join(x[0] for x in order)})
                for label,ks,table in order:
                    s.set_keyspace(ks)
                    got,result=equal_projection_query(s,q,sensors,table)
                    observations[label].append(result)
                    raw.append({'variant':label,'query_id':q['query_id'],'rep':rep,
                        'position':1+order.index((label,ks,table)),**{k:v for k,v in result.items() if k!='cql'}})
            b=observations['BASELINE'];t=observations['TS2U']
            diffs=[x['total_ms']-y['total_ms'] for x,y in zip(t,b)]
            summ.append({'query_id':q['query_id'],'baseline_p95_ms':quantile([x['total_ms'] for x in b],95),
                'ts2u_p95_ms':quantile([x['total_ms'] for x in t],95),
                'paired_mean_ts2u_minus_baseline_ms':statistics.mean(diffs),
                'paired_median_ts2u_minus_baseline_ms':statistics.median(diffs),
                'ts2u_lower_latency_pairs':sum(v<0 for v in diffs),
                'pairs':len(diffs),'baseline_mean_requests':statistics.mean(x['requests'] for x in b),
                'ts2u_mean_requests':statistics.mean(x['requests'] for x in t)})
            print('E2 paired:',q['query_id'])
    finally:cluster.shutdown()
    write_csv(out/'e2_raw_pairs.csv',raw);write_csv(out/'e2_summary.csv',summ)
    write_csv(out/'e2_orders.csv',orders)
    write_json(out/'manifest.json',{**base,**hashes,'experiment':'E2',
        'comparison':'Identical sensor projection/CQL predicates/decoder/logic, table name is the sole SQL difference',
        'native_table_row_counts_verified':size,'paired_order_seed':args.seed,
        'rounds_per_query_per_variant':args.rounds,'warmup':args.warmup,
        'cassandra_mutations':False,'hbench_routing_unchanged':True,
        'caveat':'E2 is a controlled physical-equivalence diagnosis, NOT the native HBench workload routing'})


def _diagnostic_keyspace(name,days):
    ks=f'sl_diag_e3_{name}_d{days:02d}'
    import re
    if not re.fullmatch('sl_diag_e3_[a-z0-9_]+_d[0-9]{2}',ks):
        raise ValueError('Unsafe diagnostic keyspace')
    return ks


def _create_diagnostic(session,ks,confirm):
    if confirm!='RESET_DIAGNOSTIC_KEYSPACES_ONLY':
        raise ValueError('Live E3 keyspace reset requires explicit diagnostic confirmation')
    if not ks.startswith('sl_diag_e3_'):
        raise ValueError('Refusing to modify non-diagnostic keyspace')
    session.execute(f'DROP KEYSPACE IF EXISTS {ks}')
    session.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}} AND durable_writes=true")
    session.set_keyspace(ks)


def _load_ref(session,rows,sensors):
    from iot_pipeline.reference_baseline import build_wide_records,columns_for,table_ddl,TABLE
    session.execute(table_ddl(sensors));wide=build_wide_records(rows,sensors)
    cols=['device_id','timestamp_ms',*columns_for(sensors)]
    sql=f"INSERT INTO {TABLE} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})"
    prepared=session.prepare(sql)
    for row in wide:session.execute(prepared,row)
    return len(wide)


def e3(args,out,base):
    """Synthetic time-axis extension; paired/interleaved measures with frozen routes."""
    protect_output(out)
    if args.confirm_diagnostic_reset!='RESET_DIAGNOSTIC_KEYSPACES_ONLY':
        raise ValueError('E3 may reset ONLY sl_diag_e3_* keyspaces; confirmation required')
    selected,original,queries,sensors,_,hashes=read_context()
    from iot_pipeline.stage9 import _materialize
    from iot_pipeline.shared import require_policy
    oracle=_oracle();policy=require_policy()
    rows=create_synthetic_days(original,args.days)
    queryset=probes(queries,rows,args.days)
    variants=[('BASELINE',None,_diagnostic_keyspace('reference',args.days),'reference')]
    variants.extend((h['deployment_id'],h,_diagnostic_keyspace('h_'+h['deployment_id'][2:],args.days),'hbench') for h in selected)
    cluster=connect(args.host,args.port)
    raw=[];summ=[];loads=[];orders=[]
    rng=random.Random(args.seed+args.days)
    try:
        s=cluster.connect()
        # Phase A: materialize NEW E3 shadow keyspaces. Original Stage9 is read-only.
        for variant,h,ks,kind in variants:
            _create_diagnostic(s,ks,args.confirm_diagnostic_reset)
            started=time.perf_counter_ns()
            if kind=='reference': count=_load_ref(s,rows,sensors)
            else: count=sum(_materialize(s,h,rows,sensors,policy).values())
            elapsed=(time.perf_counter_ns()-started)/1e9
            loads.append({'scale_days':args.days,'variant':variant,'logical_metric_cells':len(rows),
                          'physical_writes':count,'load_seconds':elapsed,'keyspace':ks})
            print('E3 materialized:',args.days,'days',variant,'writes',count)
        # Phase B: validate every variant/query combination BEFORE measured runs.
        for q in queryset:
            label=q['_diagnostic_query_label'];wanted=oracle(rows,q)
            if label.startswith('EXTENDED_Q12'):
                child_bounds={(ch['start_ms'],ch['end_ms_exclusive']) for ch in q['set_children']}
                if len(child_bounds)!=1:
                    raise AssertionError('Derived Q12 must use identical child windows with current Stage9 reader')
            for variant,h,ks,kind in variants:
                s.set_keyspace(ks)
                got,_=run_path(s,q,kind,sensors,h)
                double_check(got,wanted,f'E3 {args.days}d {variant} {label}')
        # Phase C: randomized balanced rounds per query. Same frozen routing.
        for q in queryset:
            label=q['_diagnostic_query_label']
            for variant,h,ks,kind in variants:
                s.set_keyspace(ks)
                for _ in range(args.warmup):run_path(s,q,kind,sensors,h)
            by_variant={v[0]:[] for v in variants}
            for rep in range(1,args.rounds+1):
                position_in_block=(rep-1)%len(variants)
                if position_in_block==0:
                    latin_base=list(variants);rng.shuffle(latin_base)
                order=latin_base[position_in_block:]+latin_base[:position_in_block]
                orders.append({'scale_days':args.days,'query_id':label,'round':rep,
                               'order':'|'.join(x[0] for x in order)})
                for position,(variant,h,ks,kind) in enumerate(order,1):
                    s.set_keyspace(ks)
                    _,stat=run_path(s,q,kind,sensors,h)
                    by_variant[variant].append(stat)
                    raw.append({'scale_days':args.days,'variant':variant,'query_id':label,
                                'rep':rep,'position':position,
                                **{k:v for k,v in stat.items() if k!='cql'}})
            for variant,h,ks,kind in variants:
                summ.append({'scale_days':args.days,'variant':variant,'query_id':label,
                             'native_fixed':label.startswith('NATIVE_FIXED'),
                             **collapse(by_variant[variant])})
            print('E3 measured:',args.days,'days',label)
    finally:cluster.shutdown()
    write_csv(out/'e3_raw.csv',raw);write_csv(out/'e3_summary.csv',summ)
    write_csv(out/'e3_load.csv',loads);write_csv(out/'e3_orders.csv',orders)
    write_json(out/'manifest.json',{**base,**hashes,'experiment':'E3','scale_days':args.days,
        'extension':'SYNTHETIC_REPLICATION_OF_NATIVE_ONE_DAY_POINTS',
        'original_metric_cells':len(original),'scaled_metric_cells':len(rows),
        'fixed_query_sample_count':len(queries),'derived_probe_count':len(queryset)-len(queries),
        'new_keyspaces':[x[2] for x in variants],'original_keyspaces_mutated':False,
        'frozen_hbench_modified':False,'same_selected_deployments':True,
        'scope':'Sensitivity to temporal series length and multi-day access, NOT official IoT Benchmark score',
        'measurement_rounds':args.rounds,'warmup':args.warmup,
        'paired_order_seed':args.seed+args.days,
        'execution_order':'Randomized balanced five-position Latin rotations across five designs; fixed query-class order',
        'limitations':['Synthetic day offsets repeat native measurements; not a new official generated dataset',
                       'Fixed Java-sample queries and five extended probes labelled separately',
                       'Sequential Python writes are not ingestion throughput',
                       'Derived Q12 uses identical child windows to preserve current Stage9 routing',
                       'Fixed query-class order and shared Cassandra resources remain potential confounders',
                       'Synthetic scales should not be treated as independent datasets']})
