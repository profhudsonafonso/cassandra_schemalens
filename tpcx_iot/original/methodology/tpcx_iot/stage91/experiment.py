"""Stage 9.1: controlled, dynamic and repeatable Cassandra experiment.

Frozen HBench inputs are NEVER modified. An external TPCx-IoT LOGICAL record
baseline is physically ported to Cassandra and measured using identical
synthetic events and query schedules. This is not TPC-compliant or an official
TPC binding. Tests may use a fake client; live measurements need Cassandra.
"""
from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
import statistics
import subprocess
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from methodology.tpcx_iot.stage4.candidates import ROOT, sha256
from methodology.tpcx_iot.stage5.runtime import ClosedWindow, bind_read_requests
from methodology.tpcx_iot.stage9.experiment import (
    BASE_MS, CassandraClient, DurableOutbox, duration_stats, events, keyspace,
    load_frozen, oracle_rows, persist_all, verify,
)

BASELINE_PATH = 'inputs/baselines/tpcx_logical_raw_cassandra.json'
OUT = 'outputs/tpcx_iot_stage91'


def load_baseline(root: Path = ROOT) -> dict:
    obj = json.loads((Path(root)/BASELINE_PATH).read_text(encoding='utf-8'))
    if obj['baseline_id'] != 'b_tpcx_logical_raw' or obj['status'] != 'RESEARCH_DERIVED_NOT_OFFICIAL_PHYSICAL_SCHEMA':
        raise ValueError('Reference baseline identity changed')
    if obj['schema_cql'].count(';') != 1 or 'ALLOW FILTERING' in obj['schema_cql'].upper():
        raise ValueError('Unsafe baseline DDL')
    return obj


def baseline_deployment(root: Path = ROOT) -> tuple[dict, dict, dict, str]:
    b=load_baseline(root)
    table=b['physical_table']
    columns=['driver_system_id','sensor_name','event_ts','event_id','sensor_reading','padding']
    rd={'plan_id':'qp__baseline','candidate_id':table,'query_id':'Q_READ_2_WINDOWS',
        'read_coverage_status':'CONDITIONAL_OFFLINE_ONLY',
        'interval_bounds':'inclusive','logical_time_ranges':2,
        'read_cql':f'SELECT {", ".join(columns)} FROM {table} WHERE sensor_name = ? AND event_ts >= ? AND event_ts <= ?;',
        'access_mode':'sensor_only','time_bucket_seconds':None,
        'merge_identity':['driver_system_id','sensor_name','event_ts','event_id']}
    wr={'plan_id':'wp__baseline','candidate_id':table,'column_order':columns,
        'write_cql':f'INSERT INTO {table} ({", ".join(columns)}) VALUES (?, ?, ?, ?, ?, ?);',
        'bucket_seconds':None}
    h={'deployment_id':table,'tables':[table],'families':['TPC_LOGICAL_RAW_BASELINE'],
       'canonical_source':table,'baseline_provenance':b['status'],
       'route':{'Q_READ_2_WINDOWS':{'candidate_id':table},
                'Q_INGEST':{'all_required_write_plans':['wp__baseline']}}}
    return h,rd,wr,b['schema_cql']


def deployment_order(hbench: list[dict], baseline: dict, repeat: int) -> list[dict]:
    """Rotate ALL six designs deterministically, reducing systematic run-order effects."""
    all_designs=list(hbench)+[baseline]
    if repeat < 0: raise ValueError('Repeat must be nonnegative')
    pivot=repeat%len(all_designs)
    return all_designs[pivot:]+all_designs[:pivot]


def make_probe(step:int,index:int,*,sensors:int,tick_ms:int=1000)->dict:
    """Frozen data clock: two fully committed 5s windows at a watermark.

    This models temporal locality, not wall-clock TPC timestamp rules. The
    past window is chosen deterministically from the available lookback, at
    most 1800s; ties are stable between repeated design deployments.
    """
    if type(step) is not int or step<11 or index<0 or sensors<1 or tick_ms<=0:
        raise ValueError('At least 12 committed steps required for dynamic probes')
    recent_end=BASE_MS + step*tick_ms
    recent=ClosedWindow(recent_end-5000,recent_end)
    # historical.end <= recent.start, historical.start >= BASE_MS.
    max_gap=min(1_795_000,recent.start_ms - BASE_MS - 5000)
    if max_gap<0: raise ValueError('Insufficient historical samples')
    gap=(int.from_bytes(hashlib.sha256(f'{index}:{step}:lookback'.encode()).digest()[:8],'big')
         % (max_gap+1))
    historical_end=recent.start_ms-gap
    history=ClosedWindow(historical_end-5000,historical_end)
    if not (history.start_ms>=BASE_MS and history.end_ms<=recent.start_ms
            and history.start_ms>=recent.start_ms-1_800_000):
        raise AssertionError('Dynamic window outside allowed range')
    return {'sensor_name':f'sensor_{index%sensors:03}','recent':recent,
            'historical':history,'watermark_step':step,'logical_query_index':index}


def expected_rows_from_watermark(probe:dict,*,stations:int,tick_ms:int=1000,pad_bytes:int=900)->list[dict]:
    """Independent narrow-slice oracle; avoid materializing an entire scale in RAM."""
    all_rows=[]
    for w in (probe['recent'],probe['historical']):
        start=max(0,math.ceil((w.start_ms-BASE_MS)/tick_ms))
        end=min(probe['watermark_step'],(w.end_ms-BASE_MS)//tick_ms)
        if end>=start:
            if end-start>15000:raise ValueError('Reference oracle bounded to 15001 steps per window')
            all_rows.extend(e for e in events(stations=stations,sensors=int(probe['sensor_name'][-3:])+1,
                                               steps=end-start+1,first_step=start,
                                               tick_ms=tick_ms,pad_bytes=pad_bytes)
                            if e['sensor_name']==probe['sensor_name'])
    return oracle_rows(all_rows,probe)


def schedule_by_step(*,seed_steps:int,bench_steps:int,reads:int)->dict[int,list[int]]:
    """Exactly `reads` logical queries at identical post-commit checkpoints."""
    if seed_steps < 12 or bench_steps<1 or reads<1:raise ValueError('Invalid fixed schedule')
    by_step=defaultdict(list)
    for j in range(reads):
        i=max(0,(j+1)*bench_steps//reads-1)
        by_step[i].append(j)
    return dict(by_step)


def _request(client,plan:dict,probe:dict,stations:int,tick_ms:int,pad_bytes:int)->dict:
    t0=time.perf_counter_ns()
    observed,_,cql_count=client.read_probe(plan,probe,[f'ST{x:03}' for x in range(stations)])
    elapsed=(time.perf_counter_ns()-t0)/1e6
    expected=expected_rows_from_watermark(probe,stations=stations,tick_ms=tick_ms,pad_bytes=pad_bytes)
    if observed!=expected:
        raise AssertionError(f'Wrong result for {probe["sensor_name"]}, snapshot {probe["watermark_step"]}: '
                             f'expected {len(expected)}, got {len(observed)}')
    return {'latency_ms':elapsed,'snapshot_step':probe['watermark_step'],
            'query_index':probe['logical_query_index'],
            'result_rows':len(observed),'cql_requests':cql_count,
            'completed_at_ns':time.perf_counter_ns()}


def persist_step(client,h:dict,events_to_insert:list[dict],write_plans:dict,ledger:DurableOutbox)->int:
    """Append a FRESH, previously unseen step without rescanning all old outbox rows.

    Every new event is durable in SQLite before writing physical copies.
    On a failure, the benchmark fails instead of silently acknowledging partial
    materialization. Stage9 repair is available separately, but NOT used to
    hide failed benchmark steps.
    """
    for ev in events_to_insert:ledger.persist(ev)
    completed=0
    for ev in events_to_insert:
        for t in h['tables']:
            client.execute_write(write_plans[t],ev)
            ledger.acknowledge(ev,t)
            completed+=1
    return completed


def measure_dynamic(client,h:dict,reads:dict,writes:dict,ledger:DurableOutbox,
                    *,stations:int,sensors:int,seed_steps:int,bench_steps:int,
                    tick_ms:int=1000,pad_bytes:int=900,read_threads:int=2,
                    read_operations:int=120)->dict:
    """Write new complete timestamp steps and concurrently read sealed snapshots.

    Snapshot watermark is published only AFTER writes to EVERY physical table
    for that step have been acknowledged. Later insert steps cannot modify
    previously sealed windows (append-only, no late arrivals).
    """
    if not 1<=read_threads<=32 or not 1<=read_operations<=100000:
        raise ValueError('Invalid read concurrency or workload size')
    plan=reads[h['route']['Q_READ_2_WINDOWS']['candidate_id']]
    client.get_prepared(plan['read_cql'])
    for table in h['tables']:client.get_prepared(writes[table]['write_cql'])
    jobs=schedule_by_step(seed_steps=seed_steps,bench_steps=bench_steps,reads=read_operations)
    inflight:list[Future]=[]
    finished=[]
    physical_writes=0
    source_writes=0
    checkpoint_trace=[]
    max_pending=max(2,read_threads*3)
    def complete(future):
        finished.append(future.result())
    with ThreadPoolExecutor(max_workers=read_threads) as pool:
        ingest_start=time.perf_counter_ns()
        for i in range(bench_steps):
            evs=events(stations=stations,sensors=sensors,steps=1,
                       first_step=seed_steps+i,tick_ms=tick_ms,pad_bytes=pad_bytes)
            count=persist_step(client,h,evs,writes,ledger)
            source_writes+=len(evs)
            physical_writes+=count
            snapshot_step=seed_steps+i
            for j in jobs.get(i,[]):
                probe=make_probe(snapshot_step,j,sensors=sensors,tick_ms=tick_ms)
                checkpoint_trace.append({'query_index':j,'watermark_step':snapshot_step,
                                         'sensor_name':probe['sensor_name'],
                                         'recent_start_ms':probe['recent'].start_ms,
                                         'recent_end_ms':probe['recent'].end_ms,
                                         'historical_start_ms':probe['historical'].start_ms,
                                         'historical_end_ms':probe['historical'].end_ms})
                inflight.append(pool.submit(_request,client,plan,probe,stations,tick_ms,pad_bytes))
                if len(inflight)>=max_pending:
                    complete(inflight.pop(0))
        ingest_end=time.perf_counter_ns()
        for f in inflight:complete(f)
    ingest_seconds=(ingest_end-ingest_start)/1e9
    if len(finished)!=read_operations or source_writes!=stations*sensors*bench_steps:
        raise AssertionError('Incomplete controlled workload')
    if ledger.unapplied_count(h['tables']):raise AssertionError('Unacknowledged projection writes')
    overlapping=sum(x['completed_at_ns']<=ingest_end for x in finished)
    if overlapping<1:raise AssertionError('No read completed while ingestion was active')
    return {'status':'PASS_DYNAMIC_RESEARCH','source_events':source_writes,
            'physical_insert_statements':physical_writes,'ingest_wall_seconds':round(ingest_seconds,6),
            'research_events_per_second':round(source_writes/ingest_seconds,4),
            'read_operations':len(finished),'read_threads':read_threads,
            'read_latency':duration_stats([x['latency_ms'] for x in finished]),
            'logical_read_requests_total':sum(x['cql_requests'] for x in finished),
            'overlap_reads_completed_during_ingestion':overlapping,
            'query_schedule_sha256':hashlib.sha256(json.dumps(checkpoint_trace,sort_keys=True).encode()).hexdigest(),
            'query_windows_dynamic':True,'snapshot_bound_to_committed_data':True,
            'all_query_results_verified':True,'query_fixed_count':True,
            'tpc_compliant':False,'official_IoTps':None,
            'method_note':'Time advances with committed synthetic event-time; not wall-clock TPC timing.'}


def count_all(client,h:dict,expected:int)->dict:
    actual={}
    for table in h['tables']:
        n=client.count_small_table(table)
        if n!=expected:raise AssertionError(f'Post-ingest row-count mismatch: {table}: {n}/{expected}')
        actual[table]=n
    return actual


def parse_nodetool_bytes(text: str) -> int | None:
    """Read exactly one table's `Space used (live)` from nodetool output.

    Default Cassandra 4.1 output is integer bytes; human units are accepted
    for compatibility, but the benchmark intentionally runs without -H.
    Ambiguous/missing values are rejected rather than silently set to zero.
    """
    found = re.findall(
        r'^\s*Space used \(live\):\s*([\d,]+(?:\.\d+)?)\s*'
        r'(bytes|B|KiB|MiB|GiB|TiB|KB|MB|GB|TB)?\s*$', text, re.M | re.I)
    if len(found) != 1:
        return None
    number, unit = found[0]
    powers = {'': 1, 'B': 1, 'BYTES': 1,
              'KIB': 1024, 'MIB': 1024**2, 'GIB': 1024**3, 'TIB': 1024**4,
              'KB': 1000, 'MB': 1000**2, 'GB': 1000**3, 'TB': 1000**4}
    try:
        value = Decimal(number.replace(',', '')) * powers[unit.upper()]
        return int(value.to_integral_value())
    except (InvalidOperation, ValueError):
        return None


def capture_storage(h: dict, ks: str, *, container: str | None, output_dir: Path,
                    flush: bool = True) -> dict:
    """Measure actual SSTable live bytes and preserve raw nodetool diagnostics.

    Docker/JMX errors, command exit status, missing output, and unparseable
    metrics are returned with a specific status and written to a diagnostic
    JSON file. Do not replace a missing measurement with a synthetic value.
    """
    if container is None:
        return {'status': 'NOT_CAPTURED', 'live_bytes': None,
                'error': 'No Docker container supplied'}
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', container):
        raise ValueError('Unsafe container')
    output_dir.mkdir(parents=True, exist_ok=True)
    details: dict[str, int] = {}
    commands: list[dict] = []
    diag_path = output_dir / 'storage_diagnostic.json'

    def finish(status: str, *, error: str | None = None, table: str | None = None) -> dict:
        result = {'status': status, 'live_bytes': sum(details.values()) if status == 'MEASURED_NODETOOL_LIVE_BYTES' else None,
                  'per_table_live_bytes': details, 'commands': commands,
                  'diagnostic_path': str(diag_path), 'keyspace': ks, 'container': container}
        if error is not None:
            result['error'] = error
        if table is not None:
            result['table'] = table
        if status == 'MEASURED_NODETOOL_LIVE_BYTES':
            result['storage_metric'] = 'Sum of nodetool Space used (live) per table after flush' if flush else 'Sum of nodetool Space used (live) per table without flush'
        diag_path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        return result

    def run(args: list[str], raw_path: Path, timeout: int):
        command = ['docker', 'exec', container, 'nodetool', *args]
        try:
            proc = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
            stdout, stderr, returncode = proc.stdout or '', proc.stderr or '', proc.returncode
            failure = ''
        except (OSError, subprocess.TimeoutExpired) as exc:
            stdout, stderr, returncode = '', str(exc), None
            failure = type(exc).__name__
        raw_path.write_text('COMMAND: ' + ' '.join(command) + '\n'
                            + 'RETURN CODE: ' + str(returncode) + '\nSTDOUT:\n' + stdout
                            + '\nSTDERR:\n' + stderr + '\n', encoding='utf-8')
        commands.append({'command': command, 'returncode': returncode,
                         'stdout_path': str(raw_path), 'stderr_tail': stderr[-1200:],
                         'exception': failure})
        return stdout, stderr, returncode

    if flush:
        out, err, rc = run(['flush', ks], output_dir / f'nodetool_flush_{ks}.txt', 180)
        if rc != 0:
            return finish('FLUSH_FAILED', error=f'nodetool flush exited {rc}: {(err or out)[-1000:]}')
    for table in h['tables']:
        out, err, rc = run(['tablestats', f'{ks}.{table}'],
                           output_dir / f'nodetool_{ks}_{table}.txt', 120)
        if rc != 0:
            return finish('TABLESTATS_FAILED', table=table,
                          error=f'nodetool tablestats exited {rc}: {(err or out)[-1000:]}')
        value = parse_nodetool_bytes(out)
        if value is None:
            return finish('TABLESTATS_UNPARSEABLE', table=table,
                          error='Expected exactly one integer `Space used (live)` line. '
                                'Inspect saved raw output; use nodetool tablestats without -H.')
        details[table] = value
    return finish('MEASURED_NODETOOL_LIVE_BYTES')


def summarize_rows(rows:list[dict])->list[dict]:
    """Summaries never change Stage8 frozen selection. No significance implied."""
    by_design=defaultdict(list)
    for r in rows:
        if r['status']=='COMPLETE_RESEARCH_ONLY':by_design[r['deployment_id']].append(r)
    result=[]
    for did,group in sorted(by_design.items()):
        ing=[r['dynamic']['research_events_per_second'] for r in group]
        lat=[r['dynamic']['read_latency']['p95_ms'] for r in group]
        stor=[r['storage']['live_bytes'] for r in group if r['storage']['live_bytes'] is not None]
        result.append({'deployment_id':did,'runs':len(group),
                       'median_ingest_events_per_s':round(statistics.median(ing),4),
                       'median_read_p95_ms':round(statistics.median(lat),6),
                       'min_ingest_events_per_s':round(min(ing),4),
                       'max_ingest_events_per_s':round(max(ing),4),
                       'min_read_p95_ms':round(min(lat),6),
                       'max_read_p95_ms':round(max(lat),6),
                       'median_storage_live_bytes':int(statistics.median(stor)) if len(stor)==len(group) else None,
                       'storage_runs':len(stor),
                       'baseline_kind':'EXTERNAL_TPC_LOGICAL_PORT' if did=='b_tpcx_logical_raw' else 'SCHEMALENS_HBENCH'})
    return result


def compare_baseline(summary:list[dict])->list[dict]:
    baselines=[r for r in summary if r['deployment_id']=='b_tpcx_logical_raw']
    if len(baselines)!=1:return []
    b=baselines[0]
    result=[]
    for x in summary:
        if x['deployment_id']==b['deployment_id']:continue
        row={'deployment_id':x['deployment_id'],'baseline_id':b['deployment_id'],
             'ingest_ratio_vs_baseline':round(x['median_ingest_events_per_s']/b['median_ingest_events_per_s'],4),
             'read_p95_ratio_vs_baseline':round(x['median_read_p95_ms']/b['median_read_p95_ms'],4)}
        if x['median_storage_live_bytes'] is not None and b['median_storage_live_bytes'] is not None and b['median_storage_live_bytes']>0:
            row['storage_ratio_vs_baseline']=round(x['median_storage_live_bytes']/b['median_storage_live_bytes'],4)
        else:row['storage_ratio_vs_baseline']=None
        result.append(row)
    return result


def write_csv(path:Path,rows:list[dict],headers:list[str]|None=None)->None:
    path.parent.mkdir(parents=True,exist_ok=True)
    fields=headers or (list(rows[0]) if rows else ['deployment_id'])
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)


def retrospective_quality(summary:list[dict],frozen_hbench_ids:list[str],
                          full_reference_ids:list[str])->dict:
    """POST-HOC only: top-k, near-best and regret across nine complete designs.

    The metric is one complete-workload p95 per design, not per-query-scale
    evidence from the historic independent-candidate benchmark.
    """
    index={r['deployment_id']:r for r in summary}
    if set(index)<set(full_reference_ids) or not frozen_hbench_ids:
        raise ValueError('Missing complete-reference measurements')
    ref=sorted((index[i] for i in full_reference_ids),key=lambda r:(r['median_read_p95_ms'],r['deployment_id']))
    best=ref[0]
    best_kept=min((index[i] for i in frozen_hbench_ids),key=lambda r:(r['median_read_p95_ms'],r['deployment_id']))
    regret=(best_kept['median_read_p95_ms']-best['median_read_p95_ms'])/best['median_read_p95_ms']
    top3={x['deployment_id'] for x in ref[:3]}
    pareto=[]
    for x in ref:
        dominated=any((y['median_read_p95_ms']<=x['median_read_p95_ms'] and
                       y['median_ingest_events_per_s']>=x['median_ingest_events_per_s'] and
                       (y['median_read_p95_ms']<x['median_read_p95_ms'] or
                        y['median_ingest_events_per_s']>x['median_ingest_events_per_s']))
                      for y in ref if y['deployment_id']!=x['deployment_id'])
        if not dominated:pareto.append(x['deployment_id'])
    return {'reference_designs':len(full_reference_ids), 'retained_designs':len(frozen_hbench_ids),
            'deployment_space_reduction':1-len(frozen_hbench_ids)/len(full_reference_ids),
            'reference_p95_best':best['deployment_id'],
            'retained_p95_best':best_kept['deployment_id'],
            'top1_preserved':best['deployment_id'] in frozen_hbench_ids,
            'top3_intersection':bool(top3.intersection(frozen_hbench_ids)),
            'near_best_within_5_percent':regret<=0.05,
            'p95_relative_regret':round(regret,6),
            'reference_2d_pareto_designs':pareto,
            'reference_2d_pareto_retained':sorted(set(pareto).intersection(frozen_hbench_ids)),
            'all_reference_2d_pareto_retained':set(pareto)<=set(frozen_hbench_ids),
            'objectives':'Median ingest events/sec maximize, median per-run p95 minimize',
            'limitations':'One workload and one scale, median of repetitions; 5% is operational, not statistical equivalence; storage omitted from 2D Pareto',
            'selection_modified_after_measurement':False}


def audit_materialized_rows(client,h:dict,*,stations:int,sensors:int,total_steps:int,
                            tick_ms:int,pad_bytes:int,max_events:int=50000)->dict:
    """Full post-ingest content audit, not just COUNT(*), for bounded sizes.

    On >max_events the audit is explicitly SKIPPED; no completeness claim is
    made. Full table scan is for controlled academic sizes only. A production
    scale needs an external correctness sampling protocol or partition audits.
    """
    n=stations*sensors*total_steps
    if n>max_events:
        return {'status':'SKIPPED_EXCEEDS_LIMIT','expected_records':n,
                'max_events':max_events,'all_source_events_checked':False}
    expected={}
    for step in range(total_steps):
        for e in events(stations=stations,sensors=sensors,steps=1,first_step=step,
                        tick_ms=tick_ms,pad_bytes=pad_bytes):
            expected[e['event_id']]=(e['driver_system_id'],e['sensor_name'],e['event_ts'],
                                     e['event_id'],e['sensor_reading'],e['padding'])
    if len(expected)!=n:raise AssertionError('Nonunique source IDs in audit generator')
    result={}
    fields='driver_system_id, sensor_name, event_ts, event_id, sensor_reading, padding'
    for table in h['tables']:
        observed={}
        for row in client.session.execute(f'SELECT {fields} FROM {table}'):
            values=tuple(getattr(row,k) for k in ('driver_system_id','sensor_name','event_ts',
                                                   'event_id','sensor_reading','padding'))
            if values[3] in observed:raise AssertionError(f'Duplicate event UUID in {table}')
            observed[values[3]]=values
        if observed!=expected:
            extra=set(observed)-set(expected)
            missing=set(expected)-set(observed)
            incorrect=[k for k in set(observed)&set(expected) if observed[k]!=expected[k]]
            raise AssertionError(f'Full content mismatch {table}: extra={len(extra)}, '
                                 f'missing={len(missing)}, changed={len(incorrect)}')
        result[table]={'rows_checked':len(observed),'full_content_match':True}
    return {'status':'PASS_ALL_RECORDS_ALL_PHYSICAL_TABLES',
            'expected_records':n,'max_events':max_events,
            'all_source_events_checked':True,'tables':result}
