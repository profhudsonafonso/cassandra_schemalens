"""Stage 9.1 command-line tool: a non-official controlled comparison."""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from methodology.tpcx_iot.stage4.candidates import ROOT, sha256
from methodology.tpcx_iot.stage9.experiment import CassandraClient, DurableOutbox, events, keyspace, load_frozen, persist_all, verify
from methodology.tpcx_iot.stage91.target_diagnostic import inspect_target
from methodology.tpcx_iot.stage91.experiment import (
    OUT, BASELINE_PATH, baseline_deployment, capture_storage, compare_baseline,
    audit_materialized_rows, count_all, deployment_order, load_baseline, measure_dynamic,
    summarize_rows, retrospective_quality, write_csv,
)


def save_json(path:Path,data)->None:
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(data,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')


def parser():
    ap=argparse.ArgumentParser(description='Stage9.1 dynamic Cassandra TPCx-IoT-inspired research evaluation')
    ap.add_argument('mode',choices=['dry-run','benchmark','verification','summarize'])
    ap.add_argument('--hosts',default='127.0.0.1')
    ap.add_argument('--port',type=int,default=9042)
    ap.add_argument('--keyspace-prefix',default='sl_tpcx')
    ap.add_argument('--replication-factor',type=int,default=1)
    ap.add_argument('--stations',type=int,default=2)
    ap.add_argument('--sensors',type=int,default=3)
    ap.add_argument('--seed-steps',type=int,default=130)
    ap.add_argument('--bench-steps',type=int,default=500)
    ap.add_argument('--read-threads',type=int,default=2)
    ap.add_argument('--read-operations',type=int,default=120)
    ap.add_argument('--repetitions',type=int,default=3)
    ap.add_argument('--tick-ms',type=int,default=1000)
    ap.add_argument('--pad-bytes',type=int,default=900)
    ap.add_argument('--nodetool-container',default=None)
    ap.add_argument('--require-storage',action='store_true')
    ap.add_argument('--max-full-audit-events',type=int,default=50000)
    ap.add_argument('--require-full-audit',action='store_true')
    ap.add_argument('--reset',action='store_true',help='DESTRUCTIVE: drop experiment-owned keyspaces')
    ap.add_argument('--output',default=None)
    return ap


def selected_designs(root):
    hb,rindex,windex,hashes=load_frozen(root)
    base,rd,wr,ddl=baseline_deployment(root)
    return hb,base,{**rindex,base['deployment_id']:rd},{**windex,base['deployment_id']:wr},ddl,hashes


def all_discarded_designs(root,hb):
    """Optional AFTER-selection verification of full compatible reference space."""
    complete=json.loads((root/'outputs/tpcx_iot_stage8/52_complete_compatible_deployments.json').read_text())
    selected={h['deployment_id'] for h in hb}
    # Only include complete deployments carrying compatible frozen Stage8 route+write plans.
    return [h for h in complete if h['deployment_id'] not in selected]


def main(argv=None):
    a=parser().parse_args(argv)
    root=ROOT
    output=Path(a.output).resolve() if a.output else (root/OUT)
    hb,base,reads,writes,baseline_ddl,hashes=selected_designs(root)
    ap=parser()
    if not (1<=a.stations<=500 and 1<=a.sensors<=200 and 12<=a.seed_steps<=100000 and
            1<=a.bench_steps<=100000 and 1<=a.read_threads<=32 and 1<=a.read_operations<=100000 and
            1<=a.repetitions<=30 and 1<=a.tick_ms<=3600000 and 0<=a.pad_bytes<=1024 and 1<=a.replication_factor<=9 and a.max_full_audit_events>=1):
        ap.error('Invalid experiment size/settings')
    if (a.seed_steps-1)*a.tick_ms<10000:
        ap.error('Seed must span at least ten seconds to fit two disjoint five-second windows')
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',a.nodetool_container or 'safe'):
        ap.error('Unsafe container identifier')
    if a.mode in ('benchmark','verification') and not a.reset:
        ap.error('Benchmark requires --reset to prevent duplicate seed/warmup and carryover')
    if a.require_storage and a.mode in ('benchmark','verification') and not a.nodetool_container:
        ap.error('--require-storage requires --nodetool-container')
    if a.mode=='verification':
        # Verification is explicitly AFTER frozen five-design HBench selection.
        # It is NOT a new selection procedure and cannot alter Stage8.
        complete=json.loads((root/'outputs/tpcx_iot_stage8/52_complete_compatible_deployments.json').read_text())
        if len(complete)!=9 or not {h['deployment_id'] for h in hb}<={h['deployment_id'] for h in complete}:
            raise ValueError('Complete reference space changed or HBench no longer contained')
        for h in complete:
            if (h['route']['Q_READ_2_WINDOWS']['candidate_id'] not in h['tables']
                 or any(t not in reads or t not in writes for t in h['tables'])):
                raise ValueError('Invalid full reference candidate routing')
        designs=complete+[base]
    else:
        designs=hb+[base]
    plan={'status':'PREPARED_NO_MEASUREMENTS','mode':a.mode,'hbench_frozen':True,
          'designs':[{'deployment_id':h['deployment_id'],'tables':h['tables'],
                      'baseline_type':h.get('baseline_provenance','SCHEMALENS_HBENCH')} for h in designs],
          'provenance':{'baseline_sha256':sha256(root/BASELINE_PATH), 'frozen_hashes':hashes},
          'settings':{k:v for k,v in vars(a).items() if k not in ('output',)},
          'official_tpcx_iot':False,'baseline_is_official_cassandra_schema':False,
          'notational_note':'TPCx-IoT logical record mapped to new Cassandra flat-table baseline; not a canonical TPC physical schema',
          'metric_note':'Event-time-driven dynamic windows; equal query count/parameters, append-only sealed checkpoints'}
    save_json(output/'70_stage91_execution_manifest.json',plan)
    if a.mode=='dry-run':
        print(json.dumps(plan,indent=2,ensure_ascii=False))
        return 0
    if a.mode=='summarize':
        input_file=output/'71_stage91_live_runs.json'
        if not input_file.exists():raise FileNotFoundError(f'No live Stage9.1 results at {input_file}')
        saved=json.loads(input_file.read_text())
        if saved.get('status')!='COMPLETE_RESEARCH_ONLY':
            raise ValueError('Cannot summarize partial or failed runs')
        rows=saved['runs']
    else:
        rows=[]
        client=CassandraClient(a.hosts.split(','),a.port)
        try:
            # Fail BEFORE creating/dropping experiment keyspaces when the Python driver
            # and nodetool's Docker container point to different Cassandra instances.
            if a.nodetool_container:
                preflight = inspect_target(client, a.nodetool_container,
                    output_path=output/'69_stage91_target_preflight.json')
                if preflight['status'] != 'MATCH':
                    raise RuntimeError(
                        f'Unsafe Cassandra storage target: {preflight["status"]}. '
                        f'{preflight["message"]} Diagnostic: {preflight["diagnostic_path"]}. '
                        'No benchmark keyspaces were modified.')
            version=client.cassandra_version()
            for rep in range(a.repetitions):
                for h in deployment_order(designs[:-1],base,rep):
                    did=h['deployment_id']
                    if did==base['deployment_id']:
                        ks=f'{a.keyspace_prefix}_b_tpcx_raw'
                        if len(ks)>48 or not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]+',ks):
                            raise ValueError('Unsafe baseline keyspace name')
                        client.session.execute(f'DROP KEYSPACE IF EXISTS {ks}')
                        client.session.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class':'SimpleStrategy','replication_factor':{a.replication_factor}}}")
                        client.session.set_keyspace(ks)
                        client.session.execute(baseline_ddl)
                        client.prepared={}
                    else:
                        ks=client.prepare_deployment(root,h,prefix=a.keyspace_prefix,rf=a.replication_factor,reset=True)
                    ledger_file=output/'outboxes'/f'{did}_repeat{rep}.sqlite'
                    for suffix in ('','-wal','-shm'):
                        ledger_file.with_name(ledger_file.name+suffix).unlink(missing_ok=True)
                    ledger=DurableOutbox(ledger_file)
                    try:
                        seed=events(stations=a.stations,sensors=a.sensors,steps=a.seed_steps,
                                    tick_ms=a.tick_ms,pad_bytes=a.pad_bytes)
                        persist_all(client,h,seed,writes,ledger)
                        if did==base['deployment_id']:
                            # Use same live smoke reference oracle with identical read contract.
                            # Stage9 verification probes assume tick=1000 and large-enough seed.
                            from methodology.tpcx_iot.stage91.experiment import make_probe,expected_rows_from_watermark
                            p=make_probe(a.seed_steps-1,0,sensors=a.sensors,tick_ms=a.tick_ms)
                            got,_,_=client.read_probe(reads[did],p,[f'ST{i:03}' for i in range(a.stations)])
                            exp=expected_rows_from_watermark(p,stations=a.stations,tick_ms=a.tick_ms,pad_bytes=a.pad_bytes)
                            if got!=exp:raise AssertionError('Baseline seed smoke failed')
                        elif a.tick_ms==1000 and a.seed_steps>=130:
                            verify(client,h,reads,seed,a.sensors,a.stations,strict_count=True)
                        else:
                            from methodology.tpcx_iot.stage91.experiment import make_probe,expected_rows_from_watermark
                            p=make_probe(a.seed_steps-1,0,sensors=a.sensors,tick_ms=a.tick_ms)
                            got,_,_=client.read_probe(reads[did],p,[f'ST{i:03}' for i in range(a.stations)])
                            if got!=expected_rows_from_watermark(p,stations=a.stations,tick_ms=a.tick_ms,pad_bytes=a.pad_bytes):
                                raise AssertionError('HBench seed smoke failed')
                        dynamic=measure_dynamic(client,h,reads,writes,ledger,stations=a.stations,
                                    sensors=a.sensors,seed_steps=a.seed_steps,bench_steps=a.bench_steps,
                                    tick_ms=a.tick_ms,pad_bytes=a.pad_bytes,read_threads=a.read_threads,
                                    read_operations=a.read_operations)
                        complete_count=count_all(client,h,a.stations*a.sensors*(a.seed_steps+a.bench_steps))
                        audit=audit_materialized_rows(client,h,stations=a.stations,sensors=a.sensors,
                                  total_steps=a.seed_steps+a.bench_steps,tick_ms=a.tick_ms,
                                  pad_bytes=a.pad_bytes,max_events=a.max_full_audit_events)
                        if a.require_full_audit and not audit['all_source_events_checked']:
                            raise RuntimeError('Full post-ingest content audit was required but skipped')
                        storage=capture_storage(h,ks,container=a.nodetool_container,
                                  output_dir=output/'storage'/f'repeat{rep}')
                        if a.require_storage and storage['live_bytes'] is None:
                            failure_path=output/'79_stage91_storage_failure.json'
                            save_json(failure_path, {'deployment_id':did,'repeat_index':rep,
                                'keyspace':ks,'storage':storage,
                                'note':'The live benchmark may have completed before storage collection failed; do not use as a completed comparison.'})
                            raise RuntimeError(
                                f'Storage capture failed for {did} at repetition {rep+1}: '
                                f'{storage.get("status")}. {storage.get("error", "")}. '
                                f'Diagnostic: {storage.get("diagnostic_path", "unavailable")}; '
                                f'failure summary: {failure_path}')
                        row={'deployment_id':did,'repeat_index':rep,'keyspace':ks,
                             'status':'COMPLETE_RESEARCH_ONLY','cassandra_version':version,
                             'table_counts':complete_count,'audit':audit,'dynamic':dynamic,'storage':storage,
                             'baseline_kind':h.get('baseline_provenance','SCHEMALENS_HBENCH'),
                             'official_IoTps':None,'tpc_compliant':False,
                             'completed_utc':datetime.now(timezone.utc).isoformat()}
                        rows.append(row)
                        save_json(output/('81_stage91_verification_runs.json' if a.mode=='verification' else '71_stage91_live_runs.json'),{'status':'IN_PROGRESS','runs':rows,'settings':plan['settings'],
                                  'hbench_frozen':True,'baseline_not_official_schema':True})
                        print(f'rep={rep+1}/{a.repetitions} {did}: writes={dynamic["source_events"]}, '
                              f'reads={dynamic["read_operations"]}, ingest={dynamic["research_events_per_second"]:.2f}/s, '
                              f'p95={dynamic["read_latency"]["p95_ms"]:.3f}ms storage={storage["live_bytes"]}')
                    finally:
                        ledger.close()
            save_json(output/('81_stage91_verification_runs.json' if a.mode=='verification' else '71_stage91_live_runs.json'),{'status':'COMPLETE_RESEARCH_ONLY','runs':rows,
                       'settings':plan['settings'],'hbench_frozen':True,'baseline_not_official_schema':True})
        finally:
            client.close()
    # Verify the same controlled query sequence is used in every design/run.
    schedule_hashes={r['dynamic']['query_schedule_sha256'] for r in rows}
    if len(schedule_hashes)>1:
        raise AssertionError('Query schedule changed between deployments; discard measurements')
    summary=summarize_rows(rows)
    write_csv(output/('82_stage91_verification_summary.csv' if a.mode=='verification' else '72_stage91_summary.csv'),summary)
    comp=compare_baseline(summary)
    write_csv(output/('83_stage91_verification_vs_baseline.csv' if a.mode=='verification' else '73_stage91_vs_tpc_logical_baseline.csv'),comp,
        ['deployment_id','baseline_id','ingest_ratio_vs_baseline','read_p95_ratio_vs_baseline','storage_ratio_vs_baseline'])
    analysis={'hbench_frozen':True,'n_runs':len(rows),
              'baseline_id':'b_tpcx_logical_raw', 'baseline_official_cassandra':False,
              'summary':summary,'vs_baseline':comp,'topk_regret_evaluated':False,
              'reason':'Full nine compatible deployments are evaluated only in verification mode.'}
    if a.mode=='verification':
        verification_ids=[h['deployment_id'] for h in designs if h['deployment_id']!=base['deployment_id']]
        analysis['retrospective_quality']=retrospective_quality(summary,[h['deployment_id'] for h in hb],verification_ids)
    save_json(output/('84_stage91_retrospective_quality.json' if a.mode=='verification' else '74_stage91_analysis.json'),analysis)
    print(f'Stage9.1 written to {output}; research comparison, NOT official TPCx-IoT.')
    return 0

if __name__=='__main__':
    raise SystemExit(main())
