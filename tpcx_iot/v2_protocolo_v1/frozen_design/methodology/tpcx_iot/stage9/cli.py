"""CLI for Stage9 dry-run, Cassandra smoke verification and mixed pilot benchmark."""
from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
from pathlib import Path
from datetime import datetime,timezone

from methodology.tpcx_iot.stage4.candidates import ROOT
from methodology.tpcx_iot.stage9.experiment import (
    CassandraClient, DurableOutbox, benchmark_one, events, keyspace, load_frozen,
    manifest, persist_all, probes, verify,
)


def write_json(path:Path,obj)->None:
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')


def build_arg_parser()->argparse.ArgumentParser:
    ap=argparse.ArgumentParser(description='SchemaLens Stage9 Cassandra research-only runner')
    ap.add_argument('mode',choices=['dry-run','smoke','benchmark','repair'])
    ap.add_argument('--hosts',default='127.0.0.1',help='Cassandra comma-separated contact points')
    ap.add_argument('--port',type=int,default=9042)
    ap.add_argument('--replication-factor',type=int,default=1)
    ap.add_argument('--keyspace-prefix',default='sl_tpcx')
    ap.add_argument('--stations',type=int,default=2)
    ap.add_argument('--sensors',type=int,default=3)
    ap.add_argument('--seed-steps',type=int,default=130)
    ap.add_argument('--bench-steps',type=int,default=80)
    ap.add_argument('--read-threads',type=int,default=2)
    ap.add_argument('--nodetool-container',default=None,help='Optional running Docker container for raw nodetool tablestats capture')
    ap.add_argument('--reset',action='store_true',help='DESTRUCTIVE: drop experiment-only keyspaces and local SQLite outboxes')
    ap.add_argument('--output',default=None,help='Where to write Stage9 runtime results (not Stage8 inputs)')
    return ap


def main(argv:list[str]|None=None)->int:
    ap=build_arg_parser()
    a=ap.parse_args(argv)
    root=ROOT
    output=Path(a.output) if a.output else root/'outputs/tpcx_iot_stage9'
    hb,reads,writes,hashes=load_frozen(root)
    if not 64<=a.seed_steps<=100000:
        ap.error('--seed-steps must be >=64 for the boundary-crossing probe')
    if a.read_threads<1 or a.read_threads>20 or not 1<=a.bench_steps<=100000:
        ap.error('Invalid concurrency or benchmark size')
    if a.mode in ('smoke','benchmark') and not a.reset:
        ap.error('Live experiments require --reset (drop only generated sl_tpcx_* keyspaces) for clean, reproducible state')
    if a.mode=='repair' and a.reset:
        ap.error('The repair mode must reuse existing keyspaces and outbox; do not pass --reset')
    if a.nodetool_container and not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',a.nodetool_container):
        ap.error('Unsafe Docker container identifier')
    settings={'nodetool_container':a.nodetool_container,'profile':'sensor-name-only/raw_read (unverified in official kit)',
              'stations':a.stations,'sensors':a.sensors,'seed_steps':a.seed_steps,
              'bench_steps':a.bench_steps,'read_threads':a.read_threads,
              'hosts':a.hosts,'port':a.port,'rf':a.replication_factor,
              'prefix':a.keyspace_prefix,'reset':a.reset,
              'data_source':'deterministic_synthetic_not_official_tpc_generator',
              'main_measurements_include_local_sqlite_outbox':True}
    plan={'manifest':manifest(root,hashes,mode=a.mode,config=settings,
                              status='PREPARED_NOT_MEASURED' if a.mode=='dry-run' else 'STARTED'),
          'hbench':[{'deployment_id':h['deployment_id'],'tables':h['tables'],
                     'read_candidate':h['route']['Q_READ_2_WINDOWS']['candidate_id'],
                     'keyspace':keyspace(h,a.keyspace_prefix)} for h in hb]}
    write_json(output/'60_stage9_execution_manifest.json',plan)
    if a.mode=='dry-run':
        print(json.dumps(plan,indent=2,ensure_ascii=False))
        print('DRY RUN ONLY: no Cassandra connection, no latency/throughput numbers.')
        return 0

    seed=events(stations=a.stations,sensors=a.sensors,steps=a.seed_steps)
    new=events(stations=a.stations,sensors=a.sensors,steps=a.bench_steps,first_step=a.seed_steps)
    report={'run_time_utc':datetime.now(timezone.utc).isoformat(),'mode':a.mode,
            'research_only':True,'tpc_compliant':False,'hbench_frozen':True,
            'replication_factor':a.replication_factor,'actual_cassandra_live_run':False,
            'status':'IN_PROGRESS','deployments':[]}
    client=None
    try:
        client=CassandraClient(a.hosts.split(','),a.port)
        report['cassandra_version']=client.cassandra_version()
        report['actual_cassandra_live_run']=True
        for h in hb:
            ks=keyspace(h,a.keyspace_prefix)
            outbox_path=output/'local_outbox'/f'{ks}.sqlite'
            if a.reset:
                # Outbox is experiment state, MUST reset together with keyspace.
                for suffix in ('','-wal','-shm'):
                    outbox_path.with_name(outbox_path.name+suffix).unlink(missing_ok=True)
            if a.mode=='repair' and not outbox_path.exists():
                report['deployments'].append({'deployment_id':h['deployment_id'],
                                               'repair_status':'SKIPPED_NO_PRIOR_LEDGER',
                                               'storage_capture_status':'NOT_REQUESTED'})
                continue
            client.prepare_deployment(root,h,prefix=a.keyspace_prefix,
                                      rf=a.replication_factor,reset=a.reset)
            ledger=DurableOutbox(outbox_path)
            try:
                persisted,seconds=persist_all(client,h,[] if a.mode=='repair' else seed,writes,ledger)
                checkpoint=verify(client,h,reads,seed,a.sensors,a.stations,strict_count=a.mode!='repair')
                result={'deployment_id':h['deployment_id'],'keyspace':ks,
                        'seed_source_events':len(seed),'physical_seed_inserts':persisted,
                        'seed_wall_seconds':round(seconds,6),'smoke_validation':checkpoint}
                if a.mode=='benchmark':
                    result['mixed_workload']=benchmark_one(client,h,reads,writes,seed,new,ledger,
                                            stations=a.stations,sensors=a.sensors,
                                            read_threads=a.read_threads)
                result['outbox_unapplied_mutations']=ledger.unapplied_count(h['tables'])
                if result['outbox_unapplied_mutations']!=0:
                    raise RuntimeError('Some materialized projections are not acknowledged')
                result['storage_capture_status']='NOT_REQUESTED'
                if a.nodetool_container:
                    # Raw Cassandra values are saved, never inferred from Stage7 proxies.
                    proc=subprocess.run(['docker','exec',a.nodetool_container,'nodetool','tablestats',ks],
                                        capture_output=True,text=True,timeout=90,check=False)
                    if proc.returncode != 0:
                        result['storage_capture_status']='CAPTURE_FAILED'
                        result['storage_capture_error']=proc.stderr[-500:]
                    else:
                        stat_file=output/f'64_nodetool_{h["deployment_id"]}.txt'
                        stat_file.write_text(proc.stdout,encoding='utf-8')
                        result['storage_capture_status']='RAW_NODETOOL_CAPTURED'
                        result['storage_capture_path']=str(stat_file)
                report['deployments'].append(result)
                # Always persist after a successfully completed deployment.
                write_json(output/('62_stage9_repair_results.json' if a.mode=='repair' else '61_stage9_live_results.json'),report)
                print(f"{a.mode}: {h['deployment_id']} passed live read/write probe; "
                      f"tables={len(h['tables'])} records={len(seed)}")
            finally:
                ledger.close()
        report['status']='COMPLETE_REPAIR_ATTEMPT' if a.mode=='repair' else 'COMPLETE_RESEARCH_ONLY'
        write_json(output/('62_stage9_repair_results.json' if a.mode=='repair' else '61_stage9_live_results.json'),report)
        summary=output/'63_stage9_summary.csv'
        with summary.open('w',newline='',encoding='utf-8') as f:
            writer=csv.DictWriter(f,fieldnames=['deployment_id','mode','smoke_correct',
                                                 'insert_statements_per_event','measured_events',
                                                 'research_ingest_rows_per_second',
                                                 'read_p95_ms','read_p99_ms','read_count',
                                                 'storage_capture_status','tpc_compliant'])
            writer.writeheader()
            for dep in report['deployments']:
                m=dep.get('mixed_workload',{})
                writer.writerow(dict(deployment_id=dep['deployment_id'],mode=a.mode,
                    smoke_correct=dep.get('smoke_validation',{}).get('status')=='PASS_LIVE_SEE_LIMITATIONS',
                    insert_statements_per_event=len(next(h for h in hb if h['deployment_id']==dep['deployment_id'])['tables']),
                    measured_events=m.get('measured_source_events'),
                    research_ingest_rows_per_second=m.get('research_ingest_rows_per_second'),
                    read_p95_ms=m.get('logical_read_latency',{}).get('p95_ms'),
                    read_p99_ms=m.get('logical_read_latency',{}).get('p99_ms'),
                    read_count=m.get('logical_two_window_reads_completed'),
                    storage_capture_status=dep.get('storage_capture_status'),
                    tpc_compliant=False))
        print(f'{a.mode}: processed the frozen HBench set; NOT an official TPCx-IoT result')
        return 0
    except Exception as exc:
        report['status']='FAILED_NOT_REPORTABLE'
        report['error']=str(exc)
        write_json(output/('62_stage9_repair_results.json' if a.mode=='repair' else '61_stage9_live_results.json'),report)
        raise
    finally:
        if client is not None:
            client.close()


if __name__=='__main__':
    raise SystemExit(main())
