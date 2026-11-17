"""Stage 9: reproducible NON-OFFICIAL live Apache Cassandra evaluation.

All five HBench deployments and CQL plans come from the frozen Stage8 outputs.
No measured value is fed to candidate activation or HBench selection.
The pilot requires a real Cassandra instance for smoke tests/measurements.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import sqlite3
import statistics
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Thread
from typing import Any, Iterable

from methodology.tpcx_iot.stage4.candidates import ROOT, read_json, sha256
from methodology.tpcx_iot.stage5.runtime import ClosedWindow, bind_read_requests, merge_read_results
from methodology.tpcx_iot.stage6.materializer import normalize_event, physical_row

OUT = 'outputs/tpcx_iot_stage9'
FROZEN = 'outputs/tpcx_iot_stage8'
BASE_MS = 1767225600000  # 2026-01-01T00:00:00Z -- fixed synthetic benchmark anchor


def percentile(values: list[float], q: float) -> float | None:
    """Linear interpolation (Type-7); caller must label this convention."""
    if not values:
        return None
    sorted_values = sorted(values)
    x = (len(values)-1)*q
    lo = math.floor(x)
    hi = math.ceil(x)
    return round(sorted_values[lo] + (sorted_values[hi]-sorted_values[lo])*(x-lo), 6)


def load_frozen(root: Path = ROOT) -> tuple[list[dict], dict[str,dict], dict[str,dict],dict[str,str]]:
    """Validate previously *selected* HBench, not recompute it from measurements."""
    root = Path(root)
    stage8 = root / FROZEN
    manifest = read_json(stage8/'58_stage8_manifest.json')
    hb = read_json(stage8/'54_hbench_selected.json')
    reads = read_json(root/'outputs/tpcx_iot_stage5/20_queryplans.json')
    writes = read_json(root/'outputs/tpcx_iot_stage6/30_writeplans.json')
    specs = read_json(root/'outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json')
    p8 = root/'config/tpcx_stage8_consolidation_policy.json'
    if (manifest['stage'] != 8 or manifest['engine'] != 'Apache Cassandra' or
        manifest['selection_uses_measured_results'] is not False or
        manifest['official_tpcx_iot_kit_verified'] is not False or
        len(hb) != 5 or manifest['hbench_ids'] != [h['deployment_id'] for h in hb] or
        manifest['consolidation_policy_sha256'] != sha256(p8)):
        raise ValueError('Stage8 frozen selection or provenance changed')
    for name,path in [
        ('stage4_candidate_specs_sha256',root/'outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json'),
        ('stage5_plans_sha256',root/'outputs/tpcx_iot_stage5/20_queryplans.json'),
        ('stage6_writeplans_sha256',root/'outputs/tpcx_iot_stage6/30_writeplans.json'),
    ]:
        if manifest['source_hashes'][name] != sha256(path):
            raise ValueError(f'Frozen input differs: {name}')
    rindex={x['candidate_id']:x for x in reads}
    windex={x['candidate_id']:x for x in writes}
    sin={x['candidate_id']:x for x in specs}
    if len(rindex)!=5 or len(windex)!=5 or len(sin)!=5:
        raise ValueError('Candidate count mismatch')
    for i,h in enumerate(hb,1):
        if h['hbench_selection_order']!=i or not set(h['tables']) <= set(sin):
            raise ValueError('Stage8 identity/order mismatch')
        route=h['route']['Q_READ_2_WINDOWS']
        if route['candidate_id'] not in h['tables'] or route['read_cql']!=rindex[route['candidate_id']]['read_cql']:
            raise ValueError('Read routing drift')
        required=h['route']['Q_INGEST']['all_required_write_plans']
        if set(required) != {windex[t]['plan_id'] for t in h['tables']}:
            raise ValueError('Missing deployment write maintenance')
        if h['canonical_source'] not in h['tables']:
            raise ValueError('Missing canonical source')
        for t in h['tables']:
            ddl=root/f'outputs/tpcx_iot_stage4/cql/schemas/{t}.cql'
            if not ddl.exists() or f'CREATE TABLE IF NOT EXISTS {t} (' not in ddl.read_text():
                raise ValueError('Missing physical DDL')
    inputs={str((root/x).relative_to(root)):sha256(root/x) for x in [
        'outputs/tpcx_iot_stage8/54_hbench_selected.json',
        'outputs/tpcx_iot_stage8/58_stage8_manifest.json',
        'config/tpcx_stage8_consolidation_policy.json',
        'outputs/tpcx_iot_stage5/20_queryplans.json',
        'outputs/tpcx_iot_stage6/30_writeplans.json',
        'outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json',
    ]}
    return hb,rindex,windex,inputs


def events(*,stations:int=2,sensors:int=3,steps:int=130,first_step:int=0,
           tick_ms:int=1000,pad_bytes:int=900) -> list[dict]:
    """Deterministic RESEARCH-only input; NOT the official TPC data generator."""
    if not (1<=stations<=500 and 1<=sensors<=200 and 1<=steps<=100000 and
            0<=first_step<=10000000 and 1<=tick_ms<=3600000 and 0<=pad_bytes<=1024):
        raise ValueError('Invalid synthetic pilot parameters')
    records=[]
    for step in range(first_step,first_step+steps):
        for station_i in range(stations):
            for sensor_i in range(sensors):
                raw=dict(driver_system_id=f'ST{station_i:03}',sensor_name=f'sensor_{sensor_i:03}',
                         event_ts=BASE_MS + step*tick_ms,
                         sensor_reading=float(station_i*100+sensor_i*10 + (step%37)/10),
                         padding='x'*pad_bytes,
                         source_record_id=f'research:{station_i}:{sensor_i}:{step}')
                records.append(normalize_event(raw))
    return records


def probes(*, sensors:int=3) -> list[dict]:
    """Each probe has two inclusive five-second windows; 60s boundary exercised."""
    return [dict(sensor_name=f'sensor_{s:03}',
                 recent=ClosedWindow(BASE_MS+58000,BASE_MS+63000),
                 historical=ClosedWindow(BASE_MS+11000,BASE_MS+16000))
            for s in range(sensors)]


def oracle_rows(source:Iterable[dict], probe:dict)->list[dict]:
    seen={}
    for e in source:
        ts=e['event_ts']
        if e['sensor_name']!=probe['sensor_name']:
            continue
        if not (probe['recent'].start_ms<=ts<=probe['recent'].end_ms or
                probe['historical'].start_ms<=ts<=probe['historical'].end_ms):
            continue
        ident=(e['driver_system_id'],e['sensor_name'],e['event_ts'],e['event_id'])
        if ident in seen and seen[ident]!=e:
            raise ValueError('Conflicting oracle input')
        seen[ident]=e
    return [dict(event_ts=e['event_ts'],sensor_reading=e['sensor_reading'])
            for ident,e in sorted(seen.items(),key=lambda v:(v[0][2],v[0][0],v[0][1],str(v[0][3])))]


def keyspace(h:dict,prefix:str='sl_tpcx') ->str:
    if not prefix or not prefix.replace('_','').isalnum() or not prefix[0].isalpha():
        raise ValueError('Unsafe keyspace prefix')
    name=f'{prefix}_{h["deployment_id"]}'
    if len(name)>48 or not name.replace('_','').isalnum():
        raise ValueError('Invalid keyspace')
    return name.lower()


class DurableOutbox:
    """Local SQLite WAL for replay on THIS HOST; NOT cross-node atomicity.

    A successful Cassandra mutation followed by failure before SQLite ACK is
    safely repeated, because the Stage6 physical event identity is stable.
    """
    def __init__(self,path:Path):
        self.path=Path(path)
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(self.path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY,payload TEXT NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS applied(event_id TEXT NOT NULL,table_name TEXT NOT NULL,PRIMARY KEY(event_id,table_name))')
        self.db.commit()

    @staticmethod
    def payload(event:dict)->str:
        return json.dumps({k:str(v) if k=='event_id' else v for k,v in event.items()},
                          sort_keys=True,separators=(',',':'))

    def persist(self,event:dict)->None:
        uid=str(event['event_id'])
        payload=self.payload(event)
        with self.db:
            found=self.db.execute('SELECT payload FROM events WHERE event_id=?',(uid,)).fetchone()
            if found and found[0]!=payload:
                raise ValueError('Conflicting physical event UUID in durable outbox')
            self.db.execute('INSERT OR IGNORE INTO events VALUES (?,?)',(uid,payload))

    def pending(self,tables:Iterable[str])->Iterable[tuple[dict,str]]:
        """Replay in source-event insertion order, writing each event to all tables."""
        table_list=list(tables)
        for (raw,) in self.db.execute('SELECT payload FROM events ORDER BY rowid').fetchall():
            event=normalize_event(json.loads(raw))
            for table in table_list:
                ack=self.db.execute('SELECT 1 FROM applied WHERE event_id=? AND table_name=?',
                                    (str(event['event_id']),table)).fetchone()
                if ack is None:
                    yield event,table

    def acknowledge(self,event:dict,table:str)->None:
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO applied VALUES (?,?)',(str(event['event_id']),table))

    def unapplied_count(self,tables:Iterable[str])->int:
        return sum(1 for _ in self.pending(tables))

    def close(self)->None:
        self.db.close()


@dataclass
class CassandraClient:
    hosts:list[str]
    port:int=9042

    def __post_init__(self):
        try:
            from cassandra.cluster import Cluster
        except ImportError as exc:
            raise RuntimeError('Install cassandra-driver: pip install -r requirements-stage9.txt') from exc
        self.cluster=Cluster(contact_points=self.hosts,port=self.port,connect_timeout=15)
        self.session=self.cluster.connect()
        self.session.default_timeout=30
        self.prepared={}

    def close(self)->None:
        self.cluster.shutdown()

    def prepare_deployment(self,root:Path,h:dict,*,prefix:str,rf:int,reset:bool)->str:
        if not 1<=rf<=9:
            raise ValueError('Replication factor must be 1..9')
        ks=keyspace(h,prefix)
        if reset:
            self.session.execute(f'DROP KEYSPACE IF EXISTS {ks}')
        self.session.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class':'SimpleStrategy','replication_factor':{rf}}}")
        self.session.set_keyspace(ks)
        for t in h['tables']:
            schema=(root/f'outputs/tpcx_iot_stage4/cql/schemas/{t}.cql').read_text()
            cql='\n'.join(line for line in schema.splitlines() if not line.lstrip().startswith('--')).strip()
            if cql.count(';')!=1 or 'ALLOW FILTERING' in cql.upper():
                raise ValueError('Untrusted schema DDL')
            self.session.execute(cql)
        self.prepared={}
        return ks

    def get_prepared(self,cql:str):
        stmt=self.prepared.get(cql)
        if stmt is None:
            stmt=self.session.prepare(cql)
            self.prepared[cql]=stmt
        return stmt

    def execute_write(self,write_plan:dict,event:dict)->None:
        cql=write_plan['write_cql']
        stmt=self.get_prepared(cql)
        row=physical_row(write_plan,event)
        self.session.execute(stmt,tuple(row[c] for c in write_plan['column_order']))

    def read_probe(self,read_plan:dict,probe:dict,stations:list[str])->tuple[list[dict],list[float],int]:
        reqs=bind_read_requests(read_plan,sensor_name=probe['sensor_name'],
                                recent=probe['recent'],historical=probe['historical'],
                                stations=stations,complete_station_roster=True,
                                roster_provenance='FULL_SYNTHETIC_GENERATOR_STATION_ROSTER')
        cql=read_plan['read_cql']
        prepared=self.get_prepared(cql)
        batches=[]
        durations=[]
        for req in reqs:
            t0=time.perf_counter_ns()
            rs=self.session.execute(prepared,tuple(req['params']))
            # Force consumption of *all* driver pages before stopping timer.
            rows=[dict(r._asdict()) for r in rs]
            durations.append((time.perf_counter_ns()-t0)/1e6)
            batches.append((req,rows))
        merged=merge_read_results(read_plan,batches,sensor_name=probe['sensor_name'],
                                  recent=probe['recent'],historical=probe['historical'])
        return merged,durations,len(reqs)

    def count_small_table(self,table:str)->int:
        return int(self.session.execute(f'SELECT COUNT(*) FROM {table}').one()[0])

    def cassandra_version(self)->str:
        return str(self.session.execute('SELECT release_version FROM system.local').one()[0])


def persist_all(client:CassandraClient,h:dict,events_to_insert:list[dict],write_plans:dict,
                ledger:DurableOutbox)->tuple[int,float]:
    """Persist first, ACK per mutation; replay before accepting an ingest event."""
    t0=time.perf_counter_ns()
    for e in events_to_insert:
        ledger.persist(e)
    done=0
    for e,table in ledger.pending(h['tables']):
        client.execute_write(write_plans[table],e)
        ledger.acknowledge(e,table)
        done+=1
    if ledger.unapplied_count(h['tables']):
        raise RuntimeError('Projection maintenance incomplete')
    return done,(time.perf_counter_ns()-t0)/1e9


def verify(client:CassandraClient,h:dict,read_plans:dict,seed:list[dict],
           sensors:int,stations:int,*,strict_count:bool=True)->dict:
    """Compare actual retrieved rows and multiplicity with independently generated oracle."""
    actual_plan=read_plans[h['route']['Q_READ_2_WINDOWS']['candidate_id']]
    station_roster=[f'ST{i:03}' for i in range(stations)]
    queries=[]
    for p in probes(sensors=sensors):
        observed,latencies,requests=client.read_probe(actual_plan,p,station_roster)
        expected=oracle_rows(seed,p)
        if observed!=expected:
            raise AssertionError(f'Mismatch for {h["deployment_id"]} / {p["sensor_name"]}: '
                                 f'expected {len(expected)} rows, observed {len(observed)}')
        queries.append({'sensor_name':p['sensor_name'],'expected_rows':len(expected),
                        'actual_rows':len(observed),'cql_requests':requests})
    rows={}
    if strict_count:
        for table in h['tables']:
            count=client.count_small_table(table)
            if count!=len(seed):
                raise AssertionError(f'Row count mismatch in {table}: {count} vs {len(seed)}')
            rows[table]=count
    return {'deployment_id':h['deployment_id'],'status':'PASS_LIVE_SEE_LIMITATIONS',
            'queries':queries,'materialized_counts':rows,
            'full_operational_coverage_claimed':False,
            'official_tpcx_iot_kit_verified':False,
            'notes':'Live synthetic equivalent read/write paths only; no fault-tolerant or official-kit validation'}


def duration_stats(ms:list[float])->dict:
    return {'n':len(ms),'mean_ms':round(statistics.mean(ms),6) if ms else None,
            'p50_ms':percentile(ms,0.50),'p95_ms':percentile(ms,0.95),
            'p99_ms':percentile(ms,0.99),'max_ms':round(max(ms),6) if ms else None}


def benchmark_one(client:CassandraClient,h:dict,read_plans:dict,write_plans:dict,
                  seed:list[dict],next_events:list[dict],ledger:DurableOutbox,
                  *,stations:int,sensors:int,read_threads:int=1)->dict:
    """RESEARCH mixed workload: writes new timestamps; reads pinned verified data.

    The fixed-window read phase is deliberately NOT the official TPCx-IoT
    recency/timestamp generator. Both ingestion and query workers are concurrent.
    """
    station_roster=[f'ST{i:03}' for i in range(stations)]
    plan=read_plans[h['route']['Q_READ_2_WINDOWS']['candidate_id']]
    # Preprepare all statements before timing concurrent work.
    client.get_prepared(plan['read_cql'])
    for t in h['tables']:
        client.get_prepared(write_plans[t]['write_cql'])
    query_jobs=probes(sensors=sensors)
    stop=Event()
    read_ms=[]
    errors=[]
    def read_loop(index:int):
        i=index
        while not stop.is_set():
            probe=query_jobs[i%len(query_jobs)]
            t0=time.perf_counter_ns()
            try:
                rows,_,_=client.read_probe(plan,probe,station_roster)
                elapsed_ms=(time.perf_counter_ns()-t0)/1e6
                if rows!=oracle_rows(seed,probe):
                    raise AssertionError('Concurrent read returned non-equivalent results')
                read_ms.append(elapsed_ms)
            except Exception as exc:
                errors.append(str(exc))
                stop.set()
            i+=1
    workers=[Thread(target=read_loop,args=(i,),daemon=True) for i in range(read_threads)]
    for w in workers:
        w.start()
    t0=time.perf_counter_ns()
    try:
        writes,seconds=persist_all(client,h,next_events,write_plans,ledger)
    finally:
        stop.set()
        for w in workers:
            w.join(timeout=45)
    if errors:
        raise AssertionError(f'Concurrent read verification failed: {errors}')
    if not read_ms:
        raise AssertionError('Zero concurrent read requests; not a mixed workload')
    return {'deployment_id':h['deployment_id'], 'status':'MEASURED_RESEARCH_ADAPTATION',
            'measured_source_events':len(next_events),'physical_insert_statements':writes,
            'ingest_wall_seconds':round(seconds,6),
            'research_ingest_rows_per_second':round(len(next_events)/seconds,3),
            'logical_two_window_reads_completed':len(read_ms),
            'logical_read_latency':duration_stats(read_ms),
            'reads_execute_concurrently_with_ingestion':True,
            'query_windows_fixed_to_seed':True,'official_IoTps':None,
            'tpc_compliant':False}


def manifest(root:Path,inputs:dict,*,mode:str,config:dict,status:str)->dict:
    return {'stage':9,'stage8_hbench_frozen':True,'hbench_count':5,
            'mode':mode,'status':status,'official_kit_verification':'PENDING',
            'database':'Apache Cassandra','research_only':True,'tpc_compliant':False,
            'measured_result_used_to_reselect_hbench':False,
            'environment':{'python':platform.python_version(),'os':platform.platform(),
                           'cpu_count':os.cpu_count()},'experiment_config':config,
            'source_sha256':inputs,'no_cluster_means_no_real_benchmark_results':True}
