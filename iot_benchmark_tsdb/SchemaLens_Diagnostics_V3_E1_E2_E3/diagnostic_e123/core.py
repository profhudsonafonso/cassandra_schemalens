"""Read-only frozen V3 contracts and instrumented Cassandra query paths.

E1 isolates client-perceived Cassandra fetch time from client transformation
and logical evaluation. Do NOT interpret driver wall time as server-only time.
"""
from __future__ import annotations
import copy
import hashlib
import json
import math
import random
import re
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

EXPECTED = '2c4f46c70fc272046b42d1c7a8e6fc7c20c4ca9d8b63cef707ff7f89fa3fa6eb'
BASELINE_ID = 'REF_NATIVE_IOT_TABLE_CASSANDRA_V3'
DAY = 86_400_000


def quantile(values, p):
    if not values: raise ValueError('No measurements')
    xs=sorted(values)
    return xs[max(0,min(len(xs)-1,math.ceil(p/100*len(xs))-1))]


def hash_file(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def safety_snapshot(exp):
    """Includes frozen manifest and every frozen input in addition to the HBench."""
    from iot_pipeline.shared import ROOT
    from iot_pipeline.stage9 import confirm_frozen
    if Path(ROOT).resolve()!=Path(exp).resolve():
        raise ValueError('Imported iot_pipeline does not belong to --experiment')
    confirm_frozen()
    m=json.loads((exp/'outputs/stage8/32_frozen_manifest.json').read_text())
    h=hash_file(exp/'outputs/stage8/28_hbench_frozen.json')
    if h!=EXPECTED: raise ValueError('Wrong frozen experiment; expected V3 native HBench hash')
    names=['outputs/stage8/28_hbench_frozen.json','outputs/stage8/32_frozen_manifest.json']
    names+=list(m['inputs_sha256'])
    names+=['outputs/stage8/30_hbench_summary.csv']
    return {rel:hash_file(exp/rel) for rel in sorted(set(names))}


def verify_stage9(exp):
    """Historical manifests must identify exactly these V3 source and HBench inputs."""
    from iot_pipeline.shared import inputs
    base=exp/'outputs/stage9_native_reference_v3/37_manifest.json'
    hb=exp/'outputs/stage9_hbench_native_v3/37_manifest.json'
    for p in (base,hb):
        if not p.exists(): raise FileNotFoundError(f'Run original Stage9 first; missing {p}')
    b=json.loads(base.read_text()); h=json.loads(hb.read_text())
    hbench=hash_file(exp/'outputs/stage8/28_hbench_frozen.json')
    rows=hash_file(inputs()['native'])
    queries=hash_file(inputs()['queries'])
    for label,data in [('baseline',b),('HBench',h)]:
        if data.get('hbench_sha256')!=hbench or data.get('source_rows_sha256')!=rows:
            raise ValueError(f'{label} Stage9 manifest hash mismatch')
    if b.get('query_instances_sha256')!=queries: raise ValueError('Baseline native query hash mismatch')
    if set(h['deployments'])!={x['deployment_id'] for x in json.loads((exp/'outputs/stage8/28_hbench_frozen.json').read_text())}:
        raise ValueError('Stage9 HBench deployments incomplete')
    return {'prior_stage9_baseline':hash_file(base),'prior_stage9_hbench':hash_file(hb),
            'native_queries_sha256':queries}


def check_readonly_sql(cql):
    if not cql.strip().upper().startswith('SELECT '):
        raise ValueError('E1/E2 may execute only SELECT statements')
    if ';' in cql.strip().rstrip(';'):
        raise ValueError('Multiple CQL statements forbidden')


@dataclass
class ReadTrace:
    ns:int=0
    calls:int=0
    returned_rows:int=0
    value_bytes_approx:int=0
    cql:list[str]=field(default_factory=list)


class TraceSession:
    """Driver execute + full result iteration wall time (NOT pure server processing)."""
    def __init__(self,session,trace):self.session,self.trace=session,trace
    def execute(self,query,params=None):
        q=str(query);check_readonly_sql(q)
        start=time.perf_counter_ns()
        data=list(self.session.execute(query,params))
        self.trace.ns+=time.perf_counter_ns()-start
        self.trace.calls+=1
        self.trace.returned_rows+=len(data)
        # Diagnostic approximation, not measured network bytes.
        self.trace.value_bytes_approx+=sum(len(repr(x)) for x in data)
        self.trace.cql.append(q)
        return data


def run_path(session, q, variant, sensors, deployment=None):
    """Identical oracle; profile fetch, conversion and logical transformation.

    For HBench Q12, preserve the *current* Stage9 route logic in E1;
    diagnostics should not silently fix old benchmark behavior.
    """
    from iot_pipeline import stage9
    from iot_pipeline.native_reference_v3 import fetch_query_records, source_oracle
    oracle=source_oracle()
    trace=ReadTrace();s=TraceSession(session,trace)
    started=time.perf_counter_ns()
    if variant=='reference': records=fetch_query_records(s,q,sensors)
    else:
        route=deployment['query_routes'][q['query_id']]
        if route['family']=='TS5':
            logical=stage9._rollup_rows(s,q)
            elapsed=time.perf_counter_ns()-started
            return logical, {'total_ms':elapsed/1e6,'db_fetch_ms':trace.ns/1e6,
                'decode_ms':max(0,elapsed-trace.ns)/1e6,'logic_ms':0.0,
                'requests':trace.calls,'cql_rows':trace.returned_rows,'response_repr_bytes':trace.value_bytes_approx,
                'cql':trace.cql}
        records=stage9._query_rows(s,route['family'],q,sensors,candidate_id=route['candidate_id'])
    fetched=time.perf_counter_ns()
    got=oracle(records,q)
    ended=time.perf_counter_ns()
    total=ended-started
    logic=ended-fetched
    return got,{'total_ms':total/1e6,'db_fetch_ms':trace.ns/1e6,
                'decode_ms':max(0,fetched-started-trace.ns)/1e6,
                'logic_ms':logic/1e6,'requests':trace.calls,
                'cql_rows':trace.returned_rows,'response_repr_bytes':trace.value_bytes_approx,
                'cql':trace.cql}


def double_check(got,expected,description):
    from iot_pipeline.offline import compare
    if not compare(got,expected):raise AssertionError('Incorrect Cassandra result: '+description)


def all_deployments():
    from iot_pipeline.stage9 import confirm_frozen
    return confirm_frozen()


def find_ts2u(deployments):
    xs=[h for h in deployments if 'ts2_iot_wide_unbucketed' in h['candidates']]
    if len(xs)!=1:raise ValueError('Expected exactly one TS2-U frozen deployment')
    return xs[0]


def ensure_tables(cluster, keyspace, tables):
    """Read Cassandra's system tables directly without a global schema refresh.

    Read-only E1/E2 require existing Stage9 keyspaces. The Cassandra Python
    driver can fail on cluster.refresh_schema_metadata() even when the
    underlying tables are accessible. No DDL, writes, or repairs are attempted.
    """
    # Both system_schema queries are keyed by keyspace_name. Never interpret
    # a missing row as permission to create or modify the original keyspaces.
    session = cluster.connect()
    ks = session.execute(
        'SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s',
        (keyspace,),
    )
    if not any(getattr(row, 'keyspace_name', None) == keyspace for row in ks):
        raise RuntimeError(
            f'Missing existing keyspace {keyspace}; run original Stage9 first')
    found = {
        str(row.table_name)
        for row in session.execute(
            'SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s',
            (keyspace,),
        )
    }
    missing = set(tables) - found
    if missing:
        raise RuntimeError(f'{keyspace} missing tables: {sorted(missing)}')


def keyspace_h(depl):return f"sl_iot_h_{depl['deployment_id'][2:]}"


def collapse(values):
    fields=['total_ms','db_fetch_ms','decode_ms','logic_ms','requests','cql_rows','response_repr_bytes']
    out={}
    for f in fields:
        v=[x[f] for x in values]
        out[f'mean_{f}']=statistics.mean(v)
        out[f'p95_{f}']=quantile(v,95)
    out['n']=len(values)
    return out


def write_json(path,obj):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj,indent=2,ensure_ascii=False,sort_keys=True,allow_nan=False)+'\n')


def write_csv(path, rows):
    import csv
    path.parent.mkdir(parents=True,exist_ok=True)
    if not rows:raise ValueError('Cannot write empty CSV')
    fields=list(rows[0])
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fields,extrasaction='ignore');w.writeheader();w.writerows(rows)


def read_context():
    from iot_pipeline.shared import inputs,read_rows,read
    from iot_pipeline.native_reference_v3 import prepare
    selected,rows,queries,sensors,_,_,meta,hashes=prepare()
    return selected,rows,queries,sensors,meta,hashes


def create_synthetic_days(rows,days):
    """Deterministic day-shift replication; preserves original point values.

    This is NOT native IoT Benchmark generation and MUST NOT feed Stage4-8.
    """
    if not isinstance(days,int) or days<1 or days>90:raise ValueError('days must be an integer between 1 and 90')
    dates={int(x['timestamp'])//DAY for x in rows}
    if len(dates)!=1:
        raise ValueError('Source spans multiple UTC days; scale extension requires one source day')
    seen=set();out=[]
    for day in range(days):
        dt=day*DAY
        for row in rows:
            r=dict(row);r['timestamp']=int(row['timestamp'])+dt
            key=(r['device_id'],r['sensor_name'],r['timestamp'])
            if key in seen:raise ValueError('Overlapping timestamp in synthetic scaling')
            seen.add(key);out.append(r)
    return out


def probes(queries,rows,days):
    """Native fixed original queries + explicitly marked exploratory probes."""
    base=copy.deepcopy(queries)
    for q in base:q['_diagnostic_query_label']=f"NATIVE_FIXED_{q['query_id']}"
    start=min(r['timestamp'] for r in rows)
    end=max(r['timestamp'] for r in rows)+1
    extra=[]
    for query_id in ('Q2','Q7','Q9','Q11','Q12'):
        q=copy.deepcopy(next(x for x in queries if x['query_id']==query_id))
        q['start_ms'],q['end_ms_exclusive']=start,end
        if query_id=='Q12':
            for ch in q['set_children']:
                ch['start_ms'],ch['end_ms_exclusive']=start,end
        q['_diagnostic_query_label']=f'EXTENDED_{query_id}_FULL_SPAN'
        q['_synthetic_parameter_extension']=True
        extra.append(q)
    return base+extra


def protect_output(out):
    if out.exists():raise FileExistsError(f'Refusing existing output path: {out}')
    if any(part in {f'stage{i}' for i in range(1,9)} for part in out.parts):
        raise ValueError('Do not write to frozen offline Stages 1-8')


def connect(host,port):
    from cassandra.cluster import Cluster
    return Cluster([host],port=port)
