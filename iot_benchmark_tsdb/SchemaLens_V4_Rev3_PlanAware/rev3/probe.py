"""Optional READ-ONLY Cassandra semantic probe for a frozen Rev3 selection.

Reuses EXISTING frozen Rev2 keyspaces when the exact candidate set was already
materialized. It NEVER creates/reloads/modifies a table or claims independent
Rev3 performance confirmation on Rev2 data.
"""
from __future__ import annotations
import argparse
import csv
import json
from pathlib import Path
from . import prebenchmark as P
from . import runtime as R
from .core import OUTPUT
from v4_auto import selector as A
from v4_rev2 import prebenchmark as REV2


def map_existing_keyspaces(rev3_designs, rev2_designs, measured_manifest):
    if measured_manifest.get('status')!='CONFIRMATORY_CASSANDRA_COMPLETE':
        raise ValueError('Need completed Rev2 benchmark for read-only reuse')
    rev2_by_members={tuple(sorted(h['candidates'])):h for h in rev2_designs}
    out={}
    for h in rev3_designs:
        parent=rev2_by_members.get(tuple(sorted(h['candidates'])))
        if parent:
            ks=measured_manifest['keyspaces'].get(parent['deployment_id'])
            if ks and ks.startswith('sl_conf_r2_'):
                out[h['deployment_id']]=ks
    return out


def main():
    ap=argparse.ArgumentParser(description='Rev3 read-only correctness and plan telemetry')
    ap.add_argument('--experiment',required=True,type=Path)
    ap.add_argument('--results',required=True,type=Path)
    ap.add_argument('--out',required=True,type=Path)
    ap.add_argument('--host',default='127.0.0.1')
    ap.add_argument('--port',type=int,default=9043)
    ap.add_argument('--page-size',type=int,default=16)
    args=ap.parse_args();exp=args.experiment.resolve();results=args.results.resolve();out=args.out.resolve()
    if out.exists() or out.is_relative_to(exp) or out.is_relative_to(results):
        raise ValueError('Output must be a new folder outside frozen artifacts')
    if not results.is_relative_to(exp):raise ValueError('Rev2 measurements must belong to experiment')
    if not 1<=args.page_size<=500:raise ValueError('Invalid page size')
    m=P.verify(exp);REV2.verify(exp)
    rev3=A.jread(exp/OUTPUT/'stage8/28_hbench_v4_rev3_frozen.json')
    catalog={p['plan_id']:p for p in A.jread(exp/OUTPUT/'stage5/15_executable_plan_catalog.json')}
    parents=A.jread(exp/REV2.OUTPUT/'stage8/28_hbench_v4_rev2_frozen.json')
    measured=A.jread(results/'manifest.json')
    if measured['frozen_HBench_sha256'] != A.digest(exp/REV2.OUTPUT/'stage8/28_hbench_v4_rev2_frozen.json'):
        raise ValueError('Measured Rev2 HBench not identical to current frozen Rev2')
    mappings=map_existing_keyspaces(rev3,parents,measured)
    if not mappings:raise ValueError('No Rev3 design matches previously materialized Rev2 tables')
    from iot_pipeline.shared import read,read_rows
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.offline import compare
    rows=read_rows(exp/'phase_a1/outputs/normalized_double_readings.csv')
    qs=read(exp/'phase_a1/outputs/query_instances_pilot.json')
    sensors=sorted({r['sensor_name'] for r in rows})
    days={r['timestamp']//86400000 for r in rows}
    oracle=source_oracle();records=[]
    from cassandra.cluster import Cluster
    cluster=Cluster([args.host],port=args.port)
    try:
        session=cluster.connect()
        for h in rev3:
            ks=mappings.get(h['deployment_id'])
            if not ks:continue
            # Schema metadata is read-only and explicitly scoped to the allowed namespace.
            exists=session.execute('SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s',(ks,))
            physical={row.table_name for row in exists}
            expected_tables={c for c in h['candidates']}
            if not expected_tables<=physical:
                raise ValueError('Preexisting Rev2 keyspace lacks required table(s): '+ks)
            session.set_keyspace(ks)
            for q in qs:
                got,trace=R.dispatch(session,q,h,catalog,sensors,days,args.page_size,oracle)
                expected=oracle(rows,q);ok=compare(got,expected)
                if not ok:raise AssertionError('Rev3 plan mismatch '+h['deployment_id']+' '+q['query_id'])
                records.append({'deployment_id':h['deployment_id'],'keyspace':ks,
                    'query_id':q['query_id'],'plan_id':h['query_routes'][q['query_id']]['plan_id'],
                    **trace,'oracle':'PASS'})
    finally:cluster.shutdown()
    if P.verify(exp)!=m:raise ValueError('Rev3 parent changed during probe')
    out.mkdir(parents=True,exist_ok=False)
    with (out/'read_only_probe.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(records[0]));w.writeheader();w.writerows(records)
    A.dump(out/'manifest.json',{'status':'REV3_POSTHOC_READ_ONLY_PROBE',
        'not_independent_confirmation':True,
        'frozen_rev3_sha256':A.digest(exp/OUTPUT/'stage8/28_hbench_v4_rev3_frozen.json'),
        'frozen_rev2_sha256':A.digest(exp/REV2.OUTPUT/'stage8/28_hbench_v4_rev2_frozen.json'),
        'routes_validated':len(records),'zero_write_operations':True,
        'metric_scope':'Python driver execute calls and iterated rows, not server-side SSTable reads',
        'limitations':['Existing Rev2 Cassandra data; post-E5 Rev3 is exploratory',
         'Only configurations whose complete table set already exists can be probed']})
    print('PASS: read-only Rev3 route validation:',len(records),'routes')
    print('Report:',out/'read_only_probe.csv')

if __name__=='__main__':main()
