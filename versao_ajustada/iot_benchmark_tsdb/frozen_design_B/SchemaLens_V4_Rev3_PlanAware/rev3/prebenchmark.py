"""Rev3 offline plan-aware freeze, additive to verified Rev2/V3 artifacts.

The EXISTING Rev2 confirmatory results must not be cited as independent
confirmation of this revision. This is a POST-E5 methodological prototype.
"""
from __future__ import annotations
import argparse
import copy
import json
import os
from pathlib import Path
import shutil
import tempfile
from v4_auto import selector as A
from v4_rev2 import prebenchmark as P
from .core import (REVISION, OUTPUT, REV2_POLICY, make_plan_catalog,
                   select_schema_and_plan)

TRACKED_FILES={
 'candidates':'stage4/10_v4_candidate_specs.json',
 'coverage':'stage5/15_cross_query_coverage.json',
 'storage':'stage7/21_storage_proxies.json',
 'writes':'stage7/22_write_proxies.json',
 'read':'stage7/23_read_proxies_rev2.json',
 'partition':'stage7/24_partition_proxies_rev2.json',
 'policy':'stage7/25_rev2_cost_policy.json',
 'rev2_hbench':'stage8/28_hbench_v4_rev2_frozen.json',
 'rev2_manifest':'stage8/manifest.json'
}


def code_hashes():
    return {p.name:A.digest(p) for p in (Path(__file__),Path(__file__).with_name('core.py'),
                                         Path(__file__).with_name('runtime.py'))}


def parent_hashes(exp):
    root=Path(exp)/P.OUTPUT
    return {name:A.digest(root/rel) for name,rel in TRACKED_FILES.items()}


def inputs(exp):
    exp=Path(exp).resolve()
    parent=P.verify(exp)
    root=exp/P.OUTPUT
    candidates=A.jread(root/TRACKED_FILES['candidates'])
    coverage=A.jread(root/TRACKED_FILES['coverage'])
    reads=A.jread(root/TRACKED_FILES['read'])
    storage=A.jread(root/TRACKED_FILES['storage'])
    writes=A.jread(root/TRACKED_FILES['writes'])
    rows_file=exp/'phase_a1/outputs/normalized_double_readings.csv'
    from iot_pipeline.shared import read_rows,read,require_policy
    rows=read_rows(rows_file)
    qs=read(exp/'phase_a1/outputs/query_instances_pilot.json')
    basepolicy=require_policy()
    return parent,candidates,coverage,reads,storage,writes,rows,qs,basepolicy


def generate(exp,write=True):
    exp=Path(exp).resolve()
    before=parent_hashes(exp)
    (parent,candidates,coverage,reads,storage,writes,rows,queries,policy)=inputs(exp)
    plans=make_plan_catalog(candidates,queries,coverage,reads,rows)
    covers,selected,pareto=select_schema_and_plan(candidates,queries,coverage,plans,storage,writes,policy)
    if before!=parent_hashes(exp):raise ValueError('Frozen Rev2 changed while deriving Rev3')
    parent_sel=A.jread(exp/P.OUTPUT/TRACKED_FILES['rev2_hbench'])
    # Copy no preexisting IDs. All Rev3 selections have deterministic new names.
    if {h['deployment_id'] for h in parent_sel}&{h['deployment_id'] for h in selected}:
        raise ValueError('New revision reused an old deployment ID')
    result={'plans':plans,'covers':covers,'selected':selected,'pareto':pareto,
            'parent':parent,'before':before,'rows':rows,'queries':queries}
    if not write:return result
    output=exp/OUTPUT
    if output.exists():raise FileExistsError('Frozen Rev3 already exists: '+str(output)+'; use --verify-only')
    output.parent.mkdir(exist_ok=True,parents=True)
    temp=Path(tempfile.mkdtemp(prefix='.rev3_staging_',dir=output.parent))
    try:
        A.dump(temp/'stage5/15_executable_plan_catalog.json',plans)
        A.csv_out(temp/'stage5/16_executable_plan_catalog.csv',plans,
            ['plan_id','query_id','candidate_id','family','physical_plan',
             'activated_for_query','cross_query_reuse','analytic_read_proxy',
             'request_count_proxy','physical_rows_proxy','returned_bytes_proxy'])
        A.dump(temp/'stage7/23_per_plan_read_proxies.json',plans)
        A.dump(temp/'stage7/25_cost_policy.json',{'policy':REV2_POLICY,
              'origin':'Unmodified prospective Rev2 engineering coefficients',
              'exceptions':'Plan-specific scan counts derived from source cardinalities, not observed latency'})
        A.dump(temp/'stage8/28_hbench_v4_rev3_frozen.json',selected)
        A.dump(temp/'stage8/29_all_compatible_covers.json',covers)
        A.csv_out(temp/'stage8/30_hbench_summary.csv',
          [{'deployment_id':h['deployment_id'],'canonical_source':h['canonical_source'],
            'families':'+'.join(h['families']),'selection_reason':h['selection_reason'],
            'read_proxy':h['analytic_cost']['read'],'storage_proxy':h['analytic_cost']['storage'],
            'write_proxy':h['analytic_cost']['write'],'score':h['preselection_score']}
           for h in selected],
          ['deployment_id','canonical_source','families','selection_reason',
           'read_proxy','storage_proxy','write_proxy','score'])
        A.csv_out(temp/'stage8/31_plan_aware_routes.csv',
          [dict(deployment_id=h['deployment_id'],query_id=qid,**route)
           for h in selected for qid,route in sorted(h['query_routes'].items())],
          ['deployment_id','query_id','candidate_id','family','plan_id','physical_plan',
           'read_proxy','reason','route_status'])
        manifest={
          'status':'FROZEN_REV3_EXPLORATORY_PLAN_AWARE',
          'revision':REVISION,
          'scope':'POST_E5_OFFLINE_METHODOLOGICAL_PROTOTYPE_NOT_INDEPENDENT_HOLDOUT',
          'prior_e5_has_informed_plan_family':'YES; NO E5 LATENCY READ BY SELECTOR',
          'no_Cassandra_used':True,'no_measured_latency_used':True,
          'new_measurements_required_for_independent_confirmation':True,
          'parent_rev2_selection_sha256':before['rev2_hbench'],
          'parent_rev2_manifest_sha256':before['rev2_manifest'],
          'parent_frozen_artifacts_sha256':before,
          'source_sha256':{
             'data':A.digest(exp/'phase_a1/outputs/normalized_double_readings.csv'),
             'queries':A.digest(exp/'phase_a1/outputs/query_instances_pilot.json')},
          'implementation_sha256':code_hashes(),
          'candidate_count':len(candidates),
          'plan_count':len(plans),'compatible_configurations':len(covers),
          'pareto_count':len(pareto),'selected_count':len(selected),
          'selected_ids':[h['deployment_id'] for h in selected],
          'limitations':[
            'Inherited logical correctness from Rev2; new TS0 LIMIT1 was independently checked in E5 only',
            'IoT Benchmark Java representative objects; no official CQL/IoTDB frequency replay',
            'Bytes proxies are not SSTable bytes; Cassandra read metrics are driver-level',
            'Model evolution was informed by E5, so existing Rev2 data is not new holdout',
            'No runtime latency may be used to change this frozen selection']}
        manifest['artifact_sha256']={str(p.relative_to(temp)):A.digest(p) for p in temp.rglob('*') if p.is_file()}
        A.dump(temp/'stage8/manifest.json',manifest)
        if before!=parent_hashes(exp):raise ValueError('Parent Rev2 changed during Rev3 freeze')
        temp.rename(output)
        verify(exp)
        print('FROZEN: Rev3 exploratory plan-aware selection:',output)
        print('Plans:',len(plans),'compatible configs:',len(covers),'selected:',len(selected))
        for h in selected:
            print(' ',h['deployment_id'],h['canonical_source'],h['selection_reason'],
                  'Q8:',h['query_routes']['Q8']['physical_plan'])
        print('SHA256:',A.digest(output/'stage8/28_hbench_v4_rev3_frozen.json'))
    except Exception:
        if temp.exists():shutil.rmtree(temp)
        raise
    return result


def verify(exp):
    exp=Path(exp).resolve()
    # All original revision checks are still required.
    P.verify(exp)
    out=exp/OUTPUT
    m=A.jread(out/'stage8/manifest.json')
    if m.get('status')!='FROZEN_REV3_EXPLORATORY_PLAN_AWARE':raise ValueError('Unexpected Rev3 freeze status')
    if m['parent_frozen_artifacts_sha256']!=parent_hashes(exp):
        raise ValueError('Rev2 frozen input changed after Rev3 selection')
    if m['implementation_sha256']!=code_hashes():
        raise ValueError('Rev3 implementation changed after freeze')
    for rel,sha in m['artifact_sha256'].items():
        if A.digest(out/rel)!=sha:raise ValueError('Rev3 artifact modified: '+rel)
    for rel,key in [('phase_a1/outputs/normalized_double_readings.csv','data'),
                    ('phase_a1/outputs/query_instances_pilot.json','queries')]:
        if A.digest(exp/rel)!=m['source_sha256'][key]:
            raise ValueError('Frozen data changed: '+rel)
    selected=A.jread(out/'stage8/28_hbench_v4_rev3_frozen.json')
    catalog={p['plan_id']:p for p in A.jread(out/'stage5/15_executable_plan_catalog.json')}
    if [h['deployment_id'] for h in selected]!=m['selected_ids']:
        raise ValueError('Selection IDs changed')
    for h in selected:
        if set(h['query_routes'])!={f'Q{i}' for i in range(1,13)}:
            raise ValueError('Selected configuration lacks complete coverage')
        for qid,route in h['query_routes'].items():
            plan=catalog.get(route['plan_id'])
            if (not plan or plan['query_id']!=qid or
                plan['candidate_id']!=route['candidate_id'] or
                plan['physical_plan']!=route['physical_plan']):
                raise ValueError('Frozen route not bound to executable plan')
    return m


def main():
    ap=argparse.ArgumentParser(description='Plan-aware Rev3 exploratory Stage5/7/8 freeze')
    ap.add_argument('--experiment',required=True,type=Path)
    ap.add_argument('--verify-only',action='store_true')
    args=ap.parse_args()
    if args.verify_only:
        verify(args.experiment);print('PASS: Rev3 and Rev2 frozen artifacts unchanged')
    else:generate(args.experiment)
if __name__=='__main__':main()
