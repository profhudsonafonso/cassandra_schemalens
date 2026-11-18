"""Pre-measurement gates: source provenance, frozen selection, and new Java holdout.

Nothing here queries Cassandra or reads latency files. Source-independent cross-checks
are mandatory and all decisions are made before timing begins.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re

EXPECTED_REV3_CODE = {
    'core.py':'420caac9e8626c4f20435a80abecb0b958f2f73bd8ee883cfd9b512fd1a88518',
    'prebenchmark.py':'5ebedc07f231f863eb96946e3195c710d12bb6c45b9f108a6e6e67e445268fad',
    'runtime.py':'7cbb1b95657dc9bd432a8b568df92b6b7048f85597c0513c97d948749bafebf4',
}
REV3 = 'outputs/v4_rev3_planaware_prebenchmark'
REV2 = 'outputs/v4_rev2_prebenchmark'
PROTO='REV3_CONFIRMATORY_PREREGISTRATION.json'

def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def read(path):
    return json.loads(Path(path).read_text(encoding='utf8'))

def dump(path,obj):
    p=Path(path)
    p.parent.mkdir(parents=True,exist_ok=True)
    with p.open('x',encoding='utf8') as fh:
        json.dump(obj,fh,sort_keys=True,indent=2,ensure_ascii=False)
        fh.write('\n')

def experiment_for(workspace):
    workspace=Path(workspace).resolve()
    manifest=read(workspace/'HOLDOUT_GENERATION_OUTPUT.json')
    exp=Path(manifest['isolated_v3_experiment']).resolve()
    if not exp.is_relative_to(workspace) or not exp.is_dir():
        raise ValueError('Generated experiment must be inside the requested new workspace')
    return exp

def hash_inputs(exp):
    return {'data':digest(exp/'phase_a1/outputs/normalized_double_readings.csv'),
            'queries':digest(exp/'phase_a1/outputs/query_instances_pilot.json')}

def assert_rev3_implementation():
    from rev3 import core,prebenchmark,runtime
    for module in (core,prebenchmark,runtime):
        expected=EXPECTED_REV3_CODE[module.__name__.split('.')[-1]+'.py']
        if digest(module.__file__)!=expected:
            raise ValueError('Rev3 implementation differs from audited frozen source: '+str(module.__file__))

def check_new_generation(exp,old_v3,old_rev2):
    from v4_rev2 import prebenchmark as R2
    old_v3=Path(old_v3).resolve();old_rev2=Path(old_rev2).resolve()
    if exp in (old_v3,old_rev2):raise ValueError('Holdout cannot be a previous experiment')
    R2.audit_parent(exp)
    old3=hash_inputs(old_v3);old2=hash_inputs(old_rev2);new=hash_inputs(exp)
    for label,old in [('historical V3',old3),('previous Rev2',old2)]:
        for k in ('data','queries'):
            if new[k]==old[k]:raise ValueError('Independent holdout hash collision with '+label+' '+k)
    qs=read(exp/'phase_a1/outputs/query_instances_pilot.json')
    if len(qs)!=12 or {q['query_id'] for q in qs}!={f'Q{i}' for i in range(1,13)}:
        raise ValueError('Current generator supports exactly 12 query objects Q1..Q12')
    if any('sample from official GenerateQueryWorkLoad Java API' not in q.get('parameter_origin','') for q in qs):
        raise ValueError('Query objects were not tagged as pinned Java samples')
    # Capture must be verified by the Rev2 freeze checker as well.
    return {'new':new,'historical_v3':old3,'previous_rev2':old2,
            'query_count':len(qs),'native_java_origin':True}

def seal(workspace,old_v3,old_rev2,*,rounds,warmup,page_size,measurement_seed,run_tag):
    workspace=Path(workspace).resolve();exp=experiment_for(workspace)
    if not 40<=rounds<=2000:raise ValueError('Minimum 40 timed repetitions per query and variant')
    if not 0<=warmup<=1000 or not 1<=page_size<=500:raise ValueError('Invalid warmup or page size')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,32}',run_tag):raise ValueError('Unsafe run tag')
    if not 0<=measurement_seed<2**32:raise ValueError('Invalid measurement seed')
    assert_rev3_implementation()
    independent=check_new_generation(exp,old_v3,old_rev2)
    from v4_rev2 import prebenchmark as R2
    from rev3 import prebenchmark as R3
    r2=R2.verify(exp);r3=R3.verify(exp)
    if not r2.get('independent_from_original_v3'):raise ValueError('Rev2 parent freeze is not independent')
    if r3['parent_rev2_selection_sha256'] != digest(exp/REV2/'stage8/28_hbench_v4_rev2_frozen.json'):
        raise ValueError('Rev3 parent selection does not match current frozen Rev2')
    selected=read(exp/REV3/'stage8/28_hbench_v4_rev3_frozen.json')
    if not selected:raise ValueError('No selected Rev3 configurations')
    manifest={
      'status':'REV3_NEW_JAVA_HOLDOUT_FROZEN_BEFORE_CASSANDRA',
      'independence_scope':'NEW_JAVA_GENERATION_AND_BENCHMARK_NOT_NEW_RESEARCH_HYPOTHESIS',
      'previous_e5_informed_Rev3':True,
      'source_workload':'PINNED_IOT_BENCHMARK_JAVA_ONE_SAMPLE_PER_CLASS',
      'not_official_iot_benchmark_sql_or_frequency_replay':True,
      'source_sha256':independent,
      'reference_v3_path':str(Path(old_v3).resolve()),'reference_rev2_path':str(Path(old_rev2).resolve()),
      'generator_preregistration_sha256':digest(workspace/'HOLDOUT_GENERATION_PREREGISTRATION.json'),
      'generator_result_sha256':digest(workspace/'HOLDOUT_GENERATION_OUTPUT.json'),
      'rev2_parent_selection_sha256':digest(exp/REV2/'stage8/28_hbench_v4_rev2_frozen.json'),
      'rev2_parent_manifest_sha256':digest(exp/REV2/'stage8/manifest.json'),
      'rev3_selection_sha256':digest(exp/REV3/'stage8/28_hbench_v4_rev3_frozen.json'),
      'rev3_manifest_sha256':digest(exp/REV3/'stage8/manifest.json'),
      'rev3_code_sha256':EXPECTED_REV3_CODE,
      'selected_ids':[h['deployment_id'] for h in selected],
      'rounds':rounds,'warmup':warmup,'page_size':page_size,
      'measurement_seed':measurement_seed,'run_tag':run_tag,
      'aggregation':'mean of per-class nearest-rank p95 (uniform Q1..Q12), not a mixture-workload p95',
      'no_measured_latency_available_to_selector':True,
      'no_cassandra_access_during_freeze':True,
      'destructive_operations_prohibited':True,
      'limitation':['Two DOUBLE aligned sensors in current Java bridge',
        'One representative Java-generated query object per class',
        'No official operation frequencies or SQL/IoTDB benchmark score',
        'Updates/concurrent TS4 maintenance outside this study']}
    p=workspace/PROTO
    dump(p,manifest)
    print('PRE-REGISTERED BEFORE TIMING:',p)
    print('Rev3 frozen SHA256:',manifest['rev3_selection_sha256'])
    print('Selected:',manifest['selected_ids'])
    return manifest

def verify(workspace):
    workspace=Path(workspace).resolve();exp=experiment_for(workspace)
    protocol=read(workspace/PROTO)
    if protocol['status']!='REV3_NEW_JAVA_HOLDOUT_FROZEN_BEFORE_CASSANDRA':raise ValueError('Invalid protocol status')
    assert_rev3_implementation()
    independent=check_new_generation(exp,protocol['reference_v3_path'],protocol['reference_rev2_path'])
    if independent!=protocol['source_sha256']:raise ValueError('Holdout data/queries or references changed')
    for file,key in [(workspace/'HOLDOUT_GENERATION_PREREGISTRATION.json','generator_preregistration_sha256'),
                     (workspace/'HOLDOUT_GENERATION_OUTPUT.json','generator_result_sha256'),
                     (exp/REV2/'stage8/28_hbench_v4_rev2_frozen.json','rev2_parent_selection_sha256'),
                     (exp/REV2/'stage8/manifest.json','rev2_parent_manifest_sha256'),
                     (exp/REV3/'stage8/28_hbench_v4_rev3_frozen.json','rev3_selection_sha256'),
                     (exp/REV3/'stage8/manifest.json','rev3_manifest_sha256')]:
        if digest(file)!=protocol[key]:raise ValueError('Frozen file or preregistration changed: '+str(file))
    from v4_rev2 import prebenchmark as R2
    from rev3 import prebenchmark as R3
    r2=R2.verify(exp);r3=R3.verify(exp)
    if not r2['independent_from_original_v3']:raise ValueError('Rev2 parent independent flag missing')
    selection=read(exp/REV3/'stage8/28_hbench_v4_rev3_frozen.json')
    if [h['deployment_id'] for h in selection]!=protocol['selected_ids']:
        raise ValueError('Frozen selected IDs changed')
    if r3['source_sha256']!=protocol['source_sha256']['new']:
        raise ValueError('Rev3 source hashes not bound to protocol')
    return exp,protocol,selection

if __name__=='__main__':
    ap=argparse.ArgumentParser(description='Pre-measurement freeze verification')
    ap.add_argument('--workspace',required=True)
    args=ap.parse_args()
    print('PASS:',verify(args.workspace)[0])
