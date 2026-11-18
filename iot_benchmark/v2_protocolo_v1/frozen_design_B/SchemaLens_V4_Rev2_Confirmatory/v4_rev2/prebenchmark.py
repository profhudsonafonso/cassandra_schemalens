"""Independent V4 Rev2 Stage7/8 freeze, no Cassandra and no Stage9 results.

Uses existing V4 Stage4-6 semantics as-is. For a new IoT Benchmark native
experiment, verifies its own V3 freeze instead of requiring V3's old fixed hash.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
from v4_auto import selector as A
from v4_auto import cli as OLD
from .cost import derive_rev2,select_rev2,REV2_POLICY,REVISION

OUTPUT='outputs/v4_rev2_prebenchmark'
BASELINE_OLD_SHA={
 'data':'ff3a2e3fa38d933e35516ad11898f2c58d7278ac591fbc29f0a12649050ac65b',
 'queries':'94ddf757690ff9aab280871a353c94252e7b28ee93c98ef4b67726c7cda3408c',
 'capture':'e5da8bb7382edbb6e50acc010fe553473758d615759a179641bfd74a5de50aeb'
}

def audit_parent(exp):
    exp=Path(exp).resolve()
    stage8=exp/'outputs/stage8'
    m=A.jread(stage8/'32_frozen_manifest.json')
    if m.get('status')!='FROZEN_PREBENCH_SELECTION' or m.get('benchmark_measured') is not False:
        raise ValueError('Parent V3 prebenchmark manifest is not frozen and unmeasured')
    frozen=stage8/'28_hbench_frozen.json'
    files={'outputs/stage8/28_hbench_frozen.json':A.digest(frozen),
           'outputs/stage8/32_frozen_manifest.json':A.digest(stage8/'32_frozen_manifest.json')}
    if not A.jread(frozen):raise ValueError('Parent V3 HBench is empty')
    for rel,expected in m['inputs_sha256'].items():
        path=exp/rel
        if not path.is_file() or A.digest(path)!=expected:
            raise ValueError('Parent V3 frozen input hash mismatch: '+rel)
        files[rel]=expected
    return files

def find_capture(exp,queries):
    expected={q.get('capture_sha256') for q in queries}
    if len(expected)!=1 or not next(iter(expected)):
        raise ValueError('Missing consistent Java capture SHA')
    expected=next(iter(expected))
    candidates=[]
    # The native Java generator writes native_queries.jsonl.  The holdout
    # preparation also writes one byte-identical q12fixed_* provenance alias,
    # which Rev1's verified_native() requires. Prefer that unique alias.
    aliases=list(exp.parent.glob('native_queries_q12fixed_*.jsonl'))
    if aliases:candidates=aliases
    else:
        for directory in (exp.parent,exp.parent.parent):
            candidates.extend(directory.glob('native_queries*.jsonl'))
    matches=sorted(set(p.resolve() for p in candidates if A.digest(p)==expected))
    if len(matches)!=1:raise ValueError('Unable to verify unique new Java capture by SHA-256')
    A.verified_native.__doc__  # A1 provenance semantics retained
    for q in queries:
        if 'sample from official GenerateQueryWorkLoad Java API' not in q.get('parameter_origin',''):
            raise ValueError('Java query-object sampling provenance missing')
    if {q['query_id'] for q in queries}!={f'Q{i}' for i in range(1,13)} or len(queries)!=12:
        raise ValueError('Expected exactly 12 representative native query objects')
    return {'sha256':expected,'file':str(matches[0])}

def check_holdout(exp,baseline=None,require_independent=False):
    if baseline and Path(baseline).resolve()==Path(exp).resolve():
        raise ValueError('Baseline holdout path is the very same experiment')
    samples={'data':A.digest(exp/'phase_a1/outputs/normalized_double_readings.csv'),
             'queries':A.digest(exp/'phase_a1/outputs/query_instances_pilot.json')}
    if require_independent:
        if not baseline:raise ValueError('Confirmatory freeze requires --reference-v3 ORIGINAL_FROZEN_PATH')
        old={k:A.digest(Path(baseline)/rel) for k,rel in {
            'data':'phase_a1/outputs/normalized_double_readings.csv',
            'queries':'phase_a1/outputs/query_instances_pilot.json'}.items()}
        if any(samples[k]==old[k] for k in samples):
            raise ValueError('Holdout must differ in BOTH dataset and Java query parameters')
        if any(samples[k]==BASELINE_OLD_SHA[k] for k in samples):
            raise ValueError('Holdout matches original V3 historical data/query; abort')
    return samples

def freeze(exp,reference_v3=None,confirmatory=False):
    exp=Path(exp).resolve()
    before=audit_parent(exp)
    if (exp/OUTPUT).exists():raise FileExistsError('Rev2 freeze already exists; use --verify-only')
    # Code and inputs both immutable in the manifest after freezing.
    files,input_hashes,base_policy,queries,rows,activation,_=OLD.get_inputs(exp)
    capture=find_capture(exp,queries)
    if confirmatory:
        # Do not accept a forged/synthetic JSONL with a merely matching SHA field.
        from phase_a1.tools.official_query_capture import validate
        from pathlib import Path as _Path
        by_class,counts=validate(_Path(capture['file']),[f'Q{i}' for i in range(1,13)])
        if not all(counts.get(f'Q{i}',0)>=1 for i in range(1,13)):
            raise ValueError('No genuine Java parameter object for some query class')
    current=check_holdout(exp,reference_v3,require_independent=confirmatory)
    from iot_pipeline.native_reference_v3 import requests_for,source_oracle
    from iot_pipeline.offline import compare
    from iot_pipeline.shared import read
    candidates,rejected=A.candidate_catalog(read(files['candidate_specs']),activation,rows,base_policy)
    coverage=A.physical_reads(rows,queries,candidates,source_oracle(),compare,requests_for,activation)
    writes,storage,reads,parts,coeff=derive_rev2(rows,queries,candidates,coverage,base_policy,requests_for)
    passing=[c for c in candidates if parts[c['candidate_id']]['hard_guard_pass']]
    if len(passing)!=len(candidates):
        rejected += [{'candidate_id':c['candidate_id'],'reason':'Rev2 estimated partition exceeds hard guard'}
                     for c in candidates if c not in passing]
        coverage=[x for x in coverage if x['candidate_id'] in {p['candidate_id'] for p in passing}]
        reads=[x for x in reads if x['candidate_id'] in {p['candidate_id'] for p in passing}]
        writes=[x for x in writes if x['candidate_id'] in {p['candidate_id'] for p in passing}]
        storage=[x for x in storage if x['candidate_id'] in {p['candidate_id'] for p in passing}]
    covers,selected,pareto=select_rev2(passing,coverage,storage,writes,reads,queries,base_policy)
    assert before==audit_parent(exp),'Parent V3 modified during selection'
    root=exp/OUTPUT;root.parent.mkdir(parents=True,exist_ok=True)
    tmp=Path(tempfile.mkdtemp(prefix='.rev2_prebench_staging_',dir=root.parent))
    try:
        A.dump(tmp/'stage4/10_v4_candidate_specs.json',passing)
        A.dump(tmp/'stage4/11_not_instantiated.json',rejected)
        A.dump(tmp/'stage5/15_cross_query_coverage.json',coverage)
        A.csv_out(tmp/'stage5/16_cross_query_coverage.csv',coverage,
           ['query_id','candidate_id','family','route_status','verification','cross_query_reuse','method'])
        A.dump(tmp/'stage6/18_v4_write_plans.json',writes)
        A.dump(tmp/'stage7/21_storage_proxies.json',storage)
        A.dump(tmp/'stage7/22_write_proxies.json',writes)
        A.dump(tmp/'stage7/23_read_proxies_rev2.json',reads)
        A.dump(tmp/'stage7/24_partition_proxies_rev2.json',list(parts.values()))
        A.dump(tmp/'stage7/25_rev2_cost_policy.json',coeff)
        A.dump(tmp/'stage8/28_hbench_v4_rev2_frozen.json',selected)
        A.dump(tmp/'stage8/29_all_compatible_covers.json',covers)
        explanations=[]
        byread={(p['query_id'],p['candidate_id']):p for p in reads}
        for h in selected:
            explanations.append({'deployment_id':h['deployment_id'],
              'canonical_source':h['canonical_source'],'selection_reason':h['selection_reason'],
              'score':h['preselection_score'],'costs':h['analytic_cost'],
              'query_explanations':[{'query_id':qid,'candidate_id':route['candidate_id'],
                 'reason':route['reason'],
                 'plan_cost_features':byread[qid,route['candidate_id']]}
                for qid,route in sorted(h['query_routes'].items())]})
        A.dump(tmp/'stage8/32_selection_explanations_rev2.json',explanations)
        rows_summary=[{'deployment_id':h['deployment_id'],'canonical_source':h['canonical_source'],
           'families':'+'.join(h['families']),'selection_reason':h['selection_reason'],
           'read_proxy':round(h['analytic_cost']['read'],6),
           'storage_proxy':h['analytic_cost']['storage'],
           'write_proxy':h['analytic_cost']['write'],
           'score':h['preselection_score']} for h in selected]
        A.csv_out(tmp/'stage8/30_hbench_v4_rev2_summary.csv',rows_summary,
          ['deployment_id','canonical_source','families','selection_reason','read_proxy','storage_proxy','write_proxy','score'])
        A.csv_out(tmp/'stage8/31_cross_query_routes.csv',
          [{'deployment_id':h['deployment_id'],'query_id':qid,**route} for h in selected for qid,route in h['query_routes'].items()],
          ['deployment_id','query_id','candidate_id','family','plan_id','route_status','reason'])
        rec={'status':'FROZEN_V4_REV2_PREBENCH','stage':8,'revision':REVISION,
            'mode':'INDEPENDENT_HOLDOUT_FROZEN_BEFORE_MEASUREMENT' if confirmatory else 'EXPLORATORY_COST_REVISION',
            'independent_from_original_v3':bool(confirmatory),
            'same_dataset_as_V3':not bool(confirmatory),
            'original_historical_v3_hbench_hash':A.EXPECTED_FROZEN,
            'parent_v3_hbench_sha256':before['outputs/stage8/28_hbench_frozen.json'],
            'parent_v3_files_sha256':before,'inputs_sha256':input_hashes,
            'java_capture':capture,'dataset_query_hashes':current,
            'reference_v3_path_at_freeze':str(Path(reference_v3).resolve()) if reference_v3 else None,
            'reference_v3_data_hash':A.digest(Path(reference_v3)/'phase_a1/outputs/normalized_double_readings.csv') if reference_v3 else None,
            'reference_v3_query_hash':A.digest(Path(reference_v3)/'phase_a1/outputs/query_instances_pilot.json') if reference_v3 else None,
            'code_sha256':{'prebenchmark':A.digest(Path(__file__)),
                           'cost':A.digest(Path(__file__).with_name('cost.py')),
                           'v4_auto_selector':A.digest(Path(A.__file__))},
            'no_Cassandra_used':True,'no_Stage9_results_used':True,
            'no_measured_latency_used':True,'official_sql_or_frequency_replay':False,
            'candidate_count':len(passing),'cover_count':len(covers),'pareto_count':len(pareto),
            'selected_count':len(selected),'selected_ids':[h['deployment_id'] for h in selected],
            'top_k':base_policy['top_k'],'native_sample_count':len(queries),'metric_cells':len(rows),
            'limitation':['A1 Java query-object samples, not official SQL or operation-frequency sequence',
                  'TS4 maintenance concurrency not tested','Proxy bytes are analytical, not SSTable bytes',
                  'Rev2 coefficients fixed by engineering hypotheses; not empirically fitted']}
        rec['artifact_sha256']={str(p.relative_to(tmp)):A.digest(p) for p in tmp.rglob('*') if p.is_file()}
        A.dump(tmp/'stage8/manifest.json',rec)
        tmp.rename(root)
    except Exception:
        shutil.rmtree(tmp,ignore_errors=True);raise
    if before!=audit_parent(exp):raise ValueError('Original Stage4-8 files modified!')
    verify(exp)
    print('FROZEN: Rev2 Stage7/8; no Cassandra:',root)
    print('Candidate count:',len(passing),'compatible covers:',len(covers),'selected:',len(selected))
    for h in selected:print(' ',h['deployment_id'],h['canonical_source'],h['selection_reason'])
    print('SHA256:',A.digest(root/'stage8/28_hbench_v4_rev2_frozen.json'))
    return rec

def verify(exp):
    exp=Path(exp).resolve();before=audit_parent(exp)
    root=exp/OUTPUT;rec=A.jread(root/'stage8/manifest.json')
    if rec['status']!='FROZEN_V4_REV2_PREBENCH':raise ValueError('Rev2 manifest status is not frozen')
    if rec['parent_v3_hbench_sha256']!=before['outputs/stage8/28_hbench_frozen.json'] or rec['parent_v3_files_sha256']!=before:
        raise ValueError('Parent frozen V3 file modified since Rev2 freeze')
    impl={'prebenchmark':A.digest(Path(__file__)),
          'cost':A.digest(Path(__file__).with_name('cost.py')),
          'v4_auto_selector':A.digest(Path(A.__file__))}
    if rec['code_sha256']!=impl:raise ValueError('Source code changed since freeze')
    for rel,sha in rec['artifact_sha256'].items():
        if A.digest(root/rel)!=sha:raise ValueError('Rev2 output modified after freeze: '+rel)
    for key,p in {'candidate_specs':'outputs/stage4/10_candidate_specs.json',
                  'activation':'phase_a1/outputs/activation_ts_only.json',
                  'queries':'phase_a1/outputs/query_instances_pilot.json',
                  'data':'phase_a1/outputs/normalized_double_readings.csv',
                  'policy':'config/pipeline_policy.json'}.items():
        if rec['inputs_sha256'][key]!=A.digest(exp/p):raise ValueError('Frozen Rev2 input changed: '+key)
    if rec.get('independent_from_original_v3'):
        if not rec.get('reference_v3_path_at_freeze'):raise ValueError('Holdout reference missing')
        ref=Path(rec['reference_v3_path_at_freeze'])
        if A.digest(ref/'phase_a1/outputs/normalized_double_readings.csv')!=rec['reference_v3_data_hash'] or A.digest(ref/'phase_a1/outputs/query_instances_pilot.json')!=rec['reference_v3_query_hash']:
            raise ValueError('Historic reference experiment modified after holdout preregistration')
    return rec

def main():
    p=argparse.ArgumentParser(description='V4 Rev2 Stage7/8 isolated offline freeze')
    p.add_argument('--experiment',type=Path,required=True)
    p.add_argument('--reference-v3',type=Path)
    p.add_argument('--confirmatory',action='store_true')
    p.add_argument('--verify-only',action='store_true')
    args=p.parse_args()
    if args.verify_only:verify(args.experiment);print('PASS: frozen Rev2 provenance and outputs unchanged')
    else:freeze(args.experiment,args.reference_v3,args.confirmatory)
if __name__=='__main__':main()
