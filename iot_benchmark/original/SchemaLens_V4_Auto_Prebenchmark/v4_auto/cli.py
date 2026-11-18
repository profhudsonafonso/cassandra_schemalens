"""CLI for automatic V4 pre-benchmark freeze, independent of V3 Stage8 writes."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import shutil
import sys
import tempfile
from . import selector as S

OUTPUT='outputs/v4_auto_prebenchmark'

def is_inside(p,root):
    try:p.resolve().relative_to(root.resolve());return True
    except ValueError:return False


def get_inputs(exp):
    if not (exp/'iot_pipeline/native_reference_v3.py').is_file():
        raise ValueError('Install existing native_reference_v3 add-on in isolated experiment first')
    if Path(__import__('iot_pipeline.shared',fromlist=['ROOT']).ROOT).resolve()!=exp:
        raise ValueError('Wrong iot_pipeline import path. Use the bundled runner that cd into the frozen experiment')
    from iot_pipeline.shared import read_rows
    # Explicit read allowlist: no outputs from Stage9 or diagnostics are consumed.
    f={
      'candidate_specs':exp/'outputs/stage4/10_candidate_specs.json',
      'activation':exp/'phase_a1/outputs/activation_ts_only.json',
      'queries':exp/'phase_a1/outputs/query_instances_pilot.json',
      'data':exp/'phase_a1/outputs/normalized_double_readings.csv',
      'policy':exp/'config/pipeline_policy.json',
    }
    for label,p in f.items():
        if not p.is_file():raise FileNotFoundError(f'Missing {label}: {p}')
    inputs_sha={label:S.digest(path) for label,path in f.items()}
    policy=S.jread(f['policy'])
    if policy.get('benchmark_metrics_allowed_for_selection') is not False:
        raise ValueError('Policy does not forbid benchmark selection')
    if policy['scope']!='SCHEMALENS_TS_ONLY_IOT_CASSANDRA':raise ValueError('Wrong policy scope')
    queries=S.jread(f['queries']);rows=read_rows(f['data'])
    if not rows:raise ValueError('Empty A1 data')
    activation=S.jread(f['activation'])
    capture=S.verified_native(exp,queries)
    return f,inputs_sha,policy,queries,rows,activation,capture


def audit_existing(exp,out):
    manifest=S.jread(out/'stage8/manifest.json')
    S.verify_old(exp)
    if manifest.get('status')!='FROZEN_V4_AUTO_PREBENCH':raise ValueError('Unfrozen auto selection')
    if manifest['v3_hbench_sha256']!=S.EXPECTED_FROZEN:raise ValueError('Frozen parent mismatch')
    current_code={'selector.py':S.digest(Path(S.__file__)), 'cli.py':S.digest(Path(__file__))}
    if manifest.get('implementation_sha256')!=current_code:
        raise ValueError('V4 selector implementation differs from the frozen revision')
    for rel,expected in manifest['file_sha256'].items():
        if S.digest(out/rel)!=expected:raise ValueError('V4 file content was modified: '+rel)
    for name,expected in manifest['original_inputs_sha256'].items():
        path=exp / {'candidate_specs':'outputs/stage4/10_candidate_specs.json',
                    'activation':'phase_a1/outputs/activation_ts_only.json',
                    'queries':'phase_a1/outputs/query_instances_pilot.json',
                    'data':'phase_a1/outputs/normalized_double_readings.csv',
                    'policy':'config/pipeline_policy.json'}[name]
        if S.digest(path)!=expected:raise ValueError('Input modified after V4 selection: '+name)
    return manifest


def run(exp):
    exp=Path(exp).resolve()
    before=S.verify_old(exp)
    f,inp,policy,queries,rows,activation,capture=get_inputs(exp)
    # This adapter imports only native logical oracle and reader; no measured results.
    from iot_pipeline.native_reference_v3 import requests_for,source_oracle
    from iot_pipeline.offline import compare
    from iot_pipeline.shared import read
    oracle=source_oracle()
    eligible,rejected=S.candidate_catalog(read(f['candidate_specs']),activation,rows,policy)
    coverage=S.physical_reads(rows,queries,eligible,oracle,compare,requests_for,activation)
    w,st,r=S.derive_analytic(rows,queries,eligible,coverage,policy,requests_for)
    allcovers,chosen,pareto=S.search_covers(eligible,coverage,st,w,r,queries,policy)
    after=S.verify_old(exp)
    if before!=after:raise ValueError('V3 files changed during auto selection!')
    target=exp/OUTPUT
    if target.exists():raise FileExistsError(f'V4 freeze already exists, refusing overwrite: {target}')
    if not is_inside(target,exp/'outputs'):raise ValueError('Output path outside experiment outputs')
    target.parent.mkdir(exist_ok=True,parents=True)
    staging=Path(tempfile.mkdtemp(prefix='.v4_auto_staging_',dir=target.parent))
    try:
        S.dump(staging/'stage4/10_v4_candidate_specs.json',eligible)
        S.dump(staging/'stage4/11_not_instantiated.json',rejected)
        S.dump(staging/'stage5/15_cross_query_coverage.json',coverage)
        S.csv_out(staging/'stage5/16_cross_query_coverage.csv',coverage,
                  ['query_id','candidate_id','family','route_status','verification','method','cross_query_reuse'])
        S.dump(staging/'stage6/18_v4_write_plans.json',w)
        S.dump(staging/'stage7/21_storage_proxies.json',st)
        S.dump(staging/'stage7/22_write_proxies.json',w)
        S.dump(staging/'stage7/23_read_proxies.json',r)
        S.dump(staging/'stage8/28_hbench_v4_auto_frozen.json',chosen)
        S.dump(staging/'stage8/29_all_valid_covers.json',allcovers)
        records=[]
        for h in chosen:
            records.append({'deployment_id':h['deployment_id'],'canonical_source':h['canonical_source'],
                      'families':'+'.join(h['families']),'selection_reason':h['selection_reason'],
                      'read_proxy':h['analytic_cost']['read'],'storage_proxy':h['analytic_cost']['storage'],
                      'write_proxy':h['analytic_cost']['write'],'score':h['preselection_score']})
        S.csv_out(staging/'stage8/30_hbench_v4_auto_summary.csv',records,
                 ['deployment_id','canonical_source','families','selection_reason','read_proxy','storage_proxy','write_proxy','score'])
        route_rows=[{'deployment_id':x['deployment_id'],'query_id':qid,**v} for x in chosen for qid,v in x['query_routes'].items()]
        S.csv_out(staging/'stage8/31_cross_query_routes.csv',route_rows,
                  ['deployment_id','query_id','candidate_id','family','plan_id','route_status','reason'])
        artifact_hashes={str(p.relative_to(staging)):S.digest(p) for p in staging.rglob('*') if p.is_file()}
        status={'stage':8,'revision':S.METHODOLOGY,'status':'FROZEN_V4_AUTO_PREBENCH',
                'v3_hbench_sha256':S.EXPECTED_FROZEN,'v3_parent_file_sha256':before,
                'native_capture_file':str(Path(capture['path']).name),'native_capture_sha256':capture['sha256'],
                'original_inputs_sha256':inp,'file_sha256':artifact_hashes,
                'implementation_sha256':{
                    'selector.py':S.digest(Path(S.__file__)),
                    'cli.py':S.digest(Path(__file__)),
                },'no_stage9_or_diagnostics_used':True,'no_cassandra_used':True,
                'no_measured_latency_or_throughput_used':True,
                'manual_V4_1_7_30_day_benchmarks_not_input':True,
                'v3_stage4_8_outputs_unchanged':True,
                'native_java_parameter_objects':len(queries),'metric_cells':len(rows),
                'eligible_candidates':len(eligible),'cross_query_coverage_tests':len(coverage),
                'number_of_compatible_covers':len(allcovers),
                'number_on_pareto_front':len(pareto),'number_selected':len(chosen),
                'top_k':policy['top_k'],'selected_ids':[x['deployment_id'] for x in chosen],
                'candidate_families_reused':['TS0','TS2','TS4'],
                'selection_policy':'Pareto first; one best canonical architecture each when capacity remains',
                'future_benchmark_only':'Existing V4 live results are exploratory, not independent hold-out. New validation must use independent generation/seed.',
                'limitations':['Table schema and query parameter objects from native Java sampler; not official IoTDB SQL/frequency replay',
                   'Cross-query correctness is against in-memory logical oracle, not official SQL engine',
                   'TS4 batch snapshot semantics only; concurrent writes/unordered updates not validated',
                   'Physical scan cost is an analytical approximation, not Cassandra profiling',
                   'No automatically integrated V4 Stage9 runner is supplied; selection is OFFLINE ONLY']}
        S.dump(staging/'stage8/manifest.json',status)
        staging.rename(target)
    except Exception:
        shutil.rmtree(staging,ignore_errors=True)
        raise
    if before!=S.verify_old(exp):raise ValueError('Original V3 changed after V4 output!')
    verified=audit_existing(exp,target)
    print('PASS: V4 AUTO PRE-BENCHMARK FROZEN, no Cassandra, no Stage9 data')
    print('Native metric cells:',len(rows),'cross-query checks:',len(coverage))
    print('Eligible physical candidates:',len(eligible),'compatible covers:',len(allcovers),
          'Pareto:',len(pareto),'selected:',len(chosen))
    for h in chosen:print(' ',h['deployment_id'],'source:',h['canonical_source'],
                         'families:',','.join(h['families']),'reason:',h['selection_reason'])
    print('HBench:',target/'stage8/28_hbench_v4_auto_frozen.json')
    print('Manifest:',target/'stage8/manifest.json')
    print('HBench SHA256:',S.digest(target/'stage8/28_hbench_v4_auto_frozen.json'))
    return verified


def main():
    p=argparse.ArgumentParser(description='V4 automatic OFFLINE selection; never reads results from Stage9')
    p.add_argument('--experiment',type=Path,required=True)
    p.add_argument('--verify-only',action='store_true')
    a=p.parse_args()
    exp=a.experiment.resolve()
    if a.verify_only:
        audit_existing(exp,exp/OUTPUT)
        print('PASS: V4 auto freeze byte-level integrity verified; V3 still frozen')
    else:run(exp)

if __name__=='__main__':main()
