"""Generate / freeze / preflight / benchmark / package a new Java holdout.

All generation and freeze operations finish before any Cassandra measurement.
The Rev2/Rev3 selectors are imported unchanged from the audited add-ons.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sys
import zipfile
from . import gate


def environment(exp):
    exp=Path(exp).resolve()
    os.chdir(exp)
    if str(exp) not in sys.path:sys.path.insert(0,str(exp))


def generate(args):
    from v4_rev2.prepare_holdout import prepare
    src=args.source_project.resolve();old3=args.reference_v3.resolve()
    old2=args.reference_rev2.resolve();workspace=args.workspace.resolve()
    if args.data_seed==args.query_seed:raise ValueError('Two distinct seeds required')
    if args.data_seed in (666,19243) or args.query_seed in (666,38421):
        raise ValueError('Do not reuse historical V3 or Rev2 generator seeds')
    if old2==old3 or src==workspace or workspace.exists():
        raise ValueError('Choose distinct reference experiments and a fresh workspace')
    if not (old2/'outputs/v4_rev2_prebenchmark/stage8/manifest.json').is_file():
        raise FileNotFoundError('Previous real Rev2 experiment missing')
    if args.devices*2*args.loops*args.batch_size>args.max_cells:
        raise ValueError('Scale exceeds bounded maximum logical cells')
    print('GENERATE: new, isolated official Java upstream data and 12 query objects')
    prepare(args)
    if args.dry_run:return
    exp=gate.experiment_for(workspace)
    environment(exp)
    audit=gate.check_new_generation(exp,old3,old2)
    reg={'status':'NEW_JAVA_GENERATION_UNMEASURED','experiment':str(exp),
        'workspace':str(workspace),'reference_v3':str(old3),'reference_rev2':str(old2),
        'source_project':str(src),'source_sha256':audit,
        'generator_seeds':{'data_seed':args.data_seed,'query_seed':args.query_seed},
        'scale':{'devices':args.devices,'loops':args.loops,'batch_size':args.batch_size,
                 'max_cells':args.max_cells,'point_step_ms':args.point_step_ms,
                 'query_interval_ms':args.query_interval_ms,'group_by_ms':args.group_by_ms},
        'no_cassandra_access_during_generation':True}
    gate.dump(workspace/'REV3_NEW_CAMPAIGN_REGISTRY.json',reg)
    print('SUCCESS: independent new source hashes, before all Rev3 timing')
    print('FRESH EXPERIMENT:',exp)


def freeze(args):
    workspace=args.workspace.resolve();exp=gate.experiment_for(workspace)
    reg=gate.read(workspace/'REV3_NEW_CAMPAIGN_REGISTRY.json')
    if reg['status']!='NEW_JAVA_GENERATION_UNMEASURED':raise ValueError('Unrecognized generation status')
    if reg['source_sha256']!=gate.check_new_generation(exp,reg['reference_v3'],reg['reference_rev2']):
        raise ValueError('Generated dataset changed after registration')
    environment(exp)
    gate.assert_rev3_implementation()
    from v4_rev2 import prebenchmark as R2
    from rev3 import prebenchmark as R3
    r2dir=exp/gate.REV2;r3dir=exp/gate.REV3
    if not r2dir.exists():R2.freeze(exp,Path(reg['reference_v3']),True)
    else:R2.verify(exp)
    if not r3dir.exists():R3.generate(exp)
    else:R3.verify(exp)
    gate.seal(workspace,reg['reference_v3'],reg['reference_rev2'],
       rounds=args.rounds,warmup=args.warmup,page_size=args.page_size,
       measurement_seed=args.measurement_seed,run_tag=args.run_tag)
    print('NO latency was used for selecting/finalizing the frozen designs')


def preflight(args):
    exp=gate.experiment_for(args.workspace)
    environment(exp)
    _,proto,selected=gate.verify(args.workspace)
    print('PASS: new-generation and freeze integrity')
    print('Oracle: 12 query objects (one sample per class, not official SQL replay)')
    print('Rounds',proto['rounds'],'Warmup',proto['warmup'],'page size',proto['page_size'])
    for h in selected:
        print(h['deployment_id'],h['canonical_source'],h['selection_reason'],
              'Q8:',h['query_routes']['Q8']['physical_plan'])
    print('Predicted physical costs:',[(h['deployment_id'],h['analytic_cost']) for h in selected])


def benchmark(args):
    exp=gate.experiment_for(args.workspace)
    environment(exp)
    from .benchmark import run
    run(args.workspace,args.host,args.port,args.confirm_create,args.max_cells)


def pack(args):
    workspace=args.workspace.resolve();exp=gate.experiment_for(workspace)
    environment(exp)
    _,p,selected=gate.verify(workspace)
    bench=exp/'outputs/rev3_independent_confirmatory'
    if not bench.is_dir() or not list(bench.glob('batch_*/manifest.json')):
        raise ValueError('Benchmark not completed; no archive can be described as complete')
    target=args.out.resolve()
    if target.exists():raise FileExistsError('No overwriting previous export ZIP')
    items={}
    workspace_files=['HOLDOUT_GENERATION_PREREGISTRATION.json',
      'HOLDOUT_GENERATION_OUTPUT.json','REV3_NEW_CAMPAIGN_REGISTRY.json',gate.PROTO]
    for name in workspace_files:
        items['provenance/'+name]=workspace/name
    file_allow=['phase_a1/outputs/normalized_double_readings.csv',
      'phase_a1/outputs/query_instances_pilot.json',
      'outputs/stage8/32_frozen_manifest.json','outputs/stage8/28_hbench_frozen.json']
    for name in file_allow:items['experiment/'+name]=exp/name
    for prefix in (gate.REV2,gate.REV3,'outputs/rev3_independent_confirmatory','outputs/rev3_postbenchmark_storage'):
        directory=exp/prefix
        for f in directory.rglob('*'):
            if f.is_file():items['experiment/'+str(f.relative_to(exp))]=f
    capture=exp.parent/'native_queries.jsonl'
    if capture.exists():items['java_capture/native_queries.jsonl']=capture
    target.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(target,'x',compression=zipfile.ZIP_DEFLATED,compresslevel=8) as z:
        for name,src in sorted(items.items()):
            if not src.is_file():raise FileNotFoundError('Required audit file missing: '+str(src))
            z.write(src,arcname=name)
    print('EXPORTED:',target,'files:',len(items))


def main():
    ap=argparse.ArgumentParser(description='SchemaLens Rev3 independent holdout campaign')
    sub=ap.add_subparsers(dest='action',required=True)
    gen=sub.add_parser('generate')
    gen.add_argument('--source-project',type=Path,required=True)
    gen.add_argument('--reference-v3',type=Path,required=True)
    gen.add_argument('--reference-rev2',type=Path,required=True)
    gen.add_argument('--workspace',type=Path,required=True)
    gen.add_argument('--devices',type=int,required=True)
    gen.add_argument('--loops',type=int,required=True)
    gen.add_argument('--batch-size',type=int,default=5)
    gen.add_argument('--max-cells',type=int,default=100000)
    gen.add_argument('--point-step-ms',type=int,default=600000)
    gen.add_argument('--query-interval-ms',type=int,required=True)
    gen.add_argument('--group-by-ms',type=int,default=3600000)
    gen.add_argument('--data-seed',type=int,required=True)
    gen.add_argument('--query-seed',type=int,required=True)
    gen.add_argument('--repo-dir',type=Path)
    gen.add_argument('--skip-java-build',action='store_true')
    gen.add_argument('--dry-run',action='store_true')
    frz=sub.add_parser('freeze')
    frz.add_argument('--workspace',type=Path,required=True)
    frz.add_argument('--rounds',type=int,default=60)
    frz.add_argument('--warmup',type=int,default=5)
    frz.add_argument('--page-size',type=int,default=16)
    frz.add_argument('--measurement-seed',type=int,required=True)
    frz.add_argument('--run-tag',type=str,required=True)
    pre=sub.add_parser('preflight')
    pre.add_argument('--workspace',type=Path,required=True)
    run=sub.add_parser('benchmark')
    run.add_argument('--workspace',type=Path,required=True)
    run.add_argument('--host',default='127.0.0.1')
    run.add_argument('--port',type=int,default=9043)
    run.add_argument('--max-cells',type=int,default=100000)
    run.add_argument('--confirm-create',default='')
    export=sub.add_parser('pack')
    export.add_argument('--workspace',type=Path,required=True)
    export.add_argument('--out',type=Path,required=True)
    args=ap.parse_args()
    actions={'generate':generate,'freeze':freeze,'preflight':preflight,'benchmark':benchmark,'pack':pack}
    actions[args.action](args)

if __name__=='__main__':main()
