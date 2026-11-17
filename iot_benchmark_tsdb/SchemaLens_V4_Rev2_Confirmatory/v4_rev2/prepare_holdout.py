"""Make an *independent* IoT Benchmark Java data generation run in a copied project.

This module never connects to Cassandra and never modifies any frozen old project.
Copies a clean code-only source tree to a new workspace; sets new Java generator
seeds/time density; delegates actual generation to the project's existing pinned
upstream Java generator (not a synthetic time-shift workaround).
"""
from __future__ import annotations
import argparse
from pathlib import Path
import hashlib
import json
import shutil
import subprocess
import sys
from . import prebenchmark as P

SRC_REL='phase_a1/inputs/iot_benchmark_pilot.properties'

def edit_properties(path,values):
    lines=path.read_text(encoding='utf8').splitlines();seen=set();out=[]
    for line in lines:
        k=line.partition('=')[0].strip()
        if k in values:
            if k in seen:raise ValueError('Duplicate Java configuration key '+k)
            seen.add(k);out.append(k+'='+str(values[k]))
        else:out.append(line)
    for k in sorted(values.keys()-seen):out.append(k+'='+str(values[k]))
    path.write_text('\n'.join(out)+'\n',encoding='utf8')

def prepare(args):
    source=args.source_project.resolve();orig=args.reference_v3.resolve();dest=args.workspace.resolve()
    old=P.audit_parent(orig)
    if old['outputs/stage8/28_hbench_frozen.json']!=P.A.EXPECTED_FROZEN:
        raise ValueError('Reference V3 is not the known historical frozen experiment')
    if dest.exists():raise FileExistsError('Workspace already exists; never overwrite holdout evidence')
    if dest==source or source in dest.parents or orig in dest.parents:
        raise ValueError('Holdout workspace must be outside old source/experiment tree')
    if args.data_seed==666 or args.query_seed==666:
        raise ValueError('Choose DIFFERENT Java DATA_SEED and QUERY_SEED from historical 666')
    if args.data_seed==args.query_seed:raise ValueError('Use different data and query generation seeds')
    cells=args.devices*2*args.loops*args.batch_size
    if cells>args.max_cells or min(args.devices,args.loops,args.batch_size)<1:
        raise ValueError('Invalid dataset scale or max logical cells exceeded')
    if args.devices<3:raise ValueError('Use at least 3 devices for cross-device validation')
    if args.point_step_ms<1000 or args.query_interval_ms<args.point_step_ms:
        raise ValueError('Positive point step and adequate query interval required')
    generator=source/'scripts/auto_official_v3.py'
    if not generator.is_file():raise FileNotFoundError('Missing pinned native Java driver: '+str(generator))
    ref_native=orig/'iot_pipeline/native_reference_v3.py'
    if not ref_native.is_file():raise FileNotFoundError('Validated Cassandra translation native_reference_v3.py missing')
    settings={'DATA_SEED':args.data_seed,'QUERY_SEED':args.query_seed,
              'POINT_STEP':args.point_step_ms,'QUERY_INTERVAL':args.query_interval_ms,
              'GROUP_BY_TIME_UNIT':args.group_by_ms}
    if args.dry_run:
        print('DRY RUN ONLY; no files created. Logical metric cells:',cells)
        print('Source:',source,'output workspace:',dest,'generator overrides:',settings)
        print('Pinned IoT Benchmark upstream commit: 62e0dc100e56443978c8b4c66472b1d8a7ef0238')
        return
    dest.mkdir(parents=True,exist_ok=False)
    try:
        copied=dest/'schema_project_isolated'
        def ignore(base,names):
            excluded={'native_auto_work','outputs','.git','__pycache__','.venv','.pytest_cache','node_modules','archive'}
            return {n for n in names if n in excluded or n.endswith('.zip') or n.endswith('.pyc')}
        shutil.copytree(source,copied,ignore=ignore)
        dst_ref=copied/'iot_pipeline/native_reference_v3.py'
        if dst_ref.exists() and dst_ref.read_bytes()!=ref_native.read_bytes():
            raise ValueError('Conflicting native reference add-on in copied project; manual inspection required')
        if not dst_ref.exists():shutil.copy2(ref_native,dst_ref)
        edit_properties(copied/SRC_REL,settings)
        provenance={'status':'HOLDOUT_GENERATOR_REGISTERED_BEFORE_STAGE8',
          'reference_v3_hbench_sha256':old['outputs/stage8/28_hbench_frozen.json'],
          'generation_settings':settings,'devices':args.devices,'loops':args.loops,
          'batch_size':args.batch_size,'max_cells':args.max_cells,
          'original_generator_script_sha256':hashlib.sha256(generator.read_bytes()).hexdigest(),
          'java_upstream_commit':'62e0dc100e56443978c8b4c66472b1d8a7ef0238',
          'sql_replay':False,'operation_frequency_replay':False,
          'copied_reference_adapter_sha256':hashlib.sha256(ref_native.read_bytes()).hexdigest(),
          'no_cassandra':True}
        (dest/'HOLDOUT_GENERATION_PREREGISTRATION.json').write_text(json.dumps(provenance,indent=2)+'\n')
        call=[sys.executable,str(copied/'scripts/auto_official_v3.py'),
              '--workdir',str(dest/'java_work'), '--devices',str(args.devices),
              '--loops',str(args.loops),'--batch-size',str(args.batch_size),
              '--max-cells',str(args.max_cells),'--samples-per-class','1']
        if args.repo_dir:call.extend(['--repo-dir',str(args.repo_dir.resolve())])
        if args.skip_java_build:call.append('--skip-java-build')
        print('Generating genuinely new Java workload:',' '.join(call),flush=True)
        subprocess.run(call,cwd=copied,check=True)
        runs=sorted((dest/'java_work/runs').glob('v3_*'))
        if len(runs)!=1:raise ValueError('Expected exactly one independent Java generator run')
        exp=runs[0]/'schemalens'
        if not (exp/'outputs/stage8/32_frozen_manifest.json').is_file():
            raise ValueError('Fresh V3 Stage8 freeze absent; inspect generator log')
        # Preserve original Java bytes. Required only because Rev1 provenance
        # checker expects the q12fixed_* filename pattern of the historic run.
        cap=runs[0]/'native_queries.jsonl'
        if not cap.is_file():raise ValueError('Native Java capture missing in new generator run')
        alias=runs[0]/'native_queries_q12fixed_confirm_rev2.jsonl'
        if alias.exists():raise FileExistsError('Existing native capture alias')
        shutil.copy2(cap,alias)
        cap_hash=hashlib.sha256(cap.read_bytes()).hexdigest()
        if cap_hash!=hashlib.sha256(alias.read_bytes()).hexdigest():raise AssertionError('Capture alias bytes changed')
        (dest/'HOLDOUT_GENERATION_OUTPUT.json').write_text(json.dumps({
            'source_java_capture_sha256':cap_hash,'alias_has_identical_bytes':True,
            'native_data_dir':str(runs[0]/'native_data'), 'isolated_v3_experiment':str(exp),
            'native_queries_alias':str(alias),'status':'JAVA_GENERATOR_COMPLETE_NO_CASSANDRA'
        },indent=2)+'\n')
        print('FRESH EXPERIMENT:',exp)
        print('NEXT: Freeze V4 Rev2 using --confirmatory --reference-v3 OLD_EXPERIMENT')
    except Exception:
        # Preserve any failed workspace for debugging; do not mask partial generation.
        raise

def main():
    p=argparse.ArgumentParser(description='Independent IoT Benchmark holdout generation; no Cassandra')
    p.add_argument('--source-project',type=Path,required=True)
    p.add_argument('--reference-v3',type=Path,required=True)
    p.add_argument('--workspace',type=Path,required=True)
    p.add_argument('--repo-dir',type=Path)
    p.add_argument('--devices',type=int,default=6)
    p.add_argument('--loops',type=int,default=300)
    p.add_argument('--batch-size',type=int,default=5)
    p.add_argument('--max-cells',type=int,default=100000)
    p.add_argument('--point-step-ms',type=int,default=600000)
    p.add_argument('--query-interval-ms',type=int,default=86400000)
    p.add_argument('--group-by-ms',type=int,default=3600000)
    p.add_argument('--data-seed',type=int,default=19243)
    p.add_argument('--query-seed',type=int,default=38421)
    p.add_argument('--skip-java-build',action='store_true')
    p.add_argument('--dry-run',action='store_true')
    prepare(p.parse_args())
if __name__=='__main__':main()
