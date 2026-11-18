#!/usr/bin/env python3
"""Diagnose/recover a partially generated V4 Rev2 IoT Java holdout.

No Cassandra, no changes to original V3/V4 HBench or failed generation.
A recovered experiment is constructed in a NEW sibling directory of the
already generated Java run and reruns A1 -> native V3 Stage8 from Java bytes.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from datetime import datetime, timezone

ORIGINAL_HBENCH_SHA = '2c4f46c70fc272046b42d1c7a8e6fc7c20c4ca9d8b63cef707ff7f89fa3fa6eb'
OLD_CAPTURE_SHA = 'e5da8bb7382edbb6e50acc010fe553473758d615759a179641bfd74a5de50aeb'
OLD_DATA_SHA = 'ff3a2e3fa38d933e35516ad11898f2c58d7278ac591fbc29f0a12649050ac65b'
ALIAS = 'native_queries_q12fixed_confirm_rev2.jsonl'
RUN_SUFFIX = 'schemalens_rev2_recovered'

def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for b in iter(lambda: stream.read(1 << 20), b''):h.update(b)
    return h.hexdigest()

def read_json(p: Path):return json.loads(p.read_text(encoding='utf-8'))

def tail(p:Path,lines:int=18):
    if not p.is_file():return '(log not present)'
    with p.open(encoding='utf-8',errors='replace') as f:
        from collections import deque
        return ''.join(deque(f,maxlen=lines)).rstrip()

def check_reference(ref: Path):
    p=ref/'outputs/stage8/28_hbench_frozen.json'
    if not p.is_file() or digest(p)!=ORIGINAL_HBENCH_SHA:
        raise ValueError('Original V3 HBench missing or its SHA-256 has changed')
    return digest(p)

def run_roots(workspace:Path):
    d=workspace/'java_work/runs'
    return sorted(x for x in d.glob('v3_*') if x.is_dir())

def choose_run(workspace:Path,explicit:Path|None):
    if explicit:
        p=explicit.resolve()
        if p.parent!=(workspace/'java_work/runs').resolve() or not p.is_dir():
            raise ValueError('--run-dir must be a v3_* directory inside this workspace/java_work/runs')
        return p
    found=run_roots(workspace)
    if len(found)!=1:raise ValueError(f'Expected exactly one Java run in {workspace}, found {len(found)}; specify --run-dir')
    return found[0]

def capture_check(workspace:Path,run:Path,reference:Path):
    old=check_reference(reference)
    prereg=workspace/'HOLDOUT_GENERATION_PREREGISTRATION.json'
    if not prereg.is_file():raise FileNotFoundError('Missing original preregistration: '+str(prereg))
    reg=read_json(prereg)
    if reg.get('java_upstream_commit')!='62e0dc100e56443978c8b4c66472b1d8a7ef0238':
        raise ValueError('Unexpected IoT Benchmark upstream commit in preregistration')
    if reg.get('reference_v3_hbench_sha256')!=old:
        raise ValueError('Holdout preregistration reference HBench does not match original')
    cap=run/'native_queries.jsonl'
    schema=run/'native_data/schema.txt'
    nativeinfo=run/'native_data/info.txt'
    cfg=run/'java_config/config.properties'
    batch=list((run/'native_data').glob('*/BigBatch_*.csv'))
    needed=[cap,schema,nativeinfo,cfg]
    for file in needed:
        if not file.is_file() or not file.stat().st_size:
            raise FileNotFoundError('Incomplete Java generation: missing '+str(file))
    if not batch:raise FileNotFoundError('No native BigBatch_*.csv files; generation incomplete')
    h=digest(cap)
    if h==OLD_CAPTURE_SHA:raise ValueError('Java query capture identical to historical V3; not a holdout')
    settings=reg.get('generation_settings',{})
    props=dict(line.split('=',1) for line in cfg.read_text(encoding='utf-8').splitlines() if line and '=' in line and not line.lstrip().startswith('#'))
    for key,value in settings.items():
        if props.get(key)!=str(value):
            raise ValueError(f'Generator configuration mismatch for {key}: expected {value}, observed {props.get(key)}')
    src=workspace/'schema_project_isolated'
    if not (src/'scripts/run_native_prebenchmark.sh').is_file():
        raise FileNotFoundError('Isolated original V3 pipeline missing: '+str(src))
    if not (src/'iot_pipeline/native_reference_v3.py').is_file():
        raise FileNotFoundError('Native reference Cassandra adapter missing from source copy')
    if reg.get('original_generator_script_sha256')!=digest(src/'scripts/auto_official_v3.py'):
        raise ValueError('Generator driver code has changed since holdout preregistration')
    if reg.get('copied_reference_adapter_sha256')!=digest(src/'iot_pipeline/native_reference_v3.py'):
        raise ValueError('Native reference adapter changed since holdout preregistration')
    validator=src/'phase_a1/tools/official_query_capture.py'
    if not validator.is_file():raise FileNotFoundError('Official Java capture validator missing')
    spec=importlib.util.spec_from_file_location('rev2_holdout_query_capture_validator',validator)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    _,counts=module.validate(cap,[f'Q{i}' for i in range(1,13)])
    if len(counts)!=12 or any(n<1 for n in counts.values()):
        raise ValueError('Incomplete independent Java query-object capture')
    return dict(capture=cap,capture_sha256=h,schema=schema,config=cfg,batches=batch,src=src,reg=reg)

def verify_stage8(exp:Path):
    stage=exp/'outputs/stage8';manifest=stage/'32_frozen_manifest.json';frozen=stage/'28_hbench_frozen.json'
    if not manifest.is_file() or not frozen.is_file():
        raise ValueError('Stage8 freeze missing; inspect recovery log, DO NOT benchmark')
    m=read_json(manifest)
    if m.get('status')!='FROZEN_PREBENCH_SELECTION' or m.get('benchmark_measured') is not False:
        raise ValueError('Stage8 manifest is not a valid unmeasured frozen prebenchmark')
    for rel, expected in m['inputs_sha256'].items():
        candidate=exp/rel
        if not candidate.is_file() or digest(candidate)!=expected:
            raise ValueError(f'Frozen Stage8 input mismatch: {rel}')
    if not read_json(frozen):raise ValueError('Frozen Stage8 HBench empty')
    if m.get('frozen_hbench_sha256')!=digest(frozen):
        raise ValueError('Stage8 frozen HBench hash mismatch')
    return digest(frozen)

def display(workspace:Path,run:Path):
    print('Workspace:',workspace,'\nJava run:',run)
    print('Existing generation status:',tail(run/'run_status.json',20))
    for rel in ('native_data/schema.txt','native_queries.jsonl','java_config/config.properties',
                'schemalens/outputs/stage8/32_frozen_manifest.json',
                f'{RUN_SUFFIX}/outputs/stage8/32_frozen_manifest.json'):
        p=run/rel;print(('OK  ' if p.is_file() else 'MISS'),p)
    for rel in ('prebenchmark.log','native_generation.log','capture.log'):
        print('\n---',rel,'(last lines) ---\n',tail(run/'logs'/rel,16))

def ignore_code(parent,names):
    path=Path(parent)
    skip={'__pycache__','.venv','.git','native_auto_work','.pytest_cache','node_modules','archive'}
    if path.name=='schema_project_isolated' or path.name=='phase_a1':skip.add('outputs')
    return {x for x in names if x in skip or x.endswith('.pyc') or x.endswith('.zip')}

def recover(workspace:Path,run:Path,reference:Path,dry_run:bool=False):
    source_info=capture_check(workspace,run,reference)
    exp=run/RUN_SUFFIX
    if exp.exists():
        raise FileExistsError('Recovery path already exists; preserving all evidence: '+str(exp))
    alias=run/ALIAS
    log=run/'rev2_recovery_prebenchmark.log'
    if log.exists():raise FileExistsError('Previous recovery log exists; inspect and preserve it: '+str(log))
    if alias.exists():
        if digest(alias)!=source_info['capture_sha256']:
            raise ValueError('Existing capture alias has different bytes')
        raise FileExistsError('Alias exists from another attempt; do not overwrite: '+str(alias))
    count=source_info['reg'].get('max_cells',100000)
    if not isinstance(count,int) or count<=0:raise ValueError('Invalid max_cells in preregistration')
    print('Java source capture SHA:',source_info['capture_sha256'])
    print('Native BigBatch files:',len(source_info['batches']))
    print('New separate experiment:',exp)
    if dry_run:
        print('DRY RUN: no output folders modified, no Cassandra used')
        return exp
    # The prebenchmark runner imports the native fixture and creates its own stage outputs.
    shutil.copytree(source_info['src'],exp,ignore=ignore_code)
    cmd=['bash',str(exp/'scripts/run_native_prebenchmark.sh'),
         str((run/'native_data').resolve()),str(source_info['capture'].resolve()),
         str(source_info['config'].resolve()),str(count)]
    print('Re-running A1 -> Stage8 without Cassandra; log:',log,flush=True)
    with log.open('x',encoding='utf-8') as stream:
        stream.write('COMMAND: '+repr(cmd)+'\n')
        stream.flush()
        with subprocess.Popen(cmd,cwd=exp,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                              encoding='utf-8',errors='replace',text=True) as proc:
            from collections import deque
            last=deque(maxlen=25)
            for line in proc.stdout:
                stream.write(line);last.append(line)
                if 'frozen' in line.lower() or 'failed' in line.lower() or 'error' in line.lower():
                    print(' ',line.rstrip(),flush=True)
            exitcode=proc.wait()
    if exitcode:
        print('FAILED generator pipeline; last lines:\n'+''.join(last),file=sys.stderr)
        raise RuntimeError(f'Isolated V3 pipeline returned code {exitcode}. Preserved log: {log}. Do not run Cassandra.')
    parent_hbench=verify_stage8(exp)
    # Rev2's native validator requires an unmodified alias of the Java capture.
    shutil.copyfile(source_info['capture'],alias)
    if digest(alias)!=source_info['capture_sha256']:
        raise AssertionError('Capture alias content changed')
    oldnorm=reference/'phase_a1/outputs/normalized_double_readings.csv'
    newnorm=exp/'phase_a1/outputs/normalized_double_readings.csv'
    oldq=reference/'phase_a1/outputs/query_instances_pilot.json'
    newq=exp/'phase_a1/outputs/query_instances_pilot.json'
    if digest(newnorm)==digest(oldnorm) or digest(newq)==digest(oldq):
        raise ValueError('Dataset or query-object samples identical to old V3! Holdout NOT acceptable.')
    meta={'status':'RECOVERED_FRESH_JAVA_V3_STAGE8_READY_FOR_REV2',
          'created_utc':datetime.now(timezone.utc).isoformat(),
          'new_experiment_path':str(exp),'java_run':str(run),
          'independent_java_data_capture_sha256':source_info['capture_sha256'],
          'native_capture_alias':str(alias),'native_capture_alias_sha256':digest(alias),
          'new_v3_stage8_hbench_sha256':parent_hbench,
          'historical_v3_stage8_hbench_sha256':ORIGINAL_HBENCH_SHA,
          'new_data_sha256':digest(newnorm),'old_data_sha256':digest(oldnorm),
          'new_queries_sha256':digest(newq),'old_queries_sha256':digest(oldq),
          'no_old_stage9_used':True,'Cassandra_used':False,
          'original_failed_experiment_preserved':True,'original_generator_log_preserved':True,
          'recovery_log':str(log)}
    outfile=run/'REV2_RECOVERY_MANIFEST.json'
    if outfile.exists():raise FileExistsError('Recovery evidence manifest already exists; preserved '+str(outfile))
    outfile.write_text(json.dumps(meta,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print('SUCCESS: valid new V3 Stage8:',exp)
    print('FRESH_EXPERIMENT='+str(exp))
    print('Original failed directory left untouched:',run/'schemalens')
    return exp

def main():
    ap=argparse.ArgumentParser(description='Recover independently generated IoT Benchmark Java run without deleting evidence')
    ap.add_argument('--workspace',type=Path,required=True)
    ap.add_argument('--run-dir',type=Path)
    ap.add_argument('--reference-v3',type=Path,required=True)
    ap.add_argument('--mode',choices=['inspect','recover'],default='inspect')
    ap.add_argument('--dry-run',action='store_true')
    args=ap.parse_args();workspace=args.workspace.resolve();reference=args.reference_v3.resolve()
    run=choose_run(workspace,args.run_dir)
    check_reference(reference)
    if args.mode=='inspect':display(workspace,run)
    else:recover(workspace,run,reference,args.dry_run)
if __name__=='__main__':
    try:main()
    except Exception as e:
        print(type(e).__name__+': '+str(e),file=sys.stderr)
        sys.exit(2)
