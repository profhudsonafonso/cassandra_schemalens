#!/usr/bin/env python3
"""Install new, standalone baseline modules into existing frozen V3 experiment.

Does NOT rerun stages1-8 and will reject any mismatch to manifest file hashes.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ADDON=Path(__file__).resolve().parents[1]
NEW_FILES={
    'iot_pipeline/native_reference_v3.py',
    'iot_pipeline/compare_native_v3.py',
    'tests/test_native_reference_v3.py',
}


def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()


def verify(exp):
    frozen=exp/'outputs/stage8/32_frozen_manifest.json'
    if not frozen.is_file():
        raise ValueError(f'Expected frozen Stage8 manifest not found at: {frozen}')
    m=json.loads(frozen.read_text(encoding='utf-8'))
    if m.get('status')!='FROZEN_PREBENCH_SELECTION' or m.get('benchmark_measured') is not False:
        raise ValueError('Experiment has no prebenchmark frozen selection')
    h=exp/'outputs/stage8/28_hbench_frozen.json'
    if not h.is_file() or digest(h)!=m.get('frozen_hbench_sha256'):
        raise ValueError('HBench file does not match frozen manifest')
    for relative, expected in m['inputs_sha256'].items():
        source=exp/relative
        if not source.is_file() or digest(source)!=expected:
            raise ValueError(f'Frozen input missing/changed: {relative}')
    validator=exp/'outputs/validation_native_stage8/VALIDATION_RESULT.json'
    if validator.is_file():
        v=json.loads(validator.read_text(encoding='utf-8'))
        if v.get('status')!='PASS' or v.get('hbench_sha256')!=digest(h):
            raise ValueError('Existing Stage8 validation report does not match frozen HBench')
    return m, digest(frozen)


def install(exp,run_checks=True,expected_hbench=None):
    exp=exp.expanduser().resolve(strict=True)
    original, original_manifest_hash = verify(exp)
    if expected_hbench and original['frozen_hbench_sha256'] != expected_hbench:
        raise ValueError('Unexpected frozen HBench hash for target experiment')
    for rel in sorted(NEW_FILES):
        src=ADDON/rel;dst=exp/rel
        if not src.is_file():raise ValueError('Addon file missing: '+str(src))
        if dst.exists() and digest(dst)!=digest(src):
            raise ValueError('Refusing to overwrite modified addon code at '+str(dst))
        dst.parent.mkdir(parents=True,exist_ok=True)
        if not dst.exists():shutil.copyfile(src,dst)
    same, second_manifest_hash=verify(exp)
    if original_manifest_hash!=second_manifest_hash:
        raise AssertionError('Installer modified frozen Stage8 manifest!')
    if not run_checks:
        return exp
    env=os.environ.copy()
    env['PYTHONPATH']=str(exp)+(os.pathsep+env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
    native_capture = exp.parent / ('native_queries_' + exp.name.removeprefix('schemalens_') + '.jsonl')
    print('Java capture:',native_capture if native_capture.is_file() else '(not located; parameter-object SHA only)', flush=True)
    reference_cmd=[sys.executable,'-m','iot_pipeline.native_reference_v3','--dry-run',
                   '--output',str(exp/'outputs/native_reference_v3_precheck')]
    if native_capture.is_file():
        reference_cmd.extend(['--capture',str(native_capture)])
    commands=[
        [sys.executable,'-m','unittest','discover','-s','tests',
         '-p','test_native_reference_v3.py','-v'],
        reference_cmd,
    ]
    for cmd in commands:
        print('\n$ '+' '.join(cmd),flush=True)
        subprocess.run(cmd,cwd=exp,env=env,check=True)
    _, after=verify(exp)
    if after!=original_manifest_hash:
        raise AssertionError('Frozen manifest unexpectedly changed after checks')
    print('\nSUCCESS: 12-query baseline equivalence PASS; frozen HBench unchanged.')
    print('V3 experiment:',exp)
    print('Next step (optional, LIVE): python3 -m iot_pipeline.native_reference_v3 --live ...')
    return exp


def autodiscover(expected_hash):
    """Locate exactly one prior Q12-fixed run with an expected frozen hash."""
    project=ADDON.parent
    root=project/'native_auto_work'/'runs'
    if not root.is_dir():
        raise ValueError(f'Could not find native experiment tree: {root}')
    matches=[]
    for frozen in root.glob('*/schemalens_q12fixed_*/outputs/stage8/32_frozen_manifest.json'):
        exp=frozen.parents[2]
        try:
            mani,_=verify(exp)
            if expected_hash is None or mani['frozen_hbench_sha256']==expected_hash:
                matches.append(exp)
        except (ValueError,FileNotFoundError):
            continue
    if len(matches)!=1:
        raise ValueError('Expected exactly one valid matching frozen experiment; '
                         f'found {len(matches)}. Specify --experiment PATH explicitly.')
    print('Discovered frozen native V3 experiment:',matches[0],flush=True)
    return matches[0]


def main():
    ap=argparse.ArgumentParser(description='Install native IoTDB-table Cassandra baseline AFTER Stage8 freeze')
    location=ap.add_mutually_exclusive_group(required=True)
    location.add_argument('--experiment',type=Path)
    location.add_argument('--auto',action='store_true',help='Find unique validated Q12-fixed experiment below native_auto_work/runs')
    ap.add_argument('--no-checks',action='store_true')
    ap.add_argument('--expected-hbench',help='Pin installation to verified HBench SHA-256')
    args=ap.parse_args()
    location = autodiscover(args.expected_hbench) if args.auto else args.experiment
    install(location,not args.no_checks,args.expected_hbench)

if __name__=='__main__':
    main()
