#!/usr/bin/env python3
"""Resume V3 after Stage8 freeze when ONLY fixture-hardcoded tests failed.

This helper does not generate data, run Stages 1–8 or connect to Cassandra.
It updates solely tests/test_stage4_rev2.py in the selected experiment and
verifies byte-for-byte preservation of Stage4-8 outputs and the HBench SHA.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from datetime import datetime, timezone

PROJECT = Path(__file__).resolve().parents[1]
FIXED_TEST = PROJECT / 'tests/test_stage4_rev2.py'
SUITES = (
    'test_iot_stages_4_to_9.py',
    'test_stage4_rev2.py',
    'test_official_bridge_v3.py',
)

def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(experiment: Path) -> dict[str, str]:
    return {str(f.relative_to(experiment)):digest(f)
            for stage in (4,5,6,7,8)
            for f in sorted((experiment/'outputs'/f'stage{stage}').rglob('*'))
            if f.is_file()}


def run(experiment: Path) -> int:
    experiment=experiment.expanduser().resolve()
    if not (experiment/'iot_pipeline/stage9.py').is_file():
        raise RuntimeError('Not a SchemaLens isolated experiment: '+str(experiment))
    frozen=experiment/'outputs/stage8/32_frozen_manifest.json'
    selected=experiment/'outputs/stage8/28_hbench_frozen.json'
    if not frozen.is_file() or not selected.is_file():
        raise RuntimeError('Stage8 not frozen in '+str(experiment))
    if not FIXED_TEST.is_file():
        raise RuntimeError('Missing repaired tests in '+str(FIXED_TEST))
    details=json.loads(frozen.read_text(encoding='utf-8'))
    if details.get('status')!='FROZEN_PREBENCH_SELECTION' or details.get('benchmark_measured') is not False:
        raise RuntimeError('Stage8 manifest is not a valid prebenchmark freeze')
    if details.get('frozen_hbench_sha256') != digest(selected):
        raise RuntimeError('HBench SHA-256 mismatch before patch; stop')

    before=snapshot(experiment)
    target=experiment/'tests/test_stage4_rev2.py'
    if not target.is_file():
        raise RuntimeError('Missing Stage4 Rev2 tests in the isolated experiment')
    if target.read_bytes()!=FIXED_TEST.read_bytes():
        backup=experiment/'tests/test_stage4_rev2.py.before_native_fix'
        if backup.exists():
            raise RuntimeError('Backup already exists; refusing overwrite: '+str(backup))
        shutil.copy2(target,backup)
        shutil.copy2(FIXED_TEST,target)
        print('Patched only tests/test_stage4_rev2.py')
        print('Original test saved as:',backup)
    else:
        print('Native-aware test already installed; no code changes needed')

    logs=experiment/'outputs/validation_native_stage8'
    logs.mkdir(parents=True,exist_ok=True)
    commands=[
        (['-m','iot_pipeline.stage9','--dry-run'],'stage9_dryrun.log'),
        *[(['-m','unittest','discover','-s','tests','-p',suite,'-v'],suite.replace('.py','.log')) for suite in SUITES]
    ]
    result={}
    env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'}
    failures=[]
    for arguments,name in commands:
        cmd=[sys.executable,*arguments]
        print('Running:', ' '.join(cmd[1:]),flush=True)
        completed=subprocess.run(cmd,cwd=experiment,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=env)
        (logs/name).write_text(completed.stdout,encoding='utf-8')
        print('\n'.join(completed.stdout.splitlines()[-9:]))
        result[name]={'returncode':completed.returncode,'log':str(logs/name)}
        if completed.returncode:
            failures.append(name)

    after=snapshot(experiment)
    if before!=after:
        files=sorted({*before,*after})
        changed=[x for x in files if before.get(x)!=after.get(x)]
        raise RuntimeError('Stage4-8 outputs changed during validation! STOP: '+repr(changed[:20]))
    if digest(selected)!=details['frozen_hbench_sha256']:
        raise RuntimeError('Frozen HBench hash changed during validation! STOP')

    report={
        'scope':'POST_FREEZE_VALIDATION_ONLY',
        'experiment':str(experiment),
        'status':'PASS' if not failures else 'FAIL',
        'no_stage1_8_rerun':True,
        'no_stage9_live_run':True,
        'Cassandra_used':False,
        'hbench_sha256_unchanged':True,
        'hbench_sha256':details['frozen_hbench_sha256'],
        'stage4_8_outputs_unchanged':True,
        'input_metric_cells':sum(1 for _ in (experiment/'phase_a1/outputs/normalized_double_readings.csv').open(encoding='utf-8'))-1,
        'checks':result,
        'failed_checks':failures,
        'note':'Only fixture-specific tests were generalized to native sensor names and row counts. This is not an official SQL replay or a performance measurement.'
    }
    reportpath=logs/'VALIDATION_RESULT.json'
    reportpath.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('\nFrozen HBench SHA-256:',details['frozen_hbench_sha256'])
    print('Stage4-8 output files preserved:',len(before))
    print('Report:',reportpath)
    print('HBench:',experiment/'outputs/stage8/30_hbench_summary.csv')
    print('Frozen manifest:',frozen)
    if failures:
        print('FAILURE in:', ', '.join(failures),file=sys.stderr)
        return 1
    print('SUCCESS: Native Stage8 validation PASS; NO benchmark executed.')
    return 0


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--experiment',type=Path,required=True,help='Existing isolated schemalens_q12fixed_* folder')
    args=ap.parse_args()
    try:
        return run(args.experiment)
    except Exception as exc:
        print('ERROR:',exc,file=sys.stderr)
        return 2

if __name__=='__main__':
    sys.exit(main())
