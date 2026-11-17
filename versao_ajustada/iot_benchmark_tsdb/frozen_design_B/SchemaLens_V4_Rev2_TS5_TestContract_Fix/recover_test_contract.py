#!/usr/bin/env python3
"""Preserve a Java holdout and V3 Stage8 freeze while repairing a stale TS5 fixture test.

A regression test assumes native Q7 GROUP BY interval equals the policy's rollup
window, which is no longer true after independently configured Java capture.
No production pipeline or frozen Stage8 artifact is edited. No Cassandra used.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

SUFFIX = 'schemalens_rev2_ts5_contract_validated'
MANIFEST = 'REV2_TS5_TEST_CONTRACT_RECOVERY_MANIFEST.json'
TEST_REL = Path('tests/test_iot_stages_4_to_9.py')
SOURCE = 'schemalens_rev2_groupby_aligned'
OLD_TEST = '''    def test_06_aligned_rollup_allowed_with_right_granularity(self):
        p=require_policy();q=copy.deepcopy(next(q for q in self.queries if q['query_id']=='Q7'))
        q['end_ms_exclusive']=q['start_ms']+2*p['rollup_window_ms']
        self.assertEqual(stage5.route_family(q,'TS5',p)[0],'CONDITIONAL')
        res=from_rollup(self.state,q)
        expected=_result(self.rows,q)
        self.assertTrue(compare(res,expected))
'''
NEW_TEST = '''    def test_06_aligned_rollup_allowed_with_right_granularity(self):
        # Construct a NON-VACUOUS positive case at the policy's TS5 granularity.
        # Native Java GROUP BY can use a different interval; in that case the
        # original query must not be silently classified as a valid TS5 route.
        p=require_policy()
        native_q=copy.deepcopy(next(q for q in self.queries if q['query_id']=='Q7'))
        if native_q['group_by_ms'] != p['rollup_window_ms']:
            self.assertEqual(stage5.route_family(native_q,'TS5',p)[0], 'INELIGIBLE')
        q=copy.deepcopy(native_q)
        g=p['rollup_window_ms']
        self.assertGreater(g, 0)
        q['group_by_ms']=g
        eligible=[r for r in self.rows
                  if r['device_id'] in q['devices'] and r['sensor_name'] in q['sensors']]
        if not eligible:
            # The captured Java Q7 may target an empty device/sensor selection.
            # This is a SYNTHETIC POSITIVE fixture, not a claim about native Q7.
            self.assertTrue(self.rows, 'No source points to build TS5 positive fixture')
            anchor=self.rows[0]
            q['devices']=[anchor['device_id']]
            q['sensors']=[anchor['sensor_name']]
            eligible=[anchor]
        self.assertTrue(eligible, 'TS5 positive fixture must include source points')
        timestamp=min(r['timestamp'] for r in eligible)
        q['start_ms']=(timestamp//g)*g
        q['end_ms_exclusive']=q['start_ms']+2*g
        self.assertEqual(stage5.route_family(q,'TS5',p)[0], 'CONDITIONAL')
        res=from_rollup(self.state,q)
        expected=_result(self.rows,q)
        self.assertTrue(res, 'TS5 positive fixture must produce nonempty results')
        self.assertTrue(compare(res, expected))
'''


def sha(path: Path) -> str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()


def read(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def replacement(original: str) -> str:
    # Exactly one expected V3 fixture contract; never mutate functional Stage5.
    if original.count(OLD_TEST)!=1:
        raise ValueError('Unexpected original test source; refusing a blind or repeated patch')
    result=original.replace(OLD_TEST, NEW_TEST, 1)
    if result.replace(NEW_TEST,OLD_TEST,1)!=original:
        raise AssertionError('Changed more than the intended test')
    return result


def dependency_module(patch_path: Path):
    script=patch_path/'recover_groupby.py'
    if not script.is_file():
        raise FileNotFoundError('Requires the previously installed SchemaLens_V4_Rev2_GroupBy_Fix next to this patch')
    spec=importlib.util.spec_from_file_location('schemalens_rev2_groupby_previous',str(script))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, sha(script)


def roots(workspace: Path,run: Path):
    if run.resolve().parent!=(workspace/'java_work/runs').resolve() or not run.name.startswith('v3_'):
        raise ValueError('The run must be a registered v3_* child of workspace/java_work/runs')


def ignore(parent, names):
    return {name for name in names if name in {'__pycache__','.pytest_cache','.git','.venv'} or name.endswith('.pyc')}


def run_suite(exp:Path, pattern:str, log_stream):
    args=[sys.executable,'-m','unittest','discover','-s','tests','-p',pattern,'-v']
    proc=subprocess.run(args,cwd=exp,capture_output=True,text=True,errors='replace',timeout=180)
    log_stream.write(f'\nCOMMAND: {args!r}\n{proc.stdout}\n{proc.stderr}\nEXIT: {proc.returncode}\n')
    log_stream.flush()
    if proc.returncode:
        lines=(proc.stdout+'\n'+proc.stderr).splitlines()
        raise RuntimeError(f'Regression suite failed: {pattern}; last lines:\n'+'\n'.join(lines[-30:]))
    return pattern


def recover(workspace:Path, run:Path, ref:Path, patch_dir:Path, dry_run:bool):
    roots(workspace,run)
    former, dependency_sha=dependency_module(patch_dir)
    before=former.capture_check(workspace,run,ref)
    src=run/SOURCE
    if not src.is_dir():raise FileNotFoundError(f'Missing original aligned source {src}')
    frozen_sha=former.verify_stage8(src)
    raw=src/TEST_REL
    if not raw.is_file():raise FileNotFoundError('Missing native regression test: '+str(raw))
    original=raw.read_text(encoding='utf-8')
    updated=replacement(original)
    java_props=former.read_props_strict(before['config'])
    native_group=int(java_props['GROUP_BY_TIME_UNIT'])
    scenario=read(src/'phase_a1/inputs/scenario.json')
    if scenario['group_by_time_unit_ms']!=native_group:
        raise ValueError('Scenario does not match independently generated Java GROUP_BY_TIME_UNIT')
    # Protect actual frozen Stage8, source data, and native query DTOs.
    new_data=src/'phase_a1/outputs/normalized_double_readings.csv'
    new_queries=src/'phase_a1/outputs/query_instances_pilot.json'
    if not new_data.is_file() or not new_queries.is_file():raise FileNotFoundError('A1 source dataset/query artifacts incomplete')
    old_data=ref/'phase_a1/outputs/normalized_double_readings.csv'
    old_queries=ref/'phase_a1/outputs/query_instances_pilot.json'
    if sha(new_data)==sha(old_data) or sha(new_queries)==sha(old_queries):
        raise ValueError('Generated data or native DTOs identical to historical V3')
    stage_manifest=src/'outputs/stage8/32_frozen_manifest.json'
    parent_sha=sha(stage_manifest)
    dst=run/SUFFIX
    log=run/'rev2_ts5_contract_recovery_tests.log'
    evidence=run/MANIFEST
    alias=run/former.ALIAS
    for exists in (dst,log,evidence):
        if exists.exists():raise FileExistsError(f'Preserving earlier attempt, refusing overwrite: {exists}')
    if alias.exists() and sha(alias)!=before['capture_sha256']:
        raise ValueError('Java capture alias contains unexpected bytes; refusing overwrite')
    print('Captured Java objects SHA256:',before['capture_sha256'])
    print('Java GROUP_BY_TIME_UNIT:',native_group,'ms')
    print('TS5 rollup window:',read(src/'config/pipeline_policy.json')['rollup_window_ms'],'ms')
    print('Existing frozen V3 Stage8 SHA256:',frozen_sha)
    print('Existing test source SHA256:',sha(raw))
    print('Target separate experiment:',dst)
    if dry_run:
        print('DRY RUN: no files changed, no Cassandra used')
        return dst
    # NEVER alter failed parent, former recoveries, V3 or V4 Rev1 freezes.
    shutil.copytree(src,dst,ignore=ignore)
    new_test=dst/TEST_REL
    new_test.write_text(updated,encoding='utf-8')
    try:
        if sha(dst/'outputs/stage8/32_frozen_manifest.json')!=parent_sha or former.verify_stage8(dst)!=frozen_sha:
            raise AssertionError('V3 parent selection changed during copy')
        print('Running native offline regression suites (no Cassandra)...',flush=True)
        suites=[]
        with log.open('x',encoding='utf-8') as out:
            for pattern in ('test_iot_stages_4_to_9.py','test_stage4_rev2.py','test_official_bridge_v3.py'):
                suites.append(run_suite(dst,pattern,out))
        if former.verify_stage8(dst)!=frozen_sha or sha(dst/'outputs/stage8/32_frozen_manifest.json')!=parent_sha:
            raise AssertionError('V3 Stage8 changed after running tests')
        if sha(dst/'phase_a1/outputs/normalized_double_readings.csv')!=sha(new_data):
            raise AssertionError('Logical source data changed')
        if sha(dst/'phase_a1/outputs/query_instances_pilot.json')!=sha(new_queries):
            raise AssertionError('Query DTOs changed')
        if not alias.exists():
            shutil.copyfile(before['capture'],alias)
        if sha(alias)!=before['capture_sha256']:
            raise ValueError('Unexpected capture alias bytes')
        report={
            'status':'V3_PARENT_FROZEN_OFFLINE_TEST_CONTRACT_REVALIDATED',
            'created_utc':datetime.now(timezone.utc).isoformat(),
            'source_experiment':str(src),'recovered_experiment':str(dst),
            'original_historical_v3_hbench_sha256':former.ORIGINAL_HBENCH_SHA,
            'new_parent_v3_hbench_sha256':frozen_sha,
            'new_parent_stage8_manifest_sha256':parent_sha,
            'native_capture_sha256':before['capture_sha256'],
            'new_normalized_data_sha256':sha(new_data),
            'new_query_dtos_sha256':sha(new_queries),
            'group_by_time_unit_java_ms':native_group,
            'ts5_policy_rollup_window_ms':read(src/'config/pipeline_policy.json')['rollup_window_ms'],
            'patched_only_test':str(TEST_REL),
            'test_before_sha256':sha(raw),'test_after_sha256':sha(new_test),
            'test_fix_reason':'Original TS5 positive fixture assumed Java Q7 grouping matched TS5 rollup window; independent Java GROUP_BY_TIME_UNIT differs. Explicitly set a policy-aligned, nonempty synthetic positive fixture and retain native Q7 as ineligible.',
            'offline_suites_passed':suites,'offline_regression_log':str(log),
            'old_frozen_outputs_unchanged':True,'Cassandra_used':False,
            'no_latency_used':True,'functional_stage5_code_changed':False,
            'source_dependency_sha256':dependency_sha,
        }
        with evidence.open('x',encoding='utf-8') as f:
            f.write(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    except Exception:
        print('VALIDATION FAILED. New copy/log retained as evidence, do NOT benchmark.',file=sys.stderr)
        raise
    print('SUCCESS: regenerated fixture contract tests passed; Stage8 SHA preserved')
    print('FRESH_EXPERIMENT='+str(dst))
    print('RECOVERY_MANIFEST='+str(evidence))
    print('No Cassandra used.')
    return dst


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--workspace',type=Path,required=True)
    p.add_argument('--run-dir',type=Path,required=True)
    p.add_argument('--reference-v3',type=Path,required=True)
    p.add_argument('--groupby-patch',type=Path,default=Path(__file__).resolve().parent.parent/'SchemaLens_V4_Rev2_GroupBy_Fix')
    p.add_argument('--dry-run',action='store_true')
    args=p.parse_args()
    recover(args.workspace.resolve(),args.run_dir.resolve(),args.reference_v3.resolve(),args.groupby_patch.resolve(),args.dry_run)


if __name__=='__main__':
    try:main()
    except Exception as exc:
        print(f'{type(exc).__name__}: {exc}',file=sys.stderr)
        sys.exit(2)
