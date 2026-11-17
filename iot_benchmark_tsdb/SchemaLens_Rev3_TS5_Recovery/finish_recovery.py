#!/usr/bin/env python3
"""Complete preregistered Rev3 campaign A/B after GROUP_BY recovery's stale TS5 unit test.

Only edits a copied test fixture; all Java files, all Stage 1-8 outputs and
legacy failed/recovered experiments remain immutable. Never connects to Cassandra.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import sys
from fixture_patch import patched_text, OLD_TEST, NEW_TEST

TEST_REL = Path('tests/test_iot_stages_4_to_9.py')
SUITES = ('test_iot_stages_4_to_9.py','test_stage4_rev2.py','test_official_bridge_v3.py')


def load_previous(patch_dir: Path):
    patch_dir=patch_dir.resolve()
    if not (patch_dir/'recover_rev3.py').is_file() or not (patch_dir/'recover_groupby.py').is_file():
        raise FileNotFoundError('Install previous SchemaLens_Rev3_GroupBy_Recovery and provide --groupby-patch')
    sys.path.insert(0,str(patch_dir))
    import recover_rev3 as U
    import recover_groupby as G
    return U,G


def ignore_caches(_folder,names):
    return {n for n in names if n in {'.git','__pycache__','.pytest_cache','.venv'} or n.endswith('.pyc')}


def run_tests(exp:Path, log:Path):
    with log.open('x',encoding='utf8') as stream:
        for suite in SUITES:
            command=[sys.executable,'-m','unittest','discover','-s','tests','-p',suite,'-v']
            p=subprocess.run(command,cwd=exp,capture_output=True,text=True,errors='replace',timeout=180)
            stream.write('COMMAND: '+repr(command)+'\n'+p.stdout+'\n'+p.stderr+'\nEXIT: '+str(p.returncode)+'\n')
            stream.flush()
            if p.returncode:
                tail='\n'.join((p.stdout+'\n'+p.stderr).splitlines()[-34:])
                raise RuntimeError(f'Failed regression suite {suite}; see {log}\n'+tail)


def recover(args):
    U,G=load_previous(args.groupby_patch)
    workspace=args.workspace.resolve();run=args.run_dir.resolve()
    if run.parent!=(workspace/'java_work/runs').resolve() or not run.name.startswith('v3_'):
        raise ValueError('Run folder must be a v3_* direct child of workspace/java_work/runs')
    old3=args.reference_v3.resolve();old2=args.reference_rev2.resolve()
    G.check_reference(old3)
    prereg=G.read_json(workspace/'HOLDOUT_GENERATION_PREREGISTRATION.json')
    U.assert_campaign_preregistration(prereg,args.campaign)
    source_info=G.capture_check(workspace,run,old3)
    src=run/f'schemalens_rev3_groupby_aligned_{args.campaign}'
    dst=run/f'schemalens_rev3_ts5_contract_validated_{args.campaign}'
    logfile=run/f'rev3_ts5_contract_{args.campaign}_tests.log'
    evidence=run/f'REV3_TS5_CONTRACT_{args.campaign}_MANIFEST.json'
    alias=run/f'native_queries_q12fixed_rev3_{args.campaign}_recovery.jsonl'
    originals=(workspace/'HOLDOUT_GENERATION_PREREGISTRATION.json',
               run/'native_queries.jsonl',run/'java_config/config.properties',
               source_info['src']/'phase_a1/inputs/scenario.json')
    if not src.is_dir():raise FileNotFoundError('Previous aligned recovery missing: '+str(src))
    aligned=G.read_json(src/'phase_a1/inputs/scenario.json')
    configured=int(G.read_props_strict(source_info['config'])['GROUP_BY_TIME_UNIT'])
    if aligned['group_by_time_unit_ms']!=configured or configured!=U.PRESETS[args.campaign]['GROUP_BY_TIME_UNIT']:
        raise ValueError('Aligned scenario must match preregistered Java GROUP_BY_TIME_UNIT')
    frozen_before=G.verify_stage8(src)
    stage_manifest=src/'outputs/stage8/32_frozen_manifest.json'
    manifest_hash=G.digest(stage_manifest)
    oldtest=src/TEST_REL
    old_text=oldtest.read_text(encoding='utf8')
    new_text=patched_text(old_text)
    if not (src/'phase_a1/outputs/normalized_double_readings.csv').is_file():
        raise FileNotFoundError('A1 source rows missing')
    if not (src/'phase_a1/outputs/query_instances_pilot.json').is_file():
        raise FileNotFoundError('A1 source queries missing')
    # Source independence and selected code provenance; the previous attempt
    # didn't create the final aliases/registries because its test failed.
    for p in (dst,logfile,evidence,alias,workspace/'HOLDOUT_GENERATION_OUTPUT.json',
              workspace/'REV3_NEW_CAMPAIGN_REGISTRY.json',workspace/'REV3_CONFIRMATORY_PREREGISTRATION.json'):
        if p.exists():raise FileExistsError('Refusing to overwrite earlier evidence: '+str(p))
    if list(run.glob('native_queries_q12fixed_*.jsonl')):
        raise FileExistsError('Existing native capture alias would create ambiguous provenance')
    protected={str(p):G.digest(p) for p in originals}
    datahash=G.digest(src/'phase_a1/outputs/normalized_double_readings.csv')
    qhash=G.digest(src/'phase_a1/outputs/query_instances_pilot.json')
    print('Java data/capture preregistration: PASS')
    print('Aligned Java group-by:',configured,'ms')
    print('Existing frozen parent V3 SHA256:',frozen_before)
    print('Old test SHA256:',G.digest(oldtest))
    print('Source (preserved):',src)
    print('New test-only recovery:',dst)
    if args.dry_run:
        print('DRY RUN: no changes; no Java generation; no Cassandra')
        return
    shutil.copytree(src,dst,ignore=ignore_caches)
    (dst/TEST_REL).write_text(new_text,encoding='utf8')
    try:
        assert G.digest(src/TEST_REL)!=G.digest(dst/TEST_REL)
        assert G.digest(dst/'outputs/stage8/32_frozen_manifest.json')==manifest_hash
        assert G.verify_stage8(dst)==frozen_before
        assert G.digest(dst/'phase_a1/outputs/normalized_double_readings.csv')==datahash
        assert G.digest(dst/'phase_a1/outputs/query_instances_pilot.json')==qhash
        print('Running 3 original offline suites; no Cassandra...',flush=True)
        run_tests(dst,logfile)
        if G.verify_stage8(dst)!=frozen_before or G.digest(dst/'outputs/stage8/32_frozen_manifest.json')!=manifest_hash:
            raise ValueError('Frozen Stage8 changed during test verification')
        if G.digest(dst/'phase_a1/outputs/normalized_double_readings.csv')!=datahash or G.digest(dst/'phase_a1/outputs/query_instances_pilot.json')!=qhash:
            raise ValueError('Normalized Java data/query instances changed')
        for p in originals:
            if G.digest(p)!=protected[str(p)]:
                raise ValueError('Protected original changed: '+str(p))
        audit=U.audit_new(dst,old3,old2,args.addon,args.rev2_addon,args.rev3_addon)
        shutil.copyfile(source_info['capture'],alias)
        if G.digest(alias)!=source_info['capture_sha256']:
            raise ValueError('Native Java capture alias changed')
        from v4_rev2 import prebenchmark as R2
        found=R2.find_capture(dst,G.read_json(dst/'phase_a1/outputs/query_instances_pilot.json'))
        if Path(found['file']).resolve()!=alias.resolve() or found['sha256']!=G.digest(alias):
            raise ValueError('Native capture provenance was not accepted by Rev2 verifier')
        output=U.output_record(dst,run,found['sha256'],alias)
        registry=U.registry_record(dst,workspace,args.source_project.resolve(),old3,old2,prereg,audit)
        info={
            'status':'REV3_TS5_TEST_CONTRACT_REVALIDATED_NO_CASSANDRA',
            'campaign':args.campaign,'created_utc':datetime.now(timezone.utc).isoformat(),
            'prior_failed_groupby_recovery':str(src),'new_experiment':str(dst),
            'java_group_by_ms':configured,'ts5_rollup_window_ms':G.read_json(dst/'config/pipeline_policy.json')['rollup_window_ms'],
            'test_only_change':str(TEST_REL),'test_before_sha256':G.digest(oldtest),
            'test_after_sha256':G.digest(dst/TEST_REL),
            'frozen_parent_hbench_sha256':frozen_before,'frozen_parent_manifest_sha256':manifest_hash,
            'source_java_capture_sha256':source_info['capture_sha256'],
            'normalized_data_sha256':datahash,'captured_query_instances_sha256':qhash,
            'preflight_audit_new_generation':audit,'offline_suites_passed':list(SUITES),
            'regression_log':str(logfile),'no_java_regeneration':True,
            'no_stage5_7_8_modification':True,'no_cassandra_calls':True,
            'no_latency_data_seen':True,
            'reason':'TS5 rollup 20 seconds cannot answer native Q7 GROUP BY 1 hour; stale fixture expected CONDITIONAL. Test corrected to assert native INELIGIBLE and synthetic aligned nonempty CONDITIONAL.'
        }
        # Important: workspace registry/output are created only after every gate passes.
        for p,obj in [(workspace/'HOLDOUT_GENERATION_OUTPUT.json',output),
                      (workspace/'REV3_NEW_CAMPAIGN_REGISTRY.json',registry),
                      (evidence,info)]:
            with p.open('x',encoding='utf8') as f:json.dump(obj,f,indent=2,sort_keys=True,ensure_ascii=False);f.write('\n')
    except Exception:
        print('FAILED: frozen source, copied destination and test log preserved. Do not benchmark.',file=sys.stderr)
        raise
    print('SUCCESS: Java data reused; original V3 Stage8 SHA256 preserved; TS5 tests valid.')
    print('FRESH_EXPERIMENT='+str(dst))
    print('No Cassandra used. Next: run_campaign.sh '+args.campaign+' freeze; preflight.')


def main():
    ap=argparse.ArgumentParser()
    for flag in ('workspace','run-dir','reference-v3','reference-rev2','addon','rev2-addon','rev3-addon','source-project','groupby-patch'):
        ap.add_argument('--'+flag,required=True,type=Path)
    ap.add_argument('--campaign',choices=['A','B'],required=True)
    ap.add_argument('--dry-run',action='store_true')
    recover(ap.parse_args())
if __name__=='__main__':main()
