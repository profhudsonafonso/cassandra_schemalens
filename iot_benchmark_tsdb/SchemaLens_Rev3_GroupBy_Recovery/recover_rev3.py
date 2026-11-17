#!/usr/bin/env python3
"""Safely recover a PREMEASURED Rev3 Java campaign blocked by A1 GROUP_BY mismatch.

Never regenerates Java, overwrites a failed attempt, modifies preregistration,
changes the frozen Rev2/Rev3 selection code, or accesses Cassandra.
"""
from __future__ import annotations
import argparse
from collections import deque
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import recover_groupby as G

PRESETS = {
 'A': dict(devices=6,loops=300,batch_size=5,max_cells=100000,DATA_SEED=531007,
           QUERY_SEED=531013,POINT_STEP=600000,QUERY_INTERVAL=86400000,GROUP_BY_TIME_UNIT=3600000),
 'B': dict(devices=9,loops=600,batch_size=5,max_cells=100000,DATA_SEED=531101,
           QUERY_SEED=531107,POINT_STEP=600000,QUERY_INTERVAL=604800000,GROUP_BY_TIME_UNIT=3600000),
}

def assert_campaign_preregistration(prereg: dict,campaign: str):
    expected = PRESETS[campaign]
    for key in ('devices','loops','batch_size','max_cells'):
        if prereg.get(key)!=expected[key]:
            raise ValueError(f'Preregistered {key} mismatch for campaign {campaign}: expected {expected[key]}, got {prereg.get(key)!r}')
    settings=prereg.get('generation_settings',{})
    for key in ('DATA_SEED','QUERY_SEED','POINT_STEP','QUERY_INTERVAL','GROUP_BY_TIME_UNIT'):
        if str(settings.get(key))!=str(expected[key]):
            raise ValueError(f'Preregistered {key} mismatch for campaign {campaign}: expected {expected[key]}, got {settings.get(key)!r}')
    if prereg.get('sql_replay') is not False or prereg.get('operation_frequency_replay') is not False:
        raise ValueError('Expected a one-sample-per-class Java generation, not an SQL/frequency replay')
    return expected

def ensure_fresh(workspace:Path, run:Path, campaign:str):
    if (workspace/'HOLDOUT_GENERATION_OUTPUT.json').exists():
        raise FileExistsError('Standard generation result already exists; do not overwrite')
    if (workspace/'REV3_NEW_CAMPAIGN_REGISTRY.json').exists():
        raise FileExistsError('Rev3 new campaign registry already exists; do not overwrite')
    if (workspace/'REV3_CONFIRMATORY_PREREGISTRATION.json').exists():
        raise FileExistsError('Frozen confirmation already exists; do not recover')
    exp=run/f'schemalens_rev3_groupby_aligned_{campaign}'
    log=run/f'rev3_groupby_recovery_{campaign}_prebenchmark.log'
    meta=run/f'REV3_GROUPBY_RECOVERY_{campaign}_MANIFEST.json'
    alias=run/f'native_queries_q12fixed_rev3_{campaign}_recovery.jsonl'
    for p in (exp,log,meta,alias):
        if p.exists():raise FileExistsError('Refusing to overwrite previous recovery evidence: '+str(p))
    if list(run.glob('native_queries_q12fixed_*.jsonl')):
        raise FileExistsError('Unexpected existing q12fixed alias would make capture provenance ambiguous')
    status=G.read_json(run/'run_status.json')
    if status.get('generate')!='OK' or status.get('capture')!='OK' or status.get('isolate')!='OK':
        raise ValueError('Java generation/capture/isolation not successfully completed')
    if status.get('prebenchmark')!='FAILED':
        raise ValueError('Only recover runs marked prebenchmark FAILED')
    return exp,log,meta,alias

def output_record(exp:Path,run:Path,cap_hash:str,alias:Path):
    return {'source_java_capture_sha256':cap_hash,
        'alias_has_identical_bytes':True,
        'native_data_dir':str((run/'native_data').resolve()),
        'isolated_v3_experiment':str(exp.resolve()),
        'native_queries_alias':str(alias.resolve()),
        'status':'JAVA_GENERATOR_COMPLETE_NO_CASSANDRA'}

def registry_record(exp:Path,workspace:Path,src:Path,old3:Path,old2:Path,
                    prereg:dict,audit:dict):
    settings=prereg['generation_settings']
    return {'status':'NEW_JAVA_GENERATION_UNMEASURED','experiment':str(exp.resolve()),
       'workspace':str(workspace.resolve()),'reference_v3':str(old3.resolve()),
       'reference_rev2':str(old2.resolve()),'source_project':str(src.resolve()),
       'source_sha256':audit,
       'generator_seeds':{'data_seed':settings['DATA_SEED'],'query_seed':settings['QUERY_SEED']},
       'scale':{'devices':prereg['devices'],'loops':prereg['loops'],
         'batch_size':prereg['batch_size'],'max_cells':prereg['max_cells'],
         'point_step_ms':settings['POINT_STEP'],
         'query_interval_ms':settings['QUERY_INTERVAL'],
         'group_by_ms':settings['GROUP_BY_TIME_UNIT']},
       'no_cassandra_access_during_generation':True,
       'recovered_prebenchmark_from_preregistered_java_data':True}

def audit_new(exp:Path,old3:Path,old2:Path,addon:Path,rev2:Path,rev3:Path):
    # The original audited Rev3 gate is loaded unchanged.
    for x in (addon,rev3,rev2,exp):
        s=str(x.resolve())
        if s not in sys.path:sys.path.insert(0,s)
    from rev3_confirmatory import gate
    gate.assert_rev3_implementation()
    return gate.check_new_generation(exp,old3,old2)

def recover(args):
    workspace=args.workspace.resolve();source=args.source_project.resolve()
    old3=args.reference_v3.resolve();old2=args.reference_rev2.resolve()
    G.check_reference(old3)
    run=G.choose_run(workspace,args.run_dir)
    prereg=G.read_json(workspace/'HOLDOUT_GENERATION_PREREGISTRATION.json')
    assert_campaign_preregistration(prereg,args.campaign)
    source_info=G.capture_check(workspace,run,old3)
    align=G.planned_scenario_alignment(source_info['src'],source_info['config'],prereg)
    if align['scenario_corrected_ms']!=PRESETS[args.campaign]['GROUP_BY_TIME_UNIT']:
        raise ValueError('Recovered group by does not equal campaign preregistration')
    exp,log,meta,alias=ensure_fresh(workspace,run,args.campaign)
    # Only a failed prebenchmark may be recovered; the original generator and CSV stay immutable.
    protected=[workspace/'HOLDOUT_GENERATION_PREREGISTRATION.json',
               run/'native_queries.jsonl',run/'java_config/config.properties',
               source_info['src']/'phase_a1/inputs/scenario.json']
    before={str(p):G.digest(p) for p in protected}
    print('Campaign:',args.campaign)
    print('Java GROUP_BY_TIME_UNIT (preregistered):',align['scenario_corrected_ms'])
    print('Isolated scenario before recovery:',align['scenario_original_ms'])
    print('Original failed experiment preserved:',run/'schemalens')
    print('New recovery experiment:',exp)
    if args.dry_run:
        print('DRY RUN: no files written, no Java regeneration, no Cassandra')
        return
    G.shutil.copytree(source_info['src'],exp,ignore=G.ignore_code)
    after_scenario=G.write_aligned_scenario(exp,align)
    cmd=['bash',str(exp/'scripts/run_native_prebenchmark.sh'),
         str((run/'native_data').resolve()),str(source_info['capture'].resolve()),
         str(source_info['config'].resolve()),str(prereg['max_cells'])]
    with log.open('x',encoding='utf-8') as stream:
        stream.write('COMMAND: '+repr(cmd)+'\n');stream.flush()
        proc=subprocess.Popen(cmd,cwd=exp,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                         text=True,encoding='utf-8',errors='replace')
        recent=deque(maxlen=30)
        for line in proc.stdout:
            stream.write(line);recent.append(line)
            if 'Error' in line or 'FAIL' in line or 'frozen' in line.lower():
                print(line.rstrip(),flush=True)
        code=proc.wait()
    if code:
        print('LAST LOG LINES:\n'+''.join(recent),file=sys.stderr)
        raise RuntimeError(f'Isolated prebenchmark failed (exit {code}). Log preserved: {log}')
    v3_hash=G.verify_stage8(exp)
    for p in protected:
        if G.digest(p)!=before[str(p)]:
            raise ValueError('Protected original data/config/preregistration was modified: '+str(p))
    audit=audit_new(exp,old3,old2,args.addon,args.rev2_addon,args.rev3_addon)
    shutil.copyfile(source_info['capture'],alias)
    if G.digest(alias)!=source_info['capture_sha256']:
        raise ValueError('Provenance alias does not match generated capture')
    # Ensure Rev2's independent capture locator sees exactly one matching alias.
    from v4_rev2 import prebenchmark as R2
    _queries=G.read_json(exp/'phase_a1/outputs/query_instances_pilot.json')
    found=R2.find_capture(exp,_queries)
    if Path(found['file']).resolve()!=alias.resolve() or found['sha256']!=G.digest(alias):
        raise ValueError('Rev2 capture provenance failed to match recovered alias')
    out=output_record(exp,run,found['sha256'],alias)
    reg=registry_record(exp,workspace,source,old3,old2,prereg,audit)
    metadata={'status':'REV3_JAVA_PREREGISTERED_GROUPBY_RECOVERED_BEFORE_CASSANDRA',
        'created_utc':datetime.now(timezone.utc).isoformat(),
        'campaign':args.campaign,'preregistration_sha256':before[str(workspace/'HOLDOUT_GENERATION_PREREGISTRATION.json')],
        'java_config_sha256':before[str(run/'java_config/config.properties')],
        'native_java_capture_sha256':before[str(run/'native_queries.jsonl')],
        'scenario_before_sha256':align['scenario_before_sha256'],
        'scenario_after_sha256':after_scenario,
        'scenario_original_group_by_ms':align['scenario_original_ms'],
        'registered_java_group_by_ms':align['scenario_corrected_ms'],
        'new_v3_hbench_sha256':v3_hash,
        'original_prebenchmark_failed':True,'java_generation_reused':True,
        'original_failure_log':str(run/'logs/prebenchmark.log'),
        'recovery_prebenchmark_log':str(log),
        'no_cassandra_before_freeze':True,
        'isolated_experiment':str(exp),'source_integrity':audit}
    # Only after the complete V3 parent and capture have been verified can
    # the ordinary Rev3 run_campaign.sh continue with freeze and preflight.
    for name,obj in [(workspace/'HOLDOUT_GENERATION_OUTPUT.json',out),
                     (workspace/'REV3_NEW_CAMPAIGN_REGISTRY.json',reg), (meta,metadata)]:
        with name.open('x',encoding='utf-8') as f:
            json.dump(obj,f,indent=2,sort_keys=True,ensure_ascii=False);f.write('\n')
    print('SUCCESS: Java data reused and new V3 Stage8 frozen; no Cassandra used')
    print('FRESH_EXPERIMENT='+str(exp))
    print('NEXT: run_campaign.sh',args.campaign,'freeze; then preflight')

def main():
    p=argparse.ArgumentParser(description='Recover Rev3 independent Java campaign A/B after A1 GROUP_BY mismatch')
    p.add_argument('--workspace',type=Path,required=True)
    p.add_argument('--source-project',type=Path,required=True)
    p.add_argument('--reference-v3',type=Path,required=True)
    p.add_argument('--reference-rev2',type=Path,required=True)
    p.add_argument('--addon',type=Path,required=True,help='SchemaLens_Rev3_Independent_Confirmatory')
    p.add_argument('--rev2-addon',type=Path,required=True,help='SchemaLens_V4_Rev2_Confirmatory')
    p.add_argument('--rev3-addon',type=Path,required=True,help='SchemaLens_V4_Rev3_PlanAware')
    p.add_argument('--run-dir',type=Path,help='Explicit generated v3_* run directory; inferred if unique')
    p.add_argument('--campaign',choices=['A','B'],required=True)
    p.add_argument('--dry-run',action='store_true')
    args=p.parse_args()
    try:recover(args)
    except Exception as e:
        print(type(e).__name__+': '+str(e),file=sys.stderr)
        raise SystemExit(2)

if __name__=='__main__':main()
