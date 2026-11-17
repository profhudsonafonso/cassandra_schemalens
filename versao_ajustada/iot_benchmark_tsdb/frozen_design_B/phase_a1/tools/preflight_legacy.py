"""Read (not install) original Stage 4-9 ZIPs, report TSBS-specific blockers."""
import argparse
import hashlib
import json
import re
from pathlib import Path
from zipfile import ZipFile

CHECKS={
  4:('methodology/tsbs/stage4/candidate_generator.py',{
     'hardcoded_10_usage_metrics':r'len\(metrics\)\s*!=\s*10',
     'hardcoded_cpu_tables':r'ts0_cpu_metric_raw',
     'requires_tsbs_cpu_schema':r'tsbs_cpu_only_schema\.json',
  }),
  5:('methodology/tsbs/stage5/planner.py',{'cpu_only_workload':r'TSBS|CPU|METRICS'}),
  6:('methodology/tsbs/stage6/tsbs_adapter.py',{'tsbs_native_input':r'TSBS|tsbs|usage_user'}),
  7:('methodology/tsbs/stage7/estimator.py',{'tsbs_proxies':r'TSBS|CPU|METRICS'}),
  8:('methodology/tsbs/stage8/consolidator.py',{
     'family_whitelist_excludes_TS4':r'\{"TS0",\s*"TS1",\s*"TS2",\s*"TS5",\s*"TS6"\}',
     'tsbs_path_contract':r'inputs/workload/query_manifest\.csv',
  }),
  9:('methodology/tsbs/stage9/dataset.py',{
     'cpu_only_native_tsbs_reader':r'TSBS|tsbs',
     '16_columns_tsbs_serialization':r'len\(p\)\s*!=\s*16',
  })
}

def run(root:Path)->dict:
    result={'stage_files':{},'status':'LEGACY_TSBS_STAGES_NOT_DIRECTLY_COMPATIBLE_WITH_IOT_BENCHMARK',
            'no_legacy_source_modified':True,'requires_new_iot_adapters':True}
    for step,(target,markers) in CHECKS.items():
        files=sorted(root.glob(f'SchemaLens_TSBS_Stage{step}_ADDON*.zip'))
        if len(files)!=1:raise FileNotFoundError(f'Expected exactly one Stage {step} ZIP in {root}, got {len(files)}')
        path=files[0]
        with ZipFile(path) as archive:
            choices=[n for n in archive.namelist() if n.endswith(target)]
            if len(choices)!=1:raise ValueError(f'Missing or ambiguous {target}: {path}')
            contents=archive.read(choices[0]).decode('utf-8')
        found={label:bool(re.search(regex,contents)) for label,regex in markers.items()}
        result['stage_files'][str(step)]={'zip_filename':path.name,
                'zip_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
                'source':choices[0], 'checks':found}
    return result

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--zips-dir',type=Path,required=True)
    parser.add_argument('--out',type=Path,default=Path('outputs/legacy_preflight.json'))
    args=parser.parse_args()
    result=run(args.zips_dir)
    args.out.parent.mkdir(parents=True,exist_ok=True)
    args.out.write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n')
    print('Legacy ZIPs inspected. TSBS restrictions discovered; no files changed.')
    for step,file in result['stage_files'].items(): print(f"Stage {step}: {file['checks']}")

if __name__=='__main__':main()
