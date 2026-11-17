#!/usr/bin/env python3
"""Resume Java query capture and Stages 1-8 without regenerating native CSVs."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

PROJECT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT))
from scripts import auto_official_v3 as auto
from phase_a1.tools.official_query_capture import validate


def cmd(argv,log,cwd):
    log.parent.mkdir(parents=True,exist_ok=True)
    print('  $ '+argv[0]+' ... (full command in '+str(log)+')',flush=True)
    with log.open('w',encoding='utf8') as f:
        f.write('COMMAND: '+repr(argv)+'\n');f.flush()
        p=subprocess.run(argv,cwd=cwd,stdout=f,stderr=subprocess.STDOUT)
    if p.returncode:
        tail='\n'.join(log.read_text(encoding='utf8',errors='replace').splitlines()[-80:])
        raise RuntimeError(f'{argv[0]} exited {p.returncode}; log: {log}\n{tail}')


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--run-dir',type=Path,required=True)
    ap.add_argument('--max-cells',type=int,default=500000)
    a=ap.parse_args()
    if a.max_cells < 1:raise RuntimeError('--max-cells must be positive')
    run=a.run_dir.expanduser().resolve()
    workspace=run.parent.parent
    upstream=workspace/'upstream_iot_benchmark'
    cpfile=workspace/'build_cache/core_classpath.txt'
    data=run/'native_data'
    config=run/'java_config/config.properties'
    java_conf=config.parent
    original=run/'native_queries.jsonl'
    for x in (upstream/'core/target/classes',cpfile,data/'schema.txt',data/'info.txt',config,original,auto.EXPORTER):
        if not x.exists():raise RuntimeError(f'Missing input (no regeneration attempted): {x}')
    if not list(data.glob('*/BigBatch_*.csv')):raise RuntimeError('Missing native CSVs')
    auto.check_git_checkout(upstream,workspace/'build_cache/check.log')
    cp=os.pathsep.join((str(upstream/'core/target/classes'),cpfile.read_text().strip()))
    if not cpfile.read_text().strip():raise RuntimeError('Empty cached classpath')
    stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    tag=f'q12fixed_{stamp}_pid{os.getpid()}'
    new_capture=run/f'native_queries_{tag}.jsonl'
    classes=run/f'capture_classes_{tag}'
    experiment=run/f'schemalens_{tag}'
    logs=run/'logs'/tag
    for x in (new_capture,classes,experiment,logs):
        if x.exists():raise RuntimeError(f'Will not overwrite: {x}')
    classes.mkdir(parents=True)
    print('\n[1/4] Recompile ONLY repaired Java exporter (no Maven)...',flush=True)
    cmd(['javac','-encoding','UTF-8','-cp',cp,'-d',str(classes),str(auto.EXPORTER)],logs/'javac.log',upstream)
    counts={}
    for line in original.read_text(encoding='utf8').splitlines():
        if line.strip():
            x=json.loads(line); counts[x['operation']]=counts.get(x['operation'],0)+1
    repetitions=counts.get('PRECISE_QUERY',0)
    if repetitions<1 or repetitions>10000:raise RuntimeError('Cannot determine original sample count')
    print('\n[2/4] Capture full DTOs for Q1-Q12; old JSONL kept intact...',flush=True)
    capture_cp=os.pathsep.join((str(classes),cp))
    cmd(auto.capture_java_command(capture_cp,java_conf,new_capture,repetitions),logs/'capture.log',upstream)
    byq,n=validate(new_capture,[f'Q{i}' for i in range(1,13)])
    # Fail closed on any unresolved reference, including future DTO variants.
    def check_refs(x,where):
        if isinstance(x,dict):
            if '$ref' in x:raise ValueError(f'Unexpected Fastjson $ref at {where}: {x["$ref"]}')
            for k,v in x.items():check_refs(v,where+'.'+str(k))
        elif isinstance(x,list):
            for i,v in enumerate(x):check_refs(v,where+f'[{i}]')
    for qid,evts in byq.items():
        for e in evts:check_refs(e['query'],qid)
    print('  Validated native objects:',n,flush=True)
    print('\n[3/4] Create fresh isolated SchemaLens copy...',flush=True)
    auto.isolate_pipeline(experiment)
    print('  Folder:',experiment,flush=True)
    print('\n[4/4] Import, activate and freeze Stage8; Stage9 DRY RUN only...',flush=True)
    cmd(['bash','scripts/run_native_prebenchmark.sh',str(data),str(new_capture),str(config),str(a.max_cells)],logs/'prebenchmark.log',experiment)
    h=experiment/'outputs/stage8/30_hbench_summary.csv'
    fm=experiment/'outputs/stage8/32_frozen_manifest.json'
    for x in (h,fm):
        if not x.is_file():raise RuntimeError(f'Stage8 missing: {x}')
    manifest={
        'run_type':'q12_fastjson_ref_fix_resume',
        'upstream_commit':auto.PINNED_SHA,
        'reused_native_csv_without_regeneration':True,
        'old_query_capture':str(original),'old_query_capture_sha256':auto.sha256(original),
        'new_query_capture':str(new_capture),'new_query_capture_sha256':auto.sha256(new_capture),
        'native_schema_sha256':auto.sha256(data/'schema.txt'),
        'native_config_sha256':auto.sha256(config),
        'captured_queries_by_class':n,
        'hbench_csv':str(h),'hbench_csv_sha256':auto.sha256(h),
        'frozen_manifest':str(fm),'frozen_manifest_sha256':auto.sha256(fm),
        'Cassandra_used':False,'Stage9_measured':False,
        'limitation':'Java parameter samples only; not exact SQL/scheduling replay',
    }
    manifest_path=run/f'RESUME_Q12_MANIFEST_{tag}.json'
    manifest_path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf8')
    print('\nSUCCESS: Stage8 frozen. NO Cassandra benchmark run.')
    print('HBench:',h)
    print('Frozen manifest:',fm)
    print('Resume manifest:',manifest_path)
    return 0

if __name__=='__main__':
    try:sys.exit(main())
    except Exception as e:
        print('\nERROR: '+str(e),file=sys.stderr)
        sys.exit(2)
