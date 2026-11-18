"""CLI for post-freeze E1/E2/E3 Cassandra diagnostic experiments."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
from .core import EXPECTED,read_context,safety_snapshot,verify_stage9,write_json,protect_output


def main(argv=None):
    ap=argparse.ArgumentParser(description='Post-freeze Cassandra V3 diagnostics; NEVER reselect HBench')
    ap.add_argument('--experiment',type=Path,required=True,help='Exact isolated frozen SchemaLens V3 path')
    ap.add_argument('--mode',choices=['preflight','e1','e2','e3'],required=True)
    ap.add_argument('--host',default='127.0.0.1')
    ap.add_argument('--port',type=int,default=9043)
    ap.add_argument('--rounds',type=int,default=40)
    ap.add_argument('--warmup',type=int,default=10)
    ap.add_argument('--seed',type=int,default=666)
    ap.add_argument('--days',type=int,choices=[1,7,30],help='E3 only: time-axis scale; one run per scale')
    ap.add_argument('--confirm-diagnostic-reset',default='',help='E3 only: RESET_DIAGNOSTIC_KEYSPACES_ONLY')
    ap.add_argument('--output',type=Path,help='Default: experiment/outputs/diagnostics_v3_e123/<mode>[_dNN]')
    args=ap.parse_args(argv)
    if args.rounds<10 or args.warmup<0:ap.error('--rounds must be >=10; --warmup >=0')
    if args.mode=='e3' and args.days is None:ap.error('E3 needs --days {1,7,30}')
    if args.mode!='e3' and args.days is not None:ap.error('--days is only for E3')
    exp=args.experiment.resolve(strict=True)
    if not (exp/'iot_pipeline/stage9.py').exists():ap.error('Invalid isolated V3 project')
    # Always import the USER'S isolated experiment modules. Never replace code.
    sys.path.insert(0,str(exp))
    sys.modules.pop('iot_pipeline',None)
    from .core import read_context
    before=safety_snapshot(exp)
    previous=verify_stage9(exp)
    chosen,rows,qs,sensors,meta,hashes=read_context()
    if len(chosen)!=4 or len(rows)!=1800 or len(qs)!=12:
        raise ValueError('Expected frozen native V3 (4 deployments, 1800 metric cells, 12 queries)')
    default=exp/'outputs/diagnostics_v3_e123'/ (args.mode + (f'_d{args.days:02d}' if args.mode=='e3' else ''))
    out=(args.output or default).resolve()
    if out==exp or (out.is_relative_to(exp) and not out.is_relative_to(exp/'outputs/diagnostics_v3_e123')):
        raise ValueError('Inside the experiment, output must be under outputs/diagnostics_v3_e123')
    base={'frozen_hbench_sha256':EXPECTED,'before_frozen_sha256':before,
          'original_stage9_manifests':previous,'original_metric_cells':len(rows),
          'query_sample_class_count':len(qs),'baseline_external_to_hbench':True,
          'native_sql_replay':False,'query_frequency_replay':False}
    if args.mode=='preflight':
        data={'status':'PASS_OFFLINE','frozen_selection_unchanged':True,
              'Cassandra_used':False,'E1_E2_readonly':True,
              'E3_shadow_keyspaces_only':True,'experiments':['e1','e2','e3'],**base}
        print(json.dumps(data,indent=2));return 0
    from .experiments import e1,e2,e3
    protect_output(out)
    # Never emit a partial success summary; create results only after full correctness.
    try:
        {'e1':e1,'e2':e2,'e3':e3}[args.mode](args,out,base)
    finally:
        after=safety_snapshot(exp)
        if after!=before:
            raise RuntimeError('CRITICAL: frozen files changed; STOP and inspect!')
    manifest=json.loads((out/'manifest.json').read_text())
    manifest['frozen_sha256_unchanged_postrun']=True
    write_json(out/'manifest.json',manifest)
    print('PASS:',args.mode.upper(),'Results:',out)
    print('Frozen HBench SHA256 unchanged:',EXPECTED)
    return 0

if __name__=='__main__':raise SystemExit(main())
