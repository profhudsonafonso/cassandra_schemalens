"""Read-only Stage9 HBench vs independent Cassandra-translated reference.

No changes to Stage4-8, no benchmark-based activation or selection.
"""
from __future__ import annotations
import argparse
import csv
import statistics
from pathlib import Path
from .shared import read,write,csv_write,sha,path4,inputs
from .stage9 import confirm_frozen
from .reference_baseline import LABEL


def csv_rows(path):
    with Path(path).open(newline='',encoding='utf-8') as fd:return list(csv.DictReader(fd))


def summarize(hbench:Path,baseline:Path):
    confirm_frozen()
    bm=read(baseline/'37_manifest.json');hm=read(hbench/'37_manifest.json')
    expected_h=sha(path4(8,'28_hbench_frozen.json'))
    expected_data=sha(inputs()['native'])
    for name,m in [('baseline',bm),('HBench',hm)]:
        if m.get('hbench_sha256')!=expected_h or m.get('source_rows_sha256')!=expected_data:
            raise ValueError(f'{name} does not match CURRENT frozen HBench/data')
    if bm.get('deployment_id')!=LABEL or bm.get('official_iot_benchmark_score') is not False:
        raise ValueError('Incorrect baseline manifest')
    if hm.get('official_iot_benchmark_score') is not False:
        raise ValueError('Unexpected HBench source')
    hb=csv_rows(hbench/'35_query_p95.csv');ref=csv_rows(baseline/'35_query_p95.csv')
    baseline_by_q={r['query_id']:r for r in ref}
    if len(baseline_by_q)!=len(ref) or len(ref)!=12 or set(baseline_by_q)!={f'Q{i}' for i in range(1,13)}:
        raise ValueError('Unexpected baseline query coverage')
    chosen=confirm_frozen();selected={h['deployment_id']:h for h in chosen}
    for r in ref+hb:
        if r['correctness']!='PASS':raise ValueError('Correctness did not pass')
        if r['query_id'] not in baseline_by_q:raise ValueError('Query not covered by baseline')
        if int(r['successful_operations'])!=int(baseline_by_q[r['query_id']]['successful_operations']):
            raise ValueError('Unequal measured repetitions')
    if set(r['deployment_id'] for r in hb)!=set(selected):
        raise ValueError('Missing or extra HBench deployments')
    if len(hb)!=12*len(selected) or len({(r['deployment_id'],r['query_id']) for r in hb})!=len(hb):
        raise ValueError('HBench query coverage incomplete/duplicated')
    comparison=[];by_h={}
    ref_avg=statistics.mean(float(r['p95_ms']) for r in ref)
    for r in hb:
        qid=r['query_id'];h=r['deployment_id'];r95=float(r['p95_ms']);b95=float(baseline_by_q[qid]['p95_ms'])
        if b95<=0:raise ValueError('Nonpositive baseline p95')
        comparison.append({'deployment_id':h,'query_id':qid,'routed_family':r['family'],
            'baseline_p95_ms':b95,'hbench_p95_ms':r95,
            'ratio_hbench_over_reference':r95/b95,
            'relative_difference_pct':100*(r95/b95-1),
            'faster_observed':r95<b95})
        by_h.setdefault(h,[]).append(r95)
    deployments=[{'deployment_id':LABEL,'families':'IoTDB_TABLE_SHAPE_REFERENCE',
                  'mean_query_p95_ms':ref_avg,'relative_difference_pct':0.0,
                  'claim':'DIAGNOSTIC_PILOT_NOT_OFFICIAL'}]
    for h,vs in by_h.items():
        avg=statistics.mean(vs)
        deployments.append({'deployment_id':h,'families':'|'.join(selected[h]['families']),
            'mean_query_p95_ms':avg,'relative_difference_pct':100*(avg/ref_avg-1),
            'claim':'DIAGNOSTIC_PILOT_NOT_OFFICIAL'})
    return comparison,deployments


def main():
    ap=argparse.ArgumentParser(description='Read-only HBench vs table-shaped baseline')
    ap.add_argument('--hbench',type=Path,required=True);ap.add_argument('--baseline',type=Path,required=True)
    ap.add_argument('--output',type=Path,required=True)
    args=ap.parse_args()
    comparisons,deps=summarize(args.hbench,args.baseline)
    if args.output.exists() and any(args.output.iterdir()):raise ValueError('Refusing to overwrite comparison files')
    args.output.mkdir(parents=True,exist_ok=True)
    csv_write(args.output/'reference_vs_hbench_by_query.csv',comparisons,
              ['deployment_id','query_id','routed_family','baseline_p95_ms',
               'hbench_p95_ms','ratio_hbench_over_reference','relative_difference_pct','faster_observed'])
    csv_write(args.output/'reference_vs_hbench_summary.csv',deps,
              ['deployment_id','families','mean_query_p95_ms','relative_difference_pct','claim'])
    write(args.output/'comparison_manifest.json',{'status':'POST_HBENCH_DIAGNOSTIC',
          'reference_is_official':False,'no_selection_changes':True,
          'metric':'Unweighted mean of twelve per-query p95 values, not pooled p95',
          'warning':'Sequential, unbalanced small fixture; ratios are descriptive, not robust speedups'})
    print('Saved diagnostic comparison against independent table-shaped Cassandra reference:',args.output)
if __name__=='__main__':main()
