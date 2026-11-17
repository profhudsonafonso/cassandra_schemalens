"""Read-only post-run comparison of the same frozen native V3 experiment.

The primary summary is the unweighted mean of 12 per-query p95 latencies.
It is not a pooled percentile, an IoT Benchmark score, or a statistical test.
"""
from __future__ import annotations
import argparse
import csv
from pathlib import Path
import statistics

from .shared import inputs,read,sha,csv_write,write,path4
from .stage9 import confirm_frozen
from .native_reference_v3 import NATIVE_LABEL


def rows_csv(p):
    with Path(p).open(newline='',encoding='utf-8') as stream:
        return list(csv.DictReader(stream))


def compare_runs(hbench:Path,baseline:Path):
    selected=confirm_frozen()
    selected_by_id={h['deployment_id']:h for h in selected}
    frozen={
        'hbench_sha256':sha(path4(8,'28_hbench_frozen.json')),
        'source_rows_sha256':sha(inputs()['native']),
        'query_instances_sha256':sha(inputs()['queries'])}
    bm=read(baseline/'37_manifest.json'); hm=read(hbench/'37_manifest.json')
    if bm.get('status')!='NATIVE_REFERENCE_POST_FREEZE' or bm.get('deployment_id')!=NATIVE_LABEL:
        raise ValueError('Not a validated native V3 baseline run')
    if bm.get('correctness')!='PASS_ALL_12_QUERIES':
        raise ValueError('Baseline failed native correctness')
    if bm.get('baseline_part_of_hbench') is not False or bm.get('official_iot_benchmark_score') is not False:
        raise ValueError('Baseline must be independent and not officially branded')
    for k,v in frozen.items():
        if bm.get(k)!=v:
            raise ValueError(f'Baseline incorrect frozen provenance: {k}')
        # The original Stage9 manifest lacks a query-file checksum. No inferred proof.
        if k!='query_instances_sha256' and hm.get(k)!=v:
            raise ValueError(f'HBench incorrect frozen provenance: {k}')
    if hm.get('query_instances_sha256') not in (None, frozen['query_instances_sha256']):
        raise ValueError('HBench native query checksum mismatch')
    if hm.get('official_iot_benchmark_score') is not False:
        raise ValueError('Unexpected official benchmark status')
    br=rows_csv(baseline/'35_query_p95.csv')
    hr=rows_csv(hbench/'35_query_p95.csv')
    expect_queries={f'Q{i}' for i in range(1,13)}
    if len(br)!=12 or {x['query_id'] for x in br}!=expect_queries or any(x['deployment_id']!=NATIVE_LABEL for x in br):
        raise ValueError('Incomplete/duplicate native baseline query cases')
    if len(hr)!=12*len(selected) or {(x['deployment_id'],x['query_id']) for x in hr}!=\
           {(hid,q) for hid in selected_by_id for q in expect_queries}:
        raise ValueError('Incomplete/duplicate HBench query cases')
    if any(x['correctness']!='PASS' for x in hr+br):
        raise ValueError('Query correctness must pass before comparing timings')
    if any(int(x['successful_operations'])!=int(br_map['successful_operations'])
           for q in expect_queries for br_map in [next(b for b in br if b['query_id']==q)]
           for x in hr if x['query_id']==q):
        raise ValueError('Unequal timed rounds by query')
    if hm.get('measurement_rounds') is not None and int(hm['measurement_rounds'])!=int(bm['measurement_rounds']):
        raise ValueError('Unequal benchmark rounds')
    if hm.get('warmup') is not None and int(hm['warmup'])!=int(bm['warmup']):
        raise ValueError('Unequal warmup repetitions')
    b_by={r['query_id']:r for r in br}
    by_h={hid:[] for hid in selected_by_id}
    detail=[]
    for row in hr:
        q=row['query_id']; hid=row['deployment_id']
        p95=float(row['p95_ms']); baseline_p95=float(b_by[q]['p95_ms'])
        if baseline_p95<=0 or p95<=0:
            raise ValueError('Nonpositive latency in measured CSV')
        by_h[hid].append(p95)
        detail.append({'deployment_id':hid,'query_id':q,
          'routed_family':row['family'],'baseline_p95_ms':baseline_p95,
          'hbench_p95_ms':p95,'relative_difference_pct':100*(p95/baseline_p95-1),
          'hbench_faster_in_this_run':p95<baseline_p95})
    avg_base=statistics.mean(float(v['p95_ms']) for v in br)
    summary=[{'deployment_id':NATIVE_LABEL,'families':'IOTDB_TABLE_TRANSLATED_CASSANDRA',
              'mean_of_query_p95_ms':avg_base,'relative_difference_pct':0.0}]
    for hid in selected_by_id:
        m=statistics.mean(by_h[hid])
        summary.append({'deployment_id':hid,'families':'|'.join(selected_by_id[hid]['families']),
                        'mean_of_query_p95_ms':m,
                        'relative_difference_pct':100*(m/avg_base-1)})
    provenance={'status':'POST_SELECTION_COMPARISON','frozen_hashes':frozen,
      'selection_modified':False,'baseline_independent':True,'native_sql_replay':False,
      'metric':'UNWEIGHTED_MEAN_OF_12_PER_QUERY_P95',
      'baseline_rounds':bm['measurement_rounds'],
      'hbench_query_sha256_explicitly_attested':hm.get('query_instances_sha256')==frozen['query_instances_sha256'],
      'caveats':['Original Stage9 manifest does not explicitly include a native query-file checksum; '
                 'frozen inputs are checked now, but past query provenance cannot be independently proven by that manifest.',
                 'Different sequential runs are subject to order/JVM/cache bias; no statistical significance claim.',
                 'SQL and mixed frequency replay from official IoT Benchmark are not performed.']}
    return detail,summary,provenance


def main():
    ap=argparse.ArgumentParser(description='Native V3 HBench vs reference (READ ONLY)')
    ap.add_argument('--hbench',type=Path,required=True)
    ap.add_argument('--baseline',type=Path,required=True)
    ap.add_argument('--output',type=Path,required=True)
    args=ap.parse_args()
    if args.output.exists() and any(args.output.iterdir()):
        raise ValueError('Comparison output already populated; refusing overwrite')
    detail,summary,provenance=compare_runs(args.hbench,args.baseline)
    args.output.mkdir(parents=True,exist_ok=True)
    csv_write(args.output/'reference_vs_hbench_by_query.csv',detail,
        ['deployment_id','query_id','routed_family','baseline_p95_ms','hbench_p95_ms',
         'relative_difference_pct','hbench_faster_in_this_run'])
    csv_write(args.output/'reference_vs_hbench_summary.csv',summary,
        ['deployment_id','families','mean_of_query_p95_ms','relative_difference_pct'])
    write(args.output/'comparison_manifest.json',provenance)
    print('Read-only comparison saved:',args.output)

if __name__=='__main__':
    main()
