"""Optional AFTER-FREEZE descriptive comparison with existing V4 exploratory data.

NEVER imported by selector or freeze CLI. Not hold-out validation and not evidence of
independent predictive selection; use a new dataset for confirmatory benchmarking.
"""
from __future__ import annotations
import argparse
import csv
from pathlib import Path
from . import selector as S
from .cli import audit_existing,OUTPUT

MAP={
 ('ts0_iot_raw',False):'H_0bd185590c5a',
 ('ts0_iot_raw',True):'H_2641c9751dce',
 ('ts2_iot_wide_unbucketed',False):'V4_TS2U',
 ('ts2_iot_wide_unbucketed',True):'V4_TS2U_TS4',
 ('ts2_iot_wide',False):'V4_TS2D',
 ('ts2_iot_wide',True):'V4_TS2D_TS4',
}


def main():
    parser=argparse.ArgumentParser(description='POST-FREEZE RETROSPECTIVE ONLY; does not change V4 selection')
    parser.add_argument('--experiment',type=Path,required=True)
    args=parser.parse_args();exp=args.experiment.resolve()
    auto=exp/OUTPUT;audit_existing(exp,auto)
    chosen=S.jread(auto/'stage8/28_hbench_v4_auto_frozen.json')
    before=S.digest(auto/'stage8/28_hbench_v4_auto_frozen.json')
    out=[]
    for days in (1,7,30):
        p=exp/f'outputs/diagnostics_v4/live_d{days:02d}/v4_aggregate.csv'
        if not p.exists():continue
        with p.open(newline='',encoding='utf8') as f:raw=list(csv.DictReader(f))
        index={(r['variant'],r['metric']):r for r in raw}
        control=index.get(('BASELINE_Q8_OPTIMIZED','NATIVE_ALL_12'))
        if not control:continue
        base=float(control['mean_of_query_p95_ms'])
        for d in chosen:
            key=MAP.get((d['canonical_source'],S.LATEST in d['candidates']))
            item=index.get((key,'NATIVE_ALL_12'))
            out.append({'scale_days':days,'auto_deployment_id':d['deployment_id'],
                        'existing_exploratory_variant':key or 'NOT_AVAILABLE',
                        'mean_of_12_p95_ms':item['mean_of_query_p95_ms'] if item else '',
                        'baseline_q8_optimized_p95_ms':base,
                        'delta_vs_baseline_pct':round((float(item['mean_of_query_p95_ms'])/base-1)*100,4) if item else '',
                        'evidence':'POST_HOC_EXISTING_EXPLORATORY; NOT_INDEPENDENT_HOLDOUT'})
    if not out:
        print('No existing V4 live aggregate files found. New freeze remains intact.');return
    report=exp/'outputs/v4_auto_posthoc'
    report.mkdir(parents=True,exist_ok=True)
    dest=report/'descriptive_existing_results.csv'
    if dest.exists():raise FileExistsError('Refusing to overwrite existing retrospective: '+str(dest))
    S.csv_out(dest,out,['scale_days','auto_deployment_id','existing_exploratory_variant','mean_of_12_p95_ms',
                       'baseline_q8_optimized_p95_ms','delta_vs_baseline_pct','evidence'])
    if S.digest(auto/'stage8/28_hbench_v4_auto_frozen.json')!=before:
        raise AssertionError('V4 auto freeze changed during post-hoc comparison')
    print('POST-HOC ONLY:',dest,'; selected configurations unchanged')

if __name__=='__main__':main()
