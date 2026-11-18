"""Verify new V4/E4 CSV metrics and write a descriptive, not causal, report."""
from __future__ import annotations
import argparse,csv,json,statistics
from collections import defaultdict
from pathlib import Path
from .core import FROZEN_SHA,BASE,BASE_OPT,pctl,save_csv


def rows(path):
    with Path(path).open(newline='',encoding='utf8') as f:return list(csv.DictReader(f))


def audit_one(folder):
    m=json.loads((folder/'manifest.json').read_text(encoding='utf8'))
    if not m.get('old_v3_hashes_unchanged') or m['original_v3_hbench_sha256']!=FROZEN_SHA or m.get('Cassandra_used') is not True:
        raise AssertionError('V4 run manifest does not prove frozen V3 integrity')
    raw=rows(folder/'v4_raw.csv');summ=rows(folder/'v4_queries.csv')
    grouped=defaultdict(list)
    for r in raw:grouped[(r['variant'],r['query_label'])].append(float(r['latency_ms']))
    if len(raw)!=m['sample_count'] or len(summ)!=m['live_validated_routes']:
        raise AssertionError('V4 row totals differ from manifest')
    for r in summ:
        v=grouped[(r['variant'],r['query_label'])]
        if len(v)!=m['rounds'] or abs(pctl(v,95)-float(r['p95_ms']))>1e-7:
            raise AssertionError('Unreproducible p95 '+str((r['variant'],r['query_label'])))
        if r['correctness']!='PASS_BEFORE_TIMING':raise AssertionError('Incorrect query in V4')
    baseline={(r['variant'],r['query_label']):float(r['p95_ms']) for r in summ}
    variants=sorted({r['variant'] for r in summ})
    overview=[]
    for variant in variants:
        native=[baseline[(variant,'NATIVE_FIXED_Q'+str(i))] for i in range(1,13)]
        q8=baseline[(variant,'NATIVE_FIXED_Q8')]
        overview.append({'days':m['scale_days'],'variant':variant,'mean_p95_native_12_ms':round(statistics.mean(native),6),
           'mean_p95_excluding_q8_ms':round(statistics.mean(v for i,v in enumerate(native,1) if i!=8),6),
           'q8_p95_ms':round(q8,6),'ratio_all_vs_baseline_q8_optimized':round(statistics.mean(native)/statistics.mean([baseline[(BASE_OPT,'NATIVE_FIXED_Q'+str(i))] for i in range(1,13)]),6),
           'ratio_excluding_q8_vs_opt_baseline':round(statistics.mean(v for i,v in enumerate(native,1) if i!=8)/statistics.mean([baseline[(BASE_OPT,'NATIVE_FIXED_Q'+str(i))] for i in range(1,13) if i!=8]),6),
           'synthetic_extension':m['derived_synthetic_days']})
    return overview, m


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--experiment',type=Path,required=True);a=ap.parse_args()
    output=a.experiment/'outputs/diagnostics_v4'
    folders=sorted(p for p in output.glob('live_d*') if (p/'manifest.json').exists())
    if not folders:raise FileNotFoundError('No completed V4/E4 Cassandra measurements')
    full=[];md=['# SchemaLens — V4 / E4 diagnostic report', '',
       'Only new diagnostic experiments. The original V3 HBench and historical measurements were not modified.',
       'Numbers below are **means of the 12 individual query p95 values**, not a pooled workload p95.',
       'The V4 designs were proposed after V3 diagnostics; NOT an independent V3 prebenchmark selection.', '']
    for folder in folders:
        overview,m=audit_one(folder);full+=overview
        lookup={r['variant']:r for r in overview}
        old,optimized=lookup[BASE]['q8_p95_ms'],lookup[BASE_OPT]['q8_p95_ms']
        md.extend([f'## {m["scale_days"]} day(s)', '',
            f'- Q8: baseline full scan **{old:.3f} ms**; optimized baseline **{optimized:.3f} ms**. Reduction = **{100*(1-optimized/old):.2f}%**.',
            '- All queries have passed correctness checks before measurement; p95 verified from raw CSV.',
            '', '| Variant | Mean of Q1–Q12 p95 (ms) | Mean without Q8 (ms) | Q8 p95 (ms) |',
            '|---|---:|---:|---:|'])
        for item in sorted(overview,key=lambda x:x['mean_p95_native_12_ms']):
            md.append(f"| {item['variant']} | {item['mean_p95_native_12_ms']:.3f} | {item['mean_p95_excluding_q8_ms']:.3f} | {item['q8_p95_ms']:.3f} |")
        md.append('')
    md.extend(['## Scientific limitations','',
       '- Optimized Q8 benefits from DESC clustering + paged early stopping; latency includes the Cassandra driver and Python.',
       '- TS4 is a batch-maintained latest-value snapshot; streaming updates, late events, deletion, and transaction consistency remain unvalidated.',
       '- 7/30-day experiments synthetically copy Java-generated native values into new days, not a new official benchmark generation.',
       '- 12 Java parameter objects are sampled once per query class; frequency-weighted official IoT Benchmark throughput is not reproduced.',
       '- Trials interleave candidates and control query order, but independent restarts and server-side metrics are still needed.',
       '- Results must not feed a retrospective rewrite of V3 Stage4-8 rules or its HBench selection.', ''])
    save_csv(output/'v4_audited_comparison.csv',full)
    (output/'V4_E4_REPORT_PT.md').write_text('\n'.join(md)+'\n',encoding='utf8')
    print('Audited',len(folders),'V4 runs. Report:',output/'V4_E4_REPORT_PT.md')

if __name__=='__main__':main()
