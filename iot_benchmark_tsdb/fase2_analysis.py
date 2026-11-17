#!/usr/bin/env python3
"""Fase 2 analyses (descriptive): python fase2_analysis.py e1 | e345 | e6

Reads only audited evidence; writes experiments/rev3_fase2/analysis/<name>_summary.json and CSVs.
"""
from __future__ import annotations

import csv
import json
import math
import statistics
import sys
import datetime as dt
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
EXP = PROJECT / 'experiments'
F2 = EXP / 'rev3_fase2'
OUT = F2 / 'analysis'
TEMPORAL = ('Q2', 'Q7', 'Q9', 'Q12')
SHORT = {'BASELINE_Q8_OPTIMIZED': 'ref', 'R3H_38939db96444': 'u', 'R3H_d2ea1974de53': 'd'}
LABEL = {'h1': 'H1 · 21 dias · 9 disp.', 'h2': 'H2 · 83 dias · 9 disp.', 'h3': 'H3 · 333 dias · 9 disp.',
         'n3': 'N3 · 21 dias · 3 disp.', 'n18': 'N18 · 21 dias · 18 disp.'}


def rcsv(p):
    with open(p, newline='') as f:
        return list(csv.DictReader(f))


def jload(p):
    return json.loads(Path(p).read_text())


def wcsv(p, rows):
    with open(p, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def audited(c, ds):
    a = jload(F2 / c / ds / 'benchmark' / 'audit.json')
    if a['status'] != 'PASS':
        raise ValueError(f'{c}/{ds} not audited')
    return rcsv(F2 / c / ds / 'benchmark' / 'query_p95.csv')


def p95_index(rows):
    """{(case, cond, cache, v): row}"""
    return {(r['case_id'], r['cond'], r['cache'], SHORT[r['variant']]): r for r in rows}


def med(xs):
    xs = list(xs)
    return round(statistics.median(xs), 4) if xs else None


def save(name, obj):
    OUT.mkdir(parents=True, exist_ok=True)
    obj['created'] = dt.datetime.now().astimezone().isoformat(timespec='seconds')
    (OUT / f'{name}_summary.json').write_text(json.dumps(obj, indent=2, ensure_ascii=False))
    print(json.dumps(obj, indent=1, ensure_ascii=False)[:4000])


def fmt(x, n=3):
    return f'{x:.{n}f}'.replace('.', ',')


# ---------------------------------------------------------------------------
def e1():
    s3 = jload(EXP / 'rev3_s3_physical_storage' / 's3_summary.json')['per_factor']
    s3rows = {(int(r['factor']), r['case_id']): r for r in rcsv(EXP / 'rev3_s3_physical_storage' / 's3_vs_s2_comparison.csv')}
    by_factor, detail = {}, []
    consistency = []
    for f in (1, 2, 4, 8):
        ds = f'd{f}'
        rec = dict(s2_median_du=s3[str(f)]['s2_median_d_over_u'], s3_median_du=s3[str(f)]['s3_median_d_over_u'],
                   s3_over_s2=s3[str(f)]['median_s3_over_s2'])
        effects = {v: [] for v in ('ref', 'u', 'd')}
        wins_u = wins_d = 0
        sst_winners = {}
        for rep in ('e1r2b', 'e1r3'):
            idx = p95_index(audited(rep, ds))
            cases = sorted({k[0] for k in idx if k[0].split('_')[0] in TEMPORAL})
            du = {'mem': [], 'sst': []}
            for c in cases:
                for cond in ('mem', 'sst'):
                    pu, pd = float(idx[c, cond, '', 'u']['p95_ms']), float(idx[c, cond, '', 'd']['p95_ms'])
                    du[cond].append(pd / pu)
                    if cond == 'sst':
                        sst_winners.setdefault(c, []).append('U' if pu < pd else 'D')
                        wins_u += pu < pd
                        wins_d += pd < pu
                for v in ('ref', 'u', 'd'):
                    r = float(idx[c, 'sst', '', v]['p95_ms']) / float(idx[c, 'mem', '', v]['p95_ms'])
                    effects[v].append(r)
                    detail.append(dict(factor=f, replicate=rep, case_id=c, variant=v,
                                       p95_mem=idx[c, 'mem', '', v]['p95_ms'], p95_sst=idx[c, 'sst', '', v]['p95_ms'],
                                       sst_over_mem=round(r, 5)))
            rec[{'e1r2b': 'r2', 'e1r3': 'r3'}[rep]] = dict(mem_median_du=med(du['mem']), sst_median_du=med(du['sst']),
                                 mem_wins_u=sum(x > 1 for x in du['mem']), sst_wins_u=sum(x > 1 for x in du['sst']))
        rec['flush_effect'] = {v: med(effects[v]) for v in effects}
        rec['wins_sst_u'], rec['wins_sst_d'] = wins_u, wins_d
        for c, w in sst_winners.items():
            r1 = s3rows[f, c]['s3_winner'][-1]
            consistency.append(len(set([r1] + w)) == 1)
        by_factor[str(f)] = rec
    OUT.mkdir(parents=True, exist_ok=True)
    wcsv(OUT / 'e1_flush_effect_detail.csv', detail)
    agree = sum(consistency)
    bf = by_factor
    find = [
        f"Mediana D/U em SSTable no 1× → 8×: r1 {fmt(bf['1']['s3_median_du'])} → {fmt(bf['8']['s3_median_du'])}; "
        f"r2 {fmt(bf['1']['r2']['sst_median_du'])} → {fmt(bf['8']['r2']['sst_median_du'])}; "
        f"r3 {fmt(bf['1']['r3']['sst_median_du'])} → {fmt(bf['8']['r3']['sst_median_du'])}.",
        f"Mesmo vencedor U/D nas três réplicas SSTable (r1, r2, r3) em {agree} de {len(consistency)} casos temporais.",
        f"Vitórias em SSTable somando r2 e r3: TS2-U {sum(v['wins_sst_u'] for v in bf.values())}, "
        f"TS2-D {sum(v['wins_sst_d'] for v in bf.values())} (de {2 * 64}).",
        "Efeito isolado do flush no 8× (SSTable/memtable, mesma sessão): "
        f"REF {(bf['8']['flush_effect']['ref'] - 1) * 100:+.1f}%, U {(bf['8']['flush_effect']['u'] - 1) * 100:+.1f}%, "
        f"D {(bf['8']['flush_effect']['d'] - 1) * 100:+.1f}%; a diferença S3/S2 (dias diferentes) era "
        f"REF {(bf['8']['s3_over_s2']['ref'] - 1) * 100:+.1f}%, U {(bf['8']['s3_over_s2']['u'] - 1) * 100:+.1f}%, "
        f"D {(bf['8']['s3_over_s2']['d'] - 1) * 100:+.1f}%.",
    ]
    save('e1', dict(by_factor=by_factor, winner_consistency=dict(agree=agree, total=len(consistency)), findings=find))


# ---------------------------------------------------------------------------
def e345():
    rows_out, by_ds = [], {}
    tot = dict(n=0, wins_u=0, wins_d=0, rev4_hits=0, always_u_hits=0, fewer_calls_hits=0)
    abs_err = []
    for c, dss in (('e3', ('h1', 'h2', 'h3')), ('e4', ('n3', 'n18'))):
        preds = {(p['dataset'], p['case_id']): p for p in jload(F2 / 'rev4' / f'predictions_{c}.json')['predictions']}
        for ds in dss:
            idx = p95_index(audited(c, ds))
            st = jload(F2 / c / ds / 'storage_pre_timing' / 'summary.json')['variants']
            pu_kb = st['sst:TS2-U']['metrics']['partition_mean_bytes'] / 1024
            pd_kb = st['sst:TS2-D']['metrics']['partition_mean_bytes'] / 1024
            r = dict(label=LABEL[ds], n=0, wins_u=0, wins_d=0, rev4_hits=0, always_u_hits=0, fewer_calls_hits=0,
                     u_partition_kb=pu_kb, d_partition_kb=pd_kb, du=[])
            for (dsk, case), p in sorted(preds.items()):
                if dsk != ds:
                    continue
                mu, md = float(idx[case, 'sst', '', 'u']['p95_ms']), float(idx[case, 'sst', '', 'd']['p95_ms'])
                win = 'TS2-U' if mu < md else 'TS2-D'
                r['n'] += 1
                r['wins_u'] += win == 'TS2-U'
                r['wins_d'] += win == 'TS2-D'
                r['rev4_hits'] += p['rev4_winner'] == win
                r['always_u_hits'] += p['always_u'] == win
                r['fewer_calls_hits'] += p['fewer_calls'] == win
                r['du'].append(md / mu)
                abs_err.append(abs(math.log(md / mu) - math.log(p['pred_d_over_u'])))
                rows_out.append(dict(campaign=c, dataset=ds, case_id=case, window_days=p['window_days'],
                                     calls_u=p['calls_u'], calls_d=p['calls_d'], rows=p['rows'], p95_u=mu, p95_d=md,
                                     measured_d_over_u=round(md / mu, 4), pred_d_over_u=p['pred_d_over_u'],
                                     measured_winner=win, rev4=p['rev4_winner'], fewer_calls=p['fewer_calls']))
            r['median_du'] = med(r.pop('du'))
            for k in tot:
                tot[k] += r[k]
            by_ds[ds] = r
    OUT.mkdir(parents=True, exist_ok=True)
    wcsv(OUT / 'e345_holdout_cases.csv', rows_out)
    long_w = [x for x in rows_out if int(x['window_days']) >= 80]
    find = [
        f"Holdout: TS2-U venceu {tot['wins_u']} e TS2-D {tot['wins_d']} de {tot['n']} casos temporais.",
        f"Acerto do vencedor: Rev4 {tot['rev4_hits']}/{tot['n']}, 'sempre U' {tot['always_u_hits']}/{tot['n']}, "
        f"'menos chamadas' {tot['fewer_calls_hits']}/{tot['n']}.",
        f"Erro absoluto mediano de ln(D/U) da Rev4: {statistics.median(abs_err):.3f} "
        f"(≈ ×{math.exp(statistics.median(abs_err)):.2f} na razão).",
        f"Partição média de U: {by_ds['h1']['u_partition_kb']:.0f} KB (21 d) → {by_ds['h3']['u_partition_kb']:.0f} KB (333 d); "
        f"mediana D/U {fmt(by_ds['h1']['median_du'])} → {fmt(by_ds['h3']['median_du'])}.",
    ]
    if long_w:
        find.append('Janelas longas (80 e 330 dias): mediana D/U ' +
                    fmt(statistics.median(x['measured_d_over_u'] for x in long_w)) +
                    f"; D venceu {sum(x['measured_winner'] == 'TS2-D' for x in long_w)} de {len(long_w)}.")
    save('e345', dict(by_dataset=by_ds, totals=tot, median_abs_log_error=round(statistics.median(abs_err), 4), findings=find))


# ---------------------------------------------------------------------------
def e6():
    out = []
    for ds in ('h1', 'h3'):
        idx = p95_index(audited('e6', ds))
        cases = sorted({k[0] for k in idx if k[0].split('_')[0] in TEMPORAL})
        ratio = {v: [] for v in ('ref', 'u', 'd')}
        du_w, du_c, res = [], [], []
        for c in cases:
            for v in ratio:
                ratio[v].append(float(idx[c, 'sst', 'cold', v]['p95_ms']) / float(idx[c, 'sst', 'warm', v]['p95_ms']))
                r = idx[c, 'sst', 'cold', v]
                if r['median_resident_bytes'] not in ('', None) and float(r['file_bytes'] or 0) > 0:
                    res.append(float(r['median_resident_bytes']) / float(r['file_bytes']))
            du_w.append(float(idx[c, 'sst', 'warm', 'd']['p95_ms']) / float(idx[c, 'sst', 'warm', 'u']['p95_ms']))
            du_c.append(float(idx[c, 'sst', 'cold', 'd']['p95_ms']) / float(idx[c, 'sst', 'cold', 'u']['p95_ms']))
        out.append(dict(dataset=ds, label=LABEL[ds], cold_over_warm={v: med(x) for v, x in ratio.items()},
                        warm_median_du=med(du_w), cold_median_du=med(du_c),
                        cold_wins_d=sum(x < 1 for x in du_c), n=len(cases),
                        resident_pct=100 * statistics.median(res) if res else float('nan')))
    find = [f"{r['label']}: frio/quente U {(r['cold_over_warm']['u'] - 1) * 100:+.0f}%, D {(r['cold_over_warm']['d'] - 1) * 100:+.0f}%; "
            f"D/U {fmt(r['warm_median_du'])} quente → {fmt(r['cold_median_du'])} frio; D venceu {r['cold_wins_d']}/{r['n']} no frio; "
            f"{r['resident_pct']:.0f}% dos bytes continuavam no page cache após a evicção (mmap)." for r in out]
    save('e6', dict(rows=out, findings=find))


if __name__ == '__main__':
    {'e1': e1, 'e345': e345, 'e6': e6}[sys.argv[1]]()
