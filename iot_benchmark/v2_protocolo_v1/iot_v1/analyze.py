"""Passo 3 - analise v1 recalculada das latencias brutas.

Saidas em <run>/analysis/: query_metrics.csv, variant_summary.csv, contrasts.csv,
per_query_ratios.csv, stage7_vs_stage9.csv, deployment_cost.csv, method_eval.json,
legacy_B_recomputed.csv, hypotheses.json, analysis.json e figuras em ../../relatorio_overleaf/figs.
"""
from __future__ import annotations

import argparse
import math
from collections import defaultdict
from pathlib import Path

import numpy as np

from common_paths import LABEL_OPT, LABEL_ORIG, PREREG, REV3, ROOT, SNAP, analysis_code_hashes, csv_x, dump, read, read_csv
import slstats as S
import method_eval as M

B = 2000
SEED = 20261117
SHORT = {}


def short_names(reg):
    names = {LABEL_ORIG: 'REF', LABEL_OPT: 'REF-Q8opt'}
    lab = {'ts0_iot_raw': 'TS0', 'ts2_iot_wide_unbucketed': 'TS2-U', 'ts2_iot_wide': 'TS2-D', 'ts4_iot_latest': 'TS4'}
    for v in reg['variants']:
        if v['kind'] == 'cover':
            names[v['label']] = '+'.join(lab[c] for c in v['candidates'])
    return names


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True, type=Path)
    ap.add_argument('--figs', type=Path, default=ROOT / 'relatorio_overleaf/figs')
    a = ap.parse_args()
    run = (ROOT / a.run) if not a.run.is_absolute() else a.run
    out = run / 'analysis'; out.mkdir(exist_ok=True)
    reg = read(PREREG); man = read(run / 'manifest.json')
    names = short_names(reg); SHORT.update(names)
    raw = read_csv(run / 'raw_latencies.csv')
    for r in raw:
        if r['oracle_ok'] != 'True':
            raise AssertionError('raw row without oracle pass')
    cube = S.index(raw)
    variants = [v['label'] for v in reg['variants']]
    covers = [v for v in reg['variants'] if v['kind'] == 'cover']
    queries = sorted({r['query_id'] for r in raw}, key=lambda q: int(q[1:]))

    # 1. per (variant, query)
    qm = []
    for v in variants:
        for q in queries:
            pooled = np.concatenate([cube[v][q][r] for r in sorted(cube[v][q])])
            qm.append({'variant': v, 'name': names[v], 'query_id': q, **{k: round(x, 6) if isinstance(x, float) else x
                                                                          for k, x in S.describe(pooled).items()}})
    csv_x(out / 'query_metrics.csv', qm, mode='w')

    # 2. mix metrics with bootstrap
    boot = S.bootstrap_mix(cube, variants, queries, B=B, seed=SEED, stat='mean')
    boot95 = S.bootstrap_mix(cube, variants, queries, B=B, seed=SEED, stat='p95')
    floor = S.noise_floor(boot, [tuple(p) for p in reg['aa_pairs']])
    fl = floor['noise_floor_abs_ln']
    summ = []
    for v in variants:
        lo, hi = S.ci(boot['samples'][v]); lo95, hi95 = S.ci(boot95['samples'][v])
        rl = S.rep_level(cube, v, queries)
        allv = np.concatenate([cube[v][q][r] for q in queries for r in cube[v][q]])
        summ.append({'variant': v, 'name': names[v], 'mix_mean_ms': boot['point'][v], 'mix_mean_ci_lo': lo, 'mix_mean_ci_hi': hi,
                     'mix_mean_p95_ms': boot95['point'][v], 'mix_p95_ci_lo': lo95, 'mix_p95_ci_hi': hi95,
                     'pooled_p50_ms': S.nearest_rank(allv, 50), 'pooled_p99_ms': S.nearest_rank(allv, 99),
                     'rep_t_ci_lo': rl['t_ci_lo'], 'rep_t_ci_hi': rl['t_ci_hi'], 'cv_between_reps_pct': rl['cv_between_reps_pct'],
                     'per_rep_mix_mean': ';'.join(f'{x:.4f}' for x in rl['per_rep'])})
    csv_x(out / 'variant_summary.csv', summ, mode='w')

    # 3. contrasts vs selector rank-1 and vs optimized reference
    rank1 = reg['hbench_ids_ranked'][0]
    con = []
    for ref in (rank1, LABEL_OPT):
        for v in variants:
            if v == ref:
                continue
            pr = S.paired_ratio(boot, v, ref)
            con.append({'a': v, 'a_name': names[v], 'b': ref, 'b_name': names[ref], 'ratio_a_over_b': pr['ratio'],
                        'ci_lo': pr['ci_lo'], 'ci_hi': pr['ci_hi'], 'verdict': S.verdict(pr, fl)})
    csv_x(out / 'contrasts.csv', con, mode='w')
    pq = []
    for v in variants:
        if v == rank1:
            continue
        for q in queries:
            r = S.per_query_ratio(cube, v, rank1, q, B=1000, seed=SEED)
            pq.append({'a_name': names[v], 'b_name': names[rank1], **r})
    csv_x(out / 'per_query_ratios.csv', pq, mode='w')

    # 4. Stage 7 x Stage 9 (calls/rows)
    catalog = {p['plan_id']: p for p in read(REV3 / 'stage5/15_executable_plan_catalog.json')}
    obs = defaultdict(set)
    for r in raw:
        obs[(r['variant'], r['query_id'])].add((int(r['execute_calls']), int(r['iterated_rows'])))
    s79 = []
    for v in covers:
        for q, pid in v['routes'].items():
            p = catalog[pid]; o = obs[(v['label'], q)]
            st = 'VARIABLE' if len(o) != 1 else ('MATCH' if next(iter(o)) == (p['request_count_proxy'], p['physical_rows_proxy']) else 'MISMATCH')
            s79.append({'deployment': v['label'], 'name': names[v['label']], 'query_id': q, 'plan_id': pid,
                        'pred_calls': p['request_count_proxy'], 'pred_rows': p['physical_rows_proxy'],
                        'obs': ';'.join(f'{c}/{r}' for c, r in sorted(o)), 'status': st})
    csv_x(out / 'stage7_vs_stage9.csv', s79, mode='w')

    # 5. storage and write per deployment
    load = {r['deployment']: r for r in read_csv(run / 'load.csv')}
    stor = read_csv(run / 'storage_tables.csv')
    ks_bytes = defaultdict(int); ks_tables = defaultdict(list)
    for r in stor:
        ks_bytes[r['keyspace']] += int(r['space_used_live_bytes']); ks_tables[r['keyspace']].append(r['table'])
    prefix = man['prefix']
    dc = []
    for v in reg['variants']:
        ks = prefix + v['keyspace_tail']; key = 'REFERENCE' if v['kind'] == 'baseline' else v['label']
        dc.append({'variant': v['label'], 'name': names[v['label']], 'storage_live_bytes': ks_bytes[ks],
                   'tables': '+'.join(sorted(ks_tables[ks])), 'load_seconds': float(load[key]['load_seconds']),
                   'physical_writes': int(load[key]['physical_writes']),
                   'pred_storage_proxy': v.get('analytic_cost', {}).get('storage'), 'pred_write_proxy': v.get('analytic_cost', {}).get('write')})
    csv_x(out / 'deployment_cost.csv', dc, mode='w')
    dcd = {d['variant']: d for d in dc}

    # 6. method evaluation in the admissible space
    measured = {v['label']: {'read': boot['point'][v['label']], 'storage': dcd[v['label']]['storage_live_bytes'],
                             'write': dcd[v['label']]['load_seconds']} for v in covers}
    best = min(measured, key=lambda i: measured[i]['read'])
    tied = {best} | {i for i in measured if i != best and S.verdict(S.paired_ratio(boot, i, best), fl) == 'EMPATE'}
    score = {v['label']: v['preselection_score'] for v in covers}
    ev = M.evaluate(measured, reg['hbench_ids_ranked'], score, tied)
    ev['random_3'] = M.random_k_expected(measured, len(reg['hbench_ids_ranked']))
    ev['names'] = {k: names[k] for k in measured}
    dump(out / 'method_eval.json', ev)
    dump(out / 'measured_space.json', {'measured': measured, 'tied_with_best': sorted(tied), 'noise_floor_abs_ln': fl})

    # 7. legacy campaign B raw latencies recomputed with v1 statistics (no new measurement)
    legacy = []
    lb = sorted((SNAP / 'outputs/rev3_independent_confirmatory').glob('batch_*/raw_latencies.csv'))
    if lb:
        lraw = [{'variant': r['variant'], 'query_id': r['query_id'], 'rep': 1, 'round': r['rep'], 'latency_ms': r['latency_ms']}
                for r in read_csv(lb[0])]
        lc = S.index(lraw); lv = sorted(lc)
        lboot = S.bootstrap_mix(lc, lv, queries, B=B, seed=SEED); lboot95 = S.bootstrap_mix(lc, lv, queries, B=B, seed=SEED, stat='p95')
        for v in lv:
            lo, hi = S.ci(lboot['samples'][v])
            legacy.append({'variant': v, 'name': names.get(v, v), 'mix_mean_ms': lboot['point'][v], 'ci_lo': lo, 'ci_hi': hi,
                           'mix_mean_p95_ms': lboot95['point'][v], 'source': str(lb[0].relative_to(SNAP))})
        csv_x(out / 'legacy_B_recomputed.csv', legacy, mode='w')

    # 8. hypotheses
    cd = {(c['a'], c['b']): c for c in con}
    u = rank1
    others_slower = [v['label'] for v in covers if v['canonical_source'] in ('ts0_iot_raw', 'ts2_iot_wide')]
    hyp = {
        'H1': {'result': u in tied, 'detail': f'melhor medido={names[best]}; empatados={[names[t] for t in sorted(tied)]}'},
        'H2': {'result': ev['near_best_5pct'], 'detail': f"regret={ev['regret']:.4f}"},
        'H3': {'result': cd[(u, LABEL_OPT)]['verdict'] == 'EMPATE' if (u, LABEL_OPT) in cd else None,
               'detail': f"razao={cd[(u, LABEL_OPT)]['ratio_a_over_b']:.4f} IC=[{cd[(u, LABEL_OPT)]['ci_lo']:.4f},{cd[(u, LABEL_OPT)]['ci_hi']:.4f}]" if (u, LABEL_OPT) in cd else ''},
        'H4': {'result': all(cd[(x, u)]['verdict'] == 'B_MAIS_RAPIDO' for x in others_slower),
               'detail': {names[x]: f"{cd[(x, u)]['ratio_a_over_b']:.3f} [{cd[(x, u)]['ci_lo']:.3f},{cd[(x, u)]['ci_hi']:.3f}] {cd[(x, u)]['verdict']}" for x in others_slower}},
        'H5': {'result': all(r['status'] == 'MATCH' for r in s79), 'detail': f"{sum(r['status']=='MATCH' for r in s79)}/{len(s79)} MATCH"},
    }
    dump(out / 'hypotheses.json', hyp)
    dump(out / 'analysis.json', {'noise_floor': floor, 'rank1': rank1, 'best': best, 'queries': queries,
                                 'raw_rows': len(raw), 'reps': sorted({int(r['rep']) for r in raw}),
                                 'bootstrap_resamples': B, 'seed': SEED, 'analysis_code_sha256': analysis_code_hashes()})
    figures(a.figs, summ, pq, dc, names, rank1, fl)
    print('noise floor %:', floor['noise_floor_pct'])
    for s_ in summ:
        print(f"{s_['name']:>14} mean={s_['mix_mean_ms']:.3f} [{s_['mix_mean_ci_lo']:.3f},{s_['mix_mean_ci_hi']:.3f}]  p95mix={s_['mix_mean_p95_ms']:.3f}  cvrep={s_['cv_between_reps_pct']}")
    print({k: v['result'] for k, v in hyp.items()})
    print({k: ev[k] for k in ('measured_best', 'top1_strict', 'top1_within_noise', 'top3', 'near_best_5pct', 'regret', 'pareto_retention', 'kendall_tau_score_vs_read')})


def figures(figdir, summ, pq, dc, names, rank1, fl):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    figdir.mkdir(parents=True, exist_ok=True)
    order = sorted(summ, key=lambda s: s['mix_mean_ms'])
    fig, ax = plt.subplots(figsize=(7, 3.4))
    y = np.arange(len(order))
    ax.barh(y, [s['mix_mean_ms'] for s in order], color=['#2a7f62' if s['variant'] == rank1 else '#7a8fa6' for s in order])
    ax.errorbar([s['mix_mean_ms'] for s in order], y, xerr=[[s['mix_mean_ms'] - s['mix_mean_ci_lo'] for s in order],
                                                          [s['mix_mean_ci_hi'] - s['mix_mean_ms'] for s in order]], fmt='none', ecolor='k', capsize=3)
    ax.set_yticks(y); ax.set_yticklabels([s['name'] for s in order]); ax.invert_yaxis()
    ax.set_xlabel('Média do mix uniforme Q1–Q12 (ms), IC 95% bootstrap')
    fig.tight_layout(); fig.savefig(figdir / 'fig_v1_mix.pdf'); plt.close(fig)
    # per-query ratio vs rank1
    qs = sorted({r['query_id'] for r in pq}, key=lambda q: int(q[1:]))
    sel = [n for n in ('TS2-D', 'TS0', 'REF', 'REF-Q8opt') if any(r['a_name'] == n for r in pq)]
    fig, ax = plt.subplots(figsize=(7.2, 3.4))
    w = 0.8 / max(1, len(sel))
    for i, n in enumerate(sel):
        rs = {r['query_id']: r for r in pq if r['a_name'] == n}
        x = np.arange(len(qs)) + i * w
        ax.bar(x, [rs[q]['ratio'] for q in qs], width=w, label=n)
    ax.axhline(1, color='k', lw=0.8)
    ax.axhspan(math.exp(-fl), math.exp(fl), color='grey', alpha=0.2, label='piso de ruído')
    ax.set_xticks(np.arange(len(qs)) + 0.4 - w / 2); ax.set_xticklabels(qs)
    ax.set_yscale('log'); ax.set_ylabel(f'razão de médias vs {names[rank1]}'); ax.legend(fontsize=7, ncol=3)
    fig.tight_layout(); fig.savefig(figdir / 'fig_v1_perquery.pdf'); plt.close(fig)


if __name__ == '__main__':
    main()
