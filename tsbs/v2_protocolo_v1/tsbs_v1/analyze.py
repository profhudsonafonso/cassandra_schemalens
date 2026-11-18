"""Passo 3 - analise v1 do TSBS recalculada das latencias brutas (coortes natural = primaria, aligned = secundaria)."""
from __future__ import annotations

import argparse
import math
from collections import defaultdict
from pathlib import Path

import numpy as np

from common_paths import AA_SUFFIX, BASELINE_ID, PREREG, ROOT, SNAP, analysis_code_hashes, csv_x, dump, read, read_csv
import slstats as S
import method_eval as M

B, SEED = 2000, 20261117
SHORT = {'ts0_metric_raw': 'TS0', 'ts2_wide_bucket_1d': 'TS2', 'ts5_global_max_minute': 'TS5gm',
         'ts5_max_by_host_hour': 'TS5hh', 'ts6_hour_host_dimension': 'TS6', 'ts1_metric_bucket_1d': 'TS1',
         'series_bigint': 'TSBS-orig'}


def names(reg):
    out = {}
    for v in reg['variants']:
        base = '+'.join(SHORT.get(c, c) for c in v['candidates'])
        pre = {'conservative': 'C:', 'conditional_aligned': 'A:'}.get(v.get('profile'), '')
        if v['kind'] == 'aa_replica':
            pre = 'C:'
        out[v['label']] = pre + base + (' (A/A)' if v['kind'] == 'aa_replica' else '')
    return out


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True, type=Path)
    ap.add_argument('--figs', type=Path, default=ROOT / 'relatorio_overleaf/figs')
    a = ap.parse_args()
    run = (ROOT / a.run) if not a.run.is_absolute() else a.run
    out = run / 'analysis'; out.mkdir(exist_ok=True)
    reg = read(PREREG); man = read(run / 'manifest.json'); nm = names(reg)
    raw = read_csv(run / 'raw_latencies.csv')
    if any(r['oracle_ok'] != 'True' for r in raw):
        raise AssertionError('raw without oracle pass')
    cube = S.index(raw)
    labels = [v['label'] for v in reg['variants']]
    covers = [v for v in reg['variants'] if v['kind'] == 'cover']
    qN = sorted({r['query_id'] for r in raw if r['query_id'].startswith('N')})
    qA = sorted({r['query_id'] for r in raw if r['query_id'].startswith('A')})
    res = {}
    for coh, Q in (('natural', qN), ('aligned', qA)):
        boot = S.bootstrap_mix(cube, labels, Q, B=B, seed=SEED)
        boot95 = S.bootstrap_mix(cube, labels, Q, B=B, seed=SEED, stat='p95')
        floor = S.noise_floor(boot, [tuple(p) for p in reg['aa_pairs']])
        res[coh] = (boot, boot95, floor, Q)
    fallbacks = defaultdict(set)
    for rep in man['repetitions']:
        for f in rep.get('fallback_routes', []):
            l, ik, _ = f.split('|', 2); fallbacks[l].add(ik)
    load = {r['variant']: r for r in read_csv(run / 'load.csv')}
    stor = defaultdict(int)
    for r in read_csv(run / 'storage_tables.csv'):
        stor[r['keyspace']] += int(r['space_used_live_bytes'])
    ksof = {r['variant']: r['keyspace'] for r in load.values()}
    summ = []
    for v in labels:
        row = {'variant': v, 'name': nm[v]}
        for coh in ('natural', 'aligned'):
            boot, boot95, floor, Q = res[coh]
            lo, hi = S.ci(boot['samples'][v]); rl = S.rep_level(cube, v, Q)
            allv = np.concatenate([cube[v][q][r] for q in Q for r in cube[v][q]])
            row.update({f'{coh}_mix_mean_ms': boot['point'][v], f'{coh}_ci_lo': lo, f'{coh}_ci_hi': hi,
                        f'{coh}_mix_p95_ms': boot95['point'][v], f'{coh}_p50_ms': S.nearest_rank(allv, 50),
                        f'{coh}_p99_ms': S.nearest_rank(allv, 99), f'{coh}_cv_rep_pct': rl['cv_between_reps_pct']})
        row.update({'fallback_instances': len(fallbacks[v]), 'storage_bytes': stor[ksof[v]],
                    'load_seconds': float(load[v]['load_seconds']), 'physical_writes': int(load[v]['physical_writes'])})
        summ.append(row)
    csv_x(out / 'variant_summary.csv', summ, mode='w')
    sd = {s['variant']: s for s in summ}
    # per-template means (natural & aligned)
    pt = []
    for v in labels:
        for q in qN + qA:
            pooled = np.concatenate([cube[v][q][r] for r in sorted(cube[v][q])])
            d = S.describe(pooled)
            pt.append({'variant': v, 'name': nm[v], 'class': q, **{k: round(x, 4) if isinstance(x, float) else x for k, x in d.items()},
                       'cql_requests': ';'.join(sorted({r['cql_requests'] for r in raw if r['variant'] == v and r['query_id'] == q}))})
    csv_x(out / 'per_class.csv', pt, mode='w')
    hb = reg['hbench_ids_ranked']
    r1 = next(v['label'] for v in covers if v['profile'] == 'conservative' and v['hbench_rank'] == 1)
    con = []
    for coh in ('natural', 'aligned'):
        boot, _, floor, _ = res[coh]
        for ref in (r1, BASELINE_ID):
            for v in labels:
                if v == ref or v.endswith(AA_SUFFIX):
                    continue
                pr = S.paired_ratio(boot, v, ref)
                con.append({'cohort': coh, 'a': v, 'a_name': nm[v], 'b': ref, 'b_name': nm[ref], 'ratio': pr['ratio'],
                            'ci_lo': pr['ci_lo'], 'ci_hi': pr['ci_hi'], 'verdict': S.verdict(pr, floor['noise_floor_abs_ln'])})
    csv_x(out / 'contrasts.csv', con, mode='w')
    # method evaluation over the measured covers (HBench + Pareto-not-selected); per profile and overall
    meth = {}
    for coh in ('natural', 'aligned'):
        boot, _, floor, _ = res[coh]; fl = floor['noise_floor_abs_ln']
        measured = {v['label']: {'read': boot['point'][v['label']], 'storage': sd[v['label']]['storage_bytes'],
                                 'write': sd[v['label']]['load_seconds']} for v in covers}
        best = min(measured, key=lambda i: measured[i]['read'])
        tied = {best} | {i for i in measured if i != best and S.verdict(S.paired_ratio(boot, i, best), fl) == 'EMPATE'}
        score = {v['label']: v['score'] for v in covers}
        ranked = sorted(hb, key=lambda h: score[h])
        ev = M.evaluate(measured, ranked, score, tied)
        ev['names'] = {k: nm[k] for k in measured}
        ev['reference_space_note'] = 'espaco de referencia = coberturas medidas (HBench + Pareto nao selecionados); 16 dominadas nao medidas'
        meth[coh] = ev
    dump(out / 'method_eval.json', meth)
    dump(out / 'measured_space.json', {coh: {'measured': {v['label']: {'read': res[coh][0]['point'][v['label']]} for v in covers},
                                             'noise_floor_abs_ln': res[coh][2]['noise_floor_abs_ln']} for coh in res})
    # hypotheses
    cd = {(c['cohort'], c['a'], c['b']): c for c in con}
    cond = [v['label'] for v in covers if v['profile'] == 'conditional_aligned' and any(c.startswith(('ts5', 'ts6')) for c in v['candidates'])]
    h3 = [cd[('aligned', x, r1)]['verdict'] == 'A_MAIS_RAPIDO' for x in cond]
    gainA = {x: cd[('aligned', x, r1)]['ratio'] for x in cond}; gainN = {x: cd[('natural', x, r1)]['ratio'] for x in cond}
    hyp = {
        'H1': {'result': all(sd[h]['natural_mix_mean_ms'] > 0 for h in hb), 'detail': f'{len(hb)}/7 deployments do HBench validados pelo oraculo independente em todas as {len(qN) + len(qA)} classes; fallbacks TS0 por variante: ' + ', '.join(f"{nm[v]}={sd[v]['fallback_instances']}" for v in labels if sd[v]['fallback_instances'])},
        'H2': {'result': bool(set(hb) & set(meth['natural']['tied_with_best'])), 'detail': f"melhor={nm[meth['natural']['measured_best']]}; empatados={[nm[t] for t in meth['natural']['tied_with_best']]}"},
        'H3': {'result': all(h3), 'detail': f"{sum(h3)}/{len(h3)} condicionais mais rapidos que {nm[r1]} (aligned); razoes " + ', '.join(f'{nm[x]}={gainA[x]:.2f}' for x in cond)},
        'H4': {'result': all(gainN[x] >= gainA[x] for x in cond), 'detail': 'razoes natural: ' + ', '.join(f'{nm[x]}={gainN[x]:.2f}' for x in cond)},
        'H5': {'result': cd[('natural', BASELINE_ID, r1)]['verdict'] == 'B_MAIS_RAPIDO', 'detail': f"TSBS-orig/{nm[r1]} = {cd[('natural', BASELINE_ID, r1)]['ratio']:.3f} [{cd[('natural', BASELINE_ID, r1)]['ci_lo']:.3f}; {cd[('natural', BASELINE_ID, r1)]['ci_hi']:.3f}]"},
    }
    dump(out / 'hypotheses.json', hyp)
    dump(out / 'analysis.json', {'noise_floor': {c: res[c][2] for c in res}, 'rank1_conservative': r1, 'raw_rows': len(raw),
                                 'classes': {'natural': qN, 'aligned': qA}, 'bootstrap_resamples': B, 'seed': SEED,
                                 'analysis_code_sha256': analysis_code_hashes()})
    figures(a.figs, summ, pt, r1)
    for c in res:
        print(c, 'noise floor %:', res[c][2]['noise_floor_pct'])
    for s in sorted(summ, key=lambda s: s['natural_mix_mean_ms']):
        print(f"{s['name']:>34} N={s['natural_mix_mean_ms']:.2f} [{s['natural_ci_lo']:.2f},{s['natural_ci_hi']:.2f}] A={s['aligned_mix_mean_ms']:.2f} fb={s['fallback_instances']} KiB={s['storage_bytes']/1024:.0f} load={s['load_seconds']:.1f}s")
    print({k: v['result'] for k, v in hyp.items()})
    for c in meth:
        print(c, {k: meth[c][k] for k in ('measured_best', 'top1_within_noise', 'near_best_5pct', 'regret', 'pareto_retention', 'kendall_tau_score_vs_read')})


def figures(figdir, summ, pt, r1):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    figdir.mkdir(parents=True, exist_ok=True)
    order = sorted(summ, key=lambda s: s['natural_mix_mean_ms'])
    fig, ax = plt.subplots(figsize=(8, 4.2))
    y = np.arange(len(order)); h = 0.38
    for k, (coh, col) in enumerate((('natural', '#3a6ea5'), ('aligned', '#c47f2c'))):
        m = [s[f'{coh}_mix_mean_ms'] for s in order]
        ax.barh(y + (k - 0.5) * h, m, height=h, color=col, label=f'coorte {coh}')
        ax.errorbar(m, y + (k - 0.5) * h, xerr=[[mm - s[f'{coh}_ci_lo'] for mm, s in zip(m, order)], [s[f'{coh}_ci_hi'] - mm for mm, s in zip(m, order)]],
                    fmt='none', ecolor='k', capsize=2)
    ax.set_yticks(y); ax.set_yticklabels([s['name'] for s in order], fontsize=7); ax.invert_yaxis()
    ax.set_xscale('log'); ax.set_xlabel('Média do mix de 16 templates (ms, log), IC 95% bootstrap'); ax.legend(fontsize=8)
    fig.tight_layout(); fig.savefig(figdir / 'fig_v1_tsbs_mix.pdf'); plt.close(fig)


if __name__ == '__main__':
    main()
