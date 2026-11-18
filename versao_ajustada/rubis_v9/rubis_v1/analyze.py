"""Passo 5 - analise v1 do RUBiS: dois bracos (4.1.12 e 3.11.11) com os mesmos dados regenerados.

Mesma estatistica nos dois bracos: media (decisao), p50/p95/p99, IC bootstrap pareado (mesmos indices de
requisicao para todos os deployments) estratificado por repeticao, IC t entre repeticoes, piso de ruido
(A/A H01 x H01_AA e par de trabalho identico H01 x H02 em browsing), contrastes contra NoSE/QPG/V8,
tau/Top-1/regret dentro de H, Htop; e a comparacao entre versoes (tau por carga, razao 4.1/3.11, vereditos).
"""
from __future__ import annotations

import argparse
import gzip
import json
from collections import defaultdict
from pathlib import Path

import numpy as np

from common_paths import AA_ID, PREREG, ROOT, WORKLOADS, csv_w, dump, read
import slstats as S
import method_eval as M

B, SEED = 2000, 20261117
H_IDS = ['H01', 'H02', 'H03', 'H04', 'H05']
ARMS = ('v41', 'v311')
LABEL = {'v41': '4.1.12', 'v311': '3.11.11'}


def load_raw(files):
    rows, errors = [], 0
    for f in files:
        counter = defaultdict(int)
        with gzip.open(f, 'rt') as fh:
            for line in fh:
                r = json.loads(line)
                key = (r['workload'], r['repetition'])
                k = r.get('seq', counter[key]); counter[key] += 1
                rows.append({'variant': r['deployment_id'], 'query_id': r['workload'], 'rep': int(r['repetition']),
                             'round': k, 'latency_ms': r['latency_ms']})
                errors += bool(r.get('error'))
    return rows, errors


def boot_workload(cube, deps, w, B=B, seed=SEED):
    """Paired stratified bootstrap of the mean (same resampled request indices for every deployment), O(V*n) memory."""
    rng = np.random.default_rng(seed)
    reps = sorted(cube[deps[0]][w])
    arr = {r: np.vstack([cube[d][w][r] for d in deps]) for r in reps}
    samples = np.empty((B, len(deps)))
    total_n = sum(a.shape[1] for a in arr.values())
    for b in range(B):
        acc = np.zeros(len(deps))
        for r in reps:
            n = arr[r].shape[1]
            acc += arr[r][:, rng.integers(0, n, n)].sum(axis=1)
        samples[b] = acc / total_n
    point = {d: float(np.concatenate([cube[d][w][r] for r in reps]).mean()) for d in deps}
    return {'point': point, 'samples': {d: samples[:, i] for i, d in enumerate(deps)}}


def analyse_arm(cube, deps, pred, arm):
    summ, boots, floors, con, inside = [], {}, {}, [], {}
    for w in WORKLOADS:
        bt = boot_workload(cube, deps, w); boots[w] = bt
        for d in deps:
            allv = np.concatenate([cube[d][w][r] for r in sorted(cube[d][w])])
            ds = S.describe(allv); lo, hi = S.ci(bt['samples'][d]); rl = S.rep_level(cube, d, [w])
            summ.append({'arm': arm, 'deployment': d, 'workload': w, 'mean_ms': ds['mean_ms'], 'ci_lo': lo, 'ci_hi': hi,
                         'p50_ms': ds['p50_ms'], 'p95_ms': ds['p95_ms'], 'p99_ms': ds['p99_ms'], 'n': ds['n'],
                         't_ci_lo': rl['t_ci_lo'], 't_ci_hi': rl['t_ci_hi'], 'cv_rep_pct': rl['cv_between_reps_pct']})
        floors[w] = S.noise_floor(bt, [('H01', AA_ID)] + ([('H01', 'H02')] if w == 'browsing' else []))
        fl = floors[w]['noise_floor_abs_ln']
        for ref in ('NoSE', 'QPG', 'V8_AUTO_H_norm'):
            for d in H_IDS:
                pr = S.paired_ratio(bt, d, ref)
                con.append({'arm': arm, 'workload': w, 'a': d, 'b': ref, 'ratio': pr['ratio'], 'ci_lo': pr['ci_lo'],
                            'ci_hi': pr['ci_hi'], 'verdict': S.verdict(pr, fl)})
        m = {h: {'read': bt['point'][h], 'storage': 0, 'write': 0} for h in H_IDS}
        best = min(m, key=lambda h: m[h]['read'])
        tied = {best} | {h for h in H_IDS if h != best and S.verdict(S.paired_ratio(bt, h, best), fl) == 'EMPATE'}
        p = {h: pred[h][w]['total'] for h in H_IDS}
        ranked = sorted(H_IDS, key=lambda h: p[h])
        ev = M.evaluate(m, ranked, p, tied)
        inside[w] = {'predicted_order': ranked, 'measured_order': sorted(H_IDS, key=lambda h: m[h]['read']),
                     'tau': ev['kendall_tau_score_vs_read'], 'top1_strict': ranked[0] == best,
                     'top1_within_noise': ranked[0] in tied, 'regret_ms': m[ranked[0]]['read'] - m[best]['read'],
                     'tied_with_best': sorted(tied)}
    means = {(r['deployment'], r['workload']): r['mean_ms'] for r in summ}
    htop = [h for h in H_IDS if not any(all(means[(g, w)] <= means[(h, w)] for w in WORKLOADS) and
                                        any(means[(g, w)] < means[(h, w)] for w in WORKLOADS) for g in H_IDS if g != h)]
    return summ, floors, con, inside, htop, means


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True)
    ap.add_argument('--figs', default=str(ROOT / 'relatorio_overleaf/figs'))
    a = ap.parse_args()
    run = (ROOT / a.run) if not Path(a.run).is_absolute() else Path(a.run)
    out = run / 'analysis'; out.mkdir(exist_ok=True)
    reg = read(PREREG); man = read(run / 'manifest.json')
    deps, pred = reg['deployments_base_order'], reg['predicted_cost']
    res, errs, nraw = {}, {}, {}
    for arm in ARMS:
        raw, errs[arm] = load_raw(sorted((run / arm / 'raw').glob('*.jsonl.gz')))
        nraw[arm] = len(raw)
        res[arm] = analyse_arm(S.index(raw), deps, pred, arm)
        del raw
        print('braco', arm, 'analisado', flush=True)
    summ = res['v41'][0] + res['v311'][0]; con = res['v41'][2] + res['v311'][2]
    csv_w(out / 'summary.csv', summ); csv_w(out / 'contrasts.csv', con)
    # version comparison (same data, same protocol)
    m41, m311 = res['v41'][5], res['v311'][5]
    tau_v = {w: M.kendall_tau([m311[(d, w)] for d in deps], [m41[(d, w)] for d in deps]) for w in WORKLOADS}
    cmp_rows = [{'workload': w, 'deployment': d, 'v311_mean_ms': m311[(d, w)], 'v41_mean_ms': m41[(d, w)],
                 'ratio_41_over_311': m41[(d, w)] / m311[(d, w)]} for w in WORKLOADS for d in deps]
    csv_w(out / 'version_comparison.csv', cmp_rows)
    vd = {(c['arm'], c['workload'], c['a'], c['b']): c['verdict'] for c in con}
    verdict_same = {f'{w}/{h}/{b}': vd[('v41', w, h, b)] == vd[('v311', w, h, b)]
                    for w in WORKLOADS for h in H_IDS for b in ('NoSE', 'QPG', 'V8_AUTO_H_norm')}
    agr = man.get('agreement_with_v9_oracle', {})
    key3 = ('browsing', 'bidding', 'update10x')
    inside41 = res['v41'][3]
    hyp = {
        'H1': {'result': all(vd[('v41', w, 'H01', b)] == 'A_MAIS_RAPIDO' for w in key3 for b in ('NoSE', 'QPG')),
               'detail': '; '.join(f"{w}/{b}: {vd[('v41', w, 'H01', b)]}" for w in key3 for b in ('NoSE', 'QPG'))},
        'H2': {'result': all(t >= 0.7 for t in tau_v.values()) and all(vd[('v41', w, 'H01', b)] == vd[('v311', w, 'H01', b)] for w in WORKLOADS for b in ('NoSE', 'QPG')),
               'detail': 'tau 3.11x4.1: ' + ', '.join(f'{w}={t:.2f}' for w, t in tau_v.items()) +
                         f"; vereditos identicos (todos os H x baselines): {sum(verdict_same.values())}/{len(verdict_same)}"},
        'H3': {'result': all(inside41[w]['tau'] >= 0.6 for w in WORKLOADS), 'detail': ', '.join(f"{w}={inside41[w]['tau']:.2f}" for w in WORKLOADS)},
        'H4': {'result': sum(inside41[w]['top1_within_noise'] for w in WORKLOADS) >= 3,
               'detail': ', '.join(f"{w}={'sim' if inside41[w]['top1_within_noise'] else 'nao'}" for w in WORKLOADS)},
        'H5': {'result': bool(agr) and all(v['status'] == 'PASS' for x in agr.values() for v in x.values()),
               'detail': '; '.join(f"{LABEL[arm]}: {sum(v['agree'] for v in x.values())}/{sum(v['statement_results'] for v in x.values())}" for arm, x in agr.items())},
    }
    dump(out / 'hypotheses.json', hyp)
    dump(out / 'analysis.json', {arm: {'noise_floor': res[arm][1], 'within_H': res[arm][3], 'htop': res[arm][4]} for arm in ARMS} |
         {'tau_311_vs_41': tau_v, 'verdict_same': verdict_same, 'raw_rows': nraw, 'errors': errs, 'bootstrap_resamples': B})
    figures(Path(a.figs), summ, deps)
    for r in res['v41'][0]:
        o = next(x for x in res['v311'][0] if x['deployment'] == r['deployment'] and x['workload'] == r['workload'])
        print(f"{r['deployment']:>15} {r['workload']:>10} 4.1={r['mean_ms']:.3f} [{r['ci_lo']:.3f},{r['ci_hi']:.3f}]  3.11={o['mean_ms']:.3f}")
    print({k: v['result'] for k, v in hyp.items()}, 'tau', tau_v)


def figures(figdir, summ, deps):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    figdir.mkdir(parents=True, exist_ok=True)
    fig, axs = plt.subplots(1, 4, figsize=(12.5, 3.8), sharey=True)
    for ax, w in zip(axs, WORKLOADS):
        y = np.arange(len(deps))
        for k, (arm, col) in enumerate((('v41', '#3a6ea5'), ('v311', '#c47f2c'))):
            d = {r['deployment']: r for r in summ if r['workload'] == w and r['arm'] == arm}
            ax.barh(y + (k - 0.5) * 0.4, [d[x]['mean_ms'] for x in deps], 0.4, color=col, label=LABEL[arm],
                    xerr=[[d[x]['mean_ms'] - d[x]['ci_lo'] for x in deps], [d[x]['ci_hi'] - d[x]['mean_ms'] for x in deps]])
        ax.set_title(w); ax.set_yticks(y); ax.set_yticklabels(deps, fontsize=7); ax.invert_yaxis(); ax.set_xlabel('ms')
    axs[0].legend(fontsize=7)
    fig.tight_layout(); fig.savefig(figdir / 'fig_v1_rubis.pdf'); plt.close(fig)


if __name__ == '__main__':
    main()
