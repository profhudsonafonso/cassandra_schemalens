"""Passo 7 - fragmentos LaTeX do relatorio RUBiS v1 (dois bracos) a partir de <run>/analysis."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from common_paths import ARMS, FROZEN, PREREG, ROOT, WORKLOADS, read

LABEL = {'v41': '4.1.12', 'v311': '3.11.11'}


def f(x, d=3):
    return f'{float(x):.{d}f}'.replace('.', '{,}')


def esc(s):
    return str(s).replace('_', '\\_').replace('%', '\\%')


def rcsv(p):
    with open(p, newline='', encoding='utf8') as fh:
        return list(csv.DictReader(fh))


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True)
    a = ap.parse_args()
    run = (ROOT / a.run) if not Path(a.run).is_absolute() else Path(a.run)
    an = run / 'analysis'; T = ROOT / 'relatorio_overleaf/tables'; T.mkdir(parents=True, exist_ok=True)
    reg = read(PREREG); man = read(run / 'manifest.json'); ana = read(an / 'analysis.json')
    deps = reg['deployments_base_order']
    idx = {(r['arm'], r['deployment'], r['workload']): r for r in rcsv(an / 'summary.csv')}
    for arm in ARMS:
        L = [' & '.join([esc(d)] + [f"{f(idx[(arm, d, w)]['mean_ms'])} [{f(idx[(arm, d, w)]['ci_lo'])}; {f(idx[(arm, d, w)]['ci_hi'])}]" for w in WORKLOADS]) + '\\\\' for d in deps]
        (T / f't_mean_{arm}.tex').write_text('\n'.join(L) + '\n')
        L = [' & '.join([esc(d)] + [f"{f(idx[(arm, d, w)]['p50_ms'], 2)} / {f(idx[(arm, d, w)]['p95_ms'], 2)} / {f(idx[(arm, d, w)]['p99_ms'], 2)}" for w in WORKLOADS]) + '\\\\' for d in deps]
        (T / f't_pct_{arm}.tex').write_text('\n'.join(L) + '\n')
    vm = {'EMPATE': 'empate', 'A_MAIS_RAPIDO': '\\textbf{H+}', 'B_MAIS_RAPIDO': 'H$-$'}
    ci = {(c['arm'], c['workload'], c['a'], c['b']): c for c in rcsv(an / 'contrasts.csv')}
    L = []
    for h in ['H01', 'H02', 'H03', 'H04', 'H05']:
        for b in ('NoSE', 'QPG', 'V8_AUTO_H_norm'):
            cells = [h, esc(b)]
            for w in WORKLOADS:
                c41, c311 = ci[('v41', w, h, b)], ci[('v311', w, h, b)]
                cells.append(f"{f(c41['ratio'], 2)} {vm[c41['verdict']]} / {f(c311['ratio'], 2)} {vm[c311['verdict']]}")
            L.append(' & '.join(cells) + '\\\\')
    (T / 't_contrasts.tex').write_text('\n'.join(L) + '\n')
    L = []
    for arm in ARMS:
        for w in WORKLOADS:
            x = ana[arm]['within_H'][w]; nf = ana[arm]['noise_floor'][w]
            L.append(f"{LABEL[arm]} & {w} & {' < '.join(x['predicted_order'])} & {' < '.join(x['measured_order'])} & {f(x['tau'], 2)} & "
                     f"{'sim' if x['top1_within_noise'] else 'não'} & {f(x['regret_ms'])} & {f(nf['noise_floor_pct'], 1)}\\%\\\\")
    (T / 't_withinH.tex').write_text('\n'.join(L) + '\n')
    vc = {(r['deployment'], r['workload']): r for r in rcsv(an / 'version_comparison.csv')}
    L = [' & '.join([esc(d)] + [f(vc[(d, w)]['ratio_41_over_311'], 3) for w in WORKLOADS]) + '\\\\' for d in deps]
    L.append('\\midrule $\\tau$ (3.11 $\\times$ 4.1) & ' + ' & '.join(f(ana['tau_311_vs_41'][w], 2) for w in WORKLOADS) + '\\\\')
    (T / 't_versions.tex').write_text('\n'.join(L) + '\n')
    hy = read(an / 'hypotheses.json'); txt = reg['hypotheses']
    L = [f"{k} & {esc(txt[k])} & {'\\textbf{confirmada}' if v['result'] else 'rejeitada'} & {esc(str(v['detail'])[:330])}\\\\" for k, v in hy.items()]
    (T / 't_hyp.tex').write_text('\n'.join(L) + '\n')
    acc = read(an / 'acceptance.json')
    (T / 't_gates.tex').write_text('\n'.join(f"{esc(k)} & {'PASS' if v else 'FAIL'}\\\\" for k, v in acc['gates'].items()) + '\n')
    (T / 't_neg.tex').write_text('\n'.join(f"{esc(k)} & {'PASS' if v else 'FAIL'}\\\\" for k, v in acc['negative_tests'].items()) + '\n')
    tm = read(FROZEN / 'TRANSFER_MANIFEST.json')
    L = [f"{e} & {tm['cohort_counts'][e]:,} & {tm['v9_cohort_counts_for_reference'][e]:,}\\\\".replace(',', '.') for e in ('items', 'users', 'bids', 'comments', 'buynow')]
    L.append(f"usuários no estado (fecho) & {tm['rows_written']['users']:,} & 20.494\\\\".replace(',', '.'))
    (T / 't_cohort.tex').write_text('\n'.join(L) + '\n')
    pf = man['preflight']['v41']
    env = {'started': man['started_utc'][:16].replace('T', ' '), 'finished': man['finished_utc'][:16].replace('T', ' '),
           'python': pf['python'], 'driver': pf['driver'], 'cpuset': pf['cpuset'],
           'rawrows': f"{sum(ana['raw_rows'].values()):,}".replace(',', '.'),
           'discarded': str(sum(not x['valid'] for x in man['repetitions'])),
           'htopA': ', '.join(ana['v41']['htop']), 'htopB': ', '.join(ana['v311']['htop']),
           'oracleN': f"{read(FROZEN / 'ORACLE_REF_MANIFEST.json')['entries']:,}".replace(',', '.')}
    (T / 'env.tex').write_text('\n'.join(f'\\newcommand{{\\env{k}}}{{{v}}}' for k, v in env.items()) + '\n')
    print('tables written', T)


if __name__ == '__main__':
    main()
