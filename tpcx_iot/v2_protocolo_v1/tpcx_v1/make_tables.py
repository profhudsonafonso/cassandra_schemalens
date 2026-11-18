"""Passo 6 - fragmentos LaTeX do relatorio TPCx-IoT a partir de <run>/analysis."""
from __future__ import annotations

import argparse
import math
from pathlib import Path

from common_paths import PREREG, ROOT, read, read_csv


def f(x, d=3):
    return f'{float(x):.{d}f}'.replace('.', '{,}')


def n0(x):
    return f'{float(x):,.0f}'.replace(',', '.')


def esc(s):
    return str(s).replace('_', '\\_').replace('%', '\\%').replace('&', '\\&')


def yn(b):
    return '\\textbf{sim}' if b in (True, 'True') else ('não' if b in (False, 'False') else '---')


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True, type=Path)
    a = ap.parse_args()
    run = (ROOT / a.run) if not a.run.is_absolute() else a.run
    an = run / 'analysis'; T = ROOT / 'relatorio_overleaf/tables'; T.mkdir(parents=True, exist_ok=True)
    reg = read(PREREG); man = read(run / 'manifest.json'); ana = read(an / 'analysis.json')
    vs = read_csv(an / 'variant_summary.csv'); vi = {v['label']: v for v in reg['variants']}
    nm = {r['variant']: r['name'] for r in vs}
    L = []
    for r in sorted(vs, key=lambda r: float(r['read_mean_ms'])):
        v = vi[r['variant']]
        role = {'baseline': 'baseline TPC', 'aa_replica': 'réplica A/A'}.get(v['kind'], f"HBench \\#{v['hbench_rank']}" if v.get('in_hbench') else 'fora do HBench')
        L.append(f"{esc(r['name'])} & {role} & {f(r['read_mean_ms'])} & [{f(r['read_mean_ci_lo'])}; {f(r['read_mean_ci_hi'])}] & {f(r['read_p50_ms'])} & {f(r['read_p95_ms'])} & {f(r['read_p99_ms'])} & {r['cql_requests_per_read']} & {f(r['ingest_eps_mean'], 0)} & {n0(float(r['storage_bytes_mean']) / 1024)}\\\\")
    (T / 't_summary.tex').write_text('\n'.join(L) + '\n')
    fl = ana['noise_floor']
    L = [f"{esc(nm[p['a']])} $\\equiv$ {esc(nm[p['b']])} & {f(p['ratio'], 4)} & [{f(p['ci_lo'], 4)}; {f(p['ci_hi'], 4)}] & {f(p['abs_ln_ci_hi'], 4)}\\\\" for p in fl['pairs']]
    (T / 't_noise.tex').write_text('\n'.join(L) + '\n')
    L = [f"{esc(nm[x['a']])} $\\equiv$ {esc(nm[x['b']])} & {f(x['ingest_ratio'], 4)} & {f(x['storage_ratio'], 4)}\\\\" for x in ana['ingest_aa']]
    (T / 't_noise_ing.tex').write_text('\n'.join(L) + '\n')
    vm = {'EMPATE': 'empate', 'A_MAIS_RAPIDO': '\\textbf{A melhor}', 'B_MAIS_RAPIDO': 'B melhor'}
    L = [f"{esc(c['a_name'])} & {esc(c['b_name'])} & {f(c['read_ratio'])} & [{f(c['ci_lo'])}; {f(c['ci_hi'])}] & {vm[c['read_verdict']]} & {f(c['ingest_ratio'])} & {vm[c['ingest_verdict']]} & {f(c['storage_ratio'], 2)}\\\\" for c in read_csv(an / 'contrasts.csv')]
    (T / 't_contrasts.tex').write_text('\n'.join(L) + '\n')
    s79 = read_csv(an / 'stage7_vs_stage9.csv')
    L = [f"{esc(r['name'])} & {r['pred_read_cql_requests']} & {r['obs_cql_requests_per_read']} & {r['pred_inserts_per_event']} & {r['obs_inserts_per_event']}\\\\" for r in s79]
    (T / 't_s79.tex').write_text('\n'.join(L) + '\n')
    ev = read(an / 'method_eval.json'); N = ev['names']; hb = reg['hbench_ids_ranked']
    L = [f"Espaço admissível medido & {ev['space_size']} deployments\\\\",
         f"HBench congelado & {ev['hbench_size']}: {', '.join(esc(N[h]) for h in hb)} (redução {f(ev['reduction'] * 100, 1)}\\%)\\\\",
         f"Melhor medido (média de leitura) & {esc(N[ev['measured_best']])} ({f(ev['measured_best_read_ms'])} ms)\\\\",
         f"Empatados com o melhor (piso de ruído) & {', '.join(esc(N[t]) for t in ev['tied_with_best'])}\\\\",
         f"Top-1 estrito / dentro do ruído & {yn(ev['top1_strict'])} / {yn(ev['top1_within_noise'])}\\\\",
         f"Top-3 / near-best 5\\% & {yn(ev['top3'])} / {yn(ev['near_best_5pct'])}\\\\",
         f"Regret (média de leitura) & {f(ev['regret'], 4)}\\\\",
         f"Pareto medido 3D (leitura, bytes, tempo de ingestão) & {', '.join(esc(N[p]) for p in ev['measured_pareto'])}\\\\",
         f"Retenção de Pareto 3D & {f(ev['pareto_retention'] * 100, 0)}\\%\\\\",
         f"Pareto medido 2D (leitura, ingestão; critério da Etapa 9.1) & {', '.join(esc(N[p]) for p in ev['measured_pareto_read_write'])}\\\\",
         f"Retenção de Pareto 2D & {f(ev['pareto_retention_read_write'] * 100, 0)}\\%\\\\",
         f"$\\tau$ de Kendall (escore $\\times$ média de leitura) & {f(ev['kendall_tau_score_vs_read'], 3)}\\\\",
         f"Aleatório $k={ev['random_k']['k']}$: regret esperado / P(contém o melhor) & {f(ev['random_k']['expected_regret'], 4)} / {f(ev['random_k']['p_contains_best'], 3)}\\\\"]
    (T / 't_method.tex').write_text('\n'.join(L) + '\n')
    L = []
    for r in read_csv(an / 'ablations.csv'):
        ids = [x for x in r['hbench'].split('|') if x]
        hbn = ', '.join(esc(N.get(x, x)) for x in ids) or ('sorteio' if r['ablation'].startswith('aleat') else '---')
        rg = 'inviável' if r['regret'] == 'INFEASIBLE' else (f(r['regret'], 3) if r['regret'] else '---')
        d = r['detail']
        if 'no-op' in d: d = 'regra não ativa: sem efeito'
        elif 'mesmos candidatos' in d: d = 'ativação perdida; mesmos candidatos'
        elif 'familias perdidas' in d:
            d = 'perde ' + d.split('familias perdidas=')[1].split(';')[0].strip("[]'") + ('; controle ausente' if 'controle' in d else '')
        L.append(f"{esc(r['ablation'])} & {hbn} & {rg} & {yn(r['near_best_5pct']) if r['near_best_5pct'] else '---'} & {esc(d)}\\\\")
    (T / 't_ablation.tex').write_text('\n'.join(L) + '\n')
    lp = an / 'legacy_stage91.csv'
    if lp.exists():
        L = [f"{esc(r['name'])} & {f(r['old_median_p95_ms'])} & {f(r['old_median_ingest_eps'], 0)} & {n0(int(r['old_storage']) / 1024)} & {f(r['new_mean_ms'])} & {f(r['new_p95_ms'])} & {f(r['new_ingest_eps'], 0)} & {n0(float(r['new_storage']) / 1024)}\\\\"
             for r in sorted(read_csv(lp), key=lambda r: float(r['new_mean_ms']))]
        (T / 't_legacy.tex').write_text('\n'.join(L) + '\n')
    hy = read(an / 'hypotheses.json'); txt = reg['hypotheses']
    L = [f"{k} & {esc(txt[k])} & {'\\textbf{confirmada}' if v['result'] else 'rejeitada'} & {esc(v['detail'])}\\\\" for k, v in hy.items()]
    (T / 't_hyp.tex').write_text('\n'.join(L) + '\n')
    acc = read(an / 'acceptance.json')
    (T / 't_gates.tex').write_text('\n'.join(f"{esc(k)} & {'PASS' if v else 'FAIL'}\\\\" for k, v in acc['gates'].items()) + '\n')
    (T / 't_neg.tex').write_text('\n'.join(f"{esc(k)} & {'PASS' if v else 'FAIL'}\\\\" for k, v in acc['negative_tests'].items()) + '\n')
    pf = man['preflight']; runs = man['runs']
    env = {'version': pf['cassandra_release_version'], 'driver': pf['driver'], 'python': pf['python'], 'cpuset': pf['cpuset'],
           'mem': f"{pf['memory_limit_bytes'] / 2**30:.0f}", 'rawreads': n0(ana['raw_reads']), 'runs': str(ana['runs_accepted']),
           'discarded': str(ana['runs_total'] - ana['runs_accepted']), 'started': pf['utc'][:16].replace('T', ' '),
           'finished': man['finished_utc'][:16].replace('T', ' '), 'others': str(len(pf['running_containers']) - 1),
           'noise': f"{f(fl['noise_floor_pct'], 1)}\\%", 'noiseing': f"{f((math.exp(ana['ingest_noise_floor_abs_ln']) - 1) * 100, 1)}\\%",
           'guardsamples': str(sum(r['isolation_guard']['samples'] for r in runs))}
    (T / 'env.tex').write_text('\n'.join(f'\\newcommand{{\\env{k}}}{{{v}}}' for k, v in env.items()) + '\n')
    print('tables written', T)


if __name__ == '__main__':
    main()
