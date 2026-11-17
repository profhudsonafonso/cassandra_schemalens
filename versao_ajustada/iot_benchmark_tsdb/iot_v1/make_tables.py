"""Passo 6 - gera os fragmentos LaTeX do relatorio a partir de <run>/analysis (numeros nunca digitados a mao)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from common_paths import PREREG, ROOT, read, read_csv


def f(x, d=3):
    return f'{float(x):.{d}f}'.replace('.', '{,}')


def pct(x, d=1):
    return f'{float(x) * 100:.{d}f}'.replace('.', '{,}') + '\\%'


def yn(b):
    return '\\textbf{sim}' if str(b) == 'True' or b is True else ('não' if str(b) in ('False',) or b is False else str(b))


def esc(s):
    return str(s).replace('_', '\\_').replace('%', '\\%')


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True, type=Path)
    a = ap.parse_args()
    run = (ROOT / a.run) if not a.run.is_absolute() else a.run
    an = run / 'analysis'; T = ROOT / 'relatorio_overleaf/tables'; T.mkdir(parents=True, exist_ok=True)
    reg = read(PREREG); man = read(run / 'manifest.json'); ana = read(an / 'analysis.json')
    vs = read_csv(an / 'variant_summary.csv'); dc = {r['variant']: r for r in read_csv(an / 'deployment_cost.csv')}
    hb = reg['hbench_ids_ranked']; vinfo = {v['label']: v for v in reg['variants']}
    rank1 = hb[0]
    # T1 mix
    lines = []
    for r in sorted(vs, key=lambda r: float(r['mix_mean_ms'])):
        v = vinfo[r['variant']]
        role = 'REF' if v['kind'] == 'baseline' else (f"HBench \\#{v['hbench_rank']}" if v['in_hbench'] else 'fora do HBench')
        lines.append(f"{esc(r['name'])} & {role} & {f(r['mix_mean_ms'])} & [{f(r['mix_mean_ci_lo'])}; {f(r['mix_mean_ci_hi'])}] & "
                     f"[{f(r['rep_t_ci_lo'])}; {f(r['rep_t_ci_hi'])}] & {f(r['mix_mean_p95_ms'])} & {f(r['pooled_p50_ms'])} & {f(r['pooled_p99_ms'])} & {f(r['cv_between_reps_pct'], 1)}\\\\")
    (T / 't_mix.tex').write_text('\n'.join(lines) + '\n')
    # T2 contrasts vs rank1 and REF-opt
    con = read_csv(an / 'contrasts.csv')
    vm = {'EMPATE': 'empate', 'A_MAIS_RAPIDO': '\\textbf{A mais rápido}', 'B_MAIS_RAPIDO': 'B mais rápido'}
    lines = [f"{esc(c['a_name'])} & {esc(c['b_name'])} & {f(c['ratio_a_over_b'])} & [{f(c['ci_lo'])}; {f(c['ci_hi'])}] & {vm[c['verdict']]}\\\\"
             for c in con]
    (T / 't_contrasts.tex').write_text('\n'.join(lines) + '\n')
    # T3 noise floor
    fl = ana['noise_floor']; names = {r['variant']: r['name'] for r in vs}
    lines = [f"{esc(names[p['a']])} $\\equiv$ {esc(names[p['b']])} & {f(p['ratio'], 4)} & [{f(p['ci_lo'], 4)}; {f(p['ci_hi'], 4)}] & {f(p['abs_ln_ci_hi'], 4)}\\\\"
             for p in fl['pairs']]
    (T / 't_noise.tex').write_text('\n'.join(lines) + '\n')
    (T / 'v_noise.tex').write_text(f"{f(fl['noise_floor_pct'], 1)}\\%")
    # T4 per-query (selected variants vs rank1)
    pq = read_csv(an / 'per_query_ratios.csv')
    qm = read_csv(an / 'query_metrics.csv')
    qs = [f'Q{i}' for i in range(1, 13)]
    mean_u = {r['query_id']: float(r['mean_ms']) for r in qm if r['variant'] == rank1}
    p95_u = {r['query_id']: float(r['p95_ms']) for r in qm if r['variant'] == rank1}
    sel = ['TS2-D', 'TS0', 'REF', 'REF-Q8opt']
    rat = {(r['a_name'], r['query_id']): r for r in pq}
    import math as _m
    aa_q = [r for r in pq if r['a_name'] == 'REF-Q8opt']
    floor_q = max(max(abs(_m.log(float(r['ci_lo']))), abs(_m.log(float(r['ci_hi'])))) for r in aa_q)
    (T / 'v_noise_q.tex').write_text(f"{f((_m.exp(floor_q) - 1) * 100, 1)}\\%")
    lines = []
    for q in qs:
        cells = [q, f(mean_u[q]), f(p95_u[q])]
        for n in sel:
            r = rat[(n, q)]
            sig = not (float(r['ci_lo']) <= 1 <= float(r['ci_hi'])) and abs(_m.log(float(r['ratio']))) >= floor_q
            cells.append(('\\textbf{' + f(r['ratio'], 2) + '}') if sig else f(r['ratio'], 2))
        lines.append(' & '.join(cells) + '\\\\')
    (T / 't_perquery.tex').write_text('\n'.join(lines) + '\n')
    # T5 cost / storage / write
    lines = []
    for r in sorted(vs, key=lambda r: float(r['mix_mean_ms'])):
        d = dc[r['variant']]
        kib = f"{int(d['storage_live_bytes']) / 1024:,.0f}".replace(',', '.')
        wr = f"{int(d['physical_writes']):,}".replace(',', '.')
        pred = '---' if not d['pred_storage_proxy'] else f"{float(d['pred_storage_proxy']) / 1024:,.0f}".replace(',', '.')
        lines.append(f"{esc(r['name'])} & {esc(d['tables'].replace('+', ', '))} & {kib} & {f(d['load_seconds'], 1)} & {wr} & {pred}\\\\")
    (T / 't_cost.tex').write_text('\n'.join(lines) + '\n')
    # T6 stage7 vs 9
    s79 = read_csv(an / 'stage7_vs_stage9.csv')
    nm = sum(r['status'] == 'MATCH' for r in s79)
    (T / 'v_s79.tex').write_text(f'{nm}/{len(s79)}')
    # T7 method eval
    ev = read(an / 'method_eval.json')
    nmz = ev['names']
    lines = [f"Espaço admissível medido & {ev['space_size']} deployments\\\\",
             f"HBench (congelado) & {ev['hbench_size']} ({', '.join(esc(nmz[h]) for h in hb)}); redução {pct(ev['reduction'], 0)}\\\\",
             f"Melhor medido (média do mix) & {esc(nmz[ev['measured_best']])} ({f(ev['measured_best_read_ms'])} ms)\\\\",
             f"Empatados com o melhor (piso de ruído) & {', '.join(esc(nmz[t]) for t in ev['tied_with_best'])}\\\\",
             f"Top-1 estrito / dentro do ruído & {yn(ev['top1_strict'])} / {yn(ev['top1_within_noise'])}\\\\",
             f"Top-3 & {yn(ev['top3'])}\\\\",
             f"Near-best 5\\% & {yn(ev['near_best_5pct'])}\\\\",
             f"Regret & {f(ev['regret'], 4)}\\\\",
             f"Fronteira de Pareto medida (leitura, bytes, escrita) & {', '.join(esc(nmz[p]) for p in ev['measured_pareto'])}\\\\",
             f"Retenção de Pareto & {pct(ev['pareto_retention'], 0)}\\\\",
             f"$\\tau$ de Kendall (escore do seletor $\\times$ média medida) & {f(ev['kendall_tau_score_vs_read'], 3)}\\\\",
             f"Aleatório $k={ev['random_3']['k']}$: regret esperado / P(contém o melhor) & {f(ev['random_3']['expected_regret'], 4)} / {f(ev['random_3']['p_contains_best'], 3)}\\\\"]
    (T / 't_method.tex').write_text('\n'.join(lines) + '\n')
    # T8 ablations
    ab = read_csv(an / 'ablations.csv')
    lines = []
    for r in ab:
        hbn = ', '.join(esc(nmz.get(h, h)) for h in r['hbench'].split('|') if h) or ('---' if r['kind'] != 'criterio' or not r['ablation'].startswith('aleat') else 'sorteio')
        reg_ = r['regret']
        regs = 'inviável' if reg_ == 'INFEASIBLE' else (f(reg_, 3) if reg_ not in ('', None) else '---')
        lines.append(f"{esc(r['ablation'])} & {hbn} & {regs} & {yn(r['near_best_5pct']) if r['near_best_5pct'] != '' else '---'} & {esc(r['detail'][:70]) if 'no-op' in r['detail'] else ''}\\\\")
    (T / 't_ablation.tex').write_text('\n'.join(lines) + '\n')
    # T9 legacy recomputed
    lp = an / 'legacy_B_recomputed.csv'
    if lp.exists():
        lg = read_csv(lp); v1 = {r['variant']: r for r in vs}
        lines = []
        for r in sorted(lg, key=lambda r: float(r['mix_mean_ms'])):
            n = v1.get(r['variant'], {})
            lines.append(f"{esc(r['name'])} & {f(r['mix_mean_p95_ms'])} & {f(r['mix_mean_ms'])} & [{f(r['ci_lo'])}; {f(r['ci_hi'])}] & "
                         f"{f(n['mix_mean_ms']) if n else '---'} & {f(n['mix_mean_p95_ms']) if n else '---'}\\\\")
        (T / 't_legacy.tex').write_text('\n'.join(lines) + '\n')
    # T10 hypotheses
    hy = read(an / 'hypotheses.json')
    txt = reg['hypotheses']
    lines = [f"{k} & {esc(txt[k])} & {'\\textbf{confirmada}' if v['result'] else 'rejeitada'} & {esc(v['detail'] if isinstance(v['detail'], str) else '; '.join(f'{a}: {b}' for a, b in v['detail'].items()))}\\\\"
             for k, v in hy.items()]
    (T / 't_hyp.tex').write_text('\n'.join(lines) + '\n')
    # T11 acceptance
    acc = read(an / 'acceptance.json')
    lines = [f"{esc(k)} & {'PASS' if v else 'FAIL'}\\\\" for k, v in acc['gates'].items()]
    (T / 't_gates.tex').write_text('\n'.join(lines) + '\n')
    lines = [f"{esc(k)} & {'PASS' if v else ('N/A' if v is None else 'FAIL')}\\\\" for k, v in acc['negative_tests'].items()]
    (T / 't_neg.tex').write_text('\n'.join(lines) + '\n')
    # environment values
    pf = man['preflight']
    reps = man['repetitions']
    env = {'version': pf['cassandra_release_version'], 'driver': pf['driver'], 'python': pf['python'],
           'cpuset': pf['cpuset'], 'mem': f"{pf['memory_limit_bytes'] / 2**30:.0f}", 'kernel': pf['kernel'],
           'rawrows': f"{ana['raw_rows']:,}".replace(',', '.'), 'windows': str(len(reps)),
           'discarded': str(sum(not r['isolation_guard']['passed'] for r in reps)),
           'winsec': f(sum(r['window_seconds'] for r in reps if r['isolation_guard']['passed']) / max(1, sum(r['isolation_guard']['passed'] for r in reps)), 0),
           'restart': f(sum(r['restart_seconds'] for r in reps) / len(reps), 0),
           'guardsamples': str(sum(r['isolation_guard']['samples'] for r in reps)),
           'started': man['preflight']['utc'][:16].replace('T', ' '), 'finished': man['finished_utc'][:16].replace('T', ' '),
           'others': str(len(pf['running_containers']) - 1), 'loadavg': f(pf['loadavg'][0], 2),
           'noise': (T / 'v_noise.tex').read_text(), 'noiseq': (T / 'v_noise_q.tex').read_text(), 'sseven': (T / 'v_s79.tex').read_text()}
    (T / 'env.tex').write_text('\n'.join(f'\\newcommand{{\\env{k}}}{{{v}}}' for k, v in env.items()) + '\n')
    print('tables written to', T)


if __name__ == '__main__':
    main()
