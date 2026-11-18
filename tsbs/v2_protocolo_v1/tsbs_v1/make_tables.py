"""Passo 6 - fragmentos LaTeX do relatorio TSBS a partir de <run>/analysis."""
from __future__ import annotations

import argparse
from pathlib import Path

from common_paths import PREREG, ROOT, read, read_csv


def f(x, d=2):
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
    for r in sorted(vs, key=lambda r: float(r['natural_mix_mean_ms'])):
        v = vi[r['variant']]
        role = {'baseline': 'baseline TSBS', 'aa_replica': 'réplica A/A'}.get(v['kind']) or (f"HBench \\#{v['hbench_rank']}" if v['in_hbench'] else 'Pareto, fora (top-k)')
        L.append(f"{esc(r['name'])} & {role} & {f(r['natural_mix_mean_ms'])} & [{f(r['natural_ci_lo'])}; {f(r['natural_ci_hi'])}] & {f(r['natural_mix_p95_ms'])} & "
                 f"{f(r['aligned_mix_mean_ms'])} & [{f(r['aligned_ci_lo'])}; {f(r['aligned_ci_hi'])}] & {r['fallback_instances']} & {n0(int(r['storage_bytes']) / 1024)} & {f(r['load_seconds'], 1)}\\\\")
    (T / 't_summary.tex').write_text('\n'.join(L) + '\n')
    L = []
    for c in ('natural', 'aligned'):
        for p in ana['noise_floor'][c]['pairs']:
            L.append(f"{c} & {esc(nm[p['a']])} $\\equiv$ {esc(nm[p['b']])} & {f(p['ratio'], 4)} & [{f(p['ci_lo'], 4)}; {f(p['ci_hi'], 4)}] & {f(ana['noise_floor'][c]['noise_floor_pct'], 1)}\\%\\\\")
    (T / 't_noise.tex').write_text('\n'.join(L) + '\n')
    vm = {'EMPATE': 'empate', 'A_MAIS_RAPIDO': '\\textbf{A mais rápido}', 'B_MAIS_RAPIDO': 'B mais rápido'}
    L = [f"{c['cohort']} & {esc(c['a_name'])} & {esc(c['b_name'])} & {f(c['ratio'], 3)} & [{f(c['ci_lo'], 3)}; {f(c['ci_hi'], 3)}] & {vm[c['verdict']]}\\\\"
         for c in read_csv(an / 'contrasts.csv')]
    (T / 't_contrasts.tex').write_text('\n'.join(L) + '\n')
    pc = read_csv(an / 'per_class.csv')
    r1 = ana['rank1_conservative']
    show = [r1, 'B_tsbs_series_bigint'] + [v['label'] for v in reg['variants'] if v['kind'] == 'cover' and v.get('hbench_rank') == 1 and v.get('profile') == 'conditional_aligned']
    idx = {(r['variant'], r['class']): r for r in pc}
    L = []
    for q in sorted({r['class'][1:] for r in pc}):
        cells = [q]
        for v in show:
            cells += [f(idx[(v, 'N' + q)]['mean_ms']), f(idx[(v, 'A' + q)]['mean_ms'])]
        cells.append(idx[(r1, 'N' + q)]['cql_requests'])
        L.append(' & '.join(cells) + '\\\\')
    (T / 't_perclass.tex').write_text('\n'.join(L) + '\n')
    (T / 'perclass_head.tex').write_text(' & '.join(f'\\multicolumn{{2}}{{c}}{{{esc(nm[v])}}}' for v in show))
    meth = read(an / 'method_eval.json'); hb = reg['hbench_ids_ranked']
    L = []
    for c in ('natural', 'aligned'):
        ev = meth[c]; N = ev['names']
        L += [f"\\multicolumn{{2}}{{l}}{{\\textbf{{Coorte {c}}}}}\\\\",
              f"Melhor medido & {esc(N[ev['measured_best']])} ({f(ev['measured_best_read_ms'])} ms)\\\\",
              f"Empatados com o melhor & {', '.join(esc(N[t]) for t in ev['tied_with_best'])}\\\\",
              f"HBench contém o melhor ou empatado & {yn(bool(set(ev['tied_with_best']) & set(hb)))}\\\\",
              f"Near-best 5\\% / regret & {yn(ev['near_best_5pct'])} / {f(ev['regret'], 4)}\\\\",
              f"Pareto medido (leitura, bytes, carga) & {', '.join(esc(N[p]) for p in ev['measured_pareto'])}\\\\",
              f"Retenção de Pareto & {f(ev['pareto_retention'] * 100, 0)}\\%\\\\",
              f"$\\tau$ de Kendall (escore $\\times$ leitura) & {f(ev['kendall_tau_score_vs_read'], 3)}\\\\"]
    (T / 't_method.tex').write_text('\n'.join(L) + '\n')
    L = []
    for r in read_csv(an / 'ablations.csv'):
        ids = [x for x in (r.get('hbench') or '').split('|') if x]
        un = len([x for x in (r.get('unmeasured') or '').split('|') if x])
        def rg(c):
            v = r.get(f'{c}_regret', '')
            if v in ('', None): return '---'
            if v in ('INVIAVEL', 'SEM_MEMBRO_MEDIDO'): return 'n/d'
            return ('$\\le$' if r.get(f'{c}_upper_bound') == 'True' else '') + f(v, 3)
        d = r['detail']
        d = 'não ativa (no-op)' if 'no-op' in d else d.replace("perde ", "perde ").replace("'", '')
        L.append(f"{esc(r['ablation'])} & {r['hbench_size']} & {un} & {yn(r.get('changed_vs_frozen')) if r.get('changed_vs_frozen') else '---'} & {rg('natural')} & {rg('aligned')} & {esc(d[:55])}\\\\")
    (T / 't_ablation.tex').write_text('\n'.join(L) + '\n')
    hy = read(an / 'hypotheses.json'); txt = reg['hypotheses']
    L = [f"{k} & {esc(txt[k])} & {'\\textbf{confirmada}' if v['result'] else 'rejeitada'} & {esc(v['detail'][:300])}\\\\" for k, v in hy.items()]
    (T / 't_hyp.tex').write_text('\n'.join(L) + '\n')
    acc = read(an / 'acceptance.json')
    (T / 't_gates.tex').write_text('\n'.join(f"{esc(k)} & {'PASS' if v else 'FAIL'}\\\\" for k, v in acc['gates'].items()) + '\n')
    (T / 't_neg.tex').write_text('\n'.join(f"{esc(k)} & {'PASS' if v else 'FAIL'}\\\\" for k, v in acc['negative_tests'].items()) + '\n')
    pf = man['preflight']; reps = man['repetitions']; d = reg['data']
    env = {'version': pf['cassandra_release_version'], 'driver': pf['driver'], 'python': pf['python'], 'cpuset': pf['cpuset'],
           'rawrows': n0(ana['raw_rows']), 'started': pf['utc'][:16].replace('T', ' '), 'finished': man['finished_utc'][:16].replace('T', ' '),
           'discarded': str(sum(not r['isolation_guard']['passed'] for r in reps)),
           'winsec': f(sum(r['window_seconds'] for r in reps if r['isolation_guard']['passed']) / 5, 0),
           'obs': n0(d['observations']), 'cells': n0(d['metric_cells']), 'hosts': str(d['hosts']),
           'datasha': d['raw_generator_sha256'][:16], 'noiseN': f(ana['noise_floor']['natural']['noise_floor_pct'], 1) + '\\%',
           'noiseA': f(ana['noise_floor']['aligned']['noise_floor_pct'], 1) + '\\%'}
    (T / 'env.tex').write_text('\n'.join(f'\\newcommand{{\\env{k}}}{{{v}}}' for k, v in env.items()) + '\n')
    print('tables written', T)


if __name__ == '__main__':
    main()
