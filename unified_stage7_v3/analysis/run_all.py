"""python -m unified_stage7_v3.analysis.run_all  ->  outputs_v3/<b>/analysis/* and docs/V3_RESULTS_COMPARISON.md
Retrospective only: nothing here feeds back into Stage 7/8 (the freeze is verified first)."""
from __future__ import annotations

import json

from .common import OUT, REPO, dump, read
from . import reuse, rubis, tsbs


def f(x, nd=3):
    return '—' if x is None else (f'{x:.{nd}f}' if isinstance(x, float) else str(x))


def split_files(b, ev3, ev2=None, abl=None, suffix=''):
    out = OUT / b / 'analysis'
    if suffix:
        out = out / suffix
    dump(out / 'preservation.json', {'v3': {k: ev3.get(k) for k in ('measured_best', 'tied_with_best', 'top1_strict', 'top1_within_noise', 'top3', 'near_best_5pct')},
                                     'v2_same_denominator': None if ev2 is None else {k: ev2.get(k) for k in ('top1_strict', 'top1_within_noise', 'top3', 'near_best_5pct')}})
    dump(out / 'pareto_retention.json', {'measured_pareto': ev3.get('measured_pareto'), 'v3': ev3.get('pareto_retention'),
                                         'v2_same_denominator': None if ev2 is None else ev2.get('pareto_retention')})
    dump(out / 'regret.json', {'v3': ev3.get('regret'), 'v2_same_denominator': None if ev2 is None else ev2.get('regret'),
                               'random_k': ev3.get('random_k')})
    dump(out / 'kendall_diagnostics.json', {'v3_balanced_structural_tiebreak_tau': ev3.get('kendall_tau_score_vs_read'),
                                            'legacy_v2_score_tau': None if ev2 is None else ev2.get('kendall_tau_score_vs_read'),
                                            'note': 'diagnostic only; never used to adjust V3'})
    if abl is not None:
        dump(out / 'ablations.json', abl)


def row(name, ev):
    return (f"| {name} | {ev['hbench_size']}/{ev['space_size']} | {f(ev['reduction'])} | {ev['top1_strict']} | {ev['top1_within_noise']} | {ev['top3']} | "
            f"{ev['near_best_5pct']} | {f(ev['regret'], 4)} | {f(ev['pareto_retention'])} | {f(ev['kendall_tau_score_vs_read'], 2)} |")


HEAD = '| seletor | HBench/espaço | DSR | Top-1 estrito | Top-1 (ruído) | Top-3 | near-best 5% | regret | retenção Pareto | τ (diag.) |\n|---|---|---|---|---|---|---|---|---|---|'


def abl_md(abl):
    L = ['| ablação | coorte/carga | |HBench| | status | Top-1 (ruído) | regret | retenção Pareto |', '|---|---|---|---|---|---|---|']
    for a in abl:
        L.append(f"| {a['ablation']} | {a.get('cohort', '')} | {a['hbench_size']} | {a['status']} | {a.get('top1_noise', '—')} | {f(a.get('regret'), 4)} | {f(a.get('pareto_retention'))} |")
    return '\n'.join(L)


def main():
    L = ['# V3 — Resultados e comparação V2 × V3', '',
         'Gerado por `python -m unified_stage7_v3.analysis.run_all` **depois** das medições; o freeze de cada benchmark é verificado antes. '
         'Kendall τ é diagnóstico retrospectivo. O modelo NNLS legado aparece só como `RETROSPECTIVE_BASELINE_ONLY`.', '',
         (REPO / 'unified_stage7_v3/analysis/discussion.md').read_text(), '', '# Resultados detalhados (gerados)', '']
    for b in ('iot', 'tpcx_iot'):
        res, rows, abl = reuse.run(b)
        split_files(b, res['v3'], res['v2_hbench_on_v3_denominator'], abl)
        L += [f'## {b} (medições V2 reutilizadas — D-V3-001)', '', f"Denominador: {res['denominator']}. Piso de ruído A/A: {f(res['noise_floor_pct'], 2)}%.", '', HEAD,
              row('V3 (Pareto + diversidade + balanced tiebreak)', res['v3']), row('V2 (HBench antigo, mesmo denominador)', res['v2_hbench_on_v3_denominator']), '',
              'V2 conforme relatado na V2 (denominador da V2): ' + json.dumps({k: res['v2_reported'].get(k) for k in ('space_size', 'hbench_size', 'top1_within_noise', 'regret', 'pareto_retention', 'kendall_tau_score_vs_read')}), '',
              '| deployment V3 | V2 | HBench V3 | HBench V2 | média (ms) | IC 95% | armaz. (B) | escrita (s) | empata c/ melhor |', '|---|---|---|---|---|---|---|---|---|']
        L += [f"| `{r['v3_deployment_id']}` | {r['v2_label']} | {r['in_hbench_v3']} | {r['in_hbench_v2']} | {r['mean_ms']:.3f} | [{r['ci_lo']:.3f}; {r['ci_hi']:.3f}] | {r['storage_bytes']:.0f} | {r['write_seconds']:.2f} | {r['tied_with_best']} |" for r in rows]
        L += ['', '### Ablações', '', abl_md(abl), '']
    (OUT / 'tsbs/benchmark/run1/manifest.json').exists() or (_ for _ in ()).throw(SystemExit('TSBS campaign not finished'))
    met, summ, abl = tsbs.run()
    for coh in met:
        split_files('tsbs', met[coh], None, abl, suffix=f'cohort_{coh}')
    L += ['## tsbs (campanha V3 nova — verificação completa)', '']
    for coh in ('natural', 'aligned'):
        ev = met[coh]
        L += [f"### coorte {coh} — piso A/A {f(ev['noise_floor_pct'], 2)}%; {ev['denominator']}", '', HEAD, row('V3', ev), '',
              f"Rank 1 V3 / TSBS original: {f(ev['rank1_vs_tsbs_original']['ratio'])} [{f(ev['rank1_vs_tsbs_original']['ci_lo'])}; {f(ev['rank1_vs_tsbs_original']['ci_hi'])}] → {ev['rank1_vs_tsbs_original']['verdict']}; "
              f"melhor do HBench ({ev['best_hbench_vs_tsbs_original']['deployment']}) / TSBS original: {f(ev['best_hbench_vs_tsbs_original']['ratio'])} → {ev['best_hbench_vs_tsbs_original']['verdict']}", '']
    L += ['| variante | deployment V3 | HBench V3 | natural (ms) | IC 95% | aligned (ms) | p95 nat. | armaz. (B) | carga (s) | escritas/s |', '|---|---|---|---|---|---|---|---|---|---|']
    for s in sorted(summ, key=lambda s: s['natural_mix_mean_ms']):
        L.append(f"| {s['variant']} | `{s['v3_deployment_id']}` | {s['in_hbench_v3']} | {s['natural_mix_mean_ms']:.2f} | [{s['natural_ci_lo']:.2f}; {s['natural_ci_hi']:.2f}] | "
                 f"{s['aligned_mix_mean_ms']:.2f} | {s['natural_p95_ms']:.2f} | {s['storage_bytes']} | {s['load_seconds']:.1f} | {s['writes_per_second']:.0f} |")
    L += ['', '### Ablações', '', abl_md(abl), '']
    if (OUT / 'rubis/benchmark/run1/manifest.json').exists():
        res, summ, con, repro, abl = rubis.run()
        dump(OUT / 'rubis/analysis/kendall_diagnostics.json', {k: {'v3_tau': v['v3_tiebreak_tau'], 'legacy_nnls_tau_RETROSPECTIVE_BASELINE_ONLY': v['legacy_nnls_RETROSPECTIVE_BASELINE_ONLY']['tau']}
                                                             for k, v in res['within_hbench'].items()})
        dump(OUT / 'rubis/analysis/ablations.json', abl)
        L += ['## rubis (campanha V3 nova — 4.1.12 principal, 3.11.11 secundária)', '', f"Denominador: {res['denominator']}.", '',
              '| braço/carga | melhor medido | empatados | ordem V3 | τ V3 | Top-1 V3 (ruído) | ordem NNLS legado (RETROSPECTIVE_BASELINE_ONLY) | τ NNLS | piso A/A % |', '|---|---|---|---|---|---|---|---|---|']
        for k, v in res['within_hbench'].items():
            lg = v['legacy_nnls_RETROSPECTIVE_BASELINE_ONLY']
            L.append(f"| {k} | {v['measured_best']} | {', '.join(v['tied_with_best'])} | {' '.join(v['v3_order'])} | {f(v['v3_tiebreak_tau'], 2)} | {v['v3_top1_noise']} | {' '.join(lg['order'])} | {f(lg['tau'], 2)} | {f(v['noise_floor_pct'], 2)} |")
        L += ['', 'Hipóteses: ' + '; '.join(f"**{k}** {'sim' if v['result'] else 'não'} — {v['detail']}" for k, v in res['hypotheses'].items()), '',
              '| braço | carga | deployment | média (ms) | IC 95% | p50 | p95 | p99 |', '|---|---|---|---|---|---|---|---|']
        L += [f"| {r['arm']} | {r['workload']} | {r['deployment']} | {r['mean_ms']:.3f} | [{r['ci_lo']:.3f}; {r['ci_hi']:.3f}] | {r['p50_ms']:.3f} | {r['p95_ms']:.3f} | {r['p99_ms']:.3f} |" for r in summ]
        L += ['', '### Reprodutibilidade entre sessões (mesmo deployment medido na V2 e na V3)', '', '| braço | carga | V3 | V2 | V3 (ms) | V2 (ms) | razão |', '|---|---|---|---|---|---|---|']
        L += [f"| {r['arm']} | {r['workload']} | {r['v3']} | {r['v2']} | {r['v3_mean_ms']:.3f} | {r['v2_mean_ms']:.3f} | {r['ratio_v3_over_v2_session']:.3f} |" for r in repro]
        L += ['', '### Ablações (RUBiS: só avaliáveis quando o HBench ablado foi medido)', '', abl_md(abl), '']
    else:
        L += ['## rubis', '', 'BLOCKED/PENDENTE: campanha ainda não concluída.', '']
    (REPO / 'docs/V3_RESULTS_COMPARISON.md').write_text('\n'.join(L) + '\n')
    print('written docs/V3_RESULTS_COMPARISON.md')


if __name__ == '__main__':
    main()
