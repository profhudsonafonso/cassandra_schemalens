"""Gera tables/*.tex e figs/*.pdf do artigo V3 a partir de outputs_v3/ (nada é digitado à mão).
.venv/bin/python methodology/artigos/v3_unified/build_tables.py
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
import sys
sys.path.insert(0, str(HERE.parents[1]))           # methodology/ (pacote unified_stage7_v3)
from unified_stage7_v3.layout import OUT as O, REPO, V2  # noqa: E402  (<benchmark>/v3_unified e <benchmark>/v2_protocolo_v1)
T = HERE / 'tables'; F = HERE / 'figs'
T.mkdir(exist_ok=True); F.mkdir(exist_ok=True)
BENCH = {'iot': 'IoT-Benchmark', 'tpcx_iot': 'TPCx-IoT', 'tsbs': 'TSBS cpu-only', 'rubis': 'RUBiS'}
SHORT = {'ts0_iot_raw': 'TS0', 'ts2_iot_wide_unbucketed': 'TS2-U', 'ts2_iot_wide': 'TS2-D', 'ts4_iot_latest': 'TS4',
         'ts0_series_raw': 'TS0', 'ts1_series_3600s': 'TS1-3600', 'ts1_series_60s': 'TS1-60', 'ts3_sensor_3600s': 'TS3-3600',
         'ts3_sensor_60s': 'TS3-60', 'ts0_metric_raw': 'TS0', 'ts2_wide_bucket_1d': 'TS2', 'ts5_global_max_minute': 'TS5gm',
         'ts5_max_by_host_hour': 'TS5hh', 'ts5_sum_count_by_host_hour': 'TS5sc', 'ts6_hour_host_dimension': 'TS6',
         'ts1_metric_bucket_1d': 'TS1-1d', 'ts1_metric_bucket_7d': 'TS1-7d', 'series_bigint': 'TSBS-orig'}


def rj(p):
    return json.loads(Path(p).read_text())


def rc(p):
    with Path(p).open() as f:
        return list(csv.DictReader(f))


def esc(s):
    return str(s).replace('_', r'\_').replace('%', r'\%').replace('&', r'\&').replace('#', r'\#')


def n(x, d=2):
    if x is None or x == '':
        return '---'
    x = float(x)
    return f'{x:,.{d}f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def pct(x, d=1):
    return '---' if x is None else n(100 * float(x), d) + r'\%'


def yn(b):
    return 'sim' if b in (True, 'True') else ('não' if b in (False, 'False') else '---')


def short_tables(tables):
    return '+'.join(SHORT.get(t, t) for t in (tables.split('+') if isinstance(tables, str) else tables))


def write(name, s):
    (T / name).write_text(s)


def pre(b, f):
    return rj(O / b / 'prebenchmark' / f)


vec = {b: {r['deployment_id']: r for r in pre(b, '05_deployment_vectors.json')} for b in BENCH}
label = {}
for b in BENCH:
    for i, r in vec[b].items():
        label[(b, i)] = short_tables(r['tables']) if b != 'rubis' else i
rubis_reg = rj(O / 'rubis/PREREGISTRATION_V3.json')
for x in rubis_reg['hbench_v3_schemas']:
    label[('rubis', x['v3_deployment_id'])] = x['schema_id']

# ------------------------------------------------------------------ 1. espaço e decisão de medição
rows = []
for b in BENCH:
    comp = pre(b, '06_complete_deployments.json'); hb = pre(b, '09_hbench_v3.json'); pf = pre(b, '07_pareto_front.json')
    cmp_ = rc(O / b / 'analysis/v2_vs_v3_hbench.csv')
    v2 = {r['v2_id'] for r in cmp_ if r['in_v2_hbench'] == 'True'}
    inter = sum(1 for r in cmp_ if r['in_v2_hbench'] == 'True' and r['v3_id'])
    jac = inter / (len(v2) + len(hb) - inter)
    reg = rj(O / b / 'PREREGISTRATION_V3.json')
    dec = 'reuso V2' if reg['measurement_decision'] == 'REUSE_V2_MEASUREMENTS' else 'nova campanha'
    rows.append(f"{BENCH[b]} & {len(pre(b, '04_candidate_footprints.json'))} & {n(comp['enumerated'], 0)} & {comp['complete']} & {len(pf)} & "
                f"{len(v2)} & {len(hb)} & {inter} & {n(jac, 2)} & {dec} \\\\")
write('space.tex', '\n'.join(rows) + '\n')

# ------------------------------------------------------------------ 2. HBench V3 de cada benchmark (vetor físico)
for b in BENCH:
    out = []
    for h in pre(b, '09_hbench_v3.json'):
        a = h['axes']
        lab = label[(b, h['deployment_id'])]
        if b == 'rubis':
            extra = [t for t in h['tables'] if t not in rj(O / 'rubis/V3_PREBENCHMARK_SCENARIO.json')['mandatory_tables']]
            lab = f"{lab} ({', '.join(extra)})"
        out.append(f"{h['rank']} & {esc(lab)} & {n(a['read_cql_requests'], 2)} & {n(a['read_logical_rows'], 1)} & {n(a['read_logical_bytes'], 0)} & "
                   f"{n(a['write_cql_mutations'], 2 if b == 'rubis' else 0)} & {n(a['maintenance_read_requests'], 2)} & {n(a['logical_storage_bytes'] / 1024, 0)} & "
                   f"{n(h['balanced_structural_tiebreak'], 3)} \\\\")
    write(f'hbench_{b}.tex', '\n'.join(out) + '\n')

# ------------------------------------------------------------------ 3. quadro V2 x V3
def mrow(name, e, size=None):
    hs = size or f"{e['hbench_size']}/{e['space_size']}"
    return (f"{name} & {hs} & {pct(e['reduction'], 0)} & {yn(e['top1_strict'])} & {yn(e['top1_within_noise'])} & {yn(e['top3'])} & "
            f"{pct(e['regret'], 2)} & {pct(e['pareto_retention'], 0)} & {n(e['kendall_tau_score_vs_read'], 2)} \\\\")


L = []
for b in ('iot', 'tpcx_iot'):
    m = rj(O / b / 'analysis/metrics.json')
    L.append(r'\multicolumn{9}{l}{\textit{' + BENCH[b] + r' --- medições V2 reutilizadas; piso A/A ' + n(m['noise_floor_pct'], 2) + r'\%}}\\')
    L.append(mrow('V2 (mesmo denominador)', m['v2_hbench_on_v3_denominator']))
    L.append(mrow(r'\textbf{V3}', m['v3']))
mt = rj(O / 'tsbs/analysis/metrics.json')
v2t = rj(V2['tsbs'] / 'outputs/v1_run1/analysis/method_eval.json')
for coh in ('natural', 'aligned'):
    L.append(r'\multicolumn{9}{l}{\textit{TSBS, coorte ' + coh + r' --- piso A/A V3 ' + n(mt[coh]['noise_floor_pct'], 2) + r'\%}}\\')
    L.append(mrow('V2 (campanha V2)', v2t[coh]))
    L.append(mrow(r'\textbf{V3} (campanha V3)', mt[coh]))
write('metrics.tex', '\n'.join(L) + '\n')

# ------------------------------------------------------------------ 4. IoT e TPCx: espaço medido
for b in ('iot', 'tpcx_iot'):
    out = []
    for r in rc(O / b / 'analysis/v2_vs_v3_results.csv'):
        lab = label[(b, r['v3_deployment_id'])]
        out.append(f"{esc(lab)} & {'$\\bullet$' if r['in_hbench_v3'] == 'True' else ''} & {'$\\bullet$' if r['in_hbench_v2'] == 'True' else ''} & "
                   f"{n(r['mean_ms'], 3)} & [{n(r['ci_lo'], 3)}; {n(r['ci_hi'], 3)}] & {n(float(r['storage_bytes']) / 1024, 0)} & {n(r['write_seconds'], 2)} & "
                   f"{'sim' if r['tied_with_best'] == 'True' else ''} & {n(r['balanced_structural_tiebreak'], 3)} \\\\")
    write(f'space_{b}.tex', '\n'.join(out) + '\n')

# ------------------------------------------------------------------ 5. TSBS: 26 variantes
vs = sorted(rc(O / 'tsbs/analysis/variant_summary.csv'), key=lambda s: float(s['natural_mix_mean_ms']))
_v3of = {s['variant']: s['v3_deployment_id'] for s in vs}
for s in vs:
    if s['variant'].endswith('_aa'):
        s['v3_deployment_id'] = _v3of[s['variant'][:-3]]
out = []
for s in vs:
    did = s['v3_deployment_id']
    lab = label.get(('tsbs', did), 'TSBS-orig' if 'series' in did else did)
    if s['variant'].endswith('_aa'):
        lab = lab + ' (A/A)'
    out.append(f"{esc(lab)} & {'$\\bullet$' if s['in_hbench_v3'] == 'True' else ''} & {n(s['natural_mix_mean_ms'])} & [{n(s['natural_ci_lo'])}; {n(s['natural_ci_hi'])}] & "
               f"{n(s['natural_p95_ms'])} & {n(s['aligned_mix_mean_ms'])} & [{n(s['aligned_ci_lo'])}; {n(s['aligned_ci_hi'])}] & "
               f"{n(int(s['storage_bytes']) / 1024, 0)} & {n(s['load_seconds'], 1)} \\\\")
write('tsbs_variants.tex', '\n'.join(out) + '\n')

# ------------------------------------------------------------------ 6. RUBiS
rs = rc(O / 'rubis/analysis/summary.csv')
deps = ['V3H01', 'V3H02', 'V3H03', 'V3H04', 'V3H05', 'NoSE', 'QPG', 'V8_AUTO_H_norm', 'V3H01_AA']
W = ('browsing', 'bidding', 'update10x', 'update100x')
for arm in ('v41', 'v311'):
    d = {(r['deployment'], r['workload']): r for r in rs if r['arm'] == arm}
    out = []
    for dep in deps:
        cells = ' & '.join(f"{n(d[(dep, w)]['mean_ms'], 3)} & {n(d[(dep, w)]['p95_ms'], 2)}" for w in W)
        out.append(f"{esc(dep.replace('V8_AUTO_H_norm', 'V8-norm'))} & {cells} \\\\")
    write(f'rubis_{arm}.tex', '\n'.join(out) + '\n')
mr = rj(O / 'rubis/analysis/metrics.json')
out = []
for arm in ('v41', 'v311'):
    for w in W:
        v = mr['within_hbench'][f'{arm}/{w}']; lg = v['legacy_nnls_RETROSPECTIVE_BASELINE_ONLY']
        out.append(f"{'4.1.12' if arm == 'v41' else '3.11.11'} & {w} & {v['measured_best']} & {len(v['tied_with_best'])} & {n(v['noise_floor_pct'], 1)}\\% & "
                   f"{n(v['v3_tiebreak_tau'], 2)} & {yn(v['v3_top1_noise'])} & {pct(v['v3_rank1_regret'], 1)} & {' '.join(x[3:] for x in lg['order'])} & "
                   f"{n(lg['tau'], 2)} & {yn(lg['top1_noise'])} \\\\")
write('rubis_within.tex', '\n'.join(out) + '\n')
con = rc(O / 'rubis/analysis/contrasts.csv')
out = []
for arm in ('v41', 'v311'):
    for w in W:
        best = mr['within_hbench'][f'{arm}/{w}']['measured_best']
        cells = []
        for bse in ('NoSE', 'QPG', 'V8_AUTO_H_norm'):
            c = next(x for x in con if x['arm'] == arm and x['workload'] == w and x['a'] == 'V3H01' and x['b'] == bse)
            v = {'A_MAIS_RAPIDO': r'$\downarrow$', 'B_MAIS_RAPIDO': r'$\uparrow$', 'EMPATE': '='}[c['verdict']]
            cells.append(f"{n(c['ratio'], 3)} [{n(c['ci_lo'], 3)}; {n(c['ci_hi'], 3)}] {v}")
        out.append(f"{'4.1.12' if arm == 'v41' else '3.11.11'} & {w} & " + ' & '.join(cells) + r' \\')
write('rubis_contrasts.tex', '\n'.join(out) + '\n')
rep = rc(O / 'rubis/analysis/v2_vs_v3_results.csv')
out = []
for r in rep:
    if r['arm'] != 'v41':
        continue
    out.append(f"{r['workload']} & {esc(r['v3'].replace('V8_AUTO_H_norm', 'V8-norm'))} & {esc(r['v2'].replace('V8_AUTO_H_norm', 'V8-norm'))} & {n(r['v3_mean_ms'], 3)} & {n(r['v2_mean_ms'], 3)} & {n(r['ratio_v3_over_v2_session'], 3)} \\\\")
write('rubis_repro.tex', '\n'.join(out) + '\n')

# ------------------------------------------------------------------ 7. ablações
NAMES = {'full_v3': 'V3 completo', 'without_read_rows': 'sem linhas lidas', 'without_read_bytes': 'sem bytes lidos',
         'without_request_count': 'sem nº de requisições', 'without_write_mutations': 'sem mutações', 'without_storage': 'sem armazenamento',
         'without_max_partition_safety': 'sem segurança de partição', 'with_risk_axes_in_dominance': 'com eixos de risco na dominância',
         'without_structural_diversity': 'sem diversidade', 'without_pareto': 'sem Pareto', 'top_k_1': '$k=1$', 'random_k': 'aleatório $k$'}
ab = {'iot': rc(O / 'iot/analysis/ablations.csv'), 'tpcx_iot': rc(O / 'tpcx_iot/analysis/ablations.csv')}
tab = rc(O / 'tsbs/analysis/ablations.csv')
ab['tsbs_n'] = [r for r in tab if r['cohort'] == 'natural']; ab['tsbs_a'] = [r for r in tab if r['cohort'] == 'aligned']
out = []
for k in NAMES:
    cells = []
    for b in ('iot', 'tpcx_iot', 'tsbs_n', 'tsbs_a'):
        r = next(x for x in ab[b] if x['ablation'] == k)
        cells.append(f"{r['hbench_size']} & {pct(r['regret'], 1)} & {pct(r['pareto_retention'], 0)}" if r['status'] == 'EVALUATED' else r'\multicolumn{3}{c}{n/a}')
    out.append(f"{NAMES[k]} & " + ' & '.join(cells) + r' \\')
write('ablations.tex', '\n'.join(out) + '\n')

# ------------------------------------------------------------------ 8. macros com números do texto
mi, mtp = rj(O / 'iot/analysis/metrics.json'), rj(O / 'tpcx_iot/analysis/metrics.json')
tsb = rj(O / 'tsbs/analysis/run_facts.json')
agr = mr['hypotheses']['ORACLE']['detail']
macros = {
    'tsbsRankOneNat': n(mt['natural']['rank1_vs_tsbs_original']['ratio'], 3),
    'tsbsRankOneNatLo': n(mt['natural']['rank1_vs_tsbs_original']['ci_lo'], 3),
    'tsbsRankOneNatHi': n(mt['natural']['rank1_vs_tsbs_original']['ci_hi'], 3),
    'tsbsRankOneAli': n(mt['aligned']['rank1_vs_tsbs_original']['ratio'], 3),
    'tsbsFloorNat': n(mt['natural']['noise_floor_pct'], 1), 'tsbsFloorAli': n(mt['aligned']['noise_floor_pct'], 1),
    'tsbsTiedNat': str(len(mt['natural']['tied_with_best'])), 'tsbsRaw': n(tsb['raw_rows'], 0),
    'iotFloor': n(mi['noise_floor_pct'], 2), 'tpcxFloor': n(mtp['noise_floor_pct'], 2),
    'tpcxTied': str(len(mtp['v3']['tied_with_best'])),
    'rubisOracle': esc(agr), 'rubisTauVer': ', '.join(f'{w} {n(t, 2)}' for w, t in mr['tau_311_vs_41'].items()),
}
write('macros.tex', '\n'.join(rf'\newcommand{{\{k}}}{{{v}}}' for k, v in macros.items()) + '\n')

# ------------------------------------------------------------------ 9. figuras
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(7.2, 6.4))
covers = [s for s in vs if 'series' not in s['v3_deployment_id']]
y = range(len(vs))
for k, (coh, col) in enumerate((('natural', '#3a6ea5'), ('aligned', '#c47f2c'))):
    m = [float(s[f'{coh}_mix_mean_ms']) for s in vs]
    lo = [m[i] - float(s[f'{coh}_ci_lo']) for i, s in enumerate(vs)]; hi = [float(s[f'{coh}_ci_hi']) - m[i] for i, s in enumerate(vs)]
    ax.barh([i + (k - 0.5) * 0.4 for i in y], m, 0.4, color=col, xerr=[lo, hi], ecolor='k', capsize=1.5, label=f'coorte {coh}')
labs = []
for s in vs:
    did = s['v3_deployment_id']; lab = label.get(('tsbs', did), 'TSBS-orig')
    lab = lab + (' (A/A)' if s['variant'].endswith('_aa') else '') + ('  [H]' if s['in_hbench_v3'] == 'True' else '')
    labs.append(lab)
ax.set_yticks(list(y)); ax.set_yticklabels(labs, fontsize=6.5); ax.invert_yaxis()
for t, s in zip(ax.get_yticklabels(), vs):
    if s['in_hbench_v3'] == 'True':
        t.set_fontweight('bold')
ax.set_xlabel('Média do mix de 16 templates (ms), IC 95% bootstrap'); ax.legend(fontsize=8, loc='lower right')
fig.tight_layout(); fig.savefig(F / 'tsbs_v3.pdf'); plt.close(fig)

fig, axs = plt.subplots(1, 4, figsize=(12.5, 3.6), sharey=True)
for ax, w in zip(axs, W):
    for k, (arm, col) in enumerate((('v41', '#3a6ea5'), ('v311', '#c47f2c'))):
        d = {r['deployment']: r for r in rs if r['workload'] == w and r['arm'] == arm}
        m = [float(d[x]['mean_ms']) for x in deps]
        ax.barh([i + (k - 0.5) * 0.4 for i in range(len(deps))], m, 0.4, color=col, label={'v41': '4.1.12', 'v311': '3.11.11'}[arm],
                xerr=[[m[i] - float(d[x]['ci_lo']) for i, x in enumerate(deps)], [float(d[x]['ci_hi']) - m[i] for i, x in enumerate(deps)]])
    ax.set_title(w); ax.set_yticks(range(len(deps))); ax.set_yticklabels([x.replace('V8_AUTO_H_norm', 'V8-norm') for x in deps], fontsize=7)
    ax.invert_yaxis(); ax.set_xlabel('ms por requisição')
axs[0].legend(fontsize=7)
fig.tight_layout(); fig.savefig(F / 'rubis_v3.pdf'); plt.close(fig)

fig, axs = plt.subplots(1, 4, figsize=(12.5, 3.2))
panels = [('iot', None), ('tpcx_iot', None), ('tsbs', 'natural'), ('tsbs', 'aligned')]
for ax, (b, coh) in zip(axs, panels):
    if b in ('iot', 'tpcx_iot'):
        pts = [(float(r['balanced_structural_tiebreak']), float(r['mean_ms']), r['in_hbench_v3'] == 'True') for r in rc(O / b / 'analysis/v2_vs_v3_results.csv')]
        ttl = BENCH[b]
    else:
        tb = pre('tsbs', '11_selection_trace.json')['tiebreak']
        pts = [(tb[s['v3_deployment_id']], float(s[f'{coh}_mix_mean_ms']), s['in_hbench_v3'] == 'True') for s in covers if not s['variant'].endswith('_aa')]
        ttl = f'TSBS ({coh})'
    for x, yv, h in pts:
        ax.scatter(x, yv, s=26, c='#c0392b' if h else '#7f8c8d', marker='o' if h else 'x')
    ax.set_title(ttl, fontsize=9); ax.set_xlabel('balanced_structural_tiebreak', fontsize=8); ax.set_ylabel('média medida (ms)', fontsize=8)
    ax.tick_params(labelsize=7)
fig.tight_layout(); fig.savefig(F / 'tiebreak_vs_measured.pdf'); plt.close(fig)
print('tabelas:', sorted(p.name for p in T.glob('*.tex')))
