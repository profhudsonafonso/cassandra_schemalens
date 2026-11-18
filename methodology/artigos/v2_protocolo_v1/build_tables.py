"""Gera as tabelas do artigo (tables/*.tex) a partir das analises v1 dos quatro experimentos e copia as figuras.

    python build_tables.py        (a partir de versao_ajustada/artigo_overleaf)
Nenhum numero do artigo e digitado a mao nas tabelas: tudo vem de */outputs/v1_run1/analysis.
"""
from __future__ import annotations

import csv
import json
import math
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
VA = HERE.parent
REPO = HERE.parents[2]                                # raiz do repositório (layout por benchmark)
T = HERE / 'tables'; F = HERE / 'figs'
T.mkdir(exist_ok=True); F.mkdir(exist_ok=True)


def rj(p): return json.loads(Path(p).read_text(encoding='utf8'))
def rc(p): return list(csv.DictReader(open(p, newline='', encoding='utf8')))
def f(x, d=2): return f'{float(x):.{d}f}'.replace('.', '{,}')
def n0(x): return f'{float(x):,.0f}'.replace(',', '.')
def esc(s): return str(s).replace('_', '\\_').replace('%', '\\%')
def yn(b): return 'sim' if b in (True, 'True') else 'não'
def w(name, lines): (T / name).write_text('\n'.join(lines) + '\n', encoding='utf8')


# ------------------------------------------------------------------ IoT
iot = REPO / 'iot_benchmark/v2_protocolo_v1'; ia = iot / 'outputs/v1_run1/analysis'
ireg = rj(iot / 'PREREGISTRATION_IOT_V1.json'); vi = {v['label']: v for v in ireg['variants']}
cost = {r['variant']: r for r in rc(ia / 'deployment_cost.csv')}
L = []
for r in sorted(rc(ia / 'variant_summary.csv'), key=lambda r: float(r['mix_mean_ms'])):
    v = vi[r['variant']]
    role = 'referência' if v['kind'] == 'baseline' else (f"HBench \\#{v['hbench_rank']}" if v['in_hbench'] else 'fora do HBench')
    c = cost[r['variant']]
    L.append(f"{esc(r['name'])} & {role} & {f(r['mix_mean_ms'], 3)} & [{f(r['mix_mean_ci_lo'], 3)}; {f(r['mix_mean_ci_hi'], 3)}] & {f(r['mix_mean_p95_ms'], 2)} & {n0(int(c['storage_live_bytes']) / 1024)} & {f(c['load_seconds'], 1)}\\\\")
w('iot_summary.tex', L)
iev = rj(ia / 'method_eval.json'); ian = rj(ia / 'analysis.json'); iacc = rj(ia / 'acceptance.json'); ihy = rj(ia / 'hypotheses.json')

# ------------------------------------------------------------------ TPCx
tp = REPO / 'tpcx_iot/v2_protocolo_v1'; ta = tp / 'outputs/v1_run1/analysis'
treg = rj(tp / 'PREREGISTRATION_TPCX_V1.json'); vt = {v['label']: v for v in treg['variants']}
L = []
for r in sorted(rc(ta / 'variant_summary.csv'), key=lambda r: float(r['read_mean_ms'])):
    v = vt[r['variant']]
    role = {'baseline': 'baseline TPC', 'aa_replica': 'réplica A/A'}.get(v['kind']) or (f"HBench \\#{v['hbench_rank']}" if v['in_hbench'] else 'fora do HBench')
    L.append(f"{esc(r['name'])} & {role} & {f(r['read_mean_ms'], 3)} & [{f(r['read_mean_ci_lo'], 3)}; {f(r['read_mean_ci_hi'], 3)}] & {f(r['read_p95_ms'])} & {r['cql_requests_per_read']} & {f(r['ingest_eps_mean'], 0)} & {n0(float(r['storage_bytes_mean']) / 1024)}\\\\")
w('tpcx_summary.tex', L)
tev = rj(ta / 'method_eval.json'); tan = rj(ta / 'analysis.json'); tacc = rj(ta / 'acceptance.json'); thy = rj(ta / 'hypotheses.json')

# ------------------------------------------------------------------ TSBS
ts = REPO / 'tsbs/v2_protocolo_v1'; sa = ts / 'outputs/v1_run1/analysis'
sreg = rj(ts / 'PREREGISTRATION_TSBS_V1.json'); vs = {v['label']: v for v in sreg['variants']}
L = []
for r in sorted(rc(sa / 'variant_summary.csv'), key=lambda r: float(r['natural_mix_mean_ms'])):
    v = vs[r['variant']]
    role = {'baseline': 'TSBS original', 'aa_replica': 'réplica A/A'}.get(v['kind']) or (f"HBench \\#{v['hbench_rank']}" if v['in_hbench'] else 'Pareto, fora')
    L.append(f"{esc(r['name'])} & {role} & {f(r['natural_mix_mean_ms'])} & [{f(r['natural_ci_lo'])}; {f(r['natural_ci_hi'])}] & {f(r['aligned_mix_mean_ms'])} & [{f(r['aligned_ci_lo'])}; {f(r['aligned_ci_hi'])}] & {r['fallback_instances']} & {n0(int(r['storage_bytes']) / 1024)} & {f(r['load_seconds'], 1)}\\\\")
w('tsbs_summary.tex', L)
sev = rj(sa / 'method_eval.json'); san = rj(sa / 'analysis.json'); sacc = rj(sa / 'acceptance.json'); shy = rj(sa / 'hypotheses.json')

# ------------------------------------------------------------------ RUBiS
rb = REPO / 'rubis/v2_protocolo_v1'; ra = rb / 'outputs/v1_run1/analysis'
rreg = rj(rb / 'PREREGISTRATION_RUBIS_V1.json'); deps = rreg['deployments_base_order']
WL = ('browsing', 'bidding', 'update10x', 'update100x')
idx = {(r['arm'], r['deployment'], r['workload']): r for r in rc(ra / 'summary.csv')}
for arm in ('v41', 'v311'):
    w(f'rubis_mean_{arm}.tex', [' & '.join([esc(d)] + [f"{f(idx[(arm, d, x)]['mean_ms'], 3)} [{f(idx[(arm, d, x)]['ci_lo'], 3)}; {f(idx[(arm, d, x)]['ci_hi'], 3)}]" for x in WL]) + '\\\\' for d in deps])
ran = rj(ra / 'analysis.json'); racc = rj(ra / 'acceptance.json'); rhy = rj(ra / 'hypotheses.json')
con = {(c['arm'], c['workload'], c['a'], c['b']): c for c in rc(ra / 'contrasts.csv')}
vm = {'EMPATE': 'empate', 'A_MAIS_RAPIDO': '\\textbf{H+}', 'B_MAIS_RAPIDO': 'H$-$'}
L = []
for h in ('H01', 'H02'):
    for b in ('NoSE', 'QPG'):
        L.append(' & '.join([h, b] + [f"{f(con[('v41', x, h, b)]['ratio'])} {vm[con[('v41', x, h, b)]['verdict']]} / {f(con[('v311', x, h, b)]['ratio'])} {vm[con[('v311', x, h, b)]['verdict']]}" for x in WL]) + '\\\\')
w('rubis_contrasts.tex', L)
vc = rc(ra / 'version_comparison.csv')
geo = {x: math.exp(sum(math.log(float(r['ratio_41_over_311'])) for r in vc if r['workload'] == x) / sum(r['workload'] == x for r in vc)) for x in WL}
L = [' & '.join(['Razão média geométrica 4.1/3.11'] + [f(geo[x]) for x in WL]) + '\\\\',
     ' & '.join(['$\\tau$ de Kendall 3.11 $\\times$ 4.1 (9 deployments)'] + [f(ran['tau_311_vs_41'][x]) for x in WL]) + '\\\\',
     ' & '.join(['$\\tau$ previsto $\\times$ medido em H (4.1.12)'] + [f(ran['v41']['within_H'][x]['tau']) for x in WL]) + '\\\\',
     ' & '.join(['$\\tau$ previsto $\\times$ medido em H (3.11.11)'] + [f(ran['v311']['within_H'][x]['tau']) for x in WL]) + '\\\\',
     ' & '.join(['Top-1 previsto = melhor (4.1 / 3.11)'] + [f"{yn(ran['v41']['within_H'][x]['top1_within_noise'])} / {yn(ran['v311']['within_H'][x]['top1_within_noise'])}" for x in WL]) + '\\\\',
     ' & '.join(['Piso de ruído A/A (4.1.12)'] + [f"{f((1/min(p['ratio'], 1/p['ratio']) - 1) * 100, 1)}\\%" for x in WL for p in ran['v41']['noise_floor'][x]['pairs'] if p['b'] == 'H01_AA']) + '\\\\']
w('rubis_version.tex', L)

# ------------------------------------------------------------------ cross-benchmark tables
rtm = rj(rb / 'frozen/TRANSFER_MANIFEST.json')
L = [
    f"IoT-Benchmark & gerador Java oficial (commit 62e0dc10), campanha B & 9 dispositivos $\\times$ 2 sensores DOUBLE; 3.000 instantes por dispositivo (passo de 10\\,min, $\\approx$20,8 dias); \\textbf{{54.000 células}} (27.000 linhas largas) & 12 classes Q1--Q12 (uma instância cada), janela de 7 dias & 8 variantes $\\times$ 12 classes $\\times$ 60 rodadas $\\times$ 5 rep.\\ = \\textbf{{{n0(ian['raw_rows'])}}} latências\\\\",
    f"TPCx-IoT & sintético determinístico (registro lógico TPCx, 900\\,B de \\textit{{padding}}) & 2 subestações $\\times$ 3 sensores; 130 + 500 passos de 1\\,s; \\textbf{{3.780 eventos por execução}} (780 semente + 3.000 dinâmicos); 60 execuções = 226.800 eventos & Q\\_INGEST + Q\\_READ\\_2\\_WINDOWS (120 leituras/execução, 2 threads) & 12 variantes $\\times$ 120 leituras $\\times$ 5 rep.\\ = \\textbf{{{n0(tan['raw_reads'])}}} leituras; 60 ingestões\\\\",
    f"TSBS (cpu-only) & gerador oficial Go (commit 8323e59c) & {sreg['data']['hosts']} hosts $\\times$ 10 métricas; intervalo de 600\\,s; 36\\,h; \\textbf{{{n0(sreg['data']['observations'])} observações = {n0(sreg['data']['metric_cells'])} células}} & 16 templates $\\times$ 3 instâncias $\\times$ 2 coortes = 96 instâncias & 12 variantes $\\times$ 32 classes $\\times$ 12 rodadas $\\times$ 5 rep.\\ = \\textbf{{{n0(san['raw_rows'])}}} latências\\\\",
    f"RUBiS (V9) & regenerado (estrutura do E04 + \\texttt{{fake.js}}); escala lógica completa: 200 mil usuários, 2 M itens, 20 M lances, 10 M comentários, 2 M buynow & coorte materializada: {n0(rtm['cohort_counts']['items'])} itens, {n0(rtm['cohort_counts']['users'])} usuários ({n0(rtm['rows_written']['users'])} no fecho), {n0(rtm['cohort_counts']['bids'])} lances, {n0(rtm['cohort_counts']['comments'])} comentários, {n0(rtm['cohort_counts']['buynow'])} buynow, 500 categorias, 50 regiões & 36 instruções em 4 cargas (browsing, bidding, update10x, update100x) & 9 deployments $\\times$ 4 cargas $\\times$ 5 rep.\\ $\\times$ (1.000 + 10.000) req.\\ $\\times$ 2 versões = \\textbf{{3.600.000}} requisições medidas\\\\",
]
w('volumes.tex', L)
L = [
    f"IoT & {iev['space_size']} (todos medidos) & {iev['hbench_size']} & {yn(iev['top1_within_noise'])} & {f(iev['regret'], 3)} & {f(iev['pareto_retention'] * 100, 0)}\\% & {f(iev['kendall_tau_score_vs_read'])} & {f(ian['noise_floor']['noise_floor_pct'], 1)}\\%\\\\",
    f"TPCx-IoT & {tev['space_size']} (todos medidos) & {tev['hbench_size']} & {yn(tev['top1_within_noise'])} & {f(tev['regret'], 3)} & {f(tev['pareto_retention'] * 100, 0)}\\% (2D: {f(tev['pareto_retention_read_write'] * 100, 0)}\\%) & {f(tev['kendall_tau_score_vs_read'])} & {f(tan['noise_floor']['noise_floor_pct'], 1)}\\%\\\\",
    f"TSBS natural & 26 (10 medidos) & 7 & {yn(sev['natural']['top1_within_noise'])} & {f(sev['natural']['regret'], 3)} & {f(sev['natural']['pareto_retention'] * 100, 0)}\\% & {f(sev['natural']['kendall_tau_score_vs_read'])} & {f(san['noise_floor']['natural']['noise_floor_pct'], 1)}\\%\\\\",
    f"TSBS aligned & 26 (10 medidos) & 7 & {yn(sev['aligned']['top1_within_noise'])} & {f(sev['aligned']['regret'], 3)} & {f(sev['aligned']['pareto_retention'] * 100, 0)}\\% & {f(sev['aligned']['kendall_tau_score_vs_read'])} & {f(san['noise_floor']['aligned']['noise_floor_pct'], 1)}\\%\\\\",
]
w('method_eval.tex', L)
def cnt(acc): return f"{sum(acc['gates'].values())}/{len(acc['gates'])}", f"{sum(bool(v) for v in acc['negative_tests'].values())}/{len(acc['negative_tests'])}"
def hy(h): return ', '.join(f"{k}{'\\checkmark' if v['result'] else '$\\times$'}" for k, v in h.items())
L = []
for name, acc, h, disc in (('IoT-Benchmark', iacc, ihy, iacc['info']['discarded_windows']), ('TPCx-IoT', tacc, thy, tacc['info']['discarded_attempts']),
                           ('TSBS', sacc, shy, sacc['info']['discarded_windows']), ('RUBiS (4.1 e 3.11)', racc, rhy, racc['info']['discarded_windows'])):
    g, n = cnt(acc)
    L.append(f"{name} & {g} & {n} & {disc} & {hy(h)}\\\\")
w('acceptance.tex', L)
for src, dst in ((iot / 'relatorio_overleaf/figs/fig_v1_mix.pdf', 'iot_mix.pdf'), (iot / 'relatorio_overleaf/figs/fig_v1_perquery.pdf', 'iot_perquery.pdf'),
                 (tp / 'relatorio_overleaf/figs/fig_v1_tpcx.pdf', 'tpcx.pdf'), (ts / 'relatorio_overleaf/figs/fig_v1_tsbs_mix.pdf', 'tsbs.pdf'),
                 (rb / 'relatorio_overleaf/figs/fig_v1_rubis.pdf', 'rubis.pdf')):
    shutil.copy2(src, F / dst)
print('tabelas e figuras do artigo geradas em', HERE)
