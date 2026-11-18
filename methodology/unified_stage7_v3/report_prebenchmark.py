"""methodology/docs/v3/V3_PREBENCHMARK_COMPARISON.md + <benchmark>/v3_unified/analysis/v2_vs_v3_hbench.csv (before any Cassandra run).
Reads only design artefacts (V2 preregistrations / V9 E15 H.json) - no measured result."""
from __future__ import annotations

import csv
import json
import subprocess
import sys

from .pareto import AXES, dominates
from .provenance import REPO

from .layout import DOCS_V3, OUT, V2, V9_RUN
OLD_METHOD = {
    'iot': 'Rev3 plan-aware (V2): proxies de requisições/linhas/bytes por plano + escore de pré-seleção ponderado (0,8 leitura / 0,1 armazenamento / 0,1 escrita), Pareto 3D (leitura, armazenamento, escrita), top-k',
    'tpcx_iot': 'Stage 7/8 TPCx (V2): cenário ilustrativo 10 estações × 200 sensores, RF 2; escore normalizado de proxies; Pareto 3D; top_k 5',
    'tsbs': 'Stage 7/8 TSBS (V2): cenário 100 hosts / 60 s / 3 dias; escore ponderado 0,8 leitura / 0,1 armazenamento / 0,1 chaves de escrita; perfis conservative e conditional_aligned; Pareto 3D; top_k 5 por perfil',
    'rubis': 'V9 E14/E15: modelo de custo NNLS calibrado (a_read 0,462 ms, b_write 0,389 ms, c_row 0,0075 ms) + ILP por mix, grade de somas ponderadas (Pareto leitura × escrita × espaço, mix bidding) e k-melhores até |H| = 5',
}
TSBS_V2_ROUTES = r'''
import json, sys
sys.path[:0] = [sys.argv[1] + '/frozen_design', sys.argv[1] + '/tsbs_v1']
from freeze import design_objects
from methodology.tsbs.stage9.instances import route_instance
deps, plans, _ = design_objects()
inst = json.load(open(sys.argv[1] + '/data/query_instances.json'))
out = {}
for did, d in deps.items():
    out[did] = {'tables': sorted(d['candidate_ids']), 'profile': d['profile'], 'routes': {i['instance_key']: route_instance(d, i, plans)[0]['plan_id'] for i in inst}}
print(json.dumps(out))
'''


def load(b, n):
    return json.loads((OUT / b / 'prebenchmark' / n).read_text())


def old_hbench(b):
    """-> list of (old_id, tables, routes or None)"""
    if b == 'iot':
        reg = json.loads((V2['iot'] / 'PREREGISTRATION_IOT_V1.json').read_text())
        v = {x['label']: x for x in reg['variants'] if x.get('kind') == 'cover'}
        return [(i, sorted(v[i]['candidates']), v[i]['routes']) for i in reg['hbench_ids_ranked']], len(v)
    if b == 'tpcx_iot':
        reg = json.loads((V2['tpcx_iot'] / 'PREREGISTRATION_TPCX_V1.json').read_text())
        v = {x['label']: x for x in reg['variants'] if x['kind'] == 'cover'}
        return [(i, sorted(v[i]['tables']), {'Q_READ_2_WINDOWS': 'qp__q_read_2_windows__' + v[i]['read_table']}) for i in reg['hbench_ids_ranked']], reg.get('admissible_space_size')
    if b == 'tsbs':
        reg = json.loads((V2['tsbs'] / 'PREREGISTRATION_TSBS_V1.json').read_text())
        r = subprocess.run([sys.executable, '-c', TSBS_V2_ROUTES, str(V2['tsbs'])], capture_output=True, text=True, check=True, cwd='/')
        deps = json.loads(r.stdout.strip().splitlines()[-1])
        return [(i, deps[i]['tables'], deps[i]['routes']) for i in reg['hbench_ids_ranked']], len(deps)
    H = json.loads((V9_RUN / 'stages/E15_composition/H.json').read_text())['H']
    out = []
    for h in H:
        rt = {q: f"q{q}:{o['main']}" + ''.join('+' + l[3] for l in o['lookups']) for q, o in h['routes'].items() if 'main' in o}
        out.append((h['schema_id'], sorted(h['canonical_ids']), rt))
    return out, None


def reasons(b):
    vec = {r['deployment_id']: r for r in load(b, '05_deployment_vectors.json')}
    comp = load(b, '06_complete_deployments.json')
    pf = {r['deployment_id'] for r in load(b, '07_pareto_front.json')}
    tr = load(b, '11_selection_trace.json')
    sel = set(tr['selected'])
    steps = {s['pick']: s for s in json.loads((OUT / b / 'prebenchmark/08_diversity_selection.json').read_text())['steps']}
    out = {}
    for i in comp['complete_ids']:
        if i in sel:
            s = steps.get(i)
            out[i] = 'HBENCH: ' + (tr['rule'] if not s else f"{s['reason']}" + (f" (novas tags: {', '.join(s.get('new_tags', [])[:4])})" if s.get('new_tags') else ''))
        elif i in pf:
            out[i] = 'PARETO, cortado pelo top_k após diversidade/desempate'
        else:
            dom = next(j for j in sorted(pf) if dominates(vec[j]['axes'], vec[i]['axes']))
            out[i] = f'DOMINADO por {dom}'
    return out, vec, comp, tr


def section(b):
    old, old_space = old_hbench(b)
    why, vec, comp, tr = reasons(b)
    hb = load(b, '09_hbench_v3.json')
    sc = json.loads((OUT / b / 'V3_PREBENCHMARK_SCENARIO.json').read_text())
    key = lambda tables: '+'.join(sorted(tables))
    new = {key(h['tables']): h for h in hb}
    oldk = {key(t): (i, t, r) for i, t, r in old}
    inter = sorted(set(new) & set(oldk)); only_old = sorted(set(oldk) - set(new)); only_new = sorted(set(new) - set(oldk))
    jac = len(inter) / len(set(new) | set(oldk))
    route_eq = {}
    for k in inter:
        r_old = oldk[k][2]
        route_eq[k] = None if r_old is None else all(r_old.get(q) == p for q, p in new[k]['routes'].items())
    rows = []
    for h in hb:
        k = key(h['tables'])
        rows.append({'benchmark': b, 'v3_rank': h['rank'], 'v3_id': h['deployment_id'], 'tables': k, 'v2_id': oldk.get(k, (None,))[0],
                     'in_v2_hbench': k in oldk, 'routes_identical_to_v2': route_eq.get(k), 'balanced_structural_tiebreak': round(h['balanced_structural_tiebreak'], 6),
                     **{a: h['axes'][a] for a in AXES}})
    for k in only_old:
        rows.append({'benchmark': b, 'v3_rank': None, 'v3_id': None, 'tables': k, 'v2_id': oldk[k][0], 'in_v2_hbench': True,
                     'routes_identical_to_v2': None})
    (OUT / b / 'analysis').mkdir(parents=True, exist_ok=True)
    with (OUT / b / 'analysis/v2_vs_v3_hbench.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=sorted({k for r in rows for k in r})); w.writeheader(); w.writerows(rows)
    L = [f'## {b}', '',
         f"- **Universo de candidatos:** {len(load(b, '04_candidate_footprints.json'))} CandidateSpecs; "
         f"{comp['enumerated']} deployments enumerados, {comp['complete']} completos/viáveis (V2: espaço admissível = {old_space if old_space is not None else 'n/d (ILP)'}).",
         f"- **Cenário (escala da campanha):** `{json.dumps({k: sc[k] for k in list(sc)[:6]}, ensure_ascii=False, default=str)[:400]}`",
         f"- **Frequências:** {load(b, '12_frozen_manifest.json')['frequency_source']} / {load(b, '12_frozen_manifest.json')['aggregation_policy']}",
         f'- **Método analítico antigo:** {OLD_METHOD[b]}',
         '- **Método novo:** PhysicalWorkVector (read_cql_requests, read_logical_rows, read_logical_bytes, write_cql_mutations, '
         'maintenance_read_requests, logical_storage_bytes) → cobertura/viabilidade → Pareto (6 eixos) → diversidade estrutural → balanced_structural_tiebreak',
         f"- **Fronteira de Pareto V3:** {len(tr['pareto_front'])} deployments; regra aplicada: `{tr['rule']}`; |HBench V3| = {len(hb)}",
         f'- **HBench antigo (V2):** {len(old)} — ' + ', '.join(f'`{i}`' for i, _, _ in old),
         f'- **HBench novo (V3):** {len(hb)} — ' + ', '.join(f"`{h['deployment_id']}`" for h in hb),
         f'- **Interseção (mesmas tabelas):** {len(inter)}; **só antigo:** {len(only_old)}; **só novo:** {len(only_new)}; **Jaccard:** {jac:.3f}',
         '- **Rotas idênticas às da V2 na interseção:** ' + (', '.join(f"{oldk[k][0]}={'sim' if v else ('não' if v is False else 'n/d')}" for k, v in route_eq.items()) or '—'),
         '', '| deployment V3 | tabelas | V2 | razão (retido/excluído) | req. | linhas | bytes | mutações | leit. manut. | armaz. (B) |',
         '|---|---|---|---|---|---|---|---|---|---|']
    order = sorted(comp['complete_ids'], key=lambda i: (not why[i].startswith('HBENCH'), not why[i].startswith('PARETO'), tr['tiebreak'][i], i))
    shown = order if len(order) <= 30 else order[:30]
    for i in shown:
        a = vec[i]['axes']; k = key(vec[i]['tables'])
        L.append(f"| `{i}` | {k.replace('+', ' + ')} | {oldk.get(k, ('',))[0] or ''} | {why[i]} | {a['read_cql_requests']:.3f} | {a['read_logical_rows']:.1f} | "
                 f"{a['read_logical_bytes']:.0f} | {a['write_cql_mutations']:.3f} | {a['maintenance_read_requests']:.3f} | {a['logical_storage_bytes']:.0f} |")
    if len(order) > len(shown):
        L.append(f'| … | {len(order) - len(shown)} deployments dominados omitidos (lista completa em `<benchmark>/v3_unified/prebenchmark/05_deployment_vectors.json`) | | | | | | | | |')
    if comp['infeasible']:
        L += ['', f"Inviáveis: {comp['infeasible']} — motivos (contagem de ocorrências): " + ', '.join(f'{k}: {v}' for k, v in sorted(comp['infeasible_reason_histogram'].items()))]
    for k in only_old:
        L.append(f'- `{oldk[k][0]}` (só V2): ' + (why.get(next((i for i in vec if key(vec[i]["tables"]) == k), ''), 'não é deployment completo/viável na V3 (regra estrutural D-V3-007 ou perfil V2)')))
    return '\n'.join(L) + '\n'


def main():
    head = ['# V3 — Comparação pré-benchmark V2 × V3', '',
            'Gerado **antes** de qualquer execução Cassandra da V3 (`python -m unified_stage7_v3.report_prebenchmark`). '
            'Lê apenas artefatos de projeto (pré-registros V2, H.json da V9) e as saídas do Stage 7/8 V3; nenhum resultado medido. '
            '**Não se interpreta aqui qual HBench é melhor.** Identidade de deployment = conjunto de tabelas; a igualdade de rotas é '
            'reportada à parte porque decide a reutilização das medições (D-V3-001).', '']
    body = [section(b) for b in ('iot', 'tpcx_iot', 'tsbs', 'rubis')]
    (DOCS_V3 / 'V3_PREBENCHMARK_COMPARISON.md').write_text('\n'.join(head + body))
    print('written docs/V3_PREBENCHMARK_COMPARISON.md')


if __name__ == '__main__':
    main()
