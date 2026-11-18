"""Passo 4 - ablacoes do TPCx-IoT (offline; nenhuma latencia nova).

Regras: cada regra TS0..TS8 / M1..M5 e NEUTRALIZADA (condicao impossivel acrescentada; mesma familia e
mesma ordem, para que as guardas de integridade do codigo congelado continuem ativas) numa copia limpa
do projeto. Os Stages 1-3 (pipeline) e o Stage 4 sao regenerados de verdade. O Stage 5 congelado exige
exatamente 5 candidatos (contrato do design), entao o Stage 8 e reaplicado com as funcoes do proprio
consolidador (_pareto, _select, mesma normalizacao pelo controle TS0) sobre as coberturas admissiveis
congeladas cujas tabelas sobreviveram.
Criterios: sem diversidade, sem Pareto, sem controle obrigatorio, k=1, aleatorio k=5.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from common_paths import ROOT, SNAP, csv_x, read
import method_eval as M

FAMS = [f'TS{i}' for i in range(9)] + [f'M{i}' for i in range(1, 6)]


def regenerate(fam, work):
    shutil.copytree(SNAP, work, ignore=shutil.ignore_patterns('__pycache__', 'stage91*', 'tpcx_iot_stage4', 'tpcx_iot_stage5',
                                                           'tpcx_iot_stage6', 'tpcx_iot_stage7', 'tpcx_iot_stage8', 'tpcx_iot_stage9'))
    if fam:
        p = work / ('config/tsdb_activation_rules.json' if fam.startswith('TS') else 'config/temporal_modifier_rules.json')
        r = json.loads(p.read_text())
        for x in r['rules']:
            if x['family'] == fam:
                x.setdefault('all', []).append({'field': 'ablation_neutralized_rule', 'op': 'eq', 'value': True})
        p.write_text(json.dumps(r, indent=2))
    env = {'PYTHONPATH': str(work), 'PATH': '/usr/bin:/bin'}
    subprocess.run([sys.executable, '-m', 'methodology.tpcx_iot.pipeline'], cwd=work, env=env, check=True, capture_output=True)
    for f in (work / 'outputs/tpcx_iot_stage1_3').glob('0[1-5]_*'):
        shutil.copy2(f, work / 'inputs/stage3_frozen' / f.name)
    subprocess.run([sys.executable, '-m', 'methodology.tpcx_iot.stage4.cli'], cwd=work, env=env, check=True, capture_output=True)
    m4 = read(work / 'outputs/tpcx_iot_stage4/14_stage4_manifest.json')
    act = {}
    import csv
    for row in csv.DictReader(open(work / 'outputs/tpcx_iot_stage1_3/03_ts_activation_matrix.csv')):
        act[row['query_id']] = json.loads(row['active_families'])
    mods = {}
    for row in csv.DictReader(open(work / 'outputs/tpcx_iot_stage1_3/04_modifier_matrix.csv')):
        mods[row['query_id']] = row.get('active_modifiers')
    return m4['candidate_ids'], act, mods


def select(cands, policy, complete, no_control=False):
    from methodology.tpcx_iot.stage8.consolidator import _pareto, _select
    rows = [dict(h) for h in complete if set(h['tables']) <= set(cands)]
    if not rows:
        return [], 'nenhuma cobertura admissivel'
    if not no_control and not any(h['tables'] == [policy['required_control_candidate']] for h in rows):
        return [], 'controle obrigatorio TS0 ausente: Stage 8 recusa (inviavel)'
    front = sorted(_pareto(rows), key=lambda r: (r['normalized_proxy_score'], r['deployment_id']))
    if no_control:
        p2 = dict(policy); best = front[0]
        p2['required_control_candidate'] = None
        sel = [best]; seen = set(best['tables'])
        other = [r for r in front if r is not best]
        while other and len(sel) < policy['top_k']:
            mx = max(len(set(x['tables']) - seen) for x in other)
            if not mx:
                break
            pick = min((r for r in other if len(set(r['tables']) - seen) == mx), key=lambda x: (x['normalized_proxy_score'], x['deployment_id']))
            sel.append(pick); seen.update(pick['tables']); other.remove(pick)
        for r in other:
            if len(sel) >= policy['top_k']:
                break
            sel.append(r)
        return sel, f'{len(rows)} coberturas; Pareto {len(front)}'
    return _select(front, policy), f'{len(rows)} coberturas; Pareto {len(front)}'


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True, type=Path)
    a = ap.parse_args()
    run = (ROOT / a.run) if not a.run.is_absolute() else a.run
    ms = read(run / 'analysis/measured_space.json'); measured, tied = ms['measured'], set(ms['tied_with_best'])
    complete = read(SNAP / 'outputs/tpcx_iot_stage8/52_complete_compatible_deployments.json')
    policy = read(SNAP / 'config/tpcx_stage8_consolidation_policy.json')
    frozen = [h['deployment_id'] for h in read(SNAP / 'outputs/tpcx_iot_stage8/54_hbench_selected.json')]
    score = {h['deployment_id']: h['normalized_proxy_score'] for h in complete}
    rows = []

    def record(kind, name, ids, detail, cands=''):
        r = {'kind': kind, 'ablation': name, 'hbench': '|'.join(ids), 'hbench_size': len(ids), 'candidates': cands, 'detail': detail}
        if ids:
            ev = M.evaluate(measured, ids, score, tied)
            r.update({k: ev[k] for k in ('top1_within_noise', 'top3', 'near_best_5pct', 'regret', 'pareto_retention')})
            r['changed_vs_frozen'] = ids != frozen
        else:
            r.update(regret='INFEASIBLE', near_best_5pct=False, top1_within_noise=False)
        rows.append(r)

    with tempfile.TemporaryDirectory() as td:
        base_c, base_act, base_mod = regenerate(None, Path(td) / 'base')
        sel, d = select(base_c, policy, complete)
        if [h['deployment_id'] for h in sel] != frozen:
            raise AssertionError('Unablated regeneration does not reproduce the frozen HBench')
        record('reference', 'nenhuma (HBench congelado)', frozen, 'Stages 1-4 regenerados + Stage 8 reaplicado = congelado', '|'.join(base_c))
        for fam in FAMS:
            c, act, mod = regenerate(fam, Path(td) / fam)
            lost = sorted({f for v in base_act.values() for f in v} - {f for v in act.values() for f in v})
            lostm = sorted({m for v in base_mod.values() for m in json.loads(v or '[]')} - {m for v in mod.values() for m in json.loads(v or '[]')})
            sel, d = select(c, policy, complete)
            if not lost and not lostm:
                d = 'regra nao ativa nesta carga: sem efeito (no-op)'
            elif sorted(c) == sorted(base_c):
                d = f'ativacao perdida {lost or lostm}, mas o Stage 4 gera os mesmos candidatos (granularidades vem da politica)'
            else:
                d = f'familias perdidas={lost}; modificadores perdidos={lostm}; ' + d
            record('regra', f'sem {fam}', [h['deployment_id'] for h in sel], d, '|'.join(c))
    front = sorted([h for h in complete if h['deployment_id'] in read(SNAP / 'outputs/tpcx_iot_stage8/53_pareto_preselection.json')['pareto_deployment_ids']],
                   key=lambda r: (r['normalized_proxy_score'], r['deployment_id']))
    record('criterio', 'sem diversidade (top-5 do Pareto por escore)', [h['deployment_id'] for h in front[:5]], 'ordem por escore normalizado')
    allc = sorted(complete, key=lambda r: (r['normalized_proxy_score'], r['deployment_id']))
    record('criterio', 'sem Pareto (top-5 por escore ponderado)', [h['deployment_id'] for h in allc[:5]], 'pesos 45/15/20/15/5')
    sel, d = select([t for h in complete for t in h['tables']], policy, complete, no_control=True)
    record('criterio', 'sem controle TS0 obrigatorio', [h['deployment_id'] for h in sel], 'diversidade a partir do melhor escore')
    record('criterio', 'so o rank-1 (k=1)', [allc[0]['deployment_id']], 'menor escore normalizado')
    rk = M.random_k_expected(measured, 5)
    rows.append({'kind': 'criterio', 'ablation': 'aleatorio k=5', 'hbench_size': 5, 'regret': rk['expected_regret'],
                 'detail': f"P(contem melhor)={rk['p_contains_best']:.3f}; {rk['combinations']} combinacoes"})
    cols = ['kind', 'ablation', 'hbench_size', 'changed_vs_frozen', 'top1_within_noise', 'top3', 'near_best_5pct', 'regret',
            'pareto_retention', 'hbench', 'candidates', 'detail']
    csv_x(run / 'analysis/ablations.csv', rows, cols, mode='w')
    for r in rows:
        print(f"{r['ablation']:<44} n={r['hbench_size']} regret={r.get('regret')} | {r['detail'][:100]}")


if __name__ == '__main__':
    main()
