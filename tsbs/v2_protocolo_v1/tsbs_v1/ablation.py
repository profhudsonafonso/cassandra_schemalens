"""Passo 4 - ablacoes do TSBS (offline; nenhuma latencia nova).

Cada regra TS0..TS8 / M1..M5 e NEUTRALIZADA (condicao impossivel; mesma familia e ordem) numa copia
limpa; o pipeline inteiro (run_tsbs = Stages 1-3, e Stages 4-8) e regenerado pelo codigo congelado.
O HBench resultante e avaliado com as medicoes v1 (coortes natural e aligned). Membros nao medidos
(fora do HBench congelado e do Pareto analitico) sao listados; nesse caso o regret informado e um
LIMITE SUPERIOR (calculado so sobre os membros medidos).
"""
from __future__ import annotations

import argparse
import csv
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
    shutil.copytree(SNAP, work, ignore=shutil.ignore_patterns('__pycache__', 'tsbs_stage4', 'tsbs_stage5', 'tsbs_stage6',
                                                           'tsbs_stage7', 'tsbs_stage8', 'tsbs_stage9*', 'tsbs_run'))
    if fam:
        p = work / ('config/tsdb_activation_rules.json' if fam.startswith('TS') else 'config/temporal_modifier_rules.json')
        r = json.loads(p.read_text())
        for x in r['rules']:
            if x['family'] == fam:
                x.setdefault('all', []).append({'field': 'ablation_neutralized_rule', 'op': 'eq', 'value': True})
        p.write_text(json.dumps(r, indent=2))
    env = {'PYTHONPATH': str(work), 'PATH': '/usr/bin:/bin'}
    for mod in ['methodology.tsbs.run_tsbs'] + [f'methodology.tsbs.stage{s}.cli' for s in range(4, 9)]:
        r = subprocess.run([sys.executable, '-m', mod], cwd=work, env=env, capture_output=True, text=True)
        if r.returncode != 0:
            return None, None, f'{mod} falhou: ' + r.stderr.strip().splitlines()[-1][:120]
    act = {}
    for row in csv.DictReader(open(work / 'outputs/tsbs_run/04_tsdb_activation_matrix.csv')):
        act[row['query_id'] if 'query_id' in row else list(row.values())[0]] = row
    fams = set()
    for row in act.values():
        fams |= {f for f in (f'TS{i}' for i in range(9)) if str(row.get(f, '')).lower() in ('true', '1')}
    mods = set()
    for row in csv.DictReader(open(work / 'outputs/tsbs_run/05_temporal_modifier_matrix.csv')):
        mods |= {m for m in (f'M{i}' for i in range(1, 6)) if str(row.get(m, '')).lower() in ('true', '1')}
    hb = read(work / 'outputs/tsbs_stage8/28_hbench_selected_deployments.json')
    return [d['deployment_id'] for d in hb], (fams, mods), None


def evaluate(ids, measured, score, tied):
    meas = [i for i in ids if i in measured]
    if not meas:
        return {'regret': 'SEM_MEMBRO_MEDIDO'}
    ev = M.evaluate(measured, meas, score, tied)
    bound = len(meas) < len(ids)
    return {'regret': ev['regret'], 'regret_is_upper_bound': bound, 'near_best_5pct': ev['near_best_5pct'],
            'contains_best_or_tied': bool(set(meas) & tied)}


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True, type=Path)
    a = ap.parse_args()
    run = (ROOT / a.run) if not a.run.is_absolute() else a.run
    ms = read(run / 'analysis/measured_space.json'); meth = read(run / 'analysis/method_eval.json')
    reg = read(ROOT / 'PREREGISTRATION_TSBS_V1.json')
    score = {v['label']: v['score'] for v in reg['variants'] if v['kind'] == 'cover'}
    frozen = reg['hbench_ids_ranked']
    rows = []
    with tempfile.TemporaryDirectory() as td:
        base, (bf, bm), err = regenerate(None, Path(td) / 'base')
        if base != frozen:
            raise AssertionError('Unablated regeneration does not reproduce the frozen HBench')
        cases = [('reference', 'nenhuma (HBench congelado)', base, 'pipeline regenerado = congelado')]
        for fam in FAMS:
            ids, am, err = regenerate(fam, Path(td) / fam)
            if err:
                cases.append(('regra', f'sem {fam}', [], err)); continue
            lost = sorted((bf - am[0]) | (bm - am[1]))
            d = 'regra nao ativa nesta carga: sem efeito (no-op)' if not lost else f'perde {lost}'
            cases.append(('regra', f'sem {fam}', ids, d))
    cover_ids = list(score)
    for kind, name, ids, d in cases:
        r = {'kind': kind, 'ablation': name, 'hbench': '|'.join(ids), 'hbench_size': len(ids), 'changed_vs_frozen': ids != frozen,
             'unmeasured': '|'.join(i for i in ids if i not in cover_ids), 'detail': d}
        for coh in ('natural', 'aligned'):
            measured = {k: {'read': v['read'], 'storage': 0, 'write': 0} for k, v in ms[coh]['measured'].items()}
            tied = set(meth[coh]['tied_with_best'])
            e = evaluate(ids, measured, score, tied) if ids else {'regret': 'INVIAVEL'}
            r[f'{coh}_regret'] = e['regret']; r[f'{coh}_upper_bound'] = e.get('regret_is_upper_bound', '')
            r[f'{coh}_contains_best'] = e.get('contains_best_or_tied', '')
        rows.append(r)
    for coh in ('natural', 'aligned'):
        measured = {k: {'read': v['read'], 'storage': 0, 'write': 0} for k, v in ms[coh]['measured'].items()}
        rk = M.random_k_expected(measured, len(frozen))
        rows.append({'kind': 'criterio', 'ablation': f'aleatorio k={len(frozen)} entre as {len(measured)} medidas ({coh})', 'hbench_size': len(frozen),
                     f'{coh}_regret': rk['expected_regret'], 'detail': f"P(contem melhor)={rk['p_contains_best']:.3f}"})
    for coh in ('natural', 'aligned'):
        measured = {k: {'read': v['read'], 'storage': 0, 'write': 0} for k, v in ms[coh]['measured'].items()}
        r1 = sorted(frozen, key=lambda h: score[h])[:1]
        e = evaluate(r1, measured, score, set(meth[coh]['tied_with_best']))
        rows.append({'kind': 'criterio', 'ablation': f'so o menor escore (k=1) ({coh})', 'hbench': r1[0], 'hbench_size': 1,
                     f'{coh}_regret': e['regret'], f'{coh}_contains_best': e['contains_best_or_tied'], 'detail': 'sem diversidade de perfis'})
    cols = ['kind', 'ablation', 'hbench_size', 'changed_vs_frozen', 'natural_regret', 'natural_upper_bound', 'natural_contains_best',
            'aligned_regret', 'aligned_upper_bound', 'aligned_contains_best', 'hbench', 'unmeasured', 'detail']
    csv_x(run / 'analysis/ablations.csv', rows, cols, mode='w')
    for r in rows:
        print(f"{r['ablation']:<48} n={r['hbench_size']} N={r.get('natural_regret')} A={r.get('aligned_regret')} unmeas={r.get('unmeasured','')[:40]} | {r['detail'][:60]}")


if __name__ == '__main__':
    main()
