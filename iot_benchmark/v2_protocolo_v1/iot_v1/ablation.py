"""Passo 4 - ablacoes (offline; nenhuma latencia nova).

Ablacao de regras: remove cada regra TS0..TS8 / M1..M5, roda de novo o motor de ativacao
vendorizado (sem alteracao) sobre os vetores de evidencia congelados, propaga para os
candidatos do Stage 4 e roda de novo o Stage 8 Rev3 (codigo congelado) sobre o subconjunto.
Ablacao de criterios: sem salvaguarda de diversidade, sem Pareto, sem plan-awareness (Rev2),
TS0 obrigatoria (regra V3) e escolha aleatoria de k.
Cada HBench resultante e avaliado com as medicoes do modo verification (measured_space.json).
"""
from __future__ import annotations

import argparse
import copy
import json
from dataclasses import asdict
from pathlib import Path

from common_paths import REV2, REV3, ROOT, SNAP, csv_x, dump, read
import method_eval as M

FAMILY_OF = {'ts0_iot_raw': 'TS0', 'ts1_iot_bucket': 'TS1', 'ts2_iot_wide': 'TS2', 'ts2_iot_wide_unbucketed': 'TS2',
             'ts4_iot_latest': 'TS4', 'ts5_iot_rollup': 'TS5'}


def activation(drop=None):
    import sys
    base = SNAP / 'phase_a1/vendor/tsdb_base'
    if str(base) not in sys.path:
        sys.path.insert(0, str(base))
    from methodology.common.rule_engine import activate, evaluate_rule
    rules = read(base / 'config/tsdb_activation_rules.json')
    mods = read(base / 'config/temporal_modifier_rules.json')
    rules = copy.deepcopy(rules); mods = copy.deepcopy(mods)
    rules['rules'] = [r for r in rules['rules'] if r['family'] != drop]
    mods['rules'] = [r for r in mods['rules'] if r['family'] != drop]
    act = read(SNAP / 'phase_a1/outputs/activation_ts_only.json')
    fams, ms, per_q = set(), set(), {}
    for rec in act:
        v = rec['features']
        a, _ = activate(v, rules)
        chosen = []
        for rule in mods['rules']:
            if evaluate_rule(v, rule, a + chosen).passed:
                chosen.append(rule['family'])
        fams |= set(a); ms |= set(chosen); per_q[rec['query_id']] = (a, chosen)
    return fams, ms, per_q


def surviving_candidates(fams, ms, candidates):
    keep = []
    for c in candidates:
        f = FAMILY_OF[c['candidate_id']]
        if f not in fams:
            continue
        if c['candidate_id'] == 'ts2_iot_wide_unbucketed' and 'M1' not in ms:
            continue  # TS2-U is the materialisation of the M1 granularity decision
        keep.append(c)
    return keep


def stage8(candidates_subset):
    from v4_auto import selector as A
    from rev3.core import make_plan_catalog, select_schema_and_plan
    from iot_pipeline.shared import read_rows, require_policy
    cands = read(REV2 / 'stage4/10_v4_candidate_specs.json')
    keep_ids = {c['candidate_id'] for c in candidates_subset}
    cands = [c for c in cands if c['candidate_id'] in keep_ids]
    cov = read(REV2 / 'stage5/15_cross_query_coverage.json')
    reads = read(REV2 / 'stage7/23_read_proxies_rev2.json')
    storage = read(REV2 / 'stage7/21_storage_proxies.json')
    writes = read(REV2 / 'stage7/22_write_proxies.json')
    rows = read_rows(SNAP / 'phase_a1/outputs/normalized_double_readings.csv')
    qs = read(SNAP / 'phase_a1/outputs/query_instances_pilot.json')
    policy = require_policy()
    try:
        plans = make_plan_catalog(cands, qs, cov, reads, rows)
        covers, selected, pareto = select_schema_and_plan(cands, qs, cov, plans, storage, writes, policy)
    except ValueError as exc:
        return None, None, None, str(exc)
    return covers, selected, pareto, None


def rank_key(h):
    return (h['preselection_score'], len(h['candidates']), h['deployment_id'])


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True, type=Path)
    a = ap.parse_args()
    run = (ROOT / a.run) if not a.run.is_absolute() else a.run
    ms_ = read(run / 'analysis/measured_space.json')
    measured, tied = ms_['measured'], set(ms_['tied_with_best'])
    all_cands = read(SNAP / 'outputs/stage4/10_candidate_specs.json')
    frozen_hb = [h['deployment_id'] for h in read(REV3 / 'stage8/28_hbench_v4_rev3_frozen.json')]
    score = {h['deployment_id']: h['preselection_score'] for h in read(REV3 / 'stage8/29_all_compatible_covers.json')}
    from v4_auto import selector as A
    full_covers, full_sel, full_par, _ = stage8(all_cands)
    if [h['deployment_id'] for h in full_sel] != frozen_hb:
        raise AssertionError('Re-running frozen Stage 8 did not reproduce the frozen HBench')
    rows = []

    def record(kind, name, hb_ids, detail, covers=None):
        hb_ids = list(hb_ids)
        unmeasured = [h for h in hb_ids if h not in measured]
        r = {'kind': kind, 'ablation': name, 'hbench': '|'.join(hb_ids), 'hbench_size': len(hb_ids),
             'admissible_covers': len(covers) if covers is not None else '', 'detail': detail,
             'unmeasured_ids': '|'.join(unmeasured)}
        if hb_ids and not unmeasured:
            ev = M.evaluate(measured, hb_ids, score, tied)
            r.update({k: ev[k] for k in ('top1_within_noise', 'top3', 'near_best_5pct', 'regret', 'pareto_retention')})
            r['changed_vs_frozen'] = hb_ids != frozen_hb
        elif not hb_ids:
            r.update({'top1_within_noise': False, 'top3': False, 'near_best_5pct': False, 'regret': 'INFEASIBLE'})
        rows.append(r)

    record('reference', 'nenhuma (HBench congelado)', frozen_hb, 'Stage 8 Rev3 congelado reproduzido', full_covers)
    base_f, base_m, _ = activation(None)
    for fam in [f'TS{i}' for i in range(9)] + [f'M{i}' for i in range(1, 6)]:
        f, m, _ = activation(fam)
        lost_f = sorted(base_f - f); lost_m = sorted(base_m - m)
        cands = surviving_candidates(f, m, all_cands)
        covers, sel, par, err = stage8(cands)
        detail = (f"familias perdidas={lost_f or '-'}; modificadores perdidos={lost_m or '-'}; "
                  f"candidatos={[c['candidate_id'] for c in cands]}" + (f'; erro={err}' if err else ''))
        if not lost_f and not lost_m:
            detail = 'regra nao ativa nesta carga: ablacao sem efeito (no-op)'
        record('regra', f'sem {fam}', [h['deployment_id'] for h in sel] if sel else [], detail, covers)
    # criteria ablations on the full frozen space
    pareto_only = sorted(full_par, key=rank_key)[:5]
    record('criterio', 'sem salvaguarda de diversidade', [h['deployment_id'] for h in pareto_only],
           'apenas fronteira de Pareto (top_k=5)', full_covers)
    by_score = sorted(full_covers, key=rank_key)[:len(frozen_hb)]
    record('criterio', 'sem Pareto (top-3 por escore ponderado)', [h['deployment_id'] for h in by_score],
           'pesos 0,8/0,1/0,1 sem filtro de dominancia', full_covers)
    by_score1 = sorted(full_covers, key=rank_key)[:1]
    record('criterio', 'so o rank-1 (sem diversidade, k=1)', [h['deployment_id'] for h in by_score1], 'k=1', full_covers)
    ts0 = [h for h in full_covers if 'ts0_iot_raw' in h['candidates']]
    ts0_par = [h for h in ts0 if not any(A.is_dominated(h, k) for k in ts0 if k is not h)]
    record('criterio', 'TS0 obrigatoria (regra V3)', [h['deployment_id'] for h in sorted(ts0_par, key=rank_key)[:5]],
           'so coberturas com ts0_iot_raw; Pareto entre elas', full_covers)
    rev2 = read(REV2 / 'stage8/28_hbench_v4_rev2_frozen.json')
    bycand = {tuple(h['candidates']): h['deployment_id'] for h in full_covers}
    mapped = [bycand.get(tuple(h['candidates'])) for h in rev2]
    record('criterio', 'sem plan-awareness (HBench Rev2)', [x for x in mapped if x],
           'conjunto de tabelas da Rev2 mapeado para as rotas Rev3 medidas (aproximacao declarada)', None)
    rk = M.random_k_expected(measured, len(frozen_hb))
    rows.append({'kind': 'criterio', 'ablation': f'aleatorio k={len(frozen_hb)}', 'hbench': '', 'hbench_size': len(frozen_hb),
                 'detail': f"P(contem melhor)={rk['p_contains_best']:.3f}; {rk['combinations']} combinacoes",
                 'regret': rk['expected_regret'], 'near_best_5pct': '', 'top1_within_noise': '', 'top3': ''})
    out = run / 'analysis'
    cols = ['kind', 'ablation', 'hbench_size', 'admissible_covers', 'changed_vs_frozen', 'top1_within_noise', 'top3',
            'near_best_5pct', 'regret', 'pareto_retention', 'hbench', 'unmeasured_ids', 'detail']
    csv_x(out / 'ablations.csv', rows, cols, mode='w')
    for r in rows:
        print(f"{r['ablation']:<42} size={r['hbench_size']} regret={r.get('regret')} near={r.get('near_best_5pct')} | {r['detail'][:90]}")


if __name__ == '__main__':
    main()
