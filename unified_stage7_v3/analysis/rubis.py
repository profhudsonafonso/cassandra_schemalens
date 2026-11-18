"""RUBiS V3 analysis (4.1.12 main, 3.11.11 secondary) + legacy NNLS ordering as RETROSPECTIVE_BASELINE_ONLY."""
from __future__ import annotations

import importlib
import json
import sys

import numpy as np

from .common import OUT, REPO, S, VA, M, ablation_table, dump, frozen, read, read_csv, write_csv, v3_space

WORKLOADS = ('browsing', 'bidding', 'update10x', 'update100x')
ARMS = ('v41', 'v311')
BASE = ('NoSE', 'QPG', 'V8_AUTO_H_norm')


def _v2():
    p = VA / 'rubis_v9/rubis_v1'
    for x in (REPO, VA / 'common', p):
        if str(x) not in sys.path:
            sys.path.insert(0, str(x))
    return importlib.import_module('analyze')


def legacy_nnls(H):
    """RETROSPECTIVE_BASELINE_ONLY: V9 E14 NNLS model (a/b/c) applied to the V3 HBench after the campaign."""
    from schemalens_v9 import compose
    C = {t['canonical_id']: t for t in json.loads((REPO / 'runs/v9_generation_001/stages/E13_route_synthesis/CANONICAL_WITH_COVER.json').read_text())['tables']}
    return {h['schema_id']: compose.predicted([C[t] for t in h['canonical_ids']])['per_mix'] for h in H}


def run(run_dir=None):
    reg = frozen('rubis')
    A = _v2()
    run_dir = run_dir or OUT / 'rubis/benchmark/run1'
    man = read(run_dir / 'manifest.json')
    hreg = read(run_dir.parent / f'{run_dir.name}_HARNESS_REGISTRATION.json')
    deps = hreg['deployments_base_order']
    H = [x['schema_id'] for x in reg['hbench_v3_schemas']]
    v3id = {x['schema_id']: x['v3_deployment_id'] for x in reg['hbench_v3_schemas']}
    _, _, tb, hb = v3_space('rubis')
    tbH = {h: tb[v3id[h]] for h in H}
    leg = legacy_nnls(hreg['v3_h'])
    summ, con, within, floors = [], [], {}, {}
    for arm in ARMS:
        raw, errs = A.load_raw(sorted((run_dir / arm / 'raw').glob('*.jsonl.gz')))
        if errs:
            raise AssertionError(f'{arm}: {errs} request errors')
        cube = S.index(raw); del raw
        for w in WORKLOADS:
            bt = A.boot_workload(cube, deps, w)
            for d in deps:
                allv = np.concatenate([cube[d][w][r] for r in sorted(cube[d][w])])
                ds = S.describe(allv); lo, hi = S.ci(bt['samples'][d])
                summ.append({'arm': arm, 'deployment': d, 'v3_deployment_id': v3id.get(d, ''), 'workload': w, 'mean_ms': ds['mean_ms'],
                             'ci_lo': lo, 'ci_hi': hi, 'p50_ms': ds['p50_ms'], 'p95_ms': ds['p95_ms'], 'p99_ms': ds['p99_ms'], 'n': ds['n']})
            fl = S.noise_floor(bt, [('V3H01', 'V3H01_AA')]); floors[(arm, w)] = fl
            f = fl['noise_floor_abs_ln']
            for ref in BASE:
                for d in H:
                    pr = S.paired_ratio(bt, d, ref)
                    con.append({'arm': arm, 'workload': w, 'a': d, 'b': ref, 'ratio': pr['ratio'], 'ci_lo': pr['ci_lo'], 'ci_hi': pr['ci_hi'],
                                'verdict': S.verdict(pr, f)})
            m = {h: {'read': bt['point'][h], 'storage': 0, 'write': 0} for h in H}
            best = min(H, key=lambda h: m[h]['read'])
            tied = {best} | {h for h in H if h != best and S.verdict(S.paired_ratio(bt, h, best), f) == 'EMPATE'}
            ev = M.evaluate(m, sorted(H, key=lambda h: tbH[h]), tbH, tied)
            lp = {h: leg[h][w]['total'] for h in H}
            evl = M.evaluate(m, sorted(H, key=lambda h: lp[h]), lp, tied)
            within[(arm, w)] = {'measured_best': best, 'tied_with_best': sorted(tied), 'noise_floor_pct': fl['noise_floor_pct'],
                                'v3_order': sorted(H, key=lambda h: tbH[h]), 'measured_order': sorted(H, key=lambda h: m[h]['read']),
                                'v3_tiebreak_tau': ev['kendall_tau_score_vs_read'], 'v3_top1_strict': ev['top1_strict'],
                                'v3_top1_noise': ev['top1_within_noise'], 'v3_rank1_regret': m[ev['measured_best']]['read'] and m[sorted(H, key=lambda h: tbH[h])[0]]['read'] / m[best]['read'] - 1,
                                'legacy_nnls_RETROSPECTIVE_BASELINE_ONLY': {'order': sorted(H, key=lambda h: lp[h]), 'tau': evl['kendall_tau_score_vs_read'],
                                                                            'top1_noise': evl['top1_within_noise'], 'predicted_total_ms': lp}}
    vd = {(c['arm'], c['workload'], c['a'], c['b']): c['verdict'] for c in con}
    key3 = ('browsing', 'bidding', 'update10x')
    bestH = {w: within[('v41', w)]['measured_best'] for w in WORKLOADS}
    m41 = {(r['deployment'], r['workload']): r['mean_ms'] for r in summ if r['arm'] == 'v41'}
    m311 = {(r['deployment'], r['workload']): r['mean_ms'] for r in summ if r['arm'] == 'v311'}
    tau_v = {w: M.kendall_tau([m311[(d, w)] for d in deps], [m41[(d, w)] for d in deps]) for w in WORKLOADS}
    hyp = {'R1': {'result': all(vd[('v41', w, bestH[w], b)] == 'A_MAIS_RAPIDO' for w in key3 for b in ('NoSE', 'QPG')),
                  'detail': '; '.join(f"{w}: best={bestH[w]} vs {b}={vd[('v41', w, bestH[w], b)]}" for w in key3 for b in ('NoSE', 'QPG'))},
           'R2': {'result': all(vd[('v311', w, bestH[w], b)] == vd[('v41', w, bestH[w], b)] for w in key3 for b in ('NoSE', 'QPG')),
                  'detail': 'tau 3.11 x 4.1 (9 deployments): ' + ', '.join(f'{w}={t:.2f}' for w, t in tau_v.items())}}
    agr = man.get('agreement_with_v9_oracle', {})
    hyp['ORACLE'] = {'result': bool(agr) and all(v['status'] == 'PASS' for x in agr.values() for v in x.values()),
                     'detail': '; '.join(f"{arm}: {sum(v['agree'] for v in x.values())}/{sum(v['statement_results'] for v in x.values())}" for arm, x in agr.items())}
    v2 = read_csv(VA / 'rubis_v9/outputs/v1_run1/analysis/summary.csv')
    v2m = {(r['arm'], r['deployment'], r['workload']): float(r['mean_ms']) for r in v2}
    same = {x['schema_id']: x['same_tables_as_v2'] for x in reg['hbench_v3_schemas'] if x['same_tables_as_v2']}
    repro = [{'arm': a, 'workload': w, 'v3': h, 'v2': o, 'v3_mean_ms': m41[(h, w)] if a == 'v41' else m311[(h, w)], 'v2_mean_ms': v2m[(a, o, w)],
              'ratio_v3_over_v2_session': (m41[(h, w)] if a == 'v41' else m311[(h, w)]) / v2m[(a, o, w)]}
             for a in ARMS for w in WORKLOADS for h, o in same.items()] + \
            [{'arm': a, 'workload': w, 'v3': b, 'v2': b, 'v3_mean_ms': (m41 if a == 'v41' else m311)[(b, w)], 'v2_mean_ms': v2m[(a, b, w)],
              'ratio_v3_over_v2_session': (m41 if a == 'v41' else m311)[(b, w)] / v2m[(a, b, w)]} for a in ARMS for w in WORKLOADS for b in BASE]
    out = OUT / 'rubis/analysis'
    write_csv(out / 'summary.csv', summ); write_csv(out / 'contrasts.csv', con); write_csv(out / 'v2_vs_v3_results.csv', repro)
    abl = ablation_table('rubis', {v3id[h]: {'read': m41[(h, 'bidding')], 'storage': 0, 'write': 0} for h in H}, {v3id[bestH['bidding']]})
    write_csv(out / 'ablations.csv', abl)
    res = {'within_hbench': {f'{a}/{w}': v for (a, w), v in within.items()}, 'hypotheses': hyp, 'tau_311_vs_41': tau_v,
           'noise_floor_pct': {f'{a}/{w}': f['noise_floor_pct'] for (a, w), f in floors.items()},
           'legacy_nnls_label': 'RETROSPECTIVE_BASELINE_ONLY', 'denominator': 'HBench V3 (5) + 3 baselines + A/A; the 256 complete deployments are not measured'}
    dump(out / 'metrics.json', res)
    return res, summ, con, repro, abl
