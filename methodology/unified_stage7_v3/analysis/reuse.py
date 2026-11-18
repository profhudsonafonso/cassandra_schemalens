"""IoT / TPCx V3 analysis: re-analysis of the V2 raw measurements (D-V3-001) with the frozen HBench V3.
Recomputed from the raw files with the same statistics code (slstats/method_eval, B=2000, seed 20261117)."""
from __future__ import annotations

from .common import REPO, B, OUT, SEED, S, V2, ablation_table, dump, evaluate, frozen, read, read_csv, tied_set, v3_space, write_csv

CFG = {
    'iot': dict(dir=V2['iot'], prereg='PREREGISTRATION_IOT_V1.json', raw='raw_latencies.csv', score='preselection_score'),
    'tpcx_iot': dict(dir=V2['tpcx_iot'], prereg='PREREGISTRATION_TPCX_V1.json', raw='raw_reads.csv', score='normalized_proxy_score'),
}


def run(b):
    reg3 = frozen(b)
    c = CFG[b]
    reg = read(c['dir'] / c['prereg'])
    run_dir = c['dir'] / 'outputs/v1_run1'
    raw = read_csv(run_dir / c['raw'])
    if any(r['oracle_ok'] != 'True' for r in raw):
        raise AssertionError('raw without oracle pass')
    if b == 'tpcx_iot':
        raw = [{'variant': r['variant'], 'query_id': 'Q_READ', 'rep': r['rep'], 'round': r['query_index'], 'latency_ms': r['latency_ms']} for r in raw]
        Q = ['Q_READ']
        vs = {r['variant']: r for r in read_csv(run_dir / 'analysis/variant_summary.csv')}
        cost = {k: (float(v['storage_bytes_mean']), float(v['ingest_seconds_mean'])) for k, v in vs.items()}
    else:
        Q = sorted({r['query_id'] for r in raw}, key=lambda q: int(q[1:]))
        dc = {r['variant']: r for r in read_csv(run_dir / 'analysis/deployment_cost.csv')}
        cost = {k: (float(v['storage_live_bytes']), float(v['load_seconds'])) for k, v in dc.items()}
    cube = S.index(raw)
    labels = [v['label'] for v in reg['variants']]
    boot = S.bootstrap_mix(cube, labels, Q, B=B, seed=SEED)
    floor = S.noise_floor(boot, [tuple(p) for p in reg['aa_pairs']]); fl = floor['noise_floor_abs_ln']
    space, tags, tb, hb = v3_space(b)
    m3 = reg3['v3_to_v2_variant']                                   # V3 id -> V2 label
    measured = {i: {'read': boot['point'][m3[i]], 'storage': cost[m3[i]][0], 'write': cost[m3[i]][1]} for i in space}
    bootv3 = {'point': {i: boot['point'][m3[i]] for i in space}, 'samples': {i: boot['samples'][m3[i]] for i in space}}
    best = min(measured, key=lambda i: measured[i]['read'])
    tied = tied_set(bootv3, list(measured), best, fl)
    ev3 = evaluate(measured, hb, {i: tb[i] for i in measured}, tied)
    inv = {v: k for k, v in m3.items()}
    covers = {v['label']: v for v in reg['variants'] if v.get('kind') == 'cover'}
    old_hb = [inv[h] for h in reg['hbench_ids_ranked'] if h in inv]
    old_score = {inv[l]: covers[l][c['score']] for l in covers if l in inv}
    ev2_same = evaluate(measured, old_hb, old_score, tied)
    ev2_reported = read(run_dir / 'analysis/method_eval.json')
    abl = ablation_table(b, measured, tied)
    out = OUT / b / 'analysis'
    rows = [{'v3_deployment_id': i, 'v2_label': m3[i], 'in_hbench_v3': i in hb, 'in_hbench_v2': i in old_hb,
             'mean_ms': measured[i]['read'], 'ci_lo': S.ci(bootv3['samples'][i])[0], 'ci_hi': S.ci(bootv3['samples'][i])[1],
             'storage_bytes': measured[i]['storage'], 'write_seconds': measured[i]['write'], 'tied_with_best': i in tied,
             'balanced_structural_tiebreak': tb[i], 'legacy_v2_score': old_score.get(i)} for i in sorted(space, key=lambda i: measured[i]['read'])]
    write_csv(out / 'v2_vs_v3_results.csv', rows)
    write_csv(out / 'ablations.csv', abl)
    res = {'v3': ev3, 'v2_hbench_on_v3_denominator': ev2_same, 'v2_reported': ev2_reported, 'noise_floor_pct': floor['noise_floor_pct'],
           'denominator': f'{len(space)} V3 complete deployments, all measured in V2 verification mode', 'source_raw': str((run_dir / c['raw']).relative_to(REPO))}
    dump(out / 'metrics.json', res)
    return res, rows, abl
