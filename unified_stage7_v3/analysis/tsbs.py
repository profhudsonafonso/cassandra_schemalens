"""TSBS V3 analysis: full verification campaign (24 V3 complete deployments + TSBS original + A/A)."""
from __future__ import annotations

from collections import defaultdict

import numpy as np

from .common import B, OUT, SEED, S, VA, ablation_table, dump, evaluate, frozen, pre, read, read_csv, tied_set, v3_space, write_csv


def run(run_dir=None):
    reg = frozen('tsbs')
    run_dir = run_dir or OUT / 'tsbs/benchmark/run1'
    man = read(run_dir / 'manifest.json')
    raw = read_csv(run_dir / 'raw_latencies.csv')
    if any(r['oracle_ok'] != 'True' for r in raw):
        raise AssertionError('raw row without oracle pass')
    cube = S.index(raw)
    variants = reg['variants']
    labels = [v['label'] for v in variants]
    covers = [v for v in variants if v['kind'] == 'cover']
    lab2v3 = {v['label']: v['v3_deployment_id'] for v in covers}
    v3lab = {v: k for k, v in lab2v3.items()}
    qN = sorted({r['query_id'] for r in raw if r['query_id'].startswith('N')})
    qA = sorted({r['query_id'] for r in raw if r['query_id'].startswith('A')})
    load = {r['variant']: r for r in read_csv(run_dir / 'load.csv')}
    stor = defaultdict(int)
    for r in read_csv(run_dir / 'storage_tables.csv'):
        stor[r['keyspace']] += int(r['space_used_live_bytes'])
    space, tags, tb, hb = v3_space('tsbs')
    out = OUT / 'tsbs/analysis'
    res, summ = {}, []
    for coh, Q in (('natural', qN), ('aligned', qA)):
        boot = S.bootstrap_mix(cube, labels, Q, B=B, seed=SEED)
        boot95 = S.bootstrap_mix(cube, labels, Q, B=B, seed=SEED, stat='p95')
        floor = S.noise_floor(boot, [tuple(p) for p in reg['aa_pairs']])
        res[coh] = (boot, boot95, floor, Q)
    for v in labels:
        row = {'variant': v, 'v3_deployment_id': lab2v3.get(v, v), 'in_hbench_v3': lab2v3.get(v) in hb}
        for coh, (boot, boot95, floor, Q) in res.items():
            lo, hi = S.ci(boot['samples'][v])
            allv = np.concatenate([cube[v][q][r] for q in Q for r in cube[v][q]])
            row.update({f'{coh}_mix_mean_ms': boot['point'][v], f'{coh}_ci_lo': lo, f'{coh}_ci_hi': hi,
                        f'{coh}_mix_p95_ms': boot95['point'][v], f'{coh}_p50_ms': S.nearest_rank(allv, 50),
                        f'{coh}_p95_ms': S.nearest_rank(allv, 95), f'{coh}_p99_ms': S.nearest_rank(allv, 99)})
        ks = load[v]['keyspace']
        row.update({'storage_bytes': stor[ks], 'load_seconds': float(load[v]['load_seconds']),
                    'physical_writes': int(load[v]['physical_writes']), 'writes_per_second': float(load[v]['writes_per_second'])})
        summ.append(row)
    write_csv(out / 'variant_summary.csv', summ)
    sd = {s['variant']: s for s in summ}
    metrics, abl, raw_out = {}, [], []
    for coh, (boot, _, floor, _) in res.items():
        fl = floor['noise_floor_abs_ln']
        measured = {lab2v3[l]: {'read': boot['point'][l], 'storage': sd[l]['storage_bytes'], 'write': sd[l]['load_seconds']} for l in lab2v3}
        best = min(measured, key=lambda i: measured[i]['read'])
        bootv3 = {'point': {lab2v3.get(k, k): x for k, x in boot['point'].items()}, 'samples': {lab2v3.get(k, k): x for k, x in boot['samples'].items()}}
        tied = tied_set(bootv3, list(measured), best, fl)
        ev = evaluate(measured, hb, {i: tb[i] for i in measured}, tied)
        base = 'B_tsbs_series_bigint'
        pr = S.paired_ratio(boot, v3lab[hb[0]], base)
        best_h = min(hb, key=lambda i: measured[i]['read'])
        pr2 = S.paired_ratio(boot, v3lab[best_h], base)
        ev.update({'noise_floor_pct': floor['noise_floor_pct'], 'denominator': f'{len(measured)} V3 complete deployments (full verification)',
                   'rank1_vs_tsbs_original': {**pr, 'verdict': S.verdict(pr, fl)},
                   'best_hbench_vs_tsbs_original': {'deployment': best_h, **pr2, 'verdict': S.verdict(pr2, fl)}})
        metrics[coh] = ev
        for r in ablation_table('tsbs', measured, tied):
            abl.append({'cohort': coh, **r})
    dump(out / 'metrics.json', metrics)
    write_csv(out / 'ablations.csv', abl)
    dump(out / 'run_facts.json', {'raw_rows': len(raw), 'classes': {'natural': qN, 'aligned': qA}, 'repetitions': len(man['repetitions']),
                                  'isolation_all_passed': all(r['isolation_guard']['passed'] for r in man['repetitions']),
                                  'image': man['preflight'].get('image'), 'cassandra': man['preflight'].get('cassandra_release_version')})
    return metrics, summ, abl
