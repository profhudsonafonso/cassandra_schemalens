"""Passo 3 - analise v1 do TPCx-IoT recalculada das latencias brutas."""
from __future__ import annotations

import argparse
import math
from collections import defaultdict
from pathlib import Path

import numpy as np

from common_paths import (AA_SUFFIX, BASELINE_ID, PREREG, ROOT, SNAP, analysis_code_hashes, csv_x, dump, read,
                          read_csv)
import slstats as S
import method_eval as M

B, SEED = 2000, 20261117
TAB = {'ts0_series_raw': 'TS0', 'ts1_series_3600s': 'TS1h', 'ts1_series_60s': 'TS1m',
       'ts3_sensor_3600s': 'TS3h', 'ts3_sensor_60s': 'TS3m', BASELINE_ID: 'BASE'}


def names(reg):
    n = {}
    for v in reg['variants']:
        base = '+'.join(TAB[t] for t in v['tables'])
        n[v['label']] = base + (' (A/A)' if v['kind'] == 'aa_replica' else '')
    return n


def tstat(vals):
    v = np.array(vals, float); k = v.size
    tc = {2: 12.706, 3: 4.303, 4: 3.182, 5: 2.776, 6: 2.571}.get(k, 1.96)
    h = tc * v.std(ddof=1) / math.sqrt(k) if k > 1 else float('nan')
    return float(v.mean()), float(v.mean() - h), float(v.mean() + h)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True, type=Path)
    ap.add_argument('--figs', type=Path, default=ROOT / 'relatorio_overleaf/figs')
    a = ap.parse_args()
    run = (ROOT / a.run) if not a.run.is_absolute() else a.run
    out = run / 'analysis'; out.mkdir(exist_ok=True)
    reg = read(PREREG); man = read(run / 'manifest.json'); nm = names(reg)
    raw = read_csv(run / 'raw_reads.csv')
    if any(r['oracle_ok'] != 'True' for r in raw):
        raise AssertionError('raw read without oracle pass')
    cube = S.index([{'variant': r['variant'], 'query_id': 'Q_READ', 'rep': r['rep'], 'round': r['query_index'],
                     'latency_ms': r['latency_ms']} for r in raw])
    labels = [v['label'] for v in reg['variants']]
    covers = [v for v in reg['variants'] if v['kind'] == 'cover']
    Q = ['Q_READ']
    runs = [r for r in man['runs'] if r['isolation_guard']['passed']]
    byv = defaultdict(list)
    for r in runs:
        byv[r['variant']].append(r)
    boot = S.bootstrap_mix(cube, labels, Q, B=B, seed=SEED)
    boot95 = S.bootstrap_mix(cube, labels, Q, B=B, seed=SEED, stat='p95')
    floor = S.noise_floor(boot, [tuple(p) for p in reg['aa_pairs']]); fl = floor['noise_floor_abs_ln']
    summ = []
    for v in labels:
        allv = np.concatenate([cube[v]['Q_READ'][r] for r in sorted(cube[v]['Q_READ'])])
        d = S.describe(allv); lo, hi = S.ci(boot['samples'][v]); lo95, hi95 = S.ci(boot95['samples'][v])
        rl = S.rep_level(cube, v, Q)
        ev = tstat([r['events_per_second'] for r in byv[v]])
        st = tstat([r['storage_live_bytes'] for r in byv[v]])
        ing = tstat([r['ingest_seconds'] for r in byv[v]])
        summ.append({'variant': v, 'name': nm[v], 'read_mean_ms': d['mean_ms'], 'read_mean_ci_lo': lo, 'read_mean_ci_hi': hi,
                     'read_p50_ms': d['p50_ms'], 'read_p95_ms': d['p95_ms'], 'read_p95_ci_lo': lo95, 'read_p95_ci_hi': hi95,
                     'read_p99_ms': d['p99_ms'], 'rep_t_ci_lo': rl['t_ci_lo'], 'rep_t_ci_hi': rl['t_ci_hi'],
                     'cv_between_reps_pct': rl['cv_between_reps_pct'], 'n_reads': d['n'],
                     'ingest_eps_mean': ev[0], 'ingest_eps_ci_lo': ev[1], 'ingest_eps_ci_hi': ev[2],
                     'ingest_seconds_mean': ing[0], 'storage_bytes_mean': st[0], 'storage_bytes_ci_lo': st[1], 'storage_bytes_ci_hi': st[2],
                     'cql_requests_per_read': ';'.join(sorted({r['cql_requests'] for r in raw if r['variant'] == v})),
                     'reads_during_ingestion_pct': 100 * sum(r['completed_during_ingestion'] == 'True' for r in raw if r['variant'] == v) / d['n']})
    csv_x(out / 'variant_summary.csv', summ, mode='w')
    sd = {s['variant']: s for s in summ}
    # ingest A/A noise
    ing_aa = []
    for a_, b_ in reg['aa_pairs']:
        ra = sd[a_]['ingest_eps_mean'] / sd[b_]['ingest_eps_mean']
        ing_aa.append({'a': a_, 'b': b_, 'ingest_ratio': ra, 'storage_ratio': sd[a_]['storage_bytes_mean'] / sd[b_]['storage_bytes_mean']})
    ing_floor = max(abs(math.log(x['ingest_ratio'])) for x in ing_aa)
    rank1 = reg['hbench_ids_ranked'][0]
    con = []
    for ref in (rank1, BASELINE_ID):
        for v in labels:
            if v == ref or v.endswith(AA_SUFFIX):
                continue
            pr = S.paired_ratio(boot, v, ref)
            ir = sd[v]['ingest_eps_mean'] / sd[ref]['ingest_eps_mean']
            con.append({'a': v, 'a_name': nm[v], 'b': ref, 'b_name': nm[ref], 'read_ratio': pr['ratio'], 'ci_lo': pr['ci_lo'],
                        'ci_hi': pr['ci_hi'], 'read_verdict': S.verdict(pr, fl), 'ingest_ratio': ir,
                        'ingest_verdict': 'EMPATE' if abs(math.log(ir)) < ing_floor else ('A_MAIS_RAPIDO' if ir > 1 else 'B_MAIS_RAPIDO'),
                        'storage_ratio': sd[v]['storage_bytes_mean'] / sd[ref]['storage_bytes_mean']})
    csv_x(out / 'contrasts.csv', con, mode='w')
    # Stage 7/8 structural prediction vs observed
    s79 = []
    for v in covers:
        obs = sorted({int(r['cql_requests']) for r in raw if r['variant'] == v['label']})
        inserts = {r['physical_inserts'] / r['source_events'] for r in byv[v['label']]}
        s79.append({'deployment': v['label'], 'name': nm[v['label']], 'pred_read_cql_requests': v['objectives']['read_cql_requests'],
                    'obs_cql_requests_per_read': ';'.join(map(str, obs)),
                    'pred_inserts_per_event': v['objectives']['insert_statements_per_event'],
                    'obs_inserts_per_event': ';'.join(f'{x:g}' for x in sorted(inserts)),
                    'status_inserts': 'MATCH' if inserts == {float(v['objectives']['insert_statements_per_event'])} else 'MISMATCH',
                    'status_cql': 'MATCH' if obs == [v['objectives']['read_cql_requests']] else 'MISMATCH'})
    csv_x(out / 'stage7_vs_stage9.csv', s79, mode='w')
    # proportionality of CQL requests: the proxy is computed for the modelled scale, measurements use 2 stations
    # method evaluation
    measured = {v['label']: {'read': sd[v['label']]['read_mean_ms'], 'storage': sd[v['label']]['storage_bytes_mean'],
                             'write': sd[v['label']]['ingest_seconds_mean']} for v in covers}
    best = min(measured, key=lambda i: measured[i]['read'])
    tied = {best} | {i for i in measured if i != best and S.verdict(S.paired_ratio(boot, i, best), fl) == 'EMPATE'}
    score = {v['label']: v['normalized_proxy_score'] for v in covers}
    ev = M.evaluate(measured, reg['hbench_ids_ranked'], score, tied)
    ev['random_k'] = M.random_k_expected(measured, len(reg['hbench_ids_ranked']))
    ev['names'] = {k: nm[k] for k in measured}
    # 2D Pareto used in Stage 9.1 (read, ingest) for comparison
    ev['measured_pareto_read_write'] = M.measured_pareto(measured, ('read', 'write'))
    ev['pareto_retention_read_write'] = len(set(ev['measured_pareto_read_write']) & set(reg['hbench_ids_ranked'])) / len(ev['measured_pareto_read_write'])
    dump(out / 'method_eval.json', ev)
    dump(out / 'measured_space.json', {'measured': measured, 'tied_with_best': sorted(tied), 'noise_floor_abs_ln': fl})
    # legacy Stage 9.1 verification (medians of per-run p95; no raw latencies were kept)
    leg = []
    lp = SNAP / 'outputs/stage91_verification/82_stage91_verification_summary.csv'
    if lp.exists():
        for r in read_csv(lp):
            did = r['deployment_id']
            if did in sd:
                leg.append({'variant': did, 'name': nm[did], 'old_median_p95_ms': float(r['median_read_p95_ms']),
                            'old_median_ingest_eps': float(r['median_ingest_events_per_s']), 'old_storage': int(r['median_storage_live_bytes']),
                            'new_mean_ms': sd[did]['read_mean_ms'], 'new_p95_ms': sd[did]['read_p95_ms'],
                            'new_ingest_eps': sd[did]['ingest_eps_mean'], 'new_storage': sd[did]['storage_bytes_mean']})
        csv_x(out / 'legacy_stage91.csv', leg, mode='w')
        common = [x['variant'] for x in leg if x['variant'] in measured]
        ev2 = {'kendall_old_p95_vs_new_mean': M.kendall_tau([next(l['old_median_p95_ms'] for l in leg if l['variant'] == c) for c in common],
                                                           [measured[c]['read'] for c in common]),
               'kendall_old_p95_vs_new_p95': M.kendall_tau([next(l['old_median_p95_ms'] for l in leg if l['variant'] == c) for c in common],
                                                          [sd[c]['read_p95_ms'] for c in common]),
               'old_best_p95': min(leg, key=lambda l: l['old_median_p95_ms'])['variant']}
        dump(out / 'legacy_compare.json', ev2)
    # hypotheses
    ts3 = [v['label'] for v in covers if v['read_table'].startswith('ts3_')]
    direct = [v['label'] for v in covers if not v['read_table'].startswith('ts3_')]
    one = [v['label'] for v in covers if len(v['tables']) == 1]; two = [v['label'] for v in covers if len(v['tables']) == 2]
    cmat = {}
    for x in ts3:
        for y in direct:
            cmat[(x, y)] = S.verdict(S.paired_ratio(boot, x, y), fl)
    h2 = all(vv == 'A_MAIS_RAPIDO' for vv in cmat.values())
    h3 = min(sd[x]['ingest_eps_ci_lo'] for x in one) > max(sd[y]['ingest_eps_ci_hi'] for y in two)
    hyp = {
        'H1': {'result': bool(set(reg['hbench_ids_ranked']) & tied), 'detail': f"melhor={nm[best]}; empatados={[nm[t] for t in sorted(tied)]}; regret={ev['regret']:.4f}"},
        'H2': {'result': h2, 'detail': f"{sum(v_ == 'A_MAIS_RAPIDO' for v_ in cmat.values())}/{len(cmat)} pares TS3-leitura mais rapidos que leitura direta"},
        'H3': {'result': h3, 'detail': f"min IC inf 1 tabela={min(sd[x]['ingest_eps_ci_lo'] for x in one):.1f} ev/s; max IC sup 2 tabelas={max(sd[y]['ingest_eps_ci_hi'] for y in two):.1f} ev/s"},
        'H4': {'result': ev['pareto_retention'] == 1.0, 'detail': f"Pareto medido={[nm[p] for p in ev['measured_pareto']]}; retencao={ev['pareto_retention']:.2f}"},
        'H5': {'result': all(r['status_cql'] == 'MATCH' for r in s79), 'detail': '; '.join(f"{r['name']}: prev {r['pred_read_cql_requests']} obs {r['obs_cql_requests_per_read']}" for r in s79)},
    }
    dump(out / 'hypotheses.json', hyp)
    dump(out / 'analysis.json', {'noise_floor': floor, 'ingest_aa': ing_aa, 'ingest_noise_floor_abs_ln': ing_floor,
                                 'rank1': rank1, 'best': best, 'raw_reads': len(raw), 'runs_accepted': len(runs),
                                 'runs_total': len(man['runs']), 'bootstrap_resamples': B, 'seed': SEED,
                                 'analysis_code_sha256': analysis_code_hashes()})
    figures(a.figs, summ, rank1, fl)
    print('noise floor read %:', floor['noise_floor_pct'], 'ingest %:', (math.exp(ing_floor) - 1) * 100)
    for s in sorted(summ, key=lambda s: s['read_mean_ms']):
        print(f"{s['name']:>16} mean={s['read_mean_ms']:.3f} [{s['read_mean_ci_lo']:.3f},{s['read_mean_ci_hi']:.3f}] p95={s['read_p95_ms']:.3f} ev/s={s['ingest_eps_mean']:.1f} bytes={s['storage_bytes_mean']:.0f} cql={s['cql_requests_per_read']}")
    print({k: v['result'] for k, v in hyp.items()})
    print({k: ev[k] for k in ('measured_best', 'top1_strict', 'top1_within_noise', 'top3', 'near_best_5pct', 'regret', 'pareto_retention', 'kendall_tau_score_vs_read', 'measured_pareto')})


def figures(figdir, summ, rank1, fl):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    figdir.mkdir(parents=True, exist_ok=True)
    order = sorted(summ, key=lambda s: s['read_mean_ms'])
    fig, ax = plt.subplots(1, 2, figsize=(9, 3.8), sharey=True)
    y = np.arange(len(order))
    ax[0].barh(y, [s['read_mean_ms'] for s in order], color=['#2a7f62' if s['variant'] == rank1 else ('#b07a2a' if 'BASE' in s['name'] else '#7a8fa6') for s in order])
    ax[0].errorbar([s['read_mean_ms'] for s in order], y, xerr=[[s['read_mean_ms'] - s['read_mean_ci_lo'] for s in order],
                                                               [s['read_mean_ci_hi'] - s['read_mean_ms'] for s in order]], fmt='none', ecolor='k', capsize=2)
    ax[0].set_yticks(y); ax[0].set_yticklabels([s['name'] for s in order], fontsize=8); ax[0].invert_yaxis()
    ax[0].set_xlabel('Leitura: média (ms), IC 95% bootstrap')
    ax[1].barh(y, [s['ingest_eps_mean'] for s in order], color='#7a8fa6')
    ax[1].errorbar([s['ingest_eps_mean'] for s in order], y, xerr=[[s['ingest_eps_mean'] - s['ingest_eps_ci_lo'] for s in order],
                                                                  [s['ingest_eps_ci_hi'] - s['ingest_eps_mean'] for s in order]], fmt='none', ecolor='k', capsize=2)
    ax[1].set_xlabel('Ingestão: eventos/s (IC 95% t, 5 rep.)')
    fig.tight_layout(); fig.savefig(figdir / 'fig_v1_tpcx.pdf'); plt.close(fig)


if __name__ == '__main__':
    main()
