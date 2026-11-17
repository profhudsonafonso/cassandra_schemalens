#!/usr/bin/env python3
"""SchemaLens Rev4 (Fase 2, E2): calibrated pre-benchmark predictor of TS2-U vs TS2-D.

    python fase2_rev4.py fit            # once; writes rev4/rev4_frozen.json (refuses to overwrite)
    python fase2_rev4.py predict e3     # after E3 validation+flush, BEFORE E3 timing
    python fase2_rev4.py predict e4

Pre-specified model (PROTOCOLO_FASE2.md, E2), fitted by ordinary least squares on the
development set (temporal cases of S2, S3 R1 and E1 r2/r3, variants U and D):

    ln(p95_ms) = t0 + t1*ln(calls) + t2*ln(rows+1) + t3*ln(partition_mean_bytes) + t4*[sst]

Inputs at prediction time come only from semantic validation (calls, rows) and the
post-flush storage snapshot (partition size): no latency of the predicted campaign is read.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
import sys
import datetime as dt
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
EXP = PROJECT / 'experiments'
F2 = EXP / 'rev3_fase2'
OUT = F2 / 'rev4'
HERE = Path(__file__).resolve()
U, D = 'R3H_38939db96444', 'R3H_d2ea1974de53'
SHORTV = {U: 'u', D: 'd'}
TEMPORAL = ('Q2', 'Q7', 'Q9', 'Q12')
TERMS = ('intercept', 'ln_calls', 'ln_rows_plus_1', 'ln_partition_mean_bytes', 'sst')


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def rcsv(p):
    with open(p, newline='') as f:
        return list(csv.DictReader(f))


def jload(p):
    return json.loads(Path(p).read_text())


def features(calls, rows, part, sst):
    return [1.0, math.log(calls), math.log(rows + 1), math.log(part), 1.0 if sst else 0.0]


def solve(X, y):
    """OLS via normal equations + Gauss-Jordan with partial pivoting (k=5)."""
    k = len(X[0])
    A = [[sum(r[i] * r[j] for r in X) for j in range(k)] + [sum(r[i] * t for r, t in zip(X, y))] for i in range(k)]
    for c in range(k):
        p = max(range(c, k), key=lambda r: abs(A[r][c]))
        A[c], A[p] = A[p], A[c]
        if abs(A[c][c]) < 1e-12:
            raise ValueError('singular design matrix')
        for r in range(k):
            if r != c:
                f = A[r][c] / A[c][c]
                A[r] = [a - f * b for a, b in zip(A[r], A[c])]
    return [A[i][k] / A[i][i] for i in range(k)]


def part_sizes_s3(f):
    s = jload(EXP / 'rev3_s3_physical_storage' / f'campaign_r1/{f}x/storage_pre_timing/summary.json')
    return {SHORTV[l]: s['variants'][l]['metrics']['partition_mean_bytes'] for l in (U, D)}


def part_sizes_f2(campaign, ds):
    s = jload(F2 / campaign / ds / 'storage_post_flush' / 'summary.json')
    return {v: s['variants'][f'sst:TS2-{v.upper()}']['metrics']['partition_mean_bytes'] for v in ('u', 'd')}


def dev_rows():
    """Yield (source, group_key, variant, calls, rows, part, sst, p95)."""
    out = []
    for f in (1, 2, 4, 8):
        parts = part_sizes_s3(f)
        for src, path, sst in (('S2', EXP / 'rev3_s2_partition_growth' / f's2_benchmark/{f}x/query_p95.csv', False),
                               ('S3r1', EXP / 'rev3_s3_physical_storage' / f'campaign_r1/{f}x/benchmark/query_p95.csv', True)):
            for r in rcsv(path):
                if r['query_id'] in TEMPORAL and r['variant'] in (U, D):
                    v = SHORTV[r['variant']]
                    out.append((src, (src, f, r['case_id']), v, int(r['execute_calls']), int(r['iterated_rows']),
                                parts[v], sst, float(r['p95_ms'])))
        for rep in ('e1r2b', 'e1r3'):
            ds = f'd{f}'
            parts2 = part_sizes_f2(rep, ds)
            for r in rcsv(F2 / rep / ds / 'benchmark' / 'query_p95.csv'):
                if r['query_id'] in TEMPORAL and r['variant'] in (U, D):
                    v = SHORTV[r['variant']]
                    out.append((rep, (rep, f, r['cond'], r['case_id']), v, int(r['execute_calls']),
                                int(r['iterated_rows']), parts2[v], r['cond'] == 'sst', float(r['p95_ms'])))
    return out


def predict_ln(theta, calls, rows, part, sst):
    return sum(t * x for t, x in zip(theta, features(calls, rows, part, sst)))


def cmd_fit():
    OUT.mkdir(parents=True, exist_ok=True)
    target = OUT / 'rev4_frozen.json'
    if target.exists():
        raise FileExistsError(target)
    for c in ('e3', 'e4'):
        for p in (F2 / c).glob('*/benchmark'):
            raise ValueError(f'holdout timing already exists ({p}); Rev4 can no longer be fitted leakage-free')
    rows = dev_rows()
    X = [features(c, r, p, s) for _, _, _, c, r, p, s, _ in rows]
    y = [math.log(t) for *_, t in rows]
    theta = solve(X, y)
    pred = [sum(a * b for a, b in zip(theta, x)) for x in X]
    ybar = statistics.mean(y)
    r2 = 1 - sum((a - b) ** 2 for a, b in zip(y, pred)) / sum((a - ybar) ** 2 for a in y)
    groups = {}
    for (src, key, v, c, r, p, s, t) in rows:
        groups.setdefault(key, {})[v] = (t, predict_ln(theta, c, r, p, s))
    hits = sum(1 for g in groups.values() if (g['d'][0] < g['u'][0]) == (g['d'][1] < g['u'][1]))
    by_src = {}
    for src, *_ in rows:
        by_src[src] = by_src.get(src, 0) + 1
    rec = dict(status='REV4_FROZEN_BEFORE_HOLDOUT_TIMING', created_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
               model='ln(p95_ms) = t0 + t1*ln(calls) + t2*ln(rows+1) + t3*ln(partition_mean_bytes) + t4*[sst]',
               coefficients=dict(zip(TERMS, [round(t, 6) for t in theta])), n_dev=len(rows), dev_sources=by_src,
               r2=round(r2, 4), dev_winner_accuracy=f'{hits}/{len(groups)}', code_sha256=sha(HERE),
               decision_rule='predict TS2-D if predicted p95_D < predicted p95_U, else TS2-U',
               inputs_at_prediction='execute_calls/iterated_rows from semantic validation; partition_mean_bytes from post-flush tablestats')
    with target.open('x') as f:
        json.dump(rec, f, indent=2)
    print(json.dumps({k: rec[k] for k in ('coefficients', 'n_dev', 'r2', 'dev_winner_accuracy')}, indent=2))


def cmd_predict(campaign):
    frozen = jload(OUT / 'rev4_frozen.json')
    if frozen['code_sha256'] != sha(HERE):
        raise ValueError('fase2_rev4.py changed after freeze')
    target = OUT / f'predictions_{campaign}.json'
    if target.exists():
        raise FileExistsError(target)
    theta = [frozen['coefficients'][t] for t in TERMS]
    sys.path.insert(0, str(PROJECT))
    import fase2_core as C
    preds = []
    for ds in C.CAMPAIGNS[campaign]['datasets']:
        if (F2 / campaign / ds / 'benchmark').exists():
            raise ValueError('timing already exists; prediction would not be blind')
        parts = part_sizes_f2(campaign, ds)
        val = jload(F2 / campaign / ds / 'semantic_validation.json')
        tr = {(r['case_id'], SHORTV[r['variant']]): r for r in val['results'] if r['variant'] in (U, D)}
        for case in sorted({k[0] for k in tr}):
            u, d = tr[case, 'u'], tr[case, 'd']
            if u['query_id'] not in TEMPORAL:
                continue
            lu = predict_ln(theta, u['execute_calls'], u['iterated_rows'], parts['u'], True)
            ld = predict_ln(theta, d['execute_calls'], d['iterated_rows'], parts['d'], True)
            if u['execute_calls'] != d['execute_calls']:
                fewer = 'TS2-U' if u['execute_calls'] < d['execute_calls'] else 'TS2-D'
            else:
                fewer = 'TS2-U' if parts['u'] < parts['d'] else 'TS2-D'
            preds.append(dict(dataset=ds, case_id=case, query_id=u['query_id'], window_days=u['window_days'],
                              calls_u=u['execute_calls'], calls_d=d['execute_calls'], rows=u['iterated_rows'],
                              part_u=parts['u'], part_d=parts['d'], pred_p95_u=round(math.exp(lu), 4),
                              pred_p95_d=round(math.exp(ld), 4), pred_d_over_u=round(math.exp(ld - lu), 4),
                              rev4_winner='TS2-D' if ld < lu else 'TS2-U', always_u='TS2-U', fewer_calls=fewer))
    rec = dict(status='REV4_PREDICTIONS_BEFORE_TIMING', campaign=campaign,
               created_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
               rev4_frozen_sha256=sha(OUT / 'rev4_frozen.json'), predictions=preds)
    with target.open('x') as f:
        json.dump(rec, f, indent=2)
    n_d = sum(p['rev4_winner'] == 'TS2-D' for p in preds)
    print(f'{campaign}: {len(preds)} predictions written before timing; Rev4 predicts D in {n_d}')


if __name__ == '__main__':
    if sys.argv[1:2] == ['fit']:
        cmd_fit()
    elif sys.argv[1:2] == ['predict'] and len(sys.argv) == 3:
        cmd_predict(sys.argv[2])
    else:
        sys.exit(__doc__)
