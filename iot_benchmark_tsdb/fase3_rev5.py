#!/usr/bin/env python3
"""SchemaLens Rev5 (Fase 3, E8): additive pre-benchmark cost model for TS2-U vs TS2-D (EMENDA_04).

    python fase3_rev5.py fit          # once, after E7 and before any E9 timing; writes rev5/rev5_frozen.json
    python fase3_rev5.py predict e9   # after E9 validation + flush, before E9 timing

    p95_ms = b0 + b1*calls + b2*calls*ln(partition_mean_bytes) + b3*rows
fitted by weighted least squares (weights 1/p95^2, i.e. relative error) on every audited SSTable-state temporal case
(S3 R1, E1 r2b/r3 sst, E3, E4, E7), variants U and D.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
EXP = PROJECT / 'experiments'
F2 = EXP / 'rev3_fase2'
OUT = F2 / 'rev5'
HERE = Path(__file__).resolve()
U, D = 'R3H_38939db96444', 'R3H_d2ea1974de53'
SHORTV = {U: 'u', D: 'd'}
TEMPORAL = ('Q2', 'Q7', 'Q9', 'Q12')
TERMS = ('b0_fixed_ms', 'b1_per_call_ms', 'b2_per_call_per_ln_partition_ms', 'b3_per_row_ms')
DEV_F2 = [('e1r2b', ['d1', 'd2', 'd4', 'd8']), ('e1r3', ['d1', 'd2', 'd4', 'd8']), ('e3', ['h1', 'h2', 'h3']),
          ('e4', ['n3', 'n18']), ('e3r2', ['h1', 'h2', 'h3']), ('e4r2', ['n3', 'n18'])]


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def rcsv(p):
    with open(p, newline='') as f:
        return list(csv.DictReader(f))


def jload(p):
    return json.loads(Path(p).read_text())


def feats(calls, rows, part):
    return [1.0, float(calls), calls * math.log(part), float(rows)]


def solve(X, y, w):
    k = len(X[0])
    A = [[sum(wi * r[i] * r[j] for r, wi in zip(X, w)) for j in range(k)] + [sum(wi * r[i] * t for r, t, wi in zip(X, y, w))]
         for i in range(k)]
    for c in range(k):
        p = max(range(c, k), key=lambda r: abs(A[r][c]))
        A[c], A[p] = A[p], A[c]
        if abs(A[c][c]) < 1e-12:
            raise ValueError('singular design')
        for r in range(k):
            if r != c:
                f = A[r][c] / A[c][c]
                A[r] = [a - f * b for a, b in zip(A[r], A[c])]
    return [A[i][k] / A[i][i] for i in range(k)]


def parts_f2(c, ds):
    v = jload(F2 / c / ds / 'storage_post_flush' / 'summary.json')['variants']
    return {x: v[f'sst:TS2-{x.upper()}']['metrics']['partition_mean_bytes'] for x in ('u', 'd')}


def dev_rows():
    out = []
    for f in (1, 2, 4, 8):
        s = jload(EXP / 'rev3_s3_physical_storage' / f'campaign_r1/{f}x/storage_pre_timing/summary.json')
        parts = {SHORTV[l]: s['variants'][l]['metrics']['partition_mean_bytes'] for l in (U, D)}
        for r in rcsv(EXP / 'rev3_s3_physical_storage' / f'campaign_r1/{f}x/benchmark/query_p95.csv'):
            if r['query_id'] in TEMPORAL and r['variant'] in (U, D):
                v = SHORTV[r['variant']]
                out.append((('s3r1', f'd{f}', r['case_id']), v, int(r['execute_calls']), int(r['iterated_rows']), parts[v],
                            float(r['p95_ms'])))
    for c, dss in DEV_F2:
        for ds in dss:
            if jload(F2 / c / ds / 'benchmark' / 'audit.json')['status'] != 'PASS':
                raise ValueError(f'{c}/{ds} not audited')
            parts = parts_f2(c, ds)
            for r in rcsv(F2 / c / ds / 'benchmark' / 'query_p95.csv'):
                if r['query_id'] in TEMPORAL and r['variant'] in (U, D) and r['cond'] == 'sst':
                    v = SHORTV[r['variant']]
                    out.append(((c, ds, r['case_id']), v, int(r['execute_calls']), int(r['iterated_rows']), parts[v],
                                float(r['p95_ms'])))
    return out


def pred(theta, calls, rows, part):
    return sum(t * x for t, x in zip(theta, feats(calls, rows, part)))


def cmd_fit():
    OUT.mkdir(parents=True, exist_ok=True)
    target = OUT / 'rev5_frozen.json'
    if target.exists():
        raise FileExistsError(target)
    if any((F2 / 'e9').glob('*/benchmark')):
        raise ValueError('E9 timing already exists; Rev5 can no longer be fitted leakage-free')
    rows = dev_rows()
    X = [feats(c, r, p) for _, _, c, r, p, _ in rows]
    y = [t for *_, t in rows]
    w = [1 / t ** 2 for t in y]
    theta = solve(X, y, w)
    rel = [abs(pred(theta, c, r, p) - t) / t for _, _, c, r, p, t in rows]
    groups = {}
    for key, v, c, r, p, t in rows:
        groups.setdefault(key, {})[v] = (t, pred(theta, c, r, p))
    hits = sum(1 for g in groups.values() if (g['d'][0] < g['u'][0]) == (g['d'][1] < g['u'][1]))
    srcs = {}
    for key, *_ in rows:
        srcs[key[0]] = srcs.get(key[0], 0) + 1
    rec = dict(status='REV5_FROZEN_BEFORE_E9_TIMING', created_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
               model='p95_ms = b0 + b1*calls + b2*calls*ln(partition_mean_bytes) + b3*rows (WLS, weights 1/p95^2)',
               coefficients=dict(zip(TERMS, [round(t, 9) for t in theta])), n_dev=len(rows), dev_sources=srcs,
               dev_median_abs_rel_error=round(statistics.median(rel), 4), dev_winner_accuracy=f'{hits}/{len(groups)}',
               dev_max_calls=max(c for _, _, c, *_ in rows), dev_max_partition_bytes=max(p for *_, p, _ in rows),
               code_sha256=sha(HERE), decision_rule='predict TS2-D if predicted p95_D < predicted p95_U, else TS2-U')
    with target.open('x') as f:
        json.dump(rec, f, indent=2)
    print(json.dumps({k: rec[k] for k in ('coefficients', 'n_dev', 'dev_median_abs_rel_error', 'dev_winner_accuracy')}, indent=1))


def cmd_predict(campaign):
    frozen = jload(OUT / 'rev5_frozen.json')
    if frozen['code_sha256'] != sha(HERE):
        raise ValueError('fase3_rev5.py changed after freeze')
    target = OUT / f'predictions_{campaign}.json'
    if target.exists():
        raise FileExistsError(target)
    theta = [frozen['coefficients'][t] for t in TERMS]
    sys.path.insert(0, str(PROJECT))
    import fase3_core  # noqa: F401  (registers the Fase 3 campaigns)
    import fase2_core as C
    out = []
    for ds in C.CAMPAIGNS[campaign]['datasets']:
        if (F2 / campaign / ds / 'benchmark').exists():
            raise ValueError('timing already exists; prediction would not be blind')
        parts = parts_f2(campaign, ds)
        val = jload(F2 / campaign / ds / 'semantic_validation.json')
        tr = {(r['case_id'], SHORTV[r['variant']]): r for r in val['results'] if r['variant'] in (U, D)}
        for case in sorted({k[0] for k in tr}):
            u, d = tr[case, 'u'], tr[case, 'd']
            if u['query_id'] not in TEMPORAL:
                continue
            pu = pred(theta, u['execute_calls'], u['iterated_rows'], parts['u'])
            pd = pred(theta, d['execute_calls'], d['iterated_rows'], parts['d'])
            out.append(dict(dataset=ds, case_id=case, query_id=u['query_id'], window_days=u['window_days'],
                            calls_u=u['execute_calls'], calls_d=d['execute_calls'], rows=u['iterated_rows'],
                            part_u=parts['u'], part_d=parts['d'], pred_p95_u=round(pu, 4), pred_p95_d=round(pd, 4),
                            pred_d_over_u=round(pd / pu, 4), rev5_winner='TS2-D' if pd < pu else 'TS2-U',
                            extrapolates_partition=max(parts.values()) > frozen['dev_max_partition_bytes'],
                            extrapolates_calls=max(u['execute_calls'], d['execute_calls']) > frozen['dev_max_calls']))
    rec = dict(status='REV5_PREDICTIONS_BEFORE_TIMING', campaign=campaign,
               created_utc=dt.datetime.now(dt.timezone.utc).isoformat(), rev5_frozen_sha256=sha(OUT / 'rev5_frozen.json'),
               predictions=out)
    with target.open('x') as f:
        json.dump(rec, f, indent=2)
    print(f"{campaign}: {len(out)} Rev5 predictions before timing; D predicted in {sum(p['rev5_winner'] == 'TS2-D' for p in out)}")


def cmd_predict_rev4(campaign):
    """Rev4 (frozen in Fase 2) predictions for a Fase 3 campaign, through the unchanged fase2_rev4 module."""
    sys.path.insert(0, str(PROJECT))
    import fase3_core  # noqa: F401
    import fase2_rev4
    fase2_rev4.cmd_predict(campaign)


if __name__ == '__main__':
    a = sys.argv[1:]
    if a == ['fit']:
        cmd_fit()
    elif len(a) == 2 and a[0] == 'predict':
        cmd_predict(a[1])
    elif len(a) == 2 and a[0] == 'predict-rev4':
        cmd_predict_rev4(a[1])
    else:
        sys.exit(__doc__)
