"""Estatistica comum do protocolo v1 (numpy).

Entrada canonica: lista de dicts com query_id, variant, rep, round, latency_ms.
Todas as funcoes recalculam a partir das latencias brutas.
"""
from __future__ import annotations

import math
from collections import defaultdict

import numpy as np


def nearest_rank(values, p):
    s = np.sort(np.asarray(values, dtype=float))
    if s.size == 0:
        raise ValueError('empty sample')
    k = min(s.size - 1, max(0, math.ceil(s.size * p / 100.0) - 1))
    return float(s[k])


def describe(values):
    v = np.asarray(values, dtype=float)
    return {'n': int(v.size), 'mean_ms': float(v.mean()), 'p50_ms': nearest_rank(v, 50),
            'p95_ms': nearest_rank(v, 95), 'p99_ms': nearest_rank(v, 99),
            'min_ms': float(v.min()), 'max_ms': float(v.max()), 'sd_ms': float(v.std(ddof=1)) if v.size > 1 else 0.0}


def index(raw):
    """-> cube[variant][query][rep] = np.array ordered by round (round-aligned)."""
    cube = defaultdict(lambda: defaultdict(dict))
    tmp = defaultdict(list)
    for r in raw:
        tmp[(r['variant'], r['query_id'], int(r['rep']))].append((int(r['round']), float(r['latency_ms'])))
    for (v, q, rep), xs in tmp.items():
        xs.sort()
        cube[v][q][rep] = np.array([x for _, x in xs])
    return cube


def _strata(cube, variant, queries):
    return [(q, rep, cube[variant][q][rep]) for q in queries for rep in sorted(cube[variant][q])]


def mix_metric(cube, variant, queries, stat='mean'):
    """Uniform mix: unweighted mean over classes of the per-class statistic (all reps pooled)."""
    vals = []
    for q in queries:
        pooled = np.concatenate([cube[variant][q][r] for r in sorted(cube[variant][q])])
        vals.append(pooled.mean() if stat == 'mean' else nearest_rank(pooled, int(stat[1:])))
    return float(np.mean(vals))


def bootstrap_mix(cube, variants, queries, B=2000, seed=0, stat='mean'):
    """Stratified (query, rep) bootstrap; same round indices for all variants (paired).

    Returns {'point': {v: x}, 'samples': {v: np.array(B)}}.
    """
    rng = np.random.default_rng(seed)
    some = variants[0]
    reps = {q: sorted(cube[some][q]) for q in queries}
    # one index matrix per (q, rep), shared by all variants -> paired resampling
    idx = {}
    for q in queries:
        for rep in reps[q]:
            n = cube[some][q][rep].size
            for v in variants:
                if cube[v][q][rep].size != n:
                    raise ValueError(f'unpaired stratum {v} {q} {rep}')
            idx[(q, rep)] = rng.integers(0, n, size=(B, n))
    out = {}
    for v in variants:
        per_q = []
        for q in queries:
            arrs = [cube[v][q][rep][idx[(q, rep)]] for rep in reps[q]]  # each (B, n)
            pooled = np.concatenate(arrs, axis=1)
            if stat == 'mean':
                per_q.append(pooled.mean(axis=1))
            else:
                p = int(stat[1:])
                k = min(pooled.shape[1] - 1, max(0, math.ceil(pooled.shape[1] * p / 100.0) - 1))
                per_q.append(np.sort(pooled, axis=1)[:, k])
        out[v] = np.mean(np.vstack(per_q), axis=0)
    point = {v: mix_metric(cube, v, queries, stat) for v in variants}
    return {'point': point, 'samples': out}


def ci(samples, alpha=0.05):
    lo, hi = np.quantile(samples, [alpha / 2, 1 - alpha / 2])
    return float(lo), float(hi)


def paired_ratio(boot, a, b):
    """Ratio a/b of the mix metric with paired bootstrap CI."""
    r = boot['samples'][a] / boot['samples'][b]
    lo, hi = ci(r)
    return {'a': a, 'b': b, 'ratio': boot['point'][a] / boot['point'][b], 'ci_lo': lo, 'ci_hi': hi,
            'ln_ratio': math.log(boot['point'][a] / boot['point'][b]),
            'abs_ln_ci_hi': float(np.quantile(np.abs(np.log(r)), 0.95))}


def per_query_ratio(cube, a, b, q, B=2000, seed=0):
    rng = np.random.default_rng(seed)
    reps = sorted(cube[a][q])
    xa = [cube[a][q][r] for r in reps]; xb = [cube[b][q][r] for r in reps]
    sa, sb = [], []
    for ra, rb in zip(xa, xb):
        ix = rng.integers(0, ra.size, size=(B, ra.size))
        sa.append(ra[ix]); sb.append(rb[ix])
    ma = np.concatenate(sa, axis=1).mean(axis=1); mb = np.concatenate(sb, axis=1).mean(axis=1)
    r = ma / mb
    pa = np.concatenate(xa).mean(); pb = np.concatenate(xb).mean()
    lo, hi = ci(r)
    return {'query_id': q, 'a': a, 'b': b, 'ratio': float(pa / pb), 'ci_lo': lo, 'ci_hi': hi}


def noise_floor(boot, aa_pairs):
    """Max over A/A pairs of the 95% quantile of |ln ratio| (bootstrap)."""
    rows = [paired_ratio(boot, a, b) for a, b in aa_pairs]
    return {'noise_floor_abs_ln': max(r['abs_ln_ci_hi'] for r in rows) if rows else None,
            'noise_floor_pct': (math.exp(max(r['abs_ln_ci_hi'] for r in rows)) - 1) * 100 if rows else None,
            'pairs': rows}


def verdict(pr, floor_abs_ln):
    if pr['ci_lo'] <= 1.0 <= pr['ci_hi'] or abs(pr['ln_ratio']) < floor_abs_ln:
        return 'EMPATE'
    return 'A_MAIS_RAPIDO' if pr['ratio'] < 1 else 'B_MAIS_RAPIDO'


def rep_level(cube, variant, queries):
    """Mix mean per repetition (for t-interval over n=5 repetitions, RUBiS-style)."""
    reps = sorted(cube[variant][queries[0]])
    vals = [float(np.mean([cube[variant][q][r].mean() for q in queries])) for r in reps]
    v = np.array(vals); n = v.size
    tcrit = {2: 12.706, 3: 4.303, 4: 3.182, 5: 2.776, 6: 2.571, 7: 2.447, 8: 2.365, 9: 2.306, 10: 2.262}.get(n, 1.96)
    half = tcrit * v.std(ddof=1) / math.sqrt(n) if n > 1 else float('nan')
    return {'per_rep': vals, 'mean': float(v.mean()), 't_ci_lo': float(v.mean() - half), 't_ci_hi': float(v.mean() + half),
            'cv_between_reps_pct': float(v.std(ddof=1) / v.mean() * 100) if n > 1 else None}
