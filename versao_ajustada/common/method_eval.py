"""Avaliacao retrospectiva do metodo (protocolo v1), comum aos benchmarks.

measured: {deployment_id: {'read': mix mean ms, 'storage': bytes, 'write': s, 'tie_group': int}}
hbench_ranked: deployment ids na ordem do seletor (rank 1 primeiro)
selector_score: {deployment_id: score do seletor (menor = melhor)} para todo o espaco
"""
from __future__ import annotations

import itertools
import math


def measured_pareto(measured, objectives=('read', 'storage', 'write')):
    ids = list(measured)

    def dom(a, b):  # a dominates b
        return all(measured[a][o] <= measured[b][o] for o in objectives) and any(
            measured[a][o] < measured[b][o] for o in objectives)
    return sorted(i for i in ids if not any(dom(j, i) for j in ids if j != i))


def kendall_tau(x, y):
    pairs = list(itertools.combinations(range(len(x)), 2))
    c = d = 0
    for i, j in pairs:
        s = (x[i] - x[j]) * (y[i] - y[j])
        c += s > 0; d += s < 0
    n0 = len(pairs)
    tx = sum(1 for i, j in pairs if x[i] == x[j]); ty = sum(1 for i, j in pairs if y[i] == y[j])
    den = math.sqrt((n0 - tx) * (n0 - ty))
    return (c - d) / den if den else float('nan')


def evaluate(measured, hbench_ranked, selector_score, tied_with_best=None, near=0.05):
    """tied_with_best: set of ids statistically tied with the measured best (noise floor)."""
    space = list(measured)
    best = min(space, key=lambda i: measured[i]['read'])
    best_val = measured[best]['read']
    tied = set(tied_with_best or {best})
    hb = [h for h in hbench_ranked if h in measured]
    hb_best = min(hb, key=lambda i: measured[i]['read']) if hb else None
    regret = measured[hb_best]['read'] / best_val - 1 if hb else float('inf')
    pareto = measured_pareto(measured)
    xs = [selector_score[i] for i in space]; ys = [measured[i]['read'] for i in space]
    return {
        'space_size': len(space), 'hbench_size': len(hb),
        'measured_best': best, 'measured_best_read_ms': best_val,
        'tied_with_best': sorted(tied),
        'top1_strict': bool(hb) and hb[0] == best,
        'top1_within_noise': bool(hb) and hb[0] in tied,
        'top3': best in hb[:3] or bool(set(hb[:3]) & tied),
        'near_best_5pct': any(measured[i]['read'] <= best_val * (1 + near) for i in hb),
        'regret': regret,
        'measured_pareto': pareto,
        'pareto_retention': (len(set(pareto) & set(hb)) / len(pareto)) if pareto else None,
        'kendall_tau_score_vs_read': kendall_tau(xs, ys),
        'reduction': 1 - len(hb) / len(space) if space else None,
    }


def random_k_expected(measured, k):
    """Expected regret and P(contains best) of choosing k deployments uniformly at random."""
    space = list(measured)
    best_val = min(measured[i]['read'] for i in space)
    combos = list(itertools.combinations(space, min(k, len(space))))
    regs = [min(measured[i]['read'] for i in c) / best_val - 1 for c in combos]
    hit = [any(measured[i]['read'] == best_val for i in c) for c in combos]
    return {'k': k, 'expected_regret': sum(regs) / len(regs), 'p_contains_best': sum(hit) / len(hit),
            'combinations': len(combos)}
