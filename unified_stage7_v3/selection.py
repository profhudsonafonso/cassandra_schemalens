"""Universal selection: Pareto -> structural diversity -> balanced_structural_tiebreak (same for all benchmarks)."""
from __future__ import annotations

import random

from .pareto import AXES, POLICY, front


def balanced_structural_tiebreak(points: dict, axes=AXES) -> dict:
    """Mean over axes of value / (min positive value of the axis over the whole pre-benchmark space). Equal weights.
    Ordering / tie-break only - NOT a predicted latency."""
    mins = {}
    for k in axes:
        pos = [p[k] for p in points.values() if p[k] is not None and p[k] > 0]
        if pos:
            mins[k] = min(pos)
    out = {}
    for i, p in points.items():
        vals = [p[k] / mins[k] for k in mins if p[k] is not None]
        out[i] = sum(vals) / len(vals) if vals and len(vals) == len(mins) else float('inf')
    return out


def select(points: dict, tags: dict, top_k=None, axes=AXES, use_pareto=True, use_diversity=True) -> dict:
    """points: {id: axes}; tags: {id: set of structural tags}. Returns selection trace."""
    top_k = top_k or POLICY['top_k']
    tb = balanced_structural_tiebreak(points, axes)
    pool = front(points, axes) if use_pareto else sorted(points)
    order = sorted(pool, key=lambda i: (tb[i], i))
    trace = {'pareto_front': front(points, axes), 'pool': order, 'top_k': top_k, 'tiebreak': tb, 'steps': []}
    if use_pareto and len(pool) <= top_k:
        trace['rule'] = 'PARETO_FITS_TOP_K_KEEP_ALL'
        trace['selected'] = order
        return trace
    if not use_diversity:
        trace['rule'] = 'TOP_K_BY_TIEBREAK'
        trace['selected'] = order[:top_k]
        return trace
    chosen, seen = [order[0]], set(tags[order[0]])
    trace['steps'].append({'pick': order[0], 'reason': 'lowest balanced_structural_tiebreak'})
    rest = order[1:]
    while rest and len(chosen) < top_k:
        best = max(rest, key=lambda i: (len(set(tags[i]) - seen), -tb[i], [-ord(c) for c in i]))
        gain = len(set(tags[best]) - seen)
        if gain == 0:
            best = rest[0]
        trace['steps'].append({'pick': best, 'new_tags': sorted(set(tags[best]) - seen), 'reason': 'structural diversity' if gain else 'tiebreak'})
        chosen.append(best); seen |= set(tags[best]); rest.remove(best)
    trace['rule'] = 'PARETO_THEN_STRUCTURAL_DIVERSITY_THEN_TIEBREAK'
    trace['selected'] = sorted(chosen, key=lambda i: (tb[i], i))
    return trace


def random_k(ids, k, seed=20261118):
    return sorted(random.Random(seed).sample(sorted(ids), min(k, len(ids))))
