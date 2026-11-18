"""Universal Pareto dominance over the policy axes (minimisation). Unknown values block dominance claims."""
from __future__ import annotations

import json
from pathlib import Path

POLICY = json.loads((Path(__file__).with_name('policy.json')).read_text())
AXES = tuple(POLICY['pareto_axes'])
TOL = POLICY['relative_tolerance']


def _le(a, b):
    return a <= b + TOL * max(1.0, abs(b))


def _lt(a, b):
    return a < b - TOL * max(1.0, abs(b))


def dominates(a: dict, b: dict, axes=AXES) -> bool:
    if any(a[k] is None or b[k] is None for k in axes):
        return False
    return all(_le(a[k], b[k]) for k in axes) and any(_lt(a[k], b[k]) for k in axes)


def front(points: dict, axes=AXES) -> list:
    """points: {deployment_id: axes dict} -> sorted ids of non-dominated points."""
    ids = sorted(points)
    return [i for i in ids if not any(dominates(points[j], points[i], axes) for j in ids if j != i)]
