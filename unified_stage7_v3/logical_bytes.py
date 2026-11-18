"""Common, versioned logical byte proxy policy (LBP-1). Not disk, network or SSTable bytes."""
from __future__ import annotations

import json
from pathlib import Path

POLICY = json.loads((Path(__file__).with_name('policy.json')).read_text())['logical_byte_policy']


class UnknownWidth(ValueError):
    pass


def column_width(col) -> float:
    t = col.cql_type.lower().strip()
    if t in POLICY['fixed_width_bytes']:
        return float(POLICY['fixed_width_bytes'][t])
    if t in POLICY['variable_width_types'] or t.startswith(('list<', 'set<', 'map<', 'frozen<')):
        if col.mean_width_bytes is None:
            raise UnknownWidth(f'UNKNOWN width for variable-width column {col.name} ({t})')
        return float(col.mean_width_bytes)
    raise UnknownWidth(f'UNKNOWN CQL type {t} for column {col.name}')


def row_width(columns, names=None):
    """Width of a row restricted to `names` (all columns when None). Returns (width or None, warnings)."""
    cols = {c.name: c for c in columns}
    sel = list(cols) if names is None else list(names)
    w, warns = 0.0, []
    for n in sel:
        if n not in cols:
            warns.append(f'UNKNOWN:column {n} not in table'); return None, warns
        try:
            w += column_width(cols[n])
        except UnknownWidth as e:
            warns.append(f'UNKNOWN:{e}'); return None, warns
    return w + POLICY['logical_row_overhead_bytes'], warns
