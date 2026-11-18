"""Shared retrospective analysis helpers (run AFTER the V3 campaign; never imported by Stage 7/8)."""
from __future__ import annotations

import csv
import json
import math
import sys
from pathlib import Path

from ..freeze import verify, verify_prereg
from ..pareto import AXES
from ..provenance import REPO, dump
from ..selection import balanced_structural_tiebreak, random_k, select

VA = REPO / 'versao_ajustada'
OUT = REPO / 'outputs_v3'
if str(VA / 'common') not in sys.path:
    sys.path.insert(0, str(VA / 'common'))
import method_eval as M  # noqa: E402
import slstats as S      # noqa: E402

B, SEED = 2000, 20261117


def read(p):
    return json.loads(Path(p).read_text())


def read_csv(p):
    with Path(p).open() as f:
        return list(csv.DictReader(f))


def write_csv(p, rows):
    p = Path(p); p.parent.mkdir(parents=True, exist_ok=True)
    keys = []
    for r in rows:
        keys += [k for k in r if k not in keys]
    with p.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=keys); w.writeheader(); w.writerows(rows)


def pre(b, n):
    return read(OUT / b / 'prebenchmark' / n)


def frozen(b):
    verify(b, rerun=False)
    return verify_prereg(b)


def v3_space(b):
    """complete deployments -> axes, tags, tiebreak, hbench order"""
    vec = {r['deployment_id']: r for r in pre(b, '05_deployment_vectors.json')}
    comp = pre(b, '06_complete_deployments.json')['complete_ids']
    tr = pre(b, '11_selection_trace.json')
    hb = [h['deployment_id'] for h in pre(b, '09_hbench_v3.json')]
    return {i: vec[i] for i in comp}, {i: set(tr['tags'][i]) for i in comp}, tr['tiebreak'], hb


def tied_set(boot, ids, best, floor):
    return {best} | {i for i in ids if i != best and S.verdict(S.paired_ratio(boot, i, best), floor) == 'EMPATE'}


def evaluate(measured, hbench, score, tied):
    ev = M.evaluate(measured, hbench, score, tied)
    ev['random_k'] = M.random_k_expected(measured, max(1, len([h for h in hbench if h in measured])))
    for k in ('regret', 'kendall_tau_score_vs_read'):
        if isinstance(ev.get(k), float) and math.isnan(ev[k]):
            ev[k] = None
    return ev


ABLATIONS = {
    'full_v3': dict(),
    'without_read_rows': dict(drop='read_logical_rows'),
    'without_read_bytes': dict(drop='read_logical_bytes'),
    'without_request_count': dict(drop='read_cql_requests'),
    'without_write_mutations': dict(drop='write_cql_mutations'),
    'without_storage': dict(drop='logical_storage_bytes'),
    'without_max_partition_safety': dict(note='IDENTICAL_BY_CONSTRUCTION: risk axes are not V3 dominance axes (D-V3-006)'),
    'with_risk_axes_in_dominance': dict(add=('max_partition_bytes', 'max_partition_rows', 'partition_count')),
    'without_structural_diversity': dict(use_diversity=False),
    'without_pareto': dict(use_pareto=False),
    'top_k_1': dict(top_k=1),
}


def ablation_selections(b):
    space, tags, _, _ = v3_space(b)
    pts = {i: r['axes'] for i, r in space.items()}
    out = {}
    for name, cfg in ABLATIONS.items():
        axes = tuple(a for a in AXES if a != cfg.get('drop')) + tuple(cfg.get('add', ()))
        tr = select(pts, tags, top_k=cfg.get('top_k'), axes=axes, use_pareto=cfg.get('use_pareto', True),
                    use_diversity=cfg.get('use_diversity', True))
        out[name] = {'hbench': tr['selected'], 'tiebreak': balanced_structural_tiebreak(pts, axes), 'rule': tr['rule'],
                     'pareto_size': len(tr['pareto_front']), 'note': cfg.get('note')}
    k = len(out['full_v3']['hbench'])
    out['random_k'] = {'hbench': random_k(list(pts), k), 'rule': f'uniform random k={k} (seed 20261118); expectation over all k-subsets in random_k_expected',
                       'tiebreak': out['full_v3']['tiebreak'], 'pareto_size': None, 'note': None}
    return out


def ablation_table(b, measured, tied, map_id=lambda i: i):
    rows = []
    for name, a in ablation_selections(b).items():
        hb = [map_id(i) for i in a['hbench']]
        unmeasured = [i for i in hb if i not in measured]
        if unmeasured:
            rows.append({'benchmark': b, 'ablation': name, 'hbench_size': len(hb), 'status': f'NOT_EVALUABLE ({len(unmeasured)} unmeasured)',
                         'note': a['note']})
            continue
        sc = {map_id(i): v for i, v in a['tiebreak'].items() if map_id(i) in measured}
        ev = evaluate(measured, sorted(hb, key=lambda i: sc[i]), sc, tied)
        rows.append({'benchmark': b, 'ablation': name, 'hbench_size': len(hb), 'status': 'EVALUATED', 'rule': a['rule'],
                     'top1_strict': ev['top1_strict'], 'top1_noise': ev['top1_within_noise'], 'top3': ev['top3'],
                     'near_best_5pct': ev['near_best_5pct'], 'regret': ev['regret'], 'pareto_retention': ev['pareto_retention'],
                     'dsr': ev['reduction'], 'hbench': ' '.join(hb), 'note': a['note']})
    return rows


__all__ = ['S', 'M', 'B', 'SEED', 'OUT', 'VA', 'REPO', 'read', 'read_csv', 'write_csv', 'pre', 'frozen', 'v3_space',
           'tied_set', 'evaluate', 'ablation_table', 'dump']
