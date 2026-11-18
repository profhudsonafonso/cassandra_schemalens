"""Hashing, exclusive writes and the V3 Stage 7/8 pipeline runner shared by every benchmark."""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
from pathlib import Path

from .aggregation import deployment_vector
from .estimator import UnifiedStage7Estimator
from .guards import OPENED, assert_no_legacy_cost
from .selection import select

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def sha_obj(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def jsonable(o):
    if dataclasses.is_dataclass(o):
        return {k: jsonable(v) for k, v in dataclasses.asdict(o).items()}
    if isinstance(o, dict):
        return {str(k): jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple, set, frozenset)):
        return [jsonable(v) for v in (sorted(o) if isinstance(o, (set, frozenset)) else o)]
    return o


def dump(path, obj):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(jsonable(obj), indent=2, sort_keys=True, ensure_ascii=False, default=str) + '\n', encoding='utf8')


def code_hashes():
    """Frozen code: Stage 7/8, adapters, freeze, plans and Stage 9 harnesses. Excluded: tests and the retrospective
    analysis package (written after measurement; it never feeds back into selection)."""
    return {str(p.relative_to(REPO)): sha(p) for p in sorted(ROOT.rglob('*.py')) + [ROOT / 'policy.json']
            if 'tests' not in p.parts and 'analysis' not in p.relative_to(ROOT).parts and '__pycache__' not in p.parts}


def run_prebenchmark(bench, canon, out: Path, deployments=None, enumerate_fn=None):
    """canon: dict with candidates, plans_by_query, writes, workload, scenario, deployments (list of frozensets) or
    enumerate_fn, deployment_tags(fn), input_hashes, legacy_hbench (ids as frozensets), id_fn."""
    assert_no_legacy_cost()
    est = UnifiedStage7Estimator(canon['candidates'])
    out.mkdir(parents=True, exist_ok=True)
    dump(out / '00_input_hashes.json', canon['input_hashes'])
    dump(out / '01_scenario.json', canon['scenario'])
    qrv = []
    for q, plans in sorted(canon['plans_by_query'].items()):
        for p in plans:
            rw, warns = est.read(p)
            qrv.append({'query_id': q, 'plan_id': p.plan_id, 'candidate_id': '+'.join(p.candidate_ids),
                        'read_cql_requests': rw.cql_requests, 'partitions_touched': rw.partitions_touched,
                        'logical_rows': rw.logical_rows, 'logical_bytes': rw.logical_bytes, 'fanout': rw.bucket_or_partition_fanout,
                        'runtime_guard': p.runtime_guard, 'valid': p.valid, 'assumptions': list(p.assumptions),
                        'warnings': list(warns), 'provenance': p.provenance})
    dump(out / '02_query_read_vectors.json', qrv)
    wv = []
    for (w, cid), plans in sorted(canon['writes'].items()):
        for wp in plans:
            ww, warns = est.write(wp)
            wv.append({'write_id': w, 'candidate_id': cid, **jsonable(ww), 'maintainable': wp.maintainable,
                       'assumptions': list(wp.assumptions), 'warnings': list(warns), 'provenance': wp.provenance})
    dump(out / '03_write_vectors.json', wv)
    fps = []
    for c in canon['candidates']:
        fp, warns = est.footprint(c.candidate_id)
        fps.append({'candidate_id': c.candidate_id, 'family': c.family, 'logical_storage_bytes': fp.logical_storage_bytes,
                    'partition_count': fp.physical_partition_count, 'max_partition_rows': fp.max_partition_rows,
                    'max_partition_bytes': fp.max_partition_bytes, 'duplication_ratio': fp.duplication_ratio,
                    'canonical_source_required': c.canonical_source_required, 'warnings': list(warns)})
    dump(out / '04_candidate_footprints.json', fps)
    deps = canon['deployments'] if canon.get('deployments') is not None else canon['enumerate_fn'](est)
    from collections import Counter
    complete, infeasible, hist, n_all = {}, [], Counter(), 0
    for d in deps:
        n_all += 1
        did = canon['id_fn'](d)
        problems = list(canon.get('feasible_fn', lambda d: [])(d))
        vec, routes, p2 = deployment_vector(d, canon['plans_by_query'], canon['writes'], canon['workload'], est, canon.get('support_fn'))
        problems += p2
        rec = {'deployment_id': did, 'tables': sorted(d), 'axes': vec.axes(), 'vector': jsonable(vec), 'routes': routes, 'problems': problems}
        if problems:
            hist.update(p.split(':')[0] if p.startswith('write') else p.split(' ')[0] + ' ' + p.split(' ')[1] for p in problems)
            if len(infeasible) < 2000:
                infeasible.append({k: rec[k] for k in ('deployment_id', 'tables', 'problems')})
        else:
            complete[did] = rec
    compact = len(complete) > 2000
    dump(out / '05_deployment_vectors.json', [({k: r[k] for k in ('deployment_id', 'tables', 'axes')} if compact else r) for r in complete.values()])
    dump(out / '06_complete_deployments.json', {'enumerated': n_all, 'complete': len(complete), 'infeasible': n_all - len(complete),
                                                'infeasible_reason_histogram': dict(hist), 'complete_ids': sorted(complete),
                                                'infeasible_examples_first_2000': infeasible})
    points = {i: r['axes'] for i, r in complete.items()}
    tags = {i: canon['deployment_tags'](frozenset(r['tables'])) for i, r in complete.items()}
    tr = select(points, tags)
    dump(out / '07_pareto_front.json', [complete[i] for i in tr['pareto_front']])
    dump(out / '08_diversity_selection.json', {k: tr[k] for k in ('rule', 'pool', 'steps', 'top_k')})
    hb = [dict(complete[i], rank=n + 1, balanced_structural_tiebreak=tr['tiebreak'][i]) for n, i in enumerate(tr['selected'])]
    dump(out / '09_hbench_v3.json', hb)
    dump(out / '10_hbench_routes.json', [{'deployment_id': h['deployment_id'], 'query_id': q, 'plan_id': p} for h in hb for q, p in sorted(h['routes'].items())])
    dump(out / '11_selection_trace.json', {'tiebreak': tr['tiebreak'], 'tags': tags, **{k: tr[k] for k in ('rule', 'pool', 'steps', 'pareto_front', 'selected')}})
    assert_no_legacy_cost()
    files = sorted(out.glob('0*.json')) + sorted(out.glob('1[01]_*.json'))
    man = {'benchmark': bench, 'policy': 'SCHEMALENS_UNIFIED_STAGE7_V3', 'created_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
           'selection_uses_measured_results': False, 'latency_files_read': [], 'benchmark_outputs_read': [],
           'files_opened_under_firewall': sorted(set(OPENED)), 'code_sha256': code_hashes(),
           'output_sha256': {f.name: sha(f) for f in files}, 'complete_deployments': len(complete),
           'pareto_front_size': len(tr['pareto_front']), 'hbench_ids': [h['deployment_id'] for h in hb],
           'selection_rule': tr['rule'], 'frequency_source': canon['workload'].frequency_source,
           'aggregation_policy': canon['workload'].aggregation_policy}
    dump(out / '12_frozen_manifest.json', man)
    return man, complete, tr
