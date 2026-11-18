"""The 15 mandatory V3 tests (spec item 21). Run: versao_ajustada/.venv/bin/python -m pytest unified_stage7_v3/tests -q"""
from __future__ import annotations

import ast
import json
import math
import shutil
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from unified_stage7_v3 import freeze as FZ
from unified_stage7_v3.adapters import generic
from unified_stage7_v3.aggregation import deployment_vector
from unified_stage7_v3.estimator import UnifiedStage7Estimator
from unified_stage7_v3.guards import FORBIDDEN, FirewallViolation, check_path, firewall
from unified_stage7_v3.model import (AccessStep, CandidateSpec, Column, Operation, QueryPlan, Workload, WritePlan, validate)
from unified_stage7_v3.pareto import AXES, dominates, front
from unified_stage7_v3.provenance import REPO, jsonable
from unified_stage7_v3.selection import balanced_structural_tiebreak, select

PKG = REPO / 'unified_stage7_v3'
OUT = REPO / 'outputs_v3'
BENCHES = ('iot', 'tpcx_iot', 'tsbs', 'rubis')
SELECTION_MODULES = ['model.py', 'logical_bytes.py', 'estimator.py', 'aggregation.py', 'pareto.py', 'selection.py',
                     'adapters/iot.py', 'adapters/tpcx_iot.py', 'adapters/tsbs.py', 'adapters/rubis.py', 'adapters/generic.py']
PY = sys.executable


def _pre(b, name):
    return json.loads((OUT / b / 'prebenchmark' / name).read_text())


def _cand(cid='t', family='F', rows=100.0, text_width=10.0, canonical=False):
    return CandidateSpec(cid, family, (Column('k', 'text', text_width), Column('ts', 'bigint'), Column('v', 'double')),
                         ('k',), ('ts',), rows, 10.0, 10.0, 100.0, canonical_source_required=canonical, tags=('table:' + cid,))


def _plan(qid='q', pid='p', cid='t', req=2.0, rows=20.0):
    return QueryPlan(qid, pid, (cid,), (AccessStep(cid, req, req, rows, ('ts', 'v')),), 1.0)


# 1 -------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize('p', ['outputs/v1_run1/raw_latencies.csv', 'x/stage9/a.json', 'benchmark_summary.json', 'q_p95.csv',
                               'latency_ms.csv', 'runs/v9_generation_001/stages/E20_benchmark/RESULT.json',
                               'runs/v9_generation_001/stages/E14_cost_model/COST_MODEL.json', 'final_run_v8_completion_012/raw.csv'])
def test_01_no_measured_latency_reaches_stage7_8(p):
    with pytest.raises(FirewallViolation):
        check_path(p)
    with firewall(), pytest.raises(FirewallViolation):
        open(REPO / p)


@pytest.mark.parametrize('b', BENCHES)
def test_01b_manifests_declare_and_prove_no_measured_input(b):
    m = _pre(b, '12_frozen_manifest.json')
    assert m['selection_uses_measured_results'] is False and m['latency_files_read'] == [] and m['benchmark_outputs_read'] == []
    opened = [f for f in m['files_opened_under_firewall'] if not f.endswith('.py')]
    assert opened and not [f for f in opened if FORBIDDEN.search(f)]


def _code_tokens(path):
    """Identifiers, attributes and string constants of executable code (docstrings excluded)."""
    tree = ast.parse(path.read_text())
    docs = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)) and n.body \
                and isinstance(n.body[0], ast.Expr) and isinstance(getattr(n.body[0], 'value', None), ast.Constant):
            docs.add(id(n.body[0].value))
    out = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Name):
            out.append(n.id)
        elif isinstance(n, ast.Attribute):
            out.append(n.attr)
        elif isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docs:
            out.append(n.value)
    return ' '.join(out).lower()


def test_01c_selection_code_has_no_latency_vocabulary():
    for rel in SELECTION_MODULES:
        src = _code_tokens(PKG / rel)
        for bad in ('raw_latenc', 'latency_ms', 'p95', 'p99', 'throughput', 'cost_model', 'a_read_ms'):
            assert bad not in src, (rel, bad)


# 2 -------------------------------------------------------------------------------------------------------------
def test_02_query_ids_do_not_influence_costs():
    est = UnifiedStage7Estimator([_cand()])
    a, _ = est.read(_plan('Q1', 'p1')); b, _ = est.read(_plan('Q_OTHER_999', 'p2'))
    assert a == b
    wl = lambda q: Workload((Operation(q, 'read', 1.0),), 'X', 'Y')
    v1, *_ = deployment_vector({'t'}, {'Q1': [_plan('Q1', 'p1')]}, {}, wl('Q1'), est)
    v2, *_ = deployment_vector({'t'}, {'ZZ': [_plan('ZZ', 'p9')]}, {}, wl('ZZ'), est)
    assert v1.axes() == v2.axes()


# 3 -------------------------------------------------------------------------------------------------------------
def test_03_family_labels_do_not_influence_costs():
    e1 = UnifiedStage7Estimator([_cand(family='TS0')]); e2 = UnifiedStage7Estimator([_cand(family='TS5_AGG')])
    assert e1.footprint('t')[0] == e2.footprint('t')[0] and e1.read(_plan())[0] == e2.read(_plan())[0]


def test_03b_relabelling_every_family_keeps_the_real_iot_selection():
    from unified_stage7_v3.adapters import iot
    from unified_stage7_v3.provenance import run_prebenchmark
    canon = iot.build()
    canon2 = dict(canon, candidates=[replace(c, family='X' + str(i)) for i, c in enumerate(canon['candidates'])])
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        _, c1, t1 = run_prebenchmark('iot', canon, Path(td) / 'a')
        _, c2, t2 = run_prebenchmark('iot', canon2, Path(td) / 'b')
    assert t1['selected'] == t2['selected'] and {k: v['axes'] for k, v in c1.items()} == {k: v['axes'] for k, v in c2.items()}


# 4 -------------------------------------------------------------------------------------------------------------
def test_04_identical_physical_plans_give_identical_vectors():
    est = UnifiedStage7Estimator([_cand('a'), _cand('b')])
    assert est.read(_plan('q1', 'route_A', 'a'))[0] == est.read(_plan('q7', 'route_B', 'b'))[0]


# 5 -------------------------------------------------------------------------------------------------------------
def test_05_identical_canonical_inputs_through_different_adapters():
    from unified_stage7_v3.adapters import iot
    canon = iot.build()
    cands = canon['candidates']; plans = [p for ps in canon['plans_by_query'].values() for p in ps]
    rebuilt_c = [generic.candidate(jsonable(c)) for c in cands]
    rebuilt_p = [generic.plan(jsonable(p)) for p in plans]
    e1, e2 = UnifiedStage7Estimator(cands), UnifiedStage7Estimator(rebuilt_c)
    for c in cands:
        assert e1.footprint(c.candidate_id) == e2.footprint(c.candidate_id)
    for p, q in zip(plans, rebuilt_p):
        assert e1.read(p) == e2.read(q)
    # same physical numbers declared by two different benchmarks' vocabularies -> same vector
    tsbs_like = _cand('ts0_metric_raw', 'TS0'); iot_like = _cand('ts0_iot_raw', 'TS0_IOT')
    assert UnifiedStage7Estimator([tsbs_like]).footprint('ts0_metric_raw')[0] == UnifiedStage7Estimator([iot_like]).footprint('ts0_iot_raw')[0]


# 6 -------------------------------------------------------------------------------------------------------------
def _numbers(o):
    if isinstance(o, dict):
        for v in o.values():
            yield from _numbers(v)
    elif isinstance(o, list):
        for v in o:
            yield from _numbers(v)
    elif isinstance(o, (int, float)) and not isinstance(o, bool):
        yield o


@pytest.mark.parametrize('b', BENCHES)
def test_06_all_quantities_finite_and_non_negative(b):
    for n in ('02_query_read_vectors.json', '03_write_vectors.json', '04_candidate_footprints.json', '05_deployment_vectors.json'):
        for x in _numbers(_pre(b, n)):
            assert math.isfinite(x) and x >= 0, (b, n, x)


@pytest.mark.parametrize('bad', [-1.0, float('nan'), float('inf')])
def test_06b_validation_rejects_invalid(bad):
    with pytest.raises(ValueError):
        validate(_cand(rows=bad))


# 7 -------------------------------------------------------------------------------------------------------------
def test_07_unknown_quantities_are_explicit_never_zero():
    c = _cand(text_width=None)
    est = UnifiedStage7Estimator([c])
    fp, warns = est.footprint('t')
    assert fp.logical_storage_bytes is None and any(w.startswith('UNKNOWN') for w in warns)
    p = QueryPlan('q', 'p', ('t',), (AccessStep('t', 1, 1, 5, ('k', 'v')),), 1)
    rw, w2 = est.read(p)
    assert rw.logical_bytes is None and any('UNKNOWN' in w for w in w2)
    known = {k: 1.0 for k in AXES}; unknown = dict(known, logical_storage_bytes=None)
    assert not dominates(known, unknown) and not dominates(unknown, known)
    assert balanced_structural_tiebreak({'a': known, 'b': unknown})['b'] == float('inf')


# 8 -------------------------------------------------------------------------------------------------------------
def test_08_no_family_specific_fudge_factor():
    for rel in SELECTION_MODULES + ['provenance.py', 'policy.json']:
        src = (PKG / rel).read_text()
        for bad in ('family_factor', 'read_family_factors', 'FAMILY_FACTOR', 'fudge'):
            assert bad not in src, (rel, bad)
    for rel in ('estimator.py', 'aggregation.py', 'pareto.py', 'selection.py', 'logical_bytes.py'):
        tree = ast.parse((PKG / rel).read_text())
        assert not [n for n in ast.walk(tree) if isinstance(n, ast.Attribute) and n.attr == 'family'], rel


# 9 -------------------------------------------------------------------------------------------------------------
def test_09_stage7_scenario_matches_stage9_scenario():
    va = REPO / 'versao_ajustada'
    tsbs = json.loads((OUT / 'tsbs/V3_PREBENCHMARK_SCENARIO.json').read_text())
    v2 = json.loads((va / 'tsbs_tsdb/PREREGISTRATION_TSBS_V1.json').read_text())
    assert tsbs['data_sha256'] == v2['data']['sha256'] and tsbs['instances_sha256'] == v2['instances']['sha256']
    assert tsbs['hosts'] == v2['data']['hosts'] and tsbs['metric_cells'] == v2['data']['metric_cells']
    tp = json.loads((OUT / 'tpcx_iot/V3_PREBENCHMARK_SCENARIO.json').read_text())
    w = json.loads((va / 'tpcx_iot_benchmark/PREREGISTRATION_TPCX_V1.json').read_text())['workload']
    for k_v3, k_v2 in (('stations', 'stations'), ('sensors', 'sensors'), ('seed_steps', 'seed_steps'), ('bench_steps', 'bench_steps'),
                       ('read_operations', 'read_operations'), ('pad_bytes', 'pad_bytes')):
        assert tp[k_v3] == w[k_v2], k_v3
    rb = json.loads((OUT / 'rubis/V3_PREBENCHMARK_SCENARIO.json').read_text())
    tm = json.loads((va / 'rubis_v9/frozen/TRANSFER_MANIFEST.json').read_text())
    assert rb['cohort_sha256'] == tm['pickle_sha256']
    assert rb['entity_counts'] == tm['fingerprints_reference']['counts']
    iot_in = _pre('iot', '00_input_hashes.json')
    csv = [k for k in iot_in if k.endswith('normalized_double_readings.csv')][0]
    v2iot = json.loads((va / 'iot_benchmark_tsdb/PREREGISTRATION_IOT_V1.json').read_text())
    assert iot_in[csv] in json.dumps(v2iot) or iot_in[csv] == FZ.sha(REPO / csv)
    assert tp.get('old_stage7_scenario_NOT_USED') and tsbs.get('old_stage7_scenario_NOT_USED') and rb.get('old_stage7_scenario_NOT_USED')


# 10-13 ---------------------------------------------------------------------------------------------------------
@pytest.fixture
def tmp_root(tmp_path):
    root = tmp_path / 'out'
    for b in ('iot',):
        shutil.copytree(OUT / b / 'prebenchmark', root / b / 'prebenchmark', ignore=shutil.ignore_patterns('FROZEN.sha256'))
        shutil.copy(OUT / b / 'V3_PREBENCHMARK_SCENARIO.json', root / b / 'V3_PREBENCHMARK_SCENARIO.json')
    return root


def test_10_hbench_frozen_before_any_benchmark_output(tmp_root):
    (tmp_root / 'iot/benchmark').mkdir(parents=True); (tmp_root / 'iot/benchmark/raw.csv').write_text('x')
    with pytest.raises(FZ.Abort):
        FZ.freeze('iot', {'measurement_decision': 'X'}, root=tmp_root, rerun=False)
    for b in BENCHES:                                   # real campaign: freeze time precedes every benchmark file
        fz = OUT / b / 'prebenchmark/FROZEN.sha256'
        bench = OUT / b / 'benchmark'
        if fz.exists() and bench.exists():
            t = json.loads(fz.read_text())['frozen_utc']
            import datetime as dt
            t0 = dt.datetime.fromisoformat(t).timestamp()
            assert all(f.stat().st_mtime >= t0 for f in bench.rglob('*') if f.is_file()), b


def test_11_frozen_hashes_verified_before_stage9(tmp_root, monkeypatch):
    FZ.freeze('iot', {'measurement_decision': 'X'}, root=tmp_root, rerun=False)
    assert FZ.verify('iot', root=tmp_root)
    calls = []

    def boom(*a, **k):
        calls.append(a); raise FZ.Abort('ABORT: test')
    import unified_stage7_v3.stage9.tsbs_v3 as T, unified_stage7_v3.stage9.rubis_v3 as R, unified_stage7_v3.stage9.reuse_v2 as U
    for mod in (T, R, U):
        monkeypatch.setattr(mod, 'verify', boom)
    with pytest.raises(FZ.Abort):
        T.run(tmp_root / 'never', smoke=False)
    with pytest.raises(FZ.Abort):
        R.run(tmp_root / 'never', smoke=False)
    with pytest.raises(FZ.Abort):
        U.main(['iot'])
    assert len(calls) == 3 and not (tmp_root / 'never').exists()


def test_12_changing_stage7_inputs_invalidates_freeze(tmp_root, tmp_path):
    extra = tmp_path / 'input.json'; extra.write_text('{"a": 1}')
    p = tmp_root / 'iot/prebenchmark/00_input_hashes.json'
    d = json.loads(p.read_text()); d[str(extra)] = FZ.sha(extra); p.write_text(json.dumps(d))
    FZ.freeze('iot', {'measurement_decision': 'X'}, root=tmp_root, rerun=False)
    assert FZ.verify('iot', root=tmp_root)
    extra.write_text('{"a": 2}')
    with pytest.raises(FZ.Abort):
        FZ.verify('iot', root=tmp_root)


def test_13_changing_stage8_output_invalidates_stage9(tmp_root):
    FZ.freeze('iot', {'measurement_decision': 'X'}, root=tmp_root, rerun=False)
    p = tmp_root / 'iot/prebenchmark/09_hbench_v3.json'
    p.write_text(p.read_text().replace('ts2_iot_wide_unbucketed', 'ts0_iot_raw', 1))
    with pytest.raises(FZ.Abort):
        FZ.verify('iot', root=tmp_root)


def test_13b_changing_preregistration_invalidates_stage9(tmp_root):
    FZ.freeze('iot', {'measurement_decision': 'X'}, root=tmp_root, rerun=False)
    pr = tmp_root / 'iot/PREREGISTRATION_V3.json'
    pr.write_text(pr.read_text().replace('"X"', '"Y"'))
    with pytest.raises(FZ.Abort):
        FZ.verify('iot', root=tmp_root)


# 14 ------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize('b', BENCHES)
def test_14_every_operation_has_a_route_and_every_write_is_maintainable(b):
    hb = _pre(b, '09_hbench_v3.json')
    reads = {r['query_id'] for r in _pre(b, '02_query_read_vectors.json')}
    assert hb
    for h in hb:
        assert not h['problems'] and set(h['routes']) == reads
    assert all(w['maintainable'] for w in _pre(b, '03_write_vectors.json')
               if w['candidate_id'] in {t for h in hb for t in h['tables']})
    if b == 'rubis':                                    # the 36 statements of the contract (28 reads + 8 writes)
        assert len(reads) + len({w['write_id'] for w in _pre(b, '03_write_vectors.json')}) == 36


TSBS_ORACLE = r'''
import json, sys
sys.path[:0] = [sys.argv[1] + '/versao_ajustada/tsbs_tsdb/frozen_design', sys.argv[1] + '/versao_ajustada/tsbs_tsdb/tsbs_v1']
from freeze import design_objects, dataset
from oracle import Oracle
from methodology.tsbs.stage6.materializer import ReferenceMaterializer
from methodology.tsbs.stage9.offline import IndexedReferenceFetcher
from methodology.tsbs.stage5.runtime import bind_read_requests, execute_mock_rows
from methodology.tsbs.stage9.benchmark import compare_values
from methodology.tsbs.workload import METRICS
_, plans, writes = design_objects(); ds = dataset()
inst = {i['instance_key']: i for i in json.load(open(sys.argv[1] + '/versao_ajustada/tsbs_tsdb/data/query_instances.json'))}
m = ReferenceMaterializer(list(writes.values()), list(METRICS)); m.apply(ds['events']); fetch = IndexedReferenceFetcher(m)
orc = Oracle(ds['events']); seen = set(); bad = []
for d in json.load(open(sys.argv[2])):
    for k, pid in d['routes'].items():
        if (k, pid) in seen: continue
        seen.add((k, pid)); i = inst[k]; p = plans[pid]
        got = execute_mock_rows(p, [(r, fetch(r)) for r in bind_read_requests(p, **i['params'])])
        try: compare_values(orc.answer(i['query_id'], i['params']), got, rel_tol=1e-9, abs_tol=1e-8)
        except AssertionError: bad.append((k, pid))
print(json.dumps({'checked': len(seen), 'bad': bad}))
'''


def test_14b_every_frozen_tsbs_route_matches_the_independent_oracle_offline(tmp_path):
    s = tmp_path / 'chk.py'; s.write_text(TSBS_ORACLE)
    r = subprocess.run([PY, str(s), str(REPO), str(OUT / 'tsbs/prebenchmark/05_deployment_vectors.json')], capture_output=True, text=True, cwd=tmp_path)
    assert r.returncode == 0, r.stderr[-2000:]
    res = json.loads(r.stdout.strip().splitlines()[-1])
    assert res['checked'] > 0 and res['bad'] == []


def test_14c_reused_benchmarks_routes_identical_to_v2_measured_routes():
    from unified_stage7_v3.plans import plan_for
    for b in ('iot', 'tpcx_iot'):
        p = plan_for(b)
        assert p['measurement_decision'] == 'REUSE_V2_MEASUREMENTS' and all(p['routes_identical_to_v2'].values())


# 15 ------------------------------------------------------------------------------------------------------------
def test_15_legacy_nnls_unreachable_from_v3_selection():
    code = ('import sys, json; from unified_stage7_v3.adapters import rubis; c = rubis.build(); '
            'print(json.dumps(sorted(m for m in sys.modules if m.startswith("schemalens_v9"))))')
    r = subprocess.run([PY, '-c', code], capture_output=True, text=True, cwd=REPO)
    assert r.returncode == 0, r.stderr[-2000:]
    mods = json.loads(r.stdout.strip().splitlines()[-1])
    assert 'schemalens_v9.cost' not in mods and 'schemalens_v9.compose' not in mods
    for p in PKG.rglob('*.py'):
        rel = p.relative_to(PKG).parts
        if 'tests' in rel or 'analysis' in rel:          # retrospective package may use the legacy model (spec item 30)
            continue
        tree = ast.parse(p.read_text())
        assert not [n for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and 'analysis' in (n.module or '')], p
        for n in ast.walk(tree):
            if isinstance(n, ast.Import):
                mods = [a.name for a in n.names]
            elif isinstance(n, ast.ImportFrom):
                mods = [n.module or ''] + [f'{n.module or ""}.{a.name}' for a in n.names]
            else:
                continue
            assert not any(m.split('.')[-1] in ('cost', 'compose') or m in ('schemalens_v9.cost', 'schemalens_v9.compose') for m in mods), (p, mods)
    with firewall(), pytest.raises(FirewallViolation):
        (REPO / 'runs/v9_generation_001/stages/E14_cost_model/COST_MODEL.json').read_text()


def test_15b_selection_is_deterministic_and_has_no_scalar_stage7_score():
    pts = {'a': {k: 1.0 for k in AXES}, 'b': {k: 2.0 for k in AXES}}
    assert select(pts, {'a': set(), 'b': set()})['selected'] == ['a'] and front(pts) == ['a']
    for b in BENCHES:
        for r in _pre(b, '02_query_read_vectors.json')[:5]:
            assert not any('score' in k or 'latency' in k for k in r)
