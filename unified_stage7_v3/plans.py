"""Pre-registered measurement plan of each benchmark (written into PREREGISTRATION_V3.json at freeze time).

Reuse rule D-V3-001 (user instruction): a benchmark is re-measured only when the set of V3 deployments to measure
contains a deployment (tables + per-operation routes) that the V2 campaign did not measure under protocol v1.
Reads V2 *design* artefacts (preregistrations: variant tables and routes) - never V2 results."""
from __future__ import annotations

import hashlib
import json

from .provenance import REPO

OUT = REPO / 'outputs_v3'
VA = REPO / 'versao_ajustada'
PRESERVATION = {
    'primary_hypothesis': 'P1 (preservation): the best measured deployment of the measured V3 space is in HBench V3, '
                          'strictly or within the A/A noise floor (Top-1 noise-equivalent).',
    'secondary': ['P2: regret of the best HBench V3 member <= max(A/A noise floor, 5%)',
                  'P3: Pareto retention: measured-Pareto deployments (decision metric x measured storage) that are in HBench V3 / all measured-Pareto deployments',
                  'D1 (diagnostic only, no threshold): Kendall tau between balanced_structural_tiebreak and the measured decision metric'],
    'not_a_hypothesis': 'Kendall tau is retrospective diagnostics; a low tau is a result, not a failure, and never feeds back into Stage 7/8',
}


def _pre(bench):
    p = OUT / bench / 'prebenchmark'
    return {n: json.loads((p / n).read_text()) for n in ('06_complete_deployments.json', '07_pareto_front.json', '09_hbench_v3.json')}, p


def tsbs_label(tables):
    return 'H_' + hashlib.sha256(('v3:' + ','.join(sorted(tables))).encode()).hexdigest()[:12]


def _vectors(p):
    return {r['deployment_id']: r for r in json.loads((p / '05_deployment_vectors.json').read_text())}


def plan_iot():
    d, p = _pre('iot')
    v2 = json.loads((VA / 'iot_benchmark_tsdb/PREREGISTRATION_IOT_V1.json').read_text())['variants']
    vec = _vectors(p)
    mapping, routes_equal = {}, {}
    for did in d['06_complete_deployments.json']['complete_ids']:
        r = vec[did]
        m = [x for x in v2 if x.get('kind') == 'cover' and sorted(x['candidates']) == r['tables']]
        mapping[did] = m[0]['label'] if m else None
        routes_equal[did] = bool(m) and m[0]['routes'] == r['routes']
    return {'measurement_decision': 'REUSE_V2_MEASUREMENTS' if all(routes_equal.values()) else 'NEW_CAMPAIGN',
            'reason': 'every V3 complete deployment (tables + per-query routes) was measured by the V2 verification-mode campaign under protocol v1',
            'v3_to_v2_variant': mapping, 'routes_identical_to_v2': routes_equal,
            'measured_space': 'verification mode: all V3 complete deployments + V2 baselines + V2 A/A pair, raw data of versao_ajustada/iot_benchmark_tsdb/outputs/v1_run1',
            'baselines': ['BASELINE_ORIGINAL', 'BASELINE_Q8_OPTIMIZED'], 'aa_control': 'V2 A/A pair (TS2-U = REF-Q8opt DDL-identical)',
            'decision_metric': 'as V2: mean latency of the uniform Q1-Q12 mix', 'denominator': 'all 3 V3 complete deployments',
            **PRESERVATION}


def plan_tpcx():
    d, p = _pre('tpcx_iot')
    reg = json.loads((VA / 'tpcx_iot_benchmark/PREREGISTRATION_TPCX_V1.json').read_text())
    v2 = {x['label']: x for x in reg['variants'] if x['kind'] == 'cover'}
    vec = _vectors(p)
    routes_equal = {}
    for did in d['06_complete_deployments.json']['complete_ids']:
        r = vec[did]
        x = v2.get(did)
        routes_equal[did] = bool(x) and sorted(x['tables']) == r['tables'] and r['routes']['Q_READ_2_WINDOWS'].endswith('__' + x['read_table'])
    return {'measurement_decision': 'REUSE_V2_MEASUREMENTS' if all(routes_equal.values()) else 'NEW_CAMPAIGN',
            'reason': 'every V3 complete deployment (tables + read table) was measured by the V2 verification-mode campaign under protocol v1',
            'v3_to_v2_variant': {k: k for k in routes_equal}, 'routes_identical_to_v2': routes_equal,
            'measured_space': 'verification mode: all 9 V3 complete deployments + baseline b_tpcx_logical_raw + 2 A/A replicas (versao_ajustada/tpcx_iot_benchmark/outputs/v1_run1)',
            'baselines': ['b_tpcx_logical_raw'], 'aa_control': [x for x in reg['aa_pairs']],
            'decision_metric': 'as V2: mean latency of Q_READ_2_WINDOWS during concurrent ingest', 'denominator': 'all 9 V3 complete deployments',
            **PRESERVATION}


def plan_tsbs():
    d, p = _pre('tsbs')
    vec = _vectors(p)
    hb = [h['deployment_id'] for h in d['09_hbench_v3.json']]
    variants = []
    for did in d['06_complete_deployments.json']['complete_ids']:
        r = vec[did]
        variants.append({'label': tsbs_label(r['tables']), 'deployment_id': tsbs_label(r['tables']), 'v3_deployment_id': did,
                         'kind': 'cover', 'candidates': r['tables'], 'in_hbench_v3': did in hb,
                         'hbench_v3_rank': hb.index(did) + 1 if did in hb else None, 'v3_routes': r['routes']})
    variants.sort(key=lambda v: (v['hbench_v3_rank'] or 99, v['label']))
    top = variants[0]
    variants.append({'label': 'B_tsbs_series_bigint', 'deployment_id': 'B_tsbs_series_bigint', 'kind': 'baseline', 'candidates': ['series_bigint'],
                     'description': 'Esquema Cassandra original do TSBS (series_bigint), igual a V2'})
    variants.append({**{k: top[k] for k in ('deployment_id', 'candidates', 'v3_routes', 'v3_deployment_id')}, 'label': top['label'] + '_aa',
                     'kind': 'aa_replica', 'replica_of': top['label']})
    v2 = json.loads((VA / 'tsbs_tsdb/PREREGISTRATION_TSBS_V1.json').read_text())
    return {'measurement_decision': 'NEW_CAMPAIGN',
            'reason': 'D-V3-008: two HBench V3 deployments (ts5_max_by_host_hour present) route 5 aligned instances to ts0_metric_raw '
                      '(universal lexicographic route rule: fewer CQL requests) whereas V2 executed them on ts5_max_by_host_hour; '
                      'these (tables, routes) pairs were never measured',
            'measured_space': 'FULL VERIFICATION: all 24 V3 complete deployments + TSBS original baseline + A/A replica of HBench V3 rank 1 (26 variants)',
            'variants': variants, 'aa_pairs': [[top['label'], top['label'] + '_aa']],
            'container': 'sl_v3_tsbs_cassandra', 'port': 9083, 'run_tag': '3a7e10',
            'protocol': 'versao_ajustada/common/protocol_v1.json unchanged; TSBS declared deviation of V2 kept identical (2 warm-ups + 12 rounds per repetition, 5 cold repetitions)',
            'warmup': v2['warmup'], 'rounds': v2['rounds'], 'interval_s': v2['interval_s'], 'measurement_seed_base': v2['measurement_seed_base'] + 300,
            'data': v2['data'], 'instances': v2['instances'], 'decision_metric': v2['decision_metric'],
            'harness': 'unified_stage7_v3/stage9/tsbs_v3.py = tsbs_v1/measure.py unchanged except the route source (frozen V3 per-instance routes) and the variant list',
            'denominator': 'all 24 V3 complete deployments', **PRESERVATION}


def plan_rubis():
    d, p = _pre('rubis')
    vec = _vectors(p)
    hb = [h['deployment_id'] for h in d['09_hbench_v3.json']]
    H = json.loads((REPO / 'runs/v9_generation_001/stages/E15_composition/H.json').read_text())['H']
    v2 = {tuple(sorted(h['canonical_ids'])): h['schema_id'] for h in H}
    deps = [{'schema_id': f'V3H{i + 1:02d}', 'v3_deployment_id': did, 'canonical_ids': vec[did]['tables'], 'v3_routes': vec[did]['routes'],
             'same_tables_as_v2': v2.get(tuple(vec[did]['tables']))} for i, did in enumerate(hb)]
    return {'measurement_decision': 'NEW_CAMPAIGN',
            'reason': 'HBench V3 contains 2 deployments never measured (V3H02 and V3H05); the whole campaign is re-run so that every deployment is measured in the same interleaved, counterbalanced session',
            'hbench_v3_schemas': deps, 'baselines': ['NoSE', 'QPG', 'V8_AUTO_H_norm'], 'aa_pairs': [['V3H01', 'V3H01_AA']],
            'arms': {'v41': 'main campaign, Cassandra 4.1.12 (protocol v1 digest)', 'v311': 'secondary 3.11.11 x 4.1.12 analysis (same harness as V2)'},
            'workloads': ['browsing', 'bidding', 'update10x', 'update100x'],
            'protocol': 'identical to the V2 RUBiS campaign (rubis_v1: 5 repetitions, ABBA arms, counterbalanced order, cold restart + flush gate, oracle of the 36 statements)',
            'harness': 'unified_stage7_v3/stage9/rubis_v3.py = rubis_v1/measure.py unchanged except the H source (frozen V3 HBench with V3 read routes)',
            'decision_metric': 'mean latency per request in each workload (V9/V2 metric)',
            'rubis_hypotheses': {'R1': 'In 4.1.12 the best HBench V3 member is faster than NoSE and QPG in browsing, bidding and update10x',
                                 'R2': 'Secondary: verdicts of R1 are the same in 3.11.11'},
            'denominator': 'measured HBench V3 (5) - the 256-deployment complete space is not measured (too large); preservation claims are restricted to HBench V3 + baselines',
            **PRESERVATION}


def plan_for(bench):
    return {'iot': plan_iot, 'tpcx_iot': plan_tpcx, 'tsbs': plan_tsbs, 'rubis': plan_rubis}[bench]()
