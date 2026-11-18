"""IoT-Benchmark adapter: converts frozen pre-benchmark artefacts (campaign B) into the canonical model.

Sources (all pre-benchmark): Rev2 candidate specs (DDL), the Rev3 executable QueryPlan catalog (per-plan request
and row counts computed on the campaign data, no timing), the campaign data CSV (cardinalities, partitions) and the
12 Java query objects. No cost weight, no family factor.
"""
from __future__ import annotations

import csv
import itertools
import json
import re
from collections import Counter
from pathlib import Path

from ..model import AccessStep, CandidateSpec, Column, Operation, QueryPlan, Workload, WritePlan
from ..provenance import REPO, sha

from ..layout import V2

SNAP = V2['iot'] / 'frozen_design_B'
REV2 = SNAP / 'outputs/v4_rev2_prebenchmark'
REV3 = SNAP / 'outputs/v4_rev3_planaware_prebenchmark'
DATA = SNAP / 'phase_a1/outputs/normalized_double_readings.csv'
QUERIES = SNAP / 'phase_a1/outputs/query_instances_pilot.json'
DAY = 86_400_000
CANONICAL = ('ts0_iot_raw', 'ts2_iot_wide_unbucketed', 'ts2_iot_wide')
EXCLUSIVE = [{'ts2_iot_wide_unbucketed', 'ts2_iot_wide'}]


def _cols(ddl, widths):
    body = ddl[ddl.index('(') + 1: ddl.rindex('PRIMARY KEY')]
    cols = []
    for part in body.split(','):
        part = part.strip()
        if part:
            n, t = part.split()[:2]
            cols.append(Column(n, t, widths.get(n)))
    pk = re.search(r'PRIMARY KEY \(\(([^)]*)\)\s*,?\s*([^)]*)\)', ddl.replace('\n', ' '))
    return tuple(cols), tuple(x.strip() for x in pk.group(1).split(',')), tuple(x.strip() for x in pk.group(2).split(',') if x.strip())


def build():
    specs = json.loads((REV2 / 'stage4/10_v4_candidate_specs.json').read_text())
    catalog = json.loads((REV3 / 'stage5/15_executable_plan_catalog.json').read_text())
    queries = {q['query_id']: q for q in json.loads(QUERIES.read_text())}
    with DATA.open() as fh:
        rows = list(csv.DictReader(fh))
    devices = sorted({r['device_id'] for r in rows}); sensors = sorted({r['sensor_name'] for r in rows})
    widths = {'device_id': sum(len(d) for d in devices) / len(devices), 'sensor_name': sum(len(s) for s in sensors) / len(sensors)}
    wide = Counter((r['device_id'], int(r['timestamp'])) for r in rows)              # logical observations
    per_dev = Counter(d for d, _ in wide); per_dev_day = Counter((d, t // DAY) for d, t in wide)
    per_series = Counter((r['device_id'], r['sensor_name']) for r in rows)
    shape = {
        'ts0_iot_raw': (len(rows), len(per_series), max(per_series.values())),
        'ts2_iot_wide_unbucketed': (len(wide), len(per_dev), max(per_dev.values())),
        'ts2_iot_wide': (len(wide), len(per_dev_day), max(per_dev_day.values())),
        'ts4_iot_latest': (len(per_series), len(per_series), 1)}
    tags = {'ts0_iot_raw': ('canonical:ts0_iot_raw', 'table:ts0_iot_raw', 'raw', 'narrow'),
            'ts2_iot_wide_unbucketed': ('canonical:ts2_iot_wide_unbucketed', 'table:ts2_iot_wide_unbucketed', 'raw', 'wide', 'bucket:none'),
            'ts2_iot_wide': ('canonical:ts2_iot_wide', 'table:ts2_iot_wide', 'raw', 'wide', 'bucket:day'),
            'ts4_iot_latest': ('table:ts4_iot_latest', 'projection', 'aggregate:latest')}
    cands = []
    for s in specs:
        cid = s['candidate_id']; cols, pk, ck = _cols(s['ddl'], widths)
        rt, parts, mx = shape[cid]
        cands.append(CandidateSpec(cid, s['family'], cols, pk, ck, rt, parts, mx, source_rows=len(rows),
                                   canonical_source_required=cid not in CANONICAL, tags=tags[cid]))
    plans = {}
    for p in catalog:
        q = queries[p['query_id']]; cid = p['candidate_id']
        if cid.startswith('ts0') or cid.startswith('ts4'):
            proj = ('latest_timestamp_ms', 'value_double') if cid.startswith('ts4') else ('timestamp_ms', 'value_double')
        else:
            proj = ('timestamp_ms',) + tuple(f'm_{x}' for x in q['sensors'])
        step = AccessStep(cid, float(p['request_count_proxy']), float(p['request_count_proxy']), float(p['physical_rows_proxy']), proj)
        operands = len(q['devices']) * (2 if p['query_id'] == 'Q12' else 1)
        plans.setdefault(p['query_id'], []).append(QueryPlan(
            p['query_id'], p['plan_id'], (cid,), (step,), operands, runtime_guard='; '.join(p.get('preconditions', [])),
            valid=p['route_status'] == 'DIRECT', assumptions=('counts from Rev3 plan catalog computed on campaign-B data, pre-benchmark',),
            provenance='frozen_design_B/outputs/v4_rev3_planaware_prebenchmark/stage5/15_executable_plan_catalog.json'))
    writes = {}
    for c in cands:   # campaign load executed by the V9-era loader: one insert per stored row, no maintenance read
        writes[('ingest', c.candidate_id)] = [WritePlan('ingest', c.candidate_id, c.rows_total, c.rows_total, 0.0, 0.0,
                                                        tuple(x.name for x in c.columns),
                                                        assumptions=('static campaign load (stage9 loader semantics)',),
                                                        provenance='campaign data CSV + DDL')]
    wl = Workload(tuple(Operation(q, 'read', 1.0) for q in sorted(queries, key=lambda x: int(x[1:])))
                  + (Operation('ingest', 'write', 1.0),), 'EXPERIMENTAL_POLICY_UNIFORM', 'UNIFORM_QUERY_CLASS')

    def enumerate_fn(est):
        out = []
        for src in CANONICAL:
            for latest in (False, True):
                out.append(frozenset({src} | ({'ts4_iot_latest'} if latest else set())))
        return out
    scenario = {'benchmark': 'IoT-Benchmark', 'campaign': 'B Rev3 (DATA_SEED 531101, QUERY_SEED 531107)', 'devices': len(devices),
                'sensors': len(sensors), 'metric_cells': len(rows), 'wide_rows': len(wide), 'history_days': round((max(int(r['timestamp']) for r in rows) - min(int(r['timestamp']) for r in rows)) / DAY, 3),
                'queries': len(queries), 'query_window_days': 7, 'executed_stage9_scenario': 'identical (same CSV and query objects measured in V2)',
                'text_widths_bytes': widths}
    return {'candidates': cands, 'plans_by_query': plans, 'writes': writes, 'workload': wl, 'scenario': scenario,
            'enumerate_fn': enumerate_fn, 'deployments': None,
            'deployment_tags': lambda d: set().union(*(tags[c] for c in d)) | {'family:' + '+'.join(sorted(d))},
            'id_fn': lambda d: 'V3IOT_' + '+'.join(sorted(d)),
            'input_hashes': {str(p.relative_to(REPO)): sha(p) for p in (REV2 / 'stage4/10_v4_candidate_specs.json', REV3 / 'stage5/15_executable_plan_catalog.json', DATA, QUERIES)}}
