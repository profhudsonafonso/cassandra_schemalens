#!/usr/bin/env python3
"""Read-only semantic audit of materialized SchemaLens Rev3 S2 datasets.

No CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, TRUNCATE or flush.
Uses frozen B selection and S1 query objects, checks every case against the
offline oracle on the corresponding S2 density dataset. Produces a new JSON
report and refuses to overwrite existing reports.

Usage (from the project root):
  python s2_validate_live.py --project . --factor 1 --port 9043
"""
from __future__ import annotations
import argparse
import hashlib
import json
import sys
from pathlib import Path

LABEL_BASE = 'BASELINE_Q8_OPTIMIZED'
LABEL_U = 'R3H_38939db96444'
LABEL_D = 'R3H_d2ea1974de53'
FROZEN_SHA = '8f88693851245838a7485300f8ce70aa850bd65c0c2d881225e9365cbcb39231'
FACTORS = (1, 2, 4, 8)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--project', type=Path, default=Path('.'))
    ap.add_argument('--factor', type=int, choices=FACTORS, required=True)
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=9043)
    args = ap.parse_args()
    if args.host not in ('127.0.0.1', 'localhost', '::1') or not 1 <= args.port <= 65535:
        ap.error('Only local Cassandra is permitted')
    project = args.project.resolve()
    s2 = project / 'experiments' / 'rev3_s2_partition_growth'
    pre = read(s2 / 's1_preregister.json')
    if pre['frozen_rev3_B_sha256'] != FROZEN_SHA:
        raise ValueError('Wrong frozen B selection')
    experiment = Path(pre['original_B_experiment']).resolve()
    workspace = Path(pre['original_B_workspace']).resolve()
    # Import the same implementation modules as the original B/S1 runner.
    sys.path[:0] = [str(experiment),
                    str(project / 'SchemaLens_Rev3_Independent_Confirmatory'),
                    str(project / 'SchemaLens_V4_Rev3_PlanAware'),
                    str(project / 'SchemaLens_V4_Rev2_Confirmatory')]
    from rev3_confirmatory import gate
    from rev3_confirmatory.benchmark import baseline_dispatch
    from rev3.runtime import dispatch
    from iot_pipeline.shared import read_rows
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.offline import compare
    from v4_rev2.runtime_helpers import DAY
    from cassandra.cluster import Cluster

    exp, protocol, selected = gate.verify(workspace)
    if Path(exp).resolve() != experiment or protocol['rev3_selection_sha256'] != FROZEN_SHA:
        raise ValueError('Frozen workspace/catalog mismatch')
    designs = {x['deployment_id']: x for x in selected}
    if LABEL_U not in designs or LABEL_D not in designs:
        raise ValueError('Frozen TS2-U/D designs not found')
    cases_file = s2 / 's1_cases.json'
    if digest(cases_file) != pre['s1_cases_sha256']:
        raise ValueError('S1 cases changed')
    cases = read(cases_file)
    if len(cases) != 18 or len({q['s1_case_id'] for q in cases}) != 18:
        raise ValueError('Expected 18 distinct S1 query cases')
    gen = read(s2 / 'generated' / 'generation_manifest.json')
    dataset = gen['datasets'][str(args.factor)]
    datafile = s2 / 'generated' / dataset['file']
    if digest(datafile) != dataset['sha256']:
        raise ValueError('Generated dataset hash mismatch')
    rows = read_rows(datafile)
    if len(rows) != 54000 * args.factor:
        raise ValueError('Unexpected logical data size')
    sensors = sorted({r['sensor_name'] for r in rows})
    if len(sensors) != 2 or len({r['device_id'] for r in rows}) != 9:
        raise ValueError('Unexpected devices/sensors')
    days = {r['timestamp'] // DAY for r in rows}
    catalog_file = experiment / gate.REV3 / 'stage5' / '15_executable_plan_catalog.json'
    catalog = {r['plan_id']: r for r in read(catalog_file)}
    names = {LABEL_BASE: f'sl_s2_d{args.factor}_ref',
             LABEL_U: f'sl_s2_d{args.factor}_u',
             LABEL_D: f'sl_s2_d{args.factor}_d'}
    manifest_file = s2 / 's2_materialized' / f'{args.factor}x' / 'materialization_manifest.json'
    m = read(manifest_file)
    if m['status'] != 'S2_MATERIALIZATION_COMPLETE_NOT_BENCHMARKED' or m['keyspaces'] != names or m['dataset_sha256'] != digest(datafile):
        raise ValueError('Materialization provenance mismatch')
    out = s2 / 's2_materialized' / f'{args.factor}x' / 'semantic_validation.json'
    if out.exists():
        raise FileExistsError(f'Refusing to overwrite existing audit {out}')
    oracle = source_oracle()
    expected = {q['s1_case_id']: oracle(rows, q) for q in cases}
    result = []
    cluster = Cluster([args.host], port=args.port)
    try:
        session = cluster.connect()
        existing = {r.keyspace_name for r in session.execute('SELECT keyspace_name FROM system_schema.keyspaces')}
        if not set(names.values()).issubset(existing):
            raise ValueError('One or more S2 keyspaces missing')
        for q in cases:
            case = q['s1_case_id']
            for label, ks in names.items():
                session.set_keyspace(ks)
                if label == LABEL_BASE:
                    got, trace = baseline_dispatch(session, q, True, sensors, days, 16, oracle)
                else:
                    got, trace = dispatch(session, q, designs[label], catalog, sensors, days, 16, oracle)
                if not compare(got, expected[case]):
                    raise AssertionError('Semantic mismatch: '+case+' / '+label)
                result.append({'case_id': case, 'query_id': q['query_id'],
                               'factor': args.factor, 'variant': label,
                               'keyspace': ks, 'status': 'PASS',
                               'execute_calls': trace['execute_calls'],
                               'iterated_rows': trace['iterated_rows'],
                               'plan': trace['physical_plan']})
            print('PASS', case, '3/3 configurations', flush=True)
    finally:
        cluster.shutdown()
    if len(result) != 54:
        raise AssertionError('Expected 54 validations')
    report = {'status': 'PASS_ALL', 'factor': args.factor,
              'source_dataset_sha256': digest(datafile),
              'case_sha256': digest(cases_file),
              'frozen_rev3_sha256': FROZEN_SHA,
              'validations': len(result),
              'measurement_status': 'NOT_BENCHMARKED',
              'telemetry_note': 'Python-side request/row counts; not server-side IO',
              'results': result}
    with out.open('x', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.write('\n')
    print('PASS ALL:', len(result), 'read-only semantic validations')
    print('AUDIT:', out)


if __name__ == '__main__':
    main()
