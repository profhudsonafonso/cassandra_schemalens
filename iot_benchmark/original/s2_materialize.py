#!/usr/bin/env python3
"""SchemaLens Rev3 S2: guarded CREATE-ONLY Cassandra materialization.

Run from project root after s2_growth.py prepare/validate.
  python s2_materialize.py check --project .
  python s2_materialize.py materialize --project . --factor 1 --confirm CREATE_NEW_S2_KEYSPACES

Never drops/truncates/alters existing keyspaces. Refuses a partial/repeated factor.
Original B source and frozen selection are verified before any database DDL.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import re
import sys
import time
from pathlib import Path

FACTORS = (1, 2, 4, 8)
LABEL_BASE = 'BASELINE_Q8_OPTIMIZED'
LABEL_U = 'R3H_38939db96444'
LABEL_D = 'R3H_d2ea1974de53'
CONFIRM = 'CREATE_NEW_S2_KEYSPACES'
EXPECTED_B_SHA = '8f88693851245838a7485300f8ce70aa850bd65c0c2d881225e9365cbcb39231'


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for part in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(part)
    return digest.hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf8'))


def write_new(path, obj):
    path = Path(path)
    with path.open('x', encoding='utf8') as stream:
        json.dump(obj, stream, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
        stream.write('\n')


def paths(project, factor):
    project = project.resolve()
    s2 = project / 'experiments' / 'rev3_s2_partition_growth'
    return s2, s2 / 'generated', s2 / 's2_materialized' / f'{factor}x'


def load_verified(project, factor):
    if factor not in FACTORS:
        raise ValueError('Unknown density factor')
    s2, generated, out = paths(project, factor)
    if not (s2 / 's1_preregister.json').is_file():
        raise FileNotFoundError('Expected S2 package below PROJECT/experiments/rev3_s2_partition_growth')
    # Reuse the validated offline S2 script; do not regenerate or overwrite data.
    sys.path.insert(0, str(project.resolve()))
    import s2_growth
    s2_growth.validate(project.resolve())
    if s2_growth.EXPECTED_B_SHA != EXPECTED_B_SHA:
        raise ValueError('S2 generator bound to a different B freeze')
    prereg = read(s2 / 's1_preregister.json')
    draft = read(s2 / 's2_preregister_draft.json')
    manifest = read(generated / 'generation_manifest.json')
    entry = manifest['datasets'][str(factor)]
    source = generated / entry['file']
    if sha(source) != entry['sha256'] or entry['cells'] != 54000 * factor:
        raise ValueError('S2 density file changed or incorrect cell count')
    if draft['original_B_rev3_sha256'] != EXPECTED_B_SHA:
        raise ValueError('Wrong frozen selection SHA in S2 draft')
    if (draft['frozen_variants']['ts2_iot_wide_unbucketed'] != LABEL_U or
            draft['frozen_variants']['ts2_iot_wide'] != LABEL_D):
        raise ValueError('S2 frozen variant IDs mismatch')
    if prereg['frozen_rev3_B_sha256'] != EXPECTED_B_SHA:
        raise ValueError('S1 does not match frozen B')
    workspace = Path(prereg['original_B_workspace']).resolve()
    experiment = Path(prereg['original_B_experiment']).resolve()
    if not workspace.is_dir() or not experiment.is_dir():
        raise FileNotFoundError('Original B workspace or experiment is unavailable')
    if out.exists():
        raise FileExistsError(f'Output for {factor}x already exists: {out}. Refusing rerun.')
    # Make the precise original B experiment take priority for iot_pipeline,
    # rather than importing the current project or some other archived version.
    sys.path.insert(0, str(experiment))
    sys.path.insert(1, str(project / 'SchemaLens_Rev3_Independent_Confirmatory'))
    sys.path.insert(2, str(project / 'SchemaLens_V4_Rev3_PlanAware'))
    sys.path.insert(3, str(project / 'SchemaLens_V4_Rev2_Confirmatory'))
    from rev3_confirmatory import gate
    verified_exp, protocol, selected = gate.verify(workspace)
    if Path(verified_exp).resolve() != experiment:
        raise ValueError('Original B experiment path does not agree with gate')
    if protocol['rev3_selection_sha256'] != EXPECTED_B_SHA:
        raise ValueError('Original B frozen SHA mismatch')
    chosen = {h['canonical_source']: h for h in selected
              if h.get('canonical_source') in ('ts2_iot_wide_unbucketed', 'ts2_iot_wide')}
    if set(chosen) != {'ts2_iot_wide_unbucketed', 'ts2_iot_wide'}:
        raise ValueError('Missing exact B TS2-U/D frozen design')
    if chosen['ts2_iot_wide_unbucketed']['deployment_id'] != LABEL_U:
        raise ValueError('Unbucketed deployment ID does not match the B selection')
    if chosen['ts2_iot_wide']['deployment_id'] != LABEL_D:
        raise ValueError('Daily bucketed deployment ID does not match the B selection')
    import iot_pipeline.shared as shared
    if Path(shared.ROOT).resolve() != experiment:
        raise ValueError(f'Unexpected iot_pipeline source: {shared.ROOT}; expected {experiment}')
    # Materializer uses this global stage4 specification catalog. It must
    # actually include the exact candidate IDs used in both B designs.
    catalog_path = shared.path4(4, '10_candidate_specs.json')
    catalog = shared.read(catalog_path)
    cids = {x['candidate_id'] for x in catalog}
    for h in chosen.values():
        if not set(h['candidates']) <= cids:
            raise ValueError('Frozen design references candidates absent from original B Stage4 catalog')
    if not (experiment / 'phase_a1/outputs/normalized_double_readings.csv').is_file():
        raise ValueError('Original B source file is missing')
    if sha(experiment / 'phase_a1/outputs/normalized_double_readings.csv') != prereg['source_data_sha256']:
        raise ValueError('Original B source hash mismatch')
    if sha(experiment / 'phase_a1/outputs/query_instances_pilot.json') != prereg['source_queries_sha256']:
        raise ValueError('Original B query hash mismatch')
    if sha(s2 / 's1_cases.json') != prereg['s1_cases_sha256']:
        raise ValueError('S1 case hash mismatch')
    names = {
        LABEL_BASE: f'sl_s2_d{factor}_ref',
        LABEL_U: f'sl_s2_d{factor}_u',
        LABEL_D: f'sl_s2_d{factor}_d',
    }
    for name in names.values():
        if not re.fullmatch(r'sl_s2_d(?:1|2|4|8)_(?:ref|u|d)', name):
            raise ValueError('Unexpected keyspace name')
    return dict(s2=s2, generated=generated, out=out, source=source,
                entry=entry, names=names, chosen=chosen, catalog_path=catalog_path,
                project=project.resolve(), experiment=experiment,
                original_workspace=workspace, protocol=protocol, prereg=prereg)


def rows_from_csv(path):
    from iot_pipeline.shared import read_rows
    rows = read_rows(path)
    sensors = sorted({r['sensor_name'] for r in rows})
    if len(sensors) != 2 or len({r['device_id'] for r in rows}) != 9:
        raise ValueError('Unexpected sensor/device count')
    return rows, sensors


def run_check(project, factor):
    x = load_verified(project, factor)
    print('PASS B frozen design, Stage4 catalog, source and S2 data hashes')
    print('Original experiment:', x['experiment'])
    print('Factor:', factor, 'cells:', x['entry']['cells'])
    print('Stage4 catalog:', x['catalog_path'])
    print('Planned new keyspaces:')
    for label, name in x['names'].items():
        print(' ', label, '=>', name)
    return x


def create_keyspace(session, name):
    if not re.fullmatch(r'sl_s2_d(?:1|2|4|8)_(?:ref|u|d)', name):
        raise ValueError('Unsafe keyspace')
    session.execute('CREATE KEYSPACE '+name+
        " WITH replication = {'class':'SimpleStrategy','replication_factor':1} AND durable_writes=true")
    session.set_keyspace(name)


def materialize(project, factor, host, port, confirm):
    if confirm != CONFIRM:
        raise PermissionError('Explicit CREATE_ONLY confirmation required')
    if host not in ('127.0.0.1', 'localhost', '::1') or not 1 <= port <= 65535:
        raise ValueError('Only local Cassandra is supported')
    x = run_check(project, factor)
    from cassandra.cluster import Cluster
    from iot_pipeline.stage9 import _materialize
    from iot_pipeline.reference_baseline import table_ddl
    from iot_pipeline.shared import require_policy
    from v4_rev2.runtime_helpers import load_wide
    rows, sensors = rows_from_csv(x['source'])
    if len(rows) != x['entry']['cells']:
        raise ValueError('In-memory row count mismatch')
    policy = require_policy()
    cluster = Cluster([host], port=port)
    created = []
    report = []
    try:
        session = cluster.connect()
        # No DDL at all if *any* of these reserved names already exists.
        for name in x['names'].values():
            present = list(session.execute(
                'SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=%s', (name,)))
            if present:
                raise FileExistsError('Refusing pre-existing Cassandra keyspace: '+name)
        x['out'].mkdir(parents=True, exist_ok=False)
        write_new(x['out']/'pre_create_manifest.json', {
            'status': 'S2_PRECREATE_VERIFIED', 'factor': factor,
            'source_sha256': sha(x['source']),
            'frozen_B_sha256': EXPECTED_B_SHA,
            'original_B_keyspaces_untouched': True,
            'new_keyspaces': x['names'], 'cassandra_host': host, 'port': port,
            'no_drop_truncate_or_alter': True,
            'materializer_source': str(x['catalog_path']),
        })
        for label in (LABEL_BASE, LABEL_U, LABEL_D):
            name = x['names'][label]
            create_keyspace(session, name)
            created.append(name)
            started = time.perf_counter()
            if label == LABEL_BASE:
                session.execute(table_ddl(sensors))
                writes = load_wide(session, rows, sensors, 'iot_reference_table', False)
            else:
                h = x['chosen']['ts2_iot_wide_unbucketed' if label == LABEL_U else 'ts2_iot_wide']
                writes = sum(_materialize(session, h, rows, sensors, policy).values())
            line = {'variant': label, 'keyspace': name, 'logical_cells': len(rows),
                    'physical_write_operations': writes,
                    'sequential_load_seconds': round(time.perf_counter()-started, 6),
                    'load_scope': 'Python sequential loader; not official ingestion throughput'}
            report.append(line)
            write_new(x['out']/f'load_{name}.json', line)
            print('LOADED', name, 'writes=', writes, flush=True)
        write_new(x['out']/'materialization_manifest.json', {
            'status': 'S2_MATERIALIZATION_COMPLETE_NOT_BENCHMARKED',
            'factor': factor, 'dataset_sha256': sha(x['source']),
            'original_B_selection_sha256': EXPECTED_B_SHA,
            'keyspaces': x['names'], 'loads': report,
            'no_prior_keyspace_modified': True,
            'no_official_java_data_generation': True,
            'client_side_load_not_throughput': True,
        })
        print('COMPLETE:', factor, 'x materialization. Benchmark NOT executed.')
    except BaseException as exc:
        if x['out'].exists() and not (x['out']/'failure_manifest.json').exists():
            write_new(x['out']/'failure_manifest.json', {
                'status': 'FAILED_PRESERVE_PARTIAL_KEYSPACES',
                'created_keyspaces': created, 'error_type': type(exc).__name__,
                'error': str(exc), 'no_automatic_cleanup': True,
            })
        raise
    finally:
        cluster.shutdown()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'materialize'))
    parser.add_argument('--factor', type=int, choices=FACTORS, default=1)
    parser.add_argument('--project', type=Path, default=Path.cwd())
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9043)
    parser.add_argument('--confirm', default='')
    args = parser.parse_args()
    if args.action == 'check':
        run_check(args.project, args.factor)
    else:
        materialize(args.project, args.factor, args.host, args.port, args.confirm)


if __name__ == '__main__':
    main()
