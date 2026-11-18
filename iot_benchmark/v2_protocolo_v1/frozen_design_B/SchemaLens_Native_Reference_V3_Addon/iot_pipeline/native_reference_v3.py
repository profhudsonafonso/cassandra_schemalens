"""Independent Cassandra TABLE-shaped reference for an immutable native Java-object V3 experiment.

This is a research translation of IoTDB table structure, NOT an upstream
Cassandra schema, a native IoTDB benchmark score, or SQL/frequency replay.

Only this module's explicit --live path writes Cassandra. Stage4-8, routing,
physical candidate definitions, and frozen HBench remain untouched.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys
import time

from .shared import ROOT, A1, inputs, read, read_rows, sha, write, csv_write, path4
from .offline import compare
from .stage9 import confirm_frozen, _pctl
from .reference_baseline import (columns_for, table_ddl, build_wide_records,
                                 TABLE)

KEYSPACE = 'sl_iot_ref_native_v3'
RESET_TOKEN = 'RESET_SL_IOT_REF_NATIVE_V3'
NATIVE_LABEL = 'REF_NATIVE_IOT_TABLE_CASSANDRA_V3'
NATIVE_SCOPE = 'JAVA_QUERY_OBJECT_SAMPLE_NOT_SQL_REPLAY'


def source_oracle():
    """Use exactly the same logical evaluator as the frozen HBench Stage9."""
    if str(A1) not in sys.path:
        sys.path.insert(0, str(A1))
    from core.bridge import _result
    return _result


def native_contract(rows: list[dict], queries: list[dict], sensors: list[str],
                    capture: Path | None = None) -> dict:
    """Reject illustrative/unrelated workloads and malformed Java parameter objects."""
    if not rows or not sensors:
        raise ValueError('Empty native dataset')
    columns_for(sensors)
    if {x['sensor_name'] for x in rows} != set(sensors):
        raise ValueError('Sensor inventory differs from normalized native dataset')
    ids = [q.get('query_id') for q in queries]
    if len(ids) != 12 or set(ids) != {f'Q{i}' for i in range(1, 13)}:
        raise ValueError('Native V3 requires exactly one sample from each of Q1-Q12')
    devices = {x['device_id'] for x in rows}
    captures = set()
    for q in queries:
        qid = q['query_id']
        origin = q.get('parameter_origin', '')
        if 'sample from official GenerateQueryWorkLoad Java API' not in origin:
            raise ValueError(f'{qid} is not a Java-generated parameter-object sample')
        cap_sha = q.get('capture_sha256')
        if not isinstance(cap_sha, str) or len(cap_sha) != 64:
            raise ValueError(f'{qid} missing native capture SHA-256')
        captures.add(cap_sha)
        if not q.get('devices') or not q.get('sensors'):
            raise ValueError(f'{qid} missing projected devices or sensors')
        if not set(q['devices']) <= devices or not set(q['sensors']) <= set(sensors):
            raise ValueError(f'{qid} references devices/sensors absent from the native data')
        if qid == 'Q12':
            children = q.get('set_children')
            if not isinstance(children, list) or len(children) < 2:
                raise ValueError('Q12 requires at least two explicit Java child operands')
            if q.get('set_op') not in {'union', 'intersect', 'except'}:
                raise ValueError('Q12 has unknown set operation')
            first_projection = tuple(children[0]['sensors'])
            for ch in children:
                if tuple(ch['sensors']) != first_projection:
                    raise ValueError('Q12 operands use incompatible projections')
                if not set(ch['devices']) <= devices or not set(ch['sensors']) <= set(sensors):
                    raise ValueError('Q12 child refers to an absent device/sensor')
                if int(ch['start_ms']) >= int(ch['end_ms_exclusive']):
                    raise ValueError('Q12 child inverted time interval')
        if qid in {'Q5','Q8'}:
            continue  # No temporal limit: validate against all timestamps.
        if qid == 'Q1':
            if not isinstance(q.get('point_ms'), int):
                raise ValueError('Q1 missing point timestamp')
        elif int(q['start_ms']) >= int(q['end_ms_exclusive']):
            raise ValueError(f'{qid} inverted time interval')
    if len(captures) != 1:
        raise ValueError('Query objects do not originate from the same Java capture')
    if capture is not None:
        capture = Path(capture)
        if not capture.is_file() or sha(capture) != next(iter(captures)):
            raise ValueError('Supplied Java capture does not match frozen query provenance')
    return {'query_classes': ids, 'capture_sha256': next(iter(captures)),
            'capture_bytes_verified': capture is not None,
            'sensor_columns': columns_for(sensors), 'logical_metric_cells': len(rows)}


def requests_for(q: dict) -> list[tuple[str, tuple[str, ...], int | None, int | None]]:
    """One Cassandra key-scoped range per device/child operand.

    Cassandra receives a half-open [start,end) bound. Q5/Q8 scan the full
    device partition. For Q12 we never replace distinct children by the
    approximate outer min/max range; overlapping child results are deduped.
    """
    qid = q['query_id']
    operands = q['set_children'] if qid == 'Q12' else [q]
    requests = []
    for op in operands:
        projection = tuple(op['sensors'])
        if qid in {'Q5', 'Q8'}:
            start = end = None
        elif qid == 'Q1':
            start = int(q['point_ms'])
            end = start + 1
        else:
            start, end = int(op['start_ms']), int(op['end_ms_exclusive'])
        if start is not None and start >= end:
            raise ValueError('Empty/inverted Cassandra query bound')
        requests.extend((device, projection, start, end) for device in op['devices'])
    return list(dict.fromkeys(requests))


def physical_to_raw(wide: list[tuple], sensors: list[str], q: dict) -> list[dict]:
    """Independent TABLE row projection emulating the Cassandra read route."""
    indexes = {s: i for i, s in enumerate(sensors)}
    out = {}
    for device, projection, start, end in requests_for(q):
        for d, t, *values in wide:
            if d != device or (start is not None and not (start <= t < end)):
                continue
            for sensor in projection:
                value = values[indexes[sensor]]
                if value is not None:
                    out[(d, sensor, t)] = dict(device_id=d, sensor_name=sensor,
                                                timestamp=t, value_double=float(value))
    return list(out.values())


def fetch_query_records(session, q: dict, sensors: list[str]) -> list[dict]:
    """Read only partition-key-bound CQL; no ALLOW FILTERING and no SQL joins."""
    columns = set(columns_for(sensors))
    out = {}
    for device, projection, start, end in requests_for(q):
        selected = [('m_' + sensor, sensor) for sensor in projection]
        if any(col not in columns for col, _ in selected):
            raise ValueError('Unknown selected Cassandra metric column')
        statement = ('SELECT timestamp_ms,' + ','.join(col for col, _ in selected)
                     + f' FROM {TABLE} WHERE device_id=%s')
        params = (device,)
        if start is not None:
            statement += ' AND timestamp_ms >= %s AND timestamp_ms < %s'
            params += (start, end)
        for dbrow in session.execute(statement, params):
            t = int(dbrow.timestamp_ms)
            for col, sensor in selected:
                value = getattr(dbrow, col, None)
                if value is not None:
                    out[(device, sensor, t)] = {
                        'device_id': device, 'sensor_name': sensor,
                        'timestamp': t, 'value_double': float(value)}
    return list(out.values())


def offline_checks(rows: list[dict], queries: list[dict], sensors: list[str]) -> tuple[int,int]:
    oracle = source_oracle()
    wide = build_wide_records(rows, sensors)
    for q in queries:
        expected = oracle(rows, q)
        result = oracle(physical_to_raw(wide, sensors, q), q)
        if not compare(expected, result):
            raise AssertionError(f'Native baseline offline mismatch on {q["query_id"]}')
    return len(wide), len(queries)


def prepare(capture: Path | None = None):
    selected = confirm_frozen()
    rows = read_rows(inputs()['native'])
    queries = read(inputs()['queries'])
    sensors = sorted({r['sensor_name'] for r in rows})
    validated = native_contract(rows, queries, sensors, capture)
    physical, passed = offline_checks(rows, queries, sensors)
    hashes = {
        'hbench_sha256': sha(path4(8,'28_hbench_frozen.json')),
        'source_rows_sha256': sha(inputs()['native']),
        'query_instances_sha256': sha(inputs()['queries']),
    }
    return selected, rows, queries, sensors, physical, passed, validated, hashes


def dry_run(output: Path, capture: Path | None = None):
    selected, rows, queries, sensors, physical, checked, meta, hashes = prepare(capture)
    output.mkdir(parents=True, exist_ok=True)
    path = output/'NATIVE_REFERENCE_PRECHECK.json'
    if path.exists():
        old = read(path)
        if old.get('frozen_hashes') != hashes:
            raise ValueError('Refusing to overwrite precheck for a different frozen experiment')
    write(path, {'status':'PASS_OFFLINE_ONLY', 'schema_class':'CASSANDRA_TRANSLATED_IOTDB_TABLE',
                 'selection_modified':False, 'Cassandra_used':False,
                 'baseline_part_of_hbench':False, 'query_semantics':'Java parameter-object sample; not SQL replay',
                 'baseline_ddl':table_ddl(sensors), 'physical_device_time_rows':physical,
                 'validated_queries':checked, 'frozen_deployment_ids':[x['deployment_id'] for x in selected],
                 'frozen_hashes':hashes, 'native_capture':meta})
    print(table_ddl(sensors))
    print(f'PASS: {physical} wide rows; {len(rows)} metric cells; {checked}/12 queries; '
          f'{len(selected)} frozen HBench deployments. NO CASSANDRA.')
    print('Precheck:',path)


def run_live(args):
    if args.confirm_reset != RESET_TOKEN:
        raise ValueError(f'Explicit destructive confirmation required: --confirm-reset {RESET_TOKEN}')
    if args.warmup < 0 or args.rounds < 5:
        raise ValueError('warmup >=0 and rounds >=5 required')
    if args.output.exists() and any(args.output.iterdir()):
        raise ValueError('Output directory not empty: refusing overwrite')
    _, rows, qs, sensors, physical, checked, meta, hashes = prepare(args.capture)
    wide = build_wide_records(rows, sensors)
    oracle = source_oracle()
    from cassandra.cluster import Cluster  # only the explicit --live action imports Cassandra
    cluster = Cluster([args.host], port=args.port)
    latencies = []
    summaries = []
    load = None
    success = False
    try:
        session = cluster.connect()
        session.execute(f'DROP KEYSPACE IF EXISTS {KEYSPACE}')
        session.execute(f"CREATE KEYSPACE {KEYSPACE} WITH replication = "
                        "{'class':'SimpleStrategy','replication_factor':1} AND durable_writes=true")
        session.set_keyspace(KEYSPACE)
        session.execute(table_ddl(sensors))
        insert_cols = ['device_id', 'timestamp_ms', *columns_for(sensors)]
        statement = session.prepare(f'INSERT INTO {TABLE} ({",".join(insert_cols)}) '
                                    f'VALUES ({",".join("?" for _ in insert_cols)})')
        started = time.perf_counter_ns()
        for row in wide:
            session.execute(statement, row)
        elapsed = (time.perf_counter_ns() - started) / 1e9
        load = {'deployment_id':NATIVE_LABEL, 'logical_metric_cells':len(rows),
                'physical_rows_written':len(wide), 'load_elapsed_seconds':elapsed,
                'logical_points_per_second':len(rows) / max(elapsed, 1e-9),
                'scope':'sequential Python Cassandra loader; not official ingestion throughput'}
        for q in qs:
            got = oracle(fetch_query_records(session, q, sensors),q)
            exp = oracle(rows,q)
            if not compare(exp, got):
                raise AssertionError(f'LIVE baseline incorrect: {q["query_id"]}; no timings collected')
        for q in qs:
            for _ in range(args.warmup):
                oracle(fetch_query_records(session, q, sensors), q)
            samples = []
            for _ in range(args.rounds):
                start = time.perf_counter_ns()
                oracle(fetch_query_records(session, q, sensors), q)
                samples.append((time.perf_counter_ns() - start)/1e6)
            summaries.append({'deployment_id':NATIVE_LABEL, 'query_id':q['query_id'],
                              'family':'REFERENCE_NATIVE_TABLE','mean_ms':statistics.mean(samples),
                              'median_ms':statistics.median(samples), 'p95_ms':_pctl(samples,95),
                              'p99_ms':_pctl(samples,99),'successful_operations':len(samples),
                              'correctness':'PASS'})
            latencies.extend({'deployment_id':NATIVE_LABEL, 'query_id':q['query_id'],
                              'rep':i+1,'latency_ms':v} for i,v in enumerate(samples))
        success = True
    finally:
        cluster.shutdown()
    if success:
        out = args.output
        out.mkdir(parents=True,exist_ok=True)
        csv_write(out/'34_load_metrics.csv',[load],['deployment_id','logical_metric_cells',
                  'physical_rows_written','load_elapsed_seconds','logical_points_per_second','scope'])
        csv_write(out/'35_query_p95.csv',summaries,['deployment_id','query_id','family',
                  'mean_ms','median_ms','p95_ms','p99_ms','successful_operations','correctness'])
        csv_write(out/'36_raw_latencies.csv',latencies,['deployment_id','query_id','rep','latency_ms'])
        write(out/'37_manifest.json',{'status':'NATIVE_REFERENCE_POST_FREEZE',
              'deployment_id':NATIVE_LABEL,'official_iot_benchmark_score':False,
              'official_cassandra_schema':False,'Cassandra_used':True,
              'baseline_part_of_hbench':False,'baseline_ddl':table_ddl(sensors),
              'native_query_sql_replay':False,'mixed_workload_frequency_replay':False,
              'semantic_scope':NATIVE_SCOPE, 'keyspace':KEYSPACE,
              **hashes, 'capture_sha256':meta['capture_sha256'],
              'native_capture_file_verified':meta['capture_bytes_verified'],
              'correctness':f'PASS_ALL_{checked}_QUERIES',
              'row_count':physical,'logical_metric_cells':len(rows),
              'measurement_rounds':args.rounds,'warmup':args.warmup,
              'limitations':['Native IoT Benchmark query-object SAMPLE, not the Java query order/frequency or SQL replay',
                             'No official Cassandra driver or official IoT Benchmark score',
                             'Client-side Python postprocessing, fixed query order; caches/order bias possible',
                             'DOUBLE/ms/homogeneous sensors only; sequential Python load is not throughput']})
        print(f'LIVE PASS: {checked} query correctness checks, {physical} physical rows; '
              f'metrics in {out}')


def main():
    parser = argparse.ArgumentParser(description='Independent V3 Cassandra table-shaped native Java-object baseline')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--dry-run',action='store_true')
    mode.add_argument('--live',action='store_true')
    parser.add_argument('--capture',type=Path,help='Original native_queries_q12fixed*.jsonl for source hash audit')
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=9043)
    parser.add_argument('--warmup',type=int,default=5)
    parser.add_argument('--rounds',type=int,default=20)
    parser.add_argument('--confirm-reset',default='')
    parser.add_argument('--output',type=Path,default=ROOT/'outputs/native_reference_v3')
    args = parser.parse_args()
    if args.dry_run:
        dry_run(args.output,args.capture)
    else:
        run_live(args)

if __name__ == '__main__':
    main()
