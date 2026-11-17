#!/usr/bin/env python3
"""SchemaLens Rev3 S2: safe, offline-only density generation and validation.

This implementation deliberately does NOT connect to Cassandra or modify frozen
B/S1 experiments. Synthetic interpolation is explicitly NOT new Java output.

Commands:
  python s2_growth.py prepare --project /path/to/project
  python s2_growth.py validate --project /path/to/project
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from collections import defaultdict
from pathlib import Path

FACTORS = (1, 2, 4, 8)
FIELDS = ('device_id', 'sensor_name', 'timestamp', 'value_double')
EXPECTED_SOURCE_SHA = '047c269a42beea21333d7dbecdbde798862346015a35a5b1ced232b05a8f2c2a'
EXPECTED_S1_CASES_SHA = '31fd36c631d5ef2430087f57bb1fdfd000dbf956e415c64db5577d8c6c32d3e7'
EXPECTED_B_SHA = '8f88693851245838a7485300f8ce70aa850bd65c0c2d881225e9365cbcb39231'


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def json_read(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def json_create(path: Path, data):
    with path.open('x', encoding='utf-8') as handle:
        json.dump(data, handle, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
        handle.write('\n')


def locations(project: Path):
    s2 = project / 'experiments' / 'rev3_s2_partition_growth'
    return s2, s2 / 'generated'


def frozen_inputs(project: Path):
    s2, _ = locations(project)
    s1 = json_read(s2 / 's1_preregister.json')
    protocol = json_read(s2 / 's2_preregister_draft.json')
    if protocol['status'] != 'DRAFT_NOT_EXECUTED':
        raise ValueError('Unexpected draft protocol status')
    if protocol['original_B_rev3_sha256'] != EXPECTED_B_SHA:
        raise ValueError('Unexpected B freeze SHA')
    if tuple(protocol['density_factors']) != FACTORS:
        raise ValueError('Unexpected density factors')
    if protocol['source_cells_at_1x'] != 54000:
        raise ValueError('Unexpected original row count')
    if digest(s2 / 's1_cases.json') != EXPECTED_S1_CASES_SHA:
        raise ValueError('The S1 cases have changed')
    if s1['frozen_rev3_B_sha256'] != EXPECTED_B_SHA:
        raise ValueError('S1 references another frozen B experiment')
    exp = Path(s1['original_B_experiment']).resolve()
    source = exp / 'phase_a1' / 'outputs' / 'normalized_double_readings.csv'
    if not source.is_file():
        raise FileNotFoundError(f'Original B data not found: {source}')
    if digest(source) != EXPECTED_SOURCE_SHA or digest(source) != s1['source_data_sha256']:
        raise ValueError('Original B source CSV hash mismatch; no generation performed')
    if s1['source_queries_sha256'] != protocol['source_queries_sha256_s1']:
        raise ValueError('Captured queries provenance mismatch')
    return s2, source, s1, protocol


def read_grouped(source: Path):
    devices = defaultdict(lambda: defaultdict(dict))
    count = 0
    with source.open(encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != FIELDS:
            raise ValueError('Unexpected source CSV column order')
        for row in reader:
            d, s, t = row['device_id'], row['sensor_name'], int(row['timestamp'])
            v = float(row['value_double'])
            if not (-float('inf') < v < float('inf')):
                raise ValueError('Non-finite source value')
            if t in devices[d][s]:
                raise ValueError(f'Duplicate original primary key: {d}/{s}/{t}')
            devices[d][s][t] = v
            count += 1
    if count != 54000 or len(devices) != 9:
        raise ValueError(f'Unexpected original source size: {count} cells, {len(devices)} devices')
    sensors = sorted({s for by_sensor in devices.values() for s in by_sensor})
    if len(sensors) != 2:
        raise ValueError('Expected exactly two sensors')
    for d, by_sensor in devices.items():
        if set(by_sensor) != set(sensors):
            raise ValueError(f'Incomplete sensor coverage for {d}')
        if set(by_sensor[sensors[0]]) != set(by_sensor[sensors[1]]):
            raise ValueError(f'Non-aligned sensor timestamps for {d}')
    return devices, sensors, count


def one_device_points(by_sensor: dict, sensors: list[str], factor: int):
    """Generate exactly n*factor unique timestamps inside the original span.

    Original timestamps and sensor values are retained. Interior samples use
    linear interpolation. The final factor-1 points are placed immediately
    before the last timestamp; this deterministic terminal treatment is logged.
    """
    times = sorted(by_sensor[sensors[0]])
    if len(times) < 2:
        raise ValueError('Need at least two original timestamps per device')
    if factor == 1:
        for t in times:
            yield t, {s: by_sensor[s][t] for s in sensors}
        return
    generated = set()
    for idx, (left, right) in enumerate(zip(times, times[1:])):
        gap = right - left
        if gap < factor * factor + factor:
            raise ValueError(f'Timestamp gap {gap} ms too small for safe factor {factor}; device interval {idx}')
        for k in range(factor):
            t = left + (gap * k) // factor
            if t in generated or t >= right:
                raise ValueError('Timestamp collision during interpolation')
            generated.add(t)
            if k == 0:
                values = {s: by_sensor[s][left] for s in sensors}
            else:
                fraction = (t - left) / gap
                values = {s: by_sensor[s][left] + (by_sensor[s][right] - by_sensor[s][left]) * fraction for s in sensors}
            yield t, values
    last, before_last = times[-1], times[-2]
    gap = last - before_last
    for t in range(last - (factor - 1), last + 1):
        if t in generated or t < before_last:
            raise ValueError('Collision in terminal interpolation')
        generated.add(t)
        if t == last:
            values = {s: by_sensor[s][last] for s in sensors}
        else:
            fraction = (t - before_last) / gap
            values = {s: by_sensor[s][before_last] + (by_sensor[s][last] - by_sensor[s][before_last]) * fraction for s in sensors}
        yield t, values
    if len(generated) != len(times) * factor:
        raise ValueError(f'Generated {len(generated)} timestamps; expected {len(times)*factor}')


def create_density_csv(target: Path, groups: dict, sensors: list[str], factor: int):
    count = 0
    with target.open('x', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for device in sorted(groups):
            for timestamp, values in one_device_points(groups[device], sensors, factor):
                for sensor in sensors:
                    writer.writerow({'device_id': device, 'sensor_name': sensor,
                                     'timestamp': timestamp, 'value_double': repr(values[sensor])})
                    count += 1
    if count != 54000 * factor:
        raise ValueError(f'Generated wrong cell count: {count} versus {54000*factor}')
    return count


def prepare(project: Path):
    s2, source, s1, protocol = frozen_inputs(project)
    groups, sensors, original_n = read_grouped(source)
    generated = s2 / 'generated'
    if generated.exists():
        raise FileExistsError(f'Refusing to overwrite S2 generation folder: {generated}')
    generated.mkdir(parents=True, exist_ok=False)
    paths = {}
    for factor in FACTORS:
        filename = f's2_density_{factor}x.csv'
        target = generated / filename
        if factor == 1:
            with target.open('xb') as handle, source.open('rb') as source_handle:
                shutil.copyfileobj(source_handle, handle)
            rows = original_n
        else:
            rows = create_density_csv(target, groups, sensors, factor)
        paths[str(factor)] = {'file': filename, 'cells': rows, 'sha256': digest(target)}
        print(f'PREPARED {factor}x: {rows} cells, sha256={paths[str(factor)]["sha256"]}', flush=True)
    manifest = {
        'status': 'S2_SYNTHETIC_DENSITY_DATA_PREPARED_NOT_BENCHMARKED',
        'generation_method': 'DETERMINISTIC_INTERPOLATION_WITH_ORIGINAL_POINTS_RETAINED',
        'not_new_java_generated_data': True,
        'not_independent_holdout': True,
        'terminal_interpolation': 'factor-1 extra points in final original timestamp interval',
        'source_sha256': digest(source),
        's1_cases_sha256': digest(s2 / 's1_cases.json'),
        's2_protocol_sha256': digest(s2 / 's2_preregister_draft.json'),
        'original_B_freeze_sha256': EXPECTED_B_SHA,
        'device_count': len(groups), 'sensors': sensors,
        'original_min_ms': min(min(groups[d][s]) for d in groups for s in sensors),
        'original_max_ms': max(max(groups[d][s]) for d in groups for s in sensors),
        'datasets': paths,
        'cassandra_changes': False,
    }
    json_create(generated / 'generation_manifest.json', manifest)
    print('Generation complete. No Cassandra access. Run: validate')


def validate(project: Path):
    s2, source, s1, protocol = frozen_inputs(project)
    generated = s2 / 'generated'
    m = json_read(generated / 'generation_manifest.json')
    if m['source_sha256'] != digest(source) or m['s1_cases_sha256'] != digest(s2 / 's1_cases.json'):
        raise ValueError('Source/case hash mismatch')
    if m['s2_protocol_sha256'] != digest(s2 / 's2_preregister_draft.json'):
        raise ValueError('S2 draft protocol changed after generation')
    original, sensors, _ = read_grouped(source)
    min_orig = min(min(original[d][s]) for d in original for s in sensors)
    max_orig = max(max(original[d][s]) for d in original for s in sensors)
    for factor in FACTORS:
        info = m['datasets'][str(factor)]
        path = generated / info['file']
        if digest(path) != info['sha256']:
            raise ValueError(f'Modified or corrupt file: {path}')
        counts = defaultdict(int)
        keyset = set()
        originals_seen = 0
        min_ts = None
        max_ts = None
        with path.open(encoding='utf-8', newline='') as stream:
            rdr = csv.DictReader(stream)
            if tuple(rdr.fieldnames or ()) != FIELDS:
                raise ValueError('Column mismatch')
            for row in rdr:
                d, s, t = row['device_id'], row['sensor_name'], int(row['timestamp'])
                v = float(row['value_double'])
                if not (-float('inf') < v < float('inf')):
                    raise ValueError('Non-finite generated value')
                if d not in original or s not in sensors:
                    raise ValueError('New device or sensor found')
                key = (d, s, t)
                if key in keyset:
                    raise ValueError('Duplicate Cassandra logical primary key')
                keyset.add(key)
                if t in original[d][s]:
                    if v != original[d][s][t]:
                        raise ValueError('Original sensor value changed at original timestamp')
                    originals_seen += 1
                counts[(d, s)] += 1
                min_ts = t if min_ts is None else min(min_ts, t)
                max_ts = t if max_ts is None else max(max_ts, t)
        if originals_seen != 54000:
            raise ValueError('Not all 54,000 original observations were retained')
        if len(keyset) != info['cells'] or len(keyset) != 54000 * factor:
            raise ValueError(f'Wrong count at {factor}x')
        if len(counts) != 18 or len(set(counts.values())) != 1:
            raise ValueError('Device/sensor density is not balanced')
        if min_ts != min_orig or max_ts != max_orig:
            raise ValueError('Temporal span changed')
        if factor == 1 and digest(path) != digest(source):
            raise ValueError('1x source is not byte-identical')
        print(f'PASS {factor}x: cells={len(keyset)}, per-device/sensor={next(iter(counts.values()))}, span=[{min_ts},{max_ts}]', flush=True)
    print('PASS ALL: data preparation validated; Cassandra untouched')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'validate'))
    parser.add_argument('--project', type=Path, default=Path.cwd())
    args = parser.parse_args()
    project = args.project.resolve()
    if args.action == 'prepare':
        prepare(project)
    else:
        validate(project)


if __name__ == '__main__':
    main()
