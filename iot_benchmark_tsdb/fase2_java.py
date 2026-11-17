#!/usr/bin/env python3
"""SchemaLens Rev3 Fase 2 - offline generation of NEW official Java IoT Benchmark data.

Reuses the pinned upstream checkout (commit 62e0dc10...) already compiled for campaign B
and the project's own V3 driver helpers (build_config, support files, JVM command line).
Never contacts Cassandra. Every output directory must not exist (no overwrite).

    python fase2_java.py generate --id h3 --devices 9 --loops 9600 --data-seed 531301 --query-seed 531311
    python fase2_java.py reproduce-b      # control: must reproduce campaign B bytes (sha 047c269a...)
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import datetime as dt
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
F2 = PROJECT / 'experiments' / 'rev3_fase2'
DATA = F2 / 'data'
B_JAVA = (PROJECT / '..' / 'schemalens_rev3_new_java_B_20261105_193618' / 'java_work').resolve()
REPO = B_JAVA / 'upstream_iot_benchmark'
CPFILE = B_JAVA / 'build_cache' / 'core_classpath.txt'
PINNED_SHA = '62e0dc100e56443978c8b4c66472b1d8a7ef0238'
B_DATA_SHA = '047c269a42beea21333d7dbecdbde798862346015a35a5b1ced232b05a8f2c2a'
FIXED = dict(batch_size=5, point_step_ms=600000, query_interval_ms=604800000, group_by_ms=3600000)
HISTORICAL_SEEDS = {666, 19243, 38421, 531007, 531013, 531101, 531107,
                    541101, 541103, 541201, 541203, 541301, 541303, 541401, 541403, 541501, 541503}

sys.path.insert(0, str(PROJECT / 'scripts'))
sys.path.insert(0, str(PROJECT / 'phase_a1'))
import auto_official_v3 as V3          # noqa: E402  (helpers only; main() is never called)
from core.bridge import read_native    # noqa: E402


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for c in iter(lambda: f.read(1 << 20), b''):
            h.update(c)
    return h.hexdigest()


def edit_properties(path, values):
    lines, seen, out = path.read_text(encoding='utf8').splitlines(), set(), []
    for line in lines:
        k = line.partition('=')[0].strip()
        if k in values:
            if k in seen:
                raise ValueError('duplicate key ' + k)
            seen.add(k)
            out.append(f'{k}={values[k]}')
        else:
            out.append(line)
    out += [f'{k}={values[k]}' for k in sorted(values.keys() - seen)]
    path.write_text('\n'.join(out) + '\n', encoding='utf8')


def generate(gid, devices, loops, data_seed, query_seed, control=False, point_step_ms=None):
    step = point_step_ms or FIXED['point_step_ms']
    head = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=REPO, capture_output=True, text=True).stdout.strip()
    if head != PINNED_SHA:
        raise ValueError(f'upstream checkout at {head}, expected {PINNED_SHA}')
    if not control and ({data_seed, query_seed} & HISTORICAL_SEEDS or data_seed == query_seed):
        raise ValueError('new campaigns need fresh, distinct seeds')
    work = DATA / gid
    work.mkdir(parents=True, exist_ok=False)
    profile = work / 'profile.properties'
    shutil.copy2(V3.PROFILE, profile)
    settings = dict(DATA_SEED=data_seed, QUERY_SEED=query_seed, POINT_STEP=step,
                    QUERY_INTERVAL=FIXED['query_interval_ms'], GROUP_BY_TIME_UNIT=FIXED['group_by_ms'])
    edit_properties(profile, settings)
    cfg_dir, native = work / 'java_config', work / 'native_data'
    config = V3.build_config(profile, cfg_dir / 'config.properties', native,
                             devices=devices, loops=loops, batch_size=FIXED['batch_size'])
    V3.copy_upstream_support_files(REPO / 'configuration/conf', cfg_dir)
    classpath = os.pathsep.join([str(REPO / 'core/target/classes'), CPFILE.read_text().strip()])
    log = work / 'native_generation.log'
    with log.open('w') as lf:
        subprocess.run(V3.native_java_command(classpath, cfg_dir), cwd=REPO, stdout=lf, stderr=subprocess.STDOUT,
                       check=True, timeout=3600)
    if not (native / 'schema.txt').is_file():
        raise RuntimeError('Java generator produced no schema.txt; see ' + str(log))
    cells = devices * 2 * loops * FIXED['batch_size']
    data = read_native(native, max_rows=cells)
    if len(data['records']) != cells:
        raise ValueError(f"{len(data['records'])} records, expected {cells}")
    out_csv = work / f'{gid}.csv'
    with out_csv.open('x', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['device_id', 'sensor_name', 'timestamp', 'value_double'])
        w.writeheader()
        w.writerows(data['records'])
    ts = [r['timestamp'] for r in data['records']]
    manifest = dict(
        status='FASE2_JAVA_DATA_GENERATED_NO_CASSANDRA', id=gid, created_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
        java_upstream_commit=PINNED_SHA, generator='official IoT Benchmark generateDataMode (core, pinned)',
        devices=devices, loops=loops, sensors=2, cells=cells, wide_rows=cells // 2, settings=settings,
        batch_size=FIXED['batch_size'], point_step_ms=step, first_ms=min(ts), last_ms=max(ts),
        history_days=round((max(ts) - min(ts)) / 86400000, 4),
        csv=out_csv.name, csv_sha256=sha(out_csv), config_sha256=sha(config), control_reproduction=control,
        native_files=len(list(native.glob('*/BigBatch_*.csv'))))
    with (work / 'manifest.json').open('x') as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    print(json.dumps({k: manifest[k] for k in ('id', 'cells', 'history_days', 'csv_sha256')}))
    return manifest


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    g = sub.add_parser('generate')
    g.add_argument('--id', required=True)
    g.add_argument('--devices', type=int, required=True)
    g.add_argument('--loops', type=int, required=True)
    g.add_argument('--data-seed', type=int, required=True)
    g.add_argument('--query-seed', type=int, required=True)
    g.add_argument('--point-step-ms', type=int, default=None)
    sub.add_parser('reproduce-b')
    a = ap.parse_args()
    if a.cmd == 'reproduce-b':
        m = generate('control_b', 9, 600, 531101, 531107, control=True)
        ok = m['csv_sha256'] == B_DATA_SHA
        print('CONTROL', 'PASS: byte-identical to campaign B' if ok else 'FAIL: differs from campaign B')
        sys.exit(0 if ok else 1)
    generate(a.id, a.devices, a.loops, a.data_seed, a.query_seed, point_step_ms=a.point_step_ms)


if __name__ == '__main__':
    main()
