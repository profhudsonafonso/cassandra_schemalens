"""Passo 2 - medicao sob o protocolo comum v1 (IoT-Benchmark).

Sequencia:
  preflight -> container dedicado (imagem fixada por digest) -> keyspaces novos (CREATE only)
  -> carga sequencial cronometrada por deployment -> flush + gate + armazenamento
  -> para cada repeticao r=1..5: reinicio a frio, gate de flush, pre-checagem do oraculo,
     aquecimento, janela continua intercalada/contrabalancada sob guarda de isolamento.
Toda resposta cronometrada e comparada com o oraculo FORA do cronometro.
A selecao congelada e os hashes sao verificados antes e depois.

Uso:
  python iot_v1/measure.py --out outputs/v1_run1            (execucao oficial; exige pre-registro)
  python iot_v1/measure.py --smoke --out outputs/smoke_N    (teste rapido, nao conta como resultado)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import re
import sys
import time
import traceback
from pathlib import Path

from common_paths import (CONTAINER, LABEL_OPT, LABEL_ORIG, PORT, PREREG, REV3, ROOT, SNAP, code_hashes, csv_x,
                          dump, dump_x, frozen_hashes, read)
import cassandra_env as E

RAW_COLS = ['rep', 'attempt', 'round', 'query_id', 'variant', 'position', 'latency_ms', 'execute_calls',
            'iterated_rows', 'fallbacks', 'physical_plan', 'plan_id', 'oracle_ok']


class Status:
    def __init__(self, out):
        self.path = Path(out) / 'status.json'; self.d = {'started_utc': dt.datetime.now(dt.timezone.utc).isoformat()}

    def __call__(self, **kw):
        self.d.update(kw); self.d['updated_utc'] = dt.datetime.now(dt.timezone.utc).isoformat()
        dump(self.path, self.d)
        print(json.dumps(kw, default=str), flush=True)


def load_inputs():
    from iot_pipeline.shared import read_rows, require_policy
    from iot_pipeline.native_reference_v3 import source_oracle
    rows = read_rows(SNAP / 'phase_a1/outputs/normalized_double_readings.csv')
    queries = read(SNAP / 'phase_a1/outputs/query_instances_pilot.json')
    catalog = {p['plan_id']: p for p in read(REV3 / 'stage5/15_executable_plan_catalog.json')}
    covers = {h['deployment_id']: h for h in read(REV3 / 'stage8/29_all_compatible_covers.json')}
    return rows, queries, catalog, covers, source_oracle(), require_policy()


def execute(session, q, label, covers, catalog, sensors, days, oracle, page_size=16):
    if label in (LABEL_ORIG, LABEL_OPT):
        from v4_rev2.runtime_helpers import read_wide
        optim = label == LABEL_OPT and q['query_id'] == 'Q8'
        recs, calls, it = read_wide(session, q, sensors, 'iot_reference_table', days,
                                    optimized_q8=optim, page_size=page_size)
        return oracle(recs, q), {'execute_calls': calls, 'iterated_rows': it, 'fallbacks': 0,
                                 'physical_plan': 'BASELINE_Q8_DESC_SPARSE' if optim else 'BASELINE_FULL_OR_RANGE',
                                 'plan_id': 'REFERENCE'}
    from rev3.runtime import dispatch
    res, tr = dispatch(session, q, covers[label], catalog, sensors, days, page_size, oracle)
    tr['plan_id'] = covers[label]['query_routes'][q['query_id']]['plan_id']
    return res, tr


def keyspace(prefix, v):
    name = prefix + v['keyspace_tail']
    if not re.fullmatch(r'sl_(v1|smoke)_iot_[0-9a-f]{8}_(reference|r3h_[0-9a-f]{12})', name):
        raise ValueError('unsafe keyspace ' + name)
    return name


def materialize(cluster, variants, prefix, rows, sensors, covers, policy, status):
    from iot_pipeline.stage9 import _materialize
    from iot_pipeline.reference_baseline import table_ddl
    from v4_rev2.runtime_helpers import load_wide
    s = cluster.connect()
    names = sorted({keyspace(prefix, v) for v in variants})
    for n in names:
        if list(s.execute('SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=%s', (n,))):
            raise FileExistsError('keyspace exists: ' + n)
    load = []
    done = set()
    for v in variants:
        ks = keyspace(prefix, v)
        if ks in done:
            continue
        s.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}}")
        s.set_keyspace(ks)
        t0 = time.perf_counter_ns()
        if v['kind'] == 'baseline':
            s.execute(table_ddl(sensors))
            writes = load_wide(s, rows, sensors, 'iot_reference_table', False)
            label = 'REFERENCE'
        else:
            per = _materialize(s, covers[v['label']], rows, sensors, policy)
            writes = sum(per.values()); label = v['label']
        sec = (time.perf_counter_ns() - t0) / 1e9
        load.append({'deployment': label, 'keyspace': ks, 'physical_writes': writes, 'load_seconds': round(sec, 4),
                     'writes_per_second': round(writes / sec, 1)})
        done.add(ks)
        status(phase='load', loaded=label, seconds=round(sec, 1), writes=writes)
    s.shutdown()
    return load, names


def run_window(sessions, labels, queries, expected, covers, catalog, sensors, days, oracle, rounds, warmup, rng,
               rep, attempt, raw_path, status):
    """Warmup + continuous interleaved timed window. Returns number of raw rows written."""
    # warmup (interleaved as well)
    for w in range(warmup):
        for q in queries:
            for label in labels:
                got, _ = execute(sessions[label], q, label, covers, catalog, sensors, days, oracle)
                from iot_pipeline.offline import compare
                if not compare(got, expected[q['query_id']]):
                    raise AssertionError(f'warmup oracle mismatch {label} {q["query_id"]}')
    from iot_pipeline.offline import compare
    buf, n = [], 0
    t_window = time.time()
    for rnd in range(1, rounds + 1):
        qorder = list(queries); rng.shuffle(qorder)
        for q in qorder:
            order = list(labels); rng.shuffle(order)
            if rnd % 2 == 0:
                order.reverse()
            for pos, label in enumerate(order, 1):
                sess = sessions[label]
                t0 = time.perf_counter_ns()
                got, tr = execute(sess, q, label, covers, catalog, sensors, days, oracle)
                el = (time.perf_counter_ns() - t0) / 1e6
                ok = compare(got, expected[q['query_id']])
                if not ok:
                    raise AssertionError(f'timed oracle mismatch {label} {q["query_id"]} rep={rep} round={rnd}')
                buf.append({'rep': rep, 'attempt': attempt, 'round': rnd, 'query_id': q['query_id'], 'variant': label,
                            'position': pos, 'latency_ms': round(el, 6), 'execute_calls': tr['execute_calls'],
                            'iterated_rows': tr['iterated_rows'], 'fallbacks': tr['fallbacks'],
                            'physical_plan': tr['physical_plan'], 'plan_id': tr['plan_id'], 'oracle_ok': ok})
        if rnd % 10 == 0:
            status(phase='timed_window', rep=rep, attempt=attempt, round=rnd, of=rounds,
                   elapsed_s=round(time.time() - t_window, 1))
    csv_x(raw_path, buf, RAW_COLS)
    return len(buf)


def _append(path, rows):
    import csv
    with Path(path).open('a', newline='', encoding='utf8') as f:
        w = csv.DictWriter(f, fieldnames=RAW_COLS, extrasaction='ignore'); w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True, type=Path)
    ap.add_argument('--smoke', action='store_true')
    a = ap.parse_args()
    proto = E.protocol()
    os.sched_setaffinity(0, set(proto['environment']['client_cpu_affinity']))
    out = (ROOT / a.out).resolve() if not a.out.is_absolute() else a.out
    out.mkdir(parents=True, exist_ok=False)
    status = Status(out)
    if a.smoke:
        import hashlib
        reg = {'variants': None, 'run_tag': hashlib.sha256(str(time.time()).encode()).hexdigest()[:8],
               'measurement_seed_base': 1, 'aa_pairs': []}
        from freeze import variants as _v
        reg['variants'] = _v()[0]
        prefix = f"sl_smoke_iot_{reg['run_tag']}_"; container = CONTAINER + '_smoke'; port = PORT + 100
        reps, warm, rounds = 2, 1, 3
    else:
        reg = read(PREREG)
        if reg['code_sha256'] != code_hashes():
            raise ValueError('Code changed after pre-registration')
        if reg['design_phase']['frozen_sha256'] != frozen_hashes():
            raise ValueError('Frozen design changed after pre-registration')
        prefix = reg['keyspace_prefix']; container = reg['container']; port = reg['port']
        reps = proto['repetitions']['repetitions']; warm = proto['repetitions']['warmup_per_query_variant_per_repetition']
        rounds = proto['repetitions']['timed_rounds_per_query_variant_per_repetition']
    variants = reg['variants']; labels = [v['label'] for v in variants]
    rows, queries, catalog, covers, oracle, policy = load_inputs()
    from v4_rev2.runtime_helpers import DAY
    sensors = sorted({r['sensor_name'] for r in rows}); days = {r['timestamp'] // DAY for r in rows}
    expected = {q['query_id']: oracle(rows, q) for q in queries}
    status(phase='start', smoke=a.smoke, variants=len(labels), reps=reps, rounds=rounds, warmup=warm)
    manifest = {'smoke': a.smoke, 'protocol': proto['protocol_id'], 'prefix': prefix, 'container': container,
                'port': port, 'reps_planned': reps, 'rounds': rounds, 'warmup': warm, 'repetitions': []}
    try:
        E.start_container(container, port, proto)
        manifest['preflight'] = E.preflight(container, port)
        if manifest['preflight']['cassandra_release_version'] != proto['environment']['cassandra_release_version']:
            raise ValueError('Cassandra version differs from protocol')
        status(phase='preflight_ok', version=manifest['preflight']['cassandra_release_version'])
        from cassandra.cluster import Cluster
        cluster = Cluster(['127.0.0.1'], port=port)
        load, names = materialize(cluster, variants, prefix, rows, sensors, covers, policy, status)
        cluster.shutdown()
        csv_x(out / 'load.csv', load)
        E.flush(container, names); E.wait_compactions(container)
        st = E.tablestats(container, names)
        storage = [{'keyspace': k[0], 'table': k[1], **v} for k, v in sorted(st.items())]
        csv_x(out / 'storage_tables.csv', storage)
        status(phase='flushed', tables=len(storage))
        seed0 = reg['measurement_seed_base']
        raw_path = out / 'raw_latencies.csv'
        for rep in range(1, reps + 1):
            attempt = 0
            while True:
                attempt += 1
                if attempt > 3:
                    raise RuntimeError(f'rep {rep}: isolation guard failed 3 times')
                rest = E.restart(container, port)
                gate = E.flush_gate(container, names)
                if not gate['passed']:
                    raise AssertionError('flush gate failed: ' + json.dumps(gate['violations'])[:500])
                cluster = Cluster(['127.0.0.1'], port=port)
                sessions = {v['label']: cluster.connect(keyspace(prefix, v)) for v in variants}
                # oracle pre-check of every route
                from iot_pipeline.offline import compare
                for q in queries:
                    for lab in labels:
                        got, _ = execute(sessions[lab], q, lab, covers, catalog, sensors, days, oracle)
                        if not compare(got, expected[q['query_id']]):
                            raise AssertionError(f'precheck oracle mismatch {lab} {q["query_id"]}')
                rng = random.Random(seed0 + 1000 * rep + attempt)
                status(phase='window_start', rep=rep, attempt=attempt, restart_s=round(rest, 1))
                tmp_raw = out / f'.rep{rep}_attempt{attempt}.csv'
                t0 = time.time()
                with E.IsolationGuard(port, names, 15.0) as guard:
                    n = run_window(sessions, labels, queries, expected, covers, catalog, sensors, days, oracle,
                                   rounds, warm, rng, rep, attempt, tmp_raw, status)
                cluster.shutdown()
                g = guard.report()
                manifest['repetitions'].append({'rep': rep, 'attempt': attempt, 'restart_seconds': rest,
                                                'flush_gate': {'passed': gate['passed'], 'tables': gate['tables']},
                                                'isolation_guard': g, 'raw_rows': n,
                                                'window_seconds': round(time.time() - t0, 1)})
                if g['passed']:
                    if raw_path.exists():
                        _append(raw_path, read_csv_rows(tmp_raw))
                        tmp_raw.unlink()
                    else:
                        tmp_raw.rename(raw_path)
                    break
                (out / 'discarded').mkdir(exist_ok=True)
                tmp_raw.rename(out / 'discarded' / tmp_raw.name.lstrip('.'))
                status(phase='window_discarded', rep=rep, attempt=attempt, guard=g)
            status(phase='rep_done', rep=rep, of=reps)
        if not a.smoke:
            if read(PREREG)['code_sha256'] != code_hashes() or reg['design_phase']['frozen_sha256'] != frozen_hashes():
                raise ValueError('Frozen inputs changed during measurement')
        manifest['status'] = 'COMPLETE'
        manifest['finished_utc'] = dt.datetime.now(dt.timezone.utc).isoformat()
        manifest['postflight'] = E.preflight(container, port)
        dump_x(out / 'manifest.json', manifest)
        status(phase='complete')
    except BaseException as exc:
        manifest['status'] = 'FAILED'; manifest['error'] = repr(exc); manifest['traceback'] = traceback.format_exc()
        dump(out / 'failure_manifest.json', manifest)
        status(phase='failed', error=repr(exc))
        raise


def read_csv_rows(p):
    from common_paths import read_csv
    return read_csv(p)


if __name__ == '__main__':
    main()
