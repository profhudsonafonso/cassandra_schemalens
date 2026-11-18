"""Passo 2 - medicao TPCx-IoT sob o protocolo comum v1.

Por repeticao r=1..5: reinicio a frio; ordem das 12 variantes sorteada (pares de repeticoes usam a
mesma permutacao, invertida na repeticao par = contrabalanco). Para cada variante, em keyspace NOVO:
  carga semente (130 passos) -> flush + gate (SSTable>=1, memtable vazia) -> verificacao da semente
  -> 10 leituras de aquecimento (oraculo) -> fase dinamica sob guarda de isolamento: writer insere
  500 passos e 2 threads fazem 120 leituras de duas janelas em checkpoints selados (cada resposta
  comparada com o oraculo independente FORA do cronometro) -> contagem + auditoria integral de todos
  os registros -> flush + nodetool tablestats.
A logica da fase dinamica replica measure_dynamic() da Etapa 9.1, guardando cada latencia bruta.
Nunca usa DROP; uma tentativa invalida vai para um keyspace novo (sufixo _aN).
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import random
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common_paths import (AA_SUFFIX, BASELINE_ID, CONTAINER, PORT, PREREG, ROOT, SNAP, WORKLOAD, code_hashes, csv_x,
                          dump, dump_x, frozen_hashes, read)
import cassandra_env as E

RAW_COLS = ['rep', 'order_position', 'variant', 'attempt', 'query_index', 'latency_ms', 'cql_requests', 'result_rows',
            'watermark_step', 'completed_during_ingestion', 'oracle_ok']


class Status:
    def __init__(self, out):
        self.path = Path(out) / 'status.json'; self.d = {}

    def __call__(self, **kw):
        self.d.update(kw); self.d['updated_utc'] = dt.datetime.now(dt.timezone.utc).isoformat()
        dump(self.path, self.d); print(json.dumps(kw, default=str), flush=True)


def designs():
    from methodology.tpcx_iot.stage9.experiment import load_frozen
    from methodology.tpcx_iot.stage91.experiment import baseline_deployment
    hb, rindex, windex, _ = load_frozen(SNAP)
    base, rd, wr, ddl = baseline_deployment(SNAP)
    complete = {h['deployment_id']: h for h in read(SNAP / 'outputs/tpcx_iot_stage8/52_complete_compatible_deployments.json')}
    complete[BASELINE_ID] = base
    return complete, {**rindex, BASELINE_ID: rd}, {**windex, BASELINE_ID: wr}, ddl


def ks_name(tag, rep, label, attempt):
    short = label.replace('b_tpcx_logical_raw', 'base').replace('h_', 'h')
    name = f'v1tpcx_{tag}_r{rep}_{short}' + (f'_a{attempt}' if attempt > 1 else '')
    if not re.fullmatch(r'v1tpcx_[0-9a-f]{6}_r\d_[a-z0-9_]{1,30}', name) or len(name) > 48:
        raise ValueError('unsafe keyspace ' + name)
    return name


def create_keyspace(client, ks, h, ddl):
    if list(client.session.execute('SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=%s', (ks,))):
        raise FileExistsError('keyspace exists ' + ks)
    client.session.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}}")
    client.session.set_keyspace(ks)
    if h['deployment_id'] == BASELINE_ID:
        client.session.execute(ddl)
    else:
        for t in h['tables']:
            schema = (SNAP / f'outputs/tpcx_iot_stage4/cql/schemas/{t}.cql').read_text()
            cql = '\n'.join(l for l in schema.splitlines() if not l.lstrip().startswith('--')).strip()
            if cql.count(';') != 1 or 'ALLOW FILTERING' in cql.upper():
                raise ValueError('untrusted DDL')
            client.session.execute(cql)
    client.prepared = {}


def timed_read(client, plan, probe, W):
    from methodology.tpcx_iot.stage91.experiment import expected_rows_from_watermark
    stations = [f'ST{x:03}' for x in range(W['stations'])]
    t0 = time.perf_counter_ns()
    observed, _, ncql = client.read_probe(plan, probe, stations)
    el = (time.perf_counter_ns() - t0) / 1e6
    done = time.perf_counter_ns()
    expected = expected_rows_from_watermark(probe, stations=W['stations'], tick_ms=W['tick_ms'], pad_bytes=W['pad_bytes'])
    ok = observed == expected
    if not ok:
        raise AssertionError(f'oracle mismatch {probe["sensor_name"]} step {probe["watermark_step"]}')
    return {'query_index': probe['logical_query_index'], 'latency_ms': round(el, 6), 'cql_requests': ncql,
            'result_rows': len(observed), 'watermark_step': probe['watermark_step'], 'completed_ns': done, 'oracle_ok': ok}


def dynamic_phase(client, h, reads, writes, ledger, W):
    """Same logic as stage91.measure_dynamic, retaining every raw read latency."""
    from methodology.tpcx_iot.stage9.experiment import events
    from methodology.tpcx_iot.stage91.experiment import make_probe, persist_step, schedule_by_step
    plan = reads[h['route']['Q_READ_2_WINDOWS']['candidate_id']]
    client.get_prepared(plan['read_cql'])
    for t in h['tables']:
        client.get_prepared(writes[t]['write_cql'])
    jobs = schedule_by_step(seed_steps=W['seed_steps'], bench_steps=W['bench_steps'], reads=W['read_operations'])
    inflight, finished, trace = [], [], []
    physical = source = 0
    max_pending = max(2, W['read_threads'] * 3)
    with ThreadPoolExecutor(max_workers=W['read_threads']) as pool:
        t0 = time.perf_counter_ns()
        for i in range(W['bench_steps']):
            evs = events(stations=W['stations'], sensors=W['sensors'], steps=1, first_step=W['seed_steps'] + i,
                         tick_ms=W['tick_ms'], pad_bytes=W['pad_bytes'])
            physical += persist_step(client, h, evs, writes, ledger); source += len(evs)
            step = W['seed_steps'] + i
            for j in jobs.get(i, []):
                probe = make_probe(step, j, sensors=W['sensors'], tick_ms=W['tick_ms'])
                trace.append([j, step, probe['sensor_name'], probe['recent'].start_ms, probe['historical'].start_ms])
                inflight.append(pool.submit(timed_read, client, plan, probe, W))
                if len(inflight) >= max_pending:
                    finished.append(inflight.pop(0).result())
        t1 = time.perf_counter_ns()
        for f in inflight:
            finished.append(f.result())
    if len(finished) != W['read_operations'] or source != W['stations'] * W['sensors'] * W['bench_steps']:
        raise AssertionError('incomplete controlled workload')
    if ledger.unapplied_count(h['tables']):
        raise AssertionError('unacknowledged projection writes')
    for r in finished:
        r['completed_during_ingestion'] = r.pop('completed_ns') <= t1
    sec = (t1 - t0) / 1e9
    return finished, {'ingest_seconds': round(sec, 6), 'events_per_second': round(source / sec, 4),
                      'source_events': source, 'physical_inserts': physical,
                      'schedule_sha256': hashlib.sha256(json.dumps(trace, sort_keys=True).encode()).hexdigest(),
                      'overlap_reads': sum(r['completed_during_ingestion'] for r in finished)}


def run_variant(v, rep, pos, attempt, tag, out, comp, reads, writes, ddl, W, port, container, status):
    from methodology.tpcx_iot.stage9.experiment import CassandraClient, DurableOutbox, events, persist_all, verify
    from methodology.tpcx_iot.stage91.experiment import (audit_materialized_rows, count_all, expected_rows_from_watermark,
                                                         make_probe)
    h = comp[v['deployment_id']]
    ks = ks_name(tag, rep, v['label'], attempt)
    client = CassandraClient(['127.0.0.1'], port)
    ledger_path = out / 'outboxes' / f'r{rep}_{v["label"]}_a{attempt}.sqlite'
    ledger = DurableOutbox(ledger_path)
    try:
        create_keyspace(client, ks, h, ddl)
        seed = events(stations=W['stations'], sensors=W['sensors'], steps=W['seed_steps'], tick_ms=W['tick_ms'], pad_bytes=W['pad_bytes'])
        t0 = time.perf_counter_ns()
        persist_all(client, h, seed, writes, ledger)
        seed_s = (time.perf_counter_ns() - t0) / 1e9
        if h['deployment_id'] != BASELINE_ID:
            verify(client, h, reads, seed, W['sensors'], W['stations'], strict_count=True)
        E.flush(container, [ks])
        gate = E.flush_gate(container, [ks])
        if not gate['passed']:
            raise AssertionError('flush gate failed ' + json.dumps(gate['violations'])[:300])
        plan = reads[h['route']['Q_READ_2_WINDOWS']['candidate_id']]
        stations = [f'ST{x:03}' for x in range(W['stations'])]
        for j in range(W['warmup_reads']):
            p = make_probe(W['seed_steps'] - 1, 10000 + j, sensors=W['sensors'], tick_ms=W['tick_ms'])
            got, _, _ = client.read_probe(plan, p, stations)
            if got != expected_rows_from_watermark(p, stations=W['stations'], tick_ms=W['tick_ms'], pad_bytes=W['pad_bytes']):
                raise AssertionError('warmup oracle mismatch')
        with E.IsolationGuard(port, [ks], 15.0) as guard:
            raw, dyn = dynamic_phase(client, h, reads, writes, ledger, W)
        g = guard.report()
        counts = count_all(client, h, W['stations'] * W['sensors'] * (W['seed_steps'] + W['bench_steps']))
        audit = audit_materialized_rows(client, h, stations=W['stations'], sensors=W['sensors'],
                                        total_steps=W['seed_steps'] + W['bench_steps'], tick_ms=W['tick_ms'],
                                        pad_bytes=W['pad_bytes'], max_events=50000)
        E.flush(container, [ks])
        st = E.tablestats(container, [ks])
        storage = sum(x['space_used_live_bytes'] for x in st.values())
    finally:
        ledger.close(); client.close()
    for r in raw:
        r.update(rep=rep, order_position=pos, variant=v['label'], attempt=attempt)
    run = {'rep': rep, 'order_position': pos, 'variant': v['label'], 'deployment_id': v['deployment_id'], 'attempt': attempt,
           'keyspace': ks, 'seed_load_seconds': round(seed_s, 4), 'flush_gate': gate['passed'], 'isolation_guard': g,
           **dyn, 'table_counts': counts, 'audit_status': audit['status'], 'storage_live_bytes': storage,
           'storage_tables': {f'{k[1]}': x['space_used_live_bytes'] for k, x in st.items()}}
    status(phase='variant_done', rep=rep, pos=pos, variant=v['label'], ev_s=dyn['events_per_second'],
           mean_read=round(sum(r['latency_ms'] for r in raw) / len(raw), 3), storage=storage, guard=g['passed'])
    return raw, run


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
    W = dict(WORKLOAD)
    if a.smoke:
        from freeze import check_snapshot, variants as _v
        reg = {'variants': _v(check_snapshot()[2])[0], 'run_tag': hashlib.sha256(str(time.time()).encode()).hexdigest()[:6],
               'measurement_seed_base': 1}
        container, port, reps = CONTAINER + '_smoke', PORT + 100, 1
        W.update(bench_steps=40, read_operations=12, warmup_reads=2)
    else:
        reg = read(PREREG)
        if reg['code_sha256'] != code_hashes():
            raise ValueError('Code changed after pre-registration')
        if reg['design_phase']['frozen_sha256'] != frozen_hashes():
            raise ValueError('Frozen design changed after pre-registration')
        container, port, reps = reg['container'], reg['port'], proto['repetitions']['repetitions']
    comp, reads, writes, ddl = designs()
    variants = reg['variants']; labels = [v['label'] for v in variants]
    manifest = {'smoke': a.smoke, 'protocol': proto['protocol_id'], 'container': container, 'port': port,
                'workload': W, 'reps_planned': reps, 'runs': [], 'orders': {}}
    raw_path = out / 'raw_reads.csv'
    first = True
    try:
        E.start_container(container, port, proto)
        manifest['preflight'] = E.preflight(container, port)
        if manifest['preflight']['cassandra_release_version'] != proto['environment']['cassandra_release_version']:
            raise ValueError('Cassandra version differs from protocol')
        status(phase='preflight_ok', version=manifest['preflight']['cassandra_release_version'])
        perms = {}
        for rep in range(1, reps + 1):
            pair = (rep + 1) // 2
            if pair not in perms:
                p = list(labels); random.Random(reg['measurement_seed_base'] + pair).shuffle(p); perms[pair] = p
            order = perms[pair] if rep % 2 == 1 else list(reversed(perms[pair]))
            manifest['orders'][rep] = order
            rest = E.restart(container, port)
            status(phase='rep_start', rep=rep, restart_s=round(rest, 1), order=order)
            for pos, lab in enumerate(order, 1):
                v = next(x for x in variants if x['label'] == lab)
                for attempt in (1, 2, 3):
                    raw, run = run_variant(v, rep, pos, attempt, reg['run_tag'], out, comp, reads, writes, ddl, W, port, container, status)
                    manifest['runs'].append(run)
                    if run['isolation_guard']['passed']:
                        break
                    (out / 'discarded').mkdir(exist_ok=True)
                    csv_x(out / 'discarded' / f'r{rep}_{lab}_a{attempt}.csv', raw, RAW_COLS)
                    status(phase='attempt_discarded', rep=rep, variant=lab, attempt=attempt)
                else:
                    raise RuntimeError(f'isolation guard failed 3 times for {lab} rep {rep}')
                if first:
                    csv_x(raw_path, raw, RAW_COLS)
                else:
                    _append(raw_path, raw)
                first = False
                dump(out / 'manifest_partial.json', manifest)
        hashes = {r['schedule_sha256'] for r in manifest['runs']}
        if len(hashes) != 1:
            raise AssertionError('query schedule differs between runs')
        if not a.smoke and (read(PREREG)['code_sha256'] != code_hashes() or reg['design_phase']['frozen_sha256'] != frozen_hashes()):
            raise ValueError('Frozen inputs changed during measurement')
        manifest['status'] = 'COMPLETE'; manifest['finished_utc'] = dt.datetime.now(dt.timezone.utc).isoformat()
        manifest['postflight'] = E.preflight(container, port)
        dump_x(out / 'manifest.json', manifest)
        status(phase='complete')
    except BaseException as exc:
        manifest['status'] = 'FAILED'; manifest['error'] = repr(exc); manifest['traceback'] = traceback.format_exc()
        dump(out / 'failure_manifest.json', manifest); status(phase='failed', error=repr(exc))
        raise


def _append(path, rows):
    import csv
    with Path(path).open('a', newline='', encoding='utf8') as f:
        csv.DictWriter(f, fieldnames=RAW_COLS, extrasaction='ignore').writerows(rows)


if __name__ == '__main__':
    main()
