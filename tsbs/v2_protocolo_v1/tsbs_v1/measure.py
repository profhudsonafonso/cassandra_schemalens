"""Passo 2 - medicao TSBS sob o protocolo comum v1 (carga somente leitura, como o IoT).

Container dedicado -> por variante, keyspace NOVO (CREATE only) com as tabelas do deployment (ou a
tabela series_bigint do baseline TSBS) -> carga sequencial cronometrada -> flush + espera de
compactacoes + tablestats. Depois 5 repeticoes: reinicio a frio, gate de flush, pre-checagem de
TODAS as rotas contra o ORACULO INDEPENDENTE (tsbs_v1/oracle.py), aquecimento, janela continua
sob guarda de isolamento: em cada rodada, ordem sorteada das classes de consulta (template x coorte)
e permutacao sorteada das variantes, invertida nas rodadas pares. Cada resposta cronometrada e
comparada com o oraculo fora do cronometro. Instancias: geradas antes de medir (pre-registro).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import re
import time
import traceback
from datetime import date
from pathlib import Path

from common_paths import (BASELINE_ID, CONTAINER, PORT, PREREG, ROOT, SNAP, code_hashes, csv_x, dump, dump_x,
                          frozen_hashes, read, sha)
import cassandra_env as E
from oracle import Oracle

RAW_COLS = ['rep', 'attempt', 'round', 'query_id', 'variant', 'position', 'instance_id', 'latency_ms', 'cql_requests',
            'route', 'oracle_ok']


class Status:
    def __init__(self, out):
        self.path = Path(out) / 'status.json'; self.d = {}

    def __call__(self, **kw):
        self.d.update(kw); self.d['updated_utc'] = dt.datetime.now(dt.timezone.utc).isoformat()
        dump(self.path, self.d); print(json.dumps(kw, default=str), flush=True)


class Session:
    """Thin read/write helper on an explicit keyspace; never drops anything."""

    def __init__(self, cluster, ks):
        self.session = cluster.connect(ks); self.keyspace = ks; self.prepared = {}

    def exec(self, cql, params):
        if cql not in self.prepared:
            self.prepared[cql] = self.session.prepare(cql)
        vals = [date.fromisoformat(v) if isinstance(v, str) and len(v) == 10 and v[4:5] == '-' and v[7:8] == '-' else v for v in params]
        return self.session.execute(self.prepared[cql], tuple(vals))

    def fetch(self, req):
        return [r._asdict() for r in self.exec(req['cql'], req['parameters'])]


def baseline_fetch(sess, req, series_ids, start_ns, end_ns):
    """TS0 logical request translated to the original TSBS series_bigint/day partitions (same logic as Stage 9 fetch_baseline)."""
    from methodology.tsbs.stage5.planner import NS_DAY
    from datetime import timedelta
    host, metric = req['hostname'], req['metric_name']; values = req['parameters']
    if 'LIMIT 1' in req['cql']:
        lo, hi = start_ns, end_ns
    elif len(values) == 3:
        lo, hi = start_ns, values[2]
    else:
        lo, hi = values[-2:]
    out = []
    for idx in range(lo // NS_DAY, (hi - 1) // NS_DAY + 1):
        day = (date(1970, 1, 1) + timedelta(days=idx)).isoformat()
        s = series_ids.get((host, metric, day))
        if s:
            out.extend(r._asdict() for r in sess.exec('SELECT timestamp_ns,value FROM baseline_series_bigint WHERE series_id=? AND timestamp_ns >=? AND timestamp_ns <?', [s, lo, hi]))
    return out


def run_query(sess, dep, inst, plans, ds, interval_s, baseline):
    """Execute one logical instance on one variant; returns (result, n_requests, route_label)."""
    from methodology.tsbs.stage5.runtime import bind_read_requests, execute_mock_rows
    from methodology.tsbs.stage9.instances import route_instance
    if baseline:
        plan = plans[f"{inst['query_id']}__ts0_metric_raw"]; label = 'BASELINE_TS0_PLAN'
        end = ds['last_ns'] + interval_s * 1_000_000_000
        fetcher = lambda r: baseline_fetch(sess, r, ds['series_ids'], ds['start_ns'], end)
    else:
        plan, decision = route_instance(dep, inst, plans); label = plan['plan_id'] + ('' if decision == 'selected' else ' [FALLBACK_TS0]')
        fetcher = sess.fetch
    reqs = bind_read_requests(plan, **inst['params'])
    return execute_mock_rows(plan, [(r, fetcher(r)) for r in reqs]), len(reqs), label


def compare(expected, got):
    from methodology.tsbs.stage9.benchmark import compare_values
    try:
        compare_values(expected, got, rel_tol=1e-9, abs_tol=1e-8)
        return True
    except AssertionError:
        return False


def ks_name(tag, label):
    name = f'v1tsbs_{tag}_' + label.lower().replace('b_tsbs_series_bigint', 'baseline')
    if not re.fullmatch(r'v1tsbs_[0-9a-f]{6}_(h_[0-9a-f]{12}|baseline)(_aa)?', name):
        raise ValueError('unsafe keyspace ' + name)
    return name


def load_variant(cluster, ks, v, deps, writes, ds, status):
    from methodology.tsbs.stage6.materializer import ReferenceMaterializer
    from methodology.tsbs.stage6.cassandra_executor import ExperimentalCassandraExecutor
    from methodology.tsbs.workload import METRICS
    from methodology.tsbs.stage5.planner import NS_DAY
    from datetime import timedelta
    s = cluster.connect()
    if list(s.execute('SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=%s', (ks,))):
        raise FileExistsError('keyspace exists ' + ks)
    s.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}}")
    s.set_keyspace(ks)
    t0 = time.perf_counter_ns()
    if v['kind'] == 'baseline' or v.get('replica_of') == BASELINE_ID:
        s.execute('CREATE TABLE baseline_series_bigint (series_id text, timestamp_ns bigint, value bigint, PRIMARY KEY (series_id, timestamp_ns))')
        ps = s.prepare('INSERT INTO baseline_series_bigint (series_id,timestamp_ns,value) VALUES (?,?,?)'); n = 0
        for e in ds['events']:
            t = e['timestamp_ns']; day = (date(1970, 1, 1) + timedelta(days=t // NS_DAY)).isoformat()
            for m in METRICS:
                s.execute(ps, (ds['series_ids'][(e['hostname'], m, day)], t, int(e[m]))); n += 1
    else:
        dep = deps[v['deployment_id']]
        for cid in sorted(dep['candidate_ids']):
            src = SNAP / f'outputs/tsbs_stage4/cql/schemas/{cid}.cql'
            ddl = '\n'.join(l for l in src.read_text().splitlines() if not l.lstrip().startswith('--')).strip()
            if ddl.count(';') != 1:
                raise ValueError('bad DDL ' + cid)
            s.execute(ddl)
        m = ReferenceMaterializer(list(writes.values()), list(METRICS), candidate_ids=dep['candidate_ids'])
        muts = m.apply(ds['events'])
        n = ExperimentalCassandraExecutor(s, list(writes.values()), expected_keyspace=ks).execute_mutations(muts, allow_experimental_writes=True)
    sec = (time.perf_counter_ns() - t0) / 1e9
    s.shutdown()
    status(phase='load', variant=v['label'], seconds=round(sec, 1), writes=n)
    return {'variant': v['label'], 'keyspace': ks, 'physical_writes': n, 'load_seconds': round(sec, 4),
            'writes_per_second': round(n / sec, 1)}


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
    reg = read(PREREG) if PREREG.exists() else None
    if a.smoke:
        import freeze
        reg = freeze.build_registration(smoke=True)
        container, port = CONTAINER + '_smoke', PORT + 100
        reps, warm, rounds = 1, 1, 2
    else:
        if reg['code_sha256'] != code_hashes():
            raise ValueError('Code changed after pre-registration')
        if reg['design_phase']['frozen_sha256'] != frozen_hashes():
            raise ValueError('Frozen design changed after pre-registration')
        if sha(ROOT / reg['data']['path']) != reg['data']['sha256'] or sha(ROOT / reg['instances']['path']) != reg['instances']['sha256']:
            raise ValueError('Data or query instances changed after pre-registration')
        container, port = reg['container'], reg['port']
        reps = proto['repetitions']['repetitions']; warm = reg['warmup']; rounds = reg['rounds']
    import freeze
    deps, plans, writes = freeze.design_objects()
    ds = freeze.dataset()
    instances = read(ROOT / reg['instances']['path'])
    classes = sorted({(i['cohort'], i['query_id']) for i in instances})
    by_class = {c: [i for i in instances if (i['cohort'], i['query_id']) == c] for c in classes}
    variants = reg['variants']; labels = [v['label'] for v in variants]
    oracle = Oracle(ds['events'])
    expected = {i['instance_key']: oracle.answer(i['query_id'], i['params']) for i in instances}
    tag = reg['run_tag']
    manifest = {'smoke': a.smoke, 'protocol': proto['protocol_id'], 'container': container, 'port': port,
                'reps_planned': reps, 'rounds': rounds, 'warmup': warm, 'classes': [f'{c}:{q}' for c, q in classes],
                'repetitions': []}
    status(phase='start', variants=len(labels), classes=len(classes), rounds=rounds, reps=reps)
    try:
        E.start_container(container, port, proto)
        manifest['preflight'] = E.preflight(container, port)
        if manifest['preflight']['cassandra_release_version'] != proto['environment']['cassandra_release_version']:
            raise ValueError('Cassandra version differs from protocol')
        from cassandra.cluster import Cluster
        cluster = Cluster(['127.0.0.1'], port=port)
        load = [load_variant(cluster, ks_name(tag, v['label']), v, deps, writes, ds, status) for v in variants]
        cluster.shutdown()
        csv_x(out / 'load.csv', load)
        names = [ks_name(tag, l) for l in labels]
        E.flush(container, names); E.wait_compactions(container)
        st = E.tablestats(container, names)
        csv_x(out / 'storage_tables.csv', [{'keyspace': k[0], 'table': k[1], **x} for k, x in sorted(st.items())])
        status(phase='flushed', tables=len(st))
        raw_path = out / 'raw_latencies.csv'
        for rep in range(1, reps + 1):
            for attempt in (1, 2, 3):
                rest = E.restart(container, port)
                gate = E.flush_gate(container, names)
                if not gate['passed']:
                    raise AssertionError('flush gate failed ' + json.dumps(gate['violations'])[:300])
                cluster = Cluster(['127.0.0.1'], port=port)
                sess = {v['label']: Session(cluster, ks_name(tag, v['label'])) for v in variants}
                vmap = {v['label']: v for v in variants}
                isbase = {l: (vmap[l]['kind'] == 'baseline' or vmap[l].get('replica_of') == BASELINE_ID) for l in labels}
                dep_of = {l: deps.get(vmap[l]['deployment_id']) for l in labels}
                fallbacks = {}
                for inst in instances:                      # oracle pre-check of every route
                    for l in labels:
                        got, n, route = run_query(sess[l], dep_of[l], inst, plans, ds, reg['interval_s'], isbase[l])
                        if not compare(expected[inst['instance_key']], got):
                            raise AssertionError(f'precheck oracle mismatch {l} {inst["instance_key"]}')
                        fallbacks[(l, inst['instance_key'])] = route
                rng = random.Random(reg['measurement_seed_base'] + 1000 * rep + attempt)
                status(phase='window_start', rep=rep, attempt=attempt, restart_s=round(rest, 1))
                buf = []
                t0 = time.time()
                with E.IsolationGuard(port, names, 15.0) as guard:
                    for w in range(warm):
                        for c in classes:
                            inst = by_class[c][w % len(by_class[c])]
                            for l in labels:
                                run_query(sess[l], dep_of[l], inst, plans, ds, reg['interval_s'], isbase[l])
                    for rnd in range(1, rounds + 1):
                        corder = list(classes); rng.shuffle(corder)
                        for c in corder:
                            inst = by_class[c][(rnd - 1) % len(by_class[c])]
                            order = list(labels); rng.shuffle(order)
                            if rnd % 2 == 0:
                                order.reverse()
                            for pos, l in enumerate(order, 1):
                                ts = time.perf_counter_ns()
                                got, n, route = run_query(sess[l], dep_of[l], inst, plans, ds, reg['interval_s'], isbase[l])
                                el = (time.perf_counter_ns() - ts) / 1e6
                                if not compare(expected[inst['instance_key']], got):
                                    raise AssertionError(f'timed oracle mismatch {l} {inst["instance_key"]} rep {rep}')
                                buf.append({'rep': rep, 'attempt': attempt, 'round': rnd, 'query_id': f'{c[0][0].upper()}{c[1]}',
                                            'variant': l, 'position': pos, 'instance_id': inst['instance_key'],
                                            'latency_ms': round(el, 6), 'cql_requests': n, 'route': route, 'oracle_ok': True})
                        if rnd % 10 == 0:
                            status(phase='timed_window', rep=rep, round=rnd, of=rounds, elapsed_s=round(time.time() - t0, 1))
                cluster.shutdown()
                g = guard.report()
                manifest['repetitions'].append({'rep': rep, 'attempt': attempt, 'restart_seconds': rest,
                                                'flush_gate': {'passed': gate['passed'], 'tables': gate['tables']},
                                                'isolation_guard': g, 'raw_rows': len(buf), 'window_seconds': round(time.time() - t0, 1),
                                                'fallback_routes': sorted({f'{k[0]}|{k[1]}|{r}' for k, r in fallbacks.items() if 'FALLBACK' in r})})
                if g['passed']:
                    csv_x(raw_path, buf, RAW_COLS, mode='a' if raw_path.exists() else 'x')
                    break
                (out / 'discarded').mkdir(exist_ok=True)
                csv_x(out / 'discarded' / f'rep{rep}_attempt{attempt}.csv', buf, RAW_COLS)
                status(phase='window_discarded', rep=rep, attempt=attempt)
            else:
                raise RuntimeError(f'rep {rep}: isolation guard failed 3 times')
            status(phase='rep_done', rep=rep, of=reps)
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


if __name__ == '__main__':
    main()
