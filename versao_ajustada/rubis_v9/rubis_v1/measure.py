"""Passo 4 - E20 do RUBiS sob o protocolo comum v1, em DOIS bracos (4.1.12 e 3.11.11) com os mesmos dados.

* Codigo V9 auditado sem alteracao: materialize_* (phase5), adaptadores (phase6.adapter_for / GeneratedAdapter),
  historias semeadas (workload_005.histories) e phase6.agreement() - aqui contra o ORACULO V9 (frozen/ORACLE_REF.pkl).
* Corpo de cada (deployment, carga, repeticao) = schemalens_v9.phase6.run_protocol, linha a linha.
* Protocolo v1: repeticao externa; antes de cada (repeticao, braco): flush + reinicio a frio + gate, com o outro
  braco PARADO; bracos em ABBA (rep impar: v41 -> v311; par: v311 -> v41); deployments na ordem da V9 nas repeticoes
  impares e invertida nas pares; replica A/A H01_AA.
* Nada e escrito em runs/v9_generation_001.

    python rubis_v1/measure.py node --arm v41|v311
    python rubis_v1/measure.py run --smoke --out outputs/smoke_1
    python rubis_v1/measure.py run --out outputs/v1_run1
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import pickle
import time
import traceback
from pathlib import Path

from common_paths import (AA_ID, ARMS, FROZEN, MEASURED, PREREG, REPS, ROOT, WARMUP, WORKLOADS, code_hashes, dump, dump_x,
                          neutralize_v9_state_writes, read, sha, use_target_port, v9_hashes)
import cassandra_env as E
import env as V


class Status:
    def __init__(self, out):
        self.path = Path(out) / 'status.json'; self.d = {}

    def __call__(self, **kw):
        self.d.update(kw); self.d['updated_utc'] = dt.datetime.now(dt.timezone.utc).isoformat()
        dump(self.path, self.d); print(json.dumps(kw, default=str, ensure_ascii=False), flush=True)


def node(arm):
    proto = V.arm_protocol(arm)
    res, problems = V.check_resources(proto)
    if problems:
        raise SystemExit('Recursos do Docker insuficientes: ' + '; '.join(problems))
    a = ARMS[arm]
    E.start_container(a['container'], a['port'], proto)
    pf = V.preflight(arm)
    if pf['cassandra_release_version'] != ARMS[arm]['version']:
        raise SystemExit('versao inesperada ' + pf['cassandra_release_version'])
    dump(ROOT / f'frozen/NODE_PREFLIGHT_{arm}.json', pf)
    print('NO PRONTO', arm, pf['cassandra_release_version'], pf['image'])


def one_block(ad, dep, ks, workload, rep, warmup, measured, dom):
    """Body of schemalens_v9.phase6.run_protocol for one (workload, repetition), unchanged."""
    from reused.methodology.protocol_012 import stats
    from reused.methodology.semantic_replay_011 import canonical, hash_value
    from reused.methodology.workload_005 import histories
    ad.reset_state()
    for req in histories(workload, rep, warmup, dom, "v9", phase="warmup"):
        ctx = {}
        for sid in req["constituent_statement_ids"]:
            ad.execute_statement(sid, req["parameters"], ctx)
    recs = []
    for k, req in enumerate(histories(workload, rep, measured, dom, "v9", phase="measured")):
        ctx, srows, err = {}, [], None
        start = time.perf_counter_ns()
        try:
            for sid in req["constituent_statement_ids"]:
                r = ad.execute_statement(sid, req["parameters"], ctx)
                srows.append({"statement_id": sid, "logical_rows": canonical(r["logical_rows"]), "canonical_hash": hash_value(r["logical_rows"]),
                              "row_count": r["row_count"], "physical_reads": r["physical_reads"], "physical_writes": r["physical_writes"],
                              "support_reads": r["support_reads"], "rows_fetched": r["rows_fetched"]})
        except Exception as exc:
            err = repr(exc)
        lat = (time.perf_counter_ns() - start) / 1e6
        recs.append({"logical_request_id": req["logical_request_id"], "deployment_id": dep, "keyspace": ks, "workload": workload,
                     "repetition": rep, "request_group": req["request_group"], "statements": srows, "latency_ms": lat, "seq": k,
                     "physical_reads": sum(x["physical_reads"] for x in srows), "physical_writes": sum(x["physical_writes"] for x in srows),
                     "support_reads": sum(x["support_reads"] for x in srows), "rows_fetched": sum(x["rows_fetched"] for x in srows),
                     "rows_returned": sum(x["row_count"] for x in srows), "error": err})
    ad.reset_state()
    s = stats(recs)
    s.update({"deployment_id": dep, "workload": workload, "repetition": rep})
    return recs, s


def keyspace_of(dep):
    from schemalens_v9 import phase5_run as PR
    return 'rubis_v9_h01_aa' if dep == AA_ID else (PR.EXTERNAL[dep][0] if dep in PR.EXTERNAL else PR.ks_of(dep))


def build_adapter(dep, s, state, closure, H, C):
    from schemalens_v9 import phase5 as M, phase6
    if dep == AA_ID:
        h = next(x for x in H if x['schema_id'] == 'H01')
        return M.GeneratedAdapter(s, 'rubis_v9_h01_aa', h, {t: C[t] for t in h['canonical_ids']}, state), 'rubis_v9_h01_aa'
    return phase6.adapter_for(dep, s, state, closure)


def materialize(s, deps, raw, state, H, C, status, arm):
    from schemalens_v9 import phase5 as M, phase5_run as PR
    rec = {}
    for dep in deps:
        status(phase='materialize', arm=arm, deployment=dep)
        if dep == AA_ID:
            h = next(x for x in H if x['schema_id'] == 'H01')
            rec[dep] = M.materialize_generated(s, 'rubis_v9_h01_aa', [C[t] for t in h['canonical_ids']], state)
        elif dep == 'V8_AUTO_H_norm':
            if ARMS[arm]['version'].startswith('4'):
                # The frozen V8 DDL was captured from 3.11 metadata and carries dclocal_read_repair_chance = 0.1 and
                # read_repair_chance = 0.0 (the 3.11 defaults), removed in Cassandra 4.0. Same table structure;
                # in-memory adaptation only (reused/v8_auto_h_norm_ddl.json is untouched). RF=1: no read repair anyway.
                orig = M.v8_norm_ddl
                d = orig()
                adapted = dict(d, ddl={t: q.replace(' AND dclocal_read_repair_chance = 0.1', '').replace(' AND read_repair_chance = 0.0', '')
                                       for t, q in d['ddl'].items()})
                M.v8_norm_ddl = lambda *a, **k: adapted
                try:
                    rec[dep] = M.materialize_v8_norm(s, PR.EXTERNAL[dep][0], raw)
                finally:
                    M.v8_norm_ddl = orig
                rec[dep]['ddl_adaptation_41'] = 'removidas dclocal_read_repair_chance=0.1 e read_repair_chance=0.0 (opcoes inexistentes no 4.x)'
            else:
                rec[dep] = M.materialize_v8_norm(s, PR.EXTERNAL[dep][0], raw)
        elif dep in ('NoSE', 'QPG'):
            rec[dep] = M.materialize_baseline(s, PR.EXTERNAL[dep][0], PR.EXTERNAL[dep][1], raw)
        else:
            h = next(x for x in H if x['schema_id'] == dep)
            rec[dep] = M.materialize_generated(s, PR.ks_of(dep), [C[t] for t in h['canonical_ids']], state)
    return rec


def run(a):
    neutralize_v9_state_writes()
    from schemalens_v9 import isolation, phase5 as M, phase5_run as PR, phase6
    from schemalens_v9.phase1 import _benchmark_domain
    from reused.methodology.semantic_replay_011 import source_closure
    proto = E.protocol()
    out = (ROOT / a.out).resolve() if not Path(a.out).is_absolute() else Path(a.out)
    out.mkdir(parents=True, exist_ok=False)
    status = Status(out)
    affinity = V.pin_client(proto)
    import freeze
    order, _ = freeze.base_order()
    arms = ['v41', 'v311']
    if a.smoke:
        reps, warm, meas = 2, 20, 200          # all 9 deployments, tiny counts
    else:
        reg = read(PREREG)
        if not reg:
            raise SystemExit('pre-registro ausente: rode freeze antes')
        if reg['code_sha256'] != code_hashes() or reg['design_phase']['v9_frozen_and_code_sha256'] != v9_hashes():
            raise SystemExit('codigo ou artefatos V9 mudaram depois do pre-registro')
        if sha(FROZEN / 'ORACLE_REF.pkl') != reg['oracle']['sha256']:
            raise SystemExit('referencia do oraculo mudou')
        order, reps, warm, meas = reg['deployments_base_order'], REPS, WARMUP, MEASURED
    manifest = {'smoke': a.smoke, 'protocol': proto['protocol_id'], 'client_affinity': affinity, 'order_base': order, 'arms': arms,
                'reps': reps, 'warmup': warm, 'measured': meas, 'repetitions': [], 'preflight': {}, 'materialization': {},
                'started_utc': dt.datetime.now(dt.timezone.utc).isoformat()}
    names = sorted({keyspace_of(d) for d in order})
    H, C = PR.h_schemas(); dom = _benchmark_domain()
    ctx = {}
    try:
        # ---- materialise the same cohort on both nodes (one node up at a time)
        for i, arm in enumerate(arms):
            other = arms[1 - i]
            V.stop(ARMS[other]['container'])
            if not V.is_running(ARMS[arm]['container']):
                E.sh(['docker', 'start', ARMS[arm]['container']]); E.wait_ready(ARMS[arm]['container'], ARMS[arm]['port'])
            use_target_port(ARMS[arm]['port']); isolation.CONTAINER = ARMS[arm]['container']
            manifest['preflight'][arm] = V.preflight(arm)
            if manifest['preflight'][arm]['cassandra_release_version'] != ARMS[arm]['version']:
                raise SystemExit(f'{arm}: versao inesperada')
            raw, ids, state = PR.cohort()
            cl, s = M.session()
            try:
                closure, _ = source_closure(s)
                manifest['materialization'][arm] = materialize(s, order, raw, state, H, C, status, arm)
            finally:
                cl.shutdown()
            for ks in names:
                E.nodetool(ARMS[arm]['container'], 'flush', ks)
            E.wait_compactions(ARMS[arm]['container'])
            st = E.tablestats(ARMS[arm]['container'], names)
            dump(out / arm / 'storage_tables.json', {f'{k[0]}.{k[1]}': v for k, v in st.items()})
            cl, s = M.session()
            fp0 = {ks: phase6.ks_fingerprint(s, ks) for ks in names}
            cl.shutdown()
            ctx[arm] = {'state': state, 'closure': closure, 'fp0': fp0}
            (out / arm / 'raw').mkdir(parents=True, exist_ok=True)
        if ctx['v41']['fp0'] != ctx['v311']['fp0']:
            raise AssertionError('os keyspaces materializados diferem entre 4.1.12 e 3.11.11')
        summaries = []
        for rep in range(reps):
            seq = order if rep % 2 == 0 else list(reversed(order))
            arm_seq = arms if rep % 2 == 0 else list(reversed(arms))
            for arm in arm_seq:
                other = [x for x in arms if x != arm][0]
                use_target_port(ARMS[arm]['port']); isolation.CONTAINER = ARMS[arm]['container']
                state, closure, fp0 = ctx[arm]['state'], ctx[arm]['closure'], ctx[arm]['fp0']
                for attempt in (1, 2, 3):
                    rest, gate = V.cold_cycle(arm, names, other)
                    if not gate['passed']:
                        raise AssertionError(f'{arm}: gate de flush falhou: ' + json.dumps(gate['violations'])[:400])
                    cl, s = M.session()
                    rec = {'rep': rep + 1, 'arm': arm, 'attempt': attempt, 'order': seq, 'restart_seconds': rest, 'gate': gate, 'deployments': []}
                    t0 = time.time(); invalid = False; files = []
                    try:
                        for pos, dep in enumerate(seq, 1):
                            ad, ks = build_adapter(dep, s, state, closure, H, C)
                            status(phase='measure', rep=rep + 1, of=reps, arm=arm, attempt=attempt, pos=pos, deployment=dep,
                                   elapsed_min=round((time.time() - t0) / 60, 1))
                            with isolation.BulkWriterWatch() as bw, V.HistoryGuard(ARMS[arm]['port'], names) as g:
                                recs_all = []
                                for w in WORKLOADS:
                                    recs, sm = one_block(ad, dep, ks, w, rep, warm, meas, dom)
                                    sm.update({'arm': arm, 'rep_index_v1': rep + 1, 'attempt': attempt, 'position': pos})
                                    summaries.append(sm); recs_all.extend(recs)
                            after = phase6.ks_fingerprint(s, ks)
                            changed = sorted(t for t in fp0[ks] if fp0[ks][t] != after.get(t))
                            iso, gr = bw.report(), g.report()
                            path = out / arm / 'raw' / f'{dep}_rep{rep + 1}_a{attempt}.jsonl.gz'
                            path.write_bytes(gzip.compress(b'\n'.join(json.dumps(r, sort_keys=True, default=str).encode() for r in recs_all) + b'\n', mtime=0))
                            files.append(path)
                            rec['deployments'].append({'deployment': dep, 'position': pos, 'keyspace': ks, 'isolation_v9': iso, 'guard_v1': gr,
                                                       'state_restoration_changed_tables': changed,
                                                       'errors': sum(1 for r in recs_all if r['error']), 'raw_file': path.name})
                            if not iso['clean'] or not gr['passed']:
                                invalid = True
                                break
                    finally:
                        cl.shutdown()
                    rec['window_minutes'] = round((time.time() - t0) / 60, 1); rec['valid'] = not invalid
                    manifest['repetitions'].append(rec); dump(out / 'manifest_partial.json', manifest)
                    if not invalid:
                        break
                    (out / arm / 'raw' / 'discarded').mkdir(exist_ok=True)
                    for f in files:
                        f.rename(out / arm / 'raw' / 'discarded' / f.name)
                    summaries = [x for x in summaries if not (x['arm'] == arm and x['rep_index_v1'] == rep + 1 and x['attempt'] == attempt)]
                    status(phase='window_discarded', rep=rep + 1, arm=arm, attempt=attempt)
                else:
                    raise RuntimeError(f'{arm} repeticao {rep + 1}: janela invalidada 3 vezes')
            status(phase='rep_done', rep=rep + 1, of=reps)
        dump(out / 'summaries.json', summaries)
        status(phase='agreement')
        if a.smoke:
            import oracle_ref
            ref = oracle_ref.compute(ctx['v41']['state'], reps=reps, warmup=warm, measured=meas)
        else:
            ref = pickle.loads((FROZEN / 'ORACLE_REF.pkl').read_bytes())
        manifest['agreement_with_v9_oracle'] = {arm: {dep: phase6.agreement([{'path': str(p)} for p in sorted((out / arm / 'raw').glob(f'{dep}_rep*.jsonl.gz'))], ref)
                                                      for dep in order} for arm in arms}
        if not a.smoke and read(PREREG)['code_sha256'] != code_hashes():
            raise ValueError('codigo mudou durante a medicao')
        manifest['status'] = 'COMPLETE'; manifest['finished_utc'] = dt.datetime.now(dt.timezone.utc).isoformat()
        dump_x(out / 'manifest.json', manifest)
        status(phase='complete', agreement={arm: {d: v['status'] for d, v in x.items()} for arm, x in manifest['agreement_with_v9_oracle'].items()})
    except BaseException as exc:
        manifest['status'] = 'FAILED'; manifest['error'] = repr(exc); manifest['traceback'] = traceback.format_exc()
        dump(out / 'failure_manifest.json', manifest); status(phase='failed', error=repr(exc))
        raise


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    n = sub.add_parser('node'); n.add_argument('--arm', required=True, choices=list(ARMS))
    r = sub.add_parser('run'); r.add_argument('--out', required=True); r.add_argument('--smoke', action='store_true')
    a = ap.parse_args()
    node(a.arm) if a.cmd == 'node' else run(a)
