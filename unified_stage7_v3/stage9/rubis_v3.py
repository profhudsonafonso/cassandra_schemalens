"""Stage 9 V3 for RUBiS: the V2 protocol-v1 harness (versao_ajustada/rubis_v9/rubis_v1/measure.py, unchanged on
disk) driven by the FROZEN V3 pre-registration, on two NEW dedicated nodes (create-only; V2 nodes untouched).

Substituted in memory only:
  * node/import write their manifests under outputs_v3/rubis/benchmark (V2 frozen/ files are never rewritten);
  * container names / ports of the two arms (sl_v3_rubis_cassandra 4.1.12, sl_v3_rubis311_cassandra 3.11.11);
  * the H source: frozen HBench V3 (canonical tables from V9 E13 + V3 read routes; write plans from the cost-free
    V9 route synthesis), keyspaces rubis_v9_v3h0N; A/A replica V3H01_AA;
  * the deployment order: V3H01, NoSE, V3H02, V8_AUTO_H_norm, V3H03, QPG, V3H04, V3H05, V3H01_AA (V2 interleaving rule).
Oracle: the frozen V2/V9 oracle reference (ORACLE_REF.pkl) over the same cohort and the same seeded histories.

    python -m unified_stage7_v3.stage9.rubis_v3 node   [--smoke]
    python -m unified_stage7_v3.stage9.rubis_v3 import [--smoke]
    python -m unified_stage7_v3.stage9.rubis_v3 run    [--smoke --out DIR]
"""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import pickle
import sys
from pathlib import Path

from ..freeze import paths, verify, verify_prereg
from ..provenance import REPO, dump

RB = REPO / 'versao_ajustada/rubis_v9'
BENCH = paths('rubis')['bench']
V3_ARMS = {'v41': ('sl_v3_rubis_cassandra', 9084), 'v311': ('sl_v3_rubis311_cassandra', 9085)}
AA = 'V3H01_AA'
AA_KS = 'rubis_v9_v3h01_aa'   # V9 materialiser requires the rubis_v9_ prefix; nodes are new and V3-only


def _setup(smoke):
    for p in (REPO, REPO / 'versao_ajustada/common', RB / 'rubis_v1'):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    CP = importlib.import_module('common_paths')
    for arm, (c, port) in V3_ARMS.items():
        CP.ARMS[arm]['container'] = c + ('_smoke' if smoke else '')
        CP.ARMS[arm]['port'] = port + (100 if smoke else 0)
    return CP


def _base(smoke):
    return BENCH.parent / 'smoke' / 'nodes' if smoke else BENCH / 'nodes'


def node(smoke=False):
    CP = _setup(smoke)
    import cassandra_env as E
    V = importlib.import_module('env')
    for arm in ('v41', 'v311'):
        proto = V.arm_protocol(arm)
        res, problems = V.check_resources(proto)
        if problems:
            raise SystemExit('BLOCKED: Docker resources: ' + '; '.join(problems))
        a = CP.ARMS[arm]
        E.start_container(a['container'], a['port'], proto)       # refuses an existing container (create-only)
        pf = V.preflight(arm)
        if pf['cassandra_release_version'] != a['version']:
            raise SystemExit('unexpected version ' + pf['cassandra_release_version'])
        dump(_base(smoke) / f'NODE_PREFLIGHT_{arm}.json', pf)
        print('NODE READY', arm, a['container'], pf['cassandra_release_version'], pf.get('image'))


def import_(smoke=False):
    """Same procedure as rubis_v1/transfer.py import_, manifest written under outputs_v3 and checked against the
    V2 reference fingerprints (identical data on both V3 nodes and identical to the V2 campaign)."""
    CP = _setup(smoke)
    T = importlib.import_module('transfer')
    from cassandra.concurrent import execute_concurrent_with_args
    v2man = json.loads((RB / 'frozen/TRANSFER_MANIFEST.json').read_text())
    blob = (RB / 'frozen/cohort_source.pkl').read_bytes()
    if hashlib.sha256(blob).hexdigest() != v2man['pickle_sha256']:
        raise SystemExit('ABORT: cohort_source.pkl changed')
    d = pickle.loads(blob)
    ref = v2man['fingerprints_reference']
    man_path = _base(smoke) / 'TRANSFER_MANIFEST_V3.json'
    man = json.loads(man_path.read_text()) if man_path.exists() else {'pickle_sha256': v2man['pickle_sha256'], 'arms': {}}
    for arm in ('v41', 'v311'):
        if arm in man['arms']:
            continue
        a = CP.ARMS[arm]
        c, s = T.connect(a['port'])
        try:
            for ks in d['ddl']:
                if list(s.execute('SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=%s', (ks,))):
                    raise FileExistsError(f'{ks} already exists on the V3 node (the node must be new)')
                s.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}}")
                for t, q in d['ddl'][ks].items():
                    s.execute(q)
            for ks, tables in (('rubis_full', d['rubis_full_rows']), (v2man['cohort_identity_keyspace'], {t: {i: r for i, r in enumerate(v)} for t, v in d['cohort_rows'].items()})):
                for t, rows in tables.items():
                    rows = list(rows.values())
                    if not rows:
                        continue
                    cols = sorted(rows[0])
                    st = s.prepare(f'INSERT INTO {ks}."{t}" ({", ".join(chr(34) + c_ + chr(34) for c_ in cols)}) VALUES ({", ".join("?" for _ in cols)})')
                    execute_concurrent_with_args(s, st, [tuple(r.get(k) for k in cols) for r in rows], concurrency=64, raise_on_first_error=True)
            raw, ids, closure, fp = T.fingerprints(s)
        finally:
            c.shutdown()
        ok = {k: fp[k] == ref[k] for k in ('raw_rows', 'ids', 'closure_rows', 'source_state_id', 'counts')}
        man['arms'][arm] = {'container': a['container'], 'port': a['port'], 'verification_vs_v2_reference': ok, 'IDENTICAL': all(ok.values()),
                            'counts': fp['counts']}
        dump(man_path, man)
        if not all(ok.values()):
            raise AssertionError(f'V3 node {arm} data differ from the V2 reference: {ok}')
        print('IMPORT OK', arm, fp['counts'])
    man['IDENTICAL'] = all(v['IDENTICAL'] for v in man['arms'].values())
    dump(man_path, man)


def v3_h_schemas(reg):
    """Frozen HBench V3 as V9 schema dicts: read routes = frozen V3 routes, write plans = V9 cost-free synthesis."""
    from ..adapters.rubis import CANON_FILE, install_campaign_stats, plan_id
    import schemalens_v9.routes as R
    from schemalens_v9.contract import READ_IDS
    install_campaign_stats()
    C = {t['canonical_id']: t for t in json.loads(CANON_FILE.read_text())['tables']}
    H = []
    for x in reg['hbench_v3_schemas']:
        S = [C[t] for t in x['canonical_ids']]
        ev = R.evaluate(S)
        if not ev['valid']:
            raise SystemExit(f'ABORT: {x["schema_id"]} invalid: {ev["invalid"]}')
        plans = dict(ev['plans'])
        for q in READ_IDS:
            opts = {plan_id(q, o): o for o in R.read_options(q, S)}
            plans[q] = opts[x['v3_routes'][str(q)]]
        H.append({'schema_id': x['schema_id'], 'v3_deployment_id': x['v3_deployment_id'], 'canonical_ids': x['canonical_ids'],
                  'tables': [C[t]['name'] for t in x['canonical_ids']], 'routes': {str(k): v for k, v in plans.items()}})
    return H, C


def order_of(reg):
    ids = [x['schema_id'] for x in reg['hbench_v3_schemas']]
    ext = ['NoSE', 'V8_AUTO_H_norm', 'QPG']
    seq = []
    for i, h in enumerate(ids):
        seq.append(h)
        if i < len(ext):
            seq.append(ext[i])
    return seq + [AA]


def run(out, smoke=False):
    if smoke:                                           # smoke = harness check BEFORE the freeze (V2 practice); never a result
        from ..plans import plan_for
        reg = plan_for('rubis')
    else:
        verify('rubis', rerun=True)
        reg = verify_prereg('rubis')
    CP = _setup(smoke)
    M = importlib.import_module('measure')
    from schemalens_v9 import phase5 as P5, phase5_run as PR
    H, C = v3_h_schemas(reg)
    PR.h_schemas = lambda: (H, C)
    order = order_of(reg)
    h1 = next(h for h in H if h['schema_id'] == 'V3H01')
    o_keyspace_of, o_build, o_mat = M.keyspace_of, M.build_adapter, M.materialize
    M.keyspace_of = lambda dep: AA_KS if dep == AA else o_keyspace_of(dep)

    def build_adapter(dep, s, state, closure, H_, C_):
        if dep == AA:
            return P5.GeneratedAdapter(s, AA_KS, h1, {t: C[t] for t in h1['canonical_ids']}, state), AA_KS
        return o_build(dep, s, state, closure, H_, C_)

    def materialize(s, deps, raw, state, H_, C_, status, arm):
        rec = o_mat(s, [d for d in deps if d != AA], raw, state, H_, C_, status, arm)
        if AA in deps:
            status(phase='materialize', arm=arm, deployment=AA)
            rec[AA] = P5.materialize_generated(s, AA_KS, [C[t] for t in h1['canonical_ids']], state)
        return rec
    M.keyspace_of, M.build_adapter, M.materialize = M.keyspace_of, build_adapter, materialize
    if out.exists():
        raise SystemExit(f'ABORT: {out} exists; V3 benchmark outputs are never overwritten')
    out.parent.mkdir(parents=True, exist_ok=True)
    hreg_path = out.parent / f'{out.name}_HARNESS_REGISTRATION.json'
    v2 = json.loads((RB / 'PREREGISTRATION_RUBIS_V1.json').read_text())
    hreg = {**{k: v2[k] for k in ('oracle', 'request_protocol', 'arm_order_rule', 'order_rule', 'decision_metric')},
            'status': 'V3_HARNESS_REGISTRATION (derived from outputs_v3/rubis/PREREGISTRATION_V3.json)',
            'deployments_base_order': order, 'code_sha256': CP.code_hashes(),
            'design_phase': {'v9_frozen_and_code_sha256': CP.v9_hashes()}, 'v3_h': H,
            'arms': {k: dict(v) for k, v in CP.ARMS.items()}}
    with hreg_path.open('x') as f:
        json.dump(hreg, f, indent=2, sort_keys=True, default=str)
    M.PREREG = hreg_path
    if smoke:
        F1 = importlib.import_module('freeze')
        F1.base_order = lambda: (order, {})
    a = argparse.Namespace(out=str(out), smoke=smoke)
    M.run(a)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('cmd', choices=['node', 'import', 'run'])
    ap.add_argument('--smoke', action='store_true')
    ap.add_argument('--out', default=None)
    a = ap.parse_args(argv)
    if a.cmd == 'node':
        node(a.smoke)
    elif a.cmd == 'import':
        import_(a.smoke)
    else:
        out = Path(a.out) if a.out else (BENCH.parent / 'smoke' / 'run1' if a.smoke else BENCH / 'run1')
        run(out.resolve(), a.smoke)


if __name__ == '__main__':
    main()
