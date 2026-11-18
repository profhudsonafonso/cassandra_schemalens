"""Passo 1 - transferencia EXATA da fonte da coorte: Cassandra 3.11.11 (V9) -> no dedicado 4.1.12.

A V9 nao le rubis_full inteiro (70,4 M linhas): ela le (a) categories/regions completas, (b) as linhas de
items/users/bids/comments/buynow das identidades da coorte (keyspace rubis_v8_c008_auto_norm) e o fecho de
usuarios referenciados (phase5.load_raw_state) e (c) o dominio do benchmark (semantic_replay_011.source_closure).
Este passo exporta exatamente essas linhas (todas as colunas, valores identicos) e as identidades da coorte,
recria-as no 4.1.12 com o mesmo DDL logico (colunas, tipos, chave primaria) e VERIFICA que load_raw_state e
source_closure produzem o MESMO hash canonico nos dois clusters.

    export : so le o 3.11.11 (SELECT); grava frozen/cohort_source.pkl + frozen/TRANSFER_MANIFEST.json
    import : cria rubis_full (subconjunto) e rubis_v8_c008_auto_norm no container v1 (novo) e verifica
"""
from __future__ import annotations

import argparse
import hashlib
import pickle

from common_paths import ARM, ARMS, FROZEN, SOURCE_PORT, dump, read, sha

BASE = ('categories', 'regions', 'users', 'items', 'bids', 'comments', 'buynow')
COHORT_TABLES = ('items', 'users', 'bids', 'comments', 'buynow')


def connect(port):
    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    c = Cluster(['127.0.0.1'], port=port)
    s = c.connect(); s.row_factory = dict_factory; s.default_timeout = 300
    return c, s


def table_ddl(meta, ks):
    cols = ', '.join(f'"{n}" {c.cql_type}' for n, c in meta.columns.items())
    pk = '(' + ', '.join(f'"{c.name}"' for c in meta.partition_key) + ')'
    ck = [c.name for c in meta.clustering_key]
    keys = ', '.join([pk] + [f'"{c}"' for c in ck])
    ddl = f'CREATE TABLE {ks}."{meta.name}" ({cols}, PRIMARY KEY ({keys}))'
    if ck:
        orders = []
        for c in meta.clustering_key:
            o = meta.columns[c.name].is_reversed
            orders.append(f'"{c.name}" {"DESC" if o else "ASC"}')
        ddl += ' WITH CLUSTERING ORDER BY (' + ', '.join(orders) + ')'
    return ddl


def canonical(obj):
    from schemalens_v9.common import sha256_obj
    return sha256_obj(obj)


def fingerprints(s):
    """What the V9 code actually reads, computed with the V9 functions themselves."""
    from schemalens_v9 import phase5 as M
    from reused.methodology.semantic_replay_011 import source_closure
    raw, ids = M.load_raw_state(s)
    closure, sid = source_closure(s)
    return raw, ids, closure, {'raw_rows': canonical({e: {str(i): M.canon_rows([r]) for i, r in rows.items()} for e, rows in raw.items()}),
                               'ids': canonical(ids), 'closure_rows': canonical({e: {str(i): str(sorted(r.items())) for i, r in rows.items()} for e, rows in closure.items()}),
                               'source_state_id': sid,
                               'counts': {e: len(v) for e, v in raw.items()}}


def export():
    from schemalens_v9 import phase5 as M
    from reused.methodology import oracle_011 as O
    out = FROZEN / 'cohort_source.pkl'
    if out.exists():
        raise FileExistsError(f'{out} ja existe (nao sobrescreve)')
    c, s = connect(SOURCE_PORT)
    try:
        rel = s.execute('SELECT release_version FROM system.local').one()['release_version']
        raw, ids, closure, fp = fingerprints(s)
        meta = c.metadata.keyspaces
        ddl = {'rubis_full': {t: table_ddl(meta['rubis_full'].tables[t], 'rubis_full') for t in BASE},
               O.COHORT_KEYSPACE: {t: table_ddl(meta[O.COHORT_KEYSPACE].tables[t], O.COHORT_KEYSPACE) for t in COHORT_TABLES}}
        rows = {t: {} for t in BASE}
        for src in (raw, closure):
            for e, d in src.items():
                for i, r in d.items():
                    rows[e][i] = dict(r)
        cohort_rows = {t: [dict(r) for r in s.execute(f'SELECT * FROM {O.COHORT_KEYSPACE}.{t}')] for t in COHORT_TABLES}
    finally:
        c.shutdown()
    FROZEN.mkdir(parents=True, exist_ok=True)
    out.write_bytes(pickle.dumps({'ddl': ddl, 'rubis_full_rows': rows, 'cohort_rows': cohort_rows,
                                  'raw': raw, 'ids': ids, 'closure': closure}, protocol=4))
    man = {'source_release_version': rel, 'source_port': SOURCE_PORT, 'fingerprints_source': fp,
           'rows_exported': {t: len(v) for t, v in rows.items()}, 'cohort_identity_rows': {t: len(v) for t, v in cohort_rows.items()},
           'cohort_identity_keyspace': O.COHORT_KEYSPACE, 'ddl': ddl, 'pickle_sha256': sha(out),
           'read_only_on_source': True}
    v9 = read(FROZEN.parent.parent.parent / 'runs/v9_generation_001/stages/E17_cohort_materialization/COHORT_MANIFEST.json', {})
    man['matches_v9_E17_cohort_counts'] = {k: man['fingerprints_source']['counts'].get(k) == v for k, v in (v9.get('state_counts') or {}).items()}
    dump(FROZEN / 'TRANSFER_MANIFEST.json', man)
    print('EXPORT OK', man['rows_exported'], man['matches_v9_E17_cohort_counts'])


def import_(arm=ARM):
    PORT, CONTAINER = ARMS[arm]['port'], ARMS[arm]['container']
    from cassandra.concurrent import execute_concurrent_with_args
    man = read(FROZEN / 'TRANSFER_MANIFEST.json')
    blob = (FROZEN / 'cohort_source.pkl').read_bytes()
    if hashlib.sha256(blob).hexdigest() != man['pickle_sha256']:
        raise ValueError('cohort_source.pkl alterado')
    d = pickle.loads(blob)
    c, s = connect(PORT)
    try:
        for ks in d['ddl']:
            if list(s.execute('SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=%s', (ks,))):
                raise FileExistsError(f'{ks} ja existe no no v1 (o no deve ser novo)')
            s.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}}")
            for t, q in d['ddl'][ks].items():
                s.execute(q)
        for ks, tables in (('rubis_full', d['rubis_full_rows']), (man['cohort_identity_keyspace'], {t: {i: r for i, r in enumerate(v)} for t, v in d['cohort_rows'].items()})):
            for t, rows in tables.items():
                rows = list(rows.values())
                if not rows:
                    continue
                cols = sorted(rows[0])
                st = s.prepare(f'INSERT INTO {ks}."{t}" ({", ".join(chr(34) + c + chr(34) for c in cols)}) VALUES ({", ".join("?" for _ in cols)})')
                execute_concurrent_with_args(s, st, [tuple(r.get(k) for k in cols) for r in rows], concurrency=64, raise_on_first_error=True)
        raw, ids, closure, fp = fingerprints(s)
    finally:
        c.shutdown()
    # Reference: the 3.11 source cluster (Mac mode) or, for regenerated data, the first node loaded.
    src = man.get('fingerprints_source') or man.get('fingerprints_reference')
    keys = ('raw_rows', 'ids', 'closure_rows', 'source_state_id', 'counts')
    if src is None:
        man['fingerprints_reference'] = fp; man['reference_arm'] = arm; src = fp
    ok = {k: fp[k] == src[k] for k in keys}
    man.setdefault('arms', {})[arm] = {'fingerprints_target': fp, 'container': CONTAINER, 'port': PORT,
                                      'verification': ok, 'IDENTICAL': all(ok.values())}
    man['IDENTICAL'] = all(v['IDENTICAL'] for v in man['arms'].values())
    dump(FROZEN / 'TRANSFER_MANIFEST.json', man)
    if not all(ok.values()):
        raise AssertionError(f'transferencia divergente: {ok}')
    print(f'IMPORT OK ({arm}): load_raw_state e source_closure identicos a referencia', fp['counts'])


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('action', choices=['export', 'import'])
    ap.add_argument('--arm', default=ARM, choices=list(ARMS))
    a = ap.parse_args()
    export() if a.action == 'export' else import_(a.arm)
