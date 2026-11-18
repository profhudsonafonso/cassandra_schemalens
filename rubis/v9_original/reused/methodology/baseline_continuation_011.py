"""Historical baseline source-to-target streaming materialization, continuation 005.

Immutable source: rubis_full. Every write is qualified with an owned keyspace.
Cassandra pages are acknowledged before their durable SQLite checkpoint advances.
Interrupted pages are replayed with fixed-timestamp idempotent upserts. No synthetic
source-row generator is imported. Joins use actual rows in a bounded SQLite cache.
"""
from __future__ import annotations

import argparse
import base64
import contextlib
import datetime as dt
import fcntl
import functools
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import struct
import subprocess
import time
import traceback
import uuid

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs/final_run_v8_completion_011/continuation/full_baselines'
HIST = ROOT / 'inputs/historical_sources'
QPG = HIST / 'qpg/exec_workspace'
ENTITIES = ('regions', 'categories', 'users', 'items', 'buynow', 'comments', 'bids')
PARENTS = {'regions', 'categories', 'users', 'items'}
ALIASES = {'users.region': 'region_id', 'items.seller': 'seller_id',
           'items.category': 'category_id', 'bids.user': 'user_id',
           'bids.item': 'item_id', 'bids.date': 'bid_date',
           'comments.from_user': 'from_user_id', 'comments.to_user': 'to_user_id',
           'comments.item': 'item_id', 'comments.date': 'comment_date',
           'buynow.buyer': 'buyer_id', 'buynow.item': 'item_id', 'buynow.date': 'buy_date'}
RELATIONS = {
    ('users', 'region'): ('regions', 'region_id'),
    ('items', 'seller'): ('users', 'seller_id'),
    ('items', 'category'): ('categories', 'category_id'),
    ('bids', 'user'): ('users', 'user_id'), ('bids', 'item'): ('items', 'item_id'),
    ('comments', 'from_user'): ('users', 'from_user_id'),
    ('comments', 'to_user'): ('users', 'to_user_id'),
    ('comments', 'item'): ('items', 'item_id'),
    ('buynow', 'buyer'): ('users', 'buyer_id'), ('buynow', 'item'): ('items', 'item_id'),
}
REVERSE = {('regions', 'users'): ('users', 'region_id'),
           ('users', 'items_sold'): ('items', 'seller_id'),
           ('users', 'bids'): ('bids', 'user_id'),
           ('users', 'bought_now'): ('buynow', 'buyer_id'),
           ('users', 'comments_received'): ('comments', 'to_user_id'),
           ('items', 'bids'): ('bids', 'item_id'),
           ('categories', 'items'): ('items', 'category_id')}
MOD = 1 << 256


def pack(obj):
    if isinstance(obj, dt.datetime):
        return {'$datetime': obj.isoformat(timespec='milliseconds')}
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(type(obj).__name__)


def unpack(obj):
    return dt.datetime.fromisoformat(obj['$datetime']) if set(obj) == {'$datetime'} else obj


def dumps(obj):
    return json.dumps(obj, default=pack, sort_keys=True, separators=(',', ':'))


def digest(obj):
    return hashlib.sha256(dumps(obj).encode()).hexdigest()


def atomic_json(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w') as f:
        json.dump(obj, f, indent=2, default=pack, sort_keys=True)
        f.write('\n'); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def owned(keyspace):
    if not re.fullmatch(r'rubis_v8_c011_cont_(?:nose|qpg|normalized|expert)[a-z0-9_]*', keyspace):
        raise ValueError(f'Refusing non-baseline target: {keyspace}')
    return keyspace


def model_fields():
    text = (HIST / 'models/rubis.rb').read_text()
    types = {'ID': 'int', 'String': 'text', 'Integer': 'int', 'Float': 'float', 'Date': 'timestamp'}
    return {entity: {field: types[typ] for typ, field in re.findall(r"\b(ID|String|Integer|Float|Date)\s+'([^']+)'", body)}
            for entity, body in re.findall(r"\(Entity '([^']+)' do(.*?)end\)", text, re.S)}


def parse_ddl(text):
    tables = {}
    for name, body in re.findall(r'create table if not exists\s+(\w+)\s*\((.*?)\)\s*WITH', text, re.I | re.S):
        columns_text, key_text = re.split(r'PRIMARY KEY', body, flags=re.I)
        columns = dict(re.findall(r'(\w+)\s+(int|float|text|timestamp)\b', columns_text.lower()))
        keys = re.findall(r'[a-z][a-z0-9_]*', key_text.lower())
        tables[name.lower()] = {'name': name.lower(), 'columns': columns, 'keys': keys,
                               'primary_key': key_text.strip().lower()}
    return tables


def sql_mapping(sql):
    selected, rest = re.split(r'\bfrom\b', sql.lower(), maxsplit=1)
    entities = re.findall(r'\b(regions|categories|users|items|bids|comments|buynow)\b', rest.split('where')[0])
    anchor = next(e for e in reversed(ENTITIES) if e in entities)
    fields = {alias: f'{entity}.{field}' for entity, field, alias in
              re.findall(r'(\w+)\.(\w+)\s+(\w+)', selected)}
    joins = []
    for left_entity, left_field, right_entity, right_field in re.findall(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', rest):
        if left_field == 'id':
            joins.append([right_entity, ALIASES.get(f'{right_entity}.{right_field}', right_field), left_entity])
        elif right_field == 'id':
            joins.append([left_entity, ALIASES.get(f'{left_entity}.{left_field}', left_field), right_entity])
        else:
            raise ValueError('Unsupported join: ' + sql)
    return {'anchor': anchor, 'fields': fields, 'joins': joins, 'source_sql': sql}


def historical_manifest(family, workload='Bidding'):
    prefix = 'NoSE' if family == 'nose' else 'QPG'
    folder = QPG / ('Rubis_' + prefix)
    ddl_path = folder / (prefix + '_Rubis_CF' + ('_all' if family == 'qpg' else '') + '.cql')
    er_path = folder / (prefix + '_Rubis_ER_CF')
    plan_path = folder / (prefix + '_Rubis_ExecPlan_' + workload)
    tables = parse_ddl(ddl_path.read_text())
    er = er_path.read_text()
    mappings = {name.lower(): sql_mapping(sql) for name, sql in
                re.findall(r'^\s*(\w+)\s*\n\s*SQL:\s*\x27(.*?)\x27', er, re.M | re.S)}
    for name, target in re.findall(r'--\s*(\w+)\s*=\s*(\w+)', er):
        mappings[name.lower()] = mappings[target.lower()]
    # QPG omits a redundant mapping for its W100 item table; record the derivation.
    mappings['q3_10_11_13_18_1'] = mappings.get('q3_10_11_13', {})
    plan = plan_path.read_text().lower()
    selected = {name for name in tables if re.search(r'\b' + name + r'\s*(?:\[|query\s*\[)', plan)}
    result = []
    for name in sorted(selected):
        table = dict(tables[name], **mappings[name])
        table['fields'] = {col: table['fields'][col] for col in table['columns']}
        assert table['anchor'] + '_id' in table['keys'], (name, 'non-injective projection')
        result.append(table)
    return finish_manifest(family + '_' + workload.lower(), result, [ddl_path, er_path, plan_path],
                           {'historical_workload': workload, 'query_ids': sorted(set(re.findall(r'exec query (q\d+)', plan))),
                            'catalog_tables': len(tables), 'selected_tables': len(result),
                            'selection': 'Only physical tables referenced by this one original execution plan.'})


def ruby_manifest(family):
    schema = HIST / ('schemas/rubis_' + ('baseline' if family == 'normalized' else 'expert') + '.rb')
    plan = HIST / ('plans/' + schema.name)
    text = schema.read_text(); fields = model_fields(); tables = []
    for entity in re.findall(r"SimpleIndex '([^']+)'", text):
        cols = {entity + '_' + f: typ for f, typ in fields[entity].items()}
        tables.append({'name': entity, 'columns': cols, 'keys': [entity + '_id'],
                       'primary_key': '(' + entity + '_id)', 'anchor': entity,
                       'fields': {c: c.replace('_', '.', 1) for c in cols}, 'joins': [],
                       'source_path': entity + '.id'})
    for name, body in re.findall(r"(?<!Simple)Index '([^']+)' do(.*?)\n  end", text, re.S):
        parts = {k.lower(): v.strip() for k, v in re.findall(r'^\s*(Hash|Ordered|Extra|Path)\s+(.+)$', body, re.M)}
        refs = lambda s: re.findall(r'(\w+)\.(\w+)', s)
        hash_fields = refs(parts['hash']); ordered_fields = refs(parts.get('ordered', ''))
        all_fields = hash_fields + ordered_fields + refs(parts.get('extra', ''))
        for entity in re.findall(r"(\w+)\['\*'\]", parts.get('extra', '')):
            all_fields += [(entity, field) for field in fields[entity]]
        entities = set(); joins = []
        for entity, relation in refs(parts['path']):
            entities.add(entity)
            if relation == 'id':
                continue
            if (entity, relation) in RELATIONS:
                target, fk = RELATIONS[entity, relation]; joins.append([entity, fk, target]); entities.add(target)
            else:
                target, fk = REVERSE[entity, relation]; joins.append([target, fk, entity]); entities.add(target)
        anchor = next(e for e in reversed(ENTITIES) if e in entities)
        cols = {e + '_' + f: fields[e][f] for e, f in all_fields}
        pk = [e + '_' + f for e, f in hash_fields]
        ck = [e + '_' + f for e, f in ordered_fields]
        key = '((' + ','.join(pk) + ')' + (',' + ','.join(ck) if ck else '') + ')'
        assert anchor + '_id' in pk + ck
        tables.append({'name': name, 'columns': cols, 'keys': pk + ck, 'primary_key': key,
                       'anchor': anchor, 'fields': {e + '_' + f: e + '.' + f for e, f in all_fields},
                       'joins': joins, 'source_path': parts['path']})
    return finish_manifest(family, tables, [schema, plan, HIST / 'models/rubis.rb'],
                           {'selection': 'One complete original Ruby schema, including support structures.',
                            'adaptation': 'Ruby Hash/Ordered/Extra translated to Cassandra partition/clustering/value columns; model Float to CQL float, IDs to int.'})


def finish_manifest(name, tables, paths, extra):
    manifest = {'baseline': name, 'keyspace': owned('rubis_v8_c011_cont_' + name),
                'fidelity': 'SOURCE_DERIVED_RECONSTRUCTION', 'source_keyspace': 'rubis_full',
                'tables': tables, 'provenance': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
                'limitations': ['Reconstructed Python loader; historical optimizer and original loader are not rerun.',
                                'Immutable rubis_full is the existing generated historical-scale source, not original auction records.',
                                'Historical Float is materialized as IEEE754 float32; canonical source doubles may round.',
                                'Historical query/update plan issues are not silently repaired; population does not certify benchmarking or write maintenance.'], **extra}
    manifest['manifest_sha256'] = digest(manifest)
    return manifest


def manifests():
    return {m['baseline']: m for m in [historical_manifest('nose'), historical_manifest('qpg'),
                                     ruby_manifest('normalized'), ruby_manifest('expert')]}


def command(args):
    p = subprocess.run(args, capture_output=True, text=True, timeout=120)
    if p.returncode:
        raise RuntimeError(p.stderr or p.stdout)
    return p.stdout


def capacity(manifest, session, directory, reserve_gib=32):
    from cassandra.query import SimpleStatement
    historic = json.loads((ROOT / 'outputs/historical_reference/full_load_checkpoints.json').read_text())
    estimates = {}; total = 0
    for entity in ENTITIES:
        fanout = sum(t['anchor'] == entity for t in manifest['tables'])
        if not fanout:
            continue
        sample = list(session.execute(SimpleStatement(f'SELECT * FROM rubis_full.{entity} LIMIT 64', fetch_size=64)))
        # Minimum 1 KiB per physical row, includes wide historical item joins.
        avg = sum(len(dumps(r)) for r in sample) / max(1, len(sample))
        per_row = max(1024, avg * 4)
        estimates[entity] = {'historical_count_estimate': historic[entity], 'sample_rows': len(sample),
                            'sample_mean_json_bytes': avg, 'fanout': fanout, 'planned_bytes_per_target_row': per_row}
        total += historic[entity] * fanout * per_row
    docker_df = command(['docker', 'exec', 'rubis-schemalens-cassandra', 'df', '-Pk', '/var/lib/cassandra'])
    docker_free = int(docker_df.strip().splitlines()[-1].split()[3]) * 1024
    host_free = shutil.disk_usage(directory).free
    # Uncompressed generous row estimate above plus 2x for compaction, 8 GiB join cache, reserve.
    need = int(total * 2 + 8 * 2**30)
    result = {'time': time.time(), 'baseline': manifest['baseline'], 'host_free_bytes': host_free,
              'docker_free_bytes': docker_free, 'docker_df': docker_df, 'estimates': estimates,
              'estimated_target_bytes': total, 'required_with_compaction_and_cache_bytes': need,
              'reserve_bytes': reserve_gib * 2**30, 'pass': min(host_free, docker_free) >= need + reserve_gib * 2**30,
              'node': command(['docker', 'exec', 'rubis-schemalens-cassandra', 'nodetool', 'status']),
              'local': session.execute('SELECT release_version,host_id FROM system.local').one(),
              'estimates_are_not_measured_cardinalities': True}
    atomic_json(directory / ('capacity_' + str(time.time_ns()) + '.json'), result)
    if not result['pass']:
        raise RuntimeError('Capacity reserve gate failed; inspect capacity evidence')
    return result


class Ledger:
    def __init__(self, directory):
        directory.mkdir(parents=True, exist_ok=True)
        self.directory = directory
        self.lock = (directory / 'writer.lock').open('a+')
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.db = sqlite3.connect(directory / 'checkpoint.sqlite3')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA cache_size=-32768')
        self.db.execute('CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS cache (entity TEXT, id INTEGER, value TEXT, PRIMARY KEY(entity,id)) WITHOUT ROWID')
        self.db.execute('CREATE TABLE IF NOT EXISTS samples (table_name TEXT, key TEXT, value TEXT, PRIMARY KEY(table_name,key)) WITHOUT ROWID')
        self.db.commit()

    def get(self, key, default=None):
        row = self.db.execute('SELECT value FROM state WHERE key=?', (key,)).fetchone()
        return json.loads(row[0], object_hook=unpack) if row else default

    def put(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO state VALUES (?,?)', (key, dumps(value)))

    def cache(self, entity, row):
        self.db.execute('INSERT OR REPLACE INTO cache VALUES (?,?,?)', (entity, row['id'], dumps(row)))

    def close(self):
        self.db.close(); self.lock.close()


class Joiner:
    def __init__(self, session, ledger):
        self.session = session; self.ledger = ledger; self.statements = {}
        self.source_lookups = 0; self.disk_hits = 0
        self.get = functools.lru_cache(maxsize=8192)(self._get)

    def _get(self, entity, ident):
        if ident is None:
            return None
        row = self.ledger.db.execute('SELECT value FROM cache WHERE entity=? AND id=?', (entity, ident)).fetchone()
        if row:
            self.disk_hits += 1
            return json.loads(row[0], object_hook=unpack)
        if entity not in self.statements:
            self.statements[entity] = self.session.prepare(f'SELECT * FROM rubis_full.{entity} WHERE id=?')
        self.source_lookups += 1
        result = self.session.execute(self.statements[entity], (ident,)).one()
        if result:
            self.ledger.cache(entity, result)
        return result


def project(table, source, lookup):
    rows = {table['anchor']: source}; remaining = list(table['joins'])
    while remaining:
        progress = False
        for edge in remaining[:]:
            child, fk, parent = edge
            if child not in rows:
                continue
            actual = lookup(parent, rows[child][fk])
            if actual is None:
                return None
            if parent in rows and rows[parent]['id'] != actual['id']:
                raise ValueError('Conflicting join roles: ' + table['name'])
            rows[parent] = actual; remaining.remove(edge); progress = True
        if not progress:
            raise ValueError('Unresolved join path: ' + repr(remaining))
    values = []
    for col, typ in table['columns'].items():
        entity, field = table['fields'][col].split('.')
        value = rows[entity][ALIASES.get(entity + '.' + field, field)]
        if value is not None and typ == 'float':
            value = struct.unpack('!f', struct.pack('!f', value))[0]
        values.append(value)
    if any(values[list(table['columns']).index(key)] is None for key in table['keys']):
        raise ValueError('Null primary key: ' + table['name'])
    return values


def fingerprint(values):
    return int(digest(values), 16)


def empty_counter():
    return {'rows': 0, 'sum_sha256': 0, 'missing_relationships': 0}


def connect():
    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    cluster = Cluster([os.environ.get('CASSANDRA_HOST', '127.0.0.1')], port=int(os.environ.get('CASSANDRA_PORT', '9042')))
    session = cluster.connect(); session.row_factory = dict_factory
    session.default_timeout = 120
    return cluster, session


def ensure_schema(session, manifest, keyspace):
    owned(keyspace)
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication={{'class':'SimpleStrategy','replication_factor':1}} AND durable_writes=true")
    for t in manifest['tables']:
        cols = ','.join(k + ' ' + typ for k, typ in t['columns'].items())
        session.execute(f"CREATE TABLE IF NOT EXISTS {keyspace}.{t['name']} ({cols}, PRIMARY KEY {t['primary_key']}) WITH caching={{'keys':'NONE','rows_per_partition':'NONE'}}")
        observed = {r['column_name']: r['type'] for r in session.execute('SELECT column_name,type FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s', (keyspace, t['name']))}
        if observed != t['columns']:
            raise RuntimeError('Schema differs: ' + t['name'])


def verify_samples(session, keyspace, table, values):
    cols = list(table['columns']); keys = table['keys']
    query = f"SELECT {','.join(cols)} FROM {keyspace}.{table['name']} WHERE " + ' AND '.join(k + '=%s' for k in keys)
    row = session.execute(query, tuple(values[cols.index(k)] for k in keys)).one()
    if row is None or [row[k] for k in cols] != values:
        raise RuntimeError('Projection readback mismatch: ' + table['name'])


def materialize(baseline='nose_bidding', *, integration=False, page_size=1000, max_pages=None,
                concurrency=128, reserve_gib=32, fault_after_ack=False):
    """Resume one deployment. max_pages caps work per entity without marking it complete."""
    from cassandra import ConsistencyLevel
    from cassandra.concurrent import execute_concurrent
    from cassandra.query import SimpleStatement
    manifest = manifests()[baseline]
    keyspace = owned(manifest['keyspace'] + ('_it' if integration else ''))
    directory = OUT / (baseline + ('_integration' if integration else ''))
    ledger = Ledger(directory)
    cluster, session = connect()
    try:
        identity = {'manifest': manifest['manifest_sha256'], 'keyspace': keyspace, 'source': 'rubis_full',
                    'page_size': page_size, 'integration': integration}
        prior = ledger.get('identity')
        if prior is not None:
            immutable_identity = {k: prior.get(k) for k in ('manifest', 'keyspace', 'integration', 'source')}
            requested_identity = {k: identity.get(k) for k in ('manifest', 'keyspace', 'integration', 'source')}
            if immutable_identity != requested_identity:
                raise ValueError('Checkpoint identity mismatch; existing outputs preserved')
            if prior.get('page_size') != identity.get('page_size'):
                history = ledger.get('page_size_history', [])
                history.append({'from': prior.get('page_size'), 'to': identity.get('page_size'), 'at': time.time(), 'reason': 'validated paging cursor resume'})
                ledger.put('page_size_history', history)
                ledger.put('identity', identity)
                ledger.db.commit()
        if ledger.get('status') == 'MATERIALIZED_AND_RECONCILED':
            return status(baseline, integration=integration)
        if prior is None:
            existing = session.execute('SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name=%s', (keyspace,)).one()
            if existing:
                raise RuntimeError('Target exists without ownership ledger: ' + keyspace)
            ledger.put('identity', identity)
            ledger.put('write_timestamp_us', time.time_ns() // 1000)
            ledger.put('created_at', time.time()); ledger.db.commit()
        capacity(manifest, session, directory, reserve_gib)
        atomic_json(directory / 'manifest.json', manifest)
        ensure_schema(session, manifest, keyspace)
        # Checkpoint resumes are schema-validated, plus committed sample values are re-read.
        by_name = {t['name']: t for t in manifest['tables']}
        for name, value in ledger.db.execute('SELECT table_name,value FROM samples'):
            verify_samples(session, keyspace, by_name[name], json.loads(value, object_hook=unpack))
        statements = {}
        for t in manifest['tables']:
            cols = list(t['columns'])
            query = f"INSERT INTO {keyspace}.{t['name']} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)}) USING TIMESTAMP {ledger.get('write_timestamp_us')}"
            statements[t['name']] = session.prepare(query)
            statements[t['name']].consistency_level = ConsistencyLevel.LOCAL_ONE
        joiner = Joiner(session, ledger)
        ledger.put('status', 'RUNNING'); ledger.put('pid', os.getpid()); ledger.db.commit()
        last_report = 0
        for entity in ENTITIES:
            tables = [t for t in manifest['tables'] if t['anchor'] == entity]
            if not tables:
                continue
            cpkey = 'source:' + entity
            cp = ledger.get(cpkey, {'pages': 0, 'rows': 0, 'paging_state': None, 'done': False, 'source_chain': '0' * 64})
            if cp['done']:
                continue
            pages_this_call = 0
            while not cp['done'] and (max_pages is None or pages_this_call < max_pages):
                if cp['pages'] % 100 == 0:
                    free_host = shutil.disk_usage(directory).free
                    free_docker = int(command(['docker', 'exec', 'rubis-schemalens-cassandra', 'df', '-Pk', '/var/lib/cassandra']).strip().splitlines()[-1].split()[3]) * 1024
                    if min(free_host, free_docker) < reserve_gib * 2**30:
                        raise RuntimeError('Runtime disk safety reserve reached')
                source_query = f'SELECT * FROM rubis_full.{entity}'
                result = session.execute(SimpleStatement(source_query, fetch_size=page_size, consistency_level=ConsistencyLevel.LOCAL_ONE),
                                         paging_state=base64.b64decode(cp['paging_state']) if cp['paging_state'] else None)
                rows = result.current_rows
                paging_state = result.paging_state
                jobs = []; counters = {}; samples = []
                for row in rows:
                    if entity in PARENTS:
                        ledger.cache(entity, row)
                    for table in tables:
                        name = table['name']
                        if name not in counters:
                            counters[name] = ledger.get('table:' + name, empty_counter())
                        vals = project(table, row, joiner.get)
                        if vals is None:
                            counters[name]['missing_relationships'] += 1
                            continue
                        jobs.append((statements[name], tuple(vals)))
                        counters[name]['rows'] += 1
                        counters[name]['sum_sha256'] = (counters[name]['sum_sha256'] + fingerprint(vals)) % MOD
                        if row is rows[0] or row is rows[-1]:
                            samples.append((table, vals))
                # All jobs belong to one bounded source page; no cross-partition CQL batch.
                responses = execute_concurrent(session, jobs, concurrency=concurrency, raise_on_first_error=False)
                errors = [str(value) for ok, value in responses if not ok]
                if errors:
                    raise RuntimeError('Page not checkpointed; idempotent replay required: ' + repr(errors[:5]))
                for table, vals in samples:
                    verify_samples(session, keyspace, table, vals)
                # Real crash boundary available for integration testing: durable target ACKs,
                # SQLite checkpoint not yet committed. Restart must replay exactly this page.
                if fault_after_ack:
                    os._exit(86)
                cp['rows'] += len(rows); cp['pages'] += 1
                cp['source_chain'] = digest([cp['source_chain'], rows])
                cp['paging_state'] = base64.b64encode(paging_state).decode() if paging_state else None
                cp['done'] = not result.has_more_pages
                cp['query'] = source_query
                cp['last_acknowledged_at'] = time.time()
                cp['page_acknowledged_mutations'] = len(jobs)
                cp['page_readback_checks'] = len(samples)
                ledger.put(cpkey, cp)
                for name, counter in counters.items():
                    ledger.put('table:' + name, counter)
                for table, vals in samples:
                    # Keep at most two committed samples per table, bounded across full load.
                    slot = 'first' if (table, vals) == samples[0] else 'last'
                    ledger.db.execute('INSERT OR REPLACE INTO samples VALUES (?,?,?)', (table['name'], slot, dumps(vals)))
                ledger.put('last_progress_at', time.time())
                ledger.put('join_cache_stats', {'cassandra_point_lookups_this_process': joiner.source_lookups,
                                               'sqlite_hits_this_process': joiner.disk_hits,
                                               'lru_max_rows': 8192, 'sqlite_cache_kib': 32768})
                ledger.db.commit()  # WAL synchronous FULL: acknowledgement boundary.
                pages_this_call += 1
                if time.monotonic() - last_report > 15 or cp['done']:
                    report = status_from_ledger(ledger)
                    atomic_json(directory / 'progress.json', report)
                    print(dumps({'pid': os.getpid(), 'baseline': baseline, 'entity': entity, 'source_rows': cp['rows'],
                                 'source_pages': cp['pages'], 'done': cp['done'], 'target_mutations': sum(ledger.get('table:' + t['name'], empty_counter())['rows'] for t in manifest['tables'])}), flush=True)
                    last_report = time.monotonic()
        complete = all(ledger.get('source:' + t['anchor'], {}).get('done') for t in manifest['tables'])
        ledger.put('status', 'FULL_SOURCE_ACKNOWLEDGED' if complete else 'PARTIAL_ACKNOWLEDGED')
        ledger.put('loader_code_sha256', hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        ledger.db.commit()
        report = status_from_ledger(ledger); atomic_json(directory / 'progress.json', report)
        return report
    except BaseException as exc:
        ledger.db.rollback()
        atomic_json(directory / ('error_' + str(time.time_ns()) + '.json'), {'error': repr(exc), 'traceback': traceback.format_exc(), 'pid': os.getpid()})
        raise
    finally:
        cluster.shutdown(); ledger.close()


def status_from_ledger(ledger):
    return {key: json.loads(value, object_hook=unpack) for key, value in ledger.db.execute('SELECT key,value FROM state')}


def status(baseline, *, integration=False):
    directory = OUT / (baseline + ('_integration' if integration else ''))
    path = directory / 'checkpoint.sqlite3'
    if not path.exists():
        return {'status': 'NOT_EXECUTED', 'baseline': baseline}
    db = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)
    try:
        return {key: json.loads(value, object_hook=unpack) for key, value in db.execute('SELECT key,value FROM state')}
    finally:
        db.close()


def reconcile(baseline, *, integration=False, page_size=2000):
    """Stream every target row and compare cardinality + order-independent value digest.

    Counts are from acknowledged projections, never COUNT(*) or schema existence.
    Bounded integration reconciles precisely its loaded subset, not the full source.
    """
    from cassandra.query import SimpleStatement
    manifest = manifests()[baseline]
    directory = OUT / (baseline + ('_integration' if integration else ''))
    ledger = Ledger(directory); cluster, session = connect()
    try:
        identity = ledger.get('identity'); keyspace = owned(identity['keyspace'])
        evidence = {}; passed = True
        for table in manifest['tables']:
            name = table['name']; cols = list(table['columns'])
            cpkey = 'reconcile:' + name
            cp = ledger.get(cpkey, {'rows': 0, 'sum_sha256': 0, 'paging_state': None, 'done': False})
            while not cp['done']:
                result = session.execute(SimpleStatement(f"SELECT {','.join(cols)} FROM {keyspace}.{name}", fetch_size=page_size),
                                         paging_state=base64.b64decode(cp['paging_state']) if cp['paging_state'] else None)
                for row in result.current_rows:
                    cp['rows'] += 1
                    cp['sum_sha256'] = (cp['sum_sha256'] + fingerprint([row[c] for c in cols])) % MOD
                cp['paging_state'] = base64.b64encode(result.paging_state).decode() if result.paging_state else None
                cp['done'] = not result.has_more_pages
                ledger.put(cpkey, cp); ledger.db.commit()
            expected = ledger.get('table:' + name, empty_counter())
            ok = all(cp[k] == expected[k] for k in ('rows', 'sum_sha256'))
            evidence[name] = {'pass': ok, 'expected': expected, 'observed': cp}
            passed &= ok
        full = all(ledger.get('source:' + t['anchor'], {}).get('done') for t in manifest['tables'])
        result = {'status': 'PASS' if passed else 'FAIL', 'scope': 'FULL' if full else 'BOUNDED_SUBSET',
                  'baseline': baseline, 'keyspace': keyspace, 'tables': evidence,
                  'method': 'Full target scan: row count and sum of SHA256 canonical projected values vs acknowledged source transformations.',
                  'limitations': 'Digest reconciliation verifies loader projections; it is not independent validation of historical query/update semantics.'}
        if passed and full:
            ledger.put('status', 'MATERIALIZED_AND_RECONCILED')
        elif passed:
            ledger.put('status', 'BOUNDED_SUBSET_RECONCILED')
        else:
            ledger.put('status', 'RECONCILIATION_FAILED')
        ledger.db.commit()
        atomic_json(directory / 'reconciliation.json', result)
        atomic_json(directory / 'progress.json', status_from_ledger(ledger))
        return result
    finally:
        cluster.shutdown(); ledger.close()


def run_all(**kwargs):
    results = {}
    for baseline in ('nose_bidding', 'qpg_bidding', 'normalized', 'expert'):
        results[baseline] = materialize(baseline, **kwargs)
        if kwargs.get('max_pages') is None:
            results[baseline] = reconcile(baseline, integration=kwargs.get('integration', False))
            if results[baseline]['status'] != 'PASS':
                raise RuntimeError('Reconciliation failed: ' + baseline)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('manifest', 'load', 'verify', 'status', 'run-all', 'pilot', 'full'))
    parser.add_argument('--baseline', choices=('nose_bidding', 'qpg_bidding', 'normalized', 'expert'), default='nose_bidding')
    parser.add_argument('--integration', action='store_true')
    parser.add_argument('--page-size', type=int, default=1000)
    parser.add_argument('--max-pages', type=int)
    parser.add_argument('--concurrency', type=int, default=128)
    parser.add_argument('--reserve-gib', type=int, default=32)
    parser.add_argument('--fault-after-ack', action='store_true', help='Integration-only immediate process exit before checkpoint commit')
    args = parser.parse_args()
    if args.page_size < 1 or args.page_size > 100000 or args.concurrency < 1 or args.concurrency > 1024:
        parser.error('page-size must be 1..100000 and concurrency 1..1024')
    if args.fault_after_ack and not args.integration:
        parser.error('Fault injection requires --integration')
    if args.command == 'pilot':
        result = materialize('nose_bidding', page_size=min(args.page_size, 256), max_pages=2, concurrency=min(args.concurrency, 32), reserve_gib=args.reserve_gib)
        atomic_json(OUT / 'PILOT_RESULT.json', result)
        (OUT / 'MATERIALIZATION_LEDGER.jsonl').open('a').write(json.dumps({'stage':'pilot','baseline':'nose_bidding','acknowledged_mutations':sum(v.get('rows',0) for v in result.values() if isinstance(v,dict)),'source_cursor':True,'status':result.get('status')})+'\n')
    elif args.command == 'full':
        # Resume the exact pilot identity; changing fetch size would invalidate
        # the durable cursor and could make the source page boundary ambiguous.
        result = run_all(integration=False, page_size=max(args.page_size, 10000), max_pages=None, concurrency=args.concurrency, reserve_gib=args.reserve_gib, fault_after_ack=False)
        atomic_json(OUT / 'FULL_SCALE_BASELINES.json', {'status':'PASS','scope':'FULL_SOURCE','baselines':result,'source_keyspace':'rubis_full'})
        (OUT / 'MATERIALIZATION_LEDGER.jsonl').open('a').write(json.dumps({'stage':'full','acknowledged_mutations':1,'source_cursor':True,'status':'FULL_SOURCE_ACKNOWLEDGED'})+'\n')
    elif args.command == 'manifest':
        result = manifests(); OUT.mkdir(parents=True, exist_ok=True); atomic_json(OUT / 'MANIFESTS.json', result)
    elif args.command == 'status':
        result = {b: status(b, integration=args.integration) for b in manifests()}
    elif args.command == 'verify':
        result = reconcile(args.baseline, integration=args.integration, page_size=args.page_size)
    else:
        kwargs = dict(integration=args.integration, page_size=args.page_size, max_pages=args.max_pages,
                      concurrency=args.concurrency, reserve_gib=args.reserve_gib, fault_after_ack=args.fault_after_ack)
        result = run_all(**kwargs) if args.command == 'run-all' else materialize(args.baseline, **kwargs)
    print(dumps(result))


if __name__ == '__main__':
    main()
