#!/usr/bin/env python3
"""SchemaLens Rev3 S3 - Physical Storage Controlled Experiment (campaign R1).

Run from the root of SchemaLens_IoT_Cassandra_V3_Automatico, in this order:

  python s3_physical.py preflight
  python s3_physical.py draft
  python s3_physical.py materialize --factor F --confirm CREATE_NEW_S3_KEYSPACES   (F = 1,2,4,8)
  python s3_physical.py flush       --factor F --confirm FLUSH_S3_KEYSPACES_ONLY
  python s3_physical.py validate    --factor F
  python s3_physical.py preregister
  python s3_physical.py run         --factor F        (strictly in order 1,2,4,8)
  python s3_physical.py audit       --factor F
  python s3_physical.py audit-all
  python s3_physical.py compare-s2
  python s3_physical.py report

Safety rules enforced in code:
  * only brand-new keyspaces matching sl_s3_r1_d{1,2,4,8}_{ref,u,d} are created;
  * no destructive or schema-altering CQL is ever issued (guarded session);
  * the frozen-B helper that recreates keyspaces is never imported;
  * nodetool is restricted to a read-only whitelist plus flush of S3 keyspaces;
  * every evidence file is opened with mode 'x' (never overwritten);
  * S2 inputs are only read and verified by SHA-256.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import platform
import random
import re
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Frozen constants (copied from the S2 preregistration / handoff; never change)
# ---------------------------------------------------------------------------
BASE = 'BASELINE_Q8_OPTIMIZED'
U = 'R3H_38939db96444'
D = 'R3H_d2ea1974de53'
LABELS = (BASE, U, D)
SHORT = {BASE: 'REF', U: 'TS2-U', D: 'TS2-D'}
SUFFIX = {BASE: 'ref', U: 'u', D: 'd'}
TABLES = {BASE: 'iot_reference_table', U: 'ts2_iot_wide_unbucketed', D: 'ts2_iot_wide'}
B_SHA = '8f88693851245838a7485300f8ce70aa850bd65c0c2d881225e9365cbcb39231'
S1_CASES_SHA = '31fd36c631d5ef2430087f57bb1fdfd000dbf956e415c64db5577d8c6c32d3e7'
S2_RUNNER_SHA = 'c5aa668f04bf79a5d6e52f7f3c5f5ba58575a322d70308e6dbf55f659eb4f11b'
DATA_SHA = {
    1: '047c269a42beea21333d7dbecdbde798862346015a35a5b1ced232b05a8f2c2a',
    2: '7389fcdf0504676be94fd557f057a46f8c210f97db34386c59d8e62f7ba41cf3',
    4: 'b5aa8eb7570d8100e211a5eaa0131f189428c993e9695148c5009176ab49ac37',
    8: 'dcdbcf015d6a8da27ca97dd1540762b133b50c5f082ec2dcc1048f43a110ae91',
}
FACTORS = (1, 2, 4, 8)
FACTOR_ORDER = (1, 2, 4, 8)          # same order as S2 (preregistered)
REPLICATE = 'r1'
SEED = 20261127                      # identical to S2 -> paired randomization
ROUNDS = 60
WARMUPS = 5
PAGE_SIZE = 16
TEMPORAL = ('Q2', 'Q7', 'Q9', 'Q12')

CONTAINER = 'schemalens_iot_cassandra_pipeline-cassandra-1'
HOST = '127.0.0.1'
PORT = 9043
CASSANDRA_RELEASE = '4.1.12'
CONFIRM_CREATE = 'CREATE_NEW_S3_KEYSPACES'
CONFIRM_FLUSH = 'FLUSH_S3_KEYSPACES_ONLY'
KS_RE = re.compile(r'sl_s3_r1_d(?:1|2|4|8)_(?:ref|u|d)')
# Statement verbs the guarded session refuses unconditionally.
FORBIDDEN_VERBS = ('DROP', 'TRUNCATE', 'ALTER', 'DELETE', 'UPDATE', 'BATCH', 'GRANT', 'REVOKE')
# nodetool sub-commands that may be invoked; everything else is refused.
NODETOOL_READONLY = ('tablestats', 'compactionstats', 'info', 'status', 'tpstats', 'version')
QUIESCE_POLL_S = 5
QUIESCE_STABLE_POLLS = 3
QUIESCE_MAX_S = 600

HERE = Path(__file__).resolve()


# ---------------------------------------------------------------------------
# Small I/O helpers (all writes refuse to overwrite)
# ---------------------------------------------------------------------------
def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def load(path):
    return json.loads(Path(path).read_text(encoding='utf8'))


def write_new(path, obj):
    with Path(path).open('x', encoding='utf8') as f:
        json.dump(obj, f, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
        f.write('\n')


def text_new(path, text):
    with Path(path).open('x', encoding='utf8') as f:
        f.write(text)


def csv_new(path, data):
    if not data:
        raise ValueError('Empty CSV is not allowed')
    with Path(path).open('x', newline='', encoding='utf8') as f:
        w = csv.DictWriter(f, fieldnames=list(data[0]))
        w.writeheader()
        w.writerows(data)


def csv_read(path):
    with Path(path).open(newline='', encoding='utf8') as f:
        return list(csv.DictReader(f))


def percentile(values, p):
    """Nearest-rank percentile (identical definition to S1/S2)."""
    values = sorted(values)
    if not values:
        raise ValueError('Empty sample')
    return values[math.ceil(len(values) * p / 100) - 1]


def now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')


def log(*parts):
    print(*parts, flush=True)


# ---------------------------------------------------------------------------
# Naming and paths
# ---------------------------------------------------------------------------
def names(factor):
    out = {label: f'sl_s3_{REPLICATE}_d{factor}_{SUFFIX[label]}' for label in LABELS}
    for ks in out.values():
        if not KS_RE.fullmatch(ks):
            raise ValueError('Unsafe S3 keyspace name: ' + ks)
    return out


def s2_names(factor):
    return {label: f'sl_s2_d{factor}_{SUFFIX[label]}' for label in LABELS}


class P:
    """Path layout of the S3 experiment."""

    def __init__(self, project):
        self.project = Path(project).resolve()
        self.s2 = self.project / 'experiments' / 'rev3_s2_partition_growth'
        self.s3 = self.project / 'experiments' / 'rev3_s3_physical_storage'
        self.camp = self.s3 / f'campaign_{REPLICATE}'
        self.protocol = self.s3 / 'S3_PROTOCOL.md'
        self.draft = self.s3 / 's3_preregister_draft.json'
        self.prereg = self.s3 / 's3_benchmark_preregister.json'
        self.preflight = self.s3 / 'preflight'
        self.orchestrator = self.project / 's3_run_all.py'

    def f(self, factor):
        return self.camp / f'{factor}x'


# ---------------------------------------------------------------------------
# External commands (docker / nodetool / cqlsh) - argument lists, never a shell
# ---------------------------------------------------------------------------
def _docker(args, timeout=600):
    r = subprocess.run(['docker', *args], capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(f'docker {" ".join(args)} failed: {r.stderr.strip()[:500]}')
    return r.stdout


def nodetool(*args):
    """Run nodetool inside the experiment container under a strict whitelist."""
    if not args:
        raise ValueError('nodetool requires a sub-command')
    cmd = args[0]
    if cmd == 'flush':
        targets = args[1:]
        # Never allow a keyspace-less flush (it would flush every table, incl. B/S2).
        if not targets or any(not KS_RE.fullmatch(t) for t in targets):
            raise PermissionError('flush only permitted on explicit S3 keyspaces')
    elif cmd == 'tablestats':
        if len(args) != 2 or not KS_RE.fullmatch(args[1]):
            raise PermissionError('tablestats only on one S3 keyspace')
    elif cmd not in NODETOOL_READONLY:
        raise PermissionError('nodetool sub-command not whitelisted: ' + cmd)
    return _docker(['exec', CONTAINER, 'nodetool', *args])


def cqlsh_describe(ks):
    if not KS_RE.fullmatch(ks):
        raise PermissionError('DESCRIBE limited to S3 keyspaces')
    return _docker(['exec', CONTAINER, 'cqlsh', '-e', f'DESCRIBE KEYSPACE {ks}'])


def sstable_files(ks):
    if not KS_RE.fullmatch(ks):
        raise PermissionError('file listing limited to S3 keyspaces')
    return _docker(['exec', CONTAINER, 'find', f'/var/lib/cassandra/data/{ks}', '-type', 'f',
                    '-printf', '%s\t%TY-%Tm-%Td %TH:%TM:%TS\t%p\n'])


def docker_stats():
    try:
        return _docker(['stats', '--no-stream', '--format', '{{json .}}'], timeout=60)
    except Exception as exc:  # telemetry only; never fatal
        return f'UNAVAILABLE: {exc}'


# ---------------------------------------------------------------------------
# nodetool tablestats parsing and the storage gate
# ---------------------------------------------------------------------------
_NUMERIC = {
    'SSTable count': 'sstable_count',
    'Space used (live)': 'space_live_bytes',
    'Space used (total)': 'space_total_bytes',
    'SSTable Compression Ratio': 'compression_ratio',
    'Number of partitions (estimate)': 'partitions_estimate',
    'Memtable cell count': 'memtable_cells',
    'Memtable data size': 'memtable_bytes',
    'Memtable switch count': 'memtable_switch_count',
    'Local read count': 'local_read_count',
    'Local write count': 'local_write_count',
    'Pending flushes': 'pending_flushes',
    'Compacted partition minimum bytes': 'partition_min_bytes',
    'Compacted partition maximum bytes': 'partition_max_bytes',
    'Compacted partition mean bytes': 'partition_mean_bytes',
    'Bloom filter space used': 'bloom_bytes',
    'Off heap memory used (total)': 'offheap_bytes',
}


def parse_tablestats(text):
    """Return {table_name: {metric: number}} from `nodetool tablestats <ks>`."""
    tables, current = {}, None
    for line in text.splitlines():
        s = line.strip()
        if s.startswith('Table:'):
            current = s.split(':', 1)[1].strip()
            tables[current] = {}
            continue
        if current is None or ':' not in s:
            continue
        key, val = (x.strip() for x in s.split(':', 1))
        if key in _NUMERIC:
            try:
                num = float(val.split()[0])
                tables[current][_NUMERIC[key]] = int(num) if num.is_integer() else num
            except (ValueError, IndexError):
                tables[current][_NUMERIC[key]] = None
    return tables


def gate_table(metrics):
    """Physical-state gate: persisted SSTables and an empty memtable."""
    problems = []
    if not metrics:
        return ['table missing from tablestats']
    if not (metrics.get('sstable_count') or 0) >= 1:
        problems.append('SSTable count < 1')
    if not (metrics.get('space_live_bytes') or 0) > 0:
        problems.append('Space used (live) == 0')
    if metrics.get('memtable_cells') != 0:
        problems.append(f"Memtable cell count = {metrics.get('memtable_cells')}")
    return problems


def compaction_state():
    text = nodetool('compactionstats')
    m = re.search(r'pending tasks:\s*(\d+)', text)
    pending = int(m.group(1)) if m else -1
    active = 'compaction type' in text.lower() or 'compaction_type' in text.lower()
    return pending, active, text


def wait_quiescent():
    """Poll compactionstats until no pending/active compaction for N polls."""
    started, stable, polls = time.time(), 0, []
    while True:
        pending, active, _ = compaction_state()
        polls.append(dict(t=now_iso(), pending=pending, active=active))
        stable = stable + 1 if (pending == 0 and not active) else 0
        if stable >= QUIESCE_STABLE_POLLS:
            return dict(status='QUIESCENT', seconds=round(time.time() - started, 1), polls=polls)
        if time.time() - started > QUIESCE_MAX_S:
            return dict(status='TIMEOUT_NOT_QUIESCENT', seconds=round(time.time() - started, 1), polls=polls)
        time.sleep(QUIESCE_POLL_S)


def storage_snapshot(paths, factor, tag, require_gate):
    """Capture tablestats/files/compaction/info/docker stats into storage_<tag>/."""
    folder = paths.f(factor) / f'storage_{tag}'
    folder.mkdir(parents=True, exist_ok=False)
    ks_names = names(factor)
    summary = dict(factor=factor, tag=tag, captured_utc=now_iso(), container=CONTAINER, variants={})
    all_ok = True
    for label in LABELS:
        ks, table = ks_names[label], TABLES[label]
        raw = nodetool('tablestats', ks)
        text_new(folder / f'{SUFFIX[label]}_tablestats.txt', raw)
        files = sstable_files(ks)
        text_new(folder / f'{SUFFIX[label]}_files.tsv', files)
        metrics = parse_tablestats(raw).get(table, {})
        data_files = [ln for ln in files.splitlines() if ln.endswith('-Data.db')]
        metrics['data_db_files'] = len(data_files)
        metrics['data_db_bytes'] = sum(int(ln.split('\t')[0]) for ln in data_files)
        problems = gate_table(metrics)
        all_ok &= not problems
        summary['variants'][label] = dict(keyspace=ks, table=table, metrics=metrics,
                                          gate='PASS' if not problems else 'FAIL', problems=problems)
    pending, active, ctext = compaction_state()
    text_new(folder / 'compactionstats.txt', ctext)
    text_new(folder / 'nodetool_info.txt', nodetool('info'))
    text_new(folder / 'tpstats.txt', nodetool('tpstats'))
    text_new(folder / 'docker_stats.jsonl', docker_stats())
    summary.update(compaction_pending=pending, compaction_active=active,
                   gate='PASS' if all_ok else 'FAIL', gate_required=require_gate)
    write_new(folder / 'summary.json', summary)
    for label in LABELS:
        v = summary['variants'][label]
        m = v['metrics']
        log(f"  [{tag}] {v['keyspace']:<18} sstables={m.get('sstable_count')} live={m.get('space_live_bytes')} "
            f"memtable_cells={m.get('memtable_cells')} part_mean={m.get('partition_mean_bytes')} "
            f"part_max={m.get('partition_max_bytes')} gate={v['gate']} {v['problems'] or ''}")
    if require_gate and not all_ok:
        raise AssertionError(f'Storage gate FAILED for factor {factor}x ({tag}); campaign must stop.')
    return summary


# ---------------------------------------------------------------------------
# Frozen B / S2 context
# ---------------------------------------------------------------------------
def verify_s2_inputs(paths):
    """Read-only verification of every S2 input that S3 reuses."""
    p1 = load(paths.s2 / 's1_preregister.json')
    if p1['frozen_rev3_B_sha256'] != B_SHA:
        raise ValueError('S1 preregistration bound to another B freeze')
    cases_file = paths.s2 / 's1_cases.json'
    if digest(cases_file) != S1_CASES_SHA or p1['s1_cases_sha256'] != S1_CASES_SHA:
        raise ValueError('s1_cases.json changed')
    cases = load(cases_file)
    if len(cases) != 18 or len({q['s1_case_id'] for q in cases}) != 18:
        raise ValueError('Expected 18 distinct cases')
    gen_path = paths.s2 / 'generated' / 'generation_manifest.json'
    gen = load(gen_path)
    data = {}
    for factor in FACTORS:
        entry = gen['datasets'][str(factor)]
        csv_path = paths.s2 / 'generated' / entry['file']
        if entry['sha256'] != DATA_SHA[factor] or digest(csv_path) != DATA_SHA[factor]:
            raise ValueError(f'S2 dataset {factor}x hash mismatch')
        if entry['cells'] != 54000 * factor:
            raise ValueError(f'S2 dataset {factor}x cell count mismatch')
        data[factor] = csv_path
    if digest(paths.project / 's2_benchmark.py') != S2_RUNNER_SHA:
        raise ValueError('s2_benchmark.py changed after S2 (must remain frozen)')
    return dict(p1=p1, cases=cases, cases_file=cases_file, gen_path=gen_path, data=data,
                experiment=Path(p1['original_B_experiment']).resolve(),
                workspace=Path(p1['original_B_workspace']).resolve())


def frozen_runtime(paths, s2):
    """Import the exact frozen-B modules (same sys.path order as S2) and verify the gate."""
    exp = s2['experiment']
    sys.path[:0] = [str(exp),
                    str(paths.project / 'SchemaLens_Rev3_Independent_Confirmatory'),
                    str(paths.project / 'SchemaLens_V4_Rev3_PlanAware'),
                    str(paths.project / 'SchemaLens_V4_Rev2_Confirmatory')]
    from rev3_confirmatory import gate
    import iot_pipeline.shared as shared
    verified, protocol, selected = gate.verify(s2['workspace'])
    if Path(verified).resolve() != exp or protocol['rev3_selection_sha256'] != B_SHA:
        raise ValueError('Frozen B gate mismatch')
    if Path(shared.ROOT).resolve() != exp:
        raise ValueError(f'iot_pipeline imported from {shared.ROOT}, expected {exp}')
    designs = {h['deployment_id']: h for h in selected}
    if designs.get(U, {}).get('canonical_source') != 'ts2_iot_wide_unbucketed' or \
            designs.get(D, {}).get('canonical_source') != 'ts2_iot_wide':
        raise ValueError('Frozen TS2-U/TS2-D designs not found')
    catalog_file = exp / gate.REV3 / 'stage5' / '15_executable_plan_catalog.json'
    catalog = {p['plan_id']: p for p in load(catalog_file)}
    stage4 = shared.path4(4, '10_candidate_specs.json')
    return dict(gate=gate, shared=shared, designs=designs, catalog=catalog,
                catalog_file=catalog_file, stage4_file=Path(stage4))


def read_dataset(path, factor):
    from iot_pipeline.shared import read_rows
    rows = read_rows(path)
    if len(rows) != 54000 * factor:
        raise ValueError('Unexpected logical row count')
    sensors = sorted({r['sensor_name'] for r in rows})
    if len(sensors) != 2 or len({r['device_id'] for r in rows}) != 9:
        raise ValueError('Unexpected sensor/device count')
    return rows, sensors


def connect():
    from cassandra.cluster import Cluster
    cluster = Cluster([HOST], port=PORT)
    session = cluster.connect()
    release = session.execute('SELECT release_version FROM system.local').one().release_version
    if release != CASSANDRA_RELEASE:
        cluster.shutdown()
        raise ValueError(f'Cassandra release {release} != {CASSANDRA_RELEASE}')
    return cluster, session


def existing_keyspaces(session):
    return {r.keyspace_name for r in session.execute('SELECT keyspace_name FROM system_schema.keyspaces')}


class GuardedSession:
    """Write-path proxy used only during materialization.

    Refuses any statement whose leading verb is destructive/schema-altering and any
    CREATE KEYSPACE outside the S3 namespace. Only execute/prepare/set_keyspace are
    exposed, which is all that _materialize() and load_wide() use.
    """

    def __init__(self, session):
        self._s = session

    @staticmethod
    def check(query):
        text = query if isinstance(query, str) else getattr(query, 'query_string', None)
        if text is None:
            return
        verb = text.strip().split(None, 1)[0].upper() if text.strip() else ''
        if verb in FORBIDDEN_VERBS:
            raise PermissionError('Forbidden CQL verb in S3: ' + verb)
        m = re.match(r'\s*CREATE\s+KEYSPACE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)', text, re.I)
        if m and not KS_RE.fullmatch(m.group(1)):
            raise PermissionError('CREATE KEYSPACE outside S3 namespace')

    def execute(self, query, *args, **kwargs):
        self.check(query)
        return self._s.execute(query, *args, **kwargs)

    def prepare(self, query, *args, **kwargs):
        self.check(query)
        return self._s.prepare(query, *args, **kwargs)

    def set_keyspace(self, ks):
        if not KS_RE.fullmatch(ks):
            raise PermissionError('set_keyspace outside S3 namespace during materialization')
        return self._s.set_keyspace(ks)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def cmd_preflight(paths):
    """Read-only environment capture. Fails if any sl_s3_* keyspace already exists."""
    paths.preflight.mkdir(parents=True, exist_ok=True)
    out = paths.preflight / 'environment_before.json'
    if out.exists():
        raise FileExistsError(f'{out} already exists')
    s2 = verify_s2_inputs(paths)
    rt = frozen_runtime(paths, s2)
    cluster, session = connect()
    try:
        present = existing_keyspaces(session)
    finally:
        cluster.shutdown()
    s3_present = sorted(k for k in present if k.startswith('sl_s3_'))
    if s3_present:
        raise FileExistsError(f'S3 keyspaces already exist: {s3_present}')

    def sh(args):
        try:
            return subprocess.run(args, capture_output=True, text=True, timeout=60).stdout
        except Exception as exc:
            return f'UNAVAILABLE: {exc}'
    import cassandra
    env = dict(
        status='S3_PREFLIGHT_READ_ONLY', captured_utc=now_iso(),
        python=sys.version, platform=platform.platform(), cassandra_driver=cassandra.__version__,
        cassandra_release=CASSANDRA_RELEASE, container=CONTAINER, host=HOST, port=PORT,
        s3_keyspaces_present=s3_present, s2_keyspaces_present=sorted(k for k in present if k.startswith('sl_s2_')),
        b_gate='PASS', b_selection_sha256=B_SHA, b_experiment=str(s2['experiment']),
        stage5_catalog_sha256=digest(rt['catalog_file']), stage4_catalog_sha256=digest(rt['stage4_file']),
        s2_dataset_sha256={str(f): DATA_SHA[f] for f in FACTORS}, s1_cases_sha256=S1_CASES_SHA,
        docker_ps=sh(['docker', 'ps', '--format', '{{.Names}}\t{{.Status}}\t{{.Ports}}']),
        nodetool_status=nodetool('status'), nodetool_info=nodetool('info'),
        compactionstats=nodetool('compactionstats'), df=sh(['df', '-h', str(paths.project)]),
        free=sh(['free', '-h']), nproc=sh(['nproc']).strip(), docker_stats=docker_stats(),
    )
    write_new(out, env)
    log('PASS preflight (read-only). S2 inputs verified, B gate PASS, no sl_s3_* keyspace present.')
    log('Saved:', out)


def cmd_draft(paths):
    if paths.draft.exists():
        raise FileExistsError('S3 draft already exists')
    if not (paths.preflight / 'environment_before.json').exists():
        raise FileNotFoundError('Run preflight first')
    s2 = verify_s2_inputs(paths)
    record = dict(
        status='S3_DRAFT_FROZEN_BEFORE_ANY_S3_DDL', created_utc=now_iso(),
        method='POST_AB_EXPLORATORY_SYNTHETIC_FIXED_HISTORY_DENSITY_SWEEP_FLUSHED_SSTABLE_STATE',
        replicate=REPLICATE, strategy='A: materialize all 12, flush all, gate, validate, preregister, then time in factor order',
        protocol_sha256=digest(paths.protocol), runner_sha256_at_draft=digest(HERE),
        frozen_B_sha256=B_SHA, s1_cases_sha256=S1_CASES_SHA,
        s2_generation_manifest_sha256=digest(s2['gen_path']),
        s2_dataset_sha256={str(f): DATA_SHA[f] for f in FACTORS},
        s2_runner_sha256=S2_RUNNER_SHA,
        keyspaces={str(f): names(f) for f in FACTORS}, labels=list(LABELS),
        factors=list(FACTORS), factor_order=list(FACTOR_ORDER),
        seed=SEED, per_factor_rng='random.Random(seed + factor) - identical to S2',
        rounds=ROUNDS, warmups=WARMUPS, page_size=PAGE_SIZE, percentile='nearest_rank',
        treatment='nodetool flush <S3 keyspace> after load, before validation and timing',
        not_applied=['manual compaction', 'cache clearing', 'autocompaction toggling', 'schema option changes'],
        storage_gate='every table: SSTable count >= 1, Space used (live) > 0, Memtable cell count == 0',
        quiescence=dict(poll_s=QUIESCE_POLL_S, stable_polls=QUIESCE_STABLE_POLLS, max_s=QUIESCE_MAX_S),
        expected=dict(observations_per_factor=18 * 3 * ROUNDS, orders_per_factor=18 * ROUNDS,
                      total_observations=4 * 18 * 3 * ROUNDS, total_orders=4 * 18 * ROUNDS,
                      semantic_validations=4 * 54),
    )
    write_new(paths.draft, record)
    log('PASS S3 draft frozen:', paths.draft)


def cmd_materialize(paths, factor, confirm):
    if confirm != CONFIRM_CREATE:
        raise PermissionError('Explicit --confirm CREATE_NEW_S3_KEYSPACES required')
    draft = load(paths.draft)
    if draft['protocol_sha256'] != digest(paths.protocol):
        raise ValueError('S3_PROTOCOL.md changed after the draft was frozen')
    out = paths.f(factor) / 'materialization'
    if out.exists():
        raise FileExistsError(f'{out} exists; refusing to rematerialize (use a new replicate)')
    s2 = verify_s2_inputs(paths)
    rt = frozen_runtime(paths, s2)
    # Make sure every frozen candidate is in the original B Stage4 catalog.
    cids = {x['candidate_id'] for x in rt['shared'].read(str(rt['stage4_file']))}
    for label in (U, D):
        if not set(rt['designs'][label]['candidates']) <= cids:
            raise ValueError('Frozen design references candidates absent from the Stage4 catalog')
    from iot_pipeline.stage9 import _materialize          # safe: CREATE TABLE IF NOT EXISTS + INSERT only
    from iot_pipeline.reference_baseline import table_ddl
    from iot_pipeline.shared import require_policy
    from v4_rev2.runtime_helpers import load_wide
    rows, sensors = read_dataset(s2['data'][factor], factor)
    policy = require_policy()
    ks_names = names(factor)
    cluster, raw_session = connect()
    session = GuardedSession(raw_session)
    created, report = [], []
    try:
        present = existing_keyspaces(raw_session)
        clash = [k for k in ks_names.values() if k in present]
        if clash:
            raise FileExistsError(f'Refusing: S3 keyspaces already exist {clash}')
        out.mkdir(parents=True, exist_ok=False)
        write_new(out / 'pre_create_manifest.json', dict(
            status='S3_PRECREATE_VERIFIED', factor=factor, captured_utc=now_iso(),
            dataset_sha256=DATA_SHA[factor], frozen_B_sha256=B_SHA, new_keyspaces=ks_names,
            no_drop_truncate_or_alter=True, prior_keyspaces_untouched=True,
            stage4_catalog=str(rt['stage4_file']), stage4_catalog_sha256=digest(rt['stage4_file'])))
        for label in LABELS:
            ks = ks_names[label]
            # Plain CREATE (no IF NOT EXISTS): a race/clash fails loudly instead of reusing data.
            session.execute('CREATE KEYSPACE ' + ks + " WITH replication = "
                            "{'class':'SimpleStrategy','replication_factor':1} AND durable_writes=true")
            created.append(ks)
            session.set_keyspace(ks)
            t0 = time.perf_counter()
            if label == BASE:
                session.execute(table_ddl(sensors))
                writes = load_wide(session, rows, sensors, TABLES[BASE], False)
            else:
                writes = sum(_materialize(session, rt['designs'][label], rows, sensors, policy).values())
            if writes != 27000 * factor:
                raise AssertionError(f'{ks}: {writes} writes, expected {27000 * factor} wide rows')
            line = dict(variant=label, keyspace=ks, table=TABLES[label], logical_cells=len(rows),
                        physical_write_operations=writes,
                        sequential_load_seconds=round(time.perf_counter() - t0, 6),
                        load_scope='Python sequential loader; not official ingestion throughput')
            report.append(line)
            write_new(out / f'load_{ks}.json', line)
            text_new(out / f'describe_{ks}.cql', cqlsh_describe(ks))
            log('LOADED', ks, 'writes=', writes, 'seconds=', line['sequential_load_seconds'])
        write_new(out / 'materialization_manifest.json', dict(
            status='S3_MATERIALIZATION_COMPLETE_NOT_FLUSHED', factor=factor, completed_utc=now_iso(),
            dataset_sha256=DATA_SHA[factor], frozen_B_sha256=B_SHA, keyspaces=ks_names, loads=report,
            describe_sha256={ks: digest(out / f'describe_{ks}.cql') for ks in ks_names.values()},
            no_prior_keyspace_modified=True, no_official_java_data_generation=True))
        log(f'COMPLETE {factor}x materialization (3 new keyspaces). Not flushed, not benchmarked.')
    except BaseException as exc:
        if out.exists() and not (out / 'failure_manifest.json').exists():
            write_new(out / 'failure_manifest.json', dict(
                status='FAILED_PRESERVE_PARTIAL_KEYSPACES', created_keyspaces=created,
                error_type=type(exc).__name__, error=str(exc), no_automatic_cleanup=True))
        raise
    finally:
        cluster.shutdown()


def cmd_flush(paths, factor, confirm):
    if confirm != CONFIRM_FLUSH:
        raise PermissionError('Explicit --confirm FLUSH_S3_KEYSPACES_ONLY required')
    fdir = paths.f(factor)
    mat = load(fdir / 'materialization' / 'materialization_manifest.json')
    if mat['status'] != 'S3_MATERIALIZATION_COMPLETE_NOT_FLUSHED' or mat['keyspaces'] != names(factor):
        raise ValueError('Materialization manifest missing or inconsistent')
    if (fdir / 'flush_manifest.json').exists() or (fdir / 'storage_post_flush').exists():
        raise FileExistsError('Flush already recorded for this factor')
    pending, active, before = compaction_state()
    flushed = []
    for label in LABELS:
        ks = names(factor)[label]
        t0 = time.perf_counter()
        nodetool('flush', ks)
        flushed.append(dict(keyspace=ks, seconds=round(time.perf_counter() - t0, 3), utc=now_iso()))
        log('FLUSHED', ks)
    quiet = wait_quiescent()
    log('Compaction quiescence:', quiet['status'], f"after {quiet['seconds']}s")
    snap = storage_snapshot(paths, factor, 'post_flush', require_gate=False)
    status = 'PASS' if snap['gate'] == 'PASS' and quiet['status'] == 'QUIESCENT' else 'FAIL'
    write_new(fdir / 'flush_manifest.json', dict(
        status=status, factor=factor, flushed=flushed, compaction_before=dict(pending=pending, active=active, raw=before),
        quiescence=quiet, storage_gate=snap['gate'],
        storage_summary_sha256=digest(fdir / 'storage_post_flush' / 'summary.json'),
        manual_compaction=False, cache_cleared=False))
    if status != 'PASS':
        raise AssertionError(f'Flush/storage gate FAILED for {factor}x; see {fdir}')
    log(f'PASS {factor}x flushed: all 3 tables persisted in SSTables, memtables empty.')


def make_executor(rt, session, sensors, days, oracle):
    from rev3_confirmatory.benchmark import baseline_dispatch
    from rev3.runtime import dispatch

    def execute(q, label):
        if label == BASE:
            return baseline_dispatch(session, q, True, sensors, days, PAGE_SIZE, oracle)
        return dispatch(session, q, rt['designs'][label], rt['catalog'], sensors, days, PAGE_SIZE, oracle)
    return execute


def cmd_validate(paths, factor):
    fdir = paths.f(factor)
    out = fdir / 'semantic_validation.json'
    if out.exists():
        raise FileExistsError(f'{out} exists')
    if load(fdir / 'flush_manifest.json')['status'] != 'PASS':
        raise ValueError('Factor not flushed / storage gate not passed')
    s2 = verify_s2_inputs(paths)
    rt = frozen_runtime(paths, s2)
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.offline import compare
    from v4_rev2.runtime_helpers import DAY
    rows, sensors = read_dataset(s2['data'][factor], factor)
    days = {r['timestamp'] // DAY for r in rows}
    oracle = source_oracle()
    expected = {q['s1_case_id']: oracle(rows, q) for q in s2['cases']}
    s2_val = load(paths.s2 / 's2_materialized' / f'{factor}x' / 'semantic_validation.json')
    s2_trace = {(r['case_id'], r['variant']): (r['execute_calls'], r['iterated_rows'], r['plan'])
                for r in s2_val['results']}
    ks_names = names(factor)
    results = []
    cluster, session = connect()
    try:
        if not set(ks_names.values()) <= existing_keyspaces(session):
            raise ValueError('S3 keyspaces missing')
        execute = make_executor(rt, session, sensors, days, oracle)
        for q in s2['cases']:
            case = q['s1_case_id']
            for label in LABELS:
                session.set_keyspace(ks_names[label])
                got, trace = execute(q, label)
                if not compare(got, expected[case]):
                    raise AssertionError(f'Semantic mismatch {case} {label}')
                tr = (trace['execute_calls'], trace['iterated_rows'], trace['physical_plan'])
                if tr != s2_trace[case, label]:
                    raise AssertionError(f'Access trace differs from S2: {case} {label} {tr} vs {s2_trace[case, label]}')
                results.append(dict(case_id=case, query_id=q['query_id'], factor=factor, variant=label,
                                    keyspace=ks_names[label], status='PASS', execute_calls=tr[0],
                                    iterated_rows=tr[1], plan=tr[2], oracle_cardinality=len(expected[case]),
                                    trace_identical_to_s2=True))
            log('PASS', case, '3/3 (oracle + trace identical to S2)')
    finally:
        cluster.shutdown()
    if len(results) != 54:
        raise AssertionError('Expected 54 validations')
    write_new(out, dict(status='PASS_ALL', factor=factor, validations=54, dataset_sha256=DATA_SHA[factor],
                        case_sha256=S1_CASES_SHA, frozen_B_sha256=B_SHA, measurement_status='NOT_BENCHMARKED',
                        telemetry_note='Python-side request/row counts; not server-side IO', results=results))
    log(f'PASS ALL {factor}x: 54/54 semantic validations, traces identical to S2.')


def prereg_inputs(paths):
    """Hashes of every per-factor artifact that must not change after preregistration."""
    out = {}
    for factor in FACTORS:
        fdir = paths.f(factor)
        out[str(factor)] = dict(
            dataset_sha256=DATA_SHA[factor],
            materialization_manifest_sha256=digest(fdir / 'materialization' / 'materialization_manifest.json'),
            flush_manifest_sha256=digest(fdir / 'flush_manifest.json'),
            storage_post_flush_summary_sha256=digest(fdir / 'storage_post_flush' / 'summary.json'),
            semantic_validation_sha256=digest(fdir / 'semantic_validation.json'))
    return out


def cmd_preregister(paths):
    if paths.prereg.exists():
        raise FileExistsError('S3 benchmark already preregistered')
    draft = load(paths.draft)
    if draft['protocol_sha256'] != digest(paths.protocol):
        raise ValueError('Protocol changed after draft')
    for factor in FACTORS:
        fdir = paths.f(factor)
        if load(fdir / 'flush_manifest.json')['status'] != 'PASS':
            raise ValueError(f'{factor}x storage gate not passed')
        if load(fdir / 'semantic_validation.json')['status'] != 'PASS_ALL':
            raise ValueError(f'{factor}x semantic validation incomplete')
        if (fdir / 'benchmark').exists():
            raise FileExistsError(f'Benchmark evidence already exists for {factor}x')
    s2 = verify_s2_inputs(paths)
    write_new(paths.prereg, dict(
        status='S3_BENCHMARK_PREREGISTERED_BEFORE_TIMING', created_utc=now_iso(),
        method=draft['method'], replicate=REPLICATE, strategy=draft['strategy'],
        frozen_B_sha256=B_SHA, runner_sha256=digest(HERE),
        orchestrator_sha256=digest(paths.orchestrator) if paths.orchestrator.exists() else None,
        protocol_sha256=digest(paths.protocol), draft_sha256=digest(paths.draft),
        s1_cases_sha256=S1_CASES_SHA, s2_generation_manifest_sha256=digest(s2['gen_path']),
        inputs=prereg_inputs(paths), keyspaces={str(f): names(f) for f in FACTORS},
        labels=list(LABELS), factor_order=list(FACTOR_ORDER), seed=SEED,
        per_factor_rng='random.Random(seed + factor)', rounds=ROUNDS, warmups=WARMUPS, page_size=PAGE_SIZE,
        randomization='shuffle cases and variant order every round; reverse variant order on even rounds (as S2)',
        timing='perf_counter_ns around dispatch+fetch+postprocessing; oracle comparison excluded',
        pre_timing_gate='storage_pre_timing snapshot must pass SSTable/memtable gate or the run aborts',
        benchmark_issues_no_ddl_dml_flush_or_compaction=True, percentile='nearest_rank',
        expected_total_observations=4 * 18 * 3 * ROUNDS))
    log('PASS S3 preregistration BEFORE any timing:', paths.prereg)


def verified_prereg(paths):
    p = load(paths.prereg)
    if p['status'] != 'S3_BENCHMARK_PREREGISTERED_BEFORE_TIMING' or p['frozen_B_sha256'] != B_SHA:
        raise ValueError('Invalid S3 preregistration')
    if p['runner_sha256'] != digest(HERE):
        raise ValueError('s3_physical.py changed after preregistration')
    if p['protocol_sha256'] != digest(paths.protocol) or p['draft_sha256'] != digest(paths.draft):
        raise ValueError('Protocol/draft changed after preregistration')
    if p['inputs'] != prereg_inputs(paths):
        raise ValueError('Per-factor inputs changed after preregistration')
    return p


def cmd_run(paths, factor):
    prereg = verified_prereg(paths)
    pos = FACTOR_ORDER.index(factor)
    for earlier in FACTOR_ORDER[:pos]:
        if not (paths.f(earlier) / 'benchmark' / 'manifest.json').exists():
            raise ValueError(f'Preregistered order violated: {earlier}x must be measured before {factor}x')
    fdir = paths.f(factor)
    dest = fdir / 'benchmark'
    if dest.exists():
        raise FileExistsError(f'{dest} exists; refusing to rerun (use a new replicate)')
    s2 = verify_s2_inputs(paths)
    rt = frozen_runtime(paths, s2)
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.offline import compare
    from v4_rev2.runtime_helpers import DAY
    rows, sensors = read_dataset(s2['data'][factor], factor)
    days = {r['timestamp'] // DAY for r in rows}
    oracle = source_oracle()
    cases = s2['cases']
    expected = {q['s1_case_id']: oracle(rows, q) for q in cases}
    counts = {c: len(v) for c, v in expected.items()}
    val = load(fdir / 'semantic_validation.json')
    recorded = {(r['case_id'], r['variant']): (int(r['execute_calls']), int(r['iterated_rows']), r['plan'])
                for r in val['results']}
    ks_names = names(factor)

    # Physical state immediately before timing: must still be SSTable-only.
    log(f'Storage pre-timing snapshot {factor}x ...')
    pre = storage_snapshot(paths, factor, 'pre_timing', require_gate=True)

    raw, orders = [], []
    started = time.time()
    dest.mkdir(parents=True, exist_ok=False)
    cluster, session = connect()
    try:
        if not set(ks_names.values()) <= existing_keyspaces(session):
            raise ValueError('S3 keyspaces missing')
        execute = make_executor(rt, session, sensors, days, oracle)
        # Untimed pre-timing oracle pass.
        for q in cases:
            for label in LABELS:
                session.set_keyspace(ks_names[label])
                got, trace = execute(q, label)
                case = q['s1_case_id']
                if not compare(got, expected[case]):
                    raise AssertionError(f'Pretiming semantic mismatch {case} {label}')
                if (trace['execute_calls'], trace['iterated_rows'], trace['physical_plan']) != recorded[case, label]:
                    raise AssertionError(f'Pretiming access trace changed {case} {label}')
        log(f'PASS {factor}x pretiming oracles 54/54')
        # Warm-ups (untimed), identical to S2.
        for q in cases:
            for label in LABELS:
                session.set_keyspace(ks_names[label])
                for _ in range(WARMUPS):
                    got, _trace = execute(q, label)
                    if not compare(got, expected[q['s1_case_id']]):
                        raise AssertionError('Warm-up semantic mismatch')
        log(f'Warm-ups done ({18 * 3 * WARMUPS}). Timing starts.')
        rng = random.Random(SEED + factor)
        for rep in range(1, ROUNDS + 1):
            queue = list(cases)
            rng.shuffle(queue)
            for q in queue:
                case = q['s1_case_id']
                variants = list(LABELS)
                rng.shuffle(variants)
                if rep % 2 == 0:
                    variants.reverse()
                orders.append(dict(factor=factor, case_id=case, rep=rep, variants_order='|'.join(variants)))
                for position, label in enumerate(variants, 1):
                    session.set_keyspace(ks_names[label])
                    t0 = time.perf_counter_ns()
                    got, trace = execute(q, label)
                    elapsed = (time.perf_counter_ns() - t0) / 1e6
                    if not compare(got, expected[case]):
                        raise AssertionError(f'Timed semantic mismatch {case}/{label}/{rep}')
                    if (trace['execute_calls'], trace['iterated_rows'], trace['physical_plan']) != recorded[case, label]:
                        raise AssertionError(f'Timed access trace changed {case}/{label}/{rep}')
                    raw.append(dict(factor=factor, case_id=case, query_id=q['query_id'],
                                    window_days=q['s1_window_days'] if q['s1_window_days'] is not None else 'CONTROL',
                                    variant=label, keyspace=ks_names[label], rep=rep, position=position,
                                    latency_ms=round(elapsed, 9), execute_calls=trace['execute_calls'],
                                    iterated_rows=trace['iterated_rows'], oracle_cardinality=counts[case],
                                    physical_plan=trace['physical_plan'], correctness='PASS'))
            if rep % 5 == 0:
                log(f'S3 factor={factor}x rounds={rep}/{ROUNDS} observations={len(raw)}')
        if len(raw) != 18 * 3 * ROUNDS or len(orders) != 18 * ROUNDS:
            raise AssertionError('Sample/order count mismatch')
    except BaseException as exc:
        write_new(dest / 'failure_manifest.json', dict(
            status='FAILED_PRESERVE_EVIDENCE', error_type=type(exc).__name__, error=str(exc),
            partial_samples=len(raw), utc=now_iso(), no_overwrite=True))
        if raw:
            csv_new(dest / 'partial_latencies.csv', raw)
        if orders:
            csv_new(dest / 'partial_orders.csv', orders)
        raise
    finally:
        cluster.shutdown()
    ended = time.time()
    summary = summarize(raw, factor)
    csv_new(dest / 'raw_latencies.csv', raw)
    csv_new(dest / 'orders.csv', orders)
    csv_new(dest / 'query_p95.csv', summary)
    write_new(dest / 'manifest.json', dict(
        status='S3_BENCHMARK_COMPLETE', factor=factor, replicate=REPLICATE,
        raw_measurements=len(raw), case_variant_groups=len(summary),
        expected_measurements=18 * 3 * ROUNDS, warmups=18 * 3 * WARMUPS,
        benchmark_preregister_sha256=digest(paths.prereg), dataset_sha256=DATA_SHA[factor],
        s1_cases_sha256=S1_CASES_SHA, frozen_selection_sha256=B_SHA, host=HOST, port=PORT,
        started_epoch_seconds=started, ended_epoch_seconds=ended,
        storage_pre_timing_gate=pre['gate'],
        storage_pre_timing_summary_sha256=digest(fdir / 'storage_pre_timing' / 'summary.json'),
        comparison_scope='S3 flushed-SSTable state, synthetic densification, client end-to-end latency',
        files_sha256={n: digest(dest / n) for n in ('raw_latencies.csv', 'orders.csv', 'query_p95.csv')}))
    log(f'COMPLETE factor={factor}x {len(raw)} observations in {ended - started:.0f}s')
    # Physical state right after timing (informational; drift is reported, not fatal).
    post = storage_snapshot(paths, factor, 'post_timing', require_gate=False)
    drift = {}
    for label in LABELS:
        a, b = pre['variants'][label]['metrics'], post['variants'][label]['metrics']
        drift[label] = {k: [a.get(k), b.get(k)] for k in
                        ('sstable_count', 'space_live_bytes', 'memtable_cells', 'data_db_files')
                        if a.get(k) != b.get(k)}
    write_new(fdir / 'post_timing_manifest.json', dict(
        factor=factor, post_gate=post['gate'], drift_pre_vs_post=drift,
        physical_state_unchanged=not any(drift.values()),
        storage_post_timing_summary_sha256=digest(fdir / 'storage_post_timing' / 'summary.json')))
    log('Physical state unchanged during timing:', not any(drift.values()), drift if any(drift.values()) else '')


def summarize(raw, factor):
    groups = defaultdict(list)
    for r in raw:
        groups[r['case_id'], r['variant']].append(r)
    if len(groups) != 54:
        raise ValueError(f'Expected 54 case/variant groups, found {len(groups)}')
    out = []
    for (case, label), rr in sorted(groups.items()):
        if len(rr) != ROUNDS:
            raise ValueError('Incomplete group')
        a = [float(x['latency_ms']) for x in rr]
        ex = rr[0]
        out.append(dict(factor=factor, case_id=case, query_id=ex['query_id'], window_days=ex['window_days'],
                        variant=label, runs=ROUNDS, mean_ms=round(statistics.mean(a), 9),
                        median_ms=round(statistics.median(a), 9), p95_ms=round(percentile(a, 95), 9),
                        p99_ms=round(percentile(a, 99), 9), execute_calls=ex['execute_calls'],
                        iterated_rows=ex['iterated_rows'], oracle_cardinality=ex['oracle_cardinality'],
                        correctness='PASS'))
    return out


def cmd_audit(paths, factor, quiet=False):
    verified_prereg(paths)
    fdir = paths.f(factor)
    dest = fdir / 'benchmark'
    m = load(dest / 'manifest.json')
    if m['status'] != 'S3_BENCHMARK_COMPLETE' or m['factor'] != factor or \
            m['benchmark_preregister_sha256'] != digest(paths.prereg):
        raise ValueError('Invalid benchmark manifest')
    for name, h in m['files_sha256'].items():
        if digest(dest / name) != h:
            raise ValueError('Result file changed: ' + name)
    if m['storage_pre_timing_gate'] != 'PASS' or \
            load(fdir / 'storage_pre_timing' / 'summary.json')['gate'] != 'PASS':
        raise ValueError('Pre-timing storage gate not PASS')
    if digest(fdir / 'storage_pre_timing' / 'summary.json') != m['storage_pre_timing_summary_sha256']:
        raise ValueError('Pre-timing storage snapshot changed')
    if not (fdir / 'post_timing_manifest.json').exists():
        raise ValueError('Post-timing storage snapshot missing')
    raw, orders, summ = (csv_read(dest / n) for n in ('raw_latencies.csv', 'orders.csv', 'query_p95.csv'))
    if len(raw) != 3240 or len(orders) != 1080 or len(summ) != 54:
        raise ValueError('Wrong sample/order/summary counts')
    case_ids = {q['s1_case_id'] for q in load(paths.s2 / 's1_cases.json')}
    perms = {}
    for o in orders:
        key = (o['case_id'], int(o['rep']))
        perm = o['variants_order'].split('|')
        if key in perms or key[0] not in case_ids or sorted(perm) != sorted(LABELS):
            raise ValueError('Invalid/duplicate permutation')
        perms[key] = perm
    # The randomization must be byte-identical to S2 (same seed, same procedure).
    s2_orders = csv_read(paths.s2 / 's2_benchmark' / f'{factor}x' / 'orders.csv')
    if [(o['case_id'], o['rep'], o['variants_order']) for o in s2_orders] != \
            [(o['case_id'], o['rep'], o['variants_order']) for o in orders]:
        raise ValueError('Randomized order differs from S2 (pairing broken)')
    ks_names = names(factor)
    seen = set()
    for r in raw:
        key = (r['case_id'], r['variant'], int(r['rep']))
        if key in seen or key[0] not in case_ids or r['variant'] not in LABELS or r['correctness'] != 'PASS':
            raise ValueError('Invalid/duplicate sample')
        seen.add(key)
        if r['keyspace'] != ks_names[r['variant']] or int(r['factor']) != factor:
            raise ValueError('Wrong keyspace/factor in sample')
        if perms[r['case_id'], int(r['rep'])][int(r['position']) - 1] != r['variant']:
            raise ValueError('Sample position does not match recorded order')
        lat = float(r['latency_ms'])
        if not (lat > 0 and math.isfinite(lat)):
            raise ValueError('Non-positive / non-finite latency')
    recomputed = summarize(raw, factor)
    orig = {(r['case_id'], r['variant']): r for r in summ}
    for r in recomputed:
        o = orig[r['case_id'], r['variant']]
        for k in ('mean_ms', 'median_ms', 'p95_ms', 'p99_ms'):
            if not math.isclose(float(r[k]), float(o[k]), rel_tol=1e-9, abs_tol=1e-7):
                raise ValueError('Summary mismatch ' + k)
        if str(r['execute_calls']) != str(o['execute_calls']) or str(r['iterated_rows']) != str(o['iterated_rows']):
            raise ValueError('Telemetry mismatch in summary')
    report = dict(status='PASS', factor=factor, samples=len(raw), orders=len(orders), groups=len(recomputed),
                  recomputed_metrics='PASS', randomization_identical_to_s2=True,
                  storage_pre_timing_gate='PASS', data_sha256=m['dataset_sha256'])
    rf = dest / 'audit.json'
    if rf.exists():
        if load(rf) != report:
            raise ValueError('Existing audit differs')
    else:
        write_new(rf, report)
    if not quiet:
        log(f'PASS audit {factor}x: {len(raw)} latencies, {len(recomputed)} groups, {len(orders)} orders, '
            f'order identical to S2')
    return recomputed


def comparison_rows(by_factor, cases):
    out = []
    for factor, rows in by_factor.items():
        idx = {(r['case_id'], r['variant']): r for r in rows}
        for q in cases:
            c = q['s1_case_id']
            b, u, d = (idx[c, label] for label in LABELS)
            pb, pu, pd = (float(x['p95_ms']) for x in (b, u, d))
            out.append(dict(factor=factor, case_id=c, query_id=q['query_id'],
                            window_days=q['s1_window_days'] if q['s1_window_days'] is not None else 'CONTROL',
                            baseline_p95_ms=pb, ts2u_p95_ms=pu, ts2d_p95_ms=pd,
                            d_over_u_ratio=round(pd / pu, 9), d_over_u_pct=round((pd / pu - 1) * 100, 5),
                            winner='TS2-U' if pu < pd else 'TS2-D' if pd < pu else 'TIE',
                            ts2u_execute_calls=u['execute_calls'], ts2d_execute_calls=d['execute_calls'],
                            ts2u_iterated_rows=u['iterated_rows'], ts2d_iterated_rows=d['iterated_rows']))
    return out


def cmd_audit_all(paths):
    by_factor = {f: cmd_audit(paths, f) for f in FACTORS}
    target = paths.s3 / 'all_density_comparison.csv'
    if target.exists():
        log('NOTE: all_density_comparison.csv already exists; per-factor audits re-passed, file kept unchanged.')
        return
    csv_new(target, comparison_rows(by_factor, load(paths.s2 / 's1_cases.json')))
    log('PASS ALL S3: 12,960 observations, 4,320 orders audited ->', target)


def cmd_compare_s2(paths):
    target = paths.s3 / 's3_vs_s2_comparison.csv'
    summary_file = paths.s3 / 's3_summary.json'
    if target.exists() or summary_file.exists():
        raise FileExistsError('S3 vs S2 comparison already exists')
    s3 = {(int(r['factor']), r['case_id']): r for r in csv_read(paths.s3 / 'all_density_comparison.csv')}
    s2 = {(int(r['factor']), r['case_id']): r for r in
          csv_read(paths.s2 / 's2_benchmark' / 'all_density_comparison.csv')}
    if set(s3) != set(s2) or len(s3) != 72:
        raise ValueError('S2/S3 comparison keys differ')
    rows = []
    for key in sorted(s3, key=lambda k: (k[0], k[1])):
        a, b = s2[key], s3[key]
        row = dict(factor=key[0], case_id=key[1], query_id=b['query_id'], window_days=b['window_days'])
        for col, short in (('baseline_p95_ms', 'ref'), ('ts2u_p95_ms', 'u'), ('ts2d_p95_ms', 'd')):
            row[f's2_{short}_p95_ms'] = float(a[col])
            row[f's3_{short}_p95_ms'] = float(b[col])
            row[f's3_over_s2_{short}'] = round(float(b[col]) / float(a[col]), 6)
        row.update(s2_d_over_u=float(a['d_over_u_ratio']), s3_d_over_u=float(b['d_over_u_ratio']),
                   s2_winner=a['winner'], s3_winner=b['winner'], winner_changed=a['winner'] != b['winner'],
                   ts2u_execute_calls=b['ts2u_execute_calls'], ts2d_execute_calls=b['ts2d_execute_calls'],
                   iterated_rows=b['ts2u_iterated_rows'])
        rows.append(row)
    csv_new(target, rows)
    per_factor = {}
    for f in FACTORS:
        t = [r for r in rows if r['factor'] == f and r['query_id'] in TEMPORAL]
        per_factor[str(f)] = dict(
            temporal_comparisons=len(t),
            s2_wins_u=sum(r['s2_winner'] == 'TS2-U' for r in t), s2_wins_d=sum(r['s2_winner'] == 'TS2-D' for r in t),
            s3_wins_u=sum(r['s3_winner'] == 'TS2-U' for r in t), s3_wins_d=sum(r['s3_winner'] == 'TS2-D' for r in t),
            s2_median_d_over_u=round(statistics.median(r['s2_d_over_u'] for r in t), 4),
            s3_median_d_over_u=round(statistics.median(r['s3_d_over_u'] for r in t), 4),
            winners_changed=sum(r['winner_changed'] for r in t),
            median_s3_over_s2={s: round(statistics.median(r[f's3_over_s2_{s}'] for r in t), 4) for s in ('ref', 'u', 'd')},
            q12_w21=next(({k: r[k] for k in ('s2_u_p95_ms', 's2_d_p95_ms', 's3_u_p95_ms', 's3_d_p95_ms',
                                              's2_d_over_u', 's3_d_over_u')}
                          for r in t if r['case_id'] == 'Q12_W21'), None),
            controls={r['case_id']: {k: r[k] for k in ('s2_ref_p95_ms', 's3_ref_p95_ms', 's2_u_p95_ms',
                                                         's3_u_p95_ms', 's2_d_p95_ms', 's3_d_p95_ms')}
                      for r in rows if r['factor'] == f and r['query_id'] not in TEMPORAL})
    storage = {}
    for f in FACTORS:
        storage[str(f)] = {}
        for tag in ('post_flush', 'pre_timing', 'post_timing'):
            s = load(paths.f(f) / f'storage_{tag}' / 'summary.json')
            storage[str(f)][tag] = {SHORT[l]: s['variants'][l]['metrics'] for l in LABELS}
        storage[str(f)]['state_unchanged_during_timing'] = load(
            paths.f(f) / 'post_timing_manifest.json')['physical_state_unchanged']
    s2_storage = {}
    for f in FACTORS:
        s2_storage[str(f)] = {}
        for label in LABELS:
            fp = paths.s2 / 'storage' / f'{s2_names(f)[label]}_tablestats.txt'
            s2_storage[str(f)][SHORT[label]] = parse_tablestats(fp.read_text()).get(TABLES[label], {})
    timing = {str(f): load(paths.f(f) / 'benchmark' / 'manifest.json') for f in FACTORS}
    write_new(summary_file, dict(
        status='S3_COMPARISON_COMPLETE', created_utc=now_iso(),
        totals=dict(s2_wins_u=sum(v['s2_wins_u'] for v in per_factor.values()),
                    s2_wins_d=sum(v['s2_wins_d'] for v in per_factor.values()),
                    s3_wins_u=sum(v['s3_wins_u'] for v in per_factor.values()),
                    s3_wins_d=sum(v['s3_wins_d'] for v in per_factor.values()),
                    winners_changed=sum(v['winners_changed'] for v in per_factor.values())),
        per_factor=per_factor, storage_s3=storage, storage_s2_post_timing_snapshot=s2_storage,
        benchmark_seconds={f: round(m['ended_epoch_seconds'] - m['started_epoch_seconds'], 1)
                           for f, m in timing.items()}))
    log('PASS S3 vs S2 comparison ->', target)
    for f, v in per_factor.items():
        log(f"  {f}x  S2 U/D wins {v['s2_wins_u']}/{v['s2_wins_d']} median D/U {v['s2_median_d_over_u']}"
            f"  |  S3 U/D wins {v['s3_wins_u']}/{v['s3_wins_d']} median D/U {v['s3_median_d_over_u']}")


def cmd_report(paths):
    out = paths.s3 / 'FINAL_AUDIT_PT.md'
    if out.exists():
        raise FileExistsError(f'{out} exists')
    s = load(paths.s3 / 's3_summary.json')
    pf, st, tot = s['per_factor'], s['storage_s3'], s['totals']
    L = ['# S3 — Auditoria final (campanha R1)', '',
         f'Gerado em {now_iso()} por `s3_physical.py report` a partir de evidências auditadas.', '',
         '## Status', '',
         '- Pré-registro antes de qualquer medição: `s3_benchmark_preregister.json`.',
         '- 216/216 validações semânticas PASS; traços de acesso idênticos aos do S2.',
         '- 12.960 latências e 4.320 ordens auditadas; ordem aleatória idêntica à do S2 (pareamento).',
         '- Gate de armazenamento antes da medição: todas as 12 tabelas com SSTable ≥ 1 e memtable vazia.', '',
         '## Vitórias nas 16 comparações temporais por densidade', '',
         '| Fator | S2 U/D | S3 U/D | Mediana D/U S2 | Mediana D/U S3 | Vencedor mudou |', '|---:|---:|---:|---:|---:|---:|']
    for f in map(str, FACTORS):
        v = pf[f]
        L.append(f"| {f}× | {v['s2_wins_u']}/{v['s2_wins_d']} | {v['s3_wins_u']}/{v['s3_wins_d']} | "
                 f"{v['s2_median_d_over_u']:.3f} | {v['s3_median_d_over_u']:.3f} | {v['winners_changed']} |")
    L += [f"| **Total** | **{tot['s2_wins_u']}/{tot['s2_wins_d']}** | **{tot['s3_wins_u']}/{tot['s3_wins_d']}** | | | **{tot['winners_changed']}** |", '',
          '## Mediana p95 S3/S2 por variante (casos temporais)', '', '| Fator | REF | TS2-U | TS2-D |', '|---:|---:|---:|---:|']
    for f in map(str, FACTORS):
        m = pf[f]['median_s3_over_s2']
        L.append(f"| {f}× | {m['ref']:.3f} | {m['u']:.3f} | {m['d']:.3f} |")
    L += ['', '## Q12 em 21 dias (p95, ms)', '', '| Fator | S2 U | S2 D | S3 U | S3 D | D/U S2 | D/U S3 |', '|---:|---:|---:|---:|---:|---:|---:|']
    for f in map(str, FACTORS):
        q = pf[f]['q12_w21']
        L.append(f"| {f}× | {q['s2_u_p95_ms']:.3f} | {q['s2_d_p95_ms']:.3f} | {q['s3_u_p95_ms']:.3f} | "
                 f"{q['s3_d_p95_ms']:.3f} | {q['s2_d_over_u']:.3f} | {q['s3_d_over_u']:.3f} |")
    L += ['', '## Estado físico S3 antes da medição', '',
          '| Fator | Variante | SSTables | Live bytes | Memtable cells | Partição média (B) | Partição máx (B) | Partições (est.) | Inalterado na medição |',
          '|---:|---|---:|---:|---:|---:|---:|---:|---|']
    for f in map(str, FACTORS):
        for v in ('REF', 'TS2-U', 'TS2-D'):
            m = st[f]['pre_timing'][v]
            L.append(f"| {f}× | {v} | {m.get('sstable_count')} | {m.get('space_live_bytes')} | {m.get('memtable_cells')} | "
                     f"{m.get('partition_mean_bytes')} | {m.get('partition_max_bytes')} | {m.get('partitions_estimate')} | "
                     f"{'sim' if st[f]['state_unchanged_during_timing'] else 'NÃO'} |")
    L += ['', '## Interpretação permitida', '',
          '- Leitura descritiva de uma única réplica (R1). A diferença S3×S2 mistura o estado físico (SSTable vs memtable) '
          'com o momento da execução (05/11 vs 10/11) e com a carga do host; não é efeito causal isolado do flush.',
          '- O page cache do SO provavelmente estava quente (SSTables recém-escritas): S3 mede "SSTable quente", não disco frio.',
          '- Contagens de chamadas e linhas são do cliente Python, não I/O do servidor.',
          '- Sem alegação de significância estatística; robustez exige réplicas independentes (r2, r3).',
          '- Evidência adicional: o container parou de forma não limpa em 10/11 (exit 255); o replay do commitlog '
          'persistiu as memtables S2 em SSTables. As medições S2 continuam válidas para o estado em que foram feitas.', '']
    text_new(out, '\n'.join(L))
    log('Report written:', out)


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('action', choices=('preflight', 'draft', 'materialize', 'flush', 'validate', 'preregister',
                                       'run', 'audit', 'audit-all', 'compare-s2', 'report'))
    ap.add_argument('--project', type=Path, default=Path.cwd())
    ap.add_argument('--factor', type=int, choices=FACTORS)
    ap.add_argument('--confirm', default='')
    a = ap.parse_args()
    paths = P(a.project)
    if not (paths.s2 / 's1_preregister.json').is_file():
        ap.error('Run from the project root (S2 package not found below experiments/)')
    needs_factor = ('materialize', 'flush', 'validate', 'run', 'audit')
    if a.action in needs_factor and a.factor is None:
        ap.error('--factor is required for ' + a.action)
    {
        'preflight': lambda: cmd_preflight(paths),
        'draft': lambda: cmd_draft(paths),
        'materialize': lambda: cmd_materialize(paths, a.factor, a.confirm),
        'flush': lambda: cmd_flush(paths, a.factor, a.confirm),
        'validate': lambda: cmd_validate(paths, a.factor),
        'preregister': lambda: cmd_preregister(paths),
        'run': lambda: cmd_run(paths, a.factor),
        'audit': lambda: cmd_audit(paths, a.factor),
        'audit-all': lambda: cmd_audit_all(paths),
        'compare-s2': lambda: cmd_compare_s2(paths),
        'report': lambda: cmd_report(paths),
    }[a.action]()


if __name__ == '__main__':
    main()
