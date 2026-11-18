#!/usr/bin/env python3
"""SchemaLens Rev3 Fase 2 - measurement engine for campaigns E1, E3, E4 and E6.

Protocol: experiments/rev3_fase2/PROTOCOLO_FASE2.md (frozen before any DDL).

    python fase2_core.py materialize --campaign C --dataset D --confirm CREATE_NEW_F2_KEYSPACES
    python fase2_core.py flush       --campaign C --dataset D --confirm FLUSH_F2_SST_ONLY
    python fase2_core.py validate    --campaign C --dataset D
    python fase2_core.py preregister --campaign C
    python fase2_core.py run         --campaign C --dataset D
    python fase2_core.py audit       --campaign C --dataset D

Safety (enforced in code): only new keyspaces sl_f2_*; no destructive CQL; nodetool
restricted to a whitelist (flush only on sl_f2_*_sst_*); evidence files opened with 'x'.
Reuses the frozen-B runtime through s3_physical.py, which is imported but never modified.
"""
from __future__ import annotations

import argparse
import copy
import json
import math
import random
import re
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

import s3_physical as S3

PROJECT = Path(__file__).resolve().parent
F2 = PROJECT / 'experiments' / 'rev3_fase2'
PROTOCOL = F2 / 'PROTOCOLO_FASE2.md'
HERE = Path(__file__).resolve()
BASE, U, D = S3.BASE, S3.U, S3.D
LABELS, SUFFIX, TABLES, SHORT = S3.LABELS, S3.SUFFIX, S3.TABLES, S3.SHORT
DAY = 86_400_000
WARMUPS, PAGE_SIZE = 5, S3.PAGE_SIZE
KS_RE = re.compile(r'sl_f2_[a-z0-9]+(?:_[a-z0-9]+){3}')
CONFIRM_CREATE, CONFIRM_FLUSH = 'CREATE_NEW_F2_KEYSPACES', 'FLUSH_F2_SST_ONLY'
TEMPORAL = S3.TEMPORAL

CAMPAIGNS = {
    # e1r2 aborted before any timing: Cassandra auto-flushed 8x memtables (MEMTABLE_LIMIT). Kept as evidence.
    'e1r2': dict(stage='E1', datasets=['d1', 'd2', 'd4', 'd8'], conds=['mem', 'sst'], seed=20261211, rounds=60,
                 abandoned=True),
    # Amendment 01: per-dataset schedule (load sst + flush first, then mem; preregister, time, retire).
    'e1r2b': dict(stage='E1', datasets=['d1', 'd2', 'd4', 'd8'], conds=['mem', 'sst'], seed=20261212, rounds=60,
                  per_dataset=True),
    'e1r3': dict(stage='E1', datasets=['d1', 'd2', 'd4', 'd8'], conds=['mem', 'sst'], seed=20261223, rounds=60,
                 per_dataset=True),
    'e3': dict(stage='E3', datasets=['h1', 'h2', 'h3'], conds=['sst'], seed=20261301, rounds=60, rev4=True),
    'e4': dict(stage='E4', datasets=['n3', 'n18'], conds=['sst'], seed=20261401, rounds=60, rev4=True),
    'e6': dict(stage='E6', datasets=['h1', 'h3'], conds=['sst'], seed=20261601, rounds=30, cache=True, ks_campaign='e3'),
}
S2_FACTORS = {'d1': 1, 'd2': 2, 'd4': 4, 'd8': 8}
EXTENSION_WINDOWS = (80, 330)

digest, load, write_new, text_new, csv_new, csv_read = S3.digest, S3.load, S3.write_new, S3.text_new, S3.csv_new, S3.csv_read
percentile, now_iso, log = S3.percentile, S3.now_iso, S3.log


# ---------------------------------------------------------------------------
# Names, paths, guarded external commands
# ---------------------------------------------------------------------------
def ks_name(campaign, ds, cond, label):
    ks = f'sl_f2_{campaign}_{ds}_{cond}_{SUFFIX[label]}'
    if not KS_RE.fullmatch(ks) or len(ks) > 48:
        raise ValueError('unsafe keyspace name ' + ks)
    return ks


def arms(campaign):
    c = CAMPAIGNS[campaign]
    caches = ('warm', 'cold') if c.get('cache') else (None,)
    return [(cond, label, cache) for cond in c['conds'] for label in LABELS for cache in caches]


def arm_id(arm):
    cond, label, cache = arm
    return f'{cond}:{SHORT[label]}' + (f':{cache}' if cache else '')


def cdir(campaign):
    return F2 / campaign


def ddir(campaign, ds):
    return cdir(campaign) / ds


def _docker(args, timeout=600):
    return S3._docker(args, timeout)


def nodetool(*args, retire=False):
    cmd = args[0]
    if cmd == 'flush':
        ok = (lambda t: KS_RE.fullmatch(t) and ('_sst_' in t or (retire and '_mem_' in t)))
        if not args[1:] or not all(ok(t) for t in args[1:]):
            raise PermissionError('flush only on explicit sl_f2_*_sst_* keyspaces (mem only when retiring)')
    elif cmd == 'tablestats':
        if len(args) != 2 or not KS_RE.fullmatch(args[1]):
            raise PermissionError('tablestats only on one sl_f2 keyspace')
    elif cmd == 'invalidatekeycache':
        if len(args) != 1:
            raise PermissionError('invalidatekeycache takes no arguments')
    elif cmd not in S3.NODETOOL_READONLY:
        raise PermissionError('nodetool sub-command not whitelisted: ' + cmd)
    return _docker(['exec', S3.CONTAINER, 'nodetool', *args])


def ks_files(ks):
    if not KS_RE.fullmatch(ks):
        raise PermissionError('file listing limited to sl_f2 keyspaces')
    return _docker(['exec', S3.CONTAINER, 'find', f'/var/lib/cassandra/data/{ks}', '-type', 'f',
                    '-printf', '%s\t%TY-%Tm-%Td %TH:%TM:%TS\t%p\n'])


def describe(ks):
    if not KS_RE.fullmatch(ks):
        raise PermissionError('DESCRIBE limited to sl_f2 keyspaces')
    return _docker(['exec', S3.CONTAINER, 'cqlsh', '-e', f'DESCRIBE KEYSPACE {ks}'])


EVICT_PY = r'''
import os, sys, subprocess
root = "/var/lib/cassandra/data/" + sys.argv[1]
files = [os.path.join(d, f) for d, _, fs in os.walk(root) for f in fs]
for p in files:
    fd = os.open(p, os.O_RDONLY)
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    os.close(fd)
res = tot = 0
for line in subprocess.run(["fincore", "-b", "-n", "-o", "RES,SIZE"] + files, capture_output=True, text=True).stdout.split("\n"):
    if line.strip():
        a, b = line.split()
        res += int(a); tot += int(b)
print(res, tot)
'''


def evict(ks):
    """Best-effort page-cache eviction of one keyspace's files; returns (resident, total) bytes after."""
    if not KS_RE.fullmatch(ks):
        raise PermissionError('eviction limited to sl_f2 keyspaces')
    out = _docker(['exec', S3.CONTAINER, 'python3', '-c', EVICT_PY, ks], timeout=120).split()
    return int(out[0]), int(out[1])


class Guarded:
    """CQL proxy for materialization: refuses destructive verbs and foreign keyspaces."""

    def __init__(self, session):
        self._s = session

    @staticmethod
    def check(query):
        text = query if isinstance(query, str) else getattr(query, 'query_string', None)
        if text is None:
            return
        verb = text.strip().split(None, 1)[0].upper() if text.strip() else ''
        if verb in S3.FORBIDDEN_VERBS:
            raise PermissionError('Forbidden CQL verb: ' + verb)
        m = re.match(r'\s*CREATE\s+KEYSPACE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)', text, re.I)
        if m and not KS_RE.fullmatch(m.group(1)):
            raise PermissionError('CREATE KEYSPACE outside sl_f2 namespace')

    def execute(self, q, *a, **k):
        self.check(q)
        return self._s.execute(q, *a, **k)

    def prepare(self, q, *a, **k):
        self.check(q)
        return self._s.prepare(q, *a, **k)

    def set_keyspace(self, ks):
        if not KS_RE.fullmatch(ks):
            raise PermissionError('set_keyspace outside sl_f2 namespace')
        return self._s.set_keyspace(ks)


# ---------------------------------------------------------------------------
# Datasets, cases, frozen runtime
# ---------------------------------------------------------------------------
_CTX = {}


def context():
    if not _CTX:
        paths = S3.P(PROJECT)
        s2 = S3.verify_s2_inputs(paths)
        rt = S3.frozen_runtime(paths, s2)
        _CTX.update(paths=paths, s2=s2, rt=rt)
    return _CTX


def dataset_info(ds):
    ctx = context()
    if ds in S2_FACTORS:
        f = S2_FACTORS[ds]
        return dict(id=ds, path=ctx['s2']['data'][f], sha256=S3.DATA_SHA[f], cells=54000 * f, devices=9,
                    origin='S2 synthetic densification (read-only)', history_days=20.8264, s2_factor=f)
    m = load(F2 / 'data' / ds / 'manifest.json')
    path = F2 / 'data' / ds / m['csv']
    return dict(id=ds, path=path, sha256=m['csv_sha256'], cells=m['cells'], devices=m['devices'],
                origin='new official Java generation (Fase 2)', history_days=m['history_days'], s2_factor=None)


def read_dataset(ds):
    info = dataset_info(ds)
    if digest(info['path']) != info['sha256']:
        raise ValueError(f'dataset {ds} hash mismatch')
    from iot_pipeline.shared import read_rows
    rows = read_rows(info['path'])
    if len(rows) != info['cells']:
        raise ValueError(f'{ds}: {len(rows)} rows, expected {info["cells"]}')
    sensors = sorted({r['sensor_name'] for r in rows})
    if len(sensors) != 2 or len({r['device_id'] for r in rows}) != info['devices']:
        raise ValueError(f'{ds}: unexpected sensors/devices')
    return info, rows, sensors


def cases_for(ds):
    """S1's 18 cases plus deterministic extensions (window, device remap) recorded in each case."""
    info = dataset_info(ds)
    base = copy.deepcopy(context()['s2']['cases'])
    n = info['devices']
    out = []
    for q in base:
        if n < 9:
            mapped = [f'd_{int(d.split("_")[1]) % n}' for d in q['devices']]
            if len(set(mapped)) != len(mapped):
                raise ValueError('device remap collapsed a multi-device case')
            if mapped != q['devices']:
                q['devices'] = mapped
                q['f2_device_remap'] = f'd_k -> d_(k mod {n})'
        out.append(q)
    for w in EXTENSION_WINDOWS:
        if w <= info['history_days']:
            for q in [c for c in out if c['s1_window_days'] == 21]:
                e = copy.deepcopy(q)
                e['end_ms_exclusive'] = e['start_ms'] + w * DAY
                e['s1_window_days'] = w
                e['s1_case_id'] = f"{e['query_id']}_W{w}"
                e['parameter_origin_f2'] = f'DETERMINISTIC_WINDOW_EXTENSION_OF_W21_TO_{w}_DAYS'
                out.append(e)
    if len({c['s1_case_id'] for c in out}) != len(out):
        raise ValueError('duplicate case ids')
    return out


def connect():
    return S3.connect()


def existing(session):
    return S3.existing_keyspaces(session)


def make_executor(session, sensors, days, oracle):
    return S3.make_executor(context()['rt'], session, sensors, days, oracle)


# ---------------------------------------------------------------------------
# Storage snapshots and gates
# ---------------------------------------------------------------------------
def gate(cond, m):
    if not m:
        return ['table missing']
    if cond == 'sst':
        return S3.gate_table(m)
    p = []
    if (m.get('sstable_count') or 0) != 0:
        p.append(f"SSTable count = {m.get('sstable_count')} (memtable condition requires 0)")
    if not (m.get('memtable_cells') or 0) > 0:
        p.append('memtable empty')
    return p


def snapshot(ks_campaign, out_campaign, ds, tag, require_gate, conds):
    folder = ddir(out_campaign, ds) / f'storage_{tag}'
    folder.mkdir(parents=True, exist_ok=False)
    summary = dict(campaign=out_campaign, dataset=ds, tag=tag, captured_utc=now_iso(), variants={})
    ok = True
    for cond in conds:
        for label in LABELS:
            ks = ks_name(ks_campaign, ds, cond, label)
            raw = nodetool('tablestats', ks)
            text_new(folder / f'{cond}_{SUFFIX[label]}_tablestats.txt', raw)
            files = ks_files(ks)
            text_new(folder / f'{cond}_{SUFFIX[label]}_files.tsv', files)
            m = S3.parse_tablestats(raw).get(TABLES[label], {})
            data_files = [ln for ln in files.splitlines() if ln.endswith('-Data.db')]
            m['data_db_files'] = len(data_files)
            m['data_db_bytes'] = sum(int(ln.split('\t')[0]) for ln in data_files)
            problems = gate(cond, m)
            ok &= not problems
            summary['variants'][f'{cond}:{SHORT[label]}'] = dict(keyspace=ks, cond=cond, label=label, metrics=m,
                                                               gate='PASS' if not problems else 'FAIL', problems=problems)
            log(f"  [{tag}] {ks:<26} sst={m.get('sstable_count')} mem_cells={m.get('memtable_cells')} "
                f"part_mean={m.get('partition_mean_bytes')} gate={'PASS' if not problems else problems}")
    pending, active, ctext = S3.compaction_state()
    text_new(folder / 'compactionstats.txt', ctext)
    text_new(folder / 'nodetool_info.txt', nodetool('info'))
    text_new(folder / 'docker_stats.jsonl', S3.docker_stats())
    summary.update(gate='PASS' if ok else 'FAIL', compaction_pending=pending, compaction_active=active)
    write_new(folder / 'summary.json', summary)
    if require_gate and not ok:
        raise AssertionError(f'storage gate FAILED: {out_campaign}/{ds}/{tag}')
    return summary


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def protocol_ok():
    draft = cdir('_protocol') / 'protocol_freeze.json'
    if not draft.exists():
        draft.parent.mkdir(parents=True, exist_ok=True)
        write_new(draft, dict(status='FASE2_PROTOCOL_FROZEN_BEFORE_DDL', created_utc=now_iso(),
                              protocol_sha256=digest(PROTOCOL), campaigns=CAMPAIGNS))
        log('Protocol frozen:', draft)
    if load(draft)['protocol_sha256'] != digest(PROTOCOL):
        raise ValueError('PROTOCOLO_FASE2.md changed after freeze')


def cmd_materialize(campaign, ds, confirm):
    if confirm != CONFIRM_CREATE:
        raise PermissionError('--confirm CREATE_NEW_F2_KEYSPACES required')
    protocol_ok()
    c = CAMPAIGNS[campaign]
    if c.get('abandoned'):
        raise ValueError(campaign + ' is abandoned')
    if c.get('ks_campaign'):
        raise ValueError(f'{campaign} reuses keyspaces of {c["ks_campaign"]}; nothing to materialize')
    out = ddir(campaign, ds) / 'materialization'
    if out.exists():
        raise FileExistsError(f'{out} exists (use a new campaign name)')
    ctx = context()
    rt = ctx['rt']
    from iot_pipeline.stage9 import _materialize
    from iot_pipeline.reference_baseline import table_ddl
    from iot_pipeline.shared import require_policy
    from v4_rev2.runtime_helpers import load_wide
    info, rows, sensors = read_dataset(ds)
    policy = require_policy()
    # Amendment 01: sst keyspaces first, each flushed right after loading, then mem keyspaces.
    order = [x for x in ('sst', 'mem') if x in c['conds']]
    wanted = {(cond, label): ks_name(campaign, ds, cond, label) for cond in order for label in LABELS}
    cluster, raw = connect()
    session = Guarded(raw)
    created, loads = [], []
    try:
        clash = [k for k in wanted.values() if k in existing(raw)]
        if clash:
            raise FileExistsError(f'keyspaces already exist: {clash}')
        out.mkdir(parents=True, exist_ok=False)
        write_new(out / 'pre_create_manifest.json', dict(campaign=campaign, dataset=ds, dataset_sha256=info['sha256'],
                                                         new_keyspaces={f'{k[0]}:{SHORT[k[1]]}': v for k, v in wanted.items()},
                                                         captured_utc=now_iso()))
        for (cond, label), ks in wanted.items():
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
            if writes != info['cells'] // 2:
                raise AssertionError(f'{ks}: {writes} writes, expected {info["cells"] // 2}')
            line = dict(cond=cond, variant=label, keyspace=ks, writes=writes,
                        seconds=round(time.perf_counter() - t0, 3))
            loads.append(line)
            text_new(out / f'describe_{ks}.cql', describe(ks))
            if cond == 'sst':
                nodetool('flush', ks)
            log('LOADED', ks, writes, 'rows in', line['seconds'], 's', '(flushed)' if cond == 'sst' else '')
        write_new(out / 'materialization_manifest.json', dict(
            status='F2_MATERIALIZED', campaign=campaign, dataset=ds, completed_utc=now_iso(),
            dataset_sha256=info['sha256'], keyspaces=[l['keyspace'] for l in loads], loads=loads,
            frozen_B_sha256=S3.B_SHA))
    except BaseException as exc:
        if out.exists() and not (out / 'failure_manifest.json').exists():
            write_new(out / 'failure_manifest.json', dict(status='FAILED_PRESERVE', created=created,
                                                          error=f'{type(exc).__name__}: {exc}'))
        raise
    finally:
        cluster.shutdown()


def cmd_flush(campaign, ds, confirm):
    if confirm != CONFIRM_FLUSH:
        raise PermissionError('--confirm FLUSH_F2_SST_ONLY required')
    c = CAMPAIGNS[campaign]
    fd = ddir(campaign, ds)
    if load(fd / 'materialization' / 'materialization_manifest.json')['status'] != 'F2_MATERIALIZED':
        raise ValueError('not materialized')
    if (fd / 'flush_manifest.json').exists():
        raise FileExistsError('flush already recorded')
    flushed = []
    if 'sst' in c['conds']:
        for label in LABELS:
            ks = ks_name(campaign, ds, 'sst', label)
            nodetool('flush', ks)
            flushed.append(ks)
            log('FLUSHED', ks)
    quiet = S3.wait_quiescent()
    snap = snapshot(campaign, campaign, ds, 'post_flush', False, c['conds'])
    status = 'PASS' if snap['gate'] == 'PASS' and quiet['status'] == 'QUIESCENT' else 'FAIL'
    write_new(fd / 'flush_manifest.json', dict(status=status, flushed=flushed, quiescence=quiet,
                                               storage_gate=snap['gate'], manual_compaction=False))
    if status != 'PASS':
        raise AssertionError(f'flush/gate FAILED for {campaign}/{ds}')
    log(f'PASS flush+gate {campaign}/{ds}')


def oracle_bundle(ds):
    context()  # put the frozen-B experiment first on sys.path before importing iot_pipeline
    from iot_pipeline.native_reference_v3 import source_oracle
    from iot_pipeline.offline import compare
    info, rows, sensors = read_dataset(ds)
    days = {r['timestamp'] // DAY for r in rows}
    oracle = source_oracle()
    cases = cases_for(ds)
    expected = {q['s1_case_id']: oracle(rows, q) for q in cases}
    return info, sensors, days, oracle, compare, cases, expected


def cmd_validate(campaign, ds):
    c = CAMPAIGNS[campaign]
    fd = ddir(campaign, ds)
    out = fd / 'semantic_validation.json'
    if out.exists():
        raise FileExistsError(out)
    if load(fd / 'flush_manifest.json')['status'] != 'PASS':
        raise ValueError('flush/gate not passed')
    info, sensors, days, oracle, compare, cases, expected = oracle_bundle(ds)
    s2_trace = None
    if info['s2_factor']:
        s2v = load(context()['paths'].s2 / 's2_materialized' / f"{info['s2_factor']}x" / 'semantic_validation.json')
        s2_trace = {(r['case_id'], r['variant']): (r['execute_calls'], r['iterated_rows'], r['plan']) for r in s2v['results']}
    results = []
    cluster, session = connect()
    try:
        execute = make_executor(session, sensors, days, oracle)
        for q in cases:
            case = q['s1_case_id']
            per_label = {}
            for cond in c['conds']:
                for label in LABELS:
                    session.set_keyspace(ks_name(campaign, ds, cond, label))
                    got, tr = execute(q, label)
                    if not compare(got, expected[case]):
                        raise AssertionError(f'semantic mismatch {case} {cond} {label}')
                    t = (tr['execute_calls'], tr['iterated_rows'], tr['physical_plan'])
                    if per_label.setdefault(label, t) != t:
                        raise AssertionError(f'trace differs between conditions {case} {label}')
                    if s2_trace is not None and t != s2_trace[case, label]:
                        raise AssertionError(f'trace differs from S2 {case} {label}')
                    results.append(dict(case_id=case, query_id=q['query_id'], window_days=q['s1_window_days'],
                                        cond=cond, variant=label, status='PASS', execute_calls=t[0],
                                        iterated_rows=t[1], plan=t[2], oracle_cardinality=len(expected[case])))
            log('PASS', case, f'{len(c["conds"]) * 3} arms')
    finally:
        cluster.shutdown()
    write_new(out, dict(status='PASS_ALL', campaign=campaign, dataset=ds, validations=len(results),
                        cases=len(cases), dataset_sha256=info['sha256'], trace_identical_to_s2=bool(s2_trace),
                        cases_sha256=S3.hashlib.sha256(json.dumps(cases, sort_keys=True).encode()).hexdigest(),
                        results=results))
    log(f'PASS ALL {campaign}/{ds}: {len(results)} validations')


def prereg_path(campaign, ds):
    c = CAMPAIGNS[campaign]
    return cdir(campaign) / (f'preregistration_{ds}.json' if c.get('per_dataset') else 'preregistration.json')


def prereg_inputs(campaign, only=None):
    c = CAMPAIGNS[campaign]
    src = c.get('ks_campaign', campaign)
    out = {}
    for ds in ([only] if only else c['datasets']):
        fd = ddir(src, ds)
        out[ds] = dict(dataset_sha256=dataset_info(ds)['sha256'],
                       materialization=digest(fd / 'materialization' / 'materialization_manifest.json'),
                       flush=digest(fd / 'flush_manifest.json'),
                       storage_post_flush=digest(fd / 'storage_post_flush' / 'summary.json'),
                       validation=digest(fd / 'semantic_validation.json'))
    return out


def cmd_preregister(campaign, only=None):
    protocol_ok()
    c = CAMPAIGNS[campaign]
    if c.get('abandoned'):
        raise ValueError(campaign + ' is abandoned')
    if bool(c.get('per_dataset')) != bool(only):
        raise ValueError('per-dataset campaigns need --dataset; others must not pass it')
    target = prereg_path(campaign, only)
    if target.exists():
        raise FileExistsError(target)
    src = c.get('ks_campaign', campaign)
    for ds in ([only] if only else c['datasets']):
        fd = ddir(src, ds)
        if load(fd / 'flush_manifest.json')['status'] != 'PASS' or \
                load(fd / 'semantic_validation.json')['status'] != 'PASS_ALL':
            raise ValueError(f'{src}/{ds} not ready')
        if (ddir(campaign, ds) / 'benchmark').exists():
            raise FileExistsError(f'benchmark evidence already exists for {campaign}/{ds}')
    rev4 = None
    if c.get('rev4'):
        frozen = F2 / 'rev4' / 'rev4_frozen.json'
        pred = F2 / 'rev4' / f'predictions_{campaign}.json'
        if not frozen.exists() or not pred.exists():
            raise FileNotFoundError('Rev4 must be frozen and predictions written before preregistering ' + campaign)
        rev4 = dict(frozen_sha256=digest(frozen), predictions_sha256=digest(pred))
    write_new(target, dict(
        status='F2_PREREGISTERED_BEFORE_TIMING', campaign=campaign, created_utc=now_iso(), spec=c,
        arms=[arm_id(a) for a in arms(campaign)], core_sha256=digest(HERE), s3_runner_sha256=digest(S3.HERE),
        protocol_sha256=digest(PROTOCOL), frozen_B_sha256=S3.B_SHA, inputs=prereg_inputs(campaign, only), rev4=rev4,
        amendments={p.name: digest(p) for p in sorted(F2.glob('EMENDA_*.md'))}, dataset=only,
        randomization='per round: shuffle cases; per case shuffle arms, reverse on even rounds; Random(seed + dataset index)',
        warmups=WARMUPS, page_size=PAGE_SIZE, percentile='nearest_rank'))
    log('PASS preregistration', target)


def verified_prereg(campaign, ds):
    p = load(prereg_path(campaign, ds))
    if p['core_sha256'] != digest(HERE) or p['protocol_sha256'] != digest(PROTOCOL):
        raise ValueError('code/protocol changed after preregistration')
    if p['inputs'] != prereg_inputs(campaign, p.get('dataset')):
        raise ValueError('inputs changed after preregistration')
    if p['rev4']:
        if p['rev4']['predictions_sha256'] != digest(F2 / 'rev4' / f'predictions_{campaign}.json'):
            raise ValueError('Rev4 predictions changed after preregistration')
    return p


def cmd_run(campaign, ds):
    c = CAMPAIGNS[campaign]
    verified_prereg(campaign, ds)
    idx = c['datasets'].index(ds)
    for earlier in c['datasets'][:idx]:
        if not (ddir(campaign, earlier) / 'benchmark' / 'manifest.json').exists():
            raise ValueError(f'order violated: {earlier} before {ds}')
    src = c.get('ks_campaign', campaign)
    fd = ddir(campaign, ds)
    dest = fd / 'benchmark'
    if dest.exists():
        raise FileExistsError(dest)
    info, sensors, days, oracle, compare, cases, expected = oracle_bundle(ds)
    val = load(ddir(src, ds) / 'semantic_validation.json')
    recorded = {(r['case_id'], r['variant']): (r['execute_calls'], r['iterated_rows'], r['plan']) for r in val['results']}
    arm_list = arms(campaign)
    rounds = c['rounds']
    fd.mkdir(parents=True, exist_ok=True)
    pre = snapshot(src, campaign, ds, 'pre_timing', True, c['conds'])
    raw, orders = [], []
    started = time.time()
    dest.mkdir(parents=True, exist_ok=False)
    cluster, session = connect()
    try:
        execute = make_executor(session, sensors, days, oracle)

        def once(q, arm):
            cond, label, _ = arm
            session.set_keyspace(ks_name(src, ds, cond, label))
            return execute(q, label)
        for q in cases:
            for arm in arm_list:
                got, tr = once(q, arm)
                if not compare(got, expected[q['s1_case_id']]):
                    raise AssertionError('pretiming mismatch')
        for q in cases:
            for arm in arm_list:
                for _ in range(WARMUPS):
                    once(q, arm)
        log(f'{campaign}/{ds}: pretiming oracles + warm-ups done; timing {len(cases)} cases x {len(arm_list)} arms x {rounds}')
        rng = random.Random(c['seed'] + idx)
        for rep in range(1, rounds + 1):
            queue = list(cases)
            rng.shuffle(queue)
            for q in queue:
                case = q['s1_case_id']
                order = list(arm_list)
                rng.shuffle(order)
                if rep % 2 == 0:
                    order.reverse()
                orders.append(dict(dataset=ds, case_id=case, rep=rep, arms_order='|'.join(arm_id(a) for a in order)))
                for pos, arm in enumerate(order, 1):
                    cond, label, cache = arm
                    resident = total = ''
                    if cache == 'cold':
                        nodetool('invalidatekeycache')
                        resident, total = evict(ks_name(src, ds, cond, label))
                    session.set_keyspace(ks_name(src, ds, cond, label))
                    t0 = time.perf_counter_ns()
                    got, tr = execute(q, label)
                    ms = (time.perf_counter_ns() - t0) / 1e6
                    if not compare(got, expected[case]):
                        raise AssertionError(f'timed mismatch {case} {arm_id(arm)} {rep}')
                    if (tr['execute_calls'], tr['iterated_rows'], tr['physical_plan']) != recorded[case, label]:
                        raise AssertionError(f'timed trace changed {case} {arm_id(arm)} {rep}')
                    raw.append(dict(campaign=campaign, dataset=ds, case_id=case, query_id=q['query_id'],
                                    window_days=q['s1_window_days'] if q['s1_window_days'] is not None else 'CONTROL',
                                    arm=arm_id(arm), cond=cond, variant=label, cache=cache or '', rep=rep, position=pos,
                                    latency_ms=round(ms, 9), execute_calls=tr['execute_calls'],
                                    iterated_rows=tr['iterated_rows'], resident_bytes_before=resident,
                                    file_bytes=total, correctness='PASS'))
            if rep % 5 == 0:
                log(f'{campaign}/{ds} round {rep}/{rounds} observations={len(raw)}')
    except BaseException as exc:
        write_new(dest / 'failure_manifest.json', dict(status='FAILED_PRESERVE', error=f'{type(exc).__name__}: {exc}',
                                                       partial=len(raw), utc=now_iso()))
        if raw:
            csv_new(dest / 'partial_latencies.csv', raw)
        raise
    finally:
        cluster.shutdown()
    ended = time.time()
    summ = summarize(raw, len(cases), len(arm_list), rounds)
    csv_new(dest / 'raw_latencies.csv', raw)
    csv_new(dest / 'orders.csv', orders)
    csv_new(dest / 'query_p95.csv', summ)
    write_new(dest / 'manifest.json', dict(
        status='F2_BENCHMARK_COMPLETE', campaign=campaign, dataset=ds, observations=len(raw),
        expected=len(cases) * len(arm_list) * rounds, cases=len(cases), arms=[arm_id(a) for a in arm_list],
        rounds=rounds, preregistration_sha256=digest(prereg_path(campaign, ds)),
        dataset_sha256=info['sha256'], started_epoch=started, ended_epoch=ended, pre_timing_gate=pre['gate'],
        files_sha256={n: digest(dest / n) for n in ('raw_latencies.csv', 'orders.csv', 'query_p95.csv')}))
    post = snapshot(src, campaign, ds, 'post_timing', False, c['conds'])
    drift = {}
    for k, v in pre['variants'].items():
        a, b = v['metrics'], post['variants'][k]['metrics']
        d = {m: [a.get(m), b.get(m)] for m in ('sstable_count', 'memtable_cells', 'data_db_files') if a.get(m) != b.get(m)}
        if d:
            drift[k] = d
    write_new(fd / 'post_timing_manifest.json', dict(post_gate=post['gate'], drift=drift,
                                                     physical_state_unchanged=not drift))
    log(f'COMPLETE {campaign}/{ds}: {len(raw)} observations in {ended - started:.0f}s; state unchanged={not drift} {drift or ""}')


def summarize(raw, n_cases, n_arms, rounds):
    groups = defaultdict(list)
    for r in raw:
        groups[r['case_id'], r['arm']].append(r)
    if len(groups) != n_cases * n_arms:
        raise ValueError('incomplete groups')
    out = []
    for (case, arm), rr in sorted(groups.items()):
        if len(rr) != rounds:
            raise ValueError('incomplete group ' + case + arm)
        a = [float(x['latency_ms']) for x in rr]
        e = rr[0]
        res = [int(x['resident_bytes_before']) for x in rr if str(x['resident_bytes_before']) != '']
        out.append(dict(dataset=e['dataset'], case_id=case, query_id=e['query_id'], window_days=e['window_days'],
                        arm=arm, cond=e['cond'], variant=e['variant'], cache=e['cache'], runs=len(a),
                        mean_ms=round(statistics.mean(a), 6), median_ms=round(statistics.median(a), 6),
                        p95_ms=round(percentile(a, 95), 6), p99_ms=round(percentile(a, 99), 6),
                        execute_calls=e['execute_calls'], iterated_rows=e['iterated_rows'],
                        median_resident_bytes=statistics.median(res) if res else '',
                        file_bytes=e['file_bytes']))
    return out


def cmd_audit(campaign, ds):
    c = CAMPAIGNS[campaign]
    dest = ddir(campaign, ds) / 'benchmark'
    m = load(dest / 'manifest.json')
    if m['status'] != 'F2_BENCHMARK_COMPLETE' or m['pre_timing_gate'] != 'PASS':
        raise ValueError('invalid manifest')
    for n, h in m['files_sha256'].items():
        if digest(dest / n) != h:
            raise ValueError('file changed ' + n)
    raw, orders, summ = (csv_read(dest / n) for n in ('raw_latencies.csv', 'orders.csv', 'query_p95.csv'))
    n_cases, arm_ids = m['cases'], m['arms']
    if len(raw) != m['expected'] or len(orders) != n_cases * c['rounds']:
        raise ValueError('counts')
    perms, seen = {}, set()
    for o in orders:
        k = (o['case_id'], int(o['rep']))
        p = o['arms_order'].split('|')
        if k in perms or sorted(p) != sorted(arm_ids):
            raise ValueError('bad permutation')
        perms[k] = p
    for r in raw:
        k = (r['case_id'], r['arm'], int(r['rep']))
        if k in seen or r['correctness'] != 'PASS':
            raise ValueError('duplicate/invalid sample')
        seen.add(k)
        if perms[r['case_id'], int(r['rep'])][int(r['position']) - 1] != r['arm']:
            raise ValueError('position mismatch')
        if not float(r['latency_ms']) > 0:
            raise ValueError('latency')
    re_sum = summarize(raw, n_cases, len(arm_ids), c['rounds'])
    orig = {(r['case_id'], r['arm']): r for r in summ}
    for r in re_sum:
        o = orig[r['case_id'], r['arm']]
        if not math.isclose(float(r['p95_ms']), float(o['p95_ms']), rel_tol=1e-9, abs_tol=1e-6):
            raise ValueError('p95 mismatch')
    rep = dict(status='PASS', campaign=campaign, dataset=ds, samples=len(raw), orders=len(orders),
               groups=len(re_sum), recomputed='PASS')
    f = dest / 'audit.json'
    if f.exists():
        if load(f) != rep:
            raise ValueError('audit differs')
    else:
        write_new(f, rep)
    log(f'PASS audit {campaign}/{ds}: {len(raw)} samples, {len(re_sum)} groups')


def cmd_retire(campaign, ds):
    """Flush the memtable-condition keyspaces of a dataset once it is measured (or its campaign abandoned),
    so their memtables cannot push later conditions over MEMTABLE_LIMIT. Data are kept, never dropped."""
    c = CAMPAIGNS[campaign]
    fd = ddir(campaign, ds)
    target = fd / 'retire_manifest.json'
    if target.exists():
        raise FileExistsError(target)
    if not c.get('abandoned') and not (fd / 'benchmark' / 'audit.json').exists():
        raise ValueError('retire only after the audited benchmark (or for an abandoned campaign)')
    done = []
    if 'mem' in c['conds']:
        for label in LABELS:
            ks = ks_name(campaign, ds, 'mem', label)
            nodetool('flush', ks, retire=True)
            done.append(ks)
    write_new(target, dict(status='MEM_KEYSPACES_RETIRED_BY_FLUSH', utc=now_iso(), flushed=done,
                           reason='abandoned campaign' if c.get('abandoned') else 'measured and audited'))
    log('RETIRED', campaign, ds, done)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('action', choices=('materialize', 'flush', 'validate', 'preregister', 'run', 'audit', 'retire', 'cases'))
    ap.add_argument('--campaign', required=True, choices=sorted(CAMPAIGNS))
    ap.add_argument('--dataset')
    ap.add_argument('--confirm', default='')
    a = ap.parse_args()
    if (a.action != 'preregister' or a.dataset) and a.dataset not in CAMPAIGNS[a.campaign]['datasets']:
        ap.error('--dataset must be one of ' + ', '.join(CAMPAIGNS[a.campaign]['datasets']))
    {'materialize': lambda: cmd_materialize(a.campaign, a.dataset, a.confirm),
     'flush': lambda: cmd_flush(a.campaign, a.dataset, a.confirm),
     'validate': lambda: cmd_validate(a.campaign, a.dataset),
     'preregister': lambda: cmd_preregister(a.campaign, a.dataset),
     'retire': lambda: cmd_retire(a.campaign, a.dataset),
     'run': lambda: cmd_run(a.campaign, a.dataset),
     'audit': lambda: cmd_audit(a.campaign, a.dataset),
     'cases': lambda: print(json.dumps([(q['s1_case_id'], q['devices'], q['s1_window_days']) for q in cases_for(a.dataset)])),
     }[a.action]()


if __name__ == '__main__':
    main()
