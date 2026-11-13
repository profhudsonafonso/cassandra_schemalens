"""Fase 8 — E23: full scale (2M items / 20M bids) for the 2 best schemas of H, NoSE and QPG, built from
rubis_full by streaming, verified, then measured COLD (drain + container restart + page-cache drop before every
repetition, same procedure as V8-012 full_cold), interleaved in one window.
"""
from __future__ import annotations

import gzip
import itertools
import json
import statistics
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from . import isolation
from . import phase5 as M
from . import phase5_run as PR
from . import phase6 as B
from .common import CONTAINER, OLD_RUN_012, RUN, now_iso, read_json, write_json
from .phase2 import colname, ddl, fk_target, resolve
from .schema_map import COLUMN_TO_ATTR, FK
from .stages import incident, load_state, save_state, set_stage, stage_dir

OUT = RUN / "stages/E23_full_scale_acceptance"
W = B.WORKLOADS
IMAGE = "cassandra:3.11.11"
COLD_MAX_KIB = 400_000
LO, HI = -(2 ** 63), 2 ** 63 - 1


# ---------------------------------------------------------------------------------------------- helpers
def session(timeout=600, reconnect=False):
    from cassandra.cluster import Cluster
    from cassandra.policies import ConstantReconnectionPolicy
    from cassandra.query import dict_factory
    c = Cluster(["127.0.0.1"], reconnection_policy=ConstantReconnectionPolicy(1.0)) if reconnect else Cluster(["127.0.0.1"])
    s = c.connect()
    s.row_factory = dict_factory
    s.default_timeout = timeout
    return c, s


def slices(n):
    step = (HI - LO) // n
    return [(LO + i * step, (LO + (i + 1) * step) if i < n - 1 else HI, i == 0) for i in range(n)]


def scan(s, table, cols, pk="id", n=256, workers=6, progress=None):
    """Yields pages (list of tuples) of rubis_full.<table> by token ranges (thread pool)."""
    from cassandra.query import SimpleStatement, tuple_factory
    out_lock = threading.Lock()
    done = [0]
    sl = slices(n)

    def work(b):
        a, z, first = b
        st = SimpleStatement(f"SELECT {', '.join(cols)} FROM rubis_full.{table} WHERE token({pk}) {'>=' if first else '>'} %s AND token({pk}) <= %s", fetch_size=5000)
        rs = s.execute(st, (a, z), timeout=600)
        rows = [tuple(r[c] for c in cols) for r in rs]
        with out_lock:
            done[0] += 1
            if progress:
                progress(done[0], n)
        return rows
    with ThreadPoolExecutor(workers) as ex:
        for rows in ex.map(work, sl):
            yield rows


def raw_columns(s, table):
    return [r["column_name"] for r in s.execute("SELECT column_name FROM system_schema.columns WHERE keyspace_name='rubis_full' AND table_name=%s", (table,))]


class Parents:
    """Compact in-memory parent tables (tuples + column index) for join lookups."""

    def __init__(self, s, needed: dict[str, set[str]]):
        self.idx, self.rows = {}, {}
        for ent, cols in needed.items():
            cols = ["id"] + sorted(set(cols) - {"id"})
            self.idx[ent] = {c: i for i, c in enumerate(cols)}
            d = {}
            for page in scan(s, ent, cols, n=64):
                for r in page:
                    d[r[0]] = r
            self.rows[ent] = d

    def get(self, ent, ident):
        if ident is None or ent not in self.rows:
            return None
        r = self.rows[ent].get(int(ident))
        if r is None:
            return None
        ix = self.idx[ent]
        return _Row(r, ix)


class _Row:
    __slots__ = ("r", "ix")

    def __init__(self, r, ix):
        self.r, self.ix = r, ix

    def __getitem__(self, k):
        return self.r[self.ix[k]]

    def get(self, k, default=None):
        i = self.ix.get(k)
        return self.r[i] if i is not None else default


def insert_pages(s, stmt, rows):
    from cassandra.concurrent import execute_concurrent_with_args
    res = execute_concurrent_with_args(s, stmt, rows, concurrency=128, raise_on_first_error=False)
    errs = [str(v) for ok, v in res if not ok]
    if errs:
        raise RuntimeError(f"insert errors: {errs[:3]}")
    return len(rows)


def count_table(s, ks, table, pk_cols, n=512):
    from cassandra.concurrent import execute_concurrent_with_args
    pk = ", ".join(pk_cols)
    q0 = s.prepare(f"SELECT count(*) AS n FROM {ks}.{table} WHERE token({pk}) >= ? AND token({pk}) <= ?")
    q1 = s.prepare(f"SELECT count(*) AS n FROM {ks}.{table} WHERE token({pk}) > ? AND token({pk}) <= ?")
    total = 0
    for a, z, first in slices(n):
        total += s.execute(q0 if first else q1, (a, z), timeout=600).one()["n"]
    return total


# ---------------------------------------------------------------------------------------------- builders
def build_generated(s, ks, tables: list[dict], cp_path, label):
    cp = read_json(cp_path, {"tables": {}})
    if not cp.get("created"):
        M.create_keyspace(s, ks)
        for t in tables:
            s.execute(ddl(t, t["name"], ks) + (" AND" if t["clustering"] else " WITH") + " caching={'keys':'NONE','rows_per_partition':'NONE'}")
        cp = {"tables": {}, "created": now_iso()}
        write_json(cp_path, cp)
    need = {}
    for t in tables:
        for r in t["partition"] + [x for x, _ in t["clustering"]] + t["columns"]:
            e2, a2, hops = resolve(t["row_entity"], r)
            if hops:
                need.setdefault(e2, set()).add(a2)
                need.setdefault(e2, set()).add("id")
    src_need = {}
    for e2, attrs in need.items():
        inv = {v: k for k, v in COLUMN_TO_ATTR[e2].items()}
        src_need[e2] = {inv[a] for a in attrs}
    parents = Parents(s, src_need)
    by_ent = {}
    for t in tables:
        by_ent.setdefault(t["row_entity"], []).append(t)
    for ent, tabs in by_ent.items():
        todo = [t for t in tabs if cp["tables"].get(t["name"], {}).get("status") != "DONE"]
        if not todo:
            continue
        for t in todo:
            # a partially written table is truncated and rebuilt (idempotent restart)
            s.execute(f"TRUNCATE {ks}.{t['name']}", timeout=600)
        cols = raw_columns(s, ent)
        c2a = COLUMN_TO_ATTR[ent]
        stmts, refs = {}, {}
        for t in todo:
            refs[t["name"]] = t["partition"] + [x for x, _ in t["clustering"]] + t["columns"]
            stmts[t["name"]] = s.prepare(f"INSERT INTO {ks}.{t['name']} ({', '.join(colname(r) for r in refs[t['name']])}) VALUES ({', '.join('?' for _ in refs[t['name']])})")
        counts = {t["name"]: {"inserted": 0, "skipped_null_key": 0} for t in todo}
        n_pages = 256 if ent in ("bids", "comments") else 64

        def prog(d, n, _e=ent):
            set_stage("E23", progress={"text": f"{label}: construindo tabelas de {_e} (faixa {d} de {n})", "done": d, "total": n})
        for page in scan(s, ent, cols, n=n_pages, progress=prog):
            batch = {t["name"]: [] for t in todo}
            for raw in page:
                row = {c2a[c]: v for c, v in zip(cols, raw) if c in c2a}
                for t in todo:
                    vals, ok = [], True
                    nkeys = len(t["partition"]) + len(t["clustering"])
                    for j, r in enumerate(refs[t["name"]]):
                        e2, a2, hops = resolve(ent, r)
                        if not hops:
                            v = row.get(a2)
                        else:
                            fkv = row.get(FK[ent][hops[0][1]])
                            p = parents.get(e2, fkv)
                            v = (p["id"] if a2 == "id" else p[{v_: k for k, v_ in COLUMN_TO_ATTR[e2].items()}[a2]]) if p is not None else None
                        if j < nkeys and v is None:
                            ok = False
                            break
                        vals.append(v)
                    if ok:
                        batch[t["name"]].append(tuple(vals))
                    else:
                        counts[t["name"]]["skipped_null_key"] += 1
            for t in todo:
                counts[t["name"]]["inserted"] += insert_pages(s, stmts[t["name"]], batch[t["name"]])
        for t in todo:
            cp["tables"][t["name"]] = {"status": "DONE", **counts[t["name"]], "finished_at": now_iso()}
            write_json(cp_path, cp)
    return cp


def build_baseline(s, ks, which, cp_path, label):
    from reused.methodology import baselines_005 as Bm
    man = Bm.manifests()[which]
    cp = read_json(cp_path, {"tables": {}})
    if not cp.get("created"):
        M.create_keyspace(s, ks)
        for t in man["tables"]:
            cols = ",".join(k + " " + typ for k, typ in t["columns"].items())
            s.execute(f"CREATE TABLE {ks}.{t['name']} ({cols}, PRIMARY KEY {t['primary_key']}) WITH caching={{'keys':'NONE','rows_per_partition':'NONE'}}")
        cp = {"tables": {}, "created": now_iso(), "manifest_sha256": man["manifest_sha256"]}
        write_json(cp_path, cp)
    need = {}
    for t in man["tables"]:
        for child, fk, parent in t["joins"]:
            need.setdefault(parent, set())
        for col, ef in t["fields"].items():
            e, f = ef.split(".")
            need.setdefault(e, set()).add(Bm.ALIASES.get(e + "." + f, f))
        for child, fk, parent in t["joins"]:
            need.setdefault(child, set()).add(fk)
    parents = Parents(s, {e: (set(raw_columns(s, e)) & cols) | {"id"} for e, cols in need.items() if e in Bm.PARENTS})
    by_anchor = {}
    for t in man["tables"]:
        by_anchor.setdefault(t["anchor"], []).append(t)
    for ent, tabs in by_anchor.items():
        todo = [t for t in tabs if cp["tables"].get(t["name"], {}).get("status") != "DONE"]
        if not todo:
            continue
        for t in todo:
            s.execute(f"TRUNCATE {ks}.{t['name']}", timeout=600)
        cols = raw_columns(s, ent)
        stmts = {t["name"]: s.prepare(f"INSERT INTO {ks}.{t['name']} ({','.join(t['columns'])}) VALUES ({','.join('?' for _ in t['columns'])})") for t in todo}
        counts = {t["name"]: {"inserted": 0, "skipped": 0} for t in todo}
        n_pages = 256 if ent in ("bids", "comments") else 64

        def prog(d, n, _e=ent):
            set_stage("E23", progress={"text": f"{label}: construindo tabelas de {_e} (faixa {d} de {n})", "done": d, "total": n})
        lookup = lambda e, i: parents.get(e, i)
        for page in scan(s, ent, cols, n=n_pages, progress=prog):
            batch = {t["name"]: [] for t in todo}
            for raw in page:
                src = dict(zip(cols, raw))
                for t in todo:
                    try:
                        vals = Bm.project(t, src, lookup)
                    except ValueError:
                        vals = None
                    if vals is None:
                        counts[t["name"]]["skipped"] += 1
                    else:
                        batch[t["name"]].append(tuple(vals))
            for t in todo:
                counts[t["name"]]["inserted"] += insert_pages(s, stmts[t["name"]], batch[t["name"]])
        for t in todo:
            cp["tables"][t["name"]] = {"status": "DONE", **counts[t["name"]], "finished_at": now_iso()}
            write_json(cp_path, cp)
    return cp


# ---------------------------------------------------------------------------------------------- adapters
DOM_USERS = list(range(10, 40))
DOM_ITEMS = list(range(10, 40))
DOM_CATS = list(range(2, 12))
DOM_REGIONS = list(range(2, 12))
DOM_COMMENTS = list(range(10, 40))


class FullGeneratedAdapter(M.GeneratedAdapter):
    """Generated routes at full scale; reset_state restores a snapshot of every partition the benchmark domain can touch."""

    def __init__(self, s, ks, schema, tables):
        self.s, self.ks, self.tables = s, ks, tables
        self.routes = {int(k): v for k, v in schema["routes"].items()}
        self.prep, self.inserted, self.updated = {}, [], set()
        it = {r["id"]: r for r in s.execute(f"SELECT id, category_id FROM rubis_full.items WHERE id IN ({','.join(map(str, DOM_ITEMS))})")}
        us = {r["id"]: r for r in s.execute(f"SELECT id, region_id FROM rubis_full.users WHERE id IN ({','.join(map(str, DOM_USERS))})")}
        dom = {"users": DOM_USERS, "items": DOM_ITEMS, "comments": DOM_COMMENTS, "bids": [], "buynow": [],
               "categories": sorted(set(DOM_CATS) | {r["category_id"] for r in it.values()}),
               "regions": sorted(set(DOM_REGIONS) | {r["region_id"] for r in us.values()})}
        self.orig_keys, self.orig_rows, self.snap = {}, {}, {}
        for tid, t in tables.items():
            vals = []
            for r in t["partition"]:
                e, a, _ = resolve(t["row_entity"], r)
                tgt = e if a == "id" else fk_target(e, a)
                vals.append([1] if a == "dummy" else dom.get(tgt, []))
            rows = []
            q = self._p(f"SELECT * FROM {ks}.{t['name']} WHERE {' AND '.join(colname(r) + ' = ?' for r in t['partition'])}")
            for combo in itertools.product(*vals):
                rows += [self._row_refs(t, r) for r in self.s.execute(q, combo)]
            keys = t["partition"] + [r for r, _ in t["clustering"]]
            self.snap[tid] = rows
            self.orig_keys[tid] = {tuple(r[k] for k in keys) for r in rows}
            if t["row_entity"] in ("items", "users"):
                self.orig_rows[tid] = rows



def domain_fingerprint(ad: FullGeneratedAdapter) -> str:
    rows = []
    for tid, t in ad.tables.items():
        keys = {tuple(r[k] for k in t["partition"]) for r in ad.snap[tid]}
        q = ad._p(f"SELECT * FROM {ad.ks}.{t['name']} WHERE {' AND '.join(colname(r) + ' = ?' for r in t['partition'])}")
        for k in sorted(keys, key=str):
            rows += [json.dumps(ad._row_refs(t, r), sort_keys=True, default=str) for r in ad.s.execute(q, k)]
    return M.sha256_obj(sorted(rows))


# ---------------------------------------------------------------------------------------------- cold
def _cached_kib():
    p = subprocess.run(["docker", "exec", CONTAINER, "grep", "^Cached:", "/proc/meminfo"], capture_output=True, text=True)
    return int(p.stdout.split()[1]) if p.returncode == 0 and p.stdout.split() else -1


def cold_start(session) -> dict:
    t0 = time.time()
    dr = subprocess.run(["docker", "exec", CONTAINER, "nodetool", "drain"], capture_output=True, text=True, timeout=600)
    rs = subprocess.run(["docker", "restart", CONTAINER], capture_output=True, text=True, timeout=600)
    t1 = time.time()
    while time.time() - t1 < 600:
        q = subprocess.run(["docker", "exec", CONTAINER, "cqlsh", "-e", "select release_version from system.local"], capture_output=True, text=True)
        if q.returncode == 0:
            try:
                session.execute("SELECT release_version FROM system.local")
                break
            except Exception:
                pass
        time.sleep(2)
    before = _cached_kib()
    d = subprocess.run(["docker", "run", "--rm", "--privileged", "--entrypoint", "sh", IMAGE, "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"],
                       capture_output=True, text=True, timeout=120)
    after = _cached_kib()
    ok = dr.returncode == 0 and rs.returncode == 0 and d.returncode == 0 and 0 <= after < COLD_MAX_KIB
    return {"at": now_iso(), "drain_rc": dr.returncode, "restart_rc": rs.returncode, "drop_rc": d.returncode,
            "page_cache_kib_before": before, "page_cache_kib_after": after, "cycle_s": round(time.time() - t0, 1), "ok": ok}


def full_reference():
    from reused.methodology.semantic_replay_011 import hash_value  # noqa: F401
    ref = {}
    for w in W:
        p = OLD_RUN_012 / f"full_cold/raw/NoSE_{w}.jsonl.gz"
        with gzip.open(p, "rt") as fh:
            for line in fh:
                r = json.loads(line)
                for st in r["statements"]:
                    ref[(w, r["repetition"], r["logical_request_id"], st["statement_id"])] = st["canonical_hash"]
    return ref
