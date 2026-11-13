"""Fase 5: E17 (coorte e materialização), E18 (corretude), E19 (baselines).

Data source: rubis_full only.  The cohort IDENTITIES come from the V8 cohort keyspace (no ID manifest existed
in V8, see inventory 3.6) and are exported once to COHORT_MANIFEST.json; every attribute value is read from
rubis_full.  Keyspaces: rubis_v9_<schema>.
"""
from __future__ import annotations

import copy
import json
from collections import defaultdict
from datetime import datetime
from typing import Any

from . import oracle_v9 as O
from .common import RUN, V9, now_iso, read_json, sha256_obj, write_json
from .contract import CONTRACT, READ_IDS, WRITE_IDS
from .phase2 import colname, coltype, ddl, needed_refs, resolve
from .schema_map import FK, REL, SCHEMA, from_full_row
from .stages import set_stage, stage_dir

ENTITIES = ("categories", "regions", "users", "items", "bids", "comments", "buynow")
KS_PREFIX = "rubis_v9_"


def session(timeout=300):
    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    c = Cluster(["127.0.0.1"])
    s = c.connect()
    s.row_factory = dict_factory
    s.default_timeout = timeout
    return c, s


# ------------------------------------------------------------------------------------ cohort state
def load_raw_state(s) -> tuple[dict, dict]:
    """rubis_full rows (source column names) for the cohort identities + referenced users (closure)."""
    from cassandra.concurrent import execute_concurrent_with_args
    ids = {}
    for ent in ("items", "users", "bids", "comments", "buynow"):
        ids[ent] = sorted(int(r["id"]) for r in s.execute(f"SELECT id FROM {O.COHORT_KEYSPACE}.{ent}"))
    raw = {e: {} for e in ENTITIES}
    for e in ("categories", "regions"):
        for r in s.execute(f"SELECT * FROM rubis_full.{e}"):
            raw[e][r["id"]] = dict(r)

    def fetch(e, wanted):
        stmt = s.prepare(f"SELECT * FROM rubis_full.{e} WHERE id=?")
        todo = [i for i in wanted if i not in raw[e]]
        for ok, res in execute_concurrent_with_args(s, stmt, [(i,) for i in todo], concurrency=64):
            row = res.one() if ok else None
            if row:
                raw[e][row["id"]] = dict(row)
    for e in ("items", "bids", "comments", "buynow", "users"):
        fetch(e, ids[e])
    ref = set()
    for e, fks in (("items", ["seller_id"]), ("bids", ["user_id"]), ("comments", ["from_user_id", "to_user_id"]), ("buynow", ["buyer_id"])):
        for r in raw[e].values():
            ref.update(int(r[k]) for k in fks if r.get(k) is not None)
    fetch("users", sorted(ref))
    return raw, ids


def model_state(raw: dict) -> dict:
    return {e: {i: from_full_row(e, r) for i, r in rows.items()} for e, rows in raw.items()}


def cohort_manifest(raw: dict, ids: dict) -> dict:
    return {"source": "rubis_full", "identities_from": O.COHORT_KEYSPACE, "exported_at": now_iso(),
            "cohort_ids": ids, "cohort_counts": {k: len(v) for k, v in ids.items()},
            "closure_users": sorted(set(raw["users"]) - set(ids["users"])),
            "state_counts": {e: len(v) for e, v in raw.items()},
            "fingerprint": sha256_obj({e: sorted(v) for e, v in raw.items()})}


# ------------------------------------------------------------------------------------ materialisation
def create_keyspace(s, ks):
    assert ks.startswith(KS_PREFIX)
    s.execute(f"DROP KEYSPACE IF EXISTS {ks}")
    s.execute(f"CREATE KEYSPACE {ks} WITH replication={{'class':'SimpleStrategy','replication_factor':1}} AND durable_writes=true")


def gen_rows(t: dict, state: dict) -> list[dict]:
    """Rows of canonical table t (refs -> values) derived from the model state; rows with a null key are skipped."""
    ent = t["row_entity"]
    keys = t["partition"] + [r for r, _ in t["clustering"]]
    allr = keys + t["columns"]
    out = {}
    for inst in state[ent].values():
        row = {}
        ok = True
        for r in allr:
            v, found = O._nav(state, ent, inst, r)
            if r.endswith(".id") and "." in r:
                v = v if found else None
            row[r] = v if found else None
            if r in keys and row[r] is None:
                ok = False
                break
        if ok:
            out[tuple(row[k] for k in keys)] = row   # same key -> one row (distinct tables, e.g. 34)
    return list(out.values())


def insert_rows(s, ks, name, refs, rows):
    from cassandra.concurrent import execute_concurrent_with_args
    stmt = s.prepare(f"INSERT INTO {ks}.{name} ({', '.join(colname(r) for r in refs)}) VALUES ({', '.join('?' for _ in refs)})")
    args = [tuple(r[x] for x in refs) for r in rows]
    res = execute_concurrent_with_args(s, stmt, args, concurrency=64, raise_on_first_error=True)
    return sum(1 for ok, _ in res if ok)


def canon_rows(rows: list[dict]) -> str:
    from reused.methodology.oracle_011 import canonical
    return sha256_obj(sorted(json.dumps(canonical(r), sort_keys=True) for r in rows))


def read_back(s, ks, name, refs) -> list[dict]:
    from cassandra.query import SimpleStatement
    cols = [colname(r) for r in refs]
    out = []
    for r in s.execute(SimpleStatement(f"SELECT {', '.join(cols)} FROM {ks}.{name}", fetch_size=5000)):
        out.append({ref: r[c] for ref, c in zip(refs, cols)})
    return out


def materialize_generated(s, ks: str, tables: list[dict], state: dict) -> dict:
    create_keyspace(s, ks)
    rec = {}
    for t in tables:
        s.execute(ddl(t, t["name"], ks))
    for t in tables:
        refs = t["partition"] + [r for r, _ in t["clustering"]] + t["columns"]
        rows = gen_rows(t, state)
        n = insert_rows(s, ks, t["name"], refs, rows)
        back = read_back(s, ks, t["name"], refs)
        rec[t["name"]] = {"expected_rows": len(rows), "acked": n, "read_back": len(back),
                          "expected_fingerprint": canon_rows(rows), "observed_fingerprint": canon_rows(back)}
        rec[t["name"]]["match"] = rec[t["name"]]["expected_fingerprint"] == rec[t["name"]]["observed_fingerprint"] and len(rows) == len(back)
    return rec


# V8 AUTO_H_norm recreated with the validated V8 derivation rule (reused/methodology/full_schemalens_012.from_*)
V8_NORM_TABLES = ("bids", "bids_by_item", "buynow", "buynow_by_buyer", "categories", "categories_by_dummy", "comments", "comments_by_id",
                  "comments_by_to_user", "comments_by_user", "items", "items_by_category", "items_by_region", "items_by_seller",
                  "items_by_user_bid", "regions", "regions_by_dummy", "users", "users_by_region")
V8_DDL_FILE = V9 / "reused/v8_auto_h_norm_ddl.json"


def v8_norm_ddl(s_meta_cluster=None) -> dict:
    """DDL (schema metadata only, no data) of the V8 cohort AUTO_H_norm, frozen into reused/ once."""
    d = read_json(V8_DDL_FILE, None)
    if d:
        return d
    from cassandra.cluster import Cluster
    c = Cluster(["127.0.0.1"])
    c.connect()
    try:
        c.refresh_schema_metadata()
        meta = c.metadata.keyspaces[O.COHORT_KEYSPACE]
        d = {t: meta.tables[t].as_cql_query() for t in V8_NORM_TABLES}
    finally:
        c.shutdown()
    write_json(V8_DDL_FILE, {"source": f"system_schema de {O.COHORT_KEYSPACE} (só metadados)", "ddl": d, "sha256": sha256_obj(d)})
    return read_json(V8_DDL_FILE)


def materialize_v8_norm(s, ks: str, raw: dict) -> dict:
    from reused.methodology import full_schemalens_012 as FS
    d = v8_norm_ddl()
    create_keyspace(s, ks)
    for t, q in d["ddl"].items():
        s.execute(q.replace(f"{O.COHORT_KEYSPACE}.{t}", f"{ks}.{t}", 1))
    users = {i: (r["nickname"], r["region_id"]) for i, r in raw["users"].items()}
    items = {i: (r["name"], r["end_date"]) for i, r in raw["items"].items()}
    rows = defaultdict(dict)
    streams = [("categories", FS.from_category), ("regions", FS.from_region), ("users", FS.from_user),
               ("items", lambda r: FS.from_item(r, users.get(r["seller_id"], (None, None))[1])), ("buynow", FS.from_buynow),
               ("comments", lambda r: FS.from_comment(r, users.get(r["from_user_id"], (None, None))[0])),
               ("bids", lambda r: FS.from_bid(r, *items.get(r["item_id"], (None, None))))]
    pk = {}
    from cassandra.cluster import Cluster
    c = Cluster(["127.0.0.1"])
    c.connect()
    c.refresh_schema_metadata()
    for t in V8_NORM_TABLES:
        tm = c.metadata.keyspaces[ks].tables[t]
        pk[t] = [col.name for col in tm.primary_key]
    c.shutdown()
    for ent, fn in streams:
        for r in raw[ent].values():
            for t, row in fn(r).items():
                if t in V8_NORM_TABLES and all(row.get(k) is not None for k in pk[t]):
                    rows[t][tuple(row[k] for k in pk[t])] = row
    rec = {}
    from cassandra.concurrent import execute_concurrent_with_args
    for t in V8_NORM_TABLES:
        lst = list(rows[t].values())
        cols = sorted(lst[0]) if lst else []
        if lst:
            stmt = s.prepare(f"INSERT INTO {ks}.{t} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})")
            execute_concurrent_with_args(s, stmt, [tuple(r[x] for x in cols) for r in lst], concurrency=64, raise_on_first_error=True)
        back = list(s.execute(f"SELECT {', '.join(cols)} FROM {ks}.{t}")) if cols else []
        rec[t] = {"expected_rows": len(lst), "read_back": len(back), "expected_fingerprint": canon_rows(lst),
                  "observed_fingerprint": canon_rows([dict(b) for b in back])}
        rec[t]["match"] = rec[t]["expected_fingerprint"] == rec[t]["observed_fingerprint"]
    return {"ddl_sha256": d["sha256"], "tables": rec}


# NoSE / QPG recreated from the historical artefacts (reused/methodology/baselines_005 manifests + project)
def materialize_baseline(s, ks: str, which: str, raw: dict) -> dict:
    import struct
    from reused.methodology import baselines_005 as B
    man = B.manifests()[which]
    create_keyspace(s, ks)
    for t in man["tables"]:
        cols = ",".join(k + " " + typ for k, typ in t["columns"].items())
        s.execute(f"CREATE TABLE {ks}.{t['name']} ({cols}, PRIMARY KEY {t['primary_key']}) WITH caching={{'keys':'NONE','rows_per_partition':'NONE'}}")
    lookup = lambda ent, i: raw[ent].get(int(i)) if i is not None else None
    rec = {}
    from cassandra.concurrent import execute_concurrent_with_args
    for t in man["tables"]:
        rows, skipped = {}, 0
        for src in raw[t["anchor"]].values():
            try:
                vals = B.project(t, src, lookup)
            except ValueError:
                vals = None
            if vals is None:
                skipped += 1
                continue
            key = tuple(vals[list(t["columns"]).index(k)] for k in t["keys"])
            rows[key] = vals
        cols = list(t["columns"])
        stmt = s.prepare(f"INSERT INTO {ks}.{t['name']} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})")
        execute_concurrent_with_args(s, stmt, list(rows.values()), concurrency=64, raise_on_first_error=True)
        n = s.execute(f"SELECT count(*) AS n FROM {ks}.{t['name']}").one()["n"]
        rec[t["name"]] = {"expected_rows": len(rows), "read_back": n, "skipped_missing_relationship": skipped, "match": n == len(rows)}
    return {"manifest_sha256": man["manifest_sha256"], "historical_ddl": man.get("ddl_path") or man.get("source"), "tables": rec}


# ------------------------------------------------------------------------------------ executor
class GeneratedAdapter:
    """Runs the frozen QueryPlans/WritePlans of a generated schema (no hand-written route)."""

    def __init__(self, s, ks: str, schema: dict, tables: dict, state0: dict):
        self.s, self.ks, self.tables = s, ks, tables
        self.routes = {int(k): v for k, v in schema["routes"].items()}
        self.prep = {}
        self.state0 = state0
        self.inserted: list[tuple[str, dict]] = []
        self.updated: set[tuple[str, int]] = set()
        self.orig_keys = {}
        self.orig_rows = {}
        for tid, t in tables.items():
            keys = t["partition"] + [r for r, _ in t["clustering"]]
            rows = gen_rows(t, state0)
            self.orig_keys[tid] = {tuple(r[k] for k in keys) for r in rows}
            if t["row_entity"] in ("items", "users"):
                self.orig_rows[tid] = rows

    def _p(self, q):
        if q not in self.prep:
            self.prep[q] = self.s.prepare(q)
        return self.prep[q]

    def _sel(self, q, args, support=False):
        rows = list(self.s.execute(self._p(q), args))
        self.reads += 1
        self.support += support
        self.fetched += len(rows)
        return rows

    def _row_refs(self, t, r):
        allr = t["partition"] + [x for x, _ in t["clustering"]] + t["columns"]
        return {ref: r[colname(ref)] for ref in allr}

    def execute_statement(self, sid: int, params: dict, ctx: dict) -> dict:
        self.reads = self.writes = self.support = self.fetched = 0
        c = CONTRACT[sid]
        rows = self._read(sid, c, params, ctx) if c["kind"] == "read" else self._write(sid, c, params, ctx)
        return {"statement_id": sid, "logical_rows": rows, "row_count": len(rows), "physical_reads": self.reads,
                "physical_writes": self.writes, "support_reads": self.support, "rows_fetched": self.fetched,
                "query_plans": [self.routes[sid].get("main")] if c["kind"] == "read" else [], "write_plans": [x["table"] for x in self.routes[sid].get("tables", [])] if c["kind"] == "write" else []}

    # ---- reads
    def _read(self, sid, c, params, ctx):
        plan = self.routes[sid]
        t = self.tables[plan["main"]]
        ent = c["entity"]
        src = {r: O._src(v, params, ctx) for r, op, v in c["where"] if op == "="}
        rng = [(r, O._src(v, params, ctx)) for r, op, v in c["where"] if op != "="]
        conds, args = [], []
        for r in t["partition"]:
            conds.append(f"{colname(r)} = ?")
            args.append(src[r])
        if plan["range_pushdown"]:
            r, v = rng[0]
            conds.append(f"{colname(r)} >= ?")
            args.append(datetime.fromisoformat(v) if isinstance(v, str) else v)
        q = f"SELECT * FROM {self.ks}.{t['name']} WHERE {' AND '.join(conds)}"
        if plan["limit_pushdown"]:
            q += f" LIMIT {c['limit']}"
        rows = [self._row_refs(t, r) for r in self._sel(q, tuple(args))]
        logic = {r for r, _, _ in c["where"]} | {r for r, _ in c["order"]} | ({c["distinct"]} if c["distinct"] else set())
        lk = plan["lookups"]
        lk_refs = {(a if rel is None else f"{rel}.{a}") for _, rel, attrs, _ in lk for a in attrs}
        early = bool(lk_refs & logic) or any(rel in c["require"] for _, rel, _, _ in lk)
        if early:
            self._lookup(ent, rows, lk)
            rows = self._post(c, ent, rows, params, ctx, rng)
        else:
            rows = self._post(c, ent, rows, params, ctx, rng)
            self._lookup(ent, rows, lk)
        out = []
        for r in rows:
            o = {}
            for key, ref in c["project"]:
                v = r.get(ref)
                if v is None and ref in c["fallback"]:
                    try:
                        v = c["fallback"][ref].format(**r)
                    except KeyError:
                        v = None
                o[key] = v
            out.append(o)
        for k, (idx, key) in c["ctx_out"].items():
            if len(out) > idx:
                ctx[k] = out[idx] if key is None else out[idx][key]
        return out

    def _lookup(self, ent, rows, lk):
        for e2, rel, attrs, tid in lk:
            L = self.tables[tid]
            q = f"SELECT {', '.join(colname(a) for a in attrs)} FROM {self.ks}.{L['name']} WHERE id = ?"
            for r in rows:
                key = r.get("id") if rel is None else (r.get(FK[ent][rel]) if FK[ent][rel] in r else r.get(f"{rel}.id"))
                got = self._sel(q, (key,), support=True) if key is not None else []
                for a in attrs:
                    r[a if rel is None else f"{rel}.{a}"] = got[0][colname(a)] if got else None
                if rel is not None:
                    r.setdefault(f"{rel}.id", key if got else None)
                    if not got:
                        r[f"{rel}.id"] = None

    def _post(self, c, ent, rows, params, ctx, rng):
        keep = []
        for r in rows:
            ok = True
            for ref, op, v in c["where"]:
                val = r.get(ref)
                v = O._src(v, params, ctx)
                if op == "=":
                    ok = val == v
                else:
                    ok = val is not None and val >= (datetime.fromisoformat(v) if isinstance(v, str) else v)
                if not ok:
                    break
            if ok and all(r.get(f"{rel}.id") is not None for rel in c["require"]):
                keep.append(r)
        for ref, d in reversed(c["order"]):
            keep.sort(key=lambda r, ref=ref: r.get(ref), reverse=(d == "desc"))
        if c["distinct"]:
            seen, out = set(), []
            for r in keep:
                if r[c["distinct"]] not in seen:
                    seen.add(r[c["distinct"]])
                    out.append(r)
            keep = out
        return keep[: c["limit"]] if c["limit"] is not None else keep

    # ---- writes
    def _write(self, sid, c, params, ctx):
        plan = self.routes[sid]
        ent = c["entity"]
        vals = {}
        if c["op"] == "INSERT":
            for a, s_ in c["values"].items():
                vals[a] = O._typed(ent, a, O._src(s_, params, ctx))
            key_id = vals["id"]
        else:
            key_id = int(O._src(c["key"], params, ctx))
            vals["id"] = key_id
            for a, ex in c["values"].items():
                vals[a] = O._typed(ent, a, O._expr(ex, params, ctx))
        sup = {}
        for sp in plan["support"]:
            e2, attrs, L = sp["entity"], sp["attrs"], self.tables[sp["table"]]
            if e2 == ent and c["op"] == "UPDATE":
                key, rel = key_id, None
            else:
                rel = self._rel_for(plan, e2, ent)
                key = vals.get(FK[ent][rel]) if rel else None
            got = self._sel(f"SELECT {', '.join(colname(a) for a in attrs)} FROM {self.ks}.{L['name']} WHERE id = ?", (key,), support=True) if key is not None else []
            for a in attrs:
                sup[(rel, a)] = got[0][colname(a)] if got else None
        for x in plan["tables"]:
            t = self.tables[x["table"]]
            keys = t["partition"] + [r for r, _ in t["clustering"]]
            allr = keys + t["columns"]

            def value(ref):
                e2, a, hops = resolve(ent, ref)
                if not hops:
                    return vals.get(a) if a in vals else sup.get((None, a))
                rel = hops[0][1]
                if a == "id" and len(hops) == 1:
                    return vals.get(FK[ent][rel])
                if len(hops) == 2:
                    return sup.get((rel, FK[hops[1][0]][hops[1][1]] if a == "id" else a))
                return sup.get((rel, a))
            if c["op"] == "INSERT":
                row = {r: value(r) for r in allr}
                if any(row[k] is None for k in keys):
                    continue
                self.s.execute(self._p(f"INSERT INTO {self.ks}.{t['name']} ({', '.join(colname(r) for r in allr)}) VALUES ({', '.join('?' for _ in allr)})"),
                               tuple(row[r] for r in allr))
                self.writes += 1
                self.inserted.append((x["table"], {k: row[k] for k in keys}))
            else:
                setc = [r for r in t["columns"] if "." not in r and r in vals]
                kv = {k: value(k) for k in keys}
                if any(v is None for v in kv.values()):
                    continue
                self.s.execute(self._p(f"UPDATE {self.ks}.{t['name']} SET {', '.join(colname(r) + ' = ?' for r in setc)} WHERE {' AND '.join(colname(k) + ' = ?' for k in keys)}"),
                               tuple(vals[r] for r in setc) + tuple(kv[k] for k in keys))
                self.writes += 1
                self.updated.add((ent, key_id))
        ev = {}
        for k, s_ in c["event"].items():
            ev[k] = vals[s_.split(":", 1)[1]] if s_.startswith("value:") else O._typed_event(O._src(s_, params, ctx))
        return [ev]

    def _rel_for(self, plan, e2, ent):
        for x in plan["tables"]:
            t = self.tables[x["table"]]
            for ref in t["partition"] + [r for r, _ in t["clustering"]] + t["columns"]:
                hops = resolve(ent, ref)[2]
                if hops and hops[0][2] == e2:
                    return hops[0][1]
        return None

    def reset_state(self) -> None:
        for tid, key in self.inserted:
            if tuple(key.values()) in self.orig_keys[tid]:
                continue
            t = self.tables[tid]
            self.s.execute(self._p(f"DELETE FROM {self.ks}.{t['name']} WHERE {' AND '.join(colname(k) + ' = ?' for k in key)}"), tuple(key.values()))
        for ent, i in self.updated:
            for tid, rows in self.orig_rows.items():
                t = self.tables[tid]
                if t["row_entity"] != ent:
                    continue
                allr = t["partition"] + [r for r, _ in t["clustering"]] + t["columns"]
                for r in rows:
                    if self._instance_of(t, r) == i:
                        self.s.execute(self._p(f"INSERT INTO {self.ks}.{t['name']} ({', '.join(colname(x) for x in allr)}) VALUES ({', '.join('?' for _ in allr)})"),
                                       tuple(r[x] for x in allr))
        self.inserted, self.updated = [], set()

    def _instance_of(self, t, r):
        return r.get("id")
