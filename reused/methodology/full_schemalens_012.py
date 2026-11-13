"""Full-scale SchemaLens materialisation and benchmark (V8-012).

Why a new materialisation: the existing ``rubis_v8_full_norm`` / ``rubis_v8_full_c3c5c7``
keyspaces were built by ``final_v8.materialize`` with placeholder values
(``comments_by_id.nickname = 'user-<id>'``, ``item_bid_shared.item_name = 'item-<id>'``)
and were later mutated in place by ``ablations_005`` (renamed items and phantom
index rows created by UPDATE upserts).  They are not used.

Stages
------
validate-derivation  every row of the oracle-validated cohort keyspace
                     ``rubis_v8_c008_auto_c3c5c7`` must equal the derivation
                     below applied to its ``rubis_full`` source row.
build                one keyspace with all 22 tables derived from ``rubis_full``
                     (resumable: paging_state checkpoint after every acknowledged page).
verify               every partition the benchmark can touch (domain users, items,
                     categories, regions) is compared with an independent scan of
                     ``rubis_full``; acknowledged mutation counts are reconciled.
bench                frozen protocol with AUTO_H_norm routes (f000) and AUTO_H_c3c5c7
                     routes (f111) on the same keyspace; per-request oracle =
                     agreement with the full-scale baselines' raw results.

The derivation rules mirror ``physical_007.verify_schemalens_projections``.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Iterable

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement, dict_factory

from . import protocol_012 as P
from . import workload_005 as w005
from .ablation_factorial_012 import FactorialAdapter
from .benchmark_005 import BENCHMARK_DOMAIN, DeploymentAdapter
from .semantic_replay_011 import source_closure

SRC = "rubis_full"
COHORT = "rubis_v8_c008_auto_c3c5c7"
TARGET = "rubis_v8_c012_full_schemalens"
OUT = P.RUN_DIR / "full_schemalens"
FULL_BASELINE_RAW = P.FROZEN_011 / "continuation/full_baselines/benchmark/raw"
ROUTES = (("SchemaLens_full_AUTO_H_norm", frozenset()), ("SchemaLens_full_AUTO_H_c3c5c7", frozenset({"C3", "C5", "C7"})))
NORM_TABLES = None  # filled from the cohort catalogue


# --------------------------------------------------------------------------- derivation (single source of truth)
def from_category(r):
    return {"categories": {"id": r["id"], "name": r["name"]}, "categories_by_dummy": {"dummy": 1, "id": r["id"], "name": r["name"]}}


def from_region(r):
    return {"regions": {"id": r["id"], "name": r["name"]}, "regions_by_dummy": {"dummy": 1, "id": r["id"], "name": r["name"]}}


def from_user(r):
    return {"users": {k: r[k] for k in ("id", "firstname", "lastname", "nickname", "password", "email", "rating", "balance", "creation_date", "region_id")},
            "users_by_region": {"region_id": r["region_id"], "user_id": r["id"], "nickname": r["nickname"]}}


def from_item(r, seller_region):
    base = {"item_id": r["id"], "end_date": r["end_date"], "name": r["name"]}
    return {"items": {k: r[k] for k in ("id", "name", "description", "initial_price", "quantity", "reserve_price", "buy_now", "nb_of_bids", "max_bid", "start_date", "end_date", "seller_id", "category_id")},
            "items_by_category": {**base, "category_id": r["category_id"], "initial_price": r["initial_price"], "max_bid": r["max_bid"], "nb_of_bids": r["nb_of_bids"]},
            "items_by_region": {**base, "region_id": seller_region, "category_id": r["category_id"], "seller_id": r["seller_id"]},
            "items_region_bounded": {**base, "region_id": seller_region, "category_id": r["category_id"]},
            "items_by_seller": {**base, "seller_id": r["seller_id"]},
            "items_selective": {"id": r["id"], "name": r["name"], "category_id": r["category_id"]}}


def from_bid(r, item_name, item_end):
    return {"bids": {k: r[k] for k in ("id", "qty", "bid", "max_bid", "bid_date", "user_id", "item_id")},
            "bids_by_item": {"item_id": r["item_id"], "bid_date": r["bid_date"], "bid_id": r["id"], "user_id": r["user_id"], "qty": r["qty"], "bid": r["bid"]},
            "item_bid_shared": {"item_id": r["item_id"], "bid_date": r["bid_date"], "bid_id": r["id"], "item_name": item_name, "user_id": r["user_id"], "qty": r["qty"], "bid": r["bid"]},
            "items_by_user_bid": {"user_id": r["user_id"], "end_date": item_end, "item_id": r["item_id"], "name": item_name}}


def from_comment(r, author_nickname):
    return {"comments": {k: r[k] for k in ("id", "rating", "comment_date", "comment", "from_user_id", "to_user_id", "item_id")},
            "comments_by_to_user": {"to_user_id": r["to_user_id"], "comment_date": r["comment_date"], "comment_id": r["id"], "rating": r["rating"], "comment": r["comment"]},
            "comments_by_user": {"user_id": r["from_user_id"], "comment_date": r["comment_date"], "comment_id": r["id"]},
            "comments_by_id": {"id": r["id"], "from_user_id": r["from_user_id"], "nickname": author_nickname}}


def from_buynow(r):
    return {"buynow": {k: r[k] for k in ("id", "qty", "buy_date", "buyer_id", "item_id")},
            "buynow_by_buyer": {"buyer_id": r["buyer_id"], "buy_date": r["buy_date"], "id": r["id"], "item_id": r["item_id"], "qty": r["qty"]}}


# target table -> (source entity, key column in target row)
SOURCE_OF = {"categories": ("categories", "id"), "categories_by_dummy": ("categories", "id"), "regions": ("regions", "id"), "regions_by_dummy": ("regions", "id"),
             "users": ("users", "id"), "users_by_region": ("users", "user_id"),
             **{t: ("items", "item_id") for t in ("items_by_category", "items_by_region", "items_region_bounded", "items_by_seller")},
             "items": ("items", "id"), "items_selective": ("items", "id"),
             "bids": ("bids", "id"), "bids_by_item": ("bids", "bid_id"), "item_bid_shared": ("bids", "bid_id"),
             "comments": ("comments", "id"), "comments_by_to_user": ("comments", "comment_id"), "comments_by_user": ("comments", "comment_id"), "comments_by_id": ("comments", "id"),
             "buynow": ("buynow", "id"), "buynow_by_buyer": ("buynow", "id")}


def norm_value(v):
    if isinstance(v, float):
        return round(v, 4)
    if hasattr(v, "isoformat"):
        return v.replace(tzinfo=None).isoformat(timespec="milliseconds")
    return v


def rows_equal(a: dict, b: dict) -> list[str]:
    return sorted(k for k in set(a) | set(b) if norm_value(a.get(k)) != norm_value(b.get(k)))


def session_for() -> tuple[Cluster, Any]:
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    session.row_factory = dict_factory
    session.default_timeout = 300
    return cluster, session


# --------------------------------------------------------------------------- stage 1
def validate_derivation() -> dict[str, Any]:
    cluster, s = session_for()
    try:
        cache: dict[tuple, Any] = {}

        def get(entity, ident):
            if (entity, ident) not in cache:
                cache[(entity, ident)] = s.execute(f"SELECT * FROM {SRC}.{entity} WHERE id=%s", (ident,)).one()
            return cache[(entity, ident)]

        def derive(entity, src):
            if entity == "categories": return from_category(src)
            if entity == "regions": return from_region(src)
            if entity == "users": return from_user(src)
            if entity == "items":
                seller = get("users", src["seller_id"]); return from_item(src, seller["region_id"] if seller else None)
            if entity == "bids":
                it = get("items", src["item_id"]); return from_bid(src, it["name"] if it else None, it["end_date"] if it else None)
            if entity == "comments":
                au = get("users", src["from_user_id"]); return from_comment(src, au["nickname"] if au else None)
            if entity == "buynow": return from_buynow(src)
            raise KeyError(entity)

        cluster.refresh_schema_metadata()
        tables = sorted(cluster.metadata.keyspaces[COHORT].tables)
        report = {}
        for t in tables:
            observed = matched = overlays = 0
            mismatches: list[dict] = []
            for row in s.execute(SimpleStatement(f"SELECT * FROM {COHORT}.{t}", fetch_size=2000)):
                observed += 1
                if t == "items_by_user_bid":
                    item = get("items", row["item_id"])
                    bidders = {b["user_id"] for b in s.execute(f"SELECT user_id FROM {SRC}.bids_by_item WHERE item_id=%s", (row["item_id"],))} if item else set()
                    if not item or row["user_id"] not in bidders:
                        overlays += 1; continue
                    expected = {"user_id": row["user_id"], "end_date": item["end_date"], "item_id": item["id"], "name": item["name"]}
                else:
                    entity, key = SOURCE_OF[t]
                    src = get(entity, row[key])
                    if src is None:
                        overlays += 1; continue
                    expected = derive(entity, src)[t]
                bad = rows_equal(row, expected)
                if bad:
                    if len(mismatches) < 5:
                        mismatches.append({"row": {k: str(v) for k, v in row.items()}, "expected": {k: str(v) for k, v in expected.items()}, "columns": bad})
                    continue
                matched += 1
            report[t] = {"observed": observed, "matched": matched, "overlays": overlays, "mismatches": observed - matched - overlays, "samples": mismatches}
        ok = all(v["mismatches"] == 0 and v["overlays"] == 0 and v["observed"] > 0 for k, v in report.items() if not k.endswith("_by_dummy")) and set(tables) == set(SOURCE_OF) | {"items_by_user_bid"}
        result = {"status": "PASS" if ok else "FAIL", "cohort": COHORT, "tables": report, "at": P.now_iso()}
        P.write_json(OUT / "DERIVATION_VALIDATION.json", result)
        return result
    finally:
        cluster.shutdown()


# --------------------------------------------------------------------------- stage 2
class Writer:
    def __init__(self, s, ks: str, tables: Iterable[str], cluster):
        self.s, self.ks, self.stmts, self.cols, self.acked = s, ks, {}, {}, defaultdict(int)
        for t in tables:
            cols = list(cluster.metadata.keyspaces[COHORT].tables[t].columns)
            st = s.prepare(f"INSERT INTO {ks}.{t} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})")
            st.consistency_level = ConsistencyLevel.ONE
            self.stmts[t], self.cols[t] = st, cols

    def write(self, derived_rows: list[dict[str, dict]]):
        by_table = defaultdict(list)
        for d in derived_rows:
            for t, row in d.items():
                if t in self.stmts:
                    by_table[t].append(tuple(row[c] for c in self.cols[t]))
        for t, args in by_table.items():
            res = execute_concurrent_with_args(self.s, self.stmts[t], args, concurrency=512, raise_on_first_error=False)
            ok = sum(1 for good, _ in res if good)
            if ok != len(args):
                first = next((r for good, r in res if not good), None)
                raise RuntimeError(f"{t}: {ok}/{len(args)} acknowledged; first error {first!r}")
            self.acked[t] += ok


def build() -> dict[str, Any]:
    if P.read_json(OUT / "DERIVATION_VALIDATION.json", {}).get("status") != "PASS":
        raise SystemExit("derivation not validated against the cohort; run validate-derivation first")
    cluster, s = session_for()
    cp_path = OUT / "BUILD_CHECKPOINT.json"
    cp = P.read_json(cp_path, {"streams": {}, "acked": {}})
    try:
        cluster.refresh_schema_metadata()
        cohort_meta = cluster.metadata.keyspaces[COHORT]
        if not cp["streams"]:
            s.execute(f"DROP KEYSPACE IF EXISTS {TARGET}")
            s.execute(f"CREATE KEYSPACE {TARGET} WITH replication={{'class':'SimpleStrategy','replication_factor':1}} AND durable_writes=true")
            for t, tm in cohort_meta.tables.items():
                s.execute(tm.as_cql_query().replace(f"{COHORT}.{t}", f"{TARGET}.{t}", 1))
            cluster.refresh_schema_metadata()
            cp["ddl_sha256"] = hashlib.sha256(json.dumps({t: tm.as_cql_query() for t, tm in cohort_meta.tables.items()}, sort_keys=True).encode()).hexdigest()
            cp["started_at"] = P.now_iso()
        writer = Writer(s, TARGET, cohort_meta.tables, cluster)
        writer.acked.update(cp.get("acked", {}))
        # lookups needed by joins (kept compact: two small tuples per key)
        users = {r["id"]: (r["nickname"], r["region_id"]) for r in s.execute(SimpleStatement(f"SELECT id,nickname,region_id FROM {SRC}.users", fetch_size=20000))}
        items: dict[int, tuple] = {}
        for r in s.execute(SimpleStatement(f"SELECT id,name,end_date FROM {SRC}.items", fetch_size=20000)):
            items[r["id"]] = (r["name"], r["end_date"])
        streams: list[tuple[str, Callable[[dict], dict]]] = [
            ("categories", from_category), ("regions", from_region), ("users", from_user),
            ("items", lambda r: from_item(r, users.get(r["seller_id"], (None, None))[1])),
            ("buynow", from_buynow),
            ("comments", lambda r: from_comment(r, users.get(r["from_user_id"], (None, None))[0])),
            ("bids", lambda r: from_bid(r, *items.get(r["item_id"], (None, None)))),
        ]
        for entity, fn in streams:
            st = cp["streams"].get(entity, {"rows": 0, "pages": 0, "paging_state": None, "done": False})
            while not st["done"]:
                stmt = SimpleStatement(f"SELECT * FROM {SRC}.{entity}", fetch_size=20000, consistency_level=ConsistencyLevel.ONE)
                res = s.execute(stmt, paging_state=bytes.fromhex(st["paging_state"]) if st["paging_state"] else None)
                rows = res.current_rows
                writer.write([fn(r) for r in rows])
                st["rows"] += len(rows); st["pages"] += 1
                st["paging_state"] = res.paging_state.hex() if res.has_more_pages else None
                st["done"] = not res.has_more_pages
                cp["streams"][entity] = st; cp["acked"] = dict(writer.acked); cp["updated_at"] = P.now_iso()
                P.write_json(cp_path, cp)
                if st["pages"] % 50 == 0:
                    print(json.dumps({"entity": entity, "rows": st["rows"]}), flush=True)
        cp["status"] = "PASS"; cp["finished_at"] = P.now_iso()
        P.write_json(cp_path, cp)
        return cp
    finally:
        cluster.shutdown()


# --------------------------------------------------------------------------- stage 3
def domain_expectation(s) -> dict[str, dict]:
    """Independent expected content of every partition the benchmark can touch, from scans of rubis_full."""
    du, di = set(BENCHMARK_DOMAIN["users"]), set(BENCHMARK_DOMAIN["items"])
    dc, dr = set(BENCHMARK_DOMAIN["categories"]), set(BENCHMARK_DOMAIN["regions"])
    users = {r["id"]: r for r in s.execute(SimpleStatement(f"SELECT * FROM {SRC}.users", fetch_size=20000))}
    exp: dict[str, dict] = defaultdict(dict)

    def put(t, key, row):
        exp[t].setdefault(key, []).append(row)
    for r in s.execute(f"SELECT * FROM {SRC}.categories"):
        put("categories_by_dummy", 1, from_category(r)["categories_by_dummy"])
    for r in s.execute(f"SELECT * FROM {SRC}.regions"):
        put("regions_by_dummy", 1, from_region(r)["regions_by_dummy"])
    for u in users.values():
        d = from_user(u)
        if u["id"] in du: put("users", u["id"], d["users"])
        if u["region_id"] in dr: put("users_by_region", u["region_id"], d["users_by_region"])
    item_meta = {}
    for r in s.execute(SimpleStatement(f"SELECT * FROM {SRC}.items", fetch_size=20000)):
        d = from_item(r, users.get(r["seller_id"], {}).get("region_id"))
        if r["id"] in di: put("items", r["id"], d["items"]); put("items_selective", r["id"], d["items_selective"])
        if r["category_id"] in dc: put("items_by_category", r["category_id"], d["items_by_category"])
        if d["items_by_region"]["region_id"] in dr:
            put("items_by_region", d["items_by_region"]["region_id"], d["items_by_region"]); put("items_region_bounded", d["items_by_region"]["region_id"], d["items_region_bounded"])
        if r["seller_id"] in du: put("items_by_seller", r["seller_id"], d["items_by_seller"])
        item_meta[r["id"]] = (r["name"], r["end_date"])
    for r in s.execute(SimpleStatement(f"SELECT * FROM {SRC}.bids", fetch_size=20000)):
        if r["item_id"] in di or r["user_id"] in du:
            d = from_bid(r, *item_meta.get(r["item_id"], (None, None)))
            if r["item_id"] in di: put("bids_by_item", r["item_id"], d["bids_by_item"]); put("item_bid_shared", r["item_id"], d["item_bid_shared"])
            if r["user_id"] in du: put("items_by_user_bid", r["user_id"], d["items_by_user_bid"])
    for r in s.execute(SimpleStatement(f"SELECT * FROM {SRC}.comments", fetch_size=20000)):
        if r["to_user_id"] in du or r["from_user_id"] in du:
            d = from_comment(r, users.get(r["from_user_id"], {}).get("nickname"))
            if r["to_user_id"] in du: put("comments_by_to_user", r["to_user_id"], d["comments_by_to_user"])
            if r["from_user_id"] in du: put("comments_by_user", r["from_user_id"], d["comments_by_user"])
    for r in s.execute(SimpleStatement(f"SELECT * FROM {SRC}.buynow", fetch_size=20000)):
        if r["buyer_id"] in du: put("buynow_by_buyer", r["buyer_id"], from_buynow(r)["buynow_by_buyer"])
    return exp


PARTITION_COLUMN = {"categories_by_dummy": "dummy", "regions_by_dummy": "dummy", "users": "id", "users_by_region": "region_id", "items": "id",
                    "items_selective": "id", "items_by_category": "category_id", "items_by_region": "region_id", "items_region_bounded": "region_id",
                    "items_by_seller": "seller_id", "bids_by_item": "item_id", "item_bid_shared": "item_id", "items_by_user_bid": "user_id",
                    "comments_by_to_user": "to_user_id", "comments_by_user": "user_id", "buynow_by_buyer": "buyer_id"}


def _canon_rows(rows: list[dict]) -> list[str]:
    seen = {json.dumps({k: norm_value(v) for k, v in sorted(r.items())}, sort_keys=True, default=str) for r in rows}
    return sorted(seen)  # set semantics: Cassandra collapses duplicate primary keys (items_by_user_bid)


def target_partitions(s, keys_by_table: dict[str, Iterable]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for t, keys in keys_by_table.items():
        col = PARTITION_COLUMN[t]
        stmt = s.prepare(f"SELECT * FROM {TARGET}.{t} WHERE {col}=?")
        out[t] = {k: _canon_rows(list(s.execute(stmt, (k,)))) for k in keys}
    return out


def domain_keys() -> dict[str, list]:
    du, di, dc, dr = (BENCHMARK_DOMAIN[k] for k in ("users", "items", "categories", "regions"))
    return {"categories_by_dummy": [1], "regions_by_dummy": [1], "users": du, "users_by_region": dr, "items": di, "items_selective": di,
            "items_by_category": dc, "items_by_region": dr, "items_region_bounded": dr, "items_by_seller": du, "bids_by_item": di,
            "item_bid_shared": di, "items_by_user_bid": du, "comments_by_to_user": du, "comments_by_user": du, "buynow_by_buyer": du}


def verify() -> dict[str, Any]:
    cp = P.read_json(OUT / "BUILD_CHECKPOINT.json", {})
    if cp.get("status") != "PASS":
        raise SystemExit("build not complete")
    cluster, s = session_for()
    try:
        exp = domain_expectation(s)
        got = target_partitions(s, domain_keys())
        tables = {}
        for t, keys in domain_keys().items():
            bad = [k for k in keys if _canon_rows(exp.get(t, {}).get(k, [])) != got[t][k]]
            tables[t] = {"partitions": len(keys), "rows": sum(len(got[t][k]) for k in keys), "mismatched_partitions": bad[:20], "status": "PASS" if not bad else "FAIL"}
        src_counts = {e: st["rows"] for e, st in cp["streams"].items()}
        one_to_one = {"bids": "bids", "bids_by_item": "bids", "item_bid_shared": "bids", "items": "items", "items_by_category": "items", "items_by_region": "items",
                      "items_region_bounded": "items", "items_by_seller": "items", "items_selective": "items", "comments": "comments", "comments_by_to_user": "comments",
                      "comments_by_user": "comments", "comments_by_id": "comments", "buynow": "buynow", "buynow_by_buyer": "buynow", "users": "users", "users_by_region": "users"}
        recon = {t: {"acked": cp["acked"].get(t), "source_rows": src_counts.get(e), "ok": cp["acked"].get(t) == src_counts.get(e)} for t, e in one_to_one.items()}
        ok = all(v["status"] == "PASS" for v in tables.values()) and all(v["ok"] for v in recon.values())
        result = {"status": "PASS" if ok else "FAIL", "keyspace": TARGET, "domain_partitions": tables, "mutation_reconciliation": recon,
                  "source_rows": src_counts, "at": P.now_iso()}
        P.write_json(OUT / "VERIFICATION.json", result)
        return result
    finally:
        cluster.shutdown()


# --------------------------------------------------------------------------- stage 4
class FullScaleAdapter(FactorialAdapter):
    """FactorialAdapter without the full-table scan of items_by_user_bid (20M rows at this scale).

    ``preexisting_user_bids`` is only consulted for (user, item) pairs whose user is in
    BENCHMARK_DOMAIN, so reading the domain users' partitions is exact."""

    def __init__(self, session, deployment_id, keyspace, closure, families):
        DeploymentAdapter.__init__(self, session, deployment_id, keyspace, "schemalens_norm", closure)
        stmt = session.prepare(f"SELECT user_id,item_id FROM {keyspace}.items_by_user_bid WHERE user_id=?")
        self.preexisting_user_bids = {(r["user_id"], r["item_id"]) for u in BENCHMARK_DOMAIN["users"] for r in session.execute(stmt, (u,))}
        self.families = frozenset(families)


def new_id_absence(s, source_state_id) -> dict[str, Any]:
    """Every id the protocol could insert must be absent from the id-keyed tables after reset."""
    ids = set()
    for w in P.WORKLOADS:
        for rep in range(P.REPETITIONS):
            for phase, n in (("warmup", P.WARMUP), ("measured", P.MEASURED)):
                for req in w005.histories(w, rep, n, BENCHMARK_DOMAIN, source_state_id, phase=phase):
                    if req["logical_event_ids"]:
                        ids.add(int(req["parameters"]["new_id"]))
    present = {}
    for t in ("items", "items_selective", "users", "bids", "comments", "comments_by_id", "buynow"):
        stmt = s.prepare(f"SELECT id FROM {TARGET}.{t} WHERE id=?")
        res = execute_concurrent_with_args(s, stmt, [(i,) for i in sorted(ids)], concurrency=256, raise_on_first_error=True)
        present[t] = sum(1 for ok, r in res if ok and list(r))
    return {"status": "PASS" if not any(present.values()) else "FAIL", "candidate_ids": len(ids), "present_after_reset": present}


def bench(allow_busy: bool = False) -> dict[str, Any]:
    if P.read_json(OUT / "VERIFICATION.json", {}).get("status") != "PASS":
        raise SystemExit("full-scale keyspace not verified")
    node = P.require_idle_node(allow_busy)
    cluster, s = session_for()
    try:
        closure, source_state_id = source_closure(s)
        reference = baseline_hashes()
        records = {}
        for deployment, families in ROUTES:
            out_path = OUT / f"{deployment}_RESULT.json"
            if P.read_json(out_path, {}).get("status") == "PASS":
                records[deployment] = P.read_json(out_path); continue
            before = target_partitions(s, domain_keys())
            adapter = FullScaleAdapter(s, deployment, TARGET, closure, families)
            started = P.now_iso()
            with P.BulkWriterWatch() as watch:
                result = P.run_protocol(adapter, deployment, TARGET, source_state_id, OUT / "raw")
            isolation = watch.report()
            after = target_partitions(s, domain_keys())
            changed = sorted(t for t in before if before[t] != after[t])
            restoration = {"status": "PASS" if not changed else "FAIL", "domain_tables_changed": changed}
            absence = new_id_absence(s, source_state_id)
            agreement = cross_agreement([P.ROOT / f["path"] for f in result["raw_files"]], reference)
            errors = sum(x["errors"] for x in result["summaries"])
            status = "PASS" if errors == 0 and restoration["status"] == "PASS" and absence["status"] == "PASS" and agreement["status"] == "PASS" else "FAIL"
            if status == "PASS" and not allow_busy and not isolation["clean"]:
                status = "INVALID_CONCURRENT_LOAD"
            rec = {"deployment_id": deployment, "families": sorted(families), "keyspace": TARGET, "status": status, "started_at": started,
                   "finished_at": P.now_iso(), "node_state": node, "isolation": isolation, "state_restoration": restoration, "new_id_absence": absence,
                   "oracle": agreement, "protocol": {k: result[k] for k in ("measured_requests_per_repetition", "warmup_requests_per_repetition", "repetitions")},
                   "summaries": result["summaries"], "raw_files": result["raw_files"], "errors": errors,
                   "scale": "full historical (rubis_full: 2M items / 20M bids / 10M comments)"}
            P.write_json(out_path, rec)
            records[deployment] = rec
            print(json.dumps({"deployment": deployment, "status": status, "oracle": {k: agreement[k] for k in ("observations", "agree_all", "status")}}), flush=True)
            if status != "PASS":
                return {"status": status}
        storage = P.keyspace_storage(TARGET)
        family = {"items_selective", "item_bid_shared", "items_region_bounded"}
        P.write_json(OUT / "STORAGE.json", {**storage, "AUTO_H_norm_live_bytes": sum(v.get("live_bytes", 0) for t, v in storage["tables"].items() if t not in family),
                                             "AUTO_H_c3c5c7_live_bytes": storage["live_bytes_total"]})
        return {"status": "PASS"}
    finally:
        cluster.shutdown()


SAMPLE_BASELINE_RAW = P.RUN_DIR / "full_baselines_sample/raw"
FULL_PROTOCOL_BASELINES = ("NoSE", "QPG")
SAMPLED_BASELINES = ("Normalized",)  # Expert cut at full scale (user decision 2026-11-10)


def baseline_hashes() -> dict[str, dict[tuple, str]]:
    """NoSE/QPG: full frozen protocol (V8-011 continuation). Normalized: frozen-prefix sample (V8-012)."""
    out: dict[str, dict] = {}
    for raw_dir, wanted in ((FULL_BASELINE_RAW, FULL_PROTOCOL_BASELINES), (SAMPLE_BASELINE_RAW, SAMPLED_BASELINES)):
        for p in sorted(raw_dir.glob("*.jsonl.gz")):
            dep = p.name.rsplit("_", 1)[0]
            if dep not in wanted:
                continue
            with gzip.open(p, "rt", encoding="utf-8") as f:
                for line in f:
                    x = json.loads(line)
                    out.setdefault(dep, {})[(x["workload"], int(x["repetition"]), x["logical_request_id"])] = x["canonical_result_hash"]
    return out


def cross_agreement(paths: list[Path], reference: dict[str, dict]) -> dict[str, Any]:
    """Per-request agreement with the independently materialised full-scale baselines.

    PASS requires: NoSE and QPG cover every observation and agree on all of them; Normalized (frozen-prefix
    sample) is present, covers at least one observation, and agrees on every observation it covers."""
    counts = defaultdict(int)
    per_baseline = defaultdict(int)
    covered = defaultdict(int)
    samples = []
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                x = json.loads(line)
                key = (x["workload"], int(x["repetition"]), x["logical_request_id"])
                counts["observations"] += 1
                available = {b: ref[key] for b, ref in reference.items() if key in ref}
                for b in available:
                    covered[b] += 1
                agree = {b for b, h in available.items() if h == x["canonical_result_hash"]}
                for b in agree:
                    per_baseline[b] += 1
                if available and len(agree) == len(available):
                    counts["agree_all"] += 1
                elif len(samples) < 20:
                    samples.append({"key": list(key), "request_group": x["request_group"], "disagreeing_baselines": sorted(set(available) - agree)})
    n = counts["observations"]
    full_ok = all(covered[b] == n and per_baseline[b] == n for b in FULL_PROTOCOL_BASELINES)
    sampled_ok = all(b in reference and covered[b] > 0 and per_baseline[b] == covered[b] for b in SAMPLED_BASELINES)
    ok = n > 0 and full_ok and sampled_ok and counts["agree_all"] == n
    return {"status": "PASS" if ok else "FAIL", "baselines_available": sorted(reference), **dict(counts),
            "coverage_per_baseline": dict(covered), "agreement_per_baseline": dict(per_baseline), "first_disagreements": samples}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=("validate-derivation", "build", "verify", "bench"))
    ap.add_argument("--allow-busy", action="store_true")
    a = ap.parse_args()
    if a.stage != "bench" and not a.allow_busy:
        P.require_idle_node(False)
    r = {"validate-derivation": validate_derivation, "build": build, "verify": verify}.get(a.stage, lambda: bench(a.allow_busy))()
    print(json.dumps({k: r.get(k) for k in ("status",)}, indent=2))
    return 0 if r.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
