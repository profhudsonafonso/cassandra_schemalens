"""Physical 2^3 factorial ablation of the SchemaLens C3 / C5 / C7 families (V8-012).

Replaces ``ablation_pipeline_011``.  That module copied one keyspace nine times
with identical DDL and labelled the copies as mechanism removals; its records
are not evidence.

What is ablated here, and why only this
---------------------------------------
The measured SchemaLens deployment AUTO_H_c3c5c7 is exactly AUTO_H_norm plus
three family tables, each with its own read routes and write maintenance:

* C3 ``items_selective``      reads: sids 3,5,13,14,18,21,24; write: 10; support read: 20
* C5 ``item_bid_shared``      reads: sids 4,6,19;               write: 20
* C7 ``items_region_bounded`` read:  sid 35;                    write: 10

Every subset F of {C3,C5,C7} is therefore a complete, executable deployment.
Each of the 8 cells gets a dedicated keyspace whose DDL is exported from the
Cassandra catalogue (clustering order preserved), a verified data copy, the
frozen 400,000-request protocol, and a per-request paired oracle against the
independently validated AUTO_H_norm raw run.  Cells f000 and f111 re-measure
the frozen AUTO_H deployments and act as a reproducibility check.

The other analytical mechanisms (sharedness, update exposure, access profile,
support-query sharing, relationships) have no switch point in the code that
produced H, so they are reported as not identifiable instead of being given
invented numbers.  See ``mechanism_records``.
"""
from __future__ import annotations

import argparse
import json
import statistics
from itertools import product
from pathlib import Path
from typing import Any

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

from . import protocol_012 as P
from .semantic_replay_011 import DeploymentAdapter008, source_closure

SOURCE = "rubis_v8_c008_auto_c3c5c7"
NORM_SOURCE = "rubis_v8_c008_auto_norm"
FAMILY_TABLE = {"C3": "items_selective", "C5": "item_bid_shared", "C7": "items_region_bounded"}
FAMILIES = ("C3", "C5", "C7")
READ_ROUTE_FAMILY = {**{sid: "C3" for sid in (3, 5, 13, 14, 18, 21, 24)}, **{sid: "C5" for sid in (4, 6, 19)}, 35: "C7"}
KIND_NORM, KIND_FULL = "schemalens_norm", "schemalens_c3c5c7"
OUT = P.RUN_DIR / "ablations"
# Complement pairs adjacent so slow drift does not align with any single factor.
RUN_ORDER = ("f000", "f111", "f100", "f011", "f010", "f101", "f001", "f110")
T_975 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262}


def cell_id(families: frozenset[str]) -> str:
    return "f" + "".join("1" if f in families else "0" for f in FAMILIES)


def cell_families(cid: str) -> frozenset[str]:
    return frozenset(f for f, bit in zip(FAMILIES, cid[1:]) if bit == "1")


CELLS = {cell_id(frozenset(f for f, on in zip(FAMILIES, bits) if on)): frozenset(f for f, on in zip(FAMILIES, bits) if on)
         for bits in product((0, 1), repeat=3)}


def keyspace_for(cid: str) -> str:
    return f"rubis_v8_c012_abl_{cid}"


def deployment_for(cid: str) -> str:
    fams = sorted(CELLS[cid])
    return "ABL_" + (cid + "_" + "".join(f.lower() for f in fams) if fams else cid + "_norm")


# --------------------------------------------------------------------------- adapter
class FactorialAdapter(DeploymentAdapter008):
    """AUTO_H_norm routes plus the read/write routes of the enabled families only."""

    def __init__(self, session, deployment_id: str, keyspace: str, closure: dict, families: frozenset[str]):
        super().__init__(session, deployment_id, keyspace, KIND_NORM, closure)
        self.families = frozenset(families)

    def _counters(self, res: dict) -> dict:
        res.update({"physical_reads": self.op_reads, "physical_writes": self.op_writes, "support_reads": self.op_support,
                    "rows_fetched": self.op_rows_fetched, "query_plans": self.op_plans, "write_plans": self.op_write_plans})
        return res

    def execute_statement(self, sid: int, params: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
        if sid == 20:
            return self._store_bid(params)
        family = READ_ROUTE_FAMILY.get(sid)
        self.kind = KIND_FULL if family in self.families else KIND_NORM
        try:
            res = super().execute_statement(sid, params, ctx)
        finally:
            self.kind = KIND_NORM
        if sid == 10 and (self.families & {"C3", "C7"}):
            ev = self.inserted_events[-1]
            assert ev[0] == "item" and ev[1] == int(params["new_id"]), ev
            _, new_id, cid, seller_region, _uid, end_dt = ev
            ks, name = self.keyspace, params["name"]
            if "C3" in self.families:
                self._w(f"INSERT INTO {ks}.items_selective (id,name,category_id) VALUES (?,?,?)", (new_id, name, cid), candidate_id="AUTO_C3_items_selective")
            if "C7" in self.families:
                self._w(f"INSERT INTO {ks}.items_region_bounded (region_id,category_id,end_date,item_id,name) VALUES (?,?,?,?,?)",
                        (seller_region, cid, end_dt, new_id, name), candidate_id="AUTO_C7_region_bounded")
            self._counters(res)
        return res

    def _store_bid(self, params: dict[str, Any]) -> dict[str, Any]:
        """StoreBid write plan of DeploymentAdapter008 with family-conditional maintenance."""
        self.op_reads = self.op_writes = self.op_support = self.op_rows_fetched = 0
        self.op_plans, self.op_write_plans = [], []
        ks = self.keyspace
        now_dt = self._dt(params["date"])
        uid, iid, new_id = int(params["user_id"]), int(params["item_id"]), int(params["new_id"])
        qty, bid_val = int(params.get("qty", 1)), float(params.get("bid", 0.0))
        self._w(f"INSERT INTO {ks}.bids (id,qty,bid,max_bid,bid_date,user_id,item_id) VALUES (?,?,?,?,?,?,?)",
                (new_id, qty, bid_val, bid_val, now_dt, uid, iid))
        self._w(f"INSERT INTO {ks}.bids_by_item (item_id,bid_date,bid_id,user_id,qty,bid) VALUES (?,?,?,?,?,?)",
                (iid, now_dt, new_id, uid, qty, bid_val))
        it = self._q(f"SELECT name,end_date FROM {ks}.items WHERE id=?", (iid,), support=True, candidate_id="AUTO_C0_items")
        iname = it[0]["name"] if it else f"item-{iid}"
        iend = it[0]["end_date"] if it else now_dt
        self._w(f"INSERT INTO {ks}.items_by_user_bid (user_id,end_date,item_id,name) VALUES (?,?,?,?)",
                (uid, iend, iid, iname), candidate_id="AUTO_C4_items_by_user_bid")
        if "C5" in self.families:
            iname_c5 = iname
            if "C3" in self.families:
                sel = self._q(f"SELECT name FROM {ks}.items_selective WHERE id=?", (iid,), support=True, candidate_id="AUTO_C3_items_selective")
                iname_c5 = sel[0]["name"] if sel else iname
            self._w(f"INSERT INTO {ks}.item_bid_shared (item_id,bid_date,bid_id,item_name,user_id,qty,bid) VALUES (?,?,?,?,?,?,?)",
                    (iid, now_dt, new_id, iname_c5, uid, qty, bid_val), candidate_id="AUTO_C5_item_bid_shared")
        is_new_item = (uid, iid) not in self.preexisting_user_bids
        self.inserted_events.append(("bid_008", new_id, iid, uid, now_dt, self._f32(bid_val), iend, is_new_item))
        return self._counters({"statement_id": 20, "logical_rows": [{"write_event": "StoreBid", "bid_id": new_id}], "row_count": 1})

    def reset_state(self) -> None:
        ks = self.keyspace
        for ev in self.inserted_events:
            if ev[0] == "item":
                _, new_id, cid, sreg, _uid, end_dt = ev
                if "C3" in self.families:
                    self.session.execute(self._prep(f"DELETE FROM {ks}.items_selective WHERE id=?"), (new_id,))
                if "C7" in self.families:
                    self.session.execute(self._prep(f"DELETE FROM {ks}.items_region_bounded WHERE region_id=? AND category_id=? AND end_date=? AND item_id=?"), (sreg, cid, end_dt, new_id))
            elif ev[0] == "bid_008" and "C5" in self.families:
                _, new_id, iid, _uid, now_dt, *_ = ev
                self.session.execute(self._prep(f"DELETE FROM {ks}.item_bid_shared WHERE item_id=? AND bid_date=? AND bid_id=?"), (iid, now_dt, new_id))
        self.kind = KIND_NORM
        super().reset_state()


# --------------------------------------------------------------------------- materialisation
def source_tables(cluster) -> tuple[list[str], dict[str, str]]:
    cluster.refresh_schema_metadata()
    src = cluster.metadata.keyspaces[SOURCE]
    norm = cluster.metadata.keyspaces[NORM_SOURCE]
    base = sorted(set(src.tables) - set(FAMILY_TABLE.values()))
    if set(base) != set(norm.tables):
        raise RuntimeError(f"base tables of {SOURCE} differ from {NORM_SOURCE}: {sorted(set(base) ^ set(norm.tables))}")
    return base, {t: src.tables[t].as_cql_query() for t in src.tables}


def materialize(cluster, session, cid: str) -> dict[str, Any]:
    base, _ = source_tables(cluster)
    tables = base + sorted(FAMILY_TABLE[f] for f in CELLS[cid])
    record = P.clone_keyspace(cluster, session, SOURCE, keyspace_for(cid), tables)
    norm_fp = P.keyspace_fingerprint(session, NORM_SOURCE)
    record.update({"cell": cid, "families": sorted(CELLS[cid]), "family_tables": {f: FAMILY_TABLE[f] for f in sorted(CELLS[cid])},
                   "base_tables_equal_to_auto_norm": {t: norm_fp.get(t) == record["fingerprint"][t] for t in base}})
    P.write_json(OUT / "materialization" / f"{cid}.json", record)
    return record


# --------------------------------------------------------------------------- run
def run(cells: list[str], *, label: str, measured: int, warmup: int, repetitions: int, allow_busy: bool,
        rematerialize: bool) -> int:
    node = P.require_idle_node(allow_busy)
    out = OUT / label
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    session.row_factory = dict_factory
    session.default_timeout = 180
    reference = P.load_reference_hashes()
    try:
        closure, source_state_id = source_closure(session)
        ledger = P.read_json(out / "RUN_LEDGER.json") if (out / "RUN_LEDGER.json").exists() else {"cells": {}}
        for cid in cells:
            if ledger["cells"].get(cid, {}).get("status") == "PASS":
                continue
            mat_path = OUT / "materialization" / f"{cid}.json"
            mat = materialize(cluster, session, cid) if (rematerialize or not mat_path.exists()) else json.loads(mat_path.read_text())
            adapter = FactorialAdapter(session, deployment_for(cid), keyspace_for(cid), closure, CELLS[cid])
            before = P.keyspace_fingerprint(session, keyspace_for(cid))
            started = P.now_iso()
            with P.BulkWriterWatch() as watch:
                result = P.run_protocol(adapter, deployment_for(cid), keyspace_for(cid), source_state_id, out / "raw",
                                        measured=measured, warmup=warmup, repetitions=repetitions)
            finished = P.now_iso()
            isolation = watch.report()
            restoration = P.state_restoration(before, P.keyspace_fingerprint(session, keyspace_for(cid)))
            oracle = P.paired_oracle([P.ROOT / f["path"] for f in result["raw_files"]], reference)
            storage = P.keyspace_storage(keyspace_for(cid))
            errors = sum(s["errors"] for s in result["summaries"])
            status = "PASS" if oracle["status"] == "PASS" and errors == 0 and restoration["status"] == "PASS" else "FAIL"
            if status == "PASS" and not allow_busy and not isolation["clean"]:
                status = "INVALID_CONCURRENT_LOAD"  # correct results, but timings are contaminated; rerun
            record = {"cell": cid, "families": sorted(CELLS[cid]), "deployment_id": deployment_for(cid), "status": status,
                      "started_at": started, "finished_at": finished, "node_state": node, "isolation": isolation, "state_restoration": restoration, "materialization": {k: mat[k] for k in ("keyspace", "tables", "ddl_sha256", "acknowledged_mutations", "base_tables_equal_to_auto_norm")},
                      "protocol": {k: result[k] for k in ("measured_requests_per_repetition", "warmup_requests_per_repetition", "repetitions")},
                      "summaries": result["summaries"], "raw_files": result["raw_files"], "oracle": oracle, "storage": storage, "errors": errors}
            P.write_json(out / f"{cid}_RESULT.json", record)
            ledger["cells"][cid] = {"status": status, "result": str((out / f"{cid}_RESULT.json").relative_to(P.ROOT))}
            ledger["order"] = ledger.get("order", []) + [cid]
            P.write_json(out / "RUN_LEDGER.json", ledger)
            print(json.dumps({"cell": cid, "status": status, "oracle": {k: oracle[k] for k in ("observations", "equal", "mismatch", "errors")}}), flush=True)
            if status != "PASS":
                return 1
        return 0
    finally:
        cluster.shutdown()


# --------------------------------------------------------------------------- analysis
def _rep_means(record: dict, metric: str) -> dict[tuple[str, int], float]:
    out = {}
    for s in record["summaries"]:
        n = s["measured_requests"] or 1
        out[(s["workload"], s["repetition"])] = s["mean_ms"] if metric == "latency" else s[metric] / n
    return out


def _ci(values: list[float]) -> dict[str, float]:
    n = len(values)
    mean = statistics.fmean(values)
    sd = statistics.stdev(values) if n > 1 else 0.0
    half = T_975.get(n - 1, 1.96) * sd / n ** 0.5 if n > 1 else 0.0
    return {"mean": mean, "sd": sd, "ci95_low": mean - half, "ci95_high": mean + half, "n": n}


def analyze(label: str) -> dict[str, Any]:
    out = OUT / label
    records = {cid: json.loads((out / f"{cid}_RESULT.json").read_text()) for cid in CELLS if (out / f"{cid}_RESULT.json").exists()}
    complete = len(records) == 8 and all(r["status"] == "PASS" for r in records.values())
    workloads = list(P.WORKLOADS)
    metrics = ("latency", "physical_reads", "physical_writes", "support_reads", "rows_fetched")
    cell_table = {}
    for cid, r in records.items():
        cell_table[cid] = {"families": r["families"], "storage_live_bytes": r["storage"]["live_bytes_total"],
                           "family_table_bytes": {t: r["storage"]["tables"].get(t, {}).get("live_bytes") for t in FAMILY_TABLE.values() if t in r["storage"]["tables"]},
                           "per_workload": {}}
        for w in workloads:
            row = {}
            for m in metrics:
                vals = [v for (wk, _), v in _rep_means(r, m).items() if wk == w]
                row[m] = _ci(vals) if vals else None
            ss = [s for s in r["summaries"] if s["workload"] == w]
            row["p95_ms_mean_over_reps"] = statistics.fmean(s["p95_ms"] for s in ss) if ss else None
            row["p99_ms_mean_over_reps"] = statistics.fmean(s["p99_ms"] for s in ss) if ss else None
            cell_table[cid]["per_workload"][w] = row
    effects: dict[str, Any] = {}
    if complete:
        reps = sorted({k[1] for k in _rep_means(records["f000"], "latency")})
        contrasts = {"C3": (0,), "C5": (1,), "C7": (2,), "C3xC5": (0, 1), "C3xC7": (0, 2), "C5xC7": (1, 2), "C3xC5xC7": (0, 1, 2)}
        for m in metrics:
            effects[m] = {}
            means = {cid: _rep_means(records[cid], m) for cid in CELLS}
            for w in workloads:
                effects[m][w] = {}
                for name, idx in contrasts.items():
                    per_rep = []
                    for rep in reps:
                        total = 0.0
                        for cid in CELLS:
                            sign = 1
                            for i in idx:
                                sign *= 1 if cid[1 + i] == "1" else -1
                            total += sign * means[cid][(w, rep)]
                        per_rep.append(total / 4.0)  # 2^(k-1) cells per level
                    effects[m][w][name] = _ci(per_rep)
        storage_effects = {}
        for name, idx in {"C3": (0,), "C5": (1,), "C7": (2,)}.items():
            total = 0.0
            for cid in CELLS:
                sign = 1 if cid[1 + idx[0]] == "1" else -1
                total += sign * cell_table[cid]["storage_live_bytes"]
            storage_effects[name] = total / 4.0
        effects["storage_live_bytes"] = storage_effects
    loo = {}
    if "f111" in records:
        full = _rep_means(records["f111"], "latency")
        for fam, cid in (("C3", "f011"), ("C5", "f101"), ("C7", "f110")):
            if cid not in records:
                continue
            reduced = _rep_means(records[cid], "latency")
            loo[fam] = {w: _ci([reduced[(w, r)] - full[(w, r)] for (wk, r) in full if wk == w]) for w in workloads}
    reproducibility = {}
    frozen = json.loads((P.FROZEN_011 / "full_scale_matched_cohort/BENCHMARK_SUMMARY.json").read_text())
    for cid, dep in (("f000", "AUTO_H_norm"), ("f111", "AUTO_H_c3c5c7")):
        if cid not in records:
            continue
        fz = {(s["workload"], s["repetition"]): s["mean_ms"] for s in frozen["summaries"] if s["deployment_id"] == dep}
        nw = _rep_means(records[cid], "latency")
        reproducibility[cid] = {"frozen_deployment": dep, "per_workload_mean_ms": {w: {"frozen": statistics.fmean(v for (wk, _), v in fz.items() if wk == w), "remeasured": statistics.fmean(v for (wk, _), v in nw.items() if wk == w)} for w in workloads}}
    result = {"run_id": P.RUN_ID, "label": label, "status": "PASS" if complete else ("PARTIAL" if records else "NOT_EXECUTED"),
              "design": "full 2^3 factorial over C3/C5/C7 family tables on the matched cohort",
              "cells_present": sorted(records), "cells": cell_table, "effects": effects, "leave_one_out_latency_delta_ms_vs_full_H": loo,
              "reproducibility_vs_frozen_011": reproducibility, "oracle": {cid: {k: r["oracle"][k] for k in ("status", "observations", "equal", "mismatch", "errors")} for cid, r in records.items()},
              "effect_definition": "Yates contrast: mean(cells with factor) - mean(cells without), computed per repetition and summarised with a t-based 95% CI (n=repetitions).",
              "mechanisms": mechanism_records(records)}
    P.write_json(out / "FACTORIAL_ANALYSIS.json", result)
    return result


def mechanism_records(records: dict[str, Any]) -> list[dict[str, Any]]:
    def measured(cid):
        r = records.get(cid)
        return "MEASURED" if r and r["status"] == "PASS" else "NOT_EXECUTED"
    not_identifiable = ("H is a hand-listed set of two deployments (design_space_005/006 and semantic_replay_011.DEPLOYMENTS); "
                        "engine.analyze computes this signal but activated_families never reads it, so removing it cannot change P, C or H "
                        "and no distinct physical deployment exists to measure.")
    return [
        {"ablation_id": "a01_no_c3_selective_normalization", "mechanism": "C3", "cell": "f011", "status": measured("f011"), "kind": "PHYSICAL_LEAVE_ONE_OUT"},
        {"ablation_id": "a02_no_c5_multiquery_sharing", "mechanism": "C5", "cell": "f101", "status": measured("f101"), "kind": "PHYSICAL_LEAVE_ONE_OUT"},
        {"ablation_id": "a03_no_c7_structural_relationship", "mechanism": "C7", "cell": "f110", "status": measured("f110"), "kind": "PHYSICAL_LEAVE_ONE_OUT"},
        {"ablation_id": "a04_no_sharedness", "mechanism": "sharedness", "status": "NOT_IDENTIFIABLE", "kind": "ANALYTICAL", "reason": not_identifiable,
         "evidence": "methodology/engine.py analyze(): 'sharedness' field vs 'fam' activation list"},
        {"ablation_id": "a05_no_update_exposure", "mechanism": "update_exposure", "status": "NOT_IDENTIFIABLE", "kind": "ANALYTICAL", "reason": not_identifiable,
         "evidence": "methodology/engine.py analyze(): 'update_exposure' field vs 'fam' activation list"},
        {"ablation_id": "a06_no_access_profile", "mechanism": "access_profile", "status": "NOT_IDENTIFIABLE", "kind": "ANALYTICAL",
         "reason": "Workload weights are used only to sample requests, never to select H; there is no selection step to ablate."},
        {"ablation_id": "a07_no_support_sharing", "mechanism": "support_query_sharing", "status": "NOT_IDENTIFIABLE", "kind": "ANALYTICAL",
         "reason": "Support reads are hand-coded per route in benchmark_005.DeploymentAdapter; no sharing switch exists."},
        {"ablation_id": "a08_no_relationships", "mechanism": "relationships", "status": "NOT_IDENTIFIABLE", "kind": "ANALYTICAL",
         "reason": "Relationship traversal is fixed in hand-written routes; removing it would make statements unanswerable rather than produce an alternative deployment."},
        {"ablation_id": "a09_no_canonicalization", "mechanism": "canonicalization", "status": "ANALYTICAL_ONLY", "kind": "ANALYTICAL",
         "reason": "Removing P->C consolidation changes only the analytical candidate count (C=P); H is hand-listed, so no physical deployment changes.",
         "evidence": "design_space_005.generate_candidate_search_space consolidation_map"},
    ]


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--cells", default=",".join(RUN_ORDER))
    r.add_argument("--label", default="protocol")
    r.add_argument("--measured", type=int, default=P.MEASURED)
    r.add_argument("--warmup", type=int, default=P.WARMUP)
    r.add_argument("--repetitions", type=int, default=P.REPETITIONS)
    r.add_argument("--allow-busy", action="store_true", help="correctness pilot only; timings are not evidence")
    r.add_argument("--reuse-keyspace", action="store_true", help="skip the fresh copy (debug only)")
    a = sub.add_parser("analyze")
    a.add_argument("--label", default="protocol")
    args = ap.parse_args()
    if args.cmd == "run":
        if args.label == "protocol" and (args.allow_busy or (args.measured, args.warmup, args.repetitions) != (P.MEASURED, P.WARMUP, P.REPETITIONS)):
            raise SystemExit("the 'protocol' label is reserved for the frozen protocol on an idle node")
        return run(args.cells.split(","), label=args.label, measured=args.measured, warmup=args.warmup,
                   repetitions=args.repetitions, allow_busy=args.allow_busy, rematerialize=not args.reuse_keyspace)
    result = analyze(args.label)
    print(json.dumps({k: result[k] for k in ("status", "cells_present", "oracle")}, indent=2, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
