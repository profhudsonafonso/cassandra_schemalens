"""Independent source oracle for the V8-011 matched-cohort replay.

The evaluator reads only canonical source tables and source projections.  It does
not call a deployment QueryPlan and applies the request mutations to an isolated
Python state before evaluating statements 34 and 35.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

ROOT = Path(__file__).resolve().parents[1]
SOURCE_REPLAY = ROOT / "outputs/final_run_v8_completion_011/matched_cohort/semantic/replay_raw"
OUT = ROOT / "outputs/final_run_v8_completion_011/oracle"
DEPLOYMENTS = ("AUTO_H_norm", "AUTO_H_c3c5c7", "nose_bidding", "qpg_bidding", "normalized", "expert")
WORKLOADS = ("browsing", "bidding", "update10x", "update100x")
COHORT_KEYSPACE = "rubis_v8_c008_auto_norm"


def canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        return {"$timestamp_utc_ms": value.replace(tzinfo=None).isoformat(timespec="milliseconds")}
    if isinstance(value, float):
        return {"$numeric_2dp": f"{round(value + 0.0, 2):.2f}"}
    if isinstance(value, dict):
        return {str(k): canonical(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [canonical(v) for v in value]
    return value


def decanonical(value: Any) -> Any:
    if isinstance(value, dict) and set(value) == {"$timestamp_utc_ms"}:
        return datetime.fromisoformat(value["$timestamp_utc_ms"])
    if isinstance(value, dict) and set(value) == {"$numeric_2dp"}:
        return float(value["$numeric_2dp"])
    if isinstance(value, dict):
        return {k: decanonical(v) for k, v in value.items()}
    if isinstance(value, list):
        return [decanonical(v) for v in value]
    return value


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(canonical(value), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_replay() -> dict[tuple[str, int, str], dict[str, dict[str, Any]]]:
    result = defaultdict(dict)
    for path in sorted(SOURCE_REPLAY.glob("*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                result[(row["workload"], int(row["repetition"]), row["logical_request_id"])][row["deployment_id"]] = row
    return result


def source_state(session) -> tuple[dict[str, dict[int, dict[str, Any]]], dict[str, Any]]:
    state = {"items": {}, "users": {}, "categories": {}, "regions": {}, "bids": {}}
    reads = []

    cohort_ids = {}
    for target_table, entity in (("items", "items"), ("users", "users"), ("categories", "categories"), ("regions", "regions"), ("bids", "bids")):
        column = "id"
        cql = f"SELECT {column} FROM {COHORT_KEYSPACE}.{target_table}"
        values = [int(row[column]) for row in session.execute(cql)]
        cohort_ids[entity] = tuple(sorted(values))
        reads.append({"cql": cql, "parameters": [], "rows": len(values), "role": "cohort_identity_only"})

    def point(table: str, identifier: int) -> dict[str, Any] | None:
        cql = f"SELECT * FROM rubis_full.{table} WHERE id=?"
        row = session.execute(session.prepare(cql), (identifier,)).one()
        reads.append({"cql": cql, "parameters": [identifier], "rows": 1 if row else 0})
        return dict(row) if row else None

    for identifier in cohort_ids["items"]:
        row = point("items", identifier)
        if row:
            state["items"][identifier] = row
    # Seller users are needed to evaluate the region predicate even when the
    # bounded deployment did not materialize that seller as a base user row.
    seller_ids = sorted({int(row["seller_id"]) for row in state["items"].values()})
    for identifier in seller_ids:
        if identifier not in state["users"]:
            row = point("users", identifier)
            if row:
                state["users"][identifier] = row
    for identifier in cohort_ids["users"]:
        if identifier not in state["users"]:
            row = point("users", identifier)
            if row:
                state["users"][identifier] = row
    for table, ids in (("categories", cohort_ids["categories"]), ("regions", cohort_ids["regions"])):
        for identifier in ids:
            row = point(table, identifier)
            if row:
                state[table][identifier] = row
    for bid_id in cohort_ids["bids"]:
        row = point("bids", bid_id)
        if row:
            state["bids"][bid_id] = {"id": row["id"], "date": row["bid_date"], "bid": row["bid"],
                                      "qty": row["qty"], "user_id": row["user_id"], "item_id": row["item_id"]}
    return state, {"source_keyspace": "rubis_full", "cohort_keyspace": COHORT_KEYSPACE,
                   "cohort_ids": {key: list(value) for key, value in cohort_ids.items()},
                   "source_reads": reads, "source_fingerprint": digest(state)}


def expected(state: dict[str, dict[int, dict[str, Any]]], statement_id: int, params: dict[str, Any]) -> list[dict[str, Any]]:
    since = params["since"]
    if isinstance(since, str):
        since = datetime.fromisoformat(since)
    if statement_id == 34:
        matched = []
        for bid in state["bids"].values():
            item = state["items"].get(int(bid["item_id"]))
            if int(bid["user_id"]) == int(params["user_id"]) and item and item["end_date"] >= since:
                matched.append({"items.id": item["id"], "items.name": item["name"]})
        return sorted({row["items.id"]: row for row in matched}.values(), key=lambda row: row["items.id"])[:25]
    if statement_id == 35:
        matched = []
        for item in state["items"].values():
            seller = state["users"].get(int(item["seller_id"]))
            if (item["category_id"] == params["category_id"] and seller and
                    seller["region_id"] == params["region_id"] and item["end_date"] >= since):
                matched.append({"items.id": item["id"], "items.name": item["name"], "items.end_date": item["end_date"]})
        return [row for row in sorted(matched, key=lambda row: (row["items.end_date"], row["items.id"]))[:25]]
    raise ValueError(statement_id)


def apply_mutation(state: dict[str, dict[int, dict[str, Any]]], statement_id: int, params: dict[str, Any], context: dict[str, Any]) -> None:
    new_id = int(params["new_id"])
    item_id = int(params["item_id"])
    user_id = int(params["user_id"])
    now = datetime.fromisoformat(params["date"])
    end = datetime.fromisoformat(params["end_date"])
    if statement_id == 10:
        state["items"][new_id] = {"id": new_id, "name": params["name"], "description": params["description"],
            "initial_price": float(params["initial_price"]), "quantity": int(params["quantity"]),
            "reserve_price": float(params["reserve_price"]), "buy_now": float(params["buy_now"]),
            "nb_of_bids": 0, "max_bid": 0.0, "start_date": now, "end_date": end,
            "seller_id": user_id, "category_id": int(params["category_id"])}
    elif statement_id == 20:
        state["bids"][new_id] = {"id": new_id, "date": now, "bid": float(params["bid"]), "qty": int(params["qty"]),
                                  "user_id": user_id, "item_id": item_id}
    elif statement_id == 22 and item_id in state["items"]:
        item = state["items"][item_id]
        item["nb_of_bids"] = int(item.get("nb_of_bids", 0)) + 1
        item["max_bid"] = max(float(item.get("max_bid", 0.0)), float(params["bid"]))
    elif statement_id == 15 and item_id in state["items"]:
        item = state["items"][item_id]
        item["quantity"] = max(0, int(item.get("quantity", 0)) - int(params["qty"]))
    context["last_mutation"] = statement_id


def run() -> dict[str, Any]:
    OUT.mkdir(parents=True, exist_ok=True)
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    session.row_factory = dict_factory
    session.default_timeout = 120
    try:
        initial, provenance = source_state(session)
        replay = load_replay()
        statement_rows = {"34": {"evaluated": 0, "passed": 0, "failed": 0}, "35": {"evaluated": 0, "passed": 0, "failed": 0}}
        failures = []
        for workload in WORKLOADS:
            for repetition in range(5):
                state = json.loads(json.dumps(initial, default=str))
                state = {table: {int(k): {key: datetime.fromisoformat(value) if key.endswith("_date") and isinstance(value, str) else value for key, value in row.items()} for k, row in rows.items()} for table, rows in state.items()}
                for (wl, rep, request_id), deployments in sorted(replay.items(), key=lambda entry: int(entry[0][2].split(":")[-1])):
                    if wl != workload or rep != repetition or set(deployments) != set(DEPLOYMENTS):
                        continue
                    exemplar = deployments["AUTO_H_norm"]
                    params = decanonical(exemplar["parameters"])
                    context = {}
                    for statement in exemplar["statements"]:
                        sid = int(statement["statement_id"])
                        if sid in (34, 35):
                            expected_rows = canonical(expected(state, sid, params))
                            for deployment in DEPLOYMENTS:
                                observed = next(row for row in deployments[deployment]["statements"] if int(row["statement_id"]) == sid)
                                key = str(sid)
                                statement_rows[key]["evaluated"] += 1
                                ok = observed["logical_rows"] == expected_rows
                                statement_rows[key]["passed"] += int(ok)
                                statement_rows[key]["failed"] += int(not ok)
                                if not ok and len(failures) < 100:
                                    failures.append({"workload": workload, "repetition": repetition, "logical_request_id": request_id,
                                                     "deployment_id": deployment, "statement_id": sid,
                                                     "expected": expected_rows, "actual": observed["logical_rows"]})
                        if sid in (10, 15, 20, 22):
                            apply_mutation(state, sid, params, context)
        resolution = {
            "statement_34": {"classification": "INDEPENDENT_SOURCE_ORACLE_EVALUATED", "description": "Source bids_by_item joined to source items; duplicate item IDs collapsed before LIMIT 25."},
            "statement_35": {"classification": "INDEPENDENT_SOURCE_ORACLE_EVALUATED", "description": "Source items joined to source seller users and filtered by source region/category/end date before deterministic LIMIT 25."},
            "remaining_differences": "Failures are preserved with expected and actual rows; no hash is relabeled as equivalent.",
        }
        result = {
            "run_id": "v8-completion-20261108-011",
            "status": "PASS" if not failures else "FAIL",
            "source_provenance": provenance,
            "cohort": {key: value for key, value in provenance["cohort_ids"].items()},
            "statements": [34, 35],
            "statement_validation": statement_rows,
            "failure_count": sum(v["failed"] for v in statement_rows.values()),
            "failures": failures,
        }
        (OUT / "STATEMENT_VALIDATION.json").write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n")
        (OUT / "42_DISCREPANCY_RESOLUTIONS.json").write_text(json.dumps(resolution, indent=2, sort_keys=True) + "\n")
        return result
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, sort_keys=True, default=str))
