"""Stage 6 TS-only WritePlan generator consuming immutable Stage 4/5 artifacts.

The scope is writing CPU observations to EACH independently materializable
candidate, and describing the maintenance of derived TS5/TS6 tables. It is not
Stage 8 deployment consolidation; no family, variant or winner is selected here.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any

FAMILIES = {"TS0", "TS1", "TS2", "TS5", "TS6"}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def safe_identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError(f"Invalid CQL identifier: {value!r}")
    return value


def build_write_plan(spec: dict, metrics: list[str]) -> dict:
    cid, fam = spec["candidate_id"], spec["family"]
    if fam not in FAMILIES:
        raise ValueError(f"Unimplemented TSDB physical writer for {cid} ({fam})")
    columns = spec["columns"]
    names = list(columns)
    if not names or len(names) != len(set(names)):
        raise ValueError(f"Invalid or repeated columns: {cid}")
    for name in [spec["table_name"], *names, *spec["partition_key"], *spec["clustering_key"]]:
        safe_identifier(name)
    keys = list(spec["partition_key"] + spec["clustering_key"])
    if any(k not in columns for k in keys):
        raise ValueError(f"Missing write key definition: {cid}")
    if fam == "TS2" and set(metrics) - set(names):
        raise ValueError(f"Wide candidate incomplete: {cid}")
    if fam == "TS6" and any(f"sum_{m}" not in columns or f"count_{m}" not in columns for m in metrics):
        raise ValueError(f"Dimension candidate misses metric aggregates: {cid}")
    table = spec["table_name"]
    insert = (f"INSERT INTO {table} ({', '.join(names)}) VALUES "
              f"({', '.join('?' for _ in names)});")
    delete = f"DELETE FROM {table} WHERE " + " AND ".join(f"{k} = ?" for k in keys) + ";"
    if insert.count("?") != len(names) or delete.count("?") != len(keys):
        raise ValueError("CQL placeholder count mismatch")
    derived = fam in {"TS5", "TS6"}
    status = ("REFERENCE_RECOMPUTE_AFFECTED_GROUP_AFTER_CANONICAL_CHANGE"
              if derived else "REFERENCE_DIRECT_FULL_ROW_UPSERT")
    return {
        "candidate_id": cid, "family": fam, "variant": spec["variant"],
        "table_name": table, "column_order": names,
        "column_types": columns, "chosen_modifiers": spec.get("chosen_modifiers", {}),
        "partition_key": list(spec["partition_key"]),
        "clustering_key": list(spec["clustering_key"]), "identity_columns": keys,
        "write_cql": insert, "delete_cql": delete,
        "write_strategy": "derived_recompute" if derived else "raw_upsert",
        "reference_maintenance": status,
        "source_observation": "TSBS_CPU_ONLY_DENSE_10_METRICS",
        "idempotence": "canonical_logical_identity_deduplicated_before_materialization",
        "late_or_corrected_event_handling": (
            "recompute_impacted_group_from_reference_membership"
            if derived else "overwrite_existing_row_for_same_primary_key"),
        "requires": ["complete_canonical_cpu_event", "single_writer_ordered_event_stream",
                     "stable_host_timestamp_identity", "external_durable_replay_for_production"],
        "live_driver_implemented": False,
        "distributed_atomic_maintenance_proven": False,
        "benchmark_verified": False,
    }


def generate(root: Path, output: Path, policy_file: Path | None = None) -> dict:
    root = root.resolve()
    output = output.resolve()
    inputs = {
        "candidate_specs": root / "outputs/tsbs_stage4/10_cassandra_candidate_specs.json",
        "stage4_manifest": root / "outputs/tsbs_stage4/14_stage4_manifest.json",
        "query_plans": root / "outputs/tsbs_stage5/15_query_plans.json",
        "stage5_manifest": root / "outputs/tsbs_stage5/17_stage5_manifest.json",
        "schema": root / "inputs/schema/tsbs_cpu_only_schema.json",
        "stage4_policy": root / "config/tsbs_stage4_physical_policy.json",
        "stage6_policy": policy_file.resolve() if policy_file else root / "config/tsbs_stage6_write_policy.json",
    }
    for name, path in inputs.items():
        if not path.is_file():
            raise FileNotFoundError(f"Stage 6 requires existing {name}: {path}")
    specs = read_json(inputs["candidate_specs"])
    stage4, stage5 = read_json(inputs["stage4_manifest"]), read_json(inputs["stage5_manifest"])
    reads = read_json(inputs["query_plans"])
    schema = read_json(inputs["schema"])
    policy = read_json(inputs["stage6_policy"])
    metrics = schema["entities"]["CPUReading"]["temporal"]["metrics"]
    if len(metrics) != 10 or len(set(metrics)) != 10:
        raise ValueError("TSBS cpu-only must have exactly 10 distinct metrics")
    if schema["dataset"] != "TSBS cpu-only (DevOps)":
        raise ValueError("Only TSBS cpu-only currently supported")
    if stage4.get("stage") != 4 or stage5.get("stage") != 5:
        raise ValueError("Stage provenance mismatch")
    if stage4["input_files_sha256"]["conceptual_schema"] != sha256(inputs["schema"]):
        raise ValueError("Stage 4 conceptual schema changed")
    if stage4["input_files_sha256"]["physical_policy"] != sha256(inputs["stage4_policy"]):
        raise ValueError("Stage 4 policy changed")
    for src, prior in [("candidate_specs", "candidate_specs"),
                       ("stage4_manifest", "stage4_manifest")]:
        if stage5["source_files_sha256"][prior] != sha256(inputs[src]):
            raise ValueError(f"Stage 5 input fingerprint mismatch: {src}")
    if policy.get("policy_version") != "tsbs-stage6-write-v1":
        raise ValueError("Unsupported Stage 6 policy")
    forbidden = ["automatic_ttl_selection", "automatic_compaction_selection",
                 "automatic_sharding_selection", "use_cassandra_counter_columns",
                 "live_cassandra_execution_default"]
    if any(policy.get(k) is not False for k in forbidden):
        raise ValueError("Stage 6 demonstration forbids automatic physical decisions and live execution")
    if policy.get("write_semantics") != "last_input_event_wins_for_same_host_and_timestamp":
        raise ValueError("Unexpected duplicate/correction semantics")
    if not isinstance(specs, list) or len(specs) != stage4["candidate_count"]:
        raise ValueError("Stage 4 candidate count inconsistent")
    if len(reads) != stage5["query_plans"]:
        raise ValueError("Stage 5 QueryPlan count inconsistent")
    if any(r.get("write_plan_complete") is not False for r in reads):
        raise ValueError("Stage 5 QueryPlan source changed unexpectedly")
    if len({s["candidate_id"] for s in specs}) != len(specs):
        raise ValueError("Duplicate candidate in Stage 4")
    if {r["candidate_id"] for r in reads} != {s["candidate_id"] for s in specs}:
        raise ValueError("Missing candidate in QueryPlans or candidate specs")
    written = sorted((build_write_plan(s, metrics) for s in specs), key=lambda p: p["candidate_id"])
    lookup = {p["candidate_id"]: p for p in written}
    records = []
    for read in reads:
        writer = lookup[read["candidate_id"]]
        if writer["family"] != read["family"] or writer["table_name"] != read["table_name"]:
            raise ValueError("WritePlan and QueryPlan disagree on candidate identity")
        records.append({"query_id": read["query_id"], "query_name": read["query_name"],
                        "candidate_id": writer["candidate_id"], "family": writer["family"],
                        "write_strategy": writer["write_strategy"],
                        "reference_write_plan_generated": True,
                        "live_cassandra_write_verified": False,
                        "durable_distributed_maintenance_verified": False})
    output.mkdir(parents=True, exist_ok=True)
    write_json(output / "18_write_plans.json", written)
    cols = ["query_id", "query_name", "candidate_id", "family", "write_strategy",
            "reference_write_plan_generated", "live_cassandra_write_verified",
            "durable_distributed_maintenance_verified"]
    with (output / "19_write_coverage_matrix.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        writer.writerows(sorted(records, key=lambda x: (x["query_id"], x["candidate_id"])))
    result = {
        "stage": 6, "scope": "TSDB_ONLY_WRITE_PLAN_REFERENCE",
        "candidate_count": len(written), "query_plan_links": len(records),
        "raw_candidates": sum(p["write_strategy"] == "raw_upsert" for p in written),
        "derived_candidates": sum(p["write_strategy"] == "derived_recompute" for p in written),
        "families": sorted({p["family"] for p in written}),
        "all_candidates_have_reference_writers": len(lookup) == len(specs),
        "source_files_sha256": {name: sha256(path) for name, path in inputs.items()},
        "C0_C9_used": False, "original_activation_modified": False,
        "live_CQL_executed": False, "distributed_atomicity_proven": False,
        "correctness": "DETERMINISTIC_IN_MEMORY_TESTS_ONLY",
        "supported_logical_ops": ["upsert_full_observation", "delete_observation", "replay", "late_insert"],
        "deferred": ["Cassandra live driver and bulk loader", "durable checkpoint/recovery and concurrent writes",
                     "host roster persistence", "TTL/compaction/shard selection",
                     "Stage 7 workload-aware cost modeling", "Stage 8 consolidation/HBench", "Stage 9 performance measurement"],
        "cautions": [
            "Only CPU-only dense observations; not general DevOps or IoT.",
            "Reference materialization stores canonical observations and grouping indexes in process memory.",
            "Cassandra aggregate upserts are not atomic with raw writes; restart/replay not solved.",
            "No result may be interpreted as benchmark latency or production readiness.",
            "Stage 4 remains schema-only; Stage 5 read plans retain runtime coverage guards.",
        ],
    }
    write_json(output / "20_stage6_manifest.json", result)
    return result
