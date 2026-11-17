"""Stage 5: QueryPlan generation from the immutable Stage 4 candidate artifacts.

All activation decisions are read from Stage 4, never recomputed here.
Coverage is STRUCTURAL and CONDITIONAL, not an assertion about Cassandra runtime,
write maintenance, or measured benchmark performance.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SUPPORTED = {"single", "maxall", "double", "high", "lastpoint", "toprollup"}
NS_MINUTE = 60 * 1_000_000_000
NS_HOUR = 60 * NS_MINUTE
NS_DAY = 24 * NS_HOUR


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def get_metric_names(schema: dict) -> list[str]:
    metrics = schema["entities"]["CPUReading"]["temporal"]["metrics"]
    if len(metrics) != 10 or len(set(metrics)) != 10 or any(not re.fullmatch(r"usage_[a-z_]+", m) for m in metrics):
        raise ValueError("Expected exactly 10 distinct TSBS cpu-only metric identifiers")
    return metrics


def physical_strategy(candidate: dict, kind: str) -> str:
    """Match a physical candidate to the logical operation class, not a query ID."""
    cid = candidate["candidate_id"]
    family = candidate["family"]
    if family == "TS0":
        return "raw_scalar"
    if family == "TS1" and kind == "high":
        return "raw_scalar_bucketed"
    if family == "TS2" and kind == "high":
        return "raw_wide_bucketed"
    if family == "TS5":
        patterns = {
            "single": "max_by_host_minute", "maxall": "max_by_host_hour",
            "double": "sum_count_by_host_hour", "toprollup": "global_max_minute",
        }
        if patterns.get(kind) and cid.endswith(patterns[kind]):
            return "derived_rollup"
    if family == "TS6" and kind == "double" and cid == "ts6_hour_host_dimension":
        return "dimension_rollup"
    raise ValueError(f"Candidate '{cid}' cannot serve query kind '{kind}'")


def _readable_cql(spec: dict, kind: str) -> str:
    """Only alter the TS0 read primitive where the logical query differs."""
    if spec["family"] == "TS0" and kind == "lastpoint":
        # DESC clustering in Stage 4; lastpoint requires one row *per metric*,
        # and a dense-metric precondition is checked at bind time.
        return (f"SELECT timestamp_ns, value FROM {spec['table_name']} "
                "WHERE hostname = ? AND metric_name = ? LIMIT 1;")
    if spec["family"] == "TS0" and kind == "toprollup":
        # No lower bound in the logical TSBS query. Avoid silently using one.
        return (f"SELECT timestamp_ns, value FROM {spec['table_name']} "
                "WHERE hostname = ? AND metric_name = ? AND timestamp_ns < ?;")
    return spec["read_cql_template"]


def _verify_cql_keys(spec: dict, cql: str) -> None:
    """Conservative read validation for the constrained CQL subset in Stage 4.

    It checks table identity, full partition-key equality, and that every
    clustering-key predicate occurs after partition equality. It DOES NOT
    substitute for validation against a live Cassandra cluster.
    """
    if "ALLOW FILTERING" in cql.upper() or ";" in cql.strip().rstrip(";"):
        raise ValueError("Unsafe/multi-statement CQL")
    assert_name = re.escape(spec["table_name"])
    if not re.search(rf"\bFROM\s+{assert_name}\s+WHERE\b", cql, re.IGNORECASE):
        raise ValueError("CQL table does not match candidate")
    where = re.split(r"\bWHERE\b", cql, flags=re.I)[1]
    for key in spec["partition_key"]:
        if not re.search(rf"\b{re.escape(key)}\s*=\s*\?", where):
            raise ValueError(f"Missing partition-key equality {key}")
    if spec["family"] == "TS0" and "LIMIT 1" in cql:
        if spec["clustering_order"] != ["DESC"]:
            raise ValueError("Latest-point plan requires DESC clustering")
    if "usage_user >" in where or "GROUP BY" in cql.upper():
        raise ValueError("Non-key filter/aggregation wrongly pushed into Cassandra CQL")


def _requirements(kind: str, strategy: str) -> tuple[list[str], str | None]:
    req = ["dataset_materialized_consistently", "query_parameters_provided"]
    if kind in {"single", "maxall", "double", "high", "lastpoint", "toprollup"}:
        req.append("host_identifiers_provided")
    if kind in {"double", "toprollup", "lastpoint"} or (kind == "high" and strategy != "raw_wide_bucketed"):
        # All-host high-cpu requires host roster too, as declared in the plan below.
        pass
    if kind in {"high", "lastpoint"} and strategy in {"raw_scalar", "raw_scalar_bucketed"}:
        req.append("cpu_metric_timestamps_are_dense")
    if kind == "lastpoint":
        req.append("cpu_metrics_same_latest_timestamp")
    if kind == "toprollup" and strategy == "derived_rollup":
        req.append("dataset_start_ns_provided")
    align = None
    if strategy == "derived_rollup":
        align = "hour" if kind in {"maxall", "double"} else "minute"
    if strategy == "dimension_rollup":
        align = "hour"
    if align:
        req.append(f"time_range_aligned_to_{align}")
    return req, align


def _operators(kind: str, strategy: str) -> list[str]:
    if kind in {"single", "maxall"}:
        return ["read_partitions", "group_by_utc_minute" if kind == "single" else "group_by_utc_hour",
                "max_by_metric_across_selected_hosts", "sort_bucket_ascending"]
    if kind == "double":
        return ["read_partitions", "group_by_hostname_utc_hour", "sum_count_then_divide_per_metric",
                "sort_bucket_then_hostname_ascending"]
    if kind == "high":
        return ["read_partitions", "pivot_metric_series_at_timestamp" if strategy != "raw_wide_bucketed" else "preserve_wide_rows",
                "filter_usage_user_strictly_gt_90", "sort_time_ascending"]
    if kind == "lastpoint":
        return ["read_latest_metric_for_every_host", "check_same_latest_timestamp",
                "assemble_one_cpu_row_per_host"]
    if kind == "toprollup":
        return ["read_all_data_before_exclusive_end", "max_by_utc_minute_across_all_hosts",
                "sort_bucket_descending", "take_first_five_buckets"]
    raise ValueError(f"Unexpected query class: {kind}")


def validate_logical_sql(query: dict, sql: str, metrics: list[str]) -> None:
    """Fail closed on drift between the constrained TSBS SQL and manifest.

    This is intentionally a small contract checker, NOT a general SQL parser.
    Query IDs never determine the result. The source SQL is logical intent,
    not executable Cassandra CQL.
    """
    kind = query["kind"]
    upper = " ".join(sql.upper().split())
    if "FROM CPUREADING" not in upper or not upper.startswith("SELECT "):
        raise ValueError(f"Unexpected logical SQL structure for {query['query_name']}")
    if kind != "lastpoint" and "C.EVENT_TS" not in upper:
        raise ValueError("Temporal query has no explicit source time axis")
    count = int(query["metric_count_from_tsbs"])
    for metric in metrics[:count]:
        if not re.search(rf"\bc\.{re.escape(metric)}\b", sql, flags=re.I):
            raise ValueError(f"Missing requested CPU metric {metric} in logical SQL")
    if kind in {"single", "maxall", "double", "toprollup"}:
        if "TIME_BUCKET(" not in upper or "GROUP BY" not in upper:
            raise ValueError("Aggregation template lost its temporal group-by")
        operation = "AVG(" if kind == "double" else "MAX("
        if operation not in upper:
            raise ValueError("Aggregation operator does not match TSBS template")
    if kind == "double" and "GROUP BY C.HOSTNAME" not in upper:
        raise ValueError("Double-groupby lost the hostname dimension")
    if kind == "high" and not re.search(r"C\.USAGE_USER\s*>\s*90(?:\.0+)?\b", upper):
        raise ValueError("High CPU threshold semantics changed")
    if kind == "lastpoint" and not (
        "ROW_NUMBER()" in upper and "PARTITION BY C.HOSTNAME" in upper
        and "C.EVENT_TS DESC" in upper
    ):
        raise ValueError("Latest-per-host semantics changed")
    if kind == "toprollup" and not (
        "LIMIT 5" in upper and "C.EVENT_TS < :END_TS" in upper
        and "ORDER BY BUCKET DESC" in upper
    ):
        raise ValueError("Last-five-buckets semantics changed")
    if kind in {"single", "maxall", "double", "high"} and not (
        "C.EVENT_TS >= :START_TS" in upper and "C.EVENT_TS < :END_TS" in upper
    ):
        raise ValueError("A half-open source time window is mandatory")


def build_plan(query: dict, spec: dict, metrics: list[str], physical_policy: dict) -> dict:
    kind = query["kind"]
    if kind not in SUPPORTED:
        raise ValueError(f"Unsupported query class {kind}")
    strategy = physical_strategy(spec, kind)
    metric_count = int(query["metric_count_from_tsbs"])
    if metric_count < 1 or metric_count > len(metrics):
        raise ValueError("Metric count outside TSBS cpu-only schema")
    selected = metrics[:metric_count]
    host_spec = query["host_count_from_tsbs"].strip()
    if host_spec != "ALL" and (not host_spec.isdecimal() or int(host_spec) < 1):
        raise ValueError(f"Invalid host count {host_spec}")
    req, alignment = _requirements(kind, strategy)
    if host_spec == "ALL":
        req.append("complete_host_roster_from_same_loaded_dataset")
    cql = _readable_cql(spec, kind)
    _verify_cql_keys(spec, cql)
    raw_days = spec.get("chosen_modifiers", {}).get("M1_bucket_days")
    if raw_days is not None and raw_days not in physical_policy["raw_bucket_days"] + physical_policy["wide_bucket_days"]:
        raise ValueError("Stage 4 bucket width disagrees with pinned policy")
    return {
        "plan_id": f"{query['query_id']}__{spec['candidate_id']}",
        "query_id": query["query_id"], "query_name": query["query_name"],
        "query_kind": kind, "candidate_id": spec["candidate_id"],
        "family": spec["family"], "physical_strategy": strategy,
        "table_name": spec["table_name"],
        "query_metrics": selected, "num_hosts": host_spec,
        "partition_key": spec["partition_key"], "clustering_key": spec["clustering_key"],
        "read_cql": cql, "read_bindings": list(spec["partition_key"]),
        "client_operators": _operators(kind, strategy),
        "requirements": sorted(req), "alignment_guard": alignment,
        "input_window": "none" if kind == "lastpoint" else ("exclusive_end_only" if kind == "toprollup" else "half_open_start_end"),
        "bucket_days": raw_days or spec.get("chosen_modifiers", {}).get("M1_storage_bucket_days"),
        "coverage_class": "STRUCTURAL_WITH_RUNTIME_GUARDS",
        "static_cql_validated": True,
        "runtime_executable_using_cassandra": False,
        "write_plan_complete": False,
        "benchmark_verified": False,
    }


def generate(root: Path, output: Path) -> dict:
    root, output = root.resolve(), output.resolve()
    locations = {
        "candidate_specs": root / "outputs/tsbs_stage4/10_cassandra_candidate_specs.json",
        "query_map": root / "outputs/tsbs_stage4/11_query_candidate_map.json",
        "read_primitives": root / "outputs/tsbs_stage4/12_query_read_primitives.json",
        "stage4_manifest": root / "outputs/tsbs_stage4/14_stage4_manifest.json",
        "query_manifest": root / "inputs/workload/query_manifest.csv",
        "schema": root / "inputs/schema/tsbs_cpu_only_schema.json",
        "stage4_physical_policy": root / "config/tsbs_stage4_physical_policy.json",
    }
    for label, path in locations.items():
        if not path.is_file():
            raise FileNotFoundError(f"Run Stage 4 first. Missing {label}: {path}")
    all_specs = load_json(locations["candidate_specs"])
    query_map = load_json(locations["query_map"])
    stage4 = load_json(locations["stage4_manifest"])
    manifest = load_csv(locations["query_manifest"])
    primitives = load_json(locations["read_primitives"])
    schema = load_json(locations["schema"])
    physical_policy = load_json(locations["stage4_physical_policy"])
    if stage4["stage"] != 4 or stage4["scope"] != "TSDB_ONLY_SCHEMA_INSTANTIATION":
        raise ValueError("Stage 4 manifest has unexpected provenance")
    if stage4["candidate_count"] != len(all_specs) or stage4["query_count"] != len(query_map):
        raise ValueError("Stage 4 input artifacts do not agree")
    for key, name in [("query_manifest", "query_manifest"), ("conceptual_schema", "schema"),
                      ("physical_policy", "stage4_physical_policy")]:
        if stage4["input_files_sha256"][key] != sha256(locations[name]):
            raise ValueError(f"Stage 4 source changed since generation: {key}")
    if {x["candidate_id"] for x in primitives} != {x["candidate_id"] for x in all_specs}:
        raise ValueError("Read primitives and physical candidates diverged")
    expected_primitive = {x["candidate_id"]: x for x in primitives}
    for candidate in all_specs:
        primitive = expected_primitive[candidate["candidate_id"]]
        if (primitive["family"] != candidate["family"] or
                primitive["query_template"] != candidate["read_cql_template"]):
            raise ValueError("Stage 4 read primitive diverged from its physical candidate")
    if len({x["candidate_id"] for x in all_specs}) != len(all_specs):
        raise ValueError("Duplicate physical candidate IDs")
    if len({x["query_id"] for x in manifest}) != len(manifest):
        raise ValueError("Duplicate manifest query IDs")
    if len({x["query_id"] for x in query_map}) != len(query_map):
        raise ValueError("Duplicate Stage 4 query IDs")
    specs = {x["candidate_id"]: x for x in all_specs}
    metadata = {x["query_id"]: x for x in manifest}
    if set(metadata) != {x["query_id"] for x in query_map}:
        raise ValueError("Stage 4 query map must cover the exact workload manifest")
    metrics = get_metric_names(schema)
    plans: list[dict] = []
    sql_source_hashes: dict[str, str] = {}
    for mapping in query_map:
        query = metadata[mapping["query_id"]]
        if query["query_name"] != mapping["query_name"] or query["kind"] != mapping["kind"]:
            raise ValueError("Stage 4 query map does not match input manifest")
        sql_basename = query["sql_file"]
        if Path(sql_basename).name != sql_basename or not sql_basename.endswith(".sql"):
            raise ValueError("Unsafe source SQL path in workload manifest")
        sql_path = root / "inputs/workload/sql" / sql_basename
        if not sql_path.is_file():
            raise FileNotFoundError(f"Missing TSBS logical SQL: {sql_path}")
        validate_logical_sql(query, sql_path.read_text(encoding="utf-8"), metrics)
        sql_source_hashes[sql_basename] = sha256(sql_path)
        if not mapping["stage4_mapping_complete"]:
            raise ValueError("Cannot verify query coverage with unmaterialized activated families")
        observed = set()
        for family, ids in mapping["candidate_ids_by_family"].items():
            if family not in set(mapping["active_families"]):
                raise ValueError("A candidate was linked to a nonactivated family")
            for cid in ids:
                if cid not in specs or specs[cid]["family"] != family:
                    raise ValueError(f"Stage 4 query/candidate mismatch {cid}")
                if cid in observed:
                    raise ValueError("Duplicate query candidate link")
                observed.add(cid)
                plans.append(build_plan(query, specs[cid], metrics, physical_policy))
        if not observed:
            raise ValueError(f"No physical read candidate for query {query['query_id']}")
    if len(plans) != stage4["query_family_candidate_links"]:
        raise ValueError("Stage 5 link cardinality must match Stage 4")
    plans.sort(key=lambda x: (x["query_id"], x["candidate_id"]))
    output.mkdir(parents=True, exist_ok=True)
    (output / "15_query_plans.json").write_text(json.dumps(plans, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    cols = ["query_id", "query_name", "family", "candidate_id", "physical_strategy", "coverage_class",
            "requires_complete_host_roster", "alignment_guard", "runtime_executable_using_cassandra", "write_plan_complete"]
    with (output / "16_coverage_matrix.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=cols)
        writer.writeheader()
        for p in plans:
            writer.writerow({k: ("complete_host_roster_from_same_loaded_dataset" in p["requirements"])
                              if k == "requires_complete_host_roster" else p.get(k, "") for k in cols})
    families = {x["family"] for x in plans}
    result = {
        "stage": 5, "scope": "TSDB_ONLY_QUERY_PLAN_STRUCTURAL_VALIDATION",
        "queries": len(query_map), "candidate_count": len(all_specs), "query_plans": len(plans),
        "families": sorted(families), "coverage_class": "STRUCTURAL_WITH_RUNTIME_GUARDS",
        "per_query_has_structural_plan": all(any(p["query_id"] == qid for p in plans) for qid in metadata),
        "source_files_sha256": {k: sha256(v) for k, v in locations.items()},
        "logical_sql_sha256": dict(sorted(sql_source_hashes.items())),
        "alignment_guarded_plans": sum(bool(p["alignment_guard"]) for p in plans),
        "raw_plans_without_time_alignment_restriction": sum(not p["alignment_guard"] for p in plans),
        "generated_read_binder": True, "CQL_executed_on_server": False,
        "database_result_equivalence_verified": False, "complete_write_plan": False,
        "materialized_aggregate_consistency_verified": False,
        "benchmarked": False, "C0_C9_used": False,
        "cautions": [
            "QueryPlans require runtime bindings and source data loaded by a future Stage 6 WritePlan.",
            "Queries spanning partial rollup windows must be rejected or use a separately justified raw fallback.",
            "All-host queries need a complete host roster from the SAME loaded dataset; Stage 4 does not materialize one.",
            "TSBS cpu-only is assumed dense for scalar multi-metric pivot/latest-point behavior.",
            "Static CQL shape checks and mock-data tests do NOT establish live Cassandra executability.",
            "TS0 is NOT equivalent to original TSBS daily-bucketed series_id baseline.",
        ],
    }
    (output / "17_stage5_manifest.json").write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return result
