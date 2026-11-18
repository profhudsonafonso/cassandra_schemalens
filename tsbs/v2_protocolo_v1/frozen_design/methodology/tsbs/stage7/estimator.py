"""Reproducible, non-ranking Stage 7 physical proxies for TSBS cpu-only.

Reads immutable Stage 3-6 artifacts. No observed benchmark result is used.
All numbers are estimates under the EXPLICIT synthetic scenario from the
Stage 3 EER adapter. Never call these values latency, real bytes or cost.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from methodology.tsbs.stage7.operators import describe

SECONDS_DAY = 86400
NS_DAY = 86400 * 10**9
NS_MINUTE = 60 * 10**9
NS_HOUR = 3600 * 10**9


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _save(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def _num(value: Any, *, positive: bool = False) -> float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError("Scenario values must be finite numeric")
    if (value <= 0 if positive else value < 0):
        raise ValueError("Scenario value violates positivity constraint")
    return float(value)


def _integer(value: Any, name: str, positive: bool = True) -> int:
    if type(value) is not int or (value <= 0 if positive else value < 0):
        raise ValueError(f"{name} must be a {'positive' if positive else 'nonnegative'} integer")
    return value


def _scenario(schema: dict, policy: dict) -> dict:
    scene = schema["scenario"]
    hosts = _integer(scene["scale_hosts"], "scale_hosts")
    interval = _integer(scene["log_interval_seconds"], "log_interval_seconds")
    days = _integer(scene["days"], "days")
    metrics = schema["entities"]["CPUReading"]["temporal"]["metrics"]
    if len(metrics) != 10 or len(set(metrics)) != 10:
        raise ValueError("Stage 7 supports the ten dense TSBS CPU metrics")
    if SECONDS_DAY % interval:
        raise ValueError("Reference scenario must use an interval dividing one day")
    start_raw = policy["scenario_start_utc"]
    if not isinstance(start_raw, str) or not start_raw.endswith("Z"):
        raise ValueError("scenario_start_utc requires a UTC ISO timestamp")
    start = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
    if start.tzinfo != timezone.utc or any((start.hour, start.minute, start.second, start.microsecond)):
        raise ValueError("The illustrative scenario starts at UTC midnight")
    return {"hosts": hosts, "interval_seconds": interval, "days": days,
            "metrics": list(metrics), "metric_count": len(metrics),
            "samples_per_day": SECONDS_DAY // interval,
            "start_day": (start.date() - datetime(1970, 1, 1, tzinfo=timezone.utc).date()).days,
            "scenario_source": "Stage 3 synthetic conceptual schema; not observed Cassandra data"}


def _days_by_bucket(scene: dict, bucket_days: int) -> list[int]:
    """Number of observed calendar days in each epoch-aligned physical bucket."""
    _integer(bucket_days, "bucket_days")
    out: dict[int, int] = {}
    for d in range(scene["start_day"], scene["start_day"] + scene["days"]):
        key = d // bucket_days
        out[key] = out.get(key, 0) + 1
    return list(out.values())


def _estimate_storage(spec: dict, scene: dict, policy: dict) -> dict:
    fam, variant = spec["family"], spec["variant"]
    h, m, s, d = scene["hosts"], scene["metric_count"], scene["samples_per_day"], scene["days"]
    daily = _days_by_bucket(scene, int(spec.get("chosen_modifiers", {}).get("M1_bucket_days", 1)))
    populated_minutes_per_day = min(1440, s)
    populated_hours_per_day = min(24, s)
    if fam == "TS0":
        total, partitions, max_rows = h * m * s * d, h * m, s * d
    elif fam == "TS1":
        total, partitions, max_rows = h * m * s * d, h * m * len(daily), s * max(daily)
    elif fam == "TS2":
        total, partitions, max_rows = h * s * d, h * len(daily), s * max(daily)
    elif fam == "TS5":
        if variant == "global_max_minute":
            total, partitions, max_rows = d * populated_minutes_per_day, d, populated_minutes_per_day
        elif variant in ("max_by_host_minute",):
            total, partitions, max_rows = h * m * d * populated_minutes_per_day, h * m * d, populated_minutes_per_day
        elif variant in ("max_by_host_hour", "sum_count_by_host_hour"):
            total, partitions, max_rows = h * m * d * populated_hours_per_day, h * m * d, populated_hours_per_day
        else:
            raise ValueError(f"Unknown Stage 4 TS5 variant {variant}")
    elif fam == "TS6":
        # TS6 primary key ((bucket_day, hour_ns), hostname): one per hour.
        total, partitions, max_rows = h * d * populated_hours_per_day, d * populated_hours_per_day, h
    else:
        raise ValueError(f"Stage 7 has no storage model for {fam}")
    if total <= 0 or partitions <= 0:
        raise ValueError("Invalid estimated footprint")
    col_bytes = 0
    for name, datatype in spec["columns"].items():
        if datatype in ("bigint", "double", "timestamp"):
            col_bytes += 8
        elif datatype in ("int", "date", "float"):
            col_bytes += 4
        elif datatype in ("text", "varchar"):
            col_bytes += _integer(policy["text_value_bytes_assumption"], "text_value_bytes_assumption")
        else:
            raise ValueError(f"Unsupported estimate-only CQL type {name}: {datatype}")
    proxy_row = col_bytes + _integer(policy["logical_row_overhead_bytes_assumption"], "logical_row_overhead_bytes_assumption", False)
    warnings: list[str] = []
    if max_rows >= _integer(policy["partition_rows_warning"], "partition_rows_warning"):
        warnings.append("PARTITION_ROW_COUNT_WARNING")
    if max_rows * proxy_row >= _integer(policy["partition_proxy_bytes_warning"], "partition_proxy_bytes_warning"):
        warnings.append("PARTITION_BYTE_PROXY_WARNING")
    if fam in {"TS5", "TS6"}:
        warnings.append("DERIVED_RECOMPUTATION_REQUIRES_DURABLE_CANONICAL_SOURCE")
    if fam == "TS6":
        warnings.append("CROSS_HOST_PARTITION_GROWS_WITH_HOST_CARDINALITY")
    return {
        "candidate_id": spec["candidate_id"], "family": fam,
        "estimated_total_rows": total, "estimated_partitions": partitions,
        "estimated_max_rows_in_one_partition": max_rows,
        "estimated_bytes_per_row_proxy": proxy_row,
        "estimated_max_partition_bytes_proxy": max_rows * proxy_row,
        "estimated_total_storage_bytes_proxy": total * proxy_row,
        "byte_proxy_is_measured": False,
        "estimate_covers": "ONLY candidate table (not required canonical source, indexes or replicas)",
        "warnings": sorted(warnings),
    }


def _writes(spec: dict, plan: dict, scene: dict) -> dict:
    cid, fam = spec["candidate_id"], spec["family"]
    if cid != plan["candidate_id"] or fam != plan["family"]:
        raise ValueError("WritePlan mismatch")
    m = scene["metric_count"]
    if fam in {"TS0", "TS1"}:
        rows = m
    elif fam == "TS2":
        rows = 1
    elif cid == "ts5_global_max_minute":
        rows = 1
    elif fam == "TS5":
        rows = m
    elif fam == "TS6":
        rows = 1
    else:
        raise ValueError(f"Unknown writer {cid}")
    derived = plan["write_strategy"] == "derived_recompute"
    if derived != (fam in {"TS5", "TS6"}):
        raise ValueError("Derived/raw writer classification disagreement")
    return {
        "candidate_id": cid, "family": fam,
        "logical_cpu_observation_metrics": m,
        "distinct_physical_group_rows_touched_by_new_observation_proxy": rows,
        "estimated_cql_mutations_per_event": "NOT_ESTIMATED_MAY_SKIP_UNCHANGED_GROUPS",
        "maintenance": "recompute_impacted_groups" if derived else "upsert_raw_rows",
        "canonical_source_required": derived,
        "recomputation_read_amplification": "UNKNOWN_NOT_BENCHMARKED" if derived else "NOT_APPLICABLE",
        "concurrent_crash_safe": False,
        "complete_write_production_feasible": False,
        "note": "Touched keys are NOT necessarily writes; Stage 6 deduplicates and compares rows before emitting mutations.",
    }


def _window_buckets(day: int, duration_days: float, bucket_days: int) -> int:
    # Conservative max intersection for half-open windows of this length,
    # independent of query's actual randomized start instant.
    return math.ceil(duration_days / bucket_days) + 1


def _read_proxy(q: dict, plan: dict, scene: dict, policy: dict) -> dict:
    kind, cid, family = plan["query_kind"], plan["candidate_id"], plan["family"]
    hosts = scene["hosts"] if plan["num_hosts"] == "ALL" else int(plan["num_hosts"])
    metrics = len(plan["query_metrics"])
    if hosts > scene["hosts"]:
        raise ValueError(f"Query {q['query_id']} requires {hosts} hosts; scenario only has {scene['hosts']}")
    raw_window = q["window_hours_from_tsbs"].strip()
    win_hours = float(raw_window) if raw_window else None
    if win_hours is not None and (not math.isfinite(win_hours) or win_hours <= 0):
        raise ValueError("Invalid query time window")
    samples = (math.ceil(win_hours * 3600 / scene["interval_seconds"])
               if win_hours is not None else None)
    strategy = plan["physical_strategy"]
    guards = sorted(set(plan["requirements"]))
    status = "CONDITIONAL_STRUCTURAL_ONLY_NO_CASSANDRA_VALIDATION"
    if family in {"TS5", "TS6"} and plan["alignment_guard"]:
        guards.append("runtime_exact_interval_alignment_required")
    if "complete_host_roster_from_same_loaded_dataset" in guards:
        guards.append("external_full_host_roster_not_materialized")
    if kind == "lastpoint":
        if strategy != "raw_scalar":
            raise ValueError("Only Stage 5 supported lastpoint raw scalar")
        requests = hosts * metrics
        rows = requests  # DESC LIMIT 1 per source series; assumes each exists.
    elif kind == "toprollup":
        if strategy == "derived_rollup":
            days = scene["days"]
            requests = days
            rows = 5 * days  # LIMIT 5 per day, before final merge
        elif strategy == "raw_scalar":
            requests = hosts * metrics
            rows = hosts * metrics * scene["samples_per_day"] * scene["days"]
        else:
            raise ValueError("Unexpected top-rollup strategy")
        guards.append("actual_random_end_time_not_available_for_exact_executions")
    else:
        if samples is None:
            raise ValueError(f"Stage 7 cannot model unbounded query {q['query_id']}")
        if strategy == "raw_scalar":
            requests, rows = hosts * metrics, hosts * metrics * samples
        elif strategy == "raw_scalar_bucketed":
            bucket = int(plan["bucket_days"])
            requests = hosts * metrics * _window_buckets(scene["start_day"], win_hours / 24, bucket)
            rows = hosts * metrics * samples
        elif strategy == "raw_wide_bucketed":
            bucket = int(plan["bucket_days"])
            requests = hosts * _window_buckets(scene["start_day"], win_hours / 24, bucket)
            rows = hosts * samples
        elif strategy == "derived_rollup":
            requests = hosts * metrics * _window_buckets(scene["start_day"], win_hours / 24, 1)
            resolution = NS_MINUTE if kind == "single" else NS_HOUR
            rows = hosts * metrics * math.ceil(win_hours * NS_HOUR / resolution)
        elif strategy == "dimension_rollup":
            requests = math.ceil(win_hours)
            rows = hosts * math.ceil(win_hours)
        else:
            raise ValueError(f"Unexpected read strategy {strategy}")
    if plan["family"] == "TS6" and kind != "double":
        raise ValueError("TS6 must be bound to double-groupby workload")
    risk_flags: list[str] = []
    if requests >= _integer(policy["max_read_request_warning"], "max_read_request_warning"):
        risk_flags.append("READ_FANOUT_WARNING")
    if rows >= _integer(policy["max_estimated_rows_read_warning"], "max_estimated_rows_read_warning"):
        risk_flags.append("ESTIMATED_ROWS_READ_WARNING")
    return {
        "plan_id": plan["plan_id"], "query_id": q["query_id"],
        "query_name": q["query_name"], "candidate_id": cid,
        "family": family, "physical_strategy": strategy,
        "estimation_scenario": "STAGE3_SYNTHETIC_SCENARIO",
        "host_count_assumed": hosts, "metric_count": metrics,
        "nominal_window_hours": win_hours,
        "estimated_cql_requests_upper_proxy": requests,
        "estimated_logical_rows_returned_upper_proxy": rows,
        "query_relative_frequency": None,
        "frequency_status": "MISSING_UNSPECIFIED",
        "coverage_status": status,
        "alignment_guard": plan["alignment_guard"],
        "requirements": sorted(set(guards)),
        "risk_flags": sorted(risk_flags),
        "measured_latency_ms": None,
        "measured_network_bytes": None,
        "stage5_coverage_retained": plan["coverage_class"],
    }


def _mozaffari(records: dict) -> list[dict]:
    if records.get("status") not in ("NOT_MEASURED_NO_PUBLISHED_COST_MODEL_IMPLEMENTED", "SUPPLIED_INDEPENDENT_EVIDENCE_NOT_IMPLEMENTED"):
        raise ValueError("Unsupported Mozaffari evidence status")
    seen = set()
    out = []
    for rec in records["records"]:
        key = (rec["relationship"], rec["direction"])
        if key in seen or any(not str(x).strip() for x in key):
            raise ValueError("Duplicate/empty directional relationship evidence")
        seen.add(key)
        values = {n: rec.get(n) for n in ("FR", "FU", "RC", "TI")}
        for name, val in values.items():
            if val is not None:
                _num(val, positive=(name in ("RC", "TI")))
        complete = all(v is not None for v in values.values()) and bool(rec.get("source"))
        out.append({"relationship": key[0], "direction": key[1],
                    **values, "source": rec.get("source"),
                    "evidence_complete": complete,
                    "cost_R": None, "cost_D": None,
                    "cost_model_status": "NOT_IMPLEMENTED_FROM_PUBLISHED_FORMULA" if complete else "UNAVAILABLE_MISSING_MEASURED_SIGNALS"})
    if not out:
        raise ValueError("Directional evidence must list at least one relationship")
    return out


def generate(root: Path, output: Path, policy_file: Path | None = None,
             mozaffari_file: Path | None = None) -> dict:
    root, output = root.resolve(), output.resolve()
    paths = {
        "candidate_specs": root / "outputs/tsbs_stage4/10_cassandra_candidate_specs.json",
        "stage4_manifest": root / "outputs/tsbs_stage4/14_stage4_manifest.json",
        "query_map": root / "outputs/tsbs_stage4/11_query_candidate_map.json",
        "query_plans": root / "outputs/tsbs_stage5/15_query_plans.json",
        "stage5_manifest": root / "outputs/tsbs_stage5/17_stage5_manifest.json",
        "write_plans": root / "outputs/tsbs_stage6/18_write_plans.json",
        "stage6_manifest": root / "outputs/tsbs_stage6/20_stage6_manifest.json",
        "manifest": root / "inputs/workload/query_manifest.csv",
        "schema": root / "inputs/schema/tsbs_cpu_only_schema.json",
        "cost_policy": policy_file.resolve() if policy_file else root / "config/tsbs_stage7_cost_policy.json",
        "mozaffari_evidence": mozaffari_file.resolve() if mozaffari_file else root / "inputs/tsbs_stage7_mozaffari_evidence.json",
    }
    for label, path in paths.items():
        if not path.is_file():
            raise FileNotFoundError(f"Run Stage 3-6 first or install Stage 7 add-on: missing {label}: {path}")
    specs = _json(paths["candidate_specs"])
    query_map = _json(paths["query_map"])
    query_plans = _json(paths["query_plans"])
    writes = _json(paths["write_plans"])
    stage4, stage5, stage6 = (_json(paths[name]) for name in ("stage4_manifest", "stage5_manifest", "stage6_manifest"))
    schema, policy, moz = (_json(paths[name]) for name in ("schema", "cost_policy", "mozaffari_evidence"))
    if any(stage.get("stage") != n for stage, n in ((stage4, 4), (stage5, 5), (stage6, 6))):
        raise ValueError("Unexpected prior stage manifest")
    if any(stage.get("C0_C9_used", stage.get("uses_cassandra_general_c0_c9")) is not False for stage in (stage4, stage5, stage6)):
        raise ValueError("General Cassandra C0-C9 scope unexpectedly present")
    for name, prior in (("candidate_specs", stage6), ("query_plans", stage6), ("schema", stage6)):
        if prior["source_files_sha256"][name] != _digest(paths[name]):
            raise ValueError(f"Source was modified since Stage 6: {name}")
    if stage5["source_files_sha256"]["stage4_manifest"] != _digest(paths["stage4_manifest"]):
        raise ValueError("Stage 4 manifest modified after Stage 5")
    if stage6["source_files_sha256"]["stage5_manifest"] != _digest(paths["stage5_manifest"]):
        raise ValueError("Stage 5 manifest modified after Stage 6")
    if len({s["candidate_id"] for s in specs}) != len(specs):
        raise ValueError("Duplicate physical candidate IDs")
    if len({p["plan_id"] for p in query_plans}) != len(query_plans):
        raise ValueError("Duplicate query plan IDs")
    if len({p["candidate_id"] for p in writes}) != len(writes):
        raise ValueError("Duplicate physical writer IDs")
    if len(specs) != stage4["candidate_count"] or len(query_plans) != stage5["query_plans"] or len(writes) != stage6["candidate_count"]:
        raise ValueError("Stage 4-6 counts diverged")
    spec_map = {s["candidate_id"]: s for s in specs}
    writer_map = {w["candidate_id"]: w for w in writes}
    if set(spec_map) != set(writer_map):
        raise ValueError("Every candidate requires its matching WritePlan")
    for cid, spec in spec_map.items():
        writer = writer_map[cid]
        for field in ("family", "table_name", "partition_key", "clustering_key"):
            if spec[field] != writer[field]:
                raise ValueError(f"Writer mismatch: {cid} / {field}")
    with paths["manifest"].open(encoding="utf-8", newline="") as source_stream:
        query_rows = list(csv.DictReader(source_stream))
    manifest_map = {q["query_id"]: q for q in query_rows}
    if len(manifest_map) != len(query_rows) or not manifest_map:
        raise ValueError("Repeated/empty query manifest")
    if {q["query_id"] for q in query_map} != set(manifest_map):
        raise ValueError("Stage 4 query map does not match workload")
    for q in query_rows:
        if q["relative_frequency"].strip() or q["frequency_source"] != "UNSPECIFIED":
            raise ValueError("This Stage 7 profile intentionally requires UNSPECIFIED workload frequencies")
    scene = _scenario(schema, policy)
    if policy.get("rank_candidates") is not False or policy.get("use_benchmark_latencies") is not False:
        raise ValueError("Stage 7 cannot rank or use benchmark outcomes")
    _num(policy["partition_rows_warning"], positive=True)
    _num(policy["partition_proxy_bytes_warning"], positive=True)
    _num(policy["max_read_request_warning"], positive=True)
    _num(policy["max_estimated_rows_read_warning"], positive=True)
    storage = [_estimate_storage(s, scene, policy) for s in sorted(specs, key=lambda s: s["candidate_id"])]
    write_stats = [_writes(s, writer_map[s["candidate_id"]], scene) for s in sorted(specs, key=lambda s: s["candidate_id"])]
    read_stats = []
    for plan in sorted(query_plans, key=lambda p: p["plan_id"]):
        cid = plan["candidate_id"]
        if cid not in spec_map or plan["family"] != spec_map[cid]["family"]:
            raise ValueError("Orphaned or mismatched QueryPlan")
        qid = plan["query_id"]
        if qid not in manifest_map or plan["query_name"] != manifest_map[qid]["query_name"]:
            raise ValueError("Orphaned or inconsistent query manifest row")
        read_stats.append(_read_proxy(manifest_map[qid], plan, scene, policy))
    expected_links = {(q["query_id"], cid) for q in query_map
                      for ids in q["candidate_ids_by_family"].values() for cid in ids}
    if expected_links != {(p["query_id"], p["candidate_id"]) for p in query_plans} or len(expected_links) != len(query_plans):
        raise ValueError("Stage 4-5 plan mapping disagreement")
    operators = [describe(spec_map[cid]) for cid in sorted(spec_map)]
    directional = _mozaffari(moz)
    output.mkdir(parents=True, exist_ok=True)
    _save(output / "21_physical_footprint_proxies.json", storage)
    _save(output / "22_write_amplification_proxies.json", write_stats)
    _save(output / "23_read_cost_proxies.json", read_stats)
    _save(output / "24_structural_transformations.json", operators)
    _save(output / "25_mozaffari_signal_audit.json", directional)
    read_columns = ["query_id", "query_name", "candidate_id", "family", "physical_strategy",
                    "host_count_assumed", "metric_count", "nominal_window_hours",
                    "estimated_cql_requests_upper_proxy", "estimated_logical_rows_returned_upper_proxy",
                    "alignment_guard", "frequency_status", "coverage_status", "risk_flags"]
    with (output / "26_query_cost_matrix.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, read_columns)
        writer.writeheader()
        for row in read_stats:
            writer.writerow({k: json.dumps(row[k]) if isinstance(row.get(k), list) else row.get(k) for k in read_columns})
    summary = {
        "stage": 7,
        "scope": "TSDB_ONLY_ANALYTICAL_COST_AND_FEASIBILITY_NOT_RANKED",
        "candidate_count": len(specs), "query_count": len(manifest_map), "query_plan_count": len(read_stats),
        "scenario": scene,
        "scenario_is_measured": False,
        "families": sorted({s["family"] for s in specs}),
        "physical_candidates_with_partition_warnings": [x["candidate_id"] for x in storage if any("PARTITION_" in w for w in x["warnings"])],
        "query_plans_with_fanout_warnings": [x["plan_id"] for x in read_stats if x["risk_flags"]],
        "aggregate_query_frequencies_available": False,
        "relative_workload_cost_computed": False,
        "candidate_ranking_performed": False,
        "Mozaffari_signals_complete": sum(r["evidence_complete"] for r in directional),
        "Mozaffari_Cost_R_Cost_D_computed": False,
        "NoSE_optimizers_executed": False,
        "Chen_optimizers_executed": False,
        "TS0_TS8_activation_modified": False,
        "C0_C9_used": False,
        "Cassandra_executed": False,
        "TSBS_benchmarked": False,
        "source_files_sha256": {k: _digest(v) for k, v in sorted(paths.items())},
        "limitations": [
            "Proxy rows and bytes are not observed costs; do not rank or select HBench from them without a prespecified scoring model.",
            "Total storage is per candidate table only, without required raw canonical source for TS5/TS6.",
            "Derived maintenance is reference in-memory recomputation, not durable or distributed-safe Cassandra writes.",
            "Alignment-sensitive plans are conditional; workload randomized time windows are not bound or checked here.",
            "An all-host query requires a complete real host catalog, which was not materialized.",
            "TS0 can create unbounded partitions if data is retained beyond the illustrative horizon.",
            "FR/FU/RC/TI directional evidence unavailable; Cost_R/Cost_D not calculated.",
            "No real hardware, network, compaction, replication, indexing, or benchmark result is used."
        ]
    }
    _save(output / "27_stage7_manifest.json", summary)
    return summary
