"""Run ONLY original SchemaLens time-series rules TS0-TS8 and modifiers M1-M5.

Usage (from repository root): python3 -m methodology.tsbs.run_tsbs
No Cassandra general-family code or benchmark latency is invoked.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import shutil
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from methodology.common.rule_engine import activate, evaluate_rule
from methodology.tsbs.evidence_builder import build_tsbs_evidence
from methodology.tsbs.workload import METRICS, TAGS, TEMPLATES, TSBS_COMMIT, logical_sql, validate_workload

ROOT = Path(__file__).resolve().parents[2]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SchemaLens TS0-TS8 (TSBS cpu-only), without Cassandra C0-C9.")
    parser.add_argument("--scale", type=int, default=100, help="Number of generated TSBS hosts (>=32 for all 16 templates)")
    parser.add_argument("--interval-seconds", type=int, default=60, help="Logical TSBS measurement period per host")
    parser.add_argument("--days", type=int, default=3, help="TSBS logical dataset duration in days")
    parser.add_argument("--retention-days", type=int, default=None, help="Explicit experiment retention policy; NOT inferred from TSBS")
    parser.add_argument("--allow-late-arrival", action="store_true", help="Explicit scenario assumption; NOT an upstream TSBS default")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "outputs" / "tsbs_run")
    return parser.parse_args(argv)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def csv_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if isinstance(value, bool):
        return str(value).lower()
    return value


def dump_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = fields or (list(rows[0]) if rows else [])
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: csv_cell(row.get(k)) for k in keys})


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_conceptual_schema(scale: int, interval: int, days: int, retention: int | None, late: bool) -> dict[str, Any]:
    # In the TSBS CPU-only generator: one CPU observation per host and interval;
    # fields=10 metrics, and tags are static host-level metadata.
    events_per_host_day = 86400 / interval
    return {
        "dataset": "TSBS cpu-only (DevOps)",
        "model_status": "CONCEPTUAL_ADAPTATION_NOT_OFFICIAL_EER",
        "tsbs_source_commit": TSBS_COMMIT,
        "scenario": {
            "scale_hosts": scale,
            "log_interval_seconds": interval,
            "days": days,
            "observations_per_host_day_estimated": events_per_host_day,
            "all_hosts_observations_per_day_estimated": scale * events_per_host_day,
            "metrics_per_observation": len(METRICS),
            "default_retention": "NOT_SPECIFIED_BY_TSBS" if retention is None else "EXPLICIT_EXPERIMENT_POLICY",
            "default_late_arrivals": "NOT_SPECIFIED_BY_TSBS" if not late else "EXPLICIT_EXPERIMENT_SCENARIO",
        },
        "entities": {
            "Host": {
                "identity": ["hostname"],
                "attributes": list(TAGS),
                "note": "Host tag metadata as defined in TSBS host.go; no separate HostMetadata entity required.",
            },
            "CPUReading": {
                "identity": ["hostname", "event_ts"],
                "attributes": ["hostname", "event_ts", *TAGS[1:], *METRICS],
                "note": "Logical observation; TSBS Cassandra baseline physically splits metric fields by series.",
                "temporal": {
                    "series_identity": ["hostname"],
                    "time_attribute": "event_ts",
                    "metrics": list(METRICS),
                    "dimensions": list(TAGS),  # hostname is both series identity and GROUP BY dimension.
                    "retention_days": retention,
                    "estimated_events_per_day": events_per_host_day,
                    "append_oriented": True,
                    "late_arrival_allowed": True if late else None,
                },
            },
        },
        "relationships": [{
            "name": "host_generates_cpu_reading", "left": "Host", "right": "CPUReading",
            "cardinality": "1:N", "semantic": "measurement_generated_by_host",
            "note": "TSDB-only activation ignores generic Cassandra containment families.",
        }],
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    validate_workload(args.scale, args.interval_seconds, args.days)
    if args.retention_days is not None and args.retention_days < 1:
        raise ValueError("--retention-days must be a positive integer when supplied")
    root = ROOT
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    schema_path = root / "inputs/schema/tsbs_cpu_only_schema.json"
    manifest_path = root / "inputs/workload/query_manifest.csv"
    config_paths = {
        "tsdb_rules": root / "config/tsdb_activation_rules.json",
        "tsdb_catalog": root / "config/tsdb_family_catalog.json",
        "modifier_rules": root / "config/temporal_modifier_rules.json",
        "temporal_policy": root / "config/temporal_policy.json",
    }
    ts_rules = read_json(config_paths["tsdb_rules"])
    ts_catalog = read_json(config_paths["tsdb_catalog"])
    modifier_rules = read_json(config_paths["modifier_rules"])
    policy = read_json(config_paths["temporal_policy"])
    schema = build_conceptual_schema(args.scale, args.interval_seconds, args.days, args.retention_days, args.allow_late_arrival)
    dump_json(schema_path, schema)

    manifest_rows: list[dict[str, Any]] = []
    records = []
    sql_paths = []
    for template in TEMPLATES:
        sql = logical_sql(template)
        sql_rel = f"inputs/workload/sql/{template.query_id}_{template.query_type}.sql"
        sql_path = root / sql_rel
        sql_path.parent.mkdir(parents=True, exist_ok=True)
        sql_path.write_text(sql, encoding="utf-8")
        sql_paths.append(sql_path)
        manifest_rows.append({
            "query_id": template.query_id, "query_name": template.query_type,
            "query_mode": "TEMPORAL", "source": "TSBS_OFFICIAL_DEVOPS_TEMPLATE",
            "kind": template.kind, "metric_count_from_tsbs": template.metric_count,
            "host_count_from_tsbs": template.host_count if template.host_count else "ALL",
            "window_hours_from_tsbs": template.window_hours,
            "description": template.description, "sql_file": sql_path.name,
            "relative_frequency": "", "frequency_source": "UNSPECIFIED",
        })
        records.append(build_tsbs_evidence(template, sql, schema, policy))
    dump_csv(manifest_path, manifest_rows)

    temporal_families = [r["family"] for r in ts_rules["rules"]]
    catalog_families = [f["family"] for f in ts_catalog["families"]]
    if temporal_families != catalog_families:
        raise ValueError("TS rule families do not match original TS family catalogue order")
    modifier_families = [r["family"] for r in modifier_rules["rules"]]
    full = []
    matrix = []
    modifier_matrix = []
    explanation_rows = []
    for record in records:
        # ORIGINAL engine; only TS rules. No C0-C9 rules/configs are loaded.
        values = record["values"]
        active_ts, ts_trace = activate(values, ts_rules)
        active_mods = []
        mod_trace = []
        for mod_rule in modifier_rules["rules"]:
            result = evaluate_rule(values, mod_rule, active_ts + active_mods)
            if result.passed:
                active_mods.append(result.family)
            mod_trace.append(asdict(result))
        item = {
            "query_id": record["query_id"],
            "query_name": record["query_name"],
            "query_mode": "TEMPORAL",
            "tsdb_families": active_ts,
            "temporal_modifiers": active_mods,
            "evidence": values,
            "provenance": record["provenance"],
            "tsdb_rule_trace": ts_trace,
            "modifier_rule_trace": mod_trace,
        }
        full.append(item)
        matrix.append({
            "query_id": item["query_id"], "query_name": item["query_name"],
            **{f: f in active_ts for f in temporal_families},
            "active_families": active_ts,
        })
        modifier_matrix.append({
            "query_id": item["query_id"], "query_name": item["query_name"],
            **{f: f in active_mods for f in modifier_families},
            "active_modifiers": active_mods,
            "bucket_granularity_status": "DEFERRED_TO_PHYSICAL_DESIGN" if "M1" in active_mods else "NOT_ACTIVATED",
        })
        explanation_rows.append({
            "query_id": item["query_id"], "query_name": item["query_name"],
            "active_ts_families": active_ts, "active_modifiers": active_mods,
            "series_identity_bound": values["series_identity_bound"],
            "returns_raw_observations": values["returns_raw_observations"],
            "has_time_range": values["has_time_range"],
            "metric_count": values["metric_count"],
            "aggregate_function_count": values["aggregate_function_count"],
            "time_bucketed": values["time_bucketed"],
            "non_time_group_dimension_count": values["non_time_group_dimension_count"],
            "latest_only": values["latest_only"],
            "latest_per_group_semantics": values["latest_per_group_semantics"],
            "late_arrival_metadata_status": values["late_arrival_metadata_status"],
            "benchmark_latency_used": False,
        })

    files = [("schema", schema_path), ("manifest", manifest_path)] + list(config_paths.items()) + [("logical_sql", p) for p in sql_paths]
    source_paths = [
        ("rule_engine", root / "methodology/common/rule_engine.py"),
        ("source_sql_evidence", root / "methodology/smart_retail/sql_evidence.py"),
        ("source_evidence_builder", root / "methodology/smart_retail/evidence_builder.py"),
        ("adapter", root / "methodology/tsbs/evidence_builder.py"),
        ("workload_generator", root / "methodology/tsbs/workload.py"),
    ]
    # Keep a self-contained copy of input/config snapshots in this run.
    # If another scenario later regenerates inputs/ under repository root,
    # this run is still auditable against immutable hashes and snapshots.
    for _, original in files:
        snapshot = out / "inputs_snapshot" / original.relative_to(root)
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(original, snapshot)
    inventory = [{
        "input_label": label, "path": str(path.relative_to(root)),
        "sha256": sha256(path), "size_bytes": path.stat().st_size,
    } for label, path in files + source_paths]
    dump_csv(out / "01_input_inventory.csv", inventory)
    dump_csv(out / "02_combined_evidence.csv", [{
        "query_id": rec["query_id"], "query_name": rec["query_name"], **rec["values"]
    } for rec in records])
    # Intentionally no 03_cassandra_activation_matrix.csv.
    dump_csv(out / "04_tsdb_activation_matrix.csv", matrix,
             ["query_id", "query_name", *temporal_families, "active_families"])
    dump_csv(out / "05_temporal_modifier_matrix.csv", modifier_matrix,
             ["query_id", "query_name", *modifier_families, "active_modifiers", "bucket_granularity_status"])
    dump_json(out / "06_full_activation_trace.json", full)
    dump_csv(out / "07_query_explanations.csv", explanation_rows)
    dump_csv(out / "08_family_catalog.csv", [{
        "family": f["family"], "name": f["name"], "purpose": f["purpose"]
    } for f in ts_catalog["families"]])

    counts_ts = Counter(f for row in full for f in row["tsdb_families"])
    counts_mod = Counter(f for row in full for f in row["temporal_modifiers"])
    summary = {
        "mode": "TSDB_ONLY",
        "template_count": len(full),
        "upstream_tsbs_commit": TSBS_COMMIT,
        "case": "cpu-only / DevOps",
        "scenario": schema["scenario"],
        "families_activated_counts": dict(sorted(counts_ts.items())),
        "modifier_activated_counts": dict(sorted(counts_mod.items())),
        "not_activated_ts_families": sorted(set(temporal_families) - set(counts_ts)),
        "rules_modified": False,
        "uses_cassandra_general_rules": False,
        "uses_benchmark_latency": False,
        "not_implemented": ["Cassandra CQL schema generation", "query-plan/write-plan coverage", "HBench", "TSBS execution"],
        "important_caveats": [
            "Q15 lastpoint = last reading per host; not the SmartRetail global LIMIT 1 pattern; TS4 stays inactive.",
            "SQL is logical evidence input, not native Cassandra CQL.",
            "M2 and M5 require explicit experiment policies, absent in default TSBS metadata.",
            "TSBS time predicates are half-open; extractor normalization is evidence-only.",
            "Q09 needs >=32 hosts; reference scenario uses 100.",
        ],
    }
    dump_json(out / "09_run_manifest.json", summary)
    print("=== SchemaLens TSBS / TS-only activation ===")
    print(f"Templates: {len(full)} | scale={args.scale}, interval={args.interval_seconds}s, days={args.days}")
    for row in full:
        print(f"{row['query_id']} {row['query_name']:<27} TS={','.join(row['tsdb_families']) or '-':<13} M={','.join(row['temporal_modifiers']) or '-'}")
    print("Family activations:", dict(sorted(counts_ts.items())))
    print("Modifier activations:", dict(sorted(counts_mod.items())))
    print("Saved:", out)
    return summary


def main() -> None:
    try:
        run(parse_args())
    except (ValueError, KeyError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
