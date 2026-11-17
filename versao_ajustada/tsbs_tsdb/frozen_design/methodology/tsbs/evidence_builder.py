"""Bridge TSBS logical DevOps workload to ORIGINAL SchemaLens TS0-TS8 rule features.

Reuses the unmodified SmartRetail SQL extractor and _derived_temporal_features.
Only source-specific canonicalization is added: TSBS time predicates use an
exclusive upper bound, whereas the demonstration extractor recognizes BETWEEN.
The rewritten SQL is NEVER used for executing a query; it is evidence-only.
"""
from __future__ import annotations

import re
from typing import Any

from methodology.smart_retail.sql_evidence import extract_sql_evidence
from methodology.smart_retail.evidence_builder import _derived_temporal_features
from methodology.tsbs.workload import Template

HALF_OPEN_RE = re.compile(
    r"\b(?P<field>[A-Za-z_]\w*\.[A-Za-z_]\w*)\s*>=\s*:(?P<start>[A-Za-z_]\w*)\s+"
    r"AND\s+(?P=field)\s*<\s*:(?P<end>[A-Za-z_]\w*)", re.IGNORECASE,
)


def sql_for_evidence_only(sql: str) -> str:
    """Normalize a half-open range into extractor-compatible syntax.

    BETWEEN here means *presence of a bounded time range*, not inclusive-end
    TSBS query semantics. ``source_sql`` always retains [start,end).
    """
    return HALF_OPEN_RE.sub(
        lambda m: f"{m.group('field')} BETWEEN :{m.group('start')} AND :{m.group('end')}", sql
    )


def build_tsbs_evidence(
    template: Template,
    sql: str,
    schema: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    ev_sql = extract_sql_evidence(sql_for_evidence_only(sql))
    if ev_sql.operation != "SELECT" or ev_sql.entities != ["CPUReading"]:
        raise ValueError(f"Unexpected TSBS SQL structure for {template.query_type}: {ev_sql.entities}")
    temporal = _derived_temporal_features(
        sql_ev=ev_sql, schema=schema, query_mode="TEMPORAL", policy=policy
    )
    # EXACT feature keys used by tsdb_activation_rules.json. The engine itself
    # is unchanged and NEVER sees query names, template kinds or outcomes.
    values: dict[str, Any] = {
        "operation": ev_sql.operation,
        "aggregate_function_count": ev_sql.aggregate_function_count,
        "group_attribute_count": len(ev_sql.group_fields),
        "equality_attributes": ev_sql.equality_fields,
        "inequality_attributes": ev_sql.between_fields,
        "ordering_attributes": [f"{x['field']} {x['direction']}" for x in ev_sql.order_fields],
        "group_attributes": ev_sql.group_fields,
        "distinct": ev_sql.distinct,
        "limit_value": ev_sql.limit_value,
        **temporal,
    }
    # Explicit, non-decision diagnostic: the original SmartRetail TS4 rule
    # recognizes a global single-series ORDER BY ... LIMIT 1, not latest per host.
    values["latest_per_group_semantics"] = template.kind == "lastpoint"
    values["late_arrival_metadata_status"] = (
        "ASSUMED_BY_USER" if schema["entities"]["CPUReading"]["temporal"].get("late_arrival_allowed")
        else "UNSPECIFIED_BY_TSBS"
    )
    return {
        "query_id": template.query_id,
        "query_name": template.query_type,
        "query_mode": "TEMPORAL",
        "source_sql": sql,
        "values": values,
        "provenance": {
            "template": template.query_type,
            "upstream": "TSBS cmd/tsbs_generate_queries/main.go + databases/cassandra/devops.go",
            "logical_sql": f"inputs/workload/sql/{template.query_id}_{template.query_type}.sql",
            "conceptual_schema": "inputs/schema/tsbs_cpu_only_schema.json (adapted model)",
            "sql_evidence": "original SmartRetail sql_evidence.py + half-open-range evidence adapter",
            "temporal_features": "original SmartRetail evidence_builder._derived_temporal_features",
            "rules": "original unchanged TS0-TS8 + M1-M5 JSON rule sets",
            "frequency": "UNSPECIFIED; not used in activation",
            "benchmark_latency": "NOT ACCESSED",
        }
    }
