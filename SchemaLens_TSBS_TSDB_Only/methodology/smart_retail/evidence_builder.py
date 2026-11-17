"""Merge SmartRetail inputs and derive Cassandra + time-series evidence.

Inputs
------
1. SmartRetail conceptual schema JSON.
2. Workload manifest CSV.
3. One SQL file per logical query.
4. SchemaLens-like conceptual evidence CSV for the running example.
5. Temporal policy JSON (only for generic thresholds such as hot-series volume).

Outputs of this module are evidence dictionaries.  It does not activate a
single family.  Activation is performed later by the generic rule engine.
"""
from __future__ import annotations

from typing import Any

from .sql_evidence import SqlEvidence, extract_sql_evidence


def _bool_label_high(value: str | None) -> bool:
    return bool(value and "HIGH" in str(value).upper())


def _entity_of(field: str) -> str:
    return field.split(".", 1)[0] if "." in field else ""


def _attribute_of(field: str) -> str:
    return field.split(".", 1)[1] if "." in field else field


def _series_entity(sql_ev: SqlEvidence, schema: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    for entity in sql_ev.entities:
        temporal = schema["entities"].get(entity, {}).get("temporal")
        if temporal:
            return entity, temporal
    return None, None


def _time_order_details(sql_ev: SqlEvidence, series_entity: str | None, time_attribute: str | None) -> tuple[bool, bool]:
    if not series_entity or not time_attribute:
        return False, False
    target = f"{series_entity}.{time_attribute}"
    ordered = False
    desc = False
    for item in sql_ev.order_fields:
        if item["field"] == target:
            ordered = True
            desc = item["direction"] == "DESC"
    return ordered, desc


def _derived_temporal_features(
    sql_ev: SqlEvidence,
    schema: dict[str, Any],
    query_mode: str,
    policy: dict[str, Any],
) -> dict[str, Any]:
    series_entity, temporal = _series_entity(sql_ev, schema)
    is_temporal = query_mode.upper() == "TEMPORAL" and temporal is not None

    if not is_temporal:
        return {
            "is_temporal_query": False,
            "temporal_source_entity": "",
            "time_attribute": "",
            "series_identity_fields": [],
            "series_identity_bound": False,
            "has_time_range": False,
            "time_ordered": False,
            "latest_only": False,
            "metric_fields_projected": [],
            "metric_count": 0,
            "dimension_equality_fields": [],
            "has_dimension_equality_anchor": False,
            "returns_raw_observations": False,
            "time_bucketed": False,
            "non_time_group_dimensions": [],
            "non_time_group_dimension_count": 0,
            "series_discovery": False,
            "model_segment_requested": False,
            "retention_days": None,
            "estimated_events_per_day": None,
            "append_oriented": False,
            "late_arrival_allowed": False,
            "hot_series": False,
        }

    assert series_entity is not None and temporal is not None
    time_attribute = temporal["time_attribute"]
    identity_fields = list(temporal["series_identity"])
    metric_names = set(temporal["metrics"])
    dimension_names = set(temporal["dimensions"])

    eq_on_series = {
        _attribute_of(field)
        for field in sql_ev.equality_fields
        if _entity_of(field) == series_entity and _attribute_of(field) in identity_fields
    }
    series_identity_bound = all(field in eq_on_series for field in identity_fields)

    target_time_field = f"{series_entity}.{time_attribute}"
    has_time_range = target_time_field in sql_ev.between_fields
    time_ordered, time_desc = _time_order_details(sql_ev, series_entity, time_attribute)
    latest_only = time_ordered and time_desc and sql_ev.limit_value == 1

    metric_fields = [
        field
        for field in sql_ev.projected_fields
        if _entity_of(field) == series_entity and _attribute_of(field) in metric_names
    ]
    # A MODEL_SEGMENTS expression references fields inside a function. The
    # generic field extractor still sees the metric field, but the query is not
    # classified as a raw-observation read below.
    dimension_eq = [
        field
        for field in sql_ev.equality_fields
        if _entity_of(field) == series_entity and _attribute_of(field) in dimension_names
    ]

    group_dimensions = [
        field
        for field in sql_ev.group_fields
        if _entity_of(field) == series_entity and _attribute_of(field) in dimension_names
    ]

    projected_identity_fields = {
        _attribute_of(field)
        for field in sql_ev.projected_fields
        if _entity_of(field) == series_entity and _attribute_of(field) in identity_fields
    }
    series_discovery = (
        sql_ev.distinct
        and bool(projected_identity_fields)
        and not metric_fields
        and bool(dimension_eq)
    )

    returns_raw = (
        not sql_ev.model_segment_requested
        and sql_ev.aggregate_function_count == 0
        and not series_discovery
        and bool(metric_fields)
    )

    events_per_day = temporal.get("estimated_events_per_day")
    threshold = policy["hot_series_events_per_day_threshold"]
    hot_series = events_per_day is not None and float(events_per_day) >= float(threshold)

    return {
        "is_temporal_query": True,
        "temporal_source_entity": series_entity,
        "time_attribute": time_attribute,
        "series_identity_fields": identity_fields,
        "series_identity_bound": series_identity_bound,
        "has_time_range": has_time_range,
        "time_ordered": time_ordered,
        "latest_only": latest_only,
        "metric_fields_projected": metric_fields,
        "metric_count": len(metric_fields),
        "dimension_equality_fields": dimension_eq,
        "has_dimension_equality_anchor": bool(dimension_eq),
        "returns_raw_observations": returns_raw,
        "time_bucketed": sql_ev.time_bucketed,
        "non_time_group_dimensions": group_dimensions,
        "non_time_group_dimension_count": len(group_dimensions),
        "series_discovery": series_discovery,
        "model_segment_requested": sql_ev.model_segment_requested,
        "retention_days": temporal.get("retention_days"),
        "estimated_events_per_day": events_per_day,
        "append_oriented": bool(temporal.get("append_oriented")),
        "late_arrival_allowed": bool(temporal.get("late_arrival_allowed")),
        "hot_series": hot_series,
    }


def build_evidence(
    manifest_rows: list[dict[str, str]],
    conceptual_rows: list[dict[str, str]],
    sql_by_query: dict[str, str],
    schema: dict[str, Any],
    temporal_policy: dict[str, Any],
) -> list[dict[str, Any]]:
    conceptual_by_id = {row["query_id"]: row for row in conceptual_rows}
    records: list[dict[str, Any]] = []

    for manifest in manifest_rows:
        query_id = manifest["query_id"]
        if query_id not in conceptual_by_id:
            raise ValueError(f"Missing conceptual evidence for {query_id}")
        if query_id not in sql_by_query:
            raise ValueError(f"Missing SQL text for {query_id}")

        conceptual = conceptual_by_id[query_id]
        sql_ev = extract_sql_evidence(sql_by_query[query_id])
        root = conceptual["selected_root"]
        root_keys = set(schema["entities"].get(root, {}).get("identity", []))

        root_identity_bound = any(
            _entity_of(field) == root and _attribute_of(field) in root_keys
            for field in sql_ev.equality_fields
        )
        has_nonroot_equality_anchor = any(
            _entity_of(field) and _entity_of(field) != root
            for field in sql_ev.equality_fields
        )

        simple_identity_lookup = (
            sql_ev.operation == "SELECT"
            and len(sql_ev.entities) == 1
            and root_identity_bound
            and len(sql_ev.equality_fields) == 1
            and len(sql_ev.between_fields) == 0
            and len(sql_ev.order_fields) == 0
        )

        schema_rc = int(conceptual["schema_rc"])
        selected_depth = int(conceptual["selected_document_depth"])
        selected_re = int(conceptual["selected_re"])
        semantics_text = f"{conceptual['dominant_semantic_group']} {conceptual['dominant_semantic_detail']}".lower()

        temporal_features = _derived_temporal_features(
            sql_ev=sql_ev,
            schema=schema,
            query_mode=manifest["query_mode"],
            policy=temporal_policy,
        )

        values: dict[str, Any] = {
            # Common identity / audit fields
            "query_id": query_id,
            "query_name": manifest["query_name"],
            "query_mode": manifest["query_mode"],
            "source_sql": sql_by_query[query_id].strip(),
            "stage_query_family": True,
            # Cassandra-family evidence compatible with Step 3 rules
            "operation": sql_ev.operation,
            "analytical_root": root,
            "entity_count": len(sql_ev.entities),
            "canonical_entities": sql_ev.entities,
            "canonical_relationship_count": schema_rc,
            "schema_rc": schema_rc,
            "selected_document_depth": selected_depth,
            "selected_re": selected_re,
            "semantic_group": conceptual["dominant_semantic_group"],
            "semantic_detail": conceptual["dominant_semantic_detail"],
            "associative_semantics": "associative" in semantics_text,
            "containment_semantics": "containment" in semantics_text or "contains" in semantics_text,
            "high_sharedness": _bool_label_high(conceptual["query_sharedness_class"]),
            "high_update_exposure": _bool_label_high(conceptual["query_volatility_class"]),
            "equality_attributes": sql_ev.equality_fields,
            "inequality_attributes": sql_ev.between_fields,
            "text_attributes": [],
            "ordering_attributes": [f"{item['field']} {item['direction']}" for item in sql_ev.order_fields],
            "group_attributes": sql_ev.group_fields,
            "root_identity_bound": root_identity_bound,
            "simple_identity_lookup": simple_identity_lookup,
            "identity_bound_write": False,
            "has_nonroot_equality_anchor": has_nonroot_equality_anchor,
            "has_access_requirement": bool(sql_ev.equality_fields or sql_ev.between_fields or sql_ev.order_fields or schema_rc),
            "inequality_count": len(sql_ev.between_fields),
            "text_predicate_count": 0,
            "ordering_count": len(sql_ev.order_fields),
            "group_attribute_count": len(sql_ev.group_fields),
            "aggregate_function_count": sql_ev.aggregate_function_count,
            "relative_frequency": manifest.get("relative_frequency", ""),
            "frequency_source": manifest.get("frequency_source", "UNSPECIFIED"),
            "frequency_available": bool(manifest.get("relative_frequency")) and manifest.get("frequency_source") != "UNSPECIFIED",
            "frequency_used_in_current_activation": False,
            # Extra parser evidence for explanation
            "distinct": sql_ev.distinct,
            "limit_value": sql_ev.limit_value,
            **temporal_features,
        }

        provenance = {
            "conceptual_schema": "inputs/schema/smart_retail_schema.json",
            "conceptual_evidence": "inputs/schema_lens/conceptual_evidence.csv (manually prepared demo evidence, not benchmark output)",
            "logical_workload": f"inputs/workload/sql/{manifest['sql_file']}",
            "query_metadata": "inputs/workload/query_manifest.csv",
            "derived_cassandra_features": "methodology/smart_retail/evidence_builder.py",
            "derived_temporal_features": "methodology/smart_retail/evidence_builder.py + sql_evidence.py",
            "frequency": "UNSPECIFIED and not used in activation",
        }
        records.append({"query_id": query_id, "values": values, "provenance": provenance})

    return records
