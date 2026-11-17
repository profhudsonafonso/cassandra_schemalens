"""Trace structural design transformations without implementing a NoSE optimizer.

The following labels describe transformations between the Stage 4 candidate
structures. They are NOT executable NoSE/Chen rewrite rules or their cost model.
"""
from __future__ import annotations


def describe(spec: dict) -> dict:
    family = spec["family"]
    variant = spec["variant"]
    if family == "TS0":
        label, src, target = "PRESERVE_RAW_SERIES", "logical CPU readings", "per-host/per-metric scalar series"
    elif family == "TS1":
        label, src, target = "BUCKET_RAW_SERIES", "unbucketed scalar series", f"{spec['chosen_modifiers']['M1_bucket_days']}-day partitions"
    elif family == "TS2":
        label, src, target = "COLOCATE_METRICS", "separate scalar metric readings", "one timestamp row with ten CPU metrics"
    elif family == "TS5":
        label, src, target = "MATERIALIZE_TEMPORAL_ROLLUP", "raw CPU readings", variant
    elif family == "TS6":
        label, src, target = "MATERIALIZE_DIMENSION_ROLLUP", "raw CPU readings", "hour x host SUM/COUNT"
    else:
        raise ValueError(f"No TSDB physical transformation description for family {family}")
    return {
        "candidate_id": spec["candidate_id"],
        "family": family,
        "operation_label": label,
        "input_representation": src,
        "output_representation": target,
        "inspiration_only": "NoSE materialization/decomposition and Chen/QPG-style workload design are related context, not implemented algorithms",
        "has_executable_rewrite_operator": False,
        "has_published_operator_cost_model": False,
    }
