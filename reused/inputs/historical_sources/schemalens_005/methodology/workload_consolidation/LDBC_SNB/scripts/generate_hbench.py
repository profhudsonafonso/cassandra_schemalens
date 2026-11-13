#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SchemaLens LDBC SNB matrix-driven physical consolidation and Hbench generation.

This version ports the Algorithms 4 and 5 implementation pattern used by
`run_imdb_physical_consolidation_from_matrix_paper_v3.py` to LDBC SNB while
preserving the existing LDBC artifact bundle as the source of concrete MongoDB
candidate specifications and executable query-plan evidence.

Paper-oriented workflow
-----------------------
  analytical matrix rows -> explanation records X
  -> candidate specifications P
  -> ConsolidateAndValidate(P, W, rho_max)                 [Algorithm 4]
       exact canonical merge
       controlled same-scope fusion
       conservative artifact-backed executable coverage
       optional structural dominance pruning
  -> ConstructAndBenchmark(C, cover, W, k)                 [Algorithm 5]
       compatible complete workload-level configurations H
       complete-deployment deduplication
       Pareto preselection Hbench

Important LDBC adaptation
-------------------------
The existing LDBC artifact bundle contains concrete candidate_id values and their
materialization metadata. A *shared* SchemaLens candidate C may therefore be:

  * exact: one or more artifact candidates with the same physical layout; or
  * fused: a deterministic union of compatible same-scope artifact candidates.

A fused shared candidate intentionally receives a new C_FUSED_* id. It is NOT
looked up as a legacy artifact candidate. Instead, the output stores its source
artifact candidate ids and a deployable physical specification. The companion
Hbench deployment runner must materialize the union represented by each selected
shared candidate. For backward routing compatibility, `route_candidate_ids_json`
still maps each workload query to a concrete artifact candidate id whose existing
execution-plan row provides the route metadata.

Coverage
--------
`origin(c)` and `cover(c)` are kept separate. Because the current LDBC bundle is
artifact-backed, ValidEquivalentPlan(q, c) is implemented conservatively:

  1. every source artifact candidate contributes the official queries for which
     the execution plan already contains that candidate id;
  2. physically equivalent artifact candidates (same deployable signature) may
     contribute additional executable query routes;
  3. fusion preserves the union of those validated component routes because the
     accepted fusion only adds compatible physical structures.

This is intentionally more conservative than guessing cross-query equivalence from
query names or family labels.

No historical or previous benchmark latency is read anywhere by this script.
Hbench is derived only from the analytical matrix, current artifact-backed physical specifications,
coverage/compatibility, and the predeclared structural penalties.

V12 analytical-reduction corrections:
1. No benchmark measurements (old or new) are read before Hbench selection.
2. After coverage validation, a safe structural dominance pass removes candidates only
   when another candidate has the same compatibility profile, covers a superset of
   queries, and requires a subset of the dominated physical structures.
3. Algorithm-5 search states are canonicalised by physical deployment rather than by
   candidate provenance. Different decompositions that materialise the same physical
   state are therefore explored once.
4. Complete deployment identity excludes source/shared candidate ids and repeated scope
   provenance. Route_h is deterministically derived from every validated candidate whose
   physical requirements are contained in the complete deployment.
5. Search-limit reporting is conservative: reaching a bound is reported as truncated.
6. V12 replaces exhaustive candidate-order enumeration in Algorithm 5 with a two-phase
   physical-state search: exact Pareto-frontier discovery using a safe monotone objective
   bound, followed (only if needed) by uniform-cost discovery of the next-best structural
   alternatives until Hbench reaches k. This certifies the analytical preselection without
   enumerating every dominated complete H.
These changes reduce redundant fusion/composition paths without consulting latency.

Outputs
-------
  explanation_records.csv
  candidate_specifications_P.csv
  exact_merge_report.csv
  fusion_report.csv
  final_canonical_merge_report.csv
  shared_candidates_C_before_safe_pruning.csv
  shared_candidates_C.csv
  coverage_matrix.csv
  dominance_pruning_report.csv
  complete_workload_configurations_H_before_deployment_dedup.csv
  deployment_dedup_report.csv
  complete_workload_configurations_H.csv
  hbench_topk.csv
  hbench_route_plan.csv
  shared_candidate_materialization_specs.json
  hbench_deployment_specs.json
  candidate_selection_warnings.csv
  summary.md
  hbench_generation_manifest.json

Example
-------
python3 run_ldbc_snb_generate_hbench_topk_from_matrix_v14_physical_storage_no_old_results.py \
  --matrix-csv ldbc_analytical_matrix.csv \
  --artifacts-dir /path/to/ldbc_snb_sf0_1_mongo_benchmark_bundle \
  --execution-plan benchmark_execution_plan.csv \
  --out-dir ldbc_hbench_matrix_outputs_v3 \
  --scale-label sf0.1 \
  --rho-max 3 \
  --top-k 5 \
  --max-fusion-size 2
"""
from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import heapq
import itertools
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

def canon(x: Any) -> str:
    return json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def short_hash(x: Any, n: int = 10) -> str:
    return hashlib.sha1(canon(x).encode("utf-8")).hexdigest()[:n]


def unique_sorted(xs: Iterable[Any]) -> List[str]:
    return sorted({str(x) for x in xs if x is not None and str(x) != "" and str(x).lower() != "nan"})


def parse_list_cell(value: Any) -> List[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    if isinstance(value, (list, tuple, set)):
        return unique_sorted(value)
    if isinstance(value, dict):
        return unique_sorted(value.keys())
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return []
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
        for loader in (ast.literal_eval, json.loads):
            try:
                obj = loader(s)
                if isinstance(obj, (list, tuple, set)):
                    return unique_sorted(obj)
                if isinstance(obj, dict):
                    return unique_sorted(obj.keys())
            except Exception:
                pass
    for sep in [";", "|", ","]:
        if sep in s:
            return unique_sorted(part.strip().strip("'\"") for part in s.split(sep))
    return [s.strip().strip("'\"")]


def get_first_existing(row: pd.Series, names: Sequence[str], default: Any = "") -> Any:
    for name in names:
        if name in row and pd.notna(row[name]):
            return row[name]
    return default


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            clean: Dict[str, Any] = {}
            for k, v in row.items():
                if isinstance(v, (list, dict, tuple, set)):
                    clean[k] = canon(v)
                else:
                    clean[k] = v
            writer.writerow(clean)


def normalize_official_id(value: Any) -> str:
    s = str(value).strip()
    if not s:
        return s
    parts = s.replace("-", "_").split("_")
    for part in parts:
        up = part.upper()
        if up.startswith(("IC", "IS", "INS")) and any(ch.isdigit() for ch in up):
            return up
    return s.upper()


def normalize_g(value: Any) -> str:
    s = str(value).strip().upper()
    if not s:
        return s
    if s.startswith("G"):
        return s
    if s.isdigit():
        return f"G{s}"
    return s


def nested_list(obj: Dict[str, Any], *keys: str) -> List[str]:
    """Return the first useful list-like value among candidate metadata keys."""
    for key in keys:
        if key not in obj:
            continue
        value = obj.get(key)
        if isinstance(value, dict):
            # For maps, retain keys as physical path names.
            return unique_sorted(value.keys())
        vals = parse_list_cell(value)
        if vals:
            return vals
    return []


# -----------------------------------------------------------------------------
# Analytical-matrix parsing and activation alpha(M_i, G)
# -----------------------------------------------------------------------------

def infer_families_from_ldbc_row(q: Dict[str, Any], fallback_families: List[str]) -> Tuple[List[str], str]:
    explicit = q.get("activated_families_raw", [])
    if explicit:
        return unique_sorted(normalize_g(x) for x in explicit), "matrix_column"

    text = " ".join([
        q.get("official_id", ""),
        q.get("query_name", ""),
        q.get("query_class", ""),
        q.get("query_type", ""),
        q.get("semantic_group", ""),
        q.get("semantic_detail", ""),
        " ".join(q.get("entities", [])),
        " ".join(q.get("relationships", [])),
    ]).lower()

    official_id = q.get("official_id", "").upper()
    if official_id.startswith("INS") or any(x in text for x in ["insert", "write", "update"]):
        fams = ["G0", "G3", "G4", "G6"]
    elif official_id == "IC7" or any(x in text for x in ["like", "liked", "recent liker"]):
        fams = ["G0", "G3", "G4", "G6"]
    elif any(x in text for x in ["knows", "friend", "foaf", "traversal", "path", "graph"]):
        fams = ["G0", "G3", "G4", "G6"]
    elif any(x in text for x in ["forum", "post", "comment", "reply", "contain", "membership"]):
        fams = ["G0", "G2", "G3", "G4", "G7", "G8", "G9"]
    elif any(x in text for x in ["tag", "place", "organisation", "organization", "descriptor"]):
        fams = ["G0", "G1", "G2", "G3"]
    else:
        fams = fallback_families or []

    # The concrete artifact plan is the admissible LDBC instantiation. Do not
    # activate a family that cannot be instantiated from the supplied bundle.
    if fallback_families:
        available = set(fallback_families)
        filtered = [g for g in fams if g in available]
        if filtered:
            return unique_sorted(filtered), "inferred_intersect_execution_plan"
        return unique_sorted(fallback_families), "fallback_from_execution_plan"
    return unique_sorted(fams), "inferred_no_plan_filter"


def row_to_query(row: pd.Series, plan_families_by_official: Dict[str, List[str]]) -> Dict[str, Any]:
    qname = str(get_first_existing(row, ["query_name", "query", "qid", "Query", "official_title", "official_id"], "")).strip()
    official = normalize_official_id(get_first_existing(row, ["official_id", "query_id", "qid", "query_name", "Query"], qname))
    if not official and qname:
        official = normalize_official_id(qname)
    if not qname:
        qname = official
    if not official:
        raise ValueError("Analytical matrix row does not contain official_id/query_name-like information.")

    root = str(get_first_existing(row, ["selected_root", "root", "ri", "root_entity", "Selected_root"], "")).strip()
    qclass = str(get_first_existing(row, ["generic_class", "query_class", "class", "workload_class", "access_class"], "")).strip()
    qtype = str(get_first_existing(row, ["query_type", "operation", "op_type", "operation_type"], "")).strip()
    sem = str(get_first_existing(row, ["dominant_semantic_group", "semantic_group", "dominant_semantic_type", "sem"], "")).strip()
    sem_detail = str(get_first_existing(row, ["dominant_semantic_detail", "semantic_detail", "semantic_types_touched"], "")).strip()
    sharedness = str(get_first_existing(row, ["query_sharedness_class", "sharedness", "query_observed_sharedness_level"], "")).strip()
    volatility = str(get_first_existing(row, ["query_volatility_class", "volatility", "query_update_volatility_level"], "")).strip()

    entities = parse_list_cell(get_first_existing(row, [
        "Qi_entities_touched", "entities_touched", "entities", "Ent", "Ent(Qi)", "touched_entities",
    ], ""))
    relationships = parse_list_cell(get_first_existing(row, [
        "Rc_selected_edges", "covered_relationships", "relationship_names_touched", "relationships", "edges", "Rel", "Rel(qi)",
    ], ""))
    explicit_families = parse_list_cell(get_first_existing(row, [
        "activated_families", "active_g_classes", "A_Qi", "A(Qi)", "families", "g_classes",
    ], ""))

    def number(names: Sequence[str], default: float = 0.0) -> float:
        raw = get_first_existing(row, names, default)
        try:
            return float(raw)
        except Exception:
            return float(default)

    q = {
        "query_name": qname,
        "official_id": official,
        "selected_root": root,
        "query_class": qclass,
        "query_type": qtype,
        "semantic_group": sem,
        "semantic_detail": sem_detail,
        "sharedness": sharedness,
        "volatility": volatility,
        "entities": unique_sorted(entities),
        "relationships": unique_sorted(relationships),
        "Rc": number(["Rc", "Rc_weighted", "relationship_count"], 0.0),
        "D": number(["D", "selected_document_depth", "depth"], 0.0),
        "Re": number(["Re", "residual_traversal", "residual_explicit_traversal"], 0.0),
        "activated_families_raw": explicit_families,
    }
    fams, source = infer_families_from_ldbc_row(q, plan_families_by_official.get(official, []))
    q["activated_families"] = fams
    q["activation_source"] = source
    return q


# -----------------------------------------------------------------------------
# Artifact normalization and gamma_spec for LDBC candidate specifications P
# -----------------------------------------------------------------------------

def normalize_plan(plan_df: pd.DataFrame, scale_label: Optional[str]) -> pd.DataFrame:
    df = plan_df.copy()
    if scale_label and "scale_label" in df.columns:
        matching = df[df["scale_label"].astype(str) == str(scale_label)]
        if matching.empty:
            available = sorted(df["scale_label"].dropna().astype(str).unique())
            raise ValueError(
                f"No execution-plan rows match --scale-label {scale_label!r}. "
                f"Available scale labels: {available}"
            )
        df = matching.copy()
    if "official_id" not in df.columns:
        if "query_name" in df.columns:
            df["official_id"] = df["query_name"].astype(str).map(normalize_official_id)
        else:
            raise ValueError("Execution plan must contain official_id or query_name.")
    else:
        df["official_id"] = df["official_id"].astype(str).map(normalize_official_id)
    if "query_name" not in df.columns:
        df["query_name"] = df["official_id"].astype(str)
    if "benchmark_group" not in df.columns:
        df["benchmark_group"] = df["final_benchmark_group"] if "final_benchmark_group" in df.columns else "unknown"
    if "final_benchmark_group" not in df.columns:
        df["final_benchmark_group"] = df["benchmark_group"]
    if "g_class" not in df.columns:
        df["g_class"] = "G?"
    if "candidate_id" not in df.columns:
        raise ValueError("Execution plan must contain candidate_id.")
    df["g_class"] = df["g_class"].astype(str).map(normalize_g)
    df["candidate_id"] = df["candidate_id"].astype(str)
    return df.reset_index(drop=True)


LDBC_ENTITY_COLLECTION = {
    "Person": "persons",
    "Post": "posts",
    "Comment": "comments",
    "Forum": "forums",
    "Place": "places",
    "Organisation": "organisations",
    "Organization": "organisations",
    "Tag": "tags",
    "TagClass": "tag_classes",
}


def entity_collection(entity: Any) -> Optional[str]:
    return LDBC_ENTITY_COLLECTION.get(str(entity).strip()) if entity is not None else None


# LDBC SNB artifacts describe conceptual relationships. Some of those
# relationships are stored as standalone edge collections in the generated CSVs,
# while others are physically represented by foreign-key fields inside an entity
# document. Algorithm 4/5 must reason about the *physical* MongoDB layout rather
# than treating every conceptual relationship name as a collection.
#
# Tuple: (physical_collection, physical_source_field, physical_target_field).
LDBC_INLINE_RELATIONSHIP_STORAGE: Dict[str, Tuple[str, str, str]] = {
    "post_has_creator_person": ("posts", "post_id", "creator_person_id"),
    "comment_has_creator_person": ("comments", "comment_id", "creator_person_id"),
    "forum_container_of_post": ("posts", "forum_id", "post_id"),
    "person_is_located_in_place": ("persons", "person_id", "place_id"),
    "post_is_located_in_place": ("posts", "post_id", "place_id"),
    "comment_is_located_in_place": ("comments", "comment_id", "place_id"),
    "organisation_is_located_in_place": ("organisations", "organisation_id", "place_id"),
    "organization_is_located_in_place": ("organisations", "organisation_id", "place_id"),
    "forum_has_moderator_person": ("forums", "forum_id", "moderator_person_id"),
    "comment_reply_of_post": ("comments", "reply_post_id", "comment_id"),
    "comment_reply_of_comment": ("comments", "reply_comment_id", "comment_id"),
    "tag_has_type_tagclass": ("tags", "tag_id", "tag_class_id"),
    # Hierarchical relationships are stored inline in the entity documents, not
    # as standalone edge collections in the official LDBC entity CSVs.
    "place_is_part_of_place": ("places", "place_id", "parent_place_id"),
    "tagclass_is_subclass_of_tagclass": ("tag_classes", "tag_class_id", "parent_tag_class_id"),
}


def physical_relationship_storage(relationship: Any, source_column: Any = None, target_column: Any = None) -> Tuple[str, str, str]:
    rel = str(relationship or "").strip()
    if rel in LDBC_INLINE_RELATIONSHIP_STORAGE:
        return LDBC_INLINE_RELATIONSHIP_STORAGE[rel]
    # Standalone relationship collections retain the artifact endpoint columns.
    return rel, str(source_column or "").strip(), str(target_column or "").strip()


def physical_edge_collection(relationship: Any) -> str:
    coll, _, _ = physical_relationship_storage(relationship)
    return coll


def index_token(collection: str, fields: Sequence[Tuple[str, int]]) -> str:
    return f"{collection}(" + ",".join(f"{f}:{int(d)}" for f, d in fields) + ")"


def infer_indexes_from_artifact(
    collections: Iterable[str],
    relationship_specs: Sequence[Dict[str, Any]],
    root_collection: Any,
    root_pk: Any,
    reverse_indexes: Sequence[str],
) -> List[str]:
    """Infer Idx_p without erasing the G4/G6 distinction.

    Entity/root indexes and source-oriented relationship indexes are common access
    structures. A target-oriented relationship index is added only for a
    bidirectional relationship or when the artifact explicitly declares the
    relationship in ``reverse_indexes`` (G6).
    """
    out: List[str] = []
    common_entity = {
        "persons": [[("person_id", 1)], [("place_id", 1)]],
        "posts": [[("post_id", 1)], [("creator_person_id", 1)], [("forum_id", 1)], [("creator_person_id", 1), ("creation_date", -1)]],
        "comments": [[("comment_id", 1)], [("creator_person_id", 1)], [("reply_post_id", 1)], [("reply_comment_id", 1)], [("creator_person_id", 1), ("creation_date", -1)]],
        "forums": [[("forum_id", 1)], [("moderator_person_id", 1)]],
        "places": [[("place_id", 1)]],
        "organisations": [[("organisation_id", 1)], [("place_id", 1)]],
        "tags": [[("tag_id", 1)], [("name", 1)]],
        "tag_classes": [[("tag_class_id", 1)]],
    }
    for coll in unique_sorted(collections):
        for fields in common_entity.get(coll, []):
            out.append(index_token(coll, fields))
    if root_collection and root_pk:
        out.append(index_token(str(root_collection), [(str(root_pk), 1)]))
    reverse = {str(x) for x in reverse_indexes or []}
    for rs in relationship_specs or []:
        if not isinstance(rs, dict):
            continue
        relationship = str(rs.get("relationship") or rs.get("collection") or "")
        coll, src, tgt = physical_relationship_storage(
            relationship, rs.get("source_column"), rs.get("target_column")
        )
        if not coll:
            continue
        if src:
            out.append(index_token(coll, [(src, 1)]))
        if bool(rs.get("is_bidirectional")) and tgt:
            out.append(index_token(coll, [(tgt, 1)]))
        if src and tgt:
            out.append(index_token(coll, [(src, 1), (tgt, 1)]))
        # reverse_indexes contains logical relationship names, not physical
        # MongoDB collection names. Preserve that semantic distinction here.
        if relationship in reverse and tgt:
            out.append(index_token(coll, [(tgt, 1)]))
            if src:
                out.append(index_token(coll, [(tgt, 1), (src, 1)]))
    return unique_sorted(out)


def family_snapshot_level(g_class: str) -> int:
    """Conservative snapshot level used only when artifact metadata does not encode one."""
    return {
        "G0": 0,
        "G1": 0,
        "G2": 1,
        "G3": 2,
        "G4": 0,
        "G5": 2,
        "G6": 1,
        "G7": 0,
        "G8": 2,
        "G9": 3,
    }.get(normalize_g(g_class), 0)


def extract_snapshot_map(mat: Dict[str, Any], g_class: str, embedded_paths: List[str], reverse_indexes: List[str]) -> Dict[str, int]:
    # Prefer explicit artifact metadata if present.
    for key in ["snapshot", "snapshot_levels", "snapshots", "redundancy_levels"]:
        value = mat.get(key)
        if isinstance(value, dict):
            out: Dict[str, int] = {}
            for k, v in value.items():
                try:
                    out[str(k)] = int(v)
                except Exception:
                    pass
            if out:
                return dict(sorted(out.items()))

    level = family_snapshot_level(g_class)
    out: Dict[str, int] = {}
    if level > 0:
        for path in embedded_paths:
            out[path] = level
        # Endpoint/reverse-index candidates represent controlled materialized access
        # paths even when the artifact stores them separately from embedded paths.
        if normalize_g(g_class) == "G6":
            for path in reverse_indexes:
                out[path] = max(out.get(path, 0), level)
    return dict(sorted(out.items()))


def artifact_physical_components(spec: Dict[str, Any], row: pd.Series) -> Dict[str, Any]:
    """Translate the current LDBC artifact schema into paper-level physical components.

    The artifact bundle stores many physical decisions as entity-level fields
    (embedded_entities, referenced_entities, summarized_entities) rather than the
    path-level names used by the generic consolidation code. Preserve those decisions
    explicitly instead of silently treating all candidates as normalized.
    """
    mat = spec.get("materialization", {}) if isinstance(spec, dict) else {}
    metrics = spec.get("metrics", {}) if isinstance(spec, dict) else {}
    if not isinstance(mat, dict): mat = {}
    if not isinstance(metrics, dict): metrics = {}

    root_entity = row.get("root_entity") or mat.get("root_entity") or mat.get("owner")
    root_collection = row.get("root_collection") or mat.get("root_collection") or mat.get("collection_name")
    root_pk = row.get("root_pk") or mat.get("root_pk") or mat.get("root_key")
    owner = mat.get("owner") or root_entity
    g_class = normalize_g(row.get("g_class") or spec.get("g_class") or "G?")

    embedded_entities = nested_list(mat, "embedded_entities")
    referenced_entities = nested_list(mat, "referenced_entities")
    summarized_entities = nested_list(mat, "summarized_entities")
    embedded_paths = unique_sorted(
        nested_list(mat, "embedded_paths", "embedded_fields", "embeddings", "embedded")
        + [f"entity:{x}" for x in embedded_entities]
    )
    reference_paths = unique_sorted(
        nested_list(mat, "reference_paths", "references", "reference_fields", "refs")
        + [f"entity:{x}" for x in referenced_entities]
    )
    logical_edge_collections = nested_list(mat, "edge_collections", "bridge_collections", "relationship_collections")
    # Physicalize conceptual relationship storage. For example,
    # comment_has_creator_person is stored by comments.creator_person_id and is
    # therefore not a standalone MongoDB collection.
    edge_collections = unique_sorted(physical_edge_collection(x) for x in logical_edge_collections if physical_edge_collection(x))
    reverse_indexes = nested_list(mat, "reverse_indexes", "reverse_index_collections", "endpoint_indexes")
    explicit_indexes = nested_list(mat, "indexes", "workload_indexes", "index_specs")
    constraints = nested_list(mat, "constraints", "maintenance_constraints", "integrity_constraints")
    accessed_entities = nested_list(mat, "accessed_entities", "entities", "entity_types")
    relationships_used = nested_list(mat, "relationships_used", "relationships", "edges")
    duplicated_paths = unique_sorted(
        nested_list(mat, "duplicated_paths", "duplicated_fields", "dup", "snapshots")
        + [f"entity:{x}" for x in summarized_entities]
    )
    relationship_specs = mat.get("relationship_specs") if isinstance(mat.get("relationship_specs"), list) else []

    collections = nested_list(mat, "collections", "collection_names", "generated_collections")
    if root_collection:
        collections.append(str(root_collection))
    collections.extend(edge_collections)
    # Referenced entities remain external physical collections.
    for ent in referenced_entities:
        coll = entity_collection(ent)
        if coll: collections.append(coll)
    # Some artifact candidates omit referenced_entities but still name accessed
    # external entities. Keep accessed entities unless they are explicitly embedded
    # or summarized into the root document.
    materialized_under_root = set(embedded_entities) | set(summarized_entities)
    for ent in accessed_entities:
        if ent in materialized_under_root:
            continue
        coll = entity_collection(ent)
        if coll: collections.append(coll)
    collections = unique_sorted(collections)

    snapshot = extract_snapshot_map(mat, g_class, embedded_paths, reverse_indexes)
    level = family_snapshot_level(g_class)
    if level > 0:
        for ent in summarized_entities:
            snapshot[f"entity:{ent}"] = max(int(snapshot.get(f"entity:{ent}", 0)), level)
    snapshot = dict(sorted(snapshot.items()))
    if not duplicated_paths and snapshot:
        duplicated_paths = unique_sorted(path for path, lvl in snapshot.items() if int(lvl) > 0)

    indexes = unique_sorted(
        explicit_indexes
        + infer_indexes_from_artifact(collections, relationship_specs, root_collection, root_pk, reverse_indexes)
    )

    scope = {
        "root_collection": root_collection,
        "root_key": root_pk,
        "owner": owner,
        "root_entity": root_entity,
    }
    layout = {
        "collections": collections,
        "emb": embedded_paths,
        "ref": reference_paths,
        "dup": unique_sorted(duplicated_paths),
        "snapshot": snapshot,
        "constraints": unique_sorted(constraints),
        "accessed_entities": unique_sorted(accessed_entities),
        "embedded_entities": unique_sorted(embedded_entities),
        "referenced_entities": unique_sorted(referenced_entities),
        "summarized_entities": unique_sorted(summarized_entities),
        "relationships_used": unique_sorted(relationships_used),
        "edge_collections": unique_sorted(edge_collections),
        "logical_edge_collections": unique_sorted(logical_edge_collections),
        "reverse_indexes": unique_sorted(reverse_indexes),
        "mongodb_pattern": row.get("mongodb_pattern") or mat.get("mongodb_pattern"),
        "document_strategy": row.get("document_strategy") or mat.get("document_strategy"),
        "g_family": row.get("g_family") or spec.get("g_family"),
        "g_role": row.get("g_role") or spec.get("g_role"),
        "g_label": row.get("g_label") or spec.get("g_label"),
        "Rc_weighted": row.get("Rc_weighted") if "Rc_weighted" in row else metrics.get("Rc_weighted"),
        "D": row.get("D") if "D" in row else metrics.get("D"),
        "Re": row.get("Re") if "Re" in row else metrics.get("Re"),
        "DeltaRratio": row.get("DeltaRratio") if "DeltaRratio" in row else metrics.get("DeltaRratio"),
    }
    return {"scope": scope, "layout": layout, "indexes": indexes, "g_class": g_class}

def nonzero_snapshot(snapshot: Dict[str, Any]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for k, v in sorted(snapshot.items()):
        try:
            iv = int(v)
        except Exception:
            continue
        if iv > 0:
            out[str(k)] = iv
    return out


def structural_lambda(c: Dict[str, Any]) -> str:
    """lambda(p): physical layout without workload indexes."""
    return canon({
        "collections": unique_sorted(c.get("collections", [])),
        "emb": unique_sorted(c.get("emb", [])),
        "ref": unique_sorted(c.get("ref", [])),
        "dup": unique_sorted(c.get("dup", [])),
        "snapshot_nonzero": nonzero_snapshot(c.get("snapshot", {})),
        "constraints": unique_sorted(c.get("constraints", [])),
    })


def candidate_deployment_signature(c: Dict[str, Any]) -> str:
    """kappa(c): canonical deployable signature including all runner-visible structures."""
    return canon({
        "scope": c.get("scope", {}),
        "collections": unique_sorted(c.get("collections", [])),
        "emb": unique_sorted(c.get("emb", [])),
        "ref": unique_sorted(c.get("ref", [])),
        "dup": unique_sorted(c.get("dup", [])),
        "snapshot_nonzero": nonzero_snapshot(c.get("snapshot", {})),
        "indexes": unique_sorted(c.get("indexes", [])),
        "constraints": unique_sorted(c.get("constraints", [])),
        "edge_collections": unique_sorted(c.get("edge_collections", [])),
        "reverse_indexes": unique_sorted(c.get("reverse_indexes", [])),
    })


def candidate_compatibility_signature(c: Dict[str, Any]) -> str:
    """Features that determine compatibility under the implemented predicates.

    Candidate families/provenance are intentionally excluded.  Under the current
    Algorithm-4/5 implementation, future compatibility depends on scope, active
    snapshot levels and explicit constraints.
    """
    return canon({
        "scope": c.get("scope", {}),
        "snapshot_nonzero": nonzero_snapshot(c.get("snapshot", {})),
        "constraints": unique_sorted(c.get("constraints", [])),
    })


def snapshot_cost(c: Dict[str, Any]) -> float:
    return float(sum(nonzero_snapshot(c.get("snapshot", {})).values()))


def physical_cost(c: Dict[str, Any]) -> float:
    """Structural complexity proxy only; no old latency and no benchmark-group penalty."""
    return round(
        1.0 * len(c.get("collections", []))
        + 0.7 * len(c.get("emb", []))
        + 0.3 * len(c.get("ref", []))
        + 0.5 * len(c.get("dup", []))
        + 0.4 * len(c.get("edge_collections", []))
        + 0.5 * len(c.get("reverse_indexes", []))
        + 0.6 * snapshot_cost(c),
        6,
    )


def storage_cost(c: Dict[str, Any]) -> float:
    return round(
        len(c.get("collections", []))
        + len(c.get("emb", []))
        + len(c.get("dup", []))
        + 0.5 * len(c.get("reverse_indexes", []))
        + snapshot_cost(c),
        6,
    )


def duplication_cost(c: Dict[str, Any]) -> float:
    return round(len(c.get("dup", [])) + snapshot_cost(c) + 0.5 * len(c.get("reverse_indexes", [])), 6)


def update_maintenance_cost(c: Dict[str, Any]) -> float:
    # Reference/explicit-edge/reverse-index families are treated as maintenance-safer
    # than rich embedded snapshots. This is a physical preselection penalty only.
    fams = {normalize_g(f) for f in c.get("families", [])}
    maintenance_safe = bool(fams) and fams.issubset({"G0", "G1", "G4", "G6", "G7"})
    factor = duplication_cost(c)
    return round(factor * (0.5 if maintenance_safe else 1.0), 6)


def selection_score(c: Dict[str, Any]) -> float:
    return round(
        physical_cost(c)
        + 0.5 * storage_cost(c)
        + 0.8 * duplication_cost(c)
        + update_maintenance_cost(c),
        6,
    )


def make_p_specs(
    queries: Dict[str, Dict[str, Any]],
    plan_df: pd.DataFrame,
    specs_by_id: Dict[str, Dict[str, Any]],
    max_candidates_per_query: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    specs: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    for official_id, q in queries.items():
        sub = plan_df[plan_df["official_id"].astype(str) == official_id].copy()
        if sub.empty:
            warnings.append({
                "official_id": official_id,
                "query_name": q.get("query_name"),
                "status": "no_execution_plan_rows",
                "activated_families": ";".join(q.get("activated_families", [])),
            })
            continue

        activated = set(q.get("activated_families") or [])
        if activated:
            filtered = sub[sub["g_class"].astype(str).isin(activated)].copy()
            if not filtered.empty:
                sub = filtered
            else:
                warnings.append({
                    "official_id": official_id,
                    "query_name": q.get("query_name"),
                    "status": "no_plan_rows_for_matrix_families_using_all_plan_families",
                    "activated_families": ";".join(sorted(activated)),
                    "available_plan_families": ";".join(sorted(sub["g_class"].dropna().astype(str).unique())),
                })

        query_specs: List[Dict[str, Any]] = []
        for _, row in sub.iterrows():
            artifact_cid = str(row["candidate_id"])
            artifact_spec = specs_by_id.get(artifact_cid, {})
            if not artifact_spec:
                warnings.append({
                    "official_id": official_id,
                    "query_name": q.get("query_name"),
                    "status": "candidate_missing_from_candidate_specs_json",
                    "candidate_id": artifact_cid,
                    "activated_families": ";".join(sorted(activated)),
                })

            comp = artifact_physical_components(artifact_spec, row)
            layout = comp["layout"]
            scope = comp["scope"]
            g_class = comp["g_class"]

            p: Dict[str, Any] = {
                "spec_id": f"{official_id}_{artifact_cid}",
                "candidate_id": artifact_cid,
                "artifact_candidate_id": artifact_cid,
                "query_name": q.get("query_name"),
                "official_id": official_id,
                "origin_queries": [official_id],
                "families": [g_class],
                "g_class": g_class,
                "benchmark_group": str(row.get("benchmark_group") or row.get("final_benchmark_group") or "unknown"),
                "config_names": [artifact_cid],
                "scope": scope,
                "scope_sig": canon(scope),
                "collections": layout["collections"],
                "emb": layout["emb"],
                "ref": layout["ref"],
                "dup": layout["dup"],
                "snapshot": layout["snapshot"],
                "indexes": comp["indexes"],
                "constraints": layout["constraints"],
                "accessed_entities": layout["accessed_entities"],
                "relationships_used": layout["relationships_used"],
                "edge_collections": layout["edge_collections"],
                "reverse_indexes": layout["reverse_indexes"],
                "mongodb_pattern": layout["mongodb_pattern"],
                "document_strategy": layout["document_strategy"],
                "coverage": [],
                "route_candidate_ids_by_query": {official_id: [artifact_cid]},
                "source_specs": [f"{official_id}_{artifact_cid}"],
                "source_candidate_ids": [artifact_cid],
                "component_deployment_signatures": [],
            }
            p["lambda_sig"] = short_hash(structural_lambda(p), 16)
            p["canonical_layout_json"] = structural_lambda(p)
            dep_sig = candidate_deployment_signature(p)
            p["deployment_signature"] = dep_sig
            p["deployment_signature_hash"] = short_hash(dep_sig, 16)
            p["component_deployment_signatures"] = [dep_sig]
            p["physical_cost"] = physical_cost(p)
            p["storage_cost"] = storage_cost(p)
            p["duplication_cost"] = duplication_cost(p)
            p["update_maintenance_cost"] = update_maintenance_cost(p)
            p["selection_score"] = selection_score(p)
            query_specs.append(p)

        query_specs.sort(key=lambda x: (x["selection_score"], x["g_class"], x["artifact_candidate_id"]))

        # Same-query exact physical duplicates do not need to enter P twice.
        dedup: List[Dict[str, Any]] = []
        seen = set()
        for p in query_specs:
            key = (p["scope_sig"], p["deployment_signature"])
            if key in seen:
                continue
            seen.add(key)
            dedup.append(p)

        if max_candidates_per_query and max_candidates_per_query > 0:
            dedup = dedup[:max_candidates_per_query]
        specs.extend(dedup)

    return specs, warnings


# -----------------------------------------------------------------------------
# Algorithm 4: ConsolidateAndValidate(P, W, rho_max)
# -----------------------------------------------------------------------------

def snapshot_join(cands: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for c in cands:
        for k, v in c.get("snapshot", {}).items():
            try:
                iv = int(v)
            except Exception:
                continue
            out[str(k)] = max(out.get(str(k), 0), iv)
    return dict(sorted(out.items()))


def union_route_maps(cands: Sequence[Dict[str, Any]]) -> Dict[str, List[str]]:
    out: Dict[str, Set[str]] = {}
    for c in cands:
        for q, ids in (c.get("route_candidate_ids_by_query") or {}).items():
            out.setdefault(str(q), set()).update(str(x) for x in ids)
    return {q: sorted(ids) for q, ids in sorted(out.items())}


def exact_canonical_merge(specs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    buckets: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for p in specs:
        buckets.setdefault((p["scope_sig"], p["lambda_sig"]), []).append(p)

    out: List[Dict[str, Any]] = []
    report: List[Dict[str, Any]] = []
    for (scope_sig, lam), members in buckets.items():
        rep = sorted(members, key=lambda x: (x["selection_score"], x["artifact_candidate_id"]))[0]
        sources = unique_sorted(s for m in members for s in m.get("source_specs", []))
        source_candidate_ids = unique_sorted(cid for m in members for cid in m.get("source_candidate_ids", []))
        origins = unique_sorted(q for m in members for q in m.get("origin_queries", []))

        c: Dict[str, Any] = {
            "candidate_id": f"C_EXACT_{short_hash([scope_sig, lam, sources])}",
            "merge_kind": "exact" if len(members) > 1 else "singleton",
            "source_specs": sources,
            "source_candidate_ids": source_candidate_ids,
            "representative_candidate_id": rep["artifact_candidate_id"],
            "origin_queries": origins,
            "families": unique_sorted(f for m in members for f in m.get("families", [])),
            "config_names": source_candidate_ids,
            "scope": rep["scope"],
            "scope_sig": scope_sig,
            "collections": unique_sorted(x for m in members for x in m.get("collections", [])),
            "emb": unique_sorted(x for m in members for x in m.get("emb", [])),
            "ref": unique_sorted(x for m in members for x in m.get("ref", [])),
            "dup": unique_sorted(x for m in members for x in m.get("dup", [])),
            "snapshot": snapshot_join(members),
            "indexes": unique_sorted(x for m in members for x in m.get("indexes", [])),
            "constraints": unique_sorted(x for m in members for x in m.get("constraints", [])),
            "accessed_entities": unique_sorted(x for m in members for x in m.get("accessed_entities", [])),
            "relationships_used": unique_sorted(x for m in members for x in m.get("relationships_used", [])),
            "edge_collections": unique_sorted(x for m in members for x in m.get("edge_collections", [])),
            "reverse_indexes": unique_sorted(x for m in members for x in m.get("reverse_indexes", [])),
            "route_candidate_ids_by_query": union_route_maps(members),
            "coverage": [],
            "component_deployment_signatures": unique_sorted(
                sig for m in members for sig in m.get("component_deployment_signatures", [])
            ),
        }
        c["lambda_sig"] = short_hash(structural_lambda(c), 16)
        c["canonical_layout_json"] = structural_lambda(c)
        c["deployment_signature"] = candidate_deployment_signature(c)
        c["deployment_signature_hash"] = short_hash(c["deployment_signature"], 16)
        c["physical_cost"] = physical_cost(c)
        c["storage_cost"] = storage_cost(c)
        c["duplication_cost"] = duplication_cost(c)
        c["update_maintenance_cost"] = update_maintenance_cost(c)
        c["selection_score"] = selection_score(c)
        out.append(c)

        report.append({
            "candidate_id": c["candidate_id"],
            "merge_kind": c["merge_kind"],
            "n_source_specs": len(sources),
            "source_specs": ";".join(sources),
            "source_candidate_ids": ";".join(source_candidate_ids),
            "origin_queries": ";".join(origins),
            "families": ";".join(c["families"]),
            "scope_sig": scope_sig,
            "lambda_sig": c["lambda_sig"],
            "indexes_union": ";".join(c["indexes"]),
        })
    return out, report


def compatible_pair(a: Dict[str, Any], b: Dict[str, Any], rho_max: int) -> Tuple[bool, str]:
    """Paper-level physical compatibility for Algorithm 4.

    Query provenance/origin overlap is deliberately NOT a rejection condition.
    Algorithm 4 defines fuseability from materialisation scope, compatible paths/constraints,
    and the snapshot ceiling.
    """
    if a["scope_sig"] != b["scope_sig"]:
        return False, "different_scope"
    sj = snapshot_join([a, b])
    if max(sj.values() or [0]) > int(rho_max):
        return False, "snapshot_ceiling"

    # Same conservative conflict rule as the IMDb v3 implementation: if both
    # candidates actively materialize the same path at different non-zero levels,
    # do not silently choose one level during this implementation pass.
    for path in set(a.get("snapshot", {})) | set(b.get("snapshot", {})):
        av = int(a.get("snapshot", {}).get(path, 0) or 0)
        bv = int(b.get("snapshot", {}).get(path, 0) or 0)
        if av > 0 and bv > 0 and av != bv:
            return False, f"conflicting_snapshot_{path}"

    # Constraints are currently opaque strings in the LDBC bundle. Opposite
    # declarations are not inferred; exact string union is therefore conservative.
    return True, "compatible"


def join_candidates(cands: Sequence[Dict[str, Any]], candidate_prefix: str = "C_FUSED") -> Dict[str, Any]:
    if not cands:
        raise ValueError("join_candidates requires at least one candidate")

    c: Dict[str, Any] = {
        "candidate_id": f"{candidate_prefix}_{short_hash([x['candidate_id'] for x in cands])}",
        "merge_kind": "fused",
        "source_specs": unique_sorted(x for cand in cands for x in cand.get("source_specs", [])),
        "source_candidate_ids": unique_sorted(x for cand in cands for x in cand.get("source_candidate_ids", [])),
        "representative_candidate_id": sorted(
            unique_sorted(x for cand in cands for x in cand.get("source_candidate_ids", []))
        )[0] if any(cand.get("source_candidate_ids") for cand in cands) else "",
        "origin_queries": unique_sorted(x for cand in cands for x in cand.get("origin_queries", [])),
        "families": unique_sorted(x for cand in cands for x in cand.get("families", [])),
        "config_names": unique_sorted(x for cand in cands for x in cand.get("config_names", [])),
        "scope": cands[0]["scope"],
        "scope_sig": cands[0]["scope_sig"],
        "collections": unique_sorted(x for cand in cands for x in cand.get("collections", [])),
        "emb": unique_sorted(x for cand in cands for x in cand.get("emb", [])),
        "ref": unique_sorted(x for cand in cands for x in cand.get("ref", [])),
        "dup": unique_sorted(x for cand in cands for x in cand.get("dup", [])),
        "snapshot": snapshot_join(cands),
        "indexes": unique_sorted(x for cand in cands for x in cand.get("indexes", [])),
        "constraints": unique_sorted(x for cand in cands for x in cand.get("constraints", [])),
        "accessed_entities": unique_sorted(x for cand in cands for x in cand.get("accessed_entities", [])),
        "relationships_used": unique_sorted(x for cand in cands for x in cand.get("relationships_used", [])),
        "edge_collections": unique_sorted(x for cand in cands for x in cand.get("edge_collections", [])),
        "reverse_indexes": unique_sorted(x for cand in cands for x in cand.get("reverse_indexes", [])),
        "route_candidate_ids_by_query": union_route_maps(cands),
        "coverage": [],
        "component_deployment_signatures": unique_sorted(
            sig for cand in cands for sig in cand.get("component_deployment_signatures", [])
        ),
    }
    c["lambda_sig"] = short_hash(structural_lambda(c), 16)
    c["canonical_layout_json"] = structural_lambda(c)
    c["deployment_signature"] = candidate_deployment_signature(c)
    c["deployment_signature_hash"] = short_hash(c["deployment_signature"], 16)
    c["physical_cost"] = physical_cost(c)
    c["storage_cost"] = storage_cost(c)
    c["duplication_cost"] = duplication_cost(c)
    c["update_maintenance_cost"] = update_maintenance_cost(c)
    c["selection_score"] = selection_score(c)
    return c


def _targeted_full_scope_anchor_search(
    members: List[Dict[str, Any]],
    rho_max: int,
    node_limit: int = 100000,
    anchor_limit: int = 64,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Find a bounded set of maximal same-scope fusion anchors.

    This is still Algorithm-4 complementary fusion: it searches compatible subsets U
    and materializes their join.  The only difference is search order.  Instead of
    enumerating every intermediate subset, it prioritizes subsets that cover all
    origin queries represented in the scope.  No benchmark latency or historical
    result is consulted.
    """
    target = set(q for c in members for q in c.get("origin_queries", []))
    if len(target) < 2:
        return [], {"nodes_visited": 0, "truncated": False, "target_origins": sorted(target)}

    by_origin: Dict[str, List[int]] = {q: [] for q in target}
    for i, c in enumerate(members):
        for q in c.get("origin_queries", []):
            if q in by_origin:
                by_origin[q].append(i)

    anchors_by_sig: Dict[str, Dict[str, Any]] = {}
    seen: Set[Tuple[Tuple[str, ...], str]] = set()
    nodes = 0
    truncated = False

    def recurse(current: Optional[Dict[str, Any]], covered: Set[str], selected: Tuple[int, ...]) -> None:
        nonlocal nodes, truncated
        if nodes >= max(1, int(node_limit)):
            truncated = True
            return
        nodes += 1

        if target.issubset(covered):
            if current is None:
                return
            sig = current.get("deployment_signature") or candidate_deployment_signature(current)
            prev = anchors_by_sig.get(sig)
            if prev is None or float(current.get("selection_score", selection_score(current))) < float(prev.get("selection_score", selection_score(prev))):
                anchors_by_sig[sig] = current
            return

        uncovered = target - covered
        # Minimum-remaining-values heuristic over *currently compatible* choices.
        best_q = None
        best_options: List[int] = []
        for q in sorted(uncovered):
            opts: List[int] = []
            for i in by_origin.get(q, []):
                cand = members[i]
                cand_origins = set(cand.get("origin_queries", []))
                # Provenance overlap is not a physical incompatibility.  After ExactMerge,
                # a candidate may already represent several origin queries.  Reject only a
                # choice that adds no new origin to this coverage-directed search state.
                if cand_origins.issubset(covered):
                    continue
                if current is not None:
                    ok, _ = compatible_pair(current, cand, rho_max)
                    if not ok:
                        continue
                opts.append(i)
            if best_q is None or len(opts) < len(best_options):
                best_q, best_options = q, opts
            if len(best_options) <= 1:
                break

        if not best_options:
            return

        best_options = sorted(
            best_options,
            key=lambda i: (
                float(members[i].get("selection_score", selection_score(members[i]))),
                -len(members[i].get("origin_queries", [])),
                members[i]["candidate_id"],
            ),
        )

        for i in best_options:
            cand = members[i]
            joined = cand if current is None else join_candidates([current, cand])
            new_covered = covered | set(cand.get("origin_queries", []))
            state_key = (tuple(sorted(new_covered)), joined.get("deployment_signature", ""))
            if state_key in seen:
                continue
            seen.add(state_key)
            recurse(joined, new_covered, selected + (i,))
            if nodes >= max(1, int(node_limit)):
                break

    recurse(None, set(), tuple())
    anchors = sorted(
        anchors_by_sig.values(),
        key=lambda c: (
            float(c.get("selection_score", selection_score(c))),
            float(c.get("physical_cost", physical_cost(c))),
            len(c.get("source_candidate_ids", [])),
            c["candidate_id"],
        ),
    )[: max(1, int(anchor_limit))]
    return anchors, {
        "nodes_visited": nodes,
        "truncated": truncated,
        "target_origins": sorted(target),
        "n_anchors": len(anchors),
    }


def controlled_same_scope_fusion(
    c_exact: List[Dict[str, Any]],
    rho_max: int,
    max_fusion_size: int = 0,
    max_fused_candidates: int = 50000,
    max_fusion_states_per_scope: int = 10000,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Bounded multi-way same-scope fusion for Algorithm 4.

    v7 preserves the v6 search-budget fix and removes an extra provenance-overlap restriction that is not part of Algorithm 4.

    In v6, two exact/shared candidates were rejected whenever their origin-query sets overlapped.
    That is too strong after ExactMerge: a shared candidate can legitimately have several origins,
    and another physically compatible candidate may overlap one of those origins while adding a
    missing workload query.  Algorithm 4 defines fuseability from physical scope, compatible paths,
    compatible constraints, and the snapshot ceiling; query provenance is metadata, not a physical
    incompatibility predicate.

    v6 also fixed an implementation bug in v5: ``max_fused_candidates`` used to stop the
    *search itself*.  With richer paper-level physical signatures, the Person scope
    could consume that global budget before the search reached large subsets, and
    later scopes (notably Post) were never fused.  The bound now limits only the
    number of ordinary intermediate fused candidates emitted into C; it never prevents
    later scopes from being searched.

    In addition, each scope gets a bounded coverage-directed anchor search that keeps
    a few maximal compatible joins covering all origin queries in that scope.  This is
    a search-order optimization only; it uses schema/workload/artifact evidence and no
    old benchmark result.
    """
    fused: List[Dict[str, Any]] = []
    report: List[Dict[str, Any]] = []
    by_scope: Dict[str, List[Dict[str, Any]]] = {}
    for c in c_exact:
        by_scope.setdefault(c["scope_sig"], []).append(c)

    emitted_regular = 0
    emitted_signatures: Set[str] = set()

    for scope_sig, members0 in sorted(by_scope.items()):
        members = sorted(
            members0,
            key=lambda c: (
                float(c.get("selection_score", selection_score(c))),
                -len(c.get("origin_queries", [])),
                c["candidate_id"],
            ),
        )
        if len(members) < 2:
            continue

        target_origins = set(q for c in members for q in c.get("origin_queries", []))
        max_r = len(members) if int(max_fusion_size) <= 0 else min(int(max_fusion_size), len(members))

        # Ordinary bounded beam over compatible subsets.  Even after the emission
        # budget is exhausted, the frontier continues so deeper compatible states can
        # still be discovered.
        frontier: List[Tuple[Tuple[int, ...], Dict[str, Any]]] = [((i,), members[i]) for i in range(len(members))]
        scope_seen: Set[Tuple[Tuple[str, ...], str]] = set()
        beam_full_anchors: Dict[str, Dict[str, Any]] = {}
        deepest_subset = 1

        for subset_size in range(2, max_r + 1):
            next_states: Dict[Tuple[Tuple[str, ...], str], Tuple[Tuple[int, ...], Dict[str, Any]]] = {}

            for indexes, current in frontier:
                last = indexes[-1]
                for j in range(last + 1, len(members)):
                    nxt = members[j]
                    ok, _ = compatible_pair(current, nxt, rho_max)
                    if not ok:
                        continue

                    joined = join_candidates([current, nxt])
                    new_indexes = indexes + (j,)
                    key = (tuple(joined.get("origin_queries", [])), joined["deployment_signature"])
                    if key in scope_seen:
                        continue
                    prev = next_states.get(key)
                    if prev is None or float(joined.get("selection_score", 0.0)) < float(prev[1].get("selection_score", 0.0)):
                        next_states[key] = (new_indexes, joined)

            ranked_states = sorted(
                next_states.values(),
                key=lambda item: (
                    -len(item[1].get("origin_queries", [])),
                    float(item[1].get("selection_score", selection_score(item[1]))),
                    len(item[1].get("source_candidate_ids", [])),
                    item[1]["candidate_id"],
                ),
            )
            if max_fusion_states_per_scope and len(ranked_states) > int(max_fusion_states_per_scope):
                ranked_states = ranked_states[: int(max_fusion_states_per_scope)]

            frontier = []
            for indexes, joined in ranked_states:
                key = (tuple(joined.get("origin_queries", [])), joined["deployment_signature"])
                if key in scope_seen:
                    continue
                scope_seen.add(key)
                frontier.append((indexes, joined))
                deepest_subset = subset_size

                if target_origins.issubset(set(joined.get("origin_queries", []))):
                    sig = joined["deployment_signature"]
                    prev = beam_full_anchors.get(sig)
                    if prev is None or float(joined.get("selection_score", 0.0)) < float(prev.get("selection_score", 0.0)):
                        beam_full_anchors[sig] = joined

                if not max_fused_candidates or emitted_regular < int(max_fused_candidates):
                    sig = joined["deployment_signature"]
                    if sig not in emitted_signatures:
                        fused.append(joined)
                        emitted_signatures.add(sig)
                        emitted_regular += 1
                        report.append({
                            "scope_sig": scope_sig,
                            "subset_size": subset_size,
                            "source_candidates": ";".join(members[i]["candidate_id"] for i in indexes),
                            "source_specs": ";".join(joined.get("source_specs", [])),
                            "source_candidate_ids": ";".join(joined.get("source_candidate_ids", [])),
                            "origin_queries": ";".join(joined.get("origin_queries", [])),
                            "fusion_status": "accepted",
                            "reason": "compatible_bounded_multiway_search",
                            "fused_candidate_id": joined["candidate_id"],
                        })

            if not frontier:
                break

        # Coverage-directed fallback/anchor search.  This guarantees that exhausting
        # the ordinary output budget cannot by itself prevent a complete same-scope
        # fusion from being represented in C.
        targeted_anchors, targeted_info = _targeted_full_scope_anchor_search(
            members,
            rho_max=rho_max,
            node_limit=max(10000, int(max_fusion_states_per_scope or 10000) * 10),
            anchor_limit=64,
        )
        anchors_by_sig: Dict[str, Dict[str, Any]] = dict(beam_full_anchors)
        for a in targeted_anchors:
            sig = a["deployment_signature"]
            prev = anchors_by_sig.get(sig)
            if prev is None or float(a.get("selection_score", 0.0)) < float(prev.get("selection_score", 0.0)):
                anchors_by_sig[sig] = a

        anchor_count = 0
        for anchor in sorted(
            anchors_by_sig.values(),
            key=lambda c: (
                float(c.get("selection_score", selection_score(c))),
                float(c.get("physical_cost", physical_cost(c))),
                len(c.get("source_candidate_ids", [])),
                c["candidate_id"],
            ),
        )[:64]:
            sig = anchor["deployment_signature"]
            if sig in emitted_signatures:
                continue
            fused.append(anchor)
            emitted_signatures.add(sig)
            anchor_count += 1
            report.append({
                "scope_sig": scope_sig,
                "subset_size": len(anchor.get("source_candidate_ids", [])),
                "source_candidates": ";".join(anchor.get("source_candidate_ids", [])),
                "source_specs": ";".join(anchor.get("source_specs", [])),
                "source_candidate_ids": ";".join(anchor.get("source_candidate_ids", [])),
                "origin_queries": ";".join(anchor.get("origin_queries", [])),
                "fusion_status": "accepted_anchor",
                "reason": "coverage_directed_full_scope_anchor",
                "fused_candidate_id": anchor["candidate_id"],
            })

        report.append({
            "scope_sig": scope_sig,
            "subset_size": "summary",
            "source_candidates": "",
            "source_specs": "",
            "source_candidate_ids": "",
            "origin_queries": ";".join(sorted(target_origins)),
            "fusion_status": "scope_summary",
            "reason": (
                f"exact_members={len(members)};deepest_subset={deepest_subset};"
                f"regular_emitted_total={emitted_regular};full_scope_anchors_added={anchor_count};"
                f"targeted_nodes={targeted_info.get('nodes_visited', 0)};"
                f"targeted_truncated={targeted_info.get('truncated', False)};max_r={max_r}"
            ),
            "fused_candidate_id": "",
        })

    return fused, report

def final_canonical_merge(raw_candidates: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for c in raw_candidates:
        buckets.setdefault(candidate_deployment_signature(c), []).append(c)

    out: List[Dict[str, Any]] = []
    report: List[Dict[str, Any]] = []
    for dep_sig, members in buckets.items():
        if len(members) == 1:
            kept = dict(members[0])
            kept["deployment_signature"] = dep_sig
            kept["deployment_signature_hash"] = short_hash(dep_sig, 16)
            out.append(kept)
            report.append({
                "canonical_candidate_id": kept["candidate_id"],
                "action": "kept_unique",
                "n_merged": 1,
                "merged_candidate_ids": kept["candidate_id"],
                "source_specs": ";".join(kept.get("source_specs", [])),
                "source_candidate_ids": ";".join(kept.get("source_candidate_ids", [])),
                "origin_queries": ";".join(kept.get("origin_queries", [])),
                "deployment_signature_hash": kept["deployment_signature_hash"],
            })
            continue

        rep = join_candidates(members, candidate_prefix="C_CANON")
        rep["candidate_id"] = f"C_CANON_{short_hash(dep_sig)}"
        rep["merge_kind"] = "canonical_after_fusion"
        rep["deployment_signature"] = dep_sig
        rep["deployment_signature_hash"] = short_hash(dep_sig, 16)
        out.append(rep)
        report.append({
            "canonical_candidate_id": rep["candidate_id"],
            "action": "merged_physical_duplicates",
            "n_merged": len(members),
            "merged_candidate_ids": ";".join(m["candidate_id"] for m in members),
            "source_specs": ";".join(rep.get("source_specs", [])),
            "source_candidate_ids": ";".join(rep.get("source_candidate_ids", [])),
            "origin_queries": ";".join(rep.get("origin_queries", [])),
            "deployment_signature_hash": rep["deployment_signature_hash"],
        })
    return out, report


def build_plan_route_evidence(
    plan_df: pd.DataFrame,
    specs_by_id: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Set[str]], Dict[Tuple[str, str], List[str]], Dict[str, str]]:
    """Build conservative executable-route evidence from the artifact execution plan.

    Returns:
      candidate_queries[candidate_id] -> official ids that have a plan row
      signature_query_candidates[(deployment_signature, official_id)] -> candidate ids
      candidate_signature[candidate_id] -> deployment signature
    """
    candidate_queries: Dict[str, Set[str]] = {}
    signature_query_candidates: Dict[Tuple[str, str], List[str]] = {}
    candidate_signature: Dict[str, str] = {}

    for _, row in plan_df.iterrows():
        cid = str(row["candidate_id"])
        qid = normalize_official_id(row["official_id"])
        candidate_queries.setdefault(cid, set()).add(qid)

        comp = artifact_physical_components(specs_by_id.get(cid, {}), row)
        tmp = {
            "scope": comp["scope"],
            "collections": comp["layout"]["collections"],
            "emb": comp["layout"]["emb"],
            "ref": comp["layout"]["ref"],
            "dup": comp["layout"]["dup"],
            "snapshot": comp["layout"]["snapshot"],
            "indexes": comp["indexes"],
            "constraints": comp["layout"]["constraints"],
        }
        sig = candidate_deployment_signature(tmp)
        candidate_signature[cid] = sig
        signature_query_candidates.setdefault((sig, qid), []).append(cid)

    for key, ids in list(signature_query_candidates.items()):
        signature_query_candidates[key] = sorted(set(ids))
    return candidate_queries, signature_query_candidates, candidate_signature


def validate_coverage(
    cands: List[Dict[str, Any]],
    queries: Dict[str, Dict[str, Any]],
    plan_df: pd.DataFrame,
    specs_by_id: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    candidate_queries, signature_query_candidates, candidate_signature = build_plan_route_evidence(plan_df, specs_by_id)
    workload_ids = set(queries.keys())

    for c in cands:
        routes: Dict[str, Set[str]] = {}

        # 1) Source artifact candidates and their explicit plan rows.
        for cid in c.get("source_candidate_ids", []):
            for qid in candidate_queries.get(str(cid), set()):
                if qid in workload_ids:
                    routes.setdefault(qid, set()).add(str(cid))

        # 2) Any artifact candidate physically equivalent to one component can
        #    provide a validated route for its query. This captures exact cross-query
        #    reuse without inferring semantics from labels.
        component_sigs = set(c.get("component_deployment_signatures", []))
        for cid in c.get("source_candidate_ids", []):
            sig = candidate_signature.get(str(cid))
            if sig:
                component_sigs.add(sig)
        for sig in component_sigs:
            for qid in workload_ids:
                for cid in signature_query_candidates.get((sig, qid), []):
                    routes.setdefault(qid, set()).add(cid)

        c["route_candidate_ids_by_query"] = {q: sorted(ids) for q, ids in sorted(routes.items())}
        c["coverage"] = sorted(routes.keys())

    return cands




def candidate_physical_feature_tokens(c: Dict[str, Any]) -> frozenset:
    tokens: Set[str] = set()
    for key in ["collections", "emb", "ref", "dup", "indexes", "constraints", "edge_collections", "reverse_indexes"]:
        for value in c.get(key, []) or []:
            tokens.add(f"{key}:{value}")
    for path, lvl in nonzero_snapshot(c.get("snapshot", {})).items():
        tokens.add(f"snapshot:{path}={int(lvl)}")
    # Scope is included so the helper is safe even when accidentally used across groups.
    tokens.add(f"scope:{canon(c.get('scope', {}) or {})}")
    return frozenset(tokens)


def deployment_physical_feature_tokens(dep: Dict[str, Any]) -> frozenset:
    tokens: Set[str] = set()
    for scope in dep.get("scopes", []) or []:
        tokens.add(f"scope:{canon(scope)}")
    for key in ["collections", "emb", "ref", "dup", "indexes", "constraints", "edge_collections", "reverse_indexes"]:
        for value in dep.get(key, []) or []:
            tokens.add(f"{key}:{value}")
    for path, lvl in nonzero_snapshot(dep.get("snapshot", {})).items():
        tokens.add(f"snapshot:{path}={int(lvl)}")
    return frozenset(tokens)

def _candidate_obj_tuple(c: Dict[str, Any]) -> Tuple[float, float, float, float]:
    return (
        float(physical_cost(c)),
        float(storage_cost(c)),
        float(duplication_cost(c)),
        float(update_maintenance_cost(c)),
    )


def _obj_no_worse(a: Sequence[float], b: Sequence[float], eps: float = 1e-12) -> bool:
    return all(float(x) <= float(y) + eps for x, y in zip(a, b))


def _obj_strictly_better(a: Sequence[float], b: Sequence[float], eps: float = 1e-12) -> bool:
    return any(float(x) < float(y) - eps for x, y in zip(a, b))


def safe_candidate_dominates(a: Dict[str, Any], b: Dict[str, Any], eps: float = 1e-12) -> Tuple[bool, str]:
    """Paper-level structural dominance that is safe for the current compatibility model.

    We only compare candidates with the same compatibility signature. Therefore replacing
    b by a cannot introduce a new scope/snapshot/constraint conflict in a later Y. Candidate
    a must cover every query covered by b and be no worse in all structural penalties.
    No family label and no benchmark latency is used.
    """
    if candidate_compatibility_signature(a) != candidate_compatibility_signature(b):
        return False, "different_compatibility_profile"
    cov_a, cov_b = set(a.get("coverage", [])), set(b.get("coverage", []))
    if not cov_b or not cov_a.issuperset(cov_b):
        return False, "coverage_not_superset"
    # Safe replacement requires the dominator's deployed structures to be a
    # subset of the dominated candidate. This preserves future union costs as
    # well as compatibility; equal-cost but different feature sets are not pruned.
    feat_a, feat_b = candidate_physical_feature_tokens(a), candidate_physical_feature_tokens(b)
    if not feat_a.issubset(feat_b):
        return False, "physical_features_not_subset"
    oa, ob = _candidate_obj_tuple(a), _candidate_obj_tuple(b)
    if not _obj_no_worse(oa, ob, eps):
        return False, "higher_structural_penalty"
    strict = cov_a != cov_b or feat_a != feat_b or _obj_strictly_better(oa, ob, eps)
    return (True, "same_compatibility_profile_superset_coverage_physical_subset") if strict else (False, "equal_not_dominating")


def safe_structural_dominance_prune(cands: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """Reduce C without latency using compatibility-preserving structural dominance.

    Candidates are grouped by the compatibility signature used by Algorithms 4/5 and
    processed in increasing structural cost / decreasing coverage order.  This is an
    analytical pruning pass, not an empirical ranking step.
    """
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for c in cands:
        groups.setdefault(candidate_compatibility_signature(c), []).append(c)

    kept_all: List[Dict[str, Any]] = []
    removed: List[Dict[str, Any]] = []
    comparisons = 0
    max_frontier = 0

    for compat_sig, members in groups.items():
        ordered = sorted(
            members,
            key=lambda c: (
                _candidate_obj_tuple(c),
                -len(c.get("coverage", [])),
                c.get("deployment_signature_hash", ""),
                c["candidate_id"],
            ),
        )
        frontier: List[Dict[str, Any]] = []
        for b in ordered:
            dominated_by: Optional[Dict[str, Any]] = None
            cov_b = set(b.get("coverage", []))
            ob = _candidate_obj_tuple(b)
            feat_b = candidate_physical_feature_tokens(b)
            for a in frontier:
                comparisons += 1
                # Cheap guards before the full predicate.
                if len(a.get("coverage", [])) < len(cov_b):
                    continue
                if not candidate_physical_feature_tokens(a).issubset(feat_b):
                    continue
                if not _obj_no_worse(_candidate_obj_tuple(a), ob):
                    continue
                ok, _ = safe_candidate_dominates(a, b)
                if ok:
                    dominated_by = a
                    break
            if dominated_by is not None:
                removed.append({
                    "removed_candidate_id": b["candidate_id"],
                    "dominated_by": dominated_by["candidate_id"],
                    "reason": "safe_compatibility_preserving_structural_dominance",
                    "compatibility_signature_hash": short_hash(compat_sig, 12),
                    "removed_coverage": ";".join(b.get("coverage", [])),
                    "dominator_coverage": ";".join(dominated_by.get("coverage", [])),
                    "removed_physical_cost": physical_cost(b),
                    "dominator_physical_cost": physical_cost(dominated_by),
                    "removed_storage_cost": storage_cost(b),
                    "dominator_storage_cost": storage_cost(dominated_by),
                    "removed_duplication_cost": duplication_cost(b),
                    "dominator_duplication_cost": duplication_cost(dominated_by),
                    "removed_update_maintenance_cost": update_maintenance_cost(b),
                    "dominator_update_maintenance_cost": update_maintenance_cost(dominated_by),
                })
            else:
                frontier.append(b)
        max_frontier = max(max_frontier, len(frontier))
        kept_all.extend(frontier)

    kept_all.sort(key=lambda c: (c.get("scope_sig", ""), selection_score(c), -len(c.get("coverage", [])), c["candidate_id"]))
    return kept_all, removed, {
        "n_input": len(cands),
        "n_output": len(kept_all),
        "n_removed": len(removed),
        "n_compatibility_groups": len(groups),
        "comparisons": comparisons,
        "max_group_frontier": max_frontier,
    }

def dominates_structural(a: Dict[str, Any], b: Dict[str, Any], eps: float = 1e-12) -> Tuple[bool, str]:
    # Conservative: only compare candidates having the same family/design intention.
    if tuple(a.get("families", [])) != tuple(b.get("families", [])):
        return False, "different_design_intention"
    cov_a, cov_b = set(a.get("coverage", [])), set(b.get("coverage", []))
    if not cov_b or not cov_a.issuperset(cov_b):
        return False, "coverage_not_superset"
    obj_a = [physical_cost(a), storage_cost(a), duplication_cost(a), update_maintenance_cost(a)]
    obj_b = [physical_cost(b), storage_cost(b), duplication_cost(b), update_maintenance_cost(b)]
    if any(x > y + eps for x, y in zip(obj_a, obj_b)):
        return False, "higher_physical_penalty"
    strict = cov_a != cov_b or any(x < y - eps for x, y in zip(obj_a, obj_b))
    return (True, "coverage_superset_or_lower_physical_penalty") if strict else (False, "equal_not_dominating")


def dominance_prune(cands: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    removed: List[Dict[str, Any]] = []
    keep: List[Dict[str, Any]] = []
    for i, b in enumerate(cands):
        dominated_by = None
        why = ""
        for j, a in enumerate(cands):
            if i == j:
                continue
            ok, reason = dominates_structural(a, b)
            if ok:
                dominated_by = a["candidate_id"]
                why = reason
                break
        if dominated_by:
            removed.append({
                "removed_candidate_id": b["candidate_id"],
                "dominated_by": dominated_by,
                "reason": why,
                "coverage": ";".join(b.get("coverage", [])),
                "physical_cost": b.get("physical_cost"),
            })
        else:
            keep.append(b)
    return keep, removed


def build_complete_cover_diagnostics(
    cands: List[Dict[str, Any]],
    queries: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Coverage diagnostics for Algorithm-5 set-composition search.

    V8 deliberately does *not* require one shared candidate to cover every query
    associated with a materialisation scope.  Algorithm 5 searches compatible
    sets Y subseteq C; therefore several same-scope candidates may jointly cover
    those queries when their physical definitions are compatible.
    """
    workload = sorted(queries.keys())
    by_query_scopes: Dict[str, Set[str]] = {q: set() for q in workload}
    by_scope: Dict[str, List[Dict[str, Any]]] = {}
    for c in cands:
        if not c.get("coverage"):
            continue
        scope = str(c.get("scope_sig", ""))
        by_scope.setdefault(scope, []).append(c)
        for q in c.get("coverage", []):
            if q in by_query_scopes:
                by_query_scopes[q].add(scope)

    query_rows: List[Dict[str, Any]] = []
    for q in workload:
        scopes = sorted(by_query_scopes[q])
        candidates = [c for c in cands if q in set(c.get("coverage", []))]
        query_rows.append({
            "record_type": "query",
            "official_id": q,
            "n_candidate_scopes": len(scopes),
            "candidate_scopes": ";".join(scopes),
            "n_validated_candidates": len(candidates),
            "max_candidate_coverage": max((len(c.get("coverage", [])) for c in candidates), default=0),
            "best_covering_candidate": max(candidates, key=lambda c: len(c.get("coverage", [])))["candidate_id"] if candidates else "",
        })

    scope_rows: List[Dict[str, Any]] = []
    for scope, members in sorted(by_scope.items()):
        union_cov = sorted({q for c in members for q in c.get("coverage", []) if q in by_query_scopes})
        forced = sorted(q for q in workload if by_query_scopes[q] == {scope})
        best = max(members, key=lambda c: len(c.get("coverage", []))) if members else None
        # This field is now informational only. Lack of one mega-candidate is not
        # an Algorithm-5 blocker because a compatible set of same-scope candidates
        # may jointly cover the forced queries.
        single_cover = [c for c in members if set(forced).issubset(set(c.get("coverage", [])))] if forced else members
        scope_rows.append({
            "record_type": "scope",
            "scope_sig": scope,
            "n_candidates": len(members),
            "union_coverage_n": len(union_cov),
            "union_coverage": ";".join(union_cov),
            "forced_queries_n": len(forced),
            "forced_queries": ";".join(forced),
            "has_single_candidate_covering_all_forced_queries": bool(single_cover),
            "single_candidate_cover_required_by_algorithm5": False,
            "best_candidate_id": best.get("candidate_id", "") if best else "",
            "best_candidate_coverage_n": len(best.get("coverage", [])) if best else 0,
            "best_candidate_coverage": ";".join(best.get("coverage", [])) if best else "",
        })
    return query_rows, scope_rows


# -----------------------------------------------------------------------------
# Algorithm 5: CompatibleCovers(C,W) -> H -> ParetoPreselect(H,k)
# -----------------------------------------------------------------------------

def _unique_scopes(selected: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_sig: Dict[str, Dict[str, Any]] = {}
    for c in selected:
        scope = c.get("scope", {}) or {}
        by_sig.setdefault(canon(scope), scope)
    return [by_sig[k] for k in sorted(by_sig)]


def complete_physical_deployment_object(selected: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Canonical physical part of h = Phi(Y), excluding provenance and routing."""
    return {
        "scopes": _unique_scopes(selected),
        "collections": unique_sorted(x for c in selected for x in c.get("collections", [])),
        "emb": unique_sorted(x for c in selected for x in c.get("emb", [])),
        "ref": unique_sorted(x for c in selected for x in c.get("ref", [])),
        "dup": unique_sorted(x for c in selected for x in c.get("dup", [])),
        "snapshot": snapshot_join(selected),
        "indexes": unique_sorted(x for c in selected for x in c.get("indexes", [])),
        "constraints": unique_sorted(x for c in selected for x in c.get("constraints", [])),
        "edge_collections": unique_sorted(x for c in selected for x in c.get("edge_collections", [])),
        "reverse_indexes": unique_sorted(x for c in selected for x in c.get("reverse_indexes", [])),
    }


def complete_deployment_object(selected: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Runner-facing h = Phi(Y): physical deployment plus provenance."""
    physical = complete_physical_deployment_object(selected)
    return {
        **physical,
        "shared_candidate_ids": sorted(c["candidate_id"] for c in selected),
        "source_candidate_ids": unique_sorted(cid for c in selected for cid in c.get("source_candidate_ids", [])),
    }


def complete_deployment_signature(selected: Sequence[Dict[str, Any]]) -> str:
    """Physical deployment identity; provenance is deliberately excluded."""
    return canon(complete_physical_deployment_object(selected))


def _physical_pseudo(dep: Dict[str, Any]) -> Dict[str, Any]:
    # H-level penalties must depend only on the deployed structures, not on which
    # family/provenance path happened to produce them.
    return {
        "collections": dep.get("collections", []),
        "emb": dep.get("emb", []),
        "ref": dep.get("ref", []),
        "dup": dep.get("dup", []),
        "snapshot": dep.get("snapshot", {}),
        "edge_collections": dep.get("edge_collections", []),
        "reverse_indexes": dep.get("reverse_indexes", []),
        "families": [],
    }


def configuration_objective(selected: Sequence[Dict[str, Any]]) -> Dict[str, float]:
    dep = complete_physical_deployment_object(selected)
    pseudo = _physical_pseudo(dep)
    return {
        "physical_cost": physical_cost(pseudo),
        "storage_cost": storage_cost(pseudo),
        "duplication_cost": duplication_cost(pseudo),
        "update_maintenance_cost": update_maintenance_cost(pseudo),
        "n_candidates": float(len(selected)),
    }


def structural_score(obj: Dict[str, float]) -> float:
    # Purely analytical/physical preselection. No measured latency enters here.
    return round(
        obj["physical_cost"]
        + 0.5 * obj["storage_cost"]
        + 0.8 * obj["duplication_cost"]
        + obj["update_maintenance_cost"]
        + 0.1 * obj["n_candidates"],
        6,
    )


def candidate_subsumed_by_physical_deployment(c: Dict[str, Any], dep: Dict[str, Any]) -> bool:
    scope_sigs = {canon(x) for x in dep.get("scopes", [])}
    if canon(c.get("scope", {}) or {}) not in scope_sigs:
        return False
    for key in ["collections", "emb", "ref", "dup", "indexes", "constraints", "edge_collections", "reverse_indexes"]:
        if not set(c.get(key, [])).issubset(set(dep.get(key, []))):
            return False
    dep_snap = nonzero_snapshot(dep.get("snapshot", {}))
    for path, lvl in nonzero_snapshot(c.get("snapshot", {})).items():
        if int(dep_snap.get(path, -1)) != int(lvl):
            return False
    return True


def derive_canonical_routes_for_deployment(
    workload: Iterable[str],
    dep: Dict[str, Any],
    all_candidates: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Derive Route_h as a deterministic function of the physical deployment.

    A validated candidate route is eligible when all physical requirements of that
    candidate are contained in h. This avoids treating different provenance decompositions
    of the same Phi(Y) as different deployments merely because they expose different
    candidate ids during construction.
    """
    route_shared: Dict[str, str] = {}
    route_artifact: Dict[str, str] = {}
    for qid in sorted(set(workload)):
        options: List[Tuple[float, int, str, str]] = []
        for c in all_candidates:
            ids = (c.get("route_candidate_ids_by_query") or {}).get(qid, [])
            if not ids:
                continue
            if not candidate_subsumed_by_physical_deployment(c, dep):
                continue
            for artifact_cid in ids:
                options.append((
                    float(c.get("selection_score", selection_score(c))),
                    len(c.get("collections", [])) + len(c.get("indexes", [])) + len(c.get("dup", [])),
                    c["candidate_id"],
                    str(artifact_cid),
                ))
        if not options:
            raise ValueError(f"No validated route physically contained in complete deployment for query {qid}")
        options.sort()
        _, _, shared_id, artifact_id = options[0]
        route_shared[qid] = shared_id
        route_artifact[qid] = artifact_id
    return route_shared, route_artifact


def physical_compatibility_profile(dep: Dict[str, Any]) -> str:
    """Compatibility-relevant profile for safe partial-state Pareto pruning."""
    return canon({
        "scopes": dep.get("scopes", []),
        "snapshot_nonzero": nonzero_snapshot(dep.get("snapshot", {})),
        "constraints": unique_sorted(dep.get("constraints", [])),
    })

def choose_route_for_query(qid: str, selected: Sequence[Dict[str, Any]]) -> Tuple[str, str]:
    """Return (shared_candidate_id, concrete_artifact_candidate_id)."""
    options: List[Tuple[float, str, str]] = []
    for c in selected:
        ids = (c.get("route_candidate_ids_by_query") or {}).get(qid, [])
        for artifact_cid in ids:
            options.append((float(c.get("selection_score", selection_score(c))), c["candidate_id"], str(artifact_cid)))
    if not options:
        raise ValueError(f"No validated route for query {qid} in selected complete configuration")
    options.sort(key=lambda x: (x[0], x[1], x[2]))
    _, shared_id, artifact_id = options[0]
    return shared_id, artifact_id


def compatible_in_complete_composition(
    a: Dict[str, Any],
    b: Dict[str, Any],
    rho_max: int,
) -> Tuple[bool, str]:
    """Compatibility predicate for Algorithm-5 compositions Y.

    Unlike Algorithm-4 fusion, Algorithm 5 does not require candidates to have
    the same materialisation scope.  Different scopes may coexist as separate
    physical components.  For same-scope candidates we reuse the Algorithm-4
    physical compatibility rules.  Across scopes we reject only physical
    contradictions that can be established from the available artifact fields.
    """
    if a.get("candidate_id") == b.get("candidate_id"):
        return True, "same_candidate"

    if a.get("scope_sig") == b.get("scope_sig"):
        return compatible_pair(a, b, rho_max)

    # Different scopes are explicitly allowed by the paper.  Still guard against
    # contradictory active snapshot levels on the same physical path.
    for path in set(a.get("snapshot", {})) | set(b.get("snapshot", {})):
        av = int(a.get("snapshot", {}).get(path, 0) or 0)
        bv = int(b.get("snapshot", {}).get(path, 0) or 0)
        if av > 0 and bv > 0 and av != bv:
            return False, f"cross_scope_conflicting_snapshot_{path}"

    # If two candidates claim ownership of the same root collection but disagree
    # about owner/root key, the physical definitions are inconsistent.
    sa = a.get("scope", {}) or {}
    sb = b.get("scope", {}) or {}
    rca, rcb = str(sa.get("root_collection", "")), str(sb.get("root_collection", ""))
    if rca and rca == rcb:
        oa, ob = str(sa.get("owner", sa.get("root_entity", ""))), str(sb.get("owner", sb.get("root_entity", "")))
        ka, kb = str(sa.get("root_key", "")), str(sb.get("root_key", ""))
        if oa and ob and oa != ob:
            return False, f"inconsistent_owner_{rca}"
        if ka and kb and ka != kb:
            return False, f"inconsistent_root_key_{rca}"

    return True, "compatible_composition"


def construct_complete_configurations(
    cands: List[Dict[str, Any]],
    queries: Dict[str, Dict[str, Any]],
    max_h_raw: int,
    rho_max: int = 3,
    target_k: int = 5,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Algorithm-5 state search without exhaustive candidate-order enumeration.

    V12 separates Algorithm-5 preselection into two purely analytical phases:

      A. Exact Pareto-frontier search. A DFS uses the monotonic four-dimensional
         physical objective as a SAFE branch bound. If an already-complete H strictly
         dominates a partial physical deployment, every extension of that partial
         deployment is also dominated and cannot enter the Pareto frontier.

      B. Top-k alternative fill. If the exact Pareto frontier contains fewer than k
         complete deployments, a uniform-cost / best-first search over UNIQUE physical
         states finds the next-lowest structural-score complete deployments. Because all
         physical penalties and the candidate-count tie-break are non-decreasing under
         extension, complete states are popped in non-decreasing analytical score.

    This reproduces the existing ParetoPreselect policy (frontier first; then next-best
    structural alternatives) without enumerating millions of provenance/order paths.
    No historical or current benchmark latency is used.
    """
    workload = set(queries.keys())
    usable = [c for c in cands if c.get("coverage")]
    cand_by_id = {c["candidate_id"]: c for c in usable}
    by_query: Dict[str, List[Dict[str, Any]]] = {q: [] for q in workload}
    for c in usable:
        for q in c.get("coverage", []):
            if q in by_query:
                by_query[q].append(c)

    missing = sorted(q for q in workload if not by_query.get(q))
    if missing:
        raise SystemExit(f"Cannot construct complete H: missing validated candidate coverage for queries: {', '.join(missing)}")

    for q in by_query:
        by_query[q] = sorted(
            by_query[q],
            key=lambda c: (-len(c.get("coverage", [])), float(c.get("selection_score", 0.0)), c["candidate_id"]),
        )

    max_configs = max(1, int(max_h_raw or 20000))
    target_k = max(1, int(target_k or 5))

    # Compatibility is deterministic and repeatedly reused by both search phases.
    compat_cache: Dict[Tuple[str, str], Tuple[bool, str]] = {}
    incompatibility_counts: Dict[str, int] = {}

    def pair_compatible(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
        aid, bid = str(a["candidate_id"]), str(b["candidate_id"])
        key = (aid, bid) if aid <= bid else (bid, aid)
        cached = compat_cache.get(key)
        if cached is None:
            cached = compatible_in_complete_composition(a, b, rho_max)
            compat_cache[key] = cached
        ok, reason = cached
        if not ok:
            incompatibility_counts[reason] = incompatibility_counts.get(reason, 0) + 1
        return ok

    def candidate_compatible_with_ids(c: Dict[str, Any], selected_ids: Sequence[str]) -> bool:
        return all(pair_compatible(cand_by_id[sid], c) for sid in selected_ids)

    def build_config(selected_ids: Sequence[str], dep: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        selected = [cand_by_id[sid] for sid in selected_ids]
        dep = dep if dep is not None else complete_physical_deployment_object(selected)
        phys_sig = canon(dep)
        obj = configuration_objective(selected)
        h_id = f"H_{short_hash(phys_sig)}"
        shared_ids = sorted(selected_ids)
        source_ids = unique_sorted(cid for c in selected for cid in c.get("source_candidate_ids", []))
        runner_dep = {
            **dep,
            "shared_candidate_ids": shared_ids,
            "source_candidate_ids": source_ids,
        }
        return {
            "configuration_id": h_id,
            "workload_config_id": h_id,
            "selected_candidate_ids": ";".join(shared_ids),
            "selected_shared_candidate_ids": ";".join(shared_ids),
            "candidate_ids": ";".join(source_ids),
            "selected_source_candidate_ids": ";".join(source_ids),
            "selected_config_names": ";".join(source_ids),
            "official_ids": ";".join(sorted(workload)),
            "query_names": ";".join(str(queries[q].get("query_name", q)) for q in sorted(workload)),
            "n_queries_covered": len(workload),
            "n_selected_candidates": len(selected_ids),
            "n_candidates": len(selected_ids),
            "n_materialization_scopes": len(dep.get("scopes", [])),
            "coverage": ";".join(sorted(workload)),
            "coverage_complete": True,
            "deployment_signature_hash": short_hash(phys_sig, 12),
            "deployment_signature": phys_sig,
            "deployment_spec_json": canon(runner_dep),
            **obj,
            "structural_preselection_score": structural_score(obj),
            "generation_score": structural_score(obj),
        }

    def physical_obj_from_dep(dep: Dict[str, Any]) -> Tuple[float, float, float, float]:
        pseudo = _physical_pseudo(dep)
        return (
            float(physical_cost(pseudo)),
            float(storage_cost(pseudo)),
            float(duplication_cost(pseudo)),
            float(update_maintenance_cost(pseudo)),
        )

    def frontier_of(configs: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not configs:
            return []
        by_obj: Dict[Tuple[float, float, float, float], List[Dict[str, Any]]] = {}
        for h in configs:
            by_obj.setdefault(_objective_tuple_h(h), []).append(h)
        frontier_objs: List[Tuple[float, float, float, float]] = []
        for obj in sorted(by_obj.keys(), key=lambda x: (sum(x), x)):
            if any(_obj_tuple_dominates(existing, obj) for existing in frontier_objs):
                continue
            frontier_objs = [existing for existing in frontier_objs if not _obj_tuple_dominates(obj, existing)]
            frontier_objs.append(obj)
        fset = set(frontier_objs)
        out = [h for h in configs if _objective_tuple_h(h) in fset]
        out.sort(key=lambda h: (h["structural_preselection_score"], h["n_candidates"], h["configuration_id"]))
        return out

    # ------------------------------------------------------------------
    # Phase A: exact Pareto frontier with monotone objective branch bound
    # ------------------------------------------------------------------
    pareto_complete: List[Dict[str, Any]] = []
    pareto_seen_complete: Set[str] = set()
    pareto_seen_states: Set[Tuple[Tuple[str, ...], str]] = set()
    pareto_obj_frontier: List[Tuple[float, float, float, float]] = []
    pareto_nodes = 0
    pareto_exact_prunes = 0
    pareto_bound_prunes = 0
    pareto_duplicate_complete_prunes = 0
    # V10 exhausted this workload well below this bound; keep a conservative cap.
    pareto_max_nodes = max(500000, min(5000000, max_configs * 100))
    pareto_truncated = False

    def update_obj_frontier(obj: Tuple[float, float, float, float]) -> None:
        nonlocal pareto_obj_frontier
        if any(_obj_tuple_dominates(existing, obj) for existing in pareto_obj_frontier):
            return
        pareto_obj_frontier = [existing for existing in pareto_obj_frontier if not _obj_tuple_dominates(obj, existing)]
        if obj not in pareto_obj_frontier:
            pareto_obj_frontier.append(obj)

    def dfs_pareto(selected_ids: Tuple[str, ...], covered: frozenset) -> None:
        nonlocal pareto_nodes, pareto_truncated, pareto_exact_prunes, pareto_bound_prunes, pareto_duplicate_complete_prunes
        if pareto_nodes >= pareto_max_nodes:
            pareto_truncated = True
            return
        pareto_nodes += 1
        if pareto_nodes % 10000 == 0:
            print(
                f"[Algorithm 5 / Pareto] nodes={pareto_nodes:,} complete={len(pareto_complete):,} "
                f"frontier_objs={len(pareto_obj_frontier):,} exact_prunes={pareto_exact_prunes:,} "
                f"bound_prunes={pareto_bound_prunes:,}",
                flush=True,
            )

        selected = [cand_by_id[sid] for sid in selected_ids]
        dep = complete_physical_deployment_object(selected)
        phys_sig = canon(dep)
        state_key = (tuple(sorted(covered)), phys_sig)
        if state_key in pareto_seen_states:
            pareto_exact_prunes += 1
            return
        pareto_seen_states.add(state_key)

        if selected_ids and pareto_obj_frontier:
            pobj = physical_obj_from_dep(dep)
            if any(_obj_tuple_dominates(existing, pobj) for existing in pareto_obj_frontier):
                pareto_bound_prunes += 1
                return

        if workload.issubset(covered):
            if phys_sig in pareto_seen_complete:
                pareto_duplicate_complete_prunes += 1
                return
            pareto_seen_complete.add(phys_sig)
            h = build_config(selected_ids, dep)
            pareto_complete.append(h)
            update_obj_frontier(_objective_tuple_h(h))
            return

        uncovered = workload - set(covered)
        selected_set = set(selected_ids)
        q_options: Dict[str, List[Dict[str, Any]]] = {}
        for q in uncovered:
            opts = []
            for c in by_query[q]:
                cid = c["candidate_id"]
                if cid in selected_set:
                    continue
                if not (set(c.get("coverage", [])) & uncovered):
                    continue
                if not candidate_compatible_with_ids(c, selected_ids):
                    continue
                opts.append(c)
            q_options[q] = opts
        qid = min(uncovered, key=lambda q: (len(q_options[q]), q))
        options = q_options[qid]
        if not options:
            return
        options.sort(key=lambda c: (
            -len(set(c.get("coverage", [])) & uncovered),
            float(c.get("selection_score", selection_score(c))),
            len(c.get("source_candidate_ids", [])),
            c["candidate_id"],
        ))
        for c in options:
            if pareto_truncated:
                break
            cid = c["candidate_id"]
            new_ids = tuple(sorted((*selected_ids, cid)))
            new_covered = frozenset(set(covered) | (set(c.get("coverage", [])) & workload))
            dfs_pareto(new_ids, new_covered)

    print("[Algorithm 5] Phase A: exact analytical Pareto-frontier search...", flush=True)
    dfs_pareto(tuple(), frozenset())
    exact_frontier = frontier_of(pareto_complete)
    pareto_exhausted = not pareto_truncated
    print(
        f"[Algorithm 5] Phase A done: nodes={pareto_nodes:,}, complete_seen={len(pareto_complete):,}, "
        f"Pareto_H={len(exact_frontier):,}, exhausted={pareto_exhausted}",
        flush=True,
    )

    # If Pareto phase itself is not exhausted we cannot certify the frontier.
    # Still continue to produce diagnostics, but selection_certified will be False.
    frontier_sigs = {h["deployment_signature"] for h in exact_frontier}

    # ------------------------------------------------------------------
    # Phase B: uniform-cost search for next-best structural alternatives
    # ------------------------------------------------------------------
    fill_needed = max(0, target_k - len(exact_frontier))
    fill_complete: List[Dict[str, Any]] = []
    fill_seen_complete: Set[str] = set(frontier_sigs)
    fill_nodes = 0
    fill_exact_prunes = 0
    fill_max_nodes = max(250000, min(3000000, max_configs * 50))
    fill_truncated = False
    counter = itertools.count()

    # best known scalar score for the abstract state (coverage, physical Phi).
    best_state_score: Dict[Tuple[Tuple[str, ...], str], float] = {}
    heap: List[Tuple[float, int, int, Tuple[str, ...], frozenset, Dict[str, Any], str]] = []
    empty_dep = complete_physical_deployment_object([])
    empty_sig = canon(empty_dep)
    heapq.heappush(heap, (0.0, 0, next(counter), tuple(), frozenset(), empty_dep, empty_sig))
    best_state_score[(tuple(), empty_sig)] = 0.0

    print(
        f"[Algorithm 5] Phase B: best-first fill for {fill_needed} additional H alternatives...",
        flush=True,
    )

    while heap and len(fill_complete) < fill_needed:
        if fill_nodes >= fill_max_nodes:
            fill_truncated = True
            break
        score, neg_cov, _, selected_ids, covered, dep, phys_sig = heapq.heappop(heap)
        state_key = (tuple(sorted(covered)), phys_sig)
        if score > best_state_score.get(state_key, float("inf")) + 1e-12:
            fill_exact_prunes += 1
            continue
        fill_nodes += 1
        if fill_nodes % 10000 == 0:
            print(
                f"[Algorithm 5 / Fill] nodes={fill_nodes:,} heap={len(heap):,} "
                f"complete_fill={len(fill_complete)}/{fill_needed} state_prunes={fill_exact_prunes:,} "
                f"best_score={score:.6f}",
                flush=True,
            )

        if workload.issubset(covered):
            if phys_sig not in fill_seen_complete:
                fill_seen_complete.add(phys_sig)
                h = build_config(selected_ids, dep)
                fill_complete.append(h)
                print(
                    f"[Algorithm 5 / Fill] found H {len(fill_complete)}/{fill_needed}: "
                    f"{h['configuration_id']} score={h['structural_preselection_score']}",
                    flush=True,
                )
            continue

        uncovered = workload - set(covered)
        selected_set = set(selected_ids)
        q_options: Dict[str, List[Dict[str, Any]]] = {}
        for q in uncovered:
            opts = []
            for c in by_query[q]:
                cid = c["candidate_id"]
                if cid in selected_set:
                    continue
                if not (set(c.get("coverage", [])) & uncovered):
                    continue
                if not candidate_compatible_with_ids(c, selected_ids):
                    continue
                opts.append(c)
            q_options[q] = opts
        qid = min(uncovered, key=lambda q: (len(q_options[q]), q))
        options = q_options[qid]
        if not options:
            continue
        options.sort(key=lambda c: (
            -len(set(c.get("coverage", [])) & uncovered),
            float(c.get("selection_score", selection_score(c))),
            len(c.get("source_candidate_ids", [])),
            c["candidate_id"],
        ))

        for c in options:
            cid = c["candidate_id"]
            new_ids = tuple(sorted((*selected_ids, cid)))
            new_selected = [cand_by_id[sid] for sid in new_ids]
            new_covered = frozenset(set(covered) | (set(c.get("coverage", [])) & workload))
            new_dep = complete_physical_deployment_object(new_selected)
            new_sig = canon(new_dep)
            obj = configuration_objective(new_selected)
            new_score = structural_score(obj)
            new_key = (tuple(sorted(new_covered)), new_sig)
            old_score = best_state_score.get(new_key)
            if old_score is not None and old_score <= new_score + 1e-12:
                fill_exact_prunes += 1
                continue
            best_state_score[new_key] = new_score
            heapq.heappush(
                heap,
                (new_score, -len(new_covered), next(counter), new_ids, new_covered, new_dep, new_sig),
            )

    fill_certified = (len(fill_complete) >= fill_needed) and not fill_truncated
    selection_certified = pareto_exhausted and fill_certified
    all_configs_by_sig: Dict[str, Dict[str, Any]] = {}
    for h in [*exact_frontier, *fill_complete]:
        all_configs_by_sig.setdefault(h["deployment_signature"], h)
    configs = list(all_configs_by_sig.values())
    configs.sort(key=lambda h: (h["structural_preselection_score"], h["n_candidates"], h["configuration_id"]))

    total_nodes = pareto_nodes + fill_nodes
    return configs, {
        "search_mode": "two_phase_state_search_v14_physical_storage_exact_pareto_plus_best_first_fill",
        "nodes_visited": total_nodes,
        "max_nodes": pareto_max_nodes + fill_max_nodes,
        "max_h_raw": max_configs,
        "truncated": not selection_certified,
        # This refers to the PRESELECTION search, not enumeration of every possible H.
        "search_exhausted": selection_certified,
        "selection_certified": selection_certified,
        "pareto_search_exhausted": pareto_exhausted,
        "pareto_nodes": pareto_nodes,
        "pareto_complete_seen": len(pareto_complete),
        "exact_pareto_frontier_size": len(exact_frontier),
        "pareto_exact_state_prunes": pareto_exact_prunes,
        "pareto_objective_bound_prunes": pareto_bound_prunes,
        "fill_needed": fill_needed,
        "fill_nodes": fill_nodes,
        "fill_complete_found": len(fill_complete),
        "fill_search_certified": fill_certified,
        "fill_state_prunes": fill_exact_prunes,
        "n_complete_physical_configurations": len(configs),
        "exact_state_prunes": pareto_exact_prunes + fill_exact_prunes,
        "partial_dominance_prunes": 0,
        "duplicate_complete_prunes": pareto_duplicate_complete_prunes,
        "complete_objective_bound_prunes": pareto_bound_prunes,
        "complete_objective_frontier_size_during_search": len(exact_frontier),
        "incompatibility_counts": incompatibility_counts,
        "one_candidate_per_scope": False,
        "state_identity": "covered_queries + physical_Phi(partial_Y)",
        "all_H_enumerated": False,
        "selection_policy": "exact Pareto frontier first; if frontier<k, uniform-cost next-best structural alternatives",
    }

def deduplicate_complete_deployments(configs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for h in configs:
        buckets.setdefault(str(h["deployment_signature"]), []).append(h)

    out: List[Dict[str, Any]] = []
    report: List[Dict[str, Any]] = []
    for sig, members in buckets.items():
        members = sorted(
            members,
            key=lambda x: (
                x["structural_preselection_score"],
                x["physical_cost"],
                x["duplication_cost"],
                x["n_candidates"],
                x["configuration_id"],
            ),
        )
        kept = dict(members[0])
        kept["deployment_equivalence_class_size"] = len(members)
        out.append(kept)
        report.append({
            "deployment_signature_hash": kept["deployment_signature_hash"],
            "n_equivalent_configurations": len(members),
            "kept_configuration_id": kept["configuration_id"],
            "removed_configuration_ids": ";".join(m["configuration_id"] for m in members[1:]),
            "kept_selected_candidate_ids": kept["selected_candidate_ids"],
            "kept_selected_source_candidate_ids": kept.get("selected_source_candidate_ids", ""),
            "structural_preselection_score": kept["structural_preselection_score"],
        })

    out.sort(key=lambda h: (h["structural_preselection_score"], h["n_candidates"], h["configuration_id"]))
    return out, report


def dominates_config(a: Dict[str, Any], b: Dict[str, Any], eps: float = 1e-12) -> bool:
    objectives = ["physical_cost", "storage_cost", "duplication_cost", "update_maintenance_cost"]
    no_worse = all(float(a[o]) <= float(b[o]) + eps for o in objectives)
    strictly_better = any(float(a[o]) < float(b[o]) - eps for o in objectives)
    return no_worse and strictly_better


def _objective_tuple_h(h: Dict[str, Any]) -> Tuple[float, float, float, float]:
    return (
        float(h["physical_cost"]),
        float(h["storage_cost"]),
        float(h["duplication_cost"]),
        float(h["update_maintenance_cost"]),
    )


def _obj_tuple_dominates(a: Sequence[float], b: Sequence[float], eps: float = 1e-12) -> bool:
    return all(float(x) <= float(y) + eps for x, y in zip(a, b)) and any(
        float(x) < float(y) - eps for x, y in zip(a, b)
    )


def pareto_preselect(configs: List[Dict[str, Any]], k: int) -> List[Dict[str, Any]]:
    """Pareto preselection without O(|H|^2) configuration comparisons.

    Many complete deployments share exactly the same four analytical objective values.
    We therefore compute the frontier over UNIQUE objective tuples first and only then
    expand frontier tuples back to the physically distinct H configurations. Equal
    objective tuples remain nondominated, exactly as in the original definition.
    """
    if not configs:
        raise SystemExit("No complete workload-level configurations H were generated.")

    by_obj: Dict[Tuple[float, float, float, float], List[Dict[str, Any]]] = {}
    for h in configs:
        by_obj.setdefault(_objective_tuple_h(h), []).append(h)

    frontier_objs: List[Tuple[float, float, float, float]] = []
    for obj in sorted(by_obj.keys(), key=lambda x: (sum(x), x)):
        if any(_obj_tuple_dominates(existing, obj) for existing in frontier_objs):
            continue
        frontier_objs = [existing for existing in frontier_objs if not _obj_tuple_dominates(obj, existing)]
        frontier_objs.append(obj)

    frontier_obj_set = set(frontier_objs)
    frontier = [h for h in configs if _objective_tuple_h(h) in frontier_obj_set]
    frontier.sort(key=lambda h: (h["structural_preselection_score"], h["n_candidates"], h["configuration_id"]))

    if len(frontier) >= k:
        selected = frontier[:k]
    else:
        selected_ids = {h["configuration_id"] for h in frontier}
        rest = [h for h in configs if h["configuration_id"] not in selected_ids]
        rest.sort(key=lambda h: (h["structural_preselection_score"], h["n_candidates"], h["configuration_id"]))
        selected = frontier + rest[: max(0, k - len(frontier))]

    frontier_ids = {h["configuration_id"] for h in frontier}
    for rank, h in enumerate(selected, start=1):
        h["rank"] = rank
        h["hbench_rank"] = rank
        h["pareto_frontier_size"] = len(frontier)
        h["pareto_frontier_objective_tuples"] = len(frontier_objs)
        h["hbench_selection_role"] = "pareto" if h["configuration_id"] in frontier_ids else "bounded_fill"
    return selected


def attach_routes_to_hbench(
    hbench: List[Dict[str, Any]],
    queries: Dict[str, Dict[str, Any]],
    all_candidates: Sequence[Dict[str, Any]],
) -> None:
    """Derive Route_h only for the final Hbench deployments.

    V9 derived canonical routes for every complete H during the combinatorial search.
    With thousands of C candidates and up to 20k H states that was unnecessarily
    expensive: routing is not needed to establish physical feasibility, costs, physical
    identity, or Pareto membership. V12 postpones this operation until after Hbench is
    selected, reducing route derivation from O(|H|*|W|*|C|) to O(k*|W|*|C|).
    """
    workload = sorted(queries.keys())
    candidate_tokens = {c["candidate_id"]: candidate_physical_feature_tokens(c) for c in all_candidates}
    by_query: Dict[str, List[Dict[str, Any]]] = {q: [] for q in workload}
    for c in all_candidates:
        routes = c.get("route_candidate_ids_by_query") or {}
        for q in workload:
            if routes.get(q):
                by_query[q].append(c)

    for h in hbench:
        dep = json.loads(h["deployment_signature"])
        dep_tokens = deployment_physical_feature_tokens(dep)
        route_shared: Dict[str, str] = {}
        route_artifact: Dict[str, str] = {}
        for qid in workload:
            options: List[Tuple[float, int, str, str]] = []
            for c in by_query[qid]:
                if not candidate_tokens[c["candidate_id"]].issubset(dep_tokens):
                    continue
                for artifact_cid in (c.get("route_candidate_ids_by_query") or {}).get(qid, []):
                    options.append((
                        float(c.get("selection_score", selection_score(c))),
                        len(c.get("collections", [])) + len(c.get("indexes", [])) + len(c.get("dup", [])),
                        c["candidate_id"],
                        str(artifact_cid),
                    ))
            if not options:
                raise ValueError(f"No validated route physically contained in Hbench deployment for query {qid}")
            options.sort()
            _, _, shared_id, artifact_id = options[0]
            route_shared[qid] = shared_id
            route_artifact[qid] = artifact_id

        h["route_candidate_ids_json"] = canon(route_artifact)
        h["route_shared_candidate_ids_json"] = canon(route_shared)
        h["deployment_configs"] = ";".join(f"{qid}:{route_artifact[qid]}" for qid in workload)


def route_plan_rows(hbench: List[Dict[str, Any]], plan_df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for h in hbench:
        route_artifact = json.loads(h["route_candidate_ids_json"])
        route_shared = json.loads(h["route_shared_candidate_ids_json"])
        for official_id, artifact_candidate_id in route_artifact.items():
            sub = plan_df[
                (plan_df["official_id"].astype(str) == str(official_id))
                & (plan_df["candidate_id"].astype(str) == str(artifact_candidate_id))
            ]
            if sub.empty:
                sub = plan_df[plan_df["candidate_id"].astype(str) == str(artifact_candidate_id)]
            base = sub.iloc[0].to_dict() if not sub.empty else {
                "official_id": official_id,
                "candidate_id": artifact_candidate_id,
            }
            base.update({
                "workload_config_id": h["workload_config_id"],
                "rank": h["rank"],
                "route_candidate_id": artifact_candidate_id,
                "route_shared_candidate_id": route_shared.get(official_id, ""),
            })
            rows.append(base)
    return rows


# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

def p_rows(specs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    keys = [
        "spec_id", "candidate_id", "artifact_candidate_id", "query_name", "official_id",
        "origin_queries", "families", "g_class", "benchmark_group", "config_names",
        "scope", "scope_sig", "lambda_sig", "collections", "emb", "ref", "dup",
        "snapshot", "indexes", "constraints", "accessed_entities", "relationships_used",
        "edge_collections", "reverse_indexes", "coverage", "canonical_layout_json",
        "deployment_signature_hash", "physical_cost", "storage_cost", "duplication_cost",
        "update_maintenance_cost", "selection_score",
    ]
    return [{k: p.get(k, "") for k in keys} for p in specs]


def candidate_rows(cands: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    keys = [
        "candidate_id", "merge_kind", "source_specs", "source_candidate_ids",
        "representative_candidate_id", "origin_queries", "coverage", "families", "config_names",
        "scope", "scope_sig", "lambda_sig", "collections", "emb", "ref", "dup", "snapshot",
        "indexes", "constraints", "accessed_entities", "relationships_used", "edge_collections",
        "reverse_indexes", "route_candidate_ids_by_query", "deployment_signature_hash",
        "physical_cost", "storage_cost", "duplication_cost", "update_maintenance_cost", "selection_score",
    ]
    return [{k: c.get(k, "") for k in keys} for c in cands]


def coverage_matrix_rows(cands: List[Dict[str, Any]], query_ids: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for c in cands:
        row: Dict[str, Any] = {
            "candidate_id": c["candidate_id"],
            "merge_kind": c.get("merge_kind", ""),
            "scope_sig": c.get("scope_sig", ""),
            "source_candidate_ids": ";".join(c.get("source_candidate_ids", [])),
            "origin_queries": ";".join(c.get("origin_queries", [])),
        }
        cov = set(c.get("coverage", []))
        for q in query_ids:
            row[q] = 1 if q in cov else 0
        rows.append(row)
    return rows


def hbench_rows(hbench: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    front = [
        "workload_config_id", "rank", "deployment_configs", "candidate_ids", "selected_candidate_ids",
        "selected_shared_candidate_ids", "selected_source_candidate_ids", "route_candidate_ids_json",
        "route_shared_candidate_ids_json", "official_ids", "query_names", "n_queries_covered", "n_candidates",
        "generation_score", "structural_preselection_score", "coverage_complete", "deployment_signature_hash",
        "deployment_spec_json",
    ]
    rows: List[Dict[str, Any]] = []
    for h in hbench:
        row = {k: h.get(k, "") for k in front}
        for k, v in h.items():
            if k not in row:
                row[k] = v
        rows.append(row)
    return rows


def write_materialization_json(path: Path, cands: List[Dict[str, Any]]) -> None:
    obj: Dict[str, Any] = {}
    for c in cands:
        obj[c["candidate_id"]] = {
            "candidate_id": c["candidate_id"],
            "merge_kind": c.get("merge_kind"),
            "source_candidate_ids": c.get("source_candidate_ids", []),
            "origin_queries": c.get("origin_queries", []),
            "coverage": c.get("coverage", []),
            "families": c.get("families", []),
            "scope": c.get("scope", {}),
            "collections": c.get("collections", []),
            "emb": c.get("emb", []),
            "ref": c.get("ref", []),
            "dup": c.get("dup", []),
            "snapshot": c.get("snapshot", {}),
            "indexes": c.get("indexes", []),
            "constraints": c.get("constraints", []),
            "edge_collections": c.get("edge_collections", []),
            "reverse_indexes": c.get("reverse_indexes", []),
            "route_candidate_ids_by_query": c.get("route_candidate_ids_by_query", {}),
            "deployment_signature_hash": c.get("deployment_signature_hash"),
        }
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def write_hbench_deployment_json(path: Path, hbench: List[Dict[str, Any]], cand_by_id: Dict[str, Dict[str, Any]]) -> None:
    out: List[Dict[str, Any]] = []
    for h in hbench:
        ids = [x for x in str(h.get("selected_shared_candidate_ids", "")).split(";") if x]
        selected = [cand_by_id[x] for x in ids if x in cand_by_id]
        out.append({
            "rank": h.get("rank"),
            "workload_config_id": h.get("workload_config_id"),
            "selected_shared_candidate_ids": ids,
            "selected_source_candidate_ids": [x for x in str(h.get("selected_source_candidate_ids", "")).split(";") if x],
            "route_candidate_ids": json.loads(h["route_candidate_ids_json"]),
            "route_shared_candidate_ids": json.loads(h["route_shared_candidate_ids_json"]),
            "deployment": complete_deployment_object(selected),
            "shared_candidates": {
                c["candidate_id"]: {
                    "source_candidate_ids": c.get("source_candidate_ids", []),
                    "families": c.get("families", []),
                    "scope": c.get("scope", {}),
                    "collections": c.get("collections", []),
                    "emb": c.get("emb", []),
                    "ref": c.get("ref", []),
                    "dup": c.get("dup", []),
                    "snapshot": c.get("snapshot", {}),
                    "indexes": c.get("indexes", []),
                    "constraints": c.get("constraints", []),
                    "edge_collections": c.get("edge_collections", []),
                    "reverse_indexes": c.get("reverse_indexes", []),
                    "coverage": c.get("coverage", []),
                }
                for c in selected
            },
        })
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")


def write_summary(
    out_dir: Path,
    args: argparse.Namespace,
    queries: Dict[str, Dict[str, Any]],
    p_specs: List[Dict[str, Any]],
    c_exact: List[Dict[str, Any]],
    c_fused: List[Dict[str, Any]],
    c_validated: List[Dict[str, Any]],
    c_final: List[Dict[str, Any]],
    complete_raw: List[Dict[str, Any]],
    complete: List[Dict[str, Any]],
    hbench: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
    search_info: Dict[str, Any],
    safe_prune_info: Dict[str, Any],
) -> None:
    covered = sorted({q for c in c_final for q in c.get("coverage", [])})
    missing = sorted(set(queries) - set(covered))
    lines = [
        "# SchemaLens LDBC SNB matrix-driven physical consolidation summary",
        "",
        "## Inputs",
        "",
        f"- analytical matrix: `{args.matrix_csv}`",
        f"- artifacts dir: `{args.artifacts_dir}`",
        f"- execution plan: `{args.execution_plan}`",
        f"- scale_label: `{args.scale_label}`",
        f"- rho_max: `{args.rho_max}`",
        f"- top_k: `{args.top_k}`",
        f"- max_fusion_size: `{args.max_fusion_size}`",
        f"- mandatory safe structural dominance pruning: `YES`",
        f"- optional legacy family-restricted dominance pruning: `{args.enable_dominance_pruning}`",
        "- old benchmark used during selection: `NO`",
        "- same-scope complementary fusion: `YES (physical compatibility; provenance overlap allowed)`",
        "- H construction input: `validated shared candidates C`",
        "- executable coverage source: `artifact execution-plan evidence + physical-equivalence reuse`",
        "",
        "## Counts",
        "",
        f"- queries |W|: `{len(queries)}`",
        f"- candidate specifications |P|: `{len(p_specs)}`",
        f"- exact candidates |C_exact|: `{len(c_exact)}`",
        f"- fused candidates |C_fused|: `{len(c_fused)}`",
        f"- canonical validated candidates before safe pruning: `{len(c_validated)}`",
        f"- candidates removed by safe structural dominance: `{safe_prune_info.get('n_removed')}`",
        f"- shared candidates used for H construction |C|: `{len(c_final)}`",
        f"- complete workload-level configurations before deployment dedup |H_raw|: `{len(complete_raw)}`",
        f"- complete deployable configurations after deployment dedup |H|: `{len(complete)}`",
        f"- benchmark family |Hbench|: `{len(hbench)}`",
        "",
        "## Coverage",
        "",
        f"- covered queries: `{len(covered)}/{len(queries)}`",
        f"- missing queries: `{'; '.join(missing) if missing else 'none'}`",
        "",
        "## Compatible-cover search",
        "",
        f"- mode: `{search_info.get('search_mode')}`",
        f"- nodes visited: `{search_info.get('nodes_visited')}`",
        f"- max_h_raw: `{search_info.get('max_h_raw')}`",
        f"- truncated: `{search_info.get('truncated')}`",
        f"- search exhausted: `{search_info.get('search_exhausted')}`",
        f"- exact physical-state prunes: `{search_info.get('exact_state_prunes')}`",
        f"- partial structural-dominance prunes: `{search_info.get('partial_dominance_prunes')}`",
        f"- duplicate complete physical deployments pruned during search: `{search_info.get('duplicate_complete_prunes')}`",
        f"- selection certified: `{search_info.get('selection_certified')}`",
        f"- Pareto search exhausted: `{search_info.get('pareto_search_exhausted')}`",
        f"- exact Pareto frontier size: `{search_info.get('exact_pareto_frontier_size')}`",
        f"- Pareto phase nodes: `{search_info.get('pareto_nodes')}`",
        f"- Pareto objective-bound prunes: `{search_info.get('pareto_objective_bound_prunes')}`",
        f"- alternative-fill needed: `{search_info.get('fill_needed')}`",
        f"- alternative-fill nodes: `{search_info.get('fill_nodes')}`",
        f"- alternative-fill found: `{search_info.get('fill_complete_found')}`",
        f"- alternative-fill certified: `{search_info.get('fill_search_certified')}`",
        f"- all H enumerated: `{search_info.get('all_H_enumerated')}`",
        "",
    ]

    if warnings:
        lines.extend(["## Candidate-selection warnings", ""])
        for row in warnings:
            lines.append(
                f"- `{row.get('official_id')}`: {row.get('status')} "
                f"candidate=`{row.get('candidate_id', '')}` families=`{row.get('activated_families', '')}`"
            )
        lines.append("")

    lines.extend(["## Hbench", ""])
    for h in hbench:
        lines.append(
            f"- rank {h['rank']}: `{h['workload_config_id']}` "
            f"score={h['structural_preselection_score']} "
            f"shared_candidates=`{h['selected_shared_candidate_ids']}` "
            f"source_artifacts=`{h['selected_source_candidate_ids']}`"
        )

    lines.extend([
        "",
        "## Important runner requirement",
        "",
        "`hbench_topk.csv` keeps concrete artifact route ids for query routing, but fused/shared candidates must be materialized from `hbench_deployment_specs.json`. A runner that only loads the normalized baseline and reads `route_candidate_ids_json` does not yet realize h = Phi(Y).",
        "",
    ])

    (out_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    artifacts_dir = Path(args.artifacts_dir)
    plan_path = artifacts_dir / args.execution_plan
    specs_path = artifacts_dir / "mongodb_candidate_specs_by_candidate_id.json"
    manifest_path = artifacts_dir / "benchmark_manifest.json"
    if not plan_path.exists():
        raise FileNotFoundError(f"Missing execution plan: {plan_path}")
    if not specs_path.exists():
        raise FileNotFoundError(f"Missing candidate specs: {specs_path}")

    plan_df = normalize_plan(pd.read_csv(plan_path), scale_label=args.scale_label)
    if args.official_id:
        allowed = {normalize_official_id(x) for x in args.official_id}
        plan_df = plan_df[plan_df["official_id"].astype(str).isin(allowed)].copy()
    if args.exclude_inserts:
        plan_df = plan_df[~plan_df["official_id"].astype(str).str.startswith("INS")].copy()
    if plan_df.empty:
        raise SystemExit("No execution-plan rows selected. Check --scale-label, --official-id, and --exclude-inserts.")

    specs_by_id = read_json(specs_path)
    manifest = read_json(manifest_path) if manifest_path.exists() else {}

    plan_families_by_official = {
        official: unique_sorted(group["g_class"].dropna().astype(str).map(normalize_g).tolist())
        for official, group in plan_df.groupby("official_id")
    }

    matrix = pd.read_csv(args.matrix_csv)
    queries_list = [row_to_query(row, plan_families_by_official) for _, row in matrix.iterrows()]
    queries = {q["official_id"]: q for q in queries_list}
    if args.official_id:
        allowed = {normalize_official_id(x) for x in args.official_id}
        queries = {k: v for k, v in queries.items() if k in allowed}
    if args.exclude_inserts:
        queries = {k: v for k, v in queries.items() if not k.startswith("INS")}
    if not queries:
        raise SystemExit("No analytical-matrix queries selected.")

    explanation_records: List[Dict[str, Any]] = []
    for q in queries.values():
        explanation_records.append({
            "official_id": q["official_id"],
            "query_name": q["query_name"],
            "selected_root": q["selected_root"],
            "query_class": q["query_class"],
            "query_type": q["query_type"],
            "semantic_group": q["semantic_group"],
            "semantic_detail": q["semantic_detail"],
            "entities": q["entities"],
            "relationships": q["relationships"],
            "Rc": q["Rc"],
            "D": q["D"],
            "Re": q["Re"],
            "sharedness": q["sharedness"],
            "volatility": q["volatility"],
            "activated_families": q["activated_families"],
            "activation_source": q["activation_source"],
            "available_plan_families": plan_families_by_official.get(q["official_id"], []),
        })

    print("[Stage] Building candidate specifications P...", flush=True)
    # X -> P
    p_specs, warnings = make_p_specs(
        queries=queries,
        plan_df=plan_df,
        specs_by_id=specs_by_id,
        max_candidates_per_query=args.max_candidates_per_query,
    )
    if not p_specs:
        raise SystemExit("No candidate specifications P generated. Check matrix official_id/query_name against execution plan.")

    print(f"[Stage] P built: {len(p_specs):,}. Running Algorithm 4 exact merge/fusion...", flush=True)
    # Algorithm 4: P -> C + cover
    c_exact, exact_report = exact_canonical_merge(p_specs)
    c_fused, fusion_report = controlled_same_scope_fusion(
        c_exact,
        rho_max=args.rho_max,
        max_fusion_size=args.max_fusion_size,
        max_fused_candidates=args.max_fused_candidates,
        max_fusion_states_per_scope=args.max_fusion_states_per_scope,
    )
    c_raw = c_exact + c_fused
    c_canon, final_merge_report = final_canonical_merge(c_raw)
    c_validated = validate_coverage(c_canon, queries, plan_df, specs_by_id)

    print(f"[Stage] Algorithm 4 validated C before safe pruning: {len(c_validated):,}", flush=True)
    # V12: mandatory safe analytical reduction. This uses no benchmark latency.
    c_safe, safe_dominance_report, safe_prune_info = safe_structural_dominance_prune(c_validated)
    print(f"[Stage] Safe pruning: {len(c_validated):,} -> {len(c_safe):,} candidates", flush=True)

    # Optional legacy family-restricted pass retained only for reproducibility experiments.
    if args.enable_dominance_pruning:
        c_final, legacy_dominance_report = dominance_prune(c_safe)
    else:
        c_final, legacy_dominance_report = c_safe, []
    dominance_report = safe_dominance_report + legacy_dominance_report

    # Persist Algorithm-4 evidence before attempting Algorithm 5.  If no complete
    # cover exists, these files remain available for diagnosis.
    write_csv(out_dir / "explanation_records.csv", explanation_records)
    write_csv(out_dir / "candidate_specifications_P.csv", p_rows(p_specs))
    write_csv(out_dir / "exact_merge_report.csv", exact_report)
    write_csv(out_dir / "fusion_report.csv", fusion_report)
    write_csv(out_dir / "final_canonical_merge_report.csv", final_merge_report)
    write_csv(out_dir / "shared_candidates_C_before_safe_pruning.csv", candidate_rows(c_validated))
    write_csv(out_dir / "shared_candidates_C.csv", candidate_rows(c_final))
    write_csv(out_dir / "coverage_matrix.csv", coverage_matrix_rows(c_final, sorted(queries)))
    write_csv(out_dir / "dominance_pruning_report.csv", dominance_report)
    write_csv(out_dir / "candidate_selection_warnings.csv", warnings)
    write_materialization_json(out_dir / "shared_candidate_materialization_specs.json", c_final)

    print(f"[Stage] Starting Algorithm 5 over C={len(c_final):,} (routes are deferred until Hbench)...", flush=True)
    # Algorithm 5: C + cover -> H -> Hbench
    complete_raw, search_info = construct_complete_configurations(c_final, queries, max_h_raw=args.max_h_raw, rho_max=args.rho_max, target_k=int(args.top_k))
    if not complete_raw:
        query_diag, scope_diag = build_complete_cover_diagnostics(c_final, queries)
        write_csv(out_dir / "no_complete_H_query_diagnostics.csv", query_diag)
        write_csv(out_dir / "no_complete_H_scope_diagnostics.csv", scope_diag)
        missing_q = [r for r in query_diag if int(r.get("n_validated_candidates", 0)) == 0]
        if missing_q:
            extra = " Queries without validated candidates: " + ";".join(r["official_id"] for r in missing_q)
        else:
            extra = (
                " Coverage exists query-by-query, but no physically compatible candidate set Y was found "
                "within the bounded Algorithm-5 search. Inspect no_complete_H_query_diagnostics.csv, "
                "no_complete_H_scope_diagnostics.csv, and hbench_generation_manifest.json."
            )
        raise SystemExit(
            "No complete workload-level configurations H were generated. "
            "Algorithm-4 outputs and Algorithm-5 diagnostics were written to --out-dir." + extra
        )
    complete, deployment_dedup_report = deduplicate_complete_deployments(complete_raw)
    print(f"[Algorithm 5] unique physical H={len(complete):,}; computing Pareto frontier...", flush=True)
    hbench = pareto_preselect(complete, int(args.top_k))
    required_h = int(getattr(args, "require_hbench_size", 0) or 0)
    if required_h > 0 and len(hbench) < required_h:
        raise SystemExit(
            f"Hbench contains only {len(hbench)} physically distinct complete deployments, "
            f"but --require-hbench-size={required_h}. Do not benchmark yet; inspect the "
            f"H-search diagnostics or preserve more analytically admissible alternatives."
        )
    print(f"[Algorithm 5] Hbench selected ({len(hbench)}); deriving routes only for Hbench...", flush=True)
    attach_routes_to_hbench(hbench, queries, c_final)
    route_rows = route_plan_rows(hbench, plan_df)

    # Remaining outputs
    write_csv(out_dir / "explanation_records.csv", explanation_records)
    write_csv(out_dir / "candidate_specifications_P.csv", p_rows(p_specs))
    write_csv(out_dir / "exact_merge_report.csv", exact_report)
    write_csv(out_dir / "fusion_report.csv", fusion_report)
    write_csv(out_dir / "final_canonical_merge_report.csv", final_merge_report)
    write_csv(out_dir / "shared_candidates_C_before_safe_pruning.csv", candidate_rows(c_validated))
    write_csv(out_dir / "shared_candidates_C.csv", candidate_rows(c_final))
    write_csv(out_dir / "coverage_matrix.csv", coverage_matrix_rows(c_final, sorted(queries)))
    write_csv(out_dir / "dominance_pruning_report.csv", dominance_report)
    write_csv(out_dir / "complete_workload_configurations_H_before_deployment_dedup.csv", complete_raw)
    write_csv(out_dir / "deployment_dedup_report.csv", deployment_dedup_report)
    write_csv(out_dir / "complete_workload_configurations_H.csv", complete)
    write_csv(out_dir / "hbench_topk.csv", hbench_rows(hbench))
    pd.DataFrame(route_rows).to_csv(out_dir / "hbench_route_plan.csv", index=False)
    write_csv(out_dir / "candidate_selection_warnings.csv", warnings)

    write_materialization_json(out_dir / "shared_candidate_materialization_specs.json", c_final)
    cand_by_id = {c["candidate_id"]: c for c in c_final}
    write_hbench_deployment_json(out_dir / "hbench_deployment_specs.json", hbench, cand_by_id)

    manifest_out = {
        "script": "run_ldbc_snb_generate_hbench_topk_from_matrix_v14_physical_storage_no_old_results.py",
        "matrix_csv": str(args.matrix_csv),
        "artifacts_dir": str(args.artifacts_dir),
        "execution_plan": args.execution_plan,
        "scale_label": args.scale_label,
        "rho_max": int(args.rho_max),
        "top_k": int(args.top_k),
        "max_fusion_size": int(args.max_fusion_size),
        "max_fusion_states_per_scope": int(args.max_fusion_states_per_scope),
        "max_fused_candidates": int(args.max_fused_candidates),
        "max_candidates_per_query": int(args.max_candidates_per_query),
        "max_h_raw": int(args.max_h_raw),
        "exclude_inserts": bool(args.exclude_inserts),
        "official_id_filter": args.official_id,
        "enable_dominance_pruning": bool(args.enable_dominance_pruning),
        "n_queries": len(queries),
        "n_P": len(p_specs),
        "n_C_exact": len(c_exact),
        "n_C_fused": len(c_fused),
        "n_C_validated_before_safe_pruning": len(c_validated),
        "safe_pruning": safe_prune_info,
        "n_C_final": len(c_final),
        "n_H_raw": len(complete_raw),
        "n_H": len(complete),
        "n_Hbench": len(hbench),
        "cover_search": search_info,
        "source_manifest": manifest,
    }
    (out_dir / "hbench_generation_manifest.json").write_text(
        json.dumps(manifest_out, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    write_summary(
        out_dir,
        args,
        queries,
        p_specs,
        c_exact,
        c_fused,
        c_validated,
        c_final,
        complete_raw,
        complete,
        hbench,
        warnings,
        search_info,
        safe_prune_info,
    )
    print(f"\nOutputs written to: {out_dir.resolve()}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generate LDBC SNB Hbench Top-k with paper-aligned analytical reduction and no pre-Hbench benchmark measurements."
    )
    p.add_argument("--matrix-csv", required=True, help="Path to the LDBC analytical matrix CSV.")
    p.add_argument("--artifacts-dir", required=True, help="Path to LDBC MongoDB candidate artifact directory.")
    p.add_argument("--execution-plan", default="benchmark_execution_plan.csv", help="Execution plan CSV inside --artifacts-dir.")
    p.add_argument("--out-dir", required=True, help="Directory where matrix-consolidation outputs will be written.")
    p.add_argument("--scale-label", default=None, help="Scale-label filter; when provided and absent in the plan, fail instead of silently falling back.")
    p.add_argument("--rho-max", type=int, default=3, choices=[0, 1, 2, 3], help="Snapshot ceiling rho_max used by same-scope fusion.")
    p.add_argument("--top-k", type=int, default=5, help="Number of complete configurations retained in Hbench.")
    p.add_argument("--require-hbench-size", type=int, default=0, help="Fail unless Hbench contains at least this many physically distinct complete deployments. Use 5 for the controlled five-deployment experiment.")
    p.add_argument("--max-fusion-size", type=int, default=0, help="Maximum same-scope fusion size. 0 = automatic bounded multi-way search (recommended for LDBC).")
    p.add_argument("--max-fusion-states-per-scope", type=int, default=10000, help="Beam/state bound for multi-way fusion within one materialisation scope.")
    p.add_argument("--max-fused-candidates", type=int, default=50000, help="Safety bound for accepted fused candidates.")
    p.add_argument("--max-candidates-per-query", type=int, default=0, help="Optional P-level safety bound per query. 0 keeps all activated deployable alternatives.")
    p.add_argument("--max-h-raw", type=int, default=20000, help="Maximum number of complete covers retained before deployment deduplication.")
    p.add_argument("--enable-dominance-pruning", action="store_true", help="Optionally apply the older family-restricted dominance pass after the mandatory V12 safe compatibility-preserving dominance pass.")
    p.add_argument("--official-id", nargs="*", default=None, help="Optional subset, e.g. IC1 IC7 IS1 INS1.")
    p.add_argument("--exclude-inserts", action="store_true", help="Generate Hbench only for read queries; INS queries are omitted.")
    return p


if __name__ == "__main__":
    run(build_arg_parser().parse_args())
