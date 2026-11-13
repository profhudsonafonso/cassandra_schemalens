#!/usr/bin/env python3
"""
SchemaLens IMDb physical consolidation from analytical matrix (paper-faithful dry run).

Goal
----
Generate a Top-k benchmark family Hbench from the IMDb analytical matrix, following the
paper workflow:

  analytical matrix rows -> explanation records X
  -> candidate specifications P
  -> ConsolidateAndValidate(P, W, rho_max)
       exact canonical merge
       controlled same-scope fusion
       validated coverage
  -> ConstructAndBenchmark(C, cover, W, k)
       construct complete workload-level configurations H
       Pareto preselect Hbench

Important differences from previous v4 dry run
----------------------------------------------
1. This script DOES NOT use old benchmark results during consolidation, coverage,
   complete-configuration construction, or Hbench selection.
2. Optional old benchmark CSV is used ONLY AFTER Hbench has already been selected,
   to verify whether selected H candidates were good according to previous measurements.
3. No route-clean filter is applied, because route-clean is an implementation safeguard
   and is not a core step of Algorithm 5 in the paper.
4. Complete workload configurations H are deduplicated by deployable physical signature
   before Top-k Hbench selection. This prevents different provenance paths from
   producing repeated benchmark deployments.
5. Hbench selection uses only physical penalties named by Algorithm 5 when no candidate
   measurements are available: storage, duplication, update-maintenance, and physical
   structural cost proxies.

Usage
-----
python3 run_imdb_physical_consolidation_from_matrix_paper_v3.py \
  --matrix-csv analytical_matrix_final_df.csv \
  --out-dir paper_matrix_outputs \
  --rho-max 3 \
  --top-k 5

Optional verification with old benchmark results AFTER selection:
python3 run_imdb_physical_consolidation_from_matrix_paper_v3.py \
  --matrix-csv analytical_matrix_final_df.csv \
  --out-dir paper_matrix_outputs \
  --rho-max 3 \
  --top-k 5 \
  --verify-benchmark-csv benchmark_aggregate_results_imdb_all_sfs.csv
"""
from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import itertools
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

def canon(x: Any) -> str:
    return json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def short_hash(x: Any, n: int = 10) -> str:
    return hashlib.sha1(canon(x).encode("utf-8")).hexdigest()[:n]


def unique_sorted(xs: Iterable[Any]) -> List[str]:
    return sorted({str(x) for x in xs if x is not None and str(x) != "" and str(x).lower() != "nan"})


def parse_list_cell(value: Any) -> List[str]:
    """Parse analytical-matrix cells that may be lists, JSON-ish strings, or separators."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    if isinstance(value, list):
        return unique_sorted(value)
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return []

    # Try Python/JSON list forms first.
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, (list, tuple, set)):
                return unique_sorted(parsed)
            if isinstance(parsed, dict):
                return unique_sorted(parsed.keys())
        except Exception:
            pass
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return unique_sorted(parsed)
        except Exception:
            pass

    # Common separators found in generated CSVs.
    for sep in [";", "|", ","]:
        if sep in s:
            return unique_sorted(part.strip().strip("'\"") for part in s.split(sep))

    return [s.strip().strip("'\"")]


def normalize_entity(e: str) -> str:
    e0 = str(e).strip().strip("'\"")
    mapping = {
        "watchitem": "WatchItem",
        "watch item": "WatchItem",
        "title": "WatchItem",
        "movie": "Movie",
        "series": "Series",
        "episode": "Episode",
        "genre": "Genre",
        "role": "Role",
        "person": "Person",
        "people": "Person",
        "cast": "Person",
        "subtype": "Subtype",
    }
    return mapping.get(e0.lower(), e0)


def normalize_edge(edge: str) -> str:
    e = str(edge).strip().strip("'\"")
    if not e:
        return e
    # Normalize separators and entity names when possible.
    for sep in ["--", "->", "-", "_"]:
        if sep in e:
            parts = [normalize_entity(p) for p in e.replace("--", "-").replace("->", "-").replace("_", "-").split("-") if p.strip()]
            if len(parts) >= 2:
                return "-".join(parts[:2])
    return e


def get_first_existing(row: pd.Series, names: Sequence[str], default: Any = "") -> Any:
    for n in names:
        if n in row and pd.notna(row[n]):
            return row[n]
    return default


# -----------------------------------------------------------------------------
# Matrix-driven query evidence and activation alpha(M_i, G)
# -----------------------------------------------------------------------------

def row_to_query(row: pd.Series) -> Dict[str, Any]:
    qname = str(get_first_existing(row, ["query_name", "query", "qid", "Query"], "")).strip()
    if not qname:
        raise ValueError("Analytical matrix row does not contain a query_name-like column.")

    qid = qname.split("_")[0] if "_" in qname else qname
    root = normalize_entity(get_first_existing(row, ["selected_root", "root", "ri", "Selected_root"], ""))
    query_class = str(get_first_existing(row, ["generic_class", "query_class", "class", "workload_class"], "")).strip()
    query_type = str(get_first_existing(row, ["query_type", "operation", "op_type"], "")).strip()
    sem = str(get_first_existing(row, ["dominant_semantic_group", "semantic_group", "sem"], "")).strip()
    sem_detail = str(get_first_existing(row, ["dominant_semantic_detail", "semantic_detail"], "")).strip()
    sharedness = str(get_first_existing(row, ["query_sharedness_class", "sharedness", "share"], "")).strip()
    volatility = str(get_first_existing(row, ["query_volatility_class", "volatility", "upd"], "")).strip()

    entities = parse_list_cell(get_first_existing(row, ["Qi_entities_touched", "entities", "Ent", "Ent(Qi)", "touched_entities"], ""))
    entities = unique_sorted(normalize_entity(e) for e in entities)

    edges = parse_list_cell(get_first_existing(row, ["Rc_selected_edges", "selected_edges", "edges", "Rel", "Rel(qi)", "relationships"], ""))
    edges = unique_sorted(normalize_edge(e) for e in edges)

    depth_raw = get_first_existing(row, ["selected_document_depth", "D", "depth"], 0)
    try:
        depth = int(float(depth_raw))
    except Exception:
        depth = 0

    if not root and entities:
        # Algorithm 2 already selected root before matrix construction; this is only a fallback.
        root = entities[0]

    q = {
        "query_name": qname,
        "qid": qid,
        "root": root,
        "class": query_class,
        "query_type": query_type,
        "sem": sem,
        "sem_detail": sem_detail,
        "entities": entities,
        "edges": edges,
        "depth": depth,
        "sharedness": sharedness,
        "volatility": volatility,
    }
    q["families"] = activate_families(q)
    q["required"] = infer_required_capabilities(q)
    return q


def activate_families(q: Dict[str, Any]) -> List[str]:
    """Approximation of alpha(M_i, G) using only analytical-matrix evidence.

    This is dataset-independent in the sense that it uses semantic group, touched
    entities/edges, root, class, query type, sharedness, and volatility from M_i.
    """
    sem = f"{q.get('sem','')} {q.get('sem_detail','')} {q.get('class','')} {q.get('query_type','')}".lower()
    entities = set(q.get("entities", []))
    edges = set(q.get("edges", []))

    is_local = len(entities) <= 1 or "local" in sem or (not edges and entities == {"WatchItem"})
    is_containment = "contain" in sem or {"Series", "Episode"}.issubset(entities) and any("Series-Episode" in e for e in edges)
    is_associative = "associative" in sem or "bridge" in sem or "Role" in entities or "Person" in entities
    is_association = "association" in sem or "descriptor" in sem or "Genre" in entities or "Subtype" in entities
    is_update = "update" in sem or "insert" in sem or "write" in sem

    # Containment-only access.
    if is_containment and not is_associative and q.get("root") == "Series":
        return ["G7", "G8", "G9"]

    # Mixed associative/containment access keeps bridge-oriented alternatives.
    if is_associative:
        return ["G0", "G4", "G5", "G6"]

    if is_association:
        return ["G0", "G2", "G3"]

    if is_update or is_local:
        return ["G0"]

    # Conservative fallback.
    return ["G0"]


def infer_required_capabilities(q: Dict[str, Any]) -> List[str]:
    """Infer query requirements from the analytical matrix row.

    Paper-faithful intent: ValidEquivalentPlan(q, c) should check whether a candidate
    has the capabilities needed by the query, not whether it contains every touched
    entity as a root/basic document. Therefore, subtype/context entities such as
    WatchItem in a Series-ranking query should not automatically force
    watchitem_basic, and an associative insert should not require watchitem_basic
    when the operation can be executed by an explicit bridge/index candidate.
    """
    entities = set(q.get("entities", []))
    qname = q.get("query_name", "")
    query_type = q.get("query_type", "").lower()
    sem = f"{q.get('sem','')} {q.get('sem_detail','')} {q.get('class','')} {q.get('query_type','')} {qname}".lower()
    req = set()

    is_insert = "insert" in query_type or "insert" in sem or "add" in qname.lower()
    is_update = "update" in query_type or "update" in sem
    is_ranking_series_genre = (
        "Series" in entities
        and "Genre" in entities
        and ("ranking" in sem or "top" in qname.lower() or "rating" in sem)
    )
    is_role_access = "Role" in entities or "associative" in sem
    is_person_endpoint = "Person" in entities
    is_episode_access = "Series" in entities and "Episode" in entities
    is_genre_access = "Genre" in entities
    is_search_or_recommend = "search" in sem or "recommend" in sem or "advanced" in qname.lower()

    # Writes over an associative relationship are executable through bridge/index
    # structures; they should not require WatchItem-basic fields.
    if is_insert and is_role_access:
        req.update(["role_insert", "maintenance_safe", "role_traversal"])
        if is_person_endpoint:
            req.add("person_endpoint")
        return unique_sorted(req)

    # Series-by-Genre ranking is conceptually anchored in Series+Genre even if
    # WatchItem appears as a subtype/supertype context in the matrix.
    if is_ranking_series_genre:
        req.update(["series_genre_ranking", "series_access", "genre_filter", "rating_order"])
        return unique_sorted(req)

    if is_update:
        req.update(["watchitem_basic", "watchitem_update"])
        return unique_sorted(req)

    if is_role_access:
        req.update(["role_traversal", "role_filter"])
        if is_person_endpoint:
            req.add("person_endpoint")
        # WatchItem is only required as a basic document when the query returns or
        # filters WatchItem attributes, not merely because watchitem_id is the
        # traversal parameter.
        if is_search_or_recommend:
            req.add("watchitem_basic")

    if is_episode_access:
        req.add("series_episode_access")

    if is_genre_access:
        req.add("genre_filter")
        if "Subtype" in entities or "subtype" in sem or is_search_or_recommend:
            req.add("subtype_filter")

    if "WatchItem" in entities and not req:
        req.add("watchitem_basic")
    elif "WatchItem" in entities and (is_search_or_recommend or "title" in sem or "bytitle" in qname.lower()):
        req.add("watchitem_basic")

    return unique_sorted(req)


# -----------------------------------------------------------------------------
# gamma_spec(g_j, S, F_i, r_i, M_i): structural candidate specifications
# -----------------------------------------------------------------------------

FAMILY_TO_CONFIG = {
    "G0": "watchitem_g0", "G1": "watchitem_g1", "G2": "watchitem_g2", "G3": "watchitem_g3",
    "G4": "watchitem_g4", "G5": "watchitem_g5", "G6": "watchitem_g6",
    "G7": "series_g7", "G8": "series_g8", "G9": "series_g9",
}


def root_scope(root: str, family: str) -> Tuple[str, str, str]:
    if family == "G4":
        return "roles", "role_id", "Role"
    if family == "G6":
        return "role_endpoint_index", "endpoint_id", "RoleEndpointIndex"
    if root == "Series" or family in {"G7", "G8", "G9"}:
        return "series", "series_watchitem_id", "Series"
    return "watchitems", "watchitem_id", root or "WatchItem"


def physical_cost(c: Dict[str, Any]) -> float:
    """Structural complexity proxy used only for physical penalties.

    Not a latency estimate.
    """
    return round(
        1.0 * len(c.get("collections", []))
        + 0.7 * len(c.get("emb", []))
        + 0.3 * len(c.get("ref", []))
        + 0.5 * len(c.get("dup", []))
        + 0.6 * sum(int(v) for v in c.get("snapshot", {}).values()),
        4,
    )


def make_spec(q: Dict[str, Any], family: str) -> Dict[str, Any]:
    rc, rk, owner = root_scope(q["root"], family)
    coll, emb, ref, dup, idx, cap = set(), set(), set(), set(), set(), set()
    snap: Dict[str, int] = {}

    # Root collection baseline capabilities.
    if rc == "watchitems":
        coll.add("watchitems")
        idx.add("watchitems.watchitem_id")
        cap.update(["watchitem_basic", "title_filter", "search_predicates", "watchitem_update"])
    elif rc == "series":
        coll.add("series")
        idx.add("series.series_watchitem_id")
        cap.update(["series_access", "series_episode_access", "series_genre_ranking", "genre_filter", "rating_order"])
    elif rc == "roles":
        coll.add("roles")
        idx.update(["roles.role_id", "roles.watchitem_id", "roles.person_id", "roles.role"])
        cap.update(["role_traversal", "role_filter", "role_insert", "person_endpoint", "maintenance_safe"])
    elif rc == "role_endpoint_index":
        coll.add("role_endpoint_index")
        idx.update(["role_endpoint_index.endpoint_id", "role_endpoint_index.person_id", "role_endpoint_index.watchitem_id", "role_endpoint_index.role"])
        cap.update(["role_traversal", "role_filter", "role_insert", "person_endpoint", "maintenance_safe"])

    entities = set(q["entities"])

    if family == "G0":
        for e in entities:
            snap[e] = 0
            if e == "Genre":
                coll.add("genres"); ref.add("watchitems.genre_ids"); idx.add("genres.genre_id"); cap.add("genre_filter")
            elif e == "Role":
                coll.add("roles"); ref.add("roles.watchitem_id"); idx.update(["roles.watchitem_id", "roles.role"]); cap.update(["role_traversal", "role_filter", "role_insert"])
            elif e == "Person":
                coll.add("persons"); ref.add("roles.person_id"); idx.add("persons.person_id"); cap.add("person_endpoint")
            elif e == "Series":
                coll.add("series"); idx.add("series.series_watchitem_id"); cap.update(["series_access", "rating_order"])
            elif e == "Episode":
                coll.add("episodes"); ref.add("episodes.series_watchitem_id"); idx.add("episodes.series_watchitem_id"); cap.add("series_episode_access")
            elif e == "Movie":
                snap[e] = 0
            elif e == "Subtype":
                snap[e] = 0

    elif family == "G2":
        coll.update(["watchitems", "genres"])
        emb.add("watchitems.genres_descriptor[]")
        ref.add("watchitems.genre_ids")
        dup.add("Genre")
        snap.update({"Genre": 1})
        idx.update(["watchitems.watchitem_id", "watchitems.genres.name", "watchitems.title_type", "watchitems.rating"])
        cap.update(["watchitem_basic", "genre_filter", "subtype_filter", "ranking", "rating_order", "search_predicates"])

    elif family == "G3":
        coll.update(["watchitems", "genres"])
        emb.update(["watchitems.genres_structural[]", "watchitems.subtype_fragment"])
        dup.update(["Genre", "Subtype"])
        snap.update({"Genre": 2, "Subtype": 2})
        idx.update(["watchitems.watchitem_id", "watchitems.genres.name", "watchitems.title_type", "watchitems.rating"])
        cap.update(["watchitem_basic", "genre_filter", "subtype_filter", "ranking", "rating_order", "search_predicates"])

    elif family == "G4":
        coll.update(["watchitems", "roles", "persons"])
        ref.update(["roles.watchitem_id", "roles.person_id"])
        snap.update({"Role": 0, "Person": 0})
        idx.update(["roles.watchitem_id", "roles.person_id", "roles.role"])
        cap.update(["role_traversal", "role_filter", "role_insert", "person_endpoint", "maintenance_safe"])

    elif family == "G5":
        coll.update(["watchitems", "persons"])
        emb.add("watchitems.roles_reduced[]")
        ref.add("watchitems.roles_reduced.person_id")
        dup.add("Role")
        snap.update({"Role": 2, "Person": 0})
        idx.update(["watchitems.watchitem_id", "watchitems.roles_reduced.role", "persons.person_id"])
        cap.update(["watchitem_basic", "role_traversal", "role_filter", "role_insert", "person_endpoint"])

    elif family == "G6":
        coll.update(["role_endpoint_index", "watchitems", "persons"])
        emb.add("role_endpoint_index.watchitem_role_index[]")
        ref.update(["role_endpoint_index.person_id", "role_endpoint_index.watchitem_id"])
        dup.add("RoleEndpointIndex")
        snap.update({"Role": 1, "Person": 0, "WatchItem": 1})
        idx.update(["role_endpoint_index.endpoint_id", "role_endpoint_index.person_id", "role_endpoint_index.watchitem_id", "role_endpoint_index.role"])
        cap.update(["role_traversal", "role_filter", "role_insert", "person_endpoint", "maintenance_safe"])

    elif family == "G7":
        coll.update(["series", "episodes"])
        ref.add("episodes.series_watchitem_id")
        snap.update({"Series": 0, "Episode": 0})
        idx.update(["series.series_watchitem_id", "episodes.series_watchitem_id", "series.genres.name", "series.rating"])
        cap.update(["series_access", "series_episode_access", "series_genre_ranking", "genre_filter", "rating_order"])

    elif family == "G8":
        coll.add("series")
        emb.add("series.episodes_reduced[]")
        dup.add("Episode")
        snap.update({"Series": 0, "Episode": 2})
        idx.update(["series.series_watchitem_id", "series.genres.name", "series.rating"])
        cap.update(["series_access", "series_episode_access", "series_genre_ranking", "genre_filter", "rating_order"])

    elif family == "G9":
        coll.update(["series", "episodes"])
        emb.add("series.episodes_rich[]")
        ref.add("episodes.series_watchitem_id")
        dup.add("Episode")
        snap.update({"Series": 0, "Episode": 3})
        idx.update(["series.series_watchitem_id", "episodes.series_watchitem_id", "series.genres.name", "series.rating"])
        cap.update(["series_access", "series_episode_access", "series_genre_ranking", "genre_filter", "rating_order"])

    # Preserve all touched entities in snapshot map with level 0 if not duplicated.
    for e in entities:
        snap.setdefault(e, 0)

    layout = {
        "collections": unique_sorted(coll),
        "emb": unique_sorted(emb),
        "ref": unique_sorted(ref),
        "dup": unique_sorted(dup),
        "snapshot": dict(sorted(snap.items())),
        "indexes": unique_sorted(idx),
        "constraints": [],
    }
    scope = {"owner": owner, "root_collection": rc, "root_key": rk}
    spec = {
        "spec_id": f"{q['qid']}_{family}",
        "query_name": q["query_name"],
        "origin_queries": [q["query_name"]],
        "family": family,
        "families": [family],
        "config_names": [FAMILY_TO_CONFIG.get(family, family.lower())],
        "root": q["root"],
        "scope": scope,
        "scope_sig": canon(scope),
        "lambda_sig": short_hash(layout, 16),
        "collections": layout["collections"],
        "emb": layout["emb"],
        "ref": layout["ref"],
        "dup": layout["dup"],
        "snapshot": layout["snapshot"],
        "indexes": layout["indexes"],
        "constraints": layout["constraints"],
        "capabilities": unique_sorted(cap),
        "required_from_matrix": q["required"],
        "coverage": [],
        "source_specs": [f"{q['qid']}_{family}"],
    }
    spec["physical_cost"] = physical_cost(spec)
    return spec


# -----------------------------------------------------------------------------
# Algorithm 4: ConsolidateAndValidate(P, W, rho_max)
# -----------------------------------------------------------------------------

def snapshot_join(cands: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for c in cands:
        for k, v in c.get("snapshot", {}).items():
            out[k] = max(out.get(k, 0), int(v))
    return dict(sorted(out.items()))


def exact_canonical_merge(specs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    buckets: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for p in specs:
        buckets.setdefault((p["scope_sig"], p["lambda_sig"]), []).append(p)

    out, report = [], []
    for (scope_sig, lam), members in buckets.items():
        sources = unique_sorted(s for m in members for s in m["source_specs"])
        origins = unique_sorted(q for m in members for q in m["origin_queries"])
        c = {
            "candidate_id": f"C_EXACT_{short_hash([scope_sig, lam, sources])}",
            "merge_kind": "exact" if len(members) > 1 else "singleton",
            "source_specs": sources,
            "origin_queries": origins,
            "families": unique_sorted(f for m in members for f in m["families"]),
            "config_names": unique_sorted(x for m in members for x in m["config_names"]),
            "scope": members[0]["scope"],
            "scope_sig": scope_sig,
            "lambda_sig": lam,
            "collections": unique_sorted(x for m in members for x in m["collections"]),
            "emb": unique_sorted(x for m in members for x in m["emb"]),
            "ref": unique_sorted(x for m in members for x in m["ref"]),
            "dup": unique_sorted(x for m in members for x in m["dup"]),
            "snapshot": snapshot_join(members),
            "indexes": unique_sorted(x for m in members for x in m["indexes"]),
            "constraints": unique_sorted(x for m in members for x in m["constraints"]),
            "capabilities": unique_sorted(x for m in members for x in m["capabilities"]),
            "coverage": [],
        }
        c["physical_cost"] = physical_cost(c)
        out.append(c)
        report.append({
            "candidate_id": c["candidate_id"],
            "merge_kind": c["merge_kind"],
            "n_source_specs": len(sources),
            "source_specs": ";".join(sources),
            "origin_queries": ";".join(origins),
            "config_names": ";".join(c["config_names"]),
            "scope_sig": scope_sig,
            "lambda_sig": lam,
        })
    return out, report


def compatible_pair(a: Dict[str, Any], b: Dict[str, Any], rho_max: int) -> Tuple[bool, str]:
    if a["scope_sig"] != b["scope_sig"]:
        return False, "different_scope"
    if set(a["origin_queries"]) & set(b["origin_queries"]):
        return False, "same_origin_query_alternative"
    sj = snapshot_join([a, b])
    if max(sj.values() or [0]) > rho_max:
        return False, "snapshot_ceiling"
    for path in set(a.get("snapshot", {})) | set(b.get("snapshot", {})):
        av, bv = int(a.get("snapshot", {}).get(path, 0)), int(b.get("snapshot", {}).get(path, 0))
        if av > 0 and bv > 0 and av != bv:
            return False, f"conflicting_snapshot_{path}"
    return True, "compatible"


def join_candidates(cands: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    layout = {
        "collections": unique_sorted(x for c in cands for x in c["collections"]),
        "emb": unique_sorted(x for c in cands for x in c["emb"]),
        "ref": unique_sorted(x for c in cands for x in c["ref"]),
        "dup": unique_sorted(x for c in cands for x in c["dup"]),
        "snapshot": snapshot_join(cands),
        "indexes": unique_sorted(x for c in cands for x in c["indexes"]),
        "constraints": unique_sorted(x for c in cands for x in c["constraints"]),
    }
    c = {
        "candidate_id": f"C_FUSED_{short_hash([x['candidate_id'] for x in cands])}",
        "merge_kind": "fused",
        "source_specs": unique_sorted(x for c in cands for x in c["source_specs"]),
        "origin_queries": unique_sorted(x for c in cands for x in c["origin_queries"]),
        "families": unique_sorted(x for c in cands for x in c["families"]),
        "config_names": unique_sorted(x for c in cands for x in c["config_names"]),
        "scope": cands[0]["scope"],
        "scope_sig": cands[0]["scope_sig"],
        "lambda_sig": short_hash(layout, 16),
        "collections": layout["collections"],
        "emb": layout["emb"],
        "ref": layout["ref"],
        "dup": layout["dup"],
        "snapshot": layout["snapshot"],
        "indexes": layout["indexes"],
        "constraints": layout["constraints"],
        "capabilities": unique_sorted(x for c in cands for x in c["capabilities"]),
        "coverage": [],
    }
    c["physical_cost"] = physical_cost(c)
    return c


def controlled_same_scope_fusion(c_exact: List[Dict[str, Any]], rho_max: int, max_fusion_size: int = 2) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    fused, report = [], []
    by_scope: Dict[str, List[Dict[str, Any]]] = {}
    for c in c_exact:
        by_scope.setdefault(c["scope_sig"], []).append(c)

    for scope_sig, members in by_scope.items():
        for r in range(2, min(max_fusion_size, len(members)) + 1):
            for subset in itertools.combinations(members, r):
                reasons = []
                ok_all = True
                for a, b in itertools.combinations(subset, 2):
                    ok, reason = compatible_pair(a, b, rho_max)
                    if not ok:
                        ok_all = False
                        reasons.append(reason)
                        break
                if ok_all:
                    fc = join_candidates(subset)
                    fused.append(fc)
                    report.append({
                        "scope_sig": scope_sig,
                        "subset_size": r,
                        "source_candidates": ";".join(x["candidate_id"] for x in subset),
                        "source_specs": ";".join(fc["source_specs"]),
                        "origin_queries": ";".join(fc["origin_queries"]),
                        "fusion_status": "accepted",
                        "reason": "compatible",
                        "fused_candidate_id": fc["candidate_id"],
                    })
                else:
                    report.append({
                        "scope_sig": scope_sig,
                        "subset_size": r,
                        "source_candidates": ";".join(x["candidate_id"] for x in subset),
                        "source_specs": ";".join(unique_sorted(s for x in subset for s in x["source_specs"])),
                        "origin_queries": ";".join(unique_sorted(q for x in subset for q in x["origin_queries"])),
                        "fusion_status": "rejected",
                        "reason": reasons[0] if reasons else "not_compatible",
                        "fused_candidate_id": "",
                    })
    return fused, report


def final_canonical_merge(raw_candidates: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Remove duplicate physical structures after fusion.

    This groups by scope_sig + lambda_sig + indexes + constraints. Provenance is unioned.
    """
    buckets: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]] = {}
    for c in raw_candidates:
        key = (c["scope_sig"], c["lambda_sig"], canon(c.get("indexes", [])), canon(c.get("constraints", [])))
        buckets.setdefault(key, []).append(c)

    out, report = [], []
    for key, members in buckets.items():
        if len(members) == 1:
            out.append(members[0])
            report.append({
                "canonical_candidate_id": members[0]["candidate_id"],
                "action": "kept_unique",
                "n_merged": 1,
                "merged_candidate_ids": members[0]["candidate_id"],
                "source_specs": ";".join(members[0]["source_specs"]),
                "origin_queries": ";".join(members[0]["origin_queries"]),
                "config_names": ";".join(members[0]["config_names"]),
                "lambda_sig": members[0]["lambda_sig"],
            })
            continue

        rep = join_candidates(members)
        rep["candidate_id"] = f"C_CANON_{short_hash(key)}"
        rep["merge_kind"] = "canonical_after_fusion"
        rep["lambda_sig"] = members[0]["lambda_sig"]
        rep["physical_cost"] = physical_cost(rep)
        out.append(rep)
        report.append({
            "canonical_candidate_id": rep["candidate_id"],
            "action": "merged_physical_duplicates",
            "n_merged": len(members),
            "merged_candidate_ids": ";".join(m["candidate_id"] for m in members),
            "source_specs": ";".join(rep["source_specs"]),
            "origin_queries": ";".join(rep["origin_queries"]),
            "config_names": ";".join(rep["config_names"]),
            "lambda_sig": rep["lambda_sig"],
        })
    return out, report


def covers_query(q: Dict[str, Any], c: Dict[str, Any]) -> bool:
    req = set(q.get("required", []))
    caps = set(c.get("capabilities", []))
    # ValidEquivalentPlan approximation: required capabilities must be available.
    return req.issubset(caps)


def validate_coverage(cands: List[Dict[str, Any]], queries: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    for c in cands:
        c["coverage"] = unique_sorted(qname for qname, q in queries.items() if covers_query(q, c))
    return cands


def snapshot_cost(c: Dict[str, Any]) -> float:
    return float(sum(int(v) for v in c.get("snapshot", {}).values()))


def update_maintenance_cost(c: Dict[str, Any]) -> float:
    # Physical penalty only: duplicated structures without maintenance_safe are more expensive.
    dup_factor = len(c.get("dup", [])) + snapshot_cost(c)
    maintenance_safe = "maintenance_safe" in set(c.get("capabilities", []))
    return round(dup_factor * (0.5 if maintenance_safe else 1.0), 4)


def storage_cost(c: Dict[str, Any]) -> float:
    return round(len(c.get("collections", [])) + len(c.get("emb", [])) + len(c.get("dup", [])) + snapshot_cost(c), 4)


def dominates_structural(a: Dict[str, Any], b: Dict[str, Any], eps: float = 1e-12) -> Tuple[bool, str]:
    """Conservative dominance pruning using only physical penalties.

    A dominates B only when it has the same design-intention signature, covers a
    superset, and is no worse in physical/storage/duplication/update penalties.
    """
    if tuple(a.get("config_names", [])) != tuple(b.get("config_names", [])):
        return False, "different_design_intention"
    cov_a, cov_b = set(a.get("coverage", [])), set(b.get("coverage", []))
    if not cov_b or not cov_a.issuperset(cov_b):
        return False, "coverage_not_superset"
    obj_a = [a["physical_cost"], storage_cost(a), snapshot_cost(a), update_maintenance_cost(a)]
    obj_b = [b["physical_cost"], storage_cost(b), snapshot_cost(b), update_maintenance_cost(b)]
    if any(x > y + eps for x, y in zip(obj_a, obj_b)):
        return False, "higher_physical_penalty"
    strict = cov_a != cov_b or any(x < y - eps for x, y in zip(obj_a, obj_b))
    return (True, "coverage_superset_or_lower_physical_penalty") if strict else (False, "equal_not_dominating")


def dominance_prune(cands: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    removed = []
    keep = []
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




def nonzero_snapshot(snapshot: Dict[str, Any]) -> Dict[str, int]:
    """Return only materialized/duplicated snapshot levels.

    Level 0 means external/reference context, not a duplicated deployment fragment.
    Ignoring zero-level context avoids treating provenance-only entities as different
    deployable physical structures.
    """
    return {str(k): int(v) for k, v in sorted(snapshot.items()) if int(v) > 0}


def candidate_deployment_signature(c: Dict[str, Any]) -> str:
    """Canonical deployable physical signature for one shared candidate.

    This is intentionally stricter than config_names but less provenance-sensitive
    than lambda_sig. It represents what would be deployed: scope, collections,
    embedded paths, references, duplicated paths, non-zero snapshots, indexes, and
    constraints. Source specs, origin queries, coverage, and zero-level context
    entities are not deployment identity.
    """
    return canon({
        "scope": c.get("scope", {}),
        "collections": unique_sorted(c.get("collections", [])),
        "emb": unique_sorted(c.get("emb", [])),
        "ref": unique_sorted(c.get("ref", [])),
        "dup": unique_sorted(c.get("dup", [])),
        "snapshot_nonzero": nonzero_snapshot(c.get("snapshot", {})),
        "indexes": unique_sorted(c.get("indexes", [])),
        "constraints": unique_sorted(c.get("constraints", [])),
    })


def complete_deployment_signature(selected: Sequence[Dict[str, Any]]) -> str:
    """Canonical signature for a complete workload-level deployment."""
    return canon(sorted(candidate_deployment_signature(c) for c in selected))


# -----------------------------------------------------------------------------
# Algorithm 5: Construct workload-level configurations and Hbench preselection
# -----------------------------------------------------------------------------

def configuration_objective(selected: Sequence[Dict[str, Any]]) -> Dict[str, float]:
    return {
        "physical_cost": round(sum(c["physical_cost"] for c in selected), 4),
        "storage_cost": round(sum(storage_cost(c) for c in selected), 4),
        "duplication_cost": round(sum(len(c.get("dup", [])) + snapshot_cost(c) for c in selected), 4),
        "update_maintenance_cost": round(sum(update_maintenance_cost(c) for c in selected), 4),
        "n_candidates": float(len(selected)),
    }


def structural_score(obj: Dict[str, float]) -> float:
    # Deterministic scalar only to bound Pareto frontier to k when measurements are absent.
    # Uses physical penalties from Algorithm 5: storage, duplication, update maintenance.
    return round(
        obj["physical_cost"]
        + 0.5 * obj["storage_cost"]
        + 0.8 * obj["duplication_cost"]
        + 1.0 * obj["update_maintenance_cost"]
        + 0.1 * obj["n_candidates"],
        6,
    )


def construct_complete_configurations(cands: List[Dict[str, Any]], queries: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    workload = set(queries.keys())
    usable = [c for c in cands if c.get("coverage")]
    by_scope: Dict[str, List[Dict[str, Any]]] = {}
    for c in usable:
        by_scope.setdefault(c["scope_sig"], []).append(c)

    # One selected candidate per physical scope. If same-scope candidates were compatible,
    # they should already appear as a fused/canonical candidate.
    scope_items = [(scope, members) for scope, members in sorted(by_scope.items())]
    configs = []
    seen_keys = set()
    for choices in itertools.product(*[[None] + members for _, members in scope_items]):
        selected = [c for c in choices if c is not None]
        if not selected:
            continue
        covered = set(q for c in selected for q in c.get("coverage", []))
        if not workload.issubset(covered):
            continue
        ids = sorted(c["candidate_id"] for c in selected)
        key = tuple(ids)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        obj = configuration_objective(selected)
        deployment_sig = complete_deployment_signature(selected)
        h_id = f"H_{short_hash(ids)}"
        # Simple deterministic route derivation: for each query, list all valid candidate IDs.
        routes = {q: sorted(c["candidate_id"] for c in selected if q in c.get("coverage", [])) for q in sorted(workload)}
        configs.append({
            "configuration_id": h_id,
            "selected_candidate_ids": ";".join(ids),
            "selected_config_names": ";".join(unique_sorted(x for c in selected for x in c.get("config_names", []))),
            "n_selected_candidates": len(selected),
            "coverage": ";".join(sorted(covered)),
            "coverage_complete": True,
            "routes_json": canon(routes),
            "deployment_signature_hash": short_hash(deployment_sig, 12),
            "deployment_signature": deployment_sig,
            **obj,
            "structural_preselection_score": structural_score(obj),
        })
    return configs


def deduplicate_complete_deployments(configs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Collapse complete configurations that would deploy the same physical design.

    The paper-level object to benchmark is a complete deployable configuration, not
    a provenance path. Therefore, if several H objects differ only by source-spec
    provenance but have the same deployment_signature, keep one representative
    before Pareto/Top-k preselection.
    """
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for h in configs:
        buckets.setdefault(str(h.get("deployment_signature", "")), []).append(h)

    deduped, report = [], []
    for sig, members in buckets.items():
        members_sorted = sorted(
            members,
            key=lambda x: (
                float(x["structural_preselection_score"]),
                float(x["physical_cost"]),
                float(x["duplication_cost"]),
                float(x["update_maintenance_cost"]),
                float(x["n_candidates"]),
                x["configuration_id"],
            ),
        )
        kept = dict(members_sorted[0])
        kept["deployment_equivalence_class_size"] = len(members_sorted)
        deduped.append(kept)
        report.append({
            "deployment_signature_hash": kept.get("deployment_signature_hash", short_hash(sig, 12)),
            "n_equivalent_configurations": len(members_sorted),
            "kept_configuration_id": kept["configuration_id"],
            "removed_configuration_ids": ";".join(h["configuration_id"] for h in members_sorted[1:]),
            "kept_selected_candidate_ids": kept["selected_candidate_ids"],
            "kept_selected_config_names": kept["selected_config_names"],
            "structural_preselection_score": kept["structural_preselection_score"],
        })
    deduped = sorted(deduped, key=lambda x: (x["structural_preselection_score"], x["physical_cost"], x["duplication_cost"], x["n_selected_candidates"], x["configuration_id"]))
    return deduped, report


def dominates_config(a: Dict[str, Any], b: Dict[str, Any], eps: float = 1e-12) -> bool:
    objectives = ["physical_cost", "storage_cost", "duplication_cost", "update_maintenance_cost", "n_candidates"]
    no_worse = all(float(a[o]) <= float(b[o]) + eps for o in objectives)
    strictly_better = any(float(a[o]) < float(b[o]) - eps for o in objectives)
    return no_worse and strictly_better


def pareto_preselect(configs: List[Dict[str, Any]], k: int) -> List[Dict[str, Any]]:
    frontier = []
    for i, h in enumerate(configs):
        if not any(i != j and dominates_config(other, h) for j, other in enumerate(configs)):
            frontier.append(h)
    frontier = sorted(frontier, key=lambda x: (x["structural_preselection_score"], x["physical_cost"], x["duplication_cost"], x["n_selected_candidates"], x["configuration_id"]))
    if len(frontier) >= k:
        out = frontier[:k]
    else:
        # If frontier has fewer than k, fill with next-best nondominated/near-frontier configs.
        selected_ids = {h["configuration_id"] for h in frontier}
        rest = [h for h in configs if h["configuration_id"] not in selected_ids]
        rest = sorted(rest, key=lambda x: (x["structural_preselection_score"], x["physical_cost"], x["duplication_cost"], x["n_selected_candidates"], x["configuration_id"]))
        out = frontier + rest[: max(0, k - len(frontier))]
    for rank, h in enumerate(out, 1):
        h["hbench_rank"] = rank
        h["pareto_frontier_size"] = len(frontier)
    return out


# -----------------------------------------------------------------------------
# Optional final verification using old benchmark AFTER Hbench selection
# -----------------------------------------------------------------------------

def load_old_benchmark(path: Path):
    df = pd.read_csv(path)
    df = df[(df.get("run_phase", "") == "hot") if "run_phase" in df.columns else [True] * len(df)]
    required_cols = {"config_name", "query_name", "scale_label", "p95_latency_ms"}
    if not required_cols.issubset(set(df.columns)):
        raise ValueError(f"Benchmark CSV must contain columns: {sorted(required_cols)}")
    p95 = {}
    best = {}
    for _, r in df.iterrows():
        key = (str(r["config_name"]), str(r["query_name"]), str(r["scale_label"]))
        p95[key] = float(r["p95_latency_ms"])
        bkey = (str(r["query_name"]), str(r["scale_label"]))
        best[bkey] = min(best.get(bkey, float("inf")), float(r["p95_latency_ms"]))
    scales = sorted(df["scale_label"].astype(str).unique())
    return p95, best, scales


def verify_hbench_with_old_benchmark(hbench: List[Dict[str, Any]], candidates: List[Dict[str, Any]], benchmark_csv: Path) -> List[Dict[str, Any]]:
    p95, best, scales = load_old_benchmark(benchmark_csv)
    cand_by_id = {c["candidate_id"]: c for c in candidates}
    rows = []
    for h in hbench:
        ids = [x for x in h["selected_candidate_ids"].split(";") if x]
        selected = [cand_by_id[i] for i in ids if i in cand_by_id]
        vals = []
        missed = []
        for (qname, scale), bval in best.items():
            available = []
            for c in selected:
                if qname not in set(c.get("coverage", [])):
                    continue
                for cfg in c.get("config_names", []):
                    val = p95.get((cfg, qname, scale))
                    if val is not None and bval > 0:
                        available.append(val / bval)
            if available:
                vals.append(min(available))
            else:
                missed.append(f"{qname}@{scale}")
        rows.append({
            "configuration_id": h["configuration_id"],
            "hbench_rank": h.get("hbench_rank", ""),
            "selected_candidate_ids": h["selected_candidate_ids"],
            "selected_config_names": h["selected_config_names"],
            "old_benchmark_mean_norm_p95_after_selection": round(sum(vals) / len(vals), 6) if vals else "",
            "old_benchmark_max_norm_p95_after_selection": round(max(vals), 6) if vals else "",
            "old_benchmark_near_best_5pct_after_selection": round(sum(1 for v in vals if v <= 1.05) / len(vals), 6) if vals else "",
            "n_verified_query_scale_cases": len(vals),
            "missing_query_scale_cases": ";".join(missed),
        })
    return rows


# -----------------------------------------------------------------------------
# Output utilities
# -----------------------------------------------------------------------------

def list_to_json(x: Any) -> str:
    return json.dumps(x, ensure_ascii=False, sort_keys=True)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    # Preserve a stable broad header.
    fields = []
    for r in rows:
        for k in r.keys():
            if k not in fields:
                fields.append(k)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            clean = {}
            for k, v in r.items():
                if isinstance(v, (list, dict)):
                    clean[k] = list_to_json(v)
                else:
                    clean[k] = v
            w.writerow(clean)


def candidate_rows(cands: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    keys = [
        "candidate_id", "merge_kind", "source_specs", "origin_queries", "families", "config_names",
        "scope", "scope_sig", "lambda_sig", "collections", "emb", "ref", "dup", "snapshot",
        "indexes", "constraints", "capabilities", "coverage", "physical_cost",
    ]
    return [{k: c.get(k, "") for k in keys} for c in cands]


def coverage_matrix_rows(cands: List[Dict[str, Any]], query_names: List[str]) -> List[Dict[str, Any]]:
    rows = []
    for c in cands:
        row = {"candidate_id": c["candidate_id"], "merge_kind": c.get("merge_kind", ""), "scope_sig": c.get("scope_sig", ""), "config_names": ";".join(c.get("config_names", []))}
        cov = set(c.get("coverage", []))
        for q in query_names:
            row[q] = 1 if q in cov else 0
        rows.append(row)
    return rows


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    matrix = pd.read_csv(args.matrix_csv)
    queries = {q["query_name"]: q for q in (row_to_query(row) for _, row in matrix.iterrows())}

    # Algorithm 1: X and P from analytical matrix.
    explanation_records = []
    specs = []
    for qname, q in queries.items():
        explanation_records.append({
            "query_name": qname,
            "qid": q["qid"],
            "selected_root": q["root"],
            "query_class": q["class"],
            "query_type": q["query_type"],
            "semantic_group": q["sem"],
            "semantic_detail": q["sem_detail"],
            "entities": q["entities"],
            "edges": q["edges"],
            "depth": q["depth"],
            "activated_families": q["families"],
            "required_capabilities": q["required"],
        })
        for fam in q["families"]:
            specs.append(make_spec(q, fam))

    # Algorithm 4: ConsolidateAndValidate.
    c_exact, exact_report = exact_canonical_merge(specs)
    c_fused, fusion_report = controlled_same_scope_fusion(c_exact, args.rho_max, args.max_fusion_size)
    c_raw = c_exact + c_fused
    c_canon, final_merge_report = final_canonical_merge(c_raw)
    c_validated = validate_coverage(c_canon, queries)

    if args.enable_dominance_pruning:
        c_pruned, dominance_report = dominance_prune(c_validated)
    else:
        c_pruned, dominance_report = c_validated, []

    # Algorithm 5: Compatible covers -> complete configurations -> Hbench.
    complete_configs_raw = construct_complete_configurations(c_pruned, queries)
    complete_configs, deployment_dedup_report = deduplicate_complete_deployments(complete_configs_raw)
    hbench = pareto_preselect(complete_configs, args.top_k)

    # Write outputs.
    write_csv(out_dir / "explanation_records.csv", explanation_records)
    write_csv(out_dir / "candidate_specifications_P.csv", specs)
    write_csv(out_dir / "exact_merge_report.csv", exact_report)
    write_csv(out_dir / "fusion_report.csv", fusion_report)
    write_csv(out_dir / "final_canonical_merge_report.csv", final_merge_report)
    write_csv(out_dir / "shared_candidates_C.csv", candidate_rows(c_pruned))
    write_csv(out_dir / "coverage_matrix.csv", coverage_matrix_rows(c_pruned, sorted(queries)))
    write_csv(out_dir / "dominance_pruning_report.csv", dominance_report)
    write_csv(out_dir / "complete_workload_configurations_H_before_deployment_dedup.csv", complete_configs_raw)
    write_csv(out_dir / "deployment_dedup_report.csv", deployment_dedup_report)
    write_csv(out_dir / "complete_workload_configurations_H.csv", complete_configs)
    write_csv(out_dir / "hbench_topk.csv", hbench)

    verification_rows = []
    if args.verify_benchmark_csv:
        verification_rows = verify_hbench_with_old_benchmark(hbench, c_pruned, Path(args.verify_benchmark_csv))
        write_csv(out_dir / "hbench_old_benchmark_verification_AFTER_SELECTION.csv", verification_rows)

    covered_queries = sorted({q for c in c_pruned for q in c.get("coverage", [])})
    missing = sorted(set(queries) - set(covered_queries))
    summary = f"""# SchemaLens matrix-driven physical consolidation summary

## Inputs

- analytical matrix: `{args.matrix_csv}`
- rho_max: `{args.rho_max}`
- top_k: `{args.top_k}`
- old benchmark used during selection: `NO`
- old benchmark verification after selection: `{args.verify_benchmark_csv or 'not provided'}`
- dominance pruning enabled: `{args.enable_dominance_pruning}`

## Counts

- queries |W|: `{len(queries)}`
- candidate specifications |P|: `{len(specs)}`
- exact candidates |C_exact|: `{len(c_exact)}`
- fused candidates |C_fused|: `{len(c_fused)}`
- raw candidates |C_raw|: `{len(c_raw)}`
- shared candidates after final canonical merge: `{len(c_canon)}`
- shared candidates used for configuration construction |C|: `{len(c_pruned)}`
- complete workload-level configurations before deployment dedup |H_raw|: `{len(complete_configs_raw)}`
- complete deployable configurations after deployment dedup |H|: `{len(complete_configs)}`
- benchmark family |Hbench|: `{len(hbench)}`

## Coverage

- covered queries: `{len(covered_queries)}/{len(queries)}`
- missing queries: `{'; '.join(missing) if missing else 'none'}`

## Hbench

"""
    for h in hbench:
        summary += f"- rank {h.get('hbench_rank')}: `{h['configuration_id']}` score={h['structural_preselection_score']} candidates=`{h['selected_candidate_ids']}` configs=`{h['selected_config_names']}`\n"
    if verification_rows:
        summary += "\n## Old benchmark verification AFTER selection\n\n"
        for r in verification_rows:
            summary += f"- rank {r['hbench_rank']}: mean_norm={r['old_benchmark_mean_norm_p95_after_selection']}, max_norm={r['old_benchmark_max_norm_p95_after_selection']}, near_best={r['old_benchmark_near_best_5pct_after_selection']}\n"

    (out_dir / "summary.md").write_text(summary, encoding="utf-8")

    print(summary)
    print(f"\nOutputs written to: {out_dir.resolve()}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="SchemaLens paper-faithful physical consolidation from IMDb analytical matrix.")
    p.add_argument("--matrix-csv", required=True, help="Path to IMDb analytical matrix CSV.")
    p.add_argument("--out-dir", default="paper_matrix_outputs", help="Output directory.")
    p.add_argument("--rho-max", type=int, default=3, help="Snapshot ceiling rho_max.")
    p.add_argument("--top-k", type=int, default=5, help="Number of complete configurations to keep in Hbench.")
    p.add_argument("--max-fusion-size", type=int, default=2, help="Maximum same-scope fusion subset size. Paper example uses pairwise fusion; keep 2 by default.")
    p.add_argument("--enable-dominance-pruning", action="store_true", help="Enable conservative structural dominance pruning. Off by default to avoid adding an extra pruning effect.")
    p.add_argument("--verify-benchmark-csv", default=None, help="Optional old benchmark CSV used ONLY AFTER Hbench selection for verification.")
    return p


if __name__ == "__main__":
    run(build_arg_parser().parse_args())
