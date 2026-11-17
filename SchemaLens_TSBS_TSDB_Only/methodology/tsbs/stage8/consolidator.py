"""Exact, small design-space enumeration over Stage 4 physical TSDB candidates.

Stages 4-7 are immutable inputs. This module has deliberately NO dependency on
TSBS benchmark logs: selection uses explicit synthetic *analytical* cost proxies.
Run Stage 9 on the complete HBench deployments selected here, not on individual
queries in isolation. This is NOT a measured or cost-optimal HBench.

Coverage is structural only. Conditional aggregates are never called
unconditionally executable: interval-alignment and Stage 5 runtime guards
must be checked when concrete TSBS query parameters are available.
"""
from __future__ import annotations

import csv
import hashlib
import itertools
import json
import math
from pathlib import Path
from typing import Any


INPUTS = {
    "specs": "outputs/tsbs_stage4/10_cassandra_candidate_specs.json",
    "query_map": "outputs/tsbs_stage4/11_query_candidate_map.json",
    "stage4_manifest": "outputs/tsbs_stage4/14_stage4_manifest.json",
    "read_plans": "outputs/tsbs_stage5/15_query_plans.json",
    "stage5_manifest": "outputs/tsbs_stage5/17_stage5_manifest.json",
    "write_plans": "outputs/tsbs_stage6/18_write_plans.json",
    "stage6_manifest": "outputs/tsbs_stage6/20_stage6_manifest.json",
    "footprint": "outputs/tsbs_stage7/21_physical_footprint_proxies.json",
    "writes": "outputs/tsbs_stage7/22_write_amplification_proxies.json",
    "read_costs": "outputs/tsbs_stage7/23_read_cost_proxies.json",
    "stage7_manifest": "outputs/tsbs_stage7/27_stage7_manifest.json",
    "workload": "inputs/workload/query_manifest.csv",
}


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def _unique(items: list[dict], key: str, label: str) -> dict[str, dict]:
    vals = [item[key] for item in items]
    if len(vals) != len(set(vals)):
        raise ValueError(f"Duplicate {label} keys ({key})")
    return dict(zip(vals, items))


def _finite_positive(value: Any, name: str, *, allow_zero: bool = False) -> float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"{name}: expected finite number")
    if value < 0 or (not allow_zero and value == 0):
        raise ValueError(f"{name}: expected positive value")
    return float(value)


def _weights(d: dict, keys: set[str], label: str) -> dict[str, float]:
    if set(d) != keys:
        raise ValueError(f"{label} requires {sorted(keys)}")
    result = {key: _finite_positive(v, f"{label}.{key}", allow_zero=True) for key, v in d.items()}
    if abs(sum(result.values()) - 1.0) > 1e-10:
        raise ValueError(f"{label} weights must add to exactly one")
    return result


def _policy(p: dict) -> dict:
    if p.get("scope") != "TSDB_ONLY_WORKLOAD_CONSOLIDATION_BEFORE_BENCHMARK":
        raise ValueError("Stage 8 policy has wrong scope")
    if p.get("benchmark_metrics_allowed_for_selection") is not False:
        raise ValueError("Cannot use empirical benchmark costs for design selection")
    if p.get("assume_equal_query_frequencies") is not True:
        raise ValueError("Only declared uniform illustrative frequencies supported")
    if p.get("profiles") != ["conservative", "conditional_aligned"]:
        raise ValueError("Both explicit profiles are required")
    for key in ("top_k", "exact_enumeration_limit_candidates", "max_tables_per_deployment"):
        if type(p.get(key)) is not int or p[key] <= 0:
            raise ValueError(f"{key} must be a positive integer")
    if p.get("dominance") != "pareto_read_storage_touched_write":
        raise ValueError("Only validated three-objective Pareto dominance is supported")
    if type(p.get("allow_duplicate_family_variants")) is not bool:
        raise ValueError("allow_duplicate_family_variants must be boolean")
    _weights(p["read_proxy_weights"], {"cql_requests", "logical_rows_read"}, "read")
    _weights(p["deployment_proxy_weights"], {"normalized_read", "normalized_storage", "normalized_touched_write_keys"}, "deployment")
    return p


def load_inputs(root: Path, policy_path: Path | None = None) -> dict:
    root = root.resolve()
    path = (policy_path or root / "config/tsbs_stage8_consolidation_policy.json").resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Stage 8 missing policy: {path}")
    p = _policy(_json(path))
    files = {key: root / rel for key, rel in INPUTS.items()}
    missing = [rel for key, rel in INPUTS.items() if not files[key].is_file()]
    if missing:
        raise FileNotFoundError(f"Run Stages 3-7 first. Missing: {', '.join(missing)}")
    raw = {key: _json(path) for key, path in files.items() if key != "workload"}
    with files["workload"].open(encoding="utf-8", newline="") as inp:
        raw["workload"] = list(csv.DictReader(inp))
    for step, expected in ((4, "TSDB_ONLY_SCHEMA_INSTANTIATION"),
                           (5, "TSDB_ONLY_QUERY_PLAN_STRUCTURAL_VALIDATION"),
                           (6, "TSDB_ONLY_WRITE_PLAN_REFERENCE"),
                           (7, "TSDB_ONLY_ANALYTICAL_COST_AND_FEASIBILITY_NOT_RANKED")):
        manifest = raw[f"stage{step}_manifest"]
        if manifest.get("stage") != step or manifest.get("scope") != expected:
            raise ValueError(f"Unexpected Stage {step} provenance")
        if manifest.get("C0_C9_used", False) or manifest.get("uses_cassandra_general_c0_c9", False):
            raise ValueError(f"Stage {step} includes general Cassandra activation")
    if raw["stage7_manifest"].get("C0_C9_used") is not False:
        raise ValueError("Wrong Stage 7 TSDB-only scope")
    # Enforce input integrity relative to each upstream manifest, not merely local existence.
    proof = {
        "specs": ("stage7_manifest", "candidate_specs"),
        "query_map": ("stage7_manifest", "query_map"),
        "read_plans": ("stage7_manifest", "query_plans"),
        "write_plans": ("stage7_manifest", "write_plans"),
        "workload": ("stage7_manifest", "manifest"),
        "stage4_manifest": ("stage7_manifest", "stage4_manifest"),
        "stage5_manifest": ("stage7_manifest", "stage5_manifest"),
        "stage6_manifest": ("stage7_manifest", "stage6_manifest"),
    }
    for filename, (manifest_key, checksum_name) in proof.items():
        expected = raw[manifest_key]["source_files_sha256"][checksum_name]
        if _sha(files[filename]) != expected:
            raise ValueError(f"Upstream Stage 7 integrity check failed: {filename}")
    # Stage 7 proxy outputs have no checksums in Stage 7 manifest, so persist all
    # their digests in Stage 8 output for reproducibility.
    raw["input_hashes"] = {key: _sha(path) for key, path in files.items()}
    raw["input_hashes"]["stage8_policy"] = _sha(path)
    raw["policy"] = p
    return raw


def validate(raw: dict) -> dict:
    specs = _unique(raw["specs"], "candidate_id", "candidates")
    read_plans = _unique(raw["read_plans"], "plan_id", "QueryPlans")
    write_plans = _unique(raw["write_plans"], "candidate_id", "WritePlans")
    footprints = _unique(raw["footprint"], "candidate_id", "footprints")
    writes = _unique(raw["writes"], "candidate_id", "write-cost proxies")
    rc = _unique(raw["read_costs"], "plan_id", "read-cost proxies")
    workload = _unique(raw["workload"], "query_id", "query template")
    query_map = _unique(raw["query_map"], "query_id", "Stage 4 query mapping")
    ids = set(specs)
    if ids != set(write_plans) or ids != set(footprints) or ids != set(writes):
        raise ValueError("Candidate, WritePlan, storage and write proxy IDs diverge")
    if set(read_plans) != set(rc) or set(workload) != set(query_map):
        raise ValueError("Read cost/query-plan or query-template mapping is incomplete")
    if len(workload) != raw["stage7_manifest"]["query_count"]:
        raise ValueError("Stage 7 query count disagrees with workload")
    if len(specs) != raw["stage7_manifest"]["candidate_count"] or len(read_plans) != raw["stage7_manifest"]["query_plan_count"]:
        raise ValueError("Stage 7 candidate or plan count mismatch")
    canonical = raw["policy"]["canonical_source_candidate_id"]
    if canonical not in specs or specs[canonical]["family"] != "TS0":
        raise ValueError("Canonical raw TS0 source missing")
    for qid, qmap in query_map.items():
        declared = set(itertools.chain.from_iterable(qmap["candidate_ids_by_family"].values()))
        actual = {p["candidate_id"] for p in read_plans.values() if p["query_id"] == qid}
        if not actual or actual != declared:
            raise ValueError(f"Stage 4/5 query coverage mismatch: {qid}")
    for plan_id, plan in read_plans.items():
        qid, cid = plan["query_id"], plan["candidate_id"]
        if qid not in workload or cid not in specs:
            raise ValueError(f"Read plan {plan_id} references unknown query or candidate")
        if plan["family"] != specs[cid]["family"] or rc[plan_id]["candidate_id"] != cid or rc[plan_id]["query_id"] != qid:
            raise ValueError(f"Read cost {plan_id} does not match physical candidate")
        if plan.get("static_cql_validated") is not True:
            raise ValueError(f"Static CQL validation missing: {plan_id}")
        if plan.get("coverage_class") != "STRUCTURAL_WITH_RUNTIME_GUARDS":
            raise ValueError(f"Unexpected query coverage assertion: {plan_id}")
        _finite_positive(rc[plan_id]["estimated_cql_requests_upper_proxy"], plan_id+" requests")
        _finite_positive(rc[plan_id]["estimated_logical_rows_returned_upper_proxy"], plan_id+" rows")
        if rc[plan_id].get("measured_latency_ms") is not None:
            raise ValueError("Measured latency must not influence HBench planning")
        if rc[plan_id].get("query_relative_frequency") is not None:
            raise ValueError("Unexpected measured workload frequency: explicit uniform assumption required")
    for cid, spec in specs.items():
        if spec["family"] not in {"TS0", "TS1", "TS2", "TS5", "TS6"}:
            raise ValueError(f"Stage 8 unexpected TSDB physical family: {spec['family']}")
        if write_plans[cid]["family"] != spec["family"]:
            raise ValueError(f"Write plan disagrees with candidate {cid}")
        if writes[cid]["canonical_source_required"] != (spec["family"] in ("TS5", "TS6")):
            raise ValueError(f"Derived dependency mismatch for candidate {cid}")
        _finite_positive(footprints[cid]["estimated_total_storage_bytes_proxy"], cid+" storage")
        _finite_positive(writes[cid]["distinct_physical_group_rows_touched_by_new_observation_proxy"], cid+" write keys")
    expected_baseline = {q: f"{q}__{canonical}" for q in workload}
    if not set(expected_baseline.values()).issubset(read_plans):
        raise ValueError("Complete canonical baseline query coverage missing")
    return {"specs": specs, "plans": read_plans, "writer": write_plans,
            "storage": footprints, "writes": writes, "reads": rc,
            "workload": workload, "baseline_plan_ids": expected_baseline,
            "query_order": sorted(workload)}


def read_score(plan_id: str, qid: str, state: dict, policy: dict) -> float:
    proxy = state["reads"][plan_id]
    ref = state["reads"][state["baseline_plan_ids"][qid]]
    w = policy["read_proxy_weights"]
    # Relative per-query components avoid one high-cardinality template defining
    # the entire objective. This is not a performance or monetary cost model.
    return (w["cql_requests"] * proxy["estimated_cql_requests_upper_proxy"] / ref["estimated_cql_requests_upper_proxy"]
            + w["logical_rows_read"] * proxy["estimated_logical_rows_returned_upper_proxy"] / ref["estimated_logical_rows_returned_upper_proxy"])


def _allowed_plan(plan: dict, profile: str) -> bool:
    if profile == "conservative" and plan.get("alignment_guard"):
        return False
    return True


def _make_deployment(subset: tuple[str, ...], profile: str, state: dict, policy: dict) -> dict | None:
    chosen = set(subset)
    source_id = policy["canonical_source_candidate_id"]
    if source_id not in chosen:
        return None
    if not policy["allow_duplicate_family_variants"]:
        families = [state["specs"][cid]["family"] for cid in chosen]
        if any(families.count(family) > 1 for family in ("TS1",)):
            return None
    by_q: dict[str, list[dict]] = {qid: [] for qid in state["query_order"]}
    for plan in state["plans"].values():
        if plan["candidate_id"] in chosen and _allowed_plan(plan, profile):
            by_q[plan["query_id"]].append(plan)
    if any(not plans for plans in by_q.values()):
        return None
    routes = {}
    used = set()
    query_scores = []
    for qid in state["query_order"]:
        best = min(by_q[qid], key=lambda p: (read_score(p["plan_id"], qid, state, policy), p["plan_id"]))
        used.add(best["candidate_id"])
        qs = read_score(best["plan_id"], qid, state, policy)
        query_scores.append(qs)
        routes[qid] = {
            "query_name": state["workload"][qid]["query_name"],
            "selected_plan_id": best["plan_id"],
            "candidate_id": best["candidate_id"],
            "family": best["family"],
            "relative_read_proxy": round(qs, 10),
            "alignment_guard": best["alignment_guard"],
            "unverified_runtime_requirements": best["requirements"],
            "coverage_status": "CONDITIONAL_STRUCTURAL_NOT_RUNTIME_VERIFIED",
        }
    # Canonicalize deployment: idle candidate tables are not counted as a
    # different configuration; their identical read routes do not justify writes.
    if chosen != used | {source_id}:
        return None
    baseline_storage = state["storage"][source_id]["estimated_total_storage_bytes_proxy"]
    baseline_write = state["writes"][source_id]["distinct_physical_group_rows_touched_by_new_observation_proxy"]
    values = {
        "normalized_read": sum(query_scores)/len(query_scores),
        "normalized_storage": sum(state["storage"][cid]["estimated_total_storage_bytes_proxy"] for cid in subset)/baseline_storage,
        "normalized_touched_write_keys": sum(state["writes"][cid]["distinct_physical_group_rows_touched_by_new_observation_proxy"] for cid in subset)/baseline_write,
    }
    weights = policy["deployment_proxy_weights"]
    score = sum(weights[k] * values[k] for k in weights)
    align = sorted(qid for qid, r in routes.items() if r["alignment_guard"])
    flags = sorted(set(itertools.chain.from_iterable(state["storage"][cid]["warnings"] for cid in chosen)))
    dep_id = "H_" + hashlib.sha256((profile+"|"+"|".join(subset)).encode()).hexdigest()[:12]
    return {
        "deployment_id": dep_id,
        "profile": profile,
        "candidate_ids": list(subset),
        "physical_table_names": [state["specs"][cid]["table_name"] for cid in subset],
        "table_count": len(subset),
        "covered_queries_structural": len(routes),
        "query_routes": routes,
        "required_write_plan_ids": list(subset),
        "derived_candidates_require_durable_source": [cid for cid in subset if state["writer"][cid]["write_strategy"] == "derived_recompute"],
        "normalized_proxies": {k: round(v, 10) for k,v in values.items()},
        "weighted_proxy_score": round(score, 10),
        "conditional_aligned_query_ids": align,
        "physical_warnings": flags,
        "all_queries_runtime_verified": False,
        "distributed_write_maintenance_verified": False,
        "cassandra_ddl_dml_executed": False,
        "benchmark_executed": False,
    }


def _dominates(a: dict, b: dict) -> bool:
    x, y = a["normalized_proxies"], b["normalized_proxies"]
    keys = ("normalized_read", "normalized_storage", "normalized_touched_write_keys")
    return all(x[k] <= y[k] + 1e-9 for k in keys) and any(x[k] < y[k] - 1e-9 for k in keys)


def _pareto(rows: list[dict]) -> list[dict]:
    # Optimize comparisons for reproducible tiny (<=2^15) design spaces.
    return [v for i, v in enumerate(rows)
            if not any(_dominates(u, v) for j, u in enumerate(rows) if i != j)]


def search(state: dict, policy: dict) -> dict:
    ids = sorted(state["specs"])
    mandatory = policy["canonical_source_candidate_id"]
    optional = [x for x in ids if x != mandatory]
    if len(ids) > policy["exact_enumeration_limit_candidates"]:
        raise ValueError("Exact search budget exceeded. Do not silently truncate; revise declared policy")
    records = []
    counts = {}
    for profile in policy["profiles"]:
        subsets_checked = 0
        deployments: list[dict] = []
        for n in range(min(len(optional),policy["max_tables_per_deployment"]-1)+1):
            for combo in itertools.combinations(optional, n):
                subsets_checked += 1
                subset = tuple(sorted((mandatory,)+combo))
                dep = _make_deployment(subset, profile, state, policy)
                if dep is not None:
                    deployments.append(dep)
        frontier = _pareto(deployments)
        frontier.sort(key=lambda d: (d["weighted_proxy_score"], d["table_count"], d["candidate_ids"]))
        top = frontier[:policy["top_k"]]
        for rank, row in enumerate(top, start=1):
            row["rank"] = rank
        counts[profile] = {
            "subsets_checked": subsets_checked,
            "canonical_complete_deployments": len(deployments),
            "pareto_frontier_size": len(frontier),
            "hbench_top_k_selected": len(top),
        }
        records.extend(top)
    return {"selected": records, "counts": counts}


def generate(root: Path, output: Path, policy_path: Path | None = None) -> dict:
    inputs = load_inputs(root, policy_path)
    state = validate(inputs)
    policy = inputs["policy"]
    result = search(state, policy)
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    _dump(output / "28_hbench_selected_deployments.json", result["selected"])
    coverage = []
    for dep in result["selected"]:
        for qid, route in dep["query_routes"].items():
            coverage.append({"deployment_id": dep["deployment_id"], "profile": dep["profile"],
                             "rank": dep["rank"], "query_id": qid, **route})
    _dump(output / "29_hbench_selected_query_routes.json", coverage)
    with (output / "30_hbench_summary.csv").open("w", encoding="utf-8", newline="") as f:
        fields = ["profile", "rank", "deployment_id", "table_count", "candidate_ids", "covered_queries_structural", "conditional_query_count", "normalized_read", "normalized_storage", "normalized_touched_write_keys", "weighted_proxy_score", "physical_warnings"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for dep in result["selected"]:
            writer.writerow({
                "profile": dep["profile"], "rank": dep["rank"], "deployment_id": dep["deployment_id"],
                "table_count": dep["table_count"], "candidate_ids": ";".join(dep["candidate_ids"]),
                "covered_queries_structural": dep["covered_queries_structural"],
                "conditional_query_count": len(dep["conditional_aligned_query_ids"]),
                **dep["normalized_proxies"], "weighted_proxy_score": dep["weighted_proxy_score"],
                "physical_warnings": ";".join(dep["physical_warnings"]),
            })
    # Stage 9 preparation manifest only; never an executable benchmark harness.
    with (output / "32_stage9_execution_plan.csv").open("w", encoding="utf-8", newline="") as f:
        fields = ["profile", "rank", "deployment_id", "candidate_id", "table_name", "source_ddl_path", "source_write_plan_id", "stage9_status"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for dep in result["selected"]:
            for cid in dep["candidate_ids"]:
                ddl_relative = f"outputs/tsbs_stage4/cql/schemas/{cid}.cql"
                if not (root / ddl_relative).is_file():
                    raise FileNotFoundError(f"Missing Stage 4 generated DDL: {ddl_relative}")
                writer.writerow({"profile": dep["profile"], "rank": dep["rank"],
                                 "deployment_id": dep["deployment_id"], "candidate_id": cid,
                                 "table_name": state["specs"][cid]["table_name"],
                                 "source_ddl_path": ddl_relative,
                                 "source_write_plan_id": cid,
                                 "stage9_status": "DESIGN_ONLY_NOT_VALIDATED_OR_LOADED"})
    # Export exact source DDL references, not a single merged untested executable schema.
    manifest = {
        "stage": 8,
        "scope": policy["scope"],
        "selection_is_analytical_prebenchmark_only": True,
        "uses_only_temporal_ts0_ts8_activation": True,
        "c0_c9_rules_loaded": False,
        "activation_rules_modified": False,
        "query_count": len(state["workload"]),
        "physical_candidate_count": len(state["specs"]),
        "read_plan_count": len(state["plans"]),
        "profiles": policy["profiles"],
        "counts": result["counts"],
        "query_frequency_input": "MISSING; uniform weighting assumed BY STAGE 8 POLICY ONLY",
        "score_interpretation": "ILLUSTRATIVE_ANALYTICAL_PROXIES_NOT_MS_NOT_OBSERVED_COST",
        "top_k": policy["top_k"],
        "selected_deployments": len(result["selected"]),
        "requirement": "Every deployment must include TS0 canonical raw series for correct recomputation and Q15 lastpoint",
        "runtime_guard_policy": {
            "conservative": "No rollup alignment assumed; all other Stage 5 runtime guards remain unverified",
            "conditional_aligned": "Hour/minute rollup alignment assumed; requires verification on actual TSBS generated queries; TS0 remains fallback if guard fails",
        },
        "write_plan_status": "REFERENCE_ONLY_NO_DURABLE_CASSANDRA_CONSISTENCY",
        "external_baseline": "Original TSBS Cassandra schema is outside SchemaLens selection and must be separately benchmarked",
        "input_sha256": inputs["input_hashes"],
        "cassandra_executed": False,
        "tsbs_benchmarked": False,
        "limitations": [
            "The Pareto frontier ignores runtime and cardinality risks not represented by numeric proxies; warnings must be audited.",
            "No query runtime parameters were available. Conditional plans cannot claim exact aligned-window coverage yet.",
            "Structural covers are NOT operationally demonstrated. All-host host roster is external.",
            "Source correctness and concurrency require durable write maintenance; Stage 6 is in-memory reference only.",
            "The candidate universe of nine physical structures is NOT an unrestricted exhaustive Cassandra design space.",
            "Do not report these top-k as measured best configurations or near-best within 5% before Stage 9.",
        ],
    }
    _dump(output / "31_stage8_manifest.json", manifest)
    return manifest
