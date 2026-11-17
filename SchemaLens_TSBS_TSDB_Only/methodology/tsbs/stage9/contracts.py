"""Immutable Stage 8 selection, safety and reproducible manifest handling."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

INPUTS = {
    "deployments": "outputs/tsbs_stage8/28_hbench_selected_deployments.json",
    "stage8_manifest": "outputs/tsbs_stage8/31_stage8_manifest.json",
    "specs": "outputs/tsbs_stage4/10_cassandra_candidate_specs.json",
    "plans": "outputs/tsbs_stage5/15_query_plans.json",
    "writes": "outputs/tsbs_stage6/18_write_plans.json",
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def read_inputs(root: Path, policy: Path | None = None) -> dict:
    root = root.resolve()
    policy_file = policy or root / "config/tsbs_stage9_benchmark_policy.json"
    if not policy_file.is_file():
        raise FileNotFoundError(policy_file)
    p = json.loads(policy_file.read_text(encoding="utf-8"))
    if p.get("scope") != "TSDB_ONLY_POST_SELECTION_CASSANDRA_BENCHMARK" or p.get("benchmark_results_may_affect_stage8_selection") is not False:
        raise ValueError("Stage 9 cannot change activation, candidate selection, or HBench ranking")
    for attr in ("max_observations_in_memory", "queries_per_template", "measured_repetitions", "sample_interval_seconds"):
        if type(p.get(attr)) is not int or p[attr] <= 0:
            raise ValueError(f"{attr} must be positive")
    if type(p.get("warmup_repetitions")) is not int or p["warmup_repetitions"] < 0:
        raise ValueError("warmup_repetitions must be nonnegative")
    if p.get("worker_count") != 1 or p.get("allow_approximate_answers") is not False:
        raise ValueError("Stage 9 requires exact sequential evaluation")
    paths = {k: root / v for k, v in INPUTS.items()}
    for name, path in paths.items():
        if not path.is_file():
            raise FileNotFoundError(f"Run Stages 3-8 first: {path}")
    obj = {k: json.loads(path.read_text(encoding="utf-8")) for k, path in paths.items()}
    manifest = obj["stage8_manifest"]
    if manifest.get("stage") != 8 or manifest.get("c0_c9_rules_loaded") is not False or manifest.get("selection_is_analytical_prebenchmark_only") is not True:
        raise ValueError("Invalid TSDB-only Stage 8 provenance")
    # Cross-check the candidate/plan/write hashes locked in Stage 8.
    for key in ("specs", "plans", "writes"):
        declared_name = {"specs": "specs", "plans": "read_plans", "writes": "write_plans"}[key]
        if manifest["input_sha256"][declared_name] != sha(paths[key]):
            raise ValueError(f"Stage 8 locked upstream SHA changed: {key}")
    objs = {key: {item[keyfield]: item for item in obj[key]} for key, keyfield in
            (("specs", "candidate_id"), ("plans", "plan_id"), ("writes", "candidate_id"))}
    if any(len(objs[k]) != len(obj[k]) for k in objs):
        raise ValueError("Duplicate candidate, plan, or write IDs")
    for dep in obj["deployments"]:
        if any(cid not in objs["specs"] or cid not in objs["writes"] for cid in dep["candidate_ids"]):
            raise ValueError("Deployment references unrecognized candidates")
        if set(dep["query_routes"]) != {f"Q{i:02d}" for i in range(1, 17)}:
            raise ValueError("Deployment does not cover 16 templates")
        if "ts0_metric_raw" not in dep["candidate_ids"]:
            raise ValueError("Stage 9 requires retained canonical TS0")
        for qid, route in dep["query_routes"].items():
            plan = objs["plans"].get(route["selected_plan_id"])
            if not plan or plan["query_id"] != qid or plan["candidate_id"] not in dep["candidate_ids"]:
                raise ValueError(f"Invalid deployed query route: {qid}")
    return {"policy": p, "deployments": obj["deployments"], **objs,
            "sha256": {k: sha(v) for k, v in paths.items()} | {"policy": sha(policy_file)}}


def keyspace_for(deployment_id: str) -> str:
    if not re.fullmatch(r"H_[a-f0-9]{12}", deployment_id):
        raise ValueError("Unsafe deployment identifier")
    return "sl_tsbs_" + deployment_id.lower().replace("h_", "h")


def require_reset_confirmation(keyspace: str, confirmation: str) -> None:
    if not re.fullmatch(r"sl_tsbs_h[a-f0-9]{12}", keyspace) or confirmation != keyspace:
        raise PermissionError(f"Explicit --confirm-reset-keyspace {keyspace} is required")
