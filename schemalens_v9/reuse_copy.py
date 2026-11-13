"""Copy reusable V8 components into reused/ and write reused/REUSE_MANIFEST.json.

The old repository is opened read-only (shutil.copy2 reads; nothing is written there).
Re-running is idempotent: files whose copy already matches the source hash are left alone,
and a copy that was edited afterwards is reported as modified (diff kept in reused/patches/).
"""
from __future__ import annotations

import difflib
import hashlib
import json
import shutil
from pathlib import Path

from .common import OLD, V9, now_iso, write_json

DEST = V9 / "reused"

# (source relative to OLD, destination relative to reused/, role)
FILES = [
    ("methodology/protocol_012.py", "methodology/protocol_012.py", "frozen protocol, run_protocol, isolation guard (BulkWriterWatch, require_idle_node), keyspace fingerprint/clone"),
    ("methodology/benchmark_005.py", "methodology/benchmark_005.py", "DeploymentAdapter (hand-written V8 routes for 6 deployment kinds), BENCHMARK_DOMAIN, reset_state"),
    ("methodology/semantic_replay_011.py", "methodology/semantic_replay_011.py", "DeploymentAdapter008 (corrected write maintenance + reset_state), source_closure, canonical hashing"),
    ("methodology/workload_005.py", "methodology/workload_005.py", "deterministic request histories (seeded) for the 4 workloads"),
    ("methodology/engine.py", "methodology/engine.py", "dependency of workload_005/benchmark_005 (STATEMENTS/WEIGHTS tables)"),
    ("methodology/evidence_005.py", "methodology/evidence_005.py", "dependency of workload_005/benchmark_005"),
    ("methodology/baselines_005.py", "methodology/baselines_005.py", "dependency of semantic_replay_011 (ENTITIES, baseline catalogue)"),
    ("methodology/oracle_011.py", "methodology/oracle_011.py", "independent oracle (expected answers from rubis_full, no plans); identical to the V8-011 executed snapshot"),
    ("methodology/oracle_complete_011.py", "methodology/oracle_complete_011.py", "oracle completion over the 36 statements incl. mutations"),
    ("methodology/acceptance_012.py", "methodology/acceptance_012.py", "acceptance recomputed from raw data + negative (tamper) tests; global_pass_hardcoded=False"),
    ("methodology/ablation_factorial_012.py", "methodology/ablation_factorial_012.py", "FactorialAdapter (family-conditional routes); reference for V8 family costs"),
    ("methodology/cohort_baselines_012.py", "methodology/cohort_baselines_012.py", "matched-cohort baseline measurement"),
    ("methodology/full_schemalens_012.py", "methodology/full_schemalens_012.py", "streaming build from rubis_full + derivation validation + per-partition verification"),
    ("methodology/baseline_continuation_011.py", "methodology/baseline_continuation_011.py", "streaming loader of NoSE/QPG from rubis_full with checkpoint + reconciliation"),
    ("methodology/full_rebench_012.py", "methodology/full_rebench_012.py", "same-window full-scale measurement"),
    ("methodology/full_cold_012.py", "methodology/full_cold_012.py", "cold-cache full-scale measurement (drain+restart+drop per repetition)"),
    ("methodology/orchestrator_012.py", "methodology/orchestrator_012.py", "orchestrator (task graph, retries, idle waits)"),
    ("methodology/supervisor_012.py", "methodology/supervisor_012.py", "supervisor (auto-restart with backoff)"),
    ("methodology/setup_cohort_a.py", "methodology/setup_cohort_a.py", "cohort A materialisation (derivation of the matched cohort)"),
    ("inputs/historical_sources/nose_workloads_rubis.rb", "workloads/rubis.rb", "NoSE workload, commit d275634d99069bfd88a6c3ac51020138bc130dd4"),
    ("inputs/workload/rubis_workload.json", "inputs/rubis_workload.json", "V8 parsed workload (16 groups, 36 statements)"),
    ("inputs/conceptual_schema/rubis_eer.json", "inputs/rubis_eer.json", "V8 conceptual schema (7 entities, 10 relationships)"),
    ("config/rubis_workloads.json", "config/rubis_workloads.json", "workload weights"),
    ("config/rubis_dataset.json", "config/rubis_dataset.json", "dataset scales (seed 42)"),
    ("config/catalog_C0_C8.json", "config/catalog_C0_C8.json", "V8 catalogue labels (no contracts)"),
    ("config/activation_rules.json", "config/activation_rules.json", "V8 activation thresholds (not read by the code that produced H)"),
]
TREES = [("inputs/historical_sources", "inputs/historical_sources", "NoSE/QPG historical artefacts (DDL, plans, schemas) used to reconstruct the baselines")]


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _entry(src: Path, dst: Path, role: str, previous: dict | None) -> dict:
    src_sha = sha(src)
    if not dst.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    dst_sha = sha(dst)
    modified = dst_sha != src_sha
    patch = None
    if modified:
        patch = DEST / "patches" / (str(dst.relative_to(DEST)).replace("/", "__") + ".diff")
        a = src.read_text(errors="replace").splitlines(keepends=True)
        b = dst.read_text(errors="replace").splitlines(keepends=True)
        patch.write_text("".join(difflib.unified_diff(a, b, str(src), str(dst))))
    return {"source": str(src), "source_sha256": src_sha, "copy": str(dst.relative_to(V9)), "copy_sha256": dst_sha,
            "modified_after_copy": modified, "patch": str(patch.relative_to(V9)) if patch else None, "role": role,
            "copied_at": (previous or {}).get("copied_at") or now_iso()}


def run() -> dict:
    prev = {}
    mpath = DEST / "REUSE_MANIFEST.json"
    if mpath.exists():
        prev = {e["copy"]: e for e in json.loads(mpath.read_text())["files"]}
    files = []
    for s, d, role in FILES:
        dst = DEST / d
        files.append(_entry(OLD / s, dst, role, prev.get(str(dst.relative_to(V9)))))
    for s, d, role in TREES:
        for src in sorted((OLD / s).rglob("*")):
            if src.is_file() and src.name != ".DS_Store":
                dst = DEST / d / src.relative_to(OLD / s)
                files.append(_entry(src, dst, role, prev.get(str(dst.relative_to(V9)))))
    (DEST / "methodology" / "__init__.py").touch()
    out = {"old_repository": str(OLD), "old_repository_access": "read-only", "generated_at": now_iso(),
           "files": files, "count": len(files), "modified_count": sum(f["modified_after_copy"] for f in files)}
    write_json(mpath, out)
    return out


if __name__ == "__main__":
    r = run()
    print(json.dumps({"count": r["count"], "modified": r["modified_count"]}))
