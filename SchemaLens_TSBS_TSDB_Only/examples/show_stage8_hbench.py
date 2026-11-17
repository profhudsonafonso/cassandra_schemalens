"""Show prebenchmark deployable candidate sets and their structural guards.

Usage: python3 examples/show_stage8_hbench.py
"""
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
result_path = root / "outputs/tsbs_stage8/28_hbench_selected_deployments.json"
if not result_path.is_file():
    raise SystemExit("Execute primeiro: python3 -m methodology.tsbs.stage8.cli")

selected = json.loads(result_path.read_text(encoding="utf-8"))
for profile in ("conservative", "conditional_aligned"):
    print(f"\n=== HBench preselection: {profile} ===")
    for dep in selected:
        if dep["profile"] != profile:
            continue
        print(f"\nRank {dep['rank']}: {dep['deployment_id']}  ")
        print("  Tables:", ", ".join(dep["candidate_ids"]))
        print("  Workload structural cover:", dep["covered_queries_structural"], "/ 16")
        print("  Proxy score (NOT milliseconds):", dep["weighted_proxy_score"])
        print("  Assumed aligned query guards:", dep["conditional_aligned_query_ids"])
        print("  Physical risk warnings:", dep["physical_warnings"])
        print("  Query routes:")
        for qid, route in dep["query_routes"].items():
            print(f"    {qid} -> {route['candidate_id']}"
print("\nWARNING: This ranking is analytical only. No TSBS benchmark executed.")
            )