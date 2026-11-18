"""Stage 8 command line; creates only local analytical deployment plans."""
import argparse
from pathlib import Path
from .consolidator import generate


def main():
    parser = argparse.ArgumentParser(description="TSBS TSDB-only Stage 8 analytical workload consolidation (no benchmark)")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--policy", type=Path, default=None)
    args = parser.parse_args()
    root = args.root.resolve()
    output = (args.output_dir or root / "outputs/tsbs_stage8").resolve()
    report = generate(root, output, args.policy)
    print(f"Stage 8: {report['query_count']} queries, {report['physical_candidate_count']} physical candidates, {report['read_plan_count']} plans")
    for profile, count in report["counts"].items():
        print(f"  {profile}: enumerated {count['subsets_checked']} subsets, {count['canonical_complete_deployments']} canonical covers, {count['pareto_frontier_size']} Pareto non-dominated; selected {count['hbench_top_k_selected']}")
    print("Analytical preselection only. No Cassandra or TSBS benchmark executed.")
    print(f"Outputs: {output}")


if __name__ == "__main__":
    main()
