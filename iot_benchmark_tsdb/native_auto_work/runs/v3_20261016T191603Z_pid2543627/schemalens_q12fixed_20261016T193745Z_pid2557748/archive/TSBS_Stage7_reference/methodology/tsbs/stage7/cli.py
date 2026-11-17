"""Command-line entrypoint for Stage 7 TSDB-only analytic diagnostics."""
from __future__ import annotations

import argparse
from pathlib import Path

from methodology.tsbs.stage7.estimator import generate


def main() -> None:
    parser = argparse.ArgumentParser(description="TSBS TSDB-only Stage 7, explanatory non-ranking physical proxies")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--policy", type=Path, default=None)
    parser.add_argument("--mozaffari-evidence", type=Path, default=None)
    args = parser.parse_args()
    root = args.root.resolve()
    target = (args.output_dir or root / "outputs/tsbs_stage7").resolve()
    result = generate(root, target, args.policy, args.mozaffari_evidence)
    print(f"Stage 7 TSDB-only: {result['candidate_count']} physical candidates, "
          f"{result['query_plan_count']} read cost-proxy records, "
          f"Mozaffari Cost_R/D computed={result['Mozaffari_Cost_R_Cost_D_computed']}, "
          f"benchmark executed={result['TSBS_benchmarked']}.")
    print(f"Results: {target}")


if __name__ == "__main__":
    main()
