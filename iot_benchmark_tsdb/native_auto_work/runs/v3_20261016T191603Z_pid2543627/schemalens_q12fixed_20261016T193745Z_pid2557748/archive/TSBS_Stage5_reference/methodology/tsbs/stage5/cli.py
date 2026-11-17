"""CLI: Stage 5 QueryPlan generation and structural coverage audit."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from .planner import generate

ROOT = Path(__file__).resolve().parents[3]


def main() -> None:
    cli = argparse.ArgumentParser(description="TSBS Stage 5: add-only, TS0–TS8 QueryPlan generator")
    cli.add_argument("--root", type=Path, default=ROOT, help="Existing TSBS SchemaLens root")
    cli.add_argument("--output-dir", type=Path, default=None)
    args = cli.parse_args()
    root = args.root.resolve()
    result = generate(root, args.output_dir or root / "outputs/tsbs_stage5")
    print(json.dumps({"query_plans": result["query_plans"], "queries": result["queries"],
                      "candidate_count": result["candidate_count"],
                      "output_directory": str((args.output_dir or root / "outputs/tsbs_stage5").resolve()),
                      "coverage": result["coverage_class"],
                      "live_cassandra_executed": result["CQL_executed_on_server"]}, indent=2))


if __name__ == "__main__":
    main()
