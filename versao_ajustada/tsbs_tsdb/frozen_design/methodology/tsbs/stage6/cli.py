"""Read Stage 4/5 outputs and generate Stage 6 WritePlan artifacts only."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from .planner import generate

ROOT = Path(__file__).resolve().parents[3]


def main() -> None:
    ap = argparse.ArgumentParser(description="TSBS Stage 6: TSDB-only reference WritePlans, add-on")
    ap.add_argument("--root", type=Path, default=ROOT)
    ap.add_argument("--output-dir", type=Path, default=None)
    ap.add_argument("--policy", type=Path, default=None)
    args = ap.parse_args()
    root = args.root.resolve()
    out = (args.output_dir or (root / "outputs/tsbs_stage6")).resolve()
    report = generate(root, out, args.policy)
    print(json.dumps({"stage": report["stage"], "candidates": report["candidate_count"],
                      "raw_candidates": report["raw_candidates"],
                      "derived_candidates": report["derived_candidates"],
                      "query_plan_links": report["query_plan_links"],
                      "live_CQL_executed": report["live_CQL_executed"],
                      "output_directory": str(out)}, indent=2))


if __name__ == "__main__":
    main()
