"""CLI for the additive TSBS Stage 4 Cassandra schema candidate generator."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from methodology.tsbs.stage4.candidate_generator import instantiate

ROOT = Path(__file__).resolve().parents[3]


def main() -> None:
    ap = argparse.ArgumentParser(description="Instantiate TS0-TS8 Cassandra physical candidates from Stage 3 outputs")
    ap.add_argument("--root", type=Path, default=ROOT, help="Existing SchemaLens_TSBS_TSDB_Only repository root")
    ap.add_argument("--output-dir", type=Path, default=None, help="Where generated Stage 4 files will be stored")
    ap.add_argument("--physical-policy", type=Path, default=None, help="Explicit physical-candidate policy JSON")
    args = ap.parse_args()
    root = args.root.resolve()
    target = args.output_dir or root / "outputs" / "tsbs_stage4"
    result = instantiate(root, target, args.physical_policy)
    print(json.dumps({"stage": result["stage"], "candidates": result["candidate_count"],
                      "queries": result["query_count"], "counts_by_family": result["candidate_counts_by_family"],
                      "output_directory": str(target.resolve())}, indent=2))


if __name__ == "__main__":
    main()
