"""Demonstrate the Stage 6 reference writer on 2 hosts and late corrections.

Run from repository root: python3 examples/show_stage6_writeplans.py
No Cassandra cluster/driver or non-stdlib dependencies are needed.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from methodology.tsbs.stage5.planner import NS_HOUR, NS_MINUTE
from methodology.tsbs.stage5.runtime import evaluate_with_fetcher
from methodology.tsbs.stage6.materializer import ReferenceMaterializer

WRITE_PLANS = ROOT / "outputs/tsbs_stage6/18_write_plans.json"
READ_PLANS = ROOT / "outputs/tsbs_stage5/15_query_plans.json"
SCHEMA = ROOT / "inputs/schema/tsbs_cpu_only_schema.json"


def point(metrics: list[str], host: str, timestamp_ns: int, cpu: float) -> dict:
    values = {name: float(i + 1) for i, name in enumerate(metrics)}
    values["usage_user"] = cpu
    return {"hostname": host, "timestamp_ns": timestamp_ns, **values}


def main() -> None:
    for path in [WRITE_PLANS, READ_PLANS, SCHEMA]:
        if not path.is_file():
            raise SystemExit(f"Run Stages 4, 5 and 6 first. Missing {path}")
    specs = json.loads(WRITE_PLANS.read_text())
    plans = json.loads(READ_PLANS.read_text())
    schema = json.loads(SCHEMA.read_text())
    metrics = schema["entities"]["CPUReading"]["temporal"]["metrics"]
    writer = ReferenceMaterializer(specs, metrics)
    observations = [point(metrics, "host_0", 0, 25),
                    point(metrics, "host_0", 10_000_000_000, 82),
                    point(metrics, "host_0", NS_MINUTE, 45),
                    point(metrics, "host_1", 0, 15),
                    point(metrics, "host_1", NS_MINUTE, 95)]
    created = writer.apply(observations)
    print(f"Logical observations: {len(writer.events)} | generated CQL mutations: {len(created)}")
    print("\nExample CQL mutation for TS2 (wide row):")
    sample = next(x for x in created if x["candidate_id"] == "ts2_wide_bucket_1d")
    print(sample["cql"])
    print("Bind values:", sample["parameters"][:4], "...ten CPU metrics")
    print("\nExample precomputed MAX per minute (host_0, usage_user):")
    for row in writer.rows["ts5_max_by_host_minute"].values():
        if row["hostname"] == "host_0" and row["metric_name"] == "usage_user":
            print(row)
    print("\nCorrection: older usage_user=82 is replaced by 34 (MAX can decrease):")
    fix = writer.apply([point(metrics, "host_0", 10_000_000_000, 34)])
    for change in fix:
        if (change["candidate_id"] == "ts5_max_by_host_minute"
                and change["row"] and change["row"].get("metric_name") == "usage_user"):
            print(change["operation"], change["row"])
    print("\nReference read equivalence for Q01: TS0 vs TS5")
    result = {}
    for candidate in ["ts0_metric_raw", "ts5_max_by_host_minute"]:
        p = next(p for p in plans if p["query_id"] == "Q01" and p["candidate_id"] == candidate)
        result[candidate] = evaluate_with_fetcher(p, writer.query_mock_rows,
                                                  hostnames=["host_0"], start_ns=0,
                                                  end_ns=2 * NS_MINUTE)
        print(candidate, result[candidate])
    assert result["ts0_metric_raw"] == result["ts5_max_by_host_minute"]
    print("\nPASS: reference QueryPlan results are equivalent. No Cassandra server was used.")


if __name__ == "__main__":
    main()
