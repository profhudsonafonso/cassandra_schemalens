"""Display bound query examples WITHOUT connecting to Cassandra.

Run from the existing project root:
    python3 examples/show_stage5_queryplans.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from methodology.tsbs.stage5.runtime import bind_read_requests

ROOT = Path(__file__).resolve().parents[1]
PLANS = json.loads((ROOT / "outputs/tsbs_stage5/15_query_plans.json").read_text(encoding="utf-8"))


def ns(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)


ALL_HOSTS = [f"host_{i}" for i in range(100)]
EXAMPLES = [
    ("Q13", "ts2_wide_bucket_1d", dict(hostnames=["host_1"], start_ns=ns("2026-10-15T00:00:00"),
                                           end_ns=ns("2026-10-15T01:00:00"), dense_cpu_metrics=True)),
    ("Q10", "ts6_hour_host_dimension", dict(hostnames=ALL_HOSTS, start_ns=ns("2026-10-15T00:00:00"),
                                                 end_ns=ns("2026-10-15T12:00:00"), complete_host_roster=True)),
    ("Q15", "ts0_metric_raw", dict(hostnames=ALL_HOSTS, complete_host_roster=True, dense_cpu_metrics=True)),
    ("Q16", "ts5_global_max_minute", dict(hostnames=ALL_HOSTS, dataset_start_ns=ns("2026-10-12T00:00:00"),
                                               end_ns=ns("2026-10-15T00:00:00"), complete_host_roster=True)),
]

for qid, cid, runtime in EXAMPLES:
    plan = next(x for x in PLANS if x["query_id"] == qid and x["candidate_id"] == cid)
    requests = bind_read_requests(plan, **runtime)
    print(f"\n{qid} / {cid}: {len(requests)} parametrized CQL read requests")
    if requests:
        print("CQL:", requests[0]["cql"])
        print("Bindings:", requests[0]["parameters"])

print("\nNo database was contacted; no data was read or timed.")
