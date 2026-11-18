"""Explain Stage 7 read/write/storage proxy numbers without selecting a winner.

Run from project root: python3 examples/show_stage7_costs.py
"""
from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs/tsbs_stage7"


def load(name: str) -> list[dict]:
    path = OUT / name
    if not path.is_file():
        raise SystemExit("First run: python3 -m methodology.tsbs.stage7.cli")
    return json.loads(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    storage = {x["candidate_id"]: x for x in load("21_physical_footprint_proxies.json")}
    writes = {x["candidate_id"]: x for x in load("22_write_amplification_proxies.json")}
    reads = load("23_read_cost_proxies.json")
    signals = load("25_mozaffari_signal_audit.json")
    print("\nEXEMPLO Q13 — high-cpu-1 (cargas TS0/TS1/TS2 estimadas):")
    for x in reads:
        if x["query_id"] == "Q13":
            print(f"  {x['candidate_id']:<28} requests<= {x['estimated_cql_requests_upper_proxy']:<6} "
                  f"rows<= {x['estimated_logical_rows_returned_upper_proxy']:<8} "
                  f"guard={x['alignment_guard']}")
    print("\nEXEMPLO Q10 — AVG por hora e host (TS0/TS5/TS6):")
    for x in reads:
        if x["query_id"] == "Q10":
            print(f"  {x['candidate_id']:<28} requests<= {x['estimated_cql_requests_upper_proxy']:<6} "
                  f"rows<= {x['estimated_logical_rows_returned_upper_proxy']:<8} "
                  f"guard={x['alignment_guard']}")
    print("\nExemplos de footprint e fan-out de escrita por um evento de 10 metricas:")
    for cid in ("ts0_metric_raw", "ts2_wide_bucket_1d", "ts5_sum_count_by_host_hour", "ts6_hour_host_dimension"):
        s = storage[cid]
        w = writes[cid]
        print(f"  {cid:<28} rows_total~{s['estimated_total_rows']:>9,} "
              f"max_partition_rows~{s['estimated_max_rows_in_one_partition']:>5,} "
              f"write_group_keys/event={w['distinct_physical_group_rows_touched_by_new_observation_proxy']}")
    print("\nMozaffari FR/FU/RC/TI:")
    for row in signals:
        print(f"  {row['direction']}: {row['cost_model_status']}; Cost_R={row['cost_R']}, Cost_D={row['cost_D']}")
    print("\nAtenção: esses são proxies analíticos. Não medem latência, não ranqueiam candidatos e não substituem TSBS.")
