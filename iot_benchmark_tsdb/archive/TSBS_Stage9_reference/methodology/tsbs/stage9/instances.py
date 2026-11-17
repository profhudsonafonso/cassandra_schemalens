"""Pre-generate one immutable logical workload before touching the database.

Uses the *semantics and parameters* of official TSBS templates. It is not a
byte-for-byte replay of Go-generated query streams: Python PRNG differs.
"""
from __future__ import annotations

import random
from methodology.tsbs.workload import TEMPLATES
from methodology.tsbs.stage5.planner import NS_HOUR, NS_MINUTE


def generate_instances(dataset: dict, *, interval_seconds: int, per_template: int, seed: int,
                       alignment_cohort: str = "natural") -> list[dict]:
    if alignment_cohort not in {"natural", "aligned"}:
        raise ValueError("alignment_cohort must be natural or aligned")
    if min(interval_seconds, per_template) <= 0:
        raise ValueError("interval_seconds and per_template must be positive")
    hosts = dataset["hostnames"]
    start, end = dataset["start_ns"], dataset["last_ns"] + interval_seconds * 1_000_000_000
    rng = random.Random(seed)
    result = []
    for t in TEMPLATES:
        if t.host_count > len(hosts):
            raise ValueError(f"{t.query_type} requires {t.host_count} hosts; only {len(hosts)} available")
        win = (t.window_hours or 0) * NS_HOUR
        if win > end - start:
            raise ValueError(f"{t.query_type} requires {win/NS_HOUR}h of temporal coverage")
        for replicate in range(per_template):
            selected = sorted(rng.sample(hosts, t.host_count)) if t.host_count else hosts[:]
            kwargs = {"hostnames": selected, "complete_host_roster": not bool(t.host_count),
                      "dense_cpu_metrics": True}
            if t.kind == "lastpoint":
                pass
            elif t.kind == "toprollup":
                # TSBS random end (not time range); allow arbitrary minute alignment.
                step = NS_MINUTE if alignment_cohort == "aligned" else 1_000_000_000
                delta = rng.randrange(1, (end-start)//step + 1)
                kwargs.update(end_ns=start + delta * step, dataset_start_ns=start)
            else:
                # 'natural': random seconds (TSBS-like arbitrary boundaries).
                # 'aligned': an explicit CONTROLLED secondary cohort for TS5/TS6.
                # Both use identical instances across deployments; never rewrite
                # alignment *after* seeing benchmark measurements.
                step = (NS_MINUTE if t.kind == "single" else NS_HOUR) if alignment_cohort == "aligned" else 1_000_000_000
                offset = rng.randrange((end - start - win)//step + 1)
                kwargs.update(start_ns=start + offset*step, end_ns=start+offset*step+win)
            result.append({"instance_id": f"{t.query_id}_R{replicate:03d}",
                           "query_id": t.query_id, "query_name": t.query_type,
                           "kind": t.kind, "params": kwargs})
    return result


def route_instance(deployment: dict, instance: dict, plans: dict[str, dict]) -> tuple[dict, str]:
    """Allow a rollup only when its actual runtime guard passes; otherwise TS0 fallback."""
    from methodology.tsbs.stage5.runtime import bind_read_requests
    qid = instance["query_id"]
    route = deployment["query_routes"][qid]
    selected = plans[route["selected_plan_id"]]
    try:
        bind_read_requests(selected, **instance["params"])
        return selected, "selected"
    except ValueError as exc:
        # Fail closed for structural/runtime errors: ONLY explicit alignment
        # limitations permit a fallback, not e.g. a missing host roster.
        if "aligned windows" not in str(exc):
            raise
        fallback = plans.get(f"{qid}__ts0_metric_raw")
        if not fallback or "ts0_metric_raw" not in deployment["candidate_ids"]:
            raise ValueError(f"No verified TS0 fallback for {qid}") from exc
        bind_read_requests(fallback, **instance["params"])
        return fallback, "fallback_due_to_rollup_alignment"
