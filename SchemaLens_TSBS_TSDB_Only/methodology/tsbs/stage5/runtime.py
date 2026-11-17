"""Bind Stage 5 read plans to concrete CQL requests; evaluate mocked read rows.

The query binder is usable by a FUTURE Cassandra driver adapter. This module
DOES NOT connect to Cassandra, load data, create aggregates, or perform writes.
The evaluator supports deterministic synthetic validation of query semantics.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Callable

from .planner import NS_DAY, NS_HOUR, NS_MINUTE

EPOCH = date(1970, 1, 1)


def date_from_days(index: int) -> str:
    return (EPOCH + timedelta(days=index)).isoformat()


def _partition_day(timestamp_ns: int, width: int) -> str:
    # Explicit contract to be shared with the later Stage 6 writer:
    # UTC epoch anchored; 7d partitions start each multiple of seven epoch days.
    index = timestamp_ns // NS_DAY
    return date_from_days((index // width) * width)


def _window_buckets(start_ns: int, end_ns: int, width_ns: int) -> list[int]:
    if end_ns <= start_ns:
        raise ValueError("Query requires a nonempty half-open time interval")
    return list(range(start_ns // width_ns, (end_ns - 1) // width_ns + 1))


def _host_list(plan: dict, hostnames: list[str], complete_host_roster: bool) -> list[str]:
    if not isinstance(hostnames, list) or not hostnames or len(set(hostnames)) != len(hostnames):
        raise ValueError("Provide unique nonempty hostnames from the actual loaded dataset")
    if any(not isinstance(h, str) or not h for h in hostnames):
        raise ValueError("Invalid hostname")
    expected = plan["num_hosts"]
    if expected == "ALL":
        if not complete_host_roster:
            raise ValueError("All-host query requires complete host roster of the loaded dataset")
    elif len(hostnames) != int(expected):
        raise ValueError(f"Expected {expected} selected hosts, got {len(hostnames)}")
    return hostnames


def bind_read_requests(plan: dict, *, hostnames: list[str], start_ns: int | None = None,
                       end_ns: int | None = None, dataset_start_ns: int | None = None,
                       complete_host_roster: bool = False, dense_cpu_metrics: bool = False) -> list[dict]:
    """Expand one physical plan into CQL parameter bindings.

    All-host discovery is an explicit INPUT, not a pretend CQL operation.
    Derived rollups reject partial minute/hour boundaries to avoid false
    coverage. One request per metric/host/bucket or per dimension/hour.
    """
    hosts = _host_list(plan, hostnames, complete_host_roster)
    kind, strategy, family = plan["query_kind"], plan["physical_strategy"], plan["family"]
    required = plan["requirements"]
    if "cpu_metric_timestamps_are_dense" in required and not dense_cpu_metrics:
        raise ValueError("Scalar multi-metric reads require TSBS dense CPU observations")
    if "cpu_metrics_same_latest_timestamp" in required and not dense_cpu_metrics:
        raise ValueError("TS0 latest-point reconstruction requires equal latest metric timestamps")
    if kind != "lastpoint":
        if not isinstance(end_ns, int) or isinstance(end_ns, bool):
            raise ValueError("end_ns must be an integer UTC epoch nanoseconds")
        if kind != "toprollup":
            if not isinstance(start_ns, int) or isinstance(start_ns, bool) or start_ns >= end_ns:
                raise ValueError("A nonempty start_ns/end_ns interval is required")
    guard = plan["alignment_guard"]
    if guard:
        width = NS_MINUTE if guard == "minute" else NS_HOUR
        if end_ns % width or (kind != "toprollup" and start_ns % width):
            raise ValueError(f"{plan['candidate_id']} is exact only on {guard}-aligned windows")
    if kind == "toprollup" and strategy == "derived_rollup":
        if not isinstance(dataset_start_ns, int) or isinstance(dataset_start_ns, bool) or dataset_start_ns >= end_ns:
            raise ValueError("A finite dataset_start_ns is required for historical daily rollup traversal")

    requests: list[dict] = []
    cql = plan["read_cql"]

    def emit(params: tuple, host: str | None = None, metric: str | None = None, bucket: str | None = None) -> None:
        if cql.count("?") != len(params):
            raise ValueError("CQL placeholders and bind parameters disagree")
        requests.append({"query_plan_id": plan["plan_id"], "candidate_id": plan["candidate_id"],
                         "cql": cql, "parameters": list(params), "hostname": host,
                         "metric_name": metric, "bucket_day": bucket})

    metrics = plan["query_metrics"]
    if strategy == "raw_scalar" and kind == "lastpoint":
        for h in hosts:
            for m in metrics:
                emit((h, m), h, m)
    elif strategy == "raw_scalar":
        for h in hosts:
            for m in metrics:
                if kind == "toprollup":
                    emit((h, m, end_ns), h, m)
                else:
                    emit((h, m, start_ns, end_ns), h, m)
    elif strategy == "raw_scalar_bucketed":
        width = int(plan["bucket_days"])
        for h in hosts:
            for m in metrics:
                for bucket in _window_buckets(start_ns, end_ns, width * NS_DAY):
                    day = date_from_days(bucket * width)
                    emit((h, m, day, start_ns, end_ns), h, m, day)
    elif strategy == "raw_wide_bucketed":
        width = int(plan["bucket_days"])
        for h in hosts:
            for bucket in _window_buckets(start_ns, end_ns, width * NS_DAY):
                day = date_from_days(bucket * width)
                emit((h, day, start_ns, end_ns), h, None, day)
    elif strategy == "derived_rollup" and kind == "toprollup":
        # Date DESC and LIMIT 5 per day: five most recent global minutes can be
        # recovered after merging/sorting all days. Empty days are allowed.
        first_day = dataset_start_ns // NS_DAY
        last_day = (end_ns - 1) // NS_DAY
        for day_idx in range(last_day, first_day - 1, -1):
            day = date_from_days(day_idx)
            emit((metrics[0], day, end_ns), None, metrics[0], day)
    elif strategy == "derived_rollup":
        for h in hosts:
            for m in metrics:
                for day_idx in _window_buckets(start_ns, end_ns, NS_DAY):
                    day = date_from_days(day_idx)
                    emit((h, m, day, start_ns, end_ns), h, m, day)
    elif strategy == "dimension_rollup":
        for hour in _window_buckets(start_ns, end_ns, NS_HOUR):
            hour_ns = hour * NS_HOUR
            day = _partition_day(hour_ns, 1)
            emit((day, hour_ns), None, None, day)
    else:
        raise ValueError(f"No binding strategy for {strategy}/{kind}/{family}")
    return requests


def execute_mock_rows(plan: dict, batches: list[tuple[dict, list[dict]]]) -> list[dict]:
    """Pure post-processing: consume correctly labeled mock CQL response rows.

    This verifies the declared client operators against synthetic records. It
    does NOT claim Cassandra query execution; tests must supply batches whose
    rows reflect the physical candidate representation.
    """
    kind, strategy = plan["query_kind"], plan["physical_strategy"]
    metrics = plan["query_metrics"]
    # Accumulate metric values by (hostname, timestamp_ns), and derived
    # aggregations by appropriate UTC bucket. Multiple host streams may be
    # present in the same output time bucket.
    raw: dict[tuple[str, int], dict[str, float]] = {}
    derived: list[dict] = []
    for request, rows in batches:
        for row in rows:
            if strategy in ("raw_scalar", "raw_scalar_bucketed"):
                key = (request["hostname"], int(row["timestamp_ns"]))
                payload = raw.setdefault(key, {})
                metric = request["metric_name"]
                if metric in payload and payload[metric] != row["value"]:
                    raise ValueError("Conflicting duplicate metric reading")
                payload[metric] = float(row["value"])
            elif strategy == "raw_wide_bucketed":
                key = (request["hostname"], int(row["timestamp_ns"]))
                payload = raw.setdefault(key, {})
                for metric in metrics:
                    if row.get(metric) is not None:
                        payload[metric] = float(row[metric])
            else:
                derived.append({**row, "_hostname": request["hostname"], "_metric": request["metric_name"],
                                "_hour_ns": request["parameters"][1] if strategy == "dimension_rollup" else None})

    # Re-express raw inputs as logical per-observation rows.
    if strategy.startswith("raw_"):
        if kind in {"high", "lastpoint"}:
            for (hostname, t), vals in raw.items():
                if set(vals) != set(metrics):
                    raise ValueError(f"Incomplete dense TSBS metric observation at {hostname}/{t}")
        observations = [dict(hostname=h, timestamp_ns=t, **vals) for (h, t), vals in raw.items()]
    else:
        observations = []

    if kind == "high":
        return sorted((r for r in observations if r["usage_user"] > 90.0),
                      key=lambda r: (r["timestamp_ns"], r["hostname"]))
    if kind == "lastpoint":
        latest: dict[str, dict] = {}
        for r in observations:
            if r["hostname"] not in latest or r["timestamp_ns"] > latest[r["hostname"]]["timestamp_ns"]:
                latest[r["hostname"]] = r
        return [latest[h] for h in sorted(latest)]

    resolution = NS_MINUTE if kind in {"single", "toprollup"} else NS_HOUR
    summary: dict[tuple, dict[str, Any]] = {}

    def key_for(h: str | None, ts: int) -> tuple:
        time_bucket = (ts // resolution) * resolution
        return (time_bucket, h) if kind == "double" else (time_bucket,)

    if observations:
        for r in observations:
            key = key_for(r["hostname"], int(r["timestamp_ns"]))
            dst = summary.setdefault(key, {})
            for metric in metrics:
                value = r.get(metric)
                if value is None:
                    continue
                if kind == "double":
                    dst[f"sum_{metric}"] = dst.get(f"sum_{metric}", 0) + value
                    dst[f"count_{metric}"] = dst.get(f"count_{metric}", 0) + 1
                else:
                    dst[metric] = max(dst.get(metric, float("-inf")), value)
    elif strategy in {"derived_rollup", "dimension_rollup"}:
        for r in derived:
            if strategy == "dimension_rollup":
                h, ts = r["hostname"], int(r.get("hour_ns", r["_hour_ns"]))
                key = key_for(h, ts)
                dst = summary.setdefault(key, {})
                for metric in metrics:
                    dst[f"sum_{metric}"] = dst.get(f"sum_{metric}", 0.0) + float(r[f"sum_{metric}"])
                    dst[f"count_{metric}"] = dst.get(f"count_{metric}", 0) + int(r[f"count_{metric}"])
            elif kind == "double":
                h, ts = r["_hostname"], int(r["hour_ns"])
                key = key_for(h, ts)
                dst = summary.setdefault(key, {})
                metric = r["_metric"]
                dst[f"sum_{metric}"] = dst.get(f"sum_{metric}", 0.0) + float(r["value_sum"])
                dst[f"count_{metric}"] = dst.get(f"count_{metric}", 0) + int(r["sample_count"])
            else:
                kfield = "hour_ns" if kind == "maxall" else "minute_ns"
                ts = int(r[kfield])
                key = key_for(None, ts)
                dst = summary.setdefault(key, {})
                metric = r["_metric"]
                dst[metric] = max(dst.get(metric, float("-inf")), float(r["max_value"]))

    results: list[dict] = []
    for key, values in summary.items():
        bucket = key[0]
        record: dict[str, Any] = {"bucket_ns": bucket}
        if kind == "double":
            record["hostname"] = key[1]
            for metric in metrics:
                n = values.get(f"count_{metric}", 0)
                record[f"avg_{metric}"] = values[f"sum_{metric}"] / n if n else None
        else:
            for metric in metrics:
                if metric in values:
                    record[f"max_{metric}"] = values[metric]
        results.append(record)
    reverse = kind == "toprollup"
    results.sort(key=lambda r: (r["bucket_ns"], r.get("hostname", "")), reverse=reverse)
    return results[:5] if reverse else results


def evaluate_with_fetcher(plan: dict, fetch: Callable[[dict], list[dict]], **params: Any) -> list[dict]:
    """Logical runner using caller-provided reads; tests use a synthetic fetcher.

    A genuine driver can implement fetch(request) in Stage 6+, after the
    physical tables are created and populated with verified writes.
    """
    requests = bind_read_requests(plan, **params)
    return execute_mock_rows(plan, [(req, fetch(req)) for req in requests])
