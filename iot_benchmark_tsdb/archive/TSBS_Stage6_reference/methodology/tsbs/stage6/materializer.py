"""Reference CPU-only WritePlan execution, entirely in memory.

Semantics: events are complete 10-metric observations. Upserts are deduplicated
by (hostname, timestamp_ns). A later upsert REPLACES all prior metric values.
Affected aggregates are recomputed from logical observation membership, rather
than mutating Cassandra counter columns or assuming MAX is invertible.

This is a single-process correctness oracle; NOT a durable, scalable, atomic,
or distributed Cassandra loader. Returned CQL mutations are *not executed*.
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Iterable

from methodology.tsbs.stage5.planner import NS_DAY, NS_HOUR, NS_MINUTE

EPOCH_DATE = date(1970, 1, 1)


def bucket_date(timestamp_ns: int, days: int = 1) -> str:
    """UTC epoch-anchored bucket start, aligned with Stage 5 read binder."""
    if type(days) is not int or days <= 0:
        raise ValueError("Bucket width must be a positive integer")
    return (EPOCH_DATE + timedelta(days=(timestamp_ns // NS_DAY // days) * days)).isoformat()


def _granularity(timestamp_ns: int, width: int) -> int:
    return (timestamp_ns // width) * width


def normalize_event(event: dict, metrics: list[str]) -> dict:
    """Require exact TSBS CPU observation fields; no partial or non-finite data."""
    if not isinstance(event, dict):
        raise ValueError("An observation must be a dictionary")
    op = event.get("operation", "upsert")
    host, ts = event.get("hostname"), event.get("timestamp_ns")
    if not isinstance(host, str) or not host or len(host) > 512:
        raise ValueError("Invalid TSBS hostname")
    if type(ts) is not int or ts < 0 or ts > 2**63 - 1:
        raise ValueError("timestamp_ns must be nonnegative signed bigint nanoseconds")
    if op == "delete":
        if set(event) - {"operation", "hostname", "timestamp_ns"}:
            raise ValueError("A deletion contains unexpected metric fields")
        return {"operation": "delete", "hostname": host, "timestamp_ns": ts}
    if op != "upsert":
        raise ValueError("Operation must be upsert or delete")
    expected = {"hostname", "timestamp_ns", *metrics}
    fields = set(event) - {"operation"}
    if fields != expected:
        raise ValueError(f"Full dense CPU observation required: missing={sorted(expected-fields)}, extra={sorted(fields-expected)}")
    out = {"hostname": host, "timestamp_ns": ts}
    for metric in metrics:
        v = event[metric]
        if type(v) not in {int, float} or not math.isfinite(v):
            raise ValueError(f"CPU metric {metric} must be finite numeric")
        out[metric] = float(v)
    return out


def projection_keys(spec: dict, observation: dict, metrics: list[str]) -> list[tuple]:
    """Physical primary keys touched by ONE complete logical observation."""
    cid, family = spec["candidate_id"], spec["family"]
    host, ts = observation["hostname"], observation["timestamp_ns"]
    if family == "TS0":
        return [(host, metric, ts) for metric in metrics]
    if family == "TS1":
        days = int(spec["chosen_modifiers"]["M1_bucket_days"])
        return [(host, metric, bucket_date(ts, days), ts) for metric in metrics]
    if family == "TS2":
        days = int(spec["chosen_modifiers"]["M1_bucket_days"])
        return [(host, bucket_date(ts, days), ts)]
    day = bucket_date(ts)
    if family == "TS5":
        if cid == "ts5_global_max_minute":
            return [("usage_user", day, _granularity(ts, NS_MINUTE))]
        if cid == "ts5_sum_count_by_host_hour":
            return [(host, m, day, _granularity(ts, NS_HOUR)) for m in metrics]
        if cid == "ts5_max_by_host_minute":
            return [(host, m, day, _granularity(ts, NS_MINUTE)) for m in metrics]
        if cid == "ts5_max_by_host_hour":
            return [(host, m, day, _granularity(ts, NS_HOUR)) for m in metrics]
    if family == "TS6" and cid == "ts6_hour_host_dimension":
        return [(day, _granularity(ts, NS_HOUR), host)]
    raise ValueError(f"No reference writer for candidate {cid}")


class ReferenceMaterializer:
    """Maintain canonical readings and physical candidate snapshots.

    Optimizes affected-group recomputation with membership sets. All source
    readings and their group memberships are STILL retained in RAM. Source
    records and mutations need durable journals in a real Cassandra loader.
    """

    def __init__(self, write_plans: list[dict], metrics: list[str],
                 candidate_ids: Iterable[str] | None = None):
        if len(metrics) != 10 or len(set(metrics)) != 10:
            raise ValueError("Exactly ten CPU metrics are required")
        all_plans = {p["candidate_id"]: p for p in write_plans}
        if len(all_plans) != len(write_plans):
            raise ValueError("Repeated WritePlan ID")
        selected = set(all_plans) if candidate_ids is None else set(candidate_ids)
        if not selected or not selected <= set(all_plans):
            raise ValueError("Select valid, nonempty Stage 4 candidates")
        self.plans = {cid: all_plans[cid] for cid in sorted(selected)}
        self.metrics = metrics[:]
        self.events: dict[tuple[str, int], dict] = {}
        self.rows: dict[str, dict[tuple, dict]] = {cid: {} for cid in selected}
        self._members: dict[str, dict[tuple, set[tuple[str, int]]]] = {
            cid: defaultdict(set) for cid in selected
            if all_plans[cid]["write_strategy"] == "derived_recompute"
        }

    def _render_row(self, cid: str, key: tuple) -> dict | None:
        plan = self.plans[cid]
        fam = plan["family"]
        if fam in {"TS0", "TS1"}:
            host, metric, ts = key[0], key[1], key[-1]
            record = self.events.get((host, ts))
            if record is None:
                return None
            row = {"hostname": host, "metric_name": metric, "timestamp_ns": ts, "value": record[metric]}
            if fam == "TS1":
                row["bucket_day"] = key[2]
            return row
        if fam == "TS2":
            host, day, ts = key
            record = self.events.get((host, ts))
            return (dict(hostname=host, bucket_day=day, timestamp_ns=ts,
                         **{m: record[m] for m in self.metrics}) if record else None)
        member_ids = self._members[cid].get(key, set())
        if not member_ids:
            return None
        events = [self.events[identity] for identity in sorted(member_ids)]
        if fam == "TS5":
            if cid == "ts5_global_max_minute":
                metric, day, minute_ns = key
                return {"metric_name": metric, "bucket_day": day, "minute_ns": minute_ns,
                        "max_value": max(e[metric] for e in events)}
            host, metric, day, start = key
            base = {"hostname": host, "metric_name": metric, "bucket_day": day}
            if cid == "ts5_sum_count_by_host_hour":
                return {**base, "hour_ns": start,
                        "value_sum": math.fsum(e[metric] for e in events),
                        "sample_count": len(events)}
            timecol = "minute_ns" if cid == "ts5_max_by_host_minute" else "hour_ns"
            return {**base, timecol: start, "max_value": max(e[metric] for e in events)}
        if fam == "TS6":
            day, hour_ns, host = key
            output = {"bucket_day": day, "hour_ns": hour_ns, "hostname": host}
            for m in self.metrics:
                output[f"sum_{m}"] = math.fsum(e[m] for e in events)
                output[f"count_{m}"] = len(events)
            return output
        raise ValueError(f"Unknown renderer for {cid}")

    def apply(self, source_events: Iterable[dict]) -> list[dict]:
        """Apply events in input order, return changed CQL INSERT/DELETE actions.

        Replays that do not change a full observation generate zero actions.
        Multiple updates of the same identity in one batch collapse to its last
        value before physical snapshots are updated. No CQL is executed.
        """
        final: dict[tuple[str, int], dict | None] = {}
        for incoming in source_events:
            normalized = normalize_event(incoming, self.metrics)
            identity = (normalized["hostname"], normalized["timestamp_ns"])
            final[identity] = None if normalized.get("operation") == "delete" else normalized
        touched: dict[str, set[tuple]] = {cid: set() for cid in self.plans}
        changes: list[tuple[tuple[str, int], dict | None, dict | None]] = []
        for identity, after in final.items():
            before = self.events.get(identity)
            if before == after:
                continue
            changes.append((identity, before, after))
            for cid, plan in self.plans.items():
                if before is not None:
                    touched[cid].update(projection_keys(plan, before, self.metrics))
                if after is not None:
                    touched[cid].update(projection_keys(plan, after, self.metrics))
        # Group indexes first, then canonical values, then affected-row refresh.
        for identity, before, after in changes:
            for cid, group_index in self._members.items():
                if before is not None:
                    for key in projection_keys(self.plans[cid], before, self.metrics):
                        identities = group_index[key]
                        identities.discard(identity)
                        if not identities:
                            del group_index[key]
                if after is not None:
                    for key in projection_keys(self.plans[cid], after, self.metrics):
                        group_index[key].add(identity)
            if after is None:
                self.events.pop(identity, None)
            else:
                self.events[identity] = after
        mutations: list[dict] = []
        for cid, keys in sorted(touched.items()):
            plan = self.plans[cid]
            for key in sorted(keys):
                old = self.rows[cid].get(key)
                new = self._render_row(cid, key)
                if old == new:
                    continue
                if new is None:
                    assert old is not None
                    del self.rows[cid][key]
                    operation, cql = "DELETE", plan["delete_cql"]
                    params = list(key)
                else:
                    self.rows[cid][key] = new
                    operation, cql = "UPSERT", plan["write_cql"]
                    params = [new[name] for name in plan["column_order"]]
                if len(params) != cql.count("?"):
                    raise AssertionError("Invalid generated bind count")
                mutations.append({"candidate_id": cid, "family": plan["family"],
                                  "table_name": plan["table_name"], "operation": operation,
                                  "primary_key": list(key), "cql": cql, "parameters": params,
                                  "row": new, "execution_status": "NOT_EXECUTED_REFERENCE_ONLY"})
        return mutations

    def host_roster(self) -> list[str]:
        """Correct for THIS in-memory canonical snapshot only (not persisted in Cassandra)."""
        return sorted({host for host, _ in self.events})

    def query_mock_rows(self, request: dict) -> list[dict]:
        """Return synthetic rows from a physical table for Stage 5's fetcher.

        Implements ONLY the constrained Stage 5 CQL primitive patterns. This is
        NOT a Cassandra driver, timing tool, or query optimization claim.
        """
        cid = request["candidate_id"]
        plan = self.plans[cid]
        pk = plan["partition_key"]
        params = request["parameters"]
        if len(params) < len(pk):
            raise ValueError("Not enough partition bindings")
        matches = [r.copy() for r in self.rows[cid].values()
                   if all(r[k] == v for k, v in zip(pk, params))]
        rest = params[len(pk):]
        if rest:
            key_name = plan["clustering_key"][0]
            if len(rest) == 2:
                lo, hi = rest
                matches = [r for r in matches if lo <= r[key_name] < hi]
            elif len(rest) == 1:
                hi = rest[0]
                matches = [r for r in matches if r[key_name] < hi]
            else:
                raise ValueError("Unsupported mock read predicate")
        # CQL is ordered by clustering key; the limit must apply per partition.
        ck = plan["clustering_key"]
        descending = cid in {"ts0_metric_raw", "ts5_global_max_minute"}
        matches.sort(key=lambda r: tuple(r[k] for k in ck), reverse=descending)
        if "LIMIT 1" in request["cql"]:
            matches = matches[:1]
        elif "LIMIT 5" in request["cql"]:
            matches = matches[:5]
        return matches
