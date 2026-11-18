"""Strict bounded reader of native TSBS Cassandra cpu-only data lines.

Source format: docs/cassandra.md and pkg/targets/cassandra/serializer.go.
Preserves exact original-series identifiers for the native-structure baseline.
"""
from __future__ import annotations

import gzip
import hashlib
import math
import re
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from methodology.tsbs.workload import METRICS
from methodology.tsbs.stage5.planner import NS_DAY


def load_cpu_dataset(path: Path, *, max_observations: int) -> dict:
    if not path.is_file():
        raise FileNotFoundError(path)
    if type(max_observations) is not int or max_observations < 1:
        raise ValueError("max_observations must be a positive integer")
    observed: dict[tuple[str, int], dict] = {}
    keys: dict[tuple[str, str, str], str] = {}
    host_tags: dict[str, tuple[str, ...]] = {}
    seen: dict[tuple[str, int, str], float] = {}
    line_count = 0
    digest = hashlib.sha256()
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as stream:
        for line_count, line in enumerate(stream, 1):
            digest.update(line.encode("utf-8"))
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            p = line.split(",")
            if len(p) != 16 or p[0:2] != ["series_double", "cpu"]:
                raise ValueError(f"Line {line_count}: unsupported TSBS CPU format")
            tag_items = p[2:-4]
            tags = {}
            for pair in tag_items:
                if "=" not in pair:
                    raise ValueError(f"Line {line_count}: invalid tag")
                k, v = pair.split("=", 1)
                if not k or not v or k in tags:
                    raise ValueError(f"Line {line_count}: invalid or duplicate tag")
                tags[k] = v
            host = tags.get("hostname")
            if not host:
                raise ValueError(f"Line {line_count}: missing hostname")
            old_tags = host_tags.setdefault(host, tuple(tag_items))
            if old_tags != tuple(tag_items):
                raise ValueError(f"Line {line_count}: inconsistent host tags")
            metric, day, ns_text, val_text = p[-4:]
            if metric not in METRICS or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
                raise ValueError(f"Line {line_count}: invalid field/date")
            try:
                timestamp_ns, value = int(ns_text), float(val_text)
                utc_day = (date(1970, 1, 1) + timedelta(days=timestamp_ns // NS_DAY)).isoformat()
            except (ValueError, OverflowError) as exc:
                raise ValueError(f"Line {line_count}: invalid timestamp/value") from exc
            if timestamp_ns < 0 or timestamp_ns >= 2**63 or not math.isfinite(value) or day != utc_day:
                raise ValueError(f"Line {line_count}: invalid UTC timestamp/date/value")
            ident = (host, timestamp_ns)
            if ident not in observed and len(observed) >= max_observations:
                raise ValueError(f"More than {max_observations} observations; use a smaller TSBS dataset or explicitly raise bounded policy")
            observed.setdefault(ident, {"hostname": host, "timestamp_ns": timestamp_ns})
            cell = (host, timestamp_ns, metric)
            if cell in seen and seen[cell] != value:
                raise ValueError(f"Line {line_count}: conflicting duplicate metric")
            seen[cell] = value
            observed[ident][metric] = value
            # TSBS serializer: 'cpu,' + tags joined by ',' + '#' + metric + '#' + day
            physical_key = ",".join(p[1:-4]) + "#" + metric + "#" + day
            index_key = (host, metric, day)
            if index_key in keys and keys[index_key] != physical_key:
                raise ValueError(f"Line {line_count}: conflicting series id")
            keys[index_key] = physical_key
    if not observed:
        raise ValueError("Empty dataset")
    for (host, t), row in observed.items():
        if set(row) != {"hostname", "timestamp_ns", *METRICS}:
            raise ValueError(f"Incomplete 10-metric record at {host}/{t}")
    result = sorted(observed.values(), key=lambda x: (x["timestamp_ns"], x["hostname"]))
    return {"events": result, "hostnames": sorted(host_tags), "series_ids": keys,
            "source_sha256": digest.hexdigest(), "source_lines": line_count,
            "start_ns": min(r["timestamp_ns"] for r in result),
            "last_ns": max(r["timestamp_ns"] for r in result),
            "observations": len(result), "metric_cells": len(seen)}


def write_small_fixture(path: Path, *, hosts: int = 40, hours: int = 36, interval_seconds: int = 3600) -> None:
    """Synthetic DEV fixture in exact TSBS Cassandra serialized format; not official generated data."""
    if not (1 <= hosts <= 200 and 25 <= hours <= 240 and 1 <= interval_seconds <= 3600):
        raise ValueError("Use 1..200 hosts, 25..240 hours, 1..3600 s")
    start = 1577836800 * 1_000_000_000
    rows = []
    for h in range(hosts):
        hostname = f"host_{h}"
        tags = (f"hostname={hostname},region=us-east-1,datacenter=us-east-1a,"
                f"rack=0,os=Ubuntu16.04LTS,arch=x64,team=SF,service=1,"
                f"service_version=0,service_environment=production")
        for i in range((hours * 3600) // interval_seconds):
            t = start + i * interval_seconds * 1_000_000_000
            day = (date(1970, 1, 1) + timedelta(days=t // NS_DAY)).isoformat()
            for m, metric in enumerate(METRICS):
                value = float((h * 7 + i * 13 + m * 11) % 101)
                rows.append(f"series_double,cpu,{tags},{metric},{day},{t},{value:.7f}\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        stream.writelines(rows)
