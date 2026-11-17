"""Optional CPU-only parser for TSBS Cassandra-format generated metric rows.

Format documented in TSBS docs/cassandra.md:
series_double,cpu,hostname=host_0,region=...,other tags...,usage_user,YYYY-MM-DD,timestamp_ns,value

Each single metric row becomes part of one dense logical observation. This
adapter is deliberately a bounded offline/reference parser, NOT a scalable
streaming or distributed TSBS bulk loader. Tags are checked, not persisted.
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from methodology.tsbs.stage5.planner import NS_DAY


def parse_cassandra_cpu_lines(lines: Iterable[str], metrics: list[str]) -> list[dict]:
    if len(metrics) != 10 or len(set(metrics)) != 10:
        raise ValueError("Expected complete ten-metric TSBS CPU-only schema")
    permitted = set(metrics)
    grouped: dict[tuple[str, int], dict[str, float]] = defaultdict(dict)
    host_tags: dict[str, tuple[tuple[str, str], ...]] = {}
    for number, source in enumerate(lines, 1):
        line = source.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split(",")
        if len(fields) < 7 or fields[0] != "series_double" or fields[1] != "cpu":
            raise ValueError(f"Invalid TSBS Cassandra CPU line at {number}")
        tokens = fields[2:-4]
        tags = {}
        for token in tokens:
            if "=" not in token:
                raise ValueError(f"Malformed TSBS tag at {number}")
            key, value = token.split("=", 1)
            if not key or not value or key in tags:
                raise ValueError(f"Empty/duplicated tag at {number}")
            tags[key] = value
        hostname = tags.get("hostname")
        if not hostname:
            raise ValueError(f"Missing hostname at {number}")
        fingerprint = tuple(sorted(tags.items()))
        if hostname in host_tags and host_tags[hostname] != fingerprint:
            raise ValueError(f"Inconsistent static tags for {hostname}")
        host_tags[hostname] = fingerprint
        metric, bucket_day, nanos, raw_value = fields[-4:]
        if metric not in permitted:
            raise ValueError(f"Unsupported CPU metric {metric!r} at {number}")
        try:
            ns = int(nanos)
            val = float(raw_value)
            represented = (date(1970, 1, 1) + timedelta(days=ns // NS_DAY)).isoformat()
        except (ValueError, OverflowError) as exc:
            raise ValueError(f"Invalid timestamp/value at line {number}") from exc
        if ns < 0 or ns > 2**63 - 1 or not math.isfinite(val) or represented != bucket_day:
            raise ValueError(f"Invalid UTC date or metric value at line {number}")
        observation = grouped[(hostname, ns)]
        if metric in observation and observation[metric] != val:
            raise ValueError(f"Conflicting duplicate metric {metric} for {hostname}/{ns}")
        observation[metric] = val
    result = []
    for (hostname, ns), fields in sorted(grouped.items()):
        if set(fields) != permitted:
            raise ValueError(f"Incomplete TSBS CPU record {hostname}/{ns}: {sorted(permitted-set(fields))}")
        result.append({"hostname": hostname, "timestamp_ns": ns, **fields})
    return result


def parse_cassandra_cpu_file(path: Path, metrics: list[str]) -> list[dict]:
    with path.open(encoding="utf-8") as source:
        return parse_cassandra_cpu_lines(source, metrics)
