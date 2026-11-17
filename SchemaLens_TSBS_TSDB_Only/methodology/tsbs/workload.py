"""Faithful logical-query inventory for TSBS cpu-only / devops.

These are *logical evidence SQL statements*, not executable Cassandra CQL.
The 16 registered templates and numeric parameters correspond to the TSBS
cmd/tsbs_generate_queries/main.go useCaseMatrix at pinned upstream commit.

The TSBS Go query generator chooses concrete random hosts and time windows.
This adapter keeps them symbolic, because the activation engine must never
inspect benchmark timings or select a design from concrete random values.
"""
from __future__ import annotations

from dataclasses import dataclass

METRICS = (
    "usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait",
    "usage_irq", "usage_softirq", "usage_steal", "usage_guest", "usage_guest_nice",
)
TAGS = (
    "hostname", "region", "datacenter", "rack", "os", "arch", "team",
    "service", "service_version", "service_environment",
)
TSBS_COMMIT = "8323e59c74027b108f4ad5ec5d3e498b0101a02e"


@dataclass(frozen=True)
class Template:
    query_id: str
    query_type: str
    kind: str
    metric_count: int
    host_count: int  # 0 means all hosts
    window_hours: int | None
    description: str


TEMPLATES = (
    Template("Q01", "single-groupby-1-1-1", "single", 1, 1, 1, "MAX per minute; 1 metric, 1 host, 1 hour"),
    Template("Q02", "single-groupby-1-1-12", "single", 1, 1, 12, "MAX per minute; 1 metric, 1 host, 12 hours"),
    Template("Q03", "single-groupby-1-8-1", "single", 1, 8, 1, "MAX per minute; 1 metric, 8 hosts, 1 hour"),
    Template("Q04", "single-groupby-5-1-1", "single", 5, 1, 1, "MAX per minute; 5 metrics, 1 host, 1 hour"),
    Template("Q05", "single-groupby-5-1-12", "single", 5, 1, 12, "MAX per minute; 5 metrics, 1 host, 12 hours"),
    Template("Q06", "single-groupby-5-8-1", "single", 5, 8, 1, "MAX per minute; 5 metrics, 8 hosts, 1 hour"),
    Template("Q07", "cpu-max-all-1", "maxall", 10, 1, 8, "MAX of all CPU metrics per hour; 1 host"),
    Template("Q08", "cpu-max-all-8", "maxall", 10, 8, 8, "MAX of all CPU metrics per hour; 8 hosts"),
    Template("Q09", "cpu-max-all-32-24", "maxall", 10, 32, 24, "MAX of all CPU metrics per hour; 32 hosts / 24 hours"),
    Template("Q10", "double-groupby-1", "double", 1, 0, 12, "AVG per hour and hostname; 1 metric, all hosts"),
    Template("Q11", "double-groupby-5", "double", 5, 0, 12, "AVG per hour and hostname; 5 metrics, all hosts"),
    Template("Q12", "double-groupby-all", "double", 10, 0, 12, "AVG per hour and hostname; all 10 metrics, all hosts"),
    Template("Q13", "high-cpu-1", "high", 10, 1, 12, "All CPU fields above usage_user=90; 1 host, 12 hours"),
    Template("Q14", "high-cpu-all", "high", 10, 0, 12, "All CPU fields above usage_user=90; all hosts, 12 hours"),
    Template("Q15", "lastpoint", "lastpoint", 10, 0, None, "Most recent row for EVERY host (not global LIMIT 1)"),
    Template("Q16", "groupby-orderby-limit", "toprollup", 1, 0, None, "5 latest per-minute MAX aggregates up to random end"),
)


def _hosts_filter(count: int) -> str:
    if count == 0:
        return ""   # All hosts: no hostname predicate.
    if count == 1:
        return "c.hostname = :hostname"
    return "c.hostname IN (" + ", ".join(f":host_{i}" for i in range(1, count + 1)) + ")"


def _window_filter() -> str:
    # TSBS Go generators use [start, end), not inclusive BETWEEN.
    return "c.event_ts >= :start_ts AND c.event_ts < :end_ts"


def _projection(metric_count: int) -> str:
    return ", ".join("c." + m for m in METRICS[:metric_count])


def logical_sql(template: Template) -> str:
    """Return a controlled logical SQL representation of an upstream query type.

    TIME_BUCKET and QUALIFY are conceptual operators in these statements,
    not claims about native Cassandra CQL execution.
    """
    kind = template.kind
    if kind in ("single", "maxall"):
        resolution = "1 minute" if kind == "single" else "1 hour"
        aggregated = ",\n       ".join(f"MAX(c.{m}) AS max_{m}" for m in METRICS[:template.metric_count])
        clauses = [_hosts_filter(template.host_count), _window_filter()]
        where = " AND\n      ".join(x for x in clauses if x)
        return (f"SELECT TIME_BUCKET('{resolution}', c.event_ts) AS bucket,\n       {aggregated}\n"
                f"FROM CPUReading c\nWHERE {where}\n"
                f"GROUP BY TIME_BUCKET('{resolution}', c.event_ts)\nORDER BY bucket ASC;\n")
    if kind == "double":
        aggregated = ",\n       ".join(f"AVG(c.{m}) AS avg_{m}" for m in METRICS[:template.metric_count])
        return (f"SELECT c.hostname, TIME_BUCKET('1 hour', c.event_ts) AS bucket,\n       {aggregated}\n"
                "FROM CPUReading c\nWHERE " + _window_filter() + "\n"
                "GROUP BY c.hostname, TIME_BUCKET('1 hour', c.event_ts)\n"
                "ORDER BY bucket ASC, c.hostname ASC;\n")
    if kind == "high":
        where = " AND\n      ".join(x for x in (_hosts_filter(template.host_count), _window_filter(), "c.usage_user > 90.0") if x)
        return (f"SELECT c.hostname, c.event_ts, {_projection(10)}\n"
                f"FROM CPUReading c\nWHERE {where}\nORDER BY c.event_ts ASC;\n")
    if kind == "lastpoint":
        # TSBS has LAST POINT *PER HOST*, not ORDER BY time DESC LIMIT 1.
        # QUALIFY is a LOGICAL notation only; not direct Cassandra CQL.
        return (f"SELECT c.hostname, c.event_ts, {_projection(10)}\n"
                "FROM CPUReading c\n"
                "QUALIFY ROW_NUMBER() OVER (PARTITION BY c.hostname ORDER BY c.event_ts DESC) = 1;\n")
    if kind == "toprollup":
        return ("SELECT TIME_BUCKET('1 minute', c.event_ts) AS bucket,\n"
                "       MAX(c.usage_user) AS max_usage_user\n"
                "FROM CPUReading c\nWHERE c.event_ts < :end_ts\n"
                "GROUP BY TIME_BUCKET('1 minute', c.event_ts)\n"
                "ORDER BY bucket DESC\nLIMIT 5;\n")
    raise ValueError(f"Unknown TSBS logical query type: {kind}")


def validate_workload(scale: int, seconds: int, days: int) -> None:
    if scale <= 0 or seconds <= 0 or days <= 0:
        raise ValueError("scale, interval_seconds and days must be positive")
    for t in TEMPLATES:
        if t.host_count > scale:
            raise ValueError(f"{t.query_type} requires >= {t.host_count} hosts; scale={scale}")
        if t.window_hours and t.window_hours > 24 * days:
            raise ValueError(f"{t.query_type} window exceeds {days} day dataset")
