"""Isolated-keyspace Cassandra driver; only called with explicit live opt-in.

Requires `pip install cassandra-driver`. No Docker, Cassandra server, or network
is assumed when importing this module or exercising dry-run tests.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from methodology.tsbs.stage6.cassandra_executor import ExperimentalCassandraExecutor
from .contracts import keyspace_for, require_reset_confirmation


class LiveCassandra:
    def __init__(self, *, host: str, port: int, keyspace: str, confirm_reset: str, consistency: str = "ONE"):
        require_reset_confirmation(keyspace, confirm_reset)
        if consistency != "ONE":
            raise ValueError("Controlled Stage 9 supports only consistency=ONE")
        try:
            from cassandra.cluster import Cluster
            from cassandra import ConsistencyLevel
        except ImportError as exc:
            raise RuntimeError("Install optional dependency: pip install cassandra-driver") from exc
        self.cluster = Cluster([host], port=port)
        self.session = self.cluster.connect()
        self.session.default_consistency_level = ConsistencyLevel.ONE
        self.session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        self.session.execute(f"CREATE KEYSPACE {keyspace} WITH replication = "
                             "{'class': 'SimpleStrategy', 'replication_factor': 1}")
        self.session.set_keyspace(keyspace)
        self.keyspace = keyspace
        self.prepared: dict[str, Any] = {}

    def create_schema(self, root: Path, ids: list[str]) -> None:
        for cid in sorted(ids):
            source = root / f"outputs/tsbs_stage4/cql/schemas/{cid}.cql"
            if not source.is_file():
                raise FileNotFoundError(source)
            ddl = "\n".join(line for line in source.read_text(encoding="utf-8").splitlines()
                            if not line.lstrip().startswith("--")).strip()
            if ddl.count(";") != 1 or "CREATE TABLE IF NOT EXISTS" not in ddl.upper():
                raise ValueError(f"Unexpected Stage 4 DDL: {cid}")
            self.session.execute(ddl)
        # Identical partition/clustering definitions to TSBS `series_double`,
        # without legacy COMPACT STORAGE (unsupported by current Cassandra).
        self.session.execute("CREATE TABLE baseline_series_double ("
                             "series_id text, timestamp_ns bigint, value double, "
                             "PRIMARY KEY (series_id, timestamp_ns))")

    def prepared_exec(self, cql: str, parameters: list | tuple) -> Any:
        if cql not in self.prepared:
            self.prepared[cql] = self.session.prepare(cql)
        values = [date.fromisoformat(v) if isinstance(v, str) and len(v) == 10
                  and v[4:5] == "-" and v[7:8] == "-" else v for v in parameters]
        return self.session.execute(self.prepared[cql], tuple(values))

    def write_stage6(self, write_plans: list[dict], mutations: list[dict]) -> int:
        writer = ExperimentalCassandraExecutor(self.session, write_plans,
                                              expected_keyspace=self.keyspace)
        return writer.execute_mutations(mutations, allow_experimental_writes=True)

    def write_baseline(self, dataset: dict) -> int:
        cql = "INSERT INTO baseline_series_double (series_id,timestamp_ns,value) VALUES (?,?,?)"
        n = 0
        from methodology.tsbs.workload import METRICS
        from methodology.tsbs.stage5.planner import NS_DAY
        from datetime import timedelta
        for event in dataset["events"]:
            t, host = event["timestamp_ns"], event["hostname"]
            day = (date(1970, 1, 1) + timedelta(days=t // NS_DAY)).isoformat()
            for metric in METRICS:
                series_id = dataset["series_ids"][(host, metric, day)]
                self.prepared_exec(cql, (series_id, t, event[metric]))
                n += 1
        return n

    def fetch(self, request: dict) -> list[dict]:
        rows = self.prepared_exec(request["cql"], request["parameters"])
        return [r._asdict() for r in rows]

    def fetch_baseline(self, request: dict, series_ids: dict, *, start_ns: int, end_ns: int) -> list[dict]:
        """Translate TS0 logical read to the original TSBS scalar-series/day key.

        This is a STRUCTURALLY faithful Python read adapter, not the original Go
        TSBS query runner (whose client-side index/planning must be compared
        separately). Both HBench and baseline use identical Stage 5 evaluator.
        """
        from methodology.tsbs.stage5.planner import NS_DAY
        from datetime import timedelta
        host, metric = request["hostname"], request["metric_name"]
        values = request["parameters"]
        if "LIMIT 1" in request["cql"]:
            lower, upper = start_ns, end_ns
        elif len(values) == 3:
            lower, upper = start_ns, values[2]
        else:
            lower, upper = values[-2:]
        result = []
        lower_day = lower // NS_DAY
        upper_day = (upper - 1) // NS_DAY
        for idx in range(lower_day, upper_day + 1):
            day = (date(1970, 1, 1) + timedelta(days=idx)).isoformat()
            series = series_ids.get((host, metric, day))
            if not series:
                continue
            cql = ("SELECT timestamp_ns,value FROM baseline_series_double "
                   "WHERE series_id=? AND timestamp_ns >=? AND timestamp_ns <?")
            # The baseline is explicit about every day's partition scan.
            result.extend(r._asdict() for r in self.prepared_exec(cql, [series, lower, upper]))
        return result

    def verify_sample_rows(self, materializer: Any, *, sample_count: int = 4) -> int:
        """Read exact PKs back from Cassandra; verify materialized row payloads."""
        from .benchmark import compare_values
        checked = 0
        for cid, plan in materializer.plans.items():
            table = plan["table_name"]
            cols = plan["column_order"]
            pk_cols = plan["partition_key"] + plan["clustering_key"]
            cql = (f"SELECT {', '.join(cols)} FROM {table} WHERE "
                   + " AND ".join(f"{col}=?" for col in pk_cols))
            for row in list(materializer.rows[cid].values())[:sample_count]:
                records = self.prepared_exec(cql, [row[k] for k in pk_cols])
                got = [r._asdict() for r in records]
                for actual in got:
                    if "bucket_day" in actual:
                        actual["bucket_day"] = str(actual["bucket_day"])
                if len(got) != 1:
                    raise AssertionError(f"{cid} missing row {pk_cols}")
                compare_values(row, got[0], rel_tol=1e-9, abs_tol=1e-8)
                checked += 1
        return checked

    def close(self) -> None:
        self.cluster.shutdown()
