"""Shared V8-012 measurement protocol, paired oracle and run-environment guards.

The request loop is a line-for-line generalisation of ``full_benchmark_011.run``
so that every V8-012 measurement (factorial ablations, matched-cohort
baselines) is directly comparable with the frozen AUTO_H 400,000-request run.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import statistics
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Iterable

from . import workload_005 as w005
from .benchmark_005 import BENCHMARK_DOMAIN
from .semantic_replay_011 import canonical, hash_value

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "v8-completion-20261109-012"
RUN_DIR = ROOT / "outputs/final_run_v8_completion_012"
FROZEN_011 = ROOT / "outputs/final_run_v8_completion_011"
REFERENCE_RAW = FROZEN_011 / "full_scale_matched_cohort/raw"
REFERENCE_DEPLOYMENT = "AUTO_H_norm"
WORKLOADS = w005.WORKLOADS
MEASURED = 10_000
WARMUP = 1_000
REPETITIONS = 5
# Processes that write bulk data into the shared Cassandra node.  A timed run
# must never overlap with them (they may be launched from a sibling checkout).
BULK_WRITER_PATTERNS = (
    "methodology.baseline_continuation_011",
    "methodology.sequential_baselines_011",
    "methodology.full_parallel_011",
    "methodology.baseline_benchmark_011",
    "methodology.ablation_pipeline_011",
    "methodology.orchestrator_011",
    "methodology.full_rebench_012 load",
    "methodology.full_cold_012 load",
    # any interpreter of the sibling checkout, whatever module it runs
    "Documents/rubis3/experiments/rubis_cassandra/.venv/bin/python",
)


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")
    tmp.replace(path)


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(Path(path).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# The sibling's sequential runner checks this marker first and exits (rc 75) before opening
# any Cassandra session; while it exists, relaunches of that runner are no-ops (2026-11-10).
SIBLING_PAUSE_MARKER = Path("/Users/hudsonbatista/Documents/rubis3/experiments/rubis_cassandra/outputs/final_run_v8_completion_011/continuation/PAUSE_JOB")
PAUSED_NOOP_SIGNATURE = "methodology/sequential_baselines_011.py"
IGNORED_PAUSED_NOOPS: list[str] = []


def active_bulk_writers() -> list[str]:
    found = set()
    for pattern in BULK_WRITER_PATTERNS:
        p = subprocess.run(["pgrep", "-fl", pattern], capture_output=True, text=True)
        found.update(line for line in p.stdout.splitlines() if line.strip())
    if SIBLING_PAUSE_MARKER.exists():
        noops = {line for line in found if PAUSED_NOOP_SIGNATURE in line and "methodology.baseline_" not in line}
        # only a bare runner (no child loader/benchmark) is a no-op
        if noops and not any("methodology.baseline_" in line for line in found):
            IGNORED_PAUSED_NOOPS.extend(f"{now_iso()} {line}" for line in noops)
            found -= noops
    return sorted(found)


def pending_compactions() -> int | None:
    p = subprocess.run(["docker", "exec", "rubis-schemalens-cassandra", "nodetool", "compactionstats"], capture_output=True, text=True)
    for line in p.stdout.splitlines():
        if line.strip().lower().startswith("pending tasks:"):
            try:
                return int(line.split(":", 1)[1].split()[0])
            except ValueError:
                return None
    return None


def require_idle_node(allow_busy: bool) -> dict[str, Any]:
    """Refuse timed runs while bulk writers or compactions share the node; record the check."""
    writers = active_bulk_writers()
    compactions = pending_compactions()
    state = {"checked_at": now_iso(), "bulk_writers": writers, "pending_compactions": compactions,
             "idle": not writers and compactions == 0}
    if not state["idle"] and not allow_busy:
        raise SystemExit(f"refusing timed measurement: node not idle (pending compactions={compactions}):\n" + "\n".join(writers))
    return state


class BulkWriterWatch:
    """Samples for concurrent bulk writers during a timed measurement."""

    def __init__(self, interval_s: float = 15.0):
        import threading
        self.interval_s, self.samples, self.seen = interval_s, 0, []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        while not self._stop.is_set():
            writers = active_bulk_writers()
            self.samples += 1
            if writers:
                self.seen.append({"at": now_iso(), "writers": writers})
            self._stop.wait(self.interval_s)

    def __enter__(self):
        self._thread.start(); return self

    def __exit__(self, *exc):
        self._stop.set(); self._thread.join(timeout=30)

    def report(self) -> dict[str, Any]:
        return {"samples": self.samples, "interval_s": self.interval_s, "concurrent_bulk_writer_samples": len(self.seen),
                "first_observations": self.seen[:5], "clean": not self.seen,
                "ignored_paused_sibling_runner_noops": {"count": len(IGNORED_PAUSED_NOOPS), "first": IGNORED_PAUSED_NOOPS[:5],
                                                        "marker": str(SIBLING_PAUSE_MARKER),
                                                        "reason": "runner exits rc 75 on the PAUSE_JOB marker before opening a Cassandra session"}}


def percentile(values: list[float], p: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    rank = (len(ordered) - 1) * p
    low, high = math.floor(rank), math.ceil(rank)
    return ordered[low] if low == high else ordered[low] + (rank - low) * (ordered[high] - ordered[low])


def stats(records: list[dict[str, Any]]) -> dict[str, Any]:
    latencies = [float(row["latency_ms"]) for row in records]
    mean = statistics.fmean(latencies) if latencies else 0.0
    return {
        "measured_requests": len(records),
        "mean_ms": mean,
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "throughput_requests_per_second": 1000.0 / mean if mean else 0.0,
        "errors": sum(row["error"] is not None for row in records),
        "physical_reads": sum(row["physical_reads"] for row in records),
        "physical_writes": sum(row["physical_writes"] for row in records),
        "support_reads": sum(row["support_reads"] for row in records),
        "rows_fetched": sum(row["rows_fetched"] for row in records),
        "rows_returned": sum(row["rows_returned"] for row in records),
    }


def run_protocol(adapter, deployment: str, keyspace: str, source_state_id: str, raw_dir: Path,
                 *, measured: int = MEASURED, warmup: int = WARMUP, repetitions: int = REPETITIONS,
                 workloads: Iterable[str] = WORKLOADS) -> dict[str, Any]:
    """Execute the frozen protocol for one deployment and persist raw observations."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    summaries, raw_files = [], []
    adapter.reset_state()
    for workload in workloads:
        workload_records = []
        for repetition in range(repetitions):
            adapter.reset_state()
            for request in w005.histories(workload, repetition, warmup, BENCHMARK_DOMAIN, source_state_id, phase="warmup"):
                context: dict[str, Any] = {}
                for sid in request["constituent_statement_ids"]:
                    adapter.execute_statement(sid, request["parameters"], context)
            measured_records = []
            for request in w005.histories(workload, repetition, measured, BENCHMARK_DOMAIN, source_state_id, phase="measured"):
                context = {}
                statement_rows = []
                start = time.perf_counter_ns()
                error = None
                try:
                    for sid in request["constituent_statement_ids"]:
                        result = adapter.execute_statement(sid, request["parameters"], context)
                        statement_rows.append({
                            "statement_id": sid,
                            "logical_rows": canonical(result["logical_rows"]),
                            "canonical_hash": hash_value(result["logical_rows"]),
                            "row_count": result["row_count"],
                            "physical_reads": result["physical_reads"],
                            "physical_writes": result["physical_writes"],
                            "support_reads": result["support_reads"],
                            "rows_fetched": result["rows_fetched"],
                            "query_plans": result["query_plans"],
                            "write_plans": result["write_plans"],
                        })
                except Exception as exc:  # preserved as a raw failed observation
                    error = repr(exc)
                row = {
                    "logical_request_id": request["logical_request_id"],
                    "deployment_id": deployment,
                    "keyspace": keyspace,
                    "workload": workload,
                    "repetition": repetition,
                    "request_group": request["request_group"],
                    "parameters": canonical(request["parameters"]),
                    "parameter_hash": request["parameter_hash"],
                    "source_state_id": source_state_id,
                    "statements": statement_rows,
                    "canonical_result_hash": hash_value([item["logical_rows"] for item in statement_rows]),
                    "latency_ms": (time.perf_counter_ns() - start) / 1_000_000.0,
                    "physical_reads": sum(item["physical_reads"] for item in statement_rows),
                    "physical_writes": sum(item["physical_writes"] for item in statement_rows),
                    "support_reads": sum(item["support_reads"] for item in statement_rows),
                    "rows_fetched": sum(item["rows_fetched"] for item in statement_rows),
                    "rows_returned": sum(item["row_count"] for item in statement_rows),
                    "error": error,
                }
                measured_records.append(row)
                workload_records.append(row)
            adapter.reset_state()
            summary = stats(measured_records)
            summary.update({"deployment_id": deployment, "workload": workload, "repetition": repetition})
            summaries.append(summary)
        payload = b"\n".join(json.dumps(row, sort_keys=True, default=str).encode() for row in workload_records) + b"\n"
        compressed = gzip.compress(payload, mtime=0)
        path = raw_dir / f"{deployment}_{workload}.jsonl.gz"
        path.write_bytes(compressed)
        raw_files.append({"path": str(path.relative_to(ROOT)), "records": len(workload_records),
                          "bytes": len(compressed), "sha256": hashlib.sha256(compressed).hexdigest()})
    adapter.reset_state()
    return {"deployment_id": deployment, "keyspace": keyspace, "summaries": summaries, "raw_files": raw_files,
            "measured_requests_per_repetition": measured, "warmup_requests_per_repetition": warmup,
            "repetitions": repetitions}


def load_reference_hashes(workloads: Iterable[str] = WORKLOADS) -> dict[tuple, str]:
    """canonical_result_hash of the frozen, oracle-validated AUTO_H_norm run."""
    ref: dict[tuple, str] = {}
    for workload in workloads:
        with gzip.open(REFERENCE_RAW / f"{REFERENCE_DEPLOYMENT}_{workload}.jsonl.gz", "rt", encoding="utf-8") as f:
            for line in f:
                x = json.loads(line)
                ref[(x["workload"], int(x["repetition"]), x["logical_request_id"])] = x["canonical_result_hash"]
    return ref


def paired_oracle(raw_paths: Iterable[Path], reference: dict[tuple, str]) -> dict[str, Any]:
    """Compare every raw observation with the frozen reference by logical request key."""
    counts = defaultdict(int)
    mismatches: list[dict[str, Any]] = []
    by_group = defaultdict(lambda: defaultdict(int))
    for path in raw_paths:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                x = json.loads(line)
                key = (x["workload"], int(x["repetition"]), x["logical_request_id"])
                counts["observations"] += 1
                if x.get("error") is not None:
                    counts["errors"] += 1
                expected = reference.get(key)
                if expected is None:
                    counts["unpaired"] += 1
                    continue
                if expected == x["canonical_result_hash"]:
                    counts["equal"] += 1
                else:
                    counts["mismatch"] += 1
                    by_group[x.get("request_group")]["mismatch"] += 1
                    if len(mismatches) < 50:
                        mismatches.append({"key": list(key), "request_group": x.get("request_group"),
                                           "observed": x["canonical_result_hash"], "expected": expected,
                                           "error": x.get("error")})
    ok = counts["observations"] > 0 and counts["equal"] == counts["observations"]
    return {"status": "PASS" if ok else "FAIL", "reference": f"{REFERENCE_DEPLOYMENT} frozen V8-011 raw (independently oracle-validated)",
            **{k: counts[k] for k in ("observations", "equal", "mismatch", "unpaired", "errors")},
            "mismatch_by_request_group": {k: dict(v) for k, v in by_group.items()}, "first_mismatches": mismatches}


def keyspace_fingerprint(session, keyspace: str) -> dict[str, Any]:
    """Order-independent content fingerprint of every table (rows + modular sum of row hashes)."""
    from cassandra import ConsistencyLevel
    from cassandra.query import SimpleStatement
    tables = sorted(r["table_name"] for r in session.execute(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s", (keyspace,)))
    out = {}
    for t in tables:
        rows, acc = 0, 0
        for row in session.execute(SimpleStatement(f"SELECT * FROM {keyspace}.{t}", fetch_size=5000, consistency_level=ConsistencyLevel.ONE)):
            rows += 1
            payload = json.dumps({k: row[k] for k in sorted(row)}, sort_keys=True, default=str).encode()
            acc = (acc + int.from_bytes(hashlib.sha256(payload).digest()[:16], "big")) % (1 << 128)
        out[t] = {"rows": rows, "content_sum": f"{acc:032x}"}
    return out


def clone_keyspace(cluster, session, source: str, target: str, tables: list[str] | None = None) -> dict[str, Any]:
    """Recreate ``target`` with DDL exported from the catalogue and a verified row copy."""
    from cassandra import ConsistencyLevel
    from cassandra.concurrent import execute_concurrent_with_args
    from cassandra.query import SimpleStatement
    cluster.refresh_schema_metadata()
    meta = cluster.metadata.keyspaces[source]
    tables = sorted(tables if tables is not None else meta.tables)
    session.execute(f"DROP KEYSPACE IF EXISTS {target}")
    session.execute(f"CREATE KEYSPACE {target} WITH replication={{'class':'SimpleStrategy','replication_factor':1}} AND durable_writes=true")
    ddl, copy = {}, {}
    for t in tables:
        stmt = meta.tables[t].as_cql_query().replace(f"{source}.{t}", f"{target}.{t}", 1)
        if f"{target}.{t}" not in stmt:
            raise RuntimeError(f"could not retarget DDL for {source}.{t}")
        session.execute(stmt)
        ddl[t] = stmt
    src_fp = keyspace_fingerprint(session, source)
    for t in tables:
        cols = list(meta.tables[t].columns)
        insert = session.prepare(f"INSERT INTO {target}.{t} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})")
        insert.consistency_level = ConsistencyLevel.ONE
        args = [tuple(r[c] for c in cols) for r in session.execute(SimpleStatement(
            f"SELECT {','.join(cols)} FROM {source}.{t}", fetch_size=5000, consistency_level=ConsistencyLevel.ONE))]
        results = execute_concurrent_with_args(session, insert, args, concurrency=64, raise_on_first_error=False)
        acked = sum(1 for ok, _ in results if ok)
        if acked != len(args):
            raise RuntimeError(f"copy {source}.{t}: {acked}/{len(args)} acknowledged")
        copy[t] = {"source_rows": len(args), "acknowledged_mutations": acked}
    dst_fp = keyspace_fingerprint(session, target)
    bad = [t for t in tables if src_fp.get(t) != dst_fp.get(t)]
    if bad:
        raise RuntimeError(f"fingerprint mismatch after copy {source}->{target}: {bad}")
    for t in tables:
        copy[t]["fingerprint"] = dst_fp[t]
    return {"source_keyspace": source, "keyspace": target, "tables": tables, "ddl": ddl,
            "ddl_sha256": hashlib.sha256(json.dumps(ddl, sort_keys=True).encode()).hexdigest(),
            "copy": copy, "acknowledged_mutations": sum(c["acknowledged_mutations"] for c in copy.values()),
            "fingerprint": dst_fp, "materialized_at": now_iso(), "status": "PASS"}


def state_restoration(before: dict, after: dict) -> dict[str, Any]:
    changed = sorted(t for t in set(before) | set(after) if before.get(t) != after.get(t))
    return {"status": "PASS" if not changed else "FAIL", "tables_checked": len(before), "changed_tables": changed,
            "detail": {t: {"before": before.get(t), "after": after.get(t)} for t in changed}}


def keyspace_storage(keyspace: str) -> dict[str, Any]:
    """Flush and read live SSTable bytes per table from nodetool (measured, not estimated)."""
    docker = "docker"
    subprocess.run([docker, "exec", "rubis-schemalens-cassandra", "nodetool", "flush", keyspace], capture_output=True, text=True)
    out = subprocess.run([docker, "exec", "rubis-schemalens-cassandra", "nodetool", "tablestats", keyspace], capture_output=True, text=True)
    tables, current = {}, None
    for line in out.stdout.splitlines():
        line = line.strip()
        if line.startswith("Table:"):
            current = line.split(":", 1)[1].strip(); tables[current] = {}
        elif current and line.startswith("Space used (live):"):
            tables[current]["live_bytes"] = int(line.split(":", 1)[1].strip())
        elif current and line.startswith("Number of partitions (estimate):"):
            tables[current]["partitions_estimate"] = int(line.split(":", 1)[1].strip())
    return {"keyspace": keyspace, "returncode": out.returncode, "tables": tables,
            "live_bytes_total": sum(t.get("live_bytes", 0) for t in tables.values())}
