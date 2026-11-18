"""Stage 9 CLI: offline verification or opt-in real Cassandra benchmark.

Run from project root:
  python3 -m methodology.tsbs.stage9.cli --make-fixture ./data/dev_small.csv
  python3 -m methodology.tsbs.stage9.cli --data ./data/dev_small.csv --dry-run
  python3 -m methodology.tsbs.stage9.cli --data ./data/dev_small.csv --live \\
      --host 127.0.0.1 --deployment H_815d3cd1f0cb \\
      --confirm-reset-keyspace sl_tsbs_h815d3cd1f0cb
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from pathlib import Path

from methodology.tsbs.stage6.materializer import ReferenceMaterializer
from methodology.tsbs.stage5.runtime import evaluate_with_fetcher
from methodology.tsbs.workload import METRICS
from .contracts import read_inputs, save_json, sha, keyspace_for, require_reset_confirmation
from .dataset import load_cpu_dataset, write_small_fixture
from .instances import generate_instances, route_instance
from .benchmark import compare_values, evaluate_deployment, percentile
from .offline import IndexedReferenceFetcher


def _subset(inputs: dict, *, deployment_id: str | None, all_deployments: bool) -> list[dict]:
    deployments = inputs["deployments"]
    if all_deployments:
        return deployments
    if deployment_id:
        found = [dep for dep in deployments if dep["deployment_id"] == deployment_id]
        if not found:
            raise ValueError("Unknown HBench deployment id: " + deployment_id)
        return found
    return [next(d for d in deployments if d["profile"] == "conservative" and d["rank"] == 1)]


def _reference_materializer(inputs: dict, deployment: dict, dataset: dict) -> tuple[ReferenceMaterializer, list[dict], float]:
    start = time.perf_counter()
    m = ReferenceMaterializer(list(inputs["writes"].values()), list(METRICS),
                              candidate_ids=deployment["candidate_ids"])
    mutations = m.apply(dataset["events"])
    return m, mutations, time.perf_counter() - start


def _offline_check(dep: dict, inputs: dict, dataset: dict, instances: list[dict], materializer: ReferenceMaterializer) -> dict:
    fetch = IndexedReferenceFetcher(materializer)
    status = {"deployment_id": dep["deployment_id"], "status": "DRY_RUN_NO_CASSANDRA",
              "offline_instances_validated": 0, "fallbacks": 0, "selection_changed": False}
    for inst in instances:
        selected, decision = route_instance(dep, inst, inputs["plans"])
        canonical = inputs["plans"][f"{inst['query_id']}__ts0_metric_raw"]
        result = evaluate_with_fetcher(selected, fetch, **inst["params"])
        expected = evaluate_with_fetcher(canonical, fetch, **inst["params"])
        compare_values(expected, result, rel_tol=inputs["policy"]["float_rel_tol"],
                       abs_tol=inputs["policy"]["float_abs_tol"])
        status["offline_instances_validated"] += 1
        status["fallbacks"] += (decision != "selected")
    return status


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["deployment_id", "profile", "instance_id", "query_id", "candidate_id", "route_status",
            "request_count", "correct", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "baseline_mean_ms"]
    with path.open("w", encoding="utf-8", newline="") as stream:
        w = csv.DictWriter(stream, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def run(args: argparse.Namespace) -> dict:
    root = args.root.resolve()
    if args.make_fixture is not None:
        write_small_fixture(args.make_fixture, hosts=args.fixture_hosts,
                            hours=args.fixture_hours, interval_seconds=args.fixture_interval_seconds)
        return {"fixture_created": str(args.make_fixture), "not_official_tsbs_data": True}
    if args.data is None:
        raise ValueError("--data is required (or use --make-fixture first)")
    inputs = read_inputs(root)
    selected = _subset(inputs, deployment_id=args.deployment, all_deployments=args.all)
    # Preflight ALL destructive confirmations BEFORE opening Cassandra.
    if args.live:
        for dep in selected:
            ks = keyspace_for(dep["deployment_id"])
            confirmation = ks if (args.all and args.confirm_reset_all == "RESET_ALL_SL_TSBS") else args.confirm_reset_keyspace
            require_reset_confirmation(ks, confirmation)
    source = load_cpu_dataset(args.data, max_observations=inputs["policy"]["max_observations_in_memory"])
    # Verify the declared sampling interval for one representative host.
    # A wrong interval silently changes TSBS window endpoints and invalidates
    # comparisons. The sampled host must have at least two observations.
    sample_host = source["hostnames"][0]
    sample_times = sorted(e["timestamp_ns"] for e in source["events"] if e["hostname"] == sample_host)
    expected_delta = inputs["policy"]["sample_interval_seconds"] * 1_000_000_000
    if len(sample_times) < 2 or any(b-a != expected_delta for a, b in zip(sample_times, sample_times[1:])):
        raise ValueError("TSBS input sample interval conflicts with config/tsbs_stage9_benchmark_policy.json; regenerate or correct policy")
    instances = generate_instances(source, interval_seconds=inputs["policy"]["sample_interval_seconds"],
                                   per_template=inputs["policy"]["queries_per_template"], seed=args.seed,
                                   alignment_cohort=args.alignment_cohort)
    output = root / "outputs/tsbs_stage9" if args.output is None else args.output.resolve()
    if args.live and output.exists() and not args.overwrite_results:
        # Protect previous benchmark results.
        existing = list(output.glob("34_stage9_deployment_runs.json"))
        if existing:
            raise FileExistsError("Existing Stage 9 results; specify --output NEW_FOLDER (recommended) or --overwrite-results")
    output.mkdir(parents=True, exist_ok=True)
    save_json(output / "33_stage9_workload_instances.json", instances)
    summaries, all_rows = [], []
    for dep in selected:
        materializer, mutations, materialization_s = _reference_materializer(inputs, dep, source)
        if not args.live:
            entry = _offline_check(dep, inputs, source, instances, materializer)
            entry.update({"mutation_count": len(mutations), "materialization_seconds": materialization_s,
                          "table_count": len(dep["candidate_ids"]), "observations": source["observations"]})
            summaries.append(entry)
            continue
        from .driver import LiveCassandra
        ks = keyspace_for(dep["deployment_id"])
        live = LiveCassandra(host=args.host, port=args.port, keyspace=ks, confirm_reset=ks)
        try:
            live.create_schema(root, dep["candidate_ids"])
            start_load = time.perf_counter()
            mutation_count = live.write_stage6(list(inputs["writes"].values()), mutations)
            baseline_rows_written = live.write_baseline(source)
            ingest_seconds = time.perf_counter() - start_load
            checked_rows = live.verify_sample_rows(materializer)
            start_bench = time.perf_counter()
            baseline_fetch = (lambda req: live.fetch_baseline(req, source["series_ids"],
                                start_ns=source["start_ns"],
                                end_ns=source["last_ns"] + inputs["policy"]["sample_interval_seconds"]*1_000_000_000))
            details, entry = evaluate_deployment(dep, inputs, source, materializer, instances,
                       live.fetch, baseline_fetch if inputs["policy"]["compare_to_baseline_series"] else None)
            for item in details:
                item["profile"] = dep["profile"]
            all_rows.extend(details)
            entry.update({"keyspace": ks, "ingest_seconds": ingest_seconds,
                          "write_mutations_executed": mutation_count,
                          "baseline_metric_rows_written": baseline_rows_written,
                          "table_rows_verified": checked_rows,
                          "read_benchmark_seconds": time.perf_counter()-start_bench,
                          "reference_materialization_seconds": materialization_s,
                          "dataset_observations": source["observations"],
                          "ingest_observations_per_second": source["observations"]/ingest_seconds,
                          "storage_bytes": None, "note": "Storage requires server-side nodetool snapshots/table statistics"})
            summaries.append(entry)
            save_json(output / "34_stage9_deployment_runs.json", summaries)
            _write_csv(output / "35_stage9_query_latencies.csv", all_rows)
            if entry["status"] != "PASS":
                break
        finally:
            live.close()
    save_json(output / "34_stage9_deployment_runs.json", summaries)
    _write_csv(output / "35_stage9_query_latencies.csv", all_rows)
    valid = bool(summaries) and all(s["status"] == "PASS" for s in summaries)
    manifest = {"stage": 9, "scope": "TSDB_ONLY_POST_SELECTION_CASSANDRA_BENCHMARK",
                "mode": "REAL_CASSANDRA" if args.live else "OFFLINE_DRY_RUN",
                "tsbs_native_go_runner_used": False, "go_generator_byte_identical_instances": False,
                "tsbs_original_structure_baseline_adapted": bool(args.live and inputs["policy"]["compare_to_baseline_series"]),
                "all_selected_deployments_tested": len(selected) == len(inputs["deployments"]),
                "selected_deployment_ids": [d["deployment_id"] for d in selected],
                "query_alignment_cohort": args.alignment_cohort,
                "source_input_sha256": sha(args.data), "source_lines_sha256": source["source_sha256"],
                "observations": source["observations"], "metric_cells": source["metric_cells"],
                "source_start_ns": source["start_ns"], "source_last_ns": source["last_ns"],
                "source_host_count": len(source["hostnames"]),
                "query_instances": len(instances), "upstream_sha256": inputs["sha256"],
                "benchmark_verified": bool(args.live and valid), "performance_claims_allowed": bool(args.live and valid),
                "hbench_selection_modified": False, "go_tsbs_baseline_ran": False,
                "benchmark_limitations": ["Small bounded all-in-memory source; not scalable TSBS bulk loading",
                   "Sequential single-worker client; no crash-safe or distributed aggregate maintenance",
                   "Stage 5 shared logical evaluator is not an independent oracle; validate with TSBS -print-responses too",
                   "Unaligned conditional TS5/TS6 routes fall back to TS0; count fallback rates",
                   "Physical footprint in bytes unavailable without server measurements",
                   "Do not infer near-best/optimal regret without a separate enumerated evaluation oracle"],
                "results": summaries}
    save_json(output / "36_stage9_manifest.json", manifest)
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", type=Path, default=Path.cwd())
    ap.add_argument("--data", type=Path, help="Actual TSBS --format=cassandra --use-case=cpu-only file (optionally .gz)")
    ap.add_argument("--output", type=Path, help="New output directory for each real benchmark run")
    ap.add_argument("--deployment", help="One fixed Stage 8 HBench deployment ID")
    ap.add_argument("--all", action="store_true", help="Execute all seven fixed HBench deployments")
    ap.add_argument("--seed", type=int, default=123, help="Frozen logical query-instantiation seed")
    ap.add_argument("--alignment-cohort", choices=("natural", "aligned"), default="natural",
                    help="Natural arbitrary boundaries or separately declared aligned-control cohort")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--live", action="store_true", help="Enable destructive isolated Cassandra experiment")
    mode.add_argument("--dry-run", action="store_true", help="No Cassandra: validate route equivalence in memory")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9042)
    ap.add_argument("--confirm-reset-keyspace", default="", help="Must match exactly the Stage 9 keyspace name")
    ap.add_argument("--confirm-reset-all", default="", help="For --all: RESET_ALL_SL_TSBS")
    ap.add_argument("--overwrite-results", action="store_true")
    ap.add_argument("--make-fixture", type=Path, help="Write a small SYNTHETIC format-compatible fixture and exit")
    ap.add_argument("--fixture-hosts", type=int, default=40)
    ap.add_argument("--fixture-hours", type=int, default=36)
    ap.add_argument("--fixture-interval-seconds", type=int, default=3600)
    args = ap.parse_args()
    if args.live and args.all and args.confirm_reset_all != "RESET_ALL_SL_TSBS":
        ap.error("--all --live requires --confirm-reset-all RESET_ALL_SL_TSBS")
    try:
        result = run(args)
    except (ValueError, PermissionError, FileNotFoundError, FileExistsError) as exc:
        ap.error(str(exc))
    print(json.dumps({"stage": result.get("stage"), "mode": result.get("mode"),
                      "valid": result.get("benchmark_verified"),
                      "results": [{"id": e["deployment_id"], "status": e["status"]}
                                  for e in result.get("results", [])],
                      "fixture_created": result.get("fixture_created")}, indent=2))
    if args.live and not result["benchmark_verified"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
