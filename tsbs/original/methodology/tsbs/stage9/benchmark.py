"""Paired correctness-gated end-to-end read benchmarking, not candidate ranking.

The exact same pre-generated logical query instances are used across all
preselected deployments and the TSBS scalar-series baseline. Results remain
post-selection and MUST NOT modify Stage 8 or earlier methodology.
"""
from __future__ import annotations

import hashlib
import json
import math
import statistics
import time
from datetime import date
from typing import Any, Callable

from methodology.tsbs.stage5.runtime import bind_read_requests, execute_mock_rows, evaluate_with_fetcher
from .instances import route_instance


def compare_values(expected: Any, observed: Any, *, rel_tol: float, abs_tol: float, path: str = "$", ) -> None:
    if isinstance(expected, dict):
        if not isinstance(observed, dict) or set(expected) != set(observed):
            raise AssertionError(f"{path}: result object keys differ")
        for key in expected:
            compare_values(expected[key], observed[key], rel_tol=rel_tol, abs_tol=abs_tol, path=f"{path}.{key}")
    elif isinstance(expected, list):
        if not isinstance(observed, list) or len(expected) != len(observed):
            raise AssertionError(f"{path}: result list length differs")
        for i, (a, b) in enumerate(zip(expected, observed)):
            compare_values(a, b, rel_tol=rel_tol, abs_tol=abs_tol, path=f"{path}[{i}]")
    elif isinstance(expected, (int, float)) and not isinstance(expected, bool):
        if not isinstance(observed, (int, float)) or isinstance(observed, bool) or not math.isclose(expected, observed, rel_tol=rel_tol, abs_tol=abs_tol):
            raise AssertionError(f"{path}: {expected!r} != {observed!r}")
    elif expected != observed:
        raise AssertionError(f"{path}: {expected!r} != {observed!r}")


def percentile(numbers: list[float], p: int) -> float:
    if not numbers:
        raise ValueError("Cannot compute percentile on empty list")
    ordered = sorted(numbers)
    return ordered[max(0, math.ceil(len(ordered)*p/100) - 1)]


def _reference(plan: dict, dataset: dict, materializer: Any, instance: dict) -> list[dict]:
    return evaluate_with_fetcher(plan, materializer.query_mock_rows, **instance["params"])


def _cassandra_result(plan: dict, inst: dict, fetcher: Callable) -> tuple[list[dict], int]:
    reqs = bind_read_requests(plan, **inst["params"])
    return execute_mock_rows(plan, [(req, fetcher(req)) for req in reqs]), len(reqs)


def evaluate_deployment(deployment: dict, inputs: dict, dataset: dict, materializer: Any,
                        instances: list[dict], fetcher: Callable[[dict], list[dict]],
                        baseline_fetcher: Callable[[dict], list[dict]] | None = None) -> tuple[list[dict], dict]:
    p = inputs["policy"]
    plans = inputs["plans"]
    results = []
    issues = []
    total_fallbacks = 0
    for instance in instances:
        qid = instance["query_id"]
        try:
            route, decision = route_instance(deployment, instance, plans)
            if decision != "selected":
                total_fallbacks += 1
            control_plan = plans[f"{qid}__ts0_metric_raw"]
            expected = _reference(control_plan, dataset, materializer, instance)
            # Correctness MUST precede timing. An error aborts this deployment.
            got, nrequests = _cassandra_result(route, instance, fetcher)
            compare_values(expected, got, rel_tol=p["float_rel_tol"], abs_tol=p["float_abs_tol"])
            if baseline_fetcher is not None:
                baseline_result, baseline_reqs = _cassandra_result(control_plan, instance, baseline_fetcher)
                compare_values(expected, baseline_result, rel_tol=p["float_rel_tol"], abs_tol=p["float_abs_tol"])
            latencies = []
            for _ in range(p["warmup_repetitions"]):
                _cassandra_result(route, instance, fetcher)
            for _ in range(p["measured_repetitions"]):
                start = time.perf_counter_ns()
                current, _ = _cassandra_result(route, instance, fetcher)
                duration = (time.perf_counter_ns() - start)/1e6
                compare_values(expected, current, rel_tol=p["float_rel_tol"], abs_tol=p["float_abs_tol"])
                latencies.append(duration)
            baseline_latencies = []
            if baseline_fetcher is not None:
                for _ in range(p["warmup_repetitions"]):
                    _cassandra_result(control_plan, instance, baseline_fetcher)
                for _ in range(p["measured_repetitions"]):
                    start = time.perf_counter_ns()
                    baseline_result, _ = _cassandra_result(control_plan, instance, baseline_fetcher)
                    duration = (time.perf_counter_ns() - start)/1e6
                    compare_values(expected, baseline_result, rel_tol=p["float_rel_tol"], abs_tol=p["float_abs_tol"])
                    baseline_latencies.append(duration)
            results.append({"instance_id": instance["instance_id"], "query_id": qid,
                            "deployment_id": deployment["deployment_id"],
                            "candidate_id": route["candidate_id"], "route_status": decision,
                            "correct": True, "request_count": nrequests,
                            "latency_ms": latencies,
                            "mean_ms": statistics.fmean(latencies),
                            "p50_ms": percentile(latencies, 50), "p95_ms": percentile(latencies, 95),
                            "p99_ms": percentile(latencies, 99),
                            "baseline_latency_ms": baseline_latencies,
                            "baseline_mean_ms": statistics.fmean(baseline_latencies) if baseline_latencies else None})
        except Exception as exc:
            issues.append({"instance_id": instance["instance_id"], "error": str(exc)})
            # Do not continue measuring invalid or incomplete deployments.
            break
    summary = {"deployment_id": deployment["deployment_id"], "status": "PASS" if not issues else "INVALID_NO_PERFORMANCE_CLAIM",
               "instances_validated": len(results), "instances_required": len(instances),
               "fallbacks": total_fallbacks, "errors": issues, "cassandra_executed": True,
               "selection_changed_by_benchmark": False}
    return results, summary
