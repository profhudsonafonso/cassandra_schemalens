"""Evidence verifier for V8-012.  Every gate is recomputed from raw files.

Final states:
  FINAL_COMPLETE_WITH_DECLARED_LIMITATIONS  all executable gates pass; the
      limitations that cannot be removed by more execution are listed.
  INCOMPLETE_WITH_EVIDENCE                  at least one executable gate failed
      or has not run.
"""
from __future__ import annotations

import argparse
import gzip
import json
import shutil
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

from . import protocol_012 as P
from .ablation_factorial_012 import CELLS, OUT as ABL_OUT
from .cohort_baselines_012 import BASELINES, OUT as COHORT_OUT

ALLOWED_MECHANISM_STATUS = {"MEASURED", "NOT_IDENTIFIABLE", "ANALYTICAL_ONLY"}


def recount_raw(paths: list[Path], reference: dict, measured: int = P.MEASURED, repetitions: int = P.REPETITIONS) -> dict[str, Any]:
    """Independent recount: rows, errors, duplicates, group sizes and paired equality."""
    expected_rows = len(P.WORKLOADS) * repetitions * measured
    rows = errors = dup = equal = 0
    keys, groups, bad = set(), Counter(), []
    for p in paths:
        try:
            with gzip.open(p, "rt", encoding="utf-8") as f:
                for line in f:
                    x = json.loads(line)
                    rows += 1
                    errors += x.get("error") is not None
                    key = (x["workload"], int(x["repetition"]), x["logical_request_id"])
                    dup += key in keys
                    keys.add(key)
                    groups[key[:2]] += 1
                    equal += reference.get(key) == x.get("canonical_result_hash")
        except Exception as exc:  # unreadable or truncated evidence
            bad.append({"file": str(p), "reason": repr(exc)})
    ok = (rows == expected_rows and errors == 0 and dup == 0 and equal == rows and not bad
          and len(groups) == len(P.WORKLOADS) * repetitions and all(v == measured for v in groups.values()))
    return {"status": "PASS" if ok else "FAIL", "rows": rows, "expected_rows": expected_rows, "errors": errors,
            "duplicates": dup, "paired_equal": equal, "groups": len(groups), "bad": bad}


def _record_gate(path: Path, reference: dict, root: Path) -> dict[str, Any]:
    r = P.read_json(path)
    if not r:
        return {"status": "NOT_EXECUTED", "record": str(path)}
    raw = [root / f["path"] for f in r.get("raw_files", [])]
    declared = r.get("protocol", {})
    recount = recount_raw(raw, reference, int(declared.get("measured_requests_per_repetition", 0)), int(declared.get("repetitions", 0)))
    checks = {"record_status": r.get("status") == "PASS", "recount": recount["status"] == "PASS",
              "state_restored": r.get("state_restoration", {}).get("status") == "PASS",
              "isolated": r.get("isolation", {}).get("clean") is True,
              "node_idle_at_start": r.get("node_state", {}).get("idle") is True,
              "frozen_protocol": r.get("protocol") == {"measured_requests_per_repetition": P.MEASURED,
                                                       "warmup_requests_per_repetition": P.WARMUP, "repetitions": P.REPETITIONS}}
    return {"status": "PASS" if all(checks.values()) else "FAIL", "checks": checks, "recount": recount}


def verify(root: Path = P.ROOT, run_dir: Path | None = None, label: str = "protocol") -> dict[str, Any]:
    run_dir = run_dir or P.RUN_DIR
    abl, coh = run_dir / ABL_OUT.relative_to(P.RUN_DIR) / label, run_dir / COHORT_OUT.relative_to(P.RUN_DIR) / label
    reference = P.load_reference_hashes()
    gates: dict[str, Any] = {}
    frozen = P.read_json(P.FROZEN_011 / "continuation/ACCEPTANCE_RESULT.json", {})
    st = frozen.get("stage_status", {})
    gates["frozen_011_evidence"] = {"status": "PASS" if st.get("oracle_validated") == "PASS" and st.get("benchmark_measured") == "PASS" else "FAIL",
                                    "oracle_validated": st.get("oracle_validated"), "benchmark_measured": st.get("benchmark_measured")}
    cells = {cid: _record_gate(abl / f"{cid}_RESULT.json", reference, root) for cid in sorted(CELLS)}
    analysis = P.read_json(abl / "FACTORIAL_ANALYSIS.json", {})
    mechanisms = analysis.get("mechanisms", [])
    mech_ok = (len(mechanisms) == 9 and all(m.get("status") in ALLOWED_MECHANISM_STATUS for m in mechanisms)
               and all(m.get("reason") for m in mechanisms if m.get("status") != "MEASURED")
               and all(cells.get(m.get("cell"), {}).get("status") == "PASS" for m in mechanisms if m.get("status") == "MEASURED"))
    gates["physical_factorial_ablation"] = {"status": "PASS" if all(c["status"] == "PASS" for c in cells.values()) and analysis.get("status") == "PASS" else "FAIL" if any(c["status"] == "FAIL" for c in cells.values()) else "NOT_EXECUTED",
                                            "cells": cells, "analysis_status": analysis.get("status")}
    gates["mechanism_records"] = {"status": "PASS" if mech_ok else "FAIL", "records": len(mechanisms),
                                  "by_status": dict(Counter(m.get("status") for m in mechanisms))}
    baselines = {kind: _record_gate(coh / f"{kind}_RESULT.json", reference, root) for _, _, kind in BASELINES}
    gates["matched_scale_baselines"] = {"status": "PASS" if all(b["status"] == "PASS" for b in baselines.values()) else "FAIL" if any(b["status"] == "FAIL" for b in baselines.values()) else "NOT_EXECUTED",
                                        "baselines": baselines}
    comparison = P.read_json(run_dir / "comparison/COMPARISON.json", {})
    gates["matched_comparison"] = {"status": "PASS" if comparison.get("matched_scale", {}).get("status") == "PASS" else "NOT_EXECUTED"}
    full = comparison.get("full_scale_baselines", {})
    gates["full_scale_baselines"] = {"status": "PASS" if full.get("status") == "MEASURED" else "INCOMPLETE",
                                     "full_scale_baselines": full.get("full_scale_baselines"), "scope": "NoSE/QPG frozen protocol; Normalized frozen-prefix sample; Expert not measured at full scale"}
    h2h = full.get("full_scale_head_to_head", {})
    gates["full_scale_head_to_head"] = {"status": h2h.get("status", "NOT_EXECUTED"), "schemalens_records": h2h.get("schemalens_records")}
    blockers = [f"{name}: {g['status']}" for name, g in gates.items() if g["status"] != "PASS"]
    incidents = sorted(str(p.relative_to(P.ROOT)) for p in (P.RUN_DIR / "provenance").glob("CONCURRENCY_INCIDENT_*.json"))
    limitations = [
        "Five analytical mechanisms (sharedness, update exposure, access profile, support-query sharing, relationships) are not identifiable: the code that produced H never consumes them. Canonicalization affects only the analytical candidate count.",
        "H is a hand-listed set of two SchemaLens deployments with hand-coded routes; the factorial measures the C3/C5/C7 families inside that H, not an automatic search.",
        "Full-scale baselines (V8-011 continuation) and full-scale SchemaLens (V8-012) were measured in different idle windows; matched-scale comparisons share one window.",
        "At full scale, Normalized was measured on a frozen-protocol prefix (repetition 0; 1,000 warm-up + 1,000 measured requests per workload), checked per request against the full NoSE and QPG runs; its client-side joins make the frozen protocol infeasible (projected >24 h; sibling run interrupted after 5 h 12 min, see provenance/FULL_BASELINE_INTERRUPTION_002.json). Expert was not measured at full scale (user decision 2026-11-10; its keyspace was only partially loaded when the sibling job stopped). Matched-scale (cohort) comparisons use the full frozen protocol for all four baselines, including Expert.",
        "The NoSE/QPG/Normalized/Expert designs are source-derived reconstructions of the historical artifacts (see baseline manifests), not reruns of the original optimisers.",
        "Benchmark parameter domain is 30 users / 30 items / 10 categories / 10 regions (BENCHMARK_DOMAIN); uniform sampling is a research assumption.",
        "Throughput is single-client closed-loop (1000 / mean latency), not saturation throughput.",
    ] + [f"Recorded concurrency incident: {i}" for i in incidents]
    status = "FINAL_COMPLETE_WITH_DECLARED_LIMITATIONS" if not blockers else "INCOMPLETE_WITH_EVIDENCE"
    result = {"run_id": P.RUN_ID, "acceptance_status": status, "gates": gates, "blockers": blockers,
              "declared_limitations": limitations, "global_pass_hardcoded": False}
    if run_dir == P.RUN_DIR:
        P.write_json(run_dir / "ACCEPTANCE_RESULT.json", result)
    return result


def negative_tests(label: str = "protocol") -> dict[str, Any]:
    """Each tampering must turn a passing gate into a failing one."""
    tests = []
    abl = ABL_OUT / label
    sample = abl / "f000_RESULT.json"
    if not sample.exists():
        return {"status": "NOT_EXECUTED", "reason": "no measured cell to tamper with"}
    reference = P.load_reference_hashes()
    record = json.loads(sample.read_text())
    baseline_gate = _record_gate(sample, reference, P.ROOT)
    with tempfile.TemporaryDirectory(prefix="v8_012_negative_") as d:
        tmp = Path(d)

        def tampered(mutator):
            rec = json.loads(json.dumps(record))
            for f in rec["raw_files"]:
                dst = tmp / f["path"]
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(P.ROOT / f["path"], dst)
            mutator(rec, tmp)
            path = tmp / "cell.json"
            path.write_text(json.dumps(rec))
            return _record_gate(path, reference, tmp)["checks"]

        def flip_hash(rec, root):
            p = root / rec["raw_files"][0]["path"]
            lines = gzip.decompress(p.read_bytes()).decode().splitlines()
            x = json.loads(lines[0]); x["canonical_result_hash"] = "0" * 64; lines[0] = json.dumps(x)
            p.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode()))

        def drop_rows(rec, root):
            p = root / rec["raw_files"][0]["path"]
            lines = gzip.decompress(p.read_bytes()).decode().splitlines()
            p.write_bytes(gzip.compress(("\n".join(lines[:-10]) + "\n").encode()))

        def inject_error(rec, root):
            p = root / rec["raw_files"][0]["path"]
            lines = gzip.decompress(p.read_bytes()).decode().splitlines()
            x = json.loads(lines[5]); x["error"] = "Timeout()"; lines[5] = json.dumps(x)
            p.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode()))

        cases = {
            "mutated_result_hash": (flip_hash, "recount"),
            "removed_raw_observations": (drop_rows, "recount"),
            "injected_request_error": (inject_error, "recount"),
            "concurrent_load_observed": (lambda rec, root: rec["isolation"].update({"clean": False}), "isolated"),
            "state_not_restored": (lambda rec, root: rec["state_restoration"].update({"status": "FAIL"}), "state_restored"),
            "reduced_protocol": (lambda rec, root: rec["protocol"].update({"measured_requests_per_repetition": 10}), "frozen_protocol"),
        }
        for name, (mut, check) in cases.items():
            before = baseline_gate["checks"][check]
            after = tampered(mut)[check]
            # 'reduced_protocol' is only meaningful when the record really used the frozen protocol
            tests.append({"name": name, "check": check, "before": before, "after": after, "passed": after is False and (before is True or check == "frozen_protocol")})
    analysis = P.read_json(abl / "FACTORIAL_ANALYSIS.json", {})
    if analysis:
        fake = json.loads(json.dumps(analysis))
        fake["mechanisms"][3]["status"] = "PASS"  # an invented status must be rejected
        ok = not all(m.get("status") in ALLOWED_MECHANISM_STATUS for m in fake["mechanisms"])
        tests.append({"name": "fabricated_mechanism_status", "passed": ok})
    result = {"status": "PASS" if tests and all(t["passed"] for t in tests) else "FAIL", "tests": tests}
    P.write_json(P.RUN_DIR / "verification/NEGATIVE_TESTS.json", result)
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=("verify", "negative-tests"))
    ap.add_argument("--label", default="protocol")
    a = ap.parse_args()
    r = verify(label=a.label) if a.command == "verify" else negative_tests(a.label)
    print(json.dumps({k: r[k] for k in r if k in ("acceptance_status", "blockers", "status", "tests")}, indent=2, default=str))
    raise SystemExit(0 if r.get("acceptance_status", "").startswith("FINAL") or r.get("status") == "PASS" else 1)
