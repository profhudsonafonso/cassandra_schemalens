"""Full-scale NoSE / QPG / SchemaLens re-measured in ONE idle window (V8-012 rebench).

Why: the first full-scale head-to-head compared NoSE/QPG measured on 2026-11-09
(V8-011 continuation, keyspaces released afterwards) with SchemaLens measured on
2026-11-10.  Cohort re-measurements showed 7-18 % day-to-day drift on the write
workloads, the same order as the SchemaLens margins.  This stage removes that
confound:

1. ``load``    re-materialises NoSE / QPG from immutable ``rubis_full`` with the
               *unchanged* V8-011 loader (same manifests, same projections), its
               ledgers redirected to ``full_rebench/load`` so the frozen V8-011
               evidence is never touched; then a full-scan reconciliation.
2. ``bench``   measures SchemaLens norm, NoSE, SchemaLens c3c5c7, QPG back to back
               (interleaved) with the frozen protocol, inside BulkWriterWatch.
               Every response must equal the earlier full-scale NoSE, QPG and
               SchemaLens responses for the same logical request.
3. ``compare`` paired contrasts within this window + drift against the earlier runs.
4. ``release`` drops the two re-materialised baseline keyspaces.
"""
from __future__ import annotations

import argparse
import gzip
import json
import math
import statistics
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from . import baseline_continuation_011 as BC
from . import protocol_012 as P
from . import full_schemalens_012 as FS
from .semantic_replay_011 import DeploymentAdapter008, source_closure

OUT = P.RUN_DIR / "full_rebench"
LOAD_DIR = OUT / "load"
RAW = OUT / "raw"
BASELINES = {"nose_bidding": ("NoSE", "rubis_v8_c011_cont_nose_bidding"),
             "qpg_bidding": ("QPG", "rubis_v8_c011_cont_qpg_bidding")}
SL_NORM, SL_C357 = FS.ROUTES[0], FS.ROUTES[1]
# interleaved so that slow drift inside the window does not favour either side
ORDER = ("SchemaLens_full_AUTO_H_norm", "NoSE", "SchemaLens_full_AUTO_H_c3c5c7", "QPG")
EARLIER = {"NoSE": FS.FULL_BASELINE_RAW, "QPG": FS.FULL_BASELINE_RAW,
           "SchemaLens_full_AUTO_H_norm": FS.OUT / "raw", "SchemaLens_full_AUTO_H_c3c5c7": FS.OUT / "raw"}
T975 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262}


# ------------------------------------------------------------------ load
def load(baseline: str) -> dict[str, Any]:
    BC.OUT = LOAD_DIR  # ledgers / manifests / reconciliation of this re-materialisation only
    LOAD_DIR.mkdir(parents=True, exist_ok=True)
    rec_path = LOAD_DIR / baseline / "reconciliation.json"
    if (P.read_json(rec_path, {}) or {}).get("status") == "PASS":
        return {"status": "PASS", "note": "already reconciled"}
    started = time.time()
    progress = BC.materialize(baseline, page_size=100_000, concurrency=1024)
    if progress.get("status") not in ("FULL_SOURCE_ACKNOWLEDGED", "MATERIALIZED_AND_RECONCILED"):
        raise SystemExit(f"{baseline}: load ended with status {progress.get('status')}")
    rec = BC.reconcile(baseline, page_size=10_000)
    P.write_json(LOAD_DIR / baseline / "LOAD_SUMMARY.json",
                 {"baseline": baseline, "keyspace": BASELINES[baseline][1], "status": rec["status"], "scope": rec["scope"],
                  "loader": "methodology.baseline_continuation_011 (unchanged; OUT redirected)",
                  "loader_sha256": P.sha256_file(Path(BC.__file__)), "wall_seconds": time.time() - started, "finished_at": P.now_iso(),
                  "rows_per_table": {t: v["observed"]["rows"] for t, v in rec["tables"].items()}})
    if rec["status"] != "PASS" or rec["scope"] != "FULL":
        raise SystemExit(f"{baseline}: reconciliation {rec['status']} / {rec['scope']}")
    return {"status": "PASS"}


# ------------------------------------------------------------------ bench
def earlier_hashes() -> dict[str, dict[tuple, str]]:
    out: dict[str, dict] = {}
    for dep, raw_dir in EARLIER.items():
        for w in P.WORKLOADS:
            with gzip.open(raw_dir / f"{dep}_{w}.jsonl.gz", "rt", encoding="utf-8") as f:
                for line in f:
                    x = json.loads(line)
                    out.setdefault(dep, {})[(x["workload"], int(x["repetition"]), x["logical_request_id"])] = x["canonical_result_hash"]
    return out


def agreement(paths: list[Path], ref: dict[str, dict]) -> dict[str, Any]:
    n, agree_all, per, first = 0, 0, defaultdict(int), []
    for p in paths:
        with gzip.open(p, "rt", encoding="utf-8") as f:
            for line in f:
                x = json.loads(line); n += 1
                k = (x["workload"], int(x["repetition"]), x["logical_request_id"])
                ok = {d for d, r in ref.items() if r.get(k) == x["canonical_result_hash"]}
                for d in ok:
                    per[d] += 1
                if len(ok) == len(ref):
                    agree_all += 1
                elif len(first) < 20:
                    first.append({"key": list(k), "request_group": x["request_group"], "disagree": sorted(set(ref) - ok)})
    return {"status": "PASS" if n == 200_000 and agree_all == n else "FAIL", "observations": n, "agree_all_earlier_full_runs": agree_all,
            "agreement_per_earlier_run": dict(per), "references": sorted(ref), "first_disagreements": first}


def _adapter(s, dep: str, closure):
    if dep.startswith("SchemaLens"):
        fam = dict(FS.ROUTES)[dep]
        return FS.FullScaleAdapter(s, dep, FS.TARGET, closure, fam), FS.TARGET
    kind = next(k for k, (d, _) in BASELINES.items() if d == dep)
    ks = BASELINES[kind][1]
    return DeploymentAdapter008(s, dep, ks, kind, closure), ks


def bench(allow_busy: bool = False) -> dict[str, Any]:
    for b in BASELINES:
        if (P.read_json(LOAD_DIR / b / "reconciliation.json", {}) or {}).get("status") != "PASS":
            raise SystemExit(f"{b} not reconciled")
    node = P.require_idle_node(allow_busy)
    ref = earlier_hashes()
    cluster, s = FS.session_for()
    try:
        closure, source_state_id = source_closure(s)
        for dep in ORDER:
            out_path = OUT / f"{dep}_RESULT.json"
            if (P.read_json(out_path, {}) or {}).get("status") == "PASS":
                continue
            adapter, ks = _adapter(s, dep, closure)
            before = FS.target_partitions(s, FS.domain_keys()) if dep.startswith("SchemaLens") else None
            started = P.now_iso()
            with P.BulkWriterWatch() as watch:
                res = P.run_protocol(adapter, dep, ks, source_state_id, RAW)
            isolation = watch.report()
            restoration = None
            if before is not None:
                after = FS.target_partitions(s, FS.domain_keys())
                changed = sorted(t for t in before if before[t] != after[t])
                restoration = {"status": "PASS" if not changed else "FAIL", "domain_tables_changed": changed}
            agr = agreement([P.ROOT / f["path"] for f in res["raw_files"]], ref)
            errors = sum(x["errors"] for x in res["summaries"])
            ok = errors == 0 and agr["status"] == "PASS" and (restoration is None or restoration["status"] == "PASS")
            status = "PASS" if ok else "FAIL"
            if status == "PASS" and not allow_busy and not isolation["clean"]:
                status = "INVALID_CONCURRENT_LOAD"
            rec = {"run_id": P.RUN_ID, "deployment_id": dep, "keyspace": ks, "status": status, "started_at": started, "finished_at": P.now_iso(),
                   "window": "single idle window shared by all four deployments (rebench)", "node_state_at_window_start": node,
                   "isolation": isolation, "state_restoration": restoration, "agreement_with_earlier_full_runs": agr, "errors": errors,
                   "protocol": {k: res[k] for k in ("measured_requests_per_repetition", "warmup_requests_per_repetition", "repetitions")},
                   "summaries": res["summaries"], "raw_files": res["raw_files"]}
            P.write_json(out_path, rec)
            print(json.dumps({"deployment": dep, "status": status, "agree_all": agr["agree_all_earlier_full_runs"]}), flush=True)
            if status != "PASS":
                return {"status": status}
        return {"status": "PASS"}
    finally:
        cluster.shutdown()


# ------------------------------------------------------------------ compare
def tci(values: list[float]) -> dict[str, float]:
    n = len(values); m = statistics.fmean(values); sd = statistics.stdev(values) if n > 1 else 0.0
    h = T975.get(n - 1, 1.96) * sd / math.sqrt(n) if n > 1 else float("nan")
    return {"mean": m, "sd": sd, "n": n, "ci95_low": m - h, "ci95_high": m + h}


def verdict(c: dict) -> str:
    return "SchemaLens faster" if c["ci95_high"] < 0 else "SchemaLens slower" if c["ci95_low"] > 0 else "no significant difference"


def rep_means(rec: dict) -> dict[tuple, float]:
    return {(x["workload"], x["repetition"]): x["mean_ms"] for x in rec["summaries"]}


def earlier_rep_means(dep: str) -> dict[tuple, float]:
    if dep.startswith("SchemaLens"):
        return rep_means(P.read_json(FS.OUT / f"{dep}_RESULT.json"))
    kind = next(k for k, (d, _) in BASELINES.items() if d == dep)
    return rep_means(P.read_json(P.FROZEN_011 / f"continuation/full_baselines/benchmark/BENCHMARK_{kind}.json"))


def compare() -> dict[str, Any]:
    recs = {d: P.read_json(OUT / f"{d}_RESULT.json", {}) for d in ORDER}
    if any(r.get("status") != "PASS" for r in recs.values()):
        raise SystemExit("rebench incomplete")
    means = {d: rep_means(r) for d, r in recs.items()}
    reps = range(P.REPETITIONS)
    per_dep = {d: {w: {**tci([means[d][(w, r)] for r in reps]),
                       "p95_ms_mean_over_reps": statistics.fmean(x["p95_ms"] for x in recs[d]["summaries"] if x["workload"] == w),
                       "physical_reads_per_request": sum(x["physical_reads"] for x in recs[d]["summaries"] if x["workload"] == w) / (P.MEASURED * P.REPETITIONS),
                       "physical_writes_per_request": sum(x["physical_writes"] for x in recs[d]["summaries"] if x["workload"] == w) / (P.MEASURED * P.REPETITIONS)}
                   for w in P.WORKLOADS} for d in ORDER}
    contrasts = {}
    for sl in ("SchemaLens_full_AUTO_H_norm", "SchemaLens_full_AUTO_H_c3c5c7"):
        for b in ("NoSE", "QPG"):
            for w in P.WORKLOADS:
                c = tci([means[sl][(w, r)] - means[b][(w, r)] for r in reps])
                contrasts.setdefault(sl, {}).setdefault(b, {})[w] = {"diff_ms": c, "relative": c["mean"] / per_dep[b][w]["mean"], "verdict_95": verdict(c)}
    drift = {}
    for d in ORDER:
        early = earlier_rep_means(d)
        for w in P.WORKLOADS:
            e = statistics.fmean(early[(w, r)] for r in reps); n = per_dep[d][w]["mean"]
            drift.setdefault(d, {})[w] = {"earlier_mean_ms": e, "rebench_mean_ms": n, "relative_change": (n - e) / e}
    # earlier cross-day verdicts, for the "did anything change" table
    old = P.read_json(P.RUN_DIR / "comparison/COMPARISON.json")["full_scale_baselines"]["full_scale_head_to_head"]["paired_contrasts_full_protocol"]
    changes = [{"schemalens": sl, "baseline": b, "workload": w, "cross_day": old[sl][b][w]["verdict_95"], "same_window": contrasts[sl][b][w]["verdict_95"]}
               for sl in contrasts for b in contrasts[sl] for w in P.WORKLOADS]
    windows = {d: {"started_at": r["started_at"], "finished_at": r["finished_at"], "isolation_clean": r["isolation"]["clean"]} for d, r in recs.items()}
    result = {"run_id": P.RUN_ID, "status": "PASS", "scope": "full scale (2M items / 20M bids); NoSE, QPG and both SchemaLens routes in ONE idle window",
              "order": list(ORDER), "windows": windows,
              "load": {b: P.read_json(LOAD_DIR / b / "LOAD_SUMMARY.json") for b in BASELINES},
              "correctness": {d: r["agreement_with_earlier_full_runs"] | {"first_disagreements": None} for d, r in recs.items()},
              "per_deployment": per_dep, "paired_contrasts_same_window": contrasts, "drift_vs_earlier_runs": drift,
              "verdict_changes_vs_cross_day": changes,
              "unit": "repetition means, n=5; t-based 95% CI on the per-repetition paired difference"}
    P.write_json(OUT / "REBENCH_COMPARISON.json", result)
    return result


def release() -> dict[str, Any]:
    if (P.read_json(OUT / "REBENCH_COMPARISON.json", {}) or {}).get("status") != "PASS":
        raise SystemExit("compare not PASS; keyspaces kept")
    out = {}
    for b, (_, ks) in BASELINES.items():
        p = subprocess.run(["docker", "exec", "rubis-schemalens-cassandra", "cqlsh", "-e", f"DROP KEYSPACE IF EXISTS {ks};"], capture_output=True, text=True)
        out[ks] = {"returncode": p.returncode, "stderr": p.stderr[-500:]}
    res = {"status": "PASS" if all(v["returncode"] == 0 for v in out.values()) else "FAIL", "dropped": out, "at": P.now_iso()}
    P.write_json(OUT / "RELEASE.json", res)
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=("load", "bench", "compare", "release"))
    ap.add_argument("--baseline", choices=tuple(BASELINES))
    ap.add_argument("--allow-busy", action="store_true")
    a = ap.parse_args()
    if a.stage == "load":
        if not a.baseline:
            ap.error("--baseline required")
        r = load(a.baseline)
    else:
        r = {"bench": lambda: bench(a.allow_busy), "compare": compare, "release": release}[a.stage]()
    print(json.dumps({"status": r.get("status")}))
    return 0 if r.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
