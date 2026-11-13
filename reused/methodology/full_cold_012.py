"""V8-012 cold-cache full-scale head-to-head (user request 2026-11-11).

Why: the same-window rebench (full_rebench_012) showed that full-scale latency is dominated by the
OS page-cache state of each keyspace (cold point read ~1.0 ms vs warm ~0.35 ms on this node), which
neither the cross-day comparison nor the warm same-window rebench controlled
(see outputs/.../full_rebench/DIAGNOSTIC_PAGE_CACHE.json).

Fix: identical cache state at the start of EVERY repetition of EVERY deployment.
After the protocol's own reset_state() (i.e. before each repetition's 1,000 warm-up requests) we
  1. flush + drop the Docker VM page cache (privileged helper container from the local cassandra image),
  2. invalidate Cassandra's key cache (row cache is disabled),
and record the page-cache size before/after.  Everything else (loader, protocol, histories,
interleaved order, oracle agreement, isolation, state restoration) is identical to full_rebench_012.
NoSE/QPG keyspaces are NOT dropped at the end.
"""
from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import time
from typing import Any

from . import full_rebench_012 as R
from . import full_schemalens_012 as FS
from . import protocol_012 as P
from .semantic_replay_011 import source_closure

OUT = P.RUN_DIR / "full_cold"
LOAD_DIR = OUT / "load"
RAW = OUT / "raw"
ORDER = R.ORDER
CONTAINER = "rubis-schemalens-cassandra"
IMAGE = "cassandra:3.11.11"
COLD_MAX_KIB = 400_000  # after restart+drop only JVM/mapped libs remain (~140 MB measured)


def _cached_kib() -> int:
    p = subprocess.run(["docker", "exec", CONTAINER, "grep", "^Cached:", "/proc/meminfo"], capture_output=True, text=True)
    return int(p.stdout.split()[1]) if p.returncode == 0 and p.stdout.split() else -1


def drop_caches() -> dict[str, Any]:
    before = _cached_kib()
    d = subprocess.run(["docker", "run", "--rm", "--privileged", "--entrypoint", "sh", IMAGE, "-c",
                        "sync; echo 3 > /proc/sys/vm/drop_caches"], capture_output=True, text=True, timeout=120)
    k = subprocess.run(["docker", "exec", CONTAINER, "nodetool", "invalidatekeycache"], capture_output=True, text=True, timeout=120)
    after = _cached_kib()
    ok = d.returncode == 0 and k.returncode == 0 and 0 <= after and (after < before or after < 700_000)
    return {"at": P.now_iso(), "page_cache_kib_before": before, "page_cache_kib_after": after,
            "drop_rc": d.returncode, "keycache_rc": k.returncode, "ok": ok, "stderr": (d.stderr + k.stderr)[-300:]}


def _ready(session, deadline_s: float = 600.0) -> float:
    t0 = time.time()
    while time.time() - t0 < deadline_s:
        q = subprocess.run(["docker", "exec", CONTAINER, "cqlsh", "-e", "select release_version from system.local"], capture_output=True, text=True)
        if q.returncode == 0:
            try:
                session.execute("SELECT release_version FROM system.local")
                return time.time() - t0
            except Exception:
                pass
        time.sleep(2)
    raise RuntimeError("Cassandra did not come back after restart")


def cold_start(session) -> dict[str, Any]:
    """Truly cold node: clean shutdown (drain), container restart (releases every mmap'd SSTable page and
    the JVM), wait until CQL answers through the benchmark session, then drop the OS page cache."""
    t0 = time.time()
    dr = subprocess.run(["docker", "exec", CONTAINER, "nodetool", "drain"], capture_output=True, text=True, timeout=600)
    rs = subprocess.run(["docker", "restart", CONTAINER], capture_output=True, text=True, timeout=600)
    ready_s = _ready(session)
    d = drop_caches()
    d.update({"kind": "restart+drop", "drain_rc": dr.returncode, "restart_rc": rs.returncode, "ready_s": round(ready_s, 1),
              "cycle_s": round(time.time() - t0, 1)})
    d["ok"] = dr.returncode == 0 and rs.returncode == 0 and d["drop_rc"] == 0 and 0 <= d["page_cache_kib_after"] < COLD_MAX_KIB
    return d


def load(baseline: str) -> dict[str, Any]:
    R.LOAD_DIR = LOAD_DIR  # fresh ledgers/reconciliation for this materialisation
    return R.load(baseline)


def bench(allow_busy: bool = False) -> dict[str, Any]:
    for b in R.BASELINES:
        if (P.read_json(LOAD_DIR / b / "reconciliation.json", {}) or {}).get("status") != "PASS":
            raise SystemExit(f"{b} not reconciled")
    node = P.require_idle_node(allow_busy)
    ref = R.earlier_hashes()
    from cassandra.cluster import Cluster
    from cassandra.policies import ConstantReconnectionPolicy
    from cassandra.query import dict_factory
    cluster = Cluster(["127.0.0.1"], reconnection_policy=ConstantReconnectionPolicy(1.0))
    s = cluster.connect(); s.row_factory = dict_factory; s.default_timeout = 300
    try:
        closure, source_state_id = source_closure(s)
        for dep in ORDER:
            out_path = OUT / f"{dep}_RESULT.json"
            if (P.read_json(out_path, {}) or {}).get("status") == "PASS":
                continue
            adapter, ks = R._adapter(s, dep, closure)
            drops: list[dict] = []
            orig_reset = adapter.reset_state

            calls = [0]

            def cold_reset(_orig=orig_reset, _drops=drops, _calls=calls):
                # run_protocol order: start, then (before rep, after rep) per (workload, repetition), end.
                # Odd calls precede a repetition's warm-up -> cold start there (restart + page-cache drop).
                _orig()
                if _calls[0] % 2 == 1:
                    _drops.append(cold_start(s))
                _calls[0] += 1
            adapter.reset_state = cold_reset
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
            agr = R.agreement([P.ROOT / f["path"] for f in res["raw_files"]], ref)
            errors = sum(x["errors"] for x in res["summaries"])
            # reset_state is called 2 + 2*W*R times (start, before/after each repetition, end); odd calls get a
            # cold start: the W*R "before repetition" calls plus the final end-of-protocol call -> W*R + 1 = 21
            cold_ok = len(drops) == len(P.WORKLOADS) * P.REPETITIONS + 1 and all(x["ok"] for x in drops)
            ok = errors == 0 and agr["status"] == "PASS" and cold_ok and (restoration is None or restoration["status"] == "PASS")
            status = "PASS" if ok else "FAIL"
            if status == "PASS" and not allow_busy and not isolation["clean"]:
                status = "INVALID_CONCURRENT_LOAD"
            rec = {"run_id": P.RUN_ID, "deployment_id": dep, "keyspace": ks, "status": status, "started_at": started, "finished_at": P.now_iso(),
                   "window": "single idle window; before every repetition: nodetool drain + container restart + OS page-cache drop (truly cold node)",
                   "node_state_at_window_start": node, "cache_drops": drops, "cold_start_ok": cold_ok,
                   "isolation": isolation, "state_restoration": restoration, "agreement_with_earlier_full_runs": agr, "errors": errors,
                   "protocol": {k: res[k] for k in ("measured_requests_per_repetition", "warmup_requests_per_repetition", "repetitions")},
                   "summaries": res["summaries"], "raw_files": res["raw_files"]}
            P.write_json(out_path, rec)
            print(json.dumps({"deployment": dep, "status": status, "cold_ok": cold_ok, "agree_all": agr["agree_all_earlier_full_runs"]}), flush=True)
            if status != "PASS":
                return {"status": status}
        return {"status": "PASS"}
    finally:
        cluster.shutdown()


def _contrasts(recs: dict) -> tuple[dict, dict]:
    means = {d: R.rep_means(r) for d, r in recs.items()}
    reps = range(P.REPETITIONS)
    per_dep = {d: {w: {**R.tci([means[d][(w, r)] for r in reps]),
                       "p50_ms_mean_over_reps": statistics.fmean(x["p50_ms"] for x in recs[d]["summaries"] if x["workload"] == w),
                       "p95_ms_mean_over_reps": statistics.fmean(x["p95_ms"] for x in recs[d]["summaries"] if x["workload"] == w),
                       "physical_reads_per_request": sum(x["physical_reads"] for x in recs[d]["summaries"] if x["workload"] == w) / (P.MEASURED * P.REPETITIONS),
                       "physical_writes_per_request": sum(x["physical_writes"] for x in recs[d]["summaries"] if x["workload"] == w) / (P.MEASURED * P.REPETITIONS)}
                   for w in P.WORKLOADS} for d in recs}
    con = {}
    for sl in ("SchemaLens_full_AUTO_H_norm", "SchemaLens_full_AUTO_H_c3c5c7"):
        for b in ("NoSE", "QPG"):
            for w in P.WORKLOADS:
                c = R.tci([means[sl][(w, r)] - means[b][(w, r)] for r in reps])
                con.setdefault(sl, {}).setdefault(b, {})[w] = {"diff_ms": c, "relative": c["mean"] / per_dep[b][w]["mean"], "verdict_95": R.verdict(c)}
    return per_dep, con


def compare() -> dict[str, Any]:
    recs = {d: P.read_json(OUT / f"{d}_RESULT.json", {}) for d in ORDER}
    if any(r.get("status") != "PASS" for r in recs.values()):
        raise SystemExit("cold bench incomplete")
    per_dep, con = _contrasts(recs)
    warm = P.read_json(R.OUT / "REBENCH_COMPARISON.json")["paired_contrasts_same_window"]
    cross = P.read_json(P.RUN_DIR / "comparison/COMPARISON.json")["full_scale_baselines"]["full_scale_head_to_head"]["paired_contrasts_full_protocol"]
    table = [{"schemalens": sl, "baseline": b, "workload": w, "cross_day": cross[sl][b][w]["verdict_95"],
              "same_window_warm": warm[sl][b][w]["verdict_95"], "same_window_cold": con[sl][b][w]["verdict_95"]}
             for sl in con for b in con[sl] for w in P.WORKLOADS]
    drops = [x for r in recs.values() for x in r["cache_drops"]]
    result = {"run_id": P.RUN_ID, "status": "PASS",
              "scope": "full scale (2M items / 20M bids); 4 deployments in ONE idle window; cold cache before every repetition",
              "order": list(ORDER),
              "windows": {d: {"started_at": r["started_at"], "finished_at": r["finished_at"], "isolation_clean": r["isolation"]["clean"]} for d, r in recs.items()},
              "cache_control": {"drops": len(drops), "all_ok": all(x["ok"] for x in drops),
                                "page_cache_kib_after_drop_max": max(x["page_cache_kib_after"] for x in drops),
                                "page_cache_kib_before_drop_median": statistics.median(x["page_cache_kib_before"] for x in drops)},
              "load": {b: P.read_json(LOAD_DIR / b / "LOAD_SUMMARY.json") for b in R.BASELINES},
              "correctness": {d: r["agreement_with_earlier_full_runs"] | {"first_disagreements": None} for d, r in recs.items()},
              "per_deployment": per_dep, "paired_contrasts_cold": con, "verdicts_three_designs": table,
              "unit": "repetition means, n=5; t-based 95% CI on the per-repetition paired difference"}
    P.write_json(OUT / "COLD_COMPARISON.json", result)
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=("load", "bench", "compare", "drop-test"))
    ap.add_argument("--baseline", choices=tuple(R.BASELINES))
    ap.add_argument("--allow-busy", action="store_true")
    a = ap.parse_args()
    if a.stage == "load":
        if not a.baseline:
            ap.error("--baseline required")
        r = load(a.baseline)
    elif a.stage == "drop-test":
        d = drop_caches(); print(json.dumps(d)); r = {"status": "PASS" if d["ok"] else "FAIL"}
    else:
        r = {"bench": lambda: bench(a.allow_busy), "compare": compare}[a.stage]()
    print(json.dumps({"status": r.get("status")}))
    return 0 if r.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
