"""E14 — cost model  cost(S, mix) = Σ_action freq(action, mix) · (a·reads + b·writes + c·rows_read).

Calibrated ONLY on V8-012 measurements (read-only from the old repository): the 4 matched-cohort baselines
and the 8 cells of the C3/C5/C7 factorial (f000 = AUTO_H_norm, f111 = AUTO_H_c3c5c7 re-measured in the same
period).  Fit: per-request latency ≈ d + a·reads + b·writes + c·rows, non-negative least squares on
(deployment, workload, request-group) means weighted by request count.  d is a per-request constant shared
by every schema, so it does not change rankings and is not part of cost().
Validation: Kendall τ between predicted and measured mean latency per workload, and leave-one-deployment-out.
"""
from __future__ import annotations

import gzip
import itertools
import json
from collections import defaultdict
from pathlib import Path

from .common import OLD_RUN_012, RUN, now_iso, read_json, sha256_file, write_json

WORKLOADS = ("browsing", "bidding", "update10x", "update100x")
SOURCES = [OLD_RUN_012 / "ablations/protocol/raw", OLD_RUN_012 / "cohort_baselines/protocol/raw"]
FEATURES = ("reads", "writes", "rows")


def load_points() -> tuple[dict, list[str]]:
    agg = defaultdict(lambda: [0, 0.0, 0.0, 0.0, 0.0])   # n, lat, reads, writes, rows
    files = []
    for d in SOURCES:
        for p in sorted(Path(d).glob("*.jsonl.gz")):
            files.append(str(p))
            with gzip.open(p, "rt") as fh:
                for line in fh:
                    r = json.loads(line)
                    if r.get("error"):
                        continue
                    k = (r["deployment_id"], r["workload"], r["request_group"])
                    a = agg[k]
                    a[0] += 1
                    a[1] += r["latency_ms"]
                    a[2] += r["physical_reads"]
                    a[3] += r["physical_writes"]
                    a[4] += r["rows_fetched"]
    pts = {k: {"n": v[0], "lat": v[1] / v[0], "reads": v[2] / v[0], "writes": v[3] / v[0], "rows": v[4] / v[0]} for k, v in agg.items()}
    return pts, files


def _solve(A, b):
    n = len(b)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for i in range(n):
        piv = max(range(i, n), key=lambda r: abs(M[r][i]))
        if abs(M[piv][i]) < 1e-12:
            return None
        M[i], M[piv] = M[piv], M[i]
        for r in range(n):
            if r != i:
                f = M[r][i] / M[i][i]
                M[r] = [x - f * y for x, y in zip(M[r], M[i])]
    return [M[i][n] / M[i][i] for i in range(n)]


def nnls_fit(pts: dict, keys=None) -> dict:
    keys = keys or list(pts)
    names = ("d",) + FEATURES
    best = None
    for subset in itertools.product((0, 1), repeat=len(names)):
        idx = [i for i, on in enumerate(subset) if on]
        if not idx:
            continue
        A = [[0.0] * len(idx) for _ in idx]
        b = [0.0] * len(idx)
        for k in keys:
            p = pts[k]
            x = [1.0, p["reads"], p["writes"], p["rows"]]
            xs = [x[i] for i in idx]
            for i in range(len(idx)):
                b[i] += p["n"] * xs[i] * p["lat"]
                for j in range(len(idx)):
                    A[i][j] += p["n"] * xs[i] * xs[j]
        sol = _solve(A, b)
        if sol is None or any(v < 0 for v in sol):
            continue
        coef = {n: 0.0 for n in names}
        for i, v in zip(idx, sol):
            coef[names[i]] = v
        sse = sum(pts[k]["n"] * (pts[k]["lat"] - (coef["d"] + coef["reads"] * pts[k]["reads"] + coef["writes"] * pts[k]["writes"] + coef["rows"] * pts[k]["rows"])) ** 2 for k in keys)
        if best is None or sse < best[0]:
            best = (sse, coef)
    tot = sum(pts[k]["n"] for k in keys)
    mean = sum(pts[k]["n"] * pts[k]["lat"] for k in keys) / tot
    sst = sum(pts[k]["n"] * (pts[k]["lat"] - mean) ** 2 for k in keys)
    return {"a_read_ms": best[1]["reads"], "b_write_ms": best[1]["writes"], "c_row_ms": best[1]["rows"], "d_request_ms": best[1]["d"],
            "weighted_r2": round(1 - best[0] / sst, 6) if sst else None}


def kendall_tau(x: list[float], y: list[float]) -> float:
    n, s = len(x), 0
    for i in range(n):
        for j in range(i + 1, n):
            a, b = x[i] - x[j], y[i] - y[j]
            s += (a > 0) - (a < 0) if b > 0 else ((a < 0) - (a > 0) if b < 0 else 0)
    pairs = n * (n - 1) / 2
    return s / pairs if pairs else 0.0


def per_deployment(pts: dict, coef: dict) -> dict:
    out = defaultdict(dict)
    acc = defaultdict(lambda: [0, 0.0, 0.0])
    for (dep, w, g), p in pts.items():
        a = acc[(dep, w)]
        a[0] += p["n"]
        a[1] += p["n"] * p["lat"]
        a[2] += p["n"] * (coef["a_read_ms"] * p["reads"] + coef["b_write_ms"] * p["writes"] + coef["c_row_ms"] * p["rows"])
    for (dep, w), (n, lat, pred) in acc.items():
        out[w][dep] = {"measured_ms": lat / n, "predicted_cost": pred / n}
    return out


def calibrate() -> dict:
    pts, files = load_points()
    coef = nnls_fit(pts)
    deps = sorted({k[0] for k in pts})
    pdw = per_deployment(pts, coef)
    tau = {w: round(kendall_tau([pdw[w][d]["predicted_cost"] for d in deps], [pdw[w][d]["measured_ms"] for d in deps]), 4) for w in WORKLOADS}
    loo_pred = defaultdict(dict)
    loo_coefs = {}
    for d in deps:
        c = nnls_fit(pts, [k for k in pts if k[0] != d])
        loo_coefs[d] = c
        pd = per_deployment({k: v for k, v in pts.items() if k[0] == d}, c)
        for w in WORKLOADS:
            loo_pred[w][d] = pd[w][d]["predicted_cost"]
    tau_loo = {w: round(kendall_tau([loo_pred[w][d] for d in deps], [pdw[w][d]["measured_ms"] for d in deps]), 4) for w in WORKLOADS}
    return {"calibrated_at": now_iso(), "source_files": files, "deployments": deps, "points": len(pts), "coefficients": coef,
            "per_workload": pdw, "kendall_tau": tau, "kendall_tau_leave_one_out": tau_loo, "loo_coefficients": loo_coefs}


TAU_MIN = 0.6   # D-V9-009: minimum Kendall τ (all 12 schemas, every workload) to accept the model


def stage_e14():
    from .stages import stage_dir
    r = calibrate()
    comp = [d for d in r["deployments"] if d not in ("Normalized", "Expert")]
    r["kendall_tau_competitive_only"] = {w: round(kendall_tau([r["per_workload"][w][d]["predicted_cost"] for d in comp],
                                                              [r["per_workload"][w][d]["measured_ms"] for d in comp]), 4) for w in WORKLOADS}
    r["competitive_deployments"] = comp
    r["acceptance_rule"] = f"τ ≥ {TAU_MIN} em todas as cargas, com e sem deixar um esquema de fora"
    out = stage_dir("E14") / "COST_MODEL.json"
    write_json(out, r)
    prov = RUN / "provenance/V8_INPUTS.json"
    d = read_json(prov, {"files": {}})
    for f in r["source_files"]:
        d["files"].setdefault(f, {"sha256": sha256_file(Path(f)), "read_by": "E14 (calibração)"})
    write_json(prov, d)
    checks = {"tau_all": r["kendall_tau"], "tau_leave_one_out": r["kendall_tau_leave_one_out"],
              "tau_competitive_only": r["kendall_tau_competitive_only"], "r2_weighted": r["coefficients"]["weighted_r2"],
              "tau_ok": all(v >= TAU_MIN for v in list(r["kendall_tau"].values()) + list(r["kendall_tau_leave_one_out"].values()))}
    return ("PASS" if checks["tau_ok"] else "FAIL"), [out], checks, {}
