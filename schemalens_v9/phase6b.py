"""E21 — analysis of the cohort benchmark. Every number is computed from E20 RESULT files and raw data."""
from __future__ import annotations

import statistics

from . import phase6 as B
from .common import RUN, read_json, write_json
from .cost import kendall_tau
from .stages import stage_dir

WORKLOADS = B.WORKLOADS
T975_4 = 2.776   # t(0.975, df=4), n = 5 repetitions
EXTERNAL = ("NoSE", "QPG", "V8_AUTO_H_norm")


def tci(v):
    m = statistics.fmean(v)
    sd = statistics.stdev(v) if len(v) > 1 else 0.0
    h = T975_4 * sd / len(v) ** 0.5
    return {"mean": m, "ci95_low": m - h, "ci95_high": m + h, "sd": sd, "n": len(v)}


def verdict(c, a="H", b="ref"):
    if c["ci95_high"] < 0:
        return f"{a} mais rápido"
    if c["ci95_low"] > 0:
        return f"{a} mais lento"
    return "empate"


def e21():
    seq = B.order()
    res = {d: read_json(B.OUT / f"{d}_RESULT.json") for d in seq}
    if any((r or {}).get("status") != "PASS" for r in res.values()):
        raise RuntimeError("E20 incompleto")
    H = read_json(RUN / "stages/E15_composition/H.json")["H"]
    hid = [h["schema_id"] for h in H]
    pred = {h["schema_id"]: {w: h["predicted_cost"][w]["total"] for w in WORKLOADS} for h in H}
    rep = {d: {w: [x["mean_ms"] for x in sorted(res[d]["summaries"], key=lambda x: x["repetition"]) if x["workload"] == w] for w in WORKLOADS} for d in seq}
    per = {}
    for d in seq:
        per[d] = {}
        for w in WORKLOADS:
            S = [x for x in res[d]["summaries"] if x["workload"] == w]
            n = sum(x["measured_requests"] for x in S)
            per[d][w] = {**tci(rep[d][w]), **{k: statistics.fmean(x[k] for x in S) for k in ("p50_ms", "p95_ms", "p99_ms")},
                         **{k + "_per_request": sum(x[k] for x in S) / n for k in ("physical_reads", "physical_writes", "support_reads", "rows_fetched", "rows_returned")}}
    contrasts = {}
    for h in hid:
        for ref in EXTERNAL:
            for w in WORKLOADS:
                c = tci([a - b for a, b in zip(rep[h][w], rep[ref][w])])
                contrasts.setdefault(h, {}).setdefault(ref, {})[w] = {"diff_ms": c, "relative": c["mean"] / per[ref][w]["mean"], "verdict_95": verdict(c)}
    # internal (within H): predicted vs measured order, top-k, regret, near-best, Htop
    internal = {}
    for w in WORKLOADS:
        meas = {h: per[h][w]["mean"] for h in hid}
        best = min(hid, key=lambda h: meas[h])
        pbest = min(hid, key=lambda h: pred[h][w])
        rank_m = sorted(hid, key=lambda h: meas[h])
        rank_p = sorted(hid, key=lambda h: pred[h][w])
        internal[w] = {"measured_order": rank_m, "predicted_order": rank_p,
                       "kendall_tau_predicted_vs_measured": round(kendall_tau([pred[h][w] for h in hid], [meas[h] for h in hid]), 4),
                       "top1_preserved": best == pbest, "top3_overlap": len(set(rank_m[:3]) & set(rank_p[:3])),
                       "regret_ms": meas[pbest] - meas[best], "regret_relative": (meas[pbest] - meas[best]) / meas[best],
                       "near_best_5pct": [h for h in hid if meas[h] <= 1.05 * meas[best]],
                       "near_best_note": "conjunto a até 5% do melhor medido; não é prova de equivalência"}
    vec = {h: [per[h][w]["mean"] for w in WORKLOADS] for h in hid}
    htop = [h for h in hid if not any(all(vec[o][i] <= vec[h][i] for i in range(4)) and any(vec[o][i] < vec[h][i] for i in range(4)) for o in hid if o != h)]
    # external: best measured H per workload vs NoSE/QPG (separate denominators)
    external = {w: {"best_H": min(hid, key=lambda h: per[h][w]["mean"]),
                    "vs": {ref: contrasts[min(hid, key=lambda h: per[h][w]["mean"])][ref][w]["verdict_95"] for ref in EXTERNAL}} for w in WORKLOADS}
    # write amplification: predicted physical writes per write statement vs measured
    wa = {}
    for h in H:
        exp = sum(p["writes"] for k, p in h["routes"].items() if "tables" in p)
        wa[h["schema_id"]] = {"estimated_writes_sum_over_8_writeplans": exp,
                              "measured_physical_writes_per_request": {w: per[h["schema_id"]][w]["physical_writes_per_request"] for w in WORKLOADS}}
    out = {"order": seq, "per_deployment": per, "paired_contrasts": contrasts, "internal_H": internal, "Htop_non_dominated": htop,
           "external": external, "write_amplification": wa, "predicted_cost": pred,
           "unit": "médias por repetição (n = 5); IC 95% t com 4 graus de liberdade sobre a diferença pareada por repetição",
           "denominators": {"internal": f"{len(hid)} esquemas de H", "external": "melhor esquema de H em cada carga × NoSE, QPG e SL norm da V8"}}
    p = stage_dir("E21") / "ANALYSIS.json"
    write_json(p, out)
    checks = {"Htop": htop, "kendall_tau_internal": {w: internal[w]["kendall_tau_predicted_vs_measured"] for w in WORKLOADS},
              "top1_preserved": {w: internal[w]["top1_preserved"] for w in WORKLOADS}}
    return "PASS", [p], checks, {}
