"""E22 runner: analytical ablations -> materialise changed schemas -> oracle check -> timed window vs counterpart."""
from __future__ import annotations

import json
import shutil
import statistics
import time

from . import isolation
from . import phase5 as M
from . import phase5_run as PR
from . import phase6 as B
from . import phase7 as A
from .common import RUN, now_iso, read_json, write_json
from .phase6b import tci, verdict
from .stages import incident, load_state, run_stage, save_state, set_stage, stage_dir

OUT = RUN / "stages/E22_criteria_ablations"
W = B.WORKLOADS


def ks(aid):
    return f"rubis_v9_abl_{aid.lower()}"


def e22():
    an = A.analytic()
    db = A.decision_baselines()
    plan = A.physical_plan(an, db)
    slim = {k: {kk: vv for kk, vv in r.items() if kk != "C_tables"} for k, r in an["criteria"].items()}
    write_json(OUT / "ABLATIONS_ANALYTIC.json", {"generated_at": an["generated_at"], "full_H": an["full_H"], "criteria": slim,
                                                 "decision_baselines": db["baselines"]})
    write_json(OUT / "PHYSICAL_PLAN.json", [{k: v for k, v in p.items() if k not in ("routes", "ddl")} for p in plan])
    raw, ids, state = PR.cohort()
    H, Cfull = PR.h_schemas()
    full_by_label = {h["label"]: h for h in H}
    cl, s = M.session()
    corr = {}
    try:
        for i, p in enumerate(plan):
            set_stage("E22", progress={"text": f"materializando e verificando {p['id']} ({i + 1} de {len(plan)})", "done": i, "total": len(plan)})
            Ct = an["criteria"][p["c_from"]]["C_tables"] if p["c_from"] else db["C_tables"]
            tabs = {t: Ct[t] for t in p["canonical_ids"]}
            M.materialize_generated(s, ks(p["id"]), list(tabs.values()), state)
            ad = M.GeneratedAdapter(s, ks(p["id"]), {"routes": p["routes"]}, tabs, state)
            r = PR.replay(ad, state, PR.v8_reference(), p["id"], "E22")
            corr[p["id"]] = {k: r[k] for k in ("evaluations", "agree_oracle", "agree_v8_reference", "first_mismatches")}
            p["tables_dict"] = tabs
    finally:
        cl.shutdown()
    write_json(OUT / "CORRECTNESS.json", corr)
    ok_plan = [p for p in plan if corr[p["id"]]["agree_oracle"] == corr[p["id"]]["evaluations"]]
    blocked = [p["id"] for p in plan if p not in ok_plan]
    counterpart = full_by_label["opt_bidding"]
    seq = [p["id"] for p in ok_plan]
    seq.insert(min(2, len(seq)), counterpart["schema_id"])
    raw_dir = OUT / "raw"
    for attempt in range(1, 6):
        results, invalid = {}, None
        cl, s = M.session()
        try:
            B.wait_idle_stage = None
            _wait_idle()
            for i, dep in enumerate(seq):
                if dep == counterpart["schema_id"]:
                    ad = M.GeneratedAdapter(s, PR.ks_of(dep), counterpart, {t: Cfull[t] for t in counterpart["canonical_ids"]}, state)
                    k = PR.ks_of(dep)
                else:
                    p = next(x for x in ok_plan if x["id"] == dep)
                    ad = M.GeneratedAdapter(s, ks(dep), {"routes": p["routes"]}, p["tables_dict"], state)
                    k = ks(dep)
                with isolation.BulkWriterWatch() as watch:
                    res = B.run_protocol(ad, dep, k, i, len(seq), stage="E22", raw_dir=raw_dir)
                iso = watch.report()
                agr = B.agreement(res["raw_files"], B.reference_hashes())
                results[dep] = {"summaries": res["summaries"], "raw_files": res["raw_files"], "isolation": iso, "agreement": agr,
                                "errors": sum(x["errors"] for x in res["summaries"])}
                if not iso["clean"]:
                    invalid = dep
                    break
        finally:
            cl.shutdown()
        if invalid is None:
            break
        st = load_state()
        st.setdefault("invalidated_measurements", []).append({"deployment": f"E22:{invalid}", "at": now_iso()})
        save_state(st)
        incident(f"E22: medição de {invalid} invalidada por carga concorrente; janela recomeçada")
        if raw_dir.exists():
            shutil.move(str(raw_dir), OUT / f"raw_discarded_{attempt}")
    else:
        return "FAIL", [], {"invalidated_repeatedly": True}, {}
    rep = {d: {w: [x["mean_ms"] for x in sorted(r["summaries"], key=lambda x: x["repetition"]) if x["workload"] == w] for w in W} for d, r in results.items()}
    cp = counterpart["schema_id"]
    contrasts = {}
    for p in ok_plan:
        d = p["id"]
        contrasts[d] = {"criteria": p["criteria"], "label": p["label"], "vs": cp,
                        "per_workload": {w: {"ablated_ms": statistics.fmean(rep[d][w]), "full_ms": statistics.fmean(rep[cp][w]),
                                             "diff_ms": tci([a - b for a, b in zip(rep[d][w], rep[cp][w])]),
                                             "verdict_95": verdict(tci([a - b for a, b in zip(rep[d][w], rep[cp][w])]), "ablado")} for w in W},
                        "reads_per_request": {w: sum(x["physical_reads"] for x in results[d]["summaries"] if x["workload"] == w) / 50000 for w in W},
                        "writes_per_request": {w: sum(x["physical_writes"] for x in results[d]["summaries"] if x["workload"] == w) / 50000 for w in W}}
    phys = {"window": {d: results[d]["isolation"] for d in results}, "order": seq, "counterpart": cp, "contrasts": contrasts,
            "agreement": {d: results[d]["agreement"]["status"] for d in results}, "errors": {d: results[d]["errors"] for d in results},
            "blocked_by_oracle": blocked, "measured_at": now_iso()}
    write_json(OUT / "ABLATIONS_PHYSICAL.json", phys)
    checks = {"criteria": len(A.CRITERIA), "null_results": [k for k, r in slim.items() if r["null_result"]],
              "physical_schemas_measured": len(ok_plan), "blocked": blocked,
              "agreement_all": all(v == "PASS" for v in phys["agreement"].values())}
    ok = checks["agreement_all"] and not any(phys["errors"].values())
    return ("PASS" if ok else "FAIL"), [OUT / "ABLATIONS_ANALYTIC.json", OUT / "ABLATIONS_PHYSICAL.json"], checks, {}


def _wait_idle():
    t0 = None
    while True:
        snap = isolation.snapshot()
        if snap["idle"]:
            t0 = t0 or time.time()
            set_stage("E22", progress={"text": f"aguardando nó ocioso: {int(time.time() - t0)} de {B.IDLE_S} s", "done": 0, "total": 1})
            if time.time() - t0 >= B.IDLE_S:
                return snap
        else:
            t0 = None
        time.sleep(15)


if __name__ == "__main__":
    r = run_stage("E22", e22)
    print(json.dumps({"stage": "E22", "status": r["status"], "checks": r["checks"], "error": r["error"]}, ensure_ascii=False, default=str), flush=True)
